"""
TradingView相关接口蓝图。
(终极修复版：修复 minmov 校验错误 + 全量数据返回 + 内存缓存)
"""
import pytz
import json
import hashlib
import datetime
import time
import threading
import weakref
import numpy as np
import talib
from cachetools import TTLCache
from threading import RLock, Semaphore
import bisect
from flask import Blueprint, request
from flask_login import login_required
import pinyin

from chanlun import config, fun
from chanlun.base import Market
from chanlun.cl_utils import (
    cl_data_to_tv_chart,
    kcharts_frequency_h_l_map,
    query_cl_chart_config,
    web_batch_get_cl_datas,
)
from chanlun.db import db
from chanlun.exchange import get_exchange
from chanlun.tools.log_util import LogUtil

from ..services.constants import (
    frequency_maps,
    resolution_maps,
    market_frequencys,
    market_session,
    market_timezone,
    market_types,
)
from ..services.state import history_req_counter as __history_req_counter


tv_bp = Blueprint("tv", __name__)

# 跨周期 MACD 周期倍率映射表
# key = 当前K线频率, value = 倍率（高级别周期包含多少根当前周期K线）
# 1m→5m: 5倍, 5m→30m: 6倍, 30m→d: 因市场而异
HIGHER_MACD_RATIO = {
    "1m": 5,     # 1分钟 → 5分钟 MACD，参数乘以5
    "5m": 6,     # 5分钟 → 30分钟 MACD，参数乘以6
}

# 30m → 日线的倍率因市场交易时长不同
MARKET_30M_TO_D_RATIO = {
    "a": 8,              # A股 4小时交易 = 8个30分钟
    "hk": 8,             # 港股
    "us": 13,            # 美股 6.5小时 = 13个30分钟
    "futures": 8,        # 国内期货
    "ny_futures": 13,    # 纽约期货
    "currency": 48,      # 数字货币 24小时 = 48个30分钟
    "currency_spot": 48,
    "fx": 48,            # 外汇
}

# 日线 → 周线的倍率
MARKET_D_TO_W_RATIO = {
    "a": 5,              # 5个交易日
    "hk": 5,
    "us": 5,
    "futures": 5,
    "ny_futures": 5,
    "currency": 7,       # 数字货币 7天
    "currency_spot": 7,
    "fx": 5,             # 外汇通常 5天
}

# 基础数据缓存
stock_cache = TTLCache(maxsize=100, ttl=7200)

# 图表数据计算结果缓存 (10 分钟缓存，防止短时间重复计算，减少快速切换周期时的重复计算)
chart_data_cache = TTLCache(maxsize=100, ttl=600)

# Pre-warm cache for common interval switches to reduce recomputation
_MAX_PREWARMED_SIZE = 50  # prewarmed 集合上限，防止线程异常导致无限增长
# 已"在飞行中"的 prewarm cache_key 集合，避免同一个 key 被多个 prewarm 任务重复计算。
# 设计上始终通过 discard 移除，size 不会超过同时 in-flight 的预热任务数（极小）。
chart_data_cache_stats = {"prewarmed": set()}
COMMON_INTERVALS = ["1", "5", "15", "30", "60", "1D", "1W"]  # Commonly switched intervals

# Prewarm 全局并发限制：xtquant native 不是线程安全的，且每次启动 prewarm 会拉 7 个周期，
# 多个 prewarm 线程并发会直接撞 BSON 断言。这里用信号量限制全局只有 1 个 prewarm 在飞行。
_PREWARM_MAX_CONCURRENT = 1
_prewarm_semaphore = Semaphore(_PREWARM_MAX_CONCURRENT)
# 记录最近一次 prewarm 的目标 symbol，用户快速切换时旧任务可主动放弃。
_prewarm_latest_target = {"key": None}
_prewarm_target_lock = threading.Lock()

# 缓存数据最近验证时间戳（防止非交易时段 DataPulse 反复 cache miss）
# H4: 验证时间戳直接放在 chart_data_cache 的 entry["validated_at"] 中，
# 不再单独维护 chart_data_validated_at TTLCache：
#   1) 双 TTLCache（600s vs 3600s）/ 双 maxsize（100 vs 500）会形成悬挂键，
#      mark 时拿不到 entry 却仍写独立缓存，永久堆积无对应 cache entry 的孤儿键。
#   2) entry 本身已经存了 validated_at，独立缓存纯属冗余源。
_CACHE_REVALIDATION_INTERVAL = 30  # 秒，缓存在此时间内被验证过则视为有效

cache_lock = RLock()
# 注：原 req_lock 全局 RLock 已被 H7 拆为 per-key 的 _history_req_locks（见下方），
# 全文已无任何引用，故移除避免误用。


def _stable_hash(obj) -> str:
    """
    生成稳定的 hash（不受 PYTHONHASHSEED 影响，跨进程/重启一致）。
    这样多 worker 部署、进程重启后 cache_key 仍然稳定，缓存命中率不会被打穿。
    """
    try:
        s = json.dumps(obj, sort_keys=True, default=str)
    except Exception:
        s = str(obj)
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _build_cache_key(market: str, code: str, frequency: str, cl_config: dict) -> str:
    """统一构造 chart_data_cache 的 key，确保所有调用方一致。"""
    return f"{market}_{code}_{frequency}_{_stable_hash(cl_config)}"


def _safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _normalize_unix_ts(value, default=0):
    ts = _safe_int(value, default)
    if ts > 10**12:
        ts = ts // 1000
    return ts


def _normalize_resolution(resolution: str):
    if resolution is None:
        return None
    return {"D": "1D", "W": "1W", "M": "1M"}.get(resolution, resolution)


def _parse_tv_symbol(symbol: str):
    if not symbol:
        return None, None
    symbol = symbol.strip()
    if ":" in symbol:
        market, code = symbol.split(":", 1)
        market = market.lower().strip()
        code = code.strip()
        if market in market_types and code != "":
            return market, code
    upper_symbol = symbol.upper()
    if upper_symbol.endswith(".US"):
        return "us", upper_symbol
    return None, None


def _build_chart_cache_entry(cl_chart_data: dict, is_full_snapshot: bool, validated_at: float = None):
    validated_at = time.time() if validated_at is None else validated_at
    bar_times = cl_chart_data.get("t", []) if isinstance(cl_chart_data, dict) else []
    return {
        "data": cl_chart_data,
        "min_time": bar_times[0] if len(bar_times) > 0 else None,
        "max_time": bar_times[-1] if len(bar_times) > 0 else None,
        "validated_at": validated_at,
        "is_full_snapshot": bool(is_full_snapshot),
    }


def _get_chart_cache_entry(cache_key: str):
    cached = chart_data_cache.get(cache_key)
    if cached is None:
        return None
    if isinstance(cached, dict) and "data" in cached and "validated_at" in cached:
        return cached
    if isinstance(cached, dict):
        # H4: 兼容历史遗留的 raw dict 格式（旧版本 entry 没有 validated_at 字段），
        # 取当前时间兜底，下次 set 自然会规范化。原本依赖的 chart_data_validated_at
        # 独立 TTLCache 已删除（双 TTL 不一致会形成悬挂键）。
        return _build_chart_cache_entry(cached, is_full_snapshot=True, validated_at=time.time())
    return None


def _set_chart_cache_entry(cache_key: str, cl_chart_data: dict, is_full_snapshot: bool):
    entry = _build_chart_cache_entry(cl_chart_data, is_full_snapshot=is_full_snapshot)
    chart_data_cache[cache_key] = entry
    return entry


def _mark_chart_cache_validated(cache_key: str):
    # H4: validated_at 只更新到 entry 内部；entry 本身的 TTL 由 chart_data_cache 统一管理。
    # 若 cache 已被 TTL 淘汰，没有 entry 可标记，直接返回（下次请求自然重算）。
    entry = _get_chart_cache_entry(cache_key)
    if entry is None:
        return
    entry["validated_at"] = time.time()
    chart_data_cache[cache_key] = entry


def _cache_entry_recently_validated(cache_entry: dict):
    validated_at = cache_entry.get("validated_at", 0) if isinstance(cache_entry, dict) else 0
    return (time.time() - validated_at) < _CACHE_REVALIDATION_INTERVAL


def _shape_time(shape):
    if not isinstance(shape, dict):
        return None
    points = shape.get("points")
    if isinstance(points, list) and len(points) > 0:
        last_point = points[-1]
        if isinstance(last_point, dict):
            return last_point.get("time")
    if isinstance(points, dict):
        return points.get("time")
    return None


def _merge_shape_lists(existing_shapes, new_shapes):
    if not existing_shapes:
        return list(new_shapes or [])
    if not new_shapes:
        return list(existing_shapes or [])

    new_times = [t for t in (_shape_time(shape) for shape in new_shapes) if t is not None]
    if len(new_times) == 0:
        return list(existing_shapes or [])

    min_time = min(new_times)
    max_time = max(new_times)
    merged = []
    for shape in existing_shapes:
        shape_time = _shape_time(shape)
        if shape_time is None or shape_time < min_time or shape_time > max_time:
            merged.append(shape)
    merged.extend(new_shapes)
    return sorted(merged, key=lambda shape: (_shape_time(shape) or 0))


def _merge_chart_data(existing_data: dict, new_data: dict):
    if not existing_data:
        return new_data
    if not new_data:
        return existing_data

    merged = dict(existing_data)
    merged.update(new_data)

    existing_times = existing_data.get("t", [])
    new_times = new_data.get("t", [])
    all_times = sorted(set(existing_times) | set(new_times))
    merged["t"] = all_times

    aligned_keys = [
        "c",
        "o",
        "h",
        "l",
        "v",
        "macd_dif",
        "macd_dea",
        "macd_hist",
        "macd_area",
        "higher_macd_dif",
        "higher_macd_dea",
        "higher_macd_hist",
    ]
    for key in aligned_keys:
        existing_values = existing_data.get(key, [])
        new_values = new_data.get(key, [])
        if not existing_values and not new_values:
            merged[key] = []
            continue
        merged_values = {}
        for idx, bar_time in enumerate(existing_times):
            if idx < len(existing_values):
                merged_values[bar_time] = existing_values[idx]
        for idx, bar_time in enumerate(new_times):
            if idx < len(new_values):
                val = new_values[idx]
                # 仅当新值有效时才覆盖，避免 None 覆盖已有的有效值
                if val is not None:
                    merged_values[bar_time] = val
                elif bar_time not in merged_values:
                    merged_values[bar_time] = val
        merged[key] = [merged_values.get(bar_time) for bar_time in all_times]

    for key in ["fxs", "bis", "xds", "zsds", "bi_zss", "xd_zss", "zsd_zss", "bcs", "mmds"]:
        merged[key] = _merge_shape_lists(existing_data.get(key, []), new_data.get(key, []))

    return merged


def _drawing_storage_name(chart_id: str, layout_id: str, symbol: str, resolution: str):
    return f"drawings_{layout_id}_{chart_id}_{symbol}_{resolution}"


def _legacy_drawing_storage_name(symbol: str, resolution: str):
    return f"drawings_{symbol}_{resolution}"


def prewarm_common_intervals(market, code, cl_config):
    """
    Pre-warm cache for commonly switched intervals to reduce recomputation on rapid interval switches.
    This is called asynchronously after initial data load.

    关键改动：
    1. 全局信号量 _prewarm_semaphore 限制最多 1 个 prewarm 任务同时跑，避免对 xtquant
       native 客户端的并发冲击（多线程并发是 BSON 断言崩溃的主因之一）。
    2. _prewarm_latest_target 跟踪用户最近查看的标的，用户快速切换时旧的 prewarm
       任务在每个周期处理前会主动放弃，节省资源。
    3. 单个 prewarm 线程内部串行处理多个周期，每个周期之间 sleep 一小段，避免压力过大。
    """
    target_key = f"{market}:{code}:{_stable_hash(cl_config)}"

    # 标记当前最新的目标，旧 prewarm 任务在循环中会自查并退出
    with _prewarm_target_lock:
        _prewarm_latest_target["key"] = target_key

    def _is_still_latest():
        with _prewarm_target_lock:
            return _prewarm_latest_target["key"] == target_key

    def _prewarm():
        # 非阻塞获取信号量；获取不到说明已有 prewarm 在跑，本次直接放弃
        # （新的 prewarm 比旧的更接近用户当前关注点，旧的会通过 _is_still_latest 自动退出）
        if not _prewarm_semaphore.acquire(blocking=False):
            LogUtil.info(f"[tv_history] Prewarm skipped (another in flight) for {market}:{code}")
            return
        try:
            if not _is_still_latest():
                return
            ex = get_exchange(Market(market))
            tz_sh = pytz.timezone("Asia/Shanghai")

            for interval in COMMON_INTERVALS:
                # 用户已切换到其他标的，立即放弃剩余周期
                if not _is_still_latest():
                    LogUtil.info(f"[tv_history] Prewarm aborted (user switched away) for {market}:{code}")
                    return

                cache_key = None
                try:
                    freq = resolution_maps.get(interval, interval)
                    cache_key = _build_cache_key(market, code, freq, cl_config)

                    # Skip if already in cache or being computed
                    with cache_lock:
                        if cache_key in chart_data_cache or cache_key in chart_data_cache_stats["prewarmed"]:
                            continue
                        # 防止 prewarmed 集合因线程异常无限增长
                        if len(chart_data_cache_stats["prewarmed"]) >= _MAX_PREWARMED_SIZE:
                            chart_data_cache_stats["prewarmed"].clear()
                            LogUtil.warning(f"[tv_history] prewarmed 集合已达上限 {_MAX_PREWARMED_SIZE}，已清空")
                        chart_data_cache_stats["prewarmed"].add(cache_key)

                    kline_args = {
                        'end_date': datetime.datetime.now(tz_sh).strftime("%Y-%m-%d %H:%M:%S")
                    }

                    to_frequency = None
                    if cl_config.get("enable_kchart_low_to_high") == "1":
                        frequency_low, to_frequency = kcharts_frequency_h_l_map(market, freq)
                        if frequency_low is not None and to_frequency is not None:
                            klines = ex.klines(code, frequency_low, **kline_args)
                            cd = web_batch_get_cl_datas(market, code, {frequency_low: klines}, cl_config)[0]
                        else:
                            klines = ex.klines(code, freq, **kline_args)
                            cd = web_batch_get_cl_datas(market, code, {freq: klines}, cl_config)[0]
                            to_frequency = None
                    else:
                        klines = ex.klines(code, freq, **kline_args)
                        cd = web_batch_get_cl_datas(market, code, {freq: klines}, cl_config)[0]

                    cl_chart_data = cl_data_to_tv_chart(cd, cl_config, to_frequency=to_frequency)
                    if cl_chart_data is None:
                        with cache_lock:
                            chart_data_cache_stats["prewarmed"].discard(cache_key)
                        continue

                    with cache_lock:
                        _set_chart_cache_entry(cache_key, cl_chart_data, is_full_snapshot=True)
                        chart_data_cache_stats["prewarmed"].discard(cache_key)
                        LogUtil.info(f"[tv_history] Pre-warmed cache for {market}:{code} interval {interval}")

                    # 每个周期之间小憩，避免对 native 行情接口持续压力
                    time.sleep(0.2)
                except Exception as e:
                    if cache_key is not None:
                        with cache_lock:
                            chart_data_cache_stats["prewarmed"].discard(cache_key)
                    LogUtil.error(f"[tv_history] Pre-warm failed for {interval}: {e}")
        except Exception as e:
            LogUtil.error(f"[tv_history] Pre-warm thread error: {e}")
        finally:
            _prewarm_semaphore.release()

    t = threading.Thread(target=_prewarm, daemon=True, name="IntervalPrewarmThread")
    t.start()
    LogUtil.info(f"[tv_history] Started pre-warm thread for {market}:{code}")


class _SafeLockRegistry:
    """
    线程安全的 per-key 锁注册表，使用 weakref + 引用计数避免锁正确性问题。

    原 _LimitedLockDict 的问题：
    - 用 dict 的插入顺序做 FIFO 淘汰（不是 LRU），热点 key 会被错误淘汰；
    - 更严重的：被淘汰的 RLock 如果还被另一个线程持有，新请求会拿到一把全新的锁，
      导致同一 key 上"两把锁、各串行各的"，破坏 per-key 串行化语义。

    本实现改为：
    - 锁不主动淘汰，借助 WeakValueDictionary 让无引用的锁自然被 GC；
    - 调用方使用 with registry.get(key) as lock: 模式，进入 with 时锁的强引用挂在
      调用栈上，不会被 GC，确保两个并发请求拿到同一把锁。
    """

    def __init__(self):
        self._locks = weakref.WeakValueDictionary()
        self._registry_lock = threading.Lock()

    def get(self, key: str) -> RLock:
        """返回 key 对应的 RLock。调用方应立即用 with 包住，确保引用持续。"""
        with self._registry_lock:
            lock = self._locks.get(key)
            if lock is None:
                lock = RLock()
                self._locks[key] = lock
            return lock

    def __contains__(self, key):
        return key in self._locks

    def __len__(self):
        return len(self._locks)


chart_calc_locks = _SafeLockRegistry()
# H7（保守版）：__history_req_counter 的 per-key 节流锁。
# 原 req_lock 是全局 RLock，所有 (symbol, resolution) 的请求计数访问都串行化，
# 多用户并发翻页历史时形成无谓热点。改 per-key 后不同 symbol/resolution 互不阻塞，
# 同一 key 内仍严格串行，节流语义不变。
# 注意：必须用 with _history_req_locks.get(key) 模式，详见 _SafeLockRegistry 注释。
_history_req_locks = _SafeLockRegistry()

# 全部支持的市场（用于校验配置项）
_ALL_PRELOAD_EXCHANGES = [
    "a", "hk", "fx", "us", "futures", "ny_futures", "currency", "currency_spot",
]

def _resolve_preload_exchanges():
    """从 config 读取 PRELOAD_MARKETS（list[str]）。
    - 未配置时回退默认仅预加载 a/hk/us（避免 futures/currency 这类境外/超时频发的市场拖慢启动）。
    - 配置中包含未知市场名时仅记 warning, 不中断。
    用户用不到的市场可在 config.py 中显式置空 PRELOAD_MARKETS = [] 关闭预加载。
    """
    raw = getattr(config, "PRELOAD_MARKETS", None)
    if raw is None:
        return ["a", "hk", "us"]
    if not isinstance(raw, (list, tuple)):
        LogUtil.warning(f"config.PRELOAD_MARKETS 类型应为 list, 当前为 {type(raw).__name__}, 已忽略并使用默认值")
        return ["a", "hk", "us"]
    valid = []
    for ex in raw:
        if ex in _ALL_PRELOAD_EXCHANGES:
            valid.append(ex)
        else:
            LogUtil.warning(f"config.PRELOAD_MARKETS 包含未知市场 {ex}, 已跳过")
    return valid

PRELOAD_EXCHANGES = _resolve_preload_exchanges()
PRELOAD_INTERVAL_SECONDS = 3600
# 启动后延迟多少秒才开始第一轮预加载, 让启动初期完全静默,
# 也避免抢占用户首次加载页面所需要的 CPU/网络资源。
# 可通过 config.PRELOAD_STARTUP_DELAY_SECONDS 覆盖, 默认 30s。
PRELOAD_STARTUP_DELAY_SECONDS = max(0, int(getattr(config, "PRELOAD_STARTUP_DELAY_SECONDS", 30)))
# 并发度自适应: 不超过待加载市场数量, 也不超过原上限 8。
PRELOAD_PARALLEL_WORKERS = min(8, max(1, len(PRELOAD_EXCHANGES))) if PRELOAD_EXCHANGES else 1

# 单市场单次刷新的最大耗时阈值, 仅用于日志告警, 不会强制 kill 任务。
_PRELOAD_SLOW_WARN_SECONDS = 10

# 正在异步刷新的市场集合, 防止同一市场被并发触发多次刷新而堆积慢请求。
_async_refresh_in_flight = set()
_async_refresh_lock = threading.Lock()

def _safe_all_stocks(ex, exchange: str):
    """统一的 all_stocks 调用入口。

    历史 bug：``ExchangeChangQiao`` 是 ``@fun.singleton``, A/HK/US 三个市场共享同一实例。
    如果只靠实例字段 ``default_market`` 区分市场, 后注册的市场会覆盖前面的字段, 结果
    所有市场拿到的都是最后一个市场的数据 (实测复现)。所以这里强制把 ``exchange`` 作为
    参数传下去——cq 实现的 ``all_stocks`` 接受可选 ``market`` 参数; 其它 exchange 的
    签名是无参的, 用 try/except 优雅降级即可。
    """
    try:
        return ex.all_stocks(exchange)
    except TypeError:
        # 大多数非 cq 的 exchange 是无参签名, 退回原行为。
        return ex.all_stocks()

def _preload_single_exchange(exchange: str) -> None:
    """加载单个市场的 symbol 列表并写入缓存。任何异常都被吞掉，仅记日志，避免影响其他市场。"""
    try:
        start_ts = time.time()
        ex = get_exchange(Market(exchange))
        # 短路: 如果交易所实例标记了 init_failed (例如通达信连接超时),
        # 直接跳过 all_stocks 调用, 避免再次 30s 阻塞。
        if getattr(ex, "init_failed", False):
            LogUtil.warning(f"市场 {exchange} 交易所初始化失败, 跳过本次预加载")
            return
        all_stocks = _safe_all_stocks(ex, exchange)
        processed_stocks = _process_stock_list(all_stocks)
        with cache_lock:
            stock_cache[exchange] = processed_stocks
        elapsed = time.time() - start_ts
        log_fn = LogUtil.warning if elapsed > _PRELOAD_SLOW_WARN_SECONDS else LogUtil.info
        log_fn(
            f"市场 {exchange} symbols 预加载完成，共 {len(processed_stocks)} 条，耗时 {elapsed:.2f}s"
        )
    except Exception as e:
        LogUtil.error(f"预加载市场 {exchange} symbols 失败: {e}")

def preload_symbols():
    """常驻线程入口：并行预加载配置中的市场 symbols，并周期性刷新。

    设计要点：
    1. 启动后先静默 PRELOAD_STARTUP_DELAY_SECONDS 秒再开始第一轮, 让启动初期日志干净、
       不和用户首次访问页面争抢资源。
    2. 每轮使用 ThreadPoolExecutor 并行触发, 单轮总耗时由最慢的市场决定。
    3. 单个市场失败不会影响其他市场。
    4. 该函数本身运行在 daemon 线程中, 不阻塞主进程。
    5. 如果 PRELOAD_EXCHANGES 为空, 直接退出线程, 不再周期性刷新。
    """
    # 通过局部导入避免在模块顶层增加额外依赖项的可见性
    from concurrent.futures import ThreadPoolExecutor

    if not PRELOAD_EXCHANGES:
        LogUtil.info("config.PRELOAD_MARKETS 为空, 跳过 symbols 预加载线程")
        return

    # 启动延迟: 让 web 服务先静默就绪, 用户可立即开始使用 (按需触发的市场会走异步刷新路径)。
    if PRELOAD_STARTUP_DELAY_SECONDS > 0:
        time.sleep(PRELOAD_STARTUP_DELAY_SECONDS)

    while True:
        LogUtil.info("开始预加载并更新所有市场的 symbols（并行）...")
        round_start = time.time()
        try:
            with ThreadPoolExecutor(
                max_workers=PRELOAD_PARALLEL_WORKERS,
                thread_name_prefix="SymbolPreloadWorker",
            ) as pool:
                # 提交后等待所有 future 结束（_preload_single_exchange 已自行吞异常）
                list(pool.map(_preload_single_exchange, PRELOAD_EXCHANGES))
        except Exception as e:
            LogUtil.error(f"symbols 预加载轮次异常: {e}")
        LogUtil.info(f"本轮 symbols 预加载完成，总耗时 {time.time() - round_start:.2f}s")

        time.sleep(PRELOAD_INTERVAL_SECONDS)

def start_symbol_preload_thread():
    """启动后台 symbol 预加载线程。线程为 daemon，不会阻塞进程退出，也不会阻塞调用方。"""
    t = threading.Thread(target=preload_symbols, daemon=True, name="SymbolPreloadThread")
    t.start()
    return t

@tv_bp.route("/tv/config")
@login_required
def tv_config():
    frequencys = list(
        set(market_frequencys["a"])
        | set(market_frequencys["hk"])
        | set(market_frequencys["fx"])
        | set(market_frequencys["us"])
        | set(market_frequencys["futures"])
        | set(market_frequencys["currency"])
        | set(market_frequencys["currency_spot"])
    )
    supportedResolutions = [v for k, v in frequency_maps.items() if k in frequencys]
    return {
        "supports_search": True,
        "supports_group_request": False,
        "supported_resolutions": supportedResolutions,
        "supports_marks": True,
        "supports_timescale_marks": True,
        "supports_time": False,
        "exchanges": [
            {"value": "a", "name": "沪深", "desc": "沪深A股"},
            {"value": "hk", "name": "港股", "desc": "港股"},
            {"value": "fx", "name": "外汇", "desc": "外汇"},
            {"value": "us", "name": "美股", "desc": "美股"},
            {"value": "futures", "name": "国内期货", "desc": "国内期货"},
            {"value": "ny_futures", "name": "纽约期货", "desc": "纽约期货"},
            {
                "value": "currency",
                "name": "数字货币(Futures)",
                "desc": "数字货币（合约）",
            },
            {
                "value": "currency_spot",
                "name": "数字货币(Spot)",
                "desc": "数字货币（现货）",
            },
        ],
    }


@tv_bp.route("/tv/symbol_info")
@login_required
def tv_symbol_info():
    group = request.args.get("group")
    try:
        all_symbols = get_cached_processed_stocks(group)
    except Exception:
        ex = get_exchange(Market(group))
        all_symbols = _safe_all_stocks(ex, group)

    info = {
        "symbol": [s["code"] for s in all_symbols],
        "description": [s["name"] for s in all_symbols],
        "exchange-listed": group,
        "exchange-traded": group,
    }
    return info


@tv_bp.route("/tv/symbols")
@login_required
def tv_symbols():
    raw_symbol: str = request.args.get("symbol", "")
    market, code = _parse_tv_symbol(raw_symbol)
    if market is None or code is None:
        return {"s": "error", "errmsg": f"invalid symbol: {raw_symbol}"}

    try:
        ex = get_exchange(Market(market))
    except Exception as e:
        LogUtil.error(f"[tv_symbols] get_exchange failed symbol={raw_symbol} err={e}")
        return {"s": "error", "errmsg": "invalid market"}

    stocks = ex.stock_info(code)
    if stocks is None:
        return {"s": "error", "errmsg": f"unknown symbol: {raw_symbol}"}

    if "code" not in stocks:
        stocks["code"] = code
    if "name" not in stocks:
        stocks["name"] = code

    sector = ""
    industry = ""
    if market == "a":
        try:
            gnbk = ex.stock_owner_plate(code)
            sector = " / ".join([_g["name"] for _g in gnbk["GN"]])
            industry = " / ".join([_h["name"] for _h in gnbk["HY"]])
        except Exception:
            pass

    # --- 修复 minmov 错误的关键部分 ---
    # 获取 precision，默认值设为 100 (2位小数)
    precision = stocks.get("precision")
    if precision is None:
        precision = 100
    else:
        try:
            precision = int(precision)
            if precision <= 0:
                precision = 100
        except:
            precision = 100
    # --------------------------------

    info = {
        "name": stocks["code"],
        "ticker": f"{market}:{stocks['code']}",
        "full_name": f"{market}:{stocks['code']}",
        "description": stocks["name"],
        "exchange": market,
        "type": market_types.get(market, "stock"),
        "session": market_session.get(market, "24x7"),
        "timezone": market_timezone.get(market, "Asia/Shanghai"),

        # 显式设置 minmov 和 pricescale
        "minmov": 1,
        "pricescale": precision,

        "visible_plots_set": "ohlcv",
        "supported_resolutions": [
            v for k, v in frequency_maps.items() if k in market_frequencys.get(market, [])
        ],
        "intraday_multipliers": ["1", "5", "15", "30", "60"],
        "has_intraday": True,
        "has_seconds": True if market in ["futures", "ny_futures"] else False,
        "has_daily": True,
        "has_weekly_and_monthly": True,
        "sector": sector,
        "industry": industry,
    }
    return info


def _process_stock_list(all_stocks):
    processed_list = []
    for stock in all_stocks:
        stock_copy = stock.copy()
        stock_copy['code_lower'] = stock['code'].lower()
        stock_copy['name_lower'] = stock['name'].lower()
        try:
            stock_copy['pinyin_initials'] = "".join([
                pinyin.get_initial(_p)[0] for _p in stock["name"]
            ]).lower()
        except Exception:
            stock_copy['pinyin_initials'] = ""
        processed_list.append(stock_copy)
    return processed_list


def _trigger_async_refresh(exchange: str) -> None:
    """触发后台线程异步刷新该市场的 symbols, 不阻塞调用方。
    使用 _async_refresh_in_flight 集合做单例化, 避免同一市场并发刷新堆积。
    """
    with _async_refresh_lock:
        if exchange in _async_refresh_in_flight:
            return
        _async_refresh_in_flight.add(exchange)

    def _worker():
        try:
            _preload_single_exchange(exchange)
        finally:
            with _async_refresh_lock:
                _async_refresh_in_flight.discard(exchange)

    t = threading.Thread(target=_worker, daemon=True, name=f"SymbolAsyncRefresh-{exchange}")
    t.start()

def get_cached_processed_stocks(exchange, allow_sync_fallback: bool = False):
    """获取指定市场已缓存的 symbols 列表。

    设计要点 (启动慢优化):
    - 缓存命中: 直接返回。
    - 缓存 miss + ``allow_sync_fallback=False``: 触发后台异步刷新并 raise, 适合那些"宁可
      报错也不能阻塞"的入口 (如图表页面初始化时的 symbol_info 探测)。
    - 缓存 miss + ``allow_sync_fallback=True``: 同步调用一次 ``ex.all_stocks()`` 直接构建
      并写入缓存, 适合"用户主动点搜索"这种愿意等几秒的场景。否则启动后 60s 预加载空窗期内
      搜索接口会全 500, 用户体感是"搜索框完全坏了"。
    - 同步路径里任何异常 (包括交易所连接超时) 都吞掉并返回 ``[]``, 搜索框最差是"无结果"。
    """
    with cache_lock:
        cached = stock_cache.get(exchange)
    if cached is not None:
        return cached

    if not allow_sync_fallback:
        _trigger_async_refresh(exchange)
        raise RuntimeError(
            f"市场 {exchange} symbols 尚未就绪 (后台正在加载, 请稍后重试)"
        )

    # 同步兜底路径: 直接构建一次。即使较慢 (a/hk/us 通常 1~5s), 也比让用户看到 500 好。
    try:
        ex = get_exchange(Market(exchange))
        if getattr(ex, "init_failed", False):
            LogUtil.warning(
                f"[get_cached_processed_stocks] {exchange} 交易所初始化失败, 同步兜底返回空列表"
            )
            return []
        all_stocks = _safe_all_stocks(ex, exchange) or []
        processed = _process_stock_list(all_stocks)
        with cache_lock:
            stock_cache[exchange] = processed
        LogUtil.info(
            f"[get_cached_processed_stocks] {exchange} 同步兜底加载完成, 共 {len(processed)} 条"
        )
        return processed
    except Exception as e:
        LogUtil.error(
            f"[get_cached_processed_stocks] {exchange} 同步兜底加载失败: {e}"
        )
        return []


@tv_bp.route("/tv/search")
@login_required
def tv_search():
    # 关键修复：搜索结果必须严格按"当前页面市场"过滤，避免 A 股搜出美股之类的串市场问题。
    # 触发原因：TradingView 的 Symbol Search 组件默认会传 exchange="" / "All" / 上次选中的
    # 交易所，并不一定等于当前 chart 的 market；若后端不校验直接命中错误缓存或 KeyError 退化，
    # 会让前端 datafeed 回退到内置 symbol 列表（含历史浏览过的其它市场标的）。
    query = (request.args.get("query") or "").strip()
    type_ = request.args.get("type")
    exchange = (request.args.get("exchange") or "").strip().lower()
    try:
        limit = int(request.args.get("limit", "10"))
    except (TypeError, ValueError):
        limit = 10
    if limit <= 0:
        limit = 10

    # exchange 必须是已知市场之一，否则直接拒绝；不要静默回退到任何"看似合理"的市场。
    if exchange not in market_types or exchange not in market_frequencys:
        LogUtil.warning(
            f"[tv_search] reject invalid exchange={exchange!r} query={query!r}"
        )
        return {"error": f"invalid exchange: {exchange!r}"}, 400

    # 空 query 直接返回空列表，避免对几万条 symbol 全量扫描后被 limit 截断成"看起来随机"的结果。
    if not query:
        return []

    # 用 allow_sync_fallback=True: 启动后 60s 预加载空窗期或某个市场首次访问时, 同步加载一次,
    # 避免直接 500。最差情况是返回 [], 搜索框显示"无结果"——优于"接口异常"的体感。
    try:
        processed_stocks = get_cached_processed_stocks(exchange, allow_sync_fallback=True)
    except Exception as e:
        LogUtil.error(f"[tv_search] get stocks failed exchange={exchange}: {e}")
        # 兜底也失败时仍降级为空列表而不是 500, 避免前端 datafeed 抛异常显示"加载错误"。
        processed_stocks = []

    if not processed_stocks:
        # 没有可搜的 symbol 直接返回空, 后续逻辑还有 market_session/market_timezone 取值,
        # 提前返回也能省一次循环。
        return []

    query_lower = query.lower()
    is_currency = exchange in ["currency", "currency_spot"]

    # 优先级：完全相等 > code/拼音前缀 > 任意子串包含。
    # 这样搜 "600" 不会被一堆名字含 600 的票淹没；搜 "中国" 也不会被代码含相同字符的票打乱顺序。
    exact_hits = []
    prefix_hits = []
    contains_hits = []
    for stock in processed_stocks:
        code_l = stock['code_lower']
        name_l = stock['name_lower']
        pinyin_l = stock['pinyin_initials']

        if is_currency:
            if query_lower == code_l:
                exact_hits.append(stock)
            elif code_l.startswith(query_lower):
                prefix_hits.append(stock)
            elif query_lower in code_l:
                contains_hits.append(stock)
        else:
            if query_lower == code_l or query_lower == name_l:
                exact_hits.append(stock)
            elif (code_l.startswith(query_lower)
                  or pinyin_l.startswith(query_lower)
                  or name_l.startswith(query_lower)):
                prefix_hits.append(stock)
            elif (query_lower in code_l
                  or query_lower in name_l
                  or query_lower in pinyin_l):
                contains_hits.append(stock)

        # 早停：精确+前缀已经够用就不再扫剩下的，节省 CPU。
        if len(exact_hits) + len(prefix_hits) >= limit:
            break

    res_stocks = (exact_hits + prefix_hits + contains_hits)[:limit]

    # 用 .get 防御 market_frequencys 中 exchange 因懒加载失败缺键的情况（前面已校验在表内，
    # 但懒加载 build 失败时值会是 []，这里再兜一层就不会抛）。
    supported_resolutions = [
        v for k, v in frequency_maps.items() if k in market_frequencys.get(exchange, [])
    ]
    session_value = market_session.get(exchange, "24x7")
    timezone_value = market_timezone.get(exchange, "Asia/Shanghai")

    infos = []
    for stock in res_stocks:
        infos.append(
            {
                "symbol": stock["code"],
                "name": stock["code"],
                "full_name": f"{exchange}:{stock['code']}",
                "description": stock["name"],
                "exchange": exchange,
                "ticker": f"{exchange}:{stock['code']}",
                "type": type_,
                "session": session_value,
                "timezone": timezone_value,
                "supported_resolutions": supported_resolutions,
            }
        )
    return infos

@tv_bp.route("/tv/history")
@login_required
def tv_history():
    try:
        args = request.args.to_dict()
        symbol = request.args.get("symbol", "")
        resolution = _normalize_resolution(request.args.get("resolution"))
        firstDataRequest = request.args.get("firstDataRequest", "false")
        _from = _normalize_unix_ts(request.args.get("from", "0"))
        _to = _normalize_unix_ts(request.args.get("to", "0"))

        if not symbol or not resolution:
            return {"s": "no_data"}
        if _from < 0 and _to < 0:
            return {"s": "no_data"}

        market, code = _parse_tv_symbol(symbol)
        if market is None or code is None:
            LogUtil.warning(f"[tv_history] invalid symbol: {symbol}")
            return {"s": "no_data"}

        frequency = resolution_maps.get(resolution)
        if frequency is None:
            LogUtil.warning(f"[tv_history] Unsupported resolution: {resolution}")
            return {"s": "no_data"}

        tz_sh = pytz.timezone("Asia/Shanghai")
        log_args = dict(args)
        for key in ("from", "to"):
            if key in log_args:
                ts = _normalize_unix_ts(log_args.get(key))
                if ts > 0:
                    try:
                        log_args[key] = datetime.datetime.fromtimestamp(ts, tz_sh).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    except Exception:
                        pass
        LogUtil.info(f"tv_history request args: {log_args}")

        req_tag = f"{symbol}|{resolution}|{firstDataRequest}|{_from}->{_to}"
        _symbol_res_old_k_time_key = f"{symbol}_{resolution}"
        now_time = time.time()
        if firstDataRequest == "false":
            # H7（保守版）：per-key 锁替代全局 req_lock，不同 symbol/resolution 并发不互斥。
            # 必须先 get 出 RLock 再 with，确保整个临界区内强引用持续（WeakValueDictionary 语义）。
            _req_lock = _history_req_locks.get(_symbol_res_old_k_time_key)
            with _req_lock:
                if _symbol_res_old_k_time_key not in __history_req_counter.keys():
                    __history_req_counter[_symbol_res_old_k_time_key] = {
                        "counter": 0,
                        "tm": now_time,
                    }
                else:
                    if __history_req_counter[_symbol_res_old_k_time_key]["counter"] >= 30:
                        __history_req_counter[_symbol_res_old_k_time_key] = {
                            "counter": 0,
                            "tm": now_time,
                        }
                        LogUtil.warning(
                            f"[tv_history] Too many requests in short time: {_symbol_res_old_k_time_key}, counter reset"
                        )
                    elif (
                        now_time - __history_req_counter[_symbol_res_old_k_time_key]["tm"]
                        <= 5
                    ):
                        __history_req_counter[_symbol_res_old_k_time_key]["counter"] += 1
                        __history_req_counter[_symbol_res_old_k_time_key]["tm"] = now_time
                    else:
                        __history_req_counter[_symbol_res_old_k_time_key] = {
                            "counter": 0,
                            "tm": now_time,
                        }

        cl_config = query_cl_chart_config(market, code)
        if not isinstance(cl_config, dict):
            cl_config = {}
        # 使用稳定 hash 构造 cache_key（不受 PYTHONHASHSEED 影响，进程重启后仍一致）
        cache_key = _build_cache_key(market, code, frequency, cl_config)

        cl_chart_data = None
        is_cache_hit = False
        cache_miss_reason = "cache_empty"
        is_range_request = (
            firstDataRequest == "false"
            and _from > 0
            and _to > 0
            and _to >= _from
        )

        # 内联函数：根据当前缓存项判断是否命中。提取出来供 double-check locking 复用。
        def _evaluate_cache(_cache_entry):
            if _cache_entry is None:
                return False, None, "cache_empty"
            cached_data = _cache_entry.get("data", {})
            cache_min_time = _cache_entry.get("min_time")
            cache_max_time = _cache_entry.get("max_time")
            if not is_range_request:
                if _cache_entry.get("is_full_snapshot", False):
                    return True, cached_data, None
                return False, None, "cache_partial_snapshot"
            if cache_min_time is None or cache_max_time is None:
                return False, None, "cache_no_coverage"
            if _from < cache_min_time:
                return False, None, "cache_head_gap"
            if _to > cache_max_time:
                if _cache_entry_recently_validated(_cache_entry):
                    return True, cached_data, None
                return False, None, "cache_tail_gap"
            return True, cached_data, None

        # 注意：必须先 get 出 RLock 对象再 with，确保整个临界区内引用持续存在
        # （_SafeLockRegistry 用 WeakValueDictionary 存储锁，无强引用会被 GC）
        _calc_lock = chart_calc_locks.get(cache_key)
        with _calc_lock:
            with cache_lock:
                cache_entry = _get_chart_cache_entry(cache_key)
                is_cache_hit, cl_chart_data, miss_reason = _evaluate_cache(cache_entry)
                if not is_cache_hit:
                    cache_miss_reason = miss_reason

            if not is_cache_hit:
                LogUtil.info(f"[tv_history] Cache miss ({cache_miss_reason}) req={req_tag}")
                ex = get_exchange(Market(market))
                frequency_low, kchart_to_frequency = kcharts_frequency_h_l_map(market, frequency)
                kline_args = {}
                if is_range_request:
                    kline_args["start_date"] = datetime.datetime.fromtimestamp(
                        _from, tz=tz_sh
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    kline_args["end_date"] = datetime.datetime.fromtimestamp(
                        _to, tz=tz_sh
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    LogUtil.info(
                        f"[tv_history] incremental request {code} range: {kline_args['start_date']} -> {kline_args['end_date']}"
                    )
                else:
                    kline_args["end_date"] = datetime.datetime.now(tz_sh).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )

                if (
                    cl_config.get("enable_kchart_low_to_high") == "1"
                    and kchart_to_frequency is not None
                    and frequency_low is not None
                ):
                    klines = ex.klines(code, frequency_low, **kline_args)
                    if klines is None or len(klines) == 0:
                        with cache_lock:
                            _mark_chart_cache_validated(cache_key)
                        return {"s": "no_data"}
                    cd = web_batch_get_cl_datas(
                        market, code, {frequency_low: klines}, cl_config
                    )[0]
                else:
                    kchart_to_frequency = None
                    klines = ex.klines(code, frequency, **kline_args)
                    if klines is None or len(klines) == 0:
                        with cache_lock:
                            _mark_chart_cache_validated(cache_key)
                        return {"s": "no_data"}
                    cd = web_batch_get_cl_datas(
                        market, code, {frequency: klines}, cl_config
                    )[0]

                if _to > 0 and len(klines) > 0 and _to < fun.datetime_to_int(klines.iloc[0]["date"]):
                    with cache_lock:
                        _mark_chart_cache_validated(cache_key)
                    return {"s": "no_data"}

                cl_chart_data = cl_data_to_tv_chart(
                    cd, cl_config, to_frequency=kchart_to_frequency
                )
                if cl_chart_data is None:
                    with cache_lock:
                        _mark_chart_cache_validated(cache_key)
                    return {"s": "no_data"}

                ratio = HIGHER_MACD_RATIO.get(frequency)
                if ratio is None and frequency == "30m":
                    ratio = MARKET_30M_TO_D_RATIO.get(market, 8)
                elif ratio is None and frequency == "d":
                    ratio = MARKET_D_TO_W_RATIO.get(market, 5)
                elif ratio is None and frequency == "w":
                    ratio = 4
                elif ratio is None and frequency == "m":
                    ratio = 12

                if ratio is not None:
                    try:
                        closes = np.array(cl_chart_data.get("c", []), dtype=float)
                        fast = int(cl_config.get("idx_macd_fast", 12)) * ratio
                        slow = int(cl_config.get("idx_macd_slow", 26)) * ratio
                        signal = int(cl_config.get("idx_macd_signal", 9)) * ratio
                        # 确保有足够的 bar 数量用于 MACD 计算（至少需要 slow+signal 根K线）
                        min_bars = slow + signal
                        if len(closes) > min_bars:
                            h_dif, h_dea, h_hist = talib.MACD(
                                closes,
                                fastperiod=fast,
                                slowperiod=slow,
                                signalperiod=signal,
                            )
                            h_dif_rounded = np.round(h_dif, 6)
                            h_dea_rounded = np.round(h_dea, 6)
                            h_hist_rounded = np.round(h_hist, 6)
                            cl_chart_data["higher_macd_dif"] = np.where(
                                np.isnan(h_dif_rounded), None, h_dif_rounded
                            ).tolist()
                            cl_chart_data["higher_macd_dea"] = np.where(
                                np.isnan(h_dea_rounded), None, h_dea_rounded
                            ).tolist()
                            cl_chart_data["higher_macd_hist"] = np.where(
                                np.isnan(h_hist_rounded), None, h_hist_rounded
                            ).tolist()
                    except Exception as e:
                        LogUtil.error(f"[tv_history] Scaled MACD calc failed: {e}")

                with cache_lock:
                    existing_entry = _get_chart_cache_entry(cache_key)
                    if existing_entry is not None:
                        cl_chart_data = _merge_chart_data(
                            existing_entry.get("data", {}), cl_chart_data
                        )
                    _set_chart_cache_entry(
                        cache_key,
                        cl_chart_data,
                        is_full_snapshot=(not is_range_request) or (existing_entry or {}).get("is_full_snapshot", False),
                    )

                # firstDataRequest 成功后，后台预热其他常用周期的缓存
                if firstDataRequest == "true":
                    prewarm_common_intervals(market, code, cl_config)

        if cl_chart_data is None:
            return {"s": "no_data"}

        bar_times = cl_chart_data.get("t", [])
        if not is_cache_hit:
            _fxs_cnt = len(cl_chart_data.get("fxs", []))
            _bis_cnt = len(cl_chart_data.get("bis", []))
            _xds_cnt = len(cl_chart_data.get("xds", []))
            LogUtil.info(
                f"[tv_history] Calc Finish & Cached req={req_tag}, bars={len(bar_times)}, fxs={_fxs_cnt}, bis={_bis_cnt}, xds={_xds_cnt}"
            )
        else:
            LogUtil.info(f"[tv_history] Cache Hit req={req_tag}, bars={len(bar_times)}")

        if firstDataRequest == "false" and len(bar_times) > 0:
            try:
                from_ts = _from
                to_ts = _to
                start_idx = bisect.bisect_left(bar_times, from_ts)
                # bisect_left 使 to_ts 为排他上界（与 TradingView UDF 协议一致），
                # 避免 backward request 返回与请求 to 时间戳完全相同的 K 线（会导致重复 bar + 多余请求）
                end_idx = (
                    bisect.bisect_left(bar_times, to_ts)
                    if to_ts > 0
                    else len(bar_times)
                )

                def filter_shapes(shapes):
                    res = []
                    for shape in shapes:
                        if isinstance(shape, dict) and "points" in shape:
                            pts = shape["points"]
                            if isinstance(pts, list) and len(pts) > 0:
                                t = pts[-1].get("time", 0)
                                if t >= from_ts and (to_ts == 0 or t < to_ts):
                                    res.append(shape)
                            elif isinstance(pts, dict):
                                t = pts.get("time", 0)
                                if t >= from_ts and (to_ts == 0 or t < to_ts):
                                    res.append(shape)
                    return res

                cl_chart_data = {
                    "t": cl_chart_data.get("t", [])[start_idx:end_idx],
                    "c": cl_chart_data.get("c", [])[start_idx:end_idx],
                    "o": cl_chart_data.get("o", [])[start_idx:end_idx],
                    "h": cl_chart_data.get("h", [])[start_idx:end_idx],
                    "l": cl_chart_data.get("l", [])[start_idx:end_idx],
                    "v": cl_chart_data.get("v", [])[start_idx:end_idx],
                    "macd_dif": cl_chart_data.get("macd_dif", [])[start_idx:end_idx]
                    if cl_chart_data.get("macd_dif")
                    else [],
                    "macd_dea": cl_chart_data.get("macd_dea", [])[start_idx:end_idx]
                    if cl_chart_data.get("macd_dea")
                    else [],
                    "macd_hist": cl_chart_data.get("macd_hist", [])[start_idx:end_idx]
                    if cl_chart_data.get("macd_hist")
                    else [],
                    "macd_area": cl_chart_data.get("macd_area", [])[start_idx:end_idx]
                    if cl_chart_data.get("macd_area")
                    else [],
                    "higher_macd_dif": cl_chart_data.get("higher_macd_dif", [])[start_idx:end_idx]
                    if cl_chart_data.get("higher_macd_dif")
                    else [],
                    "higher_macd_dea": cl_chart_data.get("higher_macd_dea", [])[start_idx:end_idx]
                    if cl_chart_data.get("higher_macd_dea")
                    else [],
                    "higher_macd_hist": cl_chart_data.get("higher_macd_hist", [])[start_idx:end_idx]
                    if cl_chart_data.get("higher_macd_hist")
                    else [],
                    "fxs": filter_shapes(cl_chart_data.get("fxs", [])),
                    "bis": filter_shapes(cl_chart_data.get("bis", [])),
                    "xds": filter_shapes(cl_chart_data.get("xds", [])),
                    "zsds": filter_shapes(cl_chart_data.get("zsds", [])),
                    "bi_zss": filter_shapes(cl_chart_data.get("bi_zss", [])),
                    "xd_zss": filter_shapes(cl_chart_data.get("xd_zss", [])),
                    "zsd_zss": filter_shapes(cl_chart_data.get("zsd_zss", [])),
                    "bcs": filter_shapes(cl_chart_data.get("bcs", [])),
                    "mmds": filter_shapes(cl_chart_data.get("mmds", [])),
                }
            except Exception as e:
                LogUtil.error(f"[tv_history] Slice data failed: {e}")

        # 切片后无数据，返回 no_data 阻止 TradingView 继续向前请求
        if len(cl_chart_data.get("t", [])) == 0:
            return {"s": "no_data"}

        # 裁剪超出请求 _to 的「未来」K 线
        # 交易所可能返回尚未完成的下一根 K 线（时间戳 > 当前时间），
        # TradingView 缓存后会导致 DataPulse 更新时 time order violation
        _resp_times = cl_chart_data.get("t", [])
        _resp_end = len(_resp_times)
        if _to > 0 and _resp_times and _resp_times[-1] > _to:
            _resp_end = bisect.bisect_right(_resp_times, _to)
            if _resp_end < len(_resp_times):
                LogUtil.info(
                    f"[tv_history] Trimmed {len(_resp_times) - _resp_end} future bar(s) beyond to={_to}"
                )

        def _trim(arr):
            """对列表做 [:_resp_end] 切片，不修改缓存原对象"""
            if not arr or _resp_end >= len(arr):
                return arr
            return arr[:_resp_end]

        # [DataVerify] Log response shape counts for frontend correlation
        _resp_t = _trim(cl_chart_data.get("t", []))
        LogUtil.info(
            f"[DataVerify][Backend] symbol={symbol} resolution={resolution} "
            f"update={firstDataRequest != 'true'} bars={len(_resp_t)} "
            f"fxs={len(cl_chart_data.get('fxs', []))} "
            f"bis={len(cl_chart_data.get('bis', []))} "
            f"xds={len(cl_chart_data.get('xds', []))} "
            f"zsds={len(cl_chart_data.get('zsds', []))} "
            f"bi_zss={len(cl_chart_data.get('bi_zss', []))} "
            f"bcs={len(cl_chart_data.get('bcs', []))} "
            f"mmds={len(cl_chart_data.get('mmds', []))}"
        )

        return {
            "s": "ok",
            "t": _trim(cl_chart_data.get("t", [])),
            "c": _trim(cl_chart_data.get("c", [])),
            "o": _trim(cl_chart_data.get("o", [])),
            "h": _trim(cl_chart_data.get("h", [])),
            "l": _trim(cl_chart_data.get("l", [])),
            "v": _trim(cl_chart_data.get("v", [])),
            "macd_dif": _trim(cl_chart_data.get("macd_dif", [])),
            "macd_dea": _trim(cl_chart_data.get("macd_dea", [])),
            "macd_hist": _trim(cl_chart_data.get("macd_hist", [])),
            "macd_area": _trim(cl_chart_data.get("macd_area", [])),
            "higher_macd_dif": _trim(cl_chart_data.get("higher_macd_dif", [])),
            "higher_macd_dea": _trim(cl_chart_data.get("higher_macd_dea", [])),
            "higher_macd_hist": _trim(cl_chart_data.get("higher_macd_hist", [])),
            "fxs": cl_chart_data.get("fxs", []),
            "bis": cl_chart_data.get("bis", []),
            "xds": cl_chart_data.get("xds", []),
            "zsds": cl_chart_data.get("zsds", []),
            "bi_zss": cl_chart_data.get("bi_zss", []),
            "xd_zss": cl_chart_data.get("xd_zss", []),
            "zsd_zss": cl_chart_data.get("zsd_zss", []),
            "bcs": cl_chart_data.get("bcs", []),
            "mmds": cl_chart_data.get("mmds", []),
            "update": False if firstDataRequest == "true" else True,
        }
    except Exception as e:
        req_qs = request.query_string.decode("utf-8", errors="ignore")
        LogUtil.error(f"[tv_history] unhandled error query={req_qs} err={e}", exc_info=True)
        return {"s": "no_data"}


@tv_bp.route("/tv/timescale_marks")
@login_required
def tv_timescale_marks():
    symbol = request.args.get("symbol", "")
    _from = _normalize_unix_ts(request.args.get("from"))
    _to = _normalize_unix_ts(request.args.get("to"))
    resolution = _normalize_resolution(request.args.get("resolution"))
    market, code = _parse_tv_symbol(symbol)
    if market is None or code is None:
        return []
    freq = resolution_maps.get(resolution)
    if freq is None:
        return []

    order_type_maps = {
        "buy": "买入",
        "sell": "卖出",
        "open_long": "买入开多",
        "open_short": "买入开空",
        "close_long": "卖出平多",
        "close_short": "买入平空",
    }
    marks = []

    orders = db.order_query_by_code(market, code)
    for i in range(len(orders)):
        o = orders[i]
        _dt_int = fun.datetime_to_int(o["datetime"])
        if _from <= _dt_int <= _to:
            m = {
                "id": i,
                "time": _dt_int,
                "color": (
                    "red"
                    if o["type"] in ["buy", "open_long", "close_short"]
                    else "green"
                ),
                "label": (
                    "B" if o["type"] in ["buy", "open_long", "close_short"] else "S"
                ),
                "tooltip": [
                    f"{order_type_maps[o['type']]}[{o['price']}/{o['amount']}]",
                    f"{'' if 'info' not in o else o['info']}",
                ],
                "shape": (
                    "earningUp"
                    if o["type"] in ["buy", "open_long", "close_short"]
                    else "earningDown"
                ),
            }
            marks.append(m)

    other_marks = db.marks_query(market, code)
    for i in range(len(other_marks)):
        _m = other_marks[i]
        if _m.frequency == "" or _m.frequency == freq:
            if _from <= _m.mark_time <= _to:
                marks.append(
                    {
                        "id": f"m-{i}",
                        "time": int(_m.mark_time),
                        "color": _m.mark_color,
                        "label": _m.mark_label,
                        "tooltip": _m.mark_tooltip,
                        "shape": _m.mark_shape,
                    }
                )

    return marks


@tv_bp.route("/tv/marks")
@login_required
def tv_marks():
    symbol = request.args.get("symbol", "")
    _from = _normalize_unix_ts(request.args.get("from"))
    _to = _normalize_unix_ts(request.args.get("to"))
    resolution = _normalize_resolution(request.args.get("resolution"))
    market, code = _parse_tv_symbol(symbol)
    if market is None or code is None:
        return []
    freq = resolution_maps.get(resolution)
    if freq is None:
        return []

    marks = []
    price_marks = db.marks_query_by_price(market, code, start_date=_from)
    for i in range(len(price_marks)):
        _m = price_marks[i]
        if _m.frequency == "" or _m.frequency == freq:
            if _from <= _m.mark_time <= _to:
                marks.append(
                    {
                        "id": f"m-{i}",
                        "time": int(_m.mark_time),
                        "color": _m.mark_color,
                        "text": _m.mark_text,
                        "label": _m.mark_label,
                        "labelFontColor": _m.mark_label_font_color,
                        "minSize": _m.mark_min_size,
                    }
                )

    return marks


@tv_bp.route("/tv/del_marks", methods=["POST"])
@login_required
def tv_del_marks():
    symbol = request.form["symbol"]
    market, code = _parse_tv_symbol(symbol)
    if market is None or code is None:
        return {"status": "ok"}

    db.marks_del_all_by_code(market, code)

    return {"status": "ok"}


@tv_bp.route("/tv/time")
@login_required
def tv_time():
    return fun.datetime_to_int(datetime.datetime.now())


@tv_bp.route("/tv/<version>/charts", methods=["GET", "POST", "DELETE"])
@login_required
def tv_charts(version):
    client_id = str(request.args.get("client"))
    user_id = str(request.args.get("user"))

    if request.method == "GET":
        chart_id = request.args.get("chart")
        if chart_id is None:
            chart_list = db.tv_chart_list("chart", client_id, user_id)
            return {
                "status": "ok",
                "data": [
                    {
                        "timestamp": c.timestamp,
                        "symbol": c.symbol,
                        "resolution": c.resolution,
                        "id": c.id,
                        "name": c.name,
                    }
                    for c in chart_list
                ],
            }
        else:
            chart = db.tv_chart_get("chart", chart_id, client_id, user_id)
            return {
                "status": "ok",
                "data": {
                    "content": chart.content,
                    "timestamp": chart.timestamp,
                    "name": chart.name,
                    "id": chart.id,
                },
            }
    elif request.method == "DELETE":
        chart_id = request.args.get("chart")
        db.tv_chart_del("chart", chart_id, client_id, user_id)
        return {
            "status": "ok",
        }
    else:
        name = request.form["name"]
        content = request.form["content"]
        symbol = request.form["symbol"]
        resolution = request.form["resolution"]
        chart_id = request.args.get("chart")

        if chart_id is None:
            id = db.tv_chart_save(
                "chart", client_id, user_id, name, content, symbol, resolution
            )
            return {
                "status": "ok",
                "id": id,
            }
        else:
            db.tv_chart_update(
                "chart",
                chart_id,
                client_id,
                user_id,
                name,
                content,
                symbol,
                resolution,
            )
            return {"status": "ok"}


@tv_bp.route("/tv/<version>/study_templates", methods=["GET", "POST", "DELETE"])
@login_required
def tv_study_templates(version):
    client_id = str(request.args.get("client"))
    user_id = str(request.args.get("user"))

    if request.method == "GET":
        template = request.args.get("template")
        if template is None:
            template_list = db.tv_chart_list("template", client_id, user_id)
            return {
                "status": "ok",
                "data": [{"name": t.name} for t in template_list],
            }
        else:
            template = db.tv_chart_get_by_name(
                "template", template, client_id, user_id
            )
            return {
                "status": "ok",
                "data": {"name": template.name, "content": template.content},
            }
    elif request.method == "DELETE":
        name = request.args.get("template")
        db.tv_chart_del_by_name("template", name, client_id, user_id)
        return {
            "status": "ok",
        }
    else:
        name = request.form["name"]
        content = request.form["content"]
        db.tv_chart_save("template", client_id, user_id, name, content, "", "")
        return {"status": "ok"}


@tv_bp.route("/tv/<version>/drawings", methods=["GET", "POST"])
@login_required
def tv_drawings(version):
    client_id = str(request.args.get("client"))
    user_id = str(request.args.get("user"))
    chart_id = request.args.get("chart", "default")
    layout_id = request.args.get("layout", "default")
    symbol = request.args.get("symbol", "")
    resolution = request.args.get("resolution", "")

    drawing_name = _drawing_storage_name(chart_id, layout_id, symbol, resolution)
    legacy_drawing_name = _legacy_drawing_storage_name(symbol, resolution)

    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        content = payload.get("state", {})
        db.tv_chart_save(
            "drawing",
            client_id,
            user_id,
            drawing_name,
            json.dumps(content),
            symbol,
            resolution,
        )
        return {"status": "ok"}

    if request.method == "GET":
        drawing = db.tv_chart_get_by_name("drawing", drawing_name, client_id, user_id)
        if drawing is None:
            drawing = db.tv_chart_get_by_name(
                "drawing", legacy_drawing_name, client_id, user_id
            )
            if drawing is not None:
                db.tv_chart_save(
                    "drawing",
                    client_id,
                    user_id,
                    drawing_name,
                    drawing.content,
                    symbol,
                    resolution,
                )

        if drawing:
            try:
                data = json.loads(drawing.content)
            except Exception:
                data = {}
            return {
                "status": "ok",
                "data": data,
            }
        return {
            "status": "ok",
            "data": {},
        }