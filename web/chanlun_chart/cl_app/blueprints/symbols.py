"""
多市场标的列表浏览蓝图。

提供以下接口：
- GET  /symbols                       : 渲染独立的"标的列表"页面
- GET  /symbols/list                  : JSON 数据接口，按市场返回（可选）模糊搜索 + 分页后的标的
- POST /symbols/prewarm               : 启动当前市场全量缠论数据预热（后台串行执行）
- GET  /symbols/prewarm/status        : 查询当前市场最新一次预热任务的进度
- POST /symbols/prewarm/cancel        : 取消当前市场正在执行的预热任务

实现复用 tv 蓝图中已有的 ``get_cached_processed_stocks`` 缓存能力，
不重复触发交易所连接，符合启动期预加载与异步刷新策略。

预热实现要点（2026-04 重构后）：
- 同一时刻全局只允许 1 个市场在预热（避免多市场互相冲击）；
- 在该市场内：**多标的并行 + 单标的内 4 周期并行**（最大化吞吐）：
  - 多标的并行度按市场配置（a 股 xtquant 必须 1，us/hk 等 HTTP 数据源 4）；
  - 单标的内 4 周期固定 4 个线程并行；
  - 总并发上限受 chart_calc_locks 自然约束（同一 cache_key 永远只有一个在算）；
- 计算结果直接写入 ``tv.chart_data_cache``，与用户实际查看图表时的 cache_key 完全一致，
  之后切换标的命中缓存可秒开；
- 用户最近看过的标的优先插队（每批调度时重排剩余 pending）；
- 任务对象内存维护，TTL 1 小时自动清理（避免页面长期不刷导致泄漏）。
"""

import json
import pathlib
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from flask import Blueprint, jsonify, render_template, request
from flask_login import login_required

from chanlun.cl_utils import query_cl_chart_config
from chanlun.tools.log_util import LogUtil

from ..services.constants import market_types, resolution_maps
from .tv import (
    _build_cache_key,
    _get_chart_cache_entry,
    _get_last_user_request_time,
    _get_user_recent_codes,
    cache_lock,
    chart_data_cache,
    compute_and_cache_chart_data,
    get_cached_processed_stocks,
    mark_batch_prewarm_active,
)

symbols_bp = Blueprint("symbols", __name__)

# 与 templates/index.html 顶部市场下拉保持一致的展示顺序与文案
MARKETS = [
    ("a", "沪深A股"),
    ("hk", "港股"),
    ("futures", "国内期货"),
    ("ny_futures", "纽约期货"),
    ("fx", "外汇"),
    ("us", "美股"),
    ("currency", "数字货币(合约)"),
    ("currency_spot", "数字货币(现货)"),
]

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 500

# 标的列表"仅显示个股"的 type 白名单。市场不在表中则不过滤。
# 港股/美股因数据源不返回 type 字段，本期不过滤；期货/外汇/数字货币本无个股概念。
STOCK_ONLY_TYPES_BY_MARKET: Dict[str, set] = {
    "a": {"stock_cn"},
}


@symbols_bp.route("/symbols")
@login_required
def symbols_page():
    """渲染标的列表页面。"""
    default_market = (request.args.get("market") or "a").strip().lower()
    if default_market not in market_types:
        default_market = "a"
    return render_template(
        "symbols.html",
        markets=MARKETS,
        default_market=default_market,
    )


@symbols_bp.route("/symbols/list")
@login_required
def symbols_list():
    """按市场返回标的列表（支持模糊搜索 + 分页）。

    Query 参数：
    - market    : 市场代码（必填，必须在 ``market_types`` 中）
    - q         : 模糊关键词，匹配 code / name / 拼音首字母（可选）
    - page      : 1-based 页码，默认 1
    - page_size : 每页数量，默认 50，上限 500
    """
    market = (request.args.get("market") or "").strip().lower()
    if market not in market_types:
        return jsonify({"ok": False, "msg": f"未知市场: {market!r}"}), 400

    query = (request.args.get("q") or "").strip().lower()

    # ``all=1`` 表示前端一次性拉全量（用于本地过滤+键盘连续浏览体验，
    # 避免分页造成键盘 ↑/↓ 在边界处中断）。其它情况保留分页兼容。
    return_all = (request.args.get("all") or "").strip() in ("1", "true", "yes")

    try:
        page = int(request.args.get("page", "1"))
    except (TypeError, ValueError):
        page = 1
    if page < 1:
        page = 1

    try:
        page_size = int(request.args.get("page_size", str(DEFAULT_PAGE_SIZE)))
    except (TypeError, ValueError):
        page_size = DEFAULT_PAGE_SIZE
    if page_size < 1:
        page_size = DEFAULT_PAGE_SIZE
    if page_size > MAX_PAGE_SIZE:
        page_size = MAX_PAGE_SIZE

    # allow_sync_fallback=True：用户在该页主动等待数据是合理的，宁可慢几秒也不能 500。
    try:
        all_stocks = get_cached_processed_stocks(market, allow_sync_fallback=True) or []
    except Exception as e:
        LogUtil.error(f"[symbols_list] get stocks failed market={market}: {e}")
        all_stocks = []

    # 应用 type 白名单过滤（仅对配置了的市场生效）
    allow_types = STOCK_ONLY_TYPES_BY_MARKET.get(market)
    if allow_types is not None:
        def _keep(s):
            # 未带 type 字段或 type='unknown' 的条目默认放行，避免把"识别失败"误删。
            t = s.get("type", "unknown")
            return t in allow_types or t == "unknown"
        all_stocks = [s for s in all_stocks if _keep(s)]

    if query:
        filtered = [
            s
            for s in all_stocks
            if query in s.get("code_lower", "")
            or query in s.get("name_lower", "")
            or query in s.get("pinyin_initials", "")
        ]
    else:
        filtered = all_stocks

    total = len(filtered)

    if return_all:
        page_items = filtered
        page = 1
        page_size = total
    else:
        start = (page - 1) * page_size
        end = start + page_size
        page_items = filtered[start:end]

    market_type = market_types.get(market, "")

    items = [
        {
            "code": s.get("code", ""),
            "name": s.get("name", ""),
            "pinyin": s.get("pinyin_initials", ""),
            "type": market_type,
        }
        for s in page_items
    ]

    return jsonify(
        {
            "ok": True,
            "market": market,
            "total": total,
            "page": page,
            "page_size": page_size,
            "items": items,
        }
    )

# ---------------------------------------------------------------------------
# 缠论数据预热（Pre-warm）
# ---------------------------------------------------------------------------

# 预热使用的常用周期（TV interval 表示法），通过 resolution_maps 转成项目内部 freq：
# "1D" -> d, "30" -> 30m, "5" -> 5m, "1" -> 1m
# 顺序仍然按用户最常看的优先（虽然现在并行计算，但失败时按顺序记录日志看起来更直观）。
PREWARM_INTERVALS = ["1D", "30", "5", "1"]

# 单标的内并发计算的周期数（每个周期一个线程）。
# 2026-04 调优：4 → 2。原因：实测全市场预热同时打 8 个并发请求时（2 标的 × 4 周期），
# 长桥 HTTP 连接池被占满，前端 polling 请求（每 3 秒 1 次/面板，4 面板 = 1.3 QPS）被排到
# 队列后面，导致已切换标的的旧请求耗时 10-18 秒，用户感觉切换很卡。
# 维持 2，让总在飞请求数 ≤ INFLIGHT_LIMIT，避免 4 周期同时抢一把信号量。
PREWARM_FREQ_PARALLELISM = 2

# 多个标的之间并行处理的 worker 数。按市场区分：
# - a 股 xtquant native 不是线程安全的，必须串行 → 1
# - 其他 native 数据源（tdx 期货）也保险串行 → 1
# - HTTP 数据源（长桥/futu）放开并行：
#   2026-04 二次调优（M2 落盘后）：1 → 3 (us) / 1 → 2 (hk)。
#   现在用户切到已预热标的命中 disk，毫秒级返回；批量预热可以更激进抢占数据源
#   也不会再让用户体验崩。总在飞 = code_parallelism × freq_parallelism，但实际受
#   下面 INFLIGHT_LIMIT 全局信号量约束，不会无限叠加。
PREWARM_CODE_PARALLELISM_BY_MARKET = {
    "a": 1,            # xtquant native，绝对串行
    "futures": 1,      # tdx native，保险串行
    "ny_futures": 1,   # 同上
    "us": 3,           # 长桥 HTTP，可并行 3 个标的（M2 后允许更激进）
    "hk": 2,           # futu HTTP，可并行 2 个标的
    "fx": 1,
    "currency": 1,
    "currency_spot": 1,
}
PREWARM_CODE_PARALLELISM_DEFAULT = 1

# ⚠️ 关键：全局在飞请求数信号量上限（绝对真理）。
# 这是预热可同时打到数据源的最大请求数，不论标的并行度 × 周期并行度乘出来多大，都受这个限制。
# 留出余量给用户的实时请求（用户的 tv_history 不走这个信号量，永远优先）。
# 2026-04 二次调优（M2 落盘后）：2 → 6。
# 长桥/futu 单连接池 QPS 上限实测 ~10，预热占 6，给用户实时请求和 polling 留 4 个余量。
# 用户切已预热标的现在走 disk hit 不再走 HTTP，所以可以放心吃满。
PREWARM_GLOBAL_INFLIGHT_LIMIT = 6

# 用户活跃度让位：用户最近 N 秒内有 firstDataRequest=true 的请求时，预热请求等一下再发，
# 避免把用户的实时请求挤到 HTTP 连接队列后面。
PREWARM_USER_ACTIVE_WINDOW_SECONDS = 3.0
# 让位等待时间（秒）。用户活跃时，预热请求会 sleep 这么久后再继续。
# 2026-04 二次调优：1.0 → 0.3。1s 让位过长，用户没切其它标的时也会因为单次 first=true
# 把后续预热堵 5 秒，全市场预热被腰斩。0.3s 既能让用户突发请求优先，又不会浪费太多。
PREWARM_YIELD_SLEEP_SECONDS = 0.3

# 任务对象保留时长：完成后超过此时间允许新任务启动，并允许 GC。
PREWARM_TASK_RETAIN_SECONDS = 3600
# 用户最近请求过的标的优先插队：worker 在每轮循环开始时，会把还没预热的"用户最近看过"
# 的标的提到队首。
PREWARM_USER_RECENT_TRACK_SECONDS = 600  # 10 分钟内看过的算"用户关注"

# 任务进度持久化目录与写盘频率
_PREWARM_PERSIST_DIRNAME = "prewarm_status"
# 写盘频率：每完成 N 个标的写一次（额外终态时强制写一次）。
# 50 是经验值：典型 11k 标的预热 220 次写盘，IO 开销 < 1%；同时崩溃后丢失进度 < 50 个。
_PREWARM_PERSIST_EVERY_N_DONE = 50

# 全局信号量：所有预热请求（不论标的不论周期）都要先 acquire 才能发请求。
# 这是防止打爆数据源的核心机制。
_PREWARM_INFLIGHT_SEMAPHORE = threading.Semaphore(PREWARM_GLOBAL_INFLIGHT_LIMIT)

class PrewarmTask:
    """单次预热任务的进度对象（线程安全；通过 manager 的锁外部串行化访问）。"""

    __slots__ = (
        "task_id",
        "market",
        "total",
        "done",
        "succeeded",
        "failed",
        "current_code",
        "current_name",
        "status",
        "started_at",
        "finished_at",
        "cancel_event",
        "error_msg",
    )

    def __init__(self, market: str, total: int):
        self.task_id: str = uuid.uuid4().hex
        self.market: str = market
        self.total: int = int(total)
        self.done: int = 0
        self.succeeded: int = 0
        self.failed: int = 0
        self.current_code: str = ""
        self.current_name: str = ""
        self.status: str = "running"  # running | finished | cancelled | error
        self.started_at: float = time.time()
        self.finished_at: Optional[float] = None
        self.cancel_event: threading.Event = threading.Event()
        self.error_msg: str = ""

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "market": self.market,
            "total": self.total,
            "done": self.done,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "current_code": self.current_code,
            "current_name": self.current_name,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "elapsed": (self.finished_at or time.time()) - self.started_at,
            "error_msg": self.error_msg,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PrewarmTask":
        """从持久化 json 还原任务对象（仅恢复展示字段，不恢复 cancel_event 等运行时对象）。"""
        t = cls(market=d["market"], total=int(d.get("total", 0)))
        t.task_id = d.get("task_id", t.task_id)
        t.done = int(d.get("done", 0))
        t.succeeded = int(d.get("succeeded", 0))
        t.failed = int(d.get("failed", 0))
        t.current_code = d.get("current_code", "")
        t.current_name = d.get("current_name", "")
        t.status = d.get("status", "aborted")
        t.started_at = float(d.get("started_at", time.time()))
        finished = d.get("finished_at")
        t.finished_at = float(finished) if finished is not None else None
        t.error_msg = d.get("error_msg", "")
        return t

class PrewarmManager:
    """全局单例，按 market 维度管理预热任务。

    并发约束：
    - 同一时刻全局只允许 1 个预热任务在运行（通过 ``_global_running`` 状态控制），
      原因和 ``tv.prewarm_common_intervals`` 一样：xtquant native 不是线程安全的，
      多线程并发会撞 BSON 断言。
    - 每个 market 只保留最近一次任务（覆盖更早的）。
    """

    def __init__(self):
        self._lock = threading.Lock()
        # market -> 最近一次任务（无论是否已完成）
        self._tasks: Dict[str, PrewarmTask] = {}
        # 是否有任务正在运行（全局互斥）
        self._global_running: bool = False
        # worker 线程引用（仅用于调试，不主动 join）
        self._worker_thread: Optional[threading.Thread] = None
        # 启动恢复：从磁盘载入历史 task；进程内首次构造时执行一次。
        self._load_persisted_tasks()

    # ---------------- 持久化 ----------------

    def _load_persisted_tasks(self) -> None:
        """启动时扫描 prewarm_status/*.json 还原 _tasks。
        - 损坏文件：warning + unlink + 跳过；
        - status 仍为 "running"：说明上次进程异常退出，改写为 "aborted" 并落盘。
        """
        d = self._persist_dir()
        if d is None:
            return
        try:
            files = list(d.glob("*.json"))
        except OSError as e:
            LogUtil.warning(f"[prewarm] list persist dir failed: {e}")
            return
        for path in files:
            try:
                raw = path.read_text(encoding="utf-8")
                data = json.loads(raw)
                task = PrewarmTask.from_dict(data)
            except (OSError, ValueError, KeyError) as e:
                LogUtil.warning(f"[prewarm] load persisted task corrupt path={path} err={e}, 删除")
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
                continue
            if task.status == "running":
                # 上次进程异常退出，改为 aborted 并立即写回
                task.status = "aborted"
                if task.finished_at is None:
                    task.finished_at = time.time()
                try:
                    path.write_text(
                        json.dumps(task.to_dict(), ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                except OSError as e:
                    LogUtil.warning(f"[prewarm] write back aborted state failed: {e}")
            self._tasks[task.market] = task
            LogUtil.info(
                f"[prewarm] restored task market={task.market} status={task.status} "
                f"{task.done}/{task.total}"
            )

    def _persist_dir(self) -> "pathlib.Path":
        """惰性获取持久化目录；首次调用时创建。失败返回 None 由调用方降级。"""
        from chanlun.config import get_data_path
        try:
            d = get_data_path() / _PREWARM_PERSIST_DIRNAME
            d.mkdir(parents=True, exist_ok=True)
            return d
        except OSError as e:
            LogUtil.warning(f"[prewarm] persist dir create failed: {e}")
            return None

    def _persist_task(self, task: "PrewarmTask") -> None:
        """把单个 task 状态原子写到 <data>/prewarm_status/<market>.json。
        写失败仅 warning，不影响内存进度。
        多 worker 可能并发调用（_process_one 中按 done % 50 触发），tmp 名
        带 uuid 避免互相覆盖；最终 rename 到同一目标，后到者覆盖前者，符合
        "保留最新一次写入"语义。
        """
        d = self._persist_dir()
        if d is None:
            return
        path = d / f"{task.market}.json"
        tmp = d / f"{task.market}.json.tmp.{uuid.uuid4().hex}"
        try:
            data = task.to_dict()
            tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            # Windows 上 Path.replace 是原子的（同卷），避免半截文件。
            tmp.replace(path)
        except OSError as e:
            LogUtil.warning(f"[prewarm] persist task failed market={task.market}: {e}")
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass

    # ---------------- 公开 API ----------------

    def start(self, market: str, codes: List[dict]) -> dict:
        """启动一次预热。

        参数 ``codes`` 为 ``[{"code": str, "name": str}, ...]`` 列表。
        返回 ``{"ok": bool, "msg": str, "task": dict | None}``。
        """
        if not codes:
            return {"ok": False, "msg": "标的列表为空，无需预热", "task": None}

        with self._lock:
            self._gc_old_tasks_locked()
            if self._global_running:
                # 已有任务在跑（可能是同市场也可能是其他市场）
                running_task = self._find_running_task_locked()
                msg = (
                    f"已有市场 {running_task.market!r} 的预热任务在运行 "
                    f"({running_task.done}/{running_task.total})，请稍后再试"
                    if running_task
                    else "已有预热任务在运行，请稍后再试"
                )
                return {
                    "ok": False,
                    "msg": msg,
                    "task": running_task.to_dict() if running_task else None,
                }

            task = PrewarmTask(market=market, total=len(codes))
            self._tasks[market] = task
            self._global_running = True

        # 注意：worker 线程外部启动，不持锁，避免 worker 内部反向加锁导致死锁。
        thread = threading.Thread(
            target=self._run_task,
            args=(task, codes),
            daemon=True,
            name=f"PrewarmWorker[{market}]",
        )
        self._worker_thread = thread
        thread.start()

        LogUtil.info(
            f"[prewarm] task started market={market} total={len(codes)} task_id={task.task_id}"
        )
        return {"ok": True, "msg": "预热任务已启动", "task": task.to_dict()}

    def get_status(self, market: str) -> Optional[dict]:
        with self._lock:
            task = self._tasks.get(market)
            return task.to_dict() if task else None

    def cancel(self, market: str) -> dict:
        with self._lock:
            task = self._tasks.get(market)
            if task is None:
                return {"ok": False, "msg": "该市场没有预热任务"}
            if task.status != "running":
                return {"ok": False, "msg": f"任务状态为 {task.status}，无需取消", "task": task.to_dict()}
            task.cancel_event.set()
            return {"ok": True, "msg": "已发送取消信号，将在当前标的完成后停止", "task": task.to_dict()}

    # ---------------- 内部实现 ----------------

    def _gc_old_tasks_locked(self) -> None:
        now = time.time()
        for market in list(self._tasks.keys()):
            t = self._tasks[market]
            if t.status != "running" and t.finished_at and (now - t.finished_at) > PREWARM_TASK_RETAIN_SECONDS:
                self._tasks.pop(market, None)

    def _find_running_task_locked(self) -> Optional[PrewarmTask]:
        for t in self._tasks.values():
            if t.status == "running":
                return t
        return None

    def _run_task(self, task: PrewarmTask, codes: List[dict]) -> None:
        """worker 线程主体：按市场配置的并发度并行处理多个标的。

        关键设计：
        - 多标的并行：用 ThreadPoolExecutor 启 N 个 worker（按市场配置），同时处理多个标的；
        - 单标的内 4 周期再并行：见 ``_prewarm_one_code``；
        - 用户最近看过的标的优先插队：通过维护 pending 队列 + 每轮重新排序；
        - 取消信号：cancel_event 透传到所有子线程，子线程定时检查；
        - 完成统计：用 task._lock_internal 保护 done/succeeded/failed 计数（多线程并发更新）。
        """
        market = task.market
        code_parallelism = PREWARM_CODE_PARALLELISM_BY_MARKET.get(
            market, PREWARM_CODE_PARALLELISM_DEFAULT
        )
        LogUtil.info(
            f"[prewarm] task starting market={market} total={task.total} "
            f"code_parallelism={code_parallelism} freq_parallelism={PREWARM_FREQ_PARALLELISM}"
        )

        # pending 用 list + 索引推进的方式实现"用户最近看过的标的优先插队"
        pending: List[dict] = list(codes)
        processed: set = set()
        # 多线程并发更新 task 计数器需要小锁
        counter_lock = threading.Lock()

        def _process_one(item: dict) -> None:
            if task.cancel_event.is_set():
                return
            code = item.get("code", "")
            name = item.get("name", "")
            if not code:
                with counter_lock:
                    task.done += 1
                return

            # 当前正在处理（多个 worker 时只展示最新的，无所谓哪一个）
            task.current_code = code
            task.current_name = name

            LogUtil.info(
                f"[prewarm] >>> {market}/{code} ({name}) "
                f"intervals={','.join(PREWARM_INTERVALS)} "
                f"[{task.done + 1}/{task.total}]"
            )

            try:
                cl_config = query_cl_chart_config(market, code)
            except Exception as e:
                LogUtil.error(f"[prewarm] query_cl_chart_config failed {market}/{code}: {e}")
                with counter_lock:
                    task.failed += 1
                    task.done += 1
                return

            try:
                code_ok = self._prewarm_one_code(
                    market=market,
                    code=code,
                    cl_config=cl_config,
                    cancel_event=task.cancel_event,
                )
            except Exception as e:
                LogUtil.error(f"[prewarm] _prewarm_one_code crashed {market}/{code}: {e}")
                code_ok = False

            with counter_lock:
                if code_ok:
                    task.succeeded += 1
                else:
                    task.failed += 1
                task.done += 1
                done_now = task.done

            if done_now % 100 == 0:
                LogUtil.info(
                    f"[prewarm] progress market={market} "
                    f"{done_now}/{task.total} succeeded={task.succeeded} failed={task.failed}"
                )
            # 周期性持久化任务进度（崩溃后最多丢失 _PREWARM_PERSIST_EVERY_N_DONE 个标的的进度）
            if done_now % _PREWARM_PERSIST_EVERY_N_DONE == 0:
                self._persist_task(task)

        # 注册批量预热活动状态：tv.prewarm_common_intervals 看到此标记会让位，
        # 避免逐标的旧版 prewarm 与本任务双倍争抢 chart_calc_locks / 上游 HTTP 配额。
        mark_batch_prewarm_active(market, True)
        try:
            with ThreadPoolExecutor(
                max_workers=code_parallelism,
                thread_name_prefix=f"PrewarmCode[{market}]",
            ) as executor:
                # 分批提交：每次提交 code_parallelism * 4 个任务，避免一次性把 11755 个全塞进去导致
                # 优先级调整失效（已经塞进队列的都是按提交顺序执行）。
                # 每批结束后重新按"用户最近看过"排序剩余 pending。
                batch_size = max(code_parallelism * 4, 8)
                cursor = 0
                while cursor < len(pending) and not task.cancel_event.is_set():
                    # 优先级调整：把用户最近看过且还没处理的标的提到队首
                    try:
                        hot_codes = _get_user_recent_codes(market) or []
                    except Exception:
                        hot_codes = []
                    if hot_codes:
                        pending = self._prioritize_hot_codes(
                            pending, hot_codes, processed, cursor
                        )

                    end = min(cursor + batch_size, len(pending))
                    batch = pending[cursor:end]
                    cursor = end

                    futures = {}
                    for item in batch:
                        if task.cancel_event.is_set():
                            break
                        c = item.get("code", "")
                        if c:
                            processed.add(c)
                        fut = executor.submit(_process_one, item)
                        futures[fut] = c

                    # 等本批完成再调度下一批，确保 hot_codes 重排能生效
                    for fut in as_completed(futures):
                        if task.cancel_event.is_set():
                            break
                        try:
                            fut.result()
                        except Exception as e:
                            LogUtil.error(
                                f"[prewarm] code worker error {market}/{futures[fut]}: {e}"
                            )

            with self._lock:
                if task.cancel_event.is_set():
                    task.status = "cancelled"
                else:
                    task.status = "finished"
                task.finished_at = time.time()
                task.current_code = ""
                task.current_name = ""

            LogUtil.info(
                f"[prewarm] task done market={market} status={task.status} "
                f"succeeded={task.succeeded} failed={task.failed} "
                f"elapsed={task.finished_at - task.started_at:.1f}s"
            )
        except Exception as e:
            with self._lock:
                task.status = "error"
                task.error_msg = str(e)
                task.finished_at = time.time()
            LogUtil.error(f"[prewarm] worker crashed market={market}: {e}")
        finally:
            # 终态强制持久化一次（保证崩溃 / 取消都能落盘最后状态）
            try:
                self._persist_task(task)
            except Exception as e:
                LogUtil.warning(f"[prewarm] final persist failed: {e}")

            with self._lock:
                self._global_running = False
            # 清除批量预热活动状态：必须在最外层 finally，确保异常路径也释放，
            # 否则 tv.prewarm_common_intervals 会被永久误判为"批量预热中"而不工作。
            mark_batch_prewarm_active(market, False)

    @staticmethod
    def _yield_to_user_if_active(cancel_event: threading.Event) -> None:
        """如果用户最近活跃（有 firstDataRequest=true 的请求），sleep 让出 QPS。

        关键逻辑：
        - 不会无限等待（最多让 N 次，每次 PREWARM_YIELD_SLEEP_SECONDS）；
        - 每次 sleep 后重新检查用户活跃度，用户彻底闲下来就立刻继续；
        - 检查 cancel_event 以快速响应取消。

        为什么不用 chart_calc_locks 自然处理：
        - chart_calc_locks 是 per-cache_key 的，只防止同一标的同周期重复算；
        - 但数据源的 QPS 限制是**全局的**——预热打 GDS 的 1D，会跟用户打 ZK 的 5min 抢 QPS。
        - 所以必须用一个全局信号量 + 用户活跃度感知来彻底分隔。
        """
        max_yields = 5  # 最多让 5 次（5 秒），避免预热被永久饿死
        for _ in range(max_yields):
            if cancel_event.is_set():
                return
            try:
                last_user_req = _get_last_user_request_time()
            except Exception:
                return
            idle = time.time() - last_user_req
            if idle >= PREWARM_USER_ACTIVE_WINDOW_SECONDS:
                return
            time.sleep(PREWARM_YIELD_SLEEP_SECONDS)

    @staticmethod
    def _prioritize_hot_codes(
        pending: List[dict],
        hot_codes: List[str],
        processed: set,
        cursor: int = 0,
    ) -> List[dict]:
        """把 pending[cursor:] 中属于 hot_codes 且未处理的项移到 cursor 位置（队首）。

        - 已经处理过（processed）或已被前面 batch 提交（< cursor）的不动；
        - 保持 hot_codes 内部的顺序（最近的在最前）；
        - cursor 之前的部分（已经提交给 executor）保持不变。
        """
        if not hot_codes or cursor >= len(pending):
            return pending
        hot_set = set(c for c in hot_codes if c not in processed)
        if not hot_set:
            return pending

        head = pending[:cursor]
        tail = pending[cursor:]

        front = []
        rest = []
        code_to_item = {}
        for item in tail:
            c = item.get("code", "")
            if c in hot_set:
                code_to_item[c] = item
            else:
                rest.append(item)
        for c in hot_codes:
            if c in code_to_item:
                front.append(code_to_item[c])
        return head + front + rest

    def _prewarm_one_code(
        self,
        market: str,
        code: str,
        cl_config: dict,
        cancel_event: threading.Event,
    ) -> bool:
        """预热单个标的的 4 个常用周期，**4 个周期并行计算**。返回是否至少有 1 个成功。

        关键设计变化（2026-04 重构）：
        - 旧实现：4 个周期串行，单标的总耗时 = sum(各周期)，~30s/标的；
        - 新实现：4 个周期用 ThreadPoolExecutor 并发，单标的总耗时 = max(各周期)，~10s/标的。

        为什么可以并行：
        - 每个周期独立拉数据（ex.klines）→ 独立算缠论 → 独立写 cache_key 不同的缓存；
        - 4 个周期之间没有数据依赖（higher_macd 是从当前周期 closes 算的，不依赖其他周期）；
        - cache_lock 是写缓存的细粒度锁，多个 cache_key 同时写不会冲突；
        - chart_calc_locks 是 per-cache_key 锁，确保即使用户切到该标的同一周期，
          tv_history 和这里的预热也只会有一个在算（另一个等结果）。

        移除了 _wait_for_user_idle：让 chart_calc_locks 自然处理"用户切到正在预热的标的"
        的情况，不再阻塞 worker。
        """
        any_success = False
        success_lock = threading.Lock()

        def _compute_one_freq(interval: str) -> bool:
            if cancel_event.is_set():
                return False
            freq = resolution_maps.get(interval, interval)
            cache_key = _build_cache_key(market, code, freq, cl_config)

            # 已在缓存里就跳过（用户刚看过 / 上一轮预热刚算完）
            with cache_lock:
                if cache_key in chart_data_cache:
                    return True

            # 2026-04 新增：磁盘冷层命中也算预热完成。
            # 进程重启或 RAM TTL 淘汰后，磁盘里仍有上次预热的结果——直接 warm 回 RAM
            # 而不重算，可省下整次 ex.klines + 缠论计算 + MACD 的开销。
            # _get_chart_cache_entry 内部已带磁盘 fallback + RAM 回填。
            disk_entry = _get_chart_cache_entry(cache_key)
            if disk_entry is not None and disk_entry.get("is_full_snapshot"):
                return True

            # 让位：用户最近 N 秒内有主动请求 → 预热等一下，把数据源的 QPS 让给用户
            self._yield_to_user_if_active(cancel_event)
            if cancel_event.is_set():
                return False

            # 全局信号量：限制同一时刻有多少个预热请求在打数据源
            # 用户的实时 tv_history 请求不受这个信号量限制，永远优先
            acquired = _PREWARM_INFLIGHT_SEMAPHORE.acquire(timeout=30.0)
            if not acquired:
                LogUtil.warning(
                    f"[prewarm] semaphore timeout, skip {market}/{code}/{interval}"
                )
                return False

            try:
                return bool(compute_and_cache_chart_data(market, code, freq, cl_config))
            except Exception as e:
                LogUtil.error(
                    f"[prewarm] compute failed {market}/{code} interval={interval}: {e}"
                )
                return False
            finally:
                _PREWARM_INFLIGHT_SEMAPHORE.release()

        # 单标的内 4 周期并行
        with ThreadPoolExecutor(
            max_workers=PREWARM_FREQ_PARALLELISM,
            thread_name_prefix=f"PrewarmFreq[{market}/{code}]",
        ) as freq_executor:
            future_to_interval = {
                freq_executor.submit(_compute_one_freq, interval): interval
                for interval in PREWARM_INTERVALS
            }
            for fut in as_completed(future_to_interval):
                if cancel_event.is_set():
                    break
                try:
                    if fut.result():
                        with success_lock:
                            any_success = True
                except Exception as e:
                    interval = future_to_interval[fut]
                    LogUtil.error(
                        f"[prewarm] freq worker error {market}/{code}/{interval}: {e}"
                    )

        return any_success

# 单例
_prewarm_manager = PrewarmManager()

@symbols_bp.route("/symbols/prewarm", methods=["POST"])
@login_required
def symbols_prewarm():
    """启动当前市场的全量缠论数据预热。

    Body 参数（JSON 或 form）：
    - market : 市场代码（必填）
    """
    market = (request.values.get("market") or "").strip().lower()
    if not market:
        # 兼容 JSON body
        body = request.get_json(silent=True) or {}
        market = (body.get("market") or "").strip().lower()
    if market not in market_types:
        return jsonify({"ok": False, "msg": f"未知市场: {market!r}"}), 400

    try:
        all_stocks = get_cached_processed_stocks(market, allow_sync_fallback=True) or []
    except Exception as e:
        LogUtil.error(f"[symbols_prewarm] get stocks failed market={market}: {e}")
        return jsonify({"ok": False, "msg": f"获取标的列表失败: {e}"}), 500

    codes = [
        {"code": s.get("code", ""), "name": s.get("name", "")}
        for s in all_stocks
        if s.get("code")
    ]

    result = _prewarm_manager.start(market, codes)
    status_code = 200 if result["ok"] else 409
    return jsonify(result), status_code

@symbols_bp.route("/symbols/prewarm/status")
@login_required
def symbols_prewarm_status():
    """查询某市场最近一次预热任务的进度。"""
    market = (request.args.get("market") or "").strip().lower()
    if market not in market_types:
        return jsonify({"ok": False, "msg": f"未知市场: {market!r}"}), 400

    task = _prewarm_manager.get_status(market)
    if task is None:
        return jsonify({"ok": True, "task": None})
    return jsonify({"ok": True, "task": task})

@symbols_bp.route("/symbols/prewarm/cancel", methods=["POST"])
@login_required
def symbols_prewarm_cancel():
    """取消某市场正在运行的预热任务。"""
    market = (request.values.get("market") or "").strip().lower()
    if not market:
        body = request.get_json(silent=True) or {}
        market = (body.get("market") or "").strip().lower()
    if market not in market_types:
        return jsonify({"ok": False, "msg": f"未知市场: {market!r}"}), 400

    result = _prewarm_manager.cancel(market)
    status_code = 200 if result["ok"] else 409
    return jsonify(result), status_code