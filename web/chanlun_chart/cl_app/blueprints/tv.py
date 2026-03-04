"""
TradingView相关接口蓝图。
(终极修复版：修复 minmov 校验错误 + 全量数据返回 + 内存缓存)
"""
import pytz
import json
import datetime
import time
import numpy as np
import talib
from cachetools import TTLCache
from threading import RLock
from collections import defaultdict
import bisect
from flask import Blueprint, request
from flask_login import login_required
import pinyin

from chanlun import fun
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

# 图表数据计算结果缓存 (30秒缓存，防止短时间重复计算，减少快速切换周期时的重复计算)
chart_data_cache = TTLCache(maxsize=100, ttl=600)

# Fix: Pre-warm cache for common interval switches to reduce recomputation
chart_data_cache_stats = {"accessed_intervals": defaultdict(set), "prewarmed": set()}
COMMON_INTERVALS = ["1", "5", "15", "30", "60", "1D", "1W"]  # Commonly switched intervals

# 缓存数据最近验证时间戳（防止非交易时段 DataPulse 反复 cache miss）
_CACHE_REVALIDATION_INTERVAL = 30  # 秒，缓存在此时间内被验证过则视为有效
chart_data_validated_at = {}

cache_lock = RLock()
req_lock = RLock()


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


def prewarm_common_intervals(market, code, cl_config):
    """
    Pre-warm cache for commonly switched intervals to reduce recomputation on rapid interval switches.
    This is called asynchronously after initial data load.
    """
    def _prewarm():
        try:
            ex = get_exchange(Market(market))
            tz_sh = pytz.timezone("Asia/Shanghai")
            
            for interval in COMMON_INTERVALS:
                cache_key = None
                try:
                    freq = resolution_maps.get(interval, interval)
                    cache_key = f"{market}_{code}_{freq}_{hash(json.dumps(cl_config, sort_keys=True, default=str))}"
                    
                    # Skip if already in cache or being computed
                    with cache_lock:
                        if cache_key in chart_data_cache or cache_key in chart_data_cache_stats["prewarmed"]:
                            continue
                        # Mark as prewarming to prevent duplicate computation
                        chart_data_cache_stats["prewarmed"].add(cache_key)
                    
                    # Pre-warm in background (fire and forget)
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
                    
                    # Store in cache
                    with cache_lock:
                        chart_data_cache[cache_key] = cl_chart_data
                        chart_data_validated_at[cache_key] = time.time()
                        chart_data_cache_stats["prewarmed"].discard(cache_key)
                        chart_data_cache_stats["accessed_intervals"][f"{market}:{code}"].add(interval)
                        LogUtil.info(f"[tv_history] Pre-warmed cache for {market}:{code} interval {interval}")
                except Exception as e:
                    if cache_key is not None:
                        with cache_lock:
                            chart_data_cache_stats["prewarmed"].discard(cache_key)
                    LogUtil.error(f"[tv_history] Pre-warm failed for {interval}: {e}")
        except Exception as e:
            LogUtil.error(f"[tv_history] Pre-warm thread error: {e}")
    
    # Run prewarming in background thread
    t = threading.Thread(target=_prewarm, daemon=True, name="IntervalPrewarmThread")
    t.start()
    LogUtil.info(f"[tv_history] Started pre-warm thread for {market}:{code}")


class _LimitedLockDict(defaultdict):
    """Bounded lock dict to avoid unlimited cache-key lock growth."""
    _MAX_SIZE = 200

    def __missing__(self, key):
        if len(self) >= self._MAX_SIZE:
            LogUtil.warning(f"chart_calc_locks 已达上限 {self._MAX_SIZE}，跳过清理避免删除正在使用的锁")
        self[key] = RLock()
        return self[key]

chart_calc_locks = _LimitedLockDict()

import threading

def preload_symbols():
    exchanges = ["a", "hk", "fx", "us", "futures", "ny_futures", "currency", "currency_spot"]
    while True:
        LogUtil.info("开始预加载并更新所有市场的 symbols...")
        for exchange in exchanges:
            try:
                ex = get_exchange(Market(exchange))
                all_stocks = ex.all_stocks()
                processed_stocks = _process_stock_list(all_stocks)
                with cache_lock:
                    stock_cache[exchange] = processed_stocks
                LogUtil.info(f"市场 {exchange} symbols 预加载完成，共 {len(processed_stocks)} 条")
            except Exception as e:
                LogUtil.error(f"预加载市场 {exchange} symbols 失败: {e}")
        
        # 每小时更新一次
        time.sleep(3600)

def start_symbol_preload_thread():
    t = threading.Thread(target=preload_symbols, daemon=True, name="SymbolPreloadThread")
    t.start()

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
    ex = get_exchange(Market(group))
    all_symbols = ex.all_stocks()

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


def get_cached_processed_stocks(exchange):
    with cache_lock:
        try:
            return stock_cache[exchange]
        except KeyError:
            ex = get_exchange(Market(exchange))
            all_stocks = ex.all_stocks()
            processed_stocks = _process_stock_list(all_stocks)
            stock_cache[exchange] = processed_stocks
            return processed_stocks


@tv_bp.route("/tv/search")
@login_required
def tv_search():
    query = request.args.get("query")
    type_ = request.args.get("type")
    exchange = request.args.get("exchange")
    limit_str = request.args.get("limit", "10")
    limit = int(limit_str)

    try:
        processed_stocks = get_cached_processed_stocks(exchange)
    except Exception as e:
        return {"error": f"Failed to get stocks for {exchange}: {e}"}, 500

    query_lower = query.lower()
    res_stocks = []
    is_currency = exchange in ["currency", "currency_spot"]

    for stock in processed_stocks:
        if is_currency:
            if query_lower in stock['code_lower']:
                res_stocks.append(stock)
        else:
            if (query_lower in stock['code_lower'] or
                    query_lower in stock['name_lower'] or
                    query_lower in stock['pinyin_initials']):
                res_stocks.append(stock)
        if len(res_stocks) >= limit:
            break

    supported_resolutions = [
        v for k, v in frequency_maps.items() if k in market_frequencys[exchange]
    ]

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
                "session": market_session[exchange],
                "timezone": market_timezone[exchange],
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
            with req_lock:
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
        cache_key = f"{market}_{code}_{frequency}_{hash(json.dumps(cl_config, sort_keys=True, default=str))}"

        cl_chart_data = None
        is_cache_hit = False
        cache_miss_reason = "cache_empty"

        with chart_calc_locks[cache_key]:
            with cache_lock:
                if cache_key in chart_data_cache:
                    _cached = chart_data_cache[cache_key]
                    _cached_times = _cached.get("t", []) if isinstance(_cached, dict) else []
                    if _to > 0 and len(_cached_times) > 0:
                        if _to > _cached_times[-1]:
                            # 若缓存在 _CACHE_REVALIDATION_INTERVAL 内刚被验证过，视为命中
                            _last_validated = chart_data_validated_at.get(cache_key, 0)
                            if (time.time() - _last_validated) < _CACHE_REVALIDATION_INTERVAL:
                                cl_chart_data = _cached
                                is_cache_hit = True
                            else:
                                cache_miss_reason = "cache_stale_to_exceeds_tail"
                        else:
                            cl_chart_data = _cached
                            is_cache_hit = True
                    else:
                        cl_chart_data = _cached
                        is_cache_hit = True

            if not is_cache_hit:
                LogUtil.info(f"[tv_history] Cache miss ({cache_miss_reason}) req={req_tag}")
                ex = get_exchange(Market(market))
                frequency_low, kchart_to_frequency = kcharts_frequency_h_l_map(market, frequency)
                kline_args = {}
                if (
                    firstDataRequest == "false"
                    and _from > 0
                    and _to > 0
                    and _to >= _from
                ):
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
                            chart_data_validated_at[cache_key] = time.time()
                        return {"s": "no_data"}
                    cd = web_batch_get_cl_datas(
                        market, code, {frequency_low: klines}, cl_config
                    )[0]
                else:
                    kchart_to_frequency = None
                    klines = ex.klines(code, frequency, **kline_args)
                    if klines is None or len(klines) == 0:
                        with cache_lock:
                            chart_data_validated_at[cache_key] = time.time()
                        return {"s": "no_data"}
                    cd = web_batch_get_cl_datas(
                        market, code, {frequency: klines}, cl_config
                    )[0]

                if _to > 0 and len(klines) > 0 and _to < fun.datetime_to_int(klines.iloc[0]["date"]):
                    return {"s": "no_data"}

                cl_chart_data = cl_data_to_tv_chart(
                    cd, cl_config, to_frequency=kchart_to_frequency
                )
                if cl_chart_data is None:
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
                        if len(closes) > 0:
                            fast = int(cl_config.get("idx_macd_fast", 12)) * ratio
                            slow = int(cl_config.get("idx_macd_slow", 26)) * ratio
                            signal = int(cl_config.get("idx_macd_signal", 9)) * ratio
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
                    chart_data_cache[cache_key] = cl_chart_data
                    chart_data_validated_at[cache_key] = time.time()

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

    # drawings are tied to symbol + resolution
    drawing_name = f"drawings_{symbol}_{resolution}"

    if request.method == "POST":
        content = request.json.get("state", {})
        db.tv_chart_save("drawing", client_id, user_id, drawing_name, json.dumps(content), symbol, resolution)
        return {"status": "ok"}

    if request.method == "GET":
        drawing = db.tv_chart_get_by_name("drawing", drawing_name, client_id, user_id)
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
