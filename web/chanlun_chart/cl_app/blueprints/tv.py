"""
TradingView相关接口蓝图。
(终极修复版：修复 minmov 校验错误 + 全量数据返回 + 内存缓存)
"""

import datetime
import time
from cachetools import TTLCache
from threading import RLock
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

# 基础数据缓存
stock_cache = TTLCache(maxsize=100, ttl=3600)

# 图表数据计算结果缓存 (10秒缓存，防止短时间重复计算)
chart_data_cache = TTLCache(maxsize=50, ttl=10)

cache_lock = RLock()
req_lock = RLock()
chart_calc_lock = RLock()

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
    symbol: str = request.args.get("symbol")
    symbol: list = symbol.split(":")
    market: str = symbol[0].lower()
    code: str = symbol[1]

    ex = get_exchange(Market(market))
    stocks = ex.stock_info(code)

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
        "type": market_types[market],
        "session": market_session[market],
        "timezone": market_timezone[market],

        # 显式设置 minmov 和 pricescale
        "minmov": 1,
        "pricescale": precision,

        "visible_plots_set": "ohlcv",
        "supported_resolutions": [
            v for k, v in frequency_maps.items() if k in market_frequencys[market]
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
    try:
        return stock_cache[exchange]
    except KeyError:
        pass
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
    """
    K线柱及缠论数据接口
    """
    symbol = request.args.get("symbol")
    _from = request.args.get("from")
    _to = request.args.get("to")
    resolution = request.args.get("resolution")
    firstDataRequest = request.args.get("firstDataRequest", "false")

    _symbol_res_old_k_time_key = f"{symbol}_{resolution}"
    now_time = time.time()

    s = "ok"
    if int(_from) < 0 and int(_to) < 0:
        s = "no_data"

    # 请求频率限制 (适当放宽)
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
                    s = "no_data"
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

    market = symbol.split(":")[0].lower()
    code = symbol.split(":")[1]
    ex = get_exchange(Market(market))

    if firstDataRequest == "false" and ex.now_trading() is False:
        # 历史数据请求时不应拦截，直接放行
        pass

    frequency = resolution_maps[resolution]
    cl_config = query_cl_chart_config(market, code)

    # ================= 缓存检查 =================
    cache_key = f"{market}_{code}_{frequency}_{hash(str(cl_config))}"

    cl_chart_data = None
    is_cache_hit = False

    if cache_key in chart_data_cache:
        cl_chart_data = chart_data_cache[cache_key]
        is_cache_hit = True

    if cl_chart_data is None:
        with chart_calc_lock:
            if cache_key in chart_data_cache:
                cl_chart_data = chart_data_cache[cache_key]
                is_cache_hit = True
            else:
                frequency_low, kchart_to_frequency = kcharts_frequency_h_l_map(
                    market, frequency
                )

                if (cl_config["enable_kchart_low_to_high"] == "1" and kchart_to_frequency is not None):
                    klines = ex.klines(code, frequency_low)
                    cd = web_batch_get_cl_datas(
                        market, code, {frequency_low: klines}, cl_config
                    )[0]
                else:
                    kchart_to_frequency = None
                    klines = ex.klines(code, frequency)
                    cd = web_batch_get_cl_datas(market, code, {frequency: klines}, cl_config)[0]

                if len(klines) > 0 and int(_to) < fun.datetime_to_int(klines.iloc[0]["date"]):
                    return {"s": "no_data"}

                cl_chart_data = cl_data_to_tv_chart(
                    cd, cl_config, to_frequency=kchart_to_frequency
                )

                chart_data_cache[cache_key] = cl_chart_data

    if not is_cache_hit:
        LogUtil.info(f"[tv_history] Calc Finish & Cached. Returning {len(cl_chart_data['t'])} bars (Full).")
    else:
        LogUtil.info(f"[tv_history] Cache Hit. Returning {len(cl_chart_data['t'])} bars (Full).")

    # =================================================
    # 关键：直接返回全量数据，不根据 _from 进行切片或过滤
    # 前端 bundle.js 会处理数据去重和合并
    # =================================================
    info = {
        "s": s,
        "t": cl_chart_data["t"],
        "c": cl_chart_data["c"],
        "o": cl_chart_data["o"],
        "h": cl_chart_data["h"],
        "l": cl_chart_data["l"],
        "v": cl_chart_data["v"],
        "macd_dif": cl_chart_data.get("macd_dif", []),
        "macd_dea": cl_chart_data.get("macd_dea", []),
        "macd_hist": cl_chart_data.get("macd_hist", []),
        "macd_area": cl_chart_data.get("macd_area", []),
        "fxs": cl_chart_data.get("fxs", []),
        "bis": cl_chart_data.get("bis", []),
        "xds": cl_chart_data.get("xds", []),
        "zsds": cl_chart_data.get("zsds", []),
        "bi_zss": cl_chart_data.get("bi_zss", []),
        "xd_zss": cl_chart_data.get("xd_zss", []),
        "zsd_zss": cl_chart_data.get("zsd_zss", []),
        "bcs": cl_chart_data.get("bcs", []),
        "mmds": cl_chart_data.get("mmds", []),
        "update": (
            False if firstDataRequest == "true" else True
        ),
    }
    return info


@tv_bp.route("/tv/timescale_marks")
@login_required
def tv_timescale_marks():
    symbol = request.args.get("symbol")
    _from = int(request.args.get("from"))
    _to = int(request.args.get("to"))
    resolution = request.args.get("resolution")
    market = symbol.split(":")[0]
    code = symbol.split(":")[1]

    freq = resolution_maps[resolution]

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
    symbol = request.args.get("symbol")
    _from = int(request.args.get("from"))
    _to = int(request.args.get("to"))
    resolution = request.args.get("resolution")
    market = symbol.split(":")[0]
    code = symbol.split(":")[1]

    freq = resolution_maps[resolution]

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
    market = symbol.split(":")[0]
    code = symbol.split(":")[1]

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
    chart_id = request.args.get("chart")
    layout_id = request.args.get("layout")

    if request.method == "GET":
        return {
            "status": "ok",
            "data": {},
        }
    else:
        return {"status": "ok"}