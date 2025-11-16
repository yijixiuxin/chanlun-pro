"""
TradingView相关接口蓝图。

  - `/tv/config`
  - `/tv/symbol_info`
  - `/tv/symbols`
  - `/tv/search`
  - `/tv/history`
  - `/tv/timescale_marks`
  - `/tv/marks`
  - `/tv/del_marks`
  - `/tv/time`
  - `/tv/<version>/charts`
  - `/tv/<version>/study_templates`
  - `/tv/<version>/drawings`
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

# maxsize=100：最多缓存100个交易所的数据
# ttl=3600：数据在缓存中保留1小时（3600秒）
# RLock 用于确保线程安全，防止多个请求同时穿透缓存
stock_cache = TTLCache(maxsize=100, ttl=3600)
cache_lock = RLock()

@tv_bp.route("/tv/config")
@login_required
def tv_config():
    """配置项"""
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
    """商品集合信息
    supports_search is True 则不会调用这个接口
    """
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
    """商品解析"""
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

    info = {
        "name": stocks["code"],
        "ticker": f"{market}:{stocks['code']}",
        "full_name": f"{market}:{stocks['code']}",
        "description": stocks["name"],
        "exchange": market,
        "type": market_types[market],
        "session": market_session[market],
        "timezone": market_timezone[market],
        "pricescale": (
            stocks["precision"] if "precision" in stocks.keys() else 1000
        ),
        "visible_plots_set": "ohlcv",
        "supported_resolutions": [
            v for k, v in frequency_maps.items() if k in market_frequencys[market]
        ],
        "intraday_multipliers": [
            "1",
            "2",
            "3",
            "5",
            "10",
            "15",
            "20",
            "30",
            "60",
            "120",
            "240",
        ],
        "seconds_multipliers": [
            "1",
            "2",
            "3",
            "5",
            "10",
            "15",
            "20",
            "30",
            "40",
            "50",
            "60",
        ],
        "daily_multipliers": [
            "1",
            "2",
        ],
        "minmov": 1,
        "minmov2": 0,
        "has_intraday": True,
        "has_seconds": True if market in ["futures", "ny_futures"] else False,
        "has_daily": True,
        "has_weekly_and_monthly": True,
        "sector": sector,
        "industry": industry,
    }
    return info


def _process_stock_list(all_stocks):
    """
    （核心优化）对股票列表进行一次性预处理。
    """
    processed_list = []
    for stock in all_stocks:
        # 为了不修改原始数据，我们拷贝一份
        stock_copy = stock.copy()

        # 2. 预先转为小写，搜索时不再需要转换
        stock_copy['code_lower'] = stock['code'].lower()
        stock_copy['name_lower'] = stock['name'].lower()

        # 3. （核心优化）预先计算拼音，这是最耗时的部分
        try:
            stock_copy['pinyin_initials'] = "".join([
                pinyin.get_initial(_p)[0] for _p in stock["name"]
            ]).lower()
        except Exception:
            # 处理可能的拼音转换异常
            stock_copy['pinyin_initials'] = ""

        processed_list.append(stock_copy)
    return processed_list


def get_cached_processed_stocks(exchange):
    """
    获取预处理过的股票列表，如果缓存中没有，则抓取、处理并存入缓存。
    """
    try:
        # 快速路径：缓存命中
        return stock_cache[exchange]
    except KeyError:
        # 缓存未命中，需要抓取数据
        pass

    # 4. 使用锁来防止“缓存击穿”
    with cache_lock:
        try:
            # 双重检查，可能在等待锁的时候，其他线程已经处理完了
            return stock_cache[exchange]
        except KeyError:
            # 确认缓存中没有，我们来处理
            ex = get_exchange(Market(exchange))

            # 5. （只在缓存失效时执行一次 I/O）
            all_stocks = ex.all_stocks()

            # 6. （只在缓存失效时执行一次 CPU 密集操作）
            processed_stocks = _process_stock_list(all_stocks)

            # 7. 存入缓存
            stock_cache[exchange] = processed_stocks
            return processed_stocks


@tv_bp.route("/tv/search")
@login_required
def tv_search():
    """商品检索（已优化）"""
    query = request.args.get("query")
    type_ = request.args.get("type")  # 避免与Python关键字'type'冲突
    exchange = request.args.get("exchange")
    limit_str = request.args.get("limit", "10")  # 提供一个默认值
    limit = int(limit_str)

    # 1. 获取预处理和缓存过的股票列表
    try:
        processed_stocks = get_cached_processed_stocks(exchange)
    except Exception as e:
        # 最好在这里记录日志
        return {"error": f"Failed to get stocks for {exchange}: {e}"}, 500

    # 2. 只需要转换一次查询字符串
    query_lower = query.lower()
    res_stocks = []
    is_currency = exchange in ["currency", "currency_spot"]

    # 3. （核心优化）使用for循环并提前退出
    for stock in processed_stocks:
        if is_currency:
            # 直接使用预处理过的字段
            if query_lower in stock['code_lower']:
                res_stocks.append(stock)
        else:
            # 直接使用预处理过的字段
            if (query_lower in stock['code_lower'] or
                    query_lower in stock['name_lower'] or
                    query_lower in stock['pinyin_initials']):
                res_stocks.append(stock)

        # 4. （核心优化）达到数量限制后立即停止遍历
        if len(res_stocks) >= limit:
            break

    # 5. （次要优化）在循环外计算一次即可
    supported_resolutions = [
        v
        for k, v in frequency_maps.items()
        if k in market_frequencys[exchange]
    ]

    # 6. 组装结果
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
                "type": type_,  # 使用 'type_'
                "session": market_session[exchange],
                "timezone": market_timezone[exchange],
                "supported_resolutions": supported_resolutions,  # 使用预先计算的列表
            }
        )

    return infos


@tv_bp.route("/tv/history")
@login_required
def tv_history():
    """
    K线柱
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

    if firstDataRequest == "false":
        # 判断在 5 秒内，同一个请求大于 5 次，返回 no_data
        if _symbol_res_old_k_time_key not in __history_req_counter.keys():
            __history_req_counter[_symbol_res_old_k_time_key] = {
                "counter": 0,
                "tm": now_time,
            }
        else:
            if __history_req_counter[_symbol_res_old_k_time_key]["counter"] >= 5:
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

    # 判断当前是否可交易时间
    if firstDataRequest == "false" and ex.now_trading() is False:
        return {"s": "no_data", "nextTime": int(now_time + (10 * 60))}

    frequency = resolution_maps[resolution]
    cl_config = query_cl_chart_config(market, code)
    frequency_low, kchart_to_frequency = kcharts_frequency_h_l_map(
        market, frequency
    )
    if (
        cl_config["enable_kchart_low_to_high"] == "1"
        and kchart_to_frequency is not None
    ):
        # 如果开启并设置的该级别的低级别数据，获取低级别数据，并在转换成高级图表展示
        klines = ex.klines(code, frequency_low)
        cd = web_batch_get_cl_datas(
            market, code, {frequency_low: klines}, cl_config
        )[0]
    else:
        kchart_to_frequency = None
        klines = ex.klines(code, frequency)
        cd = web_batch_get_cl_datas(market, code, {frequency: klines}, cl_config)[0]

    # 如果图表指定返回的时间太早，直接返回无数据
    if int(_to) < fun.datetime_to_int(klines.iloc[0]["date"]):
        return {"s": "no_data"}

    # 将缠论数据，转换成 tv 画图的坐标数据
    cl_chart_data = cl_data_to_tv_chart(
        cd, cl_config, to_frequency=kchart_to_frequency
    )

    # 根据 from_time 和 to_time 来获取对应的K线数据
    if firstDataRequest == "false":
        _t = cl_chart_data["t"][-10:]
        _c = cl_chart_data["c"][-10:]
        _o = cl_chart_data["o"][-10:]
        _h = cl_chart_data["h"][-10:]
        _l = cl_chart_data["l"][-10:]
        _v = cl_chart_data["v"][-10:]
        _fxs = cl_chart_data.get("fxs", [])[-5:]
        _bis = cl_chart_data.get("bis", [])[-5:]
        _xds = cl_chart_data.get("xds", [])[-5:]
        _zsds = cl_chart_data.get("zsds", [])[-5:]
        _bi_zss = cl_chart_data.get("bi_zss", [])[-5:]
        _xd_zss = cl_chart_data.get("xd_zss", [])[-5:]
        _zsd_zss = cl_chart_data.get("zsd_zss", [])[-5:]
        _bcs = cl_chart_data.get("bcs", [])[-5:]
        _mmds = cl_chart_data.get("mmds", [])[-5:]
        LogUtil.info(f"[tv_history] Returning incremental update: {len(_t)} bars")
    else:
        _t = cl_chart_data["t"]
        _c = cl_chart_data["c"]
        _o = cl_chart_data["o"]
        _h = cl_chart_data["h"]
        _l = cl_chart_data["l"]
        _v = cl_chart_data["v"]
        _fxs = cl_chart_data.get("fxs", [])
        _bis = cl_chart_data.get("bis", [])
        _xds = cl_chart_data.get("xds", [])
        _zsds = cl_chart_data.get("zsds", [])
        _bi_zss = cl_chart_data.get("bi_zss", [])
        _xd_zss = cl_chart_data.get("xd_zss", [])
        _zsd_zss = cl_chart_data.get("zsd_zss", [])
        _bcs = cl_chart_data.get("bcs", [])
        _mmds = cl_chart_data.get("mmds", [])
        LogUtil.info(f"[tv_history] Returning full data: {len(_t)} bars")
    info = {
        "s": s,
        "t": _t,
        "c": _c,
        "o": _o,
        "h": _h,
        "l": _l,
        "v": _v,
        "fxs": _fxs,
        "bis": _bis,
        "xds": _xds,
        "zsds": _zsds,
        "bi_zss": _bi_zss,
        "xd_zss": _xd_zss,
        "zsd_zss": _zsd_zss,
        "bcs": _bcs,
        "mmds": _mmds,
        "update": (
            False if firstDataRequest == "true" else True
        ),  # 是否是后续更新数据
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

    # 增加订单的信息
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

    # 增加其他自定义信息
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
    """
    服务器时间
    """
    return fun.datetime_to_int(datetime.datetime.now())


@tv_bp.route("/tv/<version>/charts", methods=["GET", "POST", "DELETE"])
@login_required
def tv_charts(version):
    """
    图表
    """
    client_id = str(request.args.get("client"))
    user_id = str(request.args.get("user"))

    if request.method == "GET":
        chart_id = request.args.get("chart")
        if chart_id is None:
            # 列出保存的图表列表
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
            # 获取图表信息
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
        # 删除操作
        chart_id = request.args.get("chart")
        db.tv_chart_del("chart", chart_id, client_id, user_id)
        return {
            "status": "ok",
        }
    else:
        # 更新与保存操作
        name = request.form["name"]
        content = request.form["content"]
        symbol = request.form["symbol"]
        resolution = request.form["resolution"]
        chart_id = request.args.get("chart")

        if chart_id is None:
            # 保存新的图表信息
            id = db.tv_chart_save(
                "chart", client_id, user_id, name, content, symbol, resolution
            )
            return {
                "status": "ok",
                "id": id,
            }
        else:
            # 保存已有的图表信息
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
    """
    图表
    """
    client_id = str(request.args.get("client"))
    user_id = str(request.args.get("user"))

    if request.method == "GET":
        template = request.args.get("template")
        if template is None:
            # 列出保存的图表列表
            template_list = db.tv_chart_list("template", client_id, user_id)
            return {
                "status": "ok",
                "data": [{"name": t.name} for t in template_list],
            }
        else:
            # 获取图表信息
            template = db.tv_chart_get_by_name(
                "template", template, client_id, user_id
            )
            return {
                "status": "ok",
                "data": {"name": template.name, "content": template.content},
            }
    elif request.method == "DELETE":
        # 删除操作
        name = request.args.get("template")
        db.tv_chart_del_by_name("template", name, client_id, user_id)
        return {
            "status": "ok",
        }
    else:
        name = request.form["name"]
        content = request.form["content"]

        # 保存图表信息
        db.tv_chart_save("template", client_id, user_id, name, content, "", "")
        return {"status": "ok"}


@tv_bp.route("/tv/<version>/drawings", methods=["GET", "POST"])
@login_required
def tv_drawings(version):
    """
    图表绘图保存与加载 TODO TV 库报错，暂时不用
    """
    client_id = str(request.args.get("client"))
    user_id = str(request.args.get("user"))
    chart_id = request.args.get("chart")
    layout_id = request.args.get("layout")

    print(
        f"{request.method} client_id={client_id}, user_id={user_id}, chart_id={chart_id}, layout_id={layout_id}"
    )

    if request.method == "GET":
        return {
            "status": "ok",
            "data": {},
        }
    else:
        # 更新与保存操作
        return {"status": "ok"}