import datetime
import json
import os
import time
import traceback
from flask_login import LoginManager, UserMixin, login_required, login_user

import pinyin

from flask import Flask, redirect, send_file
from flask import render_template
from flask import request

from chanlun.base import Market
from chanlun import fun, config
from chanlun.cl_utils import (
    kcharts_frequency_h_l_map,
    query_cl_chart_config,
    web_batch_get_cl_datas,
    cl_data_to_tv_chart,
    set_cl_chart_config,
    del_cl_chart_config,
)
from chanlun.db import db
from chanlun.exchange import get_exchange
from chanlun.config import get_data_path
from chanlun.zixuan import ZiXuan


"""


"""


def create_app(test_config=None):

    # 项目中的周期与 tv 的周期对应表
    frequency_maps = {
        "10s": "10S",
        "30s": "30S",
        "1m": "1",
        "2m": "2",
        "3m": "3",
        "5m": "5",
        "10m": "10",
        "15m": "15",
        "30m": "30",
        "60m": "60",
        "120m": "120",
        "3h": "180",
        "4h": "240",
        "d": "1D",
        "2d": "2D",
        "w": "1W",
        "m": "1M",
        "y": "12M",
    }

    resolution_maps = dict(zip(frequency_maps.values(), frequency_maps.keys()))

    # 各个市场支持的时间周期
    market_frequencys = {
        "a": list(get_exchange(Market.A).support_frequencys().keys()),
        "hk": list(get_exchange(Market.HK).support_frequencys().keys()),
        "us": list(get_exchange(Market.US).support_frequencys().keys()),
        "futures": list(get_exchange(Market.FUTURES).support_frequencys().keys()),
    }

    # 各个交易所默认的标的
    market_default_codes = {
        "a": get_exchange(Market.A).default_code(),
        "hk": get_exchange(Market.HK).default_code(),
        "us": get_exchange(Market.US).default_code(),
        "futures": get_exchange(Market.FUTURES).default_code(),
    }

    # 各个市场的交易时间
    market_session = {
        "a": "24x7",
        "hk": "0930-1201,1330-1601",
        "us": "0400-0931,0930-1631,1600-2001",
        "futures": "24x7",
    }

    # 各个交易所的时区 统一时区
    market_timezone = {
        "a": "Asia/Shanghai",
        "hk": "Asia/Shanghai",
        "us": "America/New_York",
        "futures": "Asia/Shanghai",
    }

    market_types = {
        "a": "stock",
        "hk": "stock",
        "us": "stock",
        "futures": "futures",
    }

    allow_market_codes = {
        "a": [
            {"code": "SH.000001", "name": "上证指数"},
            {"code": "SH.600519", "name": "贵州茅台"},
        ],
        "hk": [
            {"code": "KH.00700", "name": "腾讯控股"},
            {"code": "KH.01810", "name": "小米集团－Ｗ"},
        ],
        "us": [{"code": "AAPL", "name": "苹果"}, {"code": "TSLA", "name": "特斯拉"}],
        "futures": [
            {"code": "QS.RBL8", "name": "螺纹主连"},
            {"code": "QZ.MAL8", "name": "甲醇主连"},
        ],
    }

    # 记录请求次数，超过则返回 no_data
    __history_req_counter = {}

    __log = fun.get_logger()

    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.logger.addFilter(
        lambda record: "/static/" not in record.getMessage().lower()
    )  # 过滤静态资源请求日志

    @app.route("/")
    def index_show():
        """
        首页
        """

        return render_template("index.html", market_default_codes=market_default_codes)

    @app.route("/tv/config")
    def tv_config():
        """
        配置项
        """
        frequencys = list(
            set(market_frequencys["a"])
            | set(market_frequencys["hk"])
            | set(market_frequencys["us"])
            | set(market_frequencys["futures"])
        )
        supportedResolutions = [v for k, v in frequency_maps.items() if k in frequencys]
        return {
            "supports_search": True,
            "supports_group_request": False,
            "supported_resolutions": supportedResolutions,
            "supports_marks": False,
            "supports_timescale_marks": False,
            "supports_time": False,
            "exchanges": [
                {"value": "a", "name": "沪深", "desc": "沪深A股"},
                {"value": "hk", "name": "港股", "desc": "港股"},
                {"value": "us", "name": "美股", "desc": "美股"},
                {"value": "futures", "name": "期货", "desc": "期货"},
            ],
        }

    @app.route("/tv/symbol_info")
    def tv_symbol_info():
        """
        商品集合信息
        supports_search is True 则不会调用这个接口
        """
        group = request.args.get("group")

        info = {
            "symbol": [s["code"] for s in allow_market_codes[group]],
            "description": [s["name"] for s in allow_market_codes[group]],
            "exchange-listed": group,
            "exchange-traded": group,
        }
        return info

    @app.route("/tv/symbols")
    def tv_symbols():
        """
        商品解析
        """
        symbol: str = request.args.get("symbol")
        symbol: list = symbol.split(":")
        market: str = symbol[0].lower()
        code: str = symbol[1]

        ex = get_exchange(Market(market))
        if code not in [_s["code"] for _s in allow_market_codes[market]]:
            code = allow_market_codes[market][0]["code"]
        stocks = ex.stock_info(code)
        print(code, stocks)

        sector = ""
        industry = ""

        info = {
            "name": stocks["code"],
            "ticker": f'{market}:{stocks["code"]}',
            "full_name": f'{market}:{stocks["code"]}',
            "description": stocks["name"],
            "exchange": market,
            "type": market_types[market],
            "session": market_session[market],
            "timezone": market_timezone[market],
            "pricescale": 1000 if market in ["a", "hk", "us", "futures"] else 100000000,
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
            "has_intraday": True,
            "has_seconds": True if market == "futures" else False,
            "has_daily": True,
            "has_weekly_and_monthly": True,
            "sector": sector,
            "industry": industry,
        }
        return info

    @app.route("/tv/search")
    def tv_search():
        """
        商品检索
        """
        query = request.args.get("query")
        type = request.args.get("type")
        exchange = request.args.get("exchange")
        limit = request.args.get("limit")

        res_stocks = allow_market_codes[exchange]

        infos = []
        for stock in res_stocks:
            infos.append(
                {
                    "symbol": stock["code"],
                    "name": stock["code"],
                    "full_name": f'{exchange}:{stock["code"]}',
                    "description": stock["name"],
                    "exchange": exchange,
                    "ticker": f'{exchange}:{stock["code"]}',
                    "type": type,
                    "session": market_session[exchange],
                    "timezone": market_timezone[exchange],
                    "supported_resolutions": [
                        v
                        for k, v in frequency_maps.items()
                        if k in market_frequencys[exchange]
                    ],
                }
            )
        return infos

    @app.route("/tv/history")
    def tv_history():
        """
        K线柱
        """

        symbol = request.args.get("symbol")
        _from = request.args.get("from")
        _to = request.args.get("to")
        resolution = request.args.get("resolution")
        countback = request.args.get("countback")
        firstDataRequest = request.args.get("firstDataRequest", "false")

        # 从 cookie 中读取 web_user_id
        web_user_id = request.cookies.get("web_user_id")

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

        if code not in [_s["code"] for _s in allow_market_codes[market]]:
            code = allow_market_codes[market][0]["code"]

        ex = get_exchange(Market(market))

        # 判断当前是否可交易时间
        if firstDataRequest == "false" and ex.now_trading() is False:
            return {"s": "no_data", "nextTime": int(now_time + (10 * 60))}

        frequency = resolution_maps[resolution]
        cl_config = query_cl_chart_config(market, code, suffix=web_user_id)
        frequency_low, kchart_to_frequency = kcharts_frequency_h_l_map(
            market, frequency
        )
        if (
            cl_config["enable_kchart_low_to_high"] == "1"
            and kchart_to_frequency is not None
        ):
            # 如果开启并设置的该级别的低级别数据，获取低级别数据，并在转换成高级图表展示
            # s_time = time.time()
            klines = ex.klines(code, frequency_low)
            # __log.info(f'{code} - {frequency_low} enable low to high get klines time : {time.time() - s_time}')
            # s_time = time.time()
            cd = web_batch_get_cl_datas(
                market, code, {frequency_low: klines}, cl_config
            )[0]
            # __log.info(f'{code} - {frequency_low} enable low to high get cd time : {time.time() - s_time}')
        else:
            kchart_to_frequency = None
            # s_time = time.time()
            klines = ex.klines(code, frequency)
            # __log.info(f'{code} - {frequency} get klines time : {time.time() - s_time}')
            # s_time = time.time()
            cd = web_batch_get_cl_datas(market, code, {frequency: klines}, cl_config)[0]
            # __log.info(f'{code} - {frequency} get cd time : {time.time() - s_time}')

        # 将缠论数据，转换成 tv 画图的坐标数据
        # s_time = time.time()
        cl_chart_data = cl_data_to_tv_chart(
            cd, cl_config, to_frequency=kchart_to_frequency
        )
        # __log.info(f'{code} - {frequency} to tv chart data time : {time.time() - s_time}')

        info = {
            "s": s,
            "t": cl_chart_data["t"],
            "c": cl_chart_data["c"],
            "o": cl_chart_data["o"],
            "h": cl_chart_data["h"],
            "l": cl_chart_data["l"],
            "v": cl_chart_data["v"],
            "fxs": cl_chart_data["fxs"],
            "bis": cl_chart_data["bis"],
            "xds": cl_chart_data["xds"],
            "zsds": cl_chart_data["zsds"],
            "bi_zss": cl_chart_data["bi_zss"],
            "xd_zss": cl_chart_data["xd_zss"],
            "zsd_zss": cl_chart_data["zsd_zss"],
            "bcs": cl_chart_data["bcs"],
            "mmds": cl_chart_data["mmds"],
        }
        return info

    @app.route("/tv/time")
    def tv_time():
        """
        服务器时间
        """
        return fun.datetime_to_int(datetime.datetime.now())

    @app.route("/tv/<version>/charts", methods=["GET", "POST", "DELETE"])
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

    @app.route("/tv/<version>/study_templates", methods=["GET", "POST", "DELETE"])
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

    # 查询配置项
    @app.route("/get_cl_config/<market>/<code>")
    def get_cl_config(market, code: str):
        # 从 cookie 中读取 web_user_id
        web_user_id = request.cookies.get("web_user_id")
        code = code.replace("__", "/")  # 数字货币特殊处理
        cl_config = query_cl_chart_config(market, code, suffix=web_user_id)
        cl_config["market"] = market
        cl_config["code"] = code
        return render_template("options.html", **cl_config)

    # 设置配置项
    @app.route("/set_cl_config", methods=["POST"])
    def set_cl_config():
        # 从 cookie 中读取 web_user_id
        web_user_id = request.cookies.get("web_user_id")

        market = request.form["market"]
        code = request.form["code"]
        is_del = request.form["is_del"]
        if is_del == "true":
            res = del_cl_chart_config(market, code, suffix=web_user_id)
            return {"ok": res}

        keys = [
            "config_use_type",
            # 个人定制配置
            "kline_qk",
            "judge_zs_qs_level",
            # K线配置
            "kline_type",
            # 分型配置
            "fx_qy",
            "fx_qj",
            "fx_bh",
            # 笔配置
            "bi_type",
            "bi_bzh",
            "bi_qj",
            "bi_fx_cgd",
            "bi_split_k_cross_nums",
            "fx_check_k_nums",
            "allow_bi_fx_strict",
            # 线段配置
            "xd_qj",
            "zsd_qj",
            "xd_zs_max_lines_split",
            "xd_allow_bi_pohuai",
            "xd_allow_split_no_highlow",
            "xd_allow_split_zs_kz",
            "xd_allow_split_zs_more_line",
            "xd_allow_split_zs_no_direction",
            # 中枢配置
            "zs_bi_type",
            "zs_xd_type",
            "zs_qj",
            "zs_wzgx",
            # MACD 配置（计算力度背驰）
            "idx_macd_fast",
            "idx_macd_slow",
            "idx_macd_signal",
            # 买卖点计算
            "cl_mmd_cal_qs_1mmd",
            "cl_mmd_cal_not_qs_3mmd_1mmd",
            "cl_mmd_cal_qs_3mmd_1mmd",
            "cl_mmd_cal_qs_not_lh_2mmd",
            "cl_mmd_cal_qs_bc_2mmd",
            "cl_mmd_cal_3mmd_not_lh_bc_2mmd",
            "cl_mmd_cal_1mmd_not_lh_2mmd",
            "cl_mmd_cal_3mmd_xgxd_not_bc_2mmd",
            "cl_mmd_cal_not_in_zs_3mmd",
            "cl_mmd_cal_not_in_zs_gt_9_3mmd",
            # 画图配置
            "enable_kchart_low_to_high",
            "chart_show_fx",
            "chart_show_bi",
            "chart_show_xd",
            "chart_show_zsd",
            "chart_show_qsd",
            "chart_show_bi_zs",
            "chart_show_xd_zs",
            "chart_show_zsd_zs",
            "chart_show_qsd_zs",
            "chart_show_bi_mmd",
            "chart_show_xd_mmd",
            "chart_show_zsd_mmd",
            "chart_show_qsd_mmd",
            "chart_show_bi_bc",
            "chart_show_xd_bc",
            "chart_show_zsd_bc",
            "chart_show_qsd_bc",
        ]
        cl_config = {}
        for _k in keys:
            cl_config[_k] = request.form[_k]
            if _k in ["zs_bi_type", "zs_xd_type"]:
                cl_config[_k] = cl_config[_k].split(",")
            if cl_config[_k] == "":
                cl_config[_k] = "0"
            # print(f"{_k} : {cl_config[_k]}")

        res = set_cl_chart_config(market, code, cl_config, suffix=web_user_id)
        return {"ok": res}

    # 股票涨跌幅
    @app.route("/ticks", methods=["POST"])
    def ticks():
        market = request.form["market"]
        codes = request.form["codes"]
        codes = json.loads(codes)
        ex = get_exchange(Market(market))
        stock_ticks = ex.ticks(codes)
        try:
            now_trading = ex.now_trading()
            res_ticks = [
                {"code": _c, "rate": round(float(_t.rate), 2)}
                for _c, _t in stock_ticks.items()
            ]
            return {"now_trading": now_trading, "ticks": res_ticks}
        except Exception as e:
            traceback.print_exc()
        return {"now_trading": False, "ticks": []}

    # 获取自选组列表
    @app.route("/get_zixuan_groups/<market>")
    def get_zixuan_groups(market):
        return [{"name": "我的自选"}]

    # 获取自选组的股票
    @app.route("/get_zixuan_stocks/<market>/<group_name>")
    def get_zixuan_stocks(market, group_name):
        return {
            "code": 0,
            "msg": "",
            "count": 1,
            "data": [
                {"code": _s["code"], "name": _s["name"], "color": "", "memo": ""}
                for _s in allow_market_codes[market]
            ],
        }

    @app.route("/get_stock_zixuan/<market>/<code>")
    def get_stock_zixuan(market, code: str):
        return [
            {
                "zx_name": "我的关注",
                "code": code,
                "exists": 1,
            }
        ]

    return app
