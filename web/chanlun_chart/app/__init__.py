import datetime
import json
import os
import pathlib
import time
import traceback

import pinyin
import pytz
from apscheduler.events import (
    EVENT_ALL,
    EVENT_EXECUTOR_ADDED,
    EVENT_EXECUTOR_REMOVED,
    EVENT_JOBSTORE_ADDED,
    EVENT_JOBSTORE_REMOVED,
    EVENT_JOB_ADDED,
    EVENT_JOB_REMOVED,
    EVENT_JOB_MODIFIED,
    EVENT_JOB_SUBMITTED,
    EVENT_JOB_MAX_INSTANCES,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_ERROR,
    EVENT_JOB_MISSED,
)
from apscheduler.executors.tornado import TornadoExecutor
from apscheduler.schedulers.tornado import TornadoScheduler
from flask import Flask, send_file
from flask import render_template
from flask import request

from chanlun import fun
from chanlun.db import db
from chanlun.cl_utils import (
    kcharts_frequency_h_l_map,
    query_cl_chart_config,
    web_batch_get_cl_datas,
    cl_data_to_tv_chart,
    set_cl_chart_config,
    del_cl_chart_config,
)
from chanlun.exchange import get_exchange, Market
from chanlun.config import get_data_path
from chanlun.zixuan import ZiXuan
from .alert_tasks import AlertTasks
from .xuangu_tasks import XuanguTasks
from .other_tasks import OtherTasks


def create_app(test_config=None):
    # 任务对象
    scheduler = TornadoScheduler(timezone=pytz.timezone("Asia/Shanghai"))
    scheduler.add_executor(TornadoExecutor())
    scheduler.my_task_list = {}

    def run_tasks_listener(event):
        state_map = {
            EVENT_EXECUTOR_ADDED: "已添加",
            EVENT_EXECUTOR_REMOVED: "删除调度",
            EVENT_JOBSTORE_ADDED: "已添加",
            EVENT_JOBSTORE_REMOVED: "删除存储",
            EVENT_JOB_ADDED: "已添加",
            EVENT_JOB_REMOVED: "删除作业",
            EVENT_JOB_MODIFIED: "修改作业",
            EVENT_JOB_SUBMITTED: "运行中",
            EVENT_JOB_MAX_INSTANCES: "等待运行",
            EVENT_JOB_EXECUTED: "已完成",
            EVENT_JOB_ERROR: "执行异常",
            EVENT_JOB_MISSED: "未执行",
        }
        if event.code not in state_map.keys():
            return
        if hasattr(event, "job_id"):
            job_id = event.job_id
            if job_id not in scheduler.my_task_list.keys():
                scheduler.my_task_list[job_id] = {
                    "id": job_id,
                    "name": "--",
                    "update_dt": fun.datetime_to_str(datetime.datetime.now()),
                    "next_run_dt": "--",
                    "state": "未知",
                }
            scheduler.my_task_list[job_id]["update_dt"] = fun.datetime_to_str(
                datetime.datetime.now()
            )
            job = scheduler.get_job(event.job_id)
            if job is not None:
                scheduler.my_task_list[job_id]["name"] = job.name
                scheduler.my_task_list[job_id]["next_run_dt"] = fun.datetime_to_str(
                    job.next_run_time
                )
            scheduler.my_task_list[job_id]["state"] = state_map[event.code]
            # print('任务更新', task_list[job_id])
        return

    scheduler.add_listener(run_tasks_listener, EVENT_ALL)
    scheduler.start()

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
        "currency": list(get_exchange(Market.CURRENCY).support_frequencys().keys()),
    }

    # 各个交易所默认的标的
    market_default_codes = {
        "a": get_exchange(Market.A).default_code(),
        "hk": get_exchange(Market.HK).default_code(),
        "us": get_exchange(Market.US).default_code(),
        "futures": get_exchange(Market.FUTURES).default_code(),
        "currency": get_exchange(Market.CURRENCY).default_code(),
    }

    # 各个市场的交易时间
    market_session = {
        "a": "0930-1501",
        "hk": "0930-1201,1330-1601",
        "us": "0400-0931,0930-1631,1600-2001",
        "futures": "24x7",
        "currency": "24x7",
    }

    # 各个交易所的时区 统一时区
    market_timezone = {
        "a": "Asia/Shanghai",
        "hk": "Asia/Shanghai",
        "us": "America/New_York",
        "futures": "Asia/Shanghai",
        "currency": "Asia/Shanghai",
    }

    market_types = {
        "a": "stock",
        "hk": "stock",
        "us": "stock",
        "futures": "futures",
        "currency": "crypto",
    }

    # 记录请求次数，超过则返回 no_data
    __history_req_counter = {}

    _alert_tasks = AlertTasks(scheduler)
    _alert_tasks.run()

    _xuangu_tasks = XuanguTasks(scheduler)

    _other_tasks = OtherTasks(scheduler)

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
            | set(market_frequencys["currency"])
        )
        supportedResolutions = [v for k, v in frequency_maps.items() if k in frequencys]
        return {
            "supports_search": True,
            "supports_group_request": False,
            "supported_resolutions": supportedResolutions,
            "supports_marks": False,
            "supports_timescale_marks": True,
            "supports_time": False,
            "exchanges": [
                {"value": "a", "name": "沪深", "desc": "沪深A股"},
                {"value": "hk", "name": "港股", "desc": "港股"},
                {"value": "us", "name": "美股", "desc": "美股"},
                {"value": "futures", "name": "期货", "desc": "期货"},
                {"value": "currency", "name": "数字货币", "desc": "数字货币"},
            ],
        }

    @app.route("/tv/symbol_info")
    def tv_symbol_info():
        """
        商品集合信息
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
        stocks = ex.stock_info(code)

        sector = ""
        industry = ""
        if market == "a":
            gnbk = ex.stock_owner_plate(code)
            sector = " / ".join([_g["name"] for _g in gnbk["GN"]])
            industry = " / ".join([_h["name"] for _h in gnbk["HY"]])

        info = {
            "name": stocks["code"],
            "ticker": f'{market}:{stocks["code"]}',
            "full_name": f'{market}:{stocks["code"]}',
            "description": stocks["name"],
            "exchange": market,
            "type": market_types[market],
            "session": market_session[market],
            "timezone": market_timezone[market],
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

        ex = get_exchange(Market(exchange))
        all_stocks = ex.all_stocks()

        if exchange in ["us", "currency"]:
            res_stocks = [
                stock for stock in all_stocks if query.lower() in stock["code"].lower()
            ]
        else:
            res_stocks = [
                stock
                for stock in all_stocks
                if query.lower() in stock["code"].lower()
                or query.lower() in stock["name"].lower()
                or query.lower()
                in "".join([pinyin.get_pinyin(_p)[0] for _p in stock["name"]]).lower()
            ]
        res_stocks = res_stocks[0 : int(limit)]

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

    @app.route("/tv/timescale_marks")
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
        code = code.replace("__", "/")  # 数字货币特殊处理
        cl_config = query_cl_chart_config(market, code)
        cl_config["market"] = market
        cl_config["code"] = code
        return render_template("options.html", **cl_config)

    # 设置配置项
    @app.route("/set_cl_config", methods=["POST"])
    def set_cl_config():
        market = request.form["market"]
        code = request.form["code"]
        is_del = request.form["is_del"]
        if is_del == "true":
            res = del_cl_chart_config(market, code)
            return {"ok": res}

        keys = [
            "config_use_type",
            "kline_type",
            "kline_qk",
            "fx_qj",
            "fx_bh",
            "bi_type",
            "bi_bzh",
            "bi_qj",
            "bi_fx_cgd",
            "xd_qj",
            "zsd_qj",
            "zs_bi_type",
            "zs_xd_type",
            "zs_qj",
            "zs_wzgx",
            "idx_macd_fast",
            "idx_macd_slow",
            "idx_macd_signal",
            "fx_qy",
            "xd_zs_max_lines_split",
            "allow_split_one_line_to_xd",
            "allow_bi_fx_strict",
            "enable_kchart_low_to_high",
            "bi_split_k_cross_nums",
            "fx_check_k_nums",
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
            "chart_show_fx",
        ]
        cl_config = {}
        for _k in keys:
            cl_config[_k] = request.form[_k]
            if _k in ["zs_bi_type", "zs_xd_type"]:
                cl_config[_k] = cl_config[_k].split(",")
            if cl_config[_k] == "":
                cl_config[_k] = "0"
            # print(f'{_k} : {config[_k]}')

        res = set_cl_chart_config(market, code, cl_config)
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
        zx = ZiXuan(market)
        groups = zx.get_zx_groups()
        return groups

    # 获取自选组的股票
    @app.route("/get_zixuan_stocks/<market>/<group_name>")
    def get_zixuan_stocks(market, group_name):
        zx = ZiXuan(market)
        stock_list = zx.zx_stocks(group_name)
        return {"code": 0, "msg": "", "count": len(stock_list), "data": stock_list}

    @app.route("/get_stock_zixuan/<market>/<code>")
    def get_stock_zixuan(market, code: str):
        code = code.replace("__", "/")  # 数字货币特殊处理
        zx = ZiXuan(market)
        zx_groups = zx.query_code_zx_names(code)
        return zx_groups

    @app.route("/zixuan_group/<market>", methods=["GET"])
    def zixuan_group_view(market):
        zx = ZiXuan(market)
        zx_groups = zx.get_zx_groups()
        return render_template("zixuan.html", market=market, zx_groups=zx_groups)

    @app.route("/opt_zixuan_group/<market>", methods=["POST"])
    def opt_zixuan_group(market):
        """
        操作自选组
        """
        opt = request.form["opt"]
        zx_group = request.form["zx_group"]
        zx = ZiXuan(market)
        if opt == "DEL":
            return {"ok": zx.del_zx_group(zx_group)}
        else:
            return {"ok": zx.add_zx_group(zx_group)}

    @app.route("/zixuan_opt_export", methods=["GET"])
    def opt_zixuan_export():
        """
        导出自选组
        """
        market = request.args.get("market")
        zx_group = request.args.get("zx_group")
        zx = ZiXuan(market)
        stock_list = zx.zx_stocks(zx_group)
        output = ""
        for s in stock_list:
            output += f"{s['code']},{s['name']}\n"
        try:
            down_file = get_data_path() / "zx.txt"
            down_file.write_text(output, encoding="utf-8")
            return send_file(
                down_file, as_attachment=True, download_name=f"zixuan_{zx_group}.txt"
            )
        finally:
            try:
                os.remove(down_file)
            except Exception as e:
                pass

    @app.route("/zixuan_opt_import", methods=["POST"])
    def opt_zixuan_import():
        """
        导入自选
        """
        market = request.form["market"]
        zx_group = request.form["zx_group"]
        file = request.files["file"]
        import_file = get_data_path() / "zx.txt"
        file.save(import_file)
        zx = ZiXuan(market)
        ex = get_exchange(Market(market))
        all_stocks = ex.all_stocks()
        codes = [s["code"] for s in all_stocks]
        import_nums = 0
        with open(import_file, "r", encoding="utf-8") as fp:
            for line in fp.readlines():
                try:
                    import_infos = line.strip().split(",")
                    if len(import_infos) >= 2:
                        code = import_infos[0]
                        name = import_infos[1].replace(" ", "")
                    else:
                        code = import_infos[0]
                        name = None

                    # 股票代码兼容性处理
                    if market == "a":
                        code = code.replace("SHSE.", "SH.").replace("SZSE.", "SZ.")
                        if len(code) == 6:
                            if code[0:2] in ["00", "30"]:
                                code = "SZ." + code
                            else:
                                code = "SH." + code

                    if code in codes:
                        zx.add_stock(zx_group, code, name)
                        import_nums += 1
                except Exception as e:
                    print(line, e)

        try:
            os.remove(import_file)
        except Exception as e:
            pass

        return {"ok": True, "msg": f"成功导入 {import_nums} 条记录"}

    # 设置股票的自选组
    @app.route("/set_stock_zixuan", methods=["POST"])
    def set_stock_zixuan():
        market = request.form["market"]
        opt = request.form["opt"]
        group_name = request.form["group_name"]
        code = request.form["code"]
        zx = ZiXuan(market)
        if opt == "DEL":
            res = zx.del_stock(group_name, code)
        elif opt == "ADD":
            res = zx.add_stock(group_name, code, None)
        elif opt == "COLOR":
            color = request.form["color"]
            res = zx.color_stock(group_name, code, color)
        elif opt == "SORT":
            direction = request.form["direction"]
            if direction == "top":
                res = zx.sort_top_stock(group_name, code)
            else:
                res = zx.sort_bottom_stock(group_name, code)
        else:
            res = False

        return {"ok": res}

    # 警报提醒列表
    @app.route("/alert_list/<market>")
    def alert_list(market):
        al = _alert_tasks.task_list(market)
        al = [
            {
                "id": _l.id,
                "market": _l.market,
                "task_name": _l.task_name,
                "zx_group": _l.zx_group,
                "interval_minutes": _l.interval_minutes,
                "frequency": _l.frequency,
                "check_bi_type": _l.check_bi_type,
                "check_bi_beichi": _l.check_bi_beichi,
                "check_bi_mmd": _l.check_bi_mmd,
                "check_xd_type": _l.check_xd_type,
                "check_xd_beichi": _l.check_xd_beichi,
                "check_xd_mmd": _l.check_xd_mmd,
                "is_send_msg": _l.is_send_msg,
                "is_run": _l.is_run,
            }
            for _l in al
        ]
        return {"code": 0, "msg": "", "count": len(al), "data": al}

    # 警报编辑页面
    @app.route("/alert_edit/<market>/<id>")
    def alert_edit(market, id):
        alert_config = {
            "id": "",
            "market": market,
            "task_name": "",
            "zx_group": "我的关注",
            "interval_minutes": 5,
            "frequency": "5m",
            "check_bi_type": "up,down",
            "check_bi_beichi": "pz,qs",
            "check_bi_mmd": "",
            "check_xd_type": "up,down",
            "check_xd_beichi": "pz,qs",
            "check_xd_mmd": "",
            "is_send_msg": 1,
            "is_run": 1,
        }
        if id != "0":
            _alert_config = _alert_tasks.alert_get(id)
            if _alert_config is not None:
                alert_config = {
                    "id": _alert_config.id,
                    "market": _alert_config.market,
                    "task_name": _alert_config.task_name,
                    "zx_group": _alert_config.zx_group,
                    "interval_minutes": _alert_config.interval_minutes,
                    "frequency": _alert_config.frequency,
                    "check_bi_type": _alert_config.check_bi_type,
                    "check_bi_beichi": _alert_config.check_bi_beichi,
                    "check_bi_mmd": _alert_config.check_bi_mmd,
                    "check_xd_type": _alert_config.check_xd_type,
                    "check_xd_beichi": _alert_config.check_xd_beichi,
                    "check_xd_mmd": _alert_config.check_xd_mmd,
                    "is_send_msg": _alert_config.is_send_msg,
                    "is_run": _alert_config.is_run,
                }

        # 获取自选组
        zx = ZiXuan(market)
        zixuan_groups = zx.zixuan_list

        # 交易所支持周期
        frequencys = get_exchange(Market(market)).support_frequencys()

        return render_template(
            "alert.html",
            zixuan_groups=zixuan_groups,
            frequencys=frequencys,
            **alert_config,
        )

    @app.route("/alert_save", methods=["POST"])
    def alert_save():
        alert_config = {
            "id": request.form["id"],
            "market": request.form["market"],
            "task_name": request.form["task_name"],
            "interval_minutes": int(request.form["interval_minutes"]),
            "zx_group": request.form["zx_group"],
            "frequency": request.form["frequency"],
            "check_bi_type": request.form["check_bi_type"],
            "check_bi_beichi": request.form["check_bi_beichi"],
            "check_bi_mmd": request.form["check_bi_mmd"],
            "check_xd_type": request.form["check_xd_type"],
            "check_xd_beichi": request.form["check_xd_beichi"],
            "check_xd_mmd": request.form["check_xd_mmd"],
            "is_send_msg": int(request.form["is_send_msg"]),
            "is_run": int(request.form["is_run"]),
        }
        _alert_tasks.alert_save(alert_config)
        return {"ok": True}

    @app.route("/alert_del/<id>")
    def alert_del(id):
        res = _alert_tasks.alert_del(id)
        return {"ok": res}

    @app.route("/alert_records/<market>")
    def alert_records(market):
        records = db.alert_record_query(market)
        rls = [
            {
                "code": _r.stock_code,
                "name": _r.stock_name,
                "frequency": _r.frequency,
                "jh_type": _r.alert_msg,
                "is_done": _r.bi_is_done,
                "is_td": _r.bi_is_td,
                "datetime_str": fun.datetime_to_str(_r.alert_dt),
            }
            for _r in records
        ]
        return {
            "code": 0,
            "msg": "",
            "count": len(rls),
            "data": rls,
        }

    @app.route("/jobs")
    def jobs():
        return render_template("jobs.html", jobs=list(scheduler.my_task_list.values()))

    @app.route("/xuangu/task_list/<market>")
    def xuangu_task_list(market):
        # 获取自选组
        zx = ZiXuan(market)
        zixuan_groups = zx.zixuan_list

        # 交易所支持周期
        frequencys = get_exchange(Market(market)).support_frequencys()

        # 选股配置
        xuangu_task_list = _xuangu_tasks.xuangu_task_config_list()

        # task_memo
        task_infos = {
            _k: {
                "task_memo": _v["task_memo"],
                "frequency_memo": _v["frequency_memo"],
            }
            for _k, _v in xuangu_task_list.items()
        }

        return render_template(
            "xuangu_list.html",
            market=market,
            tasks=xuangu_task_list,
            task_infos=task_infos,
            zixuan_groups=zixuan_groups,
            frequencys=frequencys,
        )

    @app.route("/xuangu/task_add", methods=["POST"])
    def xuangu_task_add():
        market = request.form["market"]
        task_name = request.form["task_name"]
        frequencys = request.form["frequencys"]
        zx_group = request.form["zx_group"]
        opt_type = request.form["opt_type"]

        frequencys = frequencys.split(",")
        opt_type = opt_type.split(",")

        if task_name not in _xuangu_tasks.xuangu_task_config_list().keys():
            return {"ok": False, "msg": "选股任务不存在"}

        allow_freq_num = _xuangu_tasks.xuangu_task_config_list()[task_name][
            "frequency_num"
        ]
        if len(frequencys) != allow_freq_num:
            return {
                "ok": False,
                "msg": f"选股周期错误，该任务可选周期数量 : {allow_freq_num}",
            }

        run_res = _xuangu_tasks.run_xuangu(
            market, task_name, frequencys, opt_type, zx_group
        )

        return {
            "ok": run_res,
            "msg": "选股任务已存在，请在当前任务中查看任务" if run_res is False else "",
        }

    @app.route("/setting", methods=["GET"])
    def setting():
        # 查询配置
        proxy = db.cache_get("req_proxy")
        fs_setting = db.cache_get("fs_keys")
        set_config = {
            "fs_app_id": fs_setting["fs_app_id"] if fs_setting is not None else "",
            "fs_app_secret": (
                fs_setting["fs_app_secret"] if fs_setting is not None else ""
            ),
            "fs_user_id": fs_setting["fs_user_id"] if fs_setting is not None else "",
            "proxy_host": proxy["host"] if proxy is not None else "",
            "proxy_port": proxy["port"] if proxy is not None else "",
        }
        return render_template("setting.html", **set_config)

    @app.route("/setting/save", methods=["POST"])
    def setting_save():
        proxy = {
            "host": request.form["proxy_host"],
            "port": request.form["proxy_port"],
        }
        fs_keys = {
            "fs_app_id": request.form["fs_app_id"],
            "fs_app_secret": request.form["fs_app_secret"],
            "fs_user_id": request.form["fs_user_id"],
        }
        db.cache_set("req_proxy", proxy)
        db.cache_set("fs_keys", fs_keys)

        return {"ok": True}

    return app
