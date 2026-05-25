import random
import uuid

from flask import render_template, request, session
from flask_login import login_required

from chanlun import cl, fun
from chanlun.base import Market
from chanlun.cl_utils import cl_data_to_tv_chart, query_cl_chart_config
from chanlun.exchange import get_exchange
from chanlun.exchange.exchange import convert_stock_kline_frequency

# 回测回放 session 存储
_replay_sessions: dict = {}

# 固定周期：日线 + 30分钟
HIGH_FREQ = "d"
SMALL_FREQ = "30m"


def register_backtest_routes(app):

    @app.route("/backtest")
    @login_required
    def backtest_page():
        """回测学习页面"""
        return render_template("backtest.html")

    @app.route("/backtest/tv/history")
    @login_required
    def backtest_tv_history():
        """回测 TV 历史数据 — 仅图表初始化时调用一次，全量返回到 current_pos"""
        resolution = request.args.get("resolution")

        session_id = session.get("bt_session_id")
        rs = _replay_sessions.get(session_id)
        if rs is None:
            return {"s": "no_data"}

        if resolution == "30":
            cd = rs["cl_small"]
        else:
            cd = rs["cl_high"]

        cl_chart_data = cl_data_to_tv_chart(cd, rs["cl_config"])
        if cl_chart_data is None:
            return {"s": "no_data"}

        # 截取所有数据到 current_pos
        current_pos = rs["current_pos"]
        small_klines = rs["all_klines"]
        if current_pos < len(small_klines):
            cutoff_time = fun.datetime_to_int(small_klines.iloc[current_pos]["date"])
        else:
            cutoff_time = cl_chart_data["t"][-1] if cl_chart_data["t"] else 0

        def _slice(data_list, cutoff):
            if not data_list:
                return data_list
            result = []
            for item in data_list:
                pts = item.get("points")
                if pts:
                    if isinstance(pts, dict):
                        if pts.get("time", 0) <= cutoff:
                            result.append(item)
                    elif isinstance(pts, list) and len(pts) > 0:
                        if pts[0].get("time", 0) <= cutoff:
                            result.append(item)
                else:
                    result.append(item)
            return result

        t = [v for v in cl_chart_data["t"] if v <= cutoff_time]
        idx = len(t)

        return {
            "s": "ok",
            "t": t,
            "c": cl_chart_data["c"][:idx],
            "o": cl_chart_data["o"][:idx],
            "h": cl_chart_data["h"][:idx],
            "l": cl_chart_data["l"][:idx],
            "v": cl_chart_data["v"][:idx],
            "fxs": _slice(cl_chart_data["fxs"], cutoff_time),
            "bis": _slice(cl_chart_data["bis"], cutoff_time),
            "xds": _slice(cl_chart_data["xds"], cutoff_time),
            "zsds": _slice(cl_chart_data["zsds"], cutoff_time),
            "bi_zss": _slice(cl_chart_data["bi_zss"], cutoff_time),
            "xd_zss": _slice(cl_chart_data["xd_zss"], cutoff_time),
            "zsd_zss": _slice(cl_chart_data["zsd_zss"], cutoff_time),
            "bcs": _slice(cl_chart_data["bcs"], cutoff_time),
            "mmds": _slice(cl_chart_data["mmds"], cutoff_time),
        }

    @app.route("/backtest/start", methods=["POST"])
    @login_required
    def backtest_start():
        """随机选股，初始计算，开始回放"""
        ex = get_exchange(Market.A)
        all_stocks = ex.all_stocks()

        # 过滤：只要 60/00/30 开头的股票
        valid_stocks = [
            s for s in all_stocks if s["code"].split(".")[-1][:2] in ["60", "00", "30"]
        ]

        high_freq = HIGH_FREQ
        small_freq = SMALL_FREQ

        max_attempts = 50
        for _ in range(max_attempts):
            stock = random.choice(valid_stocks)
            klines = ex.klines(stock["code"], small_freq)
            if klines is not None and len(klines) >= 4000:
                break
        else:
            return {"ok": False, "msg": "无法找到满足条件的股票，请重试"}

        # 从倒数 1000 根开始回放
        start_pos = len(klines) - 1000

        cl_config = query_cl_chart_config("a", stock["code"])

        # 初始计算小级别（30m）
        cd_small = cl.CL(stock["code"], small_freq, cl_config)
        cd_small.process_klines(klines.iloc[: start_pos + 1])

        # 初始计算大级别（日线）
        high_klines = convert_stock_kline_frequency(
            klines.iloc[: start_pos + 1].copy(), high_freq
        )
        cd_high = cl.CL(stock["code"], high_freq, cl_config)
        cd_high.process_klines(high_klines)

        # 生成 session
        session_id = str(uuid.uuid4())
        session["bt_session_id"] = session_id

        _replay_sessions[session_id] = {
            "code": stock["code"],
            "name": stock["name"],
            "display_id": f"股票 #{random.randint(1, 9999)}",
            "small_freq": small_freq,
            "high_freq": high_freq,
            "all_klines": klines,
            "current_pos": start_pos,
            "start_pos": start_pos,
            "cl_small": cd_small,
            "cl_high": cd_high,
            "cl_config": cl_config,
            "running": True,
        }

        current_bar = klines.iloc[start_pos]
        return {
            "ok": True,
            "session_key": session_id,
            "display_id": _replay_sessions[session_id]["display_id"],
            "small_freq": small_freq,
            "high_freq": high_freq,
            "current_price": float(current_bar["close"]),
            "current_time": fun.datetime_to_str(current_bar["date"]),
        }

    @app.route("/backtest/step", methods=["POST"])
    @login_required
    def backtest_step():
        """回放前进一步，返回新的 K 线和缠论数据"""
        session_id = session.get("bt_session_id")
        rs = _replay_sessions.get(session_id)
        if rs is None or not rs["running"]:
            return {"ok": False, "msg": "回放未开始或已结束"}

        rs["current_pos"] += 1
        pos = rs["current_pos"]
        klines = rs["all_klines"]

        if pos >= len(klines):
            rs["running"] = False
            return {"ok": False, "msg": "回放已结束", "finished": True}

        # 增量更新小级别 CL
        rs["cl_small"].process_klines(klines.iloc[: pos + 1])

        # 增量更新大级别 CL
        high_klines = convert_stock_kline_frequency(
            klines.iloc[: pos + 1].copy(), rs["high_freq"]
        )
        rs["cl_high"].process_klines(high_klines)

        # 生成 TV 数据
        cl_small_data = cl_data_to_tv_chart(rs["cl_small"], rs["cl_config"])
        cl_high_data = cl_data_to_tv_chart(rs["cl_high"], rs["cl_config"])

        current_bar = klines.iloc[pos]
        new_bar = {
            "time": fun.datetime_to_int(current_bar["date"]),
            "close": float(current_bar["close"]),
            "open": float(current_bar["open"]),
            "high": float(current_bar["high"]),
            "low": float(current_bar["low"]),
            "volume": float(current_bar["volume"]),
        }

        # 检查大级别是否有新 bar
        new_high_bar = None
        if len(cl_high_data["t"]) > 0:
            last_high_time = cl_high_data["t"][-1]
            if len(cl_high_data["t"]) >= 2:
                if last_high_time != cl_high_data["t"][-2]:
                    new_high_bar = {
                        "time": last_high_time,
                        "close": cl_high_data["c"][-1],
                        "open": cl_high_data["o"][-1],
                        "high": cl_high_data["h"][-1],
                        "low": cl_high_data["l"][-1],
                        "volume": cl_high_data["v"][-1],
                    }
            elif len(cl_high_data["t"]) == 1:
                new_high_bar = {
                    "time": last_high_time,
                    "close": cl_high_data["c"][-1],
                    "open": cl_high_data["o"][-1],
                    "high": cl_high_data["h"][-1],
                    "low": cl_high_data["l"][-1],
                    "volume": cl_high_data["v"][-1],
                }

        return {
            "ok": True,
            "current_pos": pos,
            "total_bars": len(klines),
            "new_bar": new_bar,
            "new_high_bar": new_high_bar,
            "cl_small": cl_small_data,
            "cl_high": cl_high_data,
            "current_price": float(current_bar["close"]),
        }

    @app.route("/backtest/stop", methods=["POST"])
    @login_required
    def backtest_stop():
        """停止回放并清理 session"""
        session_id = session.get("bt_session_id")
        if session_id and session_id in _replay_sessions:
            del _replay_sessions[session_id]
        session.pop("bt_session_id", None)
        return {"ok": True}
