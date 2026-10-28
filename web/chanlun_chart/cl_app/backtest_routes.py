import random
import uuid

from flask import render_template, request, session
from flask_login import login_required

from chanlun import cl, fun
from chanlun.base import Market
from chanlun.cl_interface import ICL, Config
from chanlun.cl_utils import cl_data_to_tv_chart, query_cl_chart_config
from chanlun.db import db
from chanlun.exchange import get_exchange
from chanlun.exchange.exchange import convert_stock_kline_frequency
from chanlun.tools.klines_tool import klines_to_heikin_ashi_klines

# 回测回放 session 存储
_replay_sessions: dict = {}

# 固定周期：日线 + 周线
HIGH_FREQ = "w"
SMALL_FREQ = "d"

# 回测记录相关常量（初始资金与前端 BT_CONFIG.INITIAL_CAPITAL 保持一致）
BACKTEST_INITIAL_CAPITAL = 100000.0
BACKTEST_MAX_TRADE_RECORDS = 2000
BACKTEST_RECORD_LIST_LIMIT = 200
BACKTEST_DIRECTIONS = {"买入", "卖出", "平仓"}


def _bt_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _bt_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _bt_record_to_dict(record) -> dict:
    """回测记录 model 转前端可用的 dict"""
    return {
        "id": int(record.id),
        "dt": fun.datetime_to_str(record.dt) if record.dt else "",
        "stock_code": record.stock_code or "",
        "stock_name": record.stock_name or "",
        "frequency": record.frequency or "",
        "initial_capital": round(float(record.initial_capital or 0), 2),
        "final_capital": round(float(record.final_capital or 0), 2),
        "position_value": round(float(record.position_value or 0), 2),
        "unrealized_pnl": round(float(record.unrealized_pnl or 0), 2),
        "realized_pnl": round(float(record.realized_pnl or 0), 2),
        "total_value": round(float(record.total_value or 0), 2),
        "profit": round(float(record.profit or 0), 2),
        "profit_rate": round(float(record.profit_rate or 0), 2),
        "total_trades": int(record.total_trades or 0),
        "close_count": int(record.close_count or 0),
        "win_count": int(record.win_count or 0),
        "lose_count": int(record.lose_count or 0),
        "win_rate": round(float(record.win_rate or 0), 2),
        "start_time": record.start_time or "",
        "end_time": record.end_time or "",
        "start_pos": int(record.start_pos or 0),
        "end_pos": int(record.end_pos or 0),
        "total_bars": int(record.total_bars or 0),
    }


def _bt_trade_to_dict(trade) -> dict:
    """回测交易明细 model 转前端可用的 dict"""
    return {
        "id": int(trade.id),
        "seq": int(trade.seq or 0),
        "time": trade.trade_time or "",
        "direction": trade.direction or "",
        "price": round(float(trade.price or 0), 3),
        "qty": int(trade.qty or 0),
        "pnl": round(float(trade.pnl or 0), 2),
    }


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

        if resolution == "1D":
            cd: ICL = rs["cl_small"]
        else:
            cd: ICL = rs["cl_high"]

        cl_chart_data = cl_data_to_tv_chart(cd, rs["cl_config"])
        if cl_chart_data is None:
            return {"s": "no_data"}

        return {
            "s": "ok",
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
            "price": cd.get_src_klines()[-1].c,
            "mmds": cl_chart_data["mmds"],
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
            if klines is not None and len(klines) >= 2000:
                break
        else:
            return {"ok": False, "msg": "无法找到满足条件的股票，请重试"}

        # 从 800 根之后到倒数 500 根之间随机选取开始位置，避免每次回放时间固定
        start_pos = random.randint(800, len(klines) - 500)

        cl_config = query_cl_chart_config("a", stock["code"])

        cal_klines = klines.copy()
        if cl_config.get("kline_type") == Config.KLINE_TYPE_HEIKIN_ASHI.value:
            cal_klines = klines_to_heikin_ashi_klines(cal_klines)
        if cl_config.get("kline_type") == Config.KLINE_TYPE_HEIKIN_ASHI_3.value:
            cal_klines = klines_to_heikin_ashi_klines(cal_klines, smooth_num=3)

        # 初始计算小级别（日线）
        cd_small = cl.CL(stock["code"], small_freq, cl_config)
        cd_small.process_klines(cal_klines.iloc[: start_pos + 1])

        # 初始计算大级别（周线）
        high_klines = convert_stock_kline_frequency(
            klines.iloc[: start_pos + 1].copy(), high_freq
        )
        cal_high_klines = high_klines.copy()
        if cl_config.get("kline_type") == Config.KLINE_TYPE_HEIKIN_ASHI.value:
            cal_high_klines = klines_to_heikin_ashi_klines(cal_high_klines)
        if cl_config.get("kline_type") == Config.KLINE_TYPE_HEIKIN_ASHI_3.value:
            cal_high_klines = klines_to_heikin_ashi_klines(
                cal_high_klines, smooth_num=3
            )
        cd_high = cl.CL(stock["code"], high_freq, cl_config)
        cd_high.process_klines(cal_high_klines)

        # 生成 session
        session_id = str(uuid.uuid4())
        session["bt_session_id"] = session_id

        _replay_sessions[session_id] = {
            "code": stock["code"],
            "name": stock["name"],
            "display_id": "缠论股票",
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
            "start_pos": start_pos,
            "total_bars": len(klines),
            "current_price": float(current_bar["close"]),
            "current_bar_time": fun.datetime_to_int(current_bar["date"]),
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
        cal_klines = klines.copy()
        if rs["cl_config"].get("kline_type") == Config.KLINE_TYPE_HEIKIN_ASHI.value:
            cal_klines = klines_to_heikin_ashi_klines(cal_klines)
        if rs["cl_config"].get("kline_type") == Config.KLINE_TYPE_HEIKIN_ASHI_3.value:
            cal_klines = klines_to_heikin_ashi_klines(cal_klines, smooth_num=3)
        rs["cl_small"].process_klines(cal_klines.iloc[: pos + 1])

        # 增量更新大级别 CL
        high_klines = convert_stock_kline_frequency(
            klines.iloc[: pos + 1].copy(), rs["high_freq"]
        )
        cal_high_klines = high_klines.copy()
        if rs["cl_config"].get("kline_type") == Config.KLINE_TYPE_HEIKIN_ASHI.value:
            cal_high_klines = klines_to_heikin_ashi_klines(cal_high_klines)
        if rs["cl_config"].get("kline_type") == Config.KLINE_TYPE_HEIKIN_ASHI_3.value:
            cal_high_klines = klines_to_heikin_ashi_klines(
                cal_high_klines, smooth_num=3
            )
        rs["cl_high"].process_klines(cal_high_klines)

        # 生成 TV 数据
        cl_small_data = cl_data_to_tv_chart(rs["cl_small"], rs["cl_config"])
        cl_high_data = cl_data_to_tv_chart(rs["cl_high"], rs["cl_config"])

        # 图表新增的 K 线必须使用计算后的 K 线（如平均K线），否则后续每一步推送的都是原始K线
        new_bar = {
            "time": cl_small_data["t"][-1],
            "close": cl_small_data["c"][-1],
            "open": cl_small_data["o"][-1],
            "high": cl_small_data["h"][-1],
            "low": cl_small_data["l"][-1],
            "volume": cl_small_data["v"][-1],
        }

        # 大级别新 bar
        last_high_time = cl_high_data["t"][-1]
        new_high_bar = {
            "time": last_high_time,
            "close": cl_high_data["c"][-1],
            "open": cl_high_data["o"][-1],
            "high": cl_high_data["h"][-1],
            "low": cl_high_data["l"][-1],
            "volume": cl_high_data["v"][-1],
        }

        current_bar = klines.iloc[pos]
        return {
            "ok": True,
            "current_pos": pos,
            "total_bars": len(klines),
            "new_bar": new_bar,
            "new_high_bar": new_high_bar,
            "cl_small": cl_small_data,
            "cl_high": cl_high_data,
            "current_price": float(current_bar["close"]),
            "current_bar_time": new_bar["time"],
            "current_time": fun.datetime_to_str(current_bar["date"]),
        }

    @app.route("/backtest/finish", methods=["POST"])
    @login_required
    def backtest_finish():
        """回测完成：接收交易记录与资金情况，服务端重新统计后保存"""
        data = request.get_json(silent=True) or {}
        session_key = str(data.get("session_key") or "")
        session_id = session.get("bt_session_id")

        # 同一会话重复提交（网络重试/多次触发）直接返回已保存记录
        saved_id = db.backtest_record_id_by_session(session_key)
        if saved_id is not None:
            return {"ok": True, "record_id": saved_id, "duplicate": True}

        if not session_key or session_id != session_key:
            return {"ok": False, "msg": "回测会话已失效，无法保存记录"}
        rs = _replay_sessions.get(session_key)
        if rs is None:
            return {"ok": False, "msg": "回测会话已结束，无法保存记录"}

        # 交易记录校验：统计值全部由服务端重新计算，不信任前端传值
        raw_records = data.get("trade_records")
        if not isinstance(raw_records, list):
            raw_records = []
        raw_records = raw_records[-BACKTEST_MAX_TRADE_RECORDS:]

        trade_records = []
        for item in raw_records:
            if not isinstance(item, dict):
                continue
            direction = str(item.get("direction") or "")
            if direction not in BACKTEST_DIRECTIONS:
                continue
            trade_records.append(
                {
                    "time": str(item.get("time") or ""),
                    "direction": direction,
                    "price": round(_bt_float(item.get("price")), 3),
                    "qty": max(0, _bt_int(item.get("qty"))),
                    "pnl": round(_bt_float(item.get("pnl")), 2),
                }
            )

        initial_capital = _bt_float(
            data.get("initial_capital"), BACKTEST_INITIAL_CAPITAL
        )
        if initial_capital <= 0:
            initial_capital = BACKTEST_INITIAL_CAPITAL
        final_capital = _bt_float(data.get("capital"), initial_capital)
        current_price = _bt_float(data.get("current_price"))

        position = data.get("position")
        if not isinstance(position, dict):
            position = {}
        position_type = str(position.get("type") or "")
        position_qty = max(0, _bt_int(position.get("qty")))
        position_price = _bt_float(position.get("price"))

        close_trades = [t for t in trade_records if t["direction"] == "平仓"]
        realized_pnl = round(sum(t["pnl"] for t in close_trades), 2)
        close_count = len(close_trades)
        win_count = len([t for t in close_trades if t["pnl"] > 0])
        lose_count = close_count - win_count
        win_rate = round(win_count / close_count * 100, 2) if close_count else 0.0

        # 未平仓头寸按当前价计算持仓市值与浮动盈亏
        if position_type in ("long", "short") and position_qty > 0:
            position_value = round(position_qty * current_price, 2)
            if position_type == "long":
                unrealized_pnl = round(
                    (current_price - position_price) * position_qty, 2
                )
            else:
                unrealized_pnl = round(
                    (position_price - current_price) * position_qty, 2
                )
        else:
            position_value = 0.0
            unrealized_pnl = 0.0

        # 总资产 = 初始资金 + 已实现盈亏 + 浮动盈亏（空仓时即剩余资金）
        total_value = round(initial_capital + realized_pnl + unrealized_pnl, 2)
        profit = round(total_value - initial_capital, 2)

        klines = rs["all_klines"]
        start_pos = int(rs["start_pos"])
        end_pos = min(int(rs["current_pos"]), len(klines) - 1)
        summary = {
            "session_key": session_key,
            "market": "a",
            "stock_code": rs["code"],
            "stock_name": rs["name"],
            "frequency": f"{rs['small_freq']}/{rs['high_freq']}",
            "initial_capital": round(initial_capital, 2),
            "final_capital": round(final_capital, 2),
            "position_value": position_value,
            "unrealized_pnl": unrealized_pnl,
            "realized_pnl": realized_pnl,
            "total_value": total_value,
            "profit": profit,
            "profit_rate": round(profit / initial_capital * 100, 2),
            "total_trades": len(trade_records),
            "close_count": close_count,
            "win_count": win_count,
            "lose_count": lose_count,
            "win_rate": win_rate,
            "start_time": fun.datetime_to_str(klines.iloc[start_pos]["date"]),
            "end_time": fun.datetime_to_str(klines.iloc[end_pos]["date"]),
            "start_pos": start_pos,
            "end_pos": end_pos,
            "total_bars": len(klines),
        }

        try:
            record_id = db.backtest_record_save(summary, trade_records)
        except Exception as e:
            app.logger.error(f"保存回测记录失败: {e}")
            return {"ok": False, "msg": "回测记录保存失败"}

        # 记录已保存，清理回放 session
        _replay_sessions.pop(session_key, None)
        session.pop("bt_session_id", None)
        return {"ok": True, "record_id": record_id}

    @app.route("/backtest/records/page")
    @login_required
    def backtest_records_page():
        """回测学习记录弹框页面"""
        return render_template("backtest_records.html")

    @app.route("/backtest/records")
    @login_required
    def backtest_records():
        """回测学习记录列表与统计"""
        records = db.backtest_record_list(limit=BACKTEST_RECORD_LIST_LIMIT)
        return {
            "ok": True,
            "stats": db.backtest_record_stats(),
            "data": [_bt_record_to_dict(record) for record in records],
        }

    @app.route("/backtest/records/<int:record_id>")
    @login_required
    def backtest_record_detail(record_id: int):
        """单条回测记录与交易明细"""
        record, trades = db.backtest_record_get(record_id)
        if record is None:
            return {"ok": False, "msg": "回测记录不存在"}
        return {
            "ok": True,
            "record": _bt_record_to_dict(record),
            "trades": [_bt_trade_to_dict(trade) for trade in trades],
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
