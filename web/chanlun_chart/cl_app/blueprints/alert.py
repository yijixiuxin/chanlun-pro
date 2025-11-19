"""
提醒（警报）相关接口蓝图。
  - `/alert_list/<market>`
  - `/alert_edit/<market>/<id>`
  - `/alert_save`
  - `/alert_del/<id>`
  - `/alert_records/<market>`
  - `/jobs` （任务列表视图）
"""

import json

from flask import Blueprint, render_template, request, current_app
from flask_login import login_required

from chanlun.base import Market
from chanlun import fun
from chanlun.db import db
from chanlun.exchange import get_exchange
from chanlun.zixuan import ZiXuan


alert_bp = Blueprint("alert", __name__)


@alert_bp.route("/alert_list/<market>")
@login_required
def alert_list(market):
    _alert_tasks = current_app.extensions.get("alert_tasks")
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
            "check_idx_ma_info": _l.check_idx_ma_info,
            "check_idx_macd_info": _l.check_idx_macd_info,
            "is_send_msg": _l.is_send_msg,
            "is_run": _l.is_run,
        }
        for _l in al
    ]
    return {"code": 0, "msg": "", "count": len(al), "data": al}


@alert_bp.route("/alert_edit/<market>/<id>")
@login_required
def alert_edit(market, id):
    _alert_tasks = current_app.extensions.get("alert_tasks")
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
        "check_idx_ma_info_enable": 0,
        "check_idx_ma_info_slow": 10,
        "check_idx_ma_info_fast": 5,
        "check_idx_ma_info_cross_up": 0,
        "check_idx_ma_info_cross_down": 0,
        "check_idx_macd_info_enable": 0,
        "check_idx_macd_info_cross_up": 0,
        "check_idx_macd_info_cross_down": 0,
        "is_send_msg": 1,
        "is_run": 1,
    }
    if id != "0":
        _alert_config = _alert_tasks.alert_get(id)
        if _alert_config is not None:
            check_idx_ma_info = (
                json.loads(_alert_config.check_idx_ma_info)
                if _alert_config.check_idx_ma_info
                else {
                    "enable": 0,
                    "slow": 10,
                    "fast": 5,
                    "cross_up": 0,
                    "cross_down": 0,
                }
            )
            check_idx_macd_info = (
                json.loads(_alert_config.check_idx_macd_info)
                if _alert_config.check_idx_macd_info
                else {
                    "enable": 0,
                    "cross_up": 0,
                    "cross_down": 0,
                }
            )
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
                "check_idx_ma_info_enable": check_idx_ma_info["enable"],
                "check_idx_ma_info_slow": check_idx_ma_info["slow"],
                "check_idx_ma_info_fast": check_idx_ma_info["fast"],
                "check_idx_ma_info_cross_up": check_idx_ma_info["cross_up"],
                "check_idx_ma_info_cross_down": check_idx_ma_info["cross_down"],
                "check_idx_macd_info_enable": check_idx_macd_info["enable"],
                "check_idx_macd_info_cross_up": check_idx_macd_info["cross_up"],
                "check_idx_macd_info_cross_down": check_idx_macd_info["cross_down"],
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


@alert_bp.route("/alert_save", methods=["POST"])
@login_required
def alert_save():
    _alert_tasks = current_app.extensions.get("alert_tasks")
    check_idx_ma_infos = json.dumps(
        {
            "enable": (
                int(request.form["check_idx_ma_info_enable"])
                if request.form["check_idx_ma_info_enable"]
                else 0
            ),
            "slow": (
                int(request.form["check_idx_ma_info_slow"])
                if request.form["check_idx_ma_info_slow"]
                else 0
            ),
            "fast": (
                int(request.form["check_idx_ma_info_fast"])
                if request.form["check_idx_ma_info_fast"]
                else 0
            ),
            "cross_up": (
                int(request.form["check_idx_ma_info_cross_up"])
                if request.form["check_idx_ma_info_cross_up"]
                else 0
            ),
            "cross_down": (
                int(request.form["check_idx_ma_info_cross_down"])
                if request.form["check_idx_ma_info_cross_down"]
                else 0
            ),
        }
    )
    check_idx_macd_infos = json.dumps(
        {
            "enable": (
                int(request.form["check_idx_macd_info_enable"])
                if request.form["check_idx_macd_info_enable"]
                else 0
            ),
            "cross_up": (
                int(request.form["check_idx_macd_info_cross_up"])
                if request.form["check_idx_macd_info_cross_up"]
                else 0
            ),
            "cross_down": (
                int(request.form["check_idx_macd_info_cross_down"])
                if request.form["check_idx_macd_info_cross_down"]
                else 0
            ),
        }
    )
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
        "check_idx_ma_info": check_idx_ma_infos,
        "check_idx_macd_info": check_idx_macd_infos,
        "is_send_msg": int(request.form["is_send_msg"]),
        "is_run": int(request.form["is_run"]),
    }
    _alert_tasks.alert_save(alert_config)
    return {"ok": True}


@alert_bp.route("/alert_del/<id>")
@login_required
def alert_del(id):
    _alert_tasks = current_app.extensions.get("alert_tasks")
    res = _alert_tasks.alert_del(id)
    return {"ok": res}


@alert_bp.route("/alert_records/<market>")
@login_required
def alert_records(market):
    task_name = request.args.get("task_name")
    records = db.alert_record_query(market, task_name)
    rls = [
        {
            "code": _r.stock_code,
            "name": _r.stock_name,
            "frequency": _r.frequency,
            "line_type": _r.line_type,
            "msg": _r.alert_msg,
            "is_done": _r.bi_is_done,
            "is_td": _r.bi_is_td,
            "task_name": _r.task_name,
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


@alert_bp.route("/jobs")
@login_required
def jobs():
    scheduler = current_app.extensions.get("scheduler")
    return render_template("jobs.html", jobs=list(scheduler.my_task_list.values()))
