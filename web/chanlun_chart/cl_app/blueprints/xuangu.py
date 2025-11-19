"""
选股任务相关接口蓝图。

  - `/xuangu/task_list/<market>`
  - `/xuangu/task_add`
"""

from flask import Blueprint, render_template, request, current_app
from flask_login import login_required

from chanlun.base import Market
from chanlun.exchange import get_exchange
from chanlun.zixuan import ZiXuan


xuangu_bp = Blueprint("xuangu", __name__)


@xuangu_bp.route("/xuangu/task_list/<market>")
@login_required
def xuangu_task_list(market):
    # 获取自选组
    zx = ZiXuan(market)
    zixuan_groups = zx.zixuan_list

    # 交易所支持周期
    frequencys = get_exchange(Market(market)).support_frequencys()

    _xuangu_tasks = current_app.extensions.get("xuangu_tasks")
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


@xuangu_bp.route("/xuangu/task_add", methods=["POST"])
@login_required
def xuangu_task_add():
    _xuangu_tasks = current_app.extensions.get("xuangu_tasks")
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