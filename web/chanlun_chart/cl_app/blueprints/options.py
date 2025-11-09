"""
图表配置项相关接口蓝图。

  - `/get_cl_config/<market>/<code>`
  - `/set_cl_config`
"""

from flask import Blueprint, render_template, request
from flask_login import login_required

from chanlun.cl_utils import query_cl_chart_config, del_cl_chart_config, set_cl_chart_config


options_bp = Blueprint("options", __name__)


@options_bp.route("/get_cl_config/<market>/<code>")
@login_required
def get_cl_config(market, code: str):
    code = code.replace("__", "/")  # 数字货币特殊处理
    cl_config = query_cl_chart_config(market, code)
    cl_config["market"] = market
    cl_config["code"] = code
    return render_template("options.html", **cl_config)


@options_bp.route("/set_cl_config", methods=["POST"])
@login_required
def set_cl_config():
    market = request.form["market"]
    code = request.form["code"]
    is_del = request.form["is_del"]
    if is_del == "true":
        res = del_cl_chart_config(market, code)
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
        "zs_cd",
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

    res = set_cl_chart_config(market, code, cl_config)
    return {"ok": res}