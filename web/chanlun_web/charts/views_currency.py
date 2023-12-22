import datetime

from django.http import HttpResponse
from django.shortcuts import render

from chanlun import kcharts, fun
from chanlun import zixuan
from chanlun.db import db
from chanlun.cl_utils import (
    web_batch_get_cl_datas,
    query_cl_chart_config,
    kcharts_frequency_h_l_map,
)
from chanlun.exchange import get_exchange, Market
from . import utils
from .apps import login_required

"""
数字货币行情
"""


@login_required
def index_show(request):
    """
    数字货币行情首页
    :param request:
    :return:
    """
    ex = get_exchange(Market.CURRENCY)
    zx = zixuan.ZiXuan(market_type="currency")
    default_code = ex.default_code()
    support_frequencys = ex.support_frequencys()
    return render(
        request,
        "charts/currency/index.html",
        {
            "default_code": default_code,
            "support_frequencys": support_frequencys,
            "nav": "currency",
            "show_level": ["high", "low"],
            "zx_list": zx.zixuan_list,
        },
    )


@login_required
def kline_show(request):
    """
    数字货币 Kline 获取
    :param request:
    :return:
    """
    code = request.POST.get("code")
    frequency = request.POST.get("frequency")
    ex = get_exchange(Market.CURRENCY)
    orders = db.order_query_by_code("currency", code)

    cl_chart_config = query_cl_chart_config("currency", code)
    frequency_low, kchart_to_frequency = kcharts_frequency_h_l_map(
        "currency", frequency
    )
    if (
        cl_chart_config["enable_kchart_low_to_high"] == "1"
        and kchart_to_frequency is not None
    ):
        # 如果开启并设置的该级别的低级别数据，获取低级别数据，并在转换成高级图表展示
        klines = ex.klines(code, frequency=frequency_low)
        cd = web_batch_get_cl_datas(
            "currency",
            code,
            {frequency_low: klines},
            cl_chart_config,
        )[0]
        title = f"{code}:{frequency_low}->{frequency}"
        chart = kcharts.render_charts(
            title,
            cd,
            to_frequency=kchart_to_frequency,
            orders=orders,
            config=cl_chart_config,
        )
    else:
        klines = ex.klines(code, frequency=frequency)
        cd = web_batch_get_cl_datas(
            "currency",
            code,
            {frequency: klines},
            cl_chart_config,
        )[0]
        title = f"{code}:{frequency}"
        chart = kcharts.render_charts(title, cd, orders=orders, config=cl_chart_config)

    return HttpResponse(chart)


@login_required
def currency_balances(request):
    """
    查询数字货币资产
    """
    ex = get_exchange(Market.CURRENCY)
    balance = ex.balance()
    return utils.JsonResponse(balance)


@login_required
def currency_positions(request):
    """
    查询数字货币持仓
    :param request:
    :return:
    """
    ex = get_exchange(Market.CURRENCY)
    positions = ex.positions()
    return utils.JsonResponse(positions)


def pos_close(request):
    """
    数字货币平仓交易
    :param request:
    :return:
    """
    ex = get_exchange(Market.CURRENCY)
    code = request.GET.get("code")
    pos = ex.positions(code)
    if len(pos) == 0:
        return utils.json_error("当前没有持仓信息")

    pos = pos[0]
    if pos["side"] == "short":
        trade_type = "close_short"
    else:
        trade_type = "close_long"

    res = ex.order(code, trade_type, pos["contracts"])
    if res is False:
        return utils.json_error("手动平仓失败")

    # 记录操作记录
    db.order_save(
        "currency",
        code,
        code,
        trade_type,
        res["price"],
        res["amount"],
        "手动平仓",
        datetime.datetime.now(),
    )

    return utils.json_response(True)


@login_required
def opt_records(request):
    """
    数字货币操盘列表
    :param request:
    :return:
    """
    records = []
    return utils.JsonResponse(records)


@login_required
def jhs_json(request):
    """
    数字货币机会列表
    :param request:
    :return:
    """
    alert_records = db.alert_record_query("currency")
    jhs = [
        {
            "code": _a.stock_code,
            "name": _a.stock_name,
            "frequency": _a.frequency,
            "jh_type": _a.alert_msg,
            "is_done": _a.bi_is_done,
            "is_td": _a.bi_is_td,
            "datetime_str": fun.datetime_to_str(_a.alert_dt),
        }
        for _a in alert_records
    ]
    return utils.JsonResponse(jhs)
