from django.http import HttpResponse
from django.shortcuts import render

from chanlun import fun, kcharts, zixuan
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
股票行情
"""


@login_required
def index_show(request):
    """
    股票行情首页显示
    :param request:
    :return:
    """
    zx = zixuan.ZiXuan(market_type="a")
    ex = get_exchange(Market.A)
    default_code = ex.default_code()
    support_frequencys = ex.support_frequencys()
    return render(
        request,
        "charts/stock/index.html",
        {
            "nav": "stock",
            "default_code": default_code,
            "support_frequencys": support_frequencys,
            "show_level": ["high", "low"],
            "zx_list": zx.zixuan_list,
        },
    )


@login_required
def jhs_json(request):
    """
    股票机会列表
    :param request:
    :return:
    """
    alert_records = db.alert_record_query("a")
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


@login_required
def plate_json(request):
    """
    查询股票的板块信息
    :param request:
    :return:
    """
    code = request.GET.get("code")
    ex = get_exchange(Market.A)
    plates = ex.stock_owner_plate(code)
    return utils.JsonResponse(plates)


@login_required
def plate_stocks_json(request):
    """
    查询板块中的股票信息
    :param request:
    :return:
    """
    code = request.GET.get("code")
    ex = get_exchange(Market.A)
    stocks = ex.plate_stocks(code)
    return utils.JsonResponse(stocks)


@login_required
def kline_chart(request):
    """
    股票 Kline 线获取
    :param request:
    :return:
    """
    code = request.POST.get("code")
    frequency = request.POST.get("frequency")

    ex = get_exchange(Market.A)
    stock_info = ex.stock_info(code)
    orders = db.order_query_by_code("a", code)

    cl_chart_config = query_cl_chart_config("a", code)
    frequency_low, kchart_to_frequency = kcharts_frequency_h_l_map("a", frequency)
    if (
        cl_chart_config["enable_kchart_low_to_high"] == "1"
        and kchart_to_frequency is not None
    ):
        # 如果开启并设置的该级别的低级别数据，获取低级别数据，并在转换成高级图表展示
        klines = ex.klines(code, frequency=frequency_low, args={"pages": 12})
        cd = web_batch_get_cl_datas(
            "a",
            code,
            {frequency_low: klines},
            cl_chart_config,
        )[0]
        title = f'{code}-{stock_info["name"]}:{frequency_low}->{frequency}'
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
            "a",
            code,
            {frequency: klines},
            cl_chart_config,
        )[0]
        title = f'{code}-{stock_info["name"]}:{frequency}'
        chart = kcharts.render_charts(title, cd, orders=orders, config=cl_chart_config)
    return HttpResponse(chart)
