from .apps import login_required
from django.http import HttpResponse
from django.shortcuts import render

from chanlun import fun, kcharts, zixuan
from chanlun.db import db
from chanlun.cl_utils import web_batch_get_cl_datas, query_cl_chart_config
from chanlun.exchange import get_exchange, Market
from . import utils
from .apps import login_required

"""
港股行情
"""


@login_required
def index_show(request):
    """
    港股行情首页显示
    :param request:
    :return:
    """
    zx = zixuan.ZiXuan(market_type="hk")
    ex = get_exchange(Market.HK)
    default_code = ex.default_code()
    support_frequencys = ex.support_frequencys()

    return render(
        request,
        "charts/hk/index.html",
        {
            "nav": "hk",
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
    alert_records = db.alert_record_query("hk")
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
    ex = get_exchange(Market.HK)
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
    ex = get_exchange(Market.HK)
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
    kline_dt = request.POST.get("kline_dt")

    cl_chart_config = query_cl_chart_config("futures", code)

    ex = get_exchange(Market.HK)
    klines = ex.klines(
        code, frequency=frequency, end_date=None if kline_dt == "" else kline_dt
    )
    cd = web_batch_get_cl_datas(
        "hk",
        code,
        {frequency: klines},
        cl_chart_config,
    )[0]
    stock_info = ex.stock_info(code)
    orders = db.order_query_by_code("hk", code)
    chart = kcharts.render_charts(
        stock_info["code"] + ":" + stock_info["name"] + ":" + cd.get_frequency(),
        cd,
        orders=orders,
        config=cl_chart_config,
    )
    return HttpResponse(chart)
