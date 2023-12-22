from django.http import HttpResponse
from django.shortcuts import render

from chanlun import fun, kcharts, zixuan
from chanlun.db import db
from chanlun.cl_utils import web_batch_get_cl_datas, query_cl_chart_config
from chanlun.exchange import get_exchange, Market
from . import utils
from .apps import login_required

"""
美股行情
"""


@login_required
def index_show(request):
    """
    美股行情首页显示
    :param request:
    :return:
    """
    zx = zixuan.ZiXuan(market_type="us")
    ex = get_exchange(Market.US)
    default_code = ex.default_code()
    support_frequencys = ex.support_frequencys()
    return render(
        request,
        "charts/us/index.html",
        {
            "nav": "us",
            "show_level": ["high", "low"],
            "zx_list": zx.zixuan_list,
            "default_code": default_code,
            "support_frequencys": support_frequencys,
        },
    )


@login_required
def jhs_json(request):
    """
    股票机会列表
    :param request:
    :return:
    """
    alert_records = db.alert_record_query("us")
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
def kline_chart(request):
    """
    股票 Kline 线获取
    :param request:
    :return:
    """
    code = request.POST.get("code")
    frequency = request.POST.get("frequency")

    ex = get_exchange(Market.US)
    stock_info = ex.stock_info(code)
    orders = db.order_query_by_code("us", code)

    cl_chart_config = query_cl_chart_config("us", code)
    klines = ex.klines(code, frequency=frequency)
    cd = web_batch_get_cl_datas(
        "us",
        code,
        {frequency: klines},
        cl_chart_config,
    )[0]

    chart = kcharts.render_charts(
        stock_info["code"] + ":" + stock_info["name"] + ":" + cd.get_frequency(),
        cd,
        orders=orders,
        config=cl_chart_config,
    )
    return HttpResponse(chart)
