from django.http import HttpResponse
from django.shortcuts import render

from chanlun import kcharts, zixuan, fun
from chanlun.db import db
from chanlun.cl_utils import web_batch_get_cl_datas, kcharts_frequency_h_l_map
from chanlun.cl_utils import query_cl_chart_config
from chanlun.exchange import get_exchange, Market
from . import utils
from .apps import login_required

"""
期货行情
"""


@login_required
def index_show(request):
    """
    期货行情首页显示
    :param request:
    :return:
    """
    ex = get_exchange(Market.FUTURES)
    zx = zixuan.ZiXuan(market_type="futures")
    default_code = ex.default_code()
    support_frequencys = ex.support_frequencys()
    return render(
        request,
        "charts/futures/index.html",
        {
            "default_code": default_code,
            "support_frequencys": support_frequencys,
            "nav": "futures",
            "show_level": ["high", "low"],
            "zx_list": zx.zixuan_list,
        },
    )


@login_required
def kline_show(request):
    """
    期货 Kline 线获取
    :param request:
    :return:
    """
    code = request.POST.get("code")
    frequency = request.POST.get("frequency")

    ex = get_exchange(Market.FUTURES)
    orders = db.order_query_by_code("futures", code)

    cl_chart_config = query_cl_chart_config("futures", code)
    frequency_low, kchart_to_frequency = kcharts_frequency_h_l_map("futures", frequency)
    if (
        cl_chart_config["enable_kchart_low_to_high"] == "1"
        and kchart_to_frequency is not None
    ):
        # 如果开启并设置的该级别的低级别数据，获取低级别数据，并在转换成高级图表展示
        klines = ex.klines(code, frequency=frequency_low)
        cd = web_batch_get_cl_datas(
            "futures",
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
            "futures",
            code,
            {frequency: klines},
            cl_chart_config,
        )[0]
        title = f"{code}:{frequency}"
        chart = kcharts.render_charts(title, cd, orders=orders, config=cl_chart_config)
    return HttpResponse(chart)


@login_required
def jhs_json(request):
    """
    期货机会列表
    :param request:
    :return:
    """
    alert_records = db.alert_record_query("futures")
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
