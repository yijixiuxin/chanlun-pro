from .apps import login_required
from django.http import HttpResponse
from django.shortcuts import render

from chanlun import rd, kcharts, zixuan
from chanlun.cl_utils import web_batch_get_cl_datas, query_cl_chart_config
from chanlun.exchange import get_exchange, Market
from . import utils
from .apps import login_required

'''
港股行情
'''


@login_required
def index_show(request):
    """
    港股行情首页显示
    :param request:
    :return:
    """
    zx = zixuan.ZiXuan(market_type='hk')
    ex = get_exchange(Market.HK)
    default_code = ex.default_code()
    support_frequencys = ex.support_frequencys()

    return render(request, 'charts/hk/index.html', {
        'nav': 'hk',
        'default_code': default_code,
        'support_frequencys': support_frequencys,
        'show_level': ['high', 'low'],
        'zx_list': zx.zixuan_list
    })


@login_required
def jhs_json(request):
    """
    股票机会列表
    :param request:
    :return:
    """
    jhs = rd.jhs_query('hk')
    return utils.JsonResponse(jhs)


@login_required
def plate_json(request):
    """
    查询股票的板块信息
    :param request:
    :return:
    """
    code = request.GET.get('code')
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
    code = request.GET.get('code')
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
    code = request.POST.get('code')
    frequency = request.POST.get('frequency')
    kline_dt = request.POST.get('kline_dt')

    cl_chart_config = query_cl_chart_config('futures', code)

    ex = get_exchange(Market.HK)
    klines = ex.klines(code, frequency=frequency, end_date=None if kline_dt == '' else kline_dt)
    cd = web_batch_get_cl_datas('hk', code, {frequency: klines}, cl_chart_config, )[0]
    stock_info = ex.stock_info(code)
    orders = rd.order_query('hk', code)
    chart = kcharts.render_charts(
        stock_info['code'] + ':' + stock_info['name'] + ':' + cd.get_frequency(),
        cd, orders=orders, config=cl_chart_config)
    return HttpResponse(chart)
