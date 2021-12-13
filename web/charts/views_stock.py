import json
import datetime
import numpy as np

from django.http import HttpResponse
from django.shortcuts import render

from cl_v2 import cl
from cl_v2 import exchange_futu
from cl_v2 import kcharts
from cl_v2 import stock_dl_rank
from cl_v2 import fun
from cl_v2 import rd
from cl_v2 import config
from cl_v2 import exchange


# Create your views here.
def response_as_json(data):
    json_str = json.dumps(data)
    response = HttpResponse(
        json_str,
        content_type="application/json",
    )
    response["Access-Control-Allow-Origin"] = "*"
    return response


def json_response(data, code=200):
    data = {
        "code": code,
        "msg": "success",
        "data": data,
    }
    return response_as_json(data)


def json_error(error_string="error", code=500, **kwargs):
    data = {
        "code": code,
        "msg": error_string,
        "data": {}
    }
    data.update(kwargs)
    return response_as_json(data)


JsonResponse = json_response
JsonError = json_error

'''
股票行情
'''
if config.USE_FUTU:
    stock_ex = exchange_futu.ExchangeFutu()
else:
    stock_ex = exchange.Exchange()


def stock_index_show(request):
    """
    股票行情首页显示
    :param request:
    :return:
    """
    return render(request, 'stock_index.html')


def stock_my_zixuan(request):
    """
    查询我的自选列表
    :param request:
    :return:
    """
    my = request.GET.get('my')
    if my is None:
        my = '我的持仓'
    stocks = stock_ex.zixuan_stocks(my)
    return JsonResponse(stocks)


def stock_jhs(request):
    """
    股票机会列表
    :param request:
    :return:
    """
    jhs = rd.stock_jh_query()
    return JsonResponse(jhs)


def stock_plate(request):
    """
    查询股票的板块信息
    :param request:
    :return:
    """
    code = request.GET.get('code')
    plates = stock_ex.stock_owner_plate(code)
    return JsonResponse(plates)


def plate_stocks(request):
    """
    查询板块中的股票信息
    :param request:
    :return:
    """
    code = request.GET.get('code')
    stocks = stock_ex.plate_stocks(code)
    return JsonResponse(stocks)


def stock_kline_show(request):
    """
    股票 Kline 线获取
    :param request:
    :return:
    """
    code = request.GET.get('code')
    frequency = request.GET.get('frequency')
    return HttpResponse(stocks_kline_base(code, frequency))


def stocks_kline_base(code, frequency):
    """
    股票 Kline 线数据获取
    :param code:
    :param frequency:
    :return:
    """
    klines = stock_ex.klines(code, frequency=frequency)
    cd = cl.CL(code, klines, frequency)
    stock_info = stock_ex.stock_info(code)
    orders = rd.stock_order_query(code)
    orders = fun.convert_stock_order_by_frequency(orders, frequency)
    chart = kcharts.render_charts(
        stock_info['code'] + ':' + stock_info['name'] + ':' + cd.frequency,
        cd, orders=orders)
    return chart


def dl_ranks_show(request):
    """
    动量排行数据
    :param request:
    :return:
    """
    DRHY = stock_dl_rank.StockDLHYRank()
    hy_ranks = DRHY.query(length=3)
    DRGN = stock_dl_rank.StockDLGNRank()
    gn_ranks = DRGN.query(length=3)

    return render(request, 'stock_dl_ranks.html', {'hy_ranks': hy_ranks, 'gn_ranks': gn_ranks})


def dl_hy_ranks_save(request):
    """
    行业动量排名增加
    :param request:
    :return:
    """
    ranks = request.body
    ranks = ranks.decode('utf-8')

    DR = stock_dl_rank.StockDLHYRank()
    DR.add_dl_rank(ranks)
    return JsonResponse(True)


def dl_gn_ranks_save(request):
    """
    概念动量排名增加
    :param request:
    :return:
    """
    ranks = request.body
    ranks = ranks.decode('utf-8')

    DR = stock_dl_rank.StockDLGNRank()
    DR.add_dl_rank(ranks)
    return JsonResponse(True)


# 搜索股票代码
def SearchStocks(request):
    tp = request.GET.get('tp')
    search = request.GET.get('query')
    all_stocks = stock_ex.all_stocks()
    res = []
    for stock in all_stocks:
        if search in stock['code'] or search in stock['name']:
            res.append(stock)
    js = []
    for r in res:
        if tp == 'more':
            js.append({'value': '%s - %s' % (r['code'], r['name']), 'data': r['code'] + ':30m:kline'})
        else:
            js.append({'value': '%s - %s' % (r['code'], r['name']), 'data': r['code']})

    return response_as_json({'query': search, 'suggestions': js})
