from django.shortcuts import render
from django.http import HttpResponse
# Create your views here.

import json

from cl_v2 import exchange
from cl_v2 import exchange_binance
from cl_v2 import cl
from cl_v2 import kcharts

stock_ex = exchange.Exchange()
currency_ex = exchange_binance.ExchangeBinance()


def stock_index_show(request):
    """
    股票行情展示页面
    :param request:
    :return:
    """
    return render(request, 'demo_stock_index.html')


def currency_index_show(request):
    """
    数字货币行情展示页面
    :param request:
    :return:
    """
    stocks = currency_ex.all_stocks()
    return render(request, 'demo_currency_index.html', {'stocks': stocks})


def query_cl_klines(request):
    """
    缠论数据图表请求
    :param request:
    :return:
    """
    market = request.GET.get('market')
    code = request.GET.get('code')
    frequency = request.GET.get('frequency')

    if market == 'currency':
        klines = currency_ex.klines(code, frequency)
    else:
        klines = stock_ex.klines(code, frequency)

    cd = cl.CL(code, klines, frequency)
    chart = kcharts.render_charts(code + ':' + frequency, cd)
    return HttpResponse(chart)


# 搜索股票代码
def search_stock_code(request):
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


# Create your views here.
def response_as_json(data):
    json_str = json.dumps(data)
    response = HttpResponse(
        json_str,
        content_type="application/json",
    )
    response["Access-Control-Allow-Origin"] = "*"
    return response
