import json
import datetime
import numpy as np
from django.http import HttpResponse
from django.shortcuts import render

from cl_v2 import cl
from cl_v2 import kcharts
from cl_v2 import exchange_futu
from cl_v2 import exchange_binance
'''
自定义时间图表查看
'''


# 自定义 kline 时间的视图
def CustomKlineDateView(request):
    return render(request, 'custom_kline.html')


# 自定义时间获取kline数据
def CustomDateKline(request):
    market = request.GET.get('market')
    code = request.GET.get('code')
    frequency = request.GET.get('frequency')
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')
    if start_date == '':
        start_date = None
    if end_date == '':
        end_date = None

    if market == 'currency':
        exchange = exchange_binance.ExchangeBinance()
    else:
        exchange = exchange_futu.ExchangeFutu()

    klines = exchange.klines(code, frequency, start_date, end_date)
    cd = cl.CL(code, klines, frequency)
    c = kcharts.render_charts(code + ':' + frequency + ' Kline 图表', cd)
    return HttpResponse(c)


# 多图表查看
def MoreChartsView(request):
    return render(request, 'more_charts.html')


def ChartKline(request):
    code = request.GET.get('code')
    frequency = request.GET.get('frequency')
    if code[0:2] in ['SZ', 'SH', 'HK']:
        exchange = exchange_futu.ExchangeFutu()
        stock_info = exchange.stock_info(code)
        title = stock_info['code'] + ':' + stock_info['name'] + ' : ' + frequency
        klines = exchange.klines(code, frequency)
    else:
        exchange = exchange_binance.ExchangeBinance()
        title = code + ':' + frequency
        klines = exchange.klines(code, frequency)

    cd = cl.CL(code, klines, frequency)
    chart = kcharts.render_charts(title, cd)
    return HttpResponse(chart)
