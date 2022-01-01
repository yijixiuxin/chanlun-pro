import json
import datetime
import numpy as np
from django.http import HttpResponse
from django.shortcuts import render

from cl_v2 import exchange_binance
from cl_v2 import cl
from cl_v2 import fun
from cl_v2 import kcharts
from cl_v2 import rd


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
数字货币行情
'''

def currency_index_view(request):
    """
    数字货币行情首页
    :param request:
    :return:
    """
    exchange = exchange_binance.ExchangeBinance()
    stocks = exchange.all_stocks()
    return render(request, 'currency_index.html', {'stocks': stocks})


def currency_kline_show(request):
    """
    数字货币 Kline 获取
    :param request:
    :return:
    """
    code = request.GET.get('code')
    frequency = request.GET.get('frequency')
    return HttpResponse(currency_kline_base(code, frequency))


def currency_trade_open(request):
    """
    数字货币开仓交易
    :param request:
    :return:
    """
    symbol = request.GET.get('symbol')
    leverage = request.GET.get('leverage')
    open_usdt = request.GET.get('open_usdt')
    trade_type = request.GET.get('trade_type')

    exchange = exchange_binance.ExchangeBinance()
    ticks = exchange.ticks([symbol])
    amount = (float(open_usdt) / ticks[symbol].last) * int(leverage)
    res = exchange.order(symbol, trade_type, amount, {'leverage': leverage})

    # 记录操作记录
    rd.currency_opt_record_save(symbol, '手动开仓交易：交易类型 %s 开仓价格 %s 数量 %s 占用保证金 %s' % (
    trade_type, res['price'], res['amount'], open_usdt))
    return JsonResponse(True)


def currency_trade_close(request):
    """
    数字货币平仓交易
    :param request:
    :return:
    """
    exchange = exchange_binance.ExchangeBinance()

    symbol = request.GET.get('symbol')
    amount = request.GET.get('amount')
    trade_type = request.GET.get('trade_type')
    res = exchange.order(symbol, trade_type, float(amount))

    # 清空配置
    rd.currency_pos_loss_price_save(symbol, 0)
    rd.currency_pos_profit_rate_save(symbol, 0)
    rd.currency_position_check_setting_clear(symbol)
    rd.currency_open_setting_clear(symbol)
    # 记录操作记录
    rd.currency_opt_record_save(symbol, '手动平仓交易：交易类型 %s 平仓价格 %s 数量 %s' % (trade_type, res['price'], res['amount']))

    return JsonResponse(True)


def currency_open_buysell(request):
    """
    数字货币买卖设置
    :param request:
    :return:
    """
    exchange = exchange_binance.ExchangeBinance()
    # 查询账户信息
    balance = exchange.balance()
    # 查询买卖配置设置
    open_settings = rd.currency_open_setting_query()
    return render(request, 'currency_open_buysell.html', {'balance': balance, 'open_settings': open_settings})


def currency_open_buysell_save(request):
    """
    数字货币开仓买卖设置
    :param request:
    :return:
    """
    # symbol=BTC%2FUSDT&trade_type=open_long&leverage=5&open_usdt=100&frequency=30m&fangxiang=up&beichi=none&bi_done=true&td=none&mmds=
    symbol = request.GET.get('symbol')
    trade_type = request.GET.get('trade_type')
    leverage = request.GET.get('leverage')
    open_usdt = request.GET.get('open_usdt')
    frequency = request.GET.get('frequency')
    fangxiang = request.GET.get('fangxiang')
    beichi = request.GET.get('beichi')
    bi_done = request.GET.get('bi_done')
    td = request.GET.get('td')
    mmds = request.GET.get('mmds')
    set = {
        'symbol': symbol,
        'trade_type': trade_type,
        'leverage': leverage,
        'open_usdt': open_usdt,
        'frequency': frequency,
        'fangxiang': fangxiang,
        'beichi': beichi,
        'bi_done': bi_done,
        'td': td,
        'mmds': mmds
    }
    rd.currency_open_setting_save(symbol, set)
    return JsonResponse(True)


def currency_open_buysell_del(request):
    """
    数字货币开仓买卖设置删除
    :param request:
    :return:
    """
    symbol = request.GET.get('symbol')
    num = request.GET.get('num')
    rd.currency_open_setting_del(symbol, num)
    return JsonResponse(True)


def currency_positions(request):
    """
    查询数字货币持仓
    :param request:
    :return:
    """
    exchange = exchange_binance.ExchangeBinance()
    positions = exchange.positions()
    for p in positions:
        # 止损价
        loss_price = rd.currency_pos_loss_price_query(p['symbol'])
        profit_rate = rd.currency_pos_profit_rate_query(p['symbol'])
        p['profit_rate'] = profit_rate
        p['loss_price'] = loss_price
        p['loss_rate'] = 0
        if p['loss_price'] > 0:
            p['loss_rate'] = round((p['entryPrice'] - p['loss_price']) / p['entryPrice'] * 100 * p['leverage'], 2)
        # 盯盘信息
        settings = rd.currency_position_check_setting_query(p['symbol'])
        p['settings'] = settings

    return render(request, 'currency_positions.html', {'positions': positions})


def currency_pos_loss_price_save(request):
    """
    数字货币持仓止损设置
    :param request:
    :return:
    """
    symbol = request.GET.get('symbol')
    loss_price = request.GET.get('loss_price')
    rd.currency_pos_loss_price_save(symbol, loss_price)
    return JsonResponse(True)


def currency_pos_profit_rate_save(request):
    """
    数字货币持仓止盈设置
    :param request:
    :return:
    """
    symbol = request.GET.get('symbol')
    rate = request.GET.get('profit_rate')
    rd.currency_pos_profit_rate_save(symbol, rate)
    return JsonResponse(True)


def currency_pos_check_set_save(request):
    """
    数字货币持仓监控设置
    :param request:
    :return:
    """
    symbol = request.GET.get('symbol')
    # frequency=30m&fangxiang=down&beichi=none&bi_done=true&td=none&mmds=1buy%2F2buy
    frequency = request.GET.get('frequency')
    fangxiang = request.GET.get('fangxiang')
    beichi = request.GET.get('beichi')
    bi_done = request.GET.get('bi_done')
    td = request.GET.get('td')
    mmds = request.GET.get('mmds')
    set = {
        'frequency': frequency,
        'fangxiang': fangxiang,
        'beichi': beichi,
        'bi_done': bi_done,
        'td': td,
        'mmds': mmds
    }
    rd.currency_position_check_setting_add(symbol, set)
    return JsonResponse(True)


def currency_pos_check_set_del(request):
    """
    数字货币持仓监控删除
    :param request:
    :return:
    """
    symbol = request.GET.get('symbol')
    num = request.GET.get('num')
    rd.currency_position_check_setting_del(symbol, num)
    return JsonResponse(True)


def currency_jhs(request):
    """
    数字货币机会列表
    :param request:
    :return:
    """
    jhs = rd.currency_jh_query()
    return JsonResponse(jhs)


def currency_opt_records(request):
    """
    数字货币操盘列表
    :param request:
    :return:
    """
    records = rd.currency_opt_record_query(100)
    return JsonResponse(records)


def currency_kline_base(code, frequency):
    """
    数字货币行情获取
    :param code:
    :param frequency:
    :return:
    """
    exchange = exchange_binance.ExchangeBinance()
    klines = exchange.klines(code, frequency=frequency)
    cd = cl.CL(code, klines, frequency)

    orders = rd.currency_order_query(code)
    orders = fun.convert_currency_order_by_frequency(orders, frequency)

    chart = kcharts.render_charts(code + ':' + cd.frequency, cd, orders=orders)
    return chart
