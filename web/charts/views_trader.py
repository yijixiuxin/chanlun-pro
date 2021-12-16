import pickle
from django.http import HttpResponse
from django.shortcuts import render

from cl_v2 import rd
from cl_v2 import trader
from cl_v2.my import trader_currency
from cl_v2.my import trader_hk_stock


def trader_view(request):
    """
    交易对象页面展示
    :param request:
    :return:
    """
    # 数字货币交易对象
    currency_trader = pickle.loads(rd.get_byte('trader_currency'))
    currency_result = trader.traders_result([currency_trader], lambda a: a).get_html_string(
        attributes={'class': 'table'})

    # 港股交易对象
    hk_stock_trader = pickle.loads(rd.get_byte('trader_hk_stock'))

    hk_stock_result = trader.traders_result([hk_stock_trader], lambda a: a).get_html_string(
        attributes={'class': 'table'})

    return render(request, 'trader_view.html', {
        'currency_trader': currency_trader,
        'currency_result': currency_result,
        'hk_stock_trader': hk_stock_trader,
        'hk_stock_result': hk_stock_result
    })
