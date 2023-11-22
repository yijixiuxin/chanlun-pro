from django.http import HttpResponse
from django.shortcuts import render

from chanlun import rd, stock_dl_rank, kcharts, zixuan
from chanlun.cl_utils import web_batch_get_cl_datas, query_cl_chart_config, kcharts_frequency_h_l_map
from chanlun.exchange import get_exchange, Market
from . import utils
from .apps import login_required

'''
股票行情
'''


@login_required
def index_show(request):
    """
    股票行情首页显示
    :param request:
    :return:
    """
    zx = zixuan.ZiXuan(market_type='a')
    ex = get_exchange(Market.A)
    default_code = ex.default_code()
    support_frequencys = ex.support_frequencys()
    return render(request, 'charts/stock/index.html', {
        'nav': 'stock',
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
    jhs = rd.jhs_query('a')
    return utils.JsonResponse(jhs)


@login_required
def plate_json(request):
    """
    查询股票的板块信息
    :param request:
    :return:
    """
    code = request.GET.get('code')
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
    code = request.GET.get('code')
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
    code = request.POST.get('code')
    frequency = request.POST.get('frequency')

    ex = get_exchange(Market.A)
    stock_info = ex.stock_info(code)
    orders = rd.order_query('a', code)

    cl_chart_config = query_cl_chart_config('a', code)
    frequency_low, kchart_to_frequency = kcharts_frequency_h_l_map('a', frequency)
    if cl_chart_config['enable_kchart_low_to_high'] == '1' and kchart_to_frequency is not None:
        # 如果开启并设置的该级别的低级别数据，获取低级别数据，并在转换成高级图表展示
        klines = ex.klines(code, frequency=frequency_low, args={'pages': 12})
        cd = web_batch_get_cl_datas('a', code, {frequency_low: klines}, cl_chart_config, )[0]
        title = f'{code}-{stock_info["name"]}:{frequency_low}->{frequency}'
        chart = kcharts.render_charts(
            title, cd, to_frequency=kchart_to_frequency, orders=orders, config=cl_chart_config
        )
    else:
        klines = ex.klines(code, frequency=frequency)
        cd = web_batch_get_cl_datas('a', code, {frequency: klines}, cl_chart_config, )[0]
        title = f'{code}-{stock_info["name"]}:{frequency}'
        chart = kcharts.render_charts(
            title, cd, orders=orders, config=cl_chart_config
        )
    return HttpResponse(chart)


@login_required
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

    return render(request, 'charts/stock/dl_ranks.html',
                  {'nav': 'dl_ranks', 'hy_ranks': hy_ranks, 'gn_ranks': gn_ranks})


@login_required
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
    return utils.JsonResponse(True)


@login_required
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
    return utils.JsonResponse(True)
