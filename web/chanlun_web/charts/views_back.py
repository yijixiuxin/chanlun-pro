from django.http import HttpResponse
from django.shortcuts import render

from chanlun.backtesting.backtest_klines import BackTestKlines
from chanlun import kcharts
from .apps import login_required
from chanlun.cl_utils import query_cl_chart_config
import time

bk_hq: BackTestKlines


@login_required
def index_show(request):
    """
    回放行情
    :param request:
    :return:
    """
    global bk_hq
    bk_hq = None

    return render(request, 'charts/back/index.html', {'nav': 'back'})


@login_required
def kline_show(request):
    """
    回放 K线
    :param request:
    :return:
    """
    global bk_hq
    market = request.POST.get('market')
    code = request.POST.get('code')
    start = request.POST.get('start')
    end = request.POST.get('end')
    frequencys: str = request.POST.get('frequencys')
    frequencys: list = frequencys.split(',')
    f = request.POST.get('frequency')

    # 缠论配置设置
    cl_config = query_cl_chart_config(market, code)
    if bk_hq is None:
        bk_hq = BackTestKlines(market, start, end, frequencys, cl_config=cl_config)
        bk_hq.init(code, frequencys)

    is_next = bk_hq.next(f)
    if not is_next:
        return HttpResponse('')

    _s_time = time.time()
    cd = bk_hq.get_cl_data(code, f)
    print('cd use time: ', time.time() - _s_time)
    _s_time = time.time()
    chart = kcharts.render_charts(f'{code}:{f}', cd)
    print('chart use time: ', time.time() - _s_time)

    return HttpResponse(chart)
