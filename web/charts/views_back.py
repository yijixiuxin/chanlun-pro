import datetime

from django.http import HttpResponse
from django.shortcuts import render

from cl_v2 import back_klines
from cl_v2 import kcharts

bk_hq: back_klines.BackKlines = None


def ReRecordKlineIndexView(request):
    """
    回放行情
    :param request:
    :return:
    """
    global bk_hq
    bk_hq = None

    return render(request, 'rerecord_index.html')


def ReRecordKlines(request):
    """
    回放 K线
    :param request:
    :return:
    """
    global bk_hq
    market = request.GET.get('market')
    code = request.GET.get('code')
    start = request.GET.get('start')
    end = request.GET.get('end')
    frequencys: str = request.GET.get('frequencys')
    frequency: str = request.GET.get('frequency')

    start = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end = datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    if bk_hq is None or bk_hq.code != code or bk_hq.start_date != start or bk_hq.end_date != end:
        bk_hq = back_klines.BackKlines(market, code, start, end, frequencys.split(','))
        bk_hq.start()

    is_next = bk_hq.next(frequency)
    if not is_next:
        return HttpResponse('')

    cl_datas = bk_hq.cl_datas

    charts = '{'
    for cd in cl_datas.values():
        c = kcharts.render_charts(code + ':' + cd.frequency, cd)
        charts += '"' + cd.frequency + '" : ' + c + ','
    charts += '}'

    return HttpResponse(charts)

