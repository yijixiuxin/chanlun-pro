from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from django.http import HttpResponse
from django.shortcuts import render

# Create your views here.
from chanlun import kcharts
from chanlun.cl_utils import web_batch_get_cl_datas
from chanlun.exchange import exchange_tdx
from chanlun.exchange import exchange_tdx

g_code = 'SH.000001'
g_frequencys = ['y', 'm', 'w', 'd', '60m', '30m', '5m', '1m']
g_klines = {}


def task_save_klines():
    global g_code, g_frequencys, g_klines
    ex = exchange_tdx.ExchangeTDX()
    for _f in g_frequencys:
        klines = ex.klines(g_code, _f, args={'use_futu': False})
        if klines is not None:
            g_klines[_f] = klines
        print('Run get %s klines num %s' % (_f, len(klines)))
    return True


# 初始化先执行并保存行情一下
task_save_klines()

"""
异步执行后台定时任务
"""
scheduler = BackgroundScheduler()
scheduler.add_executor(ThreadPoolExecutor(5))

scheduler.add_job(task_save_klines, 'cron', hour='9-12,13-15', minute='*/5')
scheduler.start()


def index_show(request):
    """
    股票行情首页显示
    :param request:
    :return:
    """

    return render(request, 'charts/index.html', {'show_level': ['high'], })


def kline_chart(request):
    """
    股票 Kline 线获取
    :param request:
    :return:
    """
    global g_code, g_frequencys, g_klines
    frequency = request.POST.get('frequency')

    if frequency not in g_frequencys:
        frequency = 'd'

    # 缠论配置设置
    cl_config = {}
    cl_config_key = ['fx_qj', 'fx_bh', 'bi_type', 'bi_qj', 'bi_bzh', 'bi_fx_cgd', 'xd_bzh', 'xd_qj', 'zsd_bzh',
                     'zsd_qj', 'zs_bi_type',
                     'zs_xd_type', 'zs_qj', 'zs_wzgx', 'idx_macd_fast', 'idx_macd_slow', 'idx_macd_signal',
                     ]
    for _k in cl_config_key:
        cl_config[_k] = request.POST.get(_k)

    # 图表显示设置
    chart_bool_keys = ['show_bi_zs', 'show_xd_zs', 'show_bi_mmd', 'show_xd_mmd', 'show_bi_bc', 'show_xd_bc', 'show_ma',
                       'show_boll', 'idx_boll_period', 'idx_ma_period']
    chart_config = {_k: request.POST.get(_k, '1') for _k in chart_bool_keys}

    klines = g_klines[frequency]
    cd = web_batch_get_cl_datas('a', g_code, {frequency: klines}, cl_config, )[0]
    chart = kcharts.render_charts('上证指数:' + cd.get_frequency(), cd, config=chart_config)
    return HttpResponse(chart)
