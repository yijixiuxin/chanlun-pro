from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from django.shortcuts import render, redirect

from chanlun import zixuan, config
from chanlun.cl_utils import *
from chanlun.exchange import get_exchange, Market
from . import tasks
from . import utils
from .apps import login_required

import pinyin

"""
异步执行后台定时任务
"""
scheduler = BackgroundScheduler()
scheduler.add_executor(ThreadPoolExecutor(10))

scheduler.add_job(tasks.task_a_1, 'cron', hour='9-12,13-15', minute='*', second='0')
scheduler.add_job(tasks.task_a_2, 'cron', hour='9-12,13-15', minute='*', second='0')
scheduler.add_job(tasks.task_hk, 'cron', hour='9-12,13-16', minute='*', second='0')
scheduler.add_job(tasks.task_us, 'cron', hour='00-04,21-23', minute='*', second='0')
scheduler.add_job(tasks.task_futures, 'cron', second='0')
scheduler.add_job(tasks.task_currency, 'cron', second='0')
# scheduler.add_job(task_gm.task_gm_sync, 'date', next_run_time=datetime.datetime.now())
scheduler.start()


# Create your views here.

def login_index(request):
    if config.LOGIN_PWD == '':
        request.session['user'] = 'cl'
        return redirect('/')

    password = request.POST.get('password')
    msg = ''
    if password is not None:
        if password == config.LOGIN_PWD:
            request.session['user'] = 'cl'
            return redirect('/')
        else:
            msg = '密码错误'
    return render(request, 'charts/login.html', {'msg': msg})


@login_required
def zixuan_groups_json(request):
    """
    查询市场下的自选分组
    """
    market_type = request.GET.get('market_type')

    zx = zixuan.ZiXuan(market_type)
    return utils.json_response(zx.zixuan_list)


@login_required
def zixuan_stocks_json(request):
    """
    查询自选分组下的代码
    """
    market_type = request.GET.get('market_type')
    zx_name = request.GET.get('zx_name')

    zx = zixuan.ZiXuan(market_type)
    stocks = zx.zx_stocks(zx_name)
    return utils.json_response(stocks)


@login_required
def zixuan_code_zx_names_json(request):
    """
    查询代码所在的自选分组
    """
    market_type = request.GET.get('market_type')
    code = request.GET.get('code')

    zx = zixuan.ZiXuan(market_type)
    zx_names = zx.query_code_zx_names(code)
    return utils.json_response(zx_names)


@login_required
def zixuan_operation_json(request):
    """
    自选操作，添加 or 删除
    """
    market_type = request.GET.get('market_type')
    zx_name = request.GET.get('zx_name')
    opt = request.GET.get('opt')
    code = request.GET.get('code')
    name = request.GET.get('name')

    zx = zixuan.ZiXuan(market_type)
    if opt == 'add':
        zx.add_stock(zx_name, code, name)
    elif opt == 'del':
        zx.del_stock(zx_name, code)

    return utils.json_response('OK')


@login_required
def ticks(request):
    """
    获取 ticks 信息
    """
    market = request.POST.get('market')
    codes = request.POST.get('codes')
    codes = json.loads(codes)
    # print(codes)
    ex = get_exchange(Market(market))
    ticks = ex.ticks(codes)

    try:
        now_trading = ex.now_trading()
        res_ticks = [{'code': _c, 'rate': round(float(_t.rate), 2)} for _c, _t in ticks.items()]
        return utils.json_response({'now_trading': now_trading, 'ticks': res_ticks})
    except Exception as e:
        return utils.json_response({'now_trading': False, 'ticks': []})


# 搜索股票代码
@login_required
def search_code_json(request):
    market = request.GET.get('market')
    search = request.GET.get('query')
    ex = get_exchange(Market(market))
    if hasattr(ex, 'search_stocks'):
        # 如果交易所对象有提供 搜索 的功能，直接调用搜索的功能
        res = ex.search_stocks(search)
    else:
        all_stocks = ex.all_stocks()
        # print(f'{market} search : {search} , market all stocks {all_stocks}')
        res = [
            stock for stock in all_stocks
            if search.lower() in stock['code'].lower() or search.lower() in stock[
                'name'].lower() or search.lower() in ''.join([pinyin.get_pinyin(_p)[0] for _p in stock['name']]).lower()
        ]

    res_json = [{'code': r['code'], 'name': r['name']} for r in res]

    return utils.response_as_json(res_json)


# 增加订单记录
@login_required
def add_order_json(request):
    market = request.POST.get('market')
    code = request.POST.get('code')
    dt = request.POST.get('dt')
    _type = request.POST.get('type')
    price = request.POST.get('price')
    amount = request.POST.get('amount')
    info = request.POST.get('info')

    rd.order_save(market, code, {
        "code": code,
        "datetime": dt,
        "type": _type,
        "price": price,
        "amount": amount,
        "info": info
    })

    return utils.json_response('OK')


# 增加订单记录
@login_required
def clean_order_json(request):
    market = request.POST.get('market')
    code = request.POST.get('code')

    rd.order_clean(market, code)

    return utils.json_response('OK')


@login_required
def cl_chart_config(request):
    """
    读取缠论和图表配置
    """
    market = request.GET.get('market')
    code = request.GET.get('code')
    config = query_cl_chart_config(market, code)
    return render(request, 'charts/options.html', config)


@login_required
def cl_chart_config_save(request):
    """
    设置缠论和图表设置
    """
    market = request.POST.get('market')
    code = request.POST.get('code')
    is_del = request.POST.get('is_del')
    keys = ['config_use_type',
            'kline_type',
            'fx_qj', 'fx_bh', 'bi_type', 'bi_bzh', 'bi_qj', 'bi_fx_cgd',
            'xd_bzh', 'xd_qj', 'zsd_bzh', 'zsd_qj', 'zs_bi_type', 'zs_xd_type', 'zs_qj', 'zs_wzgx',
            'idx_macd_fast', 'idx_macd_slow', 'idx_macd_signal',
            'fx_qy', 'xd_zs_max_lines_split', 'allow_split_one_line_to_xd', 'allow_bi_fx_strict',
            'enable_kchart_low_to_high', 'bi_split_k_cross_nums', 'fx_check_k_nums',
            'cl_mmd_cal_qs_1mmd', 'cl_mmd_cal_not_qs_3mmd_1mmd', 'cl_mmd_cal_qs_3mmd_1mmd',
            'cl_mmd_cal_qs_not_lh_2mmd', 'cl_mmd_cal_qs_bc_2mmd', 'cl_mmd_cal_3mmd_not_lh_bc_2mmd',
            'cl_mmd_cal_1mmd_not_lh_2mmd', 'cl_mmd_cal_3mmd_xgxd_not_bc_2mmd', 'cl_mmd_cal_not_in_zs_3mmd',
            'chart_show_infos', 'chart_show_fx',
            'chart_show_bi', 'chart_show_xd', 'chart_show_zsd', 'chart_show_qsd',
            'chart_show_bi_zs', 'chart_show_xd_zs', 'chart_show_zsd_zs', 'chart_show_qsd_zs',
            'chart_show_bi_mmd', 'chart_show_xd_mmd', 'chart_show_zsd_mmd', 'chart_show_qsd_mmd',
            'chart_show_bi_bc', 'chart_show_xd_bc', 'chart_show_zsd_bc', 'chart_show_qsd_bc',
            'chart_show_ma', 'chart_show_boll',
            'chart_show_futu', 'chart_kline_nums', 'chart_show_ld',
            'chart_idx_ma_period', 'chart_idx_vol_ma_period', 'chart_idx_boll_period', 'chart_idx_rsi_period',
            'chart_idx_atr_period', 'chart_idx_cci_period', 'chart_idx_kdj_period', 'chart_qstd',
            'kline_qk',
            ]
    config = {}
    for _k in keys:
        if _k in ['zs_bi_type', 'zs_xd_type']:
            config[_k] = request.POST.getlist(_k)
            # print(f'List value: {request.POST.getlist(_k)}')
        else:
            config[_k] = request.POST.get(_k)
        # print(f'{_k} : {config[_k]}')

    if is_del == 'true':
        res = del_cl_chart_config(market, code)
    else:
        res = set_cl_chart_config(market, code, config)
    return utils.json_response(res) if res else utils.json_error()
