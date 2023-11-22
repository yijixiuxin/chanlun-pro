import datetime
import re
import pinyin

from django.shortcuts import render

from chanlun.cl_utils import web_batch_get_cl_datas, query_cl_chart_config, cl_data_to_tv_chart, \
    kcharts_frequency_h_l_map
from .apps import login_required
from .utils import *
from chanlun.exchange import *
from chanlun import fun
from chanlun import rd

'''
TradingView 行情图表
'''

# 项目中的周期与 tv 的周期对应表
frequency_maps = {
    '10s': '10S', '30s': '30S',
    '1m': '1', '2m': '2', '3m': '3', '5m': '5', '10m': '10', '15m': '15', '30m': '30', '60m': '60', '120m': '120',
    '4h': '240',
    'd': '1D', '2d': '2D',
    'w': '1W', 'm': '1M', 'y': '12M'
}

resolution_maps = dict(zip(frequency_maps.values(), frequency_maps.keys()))

# 各个市场支持的时间周期
market_frequencys = {
    'a': ['y', 'm', 'w', 'd', '120m', '60m', '30m', '15m', '5m', '1m'],
    'hk': ['y', 'm', 'w', 'd', '120m', '60m', '30m', '15m', '10m', '5m', '1m'],
    'us': ['y', 'm', 'w', 'd', '120m', '60m', '30m', '15m', '5m', '1m'],
    'futures': ['w', 'd', '60m', '30m', '15m', '10m', '6m', '5m', '3m', '2m', '1m', '30s', '10s'],
    'currency': ['w', 'd', '4h', '60m', '30m', '15m', '10m', '5m', '3m', '2m', '1m'],
}

# 各个市场的交易时间
market_session = {
    'a': '0930-1131,1300-1501',
    'hk': '0930-1231,1430-1601',
    'us': '0930-1601',
    'futures': '0900-1016,1030-1131,1330-1501,2100-2301',
    'currency': '24x7',
}

# 各个交易所的时区 统一时区
market_timezone = {
    'a': 'Asia/Shanghai',
    'hk': 'Asia/Shanghai',
    'us': 'US/Eastern',
    'futures': 'Asia/Shanghai',
    'currency': 'Asia/Shanghai',
}

market_types = {
    'a': 'stock',
    'hk': 'stock',
    'us': 'stock',
    'futures': 'futures',
    'currency': 'crypto',
}

# 记录背驰marks 记录
load_bc_marks_cache = {}
# 记录最后历史k线的最后时间
load_old_kline_times = {}


@login_required
def index_show(request):
    """
    TV 行情首页
    :param request:
    :return:
    """
    is_mobile = judge_pc_or_mobile(request.META.get("HTTP_USER_AGENT"))
    return render(request, 'charts/tv/index.html', {
        'is_mobile': is_mobile
    })


@login_required
def config(request):
    """
    配置项
    """
    frequencys = list(set(market_frequencys['a']) | set(market_frequencys['hk']) | set(market_frequencys['us']) | set(
        market_frequencys['futures']) | set(market_frequencys['currency']))
    supportedResolutions = [v for k, v in frequency_maps.items() if k in frequencys]
    return response_as_json({
        'supports_search': True,
        'supports_group_request': False,
        'supported_resolutions': supportedResolutions,
        'supports_marks': False,
        'supports_timescale_marks': False,
        'supports_time': False,
        'exchanges': [
            {'value': "a", 'name': "沪深", 'desc': "沪深A股"},
            {'value': "hk", 'name': "港股", 'desc': "港股"},
            {'value': "us", 'name': "美股", 'desc': "美股"},
            {'value': "futures", 'name': "期货", 'desc': "期货"},
            {'value': "currency", 'name': "数字货币", 'desc': "数字货币"},
        ],
    })


@login_required
def symbol_info(request):
    """
    商品集合信息
    supports_search is True 则不会调用这个接口
    """
    group = request.GET.get('group')
    ex = get_exchange(Market(group))
    all_symbols = ex.all_stocks()

    info = {
        'symbol': [s['code'] for s in all_symbols],
        'description': [s['name'] for s in all_symbols],
        'exchange-listed': group,
        'exchange-traded': group,
    }
    return response_as_json(info)


@login_required
def symbols(request):
    """
    商品解析
    """
    symbol: str = request.GET.get('symbol')
    symbol: list = symbol.split(':')
    market = symbol[0]
    code = symbol[1]

    ex = get_exchange(Market(market))
    stocks = ex.stock_info(code)

    info = {
        "name": stocks["code"],
        "ticker": f'{market}:{stocks["code"]}',
        "full_name": f'{market}:{stocks["code"]}',
        "description": stocks['name'],
        "exchange": market,
        "type": market_types[market],
        'session': market_session[market],
        'timezone': market_timezone[market],
        'supported_resolutions': [v for k, v in frequency_maps.items() if k in market_frequencys[market]],
        'has_intraday': True,
        'has_seconds': True if market == 'futures' else False,
        'has_daily': True,
        'has_weekly_and_monthly': True,
    }
    return response_as_json(info)


@login_required
def search(request):
    """
    商品检索
    """
    query = request.GET.get('query')
    type = request.GET.get('type')
    exchange = request.GET.get('exchange')
    limit = request.GET.get('limit')

    ex = get_exchange(Market(exchange))
    all_stocks = ex.all_stocks()

    res_stocks = [
        stock for stock in all_stocks
        if query.lower() in stock['code'].lower() or query.lower() in stock['name'].lower() or query.lower() in ''.join(
            [pinyin.get_pinyin(_p)[0] for _p in stock['name']]).lower()
    ]
    res_stocks = res_stocks[0:int(limit)]

    infos = []
    for stock in res_stocks:
        infos.append({
            "symbol": stock["code"],
            "name": stock["code"],
            "full_name": f'{exchange}:{stock["code"]}',
            "description": stock['name'],
            "exchange": exchange,
            "ticker": f'{exchange}:{stock["code"]}',
            "type": type,
            'session': market_session[exchange],
            'timezone': market_timezone[exchange],
            'supported_resolutions': [v for k, v in frequency_maps.items() if k in market_frequencys[exchange]],
        })
    return response_as_json(infos)


@login_required
def history(request):
    """
    K线柱
    """
    global load_bc_marks_cache, load_old_kline_times

    symbol = request.GET.get('symbol')
    _from = request.GET.get('from')
    _to = request.GET.get('to')
    resolution = request.GET.get('resolution')
    countback = request.GET.get('countback')

    _symbol_res_old_k_time_key = f'{symbol}_{resolution}'
    old_k_time = 0
    if _symbol_res_old_k_time_key in load_old_kline_times.keys():
        old_k_time = load_old_kline_times[_symbol_res_old_k_time_key]

    now_time = fun.datetime_to_int(datetime.datetime.now())
    if int(_to) < old_k_time:
        return response_as_json({'s': 'no_data', 'errmsg': '不支持历史数据加载', 'nextTime': now_time + 3})

    market = symbol.split(':')[0]
    code = symbol.split(':')[1]

    ex = get_exchange(Market(market))
    # 判断当前是否可交易时间
    if ex.now_trading() is False and int(countback) < 5:
        return response_as_json({'s': 'no_data', 'errmsg': '非交易时间', 'nextTime': now_time + (10 * 60)})

    frequency = resolution_maps[resolution]
    cl_config = query_cl_chart_config(market, code)
    frequency_low, kchart_to_frequency = kcharts_frequency_h_l_map(market, frequency)
    if cl_config['enable_kchart_low_to_high'] == '1' and kchart_to_frequency is not None:
        # 如果开启并设置的该级别的低级别数据，获取低级别数据，并在转换成高级图表展示
        klines = ex.klines(code, frequency_low)
        cd = web_batch_get_cl_datas(market, code, {frequency_low: klines}, cl_config, )[0]
    else:
        klines = ex.klines(code, frequency)
        cd = web_batch_get_cl_datas(market, code, {frequency: klines}, cl_config, )[0]

    # 将缠论数据，转换成 tv 画图的坐标数据
    cl_chart_data = cl_data_to_tv_chart(cd, cl_config, to_frequency=kchart_to_frequency)
    # 记录最开始的一根k线时间
    load_old_kline_times[_symbol_res_old_k_time_key] = fun.datetime_to_int(klines.iloc[0]['date'])
    # 将计算好的背驰 marks 保存起来
    # load_bc_marks_cache[resolution] = cl_chart_data['bc_marks']

    info = {
        's': 'ok',
        't': cl_chart_data['t'],
        'c': cl_chart_data['c'],
        'o': cl_chart_data['o'],
        'h': cl_chart_data['h'],
        'l': cl_chart_data['l'],
        'v': cl_chart_data['v'],
        'bis': cl_chart_data['bis'],
        'xds': cl_chart_data['xds'],
        'zsds': cl_chart_data['zsds'],
        'bi_zss': cl_chart_data['bi_zss'],
        'xd_zss': cl_chart_data['xd_zss'],
        'zsd_zss': cl_chart_data['zsd_zss'],
        'bcs': cl_chart_data['bcs'],
        'mmds': cl_chart_data['mmds'],
    }
    return response_as_json(info)


def marks(request):
    global load_bc_marks_cache

    symbol = request.GET.get('symbol')
    _from = int(request.GET.get('from'))
    _to = int(request.GET.get('to'))
    resolution = request.GET.get('resolution')

    bc_marks = {
        'id': [], 'time': [], 'color': [], 'text': [], 'label': [], 'labelFontColor': [], 'minSize': []
    }
    if resolution in load_bc_marks_cache.keys():
        bc_marks = load_bc_marks_cache[resolution]

    print(bc_marks)

    marks = {
        'id': [], 'time': [], 'color': [], 'text': [], 'label': [], 'labelFontColor': [], 'minSize': []
    }
    for i in range(len(marks['id'])):
        if _from <= bc_marks['time'][i] <= _to:
            marks['id'].append(bc_marks['id'][i])
            marks['time'].append(bc_marks['time'][i])
            marks['color'].append(bc_marks['color'][i])
            marks['text'].append(bc_marks['text'][i])
            marks['label'].append(bc_marks['label'][i])
            marks['labelFontColor'].append(bc_marks['labelFontColor'][i])
            marks['minSize'].append(bc_marks['minSize'][i])

    return response_as_json(bc_marks)


@login_required
def time(request):
    """
    服务器时间
    """
    return HttpResponse(fun.datetime_to_int(datetime.datetime.now()))


def charts(request):
    """
    图表
    """
    client_id = str(request.GET.get('client'))
    user_id = str(request.GET.get('user'))
    chart = request.GET.get('chart')

    key = f'charts_{client_id}_{user_id}'
    data: str = rd.Robj().get(key)
    if data is None:
        data: dict = {}
    else:
        data: dict = json.loads(data)

    if request.method == 'GET':
        # 列出保存的图表列表
        if chart is None:
            return response_as_json({
                'status': 'ok',
                'data': list(data.values()),
            })
        else:
            return response_as_json({
                'status': 'ok',
                'data': data[chart],
            })
    elif request.method == 'DELETE':
        # 删除操作
        del (data[chart])
        rd.Robj().set(key, json.dumps(data))
        return response_as_json({
            'status': 'ok',
        })
    else:
        name = request.POST.get('name')
        content = request.POST.get('content')
        symbol = request.POST.get('symbol')
        resolution = request.POST.get('resolution')

        id = fun.datetime_to_int(datetime.datetime.now())
        save_data = {
            'timestamp': id,
            'symbol': symbol,
            'resolution': resolution,
            'name': name,
            'content': content
        }
        if chart is None:
            # 保存新的图表信息
            save_data['id'] = str(id)
            data[str(id)] = save_data
            rd.Robj().set(key, json.dumps(data))
            return response_as_json({
                'status': 'ok',
                'id': id,
            })
        else:
            # 保存已有的图表信息
            save_data['id'] = chart
            data[chart] = save_data
            rd.Robj().set(key, json.dumps(data))
            return response_as_json({
                'status': 'ok',
                'id': chart,
            })


def study_templates(request):
    """
    图表
    """
    client_id = str(request.GET.get('client'))
    user_id = str(request.GET.get('user'))
    template = request.GET.get('template')

    key = f'study_templates_{client_id}_{user_id}'
    data: str = rd.Robj().get(key)
    if data is None:
        data: dict = {}
    else:
        data: dict = json.loads(data)

    if request.method == 'GET':
        # 列出保存的图表列表
        if template is None:
            return response_as_json({
                'status': 'ok',
                'data': list(data.values()),
            })
        else:
            return response_as_json({
                'status': 'ok',
                'data': data[template],
            })
    elif request.method == 'DELETE':
        # 删除操作
        del (data[template])
        rd.Robj().set(key, json.dumps(data))
        return response_as_json({
            'status': 'ok',
        })
    else:
        name = request.POST.get('name')
        content = request.POST.get('content')
        save_data = {
            'name': name,
            'content': content
        }
        # 保存图表信息
        data[name] = save_data
        rd.Robj().set(key, json.dumps(data))
        return response_as_json({
            'status': 'ok',
            'id': name,
        })


def judge_pc_or_mobile(ua):
    """
    :param ua: 访问来源头信息中的User-Agent字段内容
    :return:
    """
    factor = ua
    is_mobile = False

    _long_matches = r'googlebot-mobile|android|avantgo|blackberry|blazer|elaine|hiptop|ip(hone|od)|kindle|midp|mmp' \
                    r'|mobile|o2|opera mini|palm( os)?|pda|plucker|pocket|psp|smartphone|symbian|treo|up\.(browser|link)' \
                    r'|vodafone|wap|windows ce; (iemobile|ppc)|xiino|maemo|fennec'
    _long_matches = re.compile(_long_matches, re.IGNORECASE)
    _short_matches = r'1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s\-)|ai(ko|rn)|al(av|ca|co)' \
                     r'|amoi|an(ex|ny|yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|\-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)' \
                     r'|bl(ac|az)|br(e|v)w|bumb|bw\-(n|u)|c55\/|capi|ccwa|cdm\-|cell|chtm|cldc|cmd\-|co(mp|nd)|craw' \
                     r'|da(it|ll|ng)|dbte|dc\-s|devi|dica|dmob|do(c|p)o|ds(12|\-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8' \
                     r'|ez([4-7]0|os|wa|ze)|fetc|fly(\-|_)|g1 u|g560|gene|gf\-5|g\-mo|go(\.w|od)|gr(ad|un)|haie|hcit' \
                     r'|hd\-(m|p|t)|hei\-|hi(pt|ta)|hp( i|ip)|hs\-c|ht(c(\-| |_|a|g|p|s|t)|tp)|hu(aw|tc)|i\-(20|go|ma)' \
                     r'|i230|iac( |\-|\/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|jbro|jemu|jigs|kddi|keji' \
                     r'|kgt( |\/)|klon|kpt |kwc\-|kyo(c|k)|le(no|xi)|lg( g|\/(k|l|u)|50|54|e\-|e\/|\-[a-w])|libw|lynx' \
                     r'|m1\-w|m3ga|m50\/|ma(te|ui|xo)|mc(01|21|ca)|m\-cr|me(di|rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi' \
                     r'|de|do|t(\-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)' \
                     r'|10)|ne((c|m)\-|on|tf|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg' \
                     r'|pg(13|\-([1-8]|c))|phil|pire|pl(ay|uc)|pn\-2|po(ck|rt|se)|prox|psio|pt\-g|qa\-a|qc(07|12|21' \
                     r'|32|60|\-[2-7]|i\-)|qtek|r380|r600|raks|rim9|ro(ve|zo)|s55\/|sa(ge|ma|mm|ms|ny|va)|sc(01|h\-' \
                     r'|oo|p\-)|sdk\/|se(c(\-|0|1)|47|mc|nd|ri)|sgh\-|shar|sie(\-|m)|sk\-0|sl(45|id)|sm(al|ar|b3|it' \
                     r'|t5)|so(ft|ny)|sp(01|h\-|v\-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)|ta(gt|lk)|tcl\-|tdg\-|tel(i|m)' \
                     r'|tim\-|t\-mo|to(pl|sh)|ts(70|m\-|m3|m5)|tx\-9|up(\.b|g1|si)|utst|v400|v750|veri|vi(rg|te)' \
                     r'|vk(40|5[0-3]|\-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(\-| )|webc|whit' \
                     r'|wi(g |nc|nw)|wmlb|wonu|x700|xda(\-|2|g)|yas\-|your|zeto|zte\-'

    _short_matches = re.compile(_short_matches, re.IGNORECASE)

    if _long_matches.search(factor) is not None:
        is_mobile = True
    user_agent = factor[0:4]
    if _short_matches.search(user_agent) is not None:
        is_mobile = True

    return is_mobile
