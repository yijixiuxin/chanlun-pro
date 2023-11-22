import datetime
import json
import time
import traceback

import pinyin
import pytz
from apscheduler.events import EVENT_ALL, EVENT_EXECUTOR_ADDED, EVENT_EXECUTOR_REMOVED, EVENT_JOBSTORE_ADDED, \
    EVENT_JOBSTORE_REMOVED, EVENT_JOB_ADDED, EVENT_JOB_REMOVED, EVENT_JOB_MODIFIED, EVENT_JOB_SUBMITTED, \
    EVENT_JOB_MAX_INSTANCES, EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from flask import render_template
from flask import request

from chanlun import fun, rd
from chanlun.cl_utils import kcharts_frequency_h_l_map, query_cl_chart_config, web_batch_get_cl_datas, \
    cl_data_to_tv_chart, set_cl_chart_config, del_cl_chart_config
from chanlun.exchange import get_exchange, Market
from chanlun.zixuan import ZiXuan
from .alert_tasks import AlertTasks


def create_app(test_config=None):
    # 任务对象
    scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Shanghai'))
    scheduler.add_executor(ThreadPoolExecutor(12))
    task_list = {}
    scheduler.my_task_list = task_list

    def run_tasks_listener(event):
        state_map = {
            EVENT_EXECUTOR_ADDED: '已添加',
            EVENT_EXECUTOR_REMOVED: '删除调度',
            EVENT_JOBSTORE_ADDED: '已添加',
            EVENT_JOBSTORE_REMOVED: '删除存储',
            EVENT_JOB_ADDED: '已添加',
            EVENT_JOB_REMOVED: '删除作业',
            EVENT_JOB_MODIFIED: '修改作业',
            EVENT_JOB_SUBMITTED: '运行中',
            EVENT_JOB_MAX_INSTANCES: '等待运行',
            EVENT_JOB_EXECUTED: '已完成',
            EVENT_JOB_ERROR: '执行异常',
            EVENT_JOB_MISSED: '未执行',
        }
        if event.code not in state_map.keys():
            return
        if hasattr(event, 'job_id'):
            job_id = event.job_id
            if job_id not in task_list.keys():
                task_list[job_id] = {
                    'id': job_id,
                    'name': '--',
                    'update_dt': fun.datetime_to_str(datetime.datetime.now()),
                    'next_run_dt': '--',
                    'state': '未知',
                }
            task_list[job_id]['update_dt'] = fun.datetime_to_str(datetime.datetime.now())
            job = scheduler.get_job(event.job_id)
            if job is not None:
                task_list[job_id]['name'] = job.name
                task_list[job_id]['next_run_dt'] = fun.datetime_to_str(job.next_run_time)
            task_list[job_id]['state'] = state_map[event.code]
            # print('任务更新', task_list[job_id])
        return

    scheduler.add_listener(run_tasks_listener, EVENT_ALL)
    scheduler.start()

    # 项目中的周期与 tv 的周期对应表
    frequency_maps = {
        '10s': '10S', '30s': '30S',
        '1m': '1', '2m': '2', '3m': '3', '5m': '5', '10m': '10', '15m': '15',
        '30m': '30', '60m': '60', '120m': '120',
        '4h': '240',
        'd': '1D', '2d': '2D',
        'w': '1W', 'm': '1M', 'y': '12M'
    }

    resolution_maps = dict(zip(frequency_maps.values(), frequency_maps.keys()))

    # 各个市场支持的时间周期
    market_frequencys = {
        'a': list(get_exchange(Market.A).support_frequencys().keys()),
        'hk': list(get_exchange(Market.HK).support_frequencys().keys()),
        'us': list(get_exchange(Market.US).support_frequencys().keys()),
        'futures': list(get_exchange(Market.FUTURES).support_frequencys().keys()),
        'currency': list(get_exchange(Market.CURRENCY).support_frequencys().keys()),
    }

    # 各个交易所默认的标的
    market_default_codes = {
        'a': get_exchange(Market.A).default_code(),
        'hk': get_exchange(Market.HK).default_code(),
        'us': get_exchange(Market.US).default_code(),
        'futures': get_exchange(Market.FUTURES).default_code(),
        'currency': get_exchange(Market.CURRENCY).default_code(),
    }

    # 各个市场的交易时间
    market_session = {
        'a': '0930-1501',
        'hk': '0930-1201,1330-1601',
        'us': '0400-0931,0930-1631,1600-2001',
        'futures': '24x7',
        'currency': '24x7',
    }

    # 各个交易所的时区 统一时区
    market_timezone = {
        'a': 'Asia/Shanghai',
        'hk': 'Asia/Shanghai',
        'us': 'America/New_York',
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

    # 记录请求次数，超过则返回 no_data
    __history_req_counter = {}

    _alert_tasks = AlertTasks(scheduler)
    _alert_tasks.run()

    __log = fun.get_logger()

    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)

    @app.route('/')
    def index_show():
        """
        首页
        """

        return render_template('index.html', market_default_codes=market_default_codes)

    @app.route('/tv/config')
    def tv_config():
        """
        配置项
        """
        frequencys = list(
            set(market_frequencys['a']) | set(market_frequencys['hk']) | set(market_frequencys['us']) | set(
                market_frequencys['futures']) | set(market_frequencys['currency']))
        supportedResolutions = [v for k, v in frequency_maps.items() if k in frequencys]
        return {
            'supports_search': True,
            'supports_group_request': False,
            'supported_resolutions': supportedResolutions,
            'supports_marks': False,
            'supports_timescale_marks': True,
            'supports_time': False,
            'exchanges': [
                {'value': "a", 'name': "沪深", 'desc': "沪深A股"},
                {'value': "hk", 'name': "港股", 'desc': "港股"},
                {'value': "us", 'name': "美股", 'desc': "美股"},
                {'value': "futures", 'name': "期货", 'desc': "期货"},
                {'value': "currency", 'name': "数字货币", 'desc': "数字货币"},
            ],
        }

    @app.route('/tv/symbol_info')
    def tv_symbol_info():
        """
        商品集合信息
        supports_search is True 则不会调用这个接口
        """
        group = request.args.get('group')
        ex = get_exchange(Market(group))
        all_symbols = ex.all_stocks()

        info = {
            'symbol': [s['code'] for s in all_symbols],
            'description': [s['name'] for s in all_symbols],
            'exchange-listed': group,
            'exchange-traded': group,
        }
        return info

    @app.route('/tv/symbols')
    def tv_symbols():
        """
        商品解析
        """
        symbol: str = request.args.get('symbol')
        symbol: list = symbol.split(':')
        market: str = symbol[0].lower()
        code: str = symbol[1]

        ex = get_exchange(Market(market))
        stocks = ex.stock_info(code)

        sector = ''
        industry = ''
        if market == 'a':
            gnbk = ex.stock_owner_plate(code)
            sector = ' / '.join([_g['name'] for _g in gnbk['GN']])
            industry = ' / '.join([_h['name'] for _h in gnbk['HY']])

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
            'intraday_multipliers': ['1', '2', '3', '5', '10', '15', '20', '30', '60', '120', '240'],
            'seconds_multipliers': ['1', '2', '3', '5', '10', '15', '20', '30', '40', '50', '60'],
            'has_intraday': True,
            'has_seconds': True if market == 'futures' else False,
            'has_daily': True,
            'has_weekly_and_monthly': True,
            'sector': sector,
            'industry': industry,
        }
        return info

    @app.route('/tv/search')
    def tv_search():
        """
        商品检索
        """
        query = request.args.get('query')
        type = request.args.get('type')
        exchange = request.args.get('exchange')
        limit = request.args.get('limit')

        ex = get_exchange(Market(exchange))
        all_stocks = ex.all_stocks()

        res_stocks = [
            stock for stock in all_stocks
            if query.lower() in stock['code'].lower() or query.lower() in stock[
                'name'].lower() or query.lower() in ''.join(
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
        return infos

    @app.route('/tv/history')
    def tv_history():
        """
        K线柱
        """

        symbol = request.args.get('symbol')
        _from = request.args.get('from')
        _to = request.args.get('to')
        resolution = request.args.get('resolution')
        countback = request.args.get('countback')
        firstDataRequest = request.args.get('firstDataRequest', 'false')

        _symbol_res_old_k_time_key = f'{symbol}_{resolution}'

        now_time = time.time()

        s = 'ok'
        if int(_from) < 0 and int(_to) < 0:
            s = 'no_data'

        if firstDataRequest == 'false':
            # 判断在 5 秒内，同一个请求大于 5 次，返回 no_data
            if _symbol_res_old_k_time_key not in __history_req_counter.keys():
                __history_req_counter[_symbol_res_old_k_time_key] = {
                    'counter': 0,
                    'tm': now_time
                }
            else:
                if __history_req_counter[_symbol_res_old_k_time_key]['counter'] >= 5:
                    __history_req_counter[_symbol_res_old_k_time_key] = {
                        'counter': 0,
                        'tm': now_time
                    }
                    s = 'no_data'
                elif now_time - __history_req_counter[_symbol_res_old_k_time_key]['tm'] <= 5:
                    __history_req_counter[_symbol_res_old_k_time_key]['counter'] += 1
                    __history_req_counter[_symbol_res_old_k_time_key]['tm'] = now_time
                else:
                    __history_req_counter[_symbol_res_old_k_time_key] = {
                        'counter': 0,
                        'tm': now_time
                    }

        market = symbol.split(':')[0].lower()
        code = symbol.split(':')[1]

        ex = get_exchange(Market(market))
        # 判断当前是否可交易时间
        if firstDataRequest == 'false' and ex.now_trading() is False:
            return {'s': 'no_data', 'nextTime': int(now_time + (10 * 60))}

        frequency = resolution_maps[resolution]
        cl_config = query_cl_chart_config(market, code)
        frequency_low, kchart_to_frequency = kcharts_frequency_h_l_map(market, frequency)
        if cl_config['enable_kchart_low_to_high'] == '1' and kchart_to_frequency is not None:
            # 如果开启并设置的该级别的低级别数据，获取低级别数据，并在转换成高级图表展示
            # s_time = time.time()
            klines = ex.klines(code, frequency_low)
            # __log.info(f'{code} - {frequency_low} enable low to high get klines time : {time.time() - s_time}')
            # s_time = time.time()
            cd = web_batch_get_cl_datas(market, code, {frequency_low: klines}, cl_config)[0]
            # __log.info(f'{code} - {frequency_low} enable low to high get cd time : {time.time() - s_time}')
        else:
            kchart_to_frequency = None
            # s_time = time.time()
            klines = ex.klines(code, frequency)
            # __log.info(f'{code} - {frequency} get klines time : {time.time() - s_time}')
            # s_time = time.time()
            cd = web_batch_get_cl_datas(market, code, {frequency: klines}, cl_config)[0]
            # __log.info(f'{code} - {frequency} get cd time : {time.time() - s_time}')

        # 将缠论数据，转换成 tv 画图的坐标数据
        # s_time = time.time()
        cl_chart_data = cl_data_to_tv_chart(cd, cl_config, to_frequency=kchart_to_frequency)
        # __log.info(f'{code} - {frequency} to tv chart data time : {time.time() - s_time}')

        info = {
            's': s,
            't': cl_chart_data['t'],
            'c': cl_chart_data['c'],
            'o': cl_chart_data['o'],
            'h': cl_chart_data['h'],
            'l': cl_chart_data['l'],
            'v': cl_chart_data['v'],
            'fxs': cl_chart_data['fxs'],
            'bis': cl_chart_data['bis'],
            'xds': cl_chart_data['xds'],
            'zsds': cl_chart_data['zsds'],
            'bi_zss': cl_chart_data['bi_zss'],
            'xd_zss': cl_chart_data['xd_zss'],
            'zsd_zss': cl_chart_data['zsd_zss'],
            'bcs': cl_chart_data['bcs'],
            'mmds': cl_chart_data['mmds'],
        }
        return info

    @app.route('/tv/timescale_marks')
    def tv_timescale_marks():
        symbol = request.args.get('symbol')
        _from = int(request.args.get('from'))
        _to = int(request.args.get('to'))
        resolution = request.args.get('resolution')
        market = symbol.split(':')[0]
        code = symbol.split(':')[1]

        freq = resolution_maps[resolution]

        order_type_maps = {
            'buy': '买入', 'sell': '卖出',
            'open_long': '买入开多', 'open_short': '买入开空', 'close_long': '卖出平多', 'close_short': '买入平空'
        }
        marks = []

        # 增加订单的信息
        orders = list(rd.order_query(market, code))
        for i in range(len(orders)):
            o = orders[i]
            _dt_int = fun.str_to_timeint(o['datetime'])
            if _from <= _dt_int <= _to:
                m = {
                    'id': i,
                    'time': _dt_int,
                    'color': 'red' if o['type'] in ['buy', 'open_long', 'close_short'] else 'green',
                    'label': 'B' if o['type'] in ['buy', 'open_long', 'close_short'] else 'S',
                    'tooltip': [f"{order_type_maps[o['type']]}[{o['price']}/{o['amount']}]",
                                f"{'' if 'info' not in o else o['info']}"],
                    'shape': 'earningUp' if o['type'] in ['buy', 'open_long', 'close_short'] else 'earningDown'
                }
                marks.append(m)

        # 增加其他自定义信息
        other_marks = rd.get_code_marks(code)
        for i in range(len(other_marks)):
            _m = other_marks[i]
            if _m['show_freq'] == '' or _m['show_freq'] == freq:
                if _from <= _m['time'] <= _to:
                    marks.append({
                        'id': f'm-{i}',
                        'time': int(_m['time']),
                        'color': _m['color'],
                        'label': _m['label'],
                        'tooltip': _m['tooltip'],
                        'shape': _m['shape'],
                    })

        return marks

    @app.route('/tv/time')
    def tv_time():
        """
        服务器时间
        """
        return fun.datetime_to_int(datetime.datetime.now())

    @app.route('/tv/<version>/charts', methods=['GET', 'POST', 'DELETE'])
    def tv_charts(version):
        """
        图表
        """
        client_id = str(request.args.get('client'))
        user_id = str(request.args.get('user'))
        chart = request.args.get('chart')

        key = f'charts_{client_id}_{user_id}'
        data: str = rd.Robj().get(key)
        if data is None:
            data: dict = {}
        else:
            data: dict = json.loads(data)

        if request.method == 'GET':
            # 列出保存的图表列表
            if chart is None:
                return {
                    'status': 'ok',
                    'data': list(data.values()),
                }
            else:
                return {
                    'status': 'ok',
                    'data': data[chart],
                }
        elif request.method == 'DELETE':
            # 删除操作
            del (data[chart])
            rd.Robj().set(key, json.dumps(data))
            return {
                'status': 'ok',
            }
        else:
            name = request.form['name']
            content = request.form['content']
            symbol = request.form['symbol']
            resolution = request.form['resolution']

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
                return {
                    'status': 'ok',
                    'id': id,
                }
            else:
                # 保存已有的图表信息
                save_data['id'] = chart
                data[chart] = save_data
                rd.Robj().set(key, json.dumps(data))
                return {
                    'status': 'ok',
                    'id': chart,
                }

    @app.route('/tv/<version>/study_templates', methods=['GET', 'POST', 'DELETE'])
    def tv_study_templates(version):
        """
        图表
        """
        client_id = str(request.args.get('client'))
        user_id = str(request.args.get('user'))
        template = request.args.get('template')

        key = f'study_templates_{client_id}_{user_id}'
        data: str = rd.Robj().get(key)
        if data is None:
            data: dict = {}
        else:
            data: dict = json.loads(data)

        if request.method == 'GET':
            # 列出保存的图表列表
            if template is None:
                return {
                    'status': 'ok',
                    'data': list(data.values()),
                }
            else:
                return {
                    'status': 'ok',
                    'data': data[template],
                }
        elif request.method == 'DELETE':
            # 删除操作
            del (data[template])
            rd.Robj().set(key, json.dumps(data))
            return {
                'status': 'ok',
            }
        else:
            name = request.form['name']
            content = request.form['content']
            save_data = {
                'name': name,
                'content': content
            }
            # 保存图表信息
            data[name] = save_data
            rd.Robj().set(key, json.dumps(data))
            return {
                'status': 'ok',
                'id': name,
            }

    # 查询配置项
    @app.route('/get_cl_config/<market>/<code>')
    def get_cl_config(market, code: str):
        code = code.replace('__', '/')  # 数字货币特殊处理
        cl_config = query_cl_chart_config(market, code)
        cl_config['market'] = market
        cl_config['code'] = code
        return render_template('options.html', **cl_config)

    # 设置配置项
    @app.route('/set_cl_config', methods=['POST'])
    def set_cl_config():
        market = request.form['market']
        code = request.form['code']
        is_del = request.form['is_del']
        if is_del == 'true':
            res = del_cl_chart_config(market, code)
            return {'ok': res}

        keys = [
            'config_use_type',
            'kline_type', 'kline_qk',
            'fx_qj', 'fx_bh', 'bi_type', 'bi_bzh', 'bi_qj', 'bi_fx_cgd',
            'xd_qj', 'zsd_qj', 'zs_bi_type', 'zs_xd_type', 'zs_qj', 'zs_wzgx',
            'idx_macd_fast', 'idx_macd_slow', 'idx_macd_signal',
            'fx_qy', 'xd_zs_max_lines_split', 'allow_split_one_line_to_xd', 'allow_bi_fx_strict',
            'enable_kchart_low_to_high', 'bi_split_k_cross_nums', 'fx_check_k_nums',
            'cl_mmd_cal_qs_1mmd', 'cl_mmd_cal_not_qs_3mmd_1mmd', 'cl_mmd_cal_qs_3mmd_1mmd',
            'cl_mmd_cal_qs_not_lh_2mmd', 'cl_mmd_cal_qs_bc_2mmd', 'cl_mmd_cal_3mmd_not_lh_bc_2mmd',
            'cl_mmd_cal_1mmd_not_lh_2mmd', 'cl_mmd_cal_3mmd_xgxd_not_bc_2mmd', 'cl_mmd_cal_not_in_zs_3mmd',
            'cl_mmd_cal_not_in_zs_gt_9_3mmd',
            'chart_show_bi', 'chart_show_xd', 'chart_show_zsd', 'chart_show_qsd',
            'chart_show_bi_zs', 'chart_show_xd_zs', 'chart_show_zsd_zs', 'chart_show_qsd_zs',
            'chart_show_bi_mmd', 'chart_show_xd_mmd', 'chart_show_zsd_mmd', 'chart_show_qsd_mmd',
            'chart_show_bi_bc', 'chart_show_xd_bc', 'chart_show_zsd_bc', 'chart_show_qsd_bc',
            'chart_show_fx',
        ]
        cl_config = {}
        for _k in keys:
            cl_config[_k] = request.form[_k]
            if _k in ['zs_bi_type', 'zs_xd_type']:
                cl_config[_k] = cl_config[_k].split(',')
            if cl_config[_k] == '':
                cl_config[_k] = '0'
            # print(f'{_k} : {config[_k]}')

        res = set_cl_chart_config(market, code, cl_config)
        return {'ok': res}

    # 股票涨跌幅
    @app.route('/ticks', methods=['POST'])
    def ticks():
        market = request.form['market']
        codes = request.form['codes']
        codes = json.loads(codes)
        ex = get_exchange(Market(market))
        stock_ticks = ex.ticks(codes)
        try:
            now_trading = ex.now_trading()
            res_ticks = [{'code': _c, 'rate': round(float(_t.rate), 2)} for _c, _t in stock_ticks.items()]
            return {'now_trading': now_trading, 'ticks': res_ticks}
        except Exception as e:
            traceback.print_exc()
        return {'now_trading': False, 'ticks': []}

    # 获取自选组列表
    @app.route('/get_zixuan_groups/<market>')
    def get_zixuan_groups(market):
        zx = ZiXuan(market)
        groups = zx.zixuan_list
        return groups

    # 获取自选组的股票
    @app.route('/get_zixuan_stocks/<market>/<group_name>')
    def get_zixuan_stocks(market, group_name):
        zx = ZiXuan(market)
        stock_list = zx.zx_stocks(group_name)
        return {
            "code": 0,
            "msg": "",
            "count": len(stock_list),
            "data": stock_list
        }

    @app.route('/get_stock_zixuan/<market>/<code>')
    def get_stock_zixuan(market, code:str):
        code = code.replace('__', '/')  # 数字货币特殊处理
        zx = ZiXuan(market)
        zx_groups = zx.query_code_zx_names(code)
        return zx_groups

    # 设置股票的自选组
    @app.route('/set_stock_zixuan', methods=['POST'])
    def set_stock_zixuan():
        market = request.form['market']
        opt = request.form['opt']
        group_name = request.form['group_name']
        code = request.form['code']
        zx = ZiXuan(market)
        if opt == 'DEL':
            res = zx.del_stock(group_name, code)
        elif opt == 'ADD':
            res = zx.add_stock(group_name, code, None)
        elif opt == 'COLOR':
            color = request.form['color']
            res = zx.color_stock(group_name, code, color)
        elif opt == 'SORT':
            direction = request.form['direction']
            if direction == 'top':
                res = zx.sort_top_stock(group_name, code)
            else:
                res = zx.sort_bottom_stock(group_name, code)
        else:
            res = False

        return {'ok': res}

    # 警报提醒列表
    @app.route('/alert_list/<market>')
    def alert_list(market):
        al = _alert_tasks.alert_list()
        print(al)
        al = [_l for _l in al if _l['market'] == market]
        return {
            "code": 0,
            "msg": "",
            "count": len(al),
            "data": al
        }

    # 警报编辑页面
    @app.route('/alert_edit/<market>/<id>')
    def alert_edit(market, id):
        alert_config = {
            'id': '',
            'market': market,
            'alert_name': '',
            'interval_minutes': 5,
            'zixuan_group': '自选股',
            'frequency': '5m',
            'check_bi_bc': '',
            'check_bi_mmd': '',
            'check_xd_bc': '',
            'check_xd_mmd': '',
            'is_send_msg': '1',
            'enable': '1',
        }
        if id != '0':
            _alert_config = _alert_tasks.alert_get(id)
            if _alert_config is not None:
                alert_config = _alert_config

        # 获取自选组
        zx = ZiXuan(market)
        zixuan_groups = zx.zixuan_list

        # 交易所支持周期
        frequencys = get_exchange(Market(market)).support_frequencys()

        return render_template('alert.html', zixuan_groups=zixuan_groups, frequencys=frequencys, **alert_config)

    @app.route('/alert_save', methods=['POST'])
    def alert_save():
        alert_config = {
            'id': request.form['id'],
            'market': request.form['market'],
            'alert_name': request.form['alert_name'],
            'interval_minutes': request.form['interval_minutes'],
            'zixuan_group': request.form['zixuan_group'],
            'frequency': request.form['frequency'],
            'check_bi_bc': request.form['check_bi_bc'],
            'check_bi_mmd': request.form['check_bi_mmd'],
            'check_xd_bc': request.form['check_xd_bc'],
            'check_xd_mmd': request.form['check_xd_mmd'],
            'is_send_msg': request.form['is_send_msg'],
            'enable': request.form['enable'],
        }
        res = _alert_tasks.alert_save(alert_config)
        return {'ok': True}

    @app.route('/alert_del/<id>')
    def alert_del(id):
        res = _alert_tasks.alert_del(id)
        return {'ok': res}

    @app.route('/alert_records/<market>')
    def alert_records(market):
        records = rd.jhs_query(market)
        return {
            "code": 0,
            "msg": "",
            "count": len(records),
            "data": records,
        }

    @app.route('/jobs')
    def jobs():
        return render_template('jobs.html', jobs=list(task_list.values()))

    return app
