#coding:utf-8

from xtquant import xtdata
from xtquant import xtbson as _BSON_


class StrategyLoader:
    def __init__(this):
        this.C = None
        this.main_quote_subid = 0
        return

    def init(this):
        import os, uuid
        from xtquant import xtdata_config

        C = this.C

        C.guid = C._param.get('guid', str(uuid.uuid4()))
        C.request_id = C._param.get('requestid', '') + "_" + C.guid
        C.quote_mode = C._param.get('quote_mode', 'history') #'realtime' 'history' 'all'
        C.trade_mode = C._param.get('trade_mode', 'backtest') #'simulation' 'trading' 'backtest'
        C.do_back_test = 1 if C.trade_mode == 'backtest' else 0

        C.title = C._param.get('title', '')
        if not C.title:
            C.title = os.path.basename(os.path.abspath(C.user_script).replace('.py', ''))

        C.stock_code = C._param.get('stock_code', '')
        C.period = C._param.get('period', '')
        C.start_time = C._param.get('start_time', '')
        C.end_time = C._param.get('end_time', '')
        C.start_time_str = ''
        C.end_time_str = ''
        if type(C.period) == int:
            C.period = {
                0               :'tick'
                , 60000         :'1m'
                , 180000        :'3m'
                , 300000        :'5m'
                , 600000        :'10m'
                , 900000        :'15m'
                , 1800000       :'30m'
                , 3600000       :'1h'
                , 86400000      :'1d'
                , 604800000     :'1w'
                , 2592000000    :'1mon'
                , 7776000000    :'1q'
                , 15552000000   :'1hy'
                , 31536000000   :'1y'
            }.get(C.period, '')
        C.dividend_type = C._param.get('dividend_type', 'none')

        backtest = C._param.get('backtest', {})
        if backtest:
            C.asset = backtest.get('asset', 1000000.0)
            C.margin_ratio = backtest.get('margin_ratio', 0.05)
            C.slippage_type = backtest.get('slippage_type', 2)
            C.slippage = backtest.get('slippage', 0.0)
            C.max_vol_rate = backtest.get('max_vol_rate', 0.0)
            C.comsisson_type = backtest.get('comsisson_type', 0)
            C.open_tax = backtest.get('open_tax', 0.0)
            C.close_tax = backtest.get('close_tax', 0.0)
            C.min_commission = backtest.get('min_commission', 0.0)
            C.open_commission = backtest.get('open_commission', 0.0)
            C.close_commission = backtest.get('close_commission', 0.0)
            C.close_today_commission = backtest.get('close_today_commission', 0.0)
            C.benchmark = backtest.get('benchmark', '000300.SH')

        xtdata_config.client_guid = C._param.get('clientguid')

        from .functions import datetime_to_timetag

        if C.start_time:
            C.start_time_str = C.start_time.replace('-', '').replace(' ', '').replace(':', '')
            try:
                C.start_time_num = int(datetime_to_timetag(C.start_time_str))
            except:
                C.start_time_num = 0
        if C.end_time:
            C.end_time_str = C.end_time.replace('-', '').replace(' ', '').replace(':', '')
            try:
                C.end_time_num = int(datetime_to_timetag(C.end_time_str))
            except:
                C.end_time_num = 0

        if 1: #register
            this.create_formula()

        C.init()

        if 1: #fix param
            if '.' in C.stock_code:
                pos = C.stock_code.rfind('.')
                C.stockcode = C.stock_code[0:pos]
                C.market = C.stock_code[pos + 1:].upper()

            if C.stockcode and C.market:
                C.stock_code = C.stockcode + '.' + C.market
            C.period = C.period.lower()

        if C.stockcode == "" or C.market == "":
            raise Exception("股票代码为空")

        if 1: #create view
            if not C._param.get('requestid'):
                this.create_view(C.title)

        if 1: #post initcomplete
            init_result = {}

            config_ar = ['request_id', 'quote_mode', 'trade_mode']
            init_result['config'] = {ar: C.__getattribute__(ar) for ar in config_ar}

            quote_ar = [
                'stock_code', 'stockcode', 'market', 'period'
                , 'start_time', 'end_time', 'dividend_type'
            ]
            init_result['quote'] = {ar: C.__getattribute__(ar) for ar in quote_ar}

            trade_ar = []
            init_result['trade'] = {ar: C.__getattribute__(ar) for ar in trade_ar}

            backtest_ar = [
                'start_time', 'end_time', 'asset', 'margin_ratio', 'slippage_type', 'slippage'
                , 'max_vol_rate', 'comsisson_type', 'open_tax', 'close_tax'
                , 'min_commission', 'open_commission', 'close_commission'
                , 'close_today_commission', 'benchmark'
            ]
            init_result['backtest'] = {ar: C.__getattribute__(ar) for ar in backtest_ar}

            import datetime as dt
            if C.start_time:
                C.start_time_str = C.start_time.replace('-', '').replace(' ', '').replace(':', '')
                try:
                    C.start_time_num = int(datetime_to_timetag(C.start_time_str))
                except:
                    C.start_time_num = 0
                init_result['backtest']['start_time'] = dt.datetime.fromtimestamp(C.start_time_num / 1000).strftime('%Y-%m-%d %H:%M:%S')
            if C.end_time:
                C.end_time_str = C.end_time.replace('-', '').replace(' ', '').replace(':', '')
                try:
                    C.end_time_num = int(datetime_to_timetag(C.end_time_str))
                except:
                    C.end_time_num = 0
                init_result['backtest']['end_time'] = dt.datetime.fromtimestamp(C.end_time_num / 1000).strftime('%Y-%m-%d %H:%M:%S')

            this.call_formula('initcomplete', init_result)

        if 1:
            this.C.register_callback(0)
        return

    def shutdown(this):
        return

    def start(this):
        import time
        C = this.C

        if C.quote_mode in ['history', 'all']:
            this.load_main_history()

        C.after_init()
        this.run_bar()

        if C.quote_mode in ['realtime', 'all']:
            this.load_main_realtime()

        if C.trade_mode == 'backtest':
            time.sleep(0.4)
            C.on_backtest_finished()
        return

    def stop(this):
        if this.main_quote_subid:
            xtdata.unsubscribe_quote(this.main_quote_subid)

        this.C.stop()
        return

    def run(this):
        C = this.C

        if C.quote_mode in ['realtime', 'all']:
            xtdata.run()
        return

    def load_main_history(this):
        C = this.C

        data = xtdata.get_market_data_ex(
            field_list = ['time'], stock_list = [C.stock_code], period = C.period
            , start_time = '', end_time = '', count = -1
            , fill_data = False
        )

        C.timelist = list(data[C.stock_code]['time'])
        return

    def load_main_realtime(this):
        C = this.C

        def on_data(data):
            data = data.get(C.stock_code, [])
            if data:
                tt = data[-1]['time']
                this.on_main_quote(tt)
            return

        this.main_quote_subid = xtdata.subscribe_quote(
            stock_code = C.stock_code, period = C.period
            , start_time = '', end_time = '', count = 0
            , callback = on_data
        )
        return

    def on_main_quote(this, timetag):
        if not this.C.timelist or this.C.timelist[-1] < timetag:
            this.C.timelist.append(timetag)
        this.run_bar()
        return

    def run_bar(this):
        C = this.C

        push_timelist = []
        bar_timelist = []

        for i in range(max(C.lastrunbarpos, 0), len(C.timelist)):
            C.barpos = i
            bartime = C.timelist[i]

            push_timelist.append(bartime)
            bar_timelist.append(bartime)

            if (
                not C.start_time_num or C.start_time_num <= bartime
            ) and (
                not C.end_time_num or bartime <= C.end_time_num
            ):
                this.call_formula('runbar', {'timelist': bar_timelist})
                bar_timelist = []

                C.handlebar()

            C.lastrunbarpos = i

        if bar_timelist:
            this.call_formula('runbar', {'timelist': bar_timelist})
            bar_timelist = []

        if 1:
            push_result = {}
            push_result['timelist'] = push_timelist
            push_result['outputs'] = C.push_result
            C.push_result = {}
            this.call_formula('index', push_result)
        return

    def create_formula(this, callback = None):
        C = this.C
        client = xtdata.get_client()

        data = {
            'formulaname': '', 'stockcode': C.stock_code, 'period': C.period
            , 'starttime': C.start_time_str, 'endtime': C.end_time_str, 'count': 1
            , 'dividendtype': C.dividend_type, 'create': True, 'pyrunmode': 1
            , 'title': C.title
            , 'historycallback': 1 if callback else 0
            , 'realtimecallback': 1 if callback else 0
        }

        client.subscribeFormula(C.request_id, _BSON_.BSON.encode(data), callback)

    def call_formula(this, func, data):
        C = this.C
        client = xtdata.get_client()
        bresult = client.callFormula(C.request_id, func, _BSON_.BSON.encode(data))
        return _BSON_.BSON.decode(bresult)

    def create_view(this, title):
        C = this.C
        client = xtdata.get_client()
        data = {'viewtype': 0,'title':title, 'groupid':-1,'stockcode':C.market + C.stockcode,'period':C.period,'dividendtype':C.dividend_type}
        client.createView(C.request_id, _BSON_.BSON.encode(data))
        return


class BackTestResult:
    def __init__(self, request_id):
        self.request_id = request_id

    def get_backtest_index(self):
        import os, pandas as pd, uuid
        from .functions import get_backtest_index

        path = f'{os.getenv("TEMP")}/backtest_{uuid.uuid4()}'
        get_backtest_index(self.request_id, path)

        ret = pd.read_csv(f'{path}/backtestindex.csv', encoding = 'utf-8')
        import shutil
        shutil.rmtree(f'{path}')
        return ret

    def get_group_result(self, fields = []):
        import os, pandas as pd, uuid
        from .functions import get_group_result

        path = f'{os.getenv("TEMP")}/backtest_{uuid.uuid4()}'
        get_group_result(self.request_id, path, fields)
        if not fields:
            fields = ['order', 'deal', 'position']
        res = {}
        for f in fields:
            res[f] = pd.read_csv(f'{path}/{f}.csv', encoding = 'utf-8')
        import shutil
        shutil.rmtree(path)
        return res

class RealTimeResult:
    def __init__(self, request_id):
        self.request_id = request_id

