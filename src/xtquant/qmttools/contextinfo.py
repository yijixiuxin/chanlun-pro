#coding:utf-8

from . import functions as _FUNCS_

class ContextInfo:
    def __init__(this):
        #base
        this.request_id = ''
        this.quote_mode = '' #'realtime' 'history' 'all'
        this.trade_mode = '' #'simulation' 'trading' 'backtest'
        this.title = ''
        this.user_script = ''

        #quote
        this.stock_code = ''
        this.stockcode = ''
        this.market = ''
        this.period = ''
        this.start_time = ''
        this.end_time = ''
        this.dividend_type = ''
        this.start_time_num = None
        this.end_time_num = None

        #bar frame
        this.timelist = []
        this.barpos = -1
        this.lastrunbarpos = -1
        this.result = {}
        this.push_result = {}

        #backtest
        this.asset = 1000000.0  # 初始资金
        this.margin_ratio = 0.05  # 保证金比例
        this.slippage_type = 2  # 滑点类型
        this.slippage = 0.0  # 滑点值
        this.max_vol_rate = 0.0  # 最大成交比例
        this.comsisson_type = 0  # 手续费类型
        this.open_tax = 0.0  # 买入印花税
        this.close_tax = 0.0  # 卖出印花税
        this.min_commission = 0.0  # 最低佣金
        this.open_commission = 0.0  # 买入佣金
        this.close_commission = 0.0  # 平昨佣金
        this.close_today_commission = 0.0  # 平今佣金
        this.benchmark = '000300.SH' # 业绩基准

        this.do_back_test = None

        #reserved
        this.refresh_rate = None
        this.fund_name = None
        this.link_fund_name = None
        this.data_info_level = None
        this.time_tick_size = None
        this.subscribe_once = False
        return

    @property
    def start(this):
        return this.start_time

    @start.setter
    def start(this, value):
        this.start_time = value

    @property
    def end(this):
        return this.end_time

    @end.setter
    def end(this, value):
        this.end_time = value

    @property
    def capital(this):
        return this.asset

    @capital.setter
    def capital(this, value):
        this.asset = value

    ### qmt strategy frame ###

    def init(this):
        return

    def after_init(this):
        return

    def handlebar(this):
        return

    def on_backtest_finished(this):
        return

    def stop(this):
        return

    def account_callback(this, account_info):
        return

    def order_callback(this, order_info):
        return

    def deal_callback(this, deal_info):
        return

    def position_callback(this, position_info):
        return

    def orderError_callback(this, passorder_info, msg):
        return

    ### qmt functions - bar ###

    def is_last_bar(this):
        return this.barpos >= len(this.timelist) - 1

    def is_new_bar(this):
        return this.barpos > this.lastbarpos

    def get_bar_timetag(this, barpos = None):
        try:
            return this.timelist[barpos] if barpos is not None else this.timelist[this.barpos]
        except Exception as e:
            return None

    ### qmt functions - graph ###

    def paint(this, name, value, index = -1, drawstyle = 0, color = '', limit = ''):
        vp = {str(this.get_bar_timetag()): value}

        if name not in this.result:
            this.result[name] = {}
        this.result[name].update(vp)

        if name not in this.push_result:
            this.push_result[name] = {}
        this.push_result[name].update(vp)
        return

    ### qmt functions - quote ###

    def subscribe_quote(this, stock_code = '', period = '', dividend_type = '', result_type = '', callback = None):
        if not stock_code:
            stock_code = this.stock_code
        if not period or period == 'follow':
            period = this.period
        if not dividend_type or dividend_type == 'follow':
            dividend_type = this.dividend_type
        return _FUNCS_.subscribe_quote(stock_code, period, dividend_type, 0, result_type, callback)

    def subscribe_whole_quote(this, code_list, callback = None):
        return _FUNCS_.subscribe_whole_quote(code_list, callback)

    def unsubscribe_quote(this, subscribe_id):
        return _FUNCS_.unsubscribe_quote(subscribe_id)

    def get_market_data(
        this, fields = [], stock_code = [], start_time = '', end_time = ''
        , skip_paused = True, period = '', dividend_type = '', count = -1
    ):
        if not stock_code:
            stock_code = [this.stock_code]
        if not period or period == 'follow':
            period = this.period
        if not dividend_type or dividend_type == 'follow':
            dividend_type = this.dividend_type
        if period != 'tick' and count == -1 and len(fields) == 1:
            if not end_time or end_time == 'follow':
                if this.barpos >= 0:
                    end_time = _FUNCS_.timetag_to_datetime(this.get_bar_timetag(this.barpos))
                    count = -2
        if period == 'tick' and count == -1 and len(fields) == 1 and start_time == '' and end_time == '':
            count = -2

        return _FUNCS_.get_market_data(
            fields, stock_code, start_time, end_time
            , skip_paused, period, dividend_type, count
        )

    def get_market_data_ex(
        this, fields = [], stock_code = [], period = ''
        , start_time = '', end_time = '', count = -1
        , dividend_type = '', fill_data = True, subscribe = True
    ):
        if not stock_code:
            stock_code = [this.stock_code]
        if not period or period == 'follow':
            period = this.period
        if not dividend_type or dividend_type == 'follow':
            dividend_type = this.dividend_type

        if not this.subscribe_once and subscribe:
            this.subscribe_once = True
            if period != "tick":
                for stk in stock_code:
                    _FUNCS_.subscribe_quote(stk, period, dividend_type, -1)
            else:
                for stk in stock_code:
                    this.subscribe_whole_quote(stk)

        return _FUNCS_.get_market_data_ex(
            fields, stock_code, period
            , start_time, end_time, count
            , dividend_type, fill_data, subscribe
        )

    def get_full_tick(this, stock_code = []):
        if not stock_code:
            stock_code = [this.stock_code]
        return _FUNCS_.get_full_tick(stock_code)

    def get_divid_factors(this, stock_code = '', date = None):
        if not stock_code:
            stock_code = this.stock_code
        return _FUNCS_.get_divid_factors(stock_code, date)

    ### qmt functions - finance ###

    def get_financial_data(this, field_list, stock_list, start_date, end_date, report_type = 'announce_time'):
        raise 'not implemented, use get_raw_financial_data instead'
        return

    def get_raw_financial_data(this, field_list, stock_list, start_date, end_date, report_type = 'announce_time'):
        return _FUNCS_.get_raw_financial_data(field_list, stock_list, start_date, end_date, report_type)

    ### qmt functions - option ###

    def get_option_detail_data(this, optioncode):
        return _FUNCS_.get_option_detail_data(optioncode)

    def get_option_undl_data(this, undl_code_ref):
        return _FUNCS_.get_option_undl_data(undl_code_ref)

    def get_option_list(this, undl_code,dedate,opttype = "",isavailavle = False):
        return _FUNCS_.get_option_list(undl_code, dedate, opttype, isavailavle)

    def get_option_iv(this, opt_code):
        return _FUNCS_.get_opt_iv(opt_code, this.request_id)

    def bsm_price(this, optType, targetPrice, strikePrice, riskFree, sigma, days, dividend = 0):
        optionType = ""
        if(optType.upper() == "C"):
            optionType = "CALL"
        if(optType.upper() == "P"):
            optionType = "PUT"
        if(type(targetPrice) == list):
            result = []
            for price in targetPrice:
                bsmPrice= _FUNCS_.calc_bsm_price(optionType,strikePrice,float(price),riskFree,sigma,days,dividend, this.request_id)
                bsmPrice = round(bsmPrice,4)
                result.append(bsmPrice)
            return result
        else:
            bsmPrice = _FUNCS_.calc_bsm_price(optionType,strikePrice,targetPrice,riskFree,sigma,days,dividend, this.request_id)
            result = round(bsmPrice,4)
            return result

    def bsm_iv(this, optType, targetPrice, strikePrice, optionPrice, riskFree, days, dividend = 0):
        if(optType.upper() == "C"):
            optionType = "CALL"
        if(optType.upper() == "P"):
            optionType = "PUT"
        result = _FUNCS_.calc_bsm_iv(optionType, strikePrice, targetPrice, optionPrice, riskFree, days, dividend, this.request_id)
        result = round(result,4)
        return result

    ### qmt functions - static ###

    def get_instrument_detail(this, stock_code = '', iscomplete = False):
        if not stock_code:
            stock_code = this.stock_code
        return _FUNCS_.get_instrument_detail(stock_code, iscomplete)

    get_instrumentdetail = get_instrument_detail # compat

    def get_trading_dates(this, stock_code, start_date, end_date, count, period = '1d'):
        return _FUNCS_.get_trading_dates(stock_code, start_date, end_date, count, period)

    def get_stock_list_in_sector(this, sector_name):
        return _FUNCS_.get_stock_list_in_sector(sector_name)

    def passorder(
        this
        , opType, orderType, accountid
        , orderCode, prType, modelprice, volume
        , strategyName, quickTrade, userOrderId
    ):
        return _FUNCS_._passorder_impl(
            opType, orderType, accountid
            , orderCode, prType, modelprice, volume
            , strategyName, quickTrade, userOrderId
            , this.barpos, this.get_bar_timetag()
            , "passorder", ""
            , this.request_id
        )

    def set_auto_trade_callback(this, enable):
        return _FUNCS_._set_auto_trade_callback_impl(enable, this.request_id)

    def set_account(this, accountid):
        return _FUNCS_.set_account(accountid, this.request_id)

    def get_his_st_data(this, stock_code):
        return _FUNCS_.get_his_st_data(stock_code)

    ### private ###

    def trade_callback(this, type, result, error):
        class DetailData(object):
            def __init__(self, _obj):
                if _obj:
                    self.__dict__.update(_obj)

        if type == 'accountcallback':
            this.account_callback(DetailData(result))
        elif type == 'ordercallback':
            this.order_callback(DetailData(result))
        elif type == 'dealcallback':
            this.deal_callback(DetailData(result))
        elif type == 'positioncallback':
            this.position_callback(DetailData(result))
        elif type == 'ordererrorcallback':
            this.orderError_callback(DetailData(result.get('passorderArg')), result.get('strMsg'))

        return

    def register_callback(this, reqid):
        _FUNCS_.register_external_resp_callback(reqid, this.trade_callback)
        return

    def get_callback_cache(this, type):
        return _FUNCS_._get_callback_cache_impl(type, this.request_id)

    def get_ipo_info(this, start_time = '', end_time = ''):
        return _FUNCS_.get_ipo_info(start_time, end_time)

    def get_backtest_index(this, path):
        _FUNCS_.get_backtest_index(this.request_id, path)

    def get_group_result(this, path, fields):
        _FUNCS_.get_group_result(this.request_id, path, fields)

    def is_suspended_stock(this, stock_code, type):
        if this.barpos > len(this.timelist):
            return False

        if type == 1 or len(this.timelist) == 0:
            inst = this.get_instrument_detail(stock_code)
            return inst.get('InstrumentStatus', 0) >= 1

        return _FUNCS_.is_suspended_stock(stock_code, this.period, this.timelist[this.barpos])
