#coding:utf-8

import datetime as _DT_

from xtquant import xtdata
from xtquant import xtbson as _BSON_

def datetime_to_timetag(timelabel, format = ''):
    '''
    timelabel: str '20221231' '20221231235959'
    format: str '%Y%m%d' '%Y%m%d%H%M%S'
    return: int 1672502399000
    '''
    if not format:
        format = '%Y%m%d' if len(timelabel) == 8 else '%Y%m%d%H%M%S'
    return _DT_.datetime.strptime(timelabel, format).timestamp() * 1000

def timetag_to_datetime(timetag, format = ''):
    '''
    timetag: int 1672502399000
    format: str '%Y%m%d' '%Y%m%d%H%M%S'
    return: str '20221231' '20221231235959'
    '''
    if not format:
        format = '%Y%m%d' if timetag % 86400000 == 57600000 else '%Y%m%d%H%M%S'
    return _DT_.datetime.fromtimestamp(timetag / 1000).strftime(format)

def fetch_ContextInfo():
    import sys
    frame = sys._getframe()
    while (frame):
        loc = list(frame.f_locals.values())
        for val in loc:
            if type(val).__name__ == "ContextInfo":
                return val
        frame = frame.f_back
    return None

def subscribe_quote(stock_code, period, dividend_type, count = 0, result_type = '', callback = None):
    return xtdata.subscribe_quote(stock_code, period, '', '', count, callback)

def subscribe_whole_quote(code_list, callback = None):
    return xtdata.subscribe_whole_quote(code_list, callback)

def unsubscribe_quote(subscribe_id):
    return xtdata.unsubscribe_quote(subscribe_id)

def get_market_data(
    fields = [], stock_code = [], start_time = '', end_time = ''
    , skip_paused = True, period = '', dividend_type = '', count = -1
):
    res = {}
    if period == 'tick':
        refixed = False
        if count == -2:
            refixed = True
            count = 1
        if 'quoter' not in fields:
            return xtdata.get_market_data_ori(
                    field_list=fields, stock_list=stock_code, period=period
                    , start_time=start_time, end_time=end_time, count=count
                    , dividend_type=dividend_type, fill_data=skip_paused
                )

        fields = []
        data = xtdata.get_market_data_ori(
            field_list=fields, stock_list=stock_code, period=period
            , start_time=start_time, end_time=end_time, count=count
            , dividend_type=dividend_type, fill_data=skip_paused
        )
        fields = ['quoter']

        import pandas as pd

        stime_fmt = '%Y%m%d' if period == '1d' else '%Y%m%d%H%M%S'
        for stock in data:
            pd_data = pd.DataFrame(data[stock])
            pd_data['stime'] = [timetag_to_datetime(t, stime_fmt) for t in pd_data['time']]
            pd_data.index = pd.to_datetime((pd_data['time'] + 28800000) * 1000000)
            ans = {}
            for j, timetag in enumerate(pd_data['time']):
                d_map = {}
                for key in pd_data:
                    d_map[key] = pd_data[key][j]
                ans[str(pd_data.index[j])] = {}
                ans[str(pd_data.index[j])]['quoter'] = d_map
            res[stock] = ans

        oriData = res
            # if not pd_data.empty:
            #     if count > 0:
            #         return list(pd_data.T.to_dict().values())
            #     return pd_data.iloc[-1].to_dict()
            # return {}
        if refixed:
            count = -2
    else:
        refixed = False
        if count == -2:
            refixed = True
            count = 1
        index, data = xtdata.get_market_data_ori(
            field_list=fields, stock_list=stock_code, period=period
            , start_time=start_time, end_time=end_time, count=count
            , dividend_type=dividend_type, fill_data=skip_paused
        )
        if refixed:
            end_time = ''
            count = -1
        for i, stock in enumerate(index[0]):
            ans = {}
            for j, timetag in enumerate(index[1]):
                d_map = {}
                for key in data:
                    d_map[key] = data[key][i][j]
                ans[timetag] = d_map
            res[stock] = ans
        oriData = res

    resultDict = {}
    for code in oriData:
        for timenode in oriData[code]:
            values = []
            for field in fields:
                values.append(oriData[code][timenode][field])
            key = code + timenode
            resultDict[key] = values

    if len(fields) == 1 and len(stock_code) <= 1 and (
            (start_time == '' and end_time == '') or start_time == end_time) and (count == -1 or count == -2):
        # if resultDict:
            # keys = list(resultDict.keys())
            # if resultDict[keys[-1]]:
            #     return resultDict[keys[-1]]
        for key in resultDict:
            return resultDict[key][0]
        return -1
    import numpy as np
    import pandas as pd
    if len(stock_code) <= 1 and start_time == '' and end_time == '' and (count == -1 or count == -2):
        for key in resultDict:
            result = pd.Series(resultDict[key], index=fields)
            return result
    if len(stock_code) > 1 and start_time == '' and end_time == '' and (count == -1 or count == -2):
        values = []
        for code in stock_code:
            if code in oriData:
                if not oriData[code]:
                    values.append([np.nan])
                for timenode in oriData[code]:
                    key = code + timenode
                    values.append(resultDict[key])
            else:
                values.append([np.nan])
        result = pd.DataFrame(values, index=stock_code, columns=fields)
        return result
    if len(stock_code) <= 1 and ((start_time != '' or end_time != '') or count >= 0):
        values = []
        times = []
        for code in oriData:
            for timenode in oriData[code]:
                key = code + timenode
                times.append(timenode)
                values.append(resultDict[key])
        result = pd.DataFrame(values, index=times, columns=fields)
        return result
    if len(stock_code) > 1 and ((start_time != '' or end_time != '') or count >= 0):
        values = {}
        for code in stock_code:
            times = []
            value = []
            if code in oriData:
                for timenode in oriData[code]:
                    key = code + timenode
                    times.append(timenode)
                    value.append(resultDict[key])
            values[code] = pd.DataFrame(value, index=times, columns=fields)
        try:
            result = pd.Panel(values)
            return result
        except:
            return oriData
    return

def get_market_data_ex(
    fields = [], stock_code = [], period = ''
    , start_time = '', end_time = '', count = -1
    , dividend_type = '', fill_data = True, subscribe = True
):
    res = xtdata.get_market_data_ex(
        field_list = fields, stock_list = stock_code, period = period
        , start_time = start_time, end_time = end_time, count = count
        , dividend_type = dividend_type, fill_data = fill_data
    )
    for stock in res:
        res[stock].index.name = "stime"
    return res

def get_full_tick(stock_code):
    return xtdata.get_full_tick(stock_code)

def get_divid_factors(stock_code, date = None):
    client = xtdata.get_client()
    if date:
        data = client.get_divid_factors(stock_code, date, date)
    else:
        data = client.get_divid_factors(stock_code, '19700101', '20380119')

    res = {}
    for value in data.values():
        res[value['time']] = list(value.values())[1:]
    return res

def download_history_data(stockcode, period, startTime, endTime):
    return xtdata.download_history_data(stockcode, period, startTime, endTime)

def get_raw_financial_data(field_list, stock_list, start_date, end_date, report_type = 'announce_time'):
    client = xtdata.get_client()
    data = client.get_financial_data(stock_list, field_list, start_date, end_date, report_type)

    import time
    res = {}
    for stock in data:
        stock_data = data[stock]
        res[stock] = {}

        for field in field_list:
            fs = field.split('.')
            table_data = stock_data.get(fs[0])

            if not table_data:
                continue

            ans = {}
            for row_data in table_data:
                if row_data.get(report_type, None) == None:
                    continue
                date = time.strftime('%Y%m%d', time.localtime(row_data[report_type] / 1000))
                if start_date == '' or start_date <= date:
                    if end_date == '' or date <= end_date:
                        ans[int(row_data[report_type])] = row_data[fs[1]]
            res[stock][field] = ans
    return res

#def download_financial_data(stock_list, table_list): #暂不提供
#    return xtdata.download_financial_data(stock_list, table_list)

def get_instrument_detail(stock_code, iscomplete = False):
    return xtdata.get_instrument_detail(stock_code, iscomplete)

#def get_instrument_type(stock_code): #暂不提供
#    return xtdata.get_instrument_type(stock_code)

def get_trading_dates(stock_code, start_date, end_date, count = -1, period = '1d'):
    if period != '1d':
        return []
    market = stock_code.split('.')[0]
    trade_dates = xtdata.get_trading_dates(market, start_date, end_date)
    if count == -1:
        return trade_dates
    if count > 0:
        return trade_dates[-count:]
    return []

def get_stock_list_in_sector(sector_name):
    return xtdata.get_stock_list_in_sector(sector_name)

def download_sector_data():
    return xtdata.download_sector_data()

download_sector_weight = download_sector_data #compat

def get_his_st_data(stock_code):
    return xtdata.get_his_st_data(stock_code)


def _passorder_impl(
    optype, ordertype, accountid
    , ordercode, prtype, modelprice, volume
    , strategyName, quickTrade, userOrderId
    , barpos, bartime, func, algoName
    , requestid
):
    data = {}

    data['optype'] = optype
    data['ordertype'] = ordertype
    data['accountid'] = accountid
    data['ordercode'] = ordercode
    data['prtype'] = prtype
    data['modelprice'] = modelprice
    data['volume'] = volume
    data['strategyname'] = strategyName
    data['remark'] = userOrderId
    data['quicktrade'] = quickTrade
    data['func'] = func
    data['algoname'] = algoName
    data['barpos'] = barpos
    data['bartime'] = bartime

    client = xtdata.get_client()
    client.callFormula(requestid, 'passorder', _BSON_.BSON.encode(data))
    return


def passorder(
    opType, orderType, accountid
    , orderCode, prType, modelprice, volume
    , strategyName, quickTrade, userOrderId
    , C
):
    return C.passorder(
        opType, orderType, accountid
        , orderCode, prType, modelprice, volume
        , strategyName, quickTrade, userOrderId
    )


def get_trade_detail_data(accountid, accounttype, datatype, strategyname = ''):
    data = {}

    C = fetch_ContextInfo()
    if C is None:
        raise Exception("contextinfo could not be found in the stack")
    request_id = C.request_id

    data['accountid'] = accountid
    data['accounttype'] = accounttype
    data['datatype'] = datatype
    data['strategyname'] = strategyname

    client = xtdata.get_client()
    result_bson = client.callFormula(request_id, 'gettradedetail', _BSON_.BSON.encode(data))
    result = _BSON_.BSON.decode(result_bson)

    class DetailData(object):
        def __init__(self, _obj):
            if _obj:
                self.__dict__.update(_obj)

    out = []
    if not result:
        return out

    for item in result.get('result'):
        out.append(DetailData(item))
    return out

def register_external_resp_callback(reqid, callback):
    client = xtdata.get_client()

    status = [False, 0, 1, '']

    def on_callback(type, data, error):
        try:
            result = _BSON_.BSON.decode(data)
            callback(type, result, error)
            return True
        except:
            status[0] = True
            status[3] = 'exception'
            return True

    client.register_external_resp_callback(reqid, on_callback)

def _set_auto_trade_callback_impl(enable, requestid):
    data = {}
    data['enable'] = enable

    client = xtdata.get_client()
    client.callFormula(requestid, 'setautotradecallback', _BSON_.BSON.encode(data))
    return

def set_auto_trade_callback(C,enable):
    return C.set_auto_trade_callback(enable)

def set_account(accountid, requestid):
    data = {}
    data['accountid'] = accountid

    client = xtdata.get_client()
    client.callFormula(requestid, 'setaccount', _BSON_.BSON.encode(data))
    return

def _get_callback_cache_impl(type, requestid):
    data = {}

    data['type'] = type

    client = xtdata.get_client()
    result_bson = client.callFormula(requestid, 'getcallbackcache', _BSON_.BSON.encode(data))
    return _BSON_.BSON.decode(result_bson)

def get_account_callback_cache(data, C):
    data = C.get_callback_cache("account").get('')
    return

def get_order_callback_cache(data, C):
    data = C.get_callback_cache("order")
    return

def get_deal_callback_cache(data, C):
    data = C.get_callback_cache("deal")
    return

def get_position_callback_cache(data, C):
    data = C.get_callback_cache("position")
    return

def get_ordererror_callback_cache(data, C):
    data = C.get_callback_cache("ordererror")
    return

def get_option_detail_data(stock_code):
    return xtdata.get_option_detail_data(stock_code)

def get_option_undl_data(undl_code_ref):
    return xtdata.get_option_undl_data(undl_code_ref)

def get_option_list(undl_code,dedate,opttype = "",isavailavle = False):
    return xtdata.get_option_list(undl_code, dedate, opttype, isavailavle)

def get_opt_iv(opt_code, requestid):
    data = {}
    data['code'] = opt_code

    client = xtdata.get_client()
    result_bson = client.callFormula(requestid, 'getoptiv', _BSON_.BSON.encode(data))
    result = _BSON_.BSON.decode(result_bson)

    out = result.get('result', 0)
    return out

def calc_bsm_price(optionType,strikePrice, targetPrice, riskFree, sigma, days, dividend, requestid):
    data = {}
    data['optiontype'] = optionType
    data['strikeprice'] = strikePrice
    data['targetprice'] = targetPrice
    data['riskfree'] = riskFree
    data['sigma'] = sigma
    data['days'] = days
    data['dividend'] = dividend

    client = xtdata.get_client()
    result_bson = client.callFormula(requestid, 'calcbsmprice', _BSON_.BSON.encode(data))
    result = _BSON_.BSON.decode(result_bson)

    out = result.get('result', 0)
    return out

def calc_bsm_iv(optionType, strikePrice, targetPrice, optionPrice, riskFree, days, dividend, requestid):
    data = {}
    data['optiontype'] = optionType
    data['strikeprice'] = strikePrice
    data['targetprice'] = targetPrice
    data['optionprice'] = optionPrice
    data['riskfree'] = riskFree
    data['days'] = days
    data['dividend'] = dividend

    client = xtdata.get_client()
    result_bson = client.callFormula(requestid, 'calcbsmiv', _BSON_.BSON.encode(data))
    result = _BSON_.BSON.decode(result_bson)

    out = result.get('result', 0)
    return out

def get_ipo_info(start_time, end_time):
    return xtdata.get_ipo_info(start_time, end_time)

def get_backtest_index(requestid, path):
    import os
    path = os.path.abspath(path)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok = True)

    data = {'savePath': path}
    client = xtdata.get_client()
    bresult = client.callFormula(requestid, 'backtestresult', _BSON_.BSON.encode(data))
    return _BSON_.BSON.decode(bresult)

def get_group_result(requestid, path, fields):
    import os
    path = os.path.abspath(path)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok = True)

    data = {'savePath': path, 'fields': fields}
    client = xtdata.get_client()
    bresult = client.callFormula(requestid, 'groupresult', _BSON_.BSON.encode(data))
    return _BSON_.BSON.decode(bresult)

def subscribe_formula(formula_name, stock_code, period, start_time = "", end_time = "", count=-1, dividend_type = "none", extend_params = {}, callback = None):
    return xtdata.subscribe_formula(formula_name, stock_code, period, start_time, end_time, count, dividend_type, extend_params, callback)

def call_formula_batch(formula_names, stock_codes, period, start_time = "", end_time = "", count=-1, dividend_type = "none", extend_params = []):
    import copy
    params = []
    for name in formula_names:
        for stock in stock_codes:
            param = {
                'formulaname': name, 'stockcode': stock, 'period': period
                , 'starttime': start_time, 'endtime': end_time, 'count': count
                , 'dividendtype': dividend_type, 'extendparam': {}
                , 'create': True, 'datademand': 0
            }

            if extend_params:
                for extend in extend_params:
                    param['extendparam'] = extend
                    params.append(copy.deepcopy(param))
            else:
                params.append(param)

    client = xtdata.get_client()
    result = client.commonControl(
        'callformulabatch'
        , _BSON_.BSON.encode(
            {"params": params}
        )
    )
    result = _BSON_.BSON.decode(result)
    return result.get("result", {})

def is_suspended_stock(stock_code, period, timetag):
    client = xtdata.get_client()

    result = client.commonControl(
        'issuspendedstock'
        , _BSON_.BSON.encode({
            "stockcode": stock_code
            , "period": period
            , "timetag": timetag
        })
    )
    result = _BSON_.BSON.decode(result)
    return result.get('result', True)
