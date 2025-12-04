#coding:utf-8

import os as _OS_
import time as _TIME_
import traceback as _TRACEBACK_

from . import xtbson as _BSON_
from .metatable import *
from .metatable import get_tabular_data as _get_tabular_data


__all__ = [
    'subscribe_quote'
    , 'subscribe_whole_quote'
    , 'unsubscribe_quote'
    , 'run'
    , 'get_market_data'
    , 'get_local_data'
    , 'get_full_tick'
    , 'get_divid_factors'
    , 'get_l2_quote'
    , 'get_l2_order'
    , 'get_l2_transaction'
    , 'download_history_data'
    , 'get_financial_data'
    , 'download_financial_data'
    , 'get_instrument_detail'
    , 'get_instrument_type'
    , 'get_trading_dates'
    , 'get_sector_list'
    , 'get_stock_list_in_sector'
    , 'download_sector_data'
    , 'add_sector'
    , 'remove_sector'
    , 'get_index_weight'
    , 'download_index_weight'
    , 'get_holidays'
    , 'get_trading_calendar'
    , 'get_etf_info'
    , 'download_etf_info'
    , 'get_main_contract'
    , 'download_history_contracts'
    , 'download_cb_data'
    , 'get_cb_info'
    , 'create_sector_folder'
    , 'create_sector'
    , 'remove_stock_from_sector'
    , 'reset_sector'
    , 'get_period_list'
    , 'download_his_st_data'
    , 'get_tabular_data'
]

def try_except(func):
    import sys
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            exc_type, exc_instance, exc_traceback = sys.exc_info()
            formatted_traceback = ''.join(_TRACEBACK_.format_tb(exc_traceback))
            message = '\n{0} raise {1}:{2}'.format(
                formatted_traceback,
                exc_type.__name__,
                exc_instance
            )
            # raise exc_type(message)
            # print(message)
            return None

    return wrapper


### config

debug_mode = 0

default_data_dir = '../userdata_mini/datadir'
__data_dir_from_server = default_data_dir
data_dir = None

enable_hello = True


### connection

__client = None
__client_last_spec = ('', None)

__hk_broke_info = {}
__download_version = None


def connect(ip = '', port = None, remember_if_success = True):
    global __client
    global __data_dir_from_server
    global __download_version

    if __client:
        if __client.is_connected():
            return __client

        __client.shutdown()
        __client = None
        __data_dir_from_server = default_data_dir

    from . import xtconn

    if not port and (ip != '' and ip != '127.0.0.1' and ip != 'localhost'):
        raise Exception("远程地址不支持仅输入IP")

    if isinstance(port, int):
        if ip:
            server_list = [f'{ip}:{port}']
        else:
            server_list = [f'127.0.0.1:{port}', f'localhost:{port}']
    else:
        server_list = xtconn.scan_available_server_addr()

        default_addr = '127.0.0.1:58610'
        if not default_addr in server_list:
            server_list.append(default_addr)

    start_port = 0
    end_port = 65535

    if isinstance(port, tuple):
        start_port = port[0]
        end_port = port[1]

    if start_port > end_port:
        start_port, end_port = end_port, start_port

    __client = xtconn.connect_any(server_list, start_port, end_port)

    if not __client or not __client.is_connected():
        raise Exception("无法连接xtquant服务，请检查QMT-投研版或QMT-极简版是否开启")

    if remember_if_success:
        global __client_last_spec
        __client_last_spec = (ip, port)

    __data_dir_from_server = __client.get_data_dir()
    if __data_dir_from_server == "":
        __data_dir_from_server = _OS_.path.join(__client.get_app_dir(), default_data_dir)

    __data_dir_from_server = _OS_.path.abspath(__data_dir_from_server)

    try:
        __download_version = _BSON_call_common(
            __client.commonControl, 'getapiversion', {}
        ).get('downloadversion', None)
    except:
        pass

    hello()
    return __client


def reconnect(ip = '', port = None, remember_if_success = True):
    global __client
    global __data_dir_from_server

    if __client:
        __client.shutdown()
        __client = None
        __data_dir_from_server = default_data_dir

    return connect(ip, port, remember_if_success)


def disconnect():
    global __client
    global __data_dir_from_server

    if __client:
        __client.shutdown()
        __client = None
        __data_dir_from_server = default_data_dir
    return


def get_client():
    global __client

    if not __client or not __client.is_connected():
        global __client_last_spec

        ip, port = __client_last_spec
        __client = connect(ip, port, False)

    return __client


def hello():
    global enable_hello
    if not enable_hello:
        return

    server_info = None
    peer_addr = None
    __data_dir_from_server = None

    client = get_client()
    try:
        server_info = _BSON_.BSON.decode(client.get_server_tag())
        peer_addr = client.get_peer_addr()
        __data_dir_from_server = client.get_data_dir()
    except Exception as e:
        pass

    import datetime as dt
    cur = dt.datetime.now().strftime("%Y-%m-%d %H:%S:%M")
    print(
f'''***** xtdata连接成功 {cur}*****
服务信息: {server_info}
服务地址: {peer_addr}
数据路径: {__data_dir_from_server}
设置xtdata.enable_hello = False可隐藏此消息
'''
    )
    return


def get_data_dir():
    '''
    如果更改过`xtdata.data_dir`变量的值，优先返回变量设置的值
    没有设置过，返回服务的数据路径
    注意----设置`xtdata.data_dir`的值可以强制指定读取本地数据的位置，谨慎修改
    '''
    cl = get_client()
    global data_dir
    global __data_dir_from_server
    return data_dir if data_dir != None else __data_dir_from_server


__meta_field_list = {}

def get_field_list(metaid):
    global __meta_field_list

    if not __meta_field_list:
        x = open(_OS_.path.join(_OS_.path.dirname(__file__), 'config', 'metaInfo.json'), 'r', encoding="utf-8").read()
        metainfo = eval(x)

        for meta in metainfo:
            filed_dict = {}
            metadetail = metainfo.get(str(meta), {})
            filed = metadetail.get('fields', {})
            for key in filed:
                filed_dict[key] = filed[key].get('desc', key)

            filed_dict['G'] = 'time'
            filed_dict['S'] = 'stock'
            __meta_field_list[meta] = filed_dict

    return __meta_field_list.get(str(metaid), {})


### utils

def create_array(shape, dtype_tuple, capsule, size):
    import numpy as np
    import ctypes

    ctypes.pythonapi.PyCapsule_GetPointer.restype = ctypes.POINTER(ctypes.c_char)
    ctypes.pythonapi.PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    buff = ctypes.pythonapi.PyCapsule_GetPointer(capsule, None)
    base_type = size * buff._type_

    for dim in shape[::-1]:
        base_type = dim * base_type
    p_arr_type = ctypes.POINTER(base_type)
    obj = ctypes.cast(buff, p_arr_type).contents
    obj._base = capsule
    return np.ndarray(shape = shape, dtype = np.dtype(dtype_tuple), buffer = obj)

from .xtdatacenter import register_create_nparray as __register_create_nparray
__register_create_nparray(create_array)


def _BSON_call_common(interface, func, param):
    return _BSON_.BSON.decode(interface(func, _BSON_.BSON.encode(param)))


### function

def get_stock_list_in_sector(sector_name, real_timetag = -1):
    '''
    获取板块成份股，支持客户端左侧板块列表中任意的板块，包括自定义板块
    :param sector_name: (str)板块名称
    :real_timetag: 时间：1512748800000 或 ‘20171209’，可缺省，缺省为获取最新成份，不缺省时获取对应时间的历史成份
    :return: list
    '''
    client = get_client()
    for illegalstr in ['\\', '/', ':', '*', '?', '"', '<', '>', '|']:
        sector_name = sector_name.replace(illegalstr, '')

    if isinstance(real_timetag, str):
        if real_timetag != '':
            real_timetag = int(_TIME_.mktime(_TIME_.strptime(real_timetag, '%Y%m%d')) * 1000)
        else:
            real_timetag = -1

    return client.get_stock_list_in_sector(sector_name, real_timetag)


def get_index_weight(index_code):
    '''
    获取某只股票在某指数中的绝对权重
    :param index_code: (str)指数名称
    :return: dict
    '''
    client = get_client()
    return client.get_weight_in_index(index_code)


def get_financial_data(stock_list, table_list=[], start_time='', end_time='', report_type='report_time'):
    '''
     获取财务数据
    :param stock_list: (list)合约代码列表
    :param table_list: (list)报表名称列表
    :param start_time: (str)起始时间
    :param end_time: (str)结束时间
    :param report_type: (str) 时段筛选方式 'announce_time' / 'report_time'
    :return:
        field: list[str]
        date: list[int]
        stock: list[str]
        value: list[list[float]]
    '''
    client = get_client()
    all_table = {
        'Balance' : 'ASHAREBALANCESHEET'
        , 'Income' : 'ASHAREINCOME'
        , 'CashFlow' : 'ASHARECASHFLOW'
        , 'Capital' : 'CAPITALSTRUCTURE'
        , 'HolderNum' : 'SHAREHOLDER'
        , 'Top10Holder' : 'TOP10HOLDER'
        , 'Top10FlowHolder' : 'TOP10FLOWHOLDER'
        , 'PershareIndex' : 'PERSHAREINDEX'
    }

    if not table_list:
        table_list = list(all_table.keys())

    all_table_upper = {table.upper() : all_table[table] for table in all_table}
    req_list = []
    names = {}
    for table in table_list:
        req_table = all_table_upper.get(table.upper(), table)
        req_list.append(req_table)
        names[req_table] = table

    data = {}
    sl_len = 20
    stock_list2 = [stock_list[i : i + sl_len] for i in range(0, len(stock_list), sl_len)]
    for sl in stock_list2:
        data2 = client.get_financial_data(sl, req_list, start_time, end_time, report_type)
        for s in data2:
            data[s] = data2[s]

    import math
    def conv_date(data, key, key2):
        if key in data:
            tmp_data = data[key]
            if math.isnan(tmp_data):
                if key2 not in data or math.isnan(data[key2]):
                    data[key] = ''
                else:
                    tmp_data = data[key2]
            data[key] = _TIME_.strftime('%Y%m%d', _TIME_.localtime(tmp_data / 1000))
        return

    result = {}
    import pandas as pd
    for stock in data:
        stock_data = data[stock]
        result[stock] = {}
        for table in stock_data:
            table_data = stock_data[table]
            for row_data in table_data:
                conv_date(row_data, 'm_anntime', 'm_timetag')
                conv_date(row_data, 'm_timetag', '')
                conv_date(row_data, 'declareDate', '')
                conv_date(row_data, 'endDate', '')
            result[stock][names.get(table, table)] = pd.DataFrame(table_data)
    return result


def get_financial_data_ori(stock_list, table_list=[], start_time='', end_time='', report_type='report_time'):
    client = get_client()
    all_table = {
        'Balance' : 'ASHAREBALANCESHEET'
        , 'Income' : 'ASHAREINCOME'
        , 'CashFlow' : 'ASHARECASHFLOW'
        , 'Capital' : 'CAPITALSTRUCTURE'
        , 'HolderNum' : 'SHAREHOLDER'
        , 'Top10Holder' : 'TOP10HOLDER'
        , 'Top10FlowHolder' : 'TOP10FLOWHOLDER'
        , 'PershareIndex' : 'PERSHAREINDEX'
    }

    if not table_list:
        table_list = list(all_table.keys())

    all_table_upper = {table.upper() : all_table[table] for table in all_table}
    req_list = []
    names = {}
    for table in table_list:
        req_table = all_table_upper.get(table.upper(), table)
        req_list.append(req_table)
        names[req_table] = table

    data = {}
    sl_len = 20
    stock_list2 = [stock_list[i : i + sl_len] for i in range(0, len(stock_list), sl_len)]
    for sl in stock_list2:
        data2 = client.get_financial_data(sl, req_list, start_time, end_time, report_type)
        for s in data2:
            data[s] = data2[s]
    return data


def get_market_data_ori(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True, enable_read_from_server = True
    , data_dir = None
):
    import datetime as dt
    client = get_client()
    enable_read_from_local = period in {'1m', '5m', '15m', '30m', '60m', '1h', '1d', 'tick', '1w', '1mon', '1q', '1hy', '1y'}
    global debug_mode

    if data_dir == None:
        data_dir = get_data_dir()

    if isinstance(start_time, dt.datetime):
        start_time = int(start_time.timestamp() * 1000)
    if isinstance(end_time, dt.datetime):
        end_time = int(end_time.timestamp() * 1000)

    return client.get_market_data3(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, 'v2', enable_read_from_local, enable_read_from_server, debug_mode, data_dir)


def get_market_data(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True
):
    '''
    获取历史行情数据
    :param field_list: 行情数据字段列表，[]为全部字段
        K线可选字段：
            "time"                #时间戳
            "open"                #开盘价
            "high"                #最高价
            "low"                 #最低价
            "close"               #收盘价
            "volume"              #成交量
            "amount"              #成交额
            "settle"              #今结算
            "openInterest"        #持仓量
        分笔可选字段：
            "time"                #时间戳
            "lastPrice"           #最新价
            "open"                #开盘价
            "high"                #最高价
            "low"                 #最低价
            "lastClose"           #前收盘价
            "amount"              #成交总额
            "volume"              #成交总量
            "pvolume"             #原始成交总量
            "stockStatus"         #证券状态
            "openInt"             #持仓量
            "lastSettlementPrice" #前结算
            "askPrice1", "askPrice2", "askPrice3", "askPrice4", "askPrice5" #卖一价~卖五价
            "bidPrice1", "bidPrice2", "bidPrice3", "bidPrice4", "bidPrice5" #买一价~买五价
            "askVol1", "askVol2", "askVol3", "askVol4", "askVol5"           #卖一量~卖五量
            "bidVol1", "bidVol2", "bidVol3", "bidVol4", "bidVol5"           #买一量~买五量
    :param stock_list: 股票代码 "000001.SZ"
    :param period: 周期 分笔"tick" 分钟线"1m"/"5m"/"15m" 日线"1d"
        Level2行情快照"l2quote" Level2行情快照补充"l2quoteaux" Level2逐笔委托"l2order" Level2逐笔成交"l2transaction" Level2大单统计"l2transactioncount" Level2委买委卖队列"l2orderqueue"
        Level1逐笔成交统计一分钟“transactioncount1m” Level1逐笔成交统计日线“transactioncount1d”
        期货仓单“warehousereceipt” 期货席位“futureholderrank” 互动问答“interactiveqa”
    :param start_time: 起始时间 "20200101" "20200101093000"
    :param end_time: 结束时间 "20201231" "20201231150000"
    :param count: 数量 -1全部/n: 从结束时间向前数n个
    :param dividend_type: 除权类型"none" "front" "back" "front_ratio" "back_ratio"
    :param fill_data: 对齐时间戳时是否填充数据，仅对K线有效，分笔周期不对齐时间戳
        为True时，以缺失数据的前一条数据填充
            open、high、low、close 为前一条数据的close
            amount、volume为0
            settle、openInterest 和前一条数据相同
        为False时，缺失数据所有字段填NaN
    :return: 数据集，分笔数据和K线数据格式不同
        period为'tick'时：{stock1 : value1, stock2 : value2, ...}
            stock1, stock2, ... : 合约代码
            value1, value2, ... : np.ndarray 数据列表，按time增序排列
        period为其他K线周期时：{field1 : value1, field2 : value2, ...}
            field1, field2, ... : 数据字段
            value1, value2, ... : pd.DataFrame 字段对应的数据，各字段维度相同，index为stock_list，columns为time_list
    '''
    if period in {'1m', '5m', '15m', '30m', '60m', '1h', '1d', '1w', '1mon', '1q', '1hy', '1y'}:
        import pandas as pd
        index, data = get_market_data_ori(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data)

        result = {}
        for field in data:
            result[field] = pd.DataFrame(data[field], index = index[0], columns = index[1])
        return result

    return get_market_data_ori(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data)


def get_market_data_ex_ori(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True, enable_read_from_server = True
    , data_dir = None
):
    import datetime as dt
    client = get_client()
    enable_read_from_local = period in {'1m', '5m', '15m', '30m', '60m', '1h', '1d', 'tick', '1w', '1mon', '1q', '1hy', '1y'}
    global debug_mode

    if data_dir == None:
        data_dir = get_data_dir()

    if isinstance(start_time, dt.datetime):
        start_time = int(start_time.timestamp() * 1000)
    if isinstance(end_time, dt.datetime):
        end_time = int(end_time.timestamp() * 1000)

    return client.get_market_data3(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, 'v3', enable_read_from_local, enable_read_from_server, debug_mode, data_dir)


def get_market_data_ex(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True
):
    if period == 'hkbrokerqueue' or period == 'hkbrokerqueue2' or period == (1820, 0):
        showbrokename = period == 'hkbrokerqueue2'
        return get_broker_queue_data(stock_list, start_time, end_time, count, showbrokename)

    spec_period, meta_id, period_num = _validate_period(period)
    if meta_id > 0:
        return _get_market_data_ex_tuple_period(field_list, stock_list, (meta_id, period_num), start_time, end_time, count, dividend_type, fill_data)

    if spec_period in {'1m', '5m', '15m', '30m', '60m', '1h', '1d', '1w', '1mon', '1q', '1hy', '1y'}:
        return _get_market_data_ex_ori_221207(field_list, stock_list, spec_period, start_time, end_time, count, dividend_type, fill_data)

    import pandas as pd
    result = {}

    ifield = 'time'
    query_field_list = field_list if (not field_list) or (ifield in field_list) else [ifield] + field_list
    ori_data = get_market_data_ex_ori(query_field_list, stock_list, spec_period, start_time, end_time, count, dividend_type, fill_data)

    if not ori_data:
        return result

    fl = field_list
    stime_fmt = '%Y%m%d' if spec_period == '1d' else '%Y%m%d%H%M%S'
    if fl:
        fl2 = fl if ifield in fl else [ifield] + fl
        for s in ori_data:
            sdata = pd.DataFrame(ori_data[s], columns = fl2)
            sdata2 = sdata[fl]
            sdata2.index = [timetag_to_datetime(t, stime_fmt) for t in sdata[ifield]]
            result[s] = sdata2
    else:
        needconvert, metaid  = _needconvert_period(spec_period)
        if needconvert:
            convert_field_list = get_field_list(metaid)

            for s in ori_data:
                odata = ori_data[s]
                if convert_field_list:
                    convert_data_list = []
                    for data in odata:
                        convert_data = _convert_component_info(data, convert_field_list)
                        convert_data_list.append(convert_data)
                    odata = convert_data_list

                sdata = pd.DataFrame(odata)
                if ifield in sdata.columns:
                    sdata.index = [timetag_to_datetime(t, stime_fmt) for t in sdata[ifield]]
                result[s] = sdata
        else:
            for s in ori_data:
                sdata = pd.DataFrame(ori_data[s])
                sdata.index = [timetag_to_datetime(t, stime_fmt) for t in sdata[ifield]]
                result[s] = sdata

    return result


def _get_market_data_ex_ori_221207(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True, enable_read_from_server = True
    , data_dir = None
):
    import numpy as np
    import pandas as pd
    import datetime as dt

    client = get_client()
    enable_read_from_local = period in {'1m', '5m', '15m', '30m', '60m', '1h', '1d', 'tick', '1w', '1mon', '1q', '1hy', '1y'}
    global debug_mode

    if data_dir == None:
        data_dir = get_data_dir()

    if isinstance(start_time, dt.datetime):
        start_time = int(start_time.timestamp() * 1000)
    if isinstance(end_time, dt.datetime):
        end_time = int(end_time.timestamp() * 1000)

    ret = client.get_market_data3(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, 'v4', enable_read_from_local,
                                  enable_read_from_server, debug_mode, data_dir)
    result = {}
    for stock, index, npdatas in ret:
        data = {field: np.frombuffer(b, fi) for field, fi, b in npdatas}
        result[stock] = pd.DataFrame(data=data, index=index)
    return result

def _get_market_data_ex_221207(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True, enable_read_from_server = True
):
    ifield = 'time'
    query_field_list = field_list if (not field_list) or (ifield in field_list) else [ifield] + field_list

    if period in {'1m', '5m', '15m', '30m', '60m', '1h', '1d', '1w', '1mon', '1q', '1hy', '1y'}:
        ori_data = _get_market_data_ex_ori_221207(query_field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, enable_read_from_server)
    else:
        ori_data = get_market_data_ex_ori(query_field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, enable_read_from_server)

    import pandas as pd
    result = {}

    fl = field_list

    if fl:
        fl2 = fl if ifield in fl else [ifield] + fl
        for s in ori_data:
            sdata = pd.DataFrame(ori_data[s], columns = fl2)
            sdata2 = sdata[fl]
            sdata2.index = pd.to_datetime((sdata[ifield] + 28800000) * 1000000)
            result[s] = sdata2
    else:
        for s in ori_data:
            sdata = pd.DataFrame(ori_data[s])
            sdata.index = pd.to_datetime((sdata[ifield] + 28800000) * 1000000)
            result[s] = sdata

    return result


get_market_data3 = _get_market_data_ex_221207


def _get_market_data_ex_250414(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True, enable_read_from_server = True
    , data_dir = None
):
    client = get_client()

    enable_read_from_local = period in {
        '1m', '5m', '15m', '30m', '60m', '1h'
        , '1d', 'tick', '1w', '1mon', '1q', '1hy', '1y'
    }

    global debug_mode

    if data_dir == None:
        data_dir = get_data_dir()

    result = client.get_market_data3(
        field_list, stock_list, period
        , start_time, end_time, count
        , dividend_type, fill_data
        , 'v5', enable_read_from_local, enable_read_from_server
        , debug_mode, data_dir
    )

    import pyarrow as pa
    result = pa.ipc.open_stream(result).read_all().to_pandas()

    import pandas as pd
    result.index = pd.to_datetime(result['time'] + 28800000, unit = 'ms')

    return result


def _get_data_file_path(stocklist, period, date = '20380119'):

    if isinstance(period, tuple):
        metaid, periodNum = period
        periodstr = ''
    else:
        periodstr = period
        metaid = -1
        periodNum = -1

    data = {
        'stocklist': stocklist
        , 'period': periodstr
        , 'metaid':metaid
        , 'periodNum':periodNum
        , 'date':date
    }

    client = get_client()

    path_result = _BSON_call_common(
        client.commonControl, 'getdatafilepath'
        , data
    )
    return path_result.get('result', {})

__TUPLE_PERIODS = {
    'warehousereceipt' : (4015,86400000, '')
    , 'futureholderrank' : (5008,86400000, '')
    , 'interactiveqa' : (7011, 86400000, '')
    , 'dividendplaninfo' : (2025, 86401000, '')
    , 'etfredemptionlist' : (2004, 86401000, '')
    , 'historymaincontract' : (5004, 86400000, '')
    , 'brokerqueue' : (1820,0, '港股委托经纪人队列')
    , 'brokerqueue2' : (1820,0, '港股委托经纪人队列(对结果进行转换)')
    , 'brokerinfo' : (2038,86401000, '')
    , 'delistchangebond' : (4020, 86401000, '')
    , 'replacechangebond' : (4021, 86401000, '')
    , 'optionhistorycontract' : (9502, 86400000, '')
    , 'etfiopv1m' : (4011, 60000, '')
    , 'etfiopv1d' : (4011, 86400000, '')
    , 'announcement' : (9000, 86400000, '')
    , 'hktdetails' : (2033, 86400000, '')
    , 'stocklistchange' : (2012, 86400000, '')
    , 'riskfreerate' : (2032, 86400000, '')
    #以下内置已加
    , 'etfstatistics': (3030, 0, '')
    , 'hfetfstatistics': (1830, 0, '')
    , 'northfinancechange1m': (3006, 60000, '')
    , 'northfinancechange1d': (3006, 86400000, '')
    , 'stoppricedata': (9506, 86400000, '')
}

__STR_PERIODS = {
    (3001,60000) : '1m'
    , (3001,300000) : '5m'
    , (3001,900000) : '15m'
    , (3001,1800000) : '30m'
    , (3001,3600000) : '60m'
    , (3001,3600000) : '1h'
    , (3001,86400000) : '1d'
}

def _needconvert_period(period):
    datas = {
        'snapshotindex' : 3004
        , 'etfiopv' : 4011
    }
    return period in datas, datas.get(period, -1)

def _validate_period(period):
    '''
    验证周期的有效性。

    根据输入周期类型（元组或字符串），在预定义的周期映射中查找并返回标准化后的周期信息。

    Args:
        period (tuple | str): 需要验证的周期。
            - 如果是元组，期望格式为 (metaid, period_num)，例如 (3001, 60000)。
            - 如果是字符串，例如 '1m', 'etfiopv'。

    Returns:
        tuple: 包含三个元素的元组 (str_period, meta_id, period_num)
            - str_period (str): 标准化的周期字符串。如果输入是元组且在 __STR_PERIODS 中找不到，则为空字符串。
                               如果输入是字符串，则为原始字符串或在 __TUPLE_PERIODS 中定义的名称。
            - meta_id (int): 周期的元数据ID。如果找不到，则为 -1 (当输入为字符串时) 或元组中的原始值。
            - period_num (int): 周期的数值表示。如果找不到，则为 -1 (当输入为字符串时) 或元组中的原始值。
    '''
    if isinstance(period, tuple):
        return (__STR_PERIODS.get(period, ''), *period)
    else:
        res = __TUPLE_PERIODS.get(period, None)
        if res:
            metaid, p, desc = res
            return (period, metaid, p)
        else:
            return (period, -1, -1)


def _get_market_data_ex_tuple_period_ori(
    stock_list = [], period = ()
    , start_time = '', end_time = ''
    , count = -1
):
    client = get_client()

    data_path_dict = _get_data_file_path(stock_list, period)

    import datetime as dt
    if isinstance(start_time, dt.datetime):
        start_time = int(start_time.timestamp() * 1000)
    if isinstance(end_time, dt.datetime):
        end_time = int(end_time.timestamp() * 1000)

    ori_data = {}
    for stockcode in data_path_dict:
        file_name = data_path_dict[stockcode]
        data_list = client.read_local_data(file_name, start_time, end_time, count)

        cdata_list = []
        for data in data_list:
            cdata_list.append(_BSON_.BSON.decode(data))

        ori_data[stockcode] = cdata_list

    return ori_data


def _convert_component_info(data, convert_field_list):
    if not isinstance(data, dict):
        return data

    new_data = {}
    for key, value in data.items():
        name = convert_field_list.get(key, key)
        if isinstance(value, dict):
            new_data[name] = _convert_component_info(value, convert_field_list)
        elif isinstance(value, list):
            new_data[name] = [_convert_component_info(item, convert_field_list) for item in value]
        else:
            new_data[name] = value

    return new_data

def _get_market_data_ex_tuple_period(
    field_list = [], stock_list = [], period = None
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True, enable_read_from_server = True
):
    if not isinstance(period, tuple):
        return {}

    all_data = _get_market_data_ex_tuple_period_ori(stock_list, period, start_time, end_time, count)

    metaid, periodNum = period
    convert_field_list = get_field_list(metaid)

    import pandas as pd

    ori_data = {}
    for stockcode,data_list in all_data.items():
        if convert_field_list:
            convert_data_list = []
            for data in data_list:
                convert_data = _convert_component_info(data, convert_field_list)
                convert_data_list.append(convert_data)
            data_list = convert_data_list
        ori_data[stockcode] = pd.DataFrame(data_list)

    return ori_data


def get_local_data(field_list=[], stock_list=[], period='1d', start_time='', end_time='', count=-1,
                              dividend_type='none', fill_data=True, data_dir=None):
    if data_dir == None:
        data_dir = get_data_dir()

    if period in {'1m', '5m', '15m', '30m', '60m', '1h', '1d', '1w', '1mon', '1q', '1hy', '1y'}:
        return _get_market_data_ex_ori_221207(field_list, stock_list, period, start_time, end_time, count,
                                              dividend_type, fill_data, False, data_dir)

    import pandas as pd
    result = {}

    ifield = 'time'
    query_field_list = field_list if (not field_list) or (ifield in field_list) else [ifield] + field_list
    ori_data = get_market_data_ex_ori(query_field_list, stock_list, period, start_time, end_time, count, dividend_type,
                                      fill_data, False, data_dir)

    if not ori_data:
        return result

    fl = field_list
    stime_fmt = '%Y%m%d' if period == '1d' else '%Y%m%d%H%M%S'
    if fl:
        fl2 = fl if ifield in fl else [ifield] + fl
        for s in ori_data:
            sdata = pd.DataFrame(ori_data[s], columns = fl2)
            sdata2 = sdata[fl]
            sdata2.index = [timetag_to_datetime(t, stime_fmt) for t in sdata[ifield]]
            result[s] = sdata2
    else:
        for s in ori_data:
            sdata = pd.DataFrame(ori_data[s])
            sdata.index = [timetag_to_datetime(t, stime_fmt) for t in sdata[ifield]]
            result[s] = sdata

    return result


def get_l2_quote(field_list=[], stock_code='', start_time='', end_time='', count=-1):
    '''
    level2实时行情
    '''
    import datetime as dt
    global debug_mode
    client = get_client()

    if isinstance(start_time, dt.datetime):
        start_time = int(start_time.timestamp() * 1000)
    if isinstance(end_time, dt.datetime):
        end_time = int(end_time.timestamp() * 1000)
    datas = client.get_market_data3(field_list, [stock_code], 'l2quote', start_time, end_time, count, 'none', False, '', False, True, debug_mode, '')
    if datas:
        return datas[stock_code]
    return None


def get_l2_order(field_list=[], stock_code='', start_time='', end_time='', count=-1):
    '''
    level2逐笔委托
    '''
    import datetime as dt
    global debug_mode
    client = get_client()
    if isinstance(start_time, dt.datetime):
        start_time = int(start_time.timestamp() * 1000)
    if isinstance(end_time, dt.datetime):
        end_time = int(end_time.timestamp() * 1000)
    datas = client.get_market_data3(field_list, [stock_code], 'l2order', start_time, end_time, count, 'none', False, '', False, True, debug_mode, '')
    if datas:
        return datas[stock_code]
    return None


def get_l2_transaction(field_list=[], stock_code='', start_time='', end_time='', count=-1):
    '''
    level2逐笔成交
    '''
    import datetime as dt
    global debug_mode
    client = get_client()
    if isinstance(start_time, dt.datetime):
        start_time = int(start_time.timestamp() * 1000)
    if isinstance(end_time, dt.datetime):
        end_time = int(end_time.timestamp() * 1000)
    datas = client.get_market_data3(field_list, [stock_code], 'l2transaction', start_time, end_time, count, 'none', False, '', False, True, debug_mode, '')
    if datas:
        return datas[stock_code]
    return None


def get_divid_factors(stock_code, start_time='', end_time=''):
    '''
    获取除权除息日及对应的权息
    :param stock_code: (str)股票代码
    :param date: (str)日期
    :return: pd.DataFrame 数据集
    '''
    client = get_client()
    datas = client.get_divid_factors(stock_code, start_time, end_time)
    import pandas as pd
    datas = pd.DataFrame(datas).T
    return datas


@try_except
def getDividFactors(stock_code, date):
    client = get_client()
    resData = client.get_divid_factors(stock_code, date)
    res = {resData[i]: [resData[i + 1][j] for j in
                        range(0, len(resData[i + 1]), 1)] for i in range(0, len(resData), 2)}
    if isinstance(res, dict):
        for k, v in res.items():
            if isinstance(v, list) and len(v) > 5:
                v[5] = int(v[5])
    return res


def get_main_contract(code_market: str, start_time: str = "", end_time: str = ""):
    '''
    获取主力合约/历史主力合约
    注意：获取历史主力合约需要先调用下载函数xtdata.download_history_data(symbol, 'historymaincontract', '', '')
    Args:
        code_market: 主力连续合约code,如"IF00.IF","AP00.ZF"
        start_time: 开始时间（可不填）,格式为"%Y%m%d",默认为""
        end_time: 结束时间（可不填）,格式为"%Y%m%d",默认为""
    Return:
        str:默认取当前主力合约代码
        str:当指定start_time,不指定end_time时,返回指定日期主力合约代码
        pd.Series:当指定start_time,end_time,返回区间内主力合约组成的Series,index为时间戳
    Example:
        xtdata.get_main_contract("AP00.ZF") # 取当前主力合约

        xtdata.get_main_contract("AP00.ZF","20230101") # 取历史某一天主力合约
        
        xtdata.get_main_contract("AP00.ZF","20230101","20240306") # 取某个时间段的主力合约序列
    '''
    period = 'historymaincontract'
    marker_code = code_market.split(".")[1]
    
    if start_time == "" and end_time == "":
        client = get_client()
        return client.get_main_contract(code_market) + "." + marker_code
    elif start_time and end_time == "":
        # 当指定start_time时,返回指定日期主力合约代码\n
        data = get_market_data_ex([],[code_market],period)[code_market]
        s_timetag = datetime_to_timetag(start_time,"%Y%m%d")

        data = data.loc[data.iloc[:, 0] <= s_timetag]
        if data.shape[0] > 0:
            return data['合约在交易所的代码'].iloc[-1] + "." + marker_code
        else:
            return ''
    elif start_time and end_time:
        import pandas as pd
        data = get_market_data_ex([], [code_market], period)[code_market]
        s_timetag = datetime_to_timetag(start_time, "%Y%m%d")
        e_timetag = datetime_to_timetag(end_time, "%Y%m%d")

        data = data.loc[(data.iloc[:, 0] <= e_timetag) & (data.iloc[:, 0] >= s_timetag)]
        if data.shape[0] > 0:
            index = data.iloc[:, 0]
            values = data['合约在交易所的代码'] + "." + marker_code

            res = pd.Series(index=index.values, data=values.values)
            res = res.loc[res.ne(res.shift(1))]
            return res
        else:
            return ''


def get_sec_main_contract(code_market: str, start_time: str = "", end_time: str = ""):
    '''
    获取次主力合约/历史次主力合约
    注意：获取历史次主力合约需要先调用下载函数xtdata.download_history_data(symbol, 'historymaincontract', '', '')
    Args:
        code_market: 主力连续合约code,如"IF00.IF","AP00.ZF"
        start_time: 开始时间（可不填）,格式为"%Y%m%d",默认为""
        end_time: 结束时间（可不填）,格式为"%Y%m%d",默认为""
    Return:
        str:默认取当前次主力合约代码
        str:当指定start_time,不指定end_time时,返回指定日期次主力合约代码
        pd.Series:当指定start_time,end_time,返回区间内次主力合约组成的Series,index为时间戳
    Example:
        xtdata.get_sec_main_contract("AP00.ZF") # 取当前次主力合约

        xtdata.get_sec_main_contract("AP00.ZF","20230101") # 取历史某一天次主力合约

        xtdata.get_sec_main_contract("AP00.ZF","20230101","20240306") # 取某个时间段的次主力合约序列
    '''
    period = 'historymaincontract'
    marker_code = code_market.split(".")[1]

    if start_time == "" and end_time == "":
        client = get_client()
        code = code_market.split(".")[0]
        if code.endswith('00'):
            return client.get_main_contract(code + '1' + '.' + marker_code) + "." + marker_code
    elif start_time and end_time == "":
        # 当指定start_time时,返回指定日期主力合约代码\n
        data = get_market_data_ex([], [code_market], period)[code_market]
        s_timetag = datetime_to_timetag(start_time, "%Y%m%d")

        data = data.loc[data.iloc[:, 0] < s_timetag]
        if data.shape[0] > 0:
            return data['次主力合约代码'].iloc[-1] + "." + marker_code
        else:
            return ''
    elif start_time and end_time:
        import pandas as pd
        data = get_market_data_ex([], [code_market], period)[code_market]
        s_timetag = datetime_to_timetag(start_time, "%Y%m%d")
        e_timetag = datetime_to_timetag(end_time, "%Y%m%d")

        data = data.loc[(data.iloc[:, 0] <= e_timetag) & (data.iloc[:, 0] >= s_timetag)]
        if data.shape[0] > 0:
            index = data.iloc[:, 0]
            values = data['次主力合约代码'] + "." + marker_code

            res = pd.Series(index=index.values, data=values.values)
            res = res.loc[res.ne(res.shift(1))]
            return res
        else:
            return ''


def datetime_to_timetag(datetime, format = "%Y%m%d%H%M%S"):
    if len(datetime) == 8:
        format = "%Y%m%d"
    timetag = _TIME_.mktime(_TIME_.strptime(datetime, format))
    return timetag * 1000

def timetag_to_datetime(timetag, format):
    '''
    将毫秒时间转换成日期时间
    :param timetag: (int)时间戳毫秒数
    :param format: (str)时间格式
    :return: str
    '''
    return timetagToDateTime(timetag, format)


@try_except
def timetagToDateTime(timetag, format):
    import time
    timetag = timetag / 1000
    time_local = _TIME_.localtime(timetag)
    return _TIME_.strftime(format, time_local)


def get_trading_dates(market, start_time='', end_time='', count=-1):
    '''
    根据市场获取交易日列表
    : param market: 市场代码 e.g. 'SH','SZ','IF','DF','SF','ZF'等
    : param start_time: 起始时间 '20200101'
    : param end_time: 结束时间 '20201231'
    : param count: 数据个数，-1为全部数据
    :return list(long) 毫秒数的时间戳列表
    '''
    client = get_client()
    datas = client.get_trading_dates_by_market(market, start_time, end_time, count)
    return datas


def get_full_tick(code_list):
    '''
    获取盘口tick数据
    :param code_list: (list)stock.market组成的股票代码列表
    :return: dict
    {'stock.market': {dict}}
    '''
    import json

    client = get_client()
    resp_json = client.get_full_tick(code_list)
    return json.loads(resp_json)


def subscribe_callback_wrapper(callback):
    import traceback
    def subscribe_callback(datas):
        try:
            if type(datas) == bytes:
                datas = _BSON_.BSON.decode(datas)
            if callback:
                callback(datas)
        except:
            print('subscribe callback error:', callback)
            _TRACEBACK_.print_exc()
    return subscribe_callback

def subscribe_callback_wrapper_1820(callback):
    import traceback
    def subscribe_callback(datas):
        try:
            if type(datas) == bytes:
                datas = _BSON_.BSON.decode(datas)
            datas = _covert_hk_broke_data(datas)
            if callback:
                callback(datas)
        except:
            print('subscribe callback error:', callback)
            _TRACEBACK_.print_exc()

    return subscribe_callback


def subscribe_callback_wrapper_convert(callback, metaid):
    import traceback
    convert_field_list = get_field_list(metaid)
    def subscribe_callback(datas):
        try:
            if type(datas) == bytes:
                datas = _BSON_.BSON.decode(datas)
            if convert_field_list:
                for s in datas:
                    sdata = datas[s]
                    convert_data_list = []
                    for data in sdata:
                        convert_data = _convert_component_info(data, convert_field_list)
                        convert_data_list.append(convert_data)
                    datas[s] = convert_data_list
            if callback:
                callback(datas)
        except:
            print('subscribe callback error:', callback)
            _TRACEBACK_.print_exc()

    return subscribe_callback

def subscribe_quote(stock_code, period='1d', start_time='', end_time='', count=0, callback=None):
    '''
    订阅股票行情数据
    :param stock_code: 股票代码 e.g. "000001.SZ"
    :param period: 周期 分笔"tick" 分钟线"1m"/"5m" 日线"1d"等周期
    :param start_time: 开始时间，支持以下格式:
        - str格式: YYYYMMDD/YYYYMMDDhhmmss，e.g."20200427" "20200427093000"
        - datetime对象
        若取某日全量历史数据，时间需要具体到秒，e.g."20200427093000"
    :param end_time: 结束时间 同“开始时间”
    :param count: 数量 -1全部/n: 从结束时间向前数n个
    :param callback:
        订阅回调函数onSubscribe(datas)
        :param datas: {stock : [data1, data2, ...]} 数据字典
    :return: int 订阅序号
    '''
    return subscribe_quote2(stock_code, period, start_time, end_time, count, None, callback)

def subscribe_quote2(stock_code, period='1d', start_time='', end_time='', count=0, dividend_type = None, callback=None):
    '''
    订阅股票行情数据第二版
    与第一版相比增加了除权参数dividend_type，默认None

    :param stock_code: 股票代码 e.g. "000001.SZ"
    :param period: 周期 分笔"tick" 分钟线"1m"/"5m" 日线"1d"等周期
    :param start_time: 开始时间，支持以下格式:
        - str格式: YYYYMMDD/YYYYMMDDhhmmss，e.g."20200427" "20200427093000"
        - datetime对象
        若取某日全量历史数据，时间需要具体到秒，e.g."20200427093000"
    :param end_time: 结束时间 同“开始时间”
    :param count: 数量 -1全部/n: 从结束时间向前数n个
    :param dividend_type: 除权类型"none" "front" "back" "front_ratio" "back_ratio"
    :param callback:
        订阅回调函数onSubscribe(datas)
        :param datas: {stock : [data1, data2, ...]} 数据字典
    :return: int 订阅序号
    '''
    import datetime as dt
    if isinstance(start_time, dt.datetime):
        start_time = int(start_time.timestamp() * 1000)
    if isinstance(end_time, dt.datetime):
        end_time = int(end_time.timestamp() * 1000)

    if callback:
        needconvert, metaid = _needconvert_period(period)
        if needconvert:
            callback = subscribe_callback_wrapper_convert(callback, metaid)
        elif period == 'brokerqueue2':
            callback = subscribe_callback_wrapper_1820(callback)
        else:
            callback = subscribe_callback_wrapper(callback)

    spec_period, meta_id, period_num = _validate_period(period)
    meta = {'stockCode': stock_code, 'period': spec_period, 'metaid': meta_id, 'periodnum': period_num, 'dividendtype': dividend_type}
    region = {'startTime': start_time, 'endTime': end_time, 'count': count}

    param = {'needCallback': callback != None}
    return get_client().subscribe_quote(_BSON_.BSON.encode(meta), _BSON_.BSON.encode(region), _BSON_.BSON.encode(param), callback)


def subscribe_l2thousand(stock_code, gear_num = None, callback = None):
    '''
    订阅千档盘口

    参数:
        stock_code:
            str 股票代码 例如 '000001.SZ'
        gear_num:
            int 订阅的档位数
            传 None 表示订阅全部档位
        callback:
            def ondata(data):
                print(data)
    返回:
        int 订阅号

    示例:
        def on_data(data):
            print(data)
        seq = xtdata.subscribe_l2thousand('000001.SZ', gear_num = 10, callback = on_data)

    数据示例:
        data: dict
        {'002594.SZ': [{'time': 1733120580630, 'askPrice': [282.14, 282.18, 282.19, 282.2, 282.21, 282.22, 282.24, 282.25, 282.26, 282.29], 'askVolume': [4, 2, 3, 4, 5, 6, 54, 3, 1, 4], 'bidPrice': [282.08, 282.06, 282.05, 282.02, 282.01, 282.0, 281.99, 281.98, 281.95, 281.94], 'bidVolume': [3, 3, 3, 2, 5, 58, 1, 2, 1, 4], 'price': 282.14}]}
    '''
    if callback:
        callback = subscribe_callback_wrapper(callback)

    meta = {'stockCode': stock_code, 'period': 'l2thousand', 'metaid': -1, 'periodnum': -1}
    region = {'thousandGearNum': gear_num, 'thousandDetailGear': 0, 'thousandDetailNum': 0}

    client = get_client()
    param = {'needCallback': callback != None}
    return client.subscribe_quote(_BSON_.BSON.encode(meta), _BSON_.BSON.encode(region), _BSON_.BSON.encode(param), callback)


def subscribe_l2thousand_queue(
    stock_code, callback = None
    , gear_num = None
    , price = None
):
    '''
    订阅千档盘口队列

    参数:
        stock_code:
            str 股票代码 例如 '000001.SZ'
        callback:
            def ondata(data):
                print(data)
        gear_num:
            int 订阅的档位数
            传 None 表示订阅全部档位
        price:
            float 指定一个订阅价格档位
            list[float] 指定多个订阅价格档位
            tuple(2) 指定价格范围，从tuple[0]到tuple[1]，精度0.01
            传 None 表示订阅全部价格
    返回:
        int 订阅号

    示例:
        def ondata(data):
            print(data)

        # 订阅买卖2档
        seq1 = xtdata.subscribe_l2thousand_queue('000001.SZ', callback = ondata, gear_num = 2)

        # 订阅价格为11.3的档位
        seq2 = xtdata.subscribe_l2thousand_queue('000001.SZ', callback = ondata, price = 11.3)

        # 订阅价格范围为11.3到11.4的档位
        seq3 = xtdata.subscribe_l2thousand_queue('000001.SZ', callback = ondata, price = (11.3, 11.4))

        # 订阅价格为11.3和11.4的档位，只包含这两个具体价格
        seq4 = xtdata.subscribe_l2thousand_queue('000001.SZ', callback = ondata, price = [11.3, 11.4])

    数据示例:
        data: dict
        {'000001.SZ': [{'time': 1733120970090, 'bidGear': [1, 2], 'bidPrice': [11.36, 11.35], 'bidVolume': [[91, 100, 100, 100, 20, 100, 50, 29, 3, 51, 12, 308, 8, 20, 10, 5, 1, 1, 3, 12, 100, 100, 10, 62, 2, 35, 300, 51, 9, 100, 100, 10, 131, 5, 30, 4, 20, 30, 5, 20, 20, 12, 5, 3, 19, 11, 9, 5, 10, 5], [38, 26, 7, 15, 29, 3, 6, 25, 30, 20, 100, 17, 10, 60, 3, 38, 4, 3, 119, 200, 8, 5, 78, 2, 2, 1, 10, 22, 2, 13, 50, 2, 2, 100, 5, 8, 10, 12, 5, 10, 10, 50, 40, 3, 10, 1, 16, 10, 100, 24]], 'askGear': [1, 2], 'askPrice': [11.37, 11.38], 'askVolume': [[11, 15, 18, 10, 1, 2, 2, 1, 5, 2, 1, 5, 21, 5, 1, 67, 2, 225, 1, 3, 5, 5, 104, 4, 21, 300, 3, 18, 17, 1, 102, 6, 2, 4, 2, 2, 4, 2, 4, 2, 1, 5, 2, 2, 34, 2, 1, 60, 1, 118], [12, 7, 20, 1, 10, 6, 5, 71, 1, 7, 794, 2, 100, 40, 28, 429, 13, 16, 7, 9, 130, 6, 11, 3, 4, 1, 5, 1, 167, 11, 12, 84, 56, 7, 3, 2, 10, 1, 1, 20, 10, 1, 22, 10, 2, 70, 30, 7, 3, 1]]}]}
    '''

    if callback:
        callback = subscribe_callback_wrapper(callback)

    if gear_num is not None and price is not None:
        raise Exception('不能同时指定档位和价格')

    if gear_num is None:
        gear_num = 0
    if price is not None:
        if isinstance(price, float):
            price = [int(price * 10000)]
        elif isinstance(price, tuple):
            sprice, eprice = price
            price = [i for i in range(int(sprice * 10000), int((eprice + 0.01) * 10000), int(0.01 * 10000))]
        else:
            price = [i * 10000 for i in price]
        price.sort()

    meta = {'stockCode': stock_code, 'isSubscribeByType': True, 'gear': gear_num, 'price': price, 'period': 'l2thousandqueue',
            'metaid': -1, 'periodnum': -1}
    region = {'thousandDetailGear': 1000, 'thousandDetailNum': 1000}

    client = get_client()
    param = {'needCallback': callback != None}
    return client.subscribe_quote(_BSON_.BSON.encode(meta), _BSON_.BSON.encode(region), _BSON_.BSON.encode(param), callback)


def get_l2thousand_queue(stock_code, gear_num = None, price = None):
    '''
    获取千档盘口队列数据

    参数:
        stock_code:
            str 股票代码 例如 '000001.SZ'
        gear_num:
            int 订阅的档位数
            传 None 表示订阅全部档位
        price:
            list[float] | tuple(2) 订阅的价格范围
            传 None 表示订阅全部价格
    返回:
        dict
    '''

    if gear_num is None:
        gear_num = 0
    if price is not None:
        if isinstance(price, float):
            price = [int(price * 10000)]
        elif isinstance(price, tuple):
            sprice, eprice = price
            price = [i for i in range(int(sprice * 10000), int((eprice + 0.01) * 10000), int(0.01 * 10000))]
        else:
            price = [i * 10000 for i in price]
        price.sort()

    client = get_client()

    data = {}
    data['stockcode'] = stock_code
    data['period'] = 'l2thousand'
    data['gear'] = gear_num
    data['price'] = price

    result_bson = client.commonControl('getl2thousandqueue', _BSON_.BSON.encode(data))
    result = _BSON_.BSON.decode(result_bson)
    return result.get('result')


def _get_index_mirror_data(code_list, period):
    '''
    获取指标全推数据

    参数:
        code_list:
            list[str] 市场或股票代码列表
        period:
            str 指标全推支持周期

            hftransactioncount 大单统计
            fullspeedorderbook 全速盘口
    返回:
        dict
    '''
    client = get_client()
    data = _BSON_call_common(
        client.commonControl
        , 'getindexmirrordata'
        , {
            'stocklist': code_list
            , 'period': period
        }
    )
    return data


def get_transactioncount(code_list):
    '''
    获取大单统计数据

    参数:
        code_list:
            list[str] 市场或股票代码列表
    返回:
        dict
    '''
    return _get_index_mirror_data(code_list, 'hftransactioncount')


def get_fullspeed_orderbook(code_list):
    '''
    获取全速盘口数据

    参数:
        code_list:
            list[str] 市场或股票代码列表
    返回:
        dict
    '''
    return _get_index_mirror_data(code_list, 'fullspeedorderbook')


def subscribe_whole_quote(code_list, callback = None):
    '''
    订阅全推数据

    参数:
        code_list:
            list
                [market1, market2, ...]
                或 [stock1, stock2, ...]
        callback:
            def ondata(datas):
                pass
            datas:
                {stock1 : data1, stock2 : data2, ...}
    返回:
        int 订阅号
    示例:
        def on_data(datas):
            print(datas)
        seq1 = xtdata.subscribe_whole_quote(['SH', 'SZ'], on_data)
        seq2 = xtdata.subscribe_whole_quote(['600000.SH', '000001.SZ'], on_data)

    数据示例:
        datas: dict
            {'000001.SZ': {'time': 1733118954000, 'lastPrice': 11.39, 'open': 11.39, 'high': 11.4, 'low': 11.31, 'lastClose': 11.38, 'amount': 862127800.0, 'volume': 758613, 'pvolume': 75861284, 'stockStatus': 3, 'openInt': 13, 'transactionNum': 37062, 'lastSettlementPrice': 11.38, 'settlementPrice': 0.0, 'pe': 0.0, 'askPrice': [11.4, 11.41, 11.42, 11.43, 11.44], 'bidPrice': [11.39, 11.38, 11.370000000000001, 11.36, 11.35], 'askVol': [10929, 12401, 6671, 4555, 6708], 'bidVol': [2429, 7127, 7146, 9111, 12189], 'volRatio': 0.0, 'speed1Min': 0.0, 'speed5Min': 0.0}}
    '''
    if callback:
        callback = subscribe_callback_wrapper(callback)

    param = {'needCallback': callback != None}
    return get_client().subscribe_whole_quote(code_list, _BSON_.BSON.encode(param), callback)


def unsubscribe_quote(seq):
    '''
    :param seq: 订阅接口subscribe_quote返回的订阅号
    :return:
    '''
    client = get_client()
    return client.unsubscribe_quote(seq)


def run():
    '''阻塞线程接收行情回调'''
    import time
    client = get_client()
    while True:
        _TIME_.sleep(3)
        if not client.is_connected():
            raise Exception('行情服务连接断开')
            break
    return

def create_sector_folder(parent_node,folder_name,overwrite = True):
    '''
    创建板块目录节点
    :parent_node str: 父节点,''为'我的' （默认目录）
    :sector_name str: 要创建的板块目录名称
    :overwrite bool:是否覆盖 True为跳过，False为在folder_name后增加数字编号，编号为从1开始自增的第一个不重复的值
    '''
    client = get_client()
    data = {}
    data['parent'] = parent_node
    data['foldername'] = folder_name
    data['overwrite'] = overwrite
    result_bson = client.commonControl('createsectorfolder', _BSON_.BSON.encode(data))
    result = _BSON_.BSON.decode(result_bson)
    return result.get('result')

def create_sector(parent_node,sector_name,overwrite = True):
    '''
    创建板块
    :parent_node str: 父节点,''为'我的' （默认目录）
    :sector_name str: 要创建的板块名
    :overwrite bool:是否覆盖 True为跳过，False为在sector_name后增加数字编号，编号为从1开始自增的第一个不重复的值
    '''
    client = get_client()
    data = {}
    data['parent'] = parent_node
    data['sectorname'] = sector_name
    data['overwrite'] = overwrite
    result_bson = client.commonControl('createsector', _BSON_.BSON.encode(data))
    result = _BSON_.BSON.decode(result_bson)
    return result.get('result')

def get_sector_list():
    '''
    获取板块列表
    :return: (list[str])
    '''
    client = get_client()
    return client.get_sector_list()

def get_sector_info(sector_name = ''):
    '''
    获取板块信息
    :param sector_name: (str) 板块名称，默认为空字符串，表示获取所有板块信息
    :return: (pandas.DataFrame) 包含板块信息的数据框
    '''
    import os
    from pyarrow import feather
    import pandas as pd

    client = get_client()
    sector_dir = os.path.join(client.get_data_dir(), 'SectorData', 'latest')

    result = {'sector':[], 'category':[]}

    def _get_sector_info_from_file(sector_name):
        fe_file = os.path.join(sector_dir, sector_name + '.fe')
        try:
            fe_table = feather.read_table(source=fe_file)
            if fe_table is None:
                return None, None
        except:
            return None, None
        fe_metadata = fe_table.schema.metadata
        name = fe_metadata.get(b'original_name', b'')
        category = fe_metadata.get(b'category', b'')
        return name.decode('utf-8'), category.decode('utf-8')

    if sector_name:
        name, category = _get_sector_info_from_file(sector_name)
        if category is not None:
            result['sector'].append(name if name else sector_name)
            result['category'].append(category)
    else:
        for file in os.listdir(sector_dir):
            if not file.endswith('.fe'):
                continue
            sector_name = os.path.splitext(file)[0]
            name, category = _get_sector_info_from_file(sector_name)
            if category:
                result['sector'].append(name if name else sector_name)
                result['category'].append(category)

    return pd.DataFrame(result)

def add_sector(sector_name, stock_list):
    '''
    增加自定义板块
    :param sector_name: 板块名称 e.g. "我的自选"
    :param stock_list: (list)stock.market组成的股票代码列表
    '''
    client = get_client()
    data = {}
    data['sectorname'] = sector_name
    data['stocklist'] = stock_list
    result_bson = client.commonControl('addsector', _BSON_.BSON.encode(data))
    result = _BSON_.BSON.decode(result_bson)
    return result.get('result')

def remove_stock_from_sector(sector_name, stock_list):
    '''
    移除板块成分股
    :param sector_name: 板块名称 e.g. "我的自选"
    :stock_list: (list)stock.market组成的股票代码列表
    '''
    client = get_client()
    data = {}
    data['sectorname'] = sector_name
    data['stocklist'] = stock_list
    result_bson = client.commonControl('removestockfromsector', _BSON_.BSON.encode(data))
    result = _BSON_.BSON.decode(result_bson)
    return result.get('result')

def remove_sector(sector_name):
    '''
    删除自定义板块
    :param sector_name: 板块名称 e.g. "我的自选"
    '''
    client = get_client()
    data = {}
    data['sectorname'] = sector_name
    result_bson = client.commonControl('removesector', _BSON_.BSON.encode(data))
    result = _BSON_.BSON.decode(result_bson)
    return result.get('result')

def reset_sector(sector_name, stock_list):
    '''
    重置板块
    :param sector_name: 板块名称 e.g. "我的自选"
    :stock_list: (list)stock.market组成的股票代码列表
    '''
    client = get_client()
    data = {}
    data['sectorname'] = sector_name
    data['stocklist'] = stock_list
    result_bson = client.commonControl('resetsector', _BSON_.BSON.encode(data))
    result = _BSON_.BSON.decode(result_bson)
    return result.get('result')

def _get_instrument_detail(stock_code):
    from . import xtutil

    client = get_client()
    inst = client.get_instrument_detail(stock_code)
    if not inst:
        return None

    inst = xtutil.read_from_bson_buffer(inst)

    if len(inst) != 1:
        return None
    ret = inst[0]

    if 'ExtendInfo' in ret and ret.get('ExtendInfo').get('OptionType', -1) in [-1, 0]:
        OptionType = -1

        market = ret.get('ExchangeID')
        if market in ['SHO', 'SZO', 'CFFEX', 'IF', 'SF', 'SHFE', 'DF', 'DCE', 'INE', 'GF', 'GFEX', 'ZF', 'CZCE']:

            instrumentName = ret.get('InstrumentName')
            if '购' in instrumentName:
                OptionType = 0
            elif '沽' in instrumentName:
                OptionType = 1

            if OptionType == -1:
                ProductCode = ret.get('ProductID', '')
                if market in ['ZF', 'CZCE'] and len(ProductCode) == 3:
                    opttype = ProductCode[-1]
                    if opttype == 'C':
                        OptionType = 0
                    elif opttype == 'P':
                        OptionType = 1
                elif market in ['IF', 'CFFEX']:
                    code = ret.get('InstrumentID', '')
                    if code.find('-') >= 0:
                        if code.find('C') >= 0:
                            OptionType = 0
                        elif code.find('P') >= 0:
                            OptionType = 1
                elif ProductCode.endswith('_o'):
                    Product = ProductCode[:-2]
                    code = ret.get('InstrumentID', '')
                    if code.startswith(Product):
                        if code.find('C') >= 0:
                            OptionType = 0
                        elif code.find('P') >= 0:
                            OptionType = 1

        ret['ExtendInfo']['OptionType'] = OptionType

    return ret

def get_instrument_detail(stock_code, iscomplete = False):
    '''
    获取合约信息
    :param stock_code: 股票代码 e.g. "600000.SH"
    :return: dict
        ExchangeID(str):合约市场代码
        , InstrumentID(str):合约代码
        , InstrumentName(str):合约名称
        , ProductID(str):合约的品种ID(期货)
        , ProductName(str):合约的品种名称(期货)
        , ProductType(str):合约的类型
        , ExchangeCode(str):交易所代码
        , UniCode(str):统一规则代码
        , CreateDate(str):上市日期(期货)
        , OpenDate(str):IPO日期(股票)
        , ExpireDate(str):退市日或者到期日
        , PreClose(double):前收盘价格
        , SettlementPrice(double):前结算价格
        , UpStopPrice(double):当日涨停价
        , DownStopPrice(double):当日跌停价
        , FloatVolume(double):流通股本
        , TotalVolume(double):总股本
        , LongMarginRatio(double):多头保证金率
        , ShortMarginRatio(double):空头保证金率
        , PriceTick(double):最小变价单位
        , VolumeMultiple(int):合约乘数(对期货以外的品种，默认是1)
        , MainContract(int):主力合约标记
        , LastVolume(int):昨日持仓量
        , InstrumentStatus(int):合约停牌状态
        , IsTrading(bool):合约是否可交易
        , IsRecent(bool):是否是近月合约,
    '''

    inst = _get_instrument_detail(stock_code)
    if not inst:
        return None

    if iscomplete:
        if 'ExtendInfo' in inst:
            for field in inst['ExtendInfo']:
                inst[field] = inst['ExtendInfo'][field]
            del inst['ExtendInfo']

        def convNum2Str(field):
            if field in inst and isinstance(inst[field], int):
                inst[field] = str(inst[field])

        convNum2Str('CreateDate')
        convNum2Str('OpenDate')
        convNum2Str('ExpireDate')
        convNum2Str('EndDelivDate')
        convNum2Str('TradingDay')

        if inst.get('FloatVolume', None) is None:
            inst['FloatVolume'] = inst.get('FloatVolumn')

        if inst.get('TotalVolume', None) is None:
            inst['TotalVolume'] = inst.get('TotalVolumn')

        return inst

    field_list = [
            'ExchangeID'
            , 'InstrumentID'
            , 'InstrumentName'
            , 'ProductID'
            , 'ProductName'
            , 'ProductType'
            , 'ExchangeCode'
            , 'UniCode'
            , 'CreateDate'
            , 'OpenDate'
            , 'ExpireDate'
            , 'TradingDay'
            , 'PreClose'
            , 'SettlementPrice'
            , 'UpStopPrice'
            , 'DownStopPrice'
            , 'FloatVolume'
            , 'TotalVolume'
            , 'LongMarginRatio'
            , 'ShortMarginRatio'
            , 'PriceTick'
            , 'VolumeMultiple'
            , 'MainContract'
            , 'LastVolume'
            , 'InstrumentStatus'
            , 'IsTrading'
            , 'IsRecent'
        ]
    ret = {}
    for field in field_list:
        ret[field] = inst.get(field)

    exfield_list = [
            'ProductTradeQuota'
            , 'ContractTradeQuota'
            , 'ProductOpenInterestQuota'
            , 'ContractOpenInterestQuota'
        ]
    inst_ex = inst.get('ExtendInfo', {})
    for field in exfield_list:
        ret[field] = inst_ex.get(field)

    def convNum2Str(field):
        if field in ret and isinstance(ret[field], int):
            ret[field] = str(ret[field])
    convNum2Str('CreateDate')
    convNum2Str('OpenDate')
    convNum2Str('ExpireDate')
    convNum2Str('TradingDay')

    if ret.get('FloatVolume', None) is None:
        ret['FloatVolume'] = inst.get('FloatVolumn')

    if ret.get('TotalVolume', None) is None:
        ret['TotalVolume'] = inst.get('TotalVolumn')

    return ret


def get_instrument_detail_list(stock_list, iscomplete = False):
    '''
    获取合约信息列表

    stock_list: list
        股票代码列表 [ stock1, stock2,... ]
    iscomplete: bool
        是否返回完整信息，默认False，只返回部分信息

    return: dict
        合约信息列表 { stock1: inst1, stock2: inst2, ... }
            stock: 股票代码
            inst: 合约信息字典，格式同get_instrument_detail返回值
    '''
    return {s: get_instrument_detail(s, iscomplete) for s in stock_list}


def download_index_weight():
    '''
    下载指数权重数据
    '''
    client = get_client()
    client.down_index_weight()


def download_history_contracts(incrementally = True):
    '''
    下载过期合约数据
        incrementally: bool 是否增量
    '''
    client = get_client()
    client.down_history_contracts(incrementally)


def _download_history_data_by_metaid(stock_code, metaid, period, start_time = '', end_time = '', incrementally = True):
    cl = get_client()

    result = _BSON_call_common(
        cl.commonControl, 'downloadmetadata'
        , {
            'stockcode': stock_code
            , 'metaid': metaid
            , 'period': period
            , 'starttime': start_time
            , 'endtime': end_time
            , 'incrementally': incrementally
        }
    )
    return


def _download_history_data(stock_code, period, start_time = '', end_time = ''):
    cl = get_client()
    cl.supply_history_data(stock_code, period, start_time, end_time)
    return


def download_history_data(stock_code, period, start_time = '', end_time = '', incrementally = None):
    '''
    :param stock_code: str 品种代码，例如：'000001.SZ'
    :param period: str 数据周期
    :param start_time: 开始时间，支持以下格式:
        - str格式: YYYYMMDD/YYYYMMDDhhmmss 或 ''
            例如：'20230101' '20231231235959'
            空字符串代表全部，自动扩展到完整范围
        - datetime.datetime对象c
    :param end_time: str 结束时间 格式同开始时间
    :param incrementally: 是否增量下载
        bool: 是否增量下载
        None: 使用start_time控制，start_time为空则增量下载
    '''
    import datetime as dt

    client = get_client()

    if __download_version is None:
        if incrementally is None:
            incrementally = False if start_time else True

        spec_period, meta_id, period_num = _validate_period(period)
        if isinstance(start_time, dt.datetime):
            start_time = start_time.strftime('%Y%m%d%H%M%S')
        if isinstance(end_time, dt.datetime):
            end_time = end_time.strftime('%Y%m%d%H%M%S')

        if meta_id > 0:
            if not spec_period or not isinstance(period, tuple):
                return _download_history_data_by_metaid(stock_code, meta_id, period_num, start_time, end_time, incrementally)

        return _download_history_data(stock_code, spec_period, start_time, end_time)
    else:
        return download_history_data2([stock_code], period, start_time, end_time, None, incrementally)


supply_history_data = download_history_data

_download_msg = {}

def download_history_data2(stock_list, period, start_time='', end_time='', callback=None, incrementally = None):
    '''
    :param stock_list: 股票代码列表 e.g. ["000001.SZ"]
    :param period: 周期 分笔"tick" 分钟线"1m"/"5m" 日线"1d"
    :param start_time:  开始时间，支持以下格式:
        - str格式: YYYYMMDD/YYYYMMDDhhmmss
            例如：'20200427' '20200427093000'
            若取某日全量历史数据，时间需要具体到秒，e.g."20200427093000"
        - datetime.datetime对象
    :param end_time: 结束时间 同上，若是未来某时刻会被视作当前时间
    :return: bool 是否成功
    '''
    import datetime as dt
    
    client = get_client()

    if isinstance(stock_list, str):
        stock_list = [stock_list]

    if incrementally is None:
        incrementally = False if start_time else True

    if isinstance(start_time, dt.datetime):
        start_time = start_time.strftime('%Y%m%d%H%M%S')
    if isinstance(end_time, dt.datetime):
        end_time = end_time.strftime('%Y%m%d%H%M%S')

    param = {'incrementally' : incrementally}

    spec_period, meta_id, period_num = _validate_period(period)
    if meta_id > 0:
        param['metaid'] = meta_id
        param['period'] = period_num

    status = [False, 0, 1, '', {}]
    def on_progress(data):
        try:
            finished = data['finished']
            total = data['total']
            done = (finished >= total)

            if total < 0:
                raise Exception('下载数据失败：' + data.get('message', ''))

            if finished == total:
                if 'result' in data:
                    regino_result = _BSON_.BSON.decode(data.get('result'))
                    for stock, info in regino_result.items():
                        info['start_time'] = dt.datetime.fromtimestamp(info.get('start_time') / 1000)
                        info['end_time'] = dt.datetime.fromtimestamp(info.get('end_time') / 1000)

                        status[4][stock] = info

            status[0] = done
            status[1] = finished
            status[2] = total

            try:
                if callback:
                    callback(data)
            except:
                pass

            return done
        except:
            status[0] = True
            status[3] = data.get('message', '')
            return True
    result = client.supply_history_data2(stock_list, spec_period, start_time, end_time, _BSON_.BSON.encode(param), on_progress)
    if not result:
        import time
        try:
            while not status[0] and client.is_connected():
                _TIME_.sleep(0.1)
        except:
            if status[1] < status[2]:
                client.stop_supply_history_data2()
            _TRACEBACK_.print_exc()
        if not client.is_connected():
            raise Exception('行情服务连接断开')
        if status[3]:
            raise Exception('下载数据失败：' + status[3])
    else:
        while not status[0] and client.is_connected():
            _TIME_.sleep(0.1)

    return status[4]


def download_financial_data(stock_list, table_list=[], start_time='', end_time='', incrementally = None):
    '''
    :param stock_list: 股票代码列表
    :param table_list: 财务数据表名列表，[]为全部表
        可选范围：['Balance','Income','CashFlow','Capital','Top10FlowHolder','Top10Holder','HolderNum','PershareIndex']
    :param start_time:  开始时间，支持以下格式:
        - str格式: YYYYMMDD
            例如：'20200427'
        - datetime.datetime对象
    :param end_time: 结束时间 同上，若是未来某时刻会被视作当前时间
    '''
    client = get_client()
    if not table_list:
        table_list = ['Balance','Income','CashFlow','Capital','Top10FlowHolder','Top10Holder','HolderNum','PershareIndex']

    for table in table_list:
        download_history_data2(stock_list, table, start_time, end_time, None, incrementally)


def download_financial_data2(stock_list, table_list=[], start_time='', end_time='', callback=None):
    '''
    :param stock_list: 股票代码列表
    :param table_list: 财务数据表名列表，[]为全部表
        可选范围：['Balance','Income','CashFlow','Capital','Top10FlowHolder','Top10Holder','HolderNum','PershareIndex']
    :param start_time:  开始时间，支持以下格式:
        - str格式: YYYYMMDD
            例如：'20200427'
        - datetime.datetime对象
    :param end_time: 结束时间 同上，若是未来某时刻会被视作当前时间
    '''
    client = get_client()
    if not table_list:
        table_list = ['Balance','Income','CashFlow','Capital','Top10FlowHolder','Top10Holder','HolderNum','PershareIndex']

    import datetime as dt
    if isinstance(start_time, dt.datetime):
        start_time = start_time.strftime('%Y%m%d')
    if isinstance(end_time, dt.datetime):
        end_time = end_time.strftime('%Y%m%d')

    data = {}
    data['total'] = len(table_list) * len(stock_list)
    finish = 0
    for stock_code in stock_list:
        for table in table_list:
            client.supply_history_data(stock_code, table, start_time, end_time)

            finish = finish + 1
            try:
                data['finished'] = finish
                callback(data)
            except:
                pass

            if not client.is_connected():
                raise Exception('行情服务连接断开')
                break


def get_instrument_type(stock_code, variety_list = None):
    '''
    判断证券类型
    :param stock_code: 股票代码 e.g. "600000.SH"
    :return: dict{str : bool} {类型名：是否属于该类型}
    '''
    client = get_client()
    v_dct = client.get_stock_type(stock_code)#默认处理得到全部品种的信息
    if not v_dct:
        return {}
    v_dct1 = {}
    if variety_list == None or len(variety_list) == 0:#返回该stock_code所有的品种的T/None(False)
        v_dct1={k: v for k, v in v_dct.items() if v}
        return v_dct1

    for v in variety_list:
        if v in v_dct:
            v_dct1[v] = v_dct[v]
    return v_dct1

get_stock_type = get_instrument_type


def download_sector_data():
    '''
    下载行业板块数据
    '''
    download_history_data2([], (2009, 86400000))


def download_holiday_data(incrementally = True):
    cl = get_client()

    inst = _BSON_call_common(
        cl.commonControl
        , 'downloadholidaydata'
        , {
            'incrementally': incrementally
        }
    )
    return inst

def get_holidays():
    '''
    获取节假日列表
    :return: 8位int型日期
    '''
    client = get_client()
    return [str(d) for d in client.get_holidays()]


def get_market_last_trade_date(market):
    client = get_client()
    return client.get_market_last_trade_date(market)

def get_trading_calendar(market, start_time = '', end_time = ''):
    '''
    获取指定市场交易日历
    未来交易日根据节假日推算
    :param market: str 市场
    :param start_time: str 起始时间 '20200101'
    :param end_time: str 结束时间 '20201231'
    :return:

    note: 查看节假日未公布的未来交易日，可以使用compute_coming_trading_calendar函数
    '''
    import datetime as dt

    if market not in ["SH", "SZ"]:
        raise Exception("暂不支持除SH,SZ以外市场的交易日历")

    client = get_client()

    tdl = client.get_trading_dates_by_market(market, '', '', -1)
    tdl = [dt.datetime.fromtimestamp(tt / 1000) for tt in tdl]
    if not tdl:
        raise Exception('交易日列表为空')

    download_holiday_data(incrementally=True)
    hl = client.get_holidays()
    if not hl:
        raise Exception(f'节假日数据为空')
    hl = [dt.datetime(hh // 10000, ((hh // 100) % 100), hh % 100, 0, 0) for hh in hl]

    if start_time:
        start = dt.datetime.strptime(start_time, '%Y%m%d')
        ts = max(start - dt.timedelta(days = 1), tdl[-1])
    else:
        start = tdl[0]
        ts = tdl[-1]

    if end_time:
        end = dt.datetime.strptime(end_time, '%Y%m%d')
    else:
        end = max(dt.datetime(hl[-1].year, 12, 31, 0, 0), tdl[-1])

    if hl[-1].year < end.year:
        raise Exception(f'end_time({end_time}) 超出现有节假日数据({hl[-1].year}1231)')

    hdset = set(hl)

    res = [tt for tt in tdl if start <= tt <= end]
    tt = ts + dt.timedelta(days = 1)
    while tt <= end:
        if tt not in hdset and tt.weekday() < 5:
            res.append(tt)

        tt += dt.timedelta(days = 1)

    return [tt.strftime('%Y%m%d') for tt in res]


def is_stock_type(stock, tag):
    client = get_client()
    return client.is_stock_type(stock, tag)

def download_cb_data():
    client = get_client()
    return client.down_cb_data()

def get_cb_info(stockcode):
    client = get_client()
    inst = client.get_cb_info(stockcode)
    return _BSON_.BSON.decode(inst)

def get_option_detail_data(optioncode):
    inst = _get_instrument_detail(optioncode)
    if not inst:
        return None
    
    ret = {}
    market = inst.get('ExchangeID')
    if market == 'SHO' or market == "SZO" \
        or ((market == "CFFEX" or market == "IF") and inst.get('InstrumentID').find('-') >= 0) \
        or (market in ['SF', 'SHFE', 'DF', 'DCE', 'INE', 'GF', 'GFEX', 'ZF', 'CZCE'] and inst.get('ExtendInfo', {}).get('OptionType') in [0, 1]):
        field_list = [
            'ExchangeID'
            , 'InstrumentID'
            , 'InstrumentName'
            , 'ProductID'
            , 'ProductType'
            , 'OpenDate'
            , 'CreateDate'
            , 'ExpireDate'
            , 'PreClose'
            , 'SettlementPrice'
            , 'UpStopPrice'
            , 'DownStopPrice'
            , 'LongMarginRatio'
            , 'ShortMarginRatio'
            , 'PriceTick'
            , 'VolumeMultiple'
            , 'MaxMarketOrderVolume'
            , 'MinMarketOrderVolume'
            , 'MaxLimitOrderVolume'
            , 'MinLimitOrderVolume'
        ]
        ret = {}
        for field in field_list:
            ret[field] = inst.get(field)

        exfield_list = [
            'OptUnit'
            , 'MarginUnit'
            , 'OptUndlCode'
            , 'OptUndlUniCode'
            , 'OptUndlMarket'
            , 'OptUndlCodeFull'
            , 'OptExercisePrice'
            , 'NeeqExeType'
            , 'OptUndlRiskFreeRate'
            , 'OptUndlHistoryRate'
            , 'EndDelivDate'
            , 'OptEstimatedMargin'
        ]
        inst_ex = inst.get('ExtendInfo', {})
        for field in exfield_list:
            ret[field] = inst_ex.get(field)

        def convNum2Str(field):
            if field in ret and isinstance(ret[field], int):
                ret[field] = str(ret[field])

        convNum2Str('ExpireDate')
        convNum2Str('CreateDate')
        convNum2Str('OpenDate')
        convNum2Str('EndDelivDate')

        if 1:
            optType = ''

            if not optType:
                instrumentName = inst.get('InstrumentName')
                if '购' in instrumentName:
                    optType = 'CALL'
                elif '沽' in instrumentName:
                    optType = 'PUT'

            if not optType:
                OptionType = inst.get('ExtendInfo').get('OptionType')
                if OptionType == 0:
                    optType = 'CALL'
                elif OptionType == 1:
                    optType = 'PUT'

            ret['optType'] = optType

        ret['OptUndlCodeFull'] = ret['OptUndlUniCode'] + '.' + ret['OptUndlMarket']

        ProductCode = ret['ProductID']
        if ProductCode.endswith('_o'):
            ProductCode = ProductCode[:-2] + '.' + ret['OptUndlMarket']
        elif market in ['ZF', 'CZCE']:
            ProductCode = ProductCode[:-1] + '.' + ret['OptUndlMarket']
        else:
            ProductCode = ret['OptUndlCodeFull']
        ret['ProductCode'] = ProductCode
    return ret


def get_option_undl_data(undl_code_ref):
    def get_option_undl(opt_code):
        inst = get_option_detail_data(opt_code)
        if inst and 'OptUndlCode' in inst and 'OptUndlMarket' in inst:
            return inst['OptUndlCode'] + '.' + inst['OptUndlMarket']
        return ''

    def get_option_undl_uni(opt_code):
        inst = get_option_detail_data(opt_code)
        if inst and 'OptUndlUniCode' in inst and 'OptUndlMarket' in inst:
            return inst['OptUndlUniCode'] + '.' + inst['OptUndlMarket']
        return ''

    if undl_code_ref:
        c_undl_code_ref = undl_code_ref
        inst = get_instrument_detail(undl_code_ref)
        if inst and 'UniCode' in inst:
            marketcodeList = undl_code_ref.split('.')
            if (len(marketcodeList) != 2):
                return []
            c_undl_code_ref = inst['UniCode'] + '.' + marketcodeList[1]

        opt_list = []
        if undl_code_ref.endswith('.SH'):
            if undl_code_ref == "000016.SH" or undl_code_ref == "000300.SH" or undl_code_ref == "000852.SH" or undl_code_ref == "000905.SH":
                opt_list = get_stock_list_in_sector('中金所')
            else:
                opt_list = get_stock_list_in_sector('上证期权')
        if undl_code_ref.endswith('.SZ'):
            opt_list = get_stock_list_in_sector('深证期权')
        if undl_code_ref.endswith('.SF') or undl_code_ref.endswith('.SHFE'):
            opt_list = get_stock_list_in_sector('上期所期权')
        if undl_code_ref.endswith('.ZF') or undl_code_ref.endswith('.CZCE'):
            opt_list = get_stock_list_in_sector('郑商所期权')
        if undl_code_ref.endswith('.DF') or undl_code_ref.endswith('.DCE'):
            opt_list = get_stock_list_in_sector('大商所期权')
        if undl_code_ref.endswith('.GF') or undl_code_ref.endswith('.GFEX'):
            opt_list = get_stock_list_in_sector('广期所期权')
        if undl_code_ref.endswith('.INE'):
            opt_list = get_stock_list_in_sector('能源中心期权')
        data = []
        for opt_code in opt_list:
            undl_code = get_option_undl_uni(opt_code)
            if undl_code == c_undl_code_ref:
                data.append(opt_code)
        return data
    else:
        opt_list = []
        category_list = ['上证期权', '深证期权', '中金所', '上期所期权', '郑商所期权', '大商所期权', '广期所期权', '能源中心期权']
        for category in category_list:
            one_list = get_stock_list_in_sector(category)
            opt_list += one_list

        result = {}
        for opt_code in opt_list:
            undl_code = get_option_undl(opt_code)
            if undl_code:
                if undl_code in result:
                    result[undl_code].append(opt_code)
                else:
                    result[undl_code] = [opt_code]
        return result


def get_option_list(undl_code, dedate, opttype = "", isavailavle = False):
    result = []

    marketcodeList = undl_code.split('.')
    if (len(marketcodeList) != 2):
        return []
    undlCode = marketcodeList[0]
    undlCode_ori = undlCode
    undlMarket = marketcodeList[1]
    inst_data = get_instrument_detail(undl_code)
    if inst_data:
        undlCode = inst_data.get('UniCode', undlCode)
        undlCode_ori = inst_data.get('InstrumentID', undlCode)
    market = ""
    if (undlMarket == "SH"):
        if undlCode == "000016" or undlCode == "000300" or undlCode == "000852" or undlCode == "000905":
            market = 'IF'
        else:
            market = "SHO"
    elif (undlMarket == "SZ"):
        market = "SZO"
    else:
        market = undlMarket
    if (opttype.upper() == "C"):
        opttype = "CALL"
    elif (opttype.upper() == "P"):
        opttype = "PUT"
    optList = []
    if market == 'SHO':
        optList += get_stock_list_in_sector('上证期权')
        optList += get_stock_list_in_sector('过期上证期权')
    elif market == 'SZO':
        optList += get_stock_list_in_sector('深证期权')
        optList += get_stock_list_in_sector('过期深证期权')
    elif market == 'IF':
        optList += get_stock_list_in_sector('中金所')
        optList += get_stock_list_in_sector('过期中金所')
    elif market == 'SF' or market == 'SHFE':
        optList += get_stock_list_in_sector('上期所期权')
        optList += get_stock_list_in_sector('过期上期所')
    elif market == 'ZF' or market == 'CZCE':
        optList += get_stock_list_in_sector('郑商所期权')
        optList += get_stock_list_in_sector('过期郑商所')
    elif market == 'DF' or market == 'DCE':
        optList += get_stock_list_in_sector('大商所期权')
        optList += get_stock_list_in_sector('过期大商所')
    elif market == 'GF' or market == 'GFEX':
        optList += get_stock_list_in_sector('广期所期权')
        optList += get_stock_list_in_sector('过期广期所')
    elif market == 'INE':
        optList += get_stock_list_in_sector('能源中心期权')
        optList += get_stock_list_in_sector('过期能源中心')
    for opt in optList:
        if (opt.find(market) < 0):
            continue
        inst = get_option_detail_data(opt)
        if not inst:
            continue
        if (opttype.upper() != "" and opttype.upper() != inst["optType"]):
            continue
        if ((len(dedate) == 6 and inst['ExpireDate'].find(dedate) < 0)):
            continue
        if (len(dedate) == 8):  # option is trade,guosen demand
            createDate = inst['CreateDate']
            openDate = inst['OpenDate']
            if (createDate > '0'):
                openDate = min(openDate, createDate)
            if (openDate < '20150101' or openDate > dedate):
                continue
            endDate = inst['ExpireDate']
            if (isavailavle and endDate < dedate):
                continue
        if inst['OptUndlCode'] == undlCode or inst['OptUndlCode'] == undlCode_ori:
            result.append(opt)
    return result


def get_his_option_list(undl_code, dedate):
    '''
    获取历史上某日的指定品种期权信息列表
    :param undl_code: (str)标的代码，格式 stock.market e.g."000300.SH"
    :param date: (str)日期 格式YYYYMMDD，e.g."20200427"
    :return: dataframe
    '''
    if not dedate:
        return None

    data = get_his_option_list_batch(undl_code, dedate, dedate)
    return data.get(dedate, None)


def get_his_option_list_batch(undl_code, start_time = '', end_time = ''):
    '''
    获取历史上某段时间的指定品种期权信息列表
    :param undl_code: (str)标的代码，格式 stock.market e.g."000300.SH"
    :param start_time，start_time: (str)日期 格式YYYYMMDD，e.g."20200427"
    :return: {date : dataframe}
    '''
    split_codes = undl_code.rsplit('.', 1)
    if len(split_codes) == 2:
        stockcode = split_codes[0]
        market = split_codes[1]

    optmarket = market
    optcode = stockcode
    product = stockcode

    isstockopt = False

    if market == 'SH':
        if undl_code in ["000016.SH", "000300.SH", "000852.SH", "000905.SH"]:
            optmarket = 'IF'
        else:
            optmarket = 'SHO'
        optcode = 'XXXXXX'
        isstockopt = True
    elif market == 'SZ':
        optmarket = 'SZO'
        optcode = 'XXXXXX'
        isstockopt = True
    else:
        detail = get_instrument_detail(undl_code)
        if detail:
            optcode = detail.get('ProductID', optcode)
            product = optcode

    code = optcode + '.' + optmarket

    end_time1 = end_time
    if end_time:
        import datetime as dt
        time_tag = int(dt.datetime.strptime(end_time, '%Y%m%d').timestamp() * 1000)
        time_tag = time_tag + 31 * 86400000
        end_time1 = timetag_to_datetime(time_tag, '%Y%m%d')

    data_all = get_market_data_ex(
        []
        , [code]
        , period='optionhistorycontract'
        , start_time = start_time
        , end_time = end_time1
    ).get(code, None)

    if data_all.empty:
        return {}

    if isstockopt:
        select = f'''标的市场 == '{market}' and 标的编码 == '{stockcode}' '''
        data_all = data_all.loc[data_all.eval(select)].reset_index()

    data_all['期权完整代码'] = data_all['期权编码'] + '.' + data_all['期权市场']
    data_all['标的完整代码'] = data_all['标的编码'] + '.' + data_all['标的市场']
    data_all['期货品种'] = product

    date_list = get_trading_dates(optmarket, start_time, end_time)

    result = {}
    min_opne_date = 0
    for timetag in date_list:
        dedate = int(timetag_to_datetime(timetag, '%Y%m%d'))

        if dedate < min_opne_date:
            continue

        data1 = data_all.loc[data_all['time'] >= timetag].reset_index()
        if data1.empty:
            continue

        data_time = data1.loc[0]['time']
        select = f'''time == {data_time} and 上市日 <= {dedate} and 到期日 >= {dedate} and 方向 != '' '''

        data2 = data_all.loc[data_all.eval(select)].reset_index().drop(['index', 'time'], axis=1)
        if data2.empty:
            select = f'''time == {data_time}  '''
            data3 = data_all.loc[data_all.eval(select)].reset_index()
            min_opne_date = data3['上市日'].min()
        else:
            result[str(dedate)] = data2

    return result


def get_ipo_info(start_time = '', end_time = ''):
    client = get_client()
    data = client.get_ipo_info(start_time, end_time)
    pylist = [
        'securityCode'          #证券代码
        , 'codeName'            #代码简称
        , 'market'              #所属市场
        , 'actIssueQty'         #发行总量  单位：股
        , 'onlineIssueQty'      #网上发行量  单位：股
        , 'onlineSubCode'       #申购代码
        , 'onlineSubMaxQty'     #申购上限  单位：股
        , 'publishPrice'        #发行价格
        , 'startDate'           #申购开始日期
        , 'onlineSubMinQty'     #最小申购数，单位：股
        , 'isProfit'            #是否已盈利 0：上市时尚未盈利 1：上市时已盈利
        , 'industryPe'          #行业市盈率
        , 'beforePE'            #发行前市盈率
        , 'afterPE'             #发行后市盈率
        , 'listedDate'          #上市日期
        , 'declareDate'         #中签号公布日期
        , 'paymentDate'         #中签缴款日
        , 'lwr'                 #中签率
    ]
    result = []
    for datadict in data:
        resdict = {}
        for field in pylist:
            resdict[field] = datadict.get(field)
        result.append(resdict)
    return result


def get_markets():
    '''
    获取所有可选的市场
    返回 dict
        { <市场代码>: <市场名称>, ... }
    '''
    return {
        'SH': '上交所'
        , 'SZ': '深交所'
        , 'BJ': '北交所'
        , 'HK': '港交所'
        , 'HGT': '沪港通'
        , 'SGT': '深港通'
        , 'IF': '中金所'
        , 'SF': '上期所'
        , 'DF': '大商所'
        , 'ZF': '郑商所'
        , 'GF': '广期所'
        , 'INE': '能源交易所'
        , 'SHO': '上证期权'
        , 'SZO': '深证期权'
        , 'BKZS': '板块指数'
    }


def get_wp_market_list():
    '''
    获取所有外盘的市场
    返回 list
    '''
    return _BSON_call_common(get_client().commonControl, 'getwpmarketlist', {})


def get_his_st_data(stock_code):
    fileName = _OS_.path.join(get_data_dir(), '..', 'data', 'SH_XXXXXX_2011_86400000.csv')

    try:
        with open(fileName, "r") as f:
            datas = f.readlines()
    except:
        return {}

    status = []
    for data in datas:
        cols = data.split(',')
        if len(cols) >= 4 and cols[0] == stock_code:
            status.append((cols[2], cols[3]))

    if not status:
        return {}

    result = {}
    i = 0
    while i < len(status):
        start = status[i][0]
        flag = status[i][1]

        i += 1

        end = '20380119'
        if i < len(status):
            end = status[i][0]

        realStatus = ''
        if (flag == '1'):
            realStatus = 'ST'
        elif (flag == '2'):
            realStatus = '*ST'
        elif (flag == '3'):
            realStatus = 'PT'
        else:
            continue

        if realStatus not in result:
            result[realStatus] = []
        result[realStatus].append([start, end])

    return result


def subscribe_formula(formula_name, stock_code, period, start_time = '', end_time = '', count = -1, dividend_type = None, extend_param = {}, callback = None):
    cl = get_client()

    result = _BSON_.BSON.decode(cl.commonControl('createrequestid', _BSON_.BSON.encode({})))
    request_id = result['result']

    data = {
        'formulaname': formula_name, 'stockcode': stock_code, 'period': period
        , 'starttime': start_time, 'endtime': end_time, 'count': count
        , 'dividendtype': dividend_type if dividend_type else 'none'
        , 'extendparam': extend_param
        , 'create': True
        , 'historycallback': 1 if callback else 0
        , 'realtimecallback': 1 if callback else 0
        , 'barmode' : int(extend_param.get('barmode', 0))
    }

    if callback:
        callback = subscribe_callback_wrapper(callback)

    cl.subscribeFormula(request_id, _BSON_.BSON.encode(data), callback)
    return request_id


def get_formula_result(request_id, start_time = '', end_time = '', count = -1, timeout_second = -1):
    '''
    根据模型ID获取模型结果
    request_id: 模型ID，例如subscribe_formula返回值
    start_time: 起始时间 "20200101" "20200101093000"
    end_time: 结束时间 "20201231" "20201231150000"
    count: 数量 -1全部/n: 从结束时间向前数n个
    timeout_second: 等待时间，-1无限等待，0立即返回，+n等待n秒，超时抛异常
    '''
    res = {}

    import time
    begin_time = time.time()

    cl = get_client()
    while 1:
        status = _BSON_.BSON.decode(
            cl.commonControl(
                'checkformulafinished'
                , _BSON_.BSON.encode({
                    'requestid': request_id
                })
            )
        ).get('result', -1)

        if status == -1:
            raise Exception(f"not find formula {request_id}")

        if status == 1 or timeout_second == 0:
            res = _BSON_.BSON.decode(
                cl.commonControl(
                    'getformularesult'
                    , _BSON_.BSON.encode({
                        'requestid': request_id
                        , 'starttime': start_time
                        , 'endtime': end_time
                        , 'count': count
                    })
                )
            )
            break

        if timeout_second > 0:
            end_time = time.time()
            if end_time - begin_time > timeout_second:
                raise Exception(f"wait formula {request_id} result timeout")
        time.sleep(0.5)
    return res


def bind_formula(request_id, callback = None):
    cl = get_client()

    if callback:
        callback = subscribe_callback_wrapper(callback)

    cl.subscribeFormula(request_id, _BSON_.BSON.encode({}), callback)
    return


def unsubscribe_formula(request_id):
    cl = get_client()
    cl.unsubscribeFormula(request_id)
    return


def call_formula(
    formula_name, stock_code, period
    , start_time = '', end_time = '', count = -1
    , dividend_type = None, extend_param = {}
):
    cl = get_client()

    result = _BSON_.BSON.decode(cl.commonControl('createrequestid', _BSON_.BSON.encode({})))
    request_id = result['result']

    data = {
        'formulaname': formula_name, 'stockcode': stock_code, 'period': period
        , 'starttime': start_time, 'endtime': end_time, 'count': count
        , 'dividendtype': dividend_type if dividend_type else 'none'
        , 'extendparam': extend_param
        , 'create': True
    }

    data = cl.subscribeFormulaSync(request_id, _BSON_.BSON.encode(data))
    return _BSON_.BSON.decode(data)

gmd = get_market_data
gmd2 = get_market_data_ex
gmd3 = get_market_data3
gld = get_local_data
t2d = timetag_to_datetime
gsl = get_stock_list_in_sector


def reset_market_trading_day_list(market, datas):
    cl = get_client()

    result = _BSON_call_common(
        cl.custom_data_control, 'createmarketchange'
        , {
            'market': market
        }
    )
    cid = result['cid']

    result = _BSON_call_common(
        cl.custom_data_control, 'addtradingdaytochange'
        , {
            'cid': cid
            , 'datas': datas
            , 'coverall': True
        }
    )

    result = _BSON_call_common(
        cl.custom_data_control, 'finishmarketchange'
        , {
            'cid': cid
            #, 'abort': False
            , 'notifyupdate': True
        }
    )
    return


def reset_market_stock_list(market, datas):
    cl = get_client()

    result = _BSON_call_common(
        cl.custom_data_control, 'createmarketchange'
        , {
            'market': market
        }
    )
    cid = result['cid']

    result = _BSON_call_common(
        cl.custom_data_control, 'addstocktochange'
        , {
            'cid': cid
            , 'datas': datas
            , 'coverall': True
        }
    )

    result = _BSON_call_common(
        cl.custom_data_control, 'finishmarketchange'
        , {
            'cid': cid
            #, 'abort': False
            , 'notifyupdate': True
        }
    )
    return


def push_custom_data(meta, datas, coverall = False):
    cl = get_client()
    if period in ['1m', '5m', '15m', '30m', '60m', '1h', '2h', '3h', '4h', '1d']:
        type = 3001

    ans = []
    fields = get_field_name(type)
    for data in datas:
        params = {}
        for k, v in data.items():
            if k in fields:
                params[fields[k]] = v
        ans.append(params)

    result = _BSON_call_common(
        cl.custom_data_control, 'pushcustomdata'
        , {
            "meta": meta
            , 'datas': datas
            , 'coverall': coverall
        }
    )
    return

def get_period_list():
    client = get_client()
    result = _BSON_.BSON.decode(client.commonControl('getperiodlist', _BSON_.BSON.encode({})))
    p_list = result['result']

    result = []
    for k, v in __TUPLE_PERIODS.items():
        if len(v) >= 3 and v[2]:
            flag = True
            for i in p_list:
                if k == i.get('name', None):
                    flag = False
                    break
            if flag:
                result.append({'name': k, 'desc': v[2]})

    result.extend(p_list)
    return result


def gen_factor_index(
    data_name, formula_name, vars,  sector_list
    , start_time = '', end_time = '', period = '1d'
    , dividend_type = 'none'
):
    '''
    生成因子指数扩展数据
    '''
    running_info = {
        'result':{}
        ,'finished':0
        ,'total':0
    }
    def onPushProgress(data):
        running_info['finished'] = data.get('finished',-1)
        if running_info['finished'] == -1:
            running_info['result'] = data
        else:
            running_info['total'] = data.get('total', -1)

    callback = subscribe_callback_wrapper(onPushProgress)

    cl = get_client()
    cl.registerCommonControlCallback("genfactorindex", callback)
    _BSON_call_common(
        cl.commonControl
        , 'genfactorindex'
        , {
                'data_name': data_name
                , 'formula_name': formula_name
                , 'vars': vars
                , 'sector_list': sector_list
                , 'start_time': start_time
                , 'end_time': end_time
                , 'period': period
                , 'dividend_type': dividend_type
            }
        )

    last_finished = running_info['finished']
    time_count = 0
    while not running_info['result']:
        _TIME_.sleep(1)
        if last_finished != running_info['finished']:
            last_finished = running_info['finished']
            time_count = 0
        else:
            time_count += 1
            if time_count > 20:
                time_count = 0
                if get_client():
                    print(f'因子指数扩展数据生成进度长时间未更新，'
                          f'当前进度{running_info["finished"]}/{running_info["total"]}')

    return running_info['result']


def create_formula(formula_name, formula_content, formula_params = {}):
    '''
    创建策略

    formula_name: str 策略名称
    formula_content: str 策略内容
    formula_params: dict 策略参数

    返回: None
        如果成功，返回None
        如果失败，会抛出异常信息
    '''
    data = {
        'formula_name': formula_name
        ,'content': formula_content
    }

    if formula_params:
        data.update(formula_params)

    return _BSON_call_common(
        get_client().commonControl
        , 'createformula'
        , data
    )


def import_formula(formula_name, file_path):
    '''
    导入策略

    formula_name: str 策略名称
    file_path: str 文件路径
        一般为.rzrk文件，可以从qmt客户端导出得到
    '''
    return _BSON_call_common(get_client().commonControl
        , 'importformula'
        , {'formula_name': formula_name, 'file_path': file_path}
    )


def del_formula(formula_name):
    '''
    删除策略

    formula_name: str 策略名称
    '''
    return _BSON_call_common(get_client().commonControl
        , 'delformula'
        , {'formula_name': formula_name}
    )


def get_formulas():
    '''
    查询所有的策略
    '''
    return _BSON_call_common(get_client().commonControl, 'getformulas', {})


def read_feather(file_path):
    '''
    读取feather格式的arrow文件
    :param file_path: (str)
    :return: param_bin: (dict), df: (pandas.DataFrame)
    '''
    import sys
    if sys.version_info.major > 2:
        from pyarrow import feather
        if sys.version_info.minor > 6:
            table = feather.read_table(source=file_path, columns=None, memory_map=True, use_threads=True)
        else:
            table = feather.read_table(source=file_path, columns=None, memory_map=True)
        
        metadata = table.schema.metadata
        param_bin_bytes = metadata.get(b'param_bin')
        #param_str_bytes = metadata.get(b'param_str')

        param_bin = _BSON_.BSON.decode(param_bin_bytes)
        #param_str = param_str_bytes.decode('utf-8')
        df = table.to_pandas(use_threads=True)
        return param_bin, df

    return None, None


def write_feather(dest_path, param, df):
    '''
    将panads.DataFrame转换为arrow.Table以feather格式写入文件
    :param dest_path: (str)路径
    :param param: (dict) schema的metadata
    :param df: (pandas.DataFrame) 数据
    :return: (bool) 成功/失败
    '''
    import json, sys
    if sys.version_info.major > 2:
        from pyarrow import feather, Schema, Table
        schema = Schema.from_pandas(df).with_metadata({
            'param_bin' : _BSON_.BSON.encode(param),
            'param_str' : json.dumps(param)
        })
        table = Table.from_pandas(df, schema=schema)
        feather.write_feather(table, dest_path)
        return True

    return False


class QuoteServer:
    def __init__(self, info = {}):
        '''
        info: {
            'ip': '218.16.123.121'
            , 'port': 55300
            , 'username': 'test'
            , 'pwd': 'testpwd'
        }
        '''
        self.info = info

        ip = info.get('ip', None)
        port = info.get('port', None)

        if not ip or not port:
            raise f'invalid address, ip:{ip}, port:{port}'
        return

    def _BSON_call_common(self, interface, func, param):
        return _BSON_.BSON.decode(interface(func, _BSON_.BSON.encode(param)))

    def __str__(self):
        return str(self.info)

    def connect(self):
        '''
        连接到这个地址
        '''
        cl = get_client()

        return self._BSON_call_common(
            cl.commonControl, 'quoteserverconnect'
            , {
                'ip': self.info['ip']
                , 'port': self.info['port']
                , 'info': self.info
                , 'operation': 'login'
            }
        )

    def disconnect(self):
        '''
        断开连接
        '''
        cl = get_client()

        result = self._BSON_call_common(
            cl.commonControl, 'quoteserverconnect'
            , {
                'ip': self.info['ip']
                , 'port': self.info['port']
                , 'info': self.info
                , 'operation': 'logout'
            }
        )
        return

    def set_key(self, key_list = []):
        '''
        设置数据key到这个地址，后续会使用这个地址获取key对应的市场数据

        key_list: [key, ...]
        key:
            f'{market}_{level}'
        market:
            SH, SZ, ...
        level:
            'L1' # level 1
            'L2' # level 2
        '''
        cl = get_client()

        result = self._BSON_call_common(
            cl.commonControl, 'quoteserversetkey'
            , {
                'ip': self.info['ip']
                , 'port': self.info['port']
                , 'info': self.info
                , 'keys': key_list
            }
        )
        return

    def test_load(self):
        '''
        获取这个地址的负载情况
        '''
        cl = get_client()

        result = self._BSON_call_common(
            cl.commonControl, 'testload'
            , {
                'ip': self.info['ip']
                , 'port': self.info['port']
                , 'info': self.info
            }
        )
        return result

    def get_available_quote_key(self):
        '''
        获取这个地址可支持的数据key
        '''
        cl = get_client()

        inst = self._BSON_call_common(
            cl.commonControl, 'getavailablekey'
            , {
                'ip': self.info['ip']
                , 'port': self.info['port']
                , 'info': self.info
            }
        )
        result = inst.get('result', [])

        return result

    def get_server_list(self):
        '''
        获取这个地址的服务器组列表
        '''
        cl = get_client()

        inst = self._BSON_call_common(
            cl.commonControl, 'getserverlist'
            , {
                'ip': self.info['ip']
                , 'port': self.info['port']
                , 'info': self.info
            }
        )

        inst = inst.get('result', [])

        result = [QuoteServer(info) for info in inst]
        return result


def get_quote_server_config():
    '''
    获取连接配置

    result: [info, ...]
    '''
    cl = get_client()

    inst = _BSON_call_common(
        cl.commonControl, 'getquoteserverconfig', {}
    )
    inst = inst.get('result', [])

    result = [QuoteServer(info) for info in inst]
    return result


def get_quote_server_status():
    '''
    获取当前全局连接状态

    result: {
        quote_key: info
        , ...
    }
    '''
    cl = get_client()

    inst = _BSON_call_common(
        cl.commonControl, 'getquoteserverstatus', {}
    )
    inst = inst.get('result', [])

    result = {}
    for pair in inst:
        key = pair.get('key', '')
        info = pair.get('info', {})
        result[key] = QuoteServer(info)
    return result


def show_quote_server_status():
    '''
    获取每个key对应的连接的地址
    返回：dict，{'0_SH_L1':'ip:port', ...}
    '''
    result = {}
    cl = get_client()

    inst = _BSON_call_common(
        cl.commonControl, 'getquoteserverstatus', {}
    )
    inst = inst.get('result', [])

    for data in inst:
        key = data.get('key', '')
        info = data.get('info', {})
        result[key] = f"{info.get('ip', '')}:{info.get('port', '')}"
    return result


def watch_quote_server_status(callback):
    '''
    监控全局连接状态变化

    def callback(info):
        #info: {address : 'ip:port', status: ''}
        #status: 'connected', 'disconnected'
        return
    '''
    cl = get_client()

    if callback:
        callback = subscribe_callback_wrapper(callback)

    cl.registerCommonControlCallback("watchquoteserverstatus", callback)
    _BSON_call_common(cl.commonControl, "watchquoteserverstatus", {})
    return

def fetch_quote_server_from_config(root_path, key_list):
    root_path = _OS_.path.abspath(root_path)
    cl = get_client()
    inst = _BSON_call_common(
        cl.commonControl, 'fetchquote', {
            'dir': root_path
        }
    )

    config_dir = _OS_.path.join(root_path, 'userdata_mini', 'users', 'xtquoterconfig.xml')
    if not _OS_.path.isfile(config_dir):
        return

    import xml.etree.ElementTree as ET

    tree = ET.parse(config_dir)
    quoter_server_list = tree.find('QuoterServers')
    quoter_server_list = quoter_server_list.findall('QuoterServer')

    qs_infos = {}
    for server in quoter_server_list:
        quoter_type = server.attrib['quotertype']
        if quoter_type != '0':
            continue

        info = {'ip': server.attrib['address'], 'port': int(server.attrib['port']), 'username': server.attrib['username'], 'pwd': server.attrib['password']}
        qs = QuoteServer(info)
        relate_servers = qs.get_server_list()
        for rs in relate_servers:
            keys = rs.info.get('keys', [])
            keys = ['0_' + key for key in keys if '0_' + key in key_list]

            if keys:
                addr = (rs.info['ip'], rs.info['port'])
                rs.info['keys'] = keys
                qs_infos[addr] = rs

    servers = {}
    for qs in qs_infos.values():
        qs.info.update(qs.test_load())
        for key in qs.info['keys']:
            if key not in servers:
                servers[key] = []
            servers[key].append(qs)

    for p in servers.items():
        p[1].sort(key = lambda x: x.info.get('delay', 1000000.0))

    for key_servers in servers.items():
        for qs in key_servers[1]:
            if qs.info['load'] != 1000000.0 and qs.info['delay'] != 1000000.0 and qs.info['accessible']:
                qs.set_key([key_servers[0]])
                break

    return


def get_etf_info():
    spec_period, meta_id, period_num = _validate_period('etfredemptionlist')
    if meta_id < 0:
        return {}

    all_data = _get_market_data_ex_tuple_period_ori(['XXXXXX.SH', 'XXXXXX.SZ'], (meta_id, period_num))

    convert_field_list = get_field_list(meta_id)

    result = {}

    def _self_convert_component_info(data, convert_field_list):
        if not isinstance(data, dict):
            return data

        new_data = {}
        for key, value in data.items():
            if key in ['25', '26', '27']:
                continue

            name = convert_field_list.get(key, key)
            if isinstance(value, dict):
                new_data[name] = _self_convert_component_info(value, convert_field_list)
            elif isinstance(value, list):
                new_data[name] = [_self_convert_component_info(item, convert_field_list) for item in value]
            else:
                new_data[name] = value
        return new_data

    for stockcode, data_list in all_data.items():
        market = stockcode.split('.')[1]

        for data in data_list:
            convert_data = {'market': market}

            if convert_field_list:
                data = _self_convert_component_info(data, convert_field_list)
            convert_data.update(data)

            stock_market = ''
            if '基金代码' in data:
                stock_market = data['基金代码'] + '.' + market

            convert_market = {'1': 'SH', '2': 'SZ', '3': 'HK', '4': 'BJ'}
            if '成份股信息' in convert_data:
                for sub_data in convert_data['成份股信息']:
                    if '成份股所属市场' in sub_data and '成份股代码' in sub_data and str(sub_data['成份股所属市场']) in convert_market:
                        sub_data['成份股代码'] = sub_data['成份股代码'] + '.' + convert_market[str(sub_data['成份股所属市场'])]
                        sub_data['成份股所属市场'] = convert_market[str(sub_data['成份股所属市场'])]

            if stock_market:
                result[stock_market] = convert_data

    return result


def download_etf_info():
    for stock_code in ['XXXXXX.SH', 'XXXXXX.SZ']:
        download_history_data(stock_code, 'etfredemptionlist', '', '')

    return


def download_his_st_data():
    '''
    下载历史st数据
    '''
    cl = get_client()

    result = _BSON_call_common(
        cl.commonControl, 'downloadhisstdata', {}
    )
    return result


def get_hk_broker_dict():
    global __hk_broke_info

    if not __hk_broke_info:
        data = _get_market_data_ex_tuple_period_ori(['XXXXXX.HK'], (2038, 86401000), '', '')

        for a in data['XXXXXX.HK']:
            list = a['1']
            name = a['0']
            for id in list:
                __hk_broke_info[id] = name

    return __hk_broke_info


def _covert_hk_broke_data(ori_data = {}):
    broke_dict = get_hk_broker_dict()
    for s in ori_data:
        sdata = ori_data[s]
        for data in sdata:
            for Broker in data['bidbrokerqueues']:
                bidBrokerQueues = Broker['brokers']
                listbid = []
                for brokerid in bidBrokerQueues:
                    brokername = broke_dict.get(brokerid, '')
                    listbid.append((brokerid, brokername))
                Broker['brokers'] = listbid

            for Broker in data['askbrokerqueues']:
                askBrokerQueues = Broker['brokers']
                listask = []
                for brokerid in askBrokerQueues:
                    brokername = broke_dict.get(brokerid, '')
                    listask.append((brokerid, brokername))
                Broker['brokers'] = listask

    return ori_data


def get_broker_queue_data(stock_list = [], start_time = '', end_time = '', count = -1, show_broker_name = False):
    ori_data = get_market_data_ex_ori([], stock_list, 'hkbrokerqueue', start_time, end_time, count)

    if show_broker_name:
        return _covert_hk_broke_data(ori_data)
    return ori_data


def watch_xtquant_status(callback):
    '''
    监控xtquant连接状态变化

    def callback(info):
        #info: {address : 'ip:port', status: ''}
        #status: 'connected', 'disconnected'
        return
    '''
    if callback:
        callback = subscribe_callback_wrapper(callback)

    from . import xtconn
    xtconn.status_callback = callback
    return


def get_full_kline(field_list = [], stock_list = [], period = '1m'
    , start_time = '', end_time = '', count = 1
    , dividend_type = 'none', fill_data = True):
    '''
    k线全推获取最新交易日数据
    '''
    cl = get_client()

    all_data = _BSON_call_common(
        cl.commonControl
        , 'getfullkline'
        , {
            "stocklist": stock_list
            , "period": period
            , "starttime": start_time
            , "endtime": end_time
            , "count": count
            , "dividendtype": dividend_type
            , "fillData": fill_data
            , "fields": field_list
        }
    )

    import pandas as pd
    data = all_data.get('result', {})
    index = all_data.get('stock', [])
    column = all_data.get('stime', [])

    result = {}
    for field in data:
        result[field] = pd.DataFrame(data[field], index = index, columns = column)
    return result


def generate_index_data(
    formula_name, formula_param = {}
    , stock_list = [], period = '1d', dividend_type = 'none'
    , start_time = '', end_time = ''
    , fill_mode = 'fixed', fill_value = float('nan')
    , result_path = None
):
    '''
    formula_name:
        str 模型名称
    formula_param:
        dict 模型参数
            例如 {'param1': 1.0, 'param2': 'sym'}
    stock_list:
        list 股票列表
    period:
        str 周期
            '1m' '5m' '1d'
    dividend_type:
        str 复权方式
            'none' - 不复权
            'front_ratio' - 等比前复权
            'back_ratio' - 等比后复权
    start_time:
        str 起始时间 '20240101' '20240101000000'
        '' - '19700101'
    end_time:
        str 结束时间 '20241231' '20241231235959'
        '' - '20380119'
    fill_mode:
        str 空缺填充方式
            'fixed' - 固定值填充
            'forward' - 向前延续
    fill_value:
        float 填充数值
            float('nan') - 以NaN填充
    result_path:
        str 结果文件路径，feather格式
    '''
    cl = get_client()

    result = _BSON_call_common(cl.commonControl, 'createrequestid', {})
    request_id = result['result']

    result = _BSON_call_common(
        cl.commonControl
        , 'generateindexdata'
        , {
            'requestid': request_id
            , 'formulaname': formula_name
            , 'formulaparam': formula_param
            , 'stocklist': stock_list
            , 'period': period
            , 'dividendtype': dividend_type
            , 'starttime': start_time
            , 'endtime': end_time
            , 'fillmode': fill_mode
            , 'fillvalue': fill_value
            , 'resultpath': result_path
        }
    )

    taskid = result['taskid']

    status = _BSON_call_common(cl.commonControl, 'querytaskstatus', {'taskid': taskid})

    from tqdm import tqdm

    with tqdm(total = 1.0, dynamic_ncols = True) as pbar:
        totalcount = 1.0
        finishedcount = 0.0

        if not status.get('done', True):
            import time

            while not status.get('done', True):
                totalcount = status.get('totalcount', 1.0)
                finishedcount = status.get('finishedcount', 0.0)

                pbar.total = totalcount
                pbar.update(finishedcount - pbar.n)

                time.sleep(0.5)
                status = _BSON_call_common(cl.commonControl, 'querytaskstatus', {'taskid': taskid})

        pbar.update(totalcount - pbar.n)

    if status.get('errorcode', None):
        raise Exception(status)

    return



from .metatable import *

def download_tabular_data(stock_list, period, start_time = '', end_time = '', incrementally = None, download_type = 'validationbypage', source = ''):
    '''
    下载表数据，可以按条数或按时间范围下载

    stock_list: 股票列表
        - list
    period: 周期
        - str 例如 '1m' '5m' '1d'
    start_time: 起始时间
        - str 格式yyyyMMdd/yyyyMMddHHmmss，例如：
                '20240101' '20240101000000'
                '' 代表 '19700101'
        - datetime.datetime对象
    end_time: 结束时间
        - str 格式yyyyMMdd/yyyyMMddHHmmss，例如：
                '20241231' '20241231235959'
                '' 代表 '20380119'
        - datetime.datetime对象
    incrementally: 是否增量
        - bool 
    download_type: 下载类型
        - str
            'bypage' - 按条数下载
            'byregion' - 按时间范围下载
            'validatebypage' - 数据校验按条数下载
    source: 指定下载地址
        - str
    '''
    import datetime as dt

    if isinstance(start_time, dt.datetime):
        start_time = start_time.strftime('%Y%m%d%H%M%S')
    if isinstance(end_time, dt.datetime):
        end_time = end_time.strftime('%Y%m%d%H%M%S')

    if incrementally is None:
        incrementally = False if start_time else True

    if isinstance(stock_list, str):
        stock_list = [stock_list]

    if isinstance(period, tuple):
        metaid, periodNum = period
        periodstr = ''
    else:
        periodstr = period
        metaid = -1
        periodNum = -1

    param = {
        'stocklist': stock_list
        , 'period': periodstr
        , 'metaid': metaid
        , 'periodNum': periodNum
        , 'starttime' : start_time
        , 'endtime' : end_time
        , 'incrementally': incrementally
        , 'type': download_type
        , 'source': source
    }

    cl = get_client()

    result = _BSON_call_common(
        cl.commonControl, 'downloadtabulardata', param
    )

    seq = result['seq']


    status = _BSON_call_common(
        cl.commonControl, 'getdownloadworkprogress', {'seq' : seq}
    )

    from tqdm import tqdm

    with tqdm(total=1.0, dynamic_ncols=True) as pbar:
        if not status.get('done', True):
            import time

            while not status.get('done', True):
                #print(status)
                totalcount = status.get('totalcount', 1.0)
                finishedcount = status.get('finishedcount', 0.0)
                percentage = finishedcount / max(totalcount, 1.0)

                pbar.update(percentage - pbar.n)

                _TIME_.sleep(1)
                status = _BSON_call_common(
                    cl.commonControl, 'getdownloadworkprogress', {'seq': seq}
                )

        pbar.update(1.0 - pbar.n)

    if status.get('errormsg', None):
        raise Exception(status)

    return

def get_trading_contract_list(stockcode, date = None):
    '''
    获取当前主力合约可交易标的列表

    stockcode:
        str, 合约代码，需要用主力合约
    date:
        str, 查询日期， 8位日期格式，默认为最新交易日
    '''
    split_codes = stockcode.rsplit('.', 1)
    if len(split_codes) == 2:
        code = split_codes[0]
        market = split_codes[1].upper()
    else:
        return []

    if not date:
        date = timetag_to_datetime(get_market_last_trade_date(market), "%Y%m%d")
    date = int(date[:8])

    result_set = set()
    product_id = code.replace('00', '')

    stock_list = []

    if market == 'IF' or market == 'CFFEX':
        stock_list += get_stock_list_in_sector('中金所')
        stock_list += get_stock_list_in_sector('过期中金所')
    elif market == 'SF' or market == 'SHFE':
        stock_list += get_stock_list_in_sector('上期所')
        stock_list += get_stock_list_in_sector('过期上期所')
    elif market == 'DF' or market == 'DCE':
        stock_list += get_stock_list_in_sector('大商所')
        stock_list += get_stock_list_in_sector('过期大商所')
    elif market == 'ZF' or market == 'CZCE':
        stock_list += get_stock_list_in_sector('郑商所')
        stock_list += get_stock_list_in_sector('过期郑商所')
    elif market == 'GF' or market == 'GFEX':
        stock_list += get_stock_list_in_sector('广期所')
        stock_list += get_stock_list_in_sector('过期广期所')
    elif market == 'INE':
        stock_list += get_stock_list_in_sector('能源中心')
        stock_list += get_stock_list_in_sector('过期能源中心')

    for it in stock_list:
        inst = get_instrument_detail(it)
        if not inst:
            continue
        if inst['ProductID'] == product_id:
            s_date = int(inst['OpenDate'])
            e_date = inst['ExpireDate']
            if s_date and e_date and s_date <= date <= e_date:
                result_set.add(inst['ExchangeCode'] + '.' + market)

    return list(result_set)


def get_trading_period(stock_code):
    '''
    获取指定品质真实交易时间段
    stock_code: str 合约代码

    返回：dict
        {
            'market': market,
            'codeRegex': codeRegex,
            'product': [ product_code1, product_code2, ... ],
            'category': [ category1, category2, ... ],
            'tradings': [ trading_period1, trading_period2, ... ],
        }
        market: str 市场代码
        codeRegex: str 代码匹配规则
        product_code: str 产品代码
        category: int 证券分类

        通过指定stock_code获取到的交易时段信息已经是和这个品种匹配的，
        通常不需要使用codeRegex, product, category这三个字段

        trading_period: dict 交易时段
            {
                'status': status,
                'time': [
                    trading_day_offset
                    , [ begin_time, boundary_type ]
                    , [ end_time, boundary_type ]
                ],
            }

            status: int 交易时段类型
                2 - 盘前竞价
                3 - 连续交易
                8 - 尾盘竞价
                15 - 集合竞价对盘时段(港股)

            trading_day_offset: int 交易日偏移

            begin_time: int 开始时间, 时分秒
                例如 930000 表示 09:30:00
                    1130000 表示 11:30:00
                特殊地，负数表示小时为0点以前的时间，例如 -45500 表示前一个自然日的 20:55:00
                超过24小时的时间，表示24点后的时间，例如 263000 表示下一个自然日的 02:30:00

            end_time: int 结束时间, 时分秒

            boundary_type: int 边界类型, 范围为 -1 0 1
                以begin_time为例,
                -1表示边界时间点小于begin_time
                0表示边界时间点等于begin_time
                1表示边界时间点大于begin_time
                例如begin_time为93000, boundary_type为1, 则开始时间为09:30:00之后，且09:30:00不在范围内
                例如end_time为210000, boundary_type为-1, 则结束时间为21:00:00之前，且21:00:00不在范围内
    '''
    cl = get_client()

    result = _BSON_call_common(
        cl.commonControl
        , 'getopenclosetradetimebystock'
        , {
            "stockcode" : stock_code
        }
    )

    return result


def get_kline_trading_period(stock_code):
    '''
    获取指定品种的K线时段

    stock_code: str 合约代码

    返回: dict
        {
            'market': market,
            'codeRegex': codeRegex,
            'product': [ product_code1, product_code2, ... ],
            'category': [ category1, category2, ... ],
            'tradings': [ trading_period1, trading_period2, ... ],
        }

        market: str 市场代码
        codeRegex: str 代码匹配规则
        product_code: str 产品代码
        category: int 证券分类

        通过指定stock_code获取到的交易时段信息已经是和这个品种匹配的，
        通常不需要使用codeRegex, product, category这三个字段

        trading_period: dict 交易时段
            { 'type': type, ... }

            type: str 交易时段类型
                'auction' - 竞价交易
                'continuous' - 连续交易
            type为不同类型时，其余字段内容不同

            type为auction时，字段内容如下：
            {
                'type': 'auction',
                'source': [
                    trading_day_offset
                    , [ begin_time, boundary_type ]
                    , [ end_time, boundary_type ]
                ],
                'bartime': [ trading_day_offset, target_time ],
            }
            使用竞价交易时段合并K线时，任何在source范围内的tick数据都应该被视为时间点为bartime的tick数据

            type为continuous时，字段内容如下：
            {
                'type': 'continuous',
                'bartime': [ trading_day_offset, begin_time, end_time ],
            }
            使用连续交易时段合并K线时，在begin_time和end_time之间的tick数据按具体K线周期合并为K线

            trading_day_offset: int 交易日偏移

            begin_time: int 开始时间, 时分秒
                例如 930000 表示 09:30:00
                    1130000 表示 11:30:00
                特殊地，负数表示小时为0点以前的时间，例如 -45500 表示前一个自然日的 20:55:00
                超过24小时的时间，表示24点后的时间，例如 263000 表示下一个自然日的 02:30:00

            end_time: int 结束时间, 时分秒

            boundary_type: int 边界类型, 范围为 -1 0 1
                以begin_time为例,
                -1表示边界时间点小于begin_time
                0表示边界时间点等于begin_time
                1表示边界时间点大于begin_time
                例如begin_time为93000, boundary_type为1, 则开始时间为09:30:00之后，且09:30:00不在范围内
                例如end_time为210000, boundary_type为-1, 则结束时间为21:00:00之前，且21:00:00不在范围内
    '''
    cl = get_client()

    result = _BSON_call_common(
        cl.commonControl
        , 'getopencloseklinetimebystock'
        , {
            "stockcode": stock_code
        }
    )

    return result


def get_all_trading_periods():
    '''
    获取全部市场的真实交易时间段
    '''
    cl = get_client()

    result = _BSON_call_common(
        cl.commonControl
        , 'getopenclosealltradetime'
        , {
        }
    )

    return result.get('result', [])


def get_all_kline_trading_periods():
    '''
    获取全部市场的分割K线交易时间段
    '''
    cl = get_client()

    result = _BSON_call_common(
        cl.commonControl
        , 'getopencloseallklinetime'
        , {
        }
    )

    return result.get('result', [])


def get_authorized_market_list():
    '''
    获取所有已授权的市场
    返回 list
    '''
    return _BSON_call_common(get_client().commonControl, 'getauthorizedmarketlist', {}).get('result', [])


def compute_coming_trading_calendar(market, start_time = '', end_time = ''):
    '''
    未来交易日函数
    note: 历史交易日，可以使用get_trading_calendar函数
    '''
    if market not in ["SH", "SZ"]:
        raise Exception("暂不支持除SH,SZ以外市场的交易日历")

    data = _BSON_call_common(get_client().commonControl, 'getcomingtradedate', {}).get('result', [])

    import datetime as dt
    if start_time:
        ds = dt.datetime.strptime(start_time, '%Y%m%d')
    else:
        ds = dt.datetime(1980, 1, 1, 0, 0)

    if end_time:
        de = dt.datetime.strptime(end_time, '%Y%m%d')
    else:
        de = dt.datetime(2038, 12, 31, 0, 0)

    ss = ds.timestamp() * 1000
    se = de.timestamp() * 1000

    return [timetag_to_datetime(d, '%Y%m%d') for d in data if ss <= d <= se]


def get_tabular_formula(
        codes: list,
        fields: list,
        period: str,
        start_time: str,
        end_time: str,
        count: int = -1,
        dividend_type = 'none',
        **kwargs
):
    def _parse_fields(fields):
        tmp = {}  # { table: [{}] }
        idx = 1
        for field in fields:
            if field.find('.') != -1:
                table = field.split('.')[0]
                ifield = field.split('.')[1]

                if table not in tmp:
                    tmp[table] = []
                    val = {'key': '0', 'fieldNameCn': '股票代码', 'modelName': 'stock', 'type': 'string', 'unit': ''}
                    tmp[table].append(val)
                    val = {'key': '1', 'fieldNameCn': '时间戳', 'modelName': 'timetag', 'type': 'int', 'unit': ''}
                    tmp[table].append(val)

                idx += 1
                val = {'key': str(idx), 'fieldNameCn': ifield, 'modelName': ifield, 'unit': ''}
                tmp[table].append(val)

        return tmp
    all_fields = _parse_fields(fields)

    from .qmttools.functions import call_formula_batch
    formula_names = list(all_fields.keys())
    result = call_formula_batch(formula_names=formula_names, stock_codes=codes, period=period, start_time=start_time, end_time=end_time, count=count, dividend_type=dividend_type)

    from . import xtbson
    stock_formula_result = []
    for res in result:
        stock = res.get('stock', '')
        formula = res.get('formula', '')
        rst = res.get('result', {})
        fields = all_fields[formula]

        timelist = rst.get('timelist', [])
        outputs = rst.get('outputs', {})
        val = {}
        for i, t in enumerate(timelist):
            val['0'] = stock
            val['1'] = t
            for field in fields:
                if field['modelName'] in outputs:
                    if 'type' not in field:
                        t = type(outputs[field['modelName']][i])
                        if t == int:
                            field['type'] = 'int'
                        elif t == str:
                            field['type'] = 'string'
                        elif t == float:
                            field['type'] = 'double'

                    val[field['key']] = outputs[field['modelName']][i]
            stock_formula_result.append(xtbson.encode(val))

    head_fields = []
    for f in list(all_fields.values()):
        head_fields.extend(f)
    heads = {'modelName': '', 'tableNameCn': '', 'fields': head_fields}

    stock_formula_result.insert(0, xtbson.encode(heads))
    return stock_formula_result


def bnd_get_conversion_price(stock_code, start_time="", end_time=""):
    '''
    查询可转债转股价变动信息
    stock_code: str 转债代码
    start_time: str 开始时间（可不填）,格式为"%Y%m%d",默认为""
    end_time: str 结束时间（可不填）,格式为"%Y%m%d",默认为""
    '''
    return _get_tabular_data([stock_code], ['bond_conv_price_info'], '', start_time, end_time)


def bnd_get_call_info(stock_code, start_time="", end_time=""):
    '''
    查询可转债赎回信息
    stock_code: str 转债代码
    start_time: str 开始时间（可不填）,格式为"%Y%m%d",默认为""
    end_time: str 结束时间（可不填）,格式为"%Y%m%d",默认为""
    '''
    return _get_tabular_data([stock_code], ['bond_call_info'], '', start_time, end_time)


def bnd_get_put_info(stock_code, start_time="", end_time=""):
    '''
    查询可转债回售信息
    stock_code: str 转债代码
    start_time: str 开始时间（可不填）,格式为"%Y%m%d",默认为""
    end_time: str 结束时间（可不填）,格式为"%Y%m%d",默认为""
    '''
    return _get_tabular_data([stock_code], ['bond_put_info'], '', start_time, end_time)


def bnd_get_amount_change(stock_code, start_time="", end_time=""):
    '''
    查询可转债剩余规模变动
    stock_code: str 转债代码
    start_time: str 开始时间（可不填）,格式为"%Y%m%d",默认为""
    end_time: str 结束时间（可不填）,格式为"%Y%m%d",默认为""
    '''
    return _get_tabular_data([stock_code], ['bond_amount_chg'], '', start_time, end_time)


def get_tabular_data(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True
):
    '''
    获取历史行情数据
    :param field_list: 行情数据字段列表，[]为全部字段
        K线可选字段：
            "time"                #时间戳
            "open"                #开盘价
            "high"                #最高价
            "low"                 #最低价
            "close"               #收盘价
            "volume"              #成交量
            "amount"              #成交额
            "settle"              #今结算
            "openInterest"        #持仓量
    :param stock_list: 股票代码 "000001.SZ"
    :param period: 周期 分钟线"1m"/"5m"/"15m" 日线"1d"
    :param start_time: 起始时间 "20200101" "20200101093000"
    :param end_time: 结束时间 "20201231" "20201231150000"
    :param count: 数量 -1全部/n: 从结束时间向前数n个
    :param dividend_type: 除权类型"none" "front" "back" "front_ratio" "back_ratio"
    :param fill_data: 对齐时间戳时是否填充数据，仅对K线有效，分笔周期不对齐时间戳
        为True时，以缺失数据的前一条数据填充
            open、high、low、close 为前一条数据的close
            amount、volume为0
            settle、openInterest 和前一条数据相同
        为False时，缺失数据所有字段填NaN
    :return: pd.DataFrame 字段对应的数据，各字段维度相同，index为为time_list,包含symbol列
    '''
    if period in {'1m', '5m', '15m', '30m', '60m', '1h', '1d', '1w', '1mon', '1q', '1hy', '1y'}:
        return _get_market_data_ex_250414(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data)

    return _get_tabular_data(stock_list, field_list, period, start_time, end_time, count, dividend_type=dividend_type, fill_data=fill_data)
    
def get_order_rank(code, order_time, order_type, order_price, order_volume, order_left_volume):
    '''
    获取委托在千档队列中的排名, 需要订阅千档数据，并且数据源为本地计算的千档数据
    :param code: 股票代码 e.g. "000001.SZ"
    :param order_time: 委托时间 支持以下格式:
        - str格式: YYYYMMDD/YYYYMMDDhhmmss，e.g."20200427" "20200427093000"
        - datetime对象
    :param order_type: 委托类型，即买卖方向，e.g.'buy' 'sell'
    :param order_price: 委托价格
    :param order_volume: 委托量
    :param order_left_volume: 委托未成量
    :return: dict
        pricerank：价内排名
    '''

    import datetime as dt
    if isinstance(order_time, dt.datetime):
        timetag = int(order_time.timestamp() * 1000)
    elif isinstance(order_time, str):
        if len(order_time) == 8:  # YYYYMMDD
            timetag = int(dt.datetime.strptime(order_time, "%Y%m%d").timestamp() * 1000)
        elif len(order_time) == 14:  # YYYYMMDDhhmmss
            timetag = int(dt.datetime.strptime(order_time, "%Y%m%d%H%M%S").timestamp() * 1000)

    param = {}
    param['stockcode'] = code
    param['time'] = timetag
    param['type'] = order_type
    param['price'] = order_price
    param['volume'] = order_volume
    param['leftvolume'] = order_left_volume

    return _BSON_call_common(get_client().commonControl, 'getorderrank', param)


def get_current_connect_sub_info():
    '''
    获取当前连接订阅的数据信息
    返回: list
    例如：[{'subId': 1, 'stockCode': '000001.SZ', 'period': 60000, 'metaId': 3001, 'isSub': True}]
    '''
    return _BSON_call_common(get_client().commonControl, 'getcurrentsubinfo', {}).get('result', [])


def get_all_sub_info():
    '''
    获取客户端所有订阅信息
    返回: list
    例如: [{'addr': '42.228.16.210:55300', 'stockCode': '000300.SH', 'period': 60000, 'metaId': 3001}]
    '''
    return _BSON_call_common(get_client().commonControl, 'getallsubinfo', {}).get('result', [])
