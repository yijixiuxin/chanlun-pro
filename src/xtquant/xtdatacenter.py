#coding:utf-8

import os as _OS_

from . import datacenter as __dc

__all__ = [
    'set_token',
    'set_data_home_dir',
    'init',
    'shutdown',
    'listen',
    'get_local_server_port',
    'register_create_nparray',
    'try_create_client',
    'RPCClient',
]

### config

__curdir = _OS_.path.dirname(_OS_.path.abspath(__file__))

__rpc_config_dir = _OS_.path.join(__curdir, 'config')
__rpc_config_file = _OS_.path.join(__curdir, 'xtdata.ini')
__rpc_init_status = __dc.rpc_init(__rpc_config_dir)
if __rpc_init_status < 0:
    raise Exception(f'rpc init failed, error_code:{__rpc_init_status}, configdir:{__rpc_config_dir}')

__config_dir = _OS_.path.join(__curdir, 'config')
__data_home_dir = 'data'

__quote_token = ''

init_complete = False

### function
get_local_server_port = __dc.get_local_server_port
register_create_nparray = __dc.register_create_nparray
RPCClient = __dc.IPythonApiClient

def try_create_client():
    '''
    尝试创建RPCClient，如果失败，会抛出异常
    '''
    cl = RPCClient()
    cl.init()

    ec = cl.load_config(__rpc_config_file, 'client_xtdata')
    if ec < 0:
        raise f'load config failed, file:{__rpc_config_file}'
    return cl


def set_token(token = ''):
    '''
    设置用于登录行情服务的token，此接口应该先于init调用
    token获取地址：https://xuntou.net/#/userInfo?product=xtquant
    迅投投研服务平台 - 用户中心 - 个人设置 - 接口TOKEN
    '''
    global __quote_token
    __quote_token = token
    return


def set_data_home_dir(data_home_dir):
    '''
    设置数据存储目录，此接口应该先于init调用
    datacenter启动后，会在data_home_dir目录下建立若干目录存储数据
    如果不设置存储目录，会使用默认路径
    在datacenter作为独立行情服务的场景下，data_home_dir可以任意设置
    如果想使用现有数据，data_home_dir对应QMT的f'{安装目录}'，或对应极简模式的f'{安装目录}/userdata_mini'
    '''
    global __data_home_dir
    __data_home_dir = data_home_dir
    return


def set_config_dir(config_dir):
    '''
    设置配置文件目录，此接口应该先于init调用
    通常情况配置文件内置，不需要调用这个接口
    '''
    global __config_dir
    __config_dir = config_dir
    return


def set_kline_mirror_enabled(enable):
    '''
    设置K线全推功能是否开启，此接口应该先于init调用
    此功能默认关闭，启用后，实时K线数据将优先从K线全推获取
    此功能仅vip用户可用
    '''
    __dc.set_kline_mirror_enabled(['SH', 'SZ'] if enable else [])
    return


def set_kline_mirror_markets(markets):
    '''
    设置开启指定市场的K线全推，此接口应该先于init调用
    此功能默认关闭，启用后，实时K线数据将优先从K线全推获取
    此功能仅vip用户可用

    markets: list, 市场列表
        例如 ['SH', 'SZ', 'BJ'] 为开启上交所、深交所、北交所的K线全推
    '''
    __dc.set_kline_mirror_enabled(markets)
    return


def set_allow_optmize_address(allow_list = []):
    '''
    设置连接池，此接口应该先于init调用
    设置后，行情仅从连接池内的地址中选择连接，并且使用第一个地址作为全推行情地址
    地址格式为'127.0.0.1:55300'
    设置为空时，行情从全部的可用地址中选择连接
    '''
    __dc.set_allow_optmize_address(allow_list)
    return


def set_wholequote_market_list(market_list = []):
    '''
    设置启动时加载全推行情的市场，此接口应该先于init调用
    未设置时启动时不加载全推行情
    未加载全推行情的市场，会在实际使用数据的时候加载

    markets: list, 市场列表
        例如 ['SH', 'SZ', 'BJ'] 为启动时加载上交所、深交所、北交所的全推行情
    '''
    __dc.set_wholequote_market_list(market_list)
    return


def set_future_realtime_mode(enable):
    '''
    设置期货周末夜盘是否使用实际时间，此接口应该先于init调用
    '''
    __dc.set_future_realtime_mode(enable)
    return


def set_init_markets(markets = []):
    '''
    设置初始化的市场列表，仅加载列表市场的合约，此接口应该先于init调用

    markets: list, 市场列表
        例如 ['SH', 'SZ', 'BJ'] 为加载上交所、深交所、北交所的合约
        传空list时，加载全部市场的合约

    未设置时，默认加载全部市场的合约
    '''
    __dc.set_watch_market_list(markets)
    return


def set_index_mirror_enabled(enable):
    '''
        设置指标全推功能是否开启，此接口应该先于init调用
        此功能默认关闭
    '''
    __dc.set_index_mirror_enabled(['SH', 'SZ', 'SHO', 'SZO', 'IF', 'DF', 'SF', 'ZF', 'GF', 'INE'] if enable else [])
    return


def set_index_mirror_markets(markets):
    '''
        设置开启指定市场的指标全推，此接口应该先于init调用
        此功能默认关闭

        markets: list, 市场列表
            例如 ['SH', 'SZ', 'BJ'] 为开启上交所、深交所、北交所的指标全推
    '''
    __dc.set_index_mirror_enabled(markets)
    return


def set_kline_cutting_mode(mode):
    '''
    设置多周期数据切割按累计交易时间还是固定交易间隔，此接口应该先于init调用
    mode: str, 切割方式
        'accumulate',累计交易时间
        'fixed',固定交易间隔
        例如 期货交易时间10:00-10:15, 10:30-11:30， 以30分钟k线切割划分为依据
            按累计交易时间方式结果为10:00-10:45, 10:45-11:15，累计够30分钟，做一次切割，默认使用当前方式
            按固定交易间隔方式结果为10:00-10:15, 10:30-11:00，是按固定30分钟切割划分一次
    默认按累计交易时间
    '''
    __dc.set_kline_cutting_mode(mode)


def set_quote_time_mode_v2(enable):
    '''
        设置是否用新版交易时段，此接口应该先于init调用
    '''
    __dc.set_quote_time_version(enable)

def set_thousand_source_mode(mode):
    '''
    设置千档数据源模式：
    mode: str, 千档数据源模式
        'server',使用服务计算的千档数据
        'local' ,使用本地计算的千档数据
    示例：
        from xtquant import xtdatacenter as xtdc
        xtdc.set_thousand_source_mode('server') # 使用服务计算的千档数据
        xtdc.set_thousand_source_mode('local')  # 使用本地计算的千档数据

    默认使用服务计算的千档数据
    '''
    __dc.set_thousand_source_mode(mode)

def init(start_local_service = True):
    '''
    初始化行情模块
    start_local_service: bool
        如果start_local_service为True，会额外启动一个默认本地监听，以支持datacenter作为独立行情服务时的xtdata内置连接
    '''
    import time

    __dc.set_config_dir(str(__config_dir))
    __dc.set_data_home_dir(str(__data_home_dir))
    __dc.set_token(__quote_token)
    __dc.log_init()
    __dc.start_init_quote()

    status = __dc.get_status()
    while not status.get('init_done', False):
        status = __dc.get_status()
        time.sleep(0.5)

    from . import xtbson as bson

    result = __dc.fetch_auth_markets()

    if result['done'] == 0:
        status = bson.decode(__dc.fetch_server_list_status())

        status_show = {}

        for info in status.values():
            srv_addr = info['loginparam']['ip'] + ':' + str(info['loginparam']['port'])

            if info['errorcode'] != 0:
                status_show[srv_addr] = info['boerror']
            else:
                status_show[srv_addr] = info['resultdesc']
                if info['resultcode'] != 0:
                    status_show[srv_addr] = status_show[srv_addr] + ', ' + info['reason']

        raise ConnectionError(f'行情连接初始化异常, 未获取到市场权限, 当前连接状态:{status_show}')

    market_keys = result.get('markets', [])
    '''
    market_keys = [
        'SH', 'SZ'
        , 'IF', 'SF', 'DF', 'ZF', 'GF', 'INE'
    ]
    '''

    result = __dc.fetch_init_result([f'0_{mkt}_L1' for mkt in market_keys])

    for mkt, boinfo in result.items():
        info = bson.decode(boinfo)

        if info['done'] == 1:
            if info['errorcode'] != 0:
                srv_addr = info['loginparam']['ip'] + ':' + str(info['loginparam']['port'])
                error = info['boerror']

                raise ConnectionError(f'行情连接初始化异常 {mkt}, {srv_addr} {error}')

            if info['resultcode'] != 0:
                srv_addr = info['loginparam']['ip'] + ':' + str(info['loginparam']['port'])
                error = info['resultdesc']
                reason = info['reason']

                raise ConnectionError(f'行情连接初始化异常 {mkt}, {srv_addr} {error} {reason}')
        else:
            status = bson.decode(__dc.fetch_server_list_status())

            status_show = {}

            for info in status.values():
                srv_addr = info['loginparam']['ip'] + ':' + str(info['loginparam']['port'])

                if info['errorcode'] != 0:
                    status_show[srv_addr] = info['boerror']
                else:
                    status_show[srv_addr] = info['resultdesc']
                    if info['resultcode'] != 0:
                        status_show[srv_addr] = status_show[srv_addr] + ', ' + info['reason']

            raise ConnectionError(f'行情连接初始化异常 {mkt}, 未找到支持该市场的连接, 当前连接状态:{status_show}')

    global init_complete
    init_complete = True

    if start_local_service:
        listen('127.0.0.1', 58609)
    return


def shutdown():
    '''
    关闭行情模块，停止所有服务和监听端口
    '''
    __dc.shutdown()
    return


def listen(ip = '0.0.0.0', port = 58610):
    '''
    独立行情服务模式，启动监听端口，支持xtdata.connect接入
    ip:
        str, '0.0.0.0'
    port:
        int, 指定监听端口
        tuple, 指定监听端口范围，从port[0]至port[1]逐个尝试监听
    返回:
        (ip, port), 表示监听的结果
    示例:
        from xtquant import xtdatacenter as xtdc
        ip, port = xtdc.listen('0.0.0.0', 58610)
        ip, port = xtdc.listen('0.0.0.0', (58610, 58620))
    '''
    global init_complete
    if not init_complete:
        raise Exception("尚未初始化, 请优先调用init进行初始化")

    if isinstance(port, tuple):
        port_start, port_end = port
        result = __dc.listen(ip, port_start, port_end)
    else:
        result = __dc.listen(ip, port, port)

    if result[1] == 0:
        raise OSError(f'监听端口失败: {port}, 请检查端口是否被占用')

    return result
