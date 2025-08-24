#coding:utf-8

from . import xtbson as _BSON_

### connection

__client = None
__client_last_spec = ('', None)


def connect(ip = '', port = None, remember_if_success = True):
    global __client

    if __client:
        if __client.is_connected():
            return __client

        __client.shutdown()
        __client = None

    from . import xtconn

    start_port = 0
    end_port = 65535

    if isinstance(port, tuple):
        start_port = port[0]
        end_port = port[1]

    if start_port > end_port:
        start_port, end_port = end_port, start_port

    if not ip:
        ip = 'localhost'

    if port:
        server_list = [f'{ip}:{port}']
        __client = xtconn.connect_any(server_list, start_port, end_port)
    else:
        server_list = xtconn.scan_available_server_addr()

        default_addr = 'localhost:58610'
        if not default_addr in server_list:
            server_list.append(default_addr)

        __client = xtconn.connect_any(server_list, start_port, end_port)

    if not __client or not __client.is_connected():
        raise Exception("无法连接xtquant服务，请检查QMT-投研版或QMT-极简版是否开启")

    if remember_if_success:
        global __client_last_spec
        __client_last_spec = (ip, port)

    return __client


def reconnect(ip = '', port = None, remember_if_success = True):
    global __client

    if __client:
        __client.shutdown()
        __client = None

    return connect(ip, port, remember_if_success)


def get_client():
    global __client

    if not __client or not __client.is_connected():
        global __client_last_spec

        ip, port = __client_last_spec
        __client = connect(ip, port, False)

    return __client


### utils
def try_except(func):
    import sys
    import traceback

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            exc_type, exc_instance, exc_traceback = sys.exc_info()
            formatted_traceback = ''.join(traceback.format_tb(exc_traceback))
            message = '\n{0} raise {1}:{2}'.format(
                formatted_traceback,
                exc_type.__name__,
                exc_instance
            )
            # raise exc_type(message)
            print(message)
            return None

    return wrapper

def _BSON_call_common(interface, func, param):
    return _BSON_.BSON.decode(interface(func, _BSON_.BSON.encode(param)))

def create_view(viewID, view_type, title, group_id):
    client = get_client()
    return client.createView(viewID, view_type, title, group_id)

#def reset_view(viewID):
#    return

def close_view(viewID):
    client = get_client()
    return client.closeView(viewID)

#def set_view_index(viewID, datas):
#    '''
#    设置模型指标属性
#    index: { "output1": { "datatype": se::OutputDataType } }
#    '''
#    client = get_client()
#    return client.setViewIndex(viewID, datas)

def push_view_data(viewID, datas):
    '''
    推送模型结果数据
    datas: { "timetags: [t1, t2, ...], "outputs": { "output1": [value1, value2, ...], ... }, "overwrite": "full/increase" }
    '''
    client = get_client()
    bresult = client.pushViewData(viewID, 'index', _BSON_.BSON.encode(datas))
    return _BSON_.BSON.decode(bresult)

def switch_graph_view(stock_code = None, period = None, dividendtype = None, graphtype = None):
    cl = get_client()

    result = _BSON_call_common(
        cl.commonControl, 'switchgraphview'
        , {
            "stockcode": stock_code
            , "period": period
            , "dividendtype": dividendtype
            , "graphtype": graphtype
        }
    )

def add_schedule(schedule_name, begin_time = '', finish_time = '', interval = 60, run = False, only_work_date = False, always_run = False):
    """
    ToDo: 向客户端添加调度任务
    Args:
        schedule_name(str): 调度任务的方案名

        begin_time(str) : 定时下载开始的时间 格式为'%H%M%S'

        interval(int) : 下载任务执行间隔，单位为秒, 例如要每24小时执行一次则填写 60*60*24

        run(bool) : 是否自动运行, 默认为False

        only_work_date(bool) : 是否仅工作日运行, 默认为False

        always_run(bool) : 当前时间超过设定时间的情况下是否运行, 默认为False

    Return:
        None
    Example::

        # 向客户端添加一个每日下载沪深A股市场的日K任务
        from xtquant import xtview, xtdata
        stock_list = xtdata.get_stock_list_in_sector("沪深A股")
        xtview.add_schedule(
            schedule_name = "test计划",
            begin_time ="150500",
            interval = 60*60*24,
            run = True,
            only_work_date = True,
            always_run = False)

    """

    cl = get_client()

    result = _BSON_call_common(
        cl.commonControl, 'addschedule'
        , {
            'name': schedule_name
            , 'starttime': -1 if begin_time == '' else int(begin_time)
            , 'endtime': -1
            , 'interval': interval * 1000
            , 'run': run
            , 'onlyworkdate': only_work_date
            , 'alwaysrun': always_run
        }
    )

def add_schedule_download_task(schedule_name, stock_code = [], period = '', recentday = 0, start_time = '', end_time = '', incrementally = False):
    '''
    Args:
        stock_code(list)  : 下载标的的code组成的list

        period(str) : 下载数据的周期, 参考gmd

        recentday(int) : *仅在非增量的情况下生效*
            下载当日往前 N 个周期的数据, 当此参数不为0时,start_time不生效, 否则下载全部数据

        start_time(str) : 下载数据的起始时间 格式为 '%Y%m%d' 或者 '%Y%m%d%H%M%S'

        end_time(str) :  下载数据的结束时间 格式为 '%Y%m%d' 或者 '%Y%m%d%H%M%S'

        incrementally(bool) : 是否增量下载, 默认为False

    Return:
        None
    Example::
        # 向客户端现存的调度方案中添加一个下载任务
        xtview.add_schedule_download_task(
            schedule_name = "test计划",
            stock_code = stock_list
            period = "1d" )
    """
    '''

    d_stockcode = {}
    for stock in stock_code:
        sp_stock = stock.split('.')
        if len(sp_stock) == 2:
            if sp_stock[1] not in d_stockcode:
                d_stockcode[sp_stock[1]] = []

            d_stockcode[sp_stock[1]].append(sp_stock[0])
        else:
            d_stockcode[stock] = []

    cl = get_client()

    result = _BSON_call_common(
        cl.commonControl, 'addscheduledownloadtask'
        , {
            'name': schedule_name
            , 'market': list(d_stockcode.keys())
            , 'stockcode': list(d_stockcode.values())
            , 'period': period
            , 'recentday': recentday
            , 'starttime': start_time
            , 'endtime': end_time
            , 'incrementally': incrementally
        }
    )
    return

def modify_schedule_task(schedule_name, begin_time = '', finish_time = '', interval = 60, run = False, only_work_date = False, always_run = False):
    cl = get_client()

    result = _BSON_call_common(
        cl.commonControl, 'modifyschedule'
        , {
            'name': schedule_name
            , 'starttime': -1 if begin_time == '' else int(begin_time)
            , 'endtime': -1
            , 'interval': interval * 1000
            , 'run': run
            , 'onlyworkdate': only_work_date
            , 'alwaysrun': always_run
        }
    )

def remove_schedule(schedule_name):
    cl = get_client()

    result = _BSON_call_common(
        cl.commonControl, 'removeschedule'
        , {
            'name': schedule_name
        }
    )
    return

def remove_schedule_download_task(schedule_name, task_id):
    cl = get_client()

    result = _BSON_call_common(
        cl.commonControl, 'removescheduledownloadtask'
        , {
            'name': schedule_name
            , 'taskids': task_id
        }
    )
    return

def query_schedule_task():
    cl = get_client()

    inst = _BSON_call_common(
        cl.commonControl, 'queryschedule', {}
    )

    return inst.get('result', [])


def push_xtview_data(data_type, time, datas):
    cl = get_client()
    timeData = 0
    types = []
    numericDatas = []
    stringDatas = []
    if type(time) == int:
        name_list = list(datas.keys())
        value_list = []
        for name in name_list:
            value_list.append([datas[name]])
        timeData = [time]
    if type(time) == list:
        time_list = time
        name_list = list(datas.keys())
        value_list = list(datas.values())
        timeData = time_list

    for value in value_list:
        if isinstance(value[0], str):
            stringDatas.append(value)
            types.append(3)
        else:
            numericDatas.append(value)
            types.append(0)

    result = _BSON_call_common(
        cl.custom_data_control, 'pushxtviewdata'
        , {
            'dataType': data_type
            ,'timetags': timeData
            , 'names' : name_list
            , 'types' : types
            , 'numericDatas' : numericDatas
            , 'stringDatas' : stringDatas
        }
    )
    return


class UIPanel:
    code = ''
    period = '1d'
    figures = []
    startX = -1
    startY = -1
    width = -1
    height = -1

    def __init__(self, code, period = '1d', figures = [], startX = -1, startY = -1, width = -1, height = -1):
        self.code = code
        self.period = period
        self.figures = figures
        self.startX = startX
        self.startY = startY
        self.width = width
        self.height = height


def apply_ui_panel_control(info: list):
    '''
    控制主图界面展示
    用法：
    apply_ui_panel_control(info:list[UIPanel])

    参数：
    info,list[UIPanel]类型,每个UIPanel为一个行情页面,code为必填项
    code:str,代码市场,为必填项
    period:str,周期,'tick','1m','5m','1d'等
    figures:list,内部存放附图指标名称
    startX: int, 距屏幕左上角横坐标的位置
    startY: int, 距屏幕左上角纵坐标的位置
    width: int, 宽度
    height: int, 高度

    示例：
    from xtquant import xtview
    x=xtview.UIPanel('600000.SH','1d', figures=[{'ma': {'n1': 5}}])
    y=xtview.UIPanel(code='600030.SH',period='1m',startX=-1,startY=-1, width=-1, height=-1)
    xtview.apply_ui_panel_control([x,y])
    '''
    data = []
    for i in info:
        data.append(i.__dict__)

    result = _BSON_call_common(get_client().commonControl, 'applyuipanelcontrol', {'data': data})
    return
