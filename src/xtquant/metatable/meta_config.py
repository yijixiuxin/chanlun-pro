#coding:utf8

__TABULAR_PERIODS__ = {
    '': 0,
    '1m': 60000,
    '5m': 300000,
    '15m': 900000,
    '30m': 1800000,
    '60m': 3600000,
    '1h': 3600000,
    '1d': 86400000,
    '1w': 604800000,
    '1mon': 2592000000,
    '1q': 7776000000,
    '1hy': 15552000000,
    '1y': 31536000000,
}

__META_INFO__ = {}
__META_FIELDS__ = {}
__META_TABLES__ = {}

def download_metatable_data():
    '''
    下载metatable信息
    通常在客户端启动时自动获取，不需要手工调用
    '''
    from .. import xtdata
    cl = xtdata.get_client()

    ret = xtdata._BSON_call_common(
        cl.commonControl, 'downloadmetatabledata', {}
    )
    return ret

def _init_metainfos():
    '''
    初始化metatable
    '''
    import traceback
    from .. import xtdata, xtbson

    global __META_INFO__
    global __META_FIELDS__
    global __META_TABLES__

    cl = xtdata.get_client()
    result = xtbson.BSON.decode(cl.commonControl('getmetatabledatas', xtbson.BSON.encode({})))
    all_metainfos = result['result']

    for metainfo in all_metainfos:
        if not isinstance(metainfo, dict):
            continue

        try:
            metaid = metainfo['I']
            __META_INFO__[metaid] = metainfo

            table_name = metainfo.get('modelName', metaid)
            table_name_cn = metainfo.get('tableNameCn', '')

            __META_TABLES__[table_name] = metaid
            __META_TABLES__[table_name_cn] = metaid

            metainfo_fields = metainfo.get('fields', {})
            # metainfo_fields.pop('G', None)  # G公共时间字段特殊处理，跳过
            for key, info in metainfo_fields.items():
                field_name = info['modelName']
                __META_FIELDS__[f'{table_name}.{field_name}'] = (metaid, key)
        except:
            traceback.print_exc()
            continue
    return

def _check_metatable_key(metaid, key):
    metainfo = __META_INFO__.get(metaid, None)
    if not metainfo:
        return False

    fields = metainfo.get('fields', {})
    return key in fields


def get_metatable_list():
    '''
    获取metatable列表

    return:
        { table_code1: table_name1, table_code2: table_name2, ... }

        table_code: str
            数据表代码
        table_name: str
            数据表名称
    '''
    if not __META_INFO__:
        _init_metainfos()

    ret = {}
    for metaid, metainfo in __META_INFO__.items():
        model_name = metainfo.get('modelName', f'{metaid}')
        table_name_desc = metainfo.get('tableNameCn', '')
        ret[model_name] = table_name_desc

    return ret


def get_metatable_config(table):
    '''
    获取metatable列表原始配置信息
    '''
    if not __META_INFO__:
        _init_metainfos()

    if table not in __META_TABLES__:
        print(f'[ERROR] Unknown table {table}')

    metaid = __META_TABLES__[table]
    return __META_INFO__[metaid]


__META_TYPECONV__ = {
    'int': int(),
    'long': int(),
    'double': float(),
    'string': str(),
    'binary': bytes(),
}


def _meta_type(t):
    try:
        return __META_TYPECONV__[t]
    except:
        raise Exception(f'Unsupported type:{t}')


def get_metatable_info(table):
    '''
    获取metatable数据表信息

    table: str
        数据表代码 table_code 或 数据表名称 table_name
    return: dict
        {
            'code': table_code
            , 'name': table_name
            , 'desc': desc
            , 'fields': fields
        }

        table_code: str
            数据表代码
        table_name: str
            数据表名称
        desc: str
            描述
        fields: dict
            { 'code': field_code, 'name': field_name, 'type': field_type }
    '''
    info = get_metatable_config(table)

    fields = info.get('fields', {})
    ret = {
        'code': info.get('modelName', ''),
        'name': info.get('tableNameCn', ''),
        'desc': info.get('desc', ''),
        'fields': [
            {
                'code': field_info.get('modelName', ''),
                'name': field_info.get('fieldNameCn', ''),
                'type': type(_meta_type(field_info.get('type', ''))),
            } for key, field_info in fields.items()
        ],
    }
    return ret


def get_metatable_fields(table):
    '''
    获取metatable数据表字段信息

    table: str
        数据表代码 table_code 或 数据表名称 table_name
    return: pd.DataFrame
        columns = ['code', 'name', 'type']
    '''
    import pandas as pd
    info = get_metatable_config(table)

    fields = info.get('fields', {})
    ret = pd.DataFrame([{
        'code': field_info.get('modelName', ''),
        'name': field_info.get('fieldNameCn', ''),
        'type': type(_meta_type(field_info.get('type', ''))),
    } for key, field_info in fields.items()])
    return ret

