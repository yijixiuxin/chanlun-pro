# coding:utf-8
from collections import OrderedDict

from .meta_config import (
    __TABULAR_PERIODS__,
    __META_FIELDS__,
    __META_TABLES__,
    __META_INFO__,
    _init_metainfos,
    _meta_type,
    _check_metatable_key,
)


def parse_request_from_fields(fields):
    '''
    根据字段解析metaid和field
    '''
    table_field = OrderedDict()  # {metaid: {key}}
    key2field = OrderedDict()  # {metaid: {key: field}}
    columns = []  # table.field
    if not __META_FIELDS__:
        _init_metainfos()

    for field in fields:
        if field.find('.') == -1:  # 获取整个table的数据
            metaid = __META_TABLES__[field]
            if metaid in __META_INFO__:
                metainfo = __META_INFO__[metaid]
                table = metainfo['modelName']
                meta_table_fields = metainfo.get('fields', {})
                if not meta_table_fields:
                    continue

                table_field[metaid] = {k: _meta_type(v['type']) for k, v in meta_table_fields.items()}
                key2field[metaid] = {
                    key: f'{table}.{field_info["modelName"]}' for key, field_info in meta_table_fields.items()
                }
                columns.extend(key2field[metaid].values())

        elif field in __META_FIELDS__:
            metaid, key = __META_FIELDS__[field]
            metainfo = __META_INFO__[metaid]

            if metaid not in table_field:
                table_field[metaid] = {}
            table_field[metaid][key] = _meta_type(metainfo['fields'][key]['type'])

            if metaid not in key2field:
                key2field[metaid] = {}
            key2field[metaid][key] = field

            columns.append(field)

    return table_field, key2field, columns




def _get_tabular_data_single_ori(
        codes: list,
        metaid: int,
        keys: list,
        int_period: int,
        start_time: str,
        end_time: str,
        count: int = -1,
        **kwargs
):
    from .. import xtbson, xtdata
    import os
    CONSTKEY_CODE = 'S'

    ret_datas = []

    scan_whole = False
    scan_whole_filters = dict()    # 额外对全市场数据的查询 { field : [codes] }
    client = xtdata.get_client()
    def read_single():
        nonlocal codes, metaid, int_period, scan_whole, scan_whole_filters, client, keys, ret_datas
        if not codes:
            scan_whole = True
            return

        data_path_dict = xtdata._get_data_file_path(codes, (metaid, int_period))
        print(data_path_dict)
        for code, file_path in data_path_dict.items():
            if not file_path:
                continue

            if not os.path.exists(file_path): # 如果file_path不存在
                if code == 'XXXXXX.XX': # 不处理代码为XXXXXX.XX的情况
                    continue

                if not _check_metatable_key(metaid, CONSTKEY_CODE): # 不处理不含S字段的表
                    continue

                if CONSTKEY_CODE not in scan_whole_filters:
                    scan_whole_filters[CONSTKEY_CODE] = []
                    scan_whole = True
                scan_whole_filters[CONSTKEY_CODE].append(code)
                continue

            bson_datas = client.read_local_data(file_path, start_time, end_time, count)

            for data in bson_datas:
                idata = xtbson.decode(data)
                ndata = {k: idata[k] for k in keys if k in idata}
                ret_datas.append(ndata)

    def read_whole():
        nonlocal scan_whole, scan_whole_filters, metaid, int_period, client, keys, ret_datas
        if not scan_whole:
            return

        data_path_dict = xtdata._get_data_file_path(['XXXXXX.XX'], (metaid, int_period))
        if 'XXXXXX.XX' not in data_path_dict:
            return
        file_path = data_path_dict['XXXXXX.XX']
        if not os.path.exists(file_path):
            return

        bson_datas = client.read_local_data(file_path, start_time, end_time, -1)
        data_c = count
        for data in bson_datas:
            idata = xtbson.decode(data)

            valid = True
            for k, v in scan_whole_filters.items():
                if idata.get(k, None) not in v:
                    valid = False
                    break

            if not valid:
                continue

            ndata = {k: idata[k] for k in keys if k in idata}
            ret_datas.append(ndata)

            data_c -= 1
            if data_c == 0:
                break

    read_single()
    read_whole()

    return ret_datas


def get_tabular_data(
        codes: list,
        fields: list,
        period: str,
        start_time: str,
        end_time: str,
        count: int = -1,
        **kwargs
):
    import pandas as pd

    time_format = None
    if period in ('1m', '5m', '15m', '30m', '60m', '1h'):
        time_format = '%Y-%m-%d %H:%M:%S'
    elif period in ('1d', '1w', '1mon', '1q', '1hy', '1y'):
        time_format = '%Y-%m-%d'
    elif period == '':
        time_format = '%Y-%m-%d %H:%M:%S.%f'

    if not time_format:
        raise Exception('Unsupported period')

    int_period = __TABULAR_PERIODS__[period]

    if not isinstance(count, int) or count == 0:
        count = -1

    table_field, key2field, ori_columns = parse_request_from_fields(fields)

    dfs = []

    # 额外查询 { metaid : [codes] }
    for metaid, keys in table_field.items():
        datas = _get_tabular_data_single_ori(codes, metaid, list(keys.keys()), int_period, start_time, end_time, count)
        df = pd.DataFrame(datas)
        if df.empty:
            continue

        # 补充请求的字段
        default_null_columns = [c for c in keys if c not in df.columns]
        for c in default_null_columns:
            df.loc[:, c] = keys[c]

        df.rename(columns=key2field[metaid], inplace=True)
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    result = pd.concat(dfs, ignore_index=True)
    result = result[ori_columns]

    return result


def get_tabular_bson_head(
        fields: list
):
    '''
    根据字段解析表头
    '''
    ret = {'modelName': '', 'tableNameCn': '', 'fields': []}

    if not __META_FIELDS__:
        _init_metainfos()

    for field in fields:
        if field.find('.') == -1:  # 获取整个table的数据
            metaid = __META_TABLES__[field]
            if metaid not in __META_INFO__:
                continue

            metainfo = __META_INFO__[metaid]
            meta_table_fields = metainfo.get('fields', {})
            ret['metaId'] = metaid
            ret['modelName'] = metainfo['modelName']
            ret['tableNameCn'] = metainfo['tableNameCn']
            if not meta_table_fields:
                continue

            for k, v in meta_table_fields.items():
                ret['fields'].append({
                    'key': k,
                    'fieldNameCn': v['fieldNameCn'],
                    'modelName': v['modelName'],
                    'type': v['type'],
                    'unit': v['unit'],
                })


        elif field in __META_FIELDS__:
            metaid, key = __META_FIELDS__[field]
            metainfo = __META_INFO__[metaid]
            ret['metaId'] = metaid
            ret['modelName'] = metainfo['modelName']
            ret['tableNameCn'] = metainfo['tableNameCn']
            field_metainfo = metainfo['fields'][key]
            ret['fields'].append({
                'key': key,
                'fieldNameCn': field_metainfo['fieldNameCn'],
                'modelName': field_metainfo['modelName'],
                'type': field_metainfo['type'],
                'unit': field_metainfo['unit'],
            })

    return ret


def get_tabular_bson(
        codes: list,
        fields: list,
        period: str,
        start_time: str,
        end_time: str,
        count: int = -1,
        **kwargs
):
    from .. import xtbson
    time_format = None
    if period in ('1m', '5m', '15m', '30m', '60m', '1h'):
        time_format = '%Y-%m-%d %H:%M:%S'
    elif period in ('1d', '1w', '1mon', '1q', '1hy', '1y'):
        time_format = '%Y-%m-%d'
    elif period == '':
        time_format = '%Y-%m-%d %H:%M:%S.%f'

    if not time_format:
        raise Exception('Unsupported period')

    int_period = __TABULAR_PERIODS__[period]

    if not isinstance(count, int) or count == 0:
        count = -1

    table_field, key2field, ori_columns = parse_request_from_fields(fields)

    ret_bsons = []
    for metaid, keysinfo in table_field.items():
        table_head = get_tabular_bson_head(fields)
        ret_bsons.append(xtbson.encode(table_head))
        datas = _get_tabular_data_single_ori(codes, metaid, list(keysinfo.keys()), int_period, start_time, end_time, count)
        for d in datas:
            ret_bsons.append(xtbson.encode(d))

    return ret_bsons

