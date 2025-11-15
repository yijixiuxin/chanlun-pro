from collections import OrderedDict

from .meta_config import (
    __TABULAR_PERIODS__,
    __META_FIELDS__,
    __META_TABLES__,
    __META_INFO__,
    _init_metainfos,
)
from .get_bson import get_tabular_bson_head

def _get_tabular_feather_single_ori(
        codes: list,
        table: str,
        int_period: int,
        start_timetag: int,
        end_timetag: int,
        count: int = -1,
        **kwargs
):
    from .. import xtdata
    from pyarrow import feather as fe
    import os

    CONSTFIELD_TIME = '_time'
    CONSTFIELD_CODE = '_stock'

    file_path = os.path.join(xtdata.get_data_dir(), "EP", f"{table}_Xdat2", "data.fe")
    if not os.path.exists(file_path):
        return None, None

    fe_table = fe.read_table(file_path)

    schema = fe_table.schema
    fe_fields = [f.name for f in schema]
    def _old_arrow_filter():
        from pyarrow import dataset as ds
        nonlocal fe_table, fe_fields

        expressions = []
        if CONSTFIELD_TIME in fe_fields:
            if start_timetag > 0:
                expressions.append(ds.field(CONSTFIELD_TIME) >= start_timetag)

            if end_timetag > 0:
                expressions.append(ds.field(CONSTFIELD_TIME) <= end_timetag)

        if CONSTFIELD_CODE in fe_fields and len(codes) > 0:
            expressions.append(ds.field(CONSTFIELD_CODE).isin(codes))

        if len(expressions) > 0:
            expr = expressions[0]
            for e in expressions[1:]:
                expr = expr & e
            return ds.dataset(fe_table).to_table(filter=expr)
        else:
            return fe_table


    def _new_arrow_filter():
        from pyarrow import compute as pc
        nonlocal fe_table, fe_fields

        expressions = []
        if CONSTFIELD_TIME in fe_fields:
            if start_timetag > 0:
                expressions.append(pc.field(CONSTFIELD_TIME) >= start_timetag)
            if end_timetag > 0:
                expressions.append(pc.field(CONSTFIELD_TIME) <= end_timetag)

        if CONSTFIELD_CODE in fe_fields and len(codes) > 0:
            expressions.append(pc.field(CONSTFIELD_CODE).isin(codes))

        if len(expressions) > 0:
            expr = expressions[0]
            for e in expressions[1:]:
                expr = expr & e
            return fe_table.filter(expr)
        else:
            return fe_table

    def do_filter():
        import pyarrow as pa
        from distutils import version
        nonlocal count
        # python3.6 pyarrow-6.0.1
        # python3.7 pyarrow-12.0.1
        # python3.8~12 pyarrow-17.0.0
        paver = version.LooseVersion(pa.__version__)
        if paver <= version.LooseVersion('9.0.0'):
            _table = _old_arrow_filter()
        else:
            _table = _new_arrow_filter()
        
        if count > 0:
            start_index = max(0, _table.num_rows - count)
            _table = _table.slice(start_index, count)
        
        return _table

    return do_filter(), fe_fields


def _parse_fields(fields):
    if not __META_FIELDS__:
        _init_metainfos()

    tmp = OrderedDict() # { table: { show_fields: list(), fe_fields: list() } }
    for field in fields:
        if field.find('.') == -1:
            table = field

            if table not in __META_TABLES__:
                continue

            if table not in tmp:
                tmp[table] = {'show': list(), 'fe': list()}

            metaid = __META_TABLES__[table]
            for key, f in __META_INFO__[metaid]['fields'].items():
                if 'G' == key:
                    tmp[table]['fe'].append('_time')
                elif 'S' == key:
                    tmp[table]['fe'].append('_stock')
                else:
                    tmp[table]['fe'].append(f['modelName'])

                tmp[table]['show'].append(f['modelName'])

        else:
            table = field.split('.')[0]
            ifield = field.split('.')[1]

            if field not in __META_FIELDS__:
                continue

            metaid, key = __META_FIELDS__[field]

            if table not in tmp:
                tmp[table] = {'show': list(), 'fe': list()}

            if 'G' == key:
                tmp[table]['fe'].append('_time')
            elif 'S' == key:
                tmp[table]['fe'].append('_stock')
            else:
                tmp[table]['fe'].append(ifield)

            tmp[table]['show'].append(ifield)

    return [(tb, sd['show'], sd['fe']) for tb, sd in tmp.items()]

def _parse_keys(fields):
    if not __META_FIELDS__:
        _init_metainfos()

    tmp = OrderedDict() # { table: { show_keys: list(), fe_fields: list() } }
    for field in fields:
        if field.find('.') == -1:
            table = field

            if table not in __META_TABLES__:
                continue

            if table not in tmp:
                tmp[table] = {'show': list(), 'fe': list()}

            metaid = __META_TABLES__[table]
            for key, f in __META_INFO__[metaid]['fields'].items():
                if 'G' == key:
                    tmp[table]['fe'].append('_time')
                elif 'S' == key:
                    tmp[table]['fe'].append('_stock')
                else:
                    tmp[table]['fe'].append(f['modelName'])

                tmp[table]['show'].append(key)

        else:
            table = field.split('.')[0]
            ifield = field.split('.')[1]

            if field not in __META_FIELDS__:
                continue

            metaid, key = __META_FIELDS__[field]

            if table not in tmp:
                tmp[table] = {'show': list(), 'fe': list()}

            if 'G' == key:
                tmp[table]['fe'].append('_time')
            elif 'S' == key:
                tmp[table]['fe'].append('_stock')
            else:
                tmp[table]['fe'].append(ifield)

            tmp[table]['show'].append(key)

    return [(tb, sd['show'], sd['fe']) for tb, sd in tmp.items()]

def _datetime_to_timetag(timelabel, format=''):
    '''
    timelabel: str '20221231' '20221231235959'
    format: str '%Y%m%d' '%Y%m%d%H%M%S'
    return: int 1672502399000
    '''
    import datetime as dt
    if not format:
        format = '%Y%m%d' if len(timelabel) == 8 else '%Y%m%d%H%M%S'
    try:
        return dt.datetime.strptime(timelabel, format).timestamp() * 1000
    except:
        return 0

def _datetime_to_timetag_end(timelabel, format=''):
    '''
    timelabel: str '20221231' '20221231235959'
    format: str '%Y%m%d' '%Y%m%d%H%M%S'
    return: int 1672502399000
    '''
    import datetime as dt
    if not format:
        format = '%Y%m%d' if len(timelabel) == 8 else '%Y%m%d%H%M%S'
    try:
        if len(timelabel) == 8:
            return dt.datetime.strptime(timelabel, format).timestamp() * 1000 + 24*60*60*1000 - 1
        elif len(timelabel) == 14:
            return dt.datetime.strptime(timelabel, format).timestamp() * 1000 + 1000 - 1
    except:
        return 0

def get_tabular_fe_data(
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

    table_fields = _parse_fields(fields)

    start_timetag = _datetime_to_timetag(start_time)
    end_timetag = _datetime_to_timetag_end(end_time)

    dfs = []
    ordered_fields = []
    for table, show_fields, fe_fields in table_fields:
        fe_table, fe_table_fields = _get_tabular_feather_single_ori(codes, table, int_period, start_timetag, end_timetag, count)
        if not fe_table:
            continue

        ifields = list(set(fe_table_fields) & set(fe_fields))
        if not ifields:
            continue

        fe_table = fe_table.select(ifields)
        fe_df = fe_table.to_pandas()
        # 补充请求的字段
        default_null_columns = [f for f in fe_fields if f not in fe_table_fields]
        for c in default_null_columns:
            fe_df.loc[:, c] = pd.NA

        rename_fields = {}

        for i in range(min(len(show_fields), len(fe_fields))):
            show_field = f'{table}.{show_fields[i]}'
            rename_fields[fe_fields[i]] = show_field
            ordered_fields.append(show_field)

        fe_df.rename(columns=rename_fields, inplace=True)
        dfs.append(fe_df)

    if not dfs:
        return pd.DataFrame()

    result = pd.concat(dfs, ignore_index=True)
    return result[ordered_fields]


def get_tabular_fe_bson(
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

    table_fields = _parse_keys(fields)

    start_timetag = _datetime_to_timetag(start_time)
    end_timetag = _datetime_to_timetag_end(end_time)

    def _get_convert():
        import pyarrow as pa
        from distutils import version
        # python3.6 pyarrow-6.0.1
        # python3.7 pyarrow-12.0.1
        # python3.8~12 pyarrow-17.0.0
        def _old_arrow_convert(table):
            return table.to_pandas().to_dict(orient='records')

        def _new_arrow_convert(table):
            return table.to_pylist()

        paver = version.LooseVersion(pa.__version__)
        if paver < version.LooseVersion('7.0.0'):
            return _old_arrow_convert
        else:
            return _new_arrow_convert

    convert = _get_convert()
    ret_bsons = []
    for table, show_fields, fe_fields in table_fields:
        table_head = get_tabular_bson_head(fields)
        ret_bsons.append(xtbson.encode(table_head))

        fe_table, fe_table_fields = _get_tabular_feather_single_ori(codes, table, int_period, start_timetag, end_timetag, count)

        ifields = list()
        new_columns = list()
        for i in range(len(fe_fields)):
            if fe_fields[i] in fe_table_fields:
                ifields.append(fe_fields[i])
                new_columns.append(show_fields[i])

        if not ifields:
            continue

        fe_table = fe_table.select(ifields)
        fe_table = fe_table.rename_columns(new_columns) # key_column

        fe_datas = convert(fe_table)
        for data in fe_datas:
            ret_bsons.append(xtbson.encode(data))

    return ret_bsons

