#:  -*- coding: utf-8 -*-
"""
选股程序
"""
import json

from chanlun.cl_interface import *
from chanlun import zixuan, cl

from chanlun.exchange.exchange_polygon import ExchangePolygon

import os, csv, inspect, os.path

ex = ExchangePolygon()


def get_symbols():
    ##################################################-GET LIST OF SYMBOLS-##################################################
    FILE_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    dataFile = FILE_path + '/data/NASDAQ100.csv'
    # #dataFile = FILE_path+'/data/RUS3000.csv'

    symbols = []

    with open(dataFile) as f:
        for row in csv.reader(f):
            if row[0] not in symbols:
                symbols.append(row[0])

    # dataFile = FILE_path+'/data/SNP500.csv'
    # #dataFile = FILE_path+'/data/RUS3000.csv'

    # with open(dataFile) as f:
    #     for row in csv.reader(f) :
    #         if row[0] not in symbols:
    #             symbols.append(row[0])

    dataFile = FILE_path + '/data/SNP500.csv'
    # dataFile = FILE_path+'/data/RUS3000.csv'

    with open(dataFile) as f:
        for row in csv.reader(f):
            if row[0] not in symbols:
                symbols.append(row[0])

    return symbols


# 运行的股票代码，可通过 joinquant 目录中的脚本，在聚宽平台运行后获取（可根据自己的基本面选股方法进行选择）
codes = get_symbols()

frequency = '30m'

# 这里设置选股缠论计算的参数，要与前台展示的配置一致，不然这里选出的股票符合条件，前台页面有可能看不到
cl_config = {
    # 分型默认配置
    'fx_qj': Config.FX_QJ_K.value,
    'fx_bh': Config.FX_BH_YES.value,
    # 笔默认配置
    'bi_type': Config.BI_TYPE_NEW.value,
    'bi_bzh': Config.BI_BZH_NO.value,
    'bi_fx_cgd': Config.BI_FX_CHD_YES.value,
    'bi_qj': Config.BI_QJ_CK.value,
    # 线段默认配置
    'xd_bzh': Config.XD_BZH_YES.value,
    'xd_qj': Config.XD_QJ_DD.value,
    # 走势段默认配置
    'zsd_bzh': Config.ZSD_BZH_YES.value,
    'zsd_qj': Config.ZSD_QJ_DD.value,
    # 中枢默认配置
    'zs_bi_type': Config.ZS_TYPE_DN.value,  # 笔中枢类型
    'zs_xd_type': Config.ZS_TYPE_DN.value,  # 走势中枢类型
    'zs_qj': Config.ZS_QJ_DD.value,
    'zs_wzgx': Config.ZS_WZGX_ZGD.value,
}

print('运行股票数量：', len(codes))


def stock_is_ok(cl_data: ICL):
    """
    这里写自己的选股逻辑，出现什么情况，则选择并返回相关信息
    """
    if len(cl_data.get_xds()) == 0 or len(cl_data.get_bis()) == 0:
        return None
    xd = cl_data.get_xds()[-1]
    bi = cl_data.get_bis()[-1]
    if xd.mmd_exists(['1buy', '2buy', '3buy', 'l2buy', 'l3buy']) and bi.mmd_exists(
            ['1buy', '2buy', '3buy', 'l2buy', 'l3buy']):
        return '线段买点 【%s】 笔买点【%s】' % (xd.line_mmds(), bi.line_mmds())
    for mmd in bi.mmds:
        if mmd.zs.zs_type == 'xd' and 'buy' in mmd.name:
            return '笔出现线段买点【%s】' % bi.line_mmds()
    return None


### 直接放入自选组
zx = zixuan.ZiXuan('us')
zx_group = '选股'  # 这个需要确保在 config.py 中有进行配置

ok_stocks = []  # 保存符合要求的股票列表
for code in codes:
    try:
        klines = ex.klines(code, frequency)
        cd: ICL = cl.CL(code, frequency, cl_config).process_klines(klines)
        msg = stock_is_ok(cd)
        if msg is not None:
            stocks = ex.stock_info(code)
            print('【%s - %s 】 %s 出现机会：%s' % (stocks['code'], stocks['name'], frequency, msg))
            ok_stocks.append(stocks)
            zx.add_stock(zx_group, stocks['code'], stocks['name'])
    except Exception as e:
        print('Code : %s Run Exception : %s' % (code, e))

print('Done')
print(json.dumps(ok_stocks, ensure_ascii=False))
