#:  -*- coding: utf-8 -*-
import datetime
import time
import traceback

import pandas as pd
from gm.api import *
from tqdm.auto import tqdm

from chanlun import config, fun
from chanlun.exchange.exchange_db import ExchangeDB

"""
同步股票数据到数据库中

使用的是 掘金量化 API 获取
"""

# 可以直接提取数据，掘金终端需要打开，接口取数是通过网络请求的方式，效率一般，行情数据可通过subscribe订阅方式

# 如在远程执行，需要制定掘金终端地址  https://www.myquant.cn/docs/gm3_faq/154#b244aeed0032526e
set_serv_addr(config.GM_SERVER_ADDR)
# 设置token， 查看已有token ID,在用户-秘钥管理里获取
set_token(config.GM_TOKEN)

db_ex = ExchangeDB("a")

# 获取沪深 股票/基金/指数 代码
# fund_stocks = get_instruments(symbols=None, exchanges=['SHSE', 'SZSE'],
#                               sec_types=[SEC_TYPE_FUND])
#
# stock_stocks = get_instruments(symbols=None, exchanges=['SHSE', 'SZSE'],
#                                sec_types=[SEC_TYPE_STOCK])
# run_codes = ['SHSE.000001'] + [s['symbol'] for s in stock_stocks] + [s['symbol'] for s in fund_stocks if
#                                                                      s['symbol'][0:7] in ['SHSE.51', 'SZSE.15']]


symbols = get_symbols(sec_type1=1010, sec_type2=101001)
run_codes = [_s["exchange"] + "." + _s["sec_id"] for _s in symbols]
for _c in [
    "SH.000001",
    "SH.000010",
    "SH.000016",
    "SZ.399001",
    "SZ.399330",
    "SZ.399005",
    "SH.000300",
    "SH.000905",
    "SH.000906",
    "SH.000852",
    "SZ.399006",
    "SZ.399673",
    "SZ.399295",
    "SZ.399296",
    "SZ.399362",
    "SH.000689",
    "SH.000015",
    "SH.000092",
    "SH.000919",
    "SZ.399971",
    "SH.000685",
    "SZ.399967",
    "SZ.399973",
    "SH.000819",
    "SH.000928",
    "SZ.399998",
    "SZ.399976",
    "SZ.399808",
    "SH.000827",
    "SZ.399986",
    "SZ.399975",
    "SZ.399966",
    "SH.000992",
    "SH.000018",
    "SZ.399995",
    "SZ.399989",
    "SH.000991",
    "SH.000913",
    "SH.000933",
    "SH.000036",
    "SH.000069",
    "SH.000932",
    "SZ.399987",
]:
    run_codes.append(_c.replace("SZ.", "SZSE.").replace("SH.", "SHSE."))

print("Sync Len : ", len(run_codes))

# 当前时间
now_datetime = datetime.datetime.now()

# 默认第一次同步的起始时间，后续则进行增量更新（新版掘金 分钟数据只能下载最近的 6个月数据）
sync_frequencys = {
    "d": {"start": "2005-01-01 00:00:00"},
    # "30m": {
    #     "start": fun.datetime_to_str(
    #         datetime.datetime.now() - datetime.timedelta(days=180), "%Y-%m-%d"
    #     )
    # },
    # "5m": {
    #     "start": fun.datetime_to_str(
    #         datetime.datetime.now() - datetime.timedelta(days=180), "%Y-%m-%d"
    #     )
    # },
    # '1m': {'start': fun.datetime_to_str(datetime.datetime.now() - datetime.timedelta(days=180), '%Y-%m-%d')},
}
# 本地周期与掘金周期对应关系
fre_maps = {"1m": "60s", "5m": "300s", "30m": "1800s", "d": "1d"}

# K线数据采用后复权，可增量更新
is_update = False
for code in tqdm(run_codes):
    # 中途终端后，可以设置下次起始的代码
    # if not is_update and code == 'SHSE.600433':
    #     is_update = True
    # if not is_update:
    #     continue

    for f, dt in sync_frequencys.items():
        try:
            while True:
                time.sleep(1)
                last_dt = db_ex.query_last_datetime(code, f)
                if last_dt is None:
                    last_dt = dt["start"]
                klines = history(
                    code,
                    fre_maps[f],
                    start_time=last_dt,
                    end_time=now_datetime,
                    adjust=ADJUST_POST,
                    df=True,
                    fill_missing="Last",
                )
                klines.loc[:, "code"] = klines["symbol"]
                klines.loc[:, "date"] = pd.to_datetime(klines["eob"])
                klines = klines[
                    ["code", "date", "open", "close", "high", "low", "volume"]
                ]
                print("Run code %s frequency %s klines len %s" % (code, f, len(klines)))
                db_ex.insert_klines(code, f, klines)
                if len(klines) < 500:
                    break
        except Exception as e:
            print("执行 %s 同步K线异常" % code)
            print(traceback.format_exc())
            time.sleep(10)
