#:  -*- coding: utf-8 -*-
from chanlun.exchange.exchange_db import ExchangeDB
from chanlun.exchange.exchange_tq import ExchangeTq
import traceback
import time

"""
同步期货到数据库中

需要有 天勤专业版 权限才可以执行
"""

db_ex = ExchangeDB('futures')
line_ex = ExchangeTq()

_sync_codes = [
    'ag2206', 'al2205', 'au2206', 'bu2206', 'cu2206', 'fu2209', 'hc2210', 'ni2205', 'pb2205', 'rb2210',
    'ru2209', 'sn2205', 'sp2209', 'ss2206', 'wr2210', 'zn2206', 'SA209', 'FG209', 'TA209', 'AP210', 'OI209', 'MA209',
    'CF209', 'SR209', 'UR209', 'SF209',
    'RM209', 'SM209', 'PF206', 'PK210', 'IC2205', 'IF2205', 'IH2205', 'T2206', 'TF2206', 'TS2206', 'lu2207', 'nr2206',
    'sc2206', 'p2209', 'y2209', 'i2209', 'm2209', 'v2209', 'pp2209', 'eg2209', 'pg2206', 'l2209', 'c2209', 'j2209',
    'SA209', 'FG209', 'TA209', 'AP210', 'OI209', 'MA209', 'CF209', 'SR209', 'UR209', 'SF209'
]


def is_sync_code(_code):
    for sc in _sync_codes:
        if sc in _code:
            return True


# 从自选中获取同步股票
stocks = line_ex.all_stocks()
run_codes = []
for s in stocks:
    if 'KQ.m' in s['code'] or is_sync_code(s['code']):
        run_codes.append(s['code'])

print(run_codes)

# 创建表
db_ex.create_tables(run_codes)

sync_frequencys = {
    'w': {'start': '2000-01-01 00:00:00', 'end': '2022-04-26 00:00:00'},
    'd': {'start': '2000-01-01 00:00:00', 'end': '2022-04-26 00:00:00'},
    '60m': {'start': '2000-01-01 00:00:00', 'end': '2022-04-26 00:00:00'},
    '30m': {'start': '2000-01-01 00:00:00', 'end': '2022-04-26 00:00:00'},
    '15m': {'start': '2000-01-01 00:00:00', 'end': '2022-04-26 00:00:00'},
    '5m': {'start': '2015-01-01 00:00:00', 'end': '2022-04-26 00:00:00'},
    '1m': {'start': '2018-01-01 00:00:00', 'end': '2022-04-26 00:00:00'},
}

for code in run_codes:
    for f, dt in sync_frequencys.items():
        try:
            time.sleep(3)
            last_dt = db_ex.query_last_datetime(code, f)
            klines = line_ex.klines(code, f, start_date=dt['start'] if last_dt is None else last_dt, end_date=dt['end'])
            print('Run code %s frequency %s klines len %s' % (code, f, len(klines)))
            db_ex.insert_klines(code, f, klines)
        except Exception as e:
            print('执行 %s 同步K线异常' % code)
            print(traceback.format_exc())
            time.sleep(10)

line_ex.close_api()
