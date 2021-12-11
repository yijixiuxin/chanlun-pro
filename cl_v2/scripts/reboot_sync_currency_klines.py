import sys
cur_path = sys.path[0]
sys.path.append(sys.path[0] + "/../..")

from cl_v2 import exchange_binance
from cl_v2 import exchange_db
import traceback

"""
同步数字货币行情到数据库中
"""

exchange = exchange_db.ExchangeDB('currency')
line_exchange = exchange_binance.ExchangeBinance()

# 创建表
stocks = line_exchange.all_stocks()
codes = [s['code'] for s in stocks]
exchange.create_tables(codes)

for code in codes:
    try:
        for f in ['d', '4h', '60m', '30m', '15m', '5m', '1m']:
            while True:
                last_dt = exchange.query_last_datetime(code, f)
                if last_dt is None:
                    klines = line_exchange.klines(code, f, end_date='2020-01-01 00:00:00')
                    if len(klines) == 0:
                        klines = line_exchange.klines(code, f, start_date='2020-01-01 00:00:00')
                else:
                    klines = line_exchange.klines(code, f, start_date=last_dt)

                print('Run code %s frequency %s klines len %s' % (code, f, len(klines)))
                exchange.insert_klines(code, f, klines)
                if len(klines) <= 1:
                    break

    except Exception as e:
        print('执行 %s 同步K线异常' % code)
        print(e)
        print(traceback.format_exc())