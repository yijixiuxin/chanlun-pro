import sys

cur_path = sys.path[0]
sys.path.append(sys.path[0] + "/../..")

from cl_v2 import exchange_db
from cl_v2 import exchange_futu
import traceback
import time

"""
同步股票数据到数据库中
"""

exchange = exchange_db.ExchangeDB('hk')
line_exchange = exchange_futu.ExchangeFutu()

# 从自选中获取同步股票
stocks = line_exchange.zixuan_stocks('港股')
run_codes = [s['code'] for s in stocks]

# 创建表
exchange.create_tables(run_codes)

for code in run_codes:
    try:
        for f in ['d', '60m', '30m', '15m', '5m', '1m']:
            time.sleep(3)
            while True:
                last_dt = exchange.query_last_datetime(code, f)
                if last_dt is None:
                    klines = line_exchange.klines(code, f, end_date='2020-01-01 00:00:00', args={'is_history': True})
                    if len(klines) == 0:
                        klines = line_exchange.klines(code, f, args={'is_history': True})
                else:
                    klines = line_exchange.klines(code, f, start_date=last_dt, args={'is_history': True})

                print('Run code %s frequency %s klines len %s' % (code, f, len(klines)))
                exchange.insert_klines(code, f, klines)
                if len(klines) <= 1:
                    break

    except Exception as e:
        print('执行 %s 同步K线异常' % code)
        print(e)
        print(traceback.format_exc())
        time.sleep(10)
