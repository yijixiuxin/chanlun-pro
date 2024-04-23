#:  -*- coding: utf-8 -*-
from chanlun.exchange.exchange_binance import ExchangeBinance
from chanlun.exchange.exchange_db import ExchangeDB
import traceback
from tqdm.auto import tqdm

"""
同步数字货币行情到数据库中
"""

exchange = ExchangeDB("currency")
line_exchange = ExchangeBinance()

# 创建表
stocks = line_exchange.all_stocks()
codes = [s["code"] for s in stocks]
# codes = ['BTC/USDT']
# codes = [
#     'BTC/USDT', 'ETH/USDT', 'ETC/USDT', 'GMT/USDT', 'SOL/USDT', 'BNB/USDT', 'AVAX/USDT', 'OP/USDT', 'TRB/USDT',
#     'FIL/USDT', 'NEAR/USDT', 'LINK/USDT', 'MATIC/USDT', 'DOGE/USDT', 'ADA/USDT', 'APE/USDT', 'DOT/USDT',
#     '1000SHIB/USDT', 'ZEC/USDT', 'REN/USDT', 'FLOW/USDT', 'SAND/USDT', 'ROSE/USDT', 'XRP/USDT', 'RSR/USDT',
#     'CRV/USDT', 'FTM/USDT', 'ATOM/USDT', 'MANA/USDT', 'GALA/USDT', 'UNFI/USDT', 'DYDX/USDT', 'WAVES/USDT',
#     'LTC/USDT', 'AXS/USDT', 'THETA/USDT', 'EOS/USDT', 'BCH/USDT', 'GRT/USDT', 'RUNE/USDT'
# ]
sync_frequencys = ["w", "d", "4h", "60m", "30m", "15m", "10m", "5m", "1m"]

# TODO 同步各个周期的起始时间
f_start_time_maps = {
    "w": "2000-01-01 00:00:00",
    "d": "2000-01-01 00:00:00",
    "4h": "2000-01-01 00:00:00",
    "60m": "2000-01-01 00:00:00",
    "30m": "2000-01-01 00:00:00",
    "15m": "2000-01-01 00:00:00",
    "10m": "2000-01-01 00:00:00",
    "5m": "2000-01-01 00:00:00",
    "1m": "2000-01-01 00:00:00",
}

if __name__ == "__main__":
    for code in tqdm(codes):
        try:
            for f in sync_frequencys:
                while True:
                    try:
                        last_dt = exchange.query_last_datetime(code, f)
                        if last_dt is None:
                            klines = line_exchange.klines(
                                code,
                                f,
                                end_date=f_start_time_maps[f],
                                args={"use_online": True},
                            )
                            if len(klines) == 0:
                                klines = line_exchange.klines(
                                    code,
                                    f,
                                    start_date=f_start_time_maps[f],
                                    args={"use_online": True},
                                )
                        else:
                            klines = line_exchange.klines(
                                code, f, start_date=last_dt, args={"use_online": True}
                            )

                        tqdm.write(
                            "Run code %s frequency %s klines len %s"
                            % (code, f, len(klines))
                        )
                        exchange.insert_klines(code, f, klines)
                        if len(klines) <= 1:
                            break
                    except Exception as e:
                        tqdm.write("执行 %s 同步K线异常" % code)
                        tqdm.write(traceback.format_exc())
                        break

        except Exception as e:
            tqdm.write("执行 %s 同步K线异常" % code)
            tqdm.write(e)
            tqdm.write(traceback.format_exc())
