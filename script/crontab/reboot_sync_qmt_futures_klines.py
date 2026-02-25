#:  -*- coding: utf-8 -*-
import datetime
import time
import traceback
import re
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
from tqdm.auto import tqdm

from chanlun import config, fun
from chanlun.exchange.exchange_db import ExchangeDB
from chanlun.exchange.exchange_qmt import ExchangeQMTFutures

"""
同步 QMT 期货行情到数据库中
"""

def sync_code(code):
    ex = ExchangeQMTFutures()
    db_ex = ExchangeDB("futures")
    
    # 期货主要同步 1m, 5m, 30m, 1h, d
    sync_frequencys = ["1m", "5m", "30m", "60m", "d"]
    
    for f in sync_frequencys:
        try:
            last_dt = db_ex.query_last_datetime(code, f)
            
            start_date = None
            if last_dt:
                if isinstance(last_dt, str):
                    start_date = last_dt
                else:
                    start_date = fun.datetime_to_str(last_dt, "%Y-%m-%d %H:%M:%S")
            if f == "d":
                f = "1d"
            klines = ex.klines(code, f, start_date=start_date)
            
            if klines is None or klines.empty:
                continue
                
            if last_dt:
                klines = klines[klines["date"] > pd.to_datetime(last_dt).tz_localize(klines["date"].dt.tz)]
            
            if klines.empty:
                continue

            tqdm.write(
                f"Run code {code} frequency {f} klines len {len(klines)} 【{klines.iloc[0]['date']} - {klines.iloc[-1]['date']}】"
            )
            if f == "1d":
                f = "d"
            db_ex.insert_klines(code, f, klines)
            
        except Exception:
            print("执行 %s - %s 同步K线异常" % (code, f))
            print(traceback.format_exc())
            time.sleep(1)

    return True

def sync_all_futures():
    ex = ExchangeQMTFutures()
    stocks = ex.all_stocks()
    codes = [s["code"] for s in stocks]

    # 过滤掉非连续合约，连续合约代码末尾数字通常为 2 位，如 rb00, rb01，连续是00我们只要00
    # 具体合约代码末尾数字通常为 4 位，如 rb2310
    run_codes = []
    for code in codes:
        try:
            symbol = code.split(".")[1]
            if re.match(r"^[a-zA-Z]+\d{2}$", symbol) and symbol[-2:] == "00":
                run_codes.append(code)
        except Exception:
            continue
    codes = run_codes
    
    print(f"Total futures: {len(codes)}")
    
    # 限制并发数，避免 QMT 接口压力过大
    with ProcessPoolExecutor(max_workers=2) as executor:
        list(tqdm(executor.map(sync_code, codes), total=len(codes), desc="Sync Futures Klines"))

if __name__ == "__main__":
    sync_all_futures()
