#:  -*- coding: utf-8 -*-
import datetime
import time
import traceback
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
from tqdm.auto import tqdm

from chanlun import config, fun
from chanlun.exchange.exchange_db import ExchangeDB
from chanlun.exchange.exchange_qmt import ExchangeQMTOption

"""
同步 QMT 期权行情到数据库中
"""

def sync_code(code):
    ex = ExchangeQMTOption()
    # 期权可以存放在 futures 库或者单独的库，这里假设用 futures 库
    # 如果需要单独库，需确保 ExchangeDB 支持 "option"
    db_ex = ExchangeDB("option") 
    
    # 期权主要同步 1m, 5m, 1d
    sync_frequencys = ["1m", "5m", "1d"]
    
    for f in sync_frequencys:
        try:
            last_dt = db_ex.query_last_datetime(code, f)
            
            start_date = None
            if last_dt:
                start_date = fun.datetime_to_str(last_dt, "%Y-%m-%d %H:%M:%S")
            
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
            
            db_ex.insert_klines(code, f, klines)
            
        except Exception:
            print("执行 %s - %s 同步K线异常" % (code, f))
            print(traceback.format_exc())
            time.sleep(1)

    return True

def sync_all_options():
    ex = ExchangeQMTOption()
    stocks = ex.all_stocks()
    codes = [s["code"] for s in stocks]
    
    print(f"Total options: {len(codes)}")
    
    # 限制并发数
    with ProcessPoolExecutor(max_workers=2) as executor:
        list(tqdm(executor.map(sync_code, codes), total=len(codes), desc="Sync Option Klines"))

if __name__ == "__main__":
    sync_all_options()
