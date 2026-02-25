#:  -*- coding: utf-8 -*-
import datetime
import time
import traceback
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
from tqdm.auto import tqdm

from chanlun import config, fun
from chanlun.exchange.exchange import convert_stock_kline_frequency
from chanlun.exchange.exchange_db import ExchangeDB
from chanlun.exchange.exchange_qmt import ExchangeQMTStock

"""
同步 QMT A股行情到数据库中
"""

def sync_code(code):
    ex = ExchangeQMTStock()
    db_ex = ExchangeDB("a")
    
    # 同步周期：1m, 5m, 30m, 1d
    # 其他周期如 3m, 10m, 2h 等通过 1m/5m 合成，不需要单独同步基础数据，但如果数据库需要存储也可以
    # 通常存储基础周期即可
    sync_frequencys = ["1m", "5m", "30m", "1d"]
    
    for f in sync_frequencys:
        try:
            # 获取数据库中最后一条数据的时间
            last_dt = db_ex.query_last_datetime(code, f)
            
            # 如果没有数据，默认从较早时间开始，或者根据 QMT 限制
            # QMT 1m/5m 这种小周期可能只能获取最近一段时间
            start_date = None
            if last_dt:
                # 增量更新，从最后时间开始
                start_date = fun.datetime_to_str(last_dt, "%Y-%m-%d %H:%M:%S")
            
            # 调用 QMT 接口获取数据
            # 注意：QMT klines 接口内部已经处理了 start_date 逻辑
            # 如果是增量，start_date 传进去；如果是全量，start_date 为 None
            
            klines = ex.klines(code, f, start_date=start_date)
            
            if klines is None or klines.empty:
                continue
                
            # 如果是增量更新，需要过滤掉已经存在的数据（根据 last_dt）
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

def sync_all_stocks():
    ex = ExchangeQMTStock()
    stocks = ex.all_stocks()
    codes = [s["code"] for s in stocks]
    
    print(f"Total stocks: {len(codes)}")
    
    # 单进程同步
    # for code in tqdm(codes):
    #     sync_code(code)
        
    # 多进程同步
    with ProcessPoolExecutor(max_workers=4) as executor:
        list(tqdm(executor.map(sync_code, codes), total=len(codes), desc="Sync Stock Klines"))

if __name__ == "__main__":
    sync_all_stocks()
