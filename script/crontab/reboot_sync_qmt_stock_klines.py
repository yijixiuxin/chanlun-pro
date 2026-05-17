#:  -*- coding: utf-8 -*-
import os
import time
import traceback
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
try:
    from tqdm.auto import tqdm
except Exception:
    class _TqdmFallback:
        @staticmethod
        def write(msg):
            print(msg)

        def __call__(self, iterable=None, total=None, desc=None):
            return iterable if iterable is not None else []

    tqdm = _TqdmFallback()

from chanlun import fun
from chanlun.db import db
from chanlun.exchange.exchange_db import ExchangeDB
from chanlun.exchange.exchange_qmt import ExchangeQMTStock

"""
同步 QMT A股行情到数据库中
"""

def sync_code(code):
    try:
        ex = ExchangeQMTStock()
        db_ex = ExchangeDB("a")
    except Exception:
        print("初始化 QMT/DB 失败：")
        print(traceback.format_exc())
        return False
    
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
                if isinstance(last_dt, str):
                    start_date = last_dt
                else:
                    start_date = fun.datetime_to_str(last_dt, "%Y-%m-%d %H:%M:%S")
            
            # 调用 QMT 接口获取数据
            # 注意：QMT klines 接口内部已经处理了 start_date 逻辑
            # 如果是增量，start_date 传进去；如果是全量，start_date 为 None
            
            klines = ex.klines(code, f, start_date=start_date)
            
            if klines is None or klines.empty:
                continue
                
            # 如果是增量更新，需要过滤掉已经存在的数据（根据 last_dt）
            if last_dt:
                last_ts = pd.Timestamp(last_dt)
                tz = klines["date"].dt.tz
                if last_ts.tzinfo is None:
                    last_ts = last_ts.tz_localize(tz)
                else:
                    last_ts = last_ts.tz_convert(tz)
                klines = klines[klines["date"] > last_ts]
            
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

    limit = os.environ.get("CL_SYNC_LIMIT", "").strip()
    if limit != "":
        try:
            limit_n = int(limit)
            if limit_n > 0:
                codes = codes[:limit_n]
                print(f"Limit stocks: {len(codes)}")
        except Exception:
            pass

    workers_env = os.environ.get("CL_SYNC_WORKERS", "").strip()
    workers = 4
    if workers_env != "":
        try:
            workers = max(1, int(workers_env))
        except Exception:
            workers = 4

    if getattr(db.engine.dialect, "name", "") == "sqlite":
        workers = 1
    
    # 单进程同步
    # for code in tqdm(codes):
    #     sync_code(code)
        
    # 多进程同步
    if workers <= 1:
        for code in tqdm(codes, desc="Sync Stock Klines"):
            sync_code(code)
        return

    try:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            list(
                tqdm(
                    executor.map(sync_code, codes),
                    total=len(codes),
                    desc="Sync Stock Klines",
                )
            )
    except Exception:
        print("多进程同步失败，切换为单进程：")
        print(traceback.format_exc())
        for code in tqdm(codes, desc="Sync Stock Klines"):
            sync_code(code)

if __name__ == "__main__":
    sync_all_stocks()
