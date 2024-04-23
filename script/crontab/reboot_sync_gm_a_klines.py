#:  -*- coding: utf-8 -*-
import datetime
import time
import traceback

import pandas as pd
from gm.api import *
from tqdm.auto import tqdm

from chanlun import config, fun
from chanlun.exchange.exchange_db import ExchangeDB
from chanlun.exchange.exchange import convert_stock_kline_frequency

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

symbols = get_symbols(sec_type1=1010, sec_type2=101001)
run_codes = [_s["exchange"] + "." + _s["sec_id"] for _s in symbols]
# 增加上证指数代码
run_codes.append("SHSE.000001")

print("Sync Len : ", len(run_codes))

# 当前时间
now_datetime = datetime.datetime.now()

# 默认第一次同步的起始时间，后续则进行增量更新（新版掘金 分钟数据只能下载最近的 6个月数据）
sync_frequencys = {
    "d": {"start": "1990-01-01 00:00:00"},
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

if __name__ == "__main__":
    # K线数据采用前复权，如发现之前数据有变化，则删除并重新下载
    for code in tqdm(run_codes, desc="同步进度"):
        for f, dt in sync_frequencys.items():
            try:
                while True:
                    time.sleep(1)
                    last_dt = dt["start"]
                    last_klines = db_ex.klines(code, f, args={"limit": 100})
                    if len(last_klines) > 0:
                        last_dt = last_klines.iloc[0]["date"]

                    klines = history(
                        code,
                        fre_maps[f],
                        start_time=last_dt,
                        end_time=now_datetime,
                        adjust=ADJUST_PREV,
                        df=True,
                    )
                    klines.loc[:, "code"] = klines["symbol"]
                    klines.loc[:, "date"] = pd.to_datetime(klines["eob"])
                    klines = klines[
                        ["code", "date", "open", "close", "high", "low", "volume"]
                    ]
                    # 如果之前数据库有记录，与新请求的数据进行对比，如果收盘价有变化，则进行重新更新
                    if len(last_klines) > 0:
                        if last_klines.iloc[0]["close"] != klines.iloc[0]["close"]:
                            tqdm.write(
                                f"Run code {code} frequency {f} 数据异常，删除重新请求"
                            )
                            db_ex.del_klines_by_code_freq(code, f)
                            continue
                    tqdm.write(
                        f"Run code {code} frequency {f} klines len {len(klines)}"
                    )
                    db_ex.insert_klines(code, f, klines)
                    if len(klines) < 500:
                        break
            except Exception as e:
                tqdm.write(f"执行 {code} 同步K线异常")
                traceback.format_exc()
                time.sleep(10)

    # TODO 掘金没有周线数据，需要自己转换
    # 将日线数据转换成周线数据，并存储到数据库中
    for code in tqdm(run_codes, desc="转换进度"):
        try:
            # 获取所有日线K线
            klines_d = db_ex.klines(code, "d", args={"limit": 9999999})
            klines_w = convert_stock_kline_frequency(klines_d, "w")
            db_ex.insert_klines(code, "w", klines_w)
            tqdm.write(f"转换 {code} 周线数据：{len(klines_w)}")
        except Exception as e:
            tqdm.write(f"执行 {code} 转换数据K线异常")

    print("Done")
