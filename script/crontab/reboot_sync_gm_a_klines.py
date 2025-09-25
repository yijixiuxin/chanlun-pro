#:  -*- coding: utf-8 -*-
import datetime
import time
import traceback
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
from gm.api import ADJUST_NONE, get_symbols, history, set_serv_addr, set_token
from tqdm.auto import tqdm

from chanlun import config, fun
from chanlun.exchange.exchange import convert_stock_kline_frequency
from chanlun.exchange.exchange_db import ExchangeDB
from chanlun.exchange.exchange_tdx import ExchangeTDX

"""
同步A股行情到数据库中

使用的是 掘金量化 API 获取

同步掘金的不复权数据，进行增量更新，股票代码以：SHSE SZSE 开头

按需将掘金的不复权数据，复权&合并 为其他所需要的周期，进行研究&历史选股&策略回测等使用，股票代码以：SH  SZ 开头
"""


def to_tdx_codes(_codes):
    return [_code.replace("SHSE.", "SH.").replace("SZSE.", "SZ.") for _code in _codes]


# 如在远程执行，需要制定掘金终端地址  https://www.myquant.cn/docs/gm3_faq/154#b244aeed0032526e
set_serv_addr(config.GM_SERVER_ADDR)
# 设置token， 查看已有token ID,在用户-秘钥管理里获取
set_token(config.GM_TOKEN)

symbols = get_symbols(sec_type1=1010, sec_type2=101001)
run_codes = [_s["exchange"] + "." + _s["sec_id"] for _s in symbols]
# run_codes = []
for _c in [
    "SH.000001",  # 上证指数
    "SZ.399001",  # 深圳指数
    "SZ.399006",  # 创业板指
    "SH.000852",  # 中证1000
]:
    run_codes.append(_c.replace("SZ.", "SZSE.").replace("SH.", "SHSE."))

# run_codes = ["SHSE.603959"]
print("Sync Len : ", len(run_codes))


db_ex = ExchangeDB("a")

tdx_ex = ExchangeTDX()

# 默认第一次同步的起始时间，后续则进行增量更新
sync_frequencys = {
    "d": {
        "start": "1992-01-01",
    },
    "5m": {
        "start": fun.datetime_to_str(
            datetime.datetime.now() - datetime.timedelta(days=170), "%Y-%m-%d"
        )
    },
    # "1m": {
    #     "start": fun.datetime_to_str(
    #         datetime.datetime.now() - datetime.timedelta(days=170), "%Y-%m-%d"
    #     )
    # },
}


print(sync_frequencys)
# 本地周期与掘金周期对应关系
fre_maps = {"d": "1d", "5m": "300s", "1m": "60s"}


def sync_code(code):
    for f, dt in sync_frequencys.items():
        try:
            last_dt = db_ex.query_last_datetime(code, f)
            if last_dt is None:
                last_dt = dt["start"]
            last_dt = fun.datetime_to_str(
                fun.str_to_datetime(last_dt, "%Y-%m-%d") - datetime.timedelta(days=1),
                "%Y-%m-%d",
            )
            # print(f'{code} query last datetime use time: ', time.time() - s_time)

            now_datetime = datetime.datetime.now()
            klines = history(
                code,
                fre_maps[f],
                start_time=last_dt,
                end_time=now_datetime,
                adjust=ADJUST_NONE,
                df=True,
            )
            if len(klines) == 0:
                break
            klines.loc[:, "code"] = klines["symbol"]
            klines.loc[:, "date"] = pd.to_datetime(klines["eob"])
            klines = klines[["code", "date", "open", "close", "high", "low", "volume"]]
            # print(f'{code} query history use time: ', time.time() - s_time)

            tqdm.write(
                f"Run code {code} frequency {f} last_dt {last_dt} klines len {len(klines)} 【{klines.iloc[0]['date']} - {klines.iloc[-1]['date']}】"
            )
            # print(klines)

            # s_time = time.time()
            db_ex.insert_klines(code, f, klines)
            # print(f'{code} insert klines use time: ', time.time() - s_time)
        except Exception:
            print("执行 %s - %s 同步K线异常" % (code, f))
            print(traceback.format_exc())
            time.sleep(10)
            # utils.send_dd_msg('a', '执行 %s 同步K线异常' % code)

    return True


def convert_code(code):
    try:
        db_code = to_tdx_codes([code])[0]
        # 获取除权信息
        market, tdx_code, _type = tdx_ex.to_tdx_code(db_code)
        xdxr = tdx_ex.xdxr(market, db_code, tdx_code)

        # 统一删除，重新计算
        # db_ex.del_klines_by_code(db_code)

        # TODO 根据 5m 和 d 周期，合并&复权生成复权数据（按需写自己需要的周期即可）
        for f in ["d"]:
            gm_freq = "5m"
            if f in ["60m", "30m", "15m", "5m"]:
                gm_freq = "5m"
            elif f in ["d", "w"]:
                gm_freq = "d"
            # 获取最后一根k线数据
            last_klines = db_ex.klines(db_code, f, args={"limit": 10})
            if len(last_klines) != 0:
                try:
                    last_dt = fun.datetime_to_str(last_klines.iloc[0]["date"])
                    gm_klines = db_ex.klines(
                        code,
                        gm_freq,
                        start_date=last_dt,
                        args={"limit": 9999999},
                    )
                    gm_klines["volume"] = gm_klines["volume"] / 100  # 成交量除以100
                    gm_klines = tdx_ex.klines_fq(gm_klines, xdxr, "qfq")
                    _ks = convert_stock_kline_frequency(gm_klines, f)
                    # 对比数据是否一致
                    if (
                        _ks[_ks["date"] == last_klines.iloc[-1]["date"]].iloc[0][
                            "close"
                        ]
                        == last_klines.iloc[-1]["close"]
                    ):
                        tqdm.write(f"{code} {f} 增量更新数据：{len(_ks)}")
                        db_ex.insert_klines(db_code, f, _ks)
                        continue
                    tqdm.write(
                        f"{code} {f} 时间 ： {last_klines.iloc[-1]['date']} 原始收盘价: {last_klines.iloc[-1]['close']} 新收盘价: {_ks[_ks['date'] == last_klines.iloc[-1]['date']].iloc[0]['close']}"
                    )
                except Exception as e:
                    print(f"{code} {f} 数据对比异常：{str(e)}，进行全量更新")

            # 全量更新 TODO 根据周期数据，修改全量更新所需的其实时间（如果不设置数据量会比较大，浪费空间）
            db_ex.del_klines_by_code_freq(db_code, f)  # 先删除所有数据
            if f == "5m":
                start_dt = "2021-01-01 00:00:00"
            elif f == "15m":
                start_dt = "2023-01-01 00:00:00"
            elif f == "30m":
                start_dt = "2018-01-01 00:00:00"
            elif f == "60m":
                start_dt = "2015-01-01 00:00:00"
            else:
                start_dt = None
            gm_klines = db_ex.klines(
                code,
                gm_freq,
                start_date=start_dt,
                args={"limit": 9999999},
            )
            gm_klines["volume"] = gm_klines["volume"] / 100  # 成交量除以100
            gm_klines = tdx_ex.klines_fq(gm_klines, xdxr, "qfq")
            _ks = convert_stock_kline_frequency(gm_klines, f)
            tqdm.write(f"{code} {f} 全量更新数据：{len(_ks)}")
            db_ex.insert_klines(db_code, f, _ks)

    except Exception as e:
        print(f"Convert {code} error : {str(e)[0:200]}")
    return True


if __name__ == "__main__":
    for _code in tqdm(run_codes, desc="同步进度"):
        sync_code(_code)

    # 转换周期（测试）
    # convert_code("SHSE.600519")

    # 慢慢来进行转换
    if True:
        for code in tqdm(run_codes):
            convert_code(code)

    # 多进程转换k线
    if False:
        bar = tqdm(total=len(run_codes), desc="转换进度")
        with ProcessPoolExecutor(max_workers=6) as ex:
            for r in ex.map(convert_code, run_codes):
                bar.update(1)

    # 复权后的日线数据转换成周线数据，并进行保存
    # for code in tqdm(run_codes):
    #     try:
    #         db_code = to_tdx_codes([code])[0]
    #         klines_d = db_ex.klines(db_code, "d", args={"limit": 9999999})
    #         klines_w = convert_stock_kline_frequency(klines_d, "w")
    #         db_ex.insert_klines(db_code, "w", klines_w)
    #     except Exception as e:
    #         print(f"Convert {code} error : {str(e)}")

    # 删除多余周期数据
    # for code in tqdm(run_codes):
    #     db_code = to_tdx_codes([code])[0]
    #     tqdm.write(f"删除 {db_code} 数据")
    #     db_ex.del_klines_by_code_freq(db_code, "5m")
    #     db_ex.del_klines_by_code_freq(db_code, "30m")
    #     db_ex.del_klines_by_code_freq(db_code, "15m")
    #     db_ex.del_klines_by_code_freq(db_code, "60m")
    #     db_ex.del_klines_by_code_freq(db_code, "d")
    #     db_ex.del_klines_by_code_freq(db_code, "w")

    print("Done")
