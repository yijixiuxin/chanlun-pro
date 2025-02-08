from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
import pathlib
import pickle
import traceback

import numpy as np
import talib

from chanlun import fun
from chanlun.backtesting.backtest_klines import BackTestKlines
from chanlun.backtesting.base import Strategy
from chanlun.cl_interface import BI, ICL
from chanlun.cl_utils import (
    query_cl_chart_config,
    up_cross,
)
from chanlun.db import db
from chanlun.zixuan import ZiXuan
from chanlun.config import get_data_path
from tqdm.auto import tqdm
from chanlun import cl

"""
进行历史选股，测试选股条件是否符合自己预期

前提需要数据库中历史行情数据
通过回放历史行情数据，对历史数据进行选股
"""


class HistoryXuangu(object):

    def __init__(self):
        # 选股市场
        self.market = "a"
        # 选股日期范围
        self.xg_start_date = "2022-01-01 00:00:00"
        self.xg_end_date = "2025-01-01 00:00:00"
        # 选股周期
        self.freqencys = ["30m"]
        # 缠论配置
        self.cl_config = query_cl_chart_config(self.market, "SH.000001")

        # 加入的自选
        self.zx = ZiXuan(self.market)
        self.zx_group = "测试选股"

        # 选股结果保存文件 确保有这个目录
        self.xg_result_path = get_data_path() / "history_xuangu"
        # 文件夹不存在进行创建
        if not self.xg_result_path.exists():  # 判断是否存在
            self.xg_result_path.mkdir(parents=True, exist_ok=True)

    def clear_zx_mark(self):
        # 清除自选与标记
        self.zx.clear_zx_stocks(self.zx_group)
        db.marks_del(self.market, "XG")
        return True

    def xuangu_by_code(self, code: str):
        """
        给定一个股票代码，执行该股票的历史选股
        """

        # 如果文件存在，之前执行过，跳过
        # if pathlib.Path(self.xg_result_path / f"xg_{code}.pkl").exists():
        #     return True

        # 初始化代码的回放类
        bk = BackTestKlines(
            self.market,
            start_date=self.xg_start_date,
            end_date=self.xg_end_date,
            frequencys=self.freqencys,
            cl_config=self.cl_config,
        )
        bk.init(code, self.freqencys[-1])

        xg_res = []

        # 获取所有K线，并计算缠论数据，预知下未来，看选股准确率
        all_klines = bk.ex.klines(
            code, self.freqencys[0], end_date=self.xg_end_date, args={"limit": 20000}
        )
        all_cd: ICL = cl.CL(
            code, self.freqencys[0], config=self.cl_config
        ).process_klines(all_klines)

        while bk.next():
            # 每根k线进行回放执行
            try:
                # 获取当前k线
                klines = bk.klines(code, frequency=bk.frequencys[0])
                if len(klines) <= 100:
                    continue
                # 获取当前缠论数据对象
                cd = bk.get_cl_data(code, bk.frequencys[0])
                if len(cd.get_bis()) == 0:  # 没有笔，跳过
                    continue
                bi = cd.get_bis()[-1]
                if bi.type == "up":  # 笔向上，跳过
                    continue

                # 笔要是完成的
                if not bi.is_done():
                    continue

                # k线要是上涨的
                k = cd.get_src_klines()[-1]
                if k.c < k.o:
                    continue

                # 要在最后一个中枢的 zd 下方
                last_zs = cd.get_last_bi_zs()
                if last_zs is None:
                    continue
                if k.h >= last_zs.zd:
                    continue

                # K线成交量放大，要大于成交量的10日均线位置
                idx_volume_ma_10 = talib.MA(klines["volume"].to_numpy(), timeperiod=10)
                if k.a < idx_volume_ma_10[-1]:
                    continue

                # 获取笔开始的点，需要截取这段时间的指标数据
                cal_bi_start_i = cd.get_src_klines()[-1].index - bi.start.k.k_index

                # 进行选股逻辑判断
                # 计算 KDJ，判断一笔内的 kdj 背离，有背离的情况下，在 macd 金叉或 kdj 金叉 时记录选股点
                idx_kdj = Strategy.idx_kdj(cd, 9, 3, 3)
                idx_kdj_ks = idx_kdj["k"][-cal_bi_start_i:]
                idx_kdj_ds = idx_kdj["d"][-cal_bi_start_i:]
                idx_kdj_js = idx_kdj["j"][-cal_bi_start_i:]

                # 获取 kdj j 线向上转折的点，前后两点至少间隔 5根k线
                # FIX BUG 要判断三个点之间的关系，才能判断转折
                idx_kdj_zz_ji = []
                for i in range(2, len(idx_kdj_js)):
                    if (
                        idx_kdj_js[i] > idx_kdj_js[i - 1]
                        and idx_kdj_js[i - 1] < idx_kdj_js[i - 2]
                        and (len(idx_kdj_zz_ji) == 0 or i - idx_kdj_zz_ji[-1] > 5)
                    ):
                        idx_kdj_zz_ji.append(i - 1)
                # 少于两次向上转折，跳过
                if len(idx_kdj_zz_ji) < 2:
                    continue
                # 最后两次转折，要背驰，后一次要比前一次高
                if idx_kdj_js[idx_kdj_zz_ji[-1]] <= idx_kdj_js[idx_kdj_zz_ji[-2]]:
                    continue
                # 最小 d 值要小于 20，超卖
                if min([idx_kdj_ds[_i] for _i in idx_kdj_zz_ji]) > 20:
                    continue

                # KDJ 金叉
                judge_kdj_gold = False
                if idx_kdj_ks[-1] > idx_kdj_ds[-1] and idx_kdj_ks[-2] < idx_kdj_ds[-2]:
                    judge_kdj_gold = True

                # 计算 MACD，并判断 macd 金叉
                idx_macd = Strategy.idx_macd(cd, 6, 13, 5)
                judge_macd_gold = False
                if (
                    idx_macd["dif"][-1] > idx_macd["dea"][-1]
                    and idx_macd["dif"][-2] < idx_macd["dea"][-2]
                ):
                    judge_macd_gold = True

                # 至少有一个金叉
                if judge_kdj_gold is False and judge_macd_gold is False:
                    continue

                # TODO 记录一些其他信息
                idx_ma_5 = Strategy.idx_ma(cd, 5)
                idx_ma_5_close = k.c < idx_ma_5[-1]
                idx_ma_10 = Strategy.idx_ma(cd, 10)
                idx_ma_10_close = k.c < idx_ma_10[-1]
                idx_ma_20 = Strategy.idx_ma(cd, 20)
                idx_ma_20_close = k.c < idx_ma_20[-1]

                # TODO 检查选股的这个时间点，笔的是否真正的结束了
                bi_end_success = False
                bi_next_bi: BI = None
                for _bi in all_cd.get_bis():
                    if _bi.end.k.date == bi.end.k.date:
                        bi_end_success = True
                        bi_next_bi = all_cd.get_bis()[_bi.index + 1]
                        break

                # TODO 如果笔结束，下一笔是否能够涨幅超过 zd (包括不断创新高的情况)
                up_zd_success = False
                if bi_end_success:
                    all_bis = all_cd.get_bis()
                    while True:
                        if bi_next_bi.index + 2 >= len(all_bis):
                            break
                        if (
                            all_bis[bi_next_bi.index + 2].high > bi_next_bi.high
                            and all_bis[bi_next_bi.index + 2].low > bi_next_bi.low
                        ):
                            bi_next_bi = all_bis[bi_next_bi.index + 2]
                        else:
                            break
                    xg_next_bi_high = bi_next_bi.high
                    if xg_next_bi_high > last_zs.zd:
                        up_zd_success = True

                # 记录选股信息
                xg_res.append(
                    {
                        "code": code,
                        "xg_date": cd.get_src_klines()[-1].date,
                        # 记录指标数据
                        "kdj_j_zz_num": len(idx_kdj_zz_ji),
                        "kdj_j_zz_vals": [
                            {"i": _i, "v": idx_kdj_js[_i]} for _i in idx_kdj_zz_ji
                        ],
                        "kdj_k_zz_vals": [
                            {"i": _i, "v": idx_kdj_ks[_i]} for _i in idx_kdj_zz_ji
                        ],
                        "kdj_d_zz_vals": [
                            {"i": _i, "v": idx_kdj_ds[_i]} for _i in idx_kdj_zz_ji
                        ],
                        "macd_dif": idx_macd["dif"][-1],
                        "macd_dea": idx_macd["dea"][-1],
                        "kdj_k": idx_kdj_ks[-1],
                        "kdj_d": idx_kdj_ds[-1],
                        # 记录金叉判断条件
                        "judge_kdj_gold": judge_kdj_gold,
                        "judge_macd_gold": judge_macd_gold,
                        # 记录其他信息
                        "idx_ma_5_close": idx_ma_5_close,
                        "idx_ma_10_close": idx_ma_10_close,
                        "idx_ma_20_close": idx_ma_20_close,
                        "pre_bi_mmds": "/".join(cd.get_bis()[-2].line_mmds("|")),
                        "last_zs_line_nums": last_zs.line_num,
                        "last_zs_type": last_zs.type,
                        "last_zs_direction": last_zs.lines[0].type,
                        # 记录结果
                        "bi_end_success": bi_end_success,
                        "up_zd_success": up_zd_success,
                        "xg_next_high": bi_next_bi.high if bi_next_bi else 0,
                    }
                )
                # 添加到自选，在图表中添加记录
                self.zx.add_stock(self.zx_group, code, None)
                db.marks_add(
                    self.market,
                    code,
                    "",
                    "",
                    fun.datetime_to_int(cd.get_src_klines()[-1].date),
                    "XG",
                    f"KDJ背离，MACD死叉，预示笔的结束 [k:{idx_kdj_ks[-1]:.4f} d:{idx_kdj_ds[-1]:.4f} macd dif:{idx_macd['dif'][-1]:.4f} macd dea: {idx_macd['dea'][-1]:.4f}]",
                    "earningUp",
                    "red",
                )
                tqdm.write(
                    f"{code} - {cd.get_src_klines()[-1].date} 符合选股 笔结束 {bi_end_success} 笔后涨幅超过zd {up_zd_success}"
                )

            except Exception as e:
                print(f"{code} 选股异常")
                print(traceback.format_exc())

        # 保存选股结果
        with open(self.xg_result_path / f"xg_{code}.pkl", "wb") as fp:
            pickle.dump(xg_res, fp)

        return True


if __name__ == "__main__":

    from chanlun.exchange.exchange_tdx import ExchangeTDX

    # 要执行历史选股的股票列表
    # run_codes = ["SZ.000019"]

    ex = ExchangeTDX()
    stocks = ex.all_stocks()
    run_codes = [
        _s["code"]
        for _s in stocks
        if _s["code"][0:5] in ["SH.60", "SZ.00", "SZ.30"] and "ST" not in _s["name"]
    ]
    # run_codes = run_codes[0:100]
    print(f"选股股票数量 {len(run_codes)}")

    # 实例化
    hxg = HistoryXuangu()

    # 清除自选与标记
    # hxg.clear_zx_mark()

    print("开始选股")
    print(f"{hxg.xg_start_date} ~ {hxg.xg_end_date}")

    # TODO 测试单个选股
    # hxg.xuangu_by_code("SZ.000019")

    # TODO 单进程执行选股
    # for code in run_codes:
    #     hxg.xuangu_by_code(code)

    # TODO 多进程执行选股，根据自己 cpu 核数来调整
    with ProcessPoolExecutor(
        max_workers=24, mp_context=get_context("spawn")
    ) as executor:
        bar = tqdm(total=len(run_codes))
        for _ in executor.map(
            hxg.xuangu_by_code,
            run_codes,
        ):
            bar.update(1)

    print("Done")
