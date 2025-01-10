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
        self.xg_start_date = "2016-01-01 00:00:00"
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
        if pathlib.Path(self.xg_result_path / f"xg_{code}.pkl").exists():
            return True

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
                # 计算 KDJ，判断一笔内的 kdj 背离，有背离的情况下，在 macd 死叉时记录选股点
                idx_kdj = Strategy.idx_kdj(cd, 9, 3, 3)
                idx_kdj_ks = idx_kdj["k"][-cal_bi_start_i:]
                idx_kdj_ds = idx_kdj["d"][-cal_bi_start_i:]
                idx_kdj_kd_up_croos_is = up_cross(idx_kdj_ks, idx_kdj_ds)
                if len(idx_kdj_kd_up_croos_is) < 2:  # 必须有大于两次下穿才可以
                    continue
                # 下穿的点都要在 50 以下
                idx_kdj_k_cross_max = max(
                    [idx_kdj_ks[_i] for _i in idx_kdj_kd_up_croos_is]
                )
                if idx_kdj_k_cross_max > 50:  # 必须小于 50
                    continue

                # 下穿的两点要背驰
                if (
                    idx_kdj_ks[idx_kdj_kd_up_croos_is[-1]]
                    < idx_kdj_ks[idx_kdj_kd_up_croos_is[-2]]
                ):
                    continue

                # 计算 MACD，并判断 macd 死叉
                idx_macd = Strategy.idx_macd(cd, 6, 13, 5)
                if (
                    idx_macd["dif"][-1] > idx_macd["dea"][-1]
                    and idx_macd["dif"][-2] < idx_macd["dea"][-2]
                ):
                    pass
                else:
                    continue

                # TODO 检查选股的这个时间点，笔的是否真正的结束了
                bi_end_success = False
                bi_next_bi: BI = None
                for _bi in all_cd.get_bis():
                    if _bi.end.k.date == bi.end.k.date:
                        bi_end_success = True
                        bi_next_bi = all_cd.get_bis()[_bi.index + 1]
                        break

                # TODO 如果笔结束，下一笔是否能够涨幅超过 zd
                up_zd_success = False
                if bi_end_success and bi_next_bi.end.val > last_zs.zd:
                    up_zd_success = True

                # 记录选股信息
                xg_res.append(
                    {
                        "code": code,
                        "xg_date": cd.get_src_klines()[-1].date,
                        "up_cross_num": len(idx_kdj_kd_up_croos_is),
                        "macd_dif": idx_macd["dif"][-1],
                        "macd_dea": idx_macd["dea"][-1],
                        "kdj_k": idx_kdj_ks[-1],
                        "kdj_d": idx_kdj_ds[-1],
                        "bi_end_success": bi_end_success,
                        "up_zd_success": up_zd_success,
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
                    f"KDJ背离，MACD死叉，预示笔的结束 [k:{idx_kdj_ks[-1]} d:{idx_kdj_ds[-1]} macd dif:{idx_macd['dif'][-1]} macd dea: {idx_macd['dea'][-1]}]",
                    "earningUp",
                    "red",
                )
                tqdm.write(f"{code} - {cd.get_src_klines()[-1].date} 符合选股")

            except Exception as e:
                print(f"{code} 选股异常")
                print(traceback.format_exc())

        # 保存选股结果
        with open(self.xg_result_path / f"xg_{code}.pkl", "wb") as fp:
            pickle.dump(xg_res, fp)

        return True


if __name__ == "__main__":

    # 要执行历史选股的股票列表
    run_codes = ["SZ.000001", "SZ.000002", "SZ.000004", "SZ.000006", "SZ.000007"]

    # 实例化
    hxg = HistoryXuangu()

    # 清除自选与标记
    # hxg.clear_zx_mark()

    print("开始选股")
    print(f"{hxg.xg_start_date} ~ {hxg.xg_end_date}")

    # TODO 测试单个选股
    # hxg.xuangu_by_code("SZ.000042")

    # TODO 单进程执行选股
    # for code in run_codes:
    #     hxg.xuangu_by_code(code)

    # TODO 多进程执行选股，根据自己 cpu 核数来调整
    with ProcessPoolExecutor(
        max_workers=18, mp_context=get_context("spawn")
    ) as executor:
        bar = tqdm(total=len(run_codes))
        for _ in executor.map(
            hxg.xuangu_by_code,
            run_codes,
        ):
            bar.update(1)

    print("Done")
