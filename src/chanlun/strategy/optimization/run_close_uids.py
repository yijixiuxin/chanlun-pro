from typing import List
from chanlun.backtesting import backtest
from chanlun.strategy.strategy_a_d_mmd_test import StrategyADMMDTest
import pandas as pd
import numpy as np
import pickle
import pathlib
from itertools import combinations
from chanlun.config import get_data_path
from tqdm.auto import tqdm
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
import gc

BT = backtest.BackTest()
BT.save_file = str(get_data_path() / "backtest" / "a_d_mmd_v0_signal_no_strategy.pkl")
BT.load(BT.save_file)
# BT.base_code = 'SH.600000'

gc.collect()

# 读取当前策略所使用的 close_uid
close_uids = []
for _, _poss in BT.trader.positions_history.items():
    for _p in _poss:
        close_uids += list(_p.close_uid_profit.keys())
close_uids = list(sorted(list(set(close_uids))))
print(close_uids)

if "clear" in close_uids:
    close_uids.remove("clear")


# 获取给定平仓标记组合的盈亏和胜率
def get_close_uids_profit_rate(close_uids: list):
    pos_df = BT.positions(
        close_uids=close_uids,
    )
    pos_df["_win"] = pos_df["profit_rate"].apply(lambda x: int(x > 0))
    # pos_df = pos_df.query("code_is_zt_hys==True and index_SH_000001_k_ashi_red_1==1")
    res = pos_df.groupby(["mmd"]).agg(
        {"profit_rate": {"mean", "sum", "count"}, "_win": {"count", "sum", "mean"}}
    )
    sum_profit = res["profit_rate"]["sum"].sum()
    win_mean = res["_win"]["mean"].mean()
    gc.collect()
    return {"uids": close_uids, "sum_profit": sum_profit, "win_mean": win_mean}


if __name__ == "__main__":

    # 记录每个 close_uids 的收益
    sum_profits = {}
    save_file = pathlib.Path("./sum_profits_v0.pkl")

    def get_keys_id(keys: list):
        return "_".join(sorted(keys))

    # 获取 close_uids 中列表，任意个数的组合
    close_groups = []
    # 如果组合文件存在，从文件中读取
    if pathlib.Path("close_groups.pkl").is_file():
        with open("close_groups.pkl", "rb") as fp:
            close_groups = pickle.load(fp)
    else:
        for i in range(1, len(close_uids) + 1):
            for _cus in combinations(close_uids, i):
                _cus: List[str] = list(_cus)
                _cus.append("clear")
                if len(_cus) > 10:
                    continue
                # 过滤有冲突的close_uid
                if len([_c for _c in _cus if _c.startswith("利润回调")]) >= 2:
                    continue
                if len([_c for _c in _cus if "日均线" in _c]) >= 2:
                    continue
                _id = get_keys_id(_cus)
                if _id not in sum_profits.keys():
                    close_groups.append(_cus)
        # 将组合保存到 文件中
        with open("close_groups.pkl", "wb") as fp:
            pickle.dump(close_groups, fp)
        pass

    tqdm.write(f"计算组合数量：{len(close_groups)}")

    with ProcessPoolExecutor(
        max_workers=5, mp_context=get_context("spawn")
    ) as executor:
        bar = tqdm(total=len(close_groups), desc="计算进度")
        for _cus_profit in executor.map(get_close_uids_profit_rate, close_groups):
            bar.update(1)
            _id = get_keys_id(_cus_profit["uids"])
            # 如果 sum_profits 数量大于 10000
            if len(sum_profits) >= 100000:
                # 按照 sum_profit 从大到小排序
                rank_profit = sorted(
                    sum_profits.items(), key=lambda x: x[1]["sum_profit"], reverse=True
                )
                # 获取前 1000 个key
                top_profit = [x[0] for x in rank_profit[:1000]]
                tqdm.write(
                    f"top_profit: {[x[1]['sum_profit'] for x in rank_profit[:100]]}"
                )
                # 按照 win_mean 从大到小排序
                rank_win_mean = sorted(
                    sum_profits.items(), key=lambda x: x[1]["win_mean"], reverse=True
                )
                # 获取前 200 个key
                top_win_mean = [x[0] for x in rank_win_mean[:1000]]
                tqdm.write(
                    f"top_win_mean: {[x[1]['win_mean'] for x in rank_win_mean[:100]]}"
                )

                # 合并两个列表，并只保留再 top 列表中的 key
                top_keys = list(set(top_profit + top_win_mean))
                sum_profits_keys = list(sum_profits.keys())
                for _k in sum_profits_keys:
                    if _k not in top_keys:
                        sum_profits.pop(_k)

                tqdm.write(f"sum_profits: {len(sum_profits)}")
                # 进行保存
                with open(save_file, "wb") as fp:
                    pickle.dump(sum_profits, fp)

                gc.collect()

            # tqdm.write(f"{_id}: {_cus_profit['sum_profit']}, {_cus_profit['win_mean']}")
            sum_profits[_id] = _cus_profit

    # 进行保存
    with open(save_file, "wb") as fp:
        pickle.dump(sum_profits, fp)

    profit_ranks = sorted(
        sum_profits.items(), key=lambda x: x[1]["sum_profit"], reverse=True
    )
    ct = 0
    max_key_nums = 0
    cuids = []
    for pr in profit_ranks[0:100]:
        print("uids : ", pr[1]["uids"])
        print("profit : ", pr[1]["sum_profit"], pr[1]["win_mean"])
        print(" * " * 20)
        cuids += pr[1]["uids"]
        cuids = list(set(cuids))

    print(cuids)

    print("Done")
