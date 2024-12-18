from chanlun.backtesting import backtest
from chanlun.backtesting.signal_to_trade import SignalToTrade
from chanlun.config import get_data_path
from chanlun.strategy.strategy_a_d_mmd_test import StrategyADMMDTest

from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from tqdm.auto import tqdm
import pathlib

bt_file = get_data_path() / "backtest" / "a" / "a_d_mmd_v0_signal.pkl"

# 实盘使用的平仓条件
close_uid = [
    "利润回调5%",
    "clear",
]


def run_bt(ags: tuple):
    _f_k = ags[0]
    _f_r = ags[1]
    _n = ags[2]

    save_to_file = str(
        get_data_path()
        / "backtest"
        / f"a_d_mmd_v0_signal_to_trade_profit_{_f_k}_{_f_r}_{_n}.pkl"
    )
    if pathlib.Path(save_to_file).is_file():
        return save_to_file

    print(f"Save File {save_to_file}")
    tqdm.write(f"Filter Key {_f_k} Filter Reverse {_f_r} Max Pos {_n}")

    s_to_t = SignalToTrade("trade", mode="trade", market="a")
    s_to_t.close_uids = close_uid
    s_to_t.trade_strategy = StrategyADMMDTest(
        "test", filter_key=_f_k, filter_reverse=_f_r
    )
    # s_to_t.trade_pos_querys = pos_querys
    s_to_t.trade_max_pos = _n
    s_to_t.log = None  # 不输出日志
    bt = s_to_t.run_bt(bt_file)
    bt.result()
    bt.save_file = save_to_file
    bt.save()
    tqdm.write(" * " * 20)
    print(bt.trader.use_times)
    return save_to_file


if __name__ == "__main__":
    filter_keys = [
        "k_now_d_change",
        "loss_rate",
    ]
    filter_reverst = [True, False]
    max_pos = [2, 3, 4, 5, 6]

    loop_args = []
    for _f_k in filter_keys:
        for _f_r in filter_reverst:
            for _n in max_pos:
                loop_args.append((_f_k, _f_r, _n))

    print("回测次数：", len(loop_args))

    bt_files = []

    with ProcessPoolExecutor(
        max_workers=4, mp_context=get_context("spawn")
    ) as executor:
        bar = tqdm(total=len(loop_args), desc="回测进度")
        for _ in executor.map(run_bt, loop_args):
            bar.update(1)
            bt_files.append(_)

    # for _lags in loop_args:
    #     bt_files.append(run_bt(_lags))

    print(len(bt_files))

    # 加载并计算每个回测最终的余额
    bt_balances = []
    for _f in bt_files:
        BT = backtest.BackTest()
        BT.save_file = _f
        BT.load(BT.save_file)
        bt_balances.append({"bt_file": _f, "balance": BT.trader.balance})

    # 按照 balance 由大到小进行排序，并打印前10名
    bt_ranks = sorted(bt_balances, key=lambda b: b["balance"], reverse=True)
    for _r in bt_ranks:
        print(_r["bt_file"])
        print(_r["balance"])
        print(" * " * 20)

    print("Done")
