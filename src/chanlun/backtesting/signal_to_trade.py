# 信号回测文件，转换成交易模式

import copy
import datetime
import time
from typing import Dict, List

import pandas as pd
from tqdm.auto import tqdm

from chanlun.backtesting.backtest import BackTest
from chanlun.backtesting.backtest_trader import BackTestTrader
from chanlun.backtesting.base import Operation, Strategy
from chanlun.exchange.exchange_db import ExchangeDB

"""
信号回测结果转交易回测结果

# Demo
s_to_t = SignalToTrade("trade", mode="trade", market="a")
bt = s_to_t.run_bt("backtest/strategy_signal.pkl")
bt.result()
bt.result_by_pyfolio()
# 保存
bt.save_file = str("backtest/strategy_to_trade.pkl")
bt.save()

"""


class SignalToTrade(BackTestTrader):

    def __init__(
        self,
        name,
        mode="signal",
        market="a",
        init_balance=100000,
        fee_rate=0.0005,
        max_pos=10,
        log=None,
    ):
        super().__init__(name, mode, market, init_balance, fee_rate, max_pos, log)

        self.log = tqdm.write

        self.base_code: str = None
        self.trade_max_pos: int = None
        self.trade_start_date: str = None
        self.trade_end_date: str = None
        self.trade_mmds: List[str] = None
        self.trade_pos_querys: Dict[str, List[str]] = None
        self.close_uids: List[str] = ["clear"]

        self.allow_codes: List[str] = None  # 允许交易的代码

        self.real_trade_full_sort = "default"  # default 默认，按照信号的顺序执行；zf 按照已有信号到目前的涨幅排序执行

        self.trade_strategy: Strategy = None

        self.start_date: str = None
        self.end_date: str = None

        # 根据信号中的价格赋值
        self.code_price: Dict[str, float] = {}
        self.now_datetime: datetime.datetime = None

        # 缓存K线
        self.cache_klines: Dict[str, pd.DataFrame] = {}

        self.market: str = None
        self.ex: ExchangeDB = None
        self.frequencys = []

    def get_price(self, code):
        try:
            if code not in self.cache_klines.keys():
                s_time = time.time()
                self.cache_klines[code] = self.ex.klines(
                    code,
                    self.frequencys[-1],
                    start_date=self.start_date,
                    end_date=self.end_date,
                    args={"limit": 9999999},
                )
                self.add_times("st_cache_klines", time.time() - s_time)

            s_time = time.time()

            if self.market in ["us"]:
                kline = self.cache_klines[code][
                    self.cache_klines[code]["date"] < self.now_datetime
                ]
            elif self.market in ["currency", "futures"]:
                end_date = self.now_datetime
                kline = self.cache_klines[code][
                    self.cache_klines[code]["date"] < end_date
                ]
            else:
                kline = self.cache_klines[code][
                    self.cache_klines[code]["date"] <= self.now_datetime
                ]
            # tqdm.write(f"{self.now_datetime} {code} {kline.iloc[-1]['close']}")
            self.add_times("st_get_price", time.time() - s_time)

            return {
                "date": kline.iloc[-1]["date"],
                "open": float(kline.iloc[-1]["open"]),
                "close": float(kline.iloc[-1]["close"]),
                "high": float(kline.iloc[-1]["high"]),
                "low": float(kline.iloc[-1]["low"]),
            }
        except Exception as e:
            print(code, self.now_datetime, e)
            raise Exception(f"{code} - {self.now_datetime} 没有价格")

    def get_now_datetime(self):
        return self.now_datetime

    def run_bt(self, bt_file: str):
        BT = BackTest()
        BT.save_file = bt_file
        BT.load(BT.save_file)

        BT.mode = "trade"

        # 设置交易数据
        self.market = BT.market
        self.fee_rate = BT.fee_rate
        self.max_pos = BT.max_pos
        self.frequencys = BT.frequencys
        self.datas = BT.datas

        BT.init_balance = self.balance

        if self.base_code is not None:
            BT.base_code = self.base_code
        if self.trade_max_pos is not None:
            self.max_pos = self.trade_max_pos
        if self.trade_end_date is not None:
            BT.start_datetime = self.trade_start_date
        if self.trade_end_date is not None:
            BT.end_datetime = self.trade_end_date
        if self.trade_strategy is not None:
            BT.strategy = self.trade_strategy

        self.start_date = BT.start_datetime
        self.end_date = BT.end_datetime

        self.ex = ExchangeDB(BT.market)

        base_dates = self.ex.klines(
            BT.base_code,
            BT.next_frequency,
            start_date=BT.start_datetime,
            end_date=BT.end_datetime,
        )
        base_dates = base_dates["date"].to_list()

        # 获取所有的历史持仓
        info_keys = []
        for _, _poss in BT.trader.positions_history.items():
            for _p in _poss:
                info_keys += list(_p.info.keys())
        info_keys = list(sorted(list(set(info_keys))))

        pos_df = BT.positions(add_columns=info_keys, close_uids=self.close_uids)
        pos_df["_win"] = pos_df["profit_rate"].apply(lambda r: 1 if r > 0 else 0)
        if self.trade_mmds is not None:
            pos_df = pos_df.query("mmd in @self.trade_mmds")
        if self.allow_codes is not None:
            pos_df = pos_df.query("code in @self.allow_codes")

        if self.trade_pos_querys is not None:
            for _mmd, _qs in self.trade_pos_querys.items():
                for _q in _qs:
                    pos_df = pos_df.query(_q)

        print("原始信号数量:", len(pos_df))
        print(
            pos_df.groupby(["mmd"]).agg(
                {
                    "profit_rate": {"mean", "sum", "count"},
                    "_win": {"count", "sum", "mean"},
                }
            )
        )

        for _d in tqdm(base_dates, desc="交易进度"):
            self.now_datetime = _d
            self.datas.now_date = _d

            trade_pos_codes = self.position_codes()

            # 查询当前要平仓的仓位
            close_pos_operations: List[Dict] = []
            if len(trade_pos_codes) > 0:
                s_time = time.time()
                for _code, _poss in BT.trader.positions_history.items():
                    if _code not in trade_pos_codes:
                        continue
                    for _p in _poss:
                        for _c_p in _p.close_records:
                            if (
                                _c_p["datetime"] == _d
                                and _c_p["close_uid"] in self.close_uids
                            ):
                                if _code in ["SZ.300496"]:
                                    a = 1
                                _add_c_oper = copy.copy(_c_p)
                                _add_c_oper["code"] = _p.code
                                _add_c_oper["mmd"] = _p.mmd
                                close_pos_operations.append(_add_c_oper)
                self.add_times("st_query_close_poss", time.time() - s_time)

            # 查询当前要开仓的仓位
            s_time = time.time()
            open_pos_operations: List[Dict] = []
            for _code, _poss in BT.trader.positions_history.items():
                for _p in _poss:
                    for _o_r in _p.open_records:
                        if _o_r["datetime"] == _d:
                            _add_o_oper = copy.copy(_o_r)
                            _add_o_oper["code"] = _code
                            _add_o_oper["mmd"] = _p.mmd
                            _add_o_oper["info"] = _p.info
                            open_pos_operations.append(_add_o_oper)
            self.add_times("st_query_open_poss", time.time() - s_time)

            # 优先进行平仓操作
            for _c_oper in close_pos_operations:
                opt = Operation(
                    code=_c_oper["code"],
                    opt="sell",
                    mmd=_c_oper["mmd"],
                    msg=_c_oper["close_msg"],
                    pos_rate=_c_oper["pos_rate"],
                    close_uid="clear",
                    key=_c_oper["close_key"],
                )
                tqdm.write(
                    f"{_d} : 平仓: {_c_oper['code']} {_c_oper['mmd']} {_c_oper['close_msg']} 占比 {_c_oper['pos_rate']}"
                )
                _t_pos = [
                    p
                    for _, p in self.positions.items()
                    if p.balance != 0 and p.code == _c_oper["code"]
                ]
                if len(_t_pos) > 0:
                    self.execute(opt.code, opt, _t_pos[0])

            # 只保留有资金的持仓
            self.positions = {
                _uid: _p for _uid, _p in self.positions.items() if _p.amount != 0
            }

            # 进行开仓操作
            open_opts = []
            for _o_oper in open_pos_operations:
                opt = Operation(
                    code=_o_oper["code"],
                    opt="buy",
                    mmd=_o_oper["mmd"],
                    loss_price=_o_oper["price"],
                    info={
                        _k: _v for _k, _v in _o_oper["info"].items() if _k in info_keys
                    },
                    msg=_o_oper["open_msg"],
                    open_uid=_o_oper["open_uid"],
                    key=_o_oper["open_key"],
                    pos_rate=_o_oper["pos_rate"],
                )

                open_opts.append(opt)
            if BT.strategy.is_filter_opts():
                open_opts = BT.strategy.filter_opts(open_opts, self)

            s_time = time.time()
            for _opt in open_opts:
                if self.execute(_opt.code, _opt):
                    tqdm.write(
                        f"{_d} : 开仓: {_opt.code} {_opt.mmd} {_opt.msg} 占比 {_opt.pos_rate}"
                    )
            self.add_times("st_execute", time.time() - s_time)

            s_time = time.time()
            self.update_position_record()
            self.add_times("st_update_position_record", time.time() - s_time)

        self.end()

        self.cache_klines = {}
        BT.trader = self

        return BT
