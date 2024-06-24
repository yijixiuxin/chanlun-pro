# 信号回测文件，转换成交易模式

import time
import datetime
from chanlun import fun
from chanlun.backtesting.base import Operation, Strategy
from chanlun.backtesting.backtest_trader import BackTestTrader
from chanlun.backtesting.backtest import BackTest
from chanlun.cl_interface import *
from chanlun.exchange.exchange_db import ExchangeDB
from tqdm.auto import tqdm


"""
信号回测结果转交易回测结果

# Demo
s_to_t = SignalToTrade("trade", mode="trade", is_stock=True, is_futures=False)
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
        is_stock=True,
        is_futures=False,
        init_balance=100000,
        fee_rate=0.0005,
        max_pos=10,
        log=None,
    ):
        super().__init__(
            name, mode, is_stock, is_futures, init_balance, fee_rate, max_pos, log
        )

        self.log = tqdm.write

        self.base_code: str = None
        self.trade_max_pos: int = None
        self.trade_start_date: str = None
        self.trade_end_date: str = None
        self.trade_mmds: List[str] = None
        self.trade_pos_querys: Dict[str, List[str]] = None
        self.close_uids: List[str] = ["clear"]

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

    def get_now_datetime(self):
        return self.now_datetime

    def run_bt(self, bt_file: str):
        BT = BackTest()
        BT.save_file = bt_file
        BT.load(BT.save_file)

        BT.mode = "trade"

        # 设置交易数据
        self.market = BT.market
        self.is_stock = BT.is_stock
        self.is_futures = BT.is_futures
        self.balance = BT.init_balance
        self.fee_rate = BT.fee_rate
        self.max_pos = BT.max_pos
        self.frequencys = BT.frequencys
        self.datas = BT.datas

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
            close_poss: List[Dict] = []
            if len(trade_pos_codes) > 0:
                s_time = time.time()
                close_poss: pd.DataFrame = pos_df.query(
                    "code in @trade_pos_codes and close_datetime == @_d"
                )
                if len(close_poss) == 0:
                    close_poss = []
                else:
                    close_poss = close_poss.to_dict(orient="records")
                self.add_times("st_query_close_poss", time.time() - s_time)

            # 查询当前要开仓的仓位
            s_time = time.time()
            open_poss: List[Dict] = pos_df.query("open_datetime == @_d").to_dict(
                orient="records"
            )
            self.add_times("st_query_open_poss", time.time() - s_time)

            # 优先进行平仓操作
            for _pos in close_poss:
                opt = Operation(
                    code=_pos["code"],
                    opt="sell",
                    mmd=_pos["mmd"],
                    msg=_pos["close_msg"],
                    close_uid="clear",
                )
                # tqdm.write(
                #     f"平仓: {_pos['code']} {_pos['mmd']} {_pos['profit_rate']} {_pos['close_msg']}"
                # )
                open_uid = f"{_pos['code']}:{_pos['open_uid']}"
                _t_pos = [
                    p
                    for _, p in self.positions.items()
                    if p.balance != 0 and p.open_uid == open_uid
                ]
                if len(_t_pos) > 0:
                    self.execute(_pos["code"], opt, _t_pos[0])
            # 进行开仓操作
            open_opts = []
            for _pos in open_poss:
                opt = Operation(
                    code=_pos["code"],
                    opt="buy",
                    mmd=_pos["mmd"],
                    loss_price=_pos["loss_price"],
                    info=_pos,
                    msg=_pos["open_msg"],
                    open_uid=f"{_pos['code']}:{_pos['open_uid']}",
                )
                # tqdm.write(
                #     f"开仓: {_pos['code']} {_pos['mmd']} {_pos['price']} {_pos['open_msg']}"
                # )
                open_opts.append(opt)
            if BT.strategy.is_filter_opts():
                open_opts = BT.strategy.filter_opts(open_opts, self)

            s_time = time.time()
            for _opt in open_opts:
                self.execute(_opt.code, _opt)
            self.add_times("st_execute", time.time() - s_time)

            s_time = time.time()
            self.update_position_record()
            self.add_times("st_update_position_record", time.time() - s_time)

        self.end()

        BT.trader = self

        return BT
