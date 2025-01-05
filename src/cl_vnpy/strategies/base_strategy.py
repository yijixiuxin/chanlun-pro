import pytz
from vnpy.trader.constant import Interval
from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
)

from chanlun import cl
from chanlun.backtesting.backtest_trader import BackTestTrader
from chanlun.backtesting.base import *
from chanlun.strategy.strategy_demo import StrategyDemo
from chanlun import fun


class VNPYTrader(BackTestTrader):

    def __init__(self, name, cta):
        super().__init__(name, "online", market="futures")
        self.cta = cta

        # 固定交易手数
        self.fixed_amount = 1

    def open_buy(self, code, opt: Operation):
        """
        买入开仓
        """
        price_info = self.datas.last_k_info(code)
        price = price_info["close"]
        self.cta.write_log(
            "买入开仓做多，价格 %s 数量 %s 交易信号 %s"
            % (price, self.fixed_amount, opt.msg)
        )
        self.cta.buy(price, self.fixed_amount)
        return {"price": price, "amount": self.fixed_amount}

    def open_sell(self, code, opt: Operation):
        """
        卖出开仓
        """
        price_info = self.datas.last_k_info(code)
        price = price_info["close"]

        self.cta.write_log(
            "卖出开仓做空，价格 %s 数量 %s 交易信号 %s"
            % (price, self.fixed_amount, opt.msg)
        )
        self.cta.short(price, self.fixed_amount)
        return {"price": price, "amount": self.fixed_amount}

    def close_buy(self, code, pos: POSITION, opt: Operation):
        """
        平多仓
        """
        price_info = self.datas.last_k_info(code)
        price = price_info["close"]

        self.cta.write_log(
            "卖出平仓做多，价格 %s 数量 %s 交易信号 %s" % (price, pos.amount, opt.msg)
        )
        self.cta.sell(price, pos.amount)
        return {"price": price, "amount": pos.amount}

    def close_sell(self, code, pos: POSITION, opt: Operation):
        """
        平空仓
        """
        price_info = self.datas.last_k_info(code)
        price = price_info["close"]
        self.cta.write_log(
            "买入平仓做空，价格 %s 数量 %s 交易信号 %s" % (price, pos.amount, opt.msg)
        )
        self.cta.cover(price, pos.amount)
        return {"price": price, "amount": pos.amount}


class VNPYDatas(MarketDatas):
    """
    VNPY 的数据类，只有单个行情的，所以这里的 code 没太大用处
    """

    def __init__(self, symbol, frequencys: List[str], cl_config: dict):
        super().__init__("futures", frequencys, cl_config)

        self.symbol = symbol
        self.frequencys = frequencys
        self.now_date = None
        # 用来保存k线数据
        self.cache_klines: Dict[str, pd.DataFrame] = {}
        for f in self.frequencys:
            self.cache_klines[f] = pd.DataFrame(
                [], columns=["code", "date", "open", "close", "high", "low", "volume"]
            )

    def on_30m_bar(self, bar: BarData):
        """
        30M 周期 bar 生成后的回调
        """
        key = "30_1m"
        k = {
            "code": self.symbol,
            "date": bar.datetime,
            "open": bar.open_price,
            "close": bar.close_price,
            "high": bar.high_price,
            "low": bar.low_price,
            "volume": bar.volume,
        }
        self.now_date = bar.datetime
        self.cache_klines[key] = self.cache_klines[key].append(k, ignore_index=True)
        return True

    def on_15m_bar(self, bar: BarData):
        """
        15m 周期 bar 生成后的回调
        """
        key = "15_1m"
        k = {
            "code": self.symbol,
            "date": bar.datetime,
            "open": bar.open_price,
            "close": bar.close_price,
            "high": bar.high_price,
            "low": bar.low_price,
            "volume": bar.volume,
        }
        self.now_date = bar.datetime
        self.cache_klines[key] = self.cache_klines[key].append(k, ignore_index=True)
        return True

    def on_10m_bar(self, bar: BarData):
        """
        10m 周期 bar 生成后的回调
        """
        key = "10_1m"
        k = {
            "code": self.symbol,
            "date": bar.datetime,
            "open": bar.open_price,
            "close": bar.close_price,
            "high": bar.high_price,
            "low": bar.low_price,
            "volume": bar.volume,
        }
        self.now_date = bar.datetime
        self.cache_klines[key] = self.cache_klines[key].append(k, ignore_index=True)
        return True

    def on_5m_bar(self, bar: BarData):
        """
        5m 周期 bar 生成后的回调
        """
        key = "5_1m"
        k = {
            "code": self.symbol,
            "date": bar.datetime,
            "open": bar.open_price,
            "close": bar.close_price,
            "high": bar.high_price,
            "low": bar.low_price,
            "volume": bar.volume,
        }
        self.now_date = bar.datetime
        self.cache_klines[key] = self.cache_klines[key].append(k, ignore_index=True)
        return True

    def on_1m_bar(self, bar: BarData):
        """
        1m 周期 bar 生成后的回调
        """
        key = "1_1m"
        k = {
            "code": self.symbol,
            "date": bar.datetime,
            "open": bar.open_price,
            "close": bar.close_price,
            "high": bar.high_price,
            "low": bar.low_price,
            "volume": bar.volume,
        }
        self.now_date = bar.datetime
        self.cache_klines[key] = self.cache_klines[key].append(k, ignore_index=True)
        return True

    def klines(self, code, frequency) -> pd.DataFrame:
        return self.cache_klines[frequency]

    def last_k_info(self, code) -> dict:
        f = self.frequencys[-1]
        return {
            "date": self.cache_klines[f].iloc[-1]["date"],
            "open": float(self.cache_klines[f].iloc[-1]["open"]),
            "close": float(self.cache_klines[f].iloc[-1]["close"]),
            "high": float(self.cache_klines[f].iloc[-1]["high"]),
            "low": float(self.cache_klines[f].iloc[-1]["low"]),
        }

    def get_cl_data(self, code, frequency, cl_config: dict = None) -> ICL:
        klines = self.klines(code, frequency)
        if frequency not in self.cl_datas.keys():
            self.cl_datas[frequency] = cl.CL(
                code, frequency, self.cl_config
            ).process_klines(klines)
        else:
            self.cl_datas[frequency].process_klines(klines)
        return self.cl_datas[frequency]


class BaseStrategy(CtaTemplate):
    """
    单标的，多周期回测基类
    """

    author = "WX"
    parameters = []
    variables = []

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        # 缠论计算配置
        self.cl_config = {"xd_bzh": "xd_bzh_no"}
        self.frequencys = ["5_1m", "1_1m"]

        # 交易对象
        self.TR = VNPYTrader("backtest", self)
        # 数据对象
        self.Data = VNPYDatas(self.vt_symbol, self.frequencys, self.cl_config)

        # 这里指定缠论策略，根据策略信号进行交易
        self.STR: Strategy = StrategyDemo()
        self.TR.set_strategy(self.STR)
        self.TR.set_data(self.Data)

        # 合成的对象
        self.bgs: Dict[str, BarGenerator] = {}

        # 要运行的周期，以及回调的方法（大周期的在前面）
        self.intervals = [
            {
                "windows": 5,
                "interval": Interval.MINUTE,
                "callback": self.Data.on_5m_bar,
            },
            {
                "windows": 1,
                "interval": Interval.MINUTE,
                "callback": self.Data.on_1m_bar,
            },
        ]

        for interval in self.intervals:
            _key = "%s_%s" % (interval["windows"], interval["interval"].value)
            self.bgs[_key] = BarGenerator(
                self.on_bar,
                window=interval["windows"],
                on_window_bar=interval["callback"],
                interval=interval["interval"],
            )

    def on_init(self):
        """
        Callback when strategies is inited.
        """
        self.write_log("策略初始化")

        def update_bar(bar: BarData):
            for _, bg in self.bgs.items():
                bg.update_bar(bar)

        # 默认加载几天的历史数据（根据回测开始时间进行算，这几天的数据不参与策略计算）
        self.load_bar(5, callback=update_bar)

    def on_start(self):
        """
        Callback when strategies is started.
        """
        self.write_log("策略启动")

    def on_stop(self):
        """
        Callback when strategies is stopped.
        """
        self.write_log("策略停止")

    def on_tick(self, tick: TickData):
        """
        实盘的时候才会执行，用于生成 bar
        """
        for _key, _bg in self.bgs.items():
            # 只需要执行一下就好
            _bg.update_tick(tick)
            break

    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        for _, bg in self.bgs.items():
            bg.update_bar(bar)

        self.TR.run(self.vt_symbol)

        self.put_event()

    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        pass

    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        self.put_event()

    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        pass
