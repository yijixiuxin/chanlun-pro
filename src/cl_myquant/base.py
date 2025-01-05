from gm.api import *

from chanlun import cl
from chanlun.backtesting.backtest_trader import BackTestTrader
from chanlun.backtesting.base import *


class MyQuantData(MarketDatas):
    """
    掘进行情数据类
    """

    def __init__(self, content: Context, frequencys: List[str], cl_config=None):
        super().__init__("a", frequencys, cl_config)

        self.content = content

    @property
    def now_date(self):
        return self.content.now

    def klines(self, code, frequency, count=2000) -> pd.DataFrame:
        """
        获取的标的必须要 subscribe 订阅过才可以
        """
        klines = self.content.data(
            code,
            frequency,
            count=2000,
            fields="symbol,eob,bob,open,close,high,low,volume,frequency",
        )

        klines.loc[:, "code"] = klines["symbol"]
        klines.loc[:, "date"] = pd.to_datetime(klines["eob"])
        return klines[["code", "date", "open", "close", "high", "low", "volume"]]

    def last_k_info(self, code) -> dict:
        kline = self.klines(code, self.frequencys[-1], count=1)
        return {
            "date": kline.iloc[-1]["date"],
            "open": float(kline.iloc[-1]["open"]),
            "close": float(kline.iloc[-1]["close"]),
            "high": float(kline.iloc[-1]["high"]),
            "low": float(kline.iloc[-1]["low"]),
        }

    def init_cl_datas(self, codes, frequencys):
        """
        重新初始化指定的标的列表的缠论数据
        """
        old_key_list = self.cache_cl_datas.keys()
        key_list = []
        for frequency in frequencys:
            for code in codes:
                key = f"{code}_{frequency}"
                key_list.append(key)
                klines = self.klines(code, frequency)
                if key not in self.cache_cl_datas.keys():
                    self.cache_cl_datas[key] = cl.CL(
                        code, frequency, self.cl_config
                    ).process_klines(klines)
                else:
                    self.cache_cl_datas[key].process_klines(klines)

        # 删除不在计划数据，节省内存
        del_key_list = list(set(old_key_list).difference(set(key_list)))
        for key in del_key_list:
            del self.cache_cl_datas[key]

        return True

    def update_bars(self, bars):
        """
        更新 bars 数据到缠论数据对象中
        """
        klines = []
        frequency = None
        for b in bars:
            frequency = b["frequency"]
            klines.append(
                {
                    "symbol": b["symbol"],
                    "frequency": b["frequency"],
                    "open": b["open"],
                    "close": b["close"],
                    "high": b["high"],
                    "low": b["low"],
                    "volume": b["volume"],
                    "eob": b["eob"],
                }
            )
        klines = pd.DataFrame(klines)
        klines.loc[:, "code"] = klines["symbol"]
        klines.loc[:, "date"] = pd.to_datetime(klines["eob"])
        klines = klines[["code", "date", "open", "close", "high", "low", "volume"]]

        key = f"{klines.iloc[0]['code']}_{frequency}"
        self.cache_cl_datas[key].process_klines(klines)
        return True

    def get_cl_data(self, code, frequency, cl_config: dict = None) -> ICL:
        key = f"{code}_{frequency}"
        return self.cache_cl_datas[key]


class MyQuantTrader(BackTestTrader):
    """
    掘金交易类
    """

    def __init__(self, name, context: Context):

        super().__init__(name, mode="online", market="a")

        self.context = context

        self.max_pos = 10

    # 做多买入
    def open_buy(self, code, opt: Operation, amount: float = None):

        pos_count = len(self.context.account().positions())
        if pos_count >= self.max_pos:
            return False

        available_cash = self.context.account().cash["available"]
        use_cash = available_cash / (self.max_pos - pos_count)
        res = order_value(
            code, use_cash, OrderSide_Buy, OrderType_Market, PositionEffect_Open
        )

        msg = f"股票买入 {code} 价格 {res[0]['filled_vwap']} 数量 {res[0]['filled_volume']} 原因 {opt.msg}"
        print(msg)
        return {"price": res[0]["filled_vwap"], "amount": res[0]["filled_volume"]}

    # 做空卖出
    def open_sell(self, code, opt: Operation, amount: float = None):
        return False

    # 做多平仓
    def close_buy(self, code, pos: POSITION, opt):

        position = self.context.account().positions(code, side=PositionSide_Long)
        if position is None:
            return False
        res = order_target_volume(
            symbol=code,
            volume=0,
            position_side=PositionSide_Long,
            order_type=OrderType_Market,
        )

        msg = f"股票卖出 {code} 价格 {res[0]['filled_vwap']} 数量 {res[0]['filled_volume']} 原因 {opt.msg}"
        print(msg)
        return {"price": res[0]["filled_vwap"], "amount": res[0]["filled_volume"]}

    # 做空平仓
    def close_sell(self, code, pos: POSITION, opt):
        return False
