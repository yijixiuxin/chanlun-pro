import datetime
import math
from abc import ABC, abstractmethod
from typing import Dict, List, Union

import MyTT
import numpy as np
import pandas as pd
import talib

from chanlun.cl_interface import BI, ICL, XD, ZS
from chanlun.cl_utils import cal_zs_macd_infos
from chanlun.fun import get_logger


class POSITION:
    """
    持仓对象
    """

    def __init__(
        self,
        code: str,
        mmd: str,
        type: str = None,
        balance: float = 0,
        price: float = 0,
        amount: float = 0,
        loss_price: float = None,
        open_date: str = None,
        open_datetime: datetime.datetime = None,
        close_datetime: datetime.datetime = None,
        profit_rate: float = 0,
        max_profit_rate: float = 0,
        max_loss_rate: float = 0,
        open_msg: str = "",
        close_msg: str = "",
        info: Dict = None,
        open_uid: str = None,
    ):
        self.code: str = code
        self.mmd: str = mmd
        self.type: str = type
        self.balance: float = balance  # 持仓占用的金额
        self.release_balance: float = 0  # 平仓释放的金额
        self.price: float = price
        self.amount: float = amount  # 持仓数量，为 0 则表示没有持仓
        self.loss_price: float = loss_price
        self.open_date: str = open_date
        self.open_datetime: datetime = open_datetime
        self.close_datetime: datetime = close_datetime
        self.fee: float = 0  # 记录总过的手续费之和（开仓+平仓）
        self.profit: float = 0  # 收益金额
        self.profit_rate: float = profit_rate  # 收益率
        self.max_profit_rate: float = max_profit_rate  # 仅供参考，不太精确
        self.max_loss_rate: float = max_loss_rate  # 仅供参考，不太精确
        self.open_msg: str = open_msg
        self.close_msg: str = close_msg
        self.info: Dict = info
        self.open_uid: str = open_uid
        # 仓位控制相关
        # 记录当前开仓所占比例
        self.now_pos_rate: float = 0
        # 记录开仓的唯一key记录，避免多次重复开仓
        self.open_keys: Dict[str, float] = {}
        # 记录平仓的唯一key记录，避免多次重复平仓
        self.close_keys: Dict[str, float] = {}

        # 开仓记录信息
        self.open_records: List[dict] = []
        # 平仓记录信息
        self.close_records: List[dict] = []

    def __close_records_by_uids(self, uids: List[str] = None):
        """
        根据 uid 关闭记录
        """
        if uids is None:
            return None
        if "clear" not in uids:
            uids.append("clear")
        # 按照时间从早到晚排序
        close_profit = sorted(
            self.close_uid_profit.items(), key=lambda _r: _r[1]["close_datetime"]
        )
        for _r in close_profit:
            if _r[0] in uids:
                return _r[1]
        raise Exception(
            f"{self.code} - {self.mmd} - {self.open_datetime} 没有找到对应的平仓记录: {uids}"
        )

    def get_close_profit(self, uids: List[str] = None):
        if uids is None:
            return {
                "close_datetime": self.close_datetime,
                "profit": self.profit,
                "profit_rate": self.profit_rate,
                "max_profit_rate": self.max_profit_rate,
                "max_loss_rate": self.max_loss_rate,
                "close_msg": self.close_msg,
            }
        close_profit = self.__close_records_by_uids(uids)
        return {
            "close_datetime": close_profit["close_datetime"],
            "profit": close_profit["profit"],
            "profit_rate": close_profit["profit_rate"],
            "max_profit_rate": close_profit["max_profit_rate"],
            "max_loss_rate": close_profit["max_loss_rate"],
            "close_msg": close_profit["close_msg"],
        }

    # def __str__(self):
    #     return f'code : {self.code} mmd : {self.mmd} type : {self.type}'


class Operation:
    """
    策略返回的操作指示对象
    """

    def __init__(
        self,
        code: str,
        opt: str,
        mmd: str,
        loss_price: float = 0,
        info=None,
        msg: str = "",
        pos_rate: float = 1,
        key: str = "id",
        open_uid: str = None,
        close_uid: str = "clear",
    ):
        # TODO 历史原因，后期 opt 值修改为 open  close ，分别表示 开仓与平仓
        # TODO 但是为了兼容之前的  buy sell ，这里单独做个转换，内部还是使用 buy sell 进行判断开平仓
        opt_map = {
            "open": "buy",
            "close": "sell",
        }
        # 旧的 操作指示  buy  买入  sell  卖出 （buy 表示开仓 sell 表示平仓，新的用 open  close 进行表示了）
        # 新的 操作指示  open 开仓  close  平仓
        self.opt: str = opt if opt not in opt_map.keys() else opt_map[opt]

        # 触发指示的
        # 买卖点 例如：1buy 2buy l2buy 3buy l3buy  1sell 2sell l2sell 3sell l3sell down_pz_bc_buy
        # 背驰点 例如：down_bi_bc_buy down_pz_bc_buy down_qs_bc_buy up_bi_bc_sell up_pz_bc_sell up_qs_bc_sell
        self.mmd: str = mmd  # 触发买卖点
        self.loss_price: float = loss_price  # 止损价格
        self.info: Dict[str, object] = info  # 自定义保存的一些信息
        self.msg: str = msg
        self.pos_rate: float = pos_rate  # 开仓 or 平仓 所占的比例
        # 避免同一位置多次开平仓，需要在该位置设置一个独立的 key 值，例如当前笔结束的日期等
        self.key: str = key
        self.code: str = code  # 操作的标的代码
        # 开车的标记uid，同一个uid同时只能有一个持仓
        self.open_uid: str = f"{code}:{mmd}" if open_uid is None else open_uid
        # 平仓的标记uid，在信号模式下，只有 clear 才算彻底清仓，其他只是标记
        self.close_uid: str = close_uid

    def __str__(self):
        return f"mmd {self.mmd} opt {self.opt} loss_price {self.loss_price} msg: {self.msg}"


class MarketDatas(ABC):
    """
    市场数据类，用于在回测与实盘获取指定行情数据类
    """

    def __init__(self, market: str, frequencys: List[str], cl_config=None):
        """
        初始化
        """
        self.market = market
        self.frequencys = frequencys
        self.cl_config = cl_config

        # 按照 code_frequency 进行索引保存，存储周期对应的缠论数据
        self.cl_datas: Dict[str, ICL] = {}

        # 按照 code_frequency 进行索引保存，减少多次计算时间消耗；每次循环缓存的计算，在下次循环会重置为 {}
        self.cache_cl_datas: Dict[str, ICL] = {}

    @abstractmethod
    def klines(self, code, frequency) -> pd.DataFrame:
        """
        获取标的周期内的k线数据
        """

    @abstractmethod
    def last_k_info(self, code) -> dict:
        """
        获取最后一根K线数据，根据 frequencys 最后一个 小周期获取数据
        return dict {'date', 'open', 'close', 'high', 'low'}
        """

    @abstractmethod
    def get_cl_data(self, code, frequency, cl_config: dict = None) -> ICL:
        """
        获取标的周期的缠论数据
        @param code: 获取缠论数据的标的代码
        @param frequency: 获取的周期
        @param cl_config: 使用的缠论配置，如果是 None，则使用回测中默认的配置项
        @return : 缠论数据对象
        """

    def custom_data(self, code, frequency, args=None):
        """
        获取自定义数据
        """
        return None


class Trader(ABC):

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
        # 策略基本信息
        self.name = name
        self.mode = mode
        self.market = market
        self.max_pos = max_pos

    @abstractmethod
    def get_price(self, code) -> dict:
        """
        回测中方法，获取股票代码当前的价格，根据最小周期 k 线收盘价
        """

    @abstractmethod
    def hold_positions(self) -> List[POSITION]:
        """
        返回所有持仓记录
        """


class Strategy(ABC):
    """
    交易策略基类
    """

    def __init__(self):
        # 实盘中起效果，允许执行的 close_uid 列表
        # 有两种格式
        #       列表格式：['a', 'b', 'c']，表示只在允许的 close_uid 中才允许操作
        #       字典格式：{'buy': ['a', 'b'], 'sell' : ['c', 'd']}，表示 buy 只在做多的仓位中允许，sell 只在做空的仓位中允许
        self.allow_close_uids = None
        self.use_times = {}
        pass

    def add_times(self, key: str, use_time: float):
        if key not in self.use_times.keys():
            self.use_times[key] = {"num": 1, "times": use_time}
        else:
            self.use_times[key]["num"] += 1
            self.use_times[key]["times"] += use_time
        return True

    def write_log(self, file_name: str, msg: str):
        log = get_logger(file_name)
        log.info(msg)
        return True

    @abstractmethod
    def open(
        self, code, market_data: MarketDatas, poss: List[POSITION]
    ) -> List[Operation]:
        """
        观察行情数据，给出开仓操作建议
        :param code:
        :param market_data:
        :param poss: 当前代码的持仓列表
        :return:
        """

    @abstractmethod
    def close(
        self, code, mmd: str, pos: POSITION, market_data: MarketDatas
    ) -> Union[Operation, None, List[Operation]]:
        """
        盯当前持仓，给出平仓当下建议
        :param code:
        :param mmd:
        :param pos:
        :param market_data:
        :return:
        """

    def on_bt_loop_start(self, bt):
        """
        回测专用，每次每个代码回测循环都会执行这个方法

        @param bt: 回测 BackTest 对象
        """
        pass

    def is_filter_opts(self):
        """
        是否对产生的开盘信号进行二次过滤操作，比如在统一的时间执行开盘信号检测，在对产生的所有信号进行二次过滤，最终只执行其中过滤后的操作
        需要再实际的策略中进行方法重写
        """
        return False

    def filter_opts(
        self,
        opts: List[Operation],
        trader: Trader = None,
    ):
        """
        过滤开盘信号，返回过滤后的操作列表
        需要再实际的策略中进行方法重写
        """
        return opts

    def clear(self):
        """
        回测专用，回测结束后，清理一些不需要的变量，避免被 pickle 保存
        """
        pass

    @staticmethod
    def idx_ma(cd: ICL, period=5, is_all_prices=False):
        """
        返回 MA 指标
        """
        if is_all_prices:
            prices = np.array([k.c for k in cd.get_klines()])
        else:
            prices = np.array([k.c for k in cd.get_klines()[-(period + 120) :]])
        ma = talib.MA(prices, timeperiod=period)
        return ma

    @staticmethod
    def idx_ema(cd: ICL, period=5, is_all_prices=False):
        """
        返回 EMA 指标
        """
        if is_all_prices:
            prices = np.array([k.c for k in cd.get_klines()])
        else:
            prices = np.array([k.c for k in cd.get_klines()[-(period + 120) :]])
        ma = talib.EMA(prices, timeperiod=period)
        return ma

    @staticmethod
    def idx_boll(cd: ICL, period=20):
        """
        返回 boll 指标
        """
        prices = np.array([k.c for k in cd.get_klines()[-(period + 120) :]])
        boll_up, boll_mid, boll_low = talib.BBANDS(prices, timeperiod=period)
        return {"up": boll_up, "mid": boll_mid, "low": boll_low}

    @staticmethod
    def idx_rsi(cd: ICL, period=14):
        # 指标说明：
        # RSI的基本原理是在一个正常的股市中，多空买卖双方的力道必须得到均衡，股价才能稳定；而RSI是对于固定期间内，股价上涨总幅度平均值占总幅度平均值的比例。
        # 1. RSI值于0 - 100 之间呈常态分配，当6日RSI值为80‰以上时，股市呈超买现象，若出现M头，市场风险较大；当6日RSI值在20‰以下时，股市呈超卖现象，若出现W头，市场机会增大。
        # 2. RSI一般选用6日、12日、24日作为参考基期，基期越长越有趋势性(慢速RSI)，基期越短越有敏感性(快速RSI)。当快速RSI由下往上突破慢速RSI时，机会增大；当快速RSI由上而下跌破慢速RSI时，风险增大。
        prices = np.array([k.c for k in cd.get_klines()[-(period + 120) :]])
        rsi = talib.RSI(prices, timeperiod=period)
        return rsi

    @staticmethod
    def idx_atr(cd: ICL, period=14, end_datetime=None):
        # 原理：
        # （1）
        #     A=最高价-最低价
        #     B=（前一收盘价-最高价）的绝对值
        #     C=A与B两者较大者
        #     D=（前一收盘价-最低价）的绝对值
        #     TR=C与D两者较大者
        # （2）
        #     ATR=TR在N个周期的简单移动平均
        # 用法：
        #     在上升通道中，ATR真实波幅向上时，且TR黄线上穿ATR蓝线，此时K线收阴者可买入。下降通道中不买。

        close_prices = np.array(
            [
                k.c
                for k in cd.get_klines()[-(period + 500) :]
                if end_datetime is None or k.date <= end_datetime
            ]
        )
        high_prices = np.array(
            [
                k.h
                for k in cd.get_klines()[-(period + 500) :]
                if end_datetime is None or k.date <= end_datetime
            ]
        )
        low_prices = np.array(
            [
                k.l
                for k in cd.get_klines()[-(period + 500) :]
                if end_datetime is None or k.date <= end_datetime
            ]
        )
        atr = talib.ATR(high_prices, low_prices, close_prices, timeperiod=period)
        return atr

    @staticmethod
    def idx_cci(cd: ICL, period=14):
        # 指标说明：
        # 按市场的通行的标准，CCI指标的运行区间可分为三大类：大于﹢100、小于 - 100 和﹢100——-100 之间。
        # 1. 当CCI＞﹢100 时，表明股价已经进入非常态区间——超买区间，股价的异动现象应多加关注。
        # 2. 当CCI＜-100 时，表明股价已经进入另一个非常态区间——超卖区间，投资者可以逢低吸纳股票。
        # 3. 当CCI介于﹢100——-100 之间时表明股价处于窄幅振荡整理的区间——常态区间，投资者应以观望为主。
        close_prices = np.array([k.c for k in cd.get_klines()[-(period + 120) :]])
        high_prices = np.array([k.h for k in cd.get_klines()[-(period + 120) :]])
        low_prices = np.array([k.l for k in cd.get_klines()[-(period + 120) :]])
        cci = talib.CCI(high_prices, low_prices, close_prices, timeperiod=period)
        return cci

    @staticmethod
    def idx_kdj(cd: ICL, period=9, M1=3, M2=3, end_datetime=None):
        # 指标说明：
        # KDJ，其综合动量观念、强弱指标及移动平均线的优点，早年应用在期货投资方面，功能颇为显著，目前为股市中最常被使用的指标之一。买卖原则：
        # 1. K线由右边向下交叉D值做卖，K线由右边向上交叉D值做买。
        # 2. 高档连续二次向下交叉确认跌势，低挡连续二次向上交叉确认涨势。
        # 3. D值 < 20 % 超卖，D值 > 80 % 超买，J > 100 % 超买，J < 10 % 超卖。
        # 4. KD值于50 % 左右徘徊或交叉时，无意义。
        # 5. 投机性太强的个股不适用。
        # 6. 可观察KD值同股价的背离，以确认高低点。
        close_prices = np.array(
            [
                k.c
                for k in cd.get_klines()[-(period + 500) :]
                if end_datetime is None or k.date <= end_datetime
            ]
        )
        high_prices = np.array(
            [
                k.h
                for k in cd.get_klines()[-(period + 500) :]
                if end_datetime is None or k.date <= end_datetime
            ]
        )
        low_prices = np.array(
            [
                k.l
                for k in cd.get_klines()[-(period + 500) :]
                if end_datetime is None or k.date <= end_datetime
            ]
        )
        k, d, j = MyTT.KDJ(
            close_prices, high_prices, low_prices, N=period, M1=M1, M2=M2
        )
        return {"k": k, "d": d, "j": j}

    @staticmethod
    def idx_macd(cd: ICL, fast=12, slow=26, signal=9, end_datetime=None):
        # 指标说明：
        # MACD
        close_prices = np.array(
            [
                k.c
                for k in cd.get_klines()[-(slow + 500) :]
                if end_datetime is None or k.date <= end_datetime
            ]
        )
        macd_dif, macd_dea, macd_hist = talib.MACD(
            close_prices, fastperiod=fast, slowperiod=slow, signalperiod=signal
        )
        macd_hist *= 2

        return {"dif": macd_dif, "dea": macd_dea, "hist": macd_hist}

    @staticmethod
    def idx_mtm(cd: ICL, N=12, M=6):
        # 参数：N 间隔天数，也是求移动平均的天数，一般为6
        # MTM向上突破零，买入信号
        # MTM向下突破零，卖出信号
        close_prices = np.array([k.c for k in cd.get_klines()[-(N + 120) :]])
        mtm, mtma = MyTT.MTM(close_prices, N, M)
        return {"mtm": mtm, "mtma": mtma}

    @staticmethod
    def idx_psy(cd: ICL, N=12, M=6):
        # 原理：
        #     心理线是一种建立在研究投资人心理趋向基础上，将某段时间内投资者倾向买方还是卖方的心理与事实转化为数值，形成人气指标，做为买卖的参考。
        #     PSY＝N日内的上涨天数/N×100
        #     N一般设定为12日，最大不超过24，周线的最长不超过26。
        #
        # 用法：
        #     1.PSY>75为超买，如形成M头时为卖出信号；
        #     2.PSY<25为超卖，如形成W底时为卖出信号；
        #     3.心理线主要反映市场心理的超买或超卖，因此，当百分比值在常态区域上下移动时，一般应持观望态度；
        #     4.PSY一般不可单独使用，需配合VR指标和逆时针曲线同时使用，可提高准确度。
        close_prices = np.array([k.c for k in cd.get_klines()[-(N + 120) :]])
        psy, psya = MyTT.PSY(close_prices, N, M)
        return {"psy": psy, "psya": psya}

    @staticmethod
    def idx_dmi(cd: ICL, M1=14, M2=6):
        """
        指示投资人避免在盘整的市场中交易，一旦市场变得有利润时，DMI立刻引导投资人进场，并且在适当时机退场。
        买卖原则：
            1、+DI上交叉-DI时，可参考做买。
            2、+DI下交叉-DI时，可参考做卖。
            3、ADX于50以上向下转折时，对表市场趋势终了。
            4、当ADX滑落至+DI之下时，不宜进场交易。
            5、当ADXR介于20-25时，宜采用TBP及CDP中之反应秘诀为交易参考。
        """
        close_prices = np.array([k.c for k in cd.get_klines()[-(500):]])
        high_prices = np.array([k.h for k in cd.get_klines()[-(500):]])
        low_prices = np.array([k.l for k in cd.get_klines()[-(500):]])
        pdi, mdi, adx, adxr = MyTT.DMI(close_prices, high_prices, low_prices, M1, M2)
        return {
            "pdi": pdi,
            "mdi": mdi,
            "adx": adx,
            "adxr": adxr,
        }

    @staticmethod
    def idx_ama(cd: ICL, N=10, fast_N=2, slow_N=30) -> np.array:
        CLOSE = np.array([k.c for k in cd.get_src_klines()])

        DIR = MyTT.ABS(CLOSE - MyTT.REF(CLOSE, N))

        VIR = MyTT.SUM(MyTT.ABS(CLOSE - MyTT.REF(CLOSE, 1)), N)

        ER = DIR / VIR

        CS = ER * (2 / (fast_N + 1) - 2 / (slow_N + 1)) + 2 / (slow_N + 1)

        CQ = CS * CS
        AMA = np.zeros(len(CLOSE))
        for _i in range(len(CQ)):
            _cq = CQ[_i]
            if _cq != _cq:
                AMA[_i] = CLOSE[_i]
            else:
                AMA[_i] = AMA[_i - 1] + _cq * (CLOSE[_i] - AMA[_i - 1])

        return AMA

    @staticmethod
    def idx_atr_by_sma(CLOSE, HIGH, LOW, N: int = 20):
        TR = MyTT.MAX(
            MyTT.MAX((HIGH - LOW), MyTT.ABS(MyTT.REF(CLOSE, 1) - HIGH)),
            MyTT.ABS(MyTT.REF(CLOSE, 1) - LOW),
        )
        return MyTT.SMA(TR, N)

    @staticmethod
    def idx_sar(cd: ICL, acceleration=0.02, maximum=0.2):
        # 指标说明：
        # 1、当股票价格从SAR曲线下方开始向上突破SAR曲线时，为买入信号，预示着股票价格一轮上升行情可能展开，投资者应迅速及时地买进该股票。
        # 2、当股票价格向上突破SAR曲线后继续向上，而SAR曲线也同时向上运动时，表明上涨趋势已形成。SAR曲线对股票价格构成强劲的支撑，投资者应坚决看多或逢低买入该股票。
        # 3、当股票价格从SAR曲线上方开始向下突破SAR曲线时，为卖出信号，预示着股票价格一轮下跌行情可能展开，投资者应及时地卖出该股票。
        # 4、当股票价格向下突破SAR曲线后继续向下，而SAR曲线也同时向下运动的话，表明下跌趋势已形成，SAR曲线对价格会构成巨大的压力，投资者应坚决看空或逢高做空该股票。
        high_prices = np.array([k.h for k in cd.get_klines()])
        low_prices = np.array([k.l for k in cd.get_klines()])
        sar = talib.SAR(
            high_prices, low_prices, acceleration=acceleration, maximum=maximum
        )
        return sar

    @staticmethod
    def get_max_loss_price(
        mmd_type: str, now_price: float, stop_loss_price: float, max_loss_rate: float
    ):
        """
        获取最大可接受的止损价格
        @param mmd_type: 买卖点类型，值 buy or sell
        @param now_price: 当前价格
        @param stop_loss_price: 原始止损价格
        @param max_loss_rate: 最大可接受的止损百分比，例如 10，则可接受最大10%的损失
        """
        if mmd_type == "buy":
            return max(stop_loss_price, now_price * (1 - max_loss_rate / 100))
        elif mmd_type == "sell":
            return min(stop_loss_price, now_price * (1 + max_loss_rate / 100))

    def get_atr_stop_loss_price(
        self, cd: ICL, mmd_type: str, atr_period: int = 14, atr_m: float = 1.5
    ):
        """
        获取ATR波动率的止损价格
        """
        close_prices = np.array([k.c for k in cd.get_klines()[-(atr_period + 200) :]])
        high_prices = np.array([k.h for k in cd.get_klines()[-(atr_period + 200) :]])
        low_prices = np.array([k.l for k in cd.get_klines()[-(atr_period + 200) :]])
        atr_vals = self.idx_atr_by_sma(
            close_prices, high_prices, low_prices, atr_period
        )
        high_stop_loss_price = high_prices[-1] + atr_vals[-1] * atr_m
        low_stop_loss_price = low_prices[-1] - atr_vals[-1] * atr_m
        if mmd_type == "buy":
            return low_stop_loss_price
        else:
            return high_stop_loss_price

    def check_atr_stop_loss(
        self, cd: ICL, pos: POSITION, atr_period: int = 14, atr_m: float = 1.5
    ):
        """
        检查是否触发 ATR 移动止损
        收盘价 大于 or 小于 前一个 atr 止损价格
        """
        close_prices = np.array([k.c for k in cd.get_klines()[-(atr_period + 200) :]])
        high_prices = np.array([k.h for k in cd.get_klines()[-(atr_period + 200) :]])
        low_prices = np.array([k.l for k in cd.get_klines()[-(atr_period + 200) :]])
        atr_vals = self.idx_atr_by_sma(
            close_prices, high_prices, low_prices, atr_period
        )
        price = cd.get_src_klines()[-1].c
        high_stop_loss_price = high_prices[-2] + atr_vals[-2] * atr_m
        low_stop_loss_price = low_prices[-2] - atr_vals[-2] * atr_m
        if "buy" in pos.mmd and price <= low_stop_loss_price:
            return Operation(
                code=cd.get_code(),
                opt="close",
                mmd=pos.mmd,
                msg="%s ATR止损 （止损价格 %s 当前价格 %s）"
                % (pos.mmd, low_stop_loss_price, price),
            )
        elif "sell" in pos.mmd and price >= high_stop_loss_price:
            return Operation(
                code=cd.get_code(),
                opt="close",
                mmd=pos.mmd,
                msg="%s ATR止损 （止损价格 %s 当前价格 %s）"
                % (pos.mmd, high_stop_loss_price, price),
            )
        return None

    @staticmethod
    def check_loss(mmd: str, pos: POSITION, price: float):
        """
        检查是否触发止损，止损返回操作对象，不出发返回 None
        """
        # 止盈止损检查
        if pos.loss_price is None or pos.loss_price == 0:
            return None

        if "buy" in mmd:
            if price < pos.loss_price:
                return Operation(
                    code=pos.code,
                    opt="close",
                    mmd=mmd,
                    msg="%s 止损 （止损价格 %s 当前价格 %s）"
                    % (mmd, pos.loss_price, price),
                    close_uid="clear",
                )
        elif "sell" in mmd:
            if price > pos.loss_price:
                return Operation(
                    code=pos.code,
                    opt="close",
                    mmd=mmd,
                    msg="%s 止损 （止损价格 %s 当前价格 %s）"
                    % (mmd, pos.loss_price, price),
                    close_uid="clear",
                )
        return None

    @staticmethod
    def break_even(pos: POSITION, loss_multiple: int = 2):
        """
        保本方法，当最大盈利超过止损 N 倍的时候，将止损设置在成本价上
        """
        # 如果之前已经设置过，退出
        if pos.loss_price == pos.price:
            return None
        # 止损比例
        loss_rate = abs((pos.price - pos.loss_price) / pos.price * 100)
        if pos.max_profit_rate >= (loss_rate * loss_multiple):
            pos.loss_price = pos.price

        return True

    @staticmethod
    def check_back_return(mmd: str, pos: POSITION, price: float, max_back_rate: float):
        """
        检查是否触发最大回撤
        """
        if max_back_rate is not None:
            profit_rate = (
                (pos.price - price) / pos.price * 100
                if "sell" in mmd
                else (price - pos.price) / pos.price * 100
            )
            if profit_rate > 0 and pos.max_profit_rate - profit_rate >= max_back_rate:
                return Operation(
                    code=pos.code, opt="close", mmd=mmd, msg="%s 回调止损" % mmd
                )
        return None

    @staticmethod
    def get_max_loss_rate(win_rate: float, wlr: float):
        """
        获取最大亏损比例，通过凯利公式计算，并将结果除以2，取一半
        @param win_rate: 胜率 比如 0.5 为 50%的胜率
        @param wlr:亏损比
        @return : 返回最优的固定亏损比例
        """
        return round((win_rate - (1 - win_rate) / wlr) * 100 / 2, 2)

    @staticmethod
    def get_open_pos_rate(max_loss_rate: float, open_price: float, loss_price: float):
        """
        获取开仓的比例，根据可运行的最大亏损本金，与开仓平仓价格进行计算

        @param max_loss_rate: 最大的固定亏损百分比
        @param open_price: 开仓价格
        @param loss_price: 止损价格
        """
        balance = 100000  # 信号模式下，回测每次开仓的金额
        open_balance = (
            (max_loss_rate / 100 * balance) / abs(open_price - loss_price) * open_price
        )
        pos_rate = round(open_balance / balance, 2)
        if pos_rate > 1:
            pos_rate = 1
        return pos_rate

    @staticmethod
    def last_done_bi(bis: List[BI]):
        """
        获取最后一个 完成笔
        """
        for bi in bis[::-1]:
            if bi.is_done():
                return bi
        return None

    @staticmethod
    def last_bi(cd: ICL, _type: str = "up") -> BI:
        """
        获取最后一个给定类型的笔
        """
        if cd.get_bis()[-1].type == _type:
            return cd.get_bis()[-1]
        else:
            return cd.get_bis()[-2]

    @staticmethod
    def last_xd(cd: ICL, _type: str = "up"):
        """
        获取最后一个给定类型的线段
        """
        if cd.get_xds()[-1].type == _type:
            return cd.get_xds()[-1]
        else:
            return cd.get_xds()[-2]

    @staticmethod
    def last_done_xd(xds: List[XD]):
        """
        获取最后一个 完成线段
        """
        for xd in xds[::-1]:
            if xd.is_done():
                return xd
        return None

    @staticmethod
    def bi_td(bi: BI, cd: ICL):
        """
        判断是否笔停顿
        """
        if bi.is_done() is False:
            return False
        next_ks = cd.get_klines()[bi.end.klines[-1].k_index + 1 :]
        if len(next_ks) == 0:
            return False
        for _nk in next_ks:
            if bi.type == "up" and _nk.c < _nk.o and _nk.c < bi.end.klines[-1].l:
                return True
            elif bi.type == "down" and _nk.c > _nk.o and _nk.c > bi.end.klines[-1].h:
                return True

        return False

    @staticmethod
    def bi_mean_zz(bi: BI, cd: ICL):
        """
        平均K线，笔转折判断
        """
        # 笔要完成
        if bi.is_done() is False:
            return False
        mean_klines = cd.get_klines()
        # 如果笔向上，k线要变绿，进行转折
        if (
            bi.type == "up"
            and mean_klines[-2].o > mean_klines[-2].c
            and mean_klines[-1].h < bi.end.val
            and mean_klines[-1].c < mean_klines[-1].o
            and mean_klines[-1].c < mean_klines[-2].o
        ):
            return True
        # 如果笔向下，k线要变红，进行转折
        if (
            bi.type == "down"
            and mean_klines[-2].o < mean_klines[-2].c
            and mean_klines[-1].l > bi.end.val
            and mean_klines[-1].c > mean_klines[-1].o
            and mean_klines[-1].c > mean_klines[-2].o
        ):
            return True
        return False

    @staticmethod
    def bi_qiang_td(bi: BI, cd: ICL):
        """
        笔的强停顿判断
        判断方法：收盘价要突破分型的高低点，并且K线要是阳或阴
        距离分型太远就直接返回 False
        """
        if bi.end.done is False:
            return False
        last_k = cd.get_klines()[-1]
        # 当前bar与分型第三个bar相隔大于2个bar，直接返回 False
        if last_k.index - bi.end.klines[-1].klines[-1].index > 2:
            return False
        if bi.end.klines[-1].index == last_k.index:
            return False
        if (
            bi.end.type == "ding"
            and last_k.o > last_k.c
            and last_k.c
            < bi.end.low(cd.get_config()["fx_qj"], cd.get_config()["fx_qy"])
        ):
            return True
        elif (
            bi.end.type == "di"
            and last_k.o < last_k.c
            and last_k.c
            > bi.end.high(cd.get_config()["fx_qj"], cd.get_config()["fx_qy"])
        ):
            return True
        return False

    @staticmethod
    def bi_yanzhen_fx(bi: BI, cd: ICL):
        """
        检查是否符合笔验证分型条件
        查找与笔结束分型一致的后续分型，并且该分型不能高于或低于笔结束分型
        """
        last_k = cd.get_klines()[-1]
        price = last_k.c
        fxs = cd.get_fxs()
        next_fxs = [
            _fx for _fx in fxs if (_fx.index > bi.end.index and _fx.type == bi.end.type)
        ]
        if len(next_fxs) == 0:
            return False
        next_fx = next_fxs[0]
        # # 当前bar与验证分型第三个bar相隔大于2个bar，直接返回 False
        # if last_k.index - next_fx.klines[-1].klines[-1].index > 2:
        #     return False

        # 两个分型不能相隔太远，两个分型中间最多两根缠论K线
        if next_fx.k.k_index - bi.end.k.k_index > 3:
            return False
        if bi.type == "up":
            # 笔向上，验证下一个顶分型不高于笔的结束顶分型，并且当前价格要低于顶分型的最低价格
            if (
                next_fx.done
                and next_fx.val < bi.end.val
                and price
                < bi.end.low(cd.get_config()["fx_qj"], cd.get_config()["fx_qy"])
            ):
                return True
        elif bi.type == "down":
            # 笔向下，验证下一个底分型不低于笔的结束底分型，并且两个分型不能离得太远
            if (
                next_fx.done
                and next_fx.val > bi.end.val
                and price
                > bi.end.high(cd.get_config()["fx_qj"], cd.get_config()["fx_qy"])
            ):
                return True
        return False

    def dynamic_change_loss_by_bi(self, pos: POSITION, bis: List[BI]):
        """
        动态按照笔进行止损价格的移动
        """
        if pos.loss_price is None:
            return
        last_done_bi = self.last_done_bi(bis)
        if "buy" in pos.mmd and last_done_bi.type == "up":
            pos.loss_price = max(pos.loss_price, last_done_bi.low)
        elif "sell" in pos.mmd and last_done_bi.type == "down":
            pos.loss_price = min(pos.loss_price, last_done_bi.high)

        return

    @staticmethod
    def points_jiaodu(points: List[float], position="up"):
        """
        提供一系列数据点，给出其趋势角度，以此判断其方向
        用于判断类似 macd 背驰，macd柱子创新低而黄白线则新高
        """
        if len(points) == 0:
            return 0
        # 去一下棱角
        points = talib.MA(np.array(points), 2)
        # 先给原始数据编序号
        new_points = []
        for i in range(len(points)):
            if points[i] is not None:
                new_points.append([i, points[i]])

        # 根据位置参数，决定找分型类型
        fxs = []
        for i in range(1, len(new_points)):
            p1 = new_points[i - 1]
            p2 = new_points[i]
            p3 = new_points[i + 1] if len(new_points) > (i + 1) else None
            if (
                position == "up"
                and p1[1] <= p2[1]
                and ((p3 is not None and p2[1] >= p3[1]) or p3 is None)
            ):
                fxs.append(p2)
            elif (
                position == "down"
                and p1[1] >= p2[1]
                and ((p3 is not None and p2[1] <= p3[1]) or p3 is None)
            ):
                fxs.append(p2)

        if len(fxs) < 2:
            return 0
        # 按照大小排序
        fxs = sorted(
            fxs, key=lambda f: f[1], reverse=True if position == "up" else False
        )

        def jiaodu(_p1: list, _p2: list):
            # 计算斜率
            k = (_p1[1] - _p2[1]) / (_p1[0] - _p2[0])
            # 斜率转弧度
            k = math.atan(k)
            # 弧度转角度
            j = math.degrees(k)
            return j

        return jiaodu(fxs[0], fxs[1])

    @staticmethod
    def check_datetime_mmd(
        start_datetime: datetime.datetime, cd: ICL, check_line: str = "bi"
    ):
        """
        检查指定时间后出现的买卖点信息
        """
        mmd_infos = {
            "1buy": 0,
            "2buy": 0,
            "3buy": 0,
            "l3buy": 0,
            "1sell": 0,
            "2sell": 0,
            "3sell": 0,
            "l3sell": 0,
        }
        lines = cd.get_bis() if check_line == "bi" else cd.get_xds()
        for _l in lines[::-1]:
            if _l.start.k.date >= start_datetime:
                line_mmds = _l.line_mmds()
                for _m in line_mmds:
                    mmd_infos[_m] += 1
            else:
                break
        return mmd_infos

    @staticmethod
    def check_low_info_by_datetime(
        low_data: ICL,
        start_datetime: datetime.datetime,
        end_datetime: datetime.datetime,
    ):
        """
        检查低级别缠论数据中，时间范围内出现的信号信息
        """
        infos = {
            "qiang_ding_fx": 0,
            "qiang_di_fx": 0,
            "up_bi_bc": 0,
            "up_xd_bc": 0,
            "up_pz_bc": 0,
            "up_qs_bc": 0,
            "down_bi_bc": 0,
            "down_xd_bc": 0,
            "down_pz_bc": 0,
            "down_qs_bc": 0,
            "1buy": 0,
            "2buy": 0,
            "l2buy": 0,
            "3buy": 0,
            "l3buy": 0,
            "1sell": 0,
            "2sell": 0,
            "l2sell": 0,
            "3sell": 0,
            "l3sell": 0,
        }
        for bi in low_data.get_bis()[::-1]:
            if bi.end.k.date < start_datetime:
                break
            if start_datetime <= bi.end.k.date <= end_datetime:
                # 买卖点统计
                for mmd in bi.line_mmds():
                    infos[mmd] += 1
                # 背驰统计
                for bc in bi.line_bcs():
                    infos[f"{bi.type}_{bc}_bc"] += 1
        for xd in low_data.get_xds()[::-1]:
            if xd.end_line.end.k.date < start_datetime:
                break
            if start_datetime <= xd.end_line.end.k.date <= end_datetime:
                # 买卖点统计
                for mmd in xd.line_mmds():
                    infos[mmd] += 1
                # 背驰统计
                for bc in xd.line_bcs():
                    infos[f"{xd.type}_{bc}_bc"] += 1

        # 笔区间内的强分型统计
        fxs = [
            fx
            for fx in low_data.get_fxs()
            if start_datetime <= fx.k.date <= end_datetime
        ]
        for fx in fxs:
            if fx.ld() >= 5:
                infos[f"qiang_{fx.type}_fx"] += 1
        return infos

    @staticmethod
    def judge_macd_back_zero(cd: ICL, zs: ZS) -> int:
        """
        判断中枢的 macd 是否有回拉零轴

        @param cd:缠论数据对象
        @param zs: 中枢
        @return: 返回回拉零轴的最大次数
        """
        zs_macd_info = cal_zs_macd_infos(zs, cd)
        # 根据进入中枢第一笔，判断中枢方向，向下笔是向上中枢，看回调零轴次数；向上笔是向下中枢，看回拉零轴次数
        if zs.lines[0].type == "down":
            return max(zs_macd_info.dea_down_cross_num, zs_macd_info.dif_down_cross_num)
        if zs.lines[0].type == "up":
            return max(zs_macd_info.dea_up_cross_num, zs_macd_info.dif_up_cross_num)
        return 0


def fee_a(opt: str, price: float, amount: float):
    """
    A 股交易所费用计算方法
    """
    fee_rate = 0.3  # 单位 %
    min_fee = 5
    yhs_rate = 0.1  # 印花税 单位 % 出让方（卖出）收取
    ghf_rate = 0.02  # 过户费 单位 % 双向收取

    trade_volume = price * amount
    fee_sum = max([min_fee, trade_volume * fee_rate / 100])
    if opt == "sell":
        fee_sum += trade_volume * yhs_rate / 100
    fee_sum += trade_volume * ghf_rate / 100
    return fee_sum


def fee_us(opt: str, price: float, amlunt: float):
    """
    美股的交易费用计算
    """
    pass


if __name__ == "__main__":
    from chanlun import cl
    from chanlun.exchange.exchange_tdx import ExchangeTDX

    ex = ExchangeTDX()
    klines = ex.klines("SH.000001", "d")
    print(klines.tail(10))
    cd = cl.CL("SH.000001", "d", {}).process_klines(klines)
    ama = Strategy.idx_atr(cd)
    print(ama)
