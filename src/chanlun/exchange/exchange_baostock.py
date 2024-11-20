from typing import Union
import baostock as bs
from chanlun import fun

from chanlun.exchange.exchange import *


@fun.singleton
class ExchangeBaostock(Exchange):
    """
    Baostock 行情接口服务，非实时
    使用 baostock API 实现 : http://baostock.com/baostock/index.php/%E9%A6%96%E9%A1%B5
    """

    g_all_stocks = []

    def __init__(self):
        bs.login()

        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")

    def default_code(self):
        return "SH.000001"

    def support_frequencys(self):
        return {
            "m": "Month",
            "w": "Week",
            "d": "Day",
            "60m": "1H",
            "30m": "30m",
            "15m": "15m",
            "5m": "5m",
        }

    def all_stocks(self):
        """
        获取支持的所有股票列表
        :return:
        """
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        # TODO 节假日兼容
        day = "2022-04-18"

        rs = bs.query_all_stock(day=day)
        __all_stocks = []
        while (rs.error_code == "0") & rs.next():
            # 获取一条记录，将记录合并在一起
            row = rs.get_row_data()
            if row[0][:6] in ["sz.399", "sh.000"]:
                continue
            __all_stocks.append({"code": row[0], "name": row[2]})
        self.g_all_stocks = __all_stocks
        return self.g_all_stocks

    def now_trading(self):
        """
        返回当前是否是交易时间
        周一至周五，09:30-11:30 13:00-15:00
        """
        now_dt = datetime.datetime.now()
        if now_dt.weekday() in [5, 6]:  # 周六日不交易
            return False
        hour = now_dt.hour
        minute = now_dt.minute
        if hour == 9 and minute >= 30:
            return True
        if hour in [10, 13, 14]:
            return True
        if hour == 11 and minute < 30:
            return True
        return False

    def klines(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        end_date: str = None,
        args=None,
    ) -> [pd.DataFrame, None]:
        """
        获取 Kline 线
        :param code:
        :param frequency:
        :param start_date:
        :param end_date:
        :param args:
        :return:
        """
        if args is None:
            args = {}
        if "fq" not in args.keys():
            args["fq"] = "qfq"

        fq_map = {"qfq": "2", "hfq": "1"}
        frequency_map = {
            "m": "m",
            "w": "w",
            "d": "d",
            "60m": "60",
            "30m": "30",
            "15m": "15",
            "5m": "5",
        }
        default_start_day_map = {
            "m": 5000,
            "w": 5000,
            "d": 1000,
            "60m": 200,
            "30m": 100,
            "15m": 60,
            "5m": 20,
        }
        if frequency not in frequency_map:
            raise Exception("不支持的周期 : " + frequency)

        #### 获取沪深A股历史K线数据 ####
        # 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。
        # 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
        # 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
        if start_date is None:
            start_date = datetime.datetime.now() - datetime.timedelta(
                days=default_start_day_map[frequency]
            )
            start_date = start_date.strftime("%Y-%m-%d")

        rs = bs.query_history_k_data_plus(
            code,
            "code,date,open,low,high,close,volume",
            start_date=start_date,
            end_date=end_date,
            frequency=frequency_map[frequency],
            adjustflag=fq_map[args["fq"]],
        )
        if rs.error_code in ["10001001", "10002007"]:
            bs.login()
            return self.klines(code, frequency, start_date, end_date, args)
        if rs.error_code != "0":
            print("query_history_k_data_plus respond error_code:" + rs.error_code)
            print("query_history_k_data_plus respond  error_msg:" + rs.error_msg)
            return None

        data_list = []
        while (rs.error_code == "0") & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        kline = pd.DataFrame(data_list, columns=rs.fields)
        kline["date"] = pd.to_datetime(kline["date"])
        kline["date"] = kline["date"].apply(self.__convert_date)
        kline["open"] = pd.to_numeric(kline["open"])
        kline["close"] = pd.to_numeric(kline["close"])
        kline["high"] = pd.to_numeric(kline["high"])
        kline["low"] = pd.to_numeric(kline["low"])
        kline["volume"] = pd.to_numeric(kline["volume"])
        kline.fillna(0, inplace=True)

        if frequency in ["60m", "30m", "15m", "5m", "1m"]:
            dates = kline["date"].unique()
            new_kline = pd.DataFrame()
            for d in dates:
                dk = kline[kline["date"] == d]
                self.__run_date = None

                def append_time(_d: datetime.datetime) -> datetime.datetime:
                    if self.__run_date is None:
                        self.__run_date = datetime.datetime.strptime(
                            _d.strftime("%Y-%m-%d") + " 09:30:00", "%Y-%m-%d %H:%M:%S"
                        )
                        self.__run_date = self.__run_date + datetime.timedelta(
                            minutes=int(frequency_map[frequency])
                        )
                    else:
                        self.__run_date = self.__run_date + datetime.timedelta(
                            minutes=int(frequency_map[frequency])
                        )
                        if (
                            self.__run_date.hour == 11 and self.__run_date.minute > 30
                        ) or (self.__run_date.hour == 12):
                            self.__run_date = datetime.datetime.strptime(
                                _d.strftime("%Y-%m-%d") + " 13:00:00",
                                "%Y-%m-%d %H:%M:%S",
                            )
                            self.__run_date = self.__run_date + datetime.timedelta(
                                minutes=int(frequency_map[frequency])
                            )
                    return self.__run_date

                dk.loc[:, "date"] = dk["date"].apply(append_time)
                new_kline = pd.concat([new_kline, dk], ignore_index=True)
            kline = new_kline.sort_values("date")

        kline.loc[:, "date"] = kline["date"].dt.tz_localize(self.tz)
        return kline[["code", "date", "open", "close", "high", "low", "volume"]]

    @staticmethod
    def __convert_date(dt: datetime.datetime):
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            return dt.replace(hour=15, minute=0)
        return dt

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """
        获取股票列表的 Tick 信息
        :param codes:
        :return:
        """
        raise Exception("交易所不支持 tick 获取")

    def stock_info(self, code: str) -> Union[Dict, None]:
        """
        获取股票的基本信息
        :param code:
        :return:
        """
        rs = bs.query_stock_basic(code=code)
        data_list = []
        while (rs.error_code == "0") & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        if data_list:
            return {"code": data_list[0][0], "name": data_list[0][1]}
        return None

    def stock_owner_plate(self, code: str):
        """
        股票所属板块信息
        :param code:
        :return:
        """
        raise Exception("当前交易所接口不支持")

    def plate_stocks(self, code: str):
        """
        获取板块股票列表信息
        :param code: 板块代码
        :return:
        """
        raise Exception("当前交易所接口不支持")

    def balance(self):
        """
        账户资产信息
        :return:
        """
        raise Exception("账户资产接口不支持")

    def positions(self, code: str = ""):
        """
        当前账户持仓信息
        :param code:
        :return:
        """
        raise Exception("账户资产接口不支持")

    def order(self, code: str, o_type: str, amount: float, args=None):
        """
        下单接口
        :param args:
        :param code:
        :param o_type:
        :param amount:
        :return:
        """
        raise Exception("账户资产接口不支持")


if __name__ == "__main__":
    ex = ExchangeBaostock()
    klines = ex.klines("SZ.000001", "d")
    print(klines.tail())
