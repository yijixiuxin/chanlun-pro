import pandas as pd

from chanlun import cl
from chanlun.cl_interface import ICL


class KlinesGenerator:
    """
    K线合成，目前只支持分钟线合成，小时、日，需要考虑不同市场的交易时间，比较麻烦，不搞呢
    """

    def __init__(self, minute: int, cl_config: dict = None, dt_align_type: str = "eob"):
        """
        如果源的分钟数是 1分钟，可以合成 3、5、7、30，如果是 5分钟，可以合成 10、15、30，
        如果源是 5分钟，非要合成 13分钟数据，合出来的数据则是错误的

        bob 前对齐：如15分钟数据，10:15:00 这个时间表示的数据范围是 10:15:00 - 10:30:00
        eob 后对齐：如15分钟数据，10:15:00 这个时间表示的数据范围是 10:00:00 - 10:15:00

        合并到源的对齐方式与合并后的要一致

        @param minute: 需要合成的分钟数
        @param cl_config: 缠论对配置项
        @param dt_align_type: 时间对齐方式，bob 前对齐，eob 后对齐
        """

        self.minute = minute  # 合成后的分钟数据
        self.cl_config = cl_config  # 缠论配置项
        self.dt_align_type = dt_align_type  # 时间对齐类型

        self.to_klines: pd.DataFrame  # 合成后保存到k线数据
        self.to_cl_data: ICL  # 合成后计算的缠论数据
        self.to_klines = None
        self.to_cl_data = None

    def update_klines(self, from_klines: pd.DataFrame) -> ICL:
        if len(from_klines) == 0:
            return self.to_cl_data

        convert_klines = (
            from_klines
            if self.to_klines is None or len(self.to_klines) < 10
            else from_klines[from_klines["date"] >= self.to_klines["date"].iloc[-4]]
        )

        convert_klines.insert(0, column="date_index", value=convert_klines["date"])
        convert_klines.set_index("date_index", inplace=True)
        period_type = f"{self.minute}min"
        # 前对其

        if self.dt_align_type == "bob":
            label = "right"
            closed = "left"
            period_klines = convert_klines.resample(
                period_type, label=label, closed=closed
            ).first()
        else:
            label = "left"
            closed = "right"
            period_klines = convert_klines.resample(
                period_type, label=label, closed=closed
            ).last()
        period_klines["open"] = (
            convert_klines["open"]
            .resample(period_type, label=label, closed=closed)
            .first()
        )
        period_klines["close"] = (
            convert_klines["close"]
            .resample(period_type, label=label, closed=closed)
            .last()
        )
        period_klines["high"] = (
            convert_klines["high"]
            .resample(period_type, label=label, closed=closed)
            .max()
        )
        period_klines["low"] = (
            convert_klines["low"]
            .resample(period_type, label=label, closed=closed)
            .min()
        )
        period_klines["volume"] = (
            convert_klines["volume"]
            .resample(period_type, label=label, closed=closed)
            .sum()
        )
        if "position" in convert_klines.columns:
            period_klines["position"] = (
                convert_klines["position"]
                .resample(period_type, label=label, closed=closed)
                .last()
            )
        period_klines.dropna(inplace=True)
        period_klines.reset_index(inplace=True)
        period_klines.drop("date_index", axis=1, inplace=True)

        if self.to_klines is None:
            self.to_klines = period_klines
        else:
            self.to_klines = pd.concat(
                [self.to_klines.iloc[:-1:], period_klines.iloc[1::]], ignore_index=True
            )
            self.to_klines = self.to_klines.drop_duplicates(
                ["date"], keep="last"
            ).sort_values("date")

        # 控制一下大小
        if len(self.to_klines) > 20000:
            self.to_klines = self.to_klines.iloc[-10000::]
            self.to_cl_data = None

        if self.to_cl_data is None:
            self.to_cl_data = cl.CL(
                self.to_klines.iloc[0]["code"], str(self.minute), self.cl_config
            ).process_klines(self.to_klines)
        else:
            self.to_cl_data.process_klines(self.to_klines)
        return self.to_cl_data


if __name__ == "__main__":
    from chanlun.cl_utils import query_cl_chart_config
    from chanlun.exchange.exchange import convert_futures_kline_frequency
    from chanlun.exchange.exchange_db import ExchangeDB

    market = "futures"
    code = "SHFE.RB"
    freq = "1m"
    cl_config = query_cl_chart_config(market, code)
    ex = ExchangeDB(market)

    klines = ex.klines(code, freq)
    # 合成前的K线
    print(klines[["date", "open", "close", "high", "low", "volume"]].tail(10))

    kg = KlinesGenerator(30, cl_config, "eob")
    cd = kg.update_klines(klines)
    # 合成后的K线
    print(kg.to_klines[["date", "open", "close", "high", "low", "volume"]].tail())

    klines_day = convert_futures_kline_frequency(kg.to_klines, "d")
    print(klines_day.tail())
    print(klines_day.tail())
