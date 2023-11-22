from chanlun import fun
from chanlun.cl_interface import *
from chanlun import cl


class KlinesGenerator:
    """
    K线合成，目前只支持分钟线合成，小时、日，需要考虑不同市场的交易时间，比较麻烦，不搞呢
    """

    def __init__(self, minute: int, cl_config: dict = None, dt_align_type: str = 'eob'):
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

        # 获取k线数据的时区
        tz = from_klines.iloc[-1]['date'].tz

        convert_klines = from_klines \
            if self.to_klines is None or len(self.to_klines) < 10 else \
            from_klines[from_klines['date'] >= self.to_klines['date'].iloc[-2]]

        new_klines = {}
        for _, _k in convert_klines.iterrows():
            dt_timestamp = _k['date'].to_pydatetime().timestamp()
            if self.dt_align_type == 'bob':
                new_dt_timestamp = dt_timestamp - (dt_timestamp % (self.minute * 60))
            else:
                dt_timestamp -= 1
                new_dt_timestamp = dt_timestamp - (dt_timestamp % (self.minute * 60)) + (self.minute * 60)

            new_dt = fun.timeint_to_datetime(new_dt_timestamp, tz=tz)
            if new_dt not in new_klines.keys():
                new_klines[new_dt] = {
                    'code': _k['code'],
                    'date': new_dt,
                    'open': _k['open'],
                    'close': _k['close'],
                    'high': _k['high'],
                    'low': _k['low'],
                    'volume': float(_k['volume']),
                }
            else:
                new_klines[new_dt]['high'] = max(new_klines[new_dt]['high'], _k['high'])
                new_klines[new_dt]['low'] = min(new_klines[new_dt]['low'], _k['low'])
                new_klines[new_dt]['close'] = _k['close']
                new_klines[new_dt]['volume'] += float(_k['volume'])
        kline_pd = pd.DataFrame(new_klines.values())
        if self.to_klines is None:
            self.to_klines = kline_pd
        else:
            self.to_klines = pd.concat([self.to_klines, kline_pd], ignore_index=True)
            self.to_klines = self.to_klines.drop_duplicates(['date'], keep='last').sort_values('date')

        if self.to_cl_data is None:
            self.to_cl_data = cl.CL(self.to_klines.iloc[0]['code'], str(self.minute), self.cl_config).process_klines(
                self.to_klines)
        else:
            self.to_cl_data.process_klines(self.to_klines)
        return self.to_cl_data


if __name__ == '__main__':
    from chanlun.exchange.exchange_db import ExchangeDB
    from chanlun.cl_utils import query_cl_chart_config

    market = 'us'
    code = 'AAPL'
    freq = '10m'
    cl_config = query_cl_chart_config(market, code)
    ex = ExchangeDB(market)

    klines = ex.klines(code, freq)
    # 合成前的K线
    print(klines[['date', 'open', 'close', 'high', 'low', 'volume']].tail())

    kg = KlinesGenerator(20, cl_config, 'bob')
    cd = kg.update_klines(klines)
    # 合成后的K线
    print(kg.to_klines[['date', 'open', 'close', 'high', 'low', 'volume']].tail())
