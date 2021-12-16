# 回放行情所需
import datetime
from typing import Dict

from . import cl
from . import exchange
from . import exchange_binance
from . import exchange_db


class BackKlines:
    """
    数据库行情回放
    """

    def __init__(self, market, code, start_date, end_date, frequency=None):
        """
        配置初始化
        :param market: 市场 支持 a hk currency
        :param code:
        :param frequencys:
        :param start_date:
        :param end_date:
        """
        if frequency is None:
            frequency = ['30m', '5m']
        self.market = market
        self.code = code
        self.frequencys = frequency
        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        self.start_date = start_date
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        self.end_date = end_date

        self.now_date = start_date

        self.klines = {}
        self.show_klines = {}

        # 保存缠论数据对象
        self.cl_datas: Dict[str, cl.CL] = {}
        for f in self.frequencys:
            self.cl_datas[f] = None

        self.exchange = exchange_db.ExchangeDB(self.market)

        self.time_fmt = '%Y-%m-%d %H:%M:%S'

    def start(self):
        # 获取行情数据
        for f in self.frequencys:
            real_start_date = self._cal_start_date_by_frequency(self.start_date, f)
            self.klines[f] = self.exchange.klines(self.code,
                                                  f,
                                                  start_date=real_start_date,
                                                  end_date=self.end_date,
                                                  args={'limit': None})
            print('Code %s F %s len %s' % (self.code, f, len(self.klines[f])))
        self.next(self.frequencys[-1])

    def next(self, f):
        # 设置下一个时间
        while True:
            self.now_date = self._next_datetime(self.now_date, f)
            # print('Next date : ', self.now_date)
            if self.now_date is None or self.now_date > self.end_date:
                return False
            try:
                for f in self.frequencys:
                    self.show_klines[f] = self.klines[f][self.klines[f]['date'] <= self.now_date][-1000::]

                    # 计算缠论数据
                    if self.cl_datas[f] is None:
                        self.cl_datas[f] = cl.CL(self.code, self.show_klines[f], f)
                    else:
                        self.cl_datas[f].increment_process_kline(self.show_klines[f])

                self.convert_klines()
            except Exception as e:
                print('ERROR: %s 运行 Next 错误，当前时间 : %s，错误信息：%s' % (self.code, self.now_date, str(e)))
                # raise e
                return False
            return True

    def convert_klines(self):
        """
        转换 kline，去除未来的 kline数据
        :return:
        """
        for i in range(len(self.frequencys), 1, -1):
            min_f = self.frequencys[i - 1]
            max_f = self.frequencys[i - 2]
            if self.market == 'currency':
                new_kline = exchange_binance.ExchangeBinance.convert_kline_frequency(self.show_klines[min_f][-120::],
                                                                                     max_f)
            else:
                new_kline = exchange.Exchange.convert_kline_frequency(self.show_klines[min_f][-120::], max_f)
            self.show_klines[max_f].iloc[-1] = new_kline.iloc[-1]

        return True

    def _next_datetime(self, now_date, frequency):
        """
        根据周期，计算下一个时间的起始
        :param now_date:
        :param frequency:
        :return:
        """
        if now_date is None:
            return None

        next_klines = self.klines[frequency][self.klines[frequency]['date'] > now_date]

        if len(next_klines) == 0:
            return None
        next_date = next_klines.iloc[0]['date']
        # pre_klines = self.klines[frequency][self.klines[frequency]['date'] < next_date]
        # if len(pre_klines) < 500:
        #     if len(next_klines) > 501:
        #         next_date = next_klines.iloc[500]['date']
        #     else:
        #         return None
        return next_date

    def _cal_start_date_by_frequency(self, start_date: datetime, frequency) -> str:
        """
        按照周期，计算行情获取的开始时间
        :param start_date :
        :param frequency:
        :return:
        """
        market_days_freq_maps = {
            'a': {'d': 5000, '120m': 500, '4h': 500, '60m': 100, '30m': 100, '15m': 50, '5m': 25, '1m': 5},
            'hk': {'d': 5000, '120m': 500, '4h': 500, '60m': 100, '30m': 100, '15m': 50, '5m': 25, '1m': 5},
            'currency': {'d': 100, '120m': 100, '4h': 100, '60m': 50, '30m': 50, '15m': 25, '5m': 5, '1m': 1},
        }
        for _freq in ['d', '120m', '4h', '60m', '30m', '15m', '5m', '1m']:
            if _freq == frequency:
                return (start_date - datetime.timedelta(days=market_days_freq_maps[self.market][_freq])).strftime(
                    self.time_fmt)
        raise Exception('不支持的周期 ' + frequency)
