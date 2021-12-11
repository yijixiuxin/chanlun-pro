import datetime
import time
from dataclasses import dataclass
from typing import List, Dict

import baostock as bs
import pandas as pd


@dataclass
class Tick:
    code: str
    last: float
    high: float
    low: float
    open: float
    volume: float


class Exchange:
    """
    交易所类的默认实现
    使用 baostock API 实现 : http://baostock.com/baostock/index.php/%E9%A6%96%E9%A1%B5
    """

    __all_stocks = []
    __run_date = None

    def __init__(self):
        bs.login()

    def all_stocks(self):
        """
        获取支持的所有股票列表
        :return:
        """
        if len(self.__all_stocks) > 0:
            return _global_stocks

        day = datetime.datetime.now()
        day -= datetime.timedelta(days=1)
        while True:
            if day.weekday() in [5, 6]:
                day -= datetime.timedelta(days=1)
            else:
                break
        day = day.strftime('%Y-%m-%d')

        # TODO 节假日兼容
        day = '2021-09-17'

        rs = bs.query_all_stock(day=day)
        is_ok = False
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            row = rs.get_row_data()
            if row[0] == 'sh.600000':
                is_ok = True
            if is_ok:
                _global_stocks.append({'code': row[0], 'name': row[2]})

        return _global_stocks

    def klines(self, code: str, frequency: str,
               start_date: str = None, end_date: str = None,
               args: dict = {}) -> [pd.DataFrame, None]:
        """
        获取 Kline 线
        :param code:
        :param frequency:
        :param start_date:
        :param end_date:
        :param args:
        :return:
        """
        frequency_map = {'m': 'm', 'w': 'w', 'd': 'd', '60m': '60', '30m': '30', '15m': '15', '5m': '5', '1m': '1'}
        if frequency not in frequency_map:
            raise Exception('不支持的周期 : ' + frequency)

        #### 获取沪深A股历史K线数据 ####
        # 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。
        # 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
        # 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
        rs = bs.query_history_k_data_plus(code, "code,date,open,low,high,close,volume", start_date=start_date,
                                          end_date=end_date, frequency=frequency_map[frequency], adjustflag="2")
        if rs.error_code in ['10001001', '10002007']:
            bs.login()
            return self.klines(code, frequency, start_date, end_date, args)
        if rs.error_code != '0':
            print('query_history_k_data_plus respond error_code:' + rs.error_code)
            print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)
            return None

        data_list = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        kline = pd.DataFrame(data_list, columns=rs.fields)

        if frequency in ['60m', '30m', '15m', '5m', '1m']:
            dates = kline['date'].unique()
            new_kline = pd.DataFrame()
            for d in dates:
                dk = kline[kline['date'] == d]
                self.__run_date = None

                def append_time(d):
                    if self.__run_date is None:
                        self.__run_date = datetime.datetime.strptime(d + ' 09:30:00', "%Y-%m-%d %H:%M:%S")
                        self.__run_date = self.__run_date + datetime.timedelta(minutes=int(frequency_map[frequency]))
                    else:
                        self.__run_date = self.__run_date + datetime.timedelta(minutes=int(frequency_map[frequency]))
                        if (self.__run_date.hour == 11 and self.__run_date.minute > 30) or (self.__run_date.hour == 12):
                            self.__run_date = datetime.datetime.strptime(d + ' 13:00:00', "%Y-%m-%d %H:%M:%S")
                            self.__run_date = self.__run_date + datetime.timedelta(
                                minutes=int(frequency_map[frequency]))
                    return self.__run_date.strftime('%Y-%m-%d %H:%M:%S')

                dk['date'] = dk['date'].map(append_time)
                new_kline = new_kline.append(dk)
            kline = new_kline.sort_values('date')
        else:
            kline['date'] = kline['date'].map(
                lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%Y-%m-%d %H:%M:%S"))

        return kline[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """
        获取股票列表的 Tick 信息
        :param codes:
        :return:
        """
        raise Exception('交易所不支持 tick 获取')

    def stock_info(self, code: str) -> [Dict, None]:
        """
        获取股票的基本信息
        :param code:
        :return:
        """
        rs = bs.query_stock_basic(code=code)
        data_list = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        if len(data_list) > 0:
            return {'code': data_list[0][0], 'name': data_list[0][1]}
        return None

    def zixuan_stocks(self, zx_name: str) -> List:
        """
        获取用户自选股列表
        :param zx_name:
        :return: {'code', 'name'}
        """
        return [{'code': 'sh.000001', 'name': '上证指数'}]

    @staticmethod
    def convert_kline_frequency(klines: pd.DataFrame, to_f: str) -> pd.DataFrame:
        """
        转换 k 线到指定的周期
        :param klines:
        :param to_f:
        :return:
        """
        new_kline = {}
        freq_second_maps = {'5m': 5 * 60, '30m': 30 * 60, '60m': 60 * 60, '120m': 120 * 60, 'd': 24 * 60 * 60}
        if to_f not in freq_second_maps.keys():
            raise Exception('不支持的转换周期：' + to_f)

        seconds = freq_second_maps[to_f]

        for k in klines.iterrows():
            dt = datetime.datetime.strptime(k[1]['date'], '%Y-%m-%d %H:%M:%S')
            date_time = datetime.datetime.timestamp(datetime.datetime.strptime(k[1]['date'], '%Y-%m-%d %H:%M:%S'))
            if date_time % seconds == 0:
                new_date_time = date_time
            else:
                new_date_time = date_time - (date_time % seconds) + seconds

            if to_f in ['d']:
                new_date_time -= 8 * 60 * 60
            if to_f == '60m':
                if (dt.hour == 9) or (dt.hour == 10 and dt.minute <= 30):
                    new_date_time = datetime.datetime.timestamp(
                        datetime.datetime.strptime(dt.strftime('%Y-%m-%d 10:30:00'), '%Y-%m-%d %H:%M:%S'))
                elif (dt.hour == 10 and dt.minute >= 30) or (dt.hour == 11):
                    new_date_time = datetime.datetime.timestamp(
                        datetime.datetime.strptime(dt.strftime('%Y-%m-%d 11:30:00'), '%Y-%m-%d %H:%M:%S'))
            if to_f == '120m':
                if dt.hour == 9 or dt.hour == 10 or (dt.hour == 11 and dt.minute <= 30):
                    new_date_time = datetime.datetime.timestamp(
                        datetime.datetime.strptime(dt.strftime('%Y-%m-%d 11:30:00'), '%Y-%m-%d %H:%M:%S'))

            new_date_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(new_date_time))
            if new_date_time in new_kline:
                n_k = new_kline[new_date_time]
                if k[1]['high'] > n_k['high']:
                    n_k['high'] = k[1]['high']
                if k[1]['low'] < n_k['low']:
                    n_k['low'] = k[1]['low']
                n_k['close'] = k[1]['close']
                n_k['volume'] += float(k[1]['volume'])
                new_kline[new_date_time] = n_k
            else:
                new_kline[new_date_time] = {
                    'code': k[1]['code'],
                    'date': new_date_time,
                    'open': k[1]['open'],
                    'close': k[1]['close'],
                    'high': k[1]['high'],
                    'low': k[1]['low'],
                    'volume': float(k[1]['volume']),
                }
        kline_pd = pd.DataFrame(new_kline.values())
        return kline_pd[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]

    def stock_owner_plate(self, code: str):
        """
        股票所属板块信息
        :param code:
        :return:
        """
        return {
            'HY': [{'code': '行业代码', 'name': '行业名称'}],
            'GN': [{'code': '概念代码', 'name': '概念名称'}],
        }

    def plate_stocks(self, code: str):
        """
        获取板块股票列表信息
        :param code: 板块代码
        :return:
        """
        return [{'code': 'SH.000001', 'name': '上证指数'}]

    def balance(self):
        """
        账户资产信息
        :return:
        """
        raise Exception('账户资产接口不支持')

    def positions(self, code: str = ''):
        """
        当前账户持仓信息
        :param code:
        :return:
        """
        raise Exception('账户资产接口不支持')

    def order(self, code: str, o_type: str, amount: float, args=None):
        """
        下单接口
        :param args:
        :param code:
        :param o_type:
        :param amount:
        :return:
        """
        raise Exception('账户资产接口不支持')
