import time
import traceback
from typing import Union

from pytdx.errors import TdxConnectionError
from pytdx.exhq import TdxExHq_API
from pytdx.util import best_ip
from tenacity import retry, stop_after_attempt, wait_random, retry_if_result

from chanlun import rd
from chanlun.exchange.exchange import *
from chanlun.file_db import FileCacheDB

g_all_stocks = []
g_trade_days = None


class ExchangeTDXUS(Exchange):
    """
    通达信香港行情接口
    """

    def __init__(self):
        # super().__init__()

        # 选择最优的服务器，并保存到 redis 中
        connect_ip = rd.Robj().get('tdxex_connect_ip')
        if connect_ip is None:
            connect_ip = self.reset_tdx_ip()
            print('TDXEX 最优服务器：' + connect_ip)
        self.connect_ip = {'ip': connect_ip.split(':')[0], 'port': int(connect_ip.split(':')[1])}

        # 设置时区
        self.tz = pytz.timezone('US/Eastern')

        # 文件缓存
        self.fdb = FileCacheDB()

    def reset_tdx_ip(self):
        """
        重新选择tdx最优服务器
        """
        connect_ip = best_ip.select_best_ip('future')
        connect_ip = connect_ip['ip'] + ':' + str(connect_ip['port'])
        rd.Robj().set('tdxex_connect_ip', connect_ip)
        self.connect_ip = {'ip': connect_ip.split(':')[0], 'port': int(connect_ip.split(':')[1])}
        return connect_ip

    def default_code(self):
        return 'AAPL'

    def support_frequencys(self):
        return {
            'y': "Y", 'q': 'Q', 'm': 'M', 'w': 'W', 'd': 'D',
            '60m': '60m', '30m': '30m', '15m': '15m', '10m': '10m', '5m': '5m', '2m': '2m', '1m': '1m'
        }

    def all_stocks(self):
        """
        使用 通达信的方式获取所有股票代码
        """
        global g_all_stocks
        if len(g_all_stocks) > 0:
            return g_all_stocks
        g_all_stocks = rd.get_ex('tdx_us_all')
        if g_all_stocks is not None:
            return g_all_stocks
        g_all_stocks = []
        client = TdxExHq_API(raise_exception=True, auto_retry=True)
        with client.connect(self.connect_ip['ip'], self.connect_ip['port']):
            start_i = 0
            count = 1000
            while True:
                instruments = client.get_instrument_info(start_i, count)
                for _i in instruments:
                    if _i['category'] == 13 and _i['market'] == 74:
                        g_all_stocks.append({
                            'code': _i['code'],
                            'name': _i['name'],
                        })
                start_i += count
                if len(instruments) < count:
                    break

        print(f'美股列表从 TDX 进行获取，共获取数量：{len(g_all_stocks)}')

        if g_all_stocks:
            rd.save_ex('tdx_us_all', 24 * 60 * 60, g_all_stocks)

        return g_all_stocks

    def to_tdx_code(self, code):
        """
        转换为 tdx 对应的代码
        """
        return 74, code

    @retry(stop=stop_after_attempt(3), wait=wait_random(min=1, max=5), retry=retry_if_result(lambda _r: _r is None))
    def klines(self, code: str, frequency: str,
               start_date: str = None, end_date: str = None,
               args=None) -> Union[pd.DataFrame, None]:

        """
        通达信，不支持按照时间查找
        """
        _s_time = time.time()

        if args is None:
            args = {}
        if 'pages' not in args.keys():
            args['pages'] = 5
        else:
            args['pages'] = int(args['pages'])

        frequency_map = {
            'y': 11, 'q': 10, 'm': 6, 'w': 5, 'd': 9, '60m': 3, '30m': 2, '15m': 1, '10m': 0, '5m': 0, '2m': 8, '1m': 8
        }
        market, tdx_code = self.to_tdx_code(code)
        if market is None or start_date is not None or end_date is not None:
            print('通达信不支持的调用参数')
            return None

        # _time_s = time.time()
        try:
            client = TdxExHq_API(raise_exception=True, auto_retry=True)
            with client.connect(self.connect_ip['ip'], self.connect_ip['port']):
                klines_df: pd.DataFrame = self.fdb.get_tdx_klines(code, frequency)
                if klines_df is None:
                    # 获取 8*800 = 6400 条数据
                    klines_df = pd.concat([
                        client.to_df(
                            client.get_instrument_bars(frequency_map[frequency], market, tdx_code, (i - 1) * 700, 700)
                        )
                        for i in range(1, args['pages'] + 1)
                    ], axis=0, sort=False)
                    klines_df.loc[:, 'date'] = pd.to_datetime(klines_df['datetime'])
                    klines_df.sort_values('date', inplace=True)
                else:
                    for i in range(1, args['pages'] + 1):
                        # print(f'{code} 使用缓存，更新获取第 {i} 页')
                        _ks = client.to_df(
                            client.get_instrument_bars(frequency_map[frequency], market, tdx_code, (i - 1) * 700, 700)
                        )
                        _ks.loc[:, 'date'] = pd.to_datetime(klines_df['datetime'])
                        _ks.sort_values('date', inplace=True)
                        new_start_dt = _ks.iloc[0]['date']
                        old_end_dt = klines_df.iloc[-1]['date']
                        klines_df = pd.concat([klines_df, _ks], ignore_index=True)
                        # 如果请求的第一个时间大于缓存的最后一个时间，退出
                        # print(klines.iloc[-1], len(klines))
                        # print(old_end_dt, new_start_dt)
                        if old_end_dt >= new_start_dt:
                            break

            # 删除重复数据
            klines_df = klines_df.drop_duplicates(['date'], keep='last').sort_values('date')
            self.fdb.save_tdx_klines(code, frequency, klines_df)

            klines_df.loc[:, 'date'] = klines_df['date'].apply(self._convert_dt)
            klines_df = klines_df.sort_values('date')
            klines_df.loc[:, 'code'] = code
            klines_df.loc[:, 'volume'] = klines_df['amount']

            klines_df = klines_df[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]

            if frequency in ['10m', '2m']:
                klines_df = convert_us_tdx_kline_frequency(klines_df, frequency)

            return klines_df
        except TdxConnectionError:
            self.reset_tdx_ip()
        except Exception as e:
            print(f'tdx 获取行情异常 {code} - {frequency} Exception ：{str(e)}')
            traceback.print_exc()
        finally:
            pass
            # print(f'tdx 请求行情用时：{time.time() - _s_time}')
        return None

    def _convert_dt(self, _dt: datetime.datetime):
        """
        将通达信的中国时间，转换成美国东部时间
        """
        if _dt.hour == 15 and _dt.minute == 0:
            # 这个是日线及以上周期的数据
            _dt = _dt.replace(hour=16, minute=0, tzinfo=self.tz)
            return _dt

        _dt = _dt.replace(tzinfo=pytz.timezone('Asia/Shanghai'))

        if _dt.hour in [0, 1, 2, 3, 4, 5]:
            _dt = _dt + datetime.timedelta(days=1)
        return _dt.astimezone(self.tz)

    def stock_info(self, code: str) -> Union[Dict, None]:
        """
        获取股票名称
        """
        all_stock = self.all_stocks()
        stock = [_s for _s in all_stock if _s['code'] == code]
        if not stock:
            return None
        return {
            'code': stock[0]['code'],
            'name': stock[0]['name']
        }

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """
        如果可以使用 富途 的接口，就用 富途的，否则就用 日线的 K线计算
        使用 富途 的接口会很快，日线则很慢
        获取日线的k线，并返回最后一根k线的数据
        """
        ticks = {}
        client = TdxExHq_API(raise_exception=True, auto_retry=True)
        with client.connect(self.connect_ip['ip'], self.connect_ip['port']):
            for _code in codes:
                _market, _tdx_code = self.to_tdx_code(_code)
                if _market is None:
                    continue
                _quote = client.get_instrument_quote(_market, _tdx_code)
                # OrderedDict(
                #     [('market', 1), ('code', '00700'), ('pre_close', 362.8000183105469), ('open', 372.20001220703125),
                #      ('high', 374.8000183105469), ('low', 364.4000244140625), ('price', 367.6000061035156),
                #      ('kaicang', 0), ('zongliang', 17784504), ('xianliang', 1189500), ('neipan', 8892299),
                #      ('waipan', 8892205), ('chicang', 0), ('bid1', 0.0), ('bid2', 0.0), ('bid3', 0.0), ('bid4', 0.0),
                #      ('bid5', 0.0), ('bid_vol1', 0), ('bid_vol2', 0), ('bid_vol3', 0), ('bid_vol4', 0), ('bid_vol5', 0),
                #      ('ask1', 0.0), ('ask2', 0.0), ('ask3', 0.0), ('ask4', 0.0), ('ask5', 0.0), ('ask_vol1', 0),
                #      ('ask_vol2', 0), ('ask_vol3', 0), ('ask_vol4', 0), ('ask_vol5', 0)])
                if len(_quote) > 0:
                    _quote = _quote[0]
                    ticks[_code] = Tick(
                        code=_code, last=_quote['price'],
                        buy1=_quote['bid1'], sell1=_quote['ask1'],
                        low=_quote['low'], high=_quote['high'],
                        volume=_quote['zongliang'], open=_quote['open'],
                        rate=round(
                            (_quote['price'] - _quote['pre_close']) / _quote['price'] * 100, 2
                        )
                    )
        return ticks

    def now_trading(self):
        """
        TODO 暂时还没有找到接口，直接硬编码
        周一致周五，美国东部时间，9:30 - 16:00
        """
        tz = pytz.timezone('US/Eastern')
        now = datetime.datetime.now(tz)
        weekday = now.weekday()
        hour = now.hour
        minute = now.minute
        if weekday in [0, 1, 2, 3, 4] and ((10 <= hour < 16) or (hour == 9 and minute >= 30)):
            return True
        return False

    def balance(self):
        raise Exception('交易所不支持')

    def positions(self, code: str = ''):
        raise Exception('交易所不支持')

    def order(self, code: str, o_type: str, amount: float, args=None):
        raise Exception('交易所不支持')

    def stock_owner_plate(self, code: str):
        raise Exception('交易所不支持')

    def plate_stocks(self, code: str):
        raise Exception('交易所不支持')


if __name__ == '__main__':
    ex = ExchangeTDXUS()
    # stocks = ex.all_stocks()
    # print(len(stocks))
    # print(stocks)
    #
    # print(ex.to_tdx_code('KH.00700'))
    #
    klines = ex.klines(ex.default_code(), 'd')
    print(klines.tail(20))
    # klines = ex.klines('NU', '10m')
    # print(klines)

    # ticks = ex.ticks([ex.default_code()])
    # print(ticks)
