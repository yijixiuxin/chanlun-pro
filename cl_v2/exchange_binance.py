import datetime
import time
from typing import List, Dict

import ccxt
import pandas as pd

from . import config
from . import exchange
from . import exchange_db


class ExchangeBinance(exchange.Exchange):

    def __init__(self):
        params = {
            'apiKey': config.BINANCE_APIKEY,
            'secret': config.BINANCE_SECRET,
            # 'verbose': True,
        }
        if config.PROXY_HOST != '':
            params['proxies'] = {
                'https': "http://%s:%s" % (config.PROXY_HOST, config.PROXY_PORT),
                'http': "http://%s:%s" % (config.PROXY_HOST, config.PROXY_PORT)
            }

        self.exchange = ccxt.binanceusdm(params)

        self.db_exchange = exchange_db.ExchangeDB('currency')

        self._g_stocks = []

    def all_stocks(self):
        if len(self._g_stocks) > 0:
            return self._g_stocks

        markets = self.exchange.load_markets()
        stocks = []
        for s in markets:
            if '/' in s:
                stocks.append({'code': s, 'name': s})
        self._g_stocks = stocks
        return stocks

    def klines(self, code: str, frequency: str,
               start_date: str = None, end_date: str = None,
               args: dict = {}) -> [pd.DataFrame, None]:
        if start_date is not None or end_date is not None:
            online_klines = self._online_klines(code, frequency, start_date, end_date, args)
            self.db_exchange.insert_klines(code, frequency, online_klines)
            return online_klines

        db_klines = self.db_exchange.klines(code, frequency)
        if len(db_klines) == 0:
            online_klines = self._online_klines(code, frequency, start_date, end_date, args)
            self.db_exchange.insert_klines(code, frequency, online_klines)
            return online_klines

        last_datetime = db_klines.iloc[-1]['date'].strftime('%Y-%m-%d %H:%M:%S')
        online_klines = self._online_klines(code, frequency, start_date=last_datetime)
        self.db_exchange.insert_klines(code, frequency, online_klines)
        if len(online_klines) == 1000:
            online_klines = self._online_klines(code, frequency, start_date, end_date, args)
            self.db_exchange.insert_klines(code, frequency, online_klines)
            return online_klines

        klines = db_klines.append(online_klines)
        klines.drop_duplicates(subset=['date'], keep='last', inplace=True)
        return klines[-1000::]

    def _online_klines(self, code: str, frequency: str,
               start_date: str = None, end_date: str = None,
               args: dict = {}) -> [pd.DataFrame, None]:
        # 1m  3m  5m  15m  30m  1h  2h  4h  6h  8h  12h  1d  3d  1w  1M
        frequency_map = {'d': '1d', '4h': '4h', '60m': '1h', '30m': '30m', '15m': '15m', '5m': '5m', '1m': '1m'}
        if frequency not in frequency_map.keys():
            raise Exception('不支持的周期: ' + frequency)

        if start_date is not None:
            start_date = int(
                datetime.datetime.timestamp(datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S'))) * 1000
        if end_date is not None:
            end_date = int(
                datetime.datetime.timestamp(datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S'))) * 1000

        kline = self.exchange.fetchOHLCV(symbol=code, timeframe=frequency_map[frequency], limit=1000,
                                         params={'startTime': start_date, 'endTime': end_date})
        kline_pd = pd.DataFrame(kline, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        kline_pd['code'] = code
        kline_pd['date'] = kline_pd['date'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e3))
        # kline_pd['date'] = pd.to_datetime(kline_pd['date'].values / 1000, unit='s', utc=True).tz_convert('Asia/Shanghai')
        return kline_pd[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]



    def ticks(self, codes: List[str]) -> Dict[str, exchange.Tick]:
        res_ticks = {}
        for code in codes:
            _t = self.exchange.fetch_ticker(code)
            res_ticks[code] = exchange.Tick(
                code=code, last=_t['last'], high=_t['high'], low=_t['low'], open=_t['open'], volume=_t['quoteVolume'])
        return res_ticks

    def ticker24HrRank(self, num=20):
        """
        获取24小时交易量排行前的交易对儿
        :param num:
        :return:
        """
        tickers = self.exchange.fapiPublicGetTicker24hr()
        tickers = sorted(tickers, key=lambda d: float(d['quoteVolume']), reverse=True)
        codes = []
        for t in tickers:
            if t['symbol'][-4:] == 'USDT':
                codes.append(t['symbol'][:-4] + '/' + t['symbol'][-4:])
            if len(codes) >= num:
                break
        return codes

    def balance(self):
        b = self.exchange.fetch_balance()
        balances = {
            'total': b['USDT']['total'],
            'free': b['USDT']['free'],
            'used': b['USDT']['used'],
            'profit': b['info']['totalUnrealizedProfit']
        }
        for asset in b['info']['assets']:
            balances[asset['asset']] = {
                'total': asset['availableBalance'],
                'profit': asset['unrealizedProfit'],
            }
        return balances

    def positions(self, code: str = ''):
        position = self.exchange.fetchPositions(symbolOrSymbols=code if code != '' else None)
        """
        symbol 标的
        entryPrice 价格
        contracts 持仓数量
        side 方向 long short
        leverage 杠杠倍数
        unrealizedPnl 未实现盈亏
        initialMargin 占用保证金
        percentage 盈亏百分比
        """
        res = []
        for p in position:
            if p['entryPrice'] != 0.0:
                res.append(p)

        return res

    # 撤销所有挂单
    def cancel_all_order(self, code):
        self.exchange.cancel_all_orders(symbol=code)
        return True

    def order(self, code: str, o_type: str, amount: float, args=None):
        trade_maps = {
            'open_long': {'side': 'BUY', 'positionSide': 'LONG'},
            'open_short': {'side': 'SELL', 'positionSide': 'SHORT'},
            'close_long': {'side': 'SELL', 'positionSide': 'LONG'},
            'close_short': {'side': 'BUY', 'positionSide': 'SHORT'},
        }
        if 'open' in o_type:
            self.exchange.fapiPrivate_post_leverage(
                params={'symbol': code.replace('/', ''), 'leverage': args['leverage']})
        order = self.exchange.create_order(symbol=code,
                                           type='MARKET',
                                           side=trade_maps[o_type]['side'],
                                           amount=amount,
                                           params={'positionSide': trade_maps[o_type]['positionSide']})
        return order

    @staticmethod
    def convert_kline_frequency(klines: pd.DataFrame, to_f: str) -> pd.DataFrame:
        new_kline = {}
        f_maps = {
            '5m': 5 * 60, '15m': 15 * 60, '30m': 30 * 60, '60m': 60 * 60, '120m': 120 * 60, '4h': 4 * 60 * 60,
            'd': 24 * 60 * 60
        }
        seconds = f_maps[to_f]

        for k in klines.iterrows():
            _date = k[1]['date'].to_pydatetime()
            date_time = _date.timestamp()

            new_date_time = date_time - (date_time % seconds)
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
        kline_pd['date'] = pd.to_datetime(kline_pd['date'])
        return kline_pd[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]
