import random
from typing import List, Dict

from futu import *

from . import config
from . import exchange


class ExchangeFutu(exchange.Exchange):
    """
    富途交易所
    """
    __all_stocks = []

    def __init__(self):
        SysConfig.set_all_thread_daemon(True)
        self.ctx = None
        self.ttx = None

    def CTX(self):
        if self.ctx is None:
            self.ctx = OpenQuoteContext(host=config.FUTU_HOST, port=config.FUTU_PORT, is_encrypt=False)
        if random.randint(0, 100) > 90:
            # 随机执行，订阅数量大于  250 ，则关闭所有订阅
            ret, sub_data = self.ctx.query_subscription()
            if ret == RET_OK and sub_data['own_used'] >= 250:
                # 取消订阅
                self.ctx.unsubscribe_all()
        return self.ctx

    def TTX(self):
        if self.ttx is None:
            self.ttx = OpenSecTradeContext(filter_trdmarket=TrdMarket.HK, host=config.FUTU_HOST, port=config.FUTU_PORT,
                                           security_firm=SecurityFirm.FUTUSECURITIES)

    def all_stocks(self):
        if len(self.__all_stocks) > 0:
            return self.__all_stocks
        ret, data = self.CTX().get_plate_stock('HK.BK1910')
        if ret == RET_OK:
            for s in data.iterrows():
                self.__all_stocks.append({'code': s[1]['code'], 'name': s[1]['stock_name']})
        ret, data = self.CTX().get_plate_stock('SH.3000005')
        if ret == RET_OK:
            for s in data.iterrows():
                self.__all_stocks.append({'code': s[1]['code'], 'name': s[1]['stock_name']})
        return self.__all_stocks

    def klines(self, code: str, frequency: str,
               start_date: str = None, end_date: str = None,
               args: dict = {}) -> [pd.DataFrame, None]:
        frequency_map = {'1m': {'ktype': KLType.K_1M, 'subtype': SubType.K_1M},
                         '5m': {'ktype': KLType.K_5M, 'subtype': SubType.K_5M},
                         '15m': {'ktype': KLType.K_15M, 'subtype': SubType.K_15M},
                         '30m': {'ktype': KLType.K_30M, 'subtype': SubType.K_30M},
                         '60m': {'ktype': KLType.K_60M, 'subtype': SubType.K_60M},
                         '120m': {'ktype': KLType.K_60M, 'subtype': SubType.K_60M},
                         'd': {'ktype': KLType.K_DAY, 'subtype': SubType.K_DAY},
                         'w': {'ktype': KLType.K_WEEK, 'subtype': SubType.K_WEEK},
                         'm': {'ktype': KLType.K_MON, 'subtype': SubType.K_MON},
                         'y': {'ktype': KLType.K_YEAR, 'subtype': SubType.K_YEAR}}

        if 'is_history' not in args.keys():
            args['is_history'] = False

        if start_date is None and end_date is None and args['is_history'] is False:
            # 获取实时 K 线数据
            # 订阅
            self.CTX().subscribe([code], [frequency_map[frequency]['subtype']], is_first_push=False,
                                 subscribe_push=False)
            # 获取 K 线
            ret, kline = self.CTX().get_cur_kline(code, 1000, frequency_map[frequency]['subtype'],
                                                  AuType.QFQ)
            if ret != RET_OK:
                print(kline)
                return None
        else:
            if start_date is None and end_date is not None:
                time_format = '%Y-%m-%d %H:%M:%S'
                if len(end_date) == 10:
                    time_format = '%Y-%m-%d'
                end_datetime = dt.datetime(*time.strptime(end_date, time_format)[:6])
                if frequency == '1m':
                    start_date = (end_datetime - dt.timedelta(days=5)).strftime(time_format)
                elif frequency == '5m':
                    start_date = (end_datetime - dt.timedelta(days=25)).strftime(time_format)
                elif frequency == '30m':
                    start_date = (end_datetime - dt.timedelta(days=150)).strftime(time_format)
                elif frequency == 'd':
                    start_date = (end_datetime - dt.timedelta(days=1500)).strftime(time_format)
            ret, kline, pk = self.CTX().request_history_kline(code=code, start=start_date, end=end_date, max_count=None,
                                                              ktype=frequency_map[frequency]['ktype'])
        kline['date'] = kline['time_key']
        kline = kline[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]
        if frequency == '120m':
            kline = self.convert_kline_frequency(kline, '120m')

        return kline

    def ticks(self, codes: List[str]) -> Dict[str, exchange.Tick]:
        self.CTX().subscribe(codes, [SubType.QUOTE], subscribe_push=False)
        ret, data = self.CTX().get_stock_quote(codes)
        if ret == RET_OK:
            res_ticks = {}
            for _d in data.iterrows():
                res_ticks[_d[1]['code']] = exchange.Tick(code=_d[1]['code'],
                                                         last=_d[1]['last_price'],
                                                         high=_d[1]['high_price'],
                                                         low=_d[1]['low_price'],
                                                         open=_d[1]['open_price'],
                                                         volume=_d[1]['volume'])
            return res_ticks
        else:
            print('Ticks Error : ', data)
            return {}

    def stock_info(self, code: str) -> [Dict, None]:
        ret, data = self.CTX().get_stock_basicinfo(None, SecurityType.STOCK, [code])
        if ret == RET_OK:
            return {
                'code': data.iloc[0]['code'],
                'name': data.iloc[0]['name'],
                'lot_size': data.iloc[0]['lot_size'],
                'stock_type': data.iloc[0]['stock_type'],
            }
        return None

    def zixuan_stocks(self, zx_name: str) -> List:
        ret, data = self.CTX().get_user_security(zx_name)
        if ret is not RET_OK:
            print(data)
            return []
        res_stocks = []
        for d in data.iterrows():
            res_stocks.append({'code': d[1]['code'], 'name': d[1]['name']})
        return res_stocks

    def market_trade_days(self, market):
        """
        指定市场的交易时间
        :return:
        """
        market_map = {'hk': TradeDateMarket.HK, 'cn': TradeDateMarket.CN}
        ret, data = self.CTX().request_trading_days(market=market_map[market], start=time.strftime('%Y-%m-%d'))
        if ret == RET_OK:
            return data
        else:
            return None

    def is_trade_day(self):
        """
        当前是否是交易日
        :return:
        """
        # return True
        ret, hk_data = self.CTX().request_trading_days(TradeDateMarket.HK)
        ret, cn_data = self.CTX().request_trading_days(TradeDateMarket.CN)
        hk_data = hk_data[-1]['time']
        cn_data = cn_data[-1]['time']

        today = time.strftime('%Y-%m-%d')
        if today == hk_data or today == cn_data:
            hour = int(time.strftime('%H'))
            minute = int(time.strftime('%M'))
            if (hour == 9 and minute >= 30) or (hour == 11 and minute <= 30) or (hour in [10, 13, 14]):
                return True
            if today == hk_data and hour in [11, 15]:
                return True
        return False

    def query_kline_edu(self):
        ret, data = self.CTX().get_history_kl_quota(get_detail=False)
        if ret == RET_OK:
            print(data)
        else:
            print('error:', data)

    def stock_owner_plate(self, code: str):
        plate_infos = {'HY': [], 'GN': []}
        ret, data = self.CTX().get_owner_plate([code])
        if ret == RET_OK:
            for p in data.iterrows():
                if p[1]['plate_type'] == 'INDUSTRY':
                    plate_infos['HY'].append({'code': p[1]['plate_code'], 'name': p[1]['plate_name']})
                if p[1]['plate_type'] == 'CONCEPT':
                    plate_infos['GN'].append({'code': p[1]['plate_code'], 'name': p[1]['plate_name']})
        return plate_infos

    def plate_stocks(self, code: str):
        stocks = []
        ret, data = self.CTX().get_plate_stock(code, sort_field=SortField.CHANGE_RATE, ascend=False)
        if ret == RET_OK:
            for s in data.iterrows():
                stocks.append({'code': s[1]['code'], 'name': s[1]['stock_name']})
        return stocks

    def balance(self):
        ret, account = self.TTX().accinfo_query()
        if ret == RET_OK:
            return {
                'power': account.iloc[0]['power'],
                'max_power_short': account.iloc[0]['max_power_short'],
                'net_cash_power': account.iloc[0]['net_cash_power'],
                'total_assets': account.iloc[0]['total_assets'],
                'cash': account.iloc[0]['cash'],
                'market_val': account.iloc[0]['market_val'],
                'long_mv': account.iloc[0]['long_mv'],
                'short_mv': account.iloc[0]['short_mv'],
            }
        return None

    def positions(self, code=''):
        ret, poss = self.TTX().position_list_query(code=code)
        if ret == RET_OK:
            pos_list = []
            for _p in poss.iterrows():
                if _p[1]['qty'] == 0.0:
                    continue
                pos_list.append(
                    {
                        'code': _p[1]['code'],
                        'name': _p[1]['stock_name'],
                        'type': _p[1]['position_side'],
                        'amount': _p[1]['qty'],
                        'can_sell_amount': _p[1]['can_sell_qty'],
                        'price': _p[1]['cost_price'],
                        'profit': _p[1]['pl_ratio'],
                        'profit_val': _p[1]['pl_val'],
                    }
                )
            return pos_list
        else:
            print('Position Error : ', poss)
        return []

    def can_trade_val(self, code):
        """
        查询股票可以交易的数量
        :param code:
        :return:
        """
        ret, data = self.TTX().acctradinginfo_query(order_type=OrderType.MARKET, code=code, price=0)
        if ret == RET_OK:
            return {
                'max_cash_buy': data.iloc[0]['max_cash_buy'],
                'max_margin_buy': data.iloc[0]['max_cash_and_margin_buy'],
                'max_position_sell': data.iloc[0]['max_position_sell'],
                'max_margin_short': data.iloc[0]['max_sell_short'],
                'max_buy_back': data.iloc[0]['max_buy_back'],
            }
        else:
            print('Can Trade Val Error : ', data)
            return None

    def order(self, code, o_type, amount, args=None):
        order_type_map = {'buy': TrdSide.BUY, 'sell': TrdSide.SELL}
        trd_ctx.unlock_trade(config.FUTU_UNLOCK_PWD)  # 先解锁交易
        ret, data = self.TTX().place_order(price=0, qty=amount, code=code, order_type=OrderType.MARKET,
                                           trd_side=order_type_map[o_type])
        if ret == RET_OK:
            time.sleep(5)
            ret, o = TTX().order_list_query(order_id=data.iloc[0]['order_id'])
            if ret == RET_OK:
                return {
                    'id': o.iloc[0]['order_id'],
                    'code': o.iloc[0]['code'],
                    'name': o.iloc[0]['stock_name'],
                    'type': o.iloc[0]['trd_side'],
                    'order_type': o.iloc[0]['order_type'],
                    'order_status': o.iloc[0]['order_status'],
                    'price': o.iloc[0]['price'],
                    'amount': o.iloc[0]['qty'],
                    'dealt_amount': o.iloc[0]['dealt_qty'],
                    'dealt_avg_price': o.iloc[0]['dealt_avg_price'],
                }
            else:
                print('Order Get Order Error : ', o)
                return False
        else:
            print('Order Error : ', data)
            return False
