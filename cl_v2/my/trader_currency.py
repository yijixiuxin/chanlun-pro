from cl_v2 import exchange_binance
from cl_v2 import rd
from cl_v2 import fun
from cl_v2 import trader


class CurrencyTrader(trader.Trader):
    """
    数字货币交易者
    """

    def __init__(self, name, is_stock=True, is_futures=False, mmds=None, log=None):
        super().__init__(name, is_stock, is_futures, mmds, log)
        self.exchange = exchange_binance.ExchangeBinance()

    # 做多买入
    def open_buy(self, code, opt):
        # 固定买入 1000 USDT 的
        open_usdt = 1000
        leverage = 5
        ticks = self.exchange.ticks([code])
        amount = (open_usdt / ticks[code].last) * leverage
        res = self.exchange.order(code, 'open_long', amount, {'leverage': leverage})
        if res is False:
            fun.send_dd_msg('currency', '%s 下单失败' % code)
            return False
        msg = '开多仓 %s 价格 %s 数量 %s 原因 %s' % (code, res['price'], res['amount'], opt['msg'])
        fun.send_dd_msg('currency', msg)
        rd.currency_opt_record_save(code, '策略交易：' + msg)

        return {'price': res['price'], 'amount': res['amount']}

    # 做空卖出
    def open_sell(self, code, opt):
        # 固定卖出 1000 USDT 的
        open_usdt = 1000
        leverage = 5
        ticks = self.exchange.ticks([code])
        amount = (open_usdt / ticks[code].last) * leverage
        res = self.exchange.order(code, 'open_short', amount, {'leverage': leverage})
        if res is False:
            fun.send_dd_msg('currency', '%s 下单失败' % code)
            return False
        msg = '开空仓 %s 价格 %s 数量 %s 原因 %s' % (code, res['price'], res['amount'], opt['msg'])
        fun.send_dd_msg('currency', msg)
        rd.currency_opt_record_save(code, '策略交易：' + msg)
        return {'price': res['price'], 'amount': res['amount']}

    # 做多平仓
    def close_buy(self, code, pos: trader.POSITION, opt):
        hold_position = self.exchange.positions(code)
        if len(hold_position) == 0:
            return {'price': pos.price, 'amount': pos.amount}
        hold_position = hold_position[0]

        res = self.exchange.order(code, 'close_long', pos.amount)
        if res is False:
            fun.send_dd_msg('currency', '%s 下单失败' % code)
            return False
        msg = '平多仓 %s 价格 %s 数量 %s 盈亏 %s (%.2f%%) 原因 %s' % (
            code, res['price'], res['amount'], hold_position['unrealizedPnl'], hold_position['percentage'], opt['msg'])
        fun.send_dd_msg('currency', msg)
        rd.currency_opt_record_save(code, '策略交易：' + msg)
        return {'price': res['price'], 'amount': res['amount']}

    # 做空平仓
    def close_sell(self, code, pos: trader.POSITION, opt):
        hold_position = self.exchange.positions(code)
        if len(hold_position) == 0:
            return {'price': pos.price, 'amount': pos.amount}
        hold_position = hold_position[0]

        res = self.exchange.order(code, 'close_short', pos.amount)
        if res is False:
            fun.send_dd_msg('currency', '%s 下单失败' % code)
            return False
        msg = '平空仓 %s 价格 %s 数量 %s 盈亏 %s (%.2f%%) 原因 %s' % (
            code, res['price'], res['amount'], hold_position['unrealizedPnl'], hold_position['percentage'], opt['msg'])
        fun.send_dd_msg('currency', msg)
        rd.currency_opt_record_save(code, '策略交易：' + msg)
        return {'price': res['price'], 'amount': res['amount']}

