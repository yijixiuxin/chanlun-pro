import datetime

from chanlun import rd, fun
from chanlun import zixuan
from chanlun.exchange.exchange_tdx import ExchangeTDX
from chanlun.backtesting.backtest_trader import BackTestTrader
from chanlun.backtesting.base import Operation, POSITION

"""
使用通达信的行情接口，其中部分接口依赖 富途，需要启动富途才可正常使用
不实际产生交易，只添加持仓并发送短信通知
"""


class TraderAStock(BackTestTrader):
    """
    A股票交易对象

    没有实际的交易接口，只用来记录添加到持仓自选，并发送消息，实盘需要根据消息自行决定是否进行买卖操作
    """

    def __init__(self, name, log=None):
        super().__init__(name=name, mode='real', is_stock=True, is_futures=False, log=log)
        self.ex = ExchangeTDX()

        self.zx = zixuan.ZiXuan('a')

    # 做多买入
    def open_buy(self, code, opt: Operation, amount: float = None):
        tick = self.ex.ticks([code])
        if code not in tick.keys():
            return False

        stock = self.ex.stock_info(code)
        if stock is None:
            return False

        balance = 50000
        price = tick[code].last
        amount = balance / price
        amount = amount - amount % 100

        msg = f"股票买入 {code}-{stock['name']} 价格 {price} 数量 {amount} 原因 {opt.msg}"
        fun.send_dd_msg('a', msg)

        self.zx.add_stock('我的持仓', stock['code'], stock['name'])

        # 保存订单记录到 Redis 中，这样可以在图表中标识出买卖卖出的位置
        save_order = {
            'code': code,
            'name': stock['name'],
            'datetime': fun.datetime_to_str(datetime.datetime.now()),
            'type': 'buy',
            'price': price,
            'amount': amount,
            'info': opt.msg
        }
        rd.order_save('a', code, save_order)

        return {'price': price, 'amount': amount}

    # 做空卖出
    def open_sell(self, code, opt: Operation, amount: float = None):
        return False

    # 做多平仓
    def close_buy(self, code, pos: POSITION, opt):
        tick = self.ex.ticks([code])
        if code not in tick.keys():
            return False
        stock = self.ex.stock_info(code)
        if stock is None:
            return False

        price = tick[code].last
        msg = f"股票卖出 {code}-{stock['name']} 价格 {price} 数量 {pos.amount} 原因 {opt.msg}"
        fun.send_dd_msg('a', msg)

        self.zx.del_stock('我的持仓', stock['code'])

        # 保存订单记录到 Redis 中
        save_order = {
            'code': code,
            'name': stock['name'],
            'datetime': fun.datetime_to_str(datetime.datetime.now()),
            'type': 'sell',
            'price': price,
            'amount': pos.amount,
            'info': opt.msg
        }
        rd.order_save('a', code, save_order)

        return {'price': price, 'amount': pos.amount}

    # 做空平仓
    def close_sell(self, code, pos: POSITION, opt):
        return False
