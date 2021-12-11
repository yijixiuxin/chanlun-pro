from typing import Dict, List

from cl_v2 import cl
from cl_v2 import strategy
from cl_v2 import trader


class Strategy_Demo(strategy.Strategy):
    """
    策略 demo 示例
    实现高级别出现买卖点，并且低级别出现一类买卖点进行买卖操作
    """

    def __init__(self):
        self.profit_rate_return = 5  # 百分比
        self._cache = {}
        self._opts = {}

    def look(self, cl_datas: List[cl.CL]) -> List:
        opts = []

        data_high = cl_datas[0]

        price = data_high.klines[-1].c

        bi_high = data_high.bis[-1]

        for mmd in bi_high.mmds:
            if 'buy' in mmd and bi_high.done and bi_high.td:
                opts.append({
                    'opt': 'buy', 'mmd': mmd, 'loss_price': bi_high.low,
                    'info': {
                        'fx_datetime': bi_high.start.k.date,
                        'cl_datas': {'bi_high': bi_high, 'price': price},
                    },
                    'msg': '高级别出现 %s (TD: %s Done: %s)' % (mmd, bi_high.td, bi_high.done),
                })
            if 'sell' in mmd and bi_high.done and bi_high.td:
                opts.append({
                    'opt': 'buy', 'mmd': mmd, 'loss_price': bi_high.high,
                    'info': {
                        'fx_datetime': bi_high.start.k.date,
                        'cl_datas': {'bi_high': bi_high, 'price': price},
                    },
                    'msg': '高级别出现 %s (TD: %s Done: %s)' % (mmd, bi_high.td, bi_high.done),
                })

        return opts

    def stare(self, mmd: str, pos: trader.POSITION, cl_datas: List[cl.CL]) -> [Dict, None]:
        if pos.balance == 0:
            return None

        data_high = cl_datas[0]

        price = data_high.klines[-1].c

        bi_high = data_high.bis[-1]

        # 止盈止损检查
        if 'buy' in mmd:
            if pos.max_profit_rate > self.profit_rate_return:
                pos.loss_price = pos.price * 1.01

            if price < pos.loss_price:
                return {'opt': 'sell', 'mmd': mmd, 'msg': '止损'}
        elif 'sell' in mmd:
            if pos.max_profit_rate > self.profit_rate_return:
                pos.loss_price = pos.price * 0.99

            if price > pos.loss_price:
                return {'opt': 'sell', 'mmd': mmd, 'msg': '止损'}

        if 'buy' in mmd:
            # 做多，检查什么时候卖出
            if bi_high.type == 'up' and bi_high.done and bi_high.td:
                return {'opt': 'sell', 'mmd': mmd, 'msg': '高级别笔（TD: %s Done: %s），多仓清仓' % (bi_high.td, bi_high.td)}

        if 'sell' in mmd:
            # 做空，检查什么时候买入
            if bi_high.type == 'down' and bi_high.done and bi_high.td:
                return {'opt': 'sell', 'mmd': mmd, 'msg': '高级别笔（TD: %s Done: %s），多仓清仓' % (bi_high.td, bi_high.td)}

        return None
