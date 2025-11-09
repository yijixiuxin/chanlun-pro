import datetime
from typing import List, Union

from sqlalchemy.orm import sessionmaker

from chanlun.db_models.order import TableByOrder


class OrderDB:
    def __init__(self, session: sessionmaker):
        self.Session = session

    def order_save(
        self,
        market: str,
        stock_code: str,
        stock_name: str,
        order_type: str,
        order_price: float,
        order_amount: float,
        order_memo: str,
        order_time: Union[str, datetime.datetime],
    ):
        with self.Session() as session:
            # 保存订单
            order = TableByOrder(
                market=market,
                stock_code=stock_code,
                stock_name=stock_name,
                order_type=order_type,
                order_price=order_price,
                order_amount=order_amount,
                order_memo=order_memo,
                dt=order_time,
            )
            session.add(order)
            session.commit()

        return True

    def order_query_by_code(self, market: str, stock_code: str) -> List[TableByOrder]:
        with self.Session() as session:
            # 查询 market 下 stock_code 的所有订单
            orders = (
                session.query(TableByOrder)
                .filter(TableByOrder.market == market)
                .filter(TableByOrder.stock_code == stock_code)
                .all()
            )

        # {
        #     "code": "SH.000001",
        #     "datetime": "2021-10-19 10:09:51",
        #     "type": "buy", (允许的值：buy 买入 sell 卖出  open_long 开多  close_long 平多 open_short 开空 close_short 平空)
        #     "price": 205.8,
        #     "amount": 300.0,
        #     "info": "涨涨涨"
        # }
        return [  # 兼容之前的
            {
                "code": _o.stock_code,
                "name": _o.stock_name,
                "datetime": _o.dt,
                "type": _o.order_type,
                "price": _o.order_price,
                "amount": _o.order_amount,
                "info": _o.order_memo,
            }
            for _o in orders
        ]

    def order_clear_by_code(self, market: str, stock_code: str):
        with self.Session() as session:
            # 清除 market 下 stock_code 的所有订单
            session.query(TableByOrder).filter(TableByOrder.market == market).filter(
                TableByOrder.stock_code == stock_code
            ).delete()
            session.commit()

        return True