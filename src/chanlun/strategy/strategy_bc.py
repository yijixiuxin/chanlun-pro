from typing import Dict, List, Union

from chanlun.backtesting.base import POSITION, MarketDatas, Operation, Strategy


class StrategyBc(Strategy):
    """
    笔和段 双级别背驰 策略
    单周期
    """

    def __init__(self):
        super().__init__()

        self._max_loss_rate = 10  # 最大亏损比例设置

    def open(
        self, code, market_data: MarketDatas, poss: Dict[str, POSITION]
    ) -> List[Operation]:
        """
        开仓监控，返回开仓配置
        """
        opts = []

        data = market_data.get_cl_data(code, market_data.frequencys[0])
        # 没有笔或中枢，退出
        if (
            len(data.get_bis()) == 0
            or len(data.get_bi_zss()) == 0
            or len(data.get_xds()) == 0
            or len(data.get_xd_zss()) == 0
        ):
            return opts

        xd = data.get_xds()[-1]
        bi = data.get_bis()[-1]
        if bi.is_done() is False:
            bi = data.get_bis()[-2]

        price = data.get_klines()[-1].c

        if (
            xd.type == bi.type
            and xd.end.index == bi.end.index
            and xd.bc_exists(["pz", "qs"])
            and bi.bc_exists(["pz", "qs"])
            and self.bi_td(bi, data)
        ):

            if xd.type == "up" and xd.bc_exists(["pz"]):
                mmd = "up_pz_bc_sell"
            elif xd.type == "up" and xd.bc_exists(["qs"]):
                mmd = "up_qs_bc_sell"
            elif xd.type == "down" and xd.bc_exists(["pz"]):
                mmd = "down_pz_bc_buy"
            elif xd.type == "down" and xd.bc_exists(["qs"]):
                mmd = "down_qs_bc_buy"
            else:
                return opts

            if self._max_loss_rate is not None:
                if "buy" in mmd:
                    loss_price = price - (price * (abs(self._max_loss_rate) / 100))
                    loss_price = max(loss_price, bi.low)
                else:
                    loss_price = price + (price * (abs(self._max_loss_rate) / 100))
                    loss_price = min(loss_price, bi.high)
            else:
                if "buy" in mmd:
                    loss_price = bi.low
                else:
                    loss_price = bi.high

            opts.append(
                Operation(
                    code=code,
                    opt="buy",
                    mmd=mmd,
                    loss_price=loss_price,
                    msg="线段背驰 %s 笔背驰 %s 止损价格 %s"
                    % (xd.line_bcs(), bi.line_bcs(), loss_price),
                    info={
                        "fx_datetime": bi.end.k.date,
                        "cl_datas": {
                            "xd": xd,
                            "bi": bi,
                            "price": price,
                        },
                    },
                )
            )

        return opts

    def close(
        self, code, mmd: str, pos: POSITION, market_data: MarketDatas
    ) -> Union[Operation, None]:
        """
        持仓监控，返回平仓配置
        """
        if pos.balance == 0:
            return None

        data = market_data.get_cl_data(code, market_data.frequencys[0])
        price = data.get_klines()[-1].c

        # 止盈止损检查
        if "buy" in mmd:
            if price < pos.loss_price:
                return Operation(code, "sell", mmd, msg="%s 止损" % mmd)
        elif "sell" in mmd:
            if price > pos.loss_price:
                return Operation(code, "sell", mmd, msg="%s 止损" % mmd)

        xd = data.get_xds()[-1]
        bi = data.get_bis()[-1]
        if bi.is_done() is False:
            bi = data.get_bis()[-2]

        if "buy" in mmd and xd.type == "up":
            # 买入做多，检查卖点
            if (
                xd.type == bi.type
                and xd.end.index == bi.end.index
                and (xd.bc_exists(["pz", "xd", "qs"]) or bi.bc_exists(["pz", "qs"]))
                and self.bi_td(bi, data)
            ):
                return Operation(
                    code=code,
                    opt="sell",
                    mmd=mmd,
                    msg="%s 线段背驰 %s 笔背驰 %s，多仓清仓"
                    % (mmd, xd.line_bcs(), bi.line_bcs()),
                )
        if "sell" in mmd and xd.type == "down":
            # 卖出做空，检查买点
            if (
                xd.type == bi.type
                and xd.end.index == bi.end.index
                and (xd.bc_exists(["pz", "xd", "qs"]) or bi.bc_exists(["pz", "qs"]))
                and self.bi_td(bi, data)
            ):
                return Operation(
                    code=code,
                    opt="sell",
                    mmd=mmd,
                    msg="%s 线段背驰 %s 笔背驰 %s，空仓清仓"
                    % (mmd, xd.line_bcs(), bi.line_bcs()),
                )

        return None
