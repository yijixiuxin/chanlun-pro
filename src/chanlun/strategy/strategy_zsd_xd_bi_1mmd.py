from typing import List, Union

from chanlun.backtesting.base import POSITION, Dict, MarketDatas, Operation, Strategy


class StrategyZsdXdBi1MMD(Strategy):
    """
    市场：任意
    周期：单周期

    通过递归，做走势段信号，根据低级别的一类买卖点进行

    在高级别出现买卖点或背驰，在设置的低级别中有出现过一类买卖点，则进行开仓
    平仓反过来即可
    """

    def __init__(self):
        super().__init__()

        self._max_loss_rate = None  # 最大亏损比例设置

    def open(
        self, code, market_data: MarketDatas, poss: Dict[str, POSITION]
    ) -> List[Operation]:
        """
        开仓监控，返回开仓配置
        """
        opts = []

        data = market_data.get_cl_data(code, market_data.frequencys[-1])
        # 没有笔或中枢，退出
        if len(data.get_zsds()) < 3:
            return opts
        zsd = data.get_zsds()[-1]
        xd = data.get_xds()[-1]
        bi = self.last_done_bi(data.get_bis())
        # 如果没有背驰和买卖点，直接返回
        if len(zsd.line_bcs()) == 0 and len(zsd.line_mmds()) == 0:
            return opts
        # 三个线的方向要一致
        if zsd.type != xd.type or zsd.type != bi.type:
            return opts
        if (
            xd.mmd_exists(["1buy", "1sell", "2buy", "2sell"]) is False
            and bi.mmd_exists(["1buy", "1sell", "2buy", "2sell"]) is False
        ):
            return opts
        # 最后笔要停顿
        if self.bi_td(bi, data) is False:
            return opts

        # 设置止损价格
        price = data.get_klines()[-1].c
        if self._max_loss_rate is not None:
            if bi.type == "up":
                loss_price = min(bi.high, price * (1 + self._max_loss_rate / 100))
            else:
                loss_price = max(bi.low, price * (1 - self._max_loss_rate / 100))
        else:
            loss_price = bi.low if bi.type == "down" else bi.high

        # 买卖点开仓
        for mmd in zsd.line_mmds():
            # 线段 or 笔 出现一类买卖点
            opts.append(
                Operation(
                    code,
                    "buy",
                    mmd,
                    loss_price,
                    {},
                    f"走势段买卖点 {mmd}, 线段买卖点 {xd.line_mmds()} 笔买卖点 {bi.line_mmds()}",
                )
            )
            return opts
        # 背驰开仓
        for bc in zsd.line_bcs():
            if bc == "zsd":
                bc = "pz"
            bc_mmd = f"{bi.type}_{bc}_bc_" + ("buy" if bi.type == "down" else "sell")
            opts.append(
                Operation(
                    code,
                    "buy",
                    bc_mmd,
                    loss_price,
                    {},
                    f"走势段背驰 {bc}, 线段买卖点 {xd.line_mmds()} 笔买卖点 {bi.line_mmds()}",
                )
            )
            return opts
        return opts

    def close(
        self, code, mmd: str, pos: POSITION, market_data: MarketDatas
    ) -> Union[Operation, None]:
        """
        持仓监控，返回平仓配置
        """
        if pos.balance == 0:
            return None

        data = market_data.get_cl_data(code, market_data.frequencys[-1])
        if len(data.get_zsds()) < 3:
            return False

        # 止损判断
        loss_opt = self.check_loss(mmd, pos, data.get_klines()[-1].c)
        if loss_opt is not None:
            return loss_opt

        zsd = data.get_zsds()[-1]
        xd = data.get_xds()[-1]
        bi = self.last_done_bi(data.get_bis())
        # 如果没有背驰和买卖点，直接返回
        if len(zsd.line_bcs()) == 0 and len(zsd.line_mmds()) == 0:
            return False
        # 三个线的方向要一致
        if zsd.type != xd.type or zsd.type != bi.type:
            return False
        # 如果低级别没有一类买卖点，退出
        if (
            xd.mmd_exists(["1buy", "1sell", "2buy", "2sell"]) is False
            and bi.mmd_exists(["1buy", "1sell", "2buy", "2sell"]) is False
        ):
            return False
        # 如果最后一笔没有停顿，退出
        if self.bi_td(bi, data) is False:
            return False

        if "buy" in mmd and zsd.type == "up":
            return Operation(
                code,
                "sell",
                mmd,
                msg=f"走势段买卖点 {zsd.line_mmds()} 背驰 {zsd.line_bcs()}，线段 {xd.line_mmds()} 笔 {bi.line_mmds()}",
            )
        if "sell" in mmd and zsd.type == "down":
            return Operation(
                code,
                "sell",
                mmd,
                msg=f"走势段买卖点 {zsd.line_mmds()} 背驰 {zsd.line_bcs()}，线段 {xd.line_mmds()} 笔 {bi.line_mmds()}",
            )

        return None
