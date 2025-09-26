from typing import Dict, List, Union

from chanlun.backtesting.base import POSITION, MarketDatas, Operation, Strategy


class StrategyMultipleZsMMDS(Strategy):
    """
    多个中枢类型，并同时出现买卖点的策略

    多类型中枢计算的策略 demo

    周期：单周期
    开仓策略：所计算的中枢同时出现买卖点
    平仓策略：所计算的中枢中，只要有出现买卖点或背驰就退出
    """

    def __init__(self):
        super().__init__()

    def open(
        self, code, market_data: MarketDatas, poss: Dict[str, POSITION]
    ) -> List[Operation]:
        """
        开仓监控，返回开仓配置
        """
        opts = []

        high_data = market_data.get_cl_data(code, market_data.frequencys[0])
        # 没有笔或中枢，退出
        if len(high_data.get_bis()) == 0 or len(high_data.get_bi_zss()) == 0:
            return opts

        # 笔没有完成，退出
        high_bi = self.last_done_bi(high_data.get_bis())

        # 当前笔所有配置中枢出现的买点交集，如果没有买卖点，退出
        mmds = high_bi.line_mmds("&")
        if len(mmds) == 0:
            return opts

        # 笔没有停顿，退出
        if self.bi_td(high_bi, high_data) is False:
            return opts

        # 增加条件，买卖点对应的中枢，需要回拉零轴
        for zs_type, mmds in high_bi.zs_type_mmds.items():
            for mmd in mmds:
                if self.judge_macd_back_zero(high_data, mmd.zs) == 0:
                    return opts

        # 止损放在笔结束分型的顶底
        loss_price = high_bi.end.val

        if high_bi.mmd_exists(["1buy", "2buy", "3buy", "l3buy"], "&"):
            opts.append(
                Operation(
                    code=code,
                    opt="buy",
                    mmd=high_bi.line_mmds("&")[0],
                    loss_price=loss_price,
                    info={},
                    msg=f'高级别笔出现买卖点 {high_bi.line_mmds("&")}',
                )
            )
            return opts
        elif high_bi.mmd_exists(["1sell", "2sell", "3sell", "l3sell"], "&"):
            opts.append(
                Operation(
                    code=code,
                    opt="buy",
                    mmd=high_bi.line_mmds("&")[0],
                    loss_price=loss_price,
                    info={},
                    msg=f'高级别笔出现买卖点 {high_bi.line_mmds("&")}',
                )
            )
            return opts

        return opts

    def close(
        self, code, mmd: str, pos: POSITION, market_data: MarketDatas
    ) -> Union[Operation, None]:
        """
        持仓监控，返回平仓配置

        所计算的中枢中，只要有出现买卖点或背驰就退出
        """
        if pos.balance == 0:
            return None

        high_data = market_data.get_cl_data(code, market_data.frequencys[0])
        if len(high_data.get_bis()) == 0:
            return None

        # 止损判断
        price = high_data.get_klines()[-1].c
        loss_opt = self.check_loss(mmd, pos, price)
        if loss_opt is not None:
            return loss_opt

        high_bi = self.last_done_bi(high_data.get_bis())
        if (
            "buy" in mmd
            and (
                high_bi.mmd_exists(["1sell", "2sell", "3sell", "l3sell"], "|")
                or (high_bi.type == "up" and high_bi.bc_exists(["bi", "pz", "qs"], "|"))
            )
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(
                code,
                "sell",
                mmd,
                msg=f'高级别笔出现 卖点 {high_bi.line_mmds("|")} 或 背驰 {high_bi.line_bcs("|")}',
            )
        elif (
            "sell" in mmd
            and (
                high_bi.mmd_exists(["1buy", "2buy", "3buy", "l3buy"], "|")
                or (
                    high_bi.type == "down"
                    and high_bi.bc_exists(["bi", "pz", "qs"], "|")
                )
            )
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(
                code,
                "sell",
                mmd,
                msg=f'高级别笔出现 买点 {high_bi.line_mmds("|")} 或 背驰 {high_bi.line_bcs("|")}',
            )

        return None
