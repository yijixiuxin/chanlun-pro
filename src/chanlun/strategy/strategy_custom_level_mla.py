from typing import Dict, List, Union

from chanlun.backtesting.base import POSITION, MarketDatas, Operation, Strategy
from chanlun.backtesting.klines_generator import KlinesGenerator
from chanlun.cl_analyse import MultiLevelAnalyse


class StrategyCustomLevelMLA(Strategy):
    """
    通过低级别 K线，合成高级别K线，并用多级别分析，来判断高级别笔是否完成

    市场：期货
    周期：单周期（内部合成多周期）
    开仓策略：高级别笔出现买卖点，并多级别分析中，低级别出现盘整或趋势背驰
    平仓策略：开仓的反向笔，低级别分析出现盘整或趋势背驰，或者在收盘前进行平仓
    """

    def __init__(self, high_minutes=5):
        super().__init__()

        self.kg = KlinesGenerator(high_minutes, None, dt_align_type="bob")

    def open(
        self, code, market_data: MarketDatas, poss: Dict[str, POSITION]
    ) -> List[Operation]:
        """
        开仓监控，返回开仓配置
        """
        self.kg.cl_config = market_data.cl_config  # 使用回测配置中缠论配置项

        opts = []
        low_klines = market_data.klines(code, market_data.frequencys[0])
        high_data = self.kg.update_klines(low_klines)
        # 没有笔或中枢，退出
        if len(high_data.get_bis()) == 0:
            return opts

        # 笔没有完成，退出
        high_bi = high_data.get_bis()[-1]

        # 如果当前K线距离笔结束太远，退出
        if high_data.get_cl_klines()[-1].index - high_bi.end.k.index > 4:
            return opts

        # 当前笔没有背驰和买卖点，退出
        if len(high_bi.line_mmds()) and high_bi.bc_exists(["pz", "qs"]) is False:
            return opts

        # 买卖点、背驰对应的中枢，要回拉零轴
        for mmd in high_bi.mmds:
            if mmd.zs is not None and self.judge_macd_back_zero(high_data, mmd.zs) == 0:
                return opts
        for bc in high_bi.bcs:
            if (
                bc.bc
                and bc.type in ["pz", "qs"]
                and self.judge_macd_back_zero(high_data, bc.zs) == 0
            ):
                return opts

        # 多级别分析，低级别是否有盘整或趋势背驰
        low_data = market_data.get_cl_data(code, market_data.frequencys[0])
        mla = MultiLevelAnalyse(high_data, low_data)
        low_info = mla.low_level_qs(high_bi, "bi")
        if low_info.pz_bc is False and low_info.qs_bc is False:
            return opts

        # 判断低级别笔停顿
        if self.bi_td(low_info.last_line, low_data) is False:
            return opts

        # 收盘前不进行开仓
        # last_kline = low_data.get_klines()[-1]
        # if last_kline.date.hour in [14, 22] and last_kline.date.minute >= 55:
        #     return opts

        # 止损放在笔结束分型的顶底
        loss_price = high_bi.end.val

        for mmd in high_bi.line_mmds():
            opts.append(
                Operation(
                    code=code,
                    opt="buy",
                    mmd=mmd,
                    loss_price=loss_price,
                    info={},
                    msg=f"高级别出现买卖点 {mmd} 低级别趋势 PZBC {low_info.pz_bc} QSBC {low_info.qs_bc}",
                )
            )
            return opts
        for bc in high_bi.line_bcs():
            if bc not in ["pz", "qs"]:
                continue
            mmd = f'{high_bi.type}_{bc}_bc_{("buy" if high_bi.type == "down" else "sell")}'  # down_pz_bc_buy
            opts.append(
                Operation(
                    code=code,
                    opt="buy",
                    mmd=mmd,
                    loss_price=loss_price,
                    info={},
                    msg=f"高级别出现背驰 {bc} 低级别趋势 PZBC {low_info.pz_bc} QSBC {low_info.qs_bc}",
                )
            )
        return opts

    def close(
        self, code, mmd: str, pos: POSITION, market_data: MarketDatas
    ) -> Union[Operation, None]:
        """
        持仓监控，返回平仓配置

        持仓反向笔，低级别出现 盘整或趋势背驰退出
        """
        if pos.balance == 0:
            return None

        low_klines = market_data.klines(code, market_data.frequencys[0])
        high_data = self.kg.update_klines(low_klines)
        low_data = market_data.get_cl_data(code, market_data.frequencys[0])
        # 没有笔或中枢，退出
        if len(high_data.get_bis()) == 0:
            return None

        # 止损判断
        price = high_data.get_klines()[-1].c
        loss_opt = self.check_loss(mmd, pos, price)
        if loss_opt is not None:
            return loss_opt

        # 收盘前退出
        # last_kline = low_data.get_klines()[-1]
        # if last_kline.date.hour in [14, 22] and last_kline.date.minute >= 55:
        #     return Operation('sell', mmd, msg=f'收盘退出')

        high_bi = high_data.get_bis()[-1]

        # 低级别趋势 盘整或趋势背驰
        mla = MultiLevelAnalyse(high_data, low_data)
        low_info = mla.low_level_qs(high_bi, "bi")
        if low_info.pz_bc is False and low_info.qs_bc is False:
            return None

        # 低级别笔要停顿
        if self.bi_td(low_info.last_line, low_data) is False:
            return None

        if "buy" in mmd and high_bi.type == "up":
            return Operation(
                code,
                "sell",
                mmd,
                msg=f"高级别向上笔，低级别趋势 PZBC {low_info.pz_bc} QSBC {low_info.qs_bc}",
            )
        if "sell" in mmd and high_bi.type == "down":
            return Operation(
                code,
                "sell",
                mmd,
                msg=f"高级别向上笔，低级别趋势 PZBC {low_info.pz_bc} QSBC {low_info.qs_bc}",
            )

        return None
