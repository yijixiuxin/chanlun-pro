from chanlun.backtesting.base import *
from chanlun.cl_analyse import *
from chanlun import fun


class StrategyFuturesXDMMD(Strategy):
    """
    线段买卖点
    """

    def __init__(self):
        super().__init__()

        self._max_loss_rate = 3

    def open(self, code, market_data: MarketDatas, poss: Dict[str, POSITION]) -> List[Operation]:
        opts = []

        high_data = market_data.get_cl_data(code, market_data.frequencys[0])
        if len(high_data.get_xds()) == 0:
            return opts

        high_xd = high_data.get_xds()[-1]
        if len(high_xd.tzxls) == 1:
            high_xd = high_data.get_xds()[-2]
        high_bi = self.last_done_bi(high_data.get_bis())
        # 如果线段没有买卖点，则退出
        if len(high_xd.line_mmds()) == 0:
            return opts

        if high_data.get_klines()[-1].date >= fun.str_to_datetime('2022-06-13 09:30:00'):
            a = 1

        # 高级别最后一笔停顿
        # if high_xd.type != high_bi.type:
        #     return opts
        # if self.bi_td(high_bi, high_data) is False:
        #     return opts

        low_data = market_data.get_cl_data(code, market_data.frequencys[1])
        low_bi = self.last_done_bi(low_data.get_bis())
        if self.bi_td(low_bi, low_data) is False:
            return opts

        # 低级别分析，获取高级别线段中，包含的低级别线段
        mla = MultiLevelAnalyse(high_data, low_data)
        mla_info = mla.low_level_qs(high_xd, 'xd')
        # 如果低级别包含的线段少于3段，不操作
        if len(mla_info.lines) < 3:
            return opts

        # 根据线的数量重组中枢判断背驰
        lfa = LinesFormAnalyse(low_data)
        low_xd_bc = False
        form = None
        for n in [11, 9, 7, 5, 3]:
            if low_xd_bc is False and len(mla_info.lines) >= n:
                form = lfa.lines_analyse(n, mla_info.lines[-n:])
                if form is not None and form.form_type in ['三笔形态', '类趋势', '盘整', '趋势'] and form.is_bc_line:
                    low_xd_bc = True

        if low_xd_bc is False:
            return opts

        # 记录开仓时信息
        info = {
            'low_form_line_num': form.line_num,
            'low_form_type': form.form_type,
            'low_form_level': form.form_level,
            'low_form_qs': form.form_qs,
            'high_xd_start_date': high_xd.start.k.date,
        }

        for mmd in high_xd.get_mmds():
            # 根据 ATR 波动率 获取止损价格
            if 'buy' in mmd.name:
                stop_loss_price = self.get_atr_stop_loss_price(high_data, 'buy')
            else:
                stop_loss_price = self.get_atr_stop_loss_price(high_data, 'sell')

            # 买入向上，低级别笔要出现三买，卖出向下，低级别笔要出现三卖
            if 'buy' in mmd.name and low_bi.mmd_exists(['3buy'], '|') is False:
                continue
            elif 'sell' in mmd.name and low_bi.mmd_exists(['3sell'], '|') is False:
                continue

            opts.append(Operation(
                opt='buy', mmd=mmd.name, loss_price=stop_loss_price, info=info,
                msg=f'线段买卖点：{mmd.name}'
            ))

        return opts

    def close(self, code, mmd: str, pos: POSITION, market_data: MarketDatas) -> [Operation, None, List[Operation]]:
        if pos.balance == 0:
            return False

        high_data = market_data.get_cl_data(code, market_data.frequencys[0])
        low_data = market_data.get_cl_data(code, market_data.frequencys[1])
        price = low_data.get_klines()[-1].c

        # 止盈止损检查
        loss_opt = self.check_loss(mmd, pos, price)
        if loss_opt is not None:
            return loss_opt

        # ATR 移动止损
        # loss_opt = self.check_atr_stop_loss(high_data, pos)
        # if loss_opt is not None:
        #     return loss_opt

        opts = []
        if len(high_data.get_xds()) == 0:
            return False

        # 平仓条件，高级笔反向线段，并且低级别形态形成背驰并完成
        high_xd = high_data.get_xds()[-1]
        if len(high_xd.tzxls) == 1:
            high_xd = high_data.get_xds()[-2]
        high_bi = self.last_done_bi(high_data.get_bis())
        low_bi = self.last_done_bi(low_data.get_bis())

        if self.bi_td(low_bi, low_data) is False:
            return opts

        # if high_xd.type != high_bi.type:
        #     return opts
        #
        # if self.bi_td(high_bi, high_data) is False:
        #     return opts

        mla = MultiLevelAnalyse(high_data, low_data)
        mla_info = mla.low_level_qs(high_xd, 'xd')
        if mla_info.line_num < 3:
            return opts
        lfa = LinesFormAnalyse(low_data)
        low_xd_bc = False
        for n in [11, 9, 7, 5, 3]:
            if low_xd_bc is False and len(mla_info.lines) >= n:
                form = lfa.lines_analyse(n, mla_info.lines[-n:])
                if form is not None and form.form_type in ['三笔形态', '类趋势', '盘整', '趋势'] and form.is_bc_line:
                    low_xd_bc = True

        if low_xd_bc is False:
            return opts

        if 'buy' in mmd and high_xd.type == 'up' and low_bi.mmd_exists(['3sell'], '|'):
            opts.append(Operation(
                opt='sell', mmd=mmd, msg=f"高级别线段反向线段，低级别判断趋势完成，并且出现笔三卖"
            ))
        if 'sell' in mmd and high_xd.type == 'down' and low_bi.mmd_exists(['3buy'], '|'):
            opts.append(Operation(
                opt='sell', mmd=mmd, msg=f"高级别线段反向线段，低级别判断趋势完成，并且出现笔三买"
            ))
        return opts
