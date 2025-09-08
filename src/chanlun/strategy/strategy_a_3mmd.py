from typing import Dict, List, Union

from chanlun.backtesting.base import POSITION, MarketDatas, Operation, Strategy
from chanlun.cl_analyse import MultiLevelAnalyse
from chanlun.cl_interface import Config
from chanlun.cl_utils import cal_zs_macd_infos


class StrategyA3mmd(Strategy):
    """
    https://zhuanlan.zhihu.com/p/499188628
    根据以上文章，写的当前策略
    多周期（高低两个）策略，例如 [d, 30m]
    只做三类买卖点
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

        high_data = market_data.get_cl_data(code, market_data.frequencys[0])
        # 没有笔或中枢，退出
        if (
            len(high_data.get_bis()) == 0
            or len(high_data.get_bi_zss()) < 2
            or len(high_data.get_xds()) == 0
        ):
            return opts

        # 第一个条件：中枢要求1，第一个中枢
        # 根据中枢类型的不同，判断是否第一个中枢的方法也不同
        high_config = high_data.get_config()
        if Config.ZS_TYPE_DN.value in high_config["zs_bi_type"]:
            # 段内中枢（类同级别分解，和同级别分解还不太一样），判断段内是否只有一个中枢
            high_xd = high_data.get_xds()[-1]
            high_xd_bi_zss = [
                _zs
                for _zs in high_data.get_bi_zss()
                if _zs.start.index > high_xd.start.index
            ]
            if len(high_xd_bi_zss) != 1:
                return opts
        elif Config.ZS_TYPE_BZ.value in high_config["zs_bi_type"]:
            # 标准中枢，中枢延伸的做法，判断只要不是连续两个同向的中枢即可
            high_bi_zs_1 = high_data.get_bi_zss()[-1]
            high_bi_zs_2 = high_data.get_bi_zss()[-2]
            if high_bi_zs_2.lines[1].type == high_bi_zs_1.lines[1].type:
                return opts
        else:
            raise Exception("缠论配置，笔中枢类型错误")

        high_zs = high_data.get_bi_zss()[-1]

        # 第二个条件：中枢要求2，对称中枢
        # 通过中枢震荡来判断是否对称，振幅大于 50 就算对称中枢，#(把方向去掉) 并且中枢要有明确的方向性
        # 中枢振幅宽松点，设置成 35
        if high_zs.zf() < 35:  # or high_zs.type == 'zd':
            return opts

        # 第三个条件：中枢要求3，级别的定位
        # 根据中枢内笔数来计算中枢级别（算上中枢前后一笔，中枢内所有笔要小于等于 7 笔）
        if len(high_zs.lines) > 7:
            return opts
        # 还要判断黄白线（dif、dea）是否回抽零轴，这里使用 dif 白线 回抽零轴判断，或者两次金叉或死叉来宽松判断
        zs_macd_infos = cal_zs_macd_infos(high_zs, high_data)
        # TODO 是否粘在一起这个不好判断了，就不考虑了
        if (
            zs_macd_infos.dif_down_cross_num > 0 or zs_macd_infos.dif_up_cross_num > 0
        ) or (zs_macd_infos.die_cross_num >= 2 or zs_macd_infos.gold_cross_num >= 2):
            pass
        else:
            return opts

        high_bi = self.last_done_bi(high_data.get_bis())
        price = high_data.get_klines()[-1].c

        # 自己加的：之前反向笔、段不能有背驰
        high_up_bi = high_data.get_bis()[high_bi.index - 1]
        high_xd = high_data.get_xds()[-1]
        if high_up_bi.bc_exists(["bi", "pz", "qs"]) or (
            high_xd.type != high_bi.type and high_xd.bc_exists(["xd", "pz", "qs"])
        ):
            return opts

        # 止损点放在 分型第三根K线的 高低点
        if self._max_loss_rate is not None:
            if high_bi.type == "down":
                loss_price = price - (price * (abs(self._max_loss_rate) / 100))
                loss_price = max(loss_price, high_bi.end.klines[-1].l)
            else:
                loss_price = price + (price * (abs(self._max_loss_rate) / 100))
                loss_price = min(loss_price, high_bi.end.klines[-1].h)
        else:
            if high_bi.type == "down":
                loss_price = high_bi.end.klines[-1].l
            else:
                loss_price = high_bi.end.klines[-1].h

        if high_bi.mmd_exists(["3buy", "3sell"]):
            # 买入条件：针对本级别中枢的本级别3买卖点
            # 自己增加一个低级别背驰并且高级别停顿的买入条件
            mla = MultiLevelAnalyse(
                high_data, market_data.get_cl_data(code, market_data.frequencys[1])
            )
            low_qs = mla.low_level_qs(high_bi, "bi")
            for mmd in high_bi.line_mmds():
                btd = self.bi_qiang_td(high_bi, high_data)
                yzfx = self.bi_yanzhen_fx(high_bi, high_data)
                low_bc = (low_qs.pz_bc or low_qs.qs_bc) and self.bi_td(
                    high_bi, high_data
                )
                if btd or yzfx or low_bc:
                    opts.append(
                        Operation(
                            code=code,
                            opt="buy",
                            mmd=mmd,
                            loss_price=loss_price,
                            info={
                                "high_bi": high_bi,
                                "high_zs": high_zs,
                            },
                            msg="买入条件：本级别买点（%s 笔停顿 %s 验证分型 %s 低级别背驰 %s）,止损价格 %s"
                            % (mmd, btd, yzfx, low_bc, loss_price),
                        )
                    )
        elif high_zs.done is False:
            # 买入条件：针对本级别中枢的次级别3买卖点
            # 买入条件：针对本级别中枢的b-A

            # 中枢未完成，并且最后一笔突破了中枢高低点，但是还没有回调，这时候根据方向分型的强度，在没有回调到中枢内部进行入场
            if (high_bi.type == "up" and high_zs.gg > high_bi.high) or (
                high_bi.type == "down" and high_zs.dd < high_bi.low
            ):
                return opts

            # 查找所有强势的分型，并且和笔开始的分型类型一致，这样就跟笔的方向相反了
            # TODO 强势的分型和力度有些区别，力度=2条件比较苛刻，不过苛刻点也好
            high_max_fxs = [
                _fx
                for _fx in high_data.get_fxs()
                if (
                    _fx.done
                    and _fx.index > high_bi.start.index
                    and _fx.type == high_bi.start.type
                    and _fx.ld() >= 2
                )
            ]
            if len(high_max_fxs) == 0:
                return opts

            mmd = None
            # 两个买入条件的有些重复了，这里的止损就没有参考最大止损设置里
            # 这里定义为 类3买卖点，与本级别的区分开 TODO 这里还是需要优化
            if (
                high_bi.type == "up"
                and high_max_fxs[-1].val > high_zs.zg
                and price
                > high_max_fxs[-1].high(
                    high_data.get_config()["fx_qj"], high_data.get_config()["fx_qy"]
                )
            ):
                mmd = "l3buy"
                loss_price = high_max_fxs[-1].klines[-1].l
            elif (
                high_bi.type == "down"
                and high_max_fxs[-1].val < high_zs.zd
                and price
                < high_max_fxs[-1].low(
                    high_data.get_config()["fx_qj"], high_data.get_config()["fx_qy"]
                )
            ):
                mmd = "l3sell"
                loss_price = high_max_fxs[-1].klines[-1].h
            if mmd:
                opts.append(
                    Operation(
                        code=code,
                        opt="buy",
                        mmd=mmd,
                        loss_price=loss_price,
                        info={"high_bi": high_bi, "high_zs": high_zs},
                        msg="买入条件：次级别强势反向分型买点（%s）,止损价格 %s"
                        % (mmd, loss_price),
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

        high_data = market_data.get_cl_data(code, market_data.frequencys[0])
        price = high_data.get_klines()[-1].c
        loss_opt = self.check_loss(mmd, pos, price)
        if loss_opt:
            return loss_opt

        low_data = market_data.get_cl_data(code, market_data.frequencys[1])

        high_bi = self.last_done_bi(high_data.get_bis())
        low_bi = self.last_done_bi(low_data.get_bis())

        # 这里的均线根据缠论配置中设置的来
        idx_ma = self.idx_ma(high_data, 5)[-1]

        # 卖出条件：05均线卖出 05均线对应的关键点——走势加速
        # 均线角度变化计算比较复杂，使用笔的角度来判断，因为没有考虑到加速那一段，所以角度设定为 50
        if (
            "buy" in mmd
            and high_bi.type == "up"
            and high_bi.is_done()
            and abs(high_bi.jiaodu()) > 50
            and price < idx_ma
        ):
            return Operation(
                code=code, opt="sell", mmd=mmd, msg="笔角度大于50并且当前价格低于均线"
            )
        if (
            "sell" in mmd
            and high_bi.type == "down"
            and high_bi.is_done()
            and abs(high_bi.jiaodu()) > 50
            and price > idx_ma
        ):
            return Operation(
                code=code, opt="sell", mmd=mmd, msg="笔角度大于50并且当前价格高于均线"
            )

        # 卖出条件：标准走势卖出 标准走势对应的关键点——次级别趋势背驰和反转分型
        # 卖出条件：次级别盘整背驰卖出
        # 以上两个可以总结为 次级别 盘整与趋势背驰卖出，线上笔 done，标识高级别也出现了顶底分型
        # 避免被小级别骗出去，在加一个高级别的笔停顿条件
        mla = MultiLevelAnalyse(high_data, low_data)
        low_qs = mla.low_level_qs(high_bi, "bi")
        if (
            "buy" in mmd
            and high_bi.type == "up"
            and self.bi_td(high_bi, high_data)
            and (low_qs.pz_bc or low_qs.qs_bc)
            and low_bi.td
        ):
            return Operation(
                code=code,
                opt="sell",
                mmd=mmd,
                msg="次级别背驰 %s" % ([low_qs.pz_bc, low_qs.qs_bc]),
            )

        if (
            "sell" in mmd
            and high_bi.type == "down"
            and self.bi_td(high_bi, high_data)
            and (low_qs.pz_bc or low_qs.qs_bc)
            and low_bi.td
        ):
            return Operation(
                code=code,
                opt="sell",
                mmd=mmd,
                msg="次级别背驰 %s" % ([low_qs.pz_bc, low_qs.qs_bc]),
            )

        # 卖出条件：根据趋势背驰延伸出来的不标准走势之小转大
        # 这里简单的用高级别的验证分型来平仓
        if (
            "buy" in mmd
            and high_bi.type == "up"
            and high_bi.is_done()
            and self.bi_yanzhen_fx(high_bi, high_data)
        ):
            return Operation(code, "sell", mmd, msg="高级别验证分型平仓")
        if (
            "sell" in mmd
            and high_bi.type == "down"
            and high_bi.is_done()
            and self.bi_yanzhen_fx(high_bi, high_data)
        ):
            return Operation(code, "sell", mmd, msg="高级别验证分型平仓")

        # 卖出条件：次级别5段式背驰+破位05均线卖出
        # TODO 待实现，次级别背驰级别满足这个条件了

        # TODO 个人回看图表，考虑新增的平仓条件
        # 高级别笔背驰
        if (
            "buy" in mmd
            and high_bi.type == "up"
            and self.bi_td(high_bi, high_data)
            and high_bi.bc_exists(["bi", "pz", "qs"])
        ):
            return Operation(
                code, "sell", mmd, msg="高级别笔背驰（%s）" % high_bi.line_bcs()
            )
        if (
            "sell" in mmd
            and high_bi.type == "down"
            and self.bi_td(high_bi, high_data)
            and high_bi.bc_exists(["bi", "pz", "qs"])
        ):
            return Operation(
                code, "sell", mmd, msg="高级别笔背驰（%s）" % high_bi.line_bcs()
            )

        # 低级别笔出现一二类买卖点
        if (
            "buy" in mmd
            and low_bi.mmd_exists(["1sell", "2sell"])
            and high_bi.type == "up"
            and high_bi.is_done()
        ):
            return Operation(
                code, "sell", mmd, msg="低级别笔卖点（%s）" % low_bi.line_mmds()
            )
        if (
            "sell" in mmd
            and low_bi.mmd_exists(["1buy", "2buy"])
            and high_bi.type == "down"
            and high_bi.is_done()
        ):
            return Operation(
                code, "sell", mmd, msg="低级别笔买点（%s）" % low_bi.line_mmds()
            )

        return None
