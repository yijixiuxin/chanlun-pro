from typing import Dict, List, Union

from chanlun.backtesting.backtest import BackTest
from chanlun.backtesting.base import POSITION, MarketDatas, Operation, Strategy
from chanlun.cl_interface import ICL


class StrategyAXDTradeModel(Strategy):
    """
    市场：股票市场
    周期：多周期（推荐 d/30m/5m）

    线段交易模式，搏一搏线段的转折

    """

    def __init__(self):
        super().__init__()

        self._run_codes = []
        self._date_day_key = 0
        self._trade_infos = {
            "SHSE.60": {
                "code": "SHSE.000001",
                "name": "上证指数",
                "trade": True,
                "trade_days": [],
            },
            "SHSE.68": {
                "code": "SHSE.000688",
                "name": "科创板块",
                "trade": True,
                "trade_days": [],
            },
            "SZSE.00": {
                "code": "SZSE.399001",
                "name": "深证成指",
                "trade": True,
                "trade_days": [],
            },
            "SZSE.30": {
                "code": "SZSE.399006",
                "name": "创业板指",
                "trade": True,
                "trade_days": [],
            },
        }

        self._index_cds: Dict[str, ICL] = {}

        self._max_loss_rate = 10

    def on_bt_loop_start(self, bt: BackTest):
        """
        判断大盘，是否可以进行交易
        筛选有线段买卖点和背驰的代码
        """
        # 每天执行一次
        if bt.datas.now_date.day == self._date_day_key:
            return True
        self._date_day_key = bt.datas.now_date.day

        # 获取大盘的日线数据
        # for code_key, info in self._trade_infos.items():
        #     # 获取周线的数据，并计算，根据周线笔来筛选是否可交易
        #     index_w_klines = bt.datas.klines(info['code'], bt.frequencys[0])
        #     index_w_klines = convert_stock_kline_frequency(index_w_klines, 'w')
        #     if len(index_w_klines) == 0:
        #         continue
        #     if info['code'] in self._index_cds.keys():
        #         self._index_cds[info['code']].process_klines(index_w_klines)
        #     else:
        #         self._index_cds[info['code']] = cl.CL(info['code'], 'w', bt.cl_config).process_klines(index_w_klines)
        #     index_w_cd = self._index_cds[info['code']]
        #     # index_30m_cd = bt.datas.get_cl_data(info['code'], bt.frequencys[1])
        #     if len(index_w_cd.get_bis()) == 0:
        #         continue
        #     index_bi = index_w_cd.get_bis()[-1]
        #     if (index_bi.type == 'down' and index_bi.is_done()) or (
        #             index_bi.type == 'up' and index_bi.is_done() is False):
        #         info['trade'] = True
        #         info['trade_days'].append(fun.datetime_to_str(bt.datas.now_date, '%Y-%m-%d'))
        #     else:
        #         info['trade'] = False

        # 循环获取所有代码数据，初步判断代码是否可以进行交易
        self._run_codes = []
        for code in bt.codes:
            if self._trade_infos[code[:7]]["trade"] is False:
                continue
            cd = bt.datas.get_cl_data(code, bt.frequencys[0], bt.cl_config)
            if len(cd.get_xds()) > 0:
                xd_zss = cd.get_xd_zss()
                xd = cd.get_xds()[-1]
                if xd.type == "down" and (
                    xd.mmd_exists(["1buy", "2buy", "3buy"]) or xd.bc_exists(["pz"])
                ):
                    if xd.mmd_exists(["3buy"]) and len(xd_zss) < 2:
                        continue
                    # if xd.type == 'down' and xd.bc_exists(['pz']):
                    self._run_codes.append(code)
        # 将持仓中的代码加入进去
        self._run_codes += bt.trader.position_codes()
        # print('运行代码：', self._trade_infos)
        return True

    def open(
        self, code, market_data: MarketDatas, poss: Dict[str, POSITION]
    ) -> List[Operation]:
        opts = []

        if code not in self._run_codes:
            return opts

        cd_day = market_data.get_cl_data(code, market_data.frequencys[0])

        if len(cd_day.get_xds()) == 0:
            return opts
        xd_day = cd_day.get_xds()[-1]
        # 过滤不符合要求的线段
        if xd_day.type == "up":
            return opts
        if len(xd_day.line_mmds()) == 0 and len(xd_day.line_bcs()) == 0:
            return opts

        cd_30m = market_data.get_cl_data(code, market_data.frequencys[1])
        cd_5m = market_data.get_cl_data(code, market_data.frequencys[2])

        if len(cd_30m.get_bis()) == 0 or len(cd_5m.get_bis()) == 0:
            return opts

        price = cd_5m.get_klines()[-1].c

        # 日线笔要下跌情况，并且笔要完成
        bi_day = cd_day.get_bis()[-1]
        if (
            bi_day.type == "down"
            and bi_day.is_done()
            and price > bi_day.end.klines[-1].h
        ):
            pass
        else:
            return opts

        # 笔距离线段不能太远，太远可能随时结束
        if bi_day.index - xd_day.end_line.index > 4:
            return opts

        # 日线的笔要基本符合基本，30m一段 或者 5m五段
        xds_30m = [
            _xd
            for _xd in cd_30m.get_xds()
            if _xd.start.k.date >= bi_day.start.k.date
            and _xd.end.k.date <= bi_day.end.k.klines[-1].date
        ]
        xds_5m = [
            _xd
            for _xd in cd_5m.get_xds()
            if _xd.start.k.date >= bi_day.start.k.date
            and _xd.end.k.date <= bi_day.end.k.klines[-1].date
        ]
        if len(xds_30m) >= 1 or len(xds_5m) >= 5:
            pass
        else:
            return opts

        xds_30m = [
            _xd for _xd in cd_30m.get_xds() if _xd.start.k.date >= xd_day.start.k.date
        ]
        if len(xds_30m) < 3:
            # 30m 线段不满足条件
            return opts

        # 检查 5M 情况，第一次3买，或第一次一买，进行买入
        is_ok_5m = False
        low_level_5m_msg = ""

        # 确定只做第一个三买
        if is_ok_5m is False:
            bis_5m_3buy = [
                _bi
                for _bi in cd_5m.get_bis()
                if _bi.start.k.date >= bi_day.end.k.date
                and _bi.mmd_exists(["3buy"], "|")
            ]
            if len(bis_5m_3buy) == 1:
                is_ok_5m = True
                low_level_5m_msg = "5m 三买点"

        # 确定做第一个一买，后续笔不创新低
        if is_ok_5m is False:
            bis_5m_1buy = [
                _bi
                for _bi in cd_5m.get_bis()
                if _bi.start.k.date >= bi_day.end.k.date
                and _bi.mmd_exists(["1buy"], "|")
            ]
            if len(bis_5m_1buy) == 1:
                is_ok_5m = True
                low_level_5m_msg = "5m 一买点"
        # 确定做第一个二买
        if is_ok_5m is False:
            bis_5m_2buy = [
                _bi
                for _bi in cd_5m.get_bis()
                if _bi.start.k.date >= bi_day.end.k.date
                and _bi.mmd_exists(["2buy"], "|")
            ]
            if len(bis_5m_2buy) == 1:
                is_ok_5m = True
                low_level_5m_msg = "5m 二买点"

        if is_ok_5m is False:
            return opts
        # 计算止损价格
        bi_day = cd_day.get_bis()[-1]
        bi_30m = cd_30m.get_bis()[-1]
        bi_5m = cd_5m.get_bis()[-1]
        stop_loss_price = min([bi_day.low, bi_30m.low, bi_5m.low])
        stop_loss_price = self.get_max_loss_price(
            "buy", price, stop_loss_price, self._max_loss_rate
        )

        # 低级别线段走势段要有配合的背驰或买卖点
        xds_down_30m = [
            _xd
            for _xd in cd_30m.get_xds()
            if _xd.start.k.date >= xd_day.start.k.date and _xd.type == "down"
        ]
        zsds_down_30m = [
            _zsd
            for _zsd in cd_30m.get_zsds()
            if _zsd.start.k.date >= xd_day.start.k.date and _zsd.type == "down"
        ]
        xds_down_5m = [
            _xd
            for _xd in cd_5m.get_xds()
            if _xd.start.k.date >= xd_day.start.k.date and _xd.type == "down"
        ]
        zsds_down_5m = [
            _zsd
            for _zsd in cd_5m.get_zsds()
            if _zsd.start.k.date >= xd_day.start.k.date and _zsd.type == "down"
        ]

        score_val = 0
        for _xd in xds_down_30m[-2:]:
            if _xd.mmd_exists(["1buy", "2buy", "3buy"]) or _xd.bc_exists(
                ["xd", "pz", "qs"]
            ):
                score_val += 1
                break
        for _zsd in zsds_down_30m[-2:]:
            if _zsd.mmd_exists(["1buy", "2buy", "3buy"]) or _zsd.bc_exists(
                ["zsd", "pz", "qs"]
            ):
                score_val += 1
        for _xd in xds_down_5m[-4:]:
            if _xd.mmd_exists(["1buy", "2buy", "3buy"]) or _xd.bc_exists(
                ["xd", "pz", "qs"]
            ):
                score_val += 1
                break
        for _zsd in zsds_down_5m[-2:]:
            if _zsd.mmd_exists(["1buy", "2buy", "3buy"]) or _zsd.bc_exists(
                ["zsd", "pz", "qs"]
            ):
                score_val += 1

        if score_val < 2:
            return opts

        info = {
            "day_zsd_type": (
                0 if len(cd_day.get_zsds()) == 0 else cd_day.get_zsds()[-1].type
            ),
            "day_bi": f"{bi_day.type}_{bi_day.is_done()}",
            "xd_start_date": xd_day.start.k.date,
            "xd_end_date": xd_day.end.k.date,
            "bi_start_date": bi_day.start.k.date,
            "bi_end_date": bi_day.end.k.date,
            "open_buy_date": cd_day.get_src_klines()[-1].date,
            "xd_30m_num": len(xds_30m),
            "zs_juli_rate": 0,
            "zs_type": "",
            "zs_one_line_type": "",
            "zs_line_num": 0,
            "is_pause_loss": 0,
            "score_val": score_val,
            "low_5m_msg": low_level_5m_msg,
        }

        for mmd in xd_day.get_mmds():
            if mmd.name == "3buy":
                # 过滤三买点， 之前中枢大于九段的三买
                if mmd.zs.line_num > 9:
                    continue
                info["zs_juli_rate"] = (price - mmd.zs.zg) / mmd.zs.zg * 100
            else:
                info["zs_juli_rate"] = (mmd.zs.zd - price) / price * 100
            info["zs_type"] = mmd.zs.type
            info["zs_one_line_type"] = mmd.zs.lines[0].type
            info["zs_line_num"] = mmd.zs.line_num
            return [
                Operation(
                    code=code,
                    opt="buy",
                    mmd=mmd.name,
                    loss_price=stop_loss_price,
                    info=info,
                    msg=f"线段买卖点 {xd_day.line_mmds()}，{low_level_5m_msg}，{score_val}，止损价格 {stop_loss_price}",
                )
            ]
        for bc in xd_day.get_bcs():
            if bc.type != "pz":
                continue
            bc_mmd = f"down_{bc.type}_bc_buy"
            info["zs_juli_rate"] = (bc.zs.zd - price) / price * 100
            info["zs_type"] = bc.zs.type
            info["zs_one_line_type"] = bc.zs.lines[0].type
            info["zs_line_num"] = bc.zs.line_num
            return [
                Operation(
                    code=code,
                    opt="buy",
                    mmd=bc_mmd,
                    loss_price=stop_loss_price,
                    info=info,
                    msg=f"线段背驰 {xd_day.line_bcs()}，{low_level_5m_msg}，{score_val}，止损价格 {stop_loss_price}",
                )
            ]

        return opts

    def close(
        self, code, mmd: str, pos: POSITION, market_data: MarketDatas
    ) -> Union[Operation, None, List[Operation]]:
        """
        看大做小
        日线出现顶分型，并且收盘价小于5日均线
        30m级别三段，出现卖点
        5m级别出现三类卖点
        """
        cd_day = market_data.get_cl_data(code, market_data.frequencys[0])
        cd_30m = market_data.get_cl_data(code, market_data.frequencys[1])
        cd_5m = market_data.get_cl_data(code, market_data.frequencys[2])

        # 检查是否触发止损，如果5m向下笔背驰，并且是上午，不触发止损
        # bi_5m = cd_5m.get_bis()[-1]
        # last_day_date = cd_5m.get_klines()[-1].date
        price = cd_5m.get_klines()[-1].c
        opt = self.check_loss(mmd, pos, price)
        if opt is not None:
            # if bi_5m.bc_exists(['bi', 'pz', 'q']) and last_day_date.hour <= 11:
            #     pos.info['is_pause_loss'] = 1
            # else:
            return opt

        info = pos.info
        # 日线后续出现的顶分型，是否小于5日均线，必须要下午14点30分以后才可以
        ma5_day = self.idx_ma(cd_day, 5)[-1]
        last_day_date = cd_day.get_klines()[-1].date
        bi_day = self.last_done_bi(cd_day.get_bis())
        if (
            bi_day.type == "up"
            and bi_day.is_done()
            and last_day_date > bi_day.end.klines[-1].klines[-1].date
        ):
            if price < bi_day.end.klines[-1].l and price < ma5_day:
                # 该笔，在30m至少一段，5m至少5段
                xds_30m = [
                    _xd
                    for _xd in cd_30m.get_xds()
                    if _xd.start.k.date >= bi_day.start.k.date
                    and _xd.end.k.date <= bi_day.end.k.klines[-1].date
                ]
                xds_5m = [
                    _xd
                    for _xd in cd_5m.get_xds()
                    if _xd.start.k.date >= bi_day.start.k.date
                    and _xd.end.k.date <= bi_day.end.k.klines[-1].date
                ]
                if len(xds_30m) >= 1 or len(xds_5m) >= 5:
                    return Operation(
                        code=code,
                        opt="sell",
                        mmd=mmd,
                        msg=f"日线向上笔结束(30m:{len(xds_30m)}/5m:{len(xds_5m)})，并且价格小于5日均线，平仓退出",
                    )

        # 30M 线段 至少做3段
        xds_30m = [
            _xd for _xd in cd_30m.get_xds() if _xd.start.k.date >= info["bi_end_date"]
        ]
        if len(xds_30m) >= 3:
            bi_30m = self.last_done_bi(cd_30m.get_bis())
            if (
                bi_30m.start.k.date > info["open_buy_date"]
                and bi_30m.mmd_exists(["1sell", "2sell", "3sell"])
                and self.bi_td(bi_30m, cd_30m)
            ):
                return Operation(
                    code=code,
                    opt="sell",
                    mmd=mmd,
                    msg=f"30m级别出现卖点 {bi_30m.line_mmds()}",
                )

        # 5m 出现三类卖点，退出
        bi_day = cd_day.get_bis()[-1]
        xds_5m = [
            _xd for _xd in cd_5m.get_xds() if _xd.start.k.date >= info["bi_end_date"]
        ]
        if (
            len(xds_5m) >= 5
            and bi_day.type == "up"
            and bi_day.is_done()
            and price < bi_day.end.klines[-1].l
        ):
            bis_5m = [
                _bi
                for _bi in cd_5m.get_bis()
                if _bi.start.k.date >= bi_day.end.klines[0].klines[0].date
                and _bi.type == "up"
            ]
            for _bi in bis_5m:
                if _bi.mmd_exists(["3sell"], "|") and self.bi_td(_bi, cd_5m):
                    return Operation(
                        code=code, opt="sell", mmd=mmd, msg="5m级别，出现笔的三类卖点"
                    )

        return False
