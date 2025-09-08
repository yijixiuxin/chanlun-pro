from typing import Dict, List, Union

from chanlun.backtesting.base import POSITION, MarketDatas, Operation, Strategy
from chanlun.cl_interface import BI
from chanlun.cl_utils import cal_zs_macd_infos
from chanlun.config import get_data_path


class StrategyASingleAllMmd(Strategy):
    """
    市场：股票市场
    周期：单周期
    推荐缠论配置：
        笔中枢类型：段内中枢
        走势中枢类型：标准中枢
        中枢位置关系：zggdd 较宽松
    策略：
        根据当前笔在线段中枢的位置，在按照笔的买卖点进行操作

    注意：
        需要有上证指数的数据

    """

    def __init__(self):
        super().__init__()

        self._max_loss_rate = None

    def open(
        self, code, market_data: MarketDatas, poss: Dict[str, POSITION]
    ) -> List[Operation]:
        opts = []

        # 增加基准（上证指数）的走势过滤
        # base_data = market_data.get_cl_data('SH.000001', market_data.frequencys[0])
        # if len(base_data.get_xds()) == 0:
        #     return opts
        # base_xd = base_data.get_xds()[-1]

        high_data = market_data.get_cl_data(code, market_data.frequencys[0])
        if (
            len(high_data.get_xds()) == 0
            or len(high_data.get_xd_zss()) == 0
            or len(high_data.get_bi_zss()) == 0
            or len(high_data.get_bis()) == 0
        ):
            return opts

        # 获取 最后一个走势中枢，最后一个笔中枢，最后一个线段，最后一完成笔
        high_xd_zs = high_data.get_xd_zss()[-1]
        high_bi_zs = high_data.get_bi_zss()[-1]
        high_bi_zs_start_bi: BI = high_bi_zs.lines[1]
        high_xd = high_data.get_xds()[-1]
        high_bi = self.last_done_bi(high_data.get_bis())
        high_same_bi = high_data.get_bis()[high_bi.index - 2]  # 最后一笔的前一同向笔
        high_xd_bi_zss = [
            _zs
            for _zs in high_data.get_bi_zss()
            if _zs.start.index > high_xd.start.index
        ]  # 最后一个线段内的笔中枢
        price = high_data.get_klines()[-1].c

        # 根据最后笔的高低点止损，如果不设止损，则 loss_price = None
        loss_price = high_bi.low if high_bi.type == "down" else high_bi.high

        info = {"high_bi": high_bi}

        zs_macd_infos = cal_zs_macd_infos(high_bi_zs, high_data)

        # 笔的 1、2 类买卖点，类似左侧交易，要在走势中枢的 zg / zd 下方才可以做
        if (
            high_bi.mmd_exists(["1buy"])
            and high_bi.low <= high_xd_zs.dd
            and zs_macd_infos.dif_up_cross_num > 0
            and self.bi_td(high_bi, high_data)
        ):
            opts.append(Operation(code, "buy", "1buy", loss_price, info, "一买"))
        if (
            high_bi.mmd_exists(["1sell"])
            and high_bi.high >= high_xd_zs.gg
            and zs_macd_infos.dif_down_cross_num > 0
            and self.bi_td(high_bi, high_data)
        ):
            opts.append(Operation(code, "buy", "1sell", loss_price, info, "一卖"))

        if (
            high_bi.mmd_exists(["2buy"])
            and high_xd_zs.zd > high_bi.low > high_same_bi.low
            and zs_macd_infos.dif_up_cross_num > 0
            and self.bi_td(high_bi, high_data)
        ):
            opts.append(Operation(code, "buy", "2buy", loss_price, info, "二买"))

        if (
            high_bi.mmd_exists(["2sell"])
            and high_xd_zs.zg < high_bi.high < high_same_bi.high
            and zs_macd_infos.dif_down_cross_num > 0
            and self.bi_td(high_bi, high_data)
        ):
            opts.append(Operation(code, "buy", "2sell", loss_price, info, "二卖"))

        # 线段的背驰（做线段级别围绕中枢的震荡）要在走势中枢 zg、zd 之外才可以做
        if (
            high_xd.type == "up"
            and high_xd.bc_exists(["xd", "pz", "qs"])
            and high_xd_zs.lines[1].type == "down"
            and high_xd.high >= high_xd_zs.gg
            and high_bi.type == "up"
            and high_bi.index - high_xd.end_line.index == 2
            and price > (high_xd_zs.zg - ((high_xd_zs.zg - high_xd_zs.zd) / 2))
            and high_bi.high < high_xd.end_line.high
            and self.bi_td(high_bi, high_data)
        ):
            opts.append(
                Operation(code, "buy", "up_pz_bc_sell", loss_price, info, "线段背驰")
            )
        if (
            high_xd.type == "down"
            and high_xd.bc_exists(["xd", "pz", "qs"])
            and high_xd_zs.lines[1].type == "up"
            and high_xd.low <= high_xd_zs.dd
            and high_bi.type == "down"
            and high_bi.index - high_xd.end_line.index == 2
            and price < (high_xd_zs.zd + ((high_xd_zs.zg - high_xd_zs.zd) / 2))
            and high_bi.low > high_xd.end_line.low
            and self.bi_td(high_bi, high_data)
        ):
            opts.append(
                Operation(code, "buy", "down_pz_bc_buy", loss_price, info, "线段背驰")
            )

        # 3类买卖点，在基准的向下线段进行中，不进行开仓
        # if base_xd.type == 'down' and base_xd.is_done() is False:
        #     return opts

        # 笔的 3 类买卖点，类似右侧纪要，要在走势中枢 zd / zg （在严格点要在 zg - zd 一半以上位置）  以上位置才可以做
        # 这里先暂时用宽松的 zd zg 判断
        if (
            high_bi.mmd_exists(["3buy"])
            and high_bi.low > (high_xd_zs.zd + (high_xd_zs.zg - high_xd_zs.zd) / 2)
            and high_xd_zs.lines[-1].index == high_xd.index
            and high_bi_zs.zf() > 30
            and len(high_xd_bi_zss) <= 1
            and high_bi.get_ld(high_data)["macd"]["dif"]["max"] > 0
            and self.bi_td(high_bi, high_data)
        ):
            opts.append(Operation(code, "buy", "3buy", loss_price, info, "三买"))
        if (
            high_bi.mmd_exists(["3sell"])
            and high_bi.high < (high_xd_zs.zg - (high_xd_zs.zg - high_xd_zs.zd) / 2)
            and high_xd_zs.lines[-1].index == high_xd.index
            and high_bi_zs.zf() > 30
            and len(high_xd_bi_zss) <= 1
            and high_bi.get_ld(high_data)["macd"]["dif"]["min"] < 0
            and self.bi_td(high_bi, high_data)
        ):
            opts.append(Operation(code, "buy", "3sell", loss_price, info, "三卖"))

        # 笔的 类 3 买卖点，有根据程序判断的，也有在这里自己写的 类3买卖点 规则
        # 类3买卖点规则：
        # 之前有第一个笔中枢，并且在走势中枢一半的位置以上，进中枢的第一笔是 3买点，并且当前笔后形成笔中枢，而且高于3买点；卖点反之
        if (
            high_bi.type == "down"
            and high_bi_zs_start_bi.mmd_exists(["3buy"])
            and len(high_xd_bi_zss) == 2
            and high_xd_zs.lines[-1].index == high_xd.index
            and high_bi_zs.zf() > 30
            and high_bi.low > high_xd_zs.zd
            and high_bi.low > high_bi_zs_start_bi.low
            and high_bi.index == high_bi_zs.lines[-1].index
            and high_bi.get_ld(high_data)["macd"]["dif"]["max"] > 0
            and self.bi_td(high_bi, high_data)
        ):
            opts.append(Operation(code, "buy", "l3buy", loss_price, info, "类三买"))
        if (
            high_bi.type == "up"
            and high_bi_zs_start_bi.mmd_exists(["3sell"])
            and len(high_xd_bi_zss) == 2
            and high_xd_zs.lines[-1].index == high_xd.index
            and high_bi_zs.zf() > 30
            and high_bi.high < high_xd_zs.zg
            and high_bi.high < high_bi_zs_start_bi.high
            and high_bi.index == high_bi_zs.lines[-1].index
            and high_bi.get_ld(high_data)["macd"]["dif"]["max"] < 0
            and self.bi_td(high_bi, high_data)
        ):
            opts.append(Operation(code, "buy", "l3sell", loss_price, info, "类三卖"))

        return opts

    def close(
        self, code, mmd: str, pos: POSITION, market_data: MarketDatas
    ) -> Union[Operation, None]:
        if pos.balance == 0:
            return False

        # 卖出规则，可根据买入时的买卖点类型不同，会有一些特别的卖出条件
        high_data = market_data.get_cl_data(code, market_data.frequencys[0])

        if (
            len(high_data.get_xds()) == 0
            or len(high_data.get_xd_zss()) == 0
            or len(high_data.get_bi_zss()) == 0
            or len(high_data.get_bis()) == 0
        ):
            return False

        loss_opt = self.check_loss(mmd, pos, high_data.get_klines()[-1].c)
        if loss_opt is not None:
            return loss_opt

        pos_high_bi: BI = pos.info["high_bi"]

        # 获取 最后一个走势中枢，最后一个笔中枢，最后一个线段，最后一完成笔
        high_xd_zs = high_data.get_xd_zss()[-1]

        high_xd = high_data.get_xds()[-1]
        high_bi = self.last_done_bi(high_data.get_bis())

        # 确保使用买入后的新笔进行判断卖出条件
        if high_bi.start.index <= pos_high_bi.start.index:
            return False

        # 在线段中枢上方，出现 1、2 类买点，平仓
        if (
            "buy" in mmd
            and high_bi.mmd_exists(["1sell", "2sell"])
            and high_bi.high > high_xd_zs.zg
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(
                code, "sell", mmd, msg="线段中枢上方出现卖点 %s" % (high_bi.line_mmds())
            )
        if (
            "sell" in mmd
            and high_bi.mmd_exists(["1buy", "2buy"])
            and high_bi.low < high_xd_zs.zd
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(
                code, "sell", mmd, msg="线段中枢下方出现买点 %s" % (high_bi.line_mmds())
            )

        # 3类买点、类3类买点，出现 笔的盘整、趋势背驰，平仓
        if (
            "3buy" in mmd
            and high_bi.type == "up"
            and high_bi.bc_exists(["pz", "qs"])
            and high_bi.high > high_xd_zs.zg
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(
                code, "sell", mmd, msg="3买后出现笔背驰 %s" % (high_bi.line_bcs())
            )
        if (
            "3sell" in mmd
            and high_bi.type == "down"
            and high_bi.bc_exists(["pz", "qs"])
            and high_bi.low < high_xd_zs.zd
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(
                code, "sell", mmd, msg="3卖后出现笔背驰 %s" % (high_bi.line_bcs())
            )

        # # 3类买卖点、类3类买卖点，向上笔停顿，但是 macd 黄白线在零轴下方，平仓
        # if '3buy' in mmd and high_bi.type == 'up' and high_macd_dif < 0 and high_macd_dea < 0 and high_bi.td:
        #     return Operation('sell', mmd, msg='3类买卖点，笔向上但是没有突破macd零轴')
        # if '3sell' in mmd and high_bi.type == 'down' and high_macd_dif > 0 and high_macd_dea > 0 and high_bi.td:
        #     return Operation('sell', mmd, msg='3类买卖点，笔向下但是没有突破macd零轴')

        # 线段的 三卖点，次笔不破新高，平仓
        if (
            "buy" in mmd
            and high_xd.mmd_exists(["3sell"])
            and high_bi.type == "up"
            and high_bi.index - high_xd.end_line.index == 2
            and high_bi.high < high_xd.end_line.high
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(code, "sell", mmd, msg="线段出现三卖，并且次笔不创新高")
        if (
            "sell" in mmd
            and high_xd.mmd_exists(["3buy"])
            and high_bi.type == "down"
            and high_bi.index - high_xd.end_line.index == 2
            and high_bi.low > high_xd.end_line.low
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(code, "sell", mmd, msg="线段出现三买，并且次笔不创新低")

        # 在中枢的上方，线段背驰（线段背驰、盘整背驰、趋势背驰），次笔不破新高，平仓
        if (
            "buy" in mmd
            and high_xd.type == "up"
            and high_xd.bc_exists(["xd", "pz", "qs"])
            and high_bi.type == "up"
            and high_bi.index - high_xd.end_line.index == 2
            and high_bi.high < high_xd.end_line.high
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(code, "sell", mmd, msg="线段向上背驰，笔不创新高")
        if (
            "sell" in mmd
            and high_xd.type == "down"
            and high_xd.bc_exists(["xd", "pz", "qs"])
            and high_bi.type == "down"
            and high_bi.index - high_xd.end_line.index == 2
            and high_bi.low > high_xd.end_line.low
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(code, "sell", mmd, msg="线段向下背驰，笔不创新低")

        # 线段的背驰买入，在价格超过中枢zg点，并且出现笔背驰就卖出
        if (
            "bc_buy" in mmd
            and high_bi.type == "up"
            and high_bi.bc_exists(["bi", "pz", "qs"])
            and high_bi.high >= high_xd_zs.zg
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(
                code,
                "sell",
                mmd,
                msg="价格高于 zg，并且笔背驰 %s" % (high_bi.line_bcs()),
            )
        if (
            "bc_sell" in mmd
            and high_bi.type == "down"
            and high_bi.bc_exists(["bi", "pz", "qs"])
            and high_bi.low <= high_xd_zs.zd
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(
                code,
                "sell",
                mmd,
                msg="价格低于 zd，并且笔背驰 %s" % (high_bi.line_bcs()),
            )

        # 小转大，笔内部 macd 背驰，笔停顿卖出
        if (
            "buy" in mmd
            and high_bi.type == "up"
            and abs(high_bi.jiaodu()) >= 45
            and self.bi_td(high_bi, high_data)
        ):
            macd_list = high_data.get_idx()["macd"]["hist"][
                high_bi.start.k.k_index : high_bi.end.k.k_index + 1
            ]
            dif_list = high_data.get_idx()["macd"]["dif"][
                high_bi.start.k.k_index : high_bi.end.k.k_index + 1
            ]
            macd_jd = self.points_jiaodu(macd_list, "up")
            dif_jd = self.points_jiaodu(dif_list, "up")
            if dif_jd > 0 and macd_jd < 0:
                return Operation(
                    code,
                    "sell",
                    mmd,
                    msg="笔快速上涨，内部macd背驰 macd jd %s dif jd %s"
                    % (macd_jd, dif_jd),
                )
        if (
            "sell" in mmd
            and high_bi.type == "down"
            and abs(high_bi.jiaodu()) >= 45
            and self.bi_td(high_bi, high_data)
        ):
            macd_list = high_data.get_idx()["macd"]["hist"][
                high_bi.start.k.k_index : high_bi.end.k.k_index + 1
            ]
            dif_list = high_data.get_idx()["macd"]["dif"][
                high_bi.start.k.k_index : high_bi.end.k.k_index + 1
            ]
            macd_jd = self.points_jiaodu(macd_list, "down")
            dif_jd = self.points_jiaodu(dif_list, "down")
            if dif_jd < 0 and macd_jd > 0:
                return Operation(
                    code,
                    "sell",
                    mmd,
                    msg="笔快速上涨，内部macd背驰 macd jd %s dif jd %s"
                    % (macd_jd, dif_jd),
                )

        # 小转大，验证分型卖出
        if (
            "buy" in mmd
            and high_bi.type == "up"
            and abs(high_bi.jiaodu()) >= 45
            and self.bi_yanzhen_fx(high_bi, high_data)
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(code, "sell", mmd, msg="小转大验证分型")
        if (
            "sell" in mmd
            and high_bi.type == "down"
            and abs(high_bi.jiaodu()) >= 45
            and self.bi_yanzhen_fx(high_bi, high_data)
            and self.bi_td(high_bi, high_data)
        ):
            return Operation(code, "sell", mmd, msg="小转大验证分型")

        return False


if __name__ == "__main__":
    from chanlun.backtesting import backtest
    from chanlun.cl_utils import query_cl_chart_config

    # 获取所有股票代码
    # ex = ExchangeTDX()
    # stocks = ex.all_stocks()
    # run_codes = [
    #     _s["code"] for _s in stocks if _s["code"][0:5] in ["SH.60", "SZ.00", "SZ.30"]
    # ]
    # # 将通达信的代码转换成掘金的格式
    # run_codes = [_c.replace("SH.", "SHSE.").replace("SZ.", "SZSE.") for _c in run_codes]
    # print(f"回测代码数量：{len(run_codes)}")

    cl_config = query_cl_chart_config("a", "SH.000001")
    # 量化配置
    bt_config = {
        # 策略结果保存的文件
        "save_file": str(get_data_path() / "backtest" / "a_d_mmd_single_v0_signal.pkl"),
        # 设置策略对象
        "strategy": StrategyASingleAllMmd(),
        # 回测模式：signal 信号模式，固定金额开仓； trade 交易模式，按照实际金额开仓
        "mode": "signal",
        # 市场配置，currency 数字货币  a 沪深  hk  港股  futures  期货
        "market": "a",
        # 基准代码，用于获取回测的时间列表
        "base_code": "SH.000001",
        # 回测的标的代码
        "codes": ["SH.601009"],
        # "codes": run_codes,
        # 回测的周期，这里设置里，在策略中才能取到对应周期的数据
        "frequencys": ["d"],
        # 回测开始的时间
        "start_datetime": "2018-01-01 00:00:00",
        # 回测的结束时间
        "end_datetime": "2022-04-20 00:00:00",
        # mode 为 trade 生效，初始账户资金
        "init_balance": 1000000,
        # mode 为 trade 生效，交易手续费率
        "fee_rate": 0.0006,
        # mode 为 trade 生效，最大持仓数量（分仓）
        "max_pos": 8,
        # 缠论计算的配置，详见缠论配置说明
        "cl_config": cl_config,
    }

    BT = backtest.BackTest(bt_config)
    # BT.datas.del_volume_zero = True

    # 运行回测
    BT.run()
    # BT.run_process(max_workers=5)
    # BT.load(BT.save_file)
    # 保存回测结果到文件中
    BT.save()
    BT.result()
    print("Done")
