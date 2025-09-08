import datetime
from typing import Dict, List, Union

import pandas as pd

from chanlun.backtesting.base import POSITION, MarketDatas, Operation, Strategy, Trader
from chanlun.config import get_data_path


class StrategyADMMDTest(Strategy):
    """
    沪深A股，日线级别买卖点
    """

    def __init__(
        self, mode="test", filter_key: str = "loss_rate", filter_reverse: bool = True
    ):
        super().__init__()

        self.mode = mode
        self.filter_key: str = filter_key
        self.filter_reverse: bool = filter_reverse

        self.mmds = [
            "1buy",
            "2buy",
            "l2buy",
            "3buy",
            "l3buy",
            "1sell",
            "2sell",
            "l2sell",
            "3sell",
            "l3sell",
        ]
        self.bi_bcs = ["bi", "pz", "qs"]
        self.xd_bcs = ["xd", "pz", "qs"]

        self.zs_code = "SHSE.000001"  # 上证指数的代码

    def clear(self):
        self.tz = None
        self._cache_open_infos = []
        return super().clear()

    def is_filter_opts(self):
        return True

    def filter_opts(self, opts: List[Operation], trader: Trader = None):
        if len(opts) == 0:
            return opts
        # 按照买入和卖出操作进行分组
        buy_opts = [_o for _o in opts if _o.opt == "buy"]
        sell_opts = [_o for _o in opts if _o.opt == "sell"]
        # 按照 opts 中 info 对象中的 loss_rate，从大到小 (风险越大，收益越大，与 k_change 类似，长得多，止损越大)
        buy_opts = sorted(
            opts, key=lambda x: x.info[self.filter_key], reverse=self.filter_reverse
        )

        # 卖出的操作在前，买入的操作在后
        return sell_opts + buy_opts

    def open(
        self, code, market_data: MarketDatas, poss: Dict[str, POSITION]
    ) -> List[Operation]:
        opts = []

        # 获取日线数据
        cd_d = market_data.get_cl_data(code, market_data.frequencys[1])
        if len(cd_d.get_bis()) == 0:
            return opts
        price = cd_d.get_src_klines()[-1].c
        bi_d = cd_d.get_bis()[-1]
        # 只做向下笔的买点，向上笔跳过
        if bi_d.type == "up":
            return opts
        # 如果当前笔没有买点，跳过
        if len(bi_d.line_mmds("|")) == 0:
            return opts
        # 如果笔没有完成，则不操作
        if bi_d.is_done() is False:
            return opts

        k_now_d = cd_d.get_src_klines()[-1]
        k_pre_d = cd_d.get_src_klines()[-2]
        # 当前成加量大于昨日成交量，并且当前k线要是上涨的
        if k_now_d.c < k_pre_d.c:
            return opts
        if k_now_d.a < k_pre_d.a:
            return opts

        # 如果当日有过涨停，不进行交易
        zt_price = self.code_zt_price(code, k_pre_d.c)
        # 判断 k_now_d.h 是否在 zt_price+-0.01 之间
        if zt_price - 0.01 <= k_now_d.h <= zt_price + 0.01:
            return opts

        # 记录开仓买卖点的信息
        pos_df = []
        for _mmd in bi_d.line_mmds("|"):
            pos_df.append(
                {
                    "opt_mmd": _mmd,
                    "__open_k_date": cd_d.get_src_klines()[-1].date,
                }
            )
        pos_df = pd.DataFrame(pos_df)

        # 日线周期的 k线、笔、线段信息
        if True:
            bi_pre_d = cd_d.get_bis()[-2]
            xd_d = cd_d.get_xds()[-1]
            pos_df["k_now_d_change"] = (k_now_d.c - k_pre_d.c) / k_pre_d.c * 100
            pos_df["k_now_volume_by_pre"] = k_now_d.a / k_pre_d.a
            pos_df["bi_pre_d_mmds"] = "/".join(sorted(bi_pre_d.line_mmds("|")))
            pos_df["bi_pre_d_bcs"] = "/".join(sorted(bi_pre_d.line_bcs("|")))
            pos_df["xd_d_type"] = f"{xd_d.type}_{xd_d.is_done()}"

        # 计算指标数据
        if True:
            # 日线周期 5、10、20 均线
            idx_ma5 = self.idx_ma(cd_d, 5)
            idx_ma10 = self.idx_ma(cd_d, 10)
            idx_ma20 = self.idx_ma(cd_d, 20)
            pos_df["idx_ma_5_by_price"] = price > idx_ma5[-1]
            pos_df["idx_ma_10_by_price"] = price > idx_ma10[-1]
            pos_df["idx_ma_20_by_price"] = price > idx_ma20[-1]
            pos_df["idx_ma_5_by_ma_10"] = idx_ma5[-1] > idx_ma10[-1]
            pos_df["idx_ma_5_by_ma_20"] = idx_ma5[-1] > idx_ma20[-1]
            pos_df["idx_ma_10_by_ma_20"] = idx_ma10[-1] > idx_ma20[-1]

            # 日线macd信息
            idx_macd = cd_d.get_idx()["macd"]
            pos_df["idx_macd_hist_by_0"] = idx_macd["hist"][-1] > 0
            pos_df["idx_macd_dif_by_0"] = idx_macd["dif"][-1] > 0
            pos_df["idx_macd_dea_by_0"] = idx_macd["dea"][-1] > 0

        # 周线级别的信息
        cd_w = market_data.get_cl_data(code, market_data.frequencys[0])
        if len(cd_w.get_xds()) == 0:
            return opts

        # 周线的笔线段信息
        if True:
            bi_w = cd_w.get_bis()[-1]
            xd_w = cd_w.get_xds()[-1]
            pos_df["bi_w_type"] = f"{bi_w.type}_{bi_w.is_done()}"
            pos_df["xd_w_type"] = f"{xd_w.type}_{bi_w.is_done()}"

        # 周线的均线信息
        if True:
            # 周线周期 5、10、20 均线
            idx_ma5_w = self.idx_ma(cd_w, 5)
            idx_ma10_w = self.idx_ma(cd_w, 10)
            idx_ma20_w = self.idx_ma(cd_w, 20)
            pos_df["idx_ma_5_w_by_price"] = price > idx_ma5_w[-1]
            pos_df["idx_ma_10_w_by_price"] = price > idx_ma10_w[-1]
            pos_df["idx_ma_20_w_by_price"] = price > idx_ma20_w[-1]
            pos_df["idx_ma_5_by_ma_10_w"] = idx_ma5_w[-1] > idx_ma10_w[-1]
            pos_df["idx_ma_5_by_ma_20_w"] = idx_ma5_w[-1] > idx_ma20_w[-1]
            pos_df["idx_ma_10_by_ma_20_w"] = idx_ma10_w[-1] > idx_ma20_w[-1]

        # 获取上证指数数据
        if True:
            cd_d_zs = market_data.get_cl_data(self.zs_code, market_data.frequencys[1])
            bi_d_zs = cd_d_zs.get_bis()[-1]
            # 记录上证指数的一些信息
            pos_df["zs_bi_type"] = f"{bi_d_zs.type}_{bi_d_zs.is_done()}"
            # 上证指数，日线 5、10、20 均线
            zs_ma5 = self.idx_ma(cd_d_zs, 5)
            zs_ma10 = self.idx_ma(cd_d_zs, 10)
            zs_ma20 = self.idx_ma(cd_d_zs, 20)
            zs_price = cd_d_zs.get_src_klines()[-1].c
            pos_df["zs_ma_5_by_price"] = zs_price > zs_ma5[-1]
            pos_df["zs_ma_10_by_price"] = zs_price > zs_ma10[-1]
            pos_df["zs_ma_20_by_price"] = zs_price > zs_ma20[-1]
            pos_df["zs_ma_5_by_ma_10"] = zs_ma5[-1] > zs_ma10[-1]
            pos_df["zs_ma_5_by_ma_20"] = zs_ma5[-1] > zs_ma20[-1]
            pos_df["zs_ma_10_by_ma_20"] = zs_ma10[-1] > zs_ma20[-1]

        # 止损信息
        if True:
            # 使用当前K线的低点作为止损
            pos_df["__loss_price"] = k_now_d.l
            pos_df["loss_rate"] = (price - k_now_d.l) / price * 100

        # TODO 对信息进行过滤
        pos_querys = []
        for _q in pos_querys:
            pos_df = pos_df.query(_q)

        if len(pos_df) == 0:
            return opts

        for _, _pos in pos_df.iterrows():
            opts.append(
                Operation(
                    code=code,
                    opt="buy",
                    mmd=_pos["opt_mmd"],
                    loss_price=_pos["__loss_price"],
                    info=_pos.to_dict(),
                    msg=f"买点 {_pos['opt_mmd']} , 止损价格 {_pos['__loss_price']}",
                    open_uid=f"{code}_{bi_d.start.k.date}_{_pos['opt_mmd']}",
                )
            )

        return opts

    def close(
        self, code, mmd: str, pos: POSITION, market_data: MarketDatas
    ) -> Union[Operation, None, List[Operation]]:
        """
        平仓操作信号
        """
        opts = []
        if pos.balance <= 0:
            return opts

        open_k_date = pos.info["__open_k_date"]  # 开仓当天日期

        cd_d = market_data.get_cl_data(code, market_data.frequencys[1])
        k_now_d = cd_d.get_src_klines()[-1]
        k_pre_d = cd_d.get_src_klines()[-2]
        price = cd_d.get_src_klines()[-1].c
        open_next_klines = [_k for _k in cd_d.get_src_klines() if _k.date > open_k_date]

        # 判断当前是否是跌停价格，跌停直接返回，不操作
        dt_price = self.code_dt_price(code, k_pre_d.c)
        # 判断 price 是否在 dt_price+-0.01 之间
        if dt_price - 0.01 <= price <= dt_price + 0.01:
            pos.info["__dt_price"] = dt_price
            return opts

        is_day_close = True
        if self.mode != "test":
            now_datetime = datetime.datetime.now()
            if now_datetime.hour == 14 and now_datetime.minute >= 50:
                is_day_close = True
            else:
                is_day_close = False

        # TODO 如果之前有经历过跌停，这就进行平仓，这个不限时间，可以再开盘后就触发
        if True:
            if "__dt_price" in pos.info.keys() and k_now_d.o < pos.info["__dt_price"]:
                opts.append(
                    Operation(
                        code,
                        "sell",
                        mmd,
                        loss_price=k_now_d.o,  # 这里指定平仓使用的价格
                        msg="之前有跌停，当前价格小于跌停价格",
                        close_uid="跌停平仓",
                    )
                )

        # TODO 跳空低开，直接止损平仓，这个不限时间，可以再开盘后就触发
        if True:
            if k_now_d.o < k_pre_d.l:
                opts.append(
                    Operation(
                        code,
                        "sell",
                        mmd,
                        loss_price=k_now_d.o,  # 这里指定平仓使用的价格
                        msg="跳空低开，直接止损平仓",
                        close_uid="跳空低开",
                    )
                )

        if is_day_close is False:
            return opts

        ### 以下内容就是只有再收盘时刻才会进行检查

        # 检查是否有止损
        loss_opt = self.check_loss(mmd, pos, price)
        if loss_opt is not None:
            opts.append(loss_opt)

        # TODO 移动止损，使用昨日低点作为止损点，同样是在收盘的时候进行检查
        if True:
            if len(open_next_klines) >= 1 and k_now_d.c < k_pre_d.l:
                opts.append(
                    Operation(
                        code,
                        "sell",
                        mmd,
                        msg="移动止损，当前收盘价格，低于昨日最低价格",
                        close_uid="移动止损",
                    )
                )

        # TODO 日线顶分型，如果当前价格低于分型中间k线低点，止损
        if True:
            bi_d = self.last_done_bi(cd_d.get_bis())
            if (
                is_day_close
                and bi_d.type == "up"
                and bi_d.end.k.date > open_k_date
                and price < bi_d.end.k.l
            ):
                opts.append(
                    Operation(
                        code,
                        "sell",
                        mmd,
                        msg=f"当前价格，低于日线顶分型中间k线低点 {bi_d.end.k.l}",
                        close_uid="低于日线顶分型",
                    )
                )

        # TODO 单个阴线，并且跌破均线5
        if True:
            if k_now_d.c < k_now_d.o:
                idx_ma5 = self.idx_ma(cd_d, 5)
                if price > idx_ma5[-1]:
                    pos.info["__gt_idx_ma5"] = 1
                if (
                    "__gt_idx_ma5" in pos.info.keys()
                    and k_now_d.c < k_now_d.o
                    and k_now_d.c < idx_ma5[-1]
                ):
                    opts.append(
                        Operation(
                            code,
                            "sell",
                            mmd,
                            msg=f"低开阴线，并且价格小于5日均线： {round(idx_ma5[-1], 2)}",
                            close_uid="低于5日均线",
                        )
                    )
        # TODO 单个阴线，并且跌破均线10
        if True:
            if k_now_d.c < k_now_d.o:
                idx_ma10 = self.idx_ma(cd_d, 10)
                if price > idx_ma10[-1]:
                    pos.info["__gt_idx_ma10"] = 1
                if (
                    "__gt_idx_ma10" in pos.info.keys()
                    and k_now_d.c < k_now_d.o
                    and k_now_d.c < idx_ma10[-1]
                ):
                    opts.append(
                        Operation(
                            code,
                            "sell",
                            mmd,
                            msg=f"低开阴线，并且价格小于10日均线： {round(idx_ma10[-1], 2)}",
                            close_uid="低于10日均线",
                        )
                    )
        # TODO 单个阴线，并且跌破均线20
        if True:
            if k_now_d.c < k_now_d.o:
                idx_ma20 = self.idx_ma(cd_d, 20)
                if price > idx_ma20[-1]:
                    pos.info["__gt_idx_ma20"] = 1
                if (
                    "__gt_idx_ma20" in pos.info.keys()
                    and k_now_d.c < k_now_d.o
                    and k_now_d.c < idx_ma20[-1]
                ):
                    opts.append(
                        Operation(
                            code,
                            "sell",
                            mmd,
                            msg=f"低开阴线，并且价格小于20日均线： {round(idx_ma20[-1], 2)}",
                            close_uid="低于20日均线",
                        )
                    )

        # TODO 收盘最大盈利回调5%，止盈
        if True and len(open_next_klines) > 0:
            nex_k_high = max([_k.h for _k in open_next_klines])
            nex_k_callback_rate = (price - nex_k_high) / nex_k_high * 100
            if nex_k_callback_rate <= -5:
                opts.append(
                    Operation(
                        code,
                        "sell",
                        mmd,
                        msg=f"最高价格 {nex_k_high} 回调 ({nex_k_callback_rate}) -5%，止盈",
                        close_uid="利润回调5%",
                    )
                )
            if nex_k_callback_rate <= -10:
                opts.append(
                    Operation(
                        code,
                        "sell",
                        mmd,
                        msg=f"最高价格 {nex_k_high} 回调 ({nex_k_callback_rate}) -10%，止盈",
                        close_uid="利润回调10%",
                    )
                )
            if nex_k_callback_rate <= -15:
                opts.append(
                    Operation(
                        code,
                        "sell",
                        mmd,
                        msg=f"最高价格 {nex_k_high} 回调 ({nex_k_callback_rate}) -15%，止盈",
                        close_uid="利润回调15%",
                    )
                )
            if nex_k_callback_rate <= -20:
                opts.append(
                    Operation(
                        code,
                        "sell",
                        mmd,
                        msg=f"最高价格 {nex_k_high} 回调 ({nex_k_callback_rate}) -20%，止盈",
                        close_uid="利润回调20%",
                    )
                )
            if nex_k_callback_rate <= -30:
                opts.append(
                    Operation(
                        code,
                        "sell",
                        mmd,
                        msg=f"最高价格 {nex_k_high} 回调 ({nex_k_callback_rate}) -30%，止盈",
                        close_uid="利润回调30%",
                    )
                )
            if nex_k_callback_rate <= -50:
                opts.append(
                    Operation(
                        code,
                        "sell",
                        mmd,
                        msg=f"最高价格 {nex_k_high} 回调 ({nex_k_callback_rate}) -50%，止盈",
                        close_uid="利润回调50%",
                    )
                )

        bi_d = self.last_done_bi(cd_d.get_bis())

        # 向上笔，有盘整或卖点，在停顿时卖出，这里真正的完成平仓
        if (
            bi_d.type == "up"
            and bi_d.end.k.date > open_k_date
            and (bi_d.bc_exists(["pz", "qs"], "|") or len(bi_d.line_mmds("|")) > 0)
            and self.bi_td(bi_d, cd_d)
        ):
            opts.append(
                Operation(
                    code,
                    "sell",
                    mmd,
                    msg=f"向上笔盘整({bi_d.line_bcs('|')}) 或卖点 ({bi_d.line_mmds('|')})，卖出",
                )
            )

        return opts

    def code_zt_price(self, code: str, yester_price: float):
        """
        根据昨日收盘价，计算今天的涨停价格
        """
        zt_rate = 1.10
        if code.split(".")[1][0] in ["3"]:
            zt_rate = 1.20
        zt_price = round(yester_price * zt_rate, 2)
        return zt_price

    def code_dt_price(self, code: str, yester_price: float):
        """
        根据昨日收盘价，计算今天的跌停价格
        """
        zt_rate = 0.90
        if code.split(".")[1][0] in ["3"]:
            zt_rate = 0.80
        zt_price = round(yester_price * zt_rate, 2)
        return zt_price


if __name__ == "__main__":
    import pandas as pd

    from chanlun.backtesting import backtest
    from chanlun.cl_utils import query_cl_chart_config
    from chanlun.exchange.exchange_tdx import ExchangeTDX

    # 获取所有股票代码
    ex = ExchangeTDX()
    stocks = ex.all_stocks()
    run_codes = [
        _s["code"] for _s in stocks if _s["code"][0:5] in ["SH.60", "SZ.00", "SZ.30"]
    ]
    # 将通达信的代码转换成掘金的格式
    run_codes = [_c.replace("SH.", "SHSE.").replace("SZ.", "SZSE.") for _c in run_codes]
    print(f"回测代码数量：{len(run_codes)}")

    cl_config = query_cl_chart_config("a", "SHSE.000001")
    # 量化配置
    bt_config = {
        # 策略结果保存的文件
        "save_file": str(get_data_path() / "backtest" / "a_d_mmd_v0_signal.pkl"),
        # 设置策略对象
        "strategy": StrategyADMMDTest("test"),
        # 回测模式：signal 信号模式，固定金额开仓； trade 交易模式，按照实际金额开仓
        "mode": "signal",
        # 市场配置，currency 数字货币  a 沪深  hk  港股  futures  期货
        "market": "a",
        # 基准代码，用于获取回测的时间列表
        "base_code": "SHSE.000001",
        # 回测的标的代码
        # "codes": ["SHSE.600519"],
        "codes": run_codes,
        # 回测的周期，这里设置里，在策略中才能取到对应周期的数据
        "frequencys": ["w", "d"],
        # 回测开始的时间
        "start_datetime": "2020-01-01 00:00:00",
        # 回测的结束时间
        "end_datetime": "2024-06-01 00:00:00",
        # mode 为 trade 生效，初始账户资金
        "init_balance": 1000000,
        # mode 为 trade 生效，交易手续费率
        "fee_rate": 0.001,
        # mode 为 trade 生效，最大持仓数量（分仓）
        "max_pos": 8,
        # 缠论计算的配置，详见缠论配置说明
        "cl_config": cl_config,
    }

    BT = backtest.BackTest(bt_config)
    # BT.datas.del_volume_zero = True

    # 运行回测
    # BT.run()
    BT.run_process(max_workers=5)
    # BT.load(BT.save_file)
    # 保存回测结果到文件中
    BT.save()
    BT.result()
    print("Done")
