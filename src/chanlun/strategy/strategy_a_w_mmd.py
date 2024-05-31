import datetime
from typing import List, Union

import pandas as pd
from chanlun.backtesting.base import POSITION, MarketDatas, Operation, Strategy, Trader
from chanlun.cl_interface import Dict, List
from chanlun.config import get_data_path
from chanlun.cl_utils import klines_to_heikin_ashi_klines
from chanlun.cl_analyse import MultiLevelAnalyse
from core import cl


class StrategyAWMMD(Strategy):
    """
    沪深A股，周线级别买卖点
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

        # 获取周线数据
        cd_w = market_data.get_cl_data(code, market_data.frequencys[0])
        if len(cd_w.get_bis()) == 0:
            return opts
        price = cd_w.get_src_klines()[-1].c
        bi_w = cd_w.get_bis()[-1]
        # 只做向下笔的买点，向上笔跳过
        if bi_w.type == "up":
            return opts
        # 如果当前笔没有买点，跳过
        if len(bi_w.line_mmds("|")) == 0:
            return opts
        # 如果笔没有停顿，则不操作
        if self.bi_td(bi_w, cd_w) is False:
            return opts

        pos_df = []
        for _mmd in bi_w.line_mmds("|"):
            pos_df.append(
                {
                    "opt_mmd": _mmd,
                    "__open_k_date": cd_w.get_src_klines()[-1].date,
                }
            )
        pos_df = pd.DataFrame(pos_df)

        # 获取上证指数数据
        if True:
            cd_w_zs = market_data.get_cl_data("SH.000001", market_data.frequencys[0])
            bi_w_zs = cd_w_zs.get_bis()[-1]
            # 记录上证指数的一些信息
            pos_df["zs_bi_type"] = f"{bi_w_zs.type}_{bi_w_zs.is_done()}"

        # 止损信息
        if True:
            # 笔的结束分型低点进行止损
            pos_df["__loss_price"] = bi_w.end.val
            pos_df["loss_rate"] = (price - bi_w.end.val) / price * 100

        for _, _pos in pos_df.iterrows():
            opts.append(
                Operation(
                    code=code,
                    opt="buy",
                    mmd=_pos["opt_mmd"],
                    loss_price=_pos["__loss_price"],
                    info=_pos.to_dict(),
                    msg=f"买点 {_pos['opt_mmd']} , 止损价格 {_pos['__loss_price']}",
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

        # open_k_date = pos.info["__open_k_date"]

        cd_w = market_data.get_cl_data(code, market_data.frequencys[0])

        # 检查是否有止损
        price = cd_w.get_src_klines()[-1].c
        loss_opt = self.check_loss(mmd, pos, price)
        if loss_opt is not None:
            opts.append(loss_opt)

        bi_w = cd_w.get_bis()[-1]

        # 向上笔，有盘整或卖点，在停顿时卖出
        if (
            bi_w.type == "up"
            and (bi_w.bc_exists(["pz", "qs"], "|") or len(bi_w.line_mmds("|")) > 0)
            and self.bi_td(bi_w, cd_w)
        ):
            opts.append(
                Operation(
                    code,
                    "sell",
                    mmd,
                    msg=f"向上笔盘整({bi_w.line_bcs('|')}) 或卖点 ({bi_w.line_mmds('|')})，卖出",
                )
            )

        return opts


if __name__ == "__main__":
    from chanlun.backtesting import backtest
    from chanlun.cl_utils import query_cl_chart_config
    import pandas as pd

    from chanlun.exchange.exchange_tdx import ExchangeTDX

    # 获取所有股票代码
    ex = ExchangeTDX()
    stocks = ex.all_stocks()
    run_codes = [
        _s["code"] for _s in stocks if _s["code"][0:5] in ["SH.60", "SZ.00", "SZ.30"]
    ]

    cl_config = query_cl_chart_config("a", "SH.000001")
    # 量化配置
    bt_config = {
        # 策略结果保存的文件
        "save_file": str(get_data_path() / "backtest" / "a_w_mmd_v0_signal.pkl"),
        # 设置策略对象
        "strategy": StrategyAWMMD("test"),
        # 回测模式：signal 信号模式，固定金额开仓； trade 交易模式，按照实际金额开仓
        "mode": "signal",
        # 市场配置，currency 数字货币  a 沪深  hk  港股  futures  期货
        "market": "a",
        # 基准代码，用于获取回测的时间列表
        "base_code": "SH.600519",
        # 回测的标的代码
        # "codes": ["SH.600519"],
        "codes": run_codes,
        # 回测的周期，这里设置里，在策略中才能取到对应周期的数据
        "frequencys": ["w"],
        # 回测开始的时间
        "start_datetime": "2000-01-01 00:00:00",
        # "start_datetime": "2023-06-01 00:00:00",
        # 回测的结束时间
        "end_datetime": "2029-01-01 00:00:00",
        # 是否是股票，True 当日开仓不可平仓，False 当日开当日可平
        "is_stock": True,
        # 是否是期货，True 可做空，False 不可做空
        "is_futures": False,
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
    BT.run_process(max_workers=16)
    # BT.load(BT.save_file)
    # 保存回测结果到文件中
    BT.save()
    BT.result()
    # print(BT.positions())
    print("Done")
