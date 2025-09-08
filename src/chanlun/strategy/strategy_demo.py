from typing import Dict, List, Union

from chanlun.backtesting.base import POSITION, MarketDatas, Operation, Strategy
from chanlun.cl_interface import BI


class StrategyDemo(Strategy):
    """
    策略Demo
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

        data_now = market_data.get_cl_data(code, market_data.frequencys[0])
        # 没有笔或中枢，退出
        if len(data_now.get_bis()) == 0 or len(data_now.get_bi_zss()) == 0:
            return opts

        # 笔没有完成，退出
        bi_now = data_now.get_bis()[-1]
        if bi_now.is_done() is False:
            bi_now = data_now.get_bis()[-2]

        # 笔没有买卖点，退出
        if len(bi_now.line_mmds()) == 0:
            return opts

        # 笔没有停顿，退出
        if self.bi_td(bi_now, data_now) is False:
            return opts

        # 保证唯一
        mmds = bi_now.line_mmds()
        price = data_now.get_klines()[-1].c
        # 缠论K线 第二个 的高低点做止损点
        ck_kline_2_low = bi_now.end.klines[-2].l
        ck_kline_2_high = bi_now.end.klines[-2].h

        for mmd in mmds:
            if "buy" in mmd and price < ck_kline_2_high:
                continue
            if "sell" in mmd and price > ck_kline_2_low:
                continue

            if self._max_loss_rate is not None:
                if "buy" in mmd:
                    loss_price = price - (price * (abs(self._max_loss_rate) / 100))
                    loss_price = max(loss_price, ck_kline_2_low)
                else:
                    loss_price = price + (price * (abs(self._max_loss_rate) / 100))
                    loss_price = min(loss_price, ck_kline_2_high)
            else:
                if "buy" in mmd:
                    loss_price = ck_kline_2_low
                else:
                    loss_price = ck_kline_2_high

            opts.append(
                Operation(
                    code=code,
                    opt="buy",
                    mmd=mmd,
                    loss_price=loss_price,
                    msg="当前级别 (MMD: %s Loss: %s) "
                    % (bi_now.line_mmds(), loss_price),
                    info={
                        "fx_datetime": bi_now.end.k.date,
                        "cl_datas": {
                            "bi_now": bi_now,
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

        data_now = market_data.get_cl_data(code, market_data.frequencys[0])
        price = data_now.get_klines()[-1].c

        # 止盈止损检查
        if "buy" in mmd:
            if price < pos.loss_price:
                return Operation(code, "sell", mmd, msg="%s 止损" % mmd)
        elif "sell" in mmd:
            if price > pos.loss_price:
                return Operation(code, "sell", mmd, msg="%s 止损" % mmd)

        # 自建仓之后的反向笔
        pos_bi_now: BI
        pos_bi_now = pos.info["cl_datas"]["bi_now"]
        next_bi_now = None
        for _bi in data_now.get_bis()[::-1]:
            if (
                _bi.start.k.date > pos_bi_now.start.k.date
                and _bi.type != pos_bi_now.type
                and _bi.is_done()
            ):
                next_bi_now = _bi
                break
        if next_bi_now is None:
            return None

        if "buy" in mmd:
            # 买入做多，检查卖点，笔向上，出现停顿，并且出现 卖点或背驰
            if (
                next_bi_now.type == "up"
                and self.bi_td(next_bi_now, data_now)
                and (
                    next_bi_now.mmd_exists(
                        ["1sell", "2sell", "l2sell", "3sell", "l3sell"]
                    )
                    or next_bi_now.bc_exists(["bi", "pz", "qs"])
                )
            ):
                return Operation(
                    code=code,
                    opt="sell",
                    mmd=mmd,
                    msg="%s 当前级别出现卖点（MMDS: %s，BC：（BI: %s, PZ: %s, QS: %s）），多仓清仓"
                    % (
                        mmd,
                        next_bi_now.line_mmds(),
                        next_bi_now.bc_exists(["bi"]),
                        next_bi_now.bc_exists(["pz"]),
                        next_bi_now.bc_exists(["qs"]),
                    ),
                )

        if "sell" in mmd:
            # 买入做空，检查买点，笔向下，出现停顿，并且出现 买点或背驰
            if (
                next_bi_now.type == "down"
                and self.bi_td(next_bi_now, data_now)
                and (
                    next_bi_now.mmd_exists(["1buy", "2buy", "l2buy", "3buy", "l3buy"])
                    or next_bi_now.bc_exists(["bi", "pz", "qs"])
                )
            ):
                return Operation(
                    code=code,
                    opt="sell",
                    mmd=mmd,
                    msg="%s 当前级别出现买点（MMDS: %s，BC：（BI: %s, PZ: %s, QS: %s）），空仓清仓"
                    % (
                        mmd,
                        next_bi_now.line_mmds(),
                        next_bi_now.bc_exists(["bi"]),
                        next_bi_now.bc_exists(["pz"]),
                        next_bi_now.bc_exists(["qs"]),
                    ),
                )
        return None


if __name__ == "__main__":

    from chanlun.backtesting import backtest
    from chanlun.cl_utils import query_cl_chart_config
    from chanlun.config import get_data_path
    from chanlun.exchange.exchange_tdx import ExchangeTDX

    # 获取所有股票代码
    ex = ExchangeTDX()
    stocks = ex.all_stocks()
    run_codes = [
        _s["code"] for _s in stocks if _s["code"][0:5] in ["SH.60", "SZ.00", "SZ.30"]
    ]
    run_codes = run_codes[0:100]
    print(f"回测代码数量：{len(run_codes)}")

    cl_config = query_cl_chart_config("a", "SHSE.000001")
    # 量化配置
    bt_config = {
        # 策略结果保存的文件
        "save_file": str(get_data_path() / "backtest" / "a_demo_v0_signal.pkl"),
        # 设置策略对象
        "strategy": StrategyDemo(),
        # 回测模式：signal 信号模式，固定金额开仓； trade 交易模式，按照实际金额开仓
        "mode": "signal",
        # 市场配置，currency 数字货币  a 沪深  hk  港股  futures  期货
        "market": "a",
        # 基准代码，用于获取回测的时间列表
        "base_code": "SH.000001",
        # 回测的标的代码
        # "codes": ["SH.600519"],  # 制定单个代码进行测试
        "codes": run_codes,
        # 回测的周期，这里设置里，在策略中才能取到对应周期的数据
        "frequencys": ["d"],
        # 回测开始的时间
        "start_datetime": "2020-01-01 00:00:00",
        # 回测的结束时间
        "end_datetime": "2025-06-01 00:00:00",
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
    # BT.run()  # 单个股票用这个
    BT.run_process(max_workers=5)  # 多个股票的信号模式，用这个，充分利用多核性能
    # BT.load(BT.save_file)
    # 保存回测结果到文件中
    BT.save()
    BT.result()
    print("Done")
