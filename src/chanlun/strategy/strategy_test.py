from typing import Dict, List, Union

from chanlun.backtesting.base import POSITION, MarketDatas, Operation, Strategy
from chanlun.config import get_data_path


class StrategyTest(Strategy):
    """
    策略测试类
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

        # 获取 k 线，如果是红色，就买入，绿色就卖出
        klines = market_data.klines(code, market_data.frequencys[0])
        last_k = klines.iloc[-1]
        if last_k["close"] > last_k["open"]:
            # 红色
            opts.append(
                Operation(
                    code,
                    "buy",
                    "1buy",
                    loss_price=last_k["low"],
                    info={},
                    msg="看涨K线",
                    pos_rate=0.5,
                    key=f"{code}:{last_k['date']}",
                )
            )
        elif last_k["close"] < last_k["open"]:
            # 绿色
            opts.append(
                Operation(
                    code,
                    "buy",
                    "1sell",
                    loss_price=last_k["high"],
                    info={},
                    msg="看跌K线",
                    pos_rate=0.5,
                    key=f"{code}:{last_k['date']}",
                )
            )

        return opts

    def close(
        self, code, mmd: str, pos: POSITION, market_data: MarketDatas
    ) -> Union[Operation, None]:
        """
        持仓监控，返回平仓配置
        """
        opts = []

        klines = market_data.klines(code, market_data.frequencys[0])
        last_k = klines.iloc[-1]
        # 检查止损
        loss_opt = self.check_loss(mmd, pos, last_k["close"])
        if loss_opt is not None:
            opts.append(loss_opt)

        # 如果是买入，绿色的时候平仓一部分
        if "buy" in mmd:
            if last_k["close"] < last_k["open"]:
                opts.append(
                    Operation(
                        code,
                        "sell",
                        mmd,
                        msg="看跌k平仓",
                        pos_rate=0.5,
                        key=f"{code}:{last_k['date']}",
                    )
                )
        # 如果是卖出，红色的时候平仓一部分
        if "sell" in mmd:
            if last_k["close"] > last_k["open"]:
                opts.append(
                    Operation(
                        code,
                        "sell",
                        mmd,
                        msg="看涨k平仓",
                        pos_rate=0.5,
                        key=f"{code}:{last_k['date']}",
                    )
                )

        return opts


if __name__ == "__main__":
    from chanlun.backtesting import backtest
    from chanlun.cl_utils import query_cl_chart_config

    market = "futures"
    cl_config = query_cl_chart_config(market, "SH.000001")
    # 量化配置
    bt_config = {
        # 策略结果保存的文件
        "save_file": str(get_data_path() / "backtest" / "strategy_test.pkl"),
        # 设置策略对象
        "strategy": StrategyTest(),
        # 回测模式：signal 信号模式，固定金额开仓； trade 交易模式，按照实际金额开仓
        "mode": "signal",
        # 市场配置，currency 数字货币  a 沪深  hk  港股  futures  期货
        "market": market,
        # 基准代码，用于获取回测的时间列表
        "base_code": "SHFE.RB",
        # 回测的标的代码
        "codes": ["SHFE.RB"],
        # 回测的周期，这里设置里，在策略中才能取到对应周期的数据
        "frequencys": ["5m"],
        # 回测开始的时间
        "start_datetime": "2024-11-01 00:00:00",
        # 回测的结束时间
        "end_datetime": "2025-11-02 00:00:00",
        # mode 为 trade 生效，初始账户资金
        "init_balance": 100000,
        # mode 为 trade 生效，交易手续费率
        "fee_rate": 0.0003,
        # mode 为 trade 生效，最大持仓数量（分仓）
        "max_pos": 1,
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
