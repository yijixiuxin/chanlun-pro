import copy
import datetime
import gc
import hashlib
import os
import pickle
import time
import traceback
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path
from typing import Dict, List

import empyrical as ep
import numpy as np
import pandas as pd
import prettytable as pt
import pyfolio as pf
from pyecharts import options as opts
from pyecharts.charts import Bar, Grid, Line
from tqdm.auto import tqdm

from chanlun import cl, fun, kcharts
from chanlun.backtesting import futures_contracts
from chanlun.backtesting.backtest_klines import BackTestKlines
from chanlun.backtesting.backtest_trader import BackTestTrader
from chanlun.backtesting.base import POSITION, Strategy
from chanlun.backtesting.klines_generator import KlinesGenerator
from chanlun.backtesting.optimize import OptimizationSetting
from chanlun.cl_interface import ICL
from chanlun.exchange.exchange import (
    convert_currency_kline_frequency,
    convert_futures_kline_frequency,
    convert_stock_kline_frequency,
)


class BackTest:
    """
    回测类
    """

    def __init__(self, config: dict = None):
        # 日志记录
        self.log = fun.get_logger("my_backtest.log")
        # 资源管理
        self._resources = set()
        # 性能监控
        self._perf_stats = {}
        # 内存管理阈值
        self.memory_threshold = 0.8  # 80% 内存使用率阈值

        if config is None:
            return
        check_keys = [
            "mode",
            "market",
            "base_code",
            "codes",
            "frequencys",
            "start_datetime",
            "end_datetime",
            "init_balance",
            "fee_rate",
            "max_pos",
            "cl_config",
            "strategy",
        ]
        for _k in check_keys:
            if _k not in config.keys():
                raise Exception(f"回测配置缺少必要参数:{_k}")

        self.mode = config["mode"]
        self.market = config["market"]
        self.base_code = config["base_code"]
        self.codes = config["codes"]
        self.frequencys = config["frequencys"]
        self.start_datetime = config["start_datetime"]
        self.end_datetime = config["end_datetime"]

        self.init_balance: int = config["init_balance"]

        # 手续费的设置
        """
        # 沪深A股的手续费，在 backtest_trader.py cal_fee 方法中计算，有设置 过户费/印花税等 费率，这里只设置券商佣金，cal_fee 里计算的最少 5 元（不免5），如不满要求可自行修改
        # 期货手续费率，这个不生效，在 futures_contracts.py 中，为每个期货品种单独配置（如果没有配置回测的品种，则会报错）
        # 其他市场，默认手续费计算方式就是 成交额 * 手续费率
        """
        self.fee_rate: float = config["fee_rate"]
        self.max_pos: int = config["max_pos"]

        self.cl_config: dict = config["cl_config"]

        # 执行策略
        self.strategy: Strategy = config["strategy"]

        self.save_file: str = config.get("save_file")

        # 交易对象
        self.trader = BackTestTrader(
            "回测",
            self.mode,
            market=self.market,
            init_balance=self.init_balance,
            fee_rate=self.fee_rate,
            max_pos=self.max_pos,
            log=self.log.info,
        )
        self.trader.set_strategy(self.strategy)
        self.datas = BackTestKlines(
            self.market,
            self.start_datetime,
            self.end_datetime,
            self.frequencys,
            self.cl_config,
        )
        self.trader.set_data(self.datas)

        # 回测循环加载下次周期，默认None 为回测最小周期
        self.next_frequency = None
        # 回测中是否将数据批量加载到内存中，True 会占用大量内存，如果内存不足，建议设置为 False
        self.load_data_to_cache = True

        # 参数优化，评价指标字段，默认为最终盈利百分比总和
        self.evaluate = "profit_rate"

        self._process_re_again = False

    def save(self):
        """
        保存回测结果到配置的文件中
        """
        if self.save_file is None:
            return

        if self.strategy is not None:
            self.strategy.clear()

        save_dict = {
            "save_file": self.save_file,
            "mode": self.mode,
            "market": self.market,
            "base_code": self.base_code,
            "codes": self.codes,
            "frequencys": self.frequencys,
            "start_datetime": self.start_datetime,
            "end_datetime": self.end_datetime,
            "init_balance": self.init_balance,
            "fee_rate": self.fee_rate,
            "max_pos": self.max_pos,
            "cl_config": self.cl_config,
            "strategy": self.strategy,
            "trader": self.trader,
            "next_frequency": self.next_frequency,
        }
        # 保存策略结果到 file 中，进行页面查看
        self.log.info(f"save to : {self.save_file}")
        with open(file=self.save_file, mode="wb") as file:
            pickle.dump(save_dict, file)

    def load(self, _file: str):
        """
        从指定的文件中恢复回测结果
        """
        with open(file=_file, mode="rb") as fp:
            config_dict = pickle.load(fp)
        self.save_file = config_dict["save_file"]
        self.mode = config_dict["mode"]
        self.market = config_dict["market"]
        self.base_code = config_dict["base_code"]
        self.codes = config_dict["codes"]
        self.frequencys = config_dict["frequencys"]
        self.start_datetime = config_dict["start_datetime"]
        self.end_datetime = config_dict["end_datetime"]
        self.init_balance = config_dict["init_balance"]
        self.fee_rate = config_dict["fee_rate"]
        self.max_pos = config_dict["max_pos"]
        self.cl_config = config_dict["cl_config"]
        self.strategy = config_dict["strategy"]
        self.trader = config_dict["trader"]
        self.next_frequency = config_dict["next_frequency"]
        self.datas = BackTestKlines(
            self.market,
            self.start_datetime,
            self.end_datetime,
            self.frequencys,
            self.cl_config,
        )
        # self.log.info('Load OK')
        return

    def info(self):
        """
        输出回测信息
        """
        self.log.info(fun.now_dt())
        self.log.info(f"保存地址 : {self.save_file}")
        self.log.info(
            f"回测模式 【{self.mode}】市场 【{self.market}】初始资金 【{self.init_balance}】 手续费率 【{self.fee_rate}】"
        )
        self.log.info(f"策略 : {self.strategy}")
        self.log.info(f"基准代码 : {self.base_code}")
        self.log.info(f"回测代码 : {self.codes}")
        self.log.info(f"周期 : {self.frequencys}")
        self.log.info(
            f"起始时间 : {self.start_datetime} 结束时间 : {self.end_datetime}"
        )
        self.log.info(f"缠论配置 : {self.cl_config}")
        self.log.info(f"交易总手续费 : {self.trader.fee_total}")
        return True

    def run(
        self,
        next_frequency: str = None,
        begin_start_dt: datetime.datetime = None,
        loop_callback_fun: object = None,
    ):
        """
        执行回测
        """
        if next_frequency is None:
            next_frequency = self.frequencys[-1]

        self.next_frequency = next_frequency

        self.datas.load_data_to_cache = self.load_data_to_cache
        self.datas.init(self.base_code, next_frequency)

        if begin_start_dt is not None:
            self.log.info(f"起始数据回放位置：{begin_start_dt}")
            for _f, _dts in self.datas.loop_datetime_list.items():
                _dts = [_d for _d in _dts if _d >= begin_start_dt]

        _st = time.time()

        while True:
            is_ok = self.datas.next(next_frequency)
            if is_ok is False:
                break
            # 更新持仓盈亏与资金变化
            try:
                self.trader.update_position_record()
            except Exception:
                self.log.error(f"执行记录持仓信息 : {self.datas.now_date} 异常")
                self.log.error(traceback.format_exc())

            for code in self.codes:
                try:
                    self.strategy.on_bt_loop_start(self)
                    self.trader.run(code, is_filter=self.strategy.is_filter_opts())
                except Exception:
                    self.log.error(f"执行 {code} : {self.datas.now_date} 异常")
                    self.log.error(traceback.format_exc())
                    # raise e
            try:
                # 如果有开启操作二次过滤，则调用一下进行执行
                self.trader.buffer_opts = self.strategy.filter_opts(
                    self.trader.buffer_opts, self.trader
                )
                self.trader.run_buffer_opts()
            except Exception:
                self.log.error(f"执行 {code} 操作二次过滤 : {self.datas.now_date} 异常")
                self.log.error(traceback.format_exc())
            if loop_callback_fun:
                loop_callback_fun(self)

        # 清空持仓
        self.trader.end()
        self.trader.datas = None
        # 调用策略的清理方法
        self.strategy.clear()
        _et = time.time()

        self.log.info(f"运行完成，执行时间：{_et - _st}")
        return True

    def run_by_code(self, code: str):
        # 修改回测类中的属性，进行回测
        # 保存文件更改
        new_file = (
            self.save_file.split(".pkl")[0]
            + "_"
            + code.lower().replace(".", "_").replace("/", "_")
            + "_process_.pkl"
        )
        # 默认如果之前的回测文件还有保存，可以直接返回，如果设置 重新运行，则不返回
        if self._process_re_again is False and Path(new_file).exists():
            return new_file

        self.save_file = new_file
        # 运行币种修改为参数指定的
        self.base_code = code
        self.codes = [code]
        # 开始运行
        self.run(self.next_frequency)
        # 结果保存
        self.save()
        # 运行完成，返回回测保存的地址
        return self.save_file

    def run_process(
        self, next_frequency: str = None, max_workers: int = None, re_again=False
    ):
        """
        多进程执行回测模式
        """
        if self.mode != "signal":
            raise Exception(f"多进程回测，不支持 {self.mode} 回测模式")

        if next_frequency is None:
            next_frequency = self.frequencys[-1]
        self.next_frequency = next_frequency

        self._process_re_again = re_again

        start = time.time()
        with ProcessPoolExecutor(
            max_workers, mp_context=get_context("spawn")
        ) as executor:
            results = list(executor.map(self.run_by_code, self.codes))
            end = time.time()
            cost: int = int(end - start)
            self.log.info(f"多进程回测完成，耗时{cost}秒")

            # 记录 资金变动历史
            balance_history = {}
            # 回测结果合并
            for f in tqdm(results, desc="结果汇总"):
                BT = BackTest()
                BT.load(f)
                # 汇总结果
                for mmd, res in BT.trader.results.items():
                    for _k, _v in res.items():
                        self.trader.results[mmd][_k] += _v
                # 历史持仓合并
                for _code, _poss in BT.trader.positions_history.items():
                    self.trader.positions_history[_code] = _poss
                # 持仓盈亏合并
                for _dt, _hold_profits in BT.trader.hold_profit_history.items():
                    if _dt not in self.trader.hold_profit_history.keys():
                        self.trader.hold_profit_history[_dt] = 0
                    self.trader.hold_profit_history[_dt] += _hold_profits
                # 合并订单记录
                for _code, _orders in BT.trader.orders.items():
                    self.trader.orders[_code] = _orders
                # 资金历史记录
                balance_history[BT.base_code] = BT.trader.balance_history
                # 手续费合并
                self.trader.fee_total += BT.trader.fee_total

                # 释放内存
                BT.trader = None
                BT.strategy = None
                BT.datas = None
                del BT
                gc.collect()

                # 整理并汇总资金变动历史
            try:
                bh_df = pd.DataFrame(balance_history.values())
                bh_df = bh_df.T.sort_index().fillna(method="ffill").fillna(0)
                self.trader.balance_history = bh_df.sum(axis=1)
            except Exception:
                self.log.error("合并资金历史记录异常")
                self.log.error(traceback.format_exc())

                self.log.info("合并回测结果完成，可调用 save 方法进行保存")
            except Exception as e:
                self.log.error("多进程回测执行异常")
                self.log.error(traceback.format_exc())
                raise e
            finally:
                # 确保资源被释放
                gc.collect()
        return True

    def run_params(self, new_cl_setting: dict):
        """
        参数优化，执行不同的参数配置

        注意事项：如果有修改过 Strategy 策略文件，并需要重新进行参数优化的，需要手动将 notebook/data/bk/_optimization_*.pkl 文件删除
        注意事项：如果有修改过 Strategy 策略文件，并需要重新进行参数优化的，需要手动将 notebook/data/bk/_optimization_*.pkl 文件删除
        注意事项：如果有修改过 Strategy 策略文件，并需要重新进行参数优化的，需要手动将 notebook/data/bk/_optimization_*.pkl 文件删除

        """
        copy_cl_config = copy.deepcopy(self.cl_config)
        for k, v in new_cl_setting.items():
            if "default" in copy_cl_config.keys():
                copy_cl_config["default"][k] = v
            else:
                copy_cl_config[k] = v
        # 生成一个唯一的key，用于避免重复执行相同配置的回测
        key = f"{self.base_code}_{self.market}_{self.codes}_{self.frequencys}_{self.start_datetime}_{self.end_datetime}_{type(self.strategy)}_{copy_cl_config}"
        key = hashlib.md5(key.encode(encoding="UTF-8")).hexdigest()
        # 保存到新的文件中，进行落地
        new_save_file = f"./data/bk/_optimization_{key}.pkl"

        BT = BackTest(
            {
                "save_file": new_save_file,
                # 设置策略对象
                "strategy": self.strategy,
                # 回测模式：signal 信号模式，固定金额开仓； trade 交易模式，按照实际金额开仓
                "mode": self.mode,
                # 市场配置，currency 数字货币  a 沪深  hk  港股  futures  期货
                "market": self.market,
                # 基准代码，用于获取回测的时间列表
                "base_code": self.base_code,
                # 回测的标的代码
                "codes": self.codes,
                # 回测的周期，这里设置里，在策略中才能取到对应周期的数据
                "frequencys": self.frequencys,
                # 回测开始的时间
                "start_datetime": self.start_datetime,
                # 回测的结束时间
                "end_datetime": self.end_datetime,
                # mode 为 trade 生效，初始账户资金
                "init_balance": self.init_balance,
                # mode 为 trade 生效，交易手续费率
                "fee_rate": self.fee_rate,
                # mode 为 trade 生效，最大持仓数量（分仓）
                "max_pos": self.max_pos,
                # 缠论计算的配置，详见缠论配置说明
                "cl_config": copy_cl_config,
            }
        )

        BT.load_data_to_cache = self.load_data_to_cache

        BT.log.info(
            f"执行参数优化，参数配置：{new_cl_setting}，落地文件：{new_save_file}"
        )
        balance = 0
        try:
            # 判断文件不存在，执行回测，文件存在，加载回测结果
            if os.path.isfile(new_save_file) is False:
                BT.log.info(f"落地文件：{new_save_file} 不存在，开始执行回测")
                BT.run(
                    self.next_frequency
                )  # 节省参数优化执行的时间，这里可以手动设置每次循环的周期
                BT.save()
            else:
                BT.log.info(f"落地文件：{new_save_file} 已经存在，直接进行加载")
                BT.load(new_save_file)

            # 如果是交易模式，评价标准是最终余额，信号模式，总盈利比率
            pos_pd = BT.positions()
            balance = pos_pd[self.evaluate].sum() if len(pos_pd) > 0 else 0

            BT.log.info(f"回测{new_cl_setting} : {new_save_file} 结果：{balance}")
        except Exception:
            BT.log.error(f"执行回测异常：{new_cl_setting} : {new_save_file}")
            BT.log.error(traceback.format_exc())

        return {
            "end_balance": balance,
            "params": new_cl_setting,
            "save_file": new_save_file,
        }

    def run_optimization(
        self,
        optimization_setting: OptimizationSetting,
        max_workers: int = None,
        next_frequency: str = None,
        evaluate: str = "profit_rate",
        load_data_to_cache: bool = True,
    ):
        """
        运行参数优化
        @param optimization_setting: 优化参数对象
        @param max_workers: 最大运行进程数
        @param next_frequency: 回测每次循环的周期
        @param evaluate: 评价的指标 允许 profit_rate /  max_profit_rate
        @param load_data_to_cache: 批量优化，如果使用加载数据到内存中的做法，会占用太多内存，这里可以设置为 False，直接读取数据到方式执行
        """
        cl_settings: List[Dict] = optimization_setting.generate_cl_settings()

        self.log.info("开始执行穷举算法优化")
        self.log.info(f"参数优化空间：{len(cl_settings)}")

        self.next_frequency = next_frequency  # 每次循环的周期
        self.load_data_to_cache = load_data_to_cache
        self.evaluate = evaluate

        start = time.perf_counter()

        with ProcessPoolExecutor(
            max_workers, mp_context=get_context("spawn")
        ) as executor:
            results = list(executor.map(self.run_params, cl_settings))
            results.sort(reverse=True, key=lambda _r: _r["end_balance"])

            end = time.perf_counter()
            cost: int = int((end - start))
            self.log.info(f"穷举算法优化完成，耗时{cost}秒")
            try:
                for r in results:
                    try:
                        BT = BackTest()
                        BT.load(r["save_file"])
                        print("* * " * 10)
                        print(f'参数：{r["params"]}')
                        print(f'落地文件：{r["save_file"]}')
                        BT.result(True)
                    except Exception:
                        self.log.error(f"处理优化结果异常：{r['save_file']}")
                        self.log.error(traceback.format_exc())
                        continue
                    finally:
                        # 确保每次循环后释放资源
                        if "BT" in locals():
                            BT.trader = None
                            BT.strategy = None
                            BT.datas = None
                            del BT
                            gc.collect()
            except Exception:
                self.log.error("处理优化结果集异常")
                self.log.error(traceback.format_exc())

                return results
            except Exception as e:
                self.log.error("参数优化执行异常")
                self.log.error(traceback.format_exc())
                raise e
            finally:
                # 确保资源被释放
                gc.collect()

    def show_charts(
        self,
        code,
        frequency,
        merge_kline_freq: str = None,
        to_minutes: int = None,
        to_dt_align_type: str = "bob",
        to_frequency: str = None,
        change_cl_config=None,
        chart_config=None,
    ):
        """
        显示指定代码指定周期的图表

        @param code: 要展示的代码
        @param frequency: 要展示的数据周期
        @param merge_kline_freq: 使用 exchange.py 中的周期转换，转换成指定市场的周期，例如 a:30m  futures:60m
        @param to_minutes: 使用 K线合成的方法，合成分钟基本的K线
        @param to_dt_align_type: 使用K线合成的方法，时间对其方式  bob 前对其 eob后对其
        @param to_frequency: 展示图表时，可以将低周期转换成高周期数据
        @param change_cl_config: 覆盖回测的缠论配置项
        @param chart_config: 覆盖画图配置项
        """
        # 根据配置中指定的缠论配置进行展示图表
        if code in self.cl_config.keys():
            cl_config = self.cl_config[code]
        elif frequency in self.cl_config.keys():
            cl_config = self.cl_config[frequency]
        elif "default" in self.cl_config.keys():
            cl_config = self.cl_config["default"]
        else:
            cl_config = self.cl_config

        if change_cl_config is None:
            change_cl_config = {}
        if chart_config is None:
            chart_config = {}

        # 根据传递的参数，暂时修改其缠论配置
        if change_cl_config is None:
            change_cl_config = {}
        show_cl_config = copy.deepcopy(cl_config)
        for _i, _v in change_cl_config.items():
            show_cl_config[_i] = _v
        for _i, _v in chart_config.items():
            show_cl_config[_i] = _v

        # 获取行情数据（回测周期内所有的）
        bk = BackTestKlines(
            self.market,
            self.start_datetime,
            self.end_datetime,
            [frequency],
            cl_config=show_cl_config,
        )
        bk.klines(code, frequency)
        klines = bk.all_klines["%s-%s" % (code, frequency)]
        title = "%s - %s" % (code, frequency)
        if to_minutes is not None:
            kg = KlinesGenerator(to_minutes, show_cl_config, to_dt_align_type)
            cd: ICL = kg.update_klines(klines)
            title = "%s - (%s to %s)" % (code, frequency, to_minutes)
        elif merge_kline_freq is not None:
            m_freq_info = merge_kline_freq.split(":")
            if m_freq_info[0] == "a":
                klines = convert_stock_kline_frequency(klines, m_freq_info[1])
            elif m_freq_info[0] == "futures":
                klines = convert_futures_kline_frequency(klines, m_freq_info[1])
            else:
                klines = convert_currency_kline_frequency(klines, m_freq_info[1])
            cd: ICL = cl.CL(code, m_freq_info[1], show_cl_config).process_klines(klines)
            title = "%s - %s" % (code, "Merge " + m_freq_info[1])
        else:
            cd: ICL = cl.CL(code, frequency, show_cl_config).process_klines(klines)
        orders = self.trader.orders[code] if code in self.trader.orders else []
        # 是否屏蔽锁仓订单
        if (
            "not_show_lock_order" in show_cl_config
            and show_cl_config["not_show_lock_order"]
        ):
            orders = [o for o in orders if "锁仓" not in o["info"]]
        render = kcharts.render_charts(
            title, cd, to_frequency=to_frequency, orders=orders, config=show_cl_config
        )
        return render

    def result(self, is_print=True):
        """
        输出回测结果
        """
        res = {"mode": self.mode}
        if self.mode == "trade":
            # 实际交易所需要看的指标
            # 基准收益率
            base_klines = self.datas.ex.klines(
                self.base_code,
                self.frequencys[0],
                start_date=self.start_datetime,
                end_date=self.end_datetime,
                args={"limit": None},
            )
            base_open = float(base_klines.iloc[0]["open"])
            base_close = float(base_klines.iloc[-1]["close"])

            # 每年交易日设置
            annual_days = 240 if self.market in ["a", "us", "hk" "futures"] else 365
            # 无风险收益率
            risk_free = 0.03

            # 按照日期聚合资产变化
            new_day_balances = {}
            for dt, b in self.trader.balance_history.items():
                day = fun.str_to_datetime(
                    fun.datetime_to_str(fun.str_to_datetime(dt), "%Y-%m-%d"), "%Y-%m-%d"
                )
                new_day_balances[day] = b
            df = pd.DataFrame.from_dict(
                new_day_balances, orient="index", columns=["balance"]
            )
            df.index.name = "date"
            pre_balance = df["balance"].shift(1)
            pre_balance.iloc[0] = self.init_balance
            x = df["balance"] / pre_balance
            x[x <= 0] = np.nan
            df["return"] = np.log(x).fillna(0)
            df["highlevel"] = (
                df["balance"].rolling(min_periods=1, window=len(df), center=False).max()
            )
            df["drawdown"] = df["balance"] - df["highlevel"]
            df["ddpercent"] = df["drawdown"] / df["highlevel"] * 100
            # Calculate statistics value
            start_date = df.index[0]
            end_date = df.index[-1]
            total_days = len(df)
            end_balance = df["balance"].iloc[-1]
            max_drawdown = df["drawdown"].min()
            max_ddpercent = df["ddpercent"].min()
            max_drawdown_end = df["drawdown"].idxmin()
            if isinstance(max_drawdown_end, datetime.datetime):
                max_drawdown_start = df["balance"][:max_drawdown_end].idxmax()
                max_drawdown_duration = (max_drawdown_end - max_drawdown_start).days
            else:
                max_drawdown_duration = 0

            total_return = (end_balance / self.init_balance - 1) * 100
            annual_return = total_return / total_days * annual_days
            daily_return = df["return"].mean() * 100
            return_std = df["return"].std() * 100

            if return_std:
                daily_risk_free = risk_free / np.sqrt(annual_days)
                sharpe_ratio = (
                    (daily_return - daily_risk_free) / return_std * np.sqrt(annual_days)
                )
            else:
                sharpe_ratio = 0

            return_drawdown_ratio = -total_return / max_ddpercent

            # 总的手续费
            total_fee = self.trader.fee_total
            base_annual_return = (
                (base_close / base_open - 1) / total_days * annual_days * 100
            )

            res["start_date"] = start_date
            res["end_date"] = end_date
            res["total_days"] = total_days
            res["init_balance"] = self.init_balance
            res["end_balance"] = end_balance
            res["total_fee"] = total_fee
            res["base_return"] = (base_close - base_open) / base_open * 100
            res["base_annual_return"] = base_annual_return
            res["total_return"] = total_return
            res["annual_return"] = annual_return
            res["max_drawdown"] = max_drawdown
            res["max_ddpercent"] = max_ddpercent
            res["max_drawdown_duration"] = max_drawdown_duration
            res["daily_return"] = daily_return
            res["return_std"] = return_std
            res["sharpe_ratio"] = sharpe_ratio
            res["return_drawdown_ratio"] = return_drawdown_ratio
        else:
            res["start_date"] = ""
            res["end_date"] = ""
            res["total_days"] = 0
            res["init_balance"] = self.init_balance
            res["end_balance"] = self.trader.balance
            res["total_fee"] = 0
            res["base_return"] = 0
            res["base_annual_return"] = 0
            res["total_return"] = 0
            res["annual_return"] = 0
            res["max_drawdown"] = 0
            res["max_ddpercent"] = 0
            res["max_drawdown_duration"] = 0
            res["daily_return"] = 0
            res["return_std"] = 0
            res["sharpe_ratio"] = 0
            res["return_drawdown_ratio"] = 0

        tb = pt.PrettyTable()
        tb.field_names = [
            "买卖点",
            "成功",
            "失败",
            "胜率",
            "盈利",
            "亏损",
            "净利润",
            "回吐比例",
            "平均盈利",
            "平均亏损",
            "盈亏比",
        ]

        mmds = {
            "1buy": "一类买点",
            "2buy": "二类买点",
            "l2buy": "类二类买点",
            "3buy": "三类买点",
            "l3buy": "类三类买点",
            "down_bi_bc_buy": "下跌笔背驰",
            "down_xd_bc_buy": "下跌线段背驰",
            "down_pz_bc_buy": "下跌盘整背驰",
            "down_qs_bc_buy": "下跌趋势背驰",
            "1sell": "一类卖点",
            "2sell": "二类卖点",
            "l2sell": "类二类卖点",
            "3sell": "三类卖点",
            "l3sell": "类三类卖点",
            "up_bi_bc_sell": "上涨笔背驰",
            "up_xd_bc_sell": "上涨线段背驰",
            "up_pz_bc_sell": "上涨盘整背驰",
            "up_qs_bc_sell": "上涨趋势背驰",
        }
        total_trade_num = 0  # 总的交易数量
        total_win_num = 0  # 总的盈利数量
        total_loss_num = 0  # 总的亏损数量
        total_win_balance = 0
        total_loss_balance = 0
        for k in self.trader.results.keys():
            mmd = mmds[k]
            win_num = self.trader.results[k]["win_num"]
            loss_num = self.trader.results[k]["loss_num"]
            shenglv = (
                0
                if win_num == 0 and loss_num == 0
                else win_num / (win_num + loss_num) * 100
            )
            win_balance = self.trader.results[k]["win_balance"]
            loss_balance = self.trader.results[k]["loss_balance"]
            net_balance = win_balance - loss_balance
            back_rate = 0 if win_balance == 0 else loss_balance / win_balance * 100
            win_mean_balance = 0 if win_num == 0 else win_balance / win_num
            loss_mean_balance = 0 if loss_num == 0 else loss_balance / loss_num
            ykb = (
                0
                if loss_mean_balance == 0 or win_mean_balance == 0
                else win_mean_balance / loss_mean_balance
            )

            total_trade_num += win_num + loss_num
            total_win_num += win_num
            total_loss_num += loss_num
            total_win_balance += win_balance
            total_loss_balance += loss_balance

            tb.add_row(
                [
                    mmd,
                    win_num,
                    loss_num,
                    f"{str(round(shenglv, 2))}%",
                    round(win_balance, 2),
                    round(loss_balance, 2),
                    round(net_balance, 2),
                    round(back_rate, 2),
                    round(win_mean_balance, 2),
                    round(loss_mean_balance, 2),
                    round(ykb, 4),
                ]
            )

        total_shenglv = (
            0
            if total_win_num == 0 == total_loss_num
            else total_win_num / (total_win_num + total_loss_num) * 100
        )
        total_net_balance = total_win_balance - total_loss_balance
        total_back_rate = (
            0
            if total_win_balance == 0
            else total_loss_balance / total_win_balance * 100
        )
        total_win_mean_balance = (
            0 if total_win_num == 0 else total_win_balance / total_win_num
        )
        total_loss_mean_balance = (
            0 if total_loss_num == 0 else total_loss_balance / total_loss_num
        )
        total_ykb = (
            0
            if total_loss_mean_balance == 0
            else total_win_mean_balance / total_loss_mean_balance
        )
        tb.add_row(
            [
                "汇总",
                total_win_num,
                total_loss_num,
                f"{round(total_shenglv, 2)}%",
                round(total_win_balance, 2),
                round(total_loss_balance, 2),
                round(total_net_balance, 2),
                round(total_back_rate, 2),
                round(total_win_mean_balance, 2),
                round(total_loss_mean_balance, 2),
                round(total_ykb, 4),
            ]
        )
        res["mmd_infos"] = tb
        if is_print:
            self.print_result(res)
            return

        return res

    @staticmethod
    def print_result(res: dict):
        """
        打印结果信息
        """
        if res["mode"] == "trade":
            print(
                f'首个交易日：{res["start_date"]} 最后交易日：{res["end_date"]} 总交易日：{res["total_days"]}'
            )
            print(
                f'起始资金：{res["init_balance"]:,.2f} 结束资金：{res["end_balance"]:,.2f} 总手续费：{res["total_fee"]:,.2f}'
            )
            print(
                f'基准收益率：{res["base_return"]:,.2f}%  基准年化收益：{res["base_annual_return"]:,.2f}%%'
            )
            print(
                f'总收益率：{res["total_return"]:,.2f}% 年化收益率：{res["annual_return"]:,.2f}%'
            )
            print(
                f'最大回撤：{res["max_drawdown"]:,.2f} 百分比最大回撤：{res["max_ddpercent"]:,.2f}% 最长回撤天数：{res["max_drawdown_duration"]}'
            )
            print(
                f'日均收益率：{res["daily_return"]:,.2f}% 收益标准差：{res["return_std"]:,.2f}% Sharpe Ratio: {res["sharpe_ratio"]:,.2f} 收益回撤比：{res["return_drawdown_ratio"]:,.2f} '
            )
        print(res["mmd_infos"])
        return

    def result_by_pyfolio(self, live_start_date=None, is_return=False):
        """
        使用 pyfolio 计算回测结果
        """
        if self.mode != "trade":
            print("只有交易模式才能使用 pyfolio 计算回测结果")
            return None

        # 按照日期聚合资产变化
        new_day_balances = {}
        for dt, b in self.trader.balance_history.items():
            day = dt[0:10]  # 2022-02-22
            new_day_balances[fun.str_to_datetime(day, "%Y-%m-%d")] = b
        df = pd.DataFrame.from_dict(
            new_day_balances, orient="index", columns=["balance"]
        )
        df.index.name = "date"
        df = df.sort_index()
        df["return"] = df["balance"].pct_change()

        if is_return:
            # 夏普比率（默认无风险利率=0，年化周期=252天）
            sharpe = ep.sharpe_ratio(df["return"])

            # 卡玛比率（需计算年化收益和最大回撤）
            calmar = ep.calmar_ratio(df["return"])

            # 索提诺比率（仅考虑下行波动率）
            sortino = ep.sortino_ratio(df["return"])

            # 欧米茄比率（阈值默认为0）
            omega = ep.omega_ratio(df["return"])

            # 稳定性：年化波动率的倒数（需调整符号确保数值有意义）
            annual_volatility = ep.annual_volatility(df["return"])
            stability = 1 / annual_volatility if annual_volatility != 0 else np.nan

            return {
                "Sharpe Ratio": sharpe,
                "Calmar Ratio": calmar,
                "Sortino Ratio": sortino,
                "Omega Ratio": omega,
                "Stability (1/Annual Vol)": stability,
                "Stability": ep.stability_of_timeseries(df["return"]),
            }

        # 持仓记录
        positions = {}
        for _dt, _pos_balance in self.trader.positions_balance_history.items():
            _dt = fun.str_to_datetime(_dt[0:10], "%Y-%m-%d")
            if _dt not in positions.keys():
                positions[_dt] = {}
            for _code, _b in _pos_balance.items():
                if _code == "Cash":
                    _code = "cash"
                positions[_dt][_code] = _b
        positions = pd.DataFrame(positions).T

        # 交易记录
        transactions = []
        for _code, _orders in self.trader.orders.items():
            for _o in _orders:
                transactions.append(
                    {
                        "date": fun.str_to_datetime(
                            fun.datetime_to_str(_o["datetime"])
                        ),
                        "amount": (
                            _o["amount"]
                            if _o["type"] in ["open_long", "close_short"]
                            else -_o["amount"]
                        ),
                        "price": _o["price"],
                        "symbol": _code,
                    }
                )
        transactions = pd.DataFrame(transactions)
        transactions.set_index("date", inplace=True)

        # 获取基准的收益率
        base_klines = self.datas.ex.klines(
            self.base_code,
            "d",
            start_date=self.start_datetime,
            end_date=self.end_datetime,
            args={"limit": None},
        )
        base_klines["date"] = base_klines["date"].apply(
            lambda d: fun.str_to_datetime(d.strftime("%Y-%m-%d"), "%Y-%m-%d")
        )
        base_klines.set_index("date", inplace=True)
        base_klines["return"] = base_klines["close"].pct_change()

        pf.create_full_tear_sheet(
            returns=df["return"],
            benchmark_rets=base_klines["return"],
            positions=positions,
            transactions=transactions,
            live_start_date=live_start_date,
        )

        return None

    def backtest_charts(self):
        """
        输出盈利图表
        """
        base_prices = {"datetime": [], "val": []}
        balance_history = {"datetime": [], "val": []}
        hold_profit_history = {"datetime": [], "val": []}

        # 获取所有的交易日期节点
        base_klines = self.datas.ex.klines(
            self.base_code,
            self.next_frequency,
            start_date=self.start_datetime,
            end_date=self.end_datetime,
            args={"limit": None},
        )
        dts = list(base_klines["date"].to_list())
        base_prices["val"] = list(base_klines["close"].to_list())

        # 按照时间统计当前时间持仓累计盈亏
        _hold_profit_sums = {}
        for _dt, _p in self.trader.hold_profit_history.items():
            if _dt not in _hold_profit_sums.keys():
                _hold_profit_sums[_dt] = _p
            else:
                _hold_profit_sums[_dt] += _p

        # 按照时间累加总的收益
        for _dt in dts:
            _dt = _dt.strftime("%Y-%m-%d %H:%M:%S")
            base_prices["datetime"].append(_dt)

            # 资金余额
            if _dt in self.trader.balance_history.keys():
                balance_history["datetime"].append(_dt)
                balance_history["val"].append(self.trader.balance_history[_dt])
            else:
                balance_history["datetime"].append(_dt)
                balance_history["val"].append(
                    balance_history["val"][-1] if len(balance_history["val"]) > 0 else 0
                )

            # 当前时间持仓累计
            hold_profit_history["datetime"].append(_dt)
            if _dt in _hold_profit_sums.keys():
                hold_profit_history["val"].append(_hold_profit_sums[_dt])
            else:
                hold_profit_history["val"].append(0)

        return self.__create_backtest_charts(
            base_prices, balance_history, hold_profit_history
        )

    def backtest_charts_by_close_profit(self):
        # 获取所有的交易日期节点
        base_prices = {"datetime": [], "val": []}
        base_klines = self.datas.ex.klines(
            self.base_code,
            self.next_frequency,
            start_date=self.start_datetime,
            end_date=self.end_datetime,
            args={"limit": None},
        )
        dts = list(base_klines["date"].to_list())
        base_prices["val"] = list(base_klines["close"].to_list())

        # 获取所有的持仓历史，并按照平仓时间排序
        positions: List[POSITION] = []
        for _code in self.trader.positions_history.keys():
            positions.extend(iter(self.trader.positions_history[_code]))
        positions = sorted(positions, key=lambda p: p.close_datetime, reverse=False)

        # 持仓中的唯一买卖点
        mmds = list(set([p.mmd for p in positions]))
        # 记录不同买卖点的收益总和
        dts_mmd_nps = {_m: {} for _m in mmds}
        # 按照平仓时间统计其中的收益总和
        dts_total_nps = {}
        # 临时记录不同买卖点的收益
        tmp_mmd_nps = {_m: 0 for _m in mmds}
        tmp_total_nps = 0
        for _dt in dts:
            tmp_pos_by_dt = [
                p for p in positions if p.close_datetime == fun.datetime_to_str(_dt)
            ]
            if len(tmp_pos_by_dt) > 0:
                for _p in tmp_pos_by_dt:
                    net_profit = (_p.profit_rate / 100) * _p.balance
                    tmp_total_nps += net_profit
                    tmp_mmd_nps[_p.mmd] += net_profit
            dts_total_nps[_dt] = tmp_total_nps
            for _m, _nps in dts_mmd_nps.items():
                _nps[_dt] = tmp_mmd_nps[_m]

        # print(dts_mmd_nps)

        main_chart = (
            Line()
            .extend_axis(yaxis=opts.AxisOpts(name="基准价格", position="left"))
            .add_xaxis(xaxis_data=list(dts_total_nps.keys()))
            .add_yaxis(
                series_name="基准价格",
                y_axis=base_prices["val"],
                label_opts=opts.LabelOpts(is_show=False),
                yaxis_index=1,
            )
            .add_yaxis(
                series_name="平仓总收益",
                y_axis=np.array([_v for _k, _v in dts_total_nps.items()]),
                label_opts=opts.LabelOpts(is_show=False),
                yaxis_index=0,
            )
            .set_global_opts(
                title_opts=opts.TitleOpts(title="平仓收益汇总图表"),
                tooltip_opts=opts.TooltipOpts(
                    trigger="axis", axis_pointer_type="cross"
                ),
                legend_opts=opts.LegendOpts(
                    is_show=True, pos_left="center", background_color="yellow"
                ),
                datazoom_opts=[
                    opts.DataZoomOpts(
                        is_show=False,
                        type_="inside",
                        xaxis_index=[0, 0],
                        range_start=0,
                        range_end=100,
                    ),
                    opts.DataZoomOpts(
                        is_show=True,
                        xaxis_index=[0, 1],
                        pos_top="97%",
                        range_start=0,
                        range_end=100,
                    ),
                    opts.DataZoomOpts(
                        is_show=False, xaxis_index=[0, 2], range_start=0, range_end=100
                    ),
                ],
            )
        )
        for _mmd, _nps in dts_mmd_nps.items():
            main_chart.add_yaxis(
                series_name=_mmd,
                y_axis=np.array([_v for _k, _v in _nps.items()]),
                label_opts=opts.LabelOpts(is_show=False),
                yaxis_index=0,
            )

        chart = Grid(
            init_opts=opts.InitOpts(width="90%", height="700px", theme="white")
        )
        chart.add(
            main_chart,
            is_control_axis_index=True,
            grid_opts=opts.GridOpts(
                width="96%", height="100%", pos_left="1%", pos_right="3%"
            ),
        )
        if "JPY_PARENT_PID" in os.environ.keys() or "VSCODE_CWD" in os.environ.keys():
            return chart.render_notebook()
        else:
            return chart.dump_options()

    def __get_close_profit(self, pos: POSITION, uids: List[str] = None):
        # 记录开仓的占用保证金与手续费
        hold_balance = 0
        hold_amount = 0
        pos_amount = 0
        fee = 0
        for _or in pos.open_records:
            hold_balance += _or["hold_balance"]
            hold_amount += _or["amount"]
            pos_amount += _or["amount"]
            fee += _or["fee"]

        if uids is None:
            uids = ["clear"]

        if uids == "__max_profit":
            # 获取最大利润的 close_uid
            query_uids = [_r["close_uid"] for _r in pos.close_records]
            if pos.type == "做多":
                close_records = sorted(
                    pos.close_records, key=lambda _r: _r["price"], reverse=True
                )
            else:
                close_records = sorted(
                    pos.close_records, key=lambda _r: _r["price"], reverse=False
                )
        else:
            # 查询平仓 uids
            query_uids = self.trader.get_opt_close_uids(pos.code, pos.mmd, uids)
            if "clear" not in query_uids:
                query_uids.append("clear")

            # 按照时间从早到晚排序
            close_records = sorted(pos.close_records, key=lambda _r: _r["datetime"])

        # 记录平仓释放的保证金与手续费
        release_balance = 0
        close_price = 0
        close_datetime = None
        close_msg = ""
        max_profit_rate = 0
        max_loss_rate = 0
        for _r in close_records:
            if _r["close_uid"] in query_uids:
                release_balance += _r["release_balance"]
                fee += _r["fee"]
                close_price = _r["price"]
                pos_amount -= _r["amount"]
                close_datetime = _r["datetime"]
                close_msg = _r["close_msg"]
                max_profit_rate = _r["max_profit_rate"]
                max_loss_rate = _r["max_loss_rate"]
                if pos_amount == 0:
                    break

        if release_balance == 0:
            raise Exception(
                f"{pos.code} - {pos.mmd} - {pos.open_datetime} 没有找到对应的平仓记录: {query_uids}"
            )

        # 计算盈亏比例
        if pos.type == "做多":
            profit = release_balance - hold_balance - fee
            profit_rate = profit / hold_balance * 100
            if self.market == "futures":
                contract_config = futures_contracts.futures_contracts[pos.code]
                profit = (release_balance - hold_balance) / contract_config[
                    "margin_rate_long"
                ] - pos.fee
                profit_rate = profit / pos.balance * 100
        else:
            profit = hold_balance - release_balance - fee
            profit_rate = profit / hold_balance * 100
            if self.market == "futures":
                contract_config = futures_contracts.futures_contracts[pos.code]
                profit = (hold_balance - release_balance) / contract_config[
                    "margin_rate_short"
                ] - pos.fee
                profit_rate = profit / pos.balance * 100

        return {
            "close_datetime": close_datetime,
            "hold_amount": hold_amount,
            "close_price": close_price,
            "profit": profit,
            "profit_rate": profit_rate,
            "max_profit_rate": max_profit_rate,
            "max_loss_rate": max_loss_rate,
            "close_msg": close_msg,
            "fee": fee,
        }

    def positions(
        self,
        code: str = None,
        add_columns: List[str] = None,
        close_uids: List[str] = None,
    ):
        """
        输出历史持仓信息
        如果 code 为 str 返回 特定 code 的数据
        """
        pos_objs = []
        for _code in self.trader.positions_history.keys():
            if code is not None and _code != code:
                continue
            for p in self.trader.positions_history[_code]:
                p_profit = self.__get_close_profit(p, close_uids)
                p_obj = {
                    "code": _code,
                    "mmd": p.mmd,
                    "open_datetime": p.open_datetime,
                    "close_datetime": p_profit["close_datetime"],
                    "type": p.type,
                    "price": p.price,
                    "close_price": p_profit["close_price"],
                    "amount": p_profit["hold_amount"],
                    "fee": p_profit["fee"],
                    "loss_price": p.loss_price,
                    "profit": p_profit["profit"],
                    "profit_rate": p_profit["profit_rate"],
                    "max_profit_rate": p_profit["max_profit_rate"],
                    "max_loss_rate": p_profit["max_loss_rate"],
                    "open_msg": p.open_msg,
                    "close_msg": p_profit["close_msg"],
                    "open_uid": p.open_uid,
                }
                if add_columns is not None:
                    for _col in add_columns:
                        if _col in p.info.keys():
                            p_obj[_col] = p.info[_col]
                        else:
                            p_obj[_col] = "--"
                pos_objs.append(p_obj)

        return pd.DataFrame(pos_objs)

    def orders(self, code: str = None):
        """
        输出订单列表
        如果 code 返回 特定 code 的数据
        """
        order_objs = []
        for _code, orders in self.trader.orders.items():
            if code is not None and _code != code:
                continue
            order_objs.extend(iter(orders))
        return pd.DataFrame(order_objs)

    @staticmethod
    def __orders_pd(trades: List[BackTestTrader]):
        """
        持仓历史转换成 pandas 数据，便于做分析
        """
        order_objs = []
        for td in trades:
            for code, orders in td.orders.items():
                order_objs.extend(iter(orders))
        return pd.DataFrame(order_objs)

    @staticmethod
    def __create_backtest_charts(
        base_prices, balance_history: dict, hold_profit_history: dict
    ):
        """
        回测结果图表展示
        :return:
        """
        main_name = "资金变化"
        main_x = balance_history["datetime"]
        main_y = balance_history["val"]

        main_chart = (
            Line()
            .add_xaxis(xaxis_data=base_prices["datetime"])
            .extend_axis(yaxis=opts.AxisOpts(name="基准", position="left"))
            .extend_axis(yaxis=opts.AxisOpts(name="持仓盈亏", position="left"))
            .extend_axis(yaxis=opts.AxisOpts(name="持仓数量", position="left"))
            .add_yaxis(
                series_name=main_name,
                y_axis=main_y,
                label_opts=opts.LabelOpts(is_show=False),
                markpoint_opts=opts.MarkPointOpts(
                    data=[
                        opts.MarkPointItem(
                            type_="max",
                            name="最大值",
                            symbol="pin",
                            symbol_size=25,
                            itemstyle_opts=opts.ItemStyleOpts(color="red"),
                        ),
                        opts.MarkPointItem(
                            type_="min",
                            name="最小值",
                            symbol="pin",
                            symbol_size=25,
                            itemstyle_opts=opts.ItemStyleOpts(color="green"),
                        ),
                    ],
                    label_opts=opts.LabelOpts(color="yellow"),
                ),
            )
            .add_yaxis(
                series_name="基准",
                y_axis=base_prices["val"],
                yaxis_index=1,
                label_opts=opts.LabelOpts(is_show=False),
            )
            .set_global_opts(
                title_opts=opts.TitleOpts(title="回测结果图表展示"),
                tooltip_opts=opts.TooltipOpts(
                    trigger="axis", axis_pointer_type="cross"
                ),
                yaxis_opts=opts.AxisOpts(
                    position="right", type_="value", min_=min(main_y), max_=max(main_y)
                ),
                legend_opts=opts.LegendOpts(
                    is_show=True, pos_left="center", background_color="yellow"
                ),
                datazoom_opts=[
                    opts.DataZoomOpts(
                        is_show=False,
                        type_="inside",
                        xaxis_index=[0, 0],
                        range_start=0,
                        range_end=100,
                    ),
                    opts.DataZoomOpts(
                        is_show=True,
                        xaxis_index=[0, 1],
                        pos_top="97%",
                        range_start=0,
                        range_end=100,
                    ),
                    opts.DataZoomOpts(
                        is_show=True,
                        xaxis_index=[0, 2],
                        pos_top="97%",
                        range_start=0,
                        range_end=100,
                    ),
                    opts.DataZoomOpts(
                        is_show=False, xaxis_index=[0, 3], range_start=0, range_end=100
                    ),
                ],
            )
        )

        hold_profit_chart = (
            Bar()
            .add_xaxis(xaxis_data=hold_profit_history["datetime"])
            .add_yaxis(
                series_name="持仓盈亏变动",
                y_axis=hold_profit_history["val"],
                yaxis_index=2,
                label_opts=opts.LabelOpts(is_show=False),
            )
            .set_global_opts(
                tooltip_opts=opts.TooltipOpts(
                    trigger="axis", axis_pointer_type="cross"
                ),
                yaxis_opts=opts.AxisOpts(position="right"),
                legend_opts=opts.LegendOpts(
                    is_show=True, pos_right="20%", background_color="yellow"
                ),
            )
        )

        chart = Grid(
            init_opts=opts.InitOpts(width="90%", height="700px", theme="white")
        )
        chart.add(
            main_chart,
            is_control_axis_index=True,
            grid_opts=opts.GridOpts(
                width="96%", height="90%", pos_left="1%", pos_right="3%"
            ),
        )
        chart.add(
            hold_profit_chart,
            is_control_axis_index=True,
            grid_opts=opts.GridOpts(
                height="10%",
                width="96%",
                pos_left="1%",
                pos_right="3%",
                pos_bottom="0%",
            ),
        )
        if "JPY_PARENT_PID" in os.environ.keys() or "VSCODE_CWD" in os.environ.keys():
            return chart.render_notebook()
        else:
            return chart.dump_options()
