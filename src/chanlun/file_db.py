import datetime
import os
import hashlib
import pathlib
import pickle
import random
import time
from decimal import Decimal
from typing import Union

import pandas as pd
import pytz

from chanlun import fun
from chanlun.core import cl
from chanlun.base import Market
from chanlun.core.cl_interface import ICL
from chanlun.config import get_data_path
from chanlun.db import db
from chanlun.exchange import Exchange
from chanlun.tools.log_util import LogUtil

class FileCacheDB(object):
    """
    文件数据对象
    """

    def __init__(self):
        """
        初始化，判断文件并进行创建
        """
        self.home_path = pathlib.Path.home()
        self.project_path = get_data_path()
        # 更健壮的目录创建方式（支持多级）
        self.project_path.mkdir(parents=True, exist_ok=True)
        self.cl_data_path = self.project_path / "cl_data"
        self.cl_data_path.mkdir(parents=True, exist_ok=True)
        self.klines_path = self.project_path / "klines"
        self.klines_path.mkdir(parents=True, exist_ok=True)
        self.cache_pkl_path = self.project_path / "cache_pkl"
        self.cache_pkl_path.mkdir(parents=True, exist_ok=True)

        # 遍历 enum 中的值
        for market in Market:
            (self.cl_data_path / market.value).mkdir(parents=True, exist_ok=True)
            (self.klines_path / market.value).mkdir(parents=True, exist_ok=True)

        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")
        # self.us_tz = pytz.timezone('US/Eastern')

        # 如果内部有值变动，则会重新计算
        self.config_keys = [
            "kline_type",
            "kline_qk",
            "judge_zs_qs_level",
            "fx_qy",
            "fx_qj",
            "fx_bh",
            "bi_type",
            "bi_bzh",
            "bi_qj",
            "bi_fx_cgd",
            "bi_split_k_cross_nums",
            "fx_check_k_nums",
            "allow_bi_fx_strict",
            "xd_qj",
            "zsd_qj",
            "xd_allow_bi_pohuai",
            "xd_allow_split_no_highlow",
            "xd_allow_split_zs_kz",
            "xd_allow_split_zs_more_line",
            "xd_allow_split_zs_no_direction",
            "xd_zs_max_lines_split",
            "zs_bi_type",
            "zs_xd_type",
            "zs_qj",
            "zs_cd",
            "zs_wzgx",
            "zs_optimize",
            "cl_mmd_cal_qs_1mmd",
            "cl_mmd_cal_not_qs_3mmd_1mmd",
            "cl_mmd_cal_qs_3mmd_1mmd",
            "cl_mmd_cal_qs_not_lh_2mmd",
            "cl_mmd_cal_qs_bc_2mmd",
            "cl_mmd_cal_3mmd_not_lh_bc_2mmd",
            "cl_mmd_cal_1mmd_not_lh_2mmd",
            "cl_mmd_cal_3mmd_xgxd_not_bc_2mmd",
            "cl_mmd_cal_not_in_zs_3mmd",
            "cl_mmd_cal_not_in_zs_gt_9_3mmd",
            "idx_macd_fast",
            "idx_macd_slow",
            "idx_macd_signal",
        ]

        # 缠论的更新时间，如果与当前保存不一致，需要清空缓存的计算结果，重新计算
        self.cl_update_date = "2025-06-15"
        cache_cl_update_date = db.cache_get("__cl_update_date")
        if cache_cl_update_date != self.cl_update_date:
            db.cache_set("__cl_update_date", self.cl_update_date)
            self.clear_all_cl_data()

    def _config_md5(self, cl_config: dict) -> str:
        """
        生成稳定的配置 MD5：严格按照 self.config_keys 顺序生成，避免 dict 插入顺序差异。
        列表类型做字符串化处理以保持一致性。
        """
        parts = []
        for k in self.config_keys:
            v = cl_config.get(k, "0")
            if isinstance(v, list):
                v = ",".join(v)
            parts.append(f"{k}:{v}")
        unique_str = "|".join(parts)
        return hashlib.md5(unique_str.encode("UTF-8")).hexdigest()

    def _atomic_write_pickle(self, path: pathlib.Path, obj: object):
        """
        原子化写入 pickle，避免并发读到半写入文件。
        """
        tmp = path.with_suffix(path.suffix + f".tmp-{int(time.time() * 1000)}")
        with open(tmp, "wb") as fp:
            pickle.dump(obj, fp, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, path)

    def _atomic_write_csv(self, path: pathlib.Path, df: pd.DataFrame):
        """
        原子化写入 CSV，先写入临时文件再替换，保证读侧一致性。
        """
        tmp = path.with_suffix(path.suffix + f".tmp-{int(time.time() * 1000)}")
        df.to_csv(tmp, index=False)
        os.replace(tmp, path)

    def get_tdx_klines(
        self, market: str, code: str, frequency: str
    ) -> Union[None, pd.DataFrame]:
        """
        获取缓存在文件中的股票数据
        """
        file_pathname = (
            self.klines_path / market / f"{code.replace('.', '_')}_{frequency}.csv"
        )
        if file_pathname.is_file() is False:
            return None
        try:
            _klines = pd.read_csv(file_pathname, parse_dates=["date"])  # 直接解析日期列
        except Exception:
            file_pathname.unlink()
            return None
        if len(_klines) > 0:
            # 如果 date 有 Nan 则返回 None
            if _klines["date"].isnull().any():
                return None
            # 不返回最后一行
            _klines = _klines.iloc[0:-1:]

        # 加一个随机概率，去清理历史的缓存，避免太多占用空间
        if random.randint(0, 1000) <= 5:
            self.clear_tdx_old_klines(market)
        return _klines

    def save_tdx_klines(
        self, market: str, code: str, frequency: str, kline: pd.DataFrame
    ):
        """
        保存通达信k线数据对象到文件中
        """
        file_pathname = (
            self.klines_path / market / f"{code.replace('.', '_')}_{frequency}.csv"
        )
        self._atomic_write_csv(file_pathname, kline)
        return True

    def clear_tdx_old_klines(self, market):
        """
        删除15天前的k线数据，不活跃的，减少占用空间
        """
        del_lt_times = fun.datetime_to_int(datetime.datetime.now()) - (
            15 * 24 * 60 * 60
        )
        for filename in (self.klines_path / market).glob("*.csv"):
            try:
                if filename.stat().st_mtime < del_lt_times:
                    filename.unlink()
            except Exception:
                pass
        return True

    def get_web_cl_data(
            self,
            market: str,
            code: str,
            frequency: str,
            cl_config: dict,
            klines: pd.DataFrame,
    ) -> 'ICL':
        """
        获取web缓存的的缠论数据对象
        """
        logger = LogUtil.get_logger()  # 获取日志实例
        key = self._config_md5(cl_config)

        # 统一标识符用于日志输出
        log_id = f"[{market}-{code}-{frequency}-{key}]"

        file_pathname = (
                self.cl_data_path
                / market
                / f"{market}_{code.replace('/', '_').replace('.', '_')}_{frequency}_{key}.pkl"
        )

        cd: 'ICL' = cl.CL(code, frequency, cl_config)
        need_recompute = False

        if file_pathname.is_file():
            try:
                with open(file_pathname, "rb") as fp:
                    cd = pickle.load(fp)

                # --- 校验逻辑 ---
                cached_klines = cd.get_src_klines()

                if len(cached_klines) > 0 and len(klines) > 0:
                    # 1. 连续性校验：判断缓存末尾是否在给定数据时间范围之外
                    if cached_klines[-1].date < klines.iloc[0]["date"] or cached_klines[0].date > klines.iloc[0][
                        "date"]:
                        logger.warning(f"{log_id} 历史数据错位/不连续，将全量重算")
                        need_recompute = True

                    # 2. 数据一致性校验（防止复权导致的历史数据变更）
                    if not need_recompute and len(cached_klines) >= 2 and len(klines) >= 2:
                        cd_pre_kline = cached_klines[-2]
                        target_rows = klines[klines["date"] == cd_pre_kline.date]

                        if len(target_rows) == 0:
                            logger.warning(f"{log_id} 缓存参考点日期在输入数据中不存在，重算")
                            need_recompute = True
                        else:
                            row = target_rows.iloc[0]
                            # 校验 OHLCVA
                            is_diff = (
                                    Decimal(str(row["close"])) != Decimal(str(cd_pre_kline.c)) or
                                    Decimal(str(row["high"])) != Decimal(str(cd_pre_kline.h)) or
                                    Decimal(str(row["low"])) != Decimal(str(cd_pre_kline.l)) or
                                    Decimal(str(row["open"])) != Decimal(str(cd_pre_kline.o)) or
                                    Decimal(str(row["volume"])) != Decimal(str(cd_pre_kline.a))
                            )
                            if is_diff:
                                logger.warning(f"{log_id} 检测到历史数据差异（可能发生复权），重算")
                                need_recompute = True

                    # 3. 密度校验：检查最近100根K线数量是否对得上
                    if not need_recompute and len(cached_klines) >= 100 and len(klines) >= 100:
                        _v_cd = cached_klines[-100:]
                        _v_src = klines[(klines["date"] >= _v_cd[0].date) & (klines["date"] <= _v_cd[-1].date)]
                        if len(_v_cd) != len(_v_src):
                            logger.warning(f"{log_id} 局部数据缺失 [Cache:{len(_v_cd)} vs Src:{len(_v_src)}]，重算")
                            need_recompute = True

                if need_recompute:
                    cd = cl.CL(code, frequency, cl_config)

            except Exception as e:
                logger.error(f"{log_id} 读取缓存或校验过程异常: {str(e)}", exc_info=True)
                try:
                    if file_pathname.exists():
                        file_pathname.unlink()
                except Exception as un_e:
                    logger.error(f"{log_id} 尝试删除损坏缓存失败: {str(un_e)}")
                cd = cl.CL(code, frequency, cl_config)

        # 增量计算
        try:
            cd.process_klines(klines)
        except Exception as e:
            logger.error(f"{log_id} 执行缠论计算 process_klines 失败: {str(e)}", exc_info=True)
            return cd  # 或者按需抛出

        # 写入缓存
        try:
            self._atomic_write_pickle(file_pathname, cd)
        except Exception as e:
            logger.error(f"{log_id} 写入缓存异常: {str(e)}")

        # 随机清理旧数据
        if random.randint(0, 1000) <= 5:
            try:
                self.clear_old_web_cl_data()
            except Exception as e:
                logger.error(f"清理旧缓存数据异常: {str(e)}")

        return cd

    def clear_web_cl_data(self, market: str, code: str):
        """
        清除指定市场下标的缠论缓存对象
        """
        for filename in (self.cl_data_path / market).glob("*.pkl"):
            try:
                if f"{market}_{code.replace('/', '_').replace('.', '_')}" in str(
                    filename
                ):
                    filename.unlink()
            except Exception:
                pass
        return True

    def clear_old_web_cl_data(self):
        """
        清除时间超过15天的缓存数据
        """
        del_lt_times = fun.datetime_to_int(datetime.datetime.now()) - (
            15 * 24 * 60 * 60
        )
        for _market in Market:
            for filename in (self.cl_data_path / _market.value).glob("*.pkl"):
                try:
                    if filename.stat().st_mtime < del_lt_times:
                        filename.unlink()

                except Exception:
                    pass
        return True

    def clear_all_cl_data(self):
        """
        删除所有缓存的计算结果文件
        """
        for _market in Market:
            for filename in (self.cl_data_path / _market.value).glob("*.pkl"):
                try:
                    filename.unlink()
                except Exception:
                    pass
        return True

    def get_low_to_high_cl_data(
        self, db_ex: Exchange, market: str, code: str, frequency: str, cl_config: dict
    ) -> ICL:
        """
        专门为递归到高级别图表写的方法，初始数据量较多，所以只能从数据库中获取
        计算一次后进行落盘保存，后续读盘进行更新操作，减少重复计算的时间
        建议定时频繁的进行读取，保持更新，避免太多时间不读取，后续造成数据缺失情况
        """

        key = self._config_md5(cl_config)
        filename = (
            self.cl_data_path
            / f'{market}_{code.replace("/", "_")}_{frequency}_{key}.pkl'
        )
        cd: ICL
        if filename.is_file() is False:
            cd = cl.CL(code, frequency, cl_config)
        else:
            with open(filename, "rb") as fp:
                cd = pickle.load(fp)
        limit = 200000
        if len(cd.get_klines()) > 10000:
            limit = 1000
        klines = db_ex.klines(code, frequency, args={"limit": limit})
        cd.process_klines(klines)
        self._atomic_write_pickle(filename, cd)
        return cd

    def cache_pkl_to_file(self, filename: str, data: object):
        """
        将缓存数据持久化到文件中
        """
        self._atomic_write_pickle(self.cache_pkl_path / filename, data)

    def cache_pkl_from_file(self, filename: str) -> object:
        """
        从文件中读取数据
        """
        if (self.cache_pkl_path / filename).is_file() is False:
            return None
        with open(self.cache_pkl_path / filename, "rb") as fp:
            return pickle.load(fp)


fdb = FileCacheDB()

if __name__ == "__main__":
    from chanlun.cl_utils import query_cl_chart_config
    from chanlun.exchange.exchange_binance import ExchangeBinance

    # market = 'a'
    # code = 'SHSE.000001'
    # frequency = '5m'
    # cl_config = query_cl_chart_config(market, code)
    # ex = ExchangeDB(market)

    fdb = FileCacheDB()
    # cd = fdb.get_low_to_high_cl_data(ex, market, code, frequency, cl_config)
    # print(len(cd.get_klines()))
    # print(cd)

    ex = ExchangeBinance()
    market = "currency"
    code = "APT/USDT"
    freq = "d"
    cl_config = query_cl_chart_config(market, code)
    klines = ex.klines(code, freq)

    cd = fdb.get_web_cl_data(market, code, freq, cl_config, klines)
    print(cd)
    cl_config = query_cl_chart_config(market, code)
    cd = fdb.get_web_cl_data(market, code, freq, cl_config, klines)
    print(cd)


#     currency--APT/USDT--d 726a8925bda1d6fb6ac6fbe5b146fd5a index: 541 date: 2024-04-12 08:00:00+08:00 h: 12.223 l: 8.422 o: 11.862 c:9.775 a:42964252.4 code                       APT/USDT
# date      2024-04-12 08:00:00+08:00
# open                         11.862
# high                         12.223
# low                           8.422
# close                         9.775
# volume                   42964300.0
# Name: 541, dtype: object
