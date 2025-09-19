import datetime
from typing import List
import pandas as pd
from chanlun.core.cl_interface import Kline
from chanlun.tools.log_util import LogUtil


class KlineDataProcessor:
    """
    K线数据处理器
    封装了K线数据的存储、预处理和增量更新的全部逻辑。
    """
    def __init__(self, start_datetime: datetime.datetime = None):
        """
        初始化K线数据处理器

        Args:
            start_datetime (datetime.datetime, optional): 过滤此时间之前的数据. Defaults to None.
        """
        self.klines: List[Kline] = []
        self.start_datetime = start_datetime

    def update(self, klines_df: pd.DataFrame):
        """
        接收DataFrame并更新内部的K线数据列表。
        这是该类唯一的公共入口点。

        Args:
            klines_df (pd.DataFrame): 包含新K线数据的DataFrame。
        """
        if klines_df is None or klines_df.empty:
            LogUtil.warning("输入的K线数据为空，不进行处理。")
            return

        processed_df = self._preprocess(klines_df)
        new_klines = self._convert(processed_df)
        self._update_internal_klines(new_klines)

    def _preprocess(self, klines_df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理K线数据 (排序, 类型转换, 时间过滤)。

        Args:
            klines_df (pd.DataFrame): 原始K线数据。

        Returns:
            pd.DataFrame: 经过预处理的K线数据。
        """
        klines = klines_df.copy()

        # 确保date列是datetime类型
        if 'date' in klines.columns and not pd.api.types.is_datetime64_any_dtype(klines['date']):
            klines['date'] = pd.to_datetime(klines['date'])

        # 确保数值列是float类型
        numeric_cols = ['high', 'low', 'open', 'close', 'volume']
        for col in numeric_cols:
            if col in klines.columns:
                klines[col] = pd.to_numeric(klines[col], errors='coerce')

        klines = klines.sort_values('date').reset_index(drop=True)

        if self.start_datetime:
            klines = klines[klines['date'] >= self.start_datetime]

        return klines

    def _convert(self, df: pd.DataFrame) -> List[Kline]:
        """
        将DataFrame转换为Kline对象列表。

        Args:
            df (pd.DataFrame): 预处理后的数据。

        Returns:
            List[Kline]: Kline对象列表。
        """
        klines = []
        start_index = len(self.klines)
        for i, row in df.iterrows():
            kline = Kline(
                index=start_index + i,
                date=row['date'],
                h=float(row['high']),
                l=float(row['low']),
                o=float(row['open']),
                c=float(row['close']),
                a=float(row['volume']) if 'volume' in row else 0.0
            )
            klines.append(kline)
        return klines


    def _update_internal_klines(self, new_klines: List[Kline]):
        """
        执行K线数据的核心增量更新逻辑。
        如果现有数据为空，则直接替换；否则，进行增量更新。
        此方法可以正确处理重叠和新增的K线数据。
        """
        if not new_klines:
            return

        if not self.klines:
            # 首次加载
            self.klines = new_klines
        else:
            last_date = self.klines[-1].date

            # 找到新数据中，第一个时间大于或等于最后一根K线的K线索引
            start_index = -1
            for i, k in enumerate(new_klines):
                if k.date >= last_date:
                    start_index = i
                    break

            # 如果没有找到，说明所有新数据都是旧的
            if start_index == -1:
                LogUtil.info("输入的新K线数据均为旧数据，未进行更新。")
                return

            # 根据找到的K线时间，判断是更新还是追加
            if new_klines[start_index].date == last_date:
                # 更新最后一根K线
                self.klines[-1] = new_klines[start_index]
                # 追加剩余的新K线
                self.klines.extend(new_klines[start_index + 1:])
            else:  # new_klines[start_index].date > last_date
                # 直接追加所有找到的新K线及之后的部分
                self.klines.extend(new_klines[start_index:])

        # 最终，重新计算所有K线的索引以确保其连续和正确
        for i, k in enumerate(self.klines):
            k.index = i