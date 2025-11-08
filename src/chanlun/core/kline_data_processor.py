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

    def process_kline(self, klines_df: pd.DataFrame) -> List[Kline]:
        """
        接收DataFrame并更新内部的K线数据列表。
        这是该类唯一的公共入口点。

        Args:
            klines_df (pd.DataFrame): 包含新K线数据的DataFrame。

        Returns:
            List[Kline]: 返回增量更新或新增的K线数据列表。
        """
        if klines_df is None or klines_df.empty:
            LogUtil.warning("输入的K线数据为空，不进行处理。")
            return []

        processed_df = self._preprocess(klines_df)
        new_klines = self._convert(processed_df)

        # 调用更新方法，并返回其返回的增量数据
        return self._update_internal_klines(new_klines)

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
        (使用 to_dict('records') 代替 iterrows，性能更高)

        Args:
            df (pd.DataFrame): 预处理后的数据。

        Returns:
            List[Kline]: Kline对象列表。
        """
        klines = []
        # .to_dict('records') 比 iterrows 快得多
        # index 暂时设置为 0, 稍后在 _update_internal_klines 中修正
        for row in df.to_dict('records'):
            kline = Kline(
                index=0,  # 占位符，将在 _update_internal_klines 中被修正
                date=row['date'],
                h=float(row['high']),
                l=float(row['low']),
                o=float(row['open']),
                c=float(row['close']),
                # 使用 .get() 并确保 or 0.0 来处理 volume 可能不存在或为None的情况
                a=float(row.get('volume') or 0.0)
            )
            klines.append(kline)
        return klines

    def _update_internal_klines(self, new_klines: List[Kline]) -> List[Kline]:
        """
        执行K线数据的核心增量更新逻辑。
        (优化：避免在增量更新时全量重新计算索引，只为新数据设置索引)

        Returns:
            List[Kline]: 返回增量更新或新增的K线数据列表。
        """
        if not new_klines:
            return []

        # 用于存储增量数据的列表
        increment_klines: List[Kline] = []

        if not self.klines:
            # --- 首次加载 ---
            # 这是唯一需要全量设置索引的地方
            for i, k in enumerate(new_klines):
                k.index = i
            self.klines = new_klines
            increment_klines = new_klines
            # 首次加载，所有 new_klines 都是增量
            return increment_klines

        # --- 增量更新逻辑 ---
        last_date = self.klines[-1].date
        # 获取最后一个K线的索引
        last_index = self.klines[-1].index

        # 找到新数据中，第一个时间大于或等于最后一根K线的K线索引
        start_index = -1
        for i, k in enumerate(new_klines):
            if k.date >= last_date:
                start_index = i
                break

        # 如果没有找到 (所有新数据都是旧的)
        if start_index == -1:
            LogUtil.info("输入的新K线数据均为旧数据，未进行更新。")
            return []

        # 从找到的位置开始，都是增量数据
        increment_klines = new_klines[start_index:]

        # 根据找到的K线时间，判断是更新还是追加
        if new_klines[start_index].date == last_date:
            # --- 更新最后一根K线 ---
            update_kline = new_klines[start_index]
            update_kline.index = last_index  # 修正索引
            self.klines[-1] = update_kline

            # --- 追加剩余的新K线 ---
            klines_to_append = new_klines[start_index + 1:]
            for i, k in enumerate(klines_to_append):
                # 从 last_index + 1 开始设置新索引
                k.index = last_index + 1 + i
            self.klines.extend(klines_to_append)

        else:
            # --- 直接追加所有找到的新K线及之后的部分 ---
            klines_to_append = increment_klines
            for i, k in enumerate(klines_to_append):
                # 从 last_index + 1 开始设置新索引
                k.index = last_index + 1 + i
            self.klines.extend(klines_to_append)

        # 返回增量数据，此时其 index 已经过修正
        return increment_klines