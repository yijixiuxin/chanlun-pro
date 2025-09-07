# -*- coding: utf-8 -*-
"""
缠论分析核心实现
基于缠论技术分析理论的量化分析实现

主要功能：
1. K线数据处理和合并
2. 分型识别和标记
3. 笔、线段、走势段的计算
4. 中枢识别和分析
5. 买卖点标记
6. 背驰分析

作者：重构版本
版本：1.0.0
"""

import datetime
import copy
import logging
from typing import Dict, List, Union, Tuple, Any, Optional
import pandas as pd
import numpy as np
from talib import abstract

from chanlun.cl_interface import (
    ICL, Kline, CLKline, FX, BI, XD, ZS, MMD, BC, TZXL, XLFX,
    Config, LINE, MACD_INFOS,
    query_macd_ld, compare_ld_beichi, user_custom_mmd
)
import logging
import copy
from typing import List, Tuple, Optional

from chanlun.segment import identify_segments

# 配置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CL(ICL):
    """
    缠论分析主类
    实现缠论的完整分析流程，包括K线处理、分型识别、笔线段计算、中枢分析等
    """

    def __init__(
            self,
            code: str,
            frequency: str,
            config: Union[dict, None] = None,
            start_datetime: datetime.datetime = None,
    ):
        """
        初始化缠论分析器

        Args:
            code: 标的代码
            frequency: 分析周期
            config: 配置参数字典
            start_datetime: 开始分析时间
        """
        self.code = code
        self.frequency = frequency
        self.config = config if config else {}
        self.start_datetime = start_datetime

        # 设置默认配置
        self._init_default_config()

        # 存储各级别数据
        self.src_klines: List[Kline] = []  # 原始K线
        self.cl_klines: List[CLKline] = []  # 缠论K线、包含关系处理后的K线
        self.idx: Dict = {'macd': {'dif': [], 'dea': [], 'hist': []}}  # 技术指标
        self.fxs: List[FX] = []  # 分型列表
        self.bis: List[BI] = []  # 笔列表
        self.xds: List[XD] = []  # 线段列表
        self.zsds: List[XD] = []  # 走势段列表
        self.qsds: List[XD] = []  # 趋势段列表
        # 中枢数据
        self.bi_zss: Dict[str, List[ZS]] = {}  # 笔中枢
        self.xd_zss: Dict[str, List[ZS]] = {}  # 线段中枢
        self.zsd_zss: List[ZS] = []  # 走势段中枢
        self.qsd_zss: List[ZS] = []  # 趋势段中枢

        # 最后中枢缓存
        self._last_bi_zs: Union[ZS, None] = None
        self._last_xd_zs: Union[ZS, None] = None

        # 处理状态标记
        self._last_kline_index = -1

    def _init_default_config(self):
        """初始化默认配置参数"""
        default_config = {
            # K线类型配置
            'kline_type': Config.KLINE_TYPE_DEFAULT.value,
            'kline_qk': Config.KLINE_QK_NONE.value,

            # 分型配置
            'fx_qy': Config.FX_QY_THREE.value,
            'fx_qj': Config.FX_QJ_CK.value,
            'fx_bh': Config.FX_BH_NO.value,

            # 笔配置
            'bi_type': Config.BI_TYPE_NEW.value,
            'bi_bzh': Config.BI_BZH_YES.value,
            'bi_qj': Config.BI_QJ_DD.value,
            'bi_fx_cgd': Config.BI_FX_CHD_NO.value,

            # 线段配置
            'xd_qj': Config.XD_QJ_DD.value,
            'xd_bzh': Config.ZSD_BZH_YES.value,
            'xd_bi_pohuai': Config.XD_BI_POHUAI_NO.value,

            # 中枢配置
            'zs_type_bi': Config.ZS_TYPE_DN.value,
            'zs_type_xd': Config.ZS_TYPE_DN.value,
            'zs_qj': Config.ZS_QJ_DD.value,
            'zs_cd': Config.ZS_CD_THREE.value,
            'zs_wzgx': Config.ZS_WZGX_ZGGDD.value,

            # 其他配置
            'cal_last_zs': True,  # 是否计算最后中枢
            'use_macd_ld': True,  # 是否使用MACD力度
        }

        for key, value in default_config.items():
            if key not in self.config:
                self.config[key] = value

    def process_klines(self, klines: pd.DataFrame):
        """
        处理K线数据，计算缠论分析结果
        支持增量更新

        Args:
            klines: K线数据DataFrame，需包含date,high,low,open,close,volume列

        Returns:
            self: 返回自身以支持链式调用
        """
        if klines is None or len(klines) == 0:
            return self

        # 数据预处理
        klines = self._preprocess_klines(klines)

        # 转换为内部格式
        new_klines = self._convert_to_klines(klines)

        # 增量处理：只处理新增的K线
        if len(new_klines) == 0:
            return self

        # 更新原始K线数据
        self._update_src_klines(new_klines)

        # 处理缠论K线（包含处理）
        self._process_cl_klines()

        # 计算技术指标
        self._calculate_indicators()

        # 识别分型
        self._identify_fractals()

        # 计算笔
        self._calculate_bis()

        # 计算线段
        self._calculate_xds()

        # 计算走势段和趋势段
        # self._calculate_zsds_and_qsds()

        # # 计算中枢
        self._calculate_zss()
        #
        # # 计算买卖点和背驰
        # self._calculate_mmds_and_bcs()

        return self

    def _preprocess_klines(self, klines: pd.DataFrame) -> pd.DataFrame:
        """预处理K线数据"""
        klines = klines.copy()

        # 确保date列是datetime类型
        if 'date' in klines.columns and not pd.api.types.is_datetime64_any_dtype(klines['date']):
            klines['date'] = pd.to_datetime(klines['date'])

        # 确保数值列是float类型
        numeric_cols = ['high', 'low', 'open', 'close', 'volume']
        for col in numeric_cols:
            if col in klines.columns:
                klines[col] = pd.to_numeric(klines[col], errors='coerce')

        # 排序
        klines = klines.sort_values('date').reset_index(drop=True)

        # 过滤开始时间
        if self.start_datetime:
            klines = klines[klines['date'] >= self.start_datetime]

        return klines

    def _convert_to_klines(self, df: pd.DataFrame) -> List[Kline]:
        """将DataFrame转换为Kline对象列表"""
        klines = []
        start_index = len(self.src_klines)

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

    def _update_src_klines(self, new_klines: List[Kline]):
        """更新原始K线数据"""
        if len(new_klines) == 0:
            return

        # 如果有重叠，更新最后一根K线，添加新的K线
        if len(self.src_klines) > 0:
            # 检查是否有重叠
            last_date = self.src_klines[-1].date
            for i, kline in enumerate(new_klines):
                if kline.date == last_date:
                    # 更新最后一根K线
                    self.src_klines[-1] = kline
                    # 添加后续新K线
                    self.src_klines.extend(new_klines[i + 1:])
                    break
                elif kline.date > last_date:
                    # 添加所有新K线
                    self.src_klines.extend(new_klines[i:])
                    break
        else:
            # 第一次处理
            self.src_klines = new_klines

    def _process_cl_klines(self):
        """
        遍历原始K线，进行包含关系处理。
        Iterate through the original K-lines to handle inclusion relationships.
        """
        if len(self.src_klines) == 0:
            return

        # 逐根处理原始K线 (Process original K-lines one by one)
        for i in range(len(self.src_klines)):
            current_k = self.src_klines[i]

            # 将原始K线包装成临时的缠论K线对象
            # (Wrap the original K-line into a temporary Chanlun K-line object)
            cl_k = CLKline(
                k_index=current_k.index, date=current_k.date, h=current_k.h, l=current_k.l,
                o=current_k.o, c=current_k.c, a=current_k.a, klines=[current_k],
                index=len(self.cl_klines), _n=1
            )

            # 如果是第一根K线，直接放入结果列表
            # (If it's the first K-line, add it directly to the results list)
            if not self.cl_klines:
                self.cl_klines.append(cl_k)
                continue

            last_cl_k = self.cl_klines[-1]

            # 检查是否有缺口，有缺口则不进行包含处理
            # (Check for gaps; if a gap exists, do not process for inclusion)
            has_gap = cl_k.l > last_cl_k.h or cl_k.h < last_cl_k.l
            if has_gap:
                cl_k.q = True
                self.cl_klines.append(cl_k)
                continue

            # 判断是否需要与前一根处理过的K线合并
            # (Determine if it needs to be merged with the previously processed K-line)
            if self._need_merge(last_cl_k, cl_k):
                # --- 核心移植逻辑开始 (Core logic start) ---
                # 1. 确定合并方向 (1. Determine the merge direction)
                direction = 'up'  # 默认向上 (Default to 'up')
                if last_cl_k.up_qs is not None:
                    # 如果上一根合并K线已经有方向，则继承该方向
                    # (If the last merged K-line already has a direction, inherit it)
                    direction = last_cl_k.up_qs
                elif len(self.cl_klines) >= 2:
                    # 标准情况：比较最后两根已处理K线的高点来定方向
                    # (Standard case: compare the high points of the last two processed K-lines to set the direction)
                    prev_prev_k = self.cl_klines[-2]
                    if last_cl_k.h < prev_prev_k.h:
                        direction = 'down'
                else:
                    # 边缘情况：第一次发生包含关系，根据两根K线的高低点关系来确定初始方向
                    # (Edge case: First time an inclusion relationship occurs, determine the initial direction based on the high/low points of the two K-lines)
                    # 规则：比较高点，高点相同则比较低点。这反映了价格的最新“努力”方向。
                    # (Rule: Compare highs; if highs are equal, compare lows. This reflects the latest "effort" direction of the price.)
                    if cl_k.h > last_cl_k.h:
                        direction = 'up'
                    elif cl_k.h < last_cl_k.h:
                        direction = 'down'
                    else:  # 高点相同 (Highs are the same)
                        if cl_k.l > last_cl_k.l:
                            direction = 'up'
                        else:  # 低点相同或更低 (Lows are the same or lower)
                            direction = 'down'

                # 2. 调用合并函数，并传入确定的方向
                # (2. Call the merge function and pass the determined direction)
                merged_k = self._merge_klines(last_cl_k, cl_k, direction)
                self.cl_klines[-1] = merged_k  # 替换掉最后一根K线 (Replace the last K-line)

            else:
                # 如果没有包含关系，直接添加新K线
                # (If there's no inclusion relationship, add the new K-line directly)
                self.cl_klines.append(cl_k)

    def _need_merge(self, k1: CLKline, k2: CLKline) -> bool:
        """
        判断两根缠论K线是否存在包含关系
        (Determine if an inclusion relationship exists between two Chanlun K-lines)
        """
        k1_contains_k2 = k1.h >= k2.h and k1.l <= k2.l
        k2_contains_k1 = k2.h >= k1.h and k2.l <= k1.l
        return k1_contains_k2 or k2_contains_k1

    def _merge_klines(self, k1: CLKline, k2: CLKline, direction: str) -> CLKline:
        """
        合并两根缠论K线。
        修改点：
        - 向上合并时，date 和 k_index 来源于价格最高(h)的那根 K 线。
        - 向下合并时，date 和 k_index 来源于价格最低(l)的那根 K 线。

        Merges two Chanlun K-lines.
        Modification:
        - When merging upwards, date and k_index come from the K-line with the highest price (h).
        - When merging downwards, date and k_index come from the K-line with the lowest price (l).
        """
        if direction == 'up':
            # 向上合并: 取 高-高, 低-高 (Upward merge: take higher-high, higher-low)
            h = max(k1.h, k2.h)
            l = max(k1.l, k2.l)
            # 根据价格最高的K线确定日期和索引 (Determine date and index based on the K-line with the highest price)
            if k1.h > k2.h:
                date = k1.date
                k_index = k1.k_index
            else:  # k2.h >= k1.h, 在价格相同时，优先选择更新的K线 (if prices are equal, prefer the newer K-line)
                date = k2.date
                k_index = k2.k_index
        else:  # direction == 'down'
            # 向下合并: 取 高-低, 低-低 (Downward merge: take lower-high, lower-low)
            h = min(k1.h, k2.h)
            l = min(k1.l, k2.l)
            # 根据价格最低的K线确定日期和索引 (Determine date and index based on the K-line with the lowest price)
            if k1.l < k2.l:
                date = k1.date
                k_index = k1.k_index
            else:  # k2.l <= k1.l, 在价格相同时，优先选择更新的K线 (if prices are equal, prefer the newer K-line)
                date = k2.date
                k_index = k2.k_index

        merged = CLKline(
            k_index=k_index,  # 根据新规则更新 (Updated according to the new rule)
            date=date,  # 根据新规则更新 (Updated according to the new rule)
            h=h,
            l=l,
            o=k1.o,  # 开盘价为序列中第一根的开盘价 (Open is from the first K-line in the sequence)
            c=k2.c,  # 收盘价为序列中最后一根的收盘价 (Close is from the last K-line in the sequence)
            a=k1.a + k2.a,
            klines=k1.klines + k2.klines,  # 合并原始K线列表 (Merge the list of original K-lines)
            index=k1.index,  # 在cl_klines列表中的索引保持不变 (Index in the cl_klines list remains the same)
            _n=k1.n + k2.n,  # 累加合并的K线数量 (Accumulate the number of merged K-lines)
            _q=k1.q  # 合并后的K线继承第一根K线的缺口状态 (The merged K-line inherits the gap status of the first K-line)
        )

        # 将本次合并的方向记录下来，供后续的包含关系判断使用
        # (Record the direction of this merge for subsequent inclusion relationship checks)
        merged.up_qs = direction
        return merged

    def _calculate_indicators(self):
        """计算技术指标"""
        if len(self.cl_klines) < 26:
            return

        # 提取价格数据
        close_prices = np.array([k.c for k in self.cl_klines])

        # 计算MACD
        macd_result = abstract.MACD(close_prices)

        self.idx['macd'] = {
            'dif': macd_result[0].tolist(),
            'dea': macd_result[1].tolist(),
            'hist': macd_result[2].tolist()
        }

    def _identify_fractals(self):
        """
        在处理后的缠论K线序列中识别顶分型和底分型。
        这是构建笔的基础。
        """
        # 序列长度必须大于等于3才能形成分型
        if len(self.cl_klines) < 3:
            print("K线数量不足3根，无法形成分型。")
            return []

        # 遍历所有K线，检查每三根相邻K线是否构成一个分型
        # 我们检查的窗口是 [i-1, i, i+1]，所以i的范围是从1到倒数第二根
        for i in range(1, len(self.cl_klines) - 1):
            prev_k = self.cl_klines[i - 1]
            curr_k = self.cl_klines[i]
            next_k = self.cl_klines[i + 1]

            # --- 顶分型判断 (Top Fractal Condition) ---
            # 缠论标准定义:
            # 1. 中间K线的最高点，是三者中的【严格】最高点。
            # 2. 中间K线的最低点，也是三者中的【严格】最高点。
            is_top_fractal = (curr_k.h > prev_k.h and curr_k.h > next_k.h) and \
                             (curr_k.l > prev_k.l and curr_k.l > next_k.l)

            if is_top_fractal:
                fx = FX(
                    _type='ding',
                    k=curr_k,
                    klines=[prev_k, curr_k, next_k],
                    val=curr_k.h
                )
                self.fxs.append(fx)
                continue  # 找到顶分型后，这三根K线不能再构成底分型，跳过

            # --- 底分型判断 (Bottom Fractal Condition) ---
            # 缠论标准定义:
            # 1. 中间K线的最低点，是三者中的【严格】最低点。
            # 2. 中间K线的最高点，也是三者中的【严格】最低点。
            is_bottom_fractal = (curr_k.l < prev_k.l and curr_k.l < next_k.l) and \
                                (curr_k.h < prev_k.h and curr_k.h < next_k.h)

            if is_bottom_fractal:
                fx = FX(
                    _type='di',
                    k=curr_k,
                    klines=[prev_k, curr_k, next_k],
                    val=curr_k.l
                )
                self.fxs.append(fx)

        print(f"分型识别完成，共找到 {len(self.fxs)} 个分型。")

    def _calculate_bis(self):
        """
        根据严格的缠论规则计算笔。
        该过程通过循环迭代，不断处理和筛选分型，直至分型序列稳定。
        处理步骤包括：
        1. 合并同类型的相邻分型，保留极值。
        2. 若相邻分型K线数不足，进行取舍。
        """
        print(f"开始识别笔")
        if len(self.fxs) < 2:
            self.bis = []
            return

        # 使用索引标记需要保留的分型，避免频繁的列表操作
        processed_fxs = list(self.fxs)
        fx_count = len(processed_fxs)

        # 优化：使用标记数组替代直接删除，提升性能
        keep_flags = [True] * fx_count

        max_iterations = 100  # 防止无限循环
        iteration = 0

        while iteration < max_iterations:
            was_modified = False
            iteration += 1

            # 步骤 1: 合并同一类型的相邻分型（使用标记方式）
            i = 0
            while i < fx_count - 1:
                if not keep_flags[i]:
                    i += 1
                    continue

                # 找到下一个有效的分型
                j = i + 1
                while j < fx_count and not keep_flags[j]:
                    j += 1

                if j >= fx_count:
                    break

                f1 = processed_fxs[i]
                f2 = processed_fxs[j]

                # 缓存属性访问，减少开销
                f1_type = f1.type
                f2_type = f2.type

                if f1_type == f2_type:
                    was_modified = True
                    f1_val = f1.val
                    f2_val = f2.val

                    if f1_type == 'ding':  # 同为顶分型
                        if f2_val > f1_val:
                            keep_flags[i] = False
                            i = j
                        else:
                            keep_flags[j] = False
                    else:  # 同为底分型
                        if f2_val < f1_val:
                            keep_flags[i] = False
                            i = j
                        else:
                            keep_flags[j] = False
                else:
                    i = j

            # 步骤 2: 处理分型间K线数量不足的情况
            i = 0
            while i < fx_count - 1:
                if not keep_flags[i]:
                    i += 1
                    continue

                # 找到下一个有效的分型
                j = i + 1
                while j < fx_count and not keep_flags[j]:
                    j += 1

                if j >= fx_count:
                    break

                f1 = processed_fxs[i]
                f2 = processed_fxs[j]

                # 缓存K线索引，减少属性访问
                k_index_diff = abs(f1.k.k_index - f2.k.k_index)

                if k_index_diff < 4:
                    was_modified = True
                    f1_type = f1.type
                    f1_val = f1.val
                    f2_val = f2.val

                    if f1_type == 'ding':  # 前一个是顶
                        if f1_val > f2_val:
                            keep_flags[j] = False
                        else:
                            keep_flags[i] = False
                            i = j
                    else:  # 前一个是底
                        if f1_val < f2_val:
                            keep_flags[j] = False
                        else:
                            keep_flags[i] = False
                            i = j
                else:
                    i = j

            if not was_modified:
                break

            # 重建有效分型列表（只在必要时进行）
            if was_modified:
                new_processed_fxs = []
                for idx in range(fx_count):
                    if keep_flags[idx]:
                        new_processed_fxs.append(processed_fxs[idx])

                processed_fxs = new_processed_fxs
                fx_count = len(processed_fxs)
                keep_flags = [True] * fx_count

        # 从最终稳定后的分型序列构建笔
        self.bis = []
        bis_list = []  # 先构建列表，最后一次性赋值

        # 预分配默认配置值，减少字典查找
        default_zs_type = self.config.get('zs_type_bi')

        for i in range(len(processed_fxs) - 1):
            start_fx = processed_fxs[i]
            end_fx = processed_fxs[i + 1]

            # 缓存类型值
            start_type = start_fx.type
            end_type = end_fx.type

            # 理论上处理完后类型肯定不同，为保险起见增加检查
            if start_type == end_type:
                continue

            bi_type = 'up' if start_type == 'di' else 'down'

            # 缓存值，减少属性访问
            start_val = start_fx.val
            end_val = end_fx.val

            bi = BI(
                start=start_fx,
                end=end_fx,
                _type=bi_type,
                index=len(bis_list),
                default_zs_type=default_zs_type
            )

            if bi_type == 'up':
                bi.high = end_val
                bi.low = start_val
            else:
                bi.high = start_val
                bi.low = end_val

            bi.zs_high = bi.high
            bi.zs_low = bi.low
            bis_list.append(bi)

        self.bis = bis_list
        print(f"笔识别完成，共找到 {len(self.bis)} 个笔。")

    # === 辅助函数 ===
    def _get_bi_high(self, bi: BI) -> float:
        """获取笔的最高价"""
        if bi.high:
            return bi.high
        return max(bi.start.val, bi.end.val)

    def _get_bi_low(self, bi: BI) -> float:
        """获取笔的最低价"""
        if bi.low:
            return bi.low
        return min(bi.start.val, bi.end.val)

    def _check_bi_overlap(self, bi1: BI, bi2: BI) -> bool:
        """检查两笔的价格区间是否有重叠"""
        low1, high1 = self._get_bi_low(bi1), self._get_bi_high(bi1)
        low2, high2 = self._get_bi_low(bi2), self._get_bi_high(bi2)
        return max(low1, low2) < min(high1, high2)

    def _check_bi_inclusion(self, bi1: BI, bi2: BI) -> bool:
        """检查两笔是否存在包含关系"""
        high1, low1 = self._get_bi_high(bi1), self._get_bi_low(bi1)
        high2, low2 = self._get_bi_high(bi2), self._get_bi_low(bi2)
        bi1_contains_bi2 = high1 >= high2 and low1 <= low2
        bi2_contains_bi1 = high2 >= high1 and low2 <= low1
        return bi1_contains_bi2 or bi2_contains_bi1

    def _find_critical_bi_and_truncate(self, all_bis: List[BI]) -> int:
        """找到一个关键的笔作为分析起点，并返回其索引"""
        if len(all_bis) < 5:
            return 0

        for i in range(len(all_bis) - 4):
            bi_i = all_bis[i]
            bi_i_plus_2 = all_bis[i + 2]
            bi_i_plus_4 = all_bis[i + 4]
            is_critical = False

            if bi_i.type == 'down':
                is_start_higher = (self._get_bi_high(bi_i) > self._get_bi_high(bi_i_plus_2) and
                                   self._get_bi_high(bi_i) > self._get_bi_high(bi_i_plus_4))
                is_end_higher = (self._get_bi_low(bi_i) > self._get_bi_low(bi_i_plus_2) and
                                 self._get_bi_low(bi_i) > self._get_bi_low(bi_i_plus_4))
                if is_start_higher and is_end_higher:
                    is_critical = True
            elif bi_i.type == 'up':
                is_start_lower = (self._get_bi_low(bi_i) < self._get_bi_low(bi_i_plus_2) and
                                  self._get_bi_low(bi_i) < self._get_bi_low(bi_i_plus_4))
                is_end_lower = (self._get_bi_high(bi_i) < self._get_bi_high(bi_i_plus_2) and
                                self._get_bi_high(bi_i) < self._get_bi_high(bi_i_plus_4))
                if is_start_lower and is_end_lower:
                    is_critical = True

            if is_critical:
                logging.info(f"在索引 {i} 处找到关键笔。从此开始分析。")
                return i

        logging.warning("未找到关键笔。从索引 0 开始分析。")
        return 0

    def _get_characteristic_sequence(self, segment_bis: List[BI], segment_type: str) -> List[BI]:
        """从线段的笔列表中获取其特征序列"""
        cs_type = 'down' if segment_type == 'up' else 'up'
        return [bi for bi in segment_bis if bi.type == cs_type]

    def _process_inclusion(self, bis: Union[List[BI], List[dict]], direction: str) -> List[dict]:
        """对特征序列进行包含关系处理"""
        if not bis:
            return []

        # 判断是否需要转换
        if isinstance(bis[0], dict):
            processed = bis[:]
        else:
            processed = [self._bi_to_dict(bi) for bi in bis]

        if len(processed) < 2:
            return processed

        i = 0
        while i < len(processed) - 1:
            bi1 = processed[i]
            bi2 = processed[i + 1]

            if self._check_inclusion_dict(bi1, bi2):
                high1, low1 = bi1['high'], bi1['low']
                high2, low2 = bi2['high'], bi2['low']

                if direction == 'down':
                    new_low = min(low1, low2)
                    new_high = min(high1, high2)
                else:
                    new_high = max(high1, high2)
                    new_low = max(low1, low2)

                merged = {
                    'bi': bi1['bi'],
                    'high': new_high,
                    'low': new_low,
                    'type': bi1['type'],
                    'is_merged': True,
                    'original_bis': bi1.get('original_bis', [bi1['bi']]) + bi2.get('original_bis', [bi2['bi']])
                }

                processed[i] = merged
                del processed[i + 1]
            else:
                i += 1

        return processed

    def _bi_to_dict(self, bi: BI) -> dict:
        """将BI对象转换为字典格式"""
        return {
            'bi': bi,
            'high': self._get_bi_high(bi),
            'low': self._get_bi_low(bi),
            'type': bi.type
        }

    def _check_inclusion_dict(self, bi1: dict, bi2: dict) -> bool:
        """检查字典格式的两笔是否存在包含关系"""
        high1, low1 = bi1['high'], bi1['low']
        high2, low2 = bi2['high'], bi2['low']
        bi1_contains_bi2 = high1 >= high2 and low1 <= low2
        bi2_contains_bi1 = high2 >= high1 and low2 <= low1
        return bi1_contains_bi2 or bi2_contains_bi1

    def _check_top_fractal(self, processed_cs: List[dict]) -> tuple:
        """在特征序列中检查顶分型"""
        if len(processed_cs) < 3:
            return False, None, None

        for i in range(len(processed_cs) - 2):
            cs1, cs2, cs3 = processed_cs[i:i + 3]
            h2 = cs2['high']
            is_high_highest = h2 >= cs1['high'] and h2 >= cs3['high']
            if is_high_highest:
                return True, cs2, cs3

        return False, None, None

    def _check_bottom_fractal(self, processed_cs: List[dict]) -> tuple:
        """在特征序列中检查底分型"""
        if len(processed_cs) < 3:
            return False, None, None

        for i in range(len(processed_cs) - 2):
            cs1, cs2, cs3 = processed_cs[i:i + 3]
            l2 = cs2['low']
            is_low_lowest = l2 <= cs1['low'] and l2 <= cs3['low']
            if is_low_lowest:
                return True, cs2, cs3

        return False, None, None

    def _get_segment_end_bi_from_middle_cs(self, middle_cs: dict, all_bis: List[BI]) -> Optional[BI]:
        """根据分型的中间笔确定线段的结束笔 - 优化版本"""
        target_bi = None

        # 检查中间笔是否为合并笔
        if middle_cs.get('is_merged') and 'original_bis' in middle_cs:
            original_bis = middle_cs.get('original_bis', [])
            if original_bis:
                target_bi = original_bis[0]
            else:
                logging.warning("中间笔为合并笔，但其 'original_bis' 为空。")
                return None
        else:
            target_bi = middle_cs['bi']

        if not target_bi:
            logging.error("无法确定用于定位的目标笔。")
            return None

        try:
            # 优化：使用列表的 index 方法查找目标笔
            idx = target_bi.index
            logging.info(f"根据分型的中间笔确定线段的结束笔，idx:{idx}")
            if idx > 0:
                return all_bis[idx - 1]
            else:
                logging.warning("目标笔是列表中的第一根笔，无法找到其前一笔。")
                return None
        except ValueError:
            logging.error("目标笔在 all_bis 列表中未找到。")
            return None

    def _get_extremum_bi_from_cs(self, cs_bi: dict) -> BI:
        """从特征序列笔中获取关键的原始笔"""
        original_bis = cs_bi.get('original_bis', [cs_bi['bi']])
        return original_bis[0] if original_bis else cs_bi['bi']

    def _calculate_xds(self):
        """根据笔列表计算线段（优化版本）"""
        logging.info("开始划分线段 (优化版)...")

        def convert_bi_to_strokes(bi_list):
            """
            将BI对象列表转换为指定的数据结构

            Args:
                bi_list: List[BI] - BI对象列表

            Returns:
                dict: 包含completed_strokes和pending_stroke的字典
            """
            result = {
                'completed_strokes': [],
                'pending_stroke': None
            }

            for bi in bi_list:
                # 构建stroke字典
                stroke = {
                    'start': {
                        'k_index': bi.start.k.k_index if hasattr(bi.start.k, 'k_index') else bi.start.index,
                        'price': bi.start.val,
                        'timestamp': bi.start.k.date,
                        'type': bi.start.type  # 'top' or 'bottom'
                    },
                    'type': bi.type  # 'up' or 'down'
                }

                # 检查笔是否完成
                if bi.is_done():
                    # 已完成的笔，添加end信息
                    stroke['end'] = {
                        'k_index': bi.end.k.k_index if hasattr(bi.end.k, 'k_index') else bi.end.index,
                        'price': bi.end.val,
                        'timestamp': bi.end.k.date,
                        'type': bi.end.type  # 'top' or 'bottom'
                    }
                    result['completed_strokes'].append(stroke)
                else:
                    # 未完成的笔（通常是最后一笔）
                    stroke['end'] = None
                    result['pending_stroke'] = stroke

            return result

        # all_bis = convert_bi_to_strokes(self.bis)
        # segments_data = identify_segments(all_bis)

        all_bis = self.bis
        current_list_index = self._find_critical_bi_and_truncate(all_bis)

        if len(all_bis) < 3:
            logging.warning("笔的数量少于3，无法形成线段。")
            return

        next_segment_builder = None

        # 主循环
        while current_list_index <= len(all_bis) - 3:

            # 使用 builder 构建下一段
            if next_segment_builder:
                logging.debug(f"主循环: 使用 builder 构建新线段")
                start_bi = next_segment_builder['start_bi']
                end_bi = next_segment_builder['end_bi']

                # 使用安全的索引查找方法
                start_idx = start_bi.index
                end_idx = end_bi.index

                # 验证索引的有效性
                if start_idx < 0 or end_idx < 0:
                    logging.error(f"Builder 中的笔索引无效: start_idx={start_idx}, end_idx={end_idx}")
                    current_list_index += 1
                    next_segment_builder = None
                    continue

                if end_idx < start_idx + 2:
                    logging.info("Builder 信息不足以构成三笔，退回标准模式。")
                    current_list_index = start_idx + 1
                    next_segment_builder = None
                    continue

                current_segment_bis = all_bis[start_idx: end_idx + 1]
                current_segment = {'bis': current_segment_bis, 'type': next_segment_builder['next_segment_type']}
                current_list_index = start_idx
                next_segment_builder = None

            else:
                # 标准模式
                s1, s2, s3 = all_bis[current_list_index:current_list_index + 3]

                if not self._check_bi_overlap(s1, s3):
                    current_list_index += 1
                    continue

                current_segment = {'bis': [s1, s2, s3], 'type': s1.type}

            # 检查线段的延伸和结束
            next_check_idx = current_list_index + len(current_segment['bis'])
            is_completed = False
            break_info = None

            while next_check_idx + 1 < len(all_bis):
                segment_high = max(self._get_bi_high(bi) for bi in current_segment['bis'])
                segment_low = min(self._get_bi_low(bi) for bi in current_segment['bis'])

                bi_for_fractal_check = all_bis[next_check_idx]
                bi_for_extension_check = all_bis[next_check_idx + 1]

                # 处理上升线段
                if current_segment['type'] == 'up':
                    if self._get_bi_high(bi_for_extension_check) > segment_high:
                        current_segment['bis'].extend([bi_for_fractal_check, bi_for_extension_check])
                        next_check_idx += 2
                        continue
                    else:
                        # 检查是否结束
                        cs_existing_raw = self._get_characteristic_sequence(current_segment['bis'], 'up')
                        if not cs_existing_raw:
                            current_segment['bis'].extend([bi_for_fractal_check, bi_for_extension_check])
                            next_check_idx += 2
                            continue

                        last_cs_bi = self._process_inclusion(cs_existing_raw, 'up')[-1]
                        last_cs_original_bi = last_cs_bi['bi']

                        # 查找边界
                        lookahead_bis = all_bis[next_check_idx:]
                        bounded_lookahead_bis = []
                        for bi in lookahead_bis:
                            bounded_lookahead_bis.append(bi)
                            if bi.type == 'up' and self._get_bi_high(bi) > segment_high:
                                break

                        # 检查顶分型
                        if self._check_bi_overlap(bi_for_fractal_check, last_cs_original_bi):
                            cs_existing_raw = self._process_inclusion(cs_existing_raw, 'down')
                            processed_cs_existing = self._process_inclusion(cs_existing_raw, 'up')
                            new_cs_down_raw = [bi for bi in bounded_lookahead_bis if bi.type == 'down']
                            processed_cs_new = self._process_inclusion(new_cs_down_raw, 'up')
                            final_processed_cs = ([processed_cs_existing[-1]] + processed_cs_new
                                                  if processed_cs_existing else processed_cs_new)

                            check1_passes, cs_middle, cs_right = self._check_top_fractal(final_processed_cs)
                            if check1_passes:
                                segment_end_bi = self._get_segment_end_bi_from_middle_cs(cs_middle, all_bis)
                                if segment_end_bi:  # 确保不为None
                                    is_completed = True
                                    peak_bi = self._get_extremum_bi_from_cs(cs_middle)
                                    right_bi = self._get_extremum_bi_from_cs(cs_right)
                                    break_info = {'reason': 'top_fractal', 'next_segment_type': 'down',
                                                  'start_bi': peak_bi, 'end_bi': right_bi,
                                                  'segment_end_bi': segment_end_bi}
                        else:
                            # 双重条件检查
                            processed_cs_existing = self._process_inclusion(cs_existing_raw, 'up')
                            new_cs_down_raw = [bi for bi in bounded_lookahead_bis if bi.type == 'down']
                            processed_cs_new = self._process_inclusion(new_cs_down_raw, 'up')
                            cs_for_check1 = ([processed_cs_existing[-1]] + processed_cs_new
                                             if processed_cs_existing else processed_cs_new)

                            check1_passes, cs_middle_top, cs_right_top = self._check_top_fractal(cs_for_check1)

                            next_segment_cs_raw = [bi for bi in bounded_lookahead_bis if bi.type == 'up']
                            processed_cs2 = self._process_inclusion(next_segment_cs_raw, 'down')
                            check2_passes, _, _ = self._check_bottom_fractal(processed_cs2)

                            if check1_passes and check2_passes:
                                segment_end_bi = self._get_segment_end_bi_from_middle_cs(cs_middle_top, all_bis)
                                if segment_end_bi:  # 确保不为None
                                    is_completed = True
                                    peak_bi = self._get_extremum_bi_from_cs(cs_middle_top)
                                    right_bi_for_builder = self._get_extremum_bi_from_cs(cs_right_top)
                                    break_info = {'reason': 'dual_condition_up_break', 'next_segment_type': 'down',
                                                  'start_bi': peak_bi, 'end_bi': right_bi_for_builder,
                                                  'segment_end_bi': segment_end_bi}
                            else:
                                current_segment['bis'].extend(bounded_lookahead_bis)
                                next_check_idx += len(bounded_lookahead_bis)
                                continue

                # 处理下降线段（逻辑对称）
                elif current_segment['type'] == 'down':
                    if self._get_bi_low(bi_for_extension_check) < segment_low:
                        current_segment['bis'].extend([bi_for_fractal_check, bi_for_extension_check])
                        next_check_idx += 2
                        continue
                    else:
                        cs_existing_raw = self._get_characteristic_sequence(current_segment['bis'], 'down')
                        if not cs_existing_raw:
                            current_segment['bis'].extend([bi_for_fractal_check, bi_for_extension_check])
                            next_check_idx += 2
                            continue

                        last_cs_bi = self._process_inclusion(cs_existing_raw, 'down')[-1]
                        last_cs_original_bi = last_cs_bi['bi']
                        lookahead_bis = all_bis[next_check_idx:]
                        bounded_lookahead_bis = []

                        for bi in lookahead_bis:
                            bounded_lookahead_bis.append(bi)
                            if bi.type == 'down' and self._get_bi_low(bi) < segment_low:
                                break

                        if self._check_bi_overlap(bi_for_fractal_check, last_cs_original_bi):
                            cs_existing_raw = self._process_inclusion(cs_existing_raw, 'up')
                            processed_cs_existing = self._process_inclusion(cs_existing_raw, 'down')
                            new_cs_up_raw = [bi for bi in bounded_lookahead_bis if bi.type == 'up']
                            processed_cs_new = self._process_inclusion(new_cs_up_raw, 'down')
                            final_processed_cs = ([processed_cs_existing[-1]] + processed_cs_new
                                                  if processed_cs_existing else processed_cs_new)

                            check1_passes, cs_middle, cs_right = self._check_bottom_fractal(final_processed_cs)
                            if check1_passes:
                                segment_end_bi = self._get_segment_end_bi_from_middle_cs(cs_middle, all_bis)
                                if segment_end_bi:  # 确保不为None
                                    is_completed = True
                                    trough_bi = self._get_extremum_bi_from_cs(cs_middle)
                                    right_bi = self._get_extremum_bi_from_cs(cs_right)
                                    break_info = {'reason': 'bottom_fractal', 'next_segment_type': 'up',
                                                  'start_bi': trough_bi, 'end_bi': right_bi,
                                                  'segment_end_bi': segment_end_bi}
                        else:
                            processed_cs_existing = self._process_inclusion(cs_existing_raw, 'down')
                            new_cs_up_raw = [bi for bi in bounded_lookahead_bis if bi.type == 'up']
                            processed_cs_new = self._process_inclusion(new_cs_up_raw, 'down')
                            cs_for_check1 = ([processed_cs_existing[-1]] + processed_cs_new
                                             if processed_cs_existing else processed_cs_new)

                            check1_passes, cs_middle_bottom, cs_right_bottom = self._check_bottom_fractal(
                                cs_for_check1)

                            next_segment_cs_raw = [bi for bi in bounded_lookahead_bis if bi.type == 'down']
                            processed_cs2 = self._process_inclusion(next_segment_cs_raw, 'up')
                            check2_passes, _, _ = self._check_top_fractal(processed_cs2)

                            if check1_passes and check2_passes:
                                segment_end_bi = self._get_segment_end_bi_from_middle_cs(cs_middle_bottom, all_bis)
                                if segment_end_bi:  # 确保不为None
                                    is_completed = True
                                    trough_bi = self._get_extremum_bi_from_cs(cs_middle_bottom)
                                    right_bi_for_builder = self._get_extremum_bi_from_cs(cs_right_bottom)
                                    break_info = {'reason': 'dual_condition_down_break', 'next_segment_type': 'up',
                                                  'start_bi': trough_bi, 'end_bi': right_bi_for_builder,
                                                  'segment_end_bi': segment_end_bi}
                            else:
                                current_segment['bis'].extend(bounded_lookahead_bis)
                                next_check_idx += len(bounded_lookahead_bis)
                                continue

                # 处理完成的线段
                if is_completed and break_info:
                    final_end_bi = break_info.get('segment_end_bi')

                    if final_end_bi is None:
                        logging.error("segment_end_bi 为 None，跳过此线段")
                        current_list_index += 1
                        continue

                    final_bi_index = final_end_bi.index

                    if final_bi_index < 0 or final_bi_index < current_list_index:
                        logging.error(f"无效的索引范围: current={current_list_index}, final={final_bi_index}")
                        current_list_index += 1
                        continue

                    final_segment_bis = all_bis[current_list_index: final_bi_index + 1]

                    if not final_segment_bis:
                        logging.warning(f"线段的笔列表为空，跳过")
                        current_list_index = final_bi_index + 1
                        continue

                    # 创建XD对象
                    xd = XD(
                        start=final_segment_bis[0].start,
                        end=final_end_bi.end,
                        start_line=final_segment_bis[0],
                        end_line=final_end_bi,
                        _type=current_segment['type'],
                        index=len(self.xds),
                        default_zs_type=self.config.get('zs_type_xd', None)
                    )

                    xd.high = max(self._get_bi_high(bi) for bi in final_segment_bis)
                    xd.low = min(self._get_bi_low(bi) for bi in final_segment_bis)
                    xd.zs_high = xd.high
                    xd.zs_low = xd.low
                    xd.done = True
                    self.xds.append(xd)

                    logging.debug(f"完成 {current_segment['type']} 线段，结束于索引:{final_bi_index}")

                    # 设置下一轮的起始位置
                    if break_info.get('start_bi'):
                        next_segment_builder = break_info
                        current_list_index = break_info['start_bi'].index
                        if current_list_index < 0:
                            logging.error("无法找到下一段的起始位置")
                            break
                    else:
                        current_list_index = final_bi_index + 1

                    break  # 退出内层循环

                # 处理待定线段
            if not is_completed and next_check_idx >= len(all_bis) - 1:
                    if current_segment and current_segment.get('bis'):
                        pending_xd = XD(
                            start=all_bis[current_list_index].start,
                            end=all_bis[-1].end,
                            start_line=all_bis[current_list_index],
                            end_line=all_bis[-1],
                            _type=current_segment['type'],
                            index=len(self.xds),
                            default_zs_type=self.config.get('zs_type_xd', None)
                        )
                        pending_xd.high = max(self._get_bi_high(bi) for bi in current_segment['bis'])
                        pending_xd.low = min(self._get_bi_low(bi) for bi in current_segment['bis'])
                        pending_xd.zs_high = pending_xd.high
                        pending_xd.zs_low = pending_xd.low
                        pending_xd.done = False
                        self.xds.append(pending_xd)
                    break

        logging.info(f"线段划分结束，完成 {len(self.xds)} 个线段。")

    def _calculate_zsds_and_qsds(self):
        """计算走势段和趋势段"""
        # 暂时简化：走势段等同于线段
        self.zsds = self.xds.copy()
        self.qsds = self.xds.copy()

    def _calculate_zss(self):
        """
        计算中枢 - 主入口
        (此方法的定义保持不变)
        """
        # 调用核心的中枢查找逻辑
        zss = self.create_dn_zs(Config.ZS_TYPE_BZ.value, self.xds)
        self.xd_zss[Config.ZS_TYPE_BZ.value] = zss

        # 为了兼容性，设置最后一个中枢的属性
        if zss:
            self._last_xd_zs = zss[-1]
        else:
            self._last_xd_zs = None

    def _get_line_high_low(self, line: 'LINE') -> Tuple[Optional[float], Optional[float]]:
        """
        获取线段的高低点
        """
        if not line or not hasattr(line, 'start') or not line.start or not hasattr(line, 'end') or not line.end:
            return None, None

        start_price = line.start.val
        end_price = line.end.val

        return max(start_price, end_price), min(start_price, end_price)

    def create_dn_zs(self, zs_type: str, lines: List['LINE']) -> List['ZS']:
        """
        创建段内中枢 (V3 重构版)
        该算法严格按照 "进入段 + 中枢核心(>=3段) + 离开段" 的结构来识别一个完整的中枢。
        核心逻辑: 如果当前线段(j)不回到中枢区间，则其前一线段(j-1)为离开段。
        同时，如果一个潜在中枢(3段)被第4段直接破坏，则该中枢不成立。

        Args:
            zs_type: 中枢类型 (e.g., 'xd')
            lines: 线段对象列表

        Returns:
            包含已完成中枢和最后一个未完成中枢的列表。
        """
        if len(lines) < 4:
            return []

        zss: List[ZS] = []
        i = 1  # 主循环索引，指向潜在中枢的第一个核心段

        # 主循环: 寻找新中枢的起点
        while i <= len(lines) - 3:
            # 1. --- 识别潜在的中枢核心(至少3段) ---
            seg_a = lines[i]
            seg_b = lines[i + 1]
            seg_c = lines[i + 2]

            # 检查类型是否交替 (e.g., up-down-up)
            if not (hasattr(seg_a, 'type') and hasattr(seg_b, 'type') and hasattr(seg_c, 'type') and
                    seg_a.type != seg_b.type and seg_b.type != seg_c.type):
                i += 1
                continue

            # 2. --- 建立初始重叠区间 [ZD, ZG] ---
            g_a, d_a = self._get_line_high_low(seg_a)
            g_c, d_c = self._get_line_high_low(seg_c)

            if g_a is None or d_a is None or g_c is None or d_c is None:
                i += 1
                continue

            zg = min(g_a, g_c)
            zd = max(d_a, d_c)

            if zd >= zg:
                i += 1
                continue

            # 3. --- 找到了有效的中枢起点，开始向后延伸并寻找离开段 ---
            entry_seg = lines[i - 1]
            core_lines = [seg_a, seg_b, seg_c]
            leave_seg = None

            # 内循环：处理中枢的延伸和完成
            j = i + 3
            while j < len(lines):
                current_seg = lines[j]
                g_curr, d_curr = self._get_line_high_low(current_seg)
                if g_curr is None: break

                is_breakout = d_curr > zg or g_curr < zd

                if not is_breakout:
                    # Case A: 线段未离开中枢区间，是中枢的延伸
                    core_lines.append(current_seg)
                    # 动态更新中枢区间
                    core_highs = [self._get_line_high_low(l)[0] for l in core_lines if
                                  hasattr(l, 'type') and l.type == seg_a.type and self._get_line_high_low(l)[
                                      0] is not None]
                    core_lows = [self._get_line_high_low(l)[1] for l in core_lines if
                                 hasattr(l, 'type') and l.type == seg_a.type and self._get_line_high_low(l)[
                                     1] is not None]
                    if core_highs and core_lows:
                        zg = min(core_highs)
                        zd = max(core_lows)
                    j += 1
                else:
                    # Case B: 出现离开中枢的线段 (breakout)
                    if len(core_lines) == 3:
                        # 核心逻辑：初始3段被紧接着的第4段破坏，此中枢不成立
                        i += 1  # 从下一个位置重新开始寻找
                        leave_seg = "INVALID"  # 设置哨兵值以跳出并继续外层循环
                        break

                    # 中枢有效，前一个线段是离开段
                    leave_seg = core_lines.pop()

                    # 新的中枢寻找将从当前突破线段开始
                    i = j
                    break

            if leave_seg == "INVALID":
                continue

            # 4. --- 内循环结束后，构建中枢对象 ---
            center_type = 'up' if core_lines[0].type == 'down' else 'down'
            center = ZS(zs_type=zs_type, start=entry_seg, _type=center_type, level=0)
            center.lines = core_lines
            center.line_num = len(core_lines)

            # 基于最终的核心段，精确计算 ZG, ZD, GG, DD
            all_g = [self._get_line_high_low(l)[0] for l in core_lines if self._get_line_high_low(l)[0] is not None]
            all_d = [self._get_line_high_low(l)[1] for l in core_lines if self._get_line_high_low(l)[1] is not None]
            if all_g and all_d:
                center.gg = max(all_g)
                center.dd = min(all_d)

            final_core_highs = [self._get_line_high_low(l)[0] for l in core_lines if
                                hasattr(l, 'type') and l.type == seg_a.type and self._get_line_high_low(l)[
                                    0] is not None]
            final_core_lows = [self._get_line_high_low(l)[1] for l in core_lines if
                               hasattr(l, 'type') and l.type == seg_a.type and self._get_line_high_low(l)[
                                   1] is not None]
            if final_core_highs and final_core_lows:
                center.zg = min(final_core_highs)
                center.zd = max(final_core_lows)

            center.real = True

            if leave_seg:
                # A. 找到了离开段，是已完成的中枢
                center.end = leave_seg
                center.done = True
                zss.append(center)
                # 主循环的索引 i 已经被更新为 j，直接进入下一次循环
            else:
                # B. 线段走完仍未出现离开段，是未完成的中枢
                center.end = None
                center.done = False
                zss.append(center)
                # 消耗了所有剩余线段，结束主循环
                break

        # 重新为所有找到的中枢编号
        for idx, center in enumerate(zss):
            center.index = idx

        return zss

    def _calculate_mmds_and_bcs(self):
        """计算买卖点和背驰"""
        # 计算笔的买卖点和背驰
        for bi in self.bis:
            self._calculate_bi_mmd_bc(bi)

        # 计算线段的买卖点和背驰
        for xd in self.xds:
            self._calculate_xd_mmd_bc(xd)

    def _calculate_bi_mmd_bc(self, bi: BI):
        """计算笔的买卖点和背驰"""
        if len(self.bi_zss) == 0:
            return

        # 获取相关中枢
        for zs_type, zss in self.bi_zss.items():
            if len(zss) == 0:
                continue

            # 找到与当前笔相关的中枢
            relevant_zs = None
            for zs in reversed(zss):
                if bi.index >= zs.lines[0].index:
                    relevant_zs = zs
                    break

            if not relevant_zs:
                continue

            # 判断买卖点类型
            self._identify_mmd(bi, relevant_zs, zs_type)

            # 判断背驰
            self._identify_bc(bi, relevant_zs, zs_type)

    def _calculate_xd_mmd_bc(self, xd: XD):
        """计算线段的买卖点和背驰"""
        if len(self.xd_zss) == 0:
            return

        # 获取相关中枢
        for zs_type, zss in self.xd_zss.items():
            if len(zss) == 0:
                continue

            # 找到与当前线段相关的中枢
            relevant_zs = None
            for zs in reversed(zss):
                if xd.index >= zs.lines[0].index:
                    relevant_zs = zs
                    break

            if not relevant_zs:
                continue

            # 判断买卖点类型
            self._identify_mmd(xd, relevant_zs, zs_type)

            # 判断背驰
            self._identify_bc(xd, relevant_zs, zs_type)

    def _identify_mmd(self, line: Union[BI, XD], zs: ZS, zs_type: str):
        """识别买卖点"""
        if line.type == 'down':
            # 下跌线段，可能的买点
            if line.low < zs.zd:
                # 跌破中枢，三类买点
                line.add_mmd('3buy', zs, zs_type, '跌破中枢下沿')
            elif line.low > zs.zg:
                # 回调不跌破中枢上沿，一类买点
                line.add_mmd('1buy', zs, zs_type, '回调不破中枢上沿')
            else:
                # 在中枢内部，二类买点
                line.add_mmd('2buy', zs, zs_type, '中枢内部买点')

        elif line.type == 'up':
            # 上涨线段，可能的卖点
            if line.high > zs.zg:
                # 突破中枢，三类卖点
                line.add_mmd('3sell', zs, zs_type, '突破中枢上沿')
            elif line.high < zs.zd:
                # 反弹不破中枢下沿，一类卖点
                line.add_mmd('1sell', zs, zs_type, '反弹不破中枢下沿')
            else:
                # 在中枢内部，二类卖点
                line.add_mmd('2sell', zs, zs_type, '中枢内部卖点')

    def _identify_bc(self, line: Union[BI, XD], zs: ZS, zs_type: str):
        """识别背驰"""
        if len(zs.lines) < 2:
            return

        # 找到比较的线段
        compare_line = None
        for zs_line in reversed(zs.lines[:-1]):
            if zs_line.type == line.type:
                compare_line = zs_line
                break

        if not compare_line:
            return

        # 力度比较
        line_ld = line.get_ld(self)
        compare_ld = compare_line.get_ld(self)

        is_bc = compare_ld_beichi(compare_ld, line_ld, line.type)

        if is_bc:
            line.add_bc('bi' if isinstance(line, BI) else 'xd',
                        zs, compare_line, [compare_line], True, zs_type)

    # ICL接口实现方法

    def get_code(self) -> str:
        """返回标的代码"""
        return self.code

    def get_frequency(self) -> str:
        """返回分析周期"""
        return self.frequency

    def get_config(self) -> dict:
        """返回配置参数"""
        return self.config

    def get_src_klines(self) -> List[Kline]:
        """返回原始K线列表"""
        return self.src_klines

    def get_klines(self) -> List[Kline]:
        """返回K线列表"""
        if self.config.get('kline_type') == Config.KLINE_TYPE_CHANLUN.value:
            # 返回缠论K线对应的原始K线
            result = []
            for cl_k in self.cl_klines:
                result.extend(cl_k.klines)
            return result
        else:
            return self.src_klines

    def get_cl_klines(self) -> List[CLKline]:
        """返回缠论K线列表"""
        return self.cl_klines

    def get_idx(self) -> dict:
        """返回技术指标数据"""
        return self.idx

    def get_fxs(self) -> List[FX]:
        """返回分型列表"""
        return self.fxs

    def get_bis(self) -> List[BI]:
        """返回笔列表"""
        return self.bis

    def get_xds(self) -> List[XD]:
        """返回线段列表"""
        return self.xds

    def get_zsds(self) -> List[XD]:
        """返回走势段列表"""
        return self.zsds

    def get_qsds(self) -> List[XD]:
        """返回趋势段列表"""
        return self.qsds

    def get_bi_zss(self, zs_type: str = None) -> List[ZS]:
        """返回笔中枢列表"""
        if zs_type is None:
            zs_type = self.config.get('zs_type_bi', Config.ZS_TYPE_DN.value)
        return self.bi_zss.get(zs_type, [])

    def get_xd_zss(self, zs_type: str = None) -> List[ZS]:
        """返回线段中枢列表"""
        if zs_type is None:
            zs_type = self.config.get('zs_type_xd', Config.ZS_TYPE_DN.value)
        return self.xd_zss.get(zs_type, [])

    def get_zsd_zss(self) -> List[ZS]:
        """返回走势段中枢列表"""
        return self.zsd_zss

    def get_qsd_zss(self) -> List[ZS]:
        """返回趋势段中枢列表"""
        return self.qsd_zss

    def get_last_bi_zs(self) -> Union[ZS, None]:
        """返回最后的笔中枢"""
        if not self.config.get('cal_last_zs', True):
            return None

        if self._last_bi_zs is None and len(self.bis) >= 3:
            # 基于最后几笔计算中枢
            last_bis = self.bis[-5:] if len(self.bis) >= 5 else self.bis
            zss = self.create_dn_zs('bi', last_bis)
            self._last_bi_zs = zss[-1] if zss else None

        return self._last_bi_zs

    def get_last_xd_zs(self) -> Union[ZS, None]:
        """返回最后的线段中枢"""
        if not self.config.get('cal_last_zs', True):
            return None

        if self._last_xd_zs is None and len(self.xds) >= 3:
            # 基于最后几个线段计算中枢
            last_xds = self.xds[-5:] if len(self.xds) >= 5 else self.xds
            zss = self.create_dn_zs('xd', last_xds)
            self._last_xd_zs = zss[-1] if zss else None

        return self._last_xd_zs

    def _can_form_zs(self, lines: List[LINE]) -> bool:
        """判断线段列表是否能形成中枢"""
        if len(lines) < 3:
            return False

        # 检查前三段是否有重叠
        overlaps = []
        for i in range(len(lines) - 1):
            line1 = lines[i]
            line2 = lines[i + 1]

            # 计算重叠区间
            overlap_high = min(line1.zs_high, line2.zs_high)
            overlap_low = max(line1.zs_low, line2.zs_low)

            if overlap_high > overlap_low:
                overlaps.append((overlap_low, overlap_high))
            else:
                return False

        # 检查是否有公共重叠区间
        if len(overlaps) < 2:
            return False

        common_low = max(overlap[0] for overlap in overlaps[:2])
        common_high = min(overlap[1] for overlap in overlaps[:2])

        return common_high > common_low

    def _create_zs_from_lines(self, zs_type: str, lines: List[LINE]) -> Union[ZS, None]:
        """从线段列表创建中枢"""
        if not self._can_form_zs(lines):
            return None

        # 计算中枢区间
        zg = float('-inf')
        zd = float('inf')
        gg = float('-inf')
        dd = float('inf')

        # 前三段确定中枢区间
        first_three = lines[:3]
        for line in first_three:
            zg = max(zg, min(line.zs_high, first_three[0].zs_high))
            zd = min(zd, max(line.zs_low, first_three[0].zs_low))
            gg = max(gg, line.zs_high)
            dd = min(dd, line.zs_low)

        # 重新计算精确的中枢区间
        overlaps = []
        for i in range(len(first_three) - 1):
            line1 = first_three[i]
            line2 = first_three[i + 1]
            overlap_high = min(line1.zs_high, line2.zs_high)
            overlap_low = max(line1.zs_low, line2.zs_low)
            overlaps.append((overlap_low, overlap_high))

        zd = max(overlap[0] for overlap in overlaps)
        zg = min(overlap[1] for overlap in overlaps)

        # 判断中枢类型
        if lines[0].type == 'up' and lines[-1].type == 'down':
            zs_direction = 'down'
        elif lines[0].type == 'down' and lines[-1].type == 'up':
            zs_direction = 'up'
        else:
            zs_direction = 'zd'  # 震荡

        # 创建中枢对象
        zs = ZS(
            zs_type=zs_type,
            start=lines[0].start,
            end=lines[-1].end,
            zg=zg,
            zd=zd,
            gg=gg,
            dd=dd,
            _type=zs_direction,
            index=0,  # 会在后续调整
            line_num=len(lines),
            level=0
        )

        # 添加线段到中枢
        for line in lines:
            zs.add_line(line)

        zs.done = True
        zs.real = True

        return zs

    def beichi_pz(self, zs: ZS, now_line: LINE) -> Tuple[bool, Union[LINE, None]]:
        """
        判断中枢与指定线是否构成盘整背驰

        Args:
            zs: 中枢对象
            now_line: 当前线

        Returns:
            (是否背驰, 比较的线)
        """
        if len(zs.lines) < 2:
            return False, None

        # 找到同方向的比较线
        compare_line = None
        for line in reversed(zs.lines[:-1]):
            if line.type == now_line.type:
                compare_line = line
                break

        if not compare_line:
            return False, None

        # 力度比较
        now_ld = now_line.get_ld(self)
        compare_ld = compare_line.get_ld(self)

        is_bc = compare_ld_beichi(compare_ld, now_ld, now_line.type)

        return is_bc, compare_line

    def beichi_qs(
            self, lines: List[LINE], zss: List[ZS], now_line: LINE
    ) -> Tuple[bool, List[LINE]]:
        """
        判断指定线与之前的中枢，是否形成了趋势背驰

        Args:
            lines: 线的列表
            zss: 中枢列表
            now_line: 当前线

        Returns:
            (是否背驰, 比较的线列表)
        """
        if len(zss) < 2:
            return False, []

        # 检查最后两个中枢是否形成趋势
        last_zs = zss[-1]
        prev_zs = zss[-2]

        qs_direction = self.zss_is_qs(prev_zs, last_zs)
        if not qs_direction or qs_direction != now_line.type:
            return False, []

        # 找到进入前一个中枢的同方向线段
        compare_lines = []
        for line in lines:
            if (line.type == now_line.type and
                    line.end.k.k_index <= prev_zs.start.k.k_index):
                compare_lines.append(line)

        if not compare_lines:
            return False, []

        # 取最后一个同方向线段进行比较
        compare_line = compare_lines[-1]

        # 力度比较
        now_ld = now_line.get_ld(self)
        compare_ld = compare_line.get_ld(self)

        is_bc = compare_ld_beichi(compare_ld, now_ld, now_line.type)

        return is_bc, [compare_line]

    def zss_is_qs(self, one_zs: ZS, two_zs: ZS) -> Union[str, None]:
        """
        判断两个中枢是否形成趋势

        Args:
            one_zs: 第一个中枢
            two_zs: 第二个中枢

        Returns:
            'up' 向上趋势, 'down' 向下趋势, None 无趋势
        """
        wzgx_config = self.config.get('zs_wzgx', Config.ZS_WZGX_ZGGDD.value)

        if wzgx_config == Config.ZS_WZGX_ZGD.value:
            # 宽松比较：zg与zd
            if one_zs.zg < two_zs.zd:
                return 'up'
            elif one_zs.zd > two_zs.zg:
                return 'down'
        elif wzgx_config == Config.ZS_WZGX_ZGGDD.value:
            # 较为宽松：zg与dd, zd与gg
            if one_zs.zg < two_zs.dd:
                return 'up'
            elif one_zs.zd > two_zs.gg:
                return 'down'
        elif wzgx_config == Config.ZS_WZGX_GD.value:
            # 严格比较：gg与dd
            if one_zs.gg < two_zs.dd:
                return 'up'
            elif one_zs.dd > two_zs.gg:
                return 'down'

        return None
