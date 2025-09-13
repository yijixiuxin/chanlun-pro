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
from typing import Dict, Union
import pandas as pd
import numpy as np
from talib import abstract

from chanlun.cl_interface import (
    ICL, Kline, CLKline, FX, BI, XD, ZS,
    Config, LINE, compare_ld_beichi
)
import logging
from typing import List, Tuple, Optional


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

        # 逐根处理原始K线
        for i in range(len(self.src_klines)):
            current_k = self.src_klines[i]

            # 将原始K线包装成临时的缠论K线对象
            cl_k = CLKline(
                k_index=current_k.index, date=current_k.date, h=current_k.h, l=current_k.l,
                o=current_k.o, c=current_k.c, a=current_k.a, klines=[current_k],
                index=len(self.cl_klines), _n=1
            )

            # 如果是第一根K线，直接放入结果列表
            if not self.cl_klines:
                self.cl_klines.append(cl_k)
                continue

            last_cl_k = self.cl_klines[-1]

            # 检查是否有缺口，有缺口则不进行包含处理
            has_gap = cl_k.l > last_cl_k.h or cl_k.h < last_cl_k.l
            if has_gap:
                cl_k.q = True
                self.cl_klines.append(cl_k)
                continue

            # 判断是否需要与前一根处理过的K线合并
            if self._need_merge(last_cl_k, cl_k):
                # 1. 确定合并方向 (1. Determine the merge direction)
                direction = 'up'  # 默认向上 (Default to 'up')
                if last_cl_k.up_qs is not None:
                    # 如果上一根合并K线已经有方向，则继承该方向
                    direction = last_cl_k.up_qs
                elif len(self.cl_klines) >= 2:
                    # 标准情况：比较最后两根已处理K线的高点来定方向
                    prev_prev_k = self.cl_klines[-2]
                    if last_cl_k.h < prev_prev_k.h:
                        direction = 'down'
                else:
                    # 边缘情况：第一次发生包含关系，根据两根K线的高低点关系来确定初始方向
                    # 规则：比较高点，高点相同则比较低点。这反映了价格的最新“努力”方向。
                    if cl_k.h > last_cl_k.h:
                        direction = 'up'
                    elif cl_k.h < last_cl_k.h:
                        direction = 'down'
                    else:  # 高点相同
                        if cl_k.l > last_cl_k.l:
                            direction = 'up'
                        else:  # 低点相同或更低
                            direction = 'down'

                # 2. 调用合并函数，并传入确定的方向
                merged_k = self._merge_klines(last_cl_k, cl_k, direction)
                self.cl_klines[-1] = merged_k  # 替换掉最后一根K线
            else:
                # 如果没有包含关系，直接添加新K线
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
        """
        if direction == 'up':
            # 向上合并: 取 高-高, 低-高
            h = max(k1.h, k2.h)
            l = max(k1.l, k2.l)
            # 根据价格最高的K线确定日期和索引
            if k1.h > k2.h:
                date = k1.date
                k_index = k1.k_index
            else:  # k2.h >= k1.h, 在价格相同时，优先选择更新的K线
                date = k2.date
                k_index = k2.k_index
        else:  # direction == 'down'
            # 向下合并: 取 高-低, 低-低
            h = min(k1.h, k2.h)
            l = min(k1.l, k2.l)
            # 根据价格最低的K线确定日期和索引
            if k1.l < k2.l:
                date = k1.date
                k_index = k1.k_index
            else:  # k2.l <= k1.l, 在价格相同时，优先选择更新的K线
                date = k2.date
                k_index = k2.k_index

        merged = CLKline(
            k_index=k_index,
            date=date,
            h=h,
            l=l,
            o=h,  # 开盘价为序列中第一根的开盘价
            c=l,  # 收盘价为序列中最后一根的收盘价
            a=k1.a + k2.a,
            klines=k1.klines + k2.klines,  # 合并原始K线列表
            index=k1.index,  # 在cl_klines列表中的索引保持不变
            _n=k1.n + k2.n,  # 累加合并的K线数量
            _q=k1.q  # 合并后的K线继承第一根K线的缺口状态
        )

        # 将本次合并的方向记录下来，供后续的包含关系判断使用
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

    def _find_all_potential_fractals(self):
        """扫描所有K线，找出所有理论上成立的潜在分型。"""
        potential_fxs = []
        if len(self.cl_klines) < 3:
            return potential_fxs

        for i in range(1, len(self.cl_klines) - 1):
            prev_k, curr_k, next_k = self.cl_klines[i - 1], self.cl_klines[i], self.cl_klines[i + 1]

            is_top_fractal = (curr_k.h > prev_k.h and curr_k.h > next_k.h) and \
                             (curr_k.l > prev_k.l and curr_k.l > next_k.l)
            is_bottom_fractal = (curr_k.l < prev_k.l and curr_k.l < next_k.l) and \
                                (curr_k.h < prev_k.h and curr_k.h < next_k.h)

            if is_top_fractal:
                potential_fxs.append(FX(_type='ding', k=curr_k, klines=[prev_k, curr_k, next_k], val=curr_k.h))
            elif is_bottom_fractal:
                potential_fxs.append(FX(_type='di', k=curr_k, klines=[prev_k, curr_k, next_k], val=curr_k.l))

        return potential_fxs

    def _identify_fractals(self):
        """
        根据“重叠冲突处理”规则，筛选最终有效分型。
        新规则:
        1. 将连续的、K线重叠的潜在分型视为一个“冲突组”。
        2. 在冲突组内，根据前一个已确认的分型，通过比较保留一个最优分型作为临时的“候选分型”。
        3. 当冲突组结束（即下一个分型不再重叠时），才将这个从冲突中胜出的“候选分型”加入最终列表。
        """
        potential_fxs = self._find_all_potential_fractals()

        if not potential_fxs:
            print("K线数量不足，未找到任何分型。")
            self.fxs = []
            return

        final_fxs = [potential_fxs[0]]

        if len(potential_fxs) < 2:
            self.fxs = final_fxs
            print(f"分型识别完成，共找到 {len(self.fxs)} 个有效分型。")
            return

        # 从第二个潜在分型开始，它成为我们的第一个候选者
        candidate_fx = potential_fxs[1]
        i = 2  # 指针从第三个潜在分型开始

        while i < len(potential_fxs):
            last_confirmed_fx = final_fxs[-1]
            next_potential_fx = potential_fxs[i]

            # 检查当前候选分型与下一个潜在分型是否重叠
            is_overlapping = (next_potential_fx.k.index - 1) <= (candidate_fx.k.index + 1)

            if is_overlapping:
                # --- 情况 A: 存在重叠，进入冲突解决模式 ---
                c_type = candidate_fx.type
                n_type = next_potential_fx.type

                if c_type == 'ding' and n_type == 'ding':
                    # 两个都是顶，取更高的顶
                    if next_potential_fx.val > candidate_fx.val:
                        candidate_fx = next_potential_fx

                elif c_type == 'di' and n_type == 'di':
                    # 两个都是底，取更低的底
                    if next_potential_fx.val < candidate_fx.val:
                        candidate_fx = next_potential_fx

                else:
                    # 一顶一底，应用原有的比较规则
                    competing_top = candidate_fx if c_type == 'ding' else next_potential_fx
                    competing_bottom = candidate_fx if c_type == 'di' else next_potential_fx

                    if last_confirmed_fx.type == 'ding':
                        # 前一个是顶，比较新顶和旧顶
                        if competing_top.val > last_confirmed_fx.val:
                            # if c_type == 'di': # 待定的分型为底分型时
                            #     candidate_fx = candidate_fx
                            # else:
                            candidate_fx = competing_top  # 新顶更高，候选者变为顶
                        else:
                            candidate_fx = competing_bottom  # 否则，候选者变为底
                    else:  # last_confirmed_fx.type == 'di'
                        # 前一个是底，比较新底和旧底
                        if competing_bottom.val < last_confirmed_fx.val:
                            # if c_type == 'ding': # 待定的分型为顶分型时
                            #     candidate_fx = candidate_fx
                            # else:
                            candidate_fx = competing_bottom  # 新底更低，候选者变为底
                        else:
                            candidate_fx = competing_top  # 否则，候选者变为顶

                # 解决完一个重叠，指针前进 1，继续用新的 candidate_fx 和下一个分型比较
                i += 1

            else:
                # --- 情况 B: 不重叠，冲突结束 ---
                final_fxs.append(candidate_fx)

                # 更新 candidate_fx 为当前这个不重叠的分型，它将成为下一轮比较的起点
                candidate_fx = next_potential_fx
                i += 1

        # 循环结束后，处理最后一个留下的 candidate_fx
        if candidate_fx and (not final_fxs or candidate_fx.type != final_fxs[-1].type):
            final_fxs.append(candidate_fx)

        self.fxs = final_fxs
        print(f"分型识别完成，共找到 {len(self.fxs)} 个有效分型。")

    def _check_bi_requirement(self, fx1: FX, fx2: FX) -> bool:
        """
        检查两个分型之间是否满足形成一笔的最低要求。
        规则：相邻的顶分型和底分型必须要有一根及以上除了分型K线之外的其他K线

        参数:
            fx1: 第一个分型
            fx2: 第二个分型

        返回:
            bool: True表示满足要求，False表示不满足
        """
        # 分型类型必须不同
        if fx1.type == fx2.type:
            return False

        # 获取两个分型包含的K线范围
        # 注意：分型的klines列表包含3个缠论K线
        # 需要检查这些K线之间是否有足够的间隔

        # 获取fx1的最后一个缠论K线
        fx1_last_ck = None
        for ck in reversed(fx1.klines):
            if ck is not None:
                fx1_last_ck = ck
                break

        # 获取fx2的第一个缠论K线
        fx2_first_ck = fx2.klines[0]

        if fx1_last_ck is None or fx2_first_ck is None:
            return False

        # 计算两个分型之间的K线数量
        # 使用k_index来判断（缠论K线的索引）
        k_count_between = fx2_first_ck.index - fx1_last_ck.index - 1

        # 至少需要1根K线间隔
        has_enough_gap = k_count_between >= 1

        if not has_enough_gap:
            print(f"  分型间隔不足：fx1_last_index={fx1_last_ck.index}, "
                  f"fx2_first_index={fx2_first_ck.index}, gap={k_count_between}")

        return has_enough_gap

    def _calculate_bis(self):
        """
        根据缠论规则计算笔，严格遵循用户定义的筛选和连接逻辑。
        Calculates strokes based on Chan Lun rules, strictly following user-defined
        filtering and connection logic.
        """
        if len(self.fxs) < 2:
            self.bis = []
            return

        # 步骤 1: 筛选点（分型包含处理）
        # Step 1: Filter fractals (handling inclusion cases)
        # 规则:
        #   - 顶1 -> 底1 -> 顶2, 且 顶2 > 顶1 => 删除 顶1, 底1
        #   - 底1 -> 顶1 -> 底2, 且 底2 < 底1 => 删除 底1, 顶1
        # This is an iterative process. If a fractal is removed, the sequence changes,
        # so we restart the loop to re-evaluate the new context.

        # 创建一个副本进行操作
        # Create a copy to operate on
        processed_fxs = list(self.fxs)

        while True:
            removed = False
            if len(processed_fxs) < 3:
                break

            i = 0
            while i <= len(processed_fxs) - 3:
                fx1, fx2, fx3 = processed_fxs[i], processed_fxs[i + 1], processed_fxs[i + 2]

                # 检查连续顶分型中的包含关系
                # Check for inclusion in consecutive top fractals: Top1 -> Bottom1 -> Top2
                if fx1.type == 'ding' and fx2.type == 'di' and fx3.type == 'ding':
                    if fx3.val > fx1.val:
                        # 顶2更高，顶1和中间的底1被“X掉”
                        # Top2 is higher, so Top1 and the intermediate Bottom1 are "crossed out"
                        processed_fxs.pop(i)  # remove fx1
                        processed_fxs.pop(i)  # remove fx2
                        removed = True
                        break  # 重启外层循环 (Restart the outer loop)

                # 检查连续底分型中的包含关系
                # Check for inclusion in consecutive bottom fractals: Bottom1 -> Top1 -> Bottom2
                if fx1.type == 'di' and fx2.type == 'ding' and fx3.type == 'di':
                    if fx3.val < fx1.val:
                        # 底2更低，底1和中间的顶1被“X掉”
                        # Bottom2 is lower, so Bottom1 and the intermediate Top1 are "crossed out"
                        processed_fxs.pop(i)  # remove fx1
                        processed_fxs.pop(i)  # remove fx2
                        removed = True
                        break  # 重启外层循环 (Restart the outer loop)
                i += 1

            if not removed:
                # 如果此轮循环没有删除任何元素，说明处理完毕
                # If no elements were removed in this pass, processing is complete.
                break

        # 步骤 2 & 3: 连接笔并处理剩余的同类分型
        # Steps 2 & 3: Connect strokes and handle remaining consecutive fractals
        if len(processed_fxs) < 2:
            self.bis = []
            return

        final_fxs = []
        if processed_fxs:
            final_fxs.append(processed_fxs[0])

        i = 1
        while i < len(processed_fxs):
            last_fx = final_fxs[-1]
            current_fx = processed_fxs[i]

            # 情况 A: 分型类型不同 (Case A: Different fractal types)
            if last_fx.type != current_fx.type:
                # 检查是否满足成笔的两个关键条件
                # Check if the two key conditions for forming a stroke are met
                # 条件1: K线间隔 (Condition 1: K-line gap)
                has_gap = self._check_bi_requirement(last_fx, current_fx)
                # 条件2: 高低点 (Condition 2: High/Low points)
                is_valid_stroke = (last_fx.type == 'ding' and last_fx.val > current_fx.val) or \
                                  (last_fx.type == 'di' and last_fx.val < current_fx.val)

                if has_gap and is_valid_stroke:
                    # 满足条件，连接成笔，当前分型成为新的笔端点
                    # Conditions met, connect the stroke. The current fractal becomes the new stroke endpoint.
                    final_fxs.append(current_fx)
                else:
                    # 不满足条件，发生笔的延续
                    # Conditions not met, stroke continuation occurs.
                    # 规则：用更极端的值替换掉前一个不成立的转折点
                    # Rule: Replace the previous invalid turning point with the more extreme value.
                    if (last_fx.type == 'ding' and current_fx.val > last_fx.val) or \
                            (last_fx.type == 'di' and current_fx.val < last_fx.val):
                        final_fxs[-1] = current_fx

            # 情况 B: 分型类型相同 (Case B: Same fractal types)
            # 这是处理步骤1中留下的 底A -> 顶1 -> 顶2 -> 底B (顶1>顶2) 这类情况
            # This handles cases left over from Step 1, like B_A -> T1 -> T2 -> B_B (where T1>T2)
            else:
                if last_fx.type == 'ding' and current_fx.val > last_fx.val:
                    # 一串连续顶中，取更高的那个
                    # In a series of consecutive tops, take the higher one.
                    final_fxs[-1] = current_fx
                elif last_fx.type == 'di' and current_fx.val < last_fx.val:
                    # 一串连续底中，取更低的那个
                    # In a series of consecutive bottoms, take the lower one.
                    final_fxs[-1] = current_fx
                # 如果 current_fx 不够极端 (例如 顶1 > 顶2), 它就会被自然忽略
                # If current_fx is not more extreme (e.g., T1 > T2), it is naturally ignored.

            i += 1

        # 步骤 4: 根据最终确认的分型点构建笔列表
        # Step 4: Build the list of strokes from the finally confirmed fractal points
        self.bis = []
        if len(final_fxs) >= 2:
            for j in range(len(final_fxs) - 1):
                start_fx = final_fxs[j]
                end_fx = final_fxs[j + 1]

                if start_fx.type == end_fx.type:
                    continue

                bi_type = 'up' if start_fx.type == 'di' else 'down'
                new_bi = BI(start=start_fx, end=end_fx, _type=bi_type, index=len(self.bis))
                self.bis.append(new_bi)

        # 步骤 5: 处理待定笔 (Step 5: Handle pending stroke)
        # 待定笔是指从最后一个确认的分型点到最新一个分型点之间可能形成的笔。
        # A pending stroke is a potential stroke forming from the last confirmed fractal
        # point to the most recent fractal point.
        if final_fxs and processed_fxs:
            last_confirmed_fx = final_fxs[-1]
            last_processed_fx = processed_fxs[-1]

            # 如果最后一个处理过的分型点不是最后一个确认的笔端点，
            # 那么它们之间就存在一根待定笔。
            # If the last processed fractal is not the same as the last confirmed stroke endpoint,
            # then a pending stroke exists between them.
            if last_confirmed_fx is not last_processed_fx:
                pending_bi_type = 'up' if last_confirmed_fx.type == 'di' else 'down'
                pending_bi = BI(
                    start=last_confirmed_fx,
                    end=last_processed_fx,
                    _type=pending_bi_type,
                    index=len(self.bis)
                )
                self.bis.append(pending_bi)

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
                                current_segment['bis'].extend(bounded_lookahead_bis)
                                next_check_idx += len(bounded_lookahead_bis)
                                continue
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
                                current_segment['bis'].extend(bounded_lookahead_bis)
                                next_check_idx += len(bounded_lookahead_bis)
                                continue
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
        创建段内中枢 (优化版本)
        严格按照 "进入段 + 中枢核心(>=3段) + 离开段" 的结构来识别一个完整的中枢。

        关键优化：
        1. 修正了进入段的识别逻辑
        2. 修正了中枢区间的计算方法
        3. 改进了中枢延伸的判断逻辑
        4. 增强了边界条件处理

        Args:
            zs_type: 中枢类型 (e.g., 'xd')
            lines: 线段对象列表

        Returns:
            包含已完成中枢和最后一个未完成中枢的列表。
        """
        if len(lines) < 5:  # 至少需要5段：进入段 + 3个核心段 + 潜在离开段
            return []

        zss: List[ZS] = []
        i = 0  # 主循环索引，指向潜在的进入段

        # 主循环: 寻找新中枢
        while i <= len(lines) - 4:
            # 1. --- 尝试以当前位置为进入段构建中枢 ---
            entry_seg = lines[i]
            seg_a = lines[i + 1]  # 第一个核心段
            seg_b = lines[i + 2]  # 第二个核心段
            seg_c = lines[i + 3]  # 第三个核心段

            # 检查核心段类型是否交替且进入段与B段同类型
            if not (hasattr(entry_seg, 'type') and hasattr(seg_a, 'type') and hasattr(seg_b, 'type') and hasattr(seg_c, 'type') and
                    entry_seg.type == seg_b.type and seg_a.type == seg_c.type and seg_a.type != seg_b.type):
                i += 1
                continue

            # 2. --- 计算初始中枢区间 [ZD, ZG] ---
            # 中枢区间由同方向段的极值决定
            g_a, d_a = self._get_line_high_low(seg_a)
            g_c, d_c = self._get_line_high_low(seg_c)

            if any(p is None for p in [g_a, d_a, g_c, d_c]):
                i += 1
                continue

            zg = min(g_a, g_c)
            zd = max(d_a, d_c)

            if zd >= zg:
                i += 1
                continue

            # 3. --- 验证进入段是否有效 ---
            entry_high, entry_low = self._get_line_high_low(entry_seg)
            if entry_high is None or entry_low is None:
                i += 1
                continue

            # 进入段必须与中枢区间有交集（进入中枢）
            if entry_low > zg or entry_high < zd:
                i += 1
                continue

            # 4. --- 找到了有效的中枢起点，开始向后延伸 ---
            core_lines = [seg_a, seg_b, seg_c]
            z_segments_info = [{'high': g_a, 'low': d_a}, {'high': g_c, 'low': d_c}]
            leave_seg = None
            center_valid = True

            # 内循环：处理中枢的延伸和寻找离开段
            j = i + 4  # 指向第4段（潜在的中枢延伸段或离开段）

            while j < len(lines):
                current_seg = lines[j]
                curr_high, curr_low = self._get_line_high_low(current_seg)

                if curr_high is None or curr_low is None:
                    break

                # 判断是否为离开段
                is_leave_segment = False
                if j + 1 < len(lines):
                    successor = lines[j + 1]
                    succ_high, succ_low = self._get_line_high_low(successor)
                    if succ_high is not None and succ_low is not None:
                        if succ_low > zg or succ_high < zd:  # successor 无交集
                            is_leave_segment = True

                if is_leave_segment:
                    # 确认离开段必须与中枢有交集
                    if curr_low > zg or curr_high < zd:  # 无交集
                        center_valid = False
                    else:
                        leave_seg = current_seg
                        i = j  # 下次从离开段开始
                    break
                else:
                    # 延伸中枢
                    core_lines.append(current_seg)
                    if current_seg.type == seg_a.type:
                        z_segments_info.append({'high': curr_high, 'low': curr_low})
                    j += 1

            if not center_valid:
                i += 1
                continue

            # 5. --- 构建中枢对象 ---
            center_type = seg_b.type
            center = ZS(zs_type=zs_type, start=entry_seg, _type=center_type, level=0)
            center.lines = core_lines
            center.line_num = len(core_lines)

            # 计算最终的中枢参数 (仅基于同方向段)
            all_highs = [z['high'] for z in z_segments_info]
            all_lows = [z['low'] for z in z_segments_info]

            if all_highs and all_lows:
                center.gg = max(all_highs)
                center.dd = min(all_lows)
                center.zg = min(all_highs)
                center.zd = max(all_lows)

            center.real = True

            if leave_seg:
                # 找到了离开段，是已完成的中枢
                center.end = leave_seg
                center.done = True
            else:
                # 线段走完仍未出现离开段，是未完成的中枢
                center.end = lines[-1]
                center.done = False
                i = len(lines)  # 结束主循环

            zss.append(center)

        # 为所有中枢重新编号
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
                result.append(cl_k)
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
