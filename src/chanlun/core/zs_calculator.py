# -*- coding: utf-8 -*-
"""
中枢计算模块
负责根据笔或线段列表，识别和构建中枢。
该模块已被重构，以确保增量计算的逻辑与标准的全量计算逻辑一致。
"""
from typing import List, Tuple, Optional, Dict, Union

from chanlun.core.cl_interface import ZS, LINE, FX, Level, ZSLX
from chanlun.tools.log_util import LogUtil

class ZsCalculator:
    """
    标准中枢计算器
    功能：根据输入的线段列表，识别和构建本级别的所有中枢。
    """

    def __init__(self):
        self.all_lines: List[LINE] = []
        self.zss: List[ZS] = []
        self.pending_zs: Optional[ZS] = None

    def calculate(self, lines: List['LINE']) -> List[ZS]:
        """
        全量计算中枢。

        :param lines: 当前级别的所有线段
        :return: 计算出的所有中枢（已完成 + 进行中）
        """
        self.pending_zs = None
        self.all_lines = lines

        # 形成一个进行中枢至少需要: 1个进入段 + 3个核心段
        if len(lines) < 4:
            LogUtil.info("线段数量不足4条，无法形成中枢。")
            return []

        self._create_zs_full()

        final_zss = self.zss.copy()
        if self.pending_zs:
            final_zss.append(self.pending_zs)
        return final_zss

    def _create_zs_full(self):
        """
        核心函数：全量扫描并创建所有中枢
        """
        entry_idx = 0
        # 循环必须为至少一个进入段和3个核心段留出空间。
        while entry_idx <= len(self.all_lines) - 4:
            entry_seg = self.all_lines[entry_idx]
            core_start_idx = entry_idx + 1

            seg_a, seg_b, seg_c = self.all_lines[core_start_idx:core_start_idx + 3]

            if not (seg_a.type != seg_b.type and seg_b.type != seg_c.type):
                entry_idx += 1
                continue

            zg = min(seg_a.zs_high, seg_b.zs_high, seg_c.zs_high)
            zd = max(seg_a.zs_low, seg_b.zs_low, seg_c.zs_low)

            if zd >= zg or not (max(entry_seg.zs_low, zd) < min(entry_seg.zs_high, zg)):
                entry_idx += 1
                continue

            # 找到了一个有效的三段核心。
            LogUtil.info(f"以线段 {entry_idx} 为进入段，找到潜在中枢。")
            core_lines = [seg_a, seg_b, seg_c]

            center = ZS(zs_type='xd', start=entry_seg, _type=seg_b.type)
            center.lines = core_lines
            center.zg, center.zd = zg, zd
            center.update_boundaries()

            is_completed, exit_idx = self._extend_and_check_complete(center, core_start_idx + 3)

            if is_completed:
                if center.end is not None and len(center.lines) >= 3:
                    LogUtil.info(f"中枢完成。核心线段数: {len(center.lines)}.")
                    center.index = len(self.zss)
                    self.zss.append(center)
                    # 下一个中枢的寻找从离开段开始。
                    entry_idx = exit_idx
                else:
                    entry_idx += 1
            else:
                if len(center.lines) >= 3:
                    LogUtil.info(f"中枢成为进行时。核心线段数: {len(center.lines)}.")
                    self.pending_zs = center
                # 这是最后一个可能的中枢，所以我们跳出循环。
                break

    def _extend_and_check_complete(self, center: ZS, start_j: int) -> tuple[bool, int] | None:
        """
        检查中枢的延伸或完成。

        :param center: 当前中枢对象。
        :param start_j: 开始检查的线段索引。
        :return: 一个元组 (是否完成: bool, 离开段的索引: int)。
        """
        j = start_j
        while j < len(self.all_lines):
            # 如果是最后一个线段，则无法预读，中枢保持“进行时”
            if j == len(self.all_lines) - 1:
                current_seg = self.all_lines[j]
                is_extending = max(current_seg.zs_low, center.zd) < min(current_seg.zs_high, center.zg)
                if is_extending:
                    center.lines.append(current_seg)
                    center.update_boundaries()
                return False, j

            # 标准情况：有当前线段和下一线段可供分析
            current_seg = self.all_lines[j]
            next_seg = self.all_lines[j + 1]

            # 判断中枢是否延伸，主要取决于下一线段的行为
            next_overlaps = max(next_seg.zs_low, center.zd) < min(next_seg.zs_high, center.zg)

            if next_overlaps:
                # 情况1: 下一线段与中枢重叠 -> 中枢延伸
                # 无论当前线段是否重叠（暂时离开），都将其视为核心的一部分
                center.lines.append(current_seg)
                center.update_boundaries()
                j += 1  # 处理完当前线段，继续下一轮循环
            else:
                # # 情况2: 下一线段不与中枢重叠 -> 中枢完成
                # center.update_boundaries()
                # # 下一线段是离开段
                # center.end = current_seg
                # center.done = True
                # return True, j

                # 情况2: 下一线段不与中枢重叠 -> 中枢完成
                current_overlaps = max(current_seg.zs_low, center.zd) < min(current_seg.zs_high, center.zg)
                if current_overlaps:
                    # 情况2.1: 当前线段重叠，下一线段不重叠
                    # 当前线段是最后一个核心成员
                    center.end = current_seg
                    center.done = True
                    return True, j
                else:
                    # 情况2.2: 当前和下一线段都连续不重叠
                    # 当前线段就是离开段
                    center.end = current_seg
                    center.done = True
                    return True, j-1

class ChanlunStructureAnalyzer:
    """
    多级别缠论结构分析器

    从基础级别线段开始，逐级向上推导和分析高维度的走势结构。
    """

    def __init__(
            self,
            levels: Optional[List['Level']] = None,
            zs_calculator: Optional['ZsCalculator'] = None
    ):
        """
        初始化分析器

        Args:
            levels: 分析级别列表（从低到高）
            zs_calculator: 中枢计算器实例
        """
        self.levels: List[Level] = levels or [Level.M1, Level.M5, Level.M30, Level.D1]
        self.zs_calculator = zs_calculator or ZsCalculator()

        # 存储各级别分析结果
        self.structures_by_level: Dict[str, Dict[str, Union[List[ZSLX], List[ZS]]]] = {}

        # 配置参数
        self.min_lines_for_analysis = 4  # 最少需要的线段数
        self.extension_threshold = 9  # 延伸升级的段数阈值
        self.grouping_size = 3  # 升级时的分组大小

    def calculate(self, base_lines: List[LINE]) -> Dict:
        """
        执行多级别分析

        Args:
            base_lines: 最低级别的线段列表

        Returns:
            各级别的分析结果
        """
        if not base_lines:
            LogUtil.warning("输入线段为空，无法进行分析")
            return {}

        current_lines = base_lines

        for level_index, level in enumerate(self.levels):
            LogUtil.info(f"分析级别: {level.value}")

            # 检查线段数量
            if len(current_lines) < self.min_lines_for_analysis:
                LogUtil.info(
                    f"线段数量不足 ({len(current_lines)} < {self.min_lines_for_analysis})，"
                    f"无法分析 {level.value} 级别"
                )
                break

            # 分析当前级别，传入 level_index
            level_result = self._analyze_level(
                current_lines,
                level,
                level_index
            )

            # 存储结果
            self.structures_by_level[level.value] = level_result

            # 准备下一级别的输入
            trend_lines = level_result.get("trend_lines", [])
            if not trend_lines:
                LogUtil.info(f"{level.value} 级别未生成走势类型，分析结束")
                break

            current_lines = trend_lines

        return self.structures_by_level

    def _analyze_level(
            self,
            lines: List[LINE],
            level: Level,
            level_index: int
    ) -> Dict:
        """
        分析单个级别

        Returns:
            包含中枢和走势类型的字典
        """
        # 1. 计算当前级别中枢
        zss = self._calculate_level_zss(lines, level)
        LogUtil.info(f"识别到 {len(zss)} 个中枢")

        # 2. 处理中枢升级和生成走势类型，传入 level_index
        trend_lines = self._generate_trends(
            lines,
            zss,
            level_index
        )

        LogUtil.info(f"生成 {len(trend_lines)} 个走势类型")

        return {
            "zss": zss,
            "trend_lines": trend_lines
        }

    def _calculate_level_zss(self, lines: List[LINE], level: Level) -> List[ZS]:
        """计算当前级别的中枢"""
        zss = self.zs_calculator.calculate(lines)
        for zs in zss:
            zs.level = level
        return zss

    def _create_upgraded_trends(
            self,
            lines: List[LINE],
            current_level: Level
    ) -> List[ZSLX]:
        """
        根据“3+3+3”规则为延伸或扩展的中枢创建升级后的走势类型。
        这里的逻辑假设，构成升级的线段本身就自然地形成了交替的方向。
        """
        upgraded_trends = []
        chunk_size = 3
        for i in range(0, len(lines), chunk_size):
            chunk = lines[i:i + chunk_size]
            if len(chunk) < chunk_size:
                break  # 剩余线段不足以构成新的走势类型

            trend = ZSLX(lines=chunk, zslx_level=current_level)
            upgraded_trends.append(trend)
        return upgraded_trends

    def _generate_trends(
            self,
            lines: List[LINE],
            zss: List[ZS],
            current_level_index: int
    ) -> List[ZSLX]:
        """
        处理中枢升级（延伸/扩展）并生成走势类型。
        这是您请求的核心实现。
        """
        trend_lines: List[ZSLX] = []
        if not zss:
            return trend_lines

        # 检查是否存在下一级别，用于升级
        current_level = self.levels[current_level_index]
        next_level: Optional[Level] = None
        if current_level_index + 1 < len(self.levels):
            next_level = self.levels[current_level_index + 1]

        i = 0
        while i < len(zss):
            current_zs = zss[i]

            # 规则 1: 尝试处理延伸升级（单个中枢9段以上）
            if next_level and current_zs.is_extension_candidate(self.extension_threshold):
                new_trends = self._create_upgraded_trends(current_zs.lines, next_level)
                if new_trends:
                    trend_lines.extend(new_trends)
                    LogUtil.info(f"生成延伸走势: 从中枢 {i} 生成 {len(new_trends)} 个高级别走势")
                i += 1
                continue

            # 规则 2: 尝试处理扩展升级（相邻中枢有重叠）
            if next_level and i + 1 < len(zss):
                # 查找所有连续可扩展的中枢
                expand_end_index = i + 1
                if expand_end_index < len(zss) and zss[expand_end_index - 1].can_expand_with(zss[expand_end_index]):
                    expandable_zss = zss[i:expand_end_index + 1]

                    start_index = expandable_zss[0].start.index
                    end_index = expandable_zss[1].end.index

                    all_lines = lines[start_index:end_index+1]

                    new_trends = self._create_upgraded_trends(all_lines, current_level)
                    if new_trends:
                        trend_lines.extend(new_trends)
                        LogUtil.info(
                            f"生成扩展走势 (Pivots Upgraded): 从中枢 {i} 到 {expand_end_index - 1} 生成 {len(new_trends)} 个高级别走势")

                    i = expand_end_index
                    continue

            # 规则 3: 处理常规趋势（无升级）
            # 如果是最后一个中枢，它自身构成一个盘整
            if i + 1 >= len(zss):
                trend = ZSLX(lines=current_zs.lines, zslx_level=current_level)
                trend.add_zs(current_zs)
                trend_lines.append(trend)
                LogUtil.info(f"生成常规走势: 最后一个中枢 {i} 形成盘整")
                break

            # 查看后续中枢，判断是上涨、下跌还是盘整
            next_zs = zss[i + 1]

            # 判断为上涨趋势 (下一个中枢的低点 > 当前中枢的高点)
            if next_zs.zd > current_zs.zg:
                trend_end_index = i + 1
                while (trend_end_index + 1 < len(zss) and
                       zss[trend_end_index + 1].zd > zss[trend_end_index].zg):
                    trend_end_index += 1

                trend_zss = zss[i: trend_end_index + 1]
                all_lines = [line for zs_ in trend_zss for line in zs_.lines]
                trend = ZSLX(lines=all_lines, zslx_level=current_level)
                for zs_ in trend_zss:
                    trend.add_zs(zs_)
                trend_lines.append(trend)
                LogUtil.info(f"生成常规走势: 从中枢 {i} 到 {trend_end_index} 形成上涨")
                i = trend_end_index + 1
                continue

            # 判断为下跌趋势 (下一个中枢的高点 < 当前中枢的低点)
            elif next_zs.zg < current_zs.zd:
                trend_end_index = i + 1
                while (trend_end_index + 1 < len(zss) and
                       zss[trend_end_index + 1].zg < zss[trend_end_index].zd):
                    trend_end_index += 1

                trend_zss = zss[i: trend_end_index + 1]
                all_lines = [line for zs_ in trend_zss for line in zs_.lines]
                trend = ZSLX(lines=all_lines, zslx_level=current_level)
                for zs_ in trend_zss:
                    trend.add_zs(zs_)
                trend_lines.append(trend)
                LogUtil.info(f"生成常规走势: 从中枢 {i} 到 {trend_end_index} 形成下跌")
                i = trend_end_index + 1
                continue

            # 如果以上都不是，则当前中枢自己形成一个盘整
            else:
                trend = ZSLX(lines=current_zs.lines, zslx_level=current_level)
                trend.add_zs(current_zs)
                trend_lines.append(trend)
                LogUtil.info(f"生成常规走势: 中枢 {i} 自身形成盘整")
                i += 1
                continue

        return trend_lines