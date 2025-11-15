# -*- coding: utf-8 -*-
"""
中枢计算与多级别结构分析模块

功能：
1. ZsCalculator: 负责根据笔或线段列表，识别和构建本级别的中枢。
2. ChanlunStructureAnalyzer: 负责从基础级别开始，逐级向上推导和分析多维度的走势结构（包括中枢升级）。
"""
from __future__ import annotations
from typing import List, Tuple, Optional, Dict, Union

from chanlun.core.cl_interface import ZS, LINE, FX, Level, ZSLX
from chanlun.tools.log_util import LogUtil


class ZsCalculator:
    """
    标准中枢计算器
    功能：根据输入的线段列表，识别和构建本级别的所有中枢。
    采用全量计算方式，确保每次计算都是独立的、无状态污染的。
    """

    def __init__(self):
        self.all_lines: List[LINE] = []
        self.zss: List[ZS] = []
        self.pending_zs: Optional[ZS] = None

    def calculate(self, lines: List[LINE]) -> List[ZS]:
        """
        全量计算中枢。

        :param lines: 当前级别的所有线段
        :return: 计算出的所有中枢（已完成 + 进行中）
        """
        if not lines:
            return []
        # 1. 初始化状态
        self.zss = []
        self.pending_zs = None
        self.all_lines = lines

        # 2. 检查线段数量
        # 形成一个进行中枢至少需要: 1个进入段 + 3个核心段
        if len(lines) < 4:
            LogUtil.info("线段数量不足4条，无法形成中枢。")
            return []

        # 3. 执行核心计算
        self._create_zs_full()

        # 4. 组合并返回结果
        final_zss = self.zss.copy()
        if self.pending_zs:
            final_zss.append(self.pending_zs)
        return final_zss

    def _create_zs_full(self):
        """
        核心函数：全量扫描并创建所有中枢
        """
        entry_idx = 0
        # 循环必须为至少一个进入段和3个核心段(共4段)留出空间。
        while entry_idx <= len(self.all_lines) - 4:
            entry_seg = self.all_lines[entry_idx]
            core_start_idx = entry_idx + 1

            seg_a, seg_b, seg_c = self.all_lines[core_start_idx:core_start_idx + 3]

            # 检查线段方向是否交替
            if not (seg_a.type != seg_b.type and seg_b.type != seg_c.type):
                entry_idx += 1
                continue

            # 计算初始三段的重叠区域 (zg, zd)
            zg = min(seg_a.zs_high, seg_b.zs_high, seg_c.zs_high)
            zd = max(seg_a.zs_low, seg_b.zs_low, seg_c.zs_low)

            # 1. 检查三段核心是否有重叠
            if zd >= zg:
                entry_idx += 1
                continue

            # 2. 检查进入段是否与三段核心的重叠区有重叠
            if not (max(entry_seg.zs_low, zd) < min(entry_seg.zs_high, zg)):
                entry_idx += 1
                continue

            # 找到了一个有效的三段核心。
            # 注意：seg_c 此时被假定为核心，如果它稍后被证明是离开段，
            # _extend_and_check_complete 将负责将其移除。
            LogUtil.info(f"以线段 {entry_idx} 为进入段，找到潜在中枢。")
            core_lines = [seg_a, seg_b, seg_c]

            center = ZS(zs_type='xd', start=entry_seg, _type=seg_b.type)
            center.lines = core_lines
            # 初始中枢范围由前三段重叠决定
            center.zg, center.zd = zg, zd
            center.update_boundaries()  # 初始更新zg, zd

            # 3. 尝试延伸中枢，并检查是否完成
            is_completed, exit_idx = self._extend_and_check_complete(center, core_start_idx + 3)

            if is_completed:
                # ----------------------------------------------------
                # ** 核心验证逻辑 (Core Validation Logic) **
                # 一个有效的中枢必须满足所有条件：
                # 1. 具有进入段 (center.start is not None)
                # 2. 具有离开段 (center.end is not None)
                # 3. 至少有3个核心线段 (len(center.lines) >= 3)
                # ----------------------------------------------------
                is_valid_center = (
                        center.start is not None and
                        center.end is not None and
                        len(center.lines) >= 3
                )

                if is_valid_center:
                    # ** 有效中枢：添加并前进到离开段 **
                    LogUtil.info(f"中枢完成 (有效)。核心线段数: {len(center.lines)}.")
                    center.index = len(self.zss)
                    self.zss.append(center)
                    # 下一个中枢的寻找从离开段开始。
                    entry_idx = exit_idx
                else:
                    # ** 无效中枢：丢弃并从下一个线段开始尝试 **
                    # (例如，初始的seg_c被确认为离开段，导致核心线段 < 3)
                    LogUtil.info(f"中枢完成 (无效，len={len(center.lines)})。丢弃并从 {entry_idx + 1} 尝试。")
                    # 丢弃这个无效的中枢，entry_idx 增加 1，
                    # 从下一个线段 (self.all_lines[entry_idx + 1])
                    # 重新开始寻找新的“进入段”。
                    entry_idx += 1
            else:
                # 未完成，说明走到了所有线段的末尾
                if len(center.lines) >= 3:
                    LogUtil.info(f"中枢成为进行时。核心线段数: {len(center.lines)}.")
                    self.pending_zs = center
                # 这是最后一个可能的中枢，所以我们跳出循环。
                break

    def _extend_and_check_complete(self, center: ZS, start_j: int) -> tuple[bool, int]:
        """
        检查中枢的延伸或完成。

        :param center: 当前中枢对象 (in-out param, 会被修改)
        :param start_j: 开始检查的线段索引 (核心段 seg_c 之后的索引)
        :return: 一个元组 (是否完成: bool, 离开段的索引: int)。
                 如果未完成，返回 (False, last_index)。
                 如果已完成，返回 (True, exit_segment_index)。
        """
        j = start_j
        while j < len(self.all_lines):
            current_seg = self.all_lines[j]

            # 核心逻辑：首先检查当前线段(current_seg)是否与中枢重叠
            current_overlaps = max(current_seg.zs_low, center.zd) < min(current_seg.zs_high, center.zg)

            if current_overlaps:
                # --- 情况 1: 当前线段(j)重叠 ---
                # 它*可能*是核心，也*可能*是离开段。我们必须检查 j+1。

                # 1.1 检查是否是最后一条线段
                if j == len(self.all_lines) - 1:
                    # 这是最后一条线段，它重叠了，必须是核心成员
                    center.lines.append(current_seg)
                    center.update_boundaries()
                    LogUtil.info(f"中枢延伸至结尾，加入线段 {j}。")
                    return False, j

                # 1.2 预读下一条线段 (next_seg) 以判断是否完成
                next_seg = self.all_lines[j + 1]
                next_overlaps = max(next_seg.zs_low, center.zd) < min(next_seg.zs_high, center.zg)

                if next_overlaps:
                    # 下一线段(j+1)也重叠
                    # 这证明 current_seg(j) *不是* 离开段，它 *是* 核心成员
                    center.lines.append(current_seg)
                    center.update_boundaries()
                    LogUtil.info(f"中枢延伸，加入线段 {j}。核心线段数: {len(center.lines)}。")
                    j += 1
                    continue
                else:
                    # 下一线段(j+1)不重叠
                    # 根据定义:
                    # - "不进入中枢范围的线段" = next_seg (j+1)
                    # - "离开段" = "前一个线段" = current_seg (j)

                    # current_seg(j) 是离开段，*不要* 将它加入 center.lines
                    center.end = current_seg  # 离开段是 current_seg (j)
                    center.done = True
                    LogUtil.info(f"中枢完成。离开段为线段 {j}。")
                    return True, j  # 下一个中枢的入口是 j

            else:
                # --- 情况 2: 当前线段(j)不重叠 ---
                # 2.1 它就是第一个不进入中枢范围的线段
                # 根据定义:
                # - "不进入中枢范围的线段" = current_seg (j)
                # - "离开段" = "前一个线段" = self.all_lines[j-1]

                center.end = self.all_lines[j - 1]  # 离开段是 j-1
                center.done = True

                # *** 修正点 (使用 'is' 进行严格的对象身份检查) ***:
                # 检查 self.all_lines[j-1] (即离开段)
                # 是否 *就是* center.lines 的末尾 (例如初始的 seg_c)。
                # 如果是，说明初始的 seg_c 实际上是离开段，应将其从核心线段中移除。
                if center.lines and center.lines[-1] is center.end:
                    LogUtil.info(f"将线段 {j - 1} (离开段) 从核心 {center.lines} 中移除。")
                    center.lines.pop()

                LogUtil.info(f"中枢完成。离开段为线段 {j - 1}。")
                return True, j - 1  # 下一个中枢的入口是 j-1

        # 循环正常结束 (j == len(self.all_lines))
        # 这意味着最后一条线段是 j-1，并且它没有导致中枢完成
        # (这种情况理论上在 1.1 中被覆盖了，但作为兜底)
        return False, j - 1


class ChanlunStructureAnalyzer:
    """
    多级别缠论结构分析器

    从基础级别线段开始，逐级向上推导和分析高维度的走势结构。
    """

    def __init__(
            self,
            levels: Optional[List[Level]] = None,
            zs_calculator: Optional[ZsCalculator] = None
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
        # 格式: { "M1": {"zss": [...], "trend_lines": [...]}, "M5": ... }
        self.structures_by_level: Dict[str, Dict[str, Union[List[ZSLX], List[ZS]]]] = {}

        # 配置参数
        self.min_lines_for_analysis = 4  # 最少需要的线段数
        self.extension_threshold = 9  # 延伸升级的段数阈值
        self.grouping_size = 3  # 升级时的分组大小 (暂未使用，保留)

    def calculate(self, base_lines: List[LINE]) -> Dict:
        """
        执行多级别分析

        Args:
            base_lines: 最低级别的线段列表

        Returns:
            各级别的分析结果 (self.structures_by_level)
        """
        if not base_lines:
            LogUtil.warning("输入线段为空，无法进行分析")
            return {}

        current_lines = base_lines
        self.structures_by_level = {}

        for level_index, level in enumerate(self.levels):
            LogUtil.info(f"分析级别: {level.value}")

            # 1. 检查线段数量
            if len(current_lines) < self.min_lines_for_analysis:
                LogUtil.info(
                    f"线段数量不足 ({len(current_lines)} < {self.min_lines_for_analysis})，"
                    f"无法分析 {level.value} 级别"
                )
                break

            # 2. 分析当前级别
            level_result = self._analyze_level(
                current_lines,
                level,
                level_index
            )

            # 3. 存储结果
            self.structures_by_level[level.value] = level_result

            # 4. 准备下一级别的输入
            # 下一级别分析的 "线段" 是本级别生成的 "走势类型" (trend_lines)
            trend_lines = level_result.get("trend_lines", [])
            if not trend_lines:
                LogUtil.info(f"{level.value} 级别未生成走势类型，分析结束")
                break

            current_lines = trend_lines  # 迭代

        return self.structures_by_level

    def _analyze_level(
            self,
            lines: List[LINE],
            level: Level,
            level_index: int
    ) -> Dict:
        """
        分析单个级别：1. 计算中枢 2. 生成走势类型（含升级）

        Args:
            lines: 当前级别的输入线段
            level: 当前级别
            level_index: 当前级别的索引

        Returns:
            包含中枢和走势类型的字典
        """
        # 1. 计算当前级别中枢
        zss = self._calculate_level_zss(lines, level)
        LogUtil.info(f"识别到 {len(zss)} 个中枢")

        # 2. 处理中枢升级和生成走势类型
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
            next_level: Level,
            trend_lines: List[ZSLX],
            current_zs: ZS
    ) -> List[ZSLX]:
        """
        根据“3+3+3”规则和后续的延续逻辑，为延伸或扩展的中枢创建升级后的走势类型。
        这些“走势类型”将作为下一级别的“线段”使用。

        Args:
            lines: 用于组合成更高级别走势的基础级别线段列表。
                   (对于延伸，这是 zs.lines; 对于扩展，这是 zs1.start 到 zs2.end 的所有 lines)
            next_level: 新创建的走势类型应归属的级别（即更高级别）。
            trend_lines: 已存在的走势类型列表，用于提供上下文（确定方向）。
            current_zs: 当前中枢，用于确定初始方向（如果 trend_lines 为空）。

        Returns:
            一个新的、升级后的ZSLX走势类型列表。
        """
        upgraded_trends: List[ZSLX] = []
        i = 0

        # 1. 确定要创建的第一个走势类型的方向
        if trend_lines:
            # 如果存在之前的走势，则新走势的方向与最后一个走势相反
            reference_direction = trend_lines[-1].type
        else:
            # 否则，使用进入参考中枢的线段的方向
            reference_direction = current_zs.start.type

        current_direction = 'down' if reference_direction == 'up' else 'up'

        while i < len(lines):
            # 一个新的走势类型至少需要3段线段
            if len(lines) - i < 3:
                break

            # 基础组合是3段线段
            end_index = i + 3

            # 2. 延续规则：当已形成至少3个高级别走势后，检查后续线段是否延续当前趋势
            # (注意: 这里的 len(upgraded_trends) 是指本次调用中新生成的数量)
            if len(upgraded_trends) >= 3:
                base_j = end_index - 1  # 延续检查的基准线段
                if current_direction == 'up':
                    # 对于上涨走势，检查是否连续创出更高的高点和低点。
                    for j in range(end_index, len(lines)):
                        if lines[j].high > lines[base_j].high and lines[j].low > lines[base_j].low:
                            end_index = j + 1  # 延伸走势
                            base_j = j
                        else:
                            break  # 模式被破坏
                else:  # current_direction == 'down'
                    # 对于下跌走势，检查是否连续创出更低的高点和低点。
                    for j in range(end_index, len(lines)):
                        if lines[j].high < lines[base_j].high and lines[j].low < lines[base_j].low:
                            end_index = j + 1  # 延伸走势
                            base_j = j
                        else:
                            break  # 模式被破坏

            # 构成此走势的最终线段集合。
            # 确保 end_index 不会超出范围
            if end_index > len(lines):
                end_index = len(lines)

            trend_chunk = lines[i:end_index]

            # 如果分块后剩余线段不足 (例如最后只剩1-2条)，则停止
            if not trend_chunk:
                break

            # 计算新走势的最高点和最低点
            trend_high = max(line.high for line in trend_chunk)
            trend_low = min(line.low for line in trend_chunk)

            start: FX = trend_chunk[0].start.copy()
            end: FX = trend_chunk[-1].end.copy()

            # 修正走势的起始点和结束点的值
            if current_direction == 'down':
                start.val = trend_high
                end.val = trend_low
            else:
                start.val = trend_low
                end.val = trend_high

            # 创建新的走势类型 (ZSLX) 对象。
            new_trend = ZSLX(
                index=len(trend_lines) + len(upgraded_trends),
                zslx_level=next_level,  # 级别是 *下一级*
                _type=current_direction,
                start_line=trend_chunk[0],
                end_line=trend_chunk[-1],
                start=start,
                end=end
            )
            new_trend.high = trend_high
            new_trend.low = trend_low

            upgraded_trends.append(new_trend)

            # 为下一次迭代做准备
            i = end_index
            # 下一个走势类型的方向将相反
            current_direction = 'down' if current_direction == 'up' else 'up'

        return upgraded_trends

    def _generate_trends(
            self,
            lines: List[LINE],
            zss: List[ZS],
            current_level_index: int
    ) -> List[ZSLX]:
        """
        处理中枢升级（延伸/扩展）并生成本级别的走势类型 (ZSLX)。

        Args:
            lines: 当前级别的 *所有* 线段
            zss: 当前级别计算出的 *所有* 中枢
            current_level_index: 当前级别的索引

        Returns:
            生成的走势类型列表 (ZSLX)，将作为下一级别的 'lines' 输入。
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

            # 必须是已完成的中枢才能参与判断
            if not current_zs.done:
                i += 1
                continue

            # 规则 1: 尝试处理延伸升级（单个中枢9段以上）
            if next_level and current_zs.is_extension_candidate(self.extension_threshold):
                # 对中枢的 *核心线段* (zs.lines) 进行 3-group 升级
                new_trends = self._create_upgraded_trends(
                    current_zs.lines,
                    next_level,
                    trend_lines,
                    current_zs
                )
                if new_trends:
                    trend_lines.extend(new_trends)
                    LogUtil.info(
                        f"生成延伸走势: 从中枢 {i} (线段 {current_zs.start.index}) 生成 {len(new_trends)} 个高级别走势")
                i += 1
                continue

            # 规则 2: 处理扩展升级 (两个中枢)
            if next_level and i + 1 < len(zss):
                next_zs = zss[i + 1]
                if not next_zs.done:  # 下一个中枢也必须完成
                    # 当前中枢不延伸，但下一个未完成，无法判断扩展，处理当前中枢
                    pass  # 进入规则3
                elif current_zs.can_expand_with(next_zs):
                    # 扩展升级：取 *两个中枢之间* (从 zs.start 到 zs_next.end) 的 *所有* 线段
                    start_index = current_zs.start.index
                    end_index = next_zs.end.index
                    # 确保索引有效
                    if start_index < 0 or end_index < 0 or end_index + 1 > len(lines):
                        LogUtil.warning(f"扩展升级索引无效: {start_index}, {end_index}")
                        # 索引无效，退回到规则3
                        pass
                    else:
                        all_lines = lines[start_index:end_index + 1]
                        new_trends = self._create_upgraded_trends(
                            all_lines,
                            next_level,  # 级别是下一级
                            trend_lines,
                            current_zs
                        )
                        if new_trends:
                            trend_lines.extend(new_trends)
                            LogUtil.info(
                                f"生成扩展走势: 从中枢 {i} 到 {i + 1} 生成 {len(new_trends)} 个高级别走势")
                        # 扩展消耗了两个中枢
                        i += 2
                        continue

            # 规则 3: 当前中枢既不延伸也不扩展，调用函数处理普通走势类型构建。
            previous_zs = zss[i - 1] if i > 0 and zss[i - 1].done else None

            # *** 注意 ***：
            # 您的原始逻辑在 _handle_non_upgraded_trend 中创建 ZSLX，
            # 但这些 ZSLX 的级别是 current_level。
            # 它们 *不应该* 被添加到 trend_lines 列表中，
            # 因为 trend_lines 列表是用于 *下一级别* (next_level) 分析的。
            #
            # 缠论的原始定义中，"走势类型" (趋势/盘整) 是由同级别中枢构成的。
            # 而 "升级" 产生的才是更高级别的 "线段" (在我们的代码里也用 ZSLX 表示)。
            #
            # 因此，我将 _handle_non_upgraded_trend 的调用保留（它可以用于分析本级别结构），
            # 但 *不* 将其结果添加到 trend_lines 中。

            # (如果您希望将普通趋势也视为下一级别线段，请取消下面两行的注释)
            # new_trend = self._handle_non_upgraded_trend(current_zs, previous_zs, current_level, trend_lines)
            # if new_trend:
            #     trend_lines.append(new_trend) # <-- 这在逻辑上可能不正确

            # 仅在调试时使用：
            self._handle_non_upgraded_trend(current_zs, previous_zs, current_level, trend_lines)

            i += 1

        return trend_lines

    def _handle_non_upgraded_trend(
            self,
            current_zs: ZS,
            previous_zs: Optional[ZS],
            current_level: Level,
            trend_lines: List[ZSLX]  # 这里的 trend_lines 仅用于上下文判断
    ) -> Optional[ZSLX]:
        """
        处理普通（未升级）的走势类型构建（盘整或趋势）。

        注意：此函数生成的 ZSLX 属于 current_level，
        用于描述 *本级别* 的结构，通常 *不* 作为输入传递给下一级别。

        Args:
            current_zs: 当前正在处理的中枢。
            previous_zs: 前一个已完成的中枢。
            current_level: 当前的走势级别。
            trend_lines: 已生成的 *升级* 走势列表（用于判断是否为初始走势）。

        Returns:
            如果成功构建了新的走势类型，则返回 ZSLX 对象，否则返回 None。
        """

        # 规则 1: "如果之前没有走势类型，那么就以当前中枢的开始走势作为走势类型的类型"
        # 我们将“之前没有走势类型”理解为“这是第一个被处理的(未升级的)中枢”
        is_first_trend = not previous_zs and not trend_lines

        if is_first_trend:
            # 这是第一个被处理的普通中枢，我们将其定义为一个初始的“盘整”走势。
            direction = current_zs.start.type
            start_line = current_zs.start
            end_line = current_zs.end

            # 盘整走势的范围由其自身决定
            start_point = start_line.start.copy() if direction == 'up' else start_line.start.copy()
            end_point = end_line.end.copy() if direction == 'up' else end_line.end.copy()

            if direction == 'up':
                start_point.val = min(start_line.low, end_line.low)
                end_point.val = max(start_line.high, end_line.high)
            else:
                start_point.val = max(start_line.high, end_line.high)
                end_point.val = min(start_line.low, end_line.low)

            new_trend = ZSLX(
                zslx_level=current_level,  # 级别是当前级别
                start=start_point,
                end=end_point,
                _type=direction,  # 走势类型由进入线段决定
                start_line=start_line,
                end_line=end_line,
                index=0
            )
            new_trend.high = max(start_line.high, end_line.high)
            new_trend.low = min(start_line.low, end_line.low)

            LogUtil.info(
                f"生成初始盘整走势: [级别 {current_level.value}] 基于中枢 starting at line {current_zs.start.index}")
            return new_trend

        # 规则 2: "如果存在走势类型...根据前一个中枢和当前中枢比较位置确定当前的走势类型。"
        # 必须有前一个中枢才能进行比较来确定趋势方向。
        if not previous_zs:
            return None

        direction = None
        # 定义上涨趋势：当前中枢的高低点都高于前一个中枢
        if current_zs.zg > previous_zs.zg and current_zs.zd > previous_zs.zd:
            direction = 'up'
        # 定义下跌趋势：当前中枢的高低点都低于前一个中枢
        elif current_zs.zg < previous_zs.zg and current_zs.zd < previous_zs.zd:
            direction = 'down'
        else:
            # 两个中枢存在重叠，不构成严格的上涨或下跌趋势（即盘整），
            # 此时不生成新的 ZSLX 趋势对象。
            LogUtil.info(
                f"中枢间盘整: [级别 {current_level.value}] 中枢 {previous_zs.index} 和 {current_zs.index} 重叠")
            return None

        # 确定走势的起点和终点
        # 走势的起点是前一个中枢的离开段的端点
        start_fx = previous_zs.end.end.copy()
        # 走势的终点是当前中枢的离开段的端点
        end_fx = current_zs.end.end.copy()

        start_line = previous_zs.end
        end_line = current_zs.end

        new_trend = ZSLX(
            zslx_level=current_level,  # 级别是当前级别
            start=start_fx,
            end=end_fx,
            _type=direction,
            start_line=start_line,  # 走势从前一个中枢的离开线开始
            end_line=end_line,  # 走势到当前中枢的离开线结束
            index=(previous_zs.index or 0) + 1  # 假设的索引
        )
        LogUtil.info(
            f"生成普通走势 ({direction}): [级别 {current_level.value}] 从中枢 {previous_zs.index} 到 {current_zs.index}")
        return new_trend