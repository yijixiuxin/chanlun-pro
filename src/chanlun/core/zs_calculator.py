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

    def calculate(self, lines: List[LINE]) -> List[ZS]:
        """
        全量计算中枢。在多级别分析的场景下，每一级别都是一次全量计算。
        :param lines: 当前级别的所有线段
        :return: 计算出的所有中枢（已完成 + 进行中）
        """
        if len(lines) < 5:  # 至少需要1进入段+3核心段+1离开段
            LogUtil.info("线段数量不足5条，无法形成完整中枢。")
            return []

        self.all_lines = lines
        self.pending_zs = None

        self._create_zs_full()

        if self.pending_zs:
            self.zss.append(self.pending_zs)
        return self.zss

    def _create_zs_full(self):
        """
        核心函数：全量扫描并创建所有中枢
        """
        # entry_idx 指向潜在的"进入段"
        entry_idx = 0
        # 必须保证进入段后至少有3条核心段+1条离开段
        while entry_idx <= len(self.all_lines) - 5:
            # 规则: 中枢由"进入段"和后续至少三条"核心段"构成
            entry_seg = self.all_lines[entry_idx]

            # 核心段从进入段的下一条开始
            core_start_idx = entry_idx + 1
            seg_a, seg_b, seg_c = self.all_lines[core_start_idx], self.all_lines[core_start_idx + 1], self.all_lines[
                core_start_idx + 2]

            # 确保三根核心线段方向交替
            if not (seg_a.type != seg_b.type and seg_b.type != seg_c.type):
                entry_idx += 1  # 从下一根线段开始重新寻找
                continue

            # 规则: 中枢区间由核心三段的重叠部分决定
            zg = min(seg_a.zs_high, seg_b.zs_high, seg_c.zs_high)
            zd = max(seg_a.zs_low, seg_b.zs_low, seg_c.zs_low)

            if zd >= zg:  # 没有重叠区间，不是有效中枢
                entry_idx += 1  # 从下一根线段开始重新寻找
                continue

            # 新增逻辑: 检查进入段是否与核心三段构成的中枢区间有重叠
            if not (max(entry_seg.zs_low, zd) < min(entry_seg.zs_high, zg)):
                LogUtil.info(
                    f"进入段 {entry_idx} 与中枢区间 [{zd:.2f}, {zg:.2f}] 无重叠，跳过。(Entry segment {entry_idx} does not overlap with center range [{zd:.2f}, {zg:.2f}], skipping.)")
                entry_idx += 1  # 进入段不重叠，从下一根线段开始重新寻找
                continue

            # 找到了一个有效的三段重叠，形成中枢核心
            LogUtil.info(
                f"以线段 {entry_idx} 为进入段，找到潜在中枢核心: {core_start_idx}, {core_start_idx + 1}, {core_start_idx + 2}.")
            core_lines = [seg_a, seg_b, seg_c]

            center = ZS(zs_type='xd', start=seg_a, _type=seg_b.type, level=0)
            center.lines = core_lines
            center.line_num = len(core_lines)
            center.zg, center.zd = zg, zd
            center.gg = max(seg.zs_high for seg in core_lines)
            center.dd = min(seg.zs_low for seg in core_lines)
            center.entry = entry_seg

            # 从第4根核心线段开始，检查中枢的延伸或完成
            completed, exit_seg_idx = self._extend_and_check_complete(center, core_start_idx + 3)

            # 更新中枢的结束线段 (中枢的最后一条线段)
            if center.lines:
                center.end = center.lines[-1]

            if completed:
                # 检查完成的中枢是否满足最低要求：至少5段（进入段+3核心段+离开段）
                # center.lines 包含核心段和离开段，至少需要4段（3个中枢区间线段+1个离开段，不含进入段）
                # 而且需要确认有明确的离开动作
                if center.line_num >= 4 and center.exit is not None:
                    LogUtil.info(
                        f"中枢完成: 从 {center.start.start.k.date} 到 {center.end.end.k.date}, 共 {center.line_num + 1} 段（含进入段）。中枢区间线段数: {center.line_num - 1}（不含离开段）。")
                    center.index = len(self.zss)
                    self.zss.append(center)
                    # 修正：下一个中枢的进入段从离开段（exit_seg_idx）开始寻找
                    entry_idx = exit_seg_idx
                else:
                    LogUtil.info(
                        f"中枢未满足最低要求（至少需要进入段+3中枢区间线段+离开段=5段），当前只有 {center.line_num + 1} 段，跳过。")
                    entry_idx += 1
            else:
                # 中枢成为进行时，需要检查是否满足最低段数要求
                # 进行时的中枢至少需要：进入段 + 3中枢区间线段 = 4段（center.lines至少3段）
                if center.line_num >= 3:
                    LogUtil.info(
                        f"中枢成为进行时: 从 {center.start.start.k.date} 开始，共 {center.line_num + 1} 段（含进入段）。中枢区间线段数: {center.line_num}。")
                    self.pending_zs = center
                else:
                    LogUtil.info(
                        f"进行时中枢未满足最低要求（至少需要进入段+3中枢区间线段=4段），当前只有 {center.line_num + 1} 段，跳过。")
                break  # 结束循环

    def _extend_and_check_complete(self, center: ZS, start_j: int) -> Tuple[bool, int]:
        """
        检查中枢的延伸或完成。
        :param center: 当前中枢对象
        :param start_j: 开始检查的线段索引
        :return: (是否完成: bool, 离开段的索引: int)
        """
        j = start_j
        while j < len(self.all_lines):
            current_seg = self.all_lines[j]
            # 判断当前线段是否与中枢区间有重叠
            is_extending = max(current_seg.zs_low, center.zd) < min(current_seg.zs_high, center.zg)

            if is_extending:
                LogUtil.info(f"线段 {j} 延伸中枢。(Segment {j} extends the center.)")
                center.lines.append(current_seg)
                center.line_num += 1
                center.gg = max(center.gg, current_seg.zs_high)
                center.dd = min(center.dd, current_seg.zs_low)
                j += 1
                continue

            # -- 当线段 j 未与中枢重叠，说明发生了离开动作 --
            # 线段 j-1 是中枢的最后一段（离开段），线段 j 是真正离开中枢的段

            # 如果离开动作(线段j)后没有更多线段了，则中枢完成
            if j + 1 >= len(self.all_lines):
                LogUtil.info(
                    f"线段 {j} 离开中枢，但已是最后一段，中枢保持进行时。")
                center.lines.append(current_seg)
                center.line_num += 1
                center.gg = max(center.gg, current_seg.zs_high)
                center.dd = min(center.dd, current_seg.zs_low)
                center.done = False
                break

            # 检查下一段(j+1)是否回拉入中枢区间
            next_seg = self.all_lines[j + 1]

            if hasattr(next_seg, 'done') and not next_seg.done:
                LogUtil.info(
                    f"线段 {j} 离开，但下一段 {j + 1} 未完成，中枢保持进行时。(Segment {j} leaves, but the next segment {j + 1} is not yet complete. Center remains ongoing.)")
                center.lines.append(current_seg)
                center.line_num += 1
                center.gg = max(center.gg, current_seg.zs_high)
                center.dd = min(center.dd, current_seg.zs_low)
                center.done = False
                break

            re_enters = max(next_seg.zs_low, center.zd) < min(next_seg.zs_high, center.zg)

            if not re_enters:
                # 下一段没有回拉，离开被确认，中枢完成
                center.exit = self.all_lines[j - 1]
                center.done = True
                LogUtil.info(
                    f"线段 {j} 离开，下一段 {j + 1} 未返回，中枢完成。离开段索引: {j - 1}。")
                # 返回离开段的索引（j-1），下一个中枢从离开段开始识别
                return True, j - 1
            else:
                # 下一段回拉，中枢延伸。离开动作(j)和回拉段(j+1)都成为中枢的一部分。
                LogUtil.info(
                    f"线段 {j} 暂时离开但下一段 {j + 1} 返回，中枢延伸。")
                center.lines.extend([current_seg, next_seg])
                center.line_num += 2
                center.gg = max(center.gg, current_seg.zs_high, next_seg.zs_high)
                center.dd = min(center.dd, current_seg.zs_low, next_seg.zs_low)
                j += 2

        # 循环结束，所有后续线段都构成了延伸，中枢是"进行时"状态
        center.done = False
        return False, len(self.all_lines) - 1


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
        promoted_zss, trend_lines = self._process_zs_and_generate_trends(
            zss,
            level_index
        )

        LogUtil.info(f"生成 {len(promoted_zss)} 个升级中枢")
        LogUtil.info(f"生成 {len(trend_lines)} 个走势类型")

        # 3. 将升级中枢添加到下一级别
        if promoted_zss:
            self._add_promoted_zss(promoted_zss, level_index + 1)

        return {
            "zss": zss,
            "trend_lines": trend_lines,
            "promoted_zss": promoted_zss
        }

    def _calculate_level_zss(self, lines: List[LINE], level: Level) -> List[ZS]:
        """计算当前级别的中枢"""
        zss = self.zs_calculator.calculate(lines)
        for zs in zss:
            zs.level = level
        return zss

    def _process_zs_and_generate_trends(
            self,
            zss: List[ZS],
            current_level_index: int
    ) -> Tuple[List[ZS], List[LINE]]:
        """
        处理中枢升级（延伸/扩展）并生成走势类型

        Returns:
            (升级中枢列表, 走势类型列表)
        """
        promoted_zss = []
        trend_lines = []
        processed_indices = set()

        # 检查是否存在下一级别，用于升级
        next_level: Optional[Level] = None
        if current_level_index + 1 < len(self.levels):
            next_level = self.levels[current_level_index + 1]

        for i, current_zs in enumerate(zss):
            if i in processed_indices:
                continue

            # 只有存在更高级别时，才尝试升级
            if next_level:
                # 尝试处理延伸
                if current_zs.is_extension_candidate(self.extension_threshold):
                    promoted = self._handle_extension(current_zs, next_level)
                    if promoted:
                        promoted_zss.append(promoted)
                        processed_indices.add(i)
                        LogUtil.info(
                            f"中枢 {i} 发生延伸升级 (段数: {current_zs.line_num})"
                        )
                        continue

                # 尝试处理扩展
                if i + 1 < len(zss):
                    next_zs = zss[i + 1]
                    if current_zs.can_expand_with(next_zs):
                        promoted = self._handle_expansion(
                            current_zs, next_zs, next_level
                        )
                        if promoted:
                            promoted_zss.append(promoted)
                            processed_indices.update([i, i + 1])
                            LogUtil.info(f"中枢 {i} 和 {i + 1} 发生扩展合并")
                            continue

            # 生成走势类型
            if i + 1 < len(zss) and (i + 1) not in processed_indices:
                trend = self._generate_trend_line(current_zs, zss[i + 1], len(trend_lines))
                if trend:
                    trend_lines.append(trend)
                    LogUtil.info(f"生成 {trend.type} 走势: 中枢 {i} → {i + 1}")

        return promoted_zss, trend_lines

    def _handle_extension(self, zs: ZS, target_level: Level) -> Optional[ZS]:
        """处理中枢延伸升级"""
        if len(zs.lines) < self.grouping_size * 3:
            return None

        # 取前9段进行升级
        lines_for_promotion = zs.lines[:self.grouping_size * 3]
        return self._create_promoted_zs(
            base_zs=zs,
            lines=lines_for_promotion,
            level=target_level,
            promotion_type="延伸"
        )

    def _handle_expansion(
            self,
            zs1: ZS,
            zs2: ZS,
            target_level: Level
    ) -> Optional[ZS]:
        """处理中枢扩展合并"""
        # 合并两个中枢的所有线段
        combined_lines = zs1.lines + zs2.lines
        return self._create_promoted_zs(
            base_zs=zs1,
            lines=combined_lines,
            level=target_level,
            promotion_type="扩展"
        )

    def _create_promoted_zs(
            self,
            base_zs: ZS,
            lines: List[LINE],
            level: Level,
            promotion_type: str = "升级"
    ) -> Optional[ZS]:
        """
        创建升级后的高级别中枢
        """
        if len(lines) < self.grouping_size * 3:
            LogUtil.warning(
                f"线段数量不足 ({len(lines)} < {self.grouping_size * 3})，"
                f"无法进行{promotion_type}"
            )
            return None

        # 分组计算
        groups = []
        for i in range(0, min(len(lines), self.grouping_size * 3), self.grouping_size):
            group = lines[i:i + self.grouping_size]
            if group:
                groups.append(group)

        if len(groups) < 3:
            return None

        # 计算每组的高低点
        group_highs = [max(l.zs_high for l in g) for g in groups[:3]]
        group_lows = [min(l.zs_low for l in g) for g in groups[:3]]

        # 新中枢的ZG/ZD
        zg = min(group_highs)
        zd = max(group_lows)

        # 创建升级中枢
        promoted = ZS(
            zs_type=f"{base_zs.zs_type}_promoted",
            start=lines[0],
            end=lines[-1],
            zg=zg,
            zd=zd,
            gg=max(l.zs_high for l in lines),
            dd=min(l.zs_low for l in lines),
            _type=base_zs._type,
            level=level,
            line_num=len(lines)
        )
        promoted.lines = lines
        promoted.done = True

        return promoted

    def _generate_trend_line(
            self,
            zs1: ZS,
            zs2: ZS,
            index: int
    ) -> Optional[LINE]:
        """根据两个中枢生成走势类型线段"""
        # 判断趋势方向
        if zs2.dd > zs1.gg:  # 上涨
            start_fx = FX(_type='di', val=zs1.dd, k=zs1.start.start)
            end_fx = FX(_type='ding', val=zs2.gg, k=zs2.end.end)
            trend_type = 'up'
        elif zs2.gg < zs1.dd:  # 下跌
            start_fx = FX(_type='ding', val=zs1.gg, k=zs1.start.start)
            end_fx = FX(_type='di', val=zs2.dd, k=zs2.end.end)
            trend_type = 'down'
        else:
            return None  # 中枢重叠，不生成走势

        return LINE(
            start=start_fx,
            end=end_fx,
            _type=trend_type,
            index=index
        )

    def _add_promoted_zss(self, promoted_zss: List[ZS], level_index: int):
        """将升级中枢添加到下一级别"""
        if level_index >= len(self.levels):
            return

        next_level_name = self.levels[level_index].value
        if next_level_name not in self.structures_by_level:
            self.structures_by_level[next_level_name] = {
                "zss": [],
                "trend_lines": [],
                "promoted_zss": []
            }

        # 使用 setdefault 确保 "promoted_zss" 键存在
        self.structures_by_level[next_level_name].setdefault("promoted_zss", []).extend(
            promoted_zss
        )
