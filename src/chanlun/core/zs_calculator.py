# -*- coding: utf-8 -*-
"""
中枢计算模块
负责根据笔或线段列表，识别和构建中枢。
该模块已被重构，以确保增量计算的逻辑与标准的全量计算逻辑一致。
"""
import math
from typing import List, Tuple, Optional, Dict, Union

from chanlun.core.cl_interface import ZS, Config, LINE, FX, Kline
from chanlun.tools.log_util import LogUtil

class ZsCalculator:
    """
    标准中枢计算器
    功能：根据输入的线段列表，识别和构建本级别的所有中枢。
    """

    def __init__(self, config: dict):
        self.config = config
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
        :param center: 当前中枢对象 (The current center object)
        :param start_j: 开始检查的线段索引 (The starting segment index for the check)
        :return: (是否完成: bool, 离开段的索引: int) ((Is completed: bool, Index of the exit segment (j-1): int))
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
                LogUtil.info(f"线段 {j} 离开中枢，但已是最后一段，中枢保持进行时。(Segment {j} leaves the center, but it's the last segment. Center remains ongoing.)")
                center.lines.append(current_seg)  # Include the leaving segment
                center.line_num += 1
                center.gg = max(center.gg, current_seg.zs_high)
                center.dd = min(center.dd, current_seg.zs_low)
                center.done = False
                break # Break the loop, the function will return False at the end.

            # 检查下一段(j+1)是否回拉入中枢区间
            next_seg = self.all_lines[j + 1]

            if hasattr(next_seg, 'done') and not next_seg.done:
                # The state of the pullback is uncertain, so the center's completion is also uncertain.
                LogUtil.info(f"线段 {j} 离开，但下一段 {j+1} 未完成，中枢保持进行时。(Segment {j} leaves, but the next segment {j+1} is not yet complete. Center remains ongoing.)")
                center.lines.append(current_seg)  # Include the leaving segment
                center.line_num += 1
                center.gg = max(center.gg, current_seg.zs_high)
                center.dd = min(center.dd, current_seg.zs_low)
                center.done = False
                break # Break the loop as we've reached the end of finalized data.

            # Case 3: The next segment is complete, check if it re-enters.
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


class MultiLevelAnalyzer:
    """
    多级别走势分析器
    """

    def __init__(self, config: dict):
        self.config = config
        self.levels = self.config.get('levels', ['1m', '5m', '30m', 'D'])
        self.results: Dict[str, Dict[str, Union[List[LINE], List[ZS]]]] = {}

    def run(self, base_lines: List[LINE]):
        """
        从最底层的线段开始，执行多级别分析
        :param base_lines: 最原始、最低级别的线段列表 (例如，1分钟周期的笔构成的线段)
        """
        current_lines = base_lines
        level_index = 0

        while level_index < len(self.levels):
            level_name = self.levels[level_index]
            LogUtil.info(f"\n{'=' * 20} Analyzing Level: {level_name} (Level Index: {level_index}) {'=' * 20}")

            if len(current_lines) < 3:
                LogUtil.info(f"线段数量不足 ({len(current_lines)})，无法分析 {level_name} 级别。分析结束。")
                break

            # 1. 计算当前级别的中枢
            zs_calculator = ZsCalculator(self.config)
            level_zss = zs_calculator.calculate(current_lines)
            for zs in level_zss:
                zs.level = level_index
            LogUtil.info(f"在 {level_name} 级别找到 {len(level_zss)} 个中枢。")

            # 2. 识别中枢升级（延伸/扩展）并生成当前级别的走势类型
            next_level_index = level_index + 1
            promoted_zss, trend_lines = self._promote_and_generate_trends(level_zss, level_index, next_level_index)

            LogUtil.info(f"为下一级别生成了 {len(promoted_zss)} 个升级中枢。")
            LogUtil.info(f"在 {level_name} 级别生成了 {len(trend_lines)} 个走势类型。")

            # 3. 存储当前级别的结果
            self.results[level_name] = {
                "zss": level_zss,
                "trend_lines": trend_lines
            }
            # 将升级的中枢添加到下一级别的中枢列表中
            if promoted_zss:
                next_level_name = self.levels[next_level_index] if next_level_index < len(self.levels) else "Promoted"
                if next_level_name not in self.results:
                    self.results[next_level_name] = {"zss": [], "trend_lines": []}
                self.results[next_level_name]["zss"].extend(promoted_zss)

            # 4. 准备下一次循环：当前级别生成的走势类型成为下一级别分析的“线段”
            current_lines = trend_lines
            level_index += 1

        return self.results

    def _promote_and_generate_trends(self, zss: List[ZS], current_level: int, next_level: int) -> Tuple[
        List[ZS], List[LINE]]:
        """
        核心逻辑：处理中枢的延伸、扩展，并生成走势类型
        :param zss: 当前级别已识别的中枢列表
        :return: (推广生成的高级别中枢列表, 当前级别的走势类型列表)
        """
        promoted_zss: List[ZS] = []
        trend_lines: List[LINE] = []

        i = 0
        while i < len(zss):
            current_zs = zss[i]

            # 跳过未完成的中枢，因为它无法参与升级或趋势构建
            if not current_zs.done:
                i += 1
                continue

            # --- 规则1: 中枢延伸 (9段及以上升级) ---
            # 规则：大于等于27段小于81段，升级到30m (跳2级)
            # 规则：大于等于9段小于27段，升级到5m (跳1级)
            promoted = None
            if 27 <= current_zs.line_num < 81:
                LogUtil.info(f"发生【中枢延伸】: ZS {i} (段数: {current_zs.line_num}) 升级到 Level {current_level + 2}")
                # 升级逻辑：按每9段为一组进行处理
                promoted = self._create_promoted_zs(current_zs, current_zs.lines, current_level + 2, 9)
            elif 9 <= current_zs.line_num < 27:
                LogUtil.info(f"发生【中枢延伸】: ZS {i} (段数: {current_zs.line_num}) 升级到 Level {next_level}")
                # 升级逻辑：按每3段为一组进行处理
                promoted = self._create_promoted_zs(current_zs, current_zs.lines, next_level, 3)

            if promoted:
                promoted_zss.append(promoted)
                i += 1  # 处理完当前中枢，继续下一个
                continue

            # --- 规则2: 中枢扩展 ---
            if i + 1 < len(zss):
                next_zs = zss[i + 1]
                if next_zs.done and max(current_zs.dd, next_zs.dd) <= min(current_zs.gg, next_zs.gg):
                    LogUtil.info(f"发生【中枢扩展】: ZS {i} 和 ZS {i + 1} 合并升级到 Level {next_level}")

                    # 扩展合并的线段包括两个中枢的所有线段以及它们之间的连接段
                    # 这里简化处理，假设两个中枢的线段是连续的
                    start_idx = current_zs.start.index
                    end_idx = next_zs.end.index
                    combined_lines = self.all_lines[start_idx: end_idx + 1]

                    promoted = self._create_promoted_zs(current_zs, combined_lines, next_level, 3)
                    promoted_zss.append(promoted)
                    i += 2  # 跳过两个已合并的中枢
                    continue

            # --- 规则3: 无升级，生成普通走势类型 ---
            if i + 1 < len(zss):
                next_zs = zss[i + 1]
                start_fx, end_fx = None, None
                trend_type = ''

                # 判断是上涨趋势还是下跌趋势
                # 下一个中枢的低点 > 当前中枢的高点 -> 上涨
                if next_zs.dd > current_zs.gg:
                    start_fx = FX(_type='di', val=current_zs.dd, k=current_zs.start.start.k)
                    end_fx = FX(_type='ding', val=next_zs.gg, k=next_zs.end.end.k)
                    trend_type = 'up'
                # 下一个中枢的高点 < 当前中枢的低点 -> 下跌
                elif next_zs.gg < current_zs.dd:
                    start_fx = FX(_type='ding', val=current_zs.gg, k=current_zs.start.start.k)
                    end_fx = FX(_type='di', val=next_zs.dd, k=next_zs.end.end.k)
                    trend_type = 'down'

                if trend_type:
                    LogUtil.info(f"生成 {trend_type} 走势: 从 ZS {i} 到 ZS {i + 1}")
                    trend = LINE(start=start_fx, end=end_fx, type=trend_type, index=len(trend_lines))
                    trend_lines.append(trend)

            i += 1

        return promoted_zss, trend_lines

    def _create_promoted_zs(self, base_zs: ZS, lines: List[LINE], level: int, group_size: int) -> ZS:
        """
        根据给定的线段和分组规则，创建升级后的高级别中枢
        :param base_zs: 升级前的基础中枢 (用于获取元数据)
        :param lines: 用于构建新中枢的所有线段
        :param level: 新中枢的目标级别
        :param group_size: 分组大小 (例如，9段延伸按3段分组)
        :return: 新的、更高级别的中枢对象
        """
        if len(lines) < group_size * 3:
            LogUtil.info(f"线段数量 {len(lines)} 不足以按 {group_size * 3} 进行分组升级，跳过。")
            return None

        # 将所有线段按 group_size 分成若干组
        groups = [lines[i:i + group_size] for i in range(0, len(lines), group_size)]

        # 这里严格按照您描述的“取前9段”的逻辑，即只用前三组来定义ZG/ZD
        group1 = groups[0]
        group2 = groups[1]
        group3 = groups[2]

        g1 = max(l.zs_high for l in group1)
        d1 = min(l.zs_low for l in group1)

        g2 = max(l.zs_high for l in group2)
        d2 = min(l.zs_low for l in group2)

        g3 = max(l.zs_high for l in group3)
        d3 = min(l.zs_low for l in group3)

        # 新中枢的ZG/ZD是三组高点中的最低点和三组低点中的最高点
        zg = min(g1, g2, g3)
        zd = max(d1, d2, d3)

        promoted = ZS(zs_type=base_zs.zs_type, start=lines[0], _type=base_zs._type, level=level)
        promoted.zg, promoted.zd = zg, zd
        # GG/DD的计算可以有多种方式，这里简化为取所有段的极值
        promoted.gg = max(l.zs_high for l in lines)
        promoted.dd = min(l.zs_low for l in lines)
        promoted.lines = lines
        promoted.line_num = len(lines)
        promoted.done = True
        promoted.end = lines[-1]

        return promoted