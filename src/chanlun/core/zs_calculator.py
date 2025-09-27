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
    中枢计算器
    负责根据笔或线段列表，识别和构建中枢。
    能够处理全量及增量数据，并确保计算逻辑的正确性。
    """

    def __init__(self, config: dict):
        """
        初始化
        """
        self.config = config
        zs_type = self.config.get('zs_type_xd', Config.ZS_TYPE_BZ.value)
        # 存储所有已完成的中枢
        self.zss: Dict[str, List[ZS]] = {zs_type: []}
        # 存储当前正在进行中，尚未完成的中枢
        self.pending_zs: Dict[str, Optional[ZS]] = {zs_type: None}
        # 存储接收到的所有线段
        self.all_lines: List[LINE] = []
        # 记录下一次开始搜索新中枢的起始索引
        self._search_start_index: int = 0

    def get_zss(self) -> List[ZS]:
        """
        获取所有计算的中枢（已完成 + 进行中）。
        返回的是一个列表的拷贝，主要用于获取任意时间点的全量状态。
        """
        zs_type = self.config.get('zs_type_xd', Config.ZS_TYPE_BZ.value)
        completed_zss = self.zss.get(zs_type, [])

        all_zss = completed_zss.copy()
        pending = self.pending_zs.get(zs_type)
        if pending:
            all_zss.append(pending)
        return all_zss

    def calculate(self, lines: List[LINE]) -> List[ZS]:
        """
        根据新增的线段列表，增量计算所有类型的中枢。

        该方法现在会返回本次调用中所有发生变化的中枢列表。
        - 如果是首次（全量）计算，它会返回所有找到的已完成中枢和当前进行中的中枢。
        - 如果是后续（增量）计算，它会返回新完成的中枢，以及被更新（延伸或新创建）的进行中中枢。

        :param lines: 新的线段列表
        :return: 本次调用中新完成的或状态更新的中枢列表
        """
        if not lines:
            LogUtil.info("传入的线段列表为空，不进行计算。")
            return []

        # 在多级别分析中，每次都是全量计算
        self.all_lines = lines
        zs_type = self.config.get('zs_type_xd', 'bz')
        self.zss[zs_type] = []
        self.pending_zs[zs_type] = None
        self._search_start_index = 0

        self._create_xd_zs_full(zs_type)

        return self.get_zss()

    @staticmethod
    def _get_line_high_low(line: 'LINE') -> Tuple[Optional[float], Optional[float]]:
        """
        获取线段的高低点
        """
        if not line or not hasattr(line, 'start') or not line.start or not hasattr(line, 'end') or not line.end:
            return None, None

        start_price = line.start.val
        end_price = line.end.val

        return max(start_price, end_price), min(start_price, end_price)

    def _create_xd_zs_full(self, zs_type: str):
        """
        全量创建段内中枢
        """
        i = 0
        while i <= len(self.all_lines) - 4:
            # 尝试以 lines[i] 作为进入段来构建中枢
            entry_seg = self.all_lines[i]
            seg_a, seg_b, seg_c = self.all_lines[i + 1], self.all_lines[i + 2], self.all_lines[i + 3]

            if not (hasattr(entry_seg, 'type') and hasattr(seg_a, 'type') and hasattr(seg_b, 'type') and hasattr(seg_c,
                                                                                                                 'type') and
                    entry_seg.type == seg_b.type and seg_a.type == seg_c.type and seg_a.type != seg_b.type):
                i += 1
                continue

            # 计算初始中枢区间 [ZD, ZG]
            g_a, d_a = self._get_line_high_low(seg_a)
            g_b, d_b = self._get_line_high_low(seg_b)
            g_c, d_c = self._get_line_high_low(seg_c)

            if any(p is None for p in [g_a, d_a, g_b, d_b, g_c, d_c]):
                i += 1
                continue

            zg, zd = min(g_a, g_b, g_c), max(d_a, d_b, d_c)

            if zd >= zg:
                i += 1
                continue

            # 验证进入段是否有效
            entry_high, entry_low = self._get_line_high_low(entry_seg)
            if entry_high is None or entry_low is None or (entry_low > zg or entry_high < zd):
                i += 1
                continue

            # 找到了有效的中枢起点，创建中枢对象并开始延伸检查
            LogUtil.info(f"在索引 {i} 处找到潜在中枢，进入段: {i}，核心段: {i + 1}, {i + 2}, {i + 3}。")
            core_lines = [seg_a, seg_b, seg_c]
            center = ZS(zs_type=zs_type, start=entry_seg, _type=seg_b.type, level=0)
            center.lines, center.line_num = core_lines, len(core_lines)
            center.zg, center.zd = zg, zd
            center.gg, center.dd = max(g_a, g_c), min(d_a, d_c)  # gg/dd 由同向的 a, c 初始化
            center.real = True

            completed, next_search_start = self._extend_and_check_complete(center, i + 4)
            if completed:
                LogUtil.info(f"新发现的中枢已完成: 从 {center.start.start.k.date} 到 {center.end.end.k.date}")
                center.index = len(self.zss[zs_type])
                self.zss[zs_type].append(center)
            else:
                LogUtil.info(f"新发现的中枢成为进行时: 从 {center.start.start.k.date} 开始")
                self.pending_zs[zs_type] = center

            i = next_search_start

    def _extend_and_check_complete(self, center: ZS, start_j: int) -> Tuple[bool, int]:
        """
        从中枢核心的下一根线段开始，检查中枢的延伸或完成。
        这是修正后的核心逻辑，参考了全量计算方法。
        :param center: 当前中枢对象 (可能是 pending_zs 或新发现的)
        :param start_j: 在 self.all_lines 中开始检查的索引
        :return: (是否完成: bool, 下一个搜索起始索引: int)
        """
        lines = self.all_lines
        zg, zd = center.zg, center.zd

        j = start_j
        while j < len(lines):
            # 至少需要 j+1 根线段才能判断 j 是否为离开段
            if j + 1 >= len(lines):
                # 没有足够的线段来判断，当前线段 j 只能是延伸
                current_seg = lines[j]
                curr_high, curr_low = self._get_line_high_low(current_seg)
                if curr_low is not None and (curr_low > zg or curr_high < zd):
                    # 如果最后一根线段自身就离开了中枢，则中枢在 j-1 处结束
                    center.end = lines[j - 1]
                    center.done = True
                    return True, j
                # 否则，作为延伸并保持未完成状态
                break

            current_seg = lines[j]  # 潜在的离开段
            successor_seg = lines[j + 1]  # 用于判断离开的后续段

            curr_high, curr_low = self._get_line_high_low(current_seg)
            succ_high, succ_low = self._get_line_high_low(successor_seg)

            if any(p is None for p in [curr_high, curr_low, succ_high, succ_low]):
                # 数据无效，无法判断，暂时中断
                break

                # 核心判断：如果后继段与中枢区间无重叠，则当前段为离开段
            is_leave = (succ_low > zg or succ_high < zd)

            if is_leave:
                # 确认离开段本身必须与中枢有交集
                if curr_low > zg or curr_high < zd:
                    # 离开段无效，说明中枢在 j-1 处已经被破坏
                    center.end = lines[j - 1]
                    center.done = True
                    # 下次从 j-1 开始重新寻找，因为它可能是一个新中枢的进入段
                    return True, j - 1
                else:
                    # 找到有效的离开段，中枢完成
                    center.end = current_seg
                    center.done = True
                    # 下次从中枢的离开段开始寻找新中枢
                    return True, j

            # 如果不是离开，则为延伸
            LogUtil.info(f"线段 {j} 延伸中枢。")
            center.lines.append(current_seg)
            center.line_num += 1
            # 更新 gg/dd (中枢内同向段的最高/最低点)
            # center.lines[0] 是 seg_a
            if current_seg.type == center.lines[0].type:
                center.gg = max(center.gg, curr_high)
                center.dd = min(center.dd, curr_low)
            j += 1

        # 循环结束，说明所有线段都被作为延伸处理，中枢仍未完成
        center.end = lines[-1] if lines else center.start
        center.done = False
        return False, len(lines)


class MultiLevelAnalyzer:
    """
    多级别走势分析器
    """

    def __init__(self, config: dict):
        self.config = config
        self.levels = self.config.get('levels', ['1m', '5m', '30m', 'D'])
        # 存储每个级别的分析结果
        self.results: Dict[str, Dict[str, Union[List[LINE], List[ZS]]]] = {}

    def run(self, base_lines: List[LINE]):
        """
        从最底层的线段开始，执行多级别分析
        :param base_lines: 最原始、最低级别的线段列表
        """
        current_lines = base_lines

        for i, level_name in enumerate(self.levels):
            LogUtil.info(f"\n{'=' * 20} Analyzing Level: {level_name} {'=' * 20}")
            if len(current_lines) < 4:
                LogUtil.info(f"Not enough lines ({len(current_lines)}) to analyze level {level_name}. Stopping.")
                break

            # 1. 使用 ZsCalculator 计算当前级别的中枢
            zs_calculator = ZsCalculator(self.config)
            level_zss = zs_calculator.calculate(current_lines)

            for zs in level_zss:
                zs.level = i  # 标记中枢的级别

            LogUtil.info(f"Found {len(level_zss)} ZS at {level_name} level.")

            # 2. 识别并生成更高级别的中枢和当前级别的走势类型
            next_level_name = self.levels[i + 1] if i + 1 < len(self.levels) else "Final"
            promoted_zss, trend_lines = self._promote_and_generate_trends(level_zss, current_lines, i + 1)

            LogUtil.info(f"Generated {len(promoted_zss)} promoted ZS for {next_level_name} level.")
            LogUtil.info(f"Generated {len(trend_lines)} trend-lines for {level_name} level.")

            # 3. 存储结果
            self.results[level_name] = {
                "zss": level_zss,
                "trend_lines": trend_lines
            }
            if promoted_zss:
                # 推广出来的中枢属于下一个级别
                if next_level_name not in self.results:
                    self.results[next_level_name] = {"zss": [], "trend_lines": []}
                self.results[next_level_name]["zss"].extend(promoted_zss)

            # 4. 准备下一次循环
            current_lines = trend_lines

        return self.results

    def _promote_and_generate_trends(self, zss: List[ZS], lines: List[LINE], next_level: int) -> Tuple[
        List[ZS], List[LINE]]:
        """
        核心逻辑：处理中枢的延伸、扩展，并生成走势类型
        :param zss: 当前级别已识别的中枢列表
        :param lines: 构成这些中枢的线段列表
        :param next_level: 推广后中枢的目标级别
        :return: (推广生成的高级别中枢列表, 当前级别的走势类型列表)
        """
        promoted_zss: List[ZS] = []
        trend_lines: List[LINE] = []

        i = 0
        last_line_end_fx = lines[0].start

        while i < len(zss):
            current_zs = zss[i]

            # --- 规则1: 中枢延伸 (9段升级) ---
            if current_zs.done and len(current_zs.lines) >= 9:
                LogUtil.info(f"Found Extension at ZS index {i} (level {current_zs.level}). Promoting...")
                promoted = self._create_promoted_zs_from_extension(current_zs, next_level)
                promoted_zss.append(promoted)

                # 创建连接到这个新中枢的走势
                trend = self._create_trend_line_to_zs(last_line_end_fx, promoted, len(trend_lines))
                if trend:
                    trend_lines.append(trend)

                last_line_end_fx = trend.end if trend else last_line_end_fx
                i += 1
                continue

            # --- 规则2: 中枢扩展 ---
            if i + 1 < len(zss):
                next_zs = zss[i + 1]
                # 条件：两个连续中枢GG/DD有重叠
                if current_zs.done and next_zs.done and \
                        max(current_zs.dd, next_zs.dd) <= min(current_zs.gg, next_zs.gg):
                    LogUtil.info(f"Found Expansion between ZS index {i} and {i + 1}. Promoting...")

                    # 找到两个中枢之间的连接线段
                    try:
                        connecting_line_index = lines.index(current_zs.end)
                        connecting_line = lines[connecting_line_index]
                    except (ValueError, IndexError):
                        # 如果找不到离开段，则无法构成扩展
                        # 这种情况通常不应该发生，除非中枢定义有误
                        i += 1
                        continue

                    promoted = self._create_promoted_zs_from_expansion(current_zs, next_zs, connecting_line, next_level)
                    promoted_zss.append(promoted)

                    # 创建连接到这个新中枢的走势
                    trend = self._create_trend_line_to_zs(last_line_end_fx, promoted, len(trend_lines))
                    if trend:
                        trend_lines.append(trend)

                    last_line_end_fx = trend.end if trend else last_line_end_fx
                    i += 2  # 跳过两个已合并的中枢
                    continue

            # --- 规则3: 无升级，生成普通走势类型 ---
            if not current_zs.done:  # 如果是未完成的中枢，则趋势也未完成
                break

            if i + 1 < len(zss):
                next_zs = zss[i + 1]
                start_fx, end_fx = None, None

                # 根据方向确定走势的起止点
                if next_zs.dd > current_zs.gg:  # 上涨走势
                    start_fx = FX('di', current_zs.dd, current_zs.start.start.k)  # 简化k线来源
                    end_fx = FX('ding', next_zs.gg, next_zs.end.end.k)
                    trend_type = 'up'
                elif next_zs.gg < current_zs.dd:  # 下跌走势
                    start_fx = FX('ding', current_zs.gg, current_zs.start.start.k)
                    end_fx = FX('di', next_zs.dd, next_zs.end.end.k)
                    trend_type = 'down'

                if start_fx and end_fx:
                    trend = LINE(start_fx, end_fx, trend_type, index=len(trend_lines))
                    trend_lines.append(trend)
                    last_line_end_fx = trend.end

            i += 1

        return promoted_zss, trend_lines

    def _create_promoted_zs_from_extension(self, zs: ZS, level: int) -> ZS:
        """根据9段延伸规则创建高级别中枢"""
        # 按3+3+3形式组合
        group1_lines = zs.lines[0:3]
        group2_lines = zs.lines[3:6]
        group3_lines = zs.lines[6:9]

        g1 = max(l.high for l in group1_lines)
        d1 = min(l.low for l in group1_lines)

        g2 = max(l.high for l in group2_lines)
        d2 = min(l.low for l in group2_lines)

        g3 = max(l.high for l in group3_lines)
        d3 = min(l.low for l in group3_lines)

        # 新中枢的ZG/ZD是三组高点中的最低点和三组低点中的最高点
        zg = min(g1, g2, g3)
        zd = max(d1, d2, d3)

        promoted = ZS(zs.zs_type, zs.start, zs.type, level)
        promoted.zg, promoted.zd = zg, zd
        promoted.gg, promoted.dd = max(g1, g3), min(d1, d3)  # 简化处理
        promoted.lines = zs.lines
        promoted.line_num = len(zs.lines)
        promoted.done = True
        promoted.end = zs.end
        return promoted

    def _create_promoted_zs_from_expansion(self, zs1: ZS, zs2: ZS, connecting_line: LINE, level: int) -> ZS:
        """根据中枢扩展规则创建高级别中枢"""
        all_lines = zs1.lines + [connecting_line] + zs2.lines

        # 同样按3段式分解，这里简化处理，取前、中、后三部分
        group_size = math.ceil(len(all_lines) / 3)
        group1_lines = all_lines[:group_size]
        group2_lines = all_lines[group_size:2 * group_size]
        group3_lines = all_lines[2 * group_size:]

        g1 = max(l.high for l in group1_lines) if group1_lines else -1
        d1 = min(l.low for l in group1_lines) if group1_lines else float('inf')

        g2 = max(l.high for l in group2_lines) if group2_lines else -1
        d2 = min(l.low for l in group2_lines) if group2_lines else float('inf')

        g3 = max(l.high for l in group3_lines) if group3_lines else -1
        d3 = min(l.low for l in group3_lines) if group3_lines else float('inf')

        zg = min(g for g in [g1, g2, g3] if g != -1)
        zd = max(d for d in [d1, d2, d3] if d != float('inf'))

        promoted = ZS(zs_type = zs1.zs_type, start = zs1.start, _type = zs1.type, level = level)  # type可根据具体情况判断
        promoted.zg, promoted.zd = zg, zd
        promoted.gg, promoted.dd = max(g1, g3), min(d1, d3)
        promoted.lines = all_lines
        promoted.line_num = len(all_lines)
        promoted.done = True
        promoted.end = zs2.end
        return promoted

    def _create_trend_line_to_zs(self, last_fx: FX, zs: ZS, index: int) -> Optional[LINE]:
        """创建一个连接到新晋中枢的走势线段"""
        start_fx = last_fx
        trend_type = ''

        # 判断是上涨还是下跌进入该中枢
        if zs.dd > start_fx.val:  # 上涨
            end_fx = FX('ding', zs.gg, zs.start.end.k)
            trend_type = 'up'
        else:  # 下跌
            end_fx = FX('di', zs.dd, zs.start.end.k)
            trend_type = 'down'

        if trend_type:
            return LINE(start_fx, end_fx, trend_type, index)
        return None