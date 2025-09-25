# -*- coding: utf-8 -*-
"""
中枢计算模块
负责根据笔或线段列表，识别和构建中枢。
该模块已被重构，以确保增量计算的逻辑与标准的全量计算逻辑一致。
"""
from typing import List, Tuple, Optional, Dict

from chanlun.core.cl_interface import ZS, Config, LINE
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

        LogUtil.info(f"接收到 {len(lines)} 条新线段，开始增量计算中枢...")
        self.all_lines.extend(lines)

        zs_type = self.config.get('zs_type_xd', Config.ZS_TYPE_BZ.value)

        # 记录计算前的状态
        num_completed_before = len(self.zss.get(zs_type, []))
        pending_zs_before = self.pending_zs.get(zs_type)
        # 记录原进行中枢的线段数，用于判断是否延伸
        pending_lines_count_before = len(pending_zs_before.lines) if pending_zs_before else 0

        self._create_xd_zs_incremental(zs_type)

        # --- 根据状态变化，确定返回值 ---

        # 1. 获取新完成的中枢
        newly_completed_zss = self.zss[zs_type][num_completed_before:]

        # 2. 检查进行中中枢的变化
        updated_pending_zs_list = []
        pending_zs_after = self.pending_zs.get(zs_type)
        if pending_zs_after:
            # Case A: 新创建了一个进行中枢
            is_new_pending = not pending_zs_before
            # Case B: 原有的进行中枢被延伸了
            is_extended_pending = (
                    pending_zs_before and
                    pending_zs_after is pending_zs_before and
                    len(pending_zs_after.lines) > pending_lines_count_before
            )
            if is_new_pending or is_extended_pending:
                updated_pending_zs_list.append(pending_zs_after)

        # 组合结果并返回
        result = newly_completed_zss + updated_pending_zs_list

        # --- 日志记录 ---
        num_completed_after = len(self.zss.get(zs_type, []))
        LogUtil.info(f"中枢增量计算完成。已完成中枢: {num_completed_after} 个, "
                     f"进行中中枢: {'是' if self.pending_zs.get(zs_type) else '否'}")

        if result:
            LogUtil.info(f"本次调用返回 {len(result)} 个已更新或新完成的中枢。")

        return result

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

    def _create_xd_zs_incremental(self, zs_type: str):
        """
        创建段内中枢 (增量更新版本)
        该方法会更新 self.zss 和 self.pending_zs 状态。
        1. 检查并更新进行中的中枢 (pending_zs)。
        2. 如果没有进行中的中枢，或进行中枢完成后，从上次结束的位置继续寻找新的中枢。
        """
        # 步骤 1: 优先处理进行中的中枢
        pending = self.pending_zs.get(zs_type)
        if pending:
            LogUtil.info(f"检查进行中的中枢 (开始于 {pending.start.start.k.date})...")
            last_core_line_in_pending = pending.lines[-1]
            try:
                # 从最后一个核心段之后开始检查延伸或离开
                start_j = self.all_lines.index(last_core_line_in_pending) + 1
            except ValueError:
                LogUtil.error("严重错误: 进行中枢的线段不在总线段列表中。重置搜索。")
                self.pending_zs[zs_type] = None
                self._search_start_index = 0
            else:
                completed, next_search_start = self._extend_and_check_complete(pending, start_j)
                if completed:
                    LogUtil.info("进行中的中枢已完成。")
                    pending.index = len(self.zss[zs_type])
                    self.zss[zs_type].append(pending)
                    self.pending_zs[zs_type] = None
                    self._search_start_index = next_search_start
                else:
                    LogUtil.info("进行中的中枢已延伸，但未完成。")
                    # 进行中的中枢消耗了所有新线段，直接返回，等待下一次增量数据
                    return

        # 步骤 2: 从上次记录的索引开始，循环寻找新的中枢
        i = self._search_start_index
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
                i = next_search_start
            else:
                LogUtil.info(f"新发现的中枢成为进行时: 从 {center.start.start.k.date} 开始")
                self.pending_zs[zs_type] = center
                # 成为进行时意味着消耗了所有后续线段，结束本次循环
                i = len(self.all_lines)

        self._search_start_index = i

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
