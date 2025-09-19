# -*- coding: utf-8 -*-
"""
中枢计算模块
负责根据笔或线段列表，识别和构建中枢。
"""
from typing import List, Tuple, Optional, Dict

from chanlun.core.cl_interface import ZS, Config, LINE
from chanlun.tools.log_util import LogUtil


class ZsCalculator:
    """
    中枢计算器
    负责根据笔或线段列表，识别和构建中枢。
    能够处理全量及增量数据。
    """

    def __init__(self, config: dict):
        """
        初始化
        """
        self.config = config
        # 存储所有已完成的中枢
        self.zss: Dict[str, List[ZS]] = {Config.ZS_TYPE_BZ.value: []}
        # 存储当前正在进行中，尚未完成的中枢
        self.pending_zs: Dict[str, Optional[ZS]] = {Config.ZS_TYPE_BZ.value: None}
        # 存储接收到的所有线段
        self.all_lines: List[LINE] = []
        # 记录下一次开始搜索新中枢的起始索引
        self._search_start_index: int = 0

    def get_zss(self) -> Dict[str, List[ZS]]:
        """获取所有计算的中枢"""
        zs_type = self.config.get('zs_type_xd', Config.ZS_TYPE_BZ.value)
        return self.zss.get(zs_type)

    def calculate(self, lines: List[LINE]):
        """
        计算所有类型的中枢。
        此方法可处理增量数据。传入的 lines 为新增的线段列表。
        对于增量更新，此方法会尝试延续当前进行中的中枢，或寻找新的中枢。
        """
        if not lines:
            LogUtil.info("传入的线段列表为空，不进行计算。")
            return

        LogUtil.info(f"接收到 {len(lines)} 条新线段，开始增量计算中枢...")
        self.all_lines.extend(lines)

        # 目前只实现标准中枢的增量计算
        self._create_xd_zs_incremental(Config.ZS_TYPE_BZ.value, self.all_lines)

        LogUtil.info(f"中枢增量计算完成。已完成中枢: {len(self.zss.get(Config.ZS_TYPE_BZ.value, []))} 个, "
                     f"进行中中枢: {'是' if self.pending_zs.get(Config.ZS_TYPE_BZ.value) else '否'}")

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

    def _create_xd_zs_incremental(self, zs_type: str, lines: List['LINE']):
        """
        创建段内中枢 (增量更新版本)
        该方法会更新 self.zss 和 self.pending_zs 状态。
        1. 检查并更新进行中的中枢 (pending_zs)。
        2. 如果没有进行中的中枢，从上次结束的位置开始寻找新的中枢。
        """
        # 步骤 1: 处理进行中的中枢
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

        # 步骤 2: 从上次记录的索引开始，寻找新的中枢
        i = self._search_start_index
        while i <= len(lines) - 4:
            LogUtil.info(f"尝试从第 {i} 条线段构建中枢...")
            entry_seg = lines[i]
            seg_a, seg_b, seg_c = lines[i + 1], lines[i + 2], lines[i + 3]

            if not (hasattr(entry_seg, 'type') and hasattr(seg_a, 'type') and hasattr(seg_b, 'type') and hasattr(seg_c,
                                                                                                                 'type') and
                    entry_seg.type == seg_b.type and seg_a.type == seg_c.type and seg_a.type != seg_b.type):
                i += 1
                continue

            g_a, d_a = self._get_line_high_low(seg_a)
            g_b, d_b = self._get_line_high_low(seg_b)
            g_c, d_c = self._get_line_high_low(seg_c)

            if any(p is None for p in [g_a, d_a, g_b, d_b, g_c, d_c]):
                i += 1
                continue

            LogUtil.info(f"找到潜在中枢核心段: {i + 1}, {i + 2}, {i + 3}。")
            zg, zd = min(g_a, g_b, g_c), max(d_a, d_b, d_c)

            if zd >= zg:
                LogUtil.info(f"核心段无重叠区间 (zd: {zd} >= zg: {zg})，无法构成中枢。")
                i += 1
                continue
            LogUtil.info(f"计算出初始中枢区间为: zd={zd}, zg={zg}")

            entry_high, entry_low = self._get_line_high_low(entry_seg)
            if entry_high is None or entry_low is None:
                i += 1
                continue

            if entry_low > zg or entry_high < zd:
                LogUtil.info(f"进入段 {i} (low: {entry_low}, high: {entry_high}) 未进入中枢区间，跳过。")
                i += 1
                continue

            LogUtil.info(f"第 {i} 条线段确认为有效进入段。开始检查中枢延伸...")
            core_lines = [seg_a, seg_b, seg_c]
            center = ZS(zs_type=zs_type, start=entry_seg, _type=seg_b.type, level=0)
            center.lines, center.line_num = core_lines, len(core_lines)
            center.zg, center.zd = zg, zd

            center.gg, center.dd = max(g_a, g_b, g_c), min(d_a, d_b, d_c)
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
                i = len(lines)

        self._search_start_index = i

    def _extend_and_check_complete(self, center: ZS, start_j: int) -> Tuple[bool, int]:
        """
        从中枢核心的下一根线段开始，检查中枢的延伸或完成。
        :param center: 当前中枢对象 (可能是 pending_zs 或新发现的)
        :param start_j: 在 self.all_lines 中开始检查的索引
        :return: (是否完成: bool, 下一个搜索起始索引: int)
        """
        lines = self.all_lines
        zg, zd = center.zg, center.zd

        j = start_j
        while j < len(lines):
            current_seg = lines[j]
            curr_high, curr_low = self._get_line_high_low(current_seg)
            if curr_high is None or curr_low is None:
                break

            if curr_low > zg or curr_high < zd:
                center.end = lines[j - 1]
                center.done = True
                LogUtil.info(f"线段 {j} (H:{curr_high}, L:{curr_low}) 与中枢 (ZG:{zg}, ZD:{zd}) 出现缺口，中枢完成。")
                return True, j

            LogUtil.info(f"线段 {j} 延伸中枢。")
            center.lines.append(current_seg)
            center.line_num += 1
            center.gg = max(center.gg, curr_high)
            center.dd = min(center.dd, curr_low)
            j += 1

        center.end = lines[-1]
        center.done = False
        return False, len(lines)
