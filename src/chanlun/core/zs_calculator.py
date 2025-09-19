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
    """

    def __init__(self, config: dict):
        self.config = config
        self.zss: Dict[str, List[ZS]] = {}

    def get_zss(self) -> Dict[str, List[ZS]]:
        """获取所有计算的中枢"""
        return self.zss

    def calculate(self, lines: List[LINE]):
        """计算所有类型的中枢"""
        LogUtil.info("开始计算中枢...")
        # 目前只实现标准中枢
        bz_zss = self._create_xd_zs(Config.ZS_TYPE_BZ.value, lines)
        self.zss = {Config.ZS_TYPE_BZ.value: bz_zss}
        LogUtil.info(f"中枢计算完成，共找到 {len(bz_zss)} 个标准中枢。")

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

    def _create_xd_zs(self, zs_type: str, lines: List['LINE']) -> List['ZS']:
        """
        创建段内中枢
        严格按照 "进入段 + 中枢核心(>=3段) + 离开段" 的结构来识别一个完整的中枢。
        """
        if len(lines) < 5:
            LogUtil.info(f"线段数量 {len(lines)} 少于 5，无法形成中枢。")
            return []

        zss: List[ZS] = []
        i = 0  # 主循环索引，指向潜在的进入段

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
            z_segments_info = [{'high': g_a, 'low': d_a}, {'high': g_c, 'low': d_c}]
            leave_seg = None
            center_valid = True

            j = i + 4
            while j < len(lines):
                current_seg = lines[j]
                curr_high, curr_low = self._get_line_high_low(current_seg)

                if curr_high is None or curr_low is None:
                    break

                is_leave_segment = False
                if j + 1 < len(lines):
                    successor = lines[j + 1]
                    succ_high, succ_low = self._get_line_high_low(successor)
                    if succ_high is not None and succ_low is not None and (succ_low > zg or succ_high < zd):
                        is_leave_segment = True

                if is_leave_segment:
                    if curr_low > zg or curr_high < zd:
                        center_valid = False
                        LogUtil.info(f"当前段 {j} 未进入中枢，中枢在此之前已破坏。")
                    else:
                        leave_seg = current_seg
                        i = j
                        LogUtil.info(f"找到离开段 {j}，中枢完成。")
                    break
                else:
                    LogUtil.info(f"线段 {j} 延伸中枢。")
                    core_lines.append(current_seg)
                    if current_seg.type == seg_a.type:
                        z_segments_info.append({'high': curr_high, 'low': curr_low})
                    j += 1

            if not center_valid:
                i += 1
                continue

            LogUtil.info(f"构建中枢对象: 核心段数量 {len(core_lines)}")
            center = ZS(zs_type=zs_type, start=entry_seg, _type=seg_b.type, level=0)
            center.lines, center.line_num = core_lines, len(core_lines)
            center.zg, center.zd = zg, zd

            all_highs = [z['high'] for z in z_segments_info]
            all_lows = [z['low'] for z in z_segments_info]
            if all_highs and all_lows:
                center.gg, center.dd = max(all_highs), min(all_lows)

            center.real = True

            if leave_seg:
                center.end, center.done = leave_seg, True
                LogUtil.info(f"已完成中枢: 从 {center.start.start.k.date} 到 {center.end.end.k.date}")
            else:
                center.end, center.done = lines[-1], False
                i = len(lines)
                LogUtil.info(f"未完成中枢: 从 {center.start.start.k.date} 开始")

            zss.append(center)

        LogUtil.info("重新为所有中枢编号。")
        for idx, center in enumerate(zss):
            center.index = idx
        return zss
