# -*- coding: utf-8 -*-
"""
中枢计算模块
负责根据笔或线段列表，识别和构建中枢。
"""
from typing import List, Tuple, Optional

from chanlun.core.cl_interface import ZS, Config, LINE


def _get_line_high_low(line: 'LINE') -> Tuple[Optional[float], Optional[float]]:
    """
    获取线段的高低点
    """
    if not line or not hasattr(line, 'start') or not line.start or not hasattr(line, 'end') or not line.end:
        return None, None

    start_price = line.start.val
    end_price = line.end.val

    return max(start_price, end_price), min(start_price, end_price)


def create_xd_zs(zs_type: str, lines: List['LINE']) -> List['ZS']:
    """
    创建段内中枢 (优化版本)
    严格按照 "进入段 + 中枢核心(>=3段) + 离开段" 的结构来识别一个完整的中枢。

    关键优化：
    1. 修正了进入段的识别逻辑
    2. 修正了中枢区间的计算方法 (根据用户需求调整)
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
        if not (hasattr(entry_seg, 'type') and hasattr(seg_a, 'type') and hasattr(seg_b, 'type') and hasattr(seg_c,
                                                                                                             'type') and
                entry_seg.type == seg_b.type and seg_a.type == seg_c.type and seg_a.type != seg_b.type):
            i += 1
            continue

        # 2. --- 计算初始中枢区间 [ZD, ZG] ---
        # MODIFIED: 根据用户需求，中枢区间由前三段（a, b, c）的共同重叠区域决定
        g_a, d_a = _get_line_high_low(seg_a)
        g_b, d_b = _get_line_high_low(seg_b)
        g_c, d_c = _get_line_high_low(seg_c)

        if any(p is None for p in [g_a, d_a, g_b, d_b, g_c, d_c]):
            i += 1
            continue

        # MODIFIED: ZG取三个高点中的最低点，ZD取三个低点中的最高点
        zg = min(g_a, g_b, g_c)
        zd = max(d_a, d_b, d_c)

        # 如果不存在重叠区间，则无法构成中枢
        if zd >= zg:
            i += 1
            continue

        # 3. --- 验证进入段是否有效 ---
        entry_high, entry_low = _get_line_high_low(entry_seg)
        if entry_high is None or entry_low is None:
            i += 1
            continue

        # 进入段必须与中枢区间有交集（进入中枢）
        if entry_low > zg or entry_high < zd:
            i += 1
            continue

        # 4. --- 找到了有效的中枢起点，开始向后延伸 ---
        core_lines = [seg_a, seg_b, seg_c]
        # z_segments_info 仍然只保存同向段，用于计算最终的中枢精确范围(gg, dd)
        z_segments_info = [{'high': g_a, 'low': d_a}, {'high': g_c, 'low': d_c}]
        leave_seg = None
        center_valid = True

        # 内循环：处理中枢的延伸和寻找离开段
        j = i + 4  # 指向第4段（潜在的中枢延伸段或离开段）

        while j < len(lines):
            current_seg = lines[j]
            curr_high, curr_low = _get_line_high_low(current_seg)

            if curr_high is None or curr_low is None:
                break

            # 判断是否为离开段
            is_leave_segment = False
            # 离开的判断：当前段的下一段与中枢区间没有重叠
            # NOTE: 这里的zg, zd是初始计算后固定的值
            if j + 1 < len(lines):
                successor = lines[j + 1]
                succ_high, succ_low = _get_line_high_low(successor)
                if succ_high is not None and succ_low is not None:
                    if succ_low > zg or succ_high < zd:  # successor 无交集
                        is_leave_segment = True

            if is_leave_segment:
                # 确认离开段本身必须与中枢有交集
                if curr_low > zg or curr_high < zd:  # 无交集，说明中枢在此之前已经破坏
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

                # REMOVED: 移除中枢延伸时的zg, zd动态更新逻辑
                # 中枢区间 [zg, zd] 由初始三段确定后即固定，不再变化

                j += 1

        if not center_valid:
            i += 1
            continue

        # 5. --- 构建中枢对象 ---
        center_type = seg_b.type
        center = ZS(zs_type=zs_type, start=entry_seg, _type=center_type, level=0)
        center.lines = core_lines
        center.line_num = len(core_lines)

        # MODIFIED: 直接使用初始计算的、固定的zg和zd
        center.zg = zg
        center.zd = zd

        # 计算gg/dd (中枢内同向段的最高/最低)，这部分逻辑不变
        all_highs = [z['high'] for z in z_segments_info]
        all_lows = [z['low'] for z in z_segments_info]

        if all_highs and all_lows:
            center.gg = max(all_highs)
            center.dd = min(all_lows)

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


def calculate_zss(lines: List[LINE]) -> dict:
    """计算所有类型的中枢"""
    zss = create_xd_zs(Config.ZS_TYPE_BZ.value, lines)
    return {Config.ZS_TYPE_BZ.value: zss}
