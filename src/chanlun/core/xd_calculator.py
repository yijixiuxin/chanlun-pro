# -*- coding: utf-8 -*-
"""
线段计算模块
负责将笔合并且划分为线段。这是缠论中较为复杂的部分。
"""
from typing import List, Optional, Union, Dict

from chanlun.core.cl_interface import BI, XD
from chanlun.tools.log_util import LogUtil


class XdCalculator:
    """
    线段计算器
    负责将笔（BI）合并并划分为线段（XD）。这是缠论中较为复杂的部分。
    该计算器支持全量计算和增量计算。
    """

    def __init__(self, config: dict):
        """
        初始化线段计算器。
        :param config: 配置字典。
        """
        self.config = config
        self.xds: List[XD] = []

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

    def _find_critical_bi_and_truncate(self, all_bis: List[BI]) -> int:
        """
        找到一个关键的笔作为分析起点，并返回其索引。
        这主要用于首次全量计算时，尝试找到一个明确的趋势转折点。
        """
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
                LogUtil.info(f"在索引 {i} 处找到关键笔。从此开始分析。")
                return i

        LogUtil.warning("未找到关键笔。从索引 0 开始分析。")
        return 0

    def _get_characteristic_sequence(self, segment_bis: List[BI], segment_type: str) -> List[BI]:
        """从线段的笔列表中获取其特征序列"""
        cs_type = 'down' if segment_type == 'up' else 'up'
        return [bi for bi in segment_bis if bi.type == cs_type]

    def _bi_to_dict(self, bi: BI) -> dict:
        """将BI对象转换为包含高低点的字典格式，方便处理"""
        return {
            'bi': bi,
            'high': self._get_bi_high(bi),
            'low': self._get_bi_low(bi),
            'type': bi.type
        }

    def _check_inclusion_dict(self, bi1: dict, bi2: dict, direction: str) -> bool:
        """检查字典格式的两笔是否存在包含关系"""
        high1, low1 = bi1.get('high'), bi1.get('low')
        high2, low2 = bi2.get('high'), bi2.get('low')
        return high1 >= high2 and low1 <= low2

    def _process_inclusion(self, bis: Union[List[BI], List[dict]], direction: str) -> List[dict]:
        """
        对特征序列进行包含关系处理（重构版，写入新列表）

        :param bis: 待处理的 BI 序列（可以是对象列表或字典列表）
        :param direction: 处理方向 ('up' 或 'down')
        :return: 处理包含关系后的字典列表
        """
        if not bis:
            return []

        # 1. 确保 processed 是一个字典列表
        processed: list[dict] = []
        if isinstance(bis[0], dict):
            # 假设 bis 已经是 List[dict]
            processed = bis
        else:
            processed = [self._bi_to_dict(bi) for bi in bis]

        if len(processed) < 2:
            return processed

        # 2. 迭代处理，将结果写入 new_processed
        new_processed: list[dict] = []
        # 先将第一个元素放入新列表
        new_processed.append(processed[0])

        # 从第二个元素开始迭代
        for i in range(1, len(processed)):
            bi1: dict = new_processed[-1]  # 取新列表的最后一个元素
            bi2: dict = processed[i]  # 取原列表的当前元素

            if self._check_inclusion_dict(bi1, bi2, direction):
                # 发现包含关系，合并 bi1 和 bi2
                high1, low1 = bi1.get('high'), bi1.get('low')
                high2, low2 = bi2.get('high'), bi2.get('low')

                if direction == 'down':
                    # 向下笔的包含处理，高点取低，低点取低
                    new_low = min(low1, low2)
                    new_high = min(high1, high2)
                else:  # 'up'
                    # 向上笔的包含处理，高点取高，低点取高
                    new_high = max(high1, high2)
                    new_low = max(low1, low2)

                # 创建合并后的元素
                merged = {
                    'bi': bi1['bi'],  # K线（bi）保留第一个的
                    'high': new_high,
                    'low': new_low,
                    'type': bi1['type'],  # 类型保留第一个的
                    'is_merged': True,
                    # 合并 'original_bis' 列表
                    'original_bis': bi1.get('original_bis', [bi1.get('bi')]) + \
                                    bi2.get('original_bis', [bi2.get('bi')])
                }

                # 用合并后的元素替换新列表的最后一个元素
                new_processed[-1] = merged
            else:
                # 没有包含关系，将当前元素 bi2 添加到新列表
                new_processed.append(bi2)

        return new_processed

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
        """根据分型的中间笔确定线段的结束笔"""
        target_bi = None
        if middle_cs.get('is_merged') and 'original_bis' in middle_cs:
            original_bis = middle_cs.get('original_bis', [])
            if original_bis:
                target_bi = original_bis[0]  # 使用合并前的第一笔来定位
            else:
                LogUtil.warning("中间笔为合并笔，但其 'original_bis' 为空。")
                return None
        else:
            target_bi = middle_cs['bi']

        if not target_bi:
            LogUtil.error("无法确定用于定位的目标笔。")
            return None

        try:
            # 找到目标特征序列笔对应的主序列笔，其前一笔就是线段的结束笔
            idx = target_bi.index
            LogUtil.info(f"根据分型的中间笔确定线段的结束笔，特征序列笔索引:{idx}")
            if idx > 0:
                return all_bis[idx - 1]
            else:
                LogUtil.warning("目标笔是列表中的第一根笔，无法找到其前一笔。")
                return None
        except ValueError:
            LogUtil.error("目标笔在 all_bis 列表中未找到。")
            return None

    def _get_extremum_bi_from_cs(self, cs_bi: dict) -> BI:
        """从特征序列笔中获取关键的原始笔（用于构建下一线段）"""
        original_bis = cs_bi.get('original_bis', [cs_bi['bi']])
        return original_bis[0] if original_bis else cs_bi['bi']

    def _calculate_segment_high_low(self, current_segment: Dict) -> (float, float):
        """根据 current_segment 的类型计算其高点和低点"""
        segment_high = 0.0
        segment_low = 0.0
        bis_objects = current_segment.get('bis', [])

        if not bis_objects:
            LogUtil.warning("警告: 'bis' 列表为空, 无法计算高点和低点。")
            return segment_high, segment_low

        first_bi = bis_objects[0]
        last_bi = bis_objects[-1]
        segment_type = current_segment.get('type')

        if segment_type == 'up':
            segment_high = last_bi.end.val
            segment_low = first_bi.start.val
        elif segment_type == 'down':
            segment_high = first_bi.start.val
            segment_low = last_bi.end.val
        else:
            LogUtil.warning(f"警告: 未知的 segment type '{segment_type}'。")
        return segment_high, segment_low

    def calculate(self, bis: List[BI]) -> List[XD]:
        """
        根据笔列表计算线段。
        此方法支持全量和增量计算。
        - 全量计算：当内部线段列表为空时，从头开始计算。
        - 增量计算：当有新笔数据传入时，会从最后一个线段开始回溯，重新评估并延续计算。
        """
        LogUtil.info("开始划分线段")
        all_bis = bis

        is_incremental = bool(self.xds)
        start_index_for_delta = 0

        # 优化：如果输入数据没有新笔，则不重新计算
        if self.xds and all_bis and self.xds[-1].end_line == all_bis[-1]:
            LogUtil.info("输入数据无新笔，跳过线段计算。")
            return []

        # --- 状态处理：确定本次计算的起点 ---
        start_bi_index = 0
        if self.xds:
            # 增量更新模式
            LogUtil.info("增量模式：重新评估最近的线段。")
            last_xd = self.xds.pop()  # 弹出最后一个线段（可能是未完成的），准备重新计算
            start_index_for_delta = len(self.xds)  # 记录pop后的数量，用于返回增量

            # 在新的 all_bis 列表中定位旧的起点
            found = False
            for i, bi in enumerate(all_bis):
                if (bi.start.k.date == last_xd.start_line.start.k.date and
                        bi.end.k.date == last_xd.start_line.end.k.date and
                        bi.type == last_xd.start_line.type):
                    start_bi_index = i
                    found = True
                    break

            if not found:
                LogUtil.warning("无法在'bis'列表中定位上一线段的起点，将执行全量计算。")
                self.xds.clear()
                is_incremental = False
                start_bi_index = self._find_critical_bi_and_truncate(all_bis)
        else:
            # 全量计算模式
            self.xds.clear()
            is_incremental = False
            start_bi_index = self._find_critical_bi_and_truncate(all_bis)

        current_list_index = start_bi_index

        if len(all_bis) < 3:
            LogUtil.warning("笔的数量少于3，无法形成线段。")
            if is_incremental:
                return []  # 增量模式下，如果没有新线段形成，返回空列表
            else:
                return self.xds  # 全量模式下返回空列表

        next_segment_builder = None
        while current_list_index <= len(all_bis) - 3:
            if next_segment_builder:
                LogUtil.debug(f"主循环: 使用 builder 构建新线段")
                start_bi = next_segment_builder['start_bi']
                end_bi = next_segment_builder['end_bi']

                start_idx = start_bi.index
                end_idx = end_bi.index

                if start_idx < 0 or end_idx < 0:
                    LogUtil.error(f"Builder 中的笔索引无效: start_idx={start_idx}, end_idx={end_idx}")
                    current_list_index += 1
                    next_segment_builder = None
                    continue

                if end_idx < start_idx + 2:
                    LogUtil.info("Builder 信息不足以构成三笔，退回标准模式。")
                    current_list_index = start_idx + 1
                    next_segment_builder = None
                    continue

                current_segment_bis = all_bis[start_idx: end_idx + 1]
                current_segment = {'bis': current_segment_bis, 'type': next_segment_builder['next_segment_type']}
                current_list_index = start_idx
                next_segment_builder = None
            else:
                s1, s2, s3 = all_bis[current_list_index:current_list_index + 3]
                if not self._check_bi_overlap(s1, s3):
                    current_list_index += 1
                    continue
                current_segment = {'bis': [s1, s2, s3], 'type': s1.type}

            next_check_idx = current_list_index + len(current_segment['bis'])
            is_completed = False
            break_info = None

            # --- 线段延伸与结束判断循环 ---
            while next_check_idx + 1 < len(all_bis):
                segment_high, segment_low = self._calculate_segment_high_low(current_segment)
                bi_for_fractal_check = all_bis[next_check_idx]
                bi_for_extension_check = all_bis[next_check_idx + 1]

                # --- 处理上涨线段 ---
                if current_segment['type'] == 'up':
                    if self._get_bi_high(bi_for_extension_check) >= segment_high:
                        # 出现新高，线段延伸
                        current_segment['bis'].extend([bi_for_fractal_check, bi_for_extension_check])
                        next_check_idx += 2
                        continue
                    else:
                        # 未创新高，检查是否出现顶分型导致线段结束
                        cs_existing_raw = self._get_characteristic_sequence(current_segment['bis'], 'up')
                        if not cs_existing_raw:
                            current_segment['bis'].extend([bi_for_fractal_check, bi_for_extension_check])
                            next_check_idx += 2
                            continue

                        last_cs_bi = self._process_inclusion(cs_existing_raw, 'up')[-1]
                        last_cs_original_bi = last_cs_bi['bi']

                        lookahead_bis = all_bis[next_check_idx:]
                        bounded_lookahead_bis = []
                        for bi in lookahead_bis:
                            bounded_lookahead_bis.append(bi)
                            if bi.type == 'up' and self._get_bi_high(bi) > segment_high:
                                break

                        # 第一种情况：特征序列出现顶分型
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
                                if segment_end_bi:
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
                                if segment_end_bi:
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
                elif current_segment['type'] == 'down':
                    if self._get_bi_low(bi_for_extension_check) <= segment_low:
                        # 出现新低，线段延伸
                        current_segment['bis'].extend([bi_for_fractal_check, bi_for_extension_check])
                        next_check_idx += 2
                        continue
                    else:
                        # 未创新低，检查是否出现底分型
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

                        # 第一种情况：特征序列出现底分型
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
                                if segment_end_bi:
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

                            check1_passes, cs_middle_bottom, cs_right_bottom = self._check_bottom_fractal(cs_for_check1)
                            next_segment_cs_raw = [bi for bi in bounded_lookahead_bis if bi.type == 'down']
                            processed_cs2 = self._process_inclusion(next_segment_cs_raw, 'up')
                            check2_passes, _, _ = self._check_top_fractal(processed_cs2)

                            if check1_passes and check2_passes:
                                segment_end_bi = self._get_segment_end_bi_from_middle_cs(cs_middle_bottom, all_bis)
                                if segment_end_bi:
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

                if is_completed and break_info:
                    final_end_bi = break_info.get('segment_end_bi')
                    if final_end_bi is None:
                        LogUtil.error("segment_end_bi 为 None，跳过此线段")
                        current_list_index += 1
                        continue

                    final_bi_index = final_end_bi.index
                    if final_bi_index < 0 or final_bi_index < current_list_index:
                        LogUtil.error(f"无效的索引范围: current={current_list_index}, final={final_bi_index}")
                        current_list_index += 1
                        continue

                    final_segment_bis = all_bis[current_list_index: final_bi_index + 1]
                    if not final_segment_bis:
                        LogUtil.warning(f"线段的笔列表为空，跳过")
                        current_list_index = final_bi_index + 1
                        continue

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

                    start_bi_val = final_segment_bis[0].start.val
                    end_bi_val = final_end_bi.end.val
                    if start_bi_val > end_bi_val:
                        xd.zs_high = start_bi_val
                        xd.zs_low = end_bi_val
                    else:
                        xd.zs_high = end_bi_val
                        xd.zs_low = start_bi_val
                    xd.done = True
                    self.xds.append(xd)
                    LogUtil.debug(f"完成 {current_segment['type']} 线段，结束于索引:{final_bi_index}")

                    if break_info.get('start_bi'):
                        next_segment_builder = break_info
                        current_list_index = break_info['start_bi'].index
                        if current_list_index < 0:
                            LogUtil.error("无法找到下一段的起始位置")
                            break
                    else:
                        current_list_index = final_bi_index + 1
                    break

            # --- 处理最后一个未完成的线段 ---
            if not is_completed and next_check_idx >= len(all_bis) - 1:
                if current_segment and current_segment.get('bis'):
                    pending_bis = all_bis[current_list_index:]
                    if not pending_bis:
                        break
                    pending_xd = XD(
                        start=pending_bis[0].start,
                        end=pending_bis[-1].end,
                        start_line=pending_bis[0],
                        end_line=pending_bis[-1],
                        _type=current_segment['type'],
                        index=len(self.xds),
                        default_zs_type=self.config.get('zs_type_xd', None)
                    )
                    pending_xd.high = max(self._get_bi_high(bi) for bi in pending_bis)
                    pending_xd.low = min(self._get_bi_low(bi) for bi in pending_bis)
                    pending_xd.zs_high = pending_xd.high
                    pending_xd.zs_low = pending_xd.low
                    pending_xd.done = False
                    self.xds.append(pending_xd)
                break
        LogUtil.info(f"线段划分结束，完成 {len(self.xds)} 个线段。")
        if is_incremental:
            return self.xds[start_index_for_delta:]
        else:
            return self.xds


if __name__ == '__main__':
    xd_calculator = XdCalculator({})
    data = [{
        "start": {
            "type": "di",
            "val": 6.62,
            "index": 0,

            "k": {
                "k_index": 1798,
                "date": "2024-12-25 11:25:00",
                "h": 6.64,
                "l": 6.62,
                "o": 6.64,
                "c": 6.62,
                "a": 3399.0,
                "index": 762,
                "n": 2,

                "up_qs": "down",
                "klines": [
                    {
                        "index": 1798,
                        "date": "2024-12-25 11:25:00",
                        "h": 6.64,
                        "l": 6.62,
                        "o": 6.63,
                        "c": 6.64,
                        "a": 2426.0
                    },
                    {
                        "index": 1799,
                        "date": "2024-12-25 11:30:00",
                        "h": 6.64,
                        "l": 6.63,
                        "o": 6.64,
                        "c": 6.64,
                        "a": 973.0
                    }
                ]
            },
            "klines": [
                {
                    "k_index": 1797,
                    "date": "2024-12-25 11:20:00",
                    "h": 6.6499999999999995,
                    "l": 6.63,
                    "o": 6.6499999999999995,
                    "c": 6.63,
                    "a": 7440.0,
                    "index": 761,
                    "n": 2,

                    "up_qs": "down",
                    "klines": [
                        {
                            "index": 1796,
                            "date": "2024-12-25 11:15:00",
                            "h": 6.66,
                            "l": 6.63,
                            "o": 6.66,
                            "c": 6.64,
                            "a": 5802.0
                        },
                        {
                            "index": 1797,
                            "date": "2024-12-25 11:20:00",
                            "h": 6.6499999999999995,
                            "l": 6.63,
                            "o": 6.63,
                            "c": 6.63,
                            "a": 1638.0
                        }
                    ]
                },
                {
                    "k_index": 1798,
                    "date": "2024-12-25 11:25:00",
                    "h": 6.64,
                    "l": 6.62,
                    "o": 6.64,
                    "c": 6.62,
                    "a": 3399.0,
                    "index": 762,
                    "n": 2,

                    "up_qs": "down",
                    "klines": [
                        {
                            "index": 1798,
                            "date": "2024-12-25 11:25:00",
                            "h": 6.64,
                            "l": 6.62,
                            "o": 6.63,
                            "c": 6.64,
                            "a": 2426.0
                        },
                        {
                            "index": 1799,
                            "date": "2024-12-25 11:30:00",
                            "h": 6.64,
                            "l": 6.63,
                            "o": 6.64,
                            "c": 6.64,
                            "a": 973.0
                        }
                    ]
                },
                {
                    "k_index": 1800,
                    "date": "2024-12-25 13:05:00",
                    "h": 6.670000000000001,
                    "l": 6.65,
                    "o": 6.670000000000001,
                    "c": 6.65,
                    "a": 1847.0,
                    "index": 763,
                    "n": 2,

                    "up_qs": "up",
                    "klines": [
                        {
                            "index": 1800,
                            "date": "2024-12-25 13:05:00",
                            "h": 6.670000000000001,
                            "l": 6.640000000000001,
                            "o": 6.640000000000001,
                            "c": 6.66,
                            "a": 1296.0
                        },
                        {
                            "index": 1801,
                            "date": "2024-12-25 13:10:00",
                            "h": 6.67,
                            "l": 6.65,
                            "o": 6.66,
                            "c": 6.67,
                            "a": 551.0
                        }
                    ]
                }
            ]
        },
        "end": {
            "type": "ding",
            "val": 6.75,
            "index": 0,

            "k": {
                "k_index": 1810,
                "date": "2024-12-25 13:55:00",
                "h": 6.75,
                "l": 6.72,
                "o": 6.75,
                "c": 6.72,
                "a": 6093.0,
                "index": 766,
                "n": 4,

                "up_qs": "up",
                "klines": [
                    {
                        "index": 1808,
                        "date": "2024-12-25 13:45:00",
                        "h": 6.74,
                        "l": 6.71,
                        "o": 6.72,
                        "c": 6.7299999999999995,
                        "a": 2069.0
                    },
                    {
                        "index": 1809,
                        "date": "2024-12-25 13:50:00",
                        "h": 6.7299999999999995,
                        "l": 6.71,
                        "o": 6.72,
                        "c": 6.71,
                        "a": 1411.0
                    },
                    {
                        "index": 1810,
                        "date": "2024-12-25 13:55:00",
                        "h": 6.75,
                        "l": 6.71,
                        "o": 6.71,
                        "c": 6.74,
                        "a": 1806.0
                    },
                    {
                        "index": 1811,
                        "date": "2024-12-25 14:00:00",
                        "h": 6.739999999999999,
                        "l": 6.72,
                        "o": 6.739999999999999,
                        "c": 6.7299999999999995,
                        "a": 807.0
                    }
                ]
            },
            "klines": [
                {
                    "k_index": 1807,
                    "date": "2024-12-25 13:40:00",
                    "h": 6.72,
                    "l": 6.7,
                    "o": 6.72,
                    "c": 6.7,
                    "a": 2860.0,
                    "index": 765,
                    "n": 2,

                    "up_qs": "up",
                    "klines": [
                        {
                            "index": 1806,
                            "date": "2024-12-25 13:35:00",
                            "h": 6.71,
                            "l": 6.7,
                            "o": 6.7,
                            "c": 6.71,
                            "a": 1759.0
                        },
                        {
                            "index": 1807,
                            "date": "2024-12-25 13:40:00",
                            "h": 6.72,
                            "l": 6.7,
                            "o": 6.71,
                            "c": 6.71,
                            "a": 1101.0
                        }
                    ]
                },
                {
                    "k_index": 1810,
                    "date": "2024-12-25 13:55:00",
                    "h": 6.75,
                    "l": 6.72,
                    "o": 6.75,
                    "c": 6.72,
                    "a": 6093.0,
                    "index": 766,
                    "n": 4,

                    "up_qs": "up",
                    "klines": [
                        {
                            "index": 1808,
                            "date": "2024-12-25 13:45:00",
                            "h": 6.74,
                            "l": 6.71,
                            "o": 6.72,
                            "c": 6.7299999999999995,
                            "a": 2069.0
                        },
                        {
                            "index": 1809,
                            "date": "2024-12-25 13:50:00",
                            "h": 6.7299999999999995,
                            "l": 6.71,
                            "o": 6.72,
                            "c": 6.71,
                            "a": 1411.0
                        },
                        {
                            "index": 1810,
                            "date": "2024-12-25 13:55:00",
                            "h": 6.75,
                            "l": 6.71,
                            "o": 6.71,
                            "c": 6.74,
                            "a": 1806.0
                        },
                        {
                            "index": 1811,
                            "date": "2024-12-25 14:00:00",
                            "h": 6.739999999999999,
                            "l": 6.72,
                            "o": 6.739999999999999,
                            "c": 6.7299999999999995,
                            "a": 807.0
                        }
                    ]
                },
                {
                    "k_index": 1812,
                    "date": "2024-12-25 14:05:00",
                    "h": 6.72,
                    "l": 6.7,
                    "o": 6.72,
                    "c": 6.7,
                    "a": 1501.0,
                    "index": 767,
                    "n": 2,

                    "up_qs": "down",
                    "klines": [
                        {
                            "index": 1812,
                            "date": "2024-12-25 14:05:00",
                            "h": 6.74,
                            "l": 6.7,
                            "o": 6.72,
                            "c": 6.72,
                            "a": 1250.0
                        },
                        {
                            "index": 1813,
                            "date": "2024-12-25 14:10:00",
                            "h": 6.72,
                            "l": 6.71,
                            "o": 6.72,
                            "c": 6.72,
                            "a": 251.0
                        }
                    ]
                }
            ]
        },
        "high": 6.75,
        "low": 6.62,
        "zs_high": 0,
        "zs_low": 0,
        "type": "up",
        "index": 32,
        "mmds": [],
        "bcs": [],

        "zs_type_mmds": {},
        "zs_type_bcs": {},
        "is_split": ""
    }, {
        "start": {
            "type": "di",
            "val": 6.63,
            "index": 0,

            "k": {
                "k_index": 1820,
                "date": "2024-12-25 14:45:00",
                "h": 6.6499999999999995,
                "l": 6.63,
                "o": 6.6499999999999995,
                "c": 6.63,
                "a": 6791.0,
                "index": 772,
                "n": 2,

                "up_qs": "down",
                "klines": [
                    {
                        "index": 1819,
                        "date": "2024-12-25 14:40:00",
                        "h": 6.66,
                        "l": 6.63,
                        "o": 6.6499999999999995,
                        "c": 6.63,
                        "a": 2912.0
                    },
                    {
                        "index": 1820,
                        "date": "2024-12-25 14:45:00",
                        "h": 6.6499999999999995,
                        "l": 6.63,
                        "o": 6.63,
                        "c": 6.64,
                        "a": 3879.0
                    }
                ]
            },
            "klines": [
                {
                    "k_index": 1818,
                    "date": "2024-12-25 14:35:00",
                    "h": 6.68,
                    "l": 6.640000000000001,
                    "o": 6.68,
                    "c": 6.640000000000001,
                    "a": 6403.0,
                    "index": 771,
                    "n": 2,

                    "up_qs": "down",
                    "klines": [
                        {
                            "index": 1817,
                            "date": "2024-12-25 14:30:00",
                            "h": 6.68,
                            "l": 6.66,
                            "o": 6.66,
                            "c": 6.67,
                            "a": 1127.0
                        },
                        {
                            "index": 1818,
                            "date": "2024-12-25 14:35:00",
                            "h": 6.680000000000001,
                            "l": 6.640000000000001,
                            "o": 6.66,
                            "c": 6.640000000000001,
                            "a": 5276.0
                        }
                    ]
                },
                {
                    "k_index": 1820,
                    "date": "2024-12-25 14:45:00",
                    "h": 6.6499999999999995,
                    "l": 6.63,
                    "o": 6.6499999999999995,
                    "c": 6.63,
                    "a": 6791.0,
                    "index": 772,
                    "n": 2,

                    "up_qs": "down",
                    "klines": [
                        {
                            "index": 1819,
                            "date": "2024-12-25 14:40:00",
                            "h": 6.66,
                            "l": 6.63,
                            "o": 6.6499999999999995,
                            "c": 6.63,
                            "a": 2912.0
                        },
                        {
                            "index": 1820,
                            "date": "2024-12-25 14:45:00",
                            "h": 6.6499999999999995,
                            "l": 6.63,
                            "o": 6.63,
                            "c": 6.64,
                            "a": 3879.0
                        }
                    ]
                },
                {
                    "k_index": 1822,
                    "date": "2024-12-25 14:55:00",
                    "h": 6.66,
                    "l": 6.640000000000001,
                    "o": 6.66,
                    "c": 6.640000000000001,
                    "a": 10403.0,
                    "index": 773,
                    "n": 3,

                    "up_qs": "up",
                    "klines": [
                        {
                            "index": 1821,
                            "date": "2024-12-25 14:50:00",
                            "h": 6.66,
                            "l": 6.640000000000001,
                            "o": 6.65,
                            "c": 6.66,
                            "a": 2806.0
                        },
                        {
                            "index": 1822,
                            "date": "2024-12-25 14:55:00",
                            "h": 6.66,
                            "l": 6.640000000000001,
                            "o": 6.65,
                            "c": 6.640000000000001,
                            "a": 3682.0
                        },
                        {
                            "index": 1823,
                            "date": "2024-12-25 15:00:00",
                            "h": 6.65,
                            "l": 6.640000000000001,
                            "o": 6.640000000000001,
                            "c": 6.65,
                            "a": 3915.0
                        }
                    ]
                }
            ]
        },
        "end": {
            "type": "ding",
            "val": 6.7,
            "index": 0,

            "k": {
                "k_index": 1835,
                "date": "2024-12-26 10:30:00",
                "h": 6.7,
                "l": 6.69,
                "o": 6.7,
                "c": 6.69,
                "a": 868.0,
                "index": 779,
                "n": 1,

                "klines": [
                    {
                        "index": 1835,
                        "date": "2024-12-26 10:30:00",
                        "h": 6.7,
                        "l": 6.69,
                        "o": 6.7,
                        "c": 6.69,
                        "a": 868.0
                    }
                ]
            },
            "klines": [
                {
                    "k_index": 1834,
                    "date": "2024-12-26 10:25:00",
                    "h": 6.699999999999999,
                    "l": 6.68,
                    "o": 6.699999999999999,
                    "c": 6.68,
                    "a": 4771.0,
                    "index": 778,
                    "n": 4,

                    "up_qs": "up",
                    "klines": [
                        {
                            "index": 1831,
                            "date": "2024-12-26 10:10:00",
                            "h": 6.6899999999999995,
                            "l": 6.68,
                            "o": 6.68,
                            "c": 6.6899999999999995,
                            "a": 1285.0
                        },
                        {
                            "index": 1832,
                            "date": "2024-12-26 10:15:00",
                            "h": 6.6899999999999995,
                            "l": 6.68,
                            "o": 6.6899999999999995,
                            "c": 6.6899999999999995,
                            "a": 1035.0
                        },
                        {
                            "index": 1833,
                            "date": "2024-12-26 10:20:00",
                            "h": 6.6899999999999995,
                            "l": 6.68,
                            "o": 6.6899999999999995,
                            "c": 6.68,
                            "a": 515.0
                        },
                        {
                            "index": 1834,
                            "date": "2024-12-26 10:25:00",
                            "h": 6.699999999999999,
                            "l": 6.68,
                            "o": 6.6899999999999995,
                            "c": 6.6899999999999995,
                            "a": 1936.0
                        }
                    ]
                },
                {
                    "k_index": 1835,
                    "date": "2024-12-26 10:30:00",
                    "h": 6.7,
                    "l": 6.69,
                    "o": 6.7,
                    "c": 6.69,
                    "a": 868.0,
                    "index": 779,
                    "n": 1,

                    "klines": [
                        {
                            "index": 1835,
                            "date": "2024-12-26 10:30:00",
                            "h": 6.7,
                            "l": 6.69,
                            "o": 6.7,
                            "c": 6.69,
                            "a": 868.0
                        }
                    ]
                },
                {
                    "k_index": 1837,
                    "date": "2024-12-26 10:40:00",
                    "h": 6.6899999999999995,
                    "l": 6.68,
                    "o": 6.6899999999999995,
                    "c": 6.68,
                    "a": 2386.0,
                    "index": 780,
                    "n": 2,

                    "up_qs": "down",
                    "klines": [
                        {
                            "index": 1836,
                            "date": "2024-12-26 10:35:00",
                            "h": 6.699999999999999,
                            "l": 6.68,
                            "o": 6.6899999999999995,
                            "c": 6.68,
                            "a": 981.0
                        },
                        {
                            "index": 1837,
                            "date": "2024-12-26 10:40:00",
                            "h": 6.6899999999999995,
                            "l": 6.68,
                            "o": 6.68,
                            "c": 6.68,
                            "a": 1405.0
                        }
                    ]
                }
            ]
        },
        "high": 6.7,
        "low": 6.63,
        "zs_high": 0,
        "zs_low": 0,
        "type": "up",
        "index": 34,
        "mmds": [],
        "bcs": [],

        "zs_type_mmds": {},
        "zs_type_bcs": {},
        "is_split": ""
    }, {
        "start": {
            "type": "di",
            "val": 6.62,
            "index": 0,

            "k": {
                "k_index": 1861,
                "date": "2024-12-26 14:10:00",
                "h": 6.63,
                "l": 6.62,
                "o": 6.63,
                "c": 6.62,
                "a": 4729.0,
                "index": 787,
                "n": 2,

                "up_qs": "down",
                "klines": [
                    {
                        "index": 1860,
                        "date": "2024-12-26 14:05:00",
                        "h": 6.64,
                        "l": 6.62,
                        "o": 6.63,
                        "c": 6.62,
                        "a": 2810.0
                    },
                    {
                        "index": 1861,
                        "date": "2024-12-26 14:10:00",
                        "h": 6.63,
                        "l": 6.62,
                        "o": 6.63,
                        "c": 6.63,
                        "a": 1919.0
                    }
                ]
            },
            "klines": [
                {
                    "k_index": 1859,
                    "date": "2024-12-26 14:00:00",
                    "h": 6.6499999999999995,
                    "l": 6.63,
                    "o": 6.6499999999999995,
                    "c": 6.63,
                    "a": 3103.0,
                    "index": 786,
                    "n": 1,

                    "klines": [
                        {
                            "index": 1859,
                            "date": "2024-12-26 14:00:00",
                            "h": 6.6499999999999995,
                            "l": 6.63,
                            "o": 6.6499999999999995,
                            "c": 6.63,
                            "a": 3103.0
                        }
                    ]
                },
                {
                    "k_index": 1861,
                    "date": "2024-12-26 14:10:00",
                    "h": 6.63,
                    "l": 6.62,
                    "o": 6.63,
                    "c": 6.62,
                    "a": 4729.0,
                    "index": 787,
                    "n": 2,

                    "up_qs": "down",
                    "klines": [
                        {
                            "index": 1860,
                            "date": "2024-12-26 14:05:00",
                            "h": 6.64,
                            "l": 6.62,
                            "o": 6.63,
                            "c": 6.62,
                            "a": 2810.0
                        },
                        {
                            "index": 1861,
                            "date": "2024-12-26 14:10:00",
                            "h": 6.63,
                            "l": 6.62,
                            "o": 6.63,
                            "c": 6.63,
                            "a": 1919.0
                        }
                    ]
                },
                {
                    "k_index": 1863,
                    "date": "2024-12-26 14:20:00",
                    "h": 6.6499999999999995,
                    "l": 6.63,
                    "o": 6.6499999999999995,
                    "c": 6.63,
                    "a": 1736.0,
                    "index": 788,
                    "n": 2,

                    "up_qs": "up",
                    "klines": [
                        {
                            "index": 1862,
                            "date": "2024-12-26 14:15:00",
                            "h": 6.64,
                            "l": 6.63,
                            "o": 6.63,
                            "c": 6.64,
                            "a": 954.0
                        },
                        {
                            "index": 1863,
                            "date": "2024-12-26 14:20:00",
                            "h": 6.6499999999999995,
                            "l": 6.63,
                            "o": 6.64,
                            "c": 6.6499999999999995,
                            "a": 782.0
                        }
                    ]
                }
            ]
        },
        "end": {
            "type": "ding",
            "val": 7.1,
            "index": 0,

            "k": {
                "k_index": 1901,
                "date": "2024-12-27 13:30:00",
                "h": 7.1,
                "l": 7.07,
                "o": 7.1,
                "c": 7.07,
                "a": 44408.0,
                "index": 802,
                "n": 3,

                "up_qs": "up",
                "klines": [
                    {
                        "index": 1899,
                        "date": "2024-12-27 13:20:00",
                        "h": 7.09,
                        "l": 7.04,
                        "o": 7.05,
                        "c": 7.08,
                        "a": 22280.0
                    },
                    {
                        "index": 1900,
                        "date": "2024-12-27 13:25:00",
                        "h": 7.09,
                        "l": 7.07,
                        "o": 7.08,
                        "c": 7.08,
                        "a": 12890.0
                    },
                    {
                        "index": 1901,
                        "date": "2024-12-27 13:30:00",
                        "h": 7.1,
                        "l": 7.05,
                        "o": 7.08,
                        "c": 7.05,
                        "a": 9238.0
                    }
                ]
            },
            "klines": [
                {
                    "k_index": 1897,
                    "date": "2024-12-27 13:10:00",
                    "h": 7.07,
                    "l": 7.03,
                    "o": 7.07,
                    "c": 7.03,
                    "a": 38208.0,
                    "index": 801,
                    "n": 3,

                    "up_qs": "up",
                    "klines": [
                        {
                            "index": 1896,
                            "date": "2024-12-27 13:05:00",
                            "h": 7.07,
                            "l": 7.0,
                            "o": 7.02,
                            "c": 7.01,
                            "a": 18506.0
                        },
                        {
                            "index": 1897,
                            "date": "2024-12-27 13:10:00",
                            "h": 7.07,
                            "l": 6.99,
                            "o": 7.0,
                            "c": 7.0600000000000005,
                            "a": 12649.0
                        },
                        {
                            "index": 1898,
                            "date": "2024-12-27 13:15:00",
                            "h": 7.0600000000000005,
                            "l": 7.03,
                            "o": 7.05,
                            "c": 7.04,
                            "a": 7053.0
                        }
                    ]
                },
                {
                    "k_index": 1901,
                    "date": "2024-12-27 13:30:00",
                    "h": 7.1,
                    "l": 7.07,
                    "o": 7.1,
                    "c": 7.07,
                    "a": 44408.0,
                    "index": 802,
                    "n": 3,

                    "up_qs": "up",
                    "klines": [
                        {
                            "index": 1899,
                            "date": "2024-12-27 13:20:00",
                            "h": 7.09,
                            "l": 7.04,
                            "o": 7.05,
                            "c": 7.08,
                            "a": 22280.0
                        },
                        {
                            "index": 1900,
                            "date": "2024-12-27 13:25:00",
                            "h": 7.09,
                            "l": 7.07,
                            "o": 7.08,
                            "c": 7.08,
                            "a": 12890.0
                        },
                        {
                            "index": 1901,
                            "date": "2024-12-27 13:30:00",
                            "h": 7.1,
                            "l": 7.05,
                            "o": 7.08,
                            "c": 7.05,
                            "a": 9238.0
                        }
                    ]
                },
                {
                    "k_index": 1902,
                    "date": "2024-12-27 13:35:00",
                    "h": 7.069999999999999,
                    "l": 7.05,
                    "o": 7.05,
                    "c": 7.05,
                    "a": 3879.0,
                    "index": 803,
                    "n": 1,

                    "klines": [
                        {
                            "index": 1902,
                            "date": "2024-12-27 13:35:00",
                            "h": 7.069999999999999,
                            "l": 7.05,
                            "o": 7.05,
                            "c": 7.05,
                            "a": 3879.0
                        }
                    ]
                }
            ]
        },
        "high": 7.1,
        "low": 6.62,
        "zs_high": 0,
        "zs_low": 0,
        "type": "up",
        "index": 36,
        "mmds": [],
        "bcs": [],

        "zs_type_mmds": {},
        "zs_type_bcs": {},
        "is_split": ""
    }, {
        "start": {
            "type": "di",
            "val": 6.8100000000000005,
            "index": 0,

            "k": {
                "k_index": 1962,
                "date": "2024-12-30 14:35:00",
                "h": 6.84,
                "l": 6.8100000000000005,
                "o": 6.84,
                "c": 6.8100000000000005,
                "a": 18487.0,
                "index": 830,
                "n": 5,

                "up_qs": "down",
                "klines": [
                    {
                        "index": 1959,
                        "date": "2024-12-30 14:20:00",
                        "h": 6.859999999999999,
                        "l": 6.84,
                        "o": 6.859999999999999,
                        "c": 6.84,
                        "a": 3115.0
                    },
                    {
                        "index": 1960,
                        "date": "2024-12-30 14:25:00",
                        "h": 6.859999999999999,
                        "l": 6.84,
                        "o": 6.84,
                        "c": 6.84,
                        "a": 1141.0
                    },
                    {
                        "index": 1961,
                        "date": "2024-12-30 14:30:00",
                        "h": 6.85,
                        "l": 6.84,
                        "o": 6.85,
                        "c": 6.84,
                        "a": 1646.0
                    },
                    {
                        "index": 1962,
                        "date": "2024-12-30 14:35:00",
                        "h": 6.8500000000000005,
                        "l": 6.8100000000000005,
                        "o": 6.840000000000001,
                        "c": 6.82,
                        "a": 10434.0
                    },
                    {
                        "index": 1963,
                        "date": "2024-12-30 14:40:00",
                        "h": 6.84,
                        "l": 6.82,
                        "o": 6.83,
                        "c": 6.83,
                        "a": 2151.0
                    }
                ]
            },
            "klines": [
                {
                    "k_index": 1958,
                    "date": "2024-12-30 14:15:00",
                    "h": 6.88,
                    "l": 6.86,
                    "o": 6.87,
                    "c": 6.86,
                    "a": 911.0,
                    "index": 829,
                    "n": 1,

                    "klines": [
                        {
                            "index": 1958,
                            "date": "2024-12-30 14:15:00",
                            "h": 6.88,
                            "l": 6.86,
                            "o": 6.87,
                            "c": 6.86,
                            "a": 911.0
                        }
                    ]
                },
                {
                    "k_index": 1962,
                    "date": "2024-12-30 14:35:00",
                    "h": 6.84,
                    "l": 6.8100000000000005,
                    "o": 6.84,
                    "c": 6.8100000000000005,
                    "a": 18487.0,
                    "index": 830,
                    "n": 5,

                    "up_qs": "down",
                    "klines": [
                        {
                            "index": 1959,
                            "date": "2024-12-30 14:20:00",
                            "h": 6.859999999999999,
                            "l": 6.84,
                            "o": 6.859999999999999,
                            "c": 6.84,
                            "a": 3115.0
                        },
                        {
                            "index": 1960,
                            "date": "2024-12-30 14:25:00",
                            "h": 6.859999999999999,
                            "l": 6.84,
                            "o": 6.84,
                            "c": 6.84,
                            "a": 1141.0
                        },
                        {
                            "index": 1961,
                            "date": "2024-12-30 14:30:00",
                            "h": 6.85,
                            "l": 6.84,
                            "o": 6.85,
                            "c": 6.84,
                            "a": 1646.0
                        },
                        {
                            "index": 1962,
                            "date": "2024-12-30 14:35:00",
                            "h": 6.8500000000000005,
                            "l": 6.8100000000000005,
                            "o": 6.840000000000001,
                            "c": 6.82,
                            "a": 10434.0
                        },
                        {
                            "index": 1963,
                            "date": "2024-12-30 14:40:00",
                            "h": 6.84,
                            "l": 6.82,
                            "o": 6.83,
                            "c": 6.83,
                            "a": 2151.0
                        }
                    ]
                },
                {
                    "k_index": 1964,
                    "date": "2024-12-30 14:45:00",
                    "h": 6.85,
                    "l": 6.83,
                    "o": 6.85,
                    "c": 6.83,
                    "a": 7310.0,
                    "index": 831,
                    "n": 2,

                    "up_qs": "up",
                    "klines": [
                        {
                            "index": 1964,
                            "date": "2024-12-30 14:45:00",
                            "h": 6.85,
                            "l": 6.83,
                            "o": 6.83,
                            "c": 6.83,
                            "a": 4058.0
                        },
                        {
                            "index": 1965,
                            "date": "2024-12-30 14:50:00",
                            "h": 6.84,
                            "l": 6.83,
                            "o": 6.84,
                            "c": 6.84,
                            "a": 3252.0
                        }
                    ]
                }
            ]
        },
        "end": {
            "type": "ding",
            "val": 6.9,
            "index": 0,

            "k": {
                "k_index": 1972,
                "date": "2024-12-31 09:55:00",
                "h": 6.9,
                "l": 6.87,
                "o": 6.9,
                "c": 6.87,
                "a": 20657.0,
                "index": 835,
                "n": 8,

                "up_qs": "up",
                "klines": [
                    {
                        "index": 1972,
                        "date": "2024-12-31 09:55:00",
                        "h": 6.9,
                        "l": 6.86,
                        "o": 6.87,
                        "c": 6.87,
                        "a": 5302.0
                    },
                    {
                        "index": 1973,
                        "date": "2024-12-31 10:00:00",
                        "h": 6.88,
                        "l": 6.86,
                        "o": 6.87,
                        "c": 6.87,
                        "a": 3192.0
                    },
                    {
                        "index": 1974,
                        "date": "2024-12-31 10:05:00",
                        "h": 6.88,
                        "l": 6.86,
                        "o": 6.86,
                        "c": 6.87,
                        "a": 2307.0
                    },
                    {
                        "index": 1975,
                        "date": "2024-12-31 10:10:00",
                        "h": 6.88,
                        "l": 6.86,
                        "o": 6.86,
                        "c": 6.87,
                        "a": 1385.0
                    },
                    {
                        "index": 1976,
                        "date": "2024-12-31 10:15:00",
                        "h": 6.89,
                        "l": 6.87,
                        "o": 6.87,
                        "c": 6.89,
                        "a": 3952.0
                    },
                    {
                        "index": 1977,
                        "date": "2024-12-31 10:20:00",
                        "h": 6.89,
                        "l": 6.87,
                        "o": 6.89,
                        "c": 6.88,
                        "a": 2253.0
                    },
                    {
                        "index": 1978,
                        "date": "2024-12-31 10:25:00",
                        "h": 6.88,
                        "l": 6.87,
                        "o": 6.87,
                        "c": 6.88,
                        "a": 1085.0
                    },
                    {
                        "index": 1979,
                        "date": "2024-12-31 10:30:00",
                        "h": 6.88,
                        "l": 6.87,
                        "o": 6.87,
                        "c": 6.87,
                        "a": 1181.0
                    }
                ]
            },
            "klines": [
                {
                    "k_index": 1970,
                    "date": "2024-12-31 09:45:00",
                    "h": 6.87,
                    "l": 6.83,
                    "o": 6.87,
                    "c": 6.83,
                    "a": 8153.0,
                    "index": 834,
                    "n": 2,

                    "up_qs": "down",
                    "klines": [
                        {
                            "index": 1970,
                            "date": "2024-12-31 09:45:00",
                            "h": 6.87,
                            "l": 6.83,
                            "o": 6.85,
                            "c": 6.86,
                            "a": 5422.0
                        },
                        {
                            "index": 1971,
                            "date": "2024-12-31 09:50:00",
                            "h": 6.87,
                            "l": 6.8500000000000005,
                            "o": 6.86,
                            "c": 6.87,
                            "a": 2731.0
                        }
                    ]
                },
                {
                    "k_index": 1972,
                    "date": "2024-12-31 09:55:00",
                    "h": 6.9,
                    "l": 6.87,
                    "o": 6.9,
                    "c": 6.87,
                    "a": 20657.0,
                    "index": 835,
                    "n": 8,

                    "up_qs": "up",
                    "klines": [
                        {
                            "index": 1972,
                            "date": "2024-12-31 09:55:00",
                            "h": 6.9,
                            "l": 6.86,
                            "o": 6.87,
                            "c": 6.87,
                            "a": 5302.0
                        },
                        {
                            "index": 1973,
                            "date": "2024-12-31 10:00:00",
                            "h": 6.88,
                            "l": 6.86,
                            "o": 6.87,
                            "c": 6.87,
                            "a": 3192.0
                        },
                        {
                            "index": 1974,
                            "date": "2024-12-31 10:05:00",
                            "h": 6.88,
                            "l": 6.86,
                            "o": 6.86,
                            "c": 6.87,
                            "a": 2307.0
                        },
                        {
                            "index": 1975,
                            "date": "2024-12-31 10:10:00",
                            "h": 6.88,
                            "l": 6.86,
                            "o": 6.86,
                            "c": 6.87,
                            "a": 1385.0
                        },
                        {
                            "index": 1976,
                            "date": "2024-12-31 10:15:00",
                            "h": 6.89,
                            "l": 6.87,
                            "o": 6.87,
                            "c": 6.89,
                            "a": 3952.0
                        },
                        {
                            "index": 1977,
                            "date": "2024-12-31 10:20:00",
                            "h": 6.89,
                            "l": 6.87,
                            "o": 6.89,
                            "c": 6.88,
                            "a": 2253.0
                        },
                        {
                            "index": 1978,
                            "date": "2024-12-31 10:25:00",
                            "h": 6.88,
                            "l": 6.87,
                            "o": 6.87,
                            "c": 6.88,
                            "a": 1085.0
                        },
                        {
                            "index": 1979,
                            "date": "2024-12-31 10:30:00",
                            "h": 6.88,
                            "l": 6.87,
                            "o": 6.87,
                            "c": 6.87,
                            "a": 1181.0
                        }
                    ]
                },
                {
                    "k_index": 1984,
                    "date": "2024-12-31 10:55:00",
                    "h": 6.85,
                    "l": 6.82,
                    "o": 6.85,
                    "c": 6.82,
                    "a": 8797.0,
                    "index": 836,
                    "n": 5,

                    "up_qs": "down",
                    "klines": [
                        {
                            "index": 1980,
                            "date": "2024-12-31 10:35:00",
                            "h": 6.87,
                            "l": 6.86,
                            "o": 6.86,
                            "c": 6.86,
                            "a": 1767.0
                        },
                        {
                            "index": 1981,
                            "date": "2024-12-31 10:40:00",
                            "h": 6.87,
                            "l": 6.84,
                            "o": 6.87,
                            "c": 6.84,
                            "a": 1738.0
                        },
                        {
                            "index": 1982,
                            "date": "2024-12-31 10:45:00",
                            "h": 6.85,
                            "l": 6.84,
                            "o": 6.84,
                            "c": 6.85,
                            "a": 1331.0
                        },
                        {
                            "index": 1983,
                            "date": "2024-12-31 10:50:00",
                            "h": 6.85,
                            "l": 6.84,
                            "o": 6.85,
                            "c": 6.85,
                            "a": 464.0
                        },
                        {
                            "index": 1984,
                            "date": "2024-12-31 10:55:00",
                            "h": 6.8500000000000005,
                            "l": 6.82,
                            "o": 6.84,
                            "c": 6.82,
                            "a": 3497.0
                        }
                    ]
                }
            ]
        },
        "high": 6.9,
        "low": 6.8100000000000005,
        "zs_high": 0,
        "zs_low": 0,
        "type": "up",
        "index": 38,
        "mmds": [],
        "bcs": [],

        "zs_type_mmds": {},
        "zs_type_bcs": {},
        "is_split": ""
    }, {
        "start": {
            "type": "di",
            "val": 6.71,
            "index": 0,

            "k": {
                "k_index": 2016,
                "date": "2025-01-02 09:35:00",
                "h": 6.739999999999999,
                "l": 6.71,
                "o": 6.739999999999999,
                "c": 6.71,
                "a": 14731.0,
                "index": 852,
                "n": 2,

                "up_qs": "down",
                "klines": [
                    {
                        "index": 2015,
                        "date": "2024-12-31 15:00:00",
                        "h": 6.739999999999999,
                        "l": 6.72,
                        "o": 6.7299999999999995,
                        "c": 6.7299999999999995,
                        "a": 7402.0
                    },
                    {
                        "index": 2016,
                        "date": "2025-01-02 09:35:00",
                        "h": 6.78,
                        "l": 6.71,
                        "o": 6.72,
                        "c": 6.7299999999999995,
                        "a": 7329.0
                    }
                ]
            },
            "klines": [
                {
                    "k_index": 2014,
                    "date": "2024-12-31 14:55:00",
                    "h": 6.760000000000001,
                    "l": 6.73,
                    "o": 6.75,
                    "c": 6.73,
                    "a": 5689.0,
                    "index": 851,
                    "n": 1,

                    "klines": [
                        {
                            "index": 2014,
                            "date": "2024-12-31 14:55:00",
                            "h": 6.760000000000001,
                            "l": 6.73,
                            "o": 6.75,
                            "c": 6.73,
                            "a": 5689.0
                        }
                    ]
                },
                {
                    "k_index": 2016,
                    "date": "2025-01-02 09:35:00",
                    "h": 6.739999999999999,
                    "l": 6.71,
                    "o": 6.739999999999999,
                    "c": 6.71,
                    "a": 14731.0,
                    "index": 852,
                    "n": 2,

                    "up_qs": "down",
                    "klines": [
                        {
                            "index": 2015,
                            "date": "2024-12-31 15:00:00",
                            "h": 6.739999999999999,
                            "l": 6.72,
                            "o": 6.7299999999999995,
                            "c": 6.7299999999999995,
                            "a": 7402.0
                        },
                        {
                            "index": 2016,
                            "date": "2025-01-02 09:35:00",
                            "h": 6.78,
                            "l": 6.71,
                            "o": 6.72,
                            "c": 6.7299999999999995,
                            "a": 7329.0
                        }
                    ]
                },
                {
                    "k_index": 2017,
                    "date": "2025-01-02 09:40:00",
                    "h": 6.760000000000001,
                    "l": 6.73,
                    "o": 6.74,
                    "c": 6.75,
                    "a": 4235.0,
                    "index": 853,
                    "n": 1,

                    "klines": [
                        {
                            "index": 2017,
                            "date": "2025-01-02 09:40:00",
                            "h": 6.760000000000001,
                            "l": 6.73,
                            "o": 6.74,
                            "c": 6.75,
                            "a": 4235.0
                        }
                    ]
                }
            ]
        },
        "end": {
            "type": "ding",
            "val": 6.8100000000000005,
            "index": 0,

            "k": {
                "k_index": 2029,
                "date": "2025-01-02 10:40:00",
                "h": 6.8100000000000005,
                "l": 6.8,
                "o": 6.8100000000000005,
                "c": 6.8,
                "a": 10118.0,
                "index": 857,
                "n": 4,

                "up_qs": "up",
                "klines": [
                    {
                        "index": 2026,
                        "date": "2025-01-02 10:25:00",
                        "h": 6.8100000000000005,
                        "l": 6.78,
                        "o": 6.8100000000000005,
                        "c": 6.79,
                        "a": 3493.0
                    },
                    {
                        "index": 2027,
                        "date": "2025-01-02 10:30:00",
                        "h": 6.81,
                        "l": 6.79,
                        "o": 6.79,
                        "c": 6.81,
                        "a": 3319.0
                    },
                    {
                        "index": 2028,
                        "date": "2025-01-02 10:35:00",
                        "h": 6.81,
                        "l": 6.8,
                        "o": 6.81,
                        "c": 6.8,
                        "a": 1214.0
                    },
                    {
                        "index": 2029,
                        "date": "2025-01-02 10:40:00",
                        "h": 6.8100000000000005,
                        "l": 6.78,
                        "o": 6.8100000000000005,
                        "c": 6.8,
                        "a": 2092.0
                    }
                ]
            },
            "klines": [
                {
                    "k_index": 2025,
                    "date": "2025-01-02 10:20:00",
                    "h": 6.78,
                    "l": 6.74,
                    "o": 6.78,
                    "c": 6.74,
                    "a": 19126.0,
                    "index": 856,
                    "n": 6,

                    "up_qs": "down",
                    "klines": [
                        {
                            "index": 2020,
                            "date": "2025-01-02 09:55:00",
                            "h": 6.79,
                            "l": 6.76,
                            "o": 6.76,
                            "c": 6.779999999999999,
                            "a": 1186.0
                        },
                        {
                            "index": 2021,
                            "date": "2025-01-02 10:00:00",
                            "h": 6.79,
                            "l": 6.7700000000000005,
                            "o": 6.78,
                            "c": 6.7700000000000005,
                            "a": 2014.0
                        },
                        {
                            "index": 2022,
                            "date": "2025-01-02 10:05:00",
                            "h": 6.78,
                            "l": 6.7700000000000005,
                            "o": 6.78,
                            "c": 6.7700000000000005,
                            "a": 1091.0
                        },
                        {
                            "index": 2023,
                            "date": "2025-01-02 10:10:00",
                            "h": 6.78,
                            "l": 6.75,
                            "o": 6.78,
                            "c": 6.77,
                            "a": 3580.0
                        },
                        {
                            "index": 2024,
                            "date": "2025-01-02 10:15:00",
                            "h": 6.78,
                            "l": 6.74,
                            "o": 6.78,
                            "c": 6.74,
                            "a": 1844.0
                        },
                        {
                            "index": 2025,
                            "date": "2025-01-02 10:20:00",
                            "h": 6.82,
                            "l": 6.74,
                            "o": 6.74,
                            "c": 6.8100000000000005,
                            "a": 9411.0
                        }
                    ]
                },
                {
                    "k_index": 2029,
                    "date": "2025-01-02 10:40:00",
                    "h": 6.8100000000000005,
                    "l": 6.8,
                    "o": 6.8100000000000005,
                    "c": 6.8,
                    "a": 10118.0,
                    "index": 857,
                    "n": 4,

                    "up_qs": "up",
                    "klines": [
                        {
                            "index": 2026,
                            "date": "2025-01-02 10:25:00",
                            "h": 6.8100000000000005,
                            "l": 6.78,
                            "o": 6.8100000000000005,
                            "c": 6.79,
                            "a": 3493.0
                        },
                        {
                            "index": 2027,
                            "date": "2025-01-02 10:30:00",
                            "h": 6.81,
                            "l": 6.79,
                            "o": 6.79,
                            "c": 6.81,
                            "a": 3319.0
                        },
                        {
                            "index": 2028,
                            "date": "2025-01-02 10:35:00",
                            "h": 6.81,
                            "l": 6.8,
                            "o": 6.81,
                            "c": 6.8,
                            "a": 1214.0
                        },
                        {
                            "index": 2029,
                            "date": "2025-01-02 10:40:00",
                            "h": 6.8100000000000005,
                            "l": 6.78,
                            "o": 6.8100000000000005,
                            "c": 6.8,
                            "a": 2092.0
                        }
                    ]
                },
                {
                    "k_index": 2033,
                    "date": "2025-01-02 11:00:00",
                    "h": 6.79,
                    "l": 6.78,
                    "o": 6.79,
                    "c": 6.78,
                    "a": 3531.0,
                    "index": 858,
                    "n": 4,

                    "up_qs": "down",
                    "klines": [
                        {
                            "index": 2030,
                            "date": "2025-01-02 10:45:00",
                            "h": 6.8,
                            "l": 6.78,
                            "o": 6.79,
                            "c": 6.78,
                            "a": 1361.0
                        },
                        {
                            "index": 2031,
                            "date": "2025-01-02 10:50:00",
                            "h": 6.79,
                            "l": 6.78,
                            "o": 6.79,
                            "c": 6.79,
                            "a": 591.0
                        },
                        {
                            "index": 2032,
                            "date": "2025-01-02 10:55:00",
                            "h": 6.8,
                            "l": 6.78,
                            "o": 6.79,
                            "c": 6.78,
                            "a": 984.0
                        },
                        {
                            "index": 2033,
                            "date": "2025-01-02 11:00:00",
                            "h": 6.8,
                            "l": 6.78,
                            "o": 6.78,
                            "c": 6.79,
                            "a": 595.0
                        }
                    ]
                }
            ]
        },
        "high": 6.8100000000000005,
        "low": 6.71,
        "zs_high": 0,
        "zs_low": 0,
        "type": "up",
        "index": 40,
        "mmds": [],
        "bcs": [],

        "zs_type_mmds": {},
        "zs_type_bcs": {},
        "is_split": ""
    }]
    xd_calculator._process_inclusion(data, 'down')
    print('aa')
