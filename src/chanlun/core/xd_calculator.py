# -*- coding: utf-8 -*-
"""
线段计算模块 v2
严格按照《线段划分知识_结构化.md》实现，逻辑简洁清晰。
"""
from typing import List, Optional

from chanlun.core.cl_interface import BI, XD
from chanlun.tools.log_util import LogUtil

_log = LogUtil


def _bi_label(bi: BI) -> str:
    return f"bi[{bi.index}]{bi.type}({bi.start.val:.3f}→{bi.end.val:.3f})"


def _elem_label(e: dict) -> str:
    merged = e.get('merged_bis')
    if merged:
        return f"{{h={e['high']:.3f},l={e['low']:.3f},merged={len(merged)}}}"
    return f"{{h={e['high']:.3f},l={e['low']:.3f}}}"


# ============================================================
# 特征序列工具函数（纯函数，无状态）
# ============================================================

def _bi_to_cs_elem(bi: BI) -> dict:
    return {'bi': bi, 'high': bi.high, 'low': bi.low}


def _overlap(a, b) -> bool:
    h1, l1 = (a['high'], a['low']) if isinstance(a, dict) else (a.high, a.low)
    h2, l2 = (b['high'], b['low']) if isinstance(b, dict) else (b.high, b.low)
    return max(l1, l2) <= min(h1, h2)


def _has_inclusion(a: dict, b: dict) -> bool:
    return (a['high'] >= b['high'] and a['low'] <= b['low']) or \
           (b['high'] >= a['high'] and b['low'] <= a['low'])


def _merge_two(prev: dict, cur: dict, direction: str) -> dict:
    if direction == 'up':
        mh, ml = max(prev['high'], cur['high']), max(prev['low'], cur['low'])
    else:
        mh, ml = min(prev['high'], cur['high']), min(prev['low'], cur['low'])
    prev_bis = prev.get('merged_bis', [prev['bi']])
    cur_bis = cur.get('merged_bis', [cur['bi']])
    return {
        'bi': prev['bi'], 'high': mh, 'low': ml,
        'merged_bis': prev_bis + cur_bis,
    }


def _process_inclusion(elems: List[dict], direction: str) -> List[dict]:
    """
    特征序列包含处理（趋势感知版）。

    规则:
      - 当CS元素沿趋势方向持续创新极值时（up段CS的high不断升高/down段CS的low不断降低），
        不做包含合并，直接追加。
      - 当极值不再创新（拐点）时，对拐点处的元素与前一个元素做包含合并，
        合并后向前级联（可能继续与更前面的元素合并）。
    """
    if len(elems) < 2:
        return list(elems)

    result = [elems[0].copy()]
    for i in range(1, len(elems)):
        cur = elems[i]
        prev = result[-1]

        # 判断是否仍在趋势中（持续创新极值）
        if direction == 'up':
            still_trending = cur['high'] > prev['high']
        else:
            still_trending = cur['low'] < prev['low']

        if still_trending:
            result.append(cur.copy())
        else:
            # 拐点：检查包含并合并
            if _has_inclusion(prev, cur):
                result[-1] = _merge_two(prev, cur, direction)
                # 级联：合并后可能与更前面的元素产生包含
                while len(result) >= 2 and _has_inclusion(result[-2], result[-1]):
                    merged = _merge_two(result[-2], result[-1], direction)
                    result.pop()
                    result[-1] = merged
            else:
                result.append(cur.copy())

    return result


def _resolve_pivot_bi(elem: dict, seg_type: str):
    """从（可能被包含合并的）反向 CS 元素中，定位"枢轴反向笔"——
    即用于回溯定位原线段终点的那根反向笔。

    语义说明：
      - 一个 CS 元素可能由若干根反向笔合并而成（merged_bis）。
      - 调用者拿到的是分型中心 elem，需要据此推算"原线段终点 = 该反向笔的前一根同向笔"。
      - 选择规则：取使原线段达到方向极值的那根反向笔
          * up 段（反向 CS 是 down 笔）→ 取 high 最大的 down 笔
            原因：down 笔的起点 = 上一根 up 笔的终点；high 越大 → 上一根 up 笔涨得越高
          * down 段（反向 CS 是 up 笔）→ 取 low 最小的 up 笔
            原因：up 笔的起点 = 上一根 down 笔的终点；low 越小 → 上一根 down 笔跌得越低

    Args:
        elem: CS 元素 dict，含 'bi'（首根反向笔）和可选 'merged_bis'（合并的反向笔列表）
        seg_type: 原线段方向（'up' 或 'down'）

    Returns:
        枢轴反向笔（BI 对象）。其前一根同向笔即为原线段终点候选。
    """
    target = elem['bi']
    merged = elem.get('merged_bis')
    if merged:
        target = max(merged, key=lambda b: b.high) if seg_type == 'up' else min(merged, key=lambda b: b.low)
    return target

# ============================================================
# 模块级配置常量
# ============================================================
# _try_end 中"反向 CS 元素扫描上限"。
# 用途：防止"反向 CS 一直被包含合并、second_elems 永远凑不齐 2 个"导致
# 单次 _try_end 一直扫到数组末尾，造成 O(n²) 退化甚至无法返回。
# 经验值依据：正常一段反向走势的反向 CS 笔通常不超过十几根，50 已远超合理上限；
# 一旦触发即可判定该方向无法形成有效反向段，直接放弃本轮判定。
#
# ★ D4 优化：默认值仍为 50，但允许通过两种方式调优：
#   - 环境变量 CHANLUN_XD_LOOKAHEAD（部署侧统一控制）
#   - XdCalculator(config={'xd_safety_lookahead': N}) 实例级覆盖
# 实例级配置优先于环境变量；都没设则用默认 50。
import os as _os

def _get_default_safety_lookahead() -> int:
    raw = _os.environ.get('CHANLUN_XD_LOOKAHEAD', '50')
    try:
        v = int(raw)
        # 不接受 < 5 的过小值（容易误中止合理走势），也不接受 > 1000 的过大值（性能失控）
        if v < 5:
            return 5
        if v > 1000:
            return 1000
        return v
    except (TypeError, ValueError):
        return 50

SAFETY_LOOKAHEAD = _get_default_safety_lookahead()


# ============================================================
# XdCalculator v2
# ============================================================

class XdCalculator:

    def __init__(self, config: dict):
        self.config = config
        self.xds: List[XD] = []
        self._last_bi_snapshot: Optional[tuple] = None

    # ----------------------------------------------------------
    # 公共接口
    # ----------------------------------------------------------
    def calculate(self, bis: List[BI]) -> List[XD]:
        all_bis = bis
        is_incremental = bool(self.xds)
        start_index_for_delta = 0

        if self.xds and all_bis and self._last_bi_snapshot:
            lb = all_bis[-1]
            # ★ B3 修复：snapshot 同时校验 end.k.k_index。
            # 缠论 K 线层会发生包含合并，导致同一根 BI 的 end.k.k_index 在新 K 线
            # 到来后悄悄变化（合并/拆分），但 end.val（价格）和 index（笔序号）不变。
            # 只校验 (index, end.val) 会让 XdCalculator 误判"无变化"直接 return，
            # 但下游 ZsCalculator / BsPointCalculator 用到的 bi.end.k.k_index 已经变了，
            # 结果不一致。snapshot 增加 k_index 后任何端点漂移都会触发重算。
            last_k_index = lb.end.k.k_index if lb.end is not None and lb.end.k is not None else -1
            if (
                lb.index == self._last_bi_snapshot[0]
                and abs(lb.end.val - self._last_bi_snapshot[1]) < 1e-9
                and len(self._last_bi_snapshot) >= 3
                and last_k_index == self._last_bi_snapshot[2]
            ):
                return []

        start_bi_idx = 0
        if self.xds:
            if not all_bis:
                return []
            last_xd = self.xds.pop()
            start_index_for_delta = len(self.xds)
            # 优先用 BI.index 反查（O(1)~O(log n)），失败再回退到 O(n) 全扫描
            target_bi = last_xd.start_line
            start_bi_idx = self._locate_bi(all_bis, target_bi)
            if start_bi_idx < 0:
                self.xds.clear()
                is_incremental = False
                start_bi_idx = self._find_start(all_bis)
        else:
            self.xds.clear()
            is_incremental = False
            start_bi_idx = self._find_start(all_bis)

        if len(all_bis) < 3:
            return [] if is_incremental else self.xds

        self._bi_pos = {id(bi): i for i, bi in enumerate(all_bis)}

        mode = "增量" if is_incremental else "全量"
        _log.debug(f"XdCalculator: {mode}计算，笔数={len(all_bis)}，起始位置={start_bi_idx}")

        self._build_segments(all_bis, start_bi_idx)

        if all_bis:
            lb = all_bis[-1]
            last_k_index = lb.end.k.k_index if lb.end is not None and lb.end.k is not None else -1
            self._last_bi_snapshot = (lb.index, lb.end.val, last_k_index)

        return self.xds[start_index_for_delta:] if is_incremental else self.xds

    # ----------------------------------------------------------
    def _find_start(self, all_bis: List[BI]) -> int:
        for i in range(len(all_bis) - 2):
            if _overlap(all_bis[i], all_bis[i + 2]):
                return i
        return 0

    # ----------------------------------------------------------
    @staticmethod
    def _locate_bi(all_bis: List[BI], target: BI) -> int:
        """在 all_bis 中定位 target 笔的位置。

        优先按 BI.index 反查（猜测位置 + 邻域校验），失败回退到 O(n) 线性扫描
        + 多字段匹配（start/end k.date + type），保证在 BI 对象重建后仍能找回。
        找不到返回 -1。
        """
        # 路径 1: 利用 BI.index 猜测位置（all_bis 通常按 index 升序排列）
        try:
            tgt_idx = target.index
            if 0 <= tgt_idx < len(all_bis):
                cand = all_bis[tgt_idx]
                if (cand.type == target.type and
                        cand.start.k.date == target.start.k.date and
                        cand.end.k.date == target.end.k.date):
                    return tgt_idx
        except AttributeError:
            pass

        # 路径 2: 回退到全扫描（兼容 BI.index 失效或乱序场景）
        for i, bi in enumerate(all_bis):
            if (bi.type == target.type and
                    bi.start.k.date == target.start.k.date and
                    bi.end.k.date == target.end.k.date):
                return i
        return -1

    # ----------------------------------------------------------
    # 主循环
    # ----------------------------------------------------------
    def _build_segments(self, all_bis: List[BI], start: int):
        pos = start
        reverse_end_hint = None  # 上一段 _try_end 已探明的反向线段终点位置

        while pos + 2 < len(all_bis):
            # 确定 seg_end 初始值
            if reverse_end_hint is not None:
                # 反向线段已成立，跳过 overlap 检查，直接使用已知范围
                seg_end = reverse_end_hint
                reverse_end_hint = None
            else:
                if not _overlap(all_bis[pos], all_bis[pos + 2]):
                    pos += 1
                    continue
                seg_end = pos + 2

            seg_type = all_bis[pos].type
            seg_start = pos
            check = seg_end + 1

            # 计算 seg_high/seg_low 的初始值
            # 注意：一个段确定方向后，"反方向"那一边是固定值（段起点价），
            # "顺方向"那一边随 seg_end 推进而刷新。简化为：
            #   - up 段：seg_low = 起点常量；seg_high 跟随 seg_end.end.val 取 max
            #   - down 段：seg_high = 起点常量；seg_low 跟随 seg_end.end.val 取 min
            seg_anchor = all_bis[seg_start].start.val  # 段起点价（恒定的"反方向"边）
            if seg_type == 'up':
                seg_low = seg_anchor
                seg_high = all_bis[seg_end].end.val
            else:
                seg_high = seg_anchor
                seg_low = all_bis[seg_end].end.val

            # 维护增量 seg_cs_bis 列表（cs = 反向笔）
            # 初始范围 [seg_start, seg_end]，后续延伸/吸收时同步追加。
            # 这样 _try_end 不必每次重新过滤段内 CS 笔。
            cs_bi_type = 'down' if seg_type == 'up' else 'up'
            seg_cs_bis: List[BI] = [all_bis[i] for i in range(seg_start, seg_end + 1)
                                     if all_bis[i].type == cs_bi_type]

            bi_s = all_bis[seg_start]
            _log.info(f"[新线段] {seg_type} 起点={_bi_label(bi_s)}, seg_end={_bi_label(all_bis[seg_end])}, seg_high={seg_high:.3f}, seg_low={seg_low:.3f}")

            while check + 1 < len(all_bis):
                # 仅刷新"顺方向"那一边的极值（反方向边恒等于 seg_anchor，无需重算）
                if seg_type == 'up':
                    seg_high = max(seg_high, all_bis[seg_end].end.val)
                else:
                    seg_low = min(seg_low, all_bis[seg_end].end.val)
                next_same = all_bis[check]

                # Step 1: 延伸
                # 延伸吃掉 [check, check+1] 两根笔，其中 check 是反向笔(cs)、check+1 是同向笔。
                # 增量缓存：把 check 这根 cs 笔追加到 seg_cs_bis。
                if seg_type == 'up' and next_same.high > seg_high:
                    _log.debug(f"  [延伸] {_bi_label(next_same)} high={next_same.high:.3f} > seg_high={seg_high:.3f}")
                    if all_bis[check].type == cs_bi_type:
                        seg_cs_bis.append(all_bis[check])
                    seg_end = check + 1
                    check += 2
                    continue
                if seg_type == 'down' and next_same.low < seg_low:
                    _log.debug(f"  [延伸] {_bi_label(next_same)} low={next_same.low:.3f} < seg_low={seg_low:.3f}")
                    if all_bis[check].type == cs_bi_type:
                        seg_cs_bis.append(all_bis[check])
                    seg_end = check + 1
                    check += 2
                    continue

                # Step 2: 分型检测（传入增量缓存避免重算）
                _log.debug(f"  [检测] seg_end={_bi_label(all_bis[seg_end])}, check={_bi_label(all_bis[check])}")
                end_result = self._try_end(all_bis, seg_start, seg_end, seg_type,
                                           seg_high, seg_low, check,
                                           seg_cs_bis_cache=seg_cs_bis)
                if end_result is not None:
                    real_end, next_start, next_end = end_result
                    self._emit_segment(all_bis, seg_start, real_end, seg_type)
                    pos = next_start
                    # 用外层 check 作为反向线段已知终点（check 是反向线段同向笔）
                    if check >= next_start + 2 and check < len(all_bis):
                        reverse_end_hint = check
                    break

                # 注：原 Step 2.5「单根反向笔突破段起点即终结原段」已删除。
                # 理由（缠论 R5/R6 + 章节 8.1）：
                #   线段只能被反方向"线段"破坏，不能被一根反方向"笔"破坏；
                #   终结的唯一前提是反向特征序列分型，由 _try_end 全权负责判定。
                # 一根反向笔突破段起点只是"出现破坏可能"的线索，必须等反向方向
                # 也形成 ≥3 笔结构并出现 CS 分型后，才能据此回溯终结原段——
                # 这正是 _try_end 的职责，所以这里不再做任何提前终结。

                # Step 3: 吸收
                # 吸收吃掉 [check, check+1] 两根笔，其中 check 是 cs 笔。
                # 增量缓存：把 check 这根 cs 笔追加到 seg_cs_bis。
                if all_bis[check].type == cs_bi_type:
                    seg_cs_bis.append(all_bis[check])
                seg_end = check + 1
                check += 2
            else:
                self._emit_pending(all_bis, seg_start, seg_type)
                break

    # ----------------------------------------------------------
    # _try_end
    # ----------------------------------------------------------
    def _try_end(self, all_bis, seg_start, seg_end, seg_type,
                 seg_high, seg_low, check_pos,
                 seg_cs_bis_cache: Optional[List[BI]] = None) -> Optional[tuple]:

        cs_bi_type = 'down' if seg_type == 'up' else 'up'
        inc_dir = 'up' if seg_type == 'up' else 'down'
        frac_name = '顶分型' if seg_type == 'up' else '底分型'

        # ---- 步骤1 ----
        # 优先使用调用方传入的增量缓存（由 _build_segments 维护），
        # 否则回退为按 seg_start..seg_end 全量过滤（兜底/向后兼容）。
        # 增量维护把每次 _try_end 的 cs 笔收集成本从 O(seg_len) 降到 O(1)，
        # 在 90天 1min 数据这种长段场景下消除 O(n²) 退化。
        if seg_cs_bis_cache is not None:
            seg_cs_bis = seg_cs_bis_cache
        else:
            seg_cs_bis = [all_bis[i] for i in range(seg_start, seg_end + 1)
                          if all_bis[i].type == cs_bi_type]
        if not seg_cs_bis:
            _log.debug(f"    _try_end: 段内无CS笔 → 跳过")
            return None
        if check_pos >= len(all_bis) or all_bis[check_pos].type != cs_bi_type:
            return None

        current_cs_bi = all_bis[check_pos]

        _log.debug(f"    _try_end: 段内CS={len(seg_cs_bis)}根, 当前CS笔={_bi_label(current_cs_bi)}")

        # ---- 步骤2 ----
        if len(seg_cs_bis) >= 2:
            std_seg = _process_inclusion([_bi_to_cs_elem(bi) for bi in seg_cs_bis[:-1]], inc_dir)
            std_seg.append(_bi_to_cs_elem(seg_cs_bis[-1]))
        else:
            std_seg = [_bi_to_cs_elem(seg_cs_bis[0])]
        has_gap = not _overlap(std_seg[-1], current_cs_bi)
        _log.debug(f"    _try_end: 包含处理后std_seg={len(std_seg)}个, 缺口={'有' if has_gap else '无'} → {'第二种' if has_gap else '第一种'}")

        # ---- 步骤3 ----
        # 第一元素（属于原段的最后一根CS笔）从 std_seg 中取出冻结，
        # 按缠论 R8/章节 5.2："假设转折点前后的两个元素不可做包含处理"
        # 因此 first_elem 不能与后续收集到的元素（属于反向段或原段延续，性质未定）合并。
        # 只有 second_elems（look_elems[1:]）内部可以做包含处理。
        first_elem = std_seg.pop(-1)
        second_elems: List[dict] = []
        # 步骤4 需要构成分型 (first_elem, second_elems[0], second_elems[1])，
        # 因此本步骤必须至少收集到 2 个 second_elems（包含合并后），
        # 否则下一轮外层吸收一根 CS 后 first_elem 又会被新的反向笔替换，
        # second_elems 永远凑不到 2 个，导致死循环（segment 无限延伸）。
        min_second = 2
        # SAFETY_LOOKAHEAD: 模块级常量，详见文件顶部定义。
        ready = False
        i = check_pos
        while i < len(all_bis):
            if i - check_pos > SAFETY_LOOKAHEAD:
                _log.debug(f"    _try_end: 扫描超过{SAFETY_LOOKAHEAD}笔仍未凑齐second_elems → 放弃本轮")
                return None
            bi = all_bis[i]
            if bi.type == cs_bi_type:
                new_elem = _bi_to_cs_elem(bi)
                if second_elems and _has_inclusion(second_elems[-1], new_elem):
                    second_elems[-1] = _merge_two(second_elems[-1], new_elem, inc_dir)
                    _log.debug(f"    _try_end: {_bi_label(bi)} 与前元素包含,合并→{_elem_label(second_elems[-1])}")
                else:
                    second_elems.append(new_elem)
                    _log.debug(f"    _try_end: 收集CS {_bi_label(bi)} → second_elems={len(second_elems)}个 (first_elem冻结)")
                    if len(second_elems) >= min_second:
                        ready = True
            elif ready:
                # 已收集到足够 second_elems，遇到非CS笔 → 停止收集，去检查分型
                break
            else:
                # 未收集够 second_elems，检查同向笔是否创新极值（线段延伸）
                if seg_type == 'up' and bi.type == 'up' and bi.high > seg_high:
                    _log.debug(f"    _try_end: {_bi_label(bi)} 创新高({bi.high:.3f}>{seg_high:.3f}) → 线段应延伸,返回None")
                    return None
                if seg_type == 'down' and bi.type == 'down' and bi.low < seg_low:
                    _log.debug(f"    _try_end: {_bi_label(bi)} 创新低({bi.low:.3f}<{seg_low:.3f}) → 线段应延伸,返回None")
                    return None
            i += 1
        look_elems = [first_elem] + second_elems

        if not look_elems:
            return None

        # ---- 步骤4 ----
        # 按缠论原文章节 7.1：
        #   "取分界点前线段的最后一个特征元素（第一元素）
        #    取从转折点开始的第一笔（第二元素）"
        # 也即特征序列分型的结构是固定的：
        #   左肩 = 第一元素 = first_elem        → combined 中位置 = len(std_seg)
        #   中心 = 第二元素 = second_elems[0]   → combined 中位置 = len(std_seg) + 1
        #   右肩 = 第三元素 = second_elems[1]   → combined 中位置 = len(std_seg) + 2
        # 因此分型中心点的位置是固定的，不能在整个 combined 中贪心搜索任意位置。
        # 否则会错误地把 first_elem 之前的 std_seg 元素当成分型中心，违反 R8/章节 5.2
        # （第一元素属于原段，不能与反向段元素一起参与分型判定）。
        combined = std_seg + look_elems
        if len(combined) < 3:
            _log.debug(f"    _try_end: combined={len(combined)}个 < 3 → 不足以判断分型")
            return None
        # 分型中心固定为第二元素的位置：left=first_elem, mid=second_elems[0], right=second_elems[1]
        mid_pos = len(std_seg) + 1  # = combined 中 second_elems[0] 的索引
        if mid_pos + 1 >= len(combined):
            # second_elems 不足 2 个，无法判定分型
            elems_str = " ".join(_elem_label(e) for e in combined)
            _log.debug(f"    _try_end: combined=[{elems_str}] → second_elems<2,无法判定{frac_name}")
            return None
        left, mid, right = combined[mid_pos - 1], combined[mid_pos], combined[mid_pos + 1]
        if seg_type == 'up':
            is_frac = mid['high'] > left['high'] and mid['high'] > right['high']
        else:
            is_frac = mid['low'] < left['low'] and mid['low'] < right['low']
        elems_str = " ".join(_elem_label(e) for e in combined)
        if not is_frac:
            _log.debug(f"    _try_end: combined=[{elems_str}] mid_pos={mid_pos} → 无{frac_name}")
            return None
        frac_idx = mid_pos
        _log.debug(f"    _try_end: combined=[{elems_str}] → {frac_name}在[{frac_idx}](固定第二元素位置)")

        # ---- 步骤5 ----
        mid_elem = combined[frac_idx]
        if has_gap:
            _log.debug(f"    _try_end: 第二种情况,进入_check_type2验证...")
            if not self._check_type2(all_bis, mid_elem, seg_type):
                _log.debug(f"    _try_end: _check_type2失败 → 返回None")
                return None
            _log.debug(f"    _try_end: _check_type2成功")

        # ---- 步骤6: 定位当前线段结束位置 + 反向线段范围 ----
        target_bi = _resolve_pivot_bi(mid_elem, seg_type)

        end_bi_idx = self._bi_pos[id(target_bi)] - 1
        if end_bi_idx <= seg_start or end_bi_idx >= len(all_bis):
            _log.debug(f"    _try_end: end_bi_idx={end_bi_idx} 越界(seg_start={seg_start},len={len(all_bis)}) → 返回None")
            return None
        if all_bis[end_bi_idx].type != seg_type:
            _log.debug(f"    _try_end: 终点笔{_bi_label(all_bis[end_bi_idx])} 方向≠{seg_type} → 返回None")
            return None
        if end_bi_idx - seg_start + 1 < 3:
            _log.debug(f"    _try_end: 笔数{end_bi_idx - seg_start + 1}<3 → 返回None")
            return None

        # 反向线段: 起点=当前线段终点+1, 终点=look_elems中最远的CS笔位置
        next_start = end_bi_idx + 1
        # look_elems 的最后一个元素对应反向线段已探明的最远同向笔
        last_look = look_elems[-1]
        last_look_bis = last_look.get('merged_bis')
        if last_look_bis:
            next_end = max(self._bi_pos[id(b)] for b in last_look_bis)
        else:
            next_end = self._bi_pos[id(last_look['bi'])]
        # 确保 next_end 至少为 next_start + 2（最少3笔）
        next_end = max(next_end, next_start + 2) if next_end >= next_start else next_start + 2

        _log.debug(f"    _try_end: ✓ 线段结束于{_bi_label(all_bis[end_bi_idx])}, "
                   f"反向线段 bi[{all_bis[next_start].index}]~bi[{all_bis[min(next_end, len(all_bis)-1)].index}]")
        return end_bi_idx, next_start, next_end

    # ----------------------------------------------------------
    # _check_type2
    # ----------------------------------------------------------
    def _check_type2(self, all_bis, mid_elem, seg_type) -> bool:

        target_bi = _resolve_pivot_bi(mid_elem, seg_type)

        start_pos = self._bi_pos[id(target_bi)] + 1
        if start_pos >= len(all_bis):
            _log.debug(f"      _check_type2: start_pos={start_pos}越界 → False")
            return False

        cs2_type = 'up' if seg_type == 'up' else 'down'
        cs2_dir = 'down' if seg_type == 'up' else 'up'
        frac2_name = '底分型' if seg_type == 'up' else '顶分型'

        _log.debug(f"      _check_type2: 从{_bi_label(target_bi)}之后开始, 寻找反向线段CS({cs2_type}笔)的{frac2_name}")

        def _is_tail_fractal(elems: List[dict]) -> bool:
            """O(1) 检查最后三个元素是否构成反向段所需的分型。
            up 段的反向段找底分型(mid.low < 两侧)；down 段反向段找顶分型(mid.high > 两侧)。
            只检查尾部三元素：每次新追加/合并 cs2_elems 后调用一次即可，
            等价于原 find_frac2(cs2_elems) 的语义但耗时从 O(k) 降到 O(1)。
            """
            if len(elems) < 3:
                return False
            a, b, c = elems[-3], elems[-2], elems[-1]
            if seg_type == 'up':   # 反向 = down 段 → 找底分型
                return b['low'] < a['low'] and b['low'] < c['low']
            else:                  # 反向 = up 段   → 找顶分型
                return b['high'] > a['high'] and b['high'] > c['high']

        cs2_elems = []
        i = start_pos
        while i < len(all_bis):
            bi = all_bis[i]

            # 原线段严格创新高/低检查（> / <，等价不算创新极值）
            # 修复点：原逻辑直接 return False，会让"等价新高 + 跨段后续创新高"误判为段延伸
            #        现改为"先把这根创新高/低的笔收进 cs2_elems 再判定分型"，
            #        若分型成立则反向段成立 → return True；否则才 return False。
            is_strict_new_extreme = (
                (seg_type == 'up' and bi.type == 'up' and bi.high > target_bi.high)
                or (seg_type == 'down' and bi.type == 'down' and bi.low < target_bi.low)
            )

            if bi.type == cs2_type:
                # 包含处理
                new_elem = _bi_to_cs_elem(bi)
                if cs2_elems and _has_inclusion(cs2_elems[-1], new_elem):
                    cs2_elems[-1] = _merge_two(cs2_elems[-1], new_elem, cs2_dir)
                    _log.debug(f"      _check_type2: {_bi_label(bi)} 与前元素包含,合并→{_elem_label(cs2_elems[-1])}")
                else:
                    cs2_elems.append(new_elem)
                    _log.debug(f"      _check_type2: 收集CS {_bi_label(bi)} → cs2_elems={len(cs2_elems)}个")

                # 每次添加/合并后立即检查分型（仅看尾部三元素，O(1)）
                if _is_tail_fractal(cs2_elems):
                    _log.debug(f"      _check_type2: 尾部三元素构成{frac2_name} → True")
                    return True

                # 收完后才判断"严格创新极值停止"：
                # 此根 cs 笔创了原段方向的新极值，后续走势不可能再形成本段的反向段，
                # 必须立即停止扫描；反向段是否成立由已收集的 cs2_elems 决定。
                if is_strict_new_extreme:
                    _log.debug(f"      _check_type2: {_bi_label(bi)} 创新极值且已收进 cs2_elems → 停止扫描")
                    break
            elif is_strict_new_extreme:
                # 非 cs2 笔但创了新极值（兜底）：原段延伸，反向段不成立
                _log.debug(f"      _check_type2: {_bi_label(bi)} 非CS笔但创新极值 → 原线段延伸,False")
                return False
            # 注：原此处对每根非 cs2 笔重复 find_frac2(cs2_elems) 的 elif 分支已删除——
            # 分型只可能在新元素加入/合并时产生新结构，非 cs2 笔不会改变 cs2_elems，
            # 重复检查纯属冗余且产生大量噪音日志。
            i += 1

        if len(cs2_elems) < 3:
            _log.debug(f"      _check_type2: cs2_elems仅{len(cs2_elems)}个<3 → False")
            return False
        # 走到这里说明扫描结束（要么 i 越界，要么遇到 strict_new_extreme break）
        # 由于循环内每次追加/合并后都已经检查过尾部分型，此处只需对最终状态做一次兜底检查。
        result = _is_tail_fractal(cs2_elems)
        elems_str = " ".join(_elem_label(e) for e in cs2_elems)
        _log.debug(f"      _check_type2: 最终[{elems_str}] → {frac2_name}{'成立' if result else '不成立'} → {result}")
        return result

    # ----------------------------------------------------------
    # 输出线段
    # ----------------------------------------------------------
    def _make_xd(self, seg_bis: List[BI], seg_type: str, done: bool) -> XD:
        """构造并追加 XD 对象（_emit_segment 与 _emit_pending 的公共逻辑）。

        Args:
            seg_bis: 组成该线段的笔列表（首笔=段起点，末笔=段终点）
            seg_type: 'up' / 'down'
            done: 是否为已完成段
                  - True  → zs_high/zs_low = (起点价, 终点价) 的 max/min（已完成段中枢）
                  - False → zs_high/zs_low = 整段 high/low（未完成段无明确中枢，用宽口径）

        Returns:
            构造好的 XD 对象（已 append 到 self.xds）
        """
        xd = XD(
            start=seg_bis[0].start,
            end=seg_bis[-1].end,
            start_line=seg_bis[0],
            end_line=seg_bis[-1],
            _type=seg_type,
            index=len(self.xds),
            default_zs_type=self.config.get('zs_type_xd', None),
        )
        xd.high = max(bi.high for bi in seg_bis)
        xd.low = min(bi.low for bi in seg_bis)
        if done:
            sv, ev = seg_bis[0].start.val, seg_bis[-1].end.val
            xd.zs_high, xd.zs_low = (max(sv, ev), min(sv, ev))
        else:
            xd.zs_high, xd.zs_low = xd.high, xd.low
        xd.done = done
        self.xds.append(xd)
        return xd

    def _emit_segment(self, all_bis, start, end, seg_type):
        seg_bis = all_bis[start: end + 1]
        xd = self._make_xd(seg_bis, seg_type, done=True)
        sv, ev = seg_bis[0].start.val, seg_bis[-1].end.val
        _log.info(f"[完成] XD[{xd.index}] {seg_type} {_bi_label(seg_bis[0])}~{_bi_label(seg_bis[-1])} ({len(seg_bis)}笔) {sv:.3f}→{ev:.3f}")

    def _emit_pending(self, all_bis, start, seg_type):
        """输出未完成线段（方案 A1：全局极值优先 + 兜底末尾同向笔）。

        终点选择策略（双路保障，确保有 ≥3 根笔时必有输出）：

        主路径（全局极值）：
          扫描 candidates 中所有 seg_type 同向笔，取使段达到方向极值的那根作为终点
            - up 段 → 取 high 最大的 up 笔
            - down 段 → 取 low 最小的 down 笔
          这与已完成段的"段 high/low 应为段内极值"语义一致。

        兜底路径（确保有输出）：
          若主路径选出的极值笔位置导致 pending_bis < 3 根
          （典型场景：段第一根同向笔就是全段极值，后续震荡不再突破），
          则改用 candidates 中**最后一根**同向笔作为终点。
          这保证了"只要 candidates 有 ≥3 根 done 笔，就一定输出未完成段"。

        缠论依据：
          已完成段由 _try_end 严格判定终点；未完成段无完整反向特征序列可用，
          只能保守估计。极值优先体现"线段记录方向极值"，兜底体现"实盘需有持续反馈"。
        """
        candidates = [bi for bi in all_bis[start:] if bi.is_done()]
        if len(candidates) < 3:
            return

        # 主路径：找全局极值的同向笔
        best_idx = -1
        last_same_idx = -1  # 同时记录最后一根同向笔位置，作为兜底
        for i in range(len(candidates)):
            if candidates[i].type != seg_type:
                continue
            last_same_idx = i
            if best_idx == -1:
                best_idx = i
                continue
            cur = candidates[i]
            best = candidates[best_idx]
            if seg_type == 'up' and cur.high > best.high:
                best_idx = i
            elif seg_type == 'down' and cur.low < best.low:
                best_idx = i

        if best_idx == -1:
            return

        pending_bis = candidates[:best_idx + 1]
        # 兜底：若全局极值导致段太短（<3 根），改用最后一根同向笔
        if len(pending_bis) < 3 and last_same_idx > best_idx:
            pending_bis = candidates[:last_same_idx + 1]

        if len(pending_bis) < 3:
            return

        xd = self._make_xd(pending_bis, seg_type, done=False)
        sv, ev = pending_bis[0].start.val, pending_bis[-1].end.val
        _log.info(f"[未完成] XD[{xd.index}] {seg_type} {_bi_label(pending_bis[0])}~{_bi_label(pending_bis[-1])} ({len(pending_bis)}笔) {sv:.3f}→{ev:.3f}")