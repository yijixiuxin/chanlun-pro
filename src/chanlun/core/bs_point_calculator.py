# -*- coding: utf-8 -*-
"""
三类买卖点识别引擎

按 docs/bs_point_calculator_design.md 落地。本文件实现 Step 1（类骨架）+ Step 2（三买几何判定）。

复用资产：
- ``cl.beichi_pz`` / ``cl.beichi_qs`` / ``cl.zss_is_qs``：背驰与趋势判定（已存在）
- ``LINE.add_mmd`` / ``LINE.add_bc``：把识别结果挂到笔/线段上（已存在）
- ``MMD`` / ``BC`` 数据结构（已存在）

本期实现：
- ``BsPointCalculator``：主类骨架 + 主入口 ``calculate``
- ``_detect_3buy_3sell``：第三类买卖点（中枢离开后反抽不回中枢区间）

未实现（保留为占位 + NotImplementedError，由后续阶段补全）：
- ``_detect_1buy_1sell``：依赖 ``cl.beichi_qs``，Step 3 实现
- ``_detect_2buy_2sell``：依赖一买已识别 + ``cl.beichi_pz``，Step 4 实现
"""
from __future__ import annotations
from typing import List, Optional, TYPE_CHECKING

from chanlun.core.cl_interface import LINE, ZS
from chanlun.tools.log_util import LogUtil

if TYPE_CHECKING:
    # 避免 cl.py <-> bs_point_calculator.py 的循环 import
    from chanlun.core.cl import CL


class BsPointCalculator:
    """
    三类买卖点识别引擎

    使用方式（在 ``cl.py::process_mmd()`` 中编排）::

        bi_calc = BsPointCalculator(self, zs_type='bi')
        bi_calc.calculate(self.bi_calculator.bis, self.bis_zss_calculator.zss)

        xd_calc = BsPointCalculator(self, zs_type='xd')
        xd_calc.calculate(self.xd_calculator.xds, self.zss_calculator.zss)

    :param cl: ``CL`` 主类引用，用于复用 ``beichi_pz`` / ``beichi_qs`` / ``zss_is_qs``
    :param zs_type: 中枢类型，``'bi'`` 表示笔层，``'xd'`` 表示线段层。决定结果挂在哪一层。
    :param strict_3_mode: 三买判定模式。
                         ``True`` = 严格定义（反抽必须严格 < ZG）；
                         ``False`` = 允许 ≤ ZG（默认，与缠论原文图 19-21 实战口径一致）
    :param min_signal_interval: 1 类信号最小间隔（按 xd index 距离）。
                               同方向 + 同对照中枢的 1buy/1sell 相邻两次 xd index 差 < 此值
                               时只保留第一个，避免「一波趋势背驰被密集多次报点」。
                               默认 10（约对应同一波背驰的窗口长度）。
                               设为 0 关闭过滤（保留全部信号）。
                               注：仅作用于 1 类信号，2/3 类有自己的对照逻辑，密度天然较低。
    """

    def __init__(
        self,
        cl: 'CL',
        zs_type: str = 'xd',
        strict_3_mode: bool = False,
        min_signal_interval: int = 10,
    ):
        if zs_type not in ('bi', 'xd', 'zsd'):
            raise ValueError(
                f"zs_type 必须是 'bi' / 'xd' / 'zsd' 之一, 当前传入: {zs_type}"
            )
        if min_signal_interval < 0:
            raise ValueError(
                f"min_signal_interval 必须 >= 0, 当前传入: {min_signal_interval}"
            )
        self.cl = cl
        self.zs_type = zs_type
        self.strict_3_mode = strict_3_mode
        self.min_signal_interval = min_signal_interval

    # ------------------------------------------------------------------
    # 主入口
    # ------------------------------------------------------------------
    def calculate(self, lines: List[LINE], zss: List[ZS]) -> None:
        """
        主入口：按 1buy → 2buy → 3buy 顺序识别买卖点，结果直接写回 ``LINE.zs_type_mmds``。

        :param lines: 本级别的所有线段（or 笔）
        :param zss: 本级别已识别的中枢列表（来自 ``ZsCalculator.calculate``）
        """
        if not lines or not zss:
            return

        # 顺序不可调整：2buy 依赖 1buy 已被识别（通过 mmd_exists 反查）
        self._detect_1buy_1sell(lines, zss)
        self._detect_2buy_2sell(lines, zss)
        self._detect_3buy_3sell(lines, zss)

    # ------------------------------------------------------------------
    # 第一类买卖点（Step 3 实现，本期占位）
    # ------------------------------------------------------------------
    def _detect_1buy_1sell(self, lines: List[LINE], zss: List[ZS]) -> None:
        """
        第一类买卖点 = 趋势背驰（直接复用 ``cl.beichi_qs``）

        充要条件（按缠论原文图 39 / 第 61 讲）：
            1. 至少 2 个同向中枢，构成 'up' / 'down' 趋势（``cl.zss_is_qs`` 已校验）
            2. 当前线段方向与趋势方向一致（``cl.beichi_qs`` 已校验）
            3. 当前线段相对"进入前一个中枢的同方向线段"力度衰减（``cl.beichi_qs`` 已校验）

        命名规则：
            - 下跌趋势末段背驰 → ``1buy``
            - 上涨趋势末段背驰 → ``1sell``

        :param lines: 本级别全部线段
        :param zss: 本级别已识别的中枢列表
        """
        if len(zss) < 2:
            # cl.beichi_qs 内部已防御 len(zss)<2，但提前 short-circuit 可省一次循环
            return

        # ★ B2 信号最小间隔过滤：记录每个 (type, zs_index) 维度上"最近一次"
        # 已识别的 1 类信号 xd index，用于丢弃与之间隔 < min_signal_interval 的后续同类信号。
        # 设计目的：避免「同一波趋势背驰被密集多次报点」的密度问题
        # （例如修复前 xd[159]/[161]/[165]/[167] 在 2.5 天内对照同一段连续 4 次报 1sell）。
        last_signal_xd_idx: dict[tuple[str, int], int] = {}

        for now_line in lines:
            # ★ 偏差 #7 修复（未来函数）：beichi_qs 内部用 zss[-2:] 做趋势判定，
            # 若直接传入全量 zss，会让历史 xd 用「未来形成的中枢」做对照（事后追认），
            # 同一个 xd 在不同时间快照下会被反复识别为 1buy/1sell/无信号。
            # 正确语义：每个 now_line 只能用「自身形成时已存在的中枢」判定。
            #
            # 切片规则：zs 必须在 now_line 完成之前已经形成（zs.start 的进入段
            # 起点 K 索引 < now_line 结束 K 索引）。
            now_end_k = now_line.end.k.k_index
            valid_zss = [
                zs for zs in zss
                if zs.start is not None
                and zs.start.start is not None
                and zs.start.start.k.k_index < now_end_k
            ]
            if len(valid_zss) < 2:
                continue

            # ★ 直接复用 cl.beichi_qs（内部已做趋势校验 + 力度对比）
            try:
                is_bc, compare_lines = self.cl.beichi_qs(lines, valid_zss, now_line)
            except Exception as e:
                # 力度对比依赖 MACD，缺数据时跳过单根线段，不阻断整批
                LogUtil.warn(
                    f"[BsPointCalculator] beichi_qs 异常: line.index={now_line.index}, err={e}"
                )
                continue

            if not is_bc or not compare_lines:
                continue

            # ★ 偏差 #3 修复：趋势背驰必须创新低/高（按缠论原文段 482）
            # cl.beichi_qs 仅做 MACD 力度衰减比较，未校验价格几何条件。
            # 原文要求 1buy 必须满足「创新低 + 力度衰减」复合条件，否则
            # 仅是中枢内震荡而非趋势背驰。
            base_line = compare_lines[0]
            if now_line.type == 'down' and now_line.low >= base_line.low:
                continue  # 下跌段未创新低 → 不构成 1buy
            if now_line.type == 'up' and now_line.high <= base_line.high:
                continue  # 上涨段未创新高 → 不构成 1sell

            # 命名：下跌段背驰 → 一买；上涨段背驰 → 一卖
            mmd_name = '1buy' if now_line.type == 'down' else '1sell'

            # 去重：同一线段同一中枢同一名字不重复挂（增量计算保护）
            if self._mmd_already_attached(now_line, mmd_name, zss[-1]):
                continue

            # ★ B2 信号最小间隔过滤：检查同方向 + 同对照中枢的上一次 1 类信号
            # 是否就在 min_signal_interval 个 xd 之内，是的话跳过本次（保留更早那个）。
            # 注意：valid_zss 切片后 valid_zss[-1] 才是 now_line 实际对照的中枢，
            # 不是全量的 zss[-1]！这里用 valid_zss[-1].index 做 key 才能正确去重。
            if self.min_signal_interval > 0:
                key = (now_line.type, valid_zss[-1].index)
                last_idx = last_signal_xd_idx.get(key)
                if last_idx is not None and (now_line.index - last_idx) < self.min_signal_interval:
                    LogUtil.debug(
                        f"[BsPointCalculator] {mmd_name} 因最小间隔过滤跳过: "
                        f"line.index={now_line.index}, last_idx={last_idx}, "
                        f"interval={now_line.index - last_idx} < {self.min_signal_interval}"
                    )
                    continue
                # 无论后续 add_mmd 是否真的写入（_mmd_already_attached 已防重复），
                # 这里都先记录本次为"最近一次信号"
                last_signal_xd_idx[key] = now_line.index

            # 写入背驰
            now_line.add_bc(
                _type='qs',
                zs=zss[-1],
                compare_line=compare_lines[0],
                compare_lines=compare_lines,
                bc=True,
                zs_type=self.zs_type,
            )
            # 写入买卖点（zs 挂在"末中枢"上，符合区间套靠近末端识别的语义）
            now_line.add_mmd(
                name=mmd_name,
                zs=zss[-1],
                zs_type=self.zs_type,
                msg=(
                    f'趋势背驰：当前 {now_line.type} 段相对 '
                    f'index={compare_lines[0].index} 的同向段力度衰减'
                ),
            )
            LogUtil.debug(
                f"[BsPointCalculator] 识别到 {mmd_name}: line.index={now_line.index}, "
                f"compare_idx={compare_lines[0].index}, zs.index={zss[-1].index}"
            )

    # ------------------------------------------------------------------
    # 第二类买卖点（Step 4 实现，本期占位）
    # ------------------------------------------------------------------
    def _detect_2buy_2sell(self, lines: List[LINE], zss: List[ZS]) -> None:
        """
        第二类买卖点 = 一类买卖点后的反抽再回调（不创新低/高），或盘整背驰

        充要条件（按缠论原文图 39 / 学员补充第 61 讲）：
            1. 当前线段往前能找到同方向的 1buy / 1sell（必须先调 ``_detect_1buy_1sell``）
            2. 一买/一卖与当前段之间至少隔了 1 段反向走势（构成"反抽 → 再回调"）
            3. 满足以下任一条件：
               - **条件 A（强）**：当前段的低/高未突破一买的低/高 → 标准二买
               - **条件 B（弱）**：当前段虽创新低/高，但与一买所在段构成盘整背驰
                 （复用 ``cl.beichi_pz``）

        命名规则：
            - 下跌段（一买后再下跌）未破前低 / 盘整背驰 → ``2buy``
            - 上涨段（一卖后再上涨）未破前高 / 盘整背驰 → ``2sell``

        :param lines: 本级别全部线段
        :param zss: 本级别已识别的中枢列表
        """
        # 必须 lines 非空才有反查空间
        if not lines:
            return

        for i, now_line in enumerate(lines):
            if i < 2:
                # 二买/二卖至少需要 1 个一买 + 1 段反抽 + 当前段 = 3 段
                continue

            # ★ B4 方向 Y 增强：扩大反查窗口，扫描最近 3 个同向 1 类信号（不止 1 个），
            # 每个都尝试构建 2buy/2sell。这样能覆盖以下场景：
            # - 一波趋势中有多个 1buy 锚点（如 xd[110] + xd[132] 都是 1buy），
            #   后续反弹回踩既可对照 xd[110] 又可对照 xd[132]，应都允许识别
            # - 命中第一个就跳过后面（避免过度堆叠），但每个 prev 都尝试一次
            prev_1lines = self._find_recent_1mmd_lines(
                lines[:i], target_type=now_line.type, max_count=3
            )
            if not prev_1lines:
                continue

            # ---- 准备 valid_zss（条件 B 用，提到外面避免每个 prev 重复计算）----
            # ★ 偏差 #7 修复（未来函数）：cl.beichi_pz 用 zss[-1] 做盘整对照，
            # 必须按 now_line 时间位置切片 zss，避免历史 xd 用未来中枢做对照。
            now_end_k = now_line.end.k.k_index
            valid_zss_2 = [
                zs for zs in zss
                if zs.start is not None
                and zs.start.start is not None
                and zs.start.start.k.k_index < now_end_k
            ]

            # 对每个候选 prev_1line 尝试判定（找到第一个命中的就跳出）
            for prev_1line in prev_1lines:
                # 校验：一买与当前段之间至少有 1 段反向走势
                if i - prev_1line.index < 2:
                    continue

                # ---- 条件 A：不创新低 / 不创新高（强条件，B3 方向 Y 边界宽容：>= / <=）----
                mmd_name: Optional[str] = None
                msg = ""
                if now_line.type == 'down' and now_line.low >= prev_1line.low:
                    mmd_name = '2buy'
                    msg = (
                        f'一买后反抽再下跌未创新低: '
                        f'now.low={now_line.low} >= prev_1line.low={prev_1line.low}'
                    )
                elif now_line.type == 'up' and now_line.high <= prev_1line.high:
                    mmd_name = '2sell'
                    msg = (
                        f'一卖后反抽再上涨未创新高: '
                        f'now.high={now_line.high} <= prev_1line.high={prev_1line.high}'
                    )

                # ---- 条件 B：创新低/高但盘整背驰（弱条件，B3 方向 Y 增强：双中枢对照）----
                if mmd_name is None and valid_zss_2:
                    # 优先用末中枢，失败再尝试次末中枢（覆盖更多场景）
                    candidates_zs = [valid_zss_2[-1]]
                    if len(valid_zss_2) >= 2:
                        candidates_zs.append(valid_zss_2[-2])
                    for ref_zs_for_pz in candidates_zs:
                        try:
                            is_pz_bc, _ = self.cl.beichi_pz(ref_zs_for_pz, now_line)
                        except Exception as e:
                            LogUtil.warn(
                                f"[BsPointCalculator] beichi_pz 异常: "
                                f"line.index={now_line.index}, zs.index={ref_zs_for_pz.index}, err={e}"
                            )
                            is_pz_bc = False
                        if is_pz_bc:
                            mmd_name = '2buy' if now_line.type == 'down' else '2sell'
                            msg = (
                                f'一买后创新低但盘整背驰（对照 zs.index={ref_zs_for_pz.index}），'
                                f'归为二买/二卖'
                            )
                            break

                if mmd_name is None:
                    continue

                # 二买的 zs 字段挂在"一买所在中枢"上（保持语义一致：mmd.zs 表示参考的中枢）
                ref_zs = self._find_mmd_zs_on_line(prev_1line, target_name=(
                    '1buy' if now_line.type == 'down' else '1sell'
                ))
                if ref_zs is None:
                    # 防御：找不到一买的中枢就用切片后的最近一个中枢兜底
                    ref_zs = valid_zss_2[-1] if valid_zss_2 else None
                if ref_zs is None:
                    continue  # 无任何中枢可挂，放弃

                # 去重（增量计算保护）
                if self._mmd_already_attached(now_line, mmd_name, ref_zs):
                    break  # 已挂过同名 mmd，无需再尝试更早的 prev_1line

                now_line.add_mmd(
                    name=mmd_name,
                    zs=ref_zs,
                    zs_type=self.zs_type,
                    msg=msg,
                )
                LogUtil.debug(
                    f"[BsPointCalculator] 识别到 {mmd_name}: line.index={now_line.index}, "
                    f"prev_1line.index={prev_1line.index}, msg={msg}"
                )
                break  # 已成功挂载，跳过更早的 prev_1line（避免同 xd 重复 2buy）

    # ------------------------------------------------------------------
    # 二买的辅助方法
    # ------------------------------------------------------------------
    def _find_prev_1mmd_line(
        self, prev_lines: List[LINE], target_type: str
    ) -> Optional[LINE]:
        """
        从 ``prev_lines`` 中倒序查找最近一根挂了 ``1buy`` 或 ``1sell`` 的同方向线段。

        :param prev_lines: 候选线段列表（必须不含当前 now_line）
        :param target_type: ``'down'`` 找 1buy；``'up'`` 找 1sell
        :return: 该线段，或 ``None``
        """
        target_mmd_name = '1buy' if target_type == 'down' else '1sell'
        for line in reversed(prev_lines):
            if line.type != target_type:
                continue
            mmds = getattr(line, 'zs_type_mmds', {}).get(self.zs_type, [])
            if any(mmd.name == target_mmd_name for mmd in mmds):
                return line
        return None

    def _find_recent_1mmd_lines(
        self, prev_lines: List[LINE], target_type: str, max_count: int = 3
    ) -> List[LINE]:
        """
        从 ``prev_lines`` 中倒序查找最近 ``max_count`` 根挂了 ``1buy`` 或 ``1sell``
        的同方向线段（B3 方向 Y 增强：替代 _find_prev_1mmd_line 用于多锚点 2buy 识别）。

        返回顺序：从最近到最远（``[最近, 次近, 更早, ...]``）

        :param prev_lines: 候选线段列表（必须不含当前 now_line）
        :param target_type: ``'down'`` 找 1buy；``'up'`` 找 1sell
        :param max_count: 最多返回多少根（默认 3 根，平衡召回率和性能）
        :return: 同方向 1 类信号线段列表（最多 max_count 个）
        """
        target_mmd_name = '1buy' if target_type == 'down' else '1sell'
        result: List[LINE] = []
        for line in reversed(prev_lines):
            if line.type != target_type:
                continue
            mmds = getattr(line, 'zs_type_mmds', {}).get(self.zs_type, [])
            if any(mmd.name == target_mmd_name for mmd in mmds):
                result.append(line)
                if len(result) >= max_count:
                    break
        return result

    def _find_mmd_zs_on_line(
        self, line: LINE, target_name: str
    ) -> Optional[ZS]:
        """
        在 ``line`` 上找 ``target_name`` 对应的 MMD，并返回其 ``zs`` 字段。

        :param line: 已知挂有目标 MMD 的线段
        :param target_name: ``'1buy'`` / ``'1sell'`` 等
        :return: ``MMD.zs``，或 ``None``
        """
        mmds = getattr(line, 'zs_type_mmds', {}).get(self.zs_type, [])
        for mmd in mmds:
            if mmd.name == target_name:
                return mmd.zs
        return None

    # ------------------------------------------------------------------
    # 第三类买卖点（本期实现）
    # ------------------------------------------------------------------
    def _detect_3buy_3sell(self, lines: List[LINE], zss: List[ZS]) -> None:
        """
        第三类买卖点 = 中枢形成后，离开中枢的次级别走势 + 反抽不回中枢区间 [ZG, ZD]

        充要条件（按缠论原文图 6 / 图 19-21）：
            1. 存在已完成的中枢（``zs.done == True`` 且 ``zs.real == True``）
            2. 当前线段位于中枢离开段之后（``now_line.start.index >= zs.end.end.index``）
            3. 当前线段方向与离开段方向相反（即"反抽"）
            4. 反抽幅度未触及中枢区间：
               - 中枢向上离开 → 反抽 ``low > zs.zg`` → ``3buy``
               - 中枢向下离开 → 反抽 ``high < zs.zd`` → ``3sell``

        :param lines: 本级别所有线段
        :param zss: 本级别中枢列表
        """
        for now_line in lines:
            # ★ 偏差 #7 修复（未来函数）：3buy/3sell 关联中枢必须在 now_line
            # 完成之前就已"完成"。若直接传入全量 zss，历史 xd 会被未来才完成
            # 的中枢"事后追认"为 3buy/3sell，与实时识别语义不符。
            now_end_k = now_line.end.k.k_index
            valid_zss = [
                zs for zs in zss
                if zs.start is not None
                and zs.start.start is not None
                and zs.start.start.k.k_index < now_end_k
                # 中枢的"完成"也必须在 now_line 之前（end 段已成型）
                and zs.end is not None
                and zs.end.end is not None
                and zs.end.end.k.k_index < now_end_k
            ]

            # ★ B4 增强已回滚：3buy/3sell 必须只对照"紧邻离开段"的最近一个中枢，
            # 否则远期价格远低/高于早期中枢时，会让每个反抽段对所有早期中枢都报 3 类信号
            # （实测在 90 天数据里 3sell 数量从 48 爆炸到 1079）。
            # 缠论原文 3 类语义就是"中枢离开后的紧邻反抽"，不应放宽到所有历史中枢。
            related_zs = self._find_related_zs_for_3rd(now_line, valid_zss)
            if related_zs is None:
                continue

            mmd_name, msg = self._judge_3rd_bs_point(now_line, related_zs)
            if mmd_name is None:
                continue

            # 去重：同一线段同一中枢同一名字不重复挂
            if self._mmd_already_attached(now_line, mmd_name, related_zs):
                continue

            now_line.add_mmd(
                name=mmd_name,
                zs=related_zs,
                zs_type=self.zs_type,
                msg=msg,
            )
            LogUtil.debug(
                f"[BsPointCalculator] 识别到 {mmd_name}: line.index={now_line.index}, "
                f"zs.index={related_zs.index}, msg={msg}"
            )

    # ------------------------------------------------------------------
    # 三买的辅助方法（仅供 _detect_3buy_3sell 使用）
    # ------------------------------------------------------------------
    def _find_related_zs_for_3rd(
        self, now_line: LINE, zss: List[ZS]
    ) -> Optional[ZS]:
        """
        寻找 ``now_line`` 用于判定三买/三卖时所对应的"被离开中枢"。

        规则：
            - 中枢必须已完成（``done == True``）且有效（``real == True``）
            - ``now_line`` 必须严格在中枢离开段之后（不能是中枢内部线段，也不能是离开段本身）
            - 取所有合法中枢中"离开段最近"的那一个

        注意：B4 方向 Y 增强后，新代码改为调用 ``_find_all_candidate_zss_for_3rd``，
        本方法保留作为向后兼容（取列表第一个 = 最近的），不再被主流程调用。

        :param now_line: 候选反抽线段
        :param zss: 中枢列表（按 index 升序）
        :return: 对应中枢，或 ``None``（无合法中枢）
        """
        candidates = self._find_all_candidate_zss_for_3rd(now_line, zss)
        return candidates[0] if candidates else None

    def _find_all_candidate_zss_for_3rd(
        self, now_line: LINE, zss: List[ZS]
    ) -> List[ZS]:
        """
        B4 方向 Y 增强：扫描所有满足时间/状态条件的候选中枢，按"离开段距离 now_line 由近到远"排序。

        规则（与 _find_related_zs_for_3rd 一致）：
            - 中枢已完成（``done == True``）且有效（``real == True``）
            - ``now_line`` 必须严格在中枢离开段之后
            - ``now_line`` 不能是中枢的核心线段或离开段本身

        :param now_line: 候选反抽线段
        :param zss: 中枢列表（按 index 升序，已经过 valid_zss 时间窗口切片）
        :return: 按"离开段最近"排序的中枢列表（可能为空）
        """
        candidates: List[ZS] = []
        for zs in zss:
            if not zs.done or not zs.real:
                continue
            if zs.end is None or zs.end.end is None:
                # 完成的中枢必有 end，本分支属防御性
                continue

            # now_line 必须在中枢离开段"之后"
            if now_line.index <= zs.end.index:
                continue

            # now_line 不能是中枢的核心线段
            if now_line in zs.lines:
                continue
            # now_line 不能就是离开段
            if now_line is zs.end:
                continue

            candidates.append(zs)

        # 按 zs.end.index 倒序（最近的 zs 排第一）
        candidates.sort(key=lambda zs: zs.end.index, reverse=True)
        return candidates

    def _judge_3rd_bs_point(
        self, now_line: LINE, zs: ZS
    ) -> tuple[Optional[str], str]:
        """
        判定 ``now_line`` 相对中枢 ``zs`` 是否构成三买/三卖。

        :return: (mmd_name, msg)。``mmd_name`` 为 ``'3buy'`` / ``'3sell'`` / ``None``
        """
        leave_direction = zs.end.type  # 离开段方向
        if leave_direction not in ('up', 'down'):
            return None, ""

        # 反抽方向必须与离开段相反
        if now_line.type == leave_direction:
            return None, ""

        if leave_direction == 'up':
            # 中枢向上离开 → now_line 是 down 反抽 → 反抽未跌破 ZG → 三买
            threshold_ok = (
                now_line.low > zs.zg if self.strict_3_mode else now_line.low >= zs.zg
            )
            if threshold_ok:
                op = '>' if self.strict_3_mode else '>='
                return (
                    '3buy',
                    f'中枢向上离开后反抽未跌破 ZG: line.low={now_line.low} {op} ZG={zs.zg}',
                )
            return None, ""

        # leave_direction == 'down'
        # 中枢向下离开 → now_line 是 up 反抽 → 反抽未触及 ZD → 三卖
        threshold_ok = (
            now_line.high < zs.zd if self.strict_3_mode else now_line.high <= zs.zd
        )
        if threshold_ok:
            op = '<' if self.strict_3_mode else '<='
            return (
                '3sell',
                f'中枢向下离开后反抽未触及 ZD: line.high={now_line.high} {op} ZD={zs.zd}',
            )
        return None, ""

    def _mmd_already_attached(
        self, line: LINE, mmd_name: str, zs: ZS
    ) -> bool:
        """
        检查 ``line`` 在 ``self.zs_type`` 下是否已经挂过同名 + 同中枢的买卖点（避免增量计算重复挂）。

        BI / XD 都继承自 LINE 并提供 ``zs_type_mmds: Dict[str, List[MMD]]``，
        且都有 ``mmd_exists`` 方法。这里使用更精确的"同 MMD.zs"比较以支持去重。
        """
        existing_mmds = getattr(line, 'zs_type_mmds', {}).get(self.zs_type, [])
        for mmd in existing_mmds:
            if mmd.name == mmd_name and mmd.zs is zs:
                return True
        return False