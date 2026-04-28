# -*- coding: utf-8 -*-
import datetime
from typing import Dict, Union, List, Tuple, Any
import pandas as pd

from chanlun.core.bi_calculator import BiCalculator
# ★ F3：calculate_zss 模块已被物理删除（D3 软下线 → E3 接入 ZsCalculator → F3 硬下线）。
# 笔/段两层中枢全部统一走 ZsCalculator，create_dn_zs 也用临时 ZsCalculator 实例。
from chanlun.core.cl_interface import ICL, Kline, CLKline, FX, BI, XD, ZS, Config, LINE, compare_ld_beichi
from chanlun.core.cl_kline_process import CL_Kline_Process
from chanlun.core.kline_data_processor import KlineDataProcessor
from chanlun.core.macd import MACD
from chanlun.core.xd_calculator import XdCalculator
from chanlun.core.zs_calculator import ZsCalculator
from chanlun.tools.log_util import LogUtil


class CL(ICL):
    """
    缠论分析主类
    实现缠论的完整分析流程，包括K线处理、分型识别、笔线段计算、中枢分析等
    """

    def __init__(
            self,
            code: str,
            frequency: str,
            config: Union[dict, None] = None,
            start_datetime: datetime.datetime = None,
    ):
        """
        初始化缠论分析器

        Args:
            code: 标的代码
            frequency: 分析周期
            config: 配置参数字典
            start_datetime: 开始分析时间
        """
        self.code = code
        self.frequency = frequency
        self.config = config if config else {}
        self.start_datetime = start_datetime

        # 设置默认配置
        self._init_default_config()

        # 实例化K线数据处理器
        self.kline_processor = KlineDataProcessor(self.start_datetime)
        # 实例化缠论K线处理器，用于处理包含关系
        self.cl_kline_processor = CL_Kline_Process()
        # 实例化MACD计算器
        self.macd_calculator = MACD()
        # 实例化笔计算器
        self.bi_calculator = BiCalculator(bi_mode = 'strict')
        # 实例化线段计算器
        self.xd_calculator = XdCalculator(self.config)

        self.zss_calculator = ZsCalculator()
        # ★ E3：笔层中枢计算器，与 zss_calculator（线段层）独立维护。
        # 必须在 __init__ 里创建——process_klines 会直接调
        # self.bi_zss_calculator.calculate(...)，否则全新对象首次调用就会报
        # 'CL' object has no attribute 'bi_zss_calculator'；
        # 同时 get_bi_zss() / type_bi_zss 也都从这里读取。
        self.bi_zss_calculator = ZsCalculator()


        # 存储各级别数据
        # self.zsds: List[XD] = []  # 走势段列表
        # self.qsds: List[XD] = []  # 趋势段列表
        # 中枢数据
        # ★ G1：原 self.bi_zss 字段已废弃。笔中枢统一从 self.bi_zss_calculator
        # 经 get_bi_zss() / type_bi_zss 读取，不再维护本地缓存字典，避免双源歧义。
        self.zsd_zss: List[ZS] = []  # 走势段中枢
        self.qsd_zss: List[ZS] = []  # 趋势段中枢

        # 最后中枢缓存
        self._last_bi_zs: Union[ZS, None] = None
        self._last_xd_zs: Union[ZS, None] = None

        # ★ 性能优化：process_mmd 触发签名缓存
        # 仅当 xds + bis 尾部签名（长度/末段 K 索引/末段 done 状态）发生变化时
        # 才重新跑 process_mmd，避免每根 K 线都全量扫描 1B/2B/3B（O(N²)）。
        # ★ F1 扩展：接入笔层后，签名同时覆盖 xd / bi 尾部，
        # 任一层尾部变化都需要重跑（否则笔变了但 xd 没变会跳过笔层重算 → 笔层信号缺失）。
        self._last_mmd_sig: Union[tuple, None] = None

        # 兼容运行时期望字段
        self.debug: bool = False
        self.use_time: dict = {}

    def _init_default_config(self):
        """初始化默认配置参数"""
        default_config = {
            # 运行时兼容标识
            'config_use_type': 'common',
            # K线类型配置
            'kline_type': Config.KLINE_TYPE_DEFAULT.value,
            'kline_qk': Config.KLINE_QK_NONE.value,

            # 分型配置
            'fx_qy': Config.FX_QY_THREE.value,
            'fx_qj': Config.FX_QJ_CK.value,
            'fx_bh': Config.FX_BH_NO.value,

            # 笔配置
            'bi_type': Config.BI_TYPE_NEW.value,
            'bi_bzh': Config.BI_BZH_YES.value,
            'bi_qj': Config.BI_QJ_DD.value,
            'bi_fx_cgd': Config.BI_FX_CHD_NO.value,

            # 线段配置
            'xd_qj': Config.XD_QJ_DD.value,
            'xd_bzh': Config.ZSD_BZH_YES.value,
            'xd_bi_pohuai': Config.XD_BI_POHUAI_NO.value,

            # 中枢配置
            'zs_type_bi': Config.ZS_TYPE_BZ.value,
            'zs_type_xd': Config.ZS_TYPE_BZ.value,
            # ★ E3 补丁：ai_analyse 等老下游通过 ``cd.get_config()["zs_bi_type"]``
            # 拿"想看的笔中枢类型列表"，没设默认会 KeyError。
            # 这里给一个等价于 [zs_type_bi] 的默认值，保持向后兼容。
            'zs_bi_type': [Config.ZS_TYPE_BZ.value],
            'zs_xd_type': [Config.ZS_TYPE_BZ.value],
            'zs_qj': Config.ZS_QJ_DD.value,
            'zs_cd': Config.ZS_CD_THREE.value,
            'zs_wzgx': Config.ZS_WZGX_ZGGDD.value,
            'cal_last_zs': True,
            'use_macd_ld': True,
        }

        for key, value in default_config.items():
            if key not in self.config:
                self.config[key] = value

    def process_klines(self, klines: pd.DataFrame):
        """
        处理K线数据，计算缠论分析结果
        支持增量更新：通过对比K线数量和最后一根K线状态，避免不必要的重复计算。

        性能优化：内部流水线直接引用子计算器数据，避免 copy.deepcopy 开销。
        外部通过 get_xxx() 方法获取数据时仍会 deepcopy，保证外部调用安全。

        ★ C3 修复：流水线任一环节抛异常时，重置签名缓存与中枢/MMD 状态，
        避免「half-applied」状态被下次调用按"未变化"路径跳过。
        子计算器内部各自维护 snapshot；如果某一步失败但 snapshot 已经写入，
        外层签名却没更新，会导致下次调用看到 (snapshot 命中 + 签名命中) → 跳过，
        从而隐藏真实的数据错位。这里在 except 中把所有外层缓存清掉，
        强制下次走全量重算。
        """
        # 返回增量更新或新增的K线数据列表
        src_klines: List[Kline] = self.kline_processor.process_kline(klines)
        if not src_klines:
            return

        try:
            # 直接引用内部数据，避免 deepcopy
            # 使用MACD计算器更新指标
            self.macd_calculator.process_macd(self.kline_processor.klines)

            # 更新缠论K线：process_cl_klines 是内部状态更新器，不依赖返回值。
            # 下游直接继续消费 self.cl_kline_processor.cl_klines。
            self.cl_kline_processor.process_cl_klines(self.kline_processor.klines)

            # 计算笔和分型 - 直接引用 cl_klines
            self.bi_calculator.calculate(self.cl_kline_processor.cl_klines)

            # 计算线段 - 直接引用 bis
            self.xd_calculator.calculate(self.bi_calculator.bis)

            # 计算中枢 - 直接引用 xds
            self.zss_calculator.calculate(self.xd_calculator.xds)

            # ★ E3：笔层中枢与线段层完全对称地接入，主流程自动算。
            # 注意：bi_zss_calculator 与 zss_calculator 是两个独立实例，
            # 状态/快照互不污染；任一异常会被外层 except 统一清理。
            self.bi_zss_calculator.calculate(self.bi_calculator.bis)

            # 每次处理后重置缓存，确保下次访问时重新计算
            self._last_bi_zs = None
            self._last_xd_zs = None

            # ★ 关键修复：自动连带跑 process_mmd，避免 web 路径（fdb.get_web_cl_data）
            # 漏调用 process_mmd 导致前端永远看不到 1B/2B/3B 买卖点。
            # process_mmd 内部已通过 BsPointCalculator._mmd_already_attached 做了去重，
            # 增量调用幂等（详见 tests/test_integration_real_klines.py::test_int_r3_idempotent_real）。
            #
            # 性能优化：用 xds + bis 尾部签名做脏检查。BsPointCalculator.calculate 内部是
            # 3 个 O(N) 全量扫描，每根 K 线都跑会在长序列上产生明显卡顿。
            # 当 xds 和 bis 尾部都没有发生「新增」「末段端点变化」「末段 done 翻转」
            # 时直接复用上一轮已经挂在 LINE.zs_type_mmds 上的结果，可省 90%+ 的重复计算。
            #
            # ★ F1 扩展：签名扩展到 (xd_sig, bi_sig)，覆盖笔层接入后的脏检需要。
            # bis 在分钟级序列中变化频率比 xds 高（一个段往往包含多笔），
            # 这里用一个 6 元组同时承载两层尾部状态。
            new_sig = (
                self._calc_layer_sig(self.xd_calculator.xds),
                self._calc_layer_sig(self.bi_calculator.bis),
            )
            if new_sig != self._last_mmd_sig:
                self.process_mmd()
                self._last_mmd_sig = new_sig
        except Exception:
            # ★ C3 修复：任意子步骤失败 → 把外层签名 / 中枢缓存全部清空，
            # 强制下一次调用走全量分支。这样即使内部某个 calculator 留下了
            # 脏 snapshot，也会被下一次的全量重算覆盖掉。
            self._last_mmd_sig = None
            self._last_bi_zs = None
            self._last_xd_zs = None
            # ★ E5 扩展：显式清掉两个 ZsCalculator 的内部 snapshot，
            # 让下次 calculate 必走全量。
            # 之前只清了 cl 层的缓存，但子计算器自己的
            # _last_lines_count / _last_tail_snapshot 仍可能是「半截状态」
            # （例如 zss_calculator 跑完了 xd 中枢，bi_zss_calculator 抛错前
            # 已经写了一部分 zss）。下一次进来时若 cl 层重算成功，
            # bi_zss_calculator 看到 lines 长度增加，会以为是普通增量，
            # 从一个不存在的 entry_idx 继续 → 漏识别中枢。
            # 这里强制把内部状态推平。
            try:
                self.zss_calculator._last_lines_count = 0
                self.zss_calculator._last_tail_snapshot = None
                self.bi_zss_calculator._last_lines_count = 0
                self.bi_zss_calculator._last_tail_snapshot = None
            except AttributeError:
                # 兼容老版本 ZsCalculator（没有这两个字段）
                pass
            raise

        return self

    # --- ICL 接口实现 ---
    def get_code(self) -> str:
        """返回标的代码"""
        return self.code

    def get_frequency(self) -> str:
        """返回分析周期"""
        return self.frequency

    def get_config(self) -> dict:
        """返回配置参数"""
        return self.config

    def get_src_klines(self) -> List[Kline]:
        """返回原始K线列表（浅拷贝，防止外部修改列表结构）"""
        return list(self.kline_processor.klines)

    def get_klines(self) -> List[Any]:
        """返回K线列表"""
        if self.config.get('kline_type') == Config.KLINE_TYPE_CHANLUN.value:
            return self.get_cl_klines()
        else:
            return self.get_src_klines()

    def get_cl_klines(self) -> List[CLKline]:
        """返回缠论K线列表（浅拷贝）"""
        return list(self.cl_kline_processor.cl_klines)

    def get_idx(self) -> dict:
        """返回技术指标数据"""
        # 从MACD计算器获取结果
        return self.macd_calculator.get_results()

    def get_fxs(self) -> List[FX]:
        """返回分型列表（浅拷贝）"""
        return list(self.bi_calculator.fxs)

    def get_bis(self) -> List[BI]:
        """返回笔列表（浅拷贝）"""
        return list(self.bi_calculator.bis)

    def get_xds(self) -> List[XD]:
        """返回线段列表（浅拷贝）"""
        return list(self.xd_calculator.xds)

    def get_zsds(self) -> List[XD]:
        """返回走势段列表"""
        return []

    def get_qsds(self) -> List[XD]:
        """返回趋势段列表"""
        return []

    def get_bi_zss(self, zs_type: str = None) -> List[ZS]:
        """返回笔中枢列表

        ★ E3 接入：从 ``bi_zss_calculator`` 读取最新状态，与 ``get_xd_zss``
        完全对称（已完成 zss + 当前 pending_zs）。

        参数 ``zs_type`` 在 ZsCalculator 当前实现里只有 BZ（标准）一种走法，
        参数保留只是为了与历史接口签名兼容，未来接入多类型时再做分发。
        """
        zss = list(self.bi_zss_calculator.zss)
        if self.bi_zss_calculator.pending_zs is not None:
            zss.append(self.bi_zss_calculator.pending_zs)
        return zss

    def get_xd_zss(self, zs_type: str = None) -> List[ZS]:
        """返回线段中枢字典"""
        zss = list(self.zss_calculator.zss)
        if self.zss_calculator.pending_zs:
            zss.append(self.zss_calculator.pending_zs)
        return zss

    def get_zsd_zss(self) -> List[ZS]:
        """返回走势段中枢列表"""
        return self.zsd_zss

    def get_qsd_zss(self) -> List[ZS]:
        """返回趋势段中枢列表"""
        return self.qsd_zss

    def get_last_bi_zs(self) -> Union[ZS, None]:
        """返回最后的笔中枢

        ★ E3 接入 + 复用 C5 在线段层的修复思路：
        直接读 ``bi_zss_calculator`` 的最新状态，与 ``get_bi_zss()`` 尾部一致。
          - 优先取 ``pending_zs``（最新但未完成的那个）
          - 退而取 ``zss[-1]``（最后一个已完成的）
        不再用 ``self.bis[-5:] + create_dn_zs`` 截尾重算的老套路
        （该写法在 xd 层已被 C5 证伪 —— 数据范围少了进入段定位会偏移）。
        """
        if not self.config.get('cal_last_zs', True):
            return None

        if self._last_bi_zs is None:
            if self.bi_zss_calculator.pending_zs is not None:
                self._last_bi_zs = self.bi_zss_calculator.pending_zs
            elif self.bi_zss_calculator.zss:
                self._last_bi_zs = self.bi_zss_calculator.zss[-1]

        return self._last_bi_zs

    def get_last_xd_zs(self) -> Union[ZS, None]:
        """
        返回最后的线段中枢。

        ★ C5 修复：之前用 self.xds[-5:] + create_dn_zs 重算，
        在主流程外又跑一次中枢识别，结果可能与 self.zss_calculator.zss
        不一致（数据范围少了，进入段定位会偏移）。
        现在直接复用 zss_calculator 的状态：
          - 优先取 pending_zs（最近一个未完成中枢）
          - 退而取 zss[-1]（最后一个已完成中枢）
        这样和 get_xd_zss() 的尾部完全一致，外部消费不会有口径差异。
        """
        if not self.config.get('cal_last_zs', True):
            return None

        if self._last_xd_zs is None:
            # 优先 pending（最新但未完成），其次取最后一个完成的
            if self.zss_calculator.pending_zs is not None:
                self._last_xd_zs = self.zss_calculator.pending_zs
            elif self.zss_calculator.zss:
                self._last_xd_zs = self.zss_calculator.zss[-1]

        return self._last_xd_zs

    def beichi_pz(self, zs: ZS, now_line: LINE) -> Tuple[bool, Union[LINE, None]]:
        """
        判断中枢与指定线是否构成盘整背驰

        Args:
            zs: 中枢对象
            now_line: 当前线

        Returns:
            (是否背驰, 比较的线)
        """
        if len(zs.lines) < 2:
            return False, None

        # 找到同方向的比较线
        compare_line = None
        for line in reversed(zs.lines[:-1]):
            if line.type == now_line.type:
                compare_line = line
                break

        if not compare_line:
            return False, None

        # 力度比较
        now_ld = now_line.get_ld(self)
        compare_ld = compare_line.get_ld(self)

        is_bc = compare_ld_beichi(compare_ld, now_ld, now_line.type)

        return is_bc, compare_line

    def beichi_qs(
            self, lines: List[LINE], zss: List[ZS], now_line: LINE
    ) -> Tuple[bool, List[LINE]]:
        """
        判断指定线与之前的中枢，是否形成了趋势背驰

        Args:
            lines: 线的列表
            zss: 中枢列表
            now_line: 当前线

        Returns:
            (是否背驰, 比较的线列表)
        """
        if len(zss) < 2:
            return False, []

        # 检查最后两个中枢是否形成趋势
        last_zs = zss[-1]
        prev_zs = zss[-2]

        qs_direction = self.zss_is_qs(prev_zs, last_zs)
        if not qs_direction or qs_direction != now_line.type:
            return False, []

        # 找到进入前一个中枢的同方向线段
        # ★ 修复：ZS.start 是 LINE/XD/BI 对象（不是 FX），需要用 .start.k.k_index
        # 取前一个中枢的「进入段」起点 K 索引作为时间边界。
        # ★ C6 修复：原代码 prev_zs.start.start.k.k_index 三层链式访问，
        # 任一环节为 None 就抛 AttributeError 导致整次 process_mmd 失败。
        # 中枢的 start（进入段）/start.start（进入段的起点 FX）/start.k（FX 对应的缠论 K 线）
        # 在某些边界 case（首根中枢、xd 重新构造）下可能临时为空。
        prev_zs_start_k_index = self._safe_line_start_k_index(prev_zs.start)
        if prev_zs_start_k_index is None:
            return False, []

        compare_lines = []
        for line in lines:
            line_end_k_index = self._safe_line_end_k_index(line)
            if line_end_k_index is None:
                continue
            if line.type == now_line.type and line_end_k_index <= prev_zs_start_k_index:
                compare_lines.append(line)

        if not compare_lines:
            return False, []

        # 取最后一个同方向线段进行比较
        compare_line = compare_lines[-1]

        # 力度比较
        now_ld = now_line.get_ld(self)
        compare_ld = compare_line.get_ld(self)

        is_bc = compare_ld_beichi(compare_ld, now_ld, now_line.type)

        return is_bc, [compare_line]

    def zss_is_qs(self, one_zs: ZS, two_zs: ZS) -> Union[str, None]:
        """
        判断两个中枢是否形成趋势

        Args:
            one_zs: 第一个中枢
            two_zs: 第二个中枢

        Returns:
            'up' 向上趋势, 'down' 向下趋势, None 无趋势
        """
        wzgx_config = self.config.get('zs_wzgx', Config.ZS_WZGX_ZGGDD.value)

        if wzgx_config == Config.ZS_WZGX_ZGD.value:
            # 宽松比较：zg与zd
            if one_zs.zg < two_zs.zd:
                return 'up'
            elif one_zs.zd > two_zs.zg:
                return 'down'
        elif wzgx_config == Config.ZS_WZGX_ZGGDD.value:
            # 较为宽松：zg与dd, zd与gg
            if one_zs.zg < two_zs.dd:
                return 'up'
            elif one_zs.zd > two_zs.gg:
                return 'down'
        elif wzgx_config == Config.ZS_WZGX_GD.value:
            # 严格比较：gg与dd
            if one_zs.gg < two_zs.dd:
                return 'up'
            elif one_zs.dd > two_zs.gg:
                return 'down'

        return None

    def create_dn_zs(
        self,
        zs_type: str,
        lines: List[LINE],
        max_line_num: int = 999,
        zs_include_last_line=True,
        ) -> List[ZS]:
        """
        ★ F3：calculate_zss 模块已物理删除，本方法只走 ZsCalculator。
        - bi / xd 都用临时 ZsCalculator 实例计算，保证两套口径完全一致
        - max_line_num / zs_include_last_line 在新实现里没有对应语义，
          保留入参签名仅为兼容老调用方
        - 主流程层（process_klines 阶段）已经分别由 bi_zss_calculator
          和 zss_calculator 管理增量状态；create_dn_zs 是一次性纯函数式调用，
          不持有状态，每次实例化新计算器即可
        """
        if not lines:
            return []
        tmp_calc = ZsCalculator()
        return tmp_calc.calculate(lines)

    # --- 兼容属性与方法 ---
    @property
    def idx(self) -> dict:
        return self.macd_calculator.get_results()

    @property
    def src_klines(self) -> List[Kline]:
        return self.kline_processor.klines

    @property
    def cl_klines(self) -> List[CLKline]:
        return self.cl_kline_processor.cl_klines

    @property
    def fxs(self) -> List[FX]:
        return self.bi_calculator.fxs

    @property
    def bis(self) -> List[BI]:
        return self.bi_calculator.bis

    @property
    def xds(self) -> List[XD]:
        return self.xd_calculator.xds

    @property
    def zsds(self) -> List[XD]:
        return self.get_zsds()

    @property
    def qsds(self) -> List[XD]:
        return self.get_qsds()

    @property
    def last_bi_zs(self) -> Union[ZS, None]:
        return self.get_last_bi_zs()

    @property
    def last_xd_zs(self) -> Union[ZS, None]:
        return self.get_last_xd_zs()

    @property
    def type_bi_zss(self) -> dict:
        return {Config.ZS_TYPE_BZ.value: self.get_bi_zss(Config.ZS_TYPE_BZ.value)}

    @property
    def type_xd_zss(self) -> dict:
        return {Config.ZS_TYPE_BZ.value: self.get_xd_zss(Config.ZS_TYPE_BZ.value)}

    @property
    def type_zsd_zss(self) -> dict:
        return {Config.ZS_TYPE_BZ.value: self.get_zsd_zss()}

    def default_bi_zs_type(self) -> str:
        return self.config.get('zs_type_bi', Config.ZS_TYPE_BZ.value)

    def default_xd_zs_type(self) -> str:
        return self.config.get('zs_type_xd', Config.ZS_TYPE_BZ.value)

    def write_debug_log(self, msg: str):
        if self.debug:
            LogUtil.debug(msg)

    def _add_time(self, key: str, value: float):
        self.use_time[key] = value

    # --- process_xxx 系列：手工分步触发的兼容入口 ---
    # ★ C4 说明：这些方法保留给历史调用方（notebook、单测、外部脚本）。
    # 子计算器内部都已经做了脏检查（snapshot 比对），所以即便调用方
    # 多次调用同一个 process_xxx，也不会真的全量重算，行为天然幂等。
    # 这里只做 thin wrapper，不再做额外缓存判断，避免和子计算器
    # 内部逻辑双重判定时出现状态分裂。

    def process_idx(self):
        self.macd_calculator.process_macd(self.kline_processor.klines)
        return self

    def process_fx(self):
        # fxs 现在由 BiCalculator 一并产出，没有独立的 fx 阶段。
        self.bi_calculator.calculate(self.cl_kline_processor.cl_klines)
        return self

    def process_bi(self):
        self.bi_calculator.calculate(self.cl_kline_processor.cl_klines)
        return self

    def process_up_line(self):
        self.xd_calculator.calculate(self.bi_calculator.bis)
        return self

    def process_zs(self):
        self.zss_calculator.calculate(self.xd_calculator.xds)
        # 中枢有变化时，最后中枢缓存必须失效，
        # 否则下次 last_xd_zs 仍是旧值。
        self._last_xd_zs = None
        self._last_bi_zs = None
        return self

    @staticmethod
    def _safe_line_start_k_index(line) -> Union[int, None]:
        """安全取 line.start.k.k_index，任一环节为 None 返回 None。

        ★ C6 配套：缠论对象链 line.start (FX) → .k (CLKline) → .k_index (int)
        上层调用方很多地方默认它存在，但首根中枢/异常构造下可能缺失。
        """
        try:
            start = getattr(line, 'start', None)
            if start is None:
                return None
            k = getattr(start, 'k', None)
            if k is None:
                return None
            return getattr(k, 'k_index', None)
        except AttributeError:
            return None

    @staticmethod
    def _safe_line_end_k_index(line) -> Union[int, None]:
        """安全取 line.end.k.k_index，任一环节为 None 返回 None。"""
        try:
            end = getattr(line, 'end', None)
            if end is None:
                return None
            k = getattr(end, 'k', None)
            if k is None:
                return None
            return getattr(k, 'k_index', None)
        except AttributeError:
            return None

    @staticmethod
    def _calc_layer_sig(lines) -> tuple:
        """
        ★ F1 辅助：构造单一 line 层（xds 或 bis）的尾部签名。

        签名包含 (长度, 末段 end.k.k_index, 末段 done)：
          - 长度变化 → 新增了段
          - end.k.k_index 变化 → 末段端点漂移（包含合并、k_index 重排）
          - done 翻转 → pending 段变 confirmed 或反之

        任一项变化都意味着 BsPointCalculator 需要重新扫描。
        统一抽出来给 xd / bi 两层共用，避免重复代码。
        """
        if not lines:
            return (0, -1, False)
        last_line = lines[-1]
        end_k_idx = -1
        try:
            if last_line.end is not None and last_line.end.k is not None:
                end_k_idx = last_line.end.k.k_index
        except AttributeError:
            end_k_idx = -1
        return (
            len(lines),
            end_k_idx,
            bool(getattr(last_line, 'done', False)),
        )

    def process_mmd(self):
        """
        计算三类买卖点（1buy/1sell, 2buy/2sell, 3buy/3sell）。

        ★ F1 接入：本轮把笔层（``zs_type='bi'``）也接入 ``BsPointCalculator``，
        与线段层（``zs_type='xd'``）完全对称。两层共用同一个识别引擎，
        只是输入不同：
          - xd 层：xds + xd 中枢
          - bi 层：bis + bi 中枢

        识别能力（双层一致）：
          - ``_detect_1buy_1sell`` ✅ 趋势背驰（复用 ``self.beichi_qs``）
          - ``_detect_2buy_2sell`` ✅ 反抽不破前低/高 + 盘整背驰
          - ``_detect_3buy_3sell`` ✅ 中枢离开后反抽不回中枢区间

        前置依赖（E3 已落地）：
          - ``self.bi_zss_calculator`` 已在 ``process_klines`` 中自动算笔中枢
          - ``self.get_bi_zss()`` 与 ``self.get_xd_zss()`` 完全对称
            （都包含 pending_zs，不会丢末段中枢上的 1buy/1sell）

        失败隔离：
          xd 层 / bi 层任一层抛异常，由 ``process_klines`` 的外层 except 清理；
          内部不做单独的 try 包裹，让异常上抛能完整命中清理路径。
        """
        from chanlun.core.bs_point_calculator import BsPointCalculator

        # --- 线段层（已稳定，原 D2 实现） ---
        xds = self.xd_calculator.xds
        # ★ 修复：必须包含 pending_zs（最后一个未完成中枢），否则末段 1buy/1sell 永远丢失
        # zss_calculator.zss 只包含已完成中枢，但趋势背驰常常发生在最后一个中枢未完成时。
        xd_zss = self.get_xd_zss()
        if xds and xd_zss:
            BsPointCalculator(self, zs_type='xd').calculate(xds, xd_zss)

        # --- 笔层（F1 新增） ---
        # 笔层中短线信号：分钟级 1B/2B/3B 给短线交易者使用。
        # 与 xd 层完全对称地调用，结果挂在 BI.zs_type_mmds['bi'] 上，
        # 不会与 XD.zs_type_mmds['xd'] 冲突（两套 dict 是 LINE 实例属性）。
        bis = self.bi_calculator.bis
        bi_zss = self.get_bi_zss()
        if bis and bi_zss:
            BsPointCalculator(self, zs_type='bi').calculate(bis, bi_zss)

        return self