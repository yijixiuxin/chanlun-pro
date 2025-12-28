# -*- coding: utf-8 -*-
import copy
import datetime
from typing import Dict, Union, List, Tuple, Any
import pandas as pd

from chanlun.core.bi_calculator import BiCalculator
from chanlun.core.calculate_zss import create_xd_zs
from chanlun.core.cl_interface import ICL, Kline, CLKline, FX, BI, XD, ZS, Config, LINE, compare_ld_beichi
from chanlun.core.cl_kline_process import CL_Kline_Process
from chanlun.core.kline_data_processor import KlineDataProcessor
from chanlun.core.macd import MACD
from chanlun.core.xd_calculator import XdCalculator
from chanlun.core.zs_calculator import ZsCalculator, ChanlunStructureAnalyzer
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
        self.bi_calculator = BiCalculator()
        # 实例化线段计算器
        self.xd_calculator = XdCalculator(self.config)

        self.zss_calculator = ZsCalculator()

        self.chanlun_structure_analyzer = ChanlunStructureAnalyzer()

        # 存储各级别数据
        # self.zsds: List[XD] = []  # 走势段列表
        # self.qsds: List[XD] = []  # 趋势段列表
        # 中枢数据
        self.bi_zss: Dict[str, List[ZS]] = {}  # 笔中枢
        self.zsd_zss: List[ZS] = []  # 走势段中枢
        self.qsd_zss: List[ZS] = []  # 趋势段中枢

        # 最后中枢缓存
        self._last_bi_zs: Union[ZS, None] = None
        self._last_xd_zs: Union[ZS, None] = None

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
        """
        # 返回增量更新或新增的K线数据列表
        src_klines: List[Kline] = self.kline_processor.process_kline(klines)
        if not src_klines:
            LogUtil.info("没有新的源K线需要处理。")
            return

        LogUtil.info(f"为 {self.code}@{self.frequency} 处理 {len(src_klines)} 根新增/更新的K线")
        # 使用MACD计算器更新指标
        self.macd_calculator.process_macd(self.get_src_klines())

        # 更新缠论K线
        self.cl_kline_processor.process_cl_klines(self.get_klines())
        
        # 获取全量缠论K线，确保计算器能访问完整历史
        # 计算笔和分型
        # 传入全量列表，BiCalculator 内部会根据状态进行增量计算
        LogUtil.info(f"Step 2: 计算笔 (Bi)...")
        self.bi_calculator.calculate(self.get_cl_klines())
        bis = self.get_bis()

        # 计算线段
        # 传入全量列表，XdCalculator 内部会根据状态进行增量计算
        LogUtil.info(f"Step 3: 计算线段 (Xd)...")
        self.xd_calculator.calculate(bis)
        xds = self.get_xds()
        msg = f"Step 3 Done: 线段列表总数: {len(xds)}"
        if xds:
             msg += f", 最后线段: {xds[-1].index} (Done={xds[-1].done})"
        LogUtil.info(msg)

        # 计算中枢
        # 目前 ZsCalculator 为全量计算
        LogUtil.info(f"Step 4: 计算中枢 (Zs)...")
        zss = self.zss_calculator.calculate(xds)
        msg = f"Step 4 Done: 中枢列表总数: {len(zss)}"
        if zss:
            msg += f", 最后中枢: {zss[-1].index} (Done={zss[-1].done})"
        LogUtil.info(msg)

        # results = self.chanlun_structure_analyzer.calculate(xds)
        # 计算买卖点和背驰
        # calculate_line_signals(self, self.xds, self.xd_zss)
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
        """返回原始K线列表"""
        return copy.deepcopy(self.kline_processor.klines)

    def get_klines(self) -> List[Any]:
        """返回K线列表"""
        if self.config.get('kline_type') == Config.KLINE_TYPE_CHANLUN.value:
            return self.get_cl_klines()
        else:
            return self.get_src_klines()

    def get_cl_klines(self) -> List[CLKline]:
        """返回缠论K线列表"""
        return copy.deepcopy(self.cl_kline_processor.cl_klines)

    def get_idx(self) -> dict:
        """返回技术指标数据"""
        # 从MACD计算器获取结果
        return self.macd_calculator.get_results()

    def get_fxs(self) -> List[FX]:
        """返回分型列表"""
        return copy.deepcopy(self.bi_calculator.fxs)

    def get_bis(self) -> List[BI]:
        """返回笔列表"""
        return copy.deepcopy(self.bi_calculator.bis)

    def get_xds(self) -> List[XD]:
        """返回线段列表"""
        return copy.deepcopy(self.xd_calculator.xds)

    def get_zsds(self) -> List[XD]:
        """返回走势段列表"""
        return []

    def get_qsds(self) -> List[XD]:
        """返回趋势段列表"""
        return []

    def get_bi_zss(self, zs_type: str = None) -> List[ZS]:
        """返回笔中枢列表"""
        if zs_type is None:
            zs_type = self.config.get('zs_type_bi', Config.ZS_TYPE_DN.value)
        return self.bi_zss.get(zs_type, [])

    def get_xd_zss(self, zs_type: str = None) -> List[ZS]:
        """返回线段中枢字典"""
        zss = copy.deepcopy(self.zss_calculator.zss)
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
        """返回最后的笔中枢"""
        if not self.config.get('cal_last_zs', True):
            return None

        if self._last_bi_zs is None and len(self.bis) >= 3:
            # 基于最后几笔计算中枢
            last_bis = self.bis[-5:] if len(self.bis) >= 5 else self.bis
            zss = self.create_dn_zs('bi', last_bis)
            self._last_bi_zs = zss[-1] if zss else None

        return self._last_bi_zs

    def get_last_xd_zs(self) -> Union[ZS, None]:
        """返回最后的线段中枢"""
        if not self.config.get('cal_last_zs', True):
            return None

        if self._last_xd_zs is None and len(self.xds) >= 3:
            # 基于最后几个线段计算中枢
            last_xds = self.xds[-5:] if len(self.xds) >= 5 else self.xds
            zss = self.create_dn_zs('xd', last_xds)
            self._last_xd_zs = zss[-1] if zss else None

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
        compare_lines = []
        for line in lines:
            if (line.type == now_line.type and
                    line.end.k.k_index <= prev_zs.start.k.k_index):
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
        return create_xd_zs(zs_type, lines)

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

    def process_idx(self):
        self.macd_calculator.process_macd(self.get_src_klines())
        return self

    def process_fx(self):
        # 通过计算器生成 fxs
        self.bi_calculator.calculate(self.get_cl_klines())
        return self

    def process_bi(self):
        # 通过计算器生成 bis
        self.bi_calculator.calculate(self.get_cl_klines())
        return self

    def process_up_line(self):
        # 通过计算器生成 xds
        self.xd_calculator.calculate(self.get_bis())
        return self

    def process_zs(self):
        # 通过计算器生成中枢
        self.zss_calculator.calculate(self.get_xds())
        return self

    def process_mmd(self):
        # 占位：买卖点计算在各线对象中维护
        return self