# -*- coding: utf-8 -*-
"""
缠论分析核心实现 (重构版)
该文件包含缠论分析的主类 CL，负责协调整个分析流程。
它导入并调用其他模块来执行具体的计算任务，如分型识别、笔计算、中枢构建等。
"""

import datetime
from typing import Dict, Union, List, Tuple
import pandas as pd
import logging

from chanlun.core.calculate_bis import calculate_bis
from chanlun.core.calculate_indicators import calculate_indicators
from chanlun.core.calculate_line_signals import calculate_line_signals
from chanlun.core.calculate_trends import calculate_trends
from chanlun.core.calculate_xds import calculate_xds
from chanlun.core.calculate_zss import calculate_zss, create_dn_zs
from chanlun.core.cl_interface import ICL, Kline, CLKline, FX, BI, XD, ZS, Config, LINE, compare_ld_beichi
from chanlun.core.identify_fractals import identify_fractals

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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

        # 存储各级别数据
        self.src_klines: List[Kline] = []  # 原始K线
        self.cl_klines: List[CLKline] = []  # 缠论K线、包含关系处理后的K线
        self.idx: Dict = {'macd': {'dif': [], 'dea': [], 'hist': []}}  # 技术指标
        self.fxs: List[FX] = []  # 分型列表
        self.bis: List[BI] = []  # 笔列表
        self.xds: List[XD] = []  # 线段列表
        self.zsds: List[XD] = []  # 走势段列表
        self.qsds: List[XD] = []  # 趋势段列表
        # 中枢数据
        self.bi_zss: Dict[str, List[ZS]] = {}  # 笔中枢
        self.xd_zss: Dict[str, List[ZS]] = {}  # 线段中枢
        self.zsd_zss: List[ZS] = []  # 走势段中枢
        self.qsd_zss: List[ZS] = []  # 趋势段中枢

        # 最后中枢缓存
        self._last_bi_zs: Union[ZS, None] = None
        self._last_xd_zs: Union[ZS, None] = None

        # 处理状态标记
        self._last_kline_index = -1

    def _init_default_config(self):
        """初始化默认配置参数"""
        default_config = {
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
            'zs_type_bi': Config.ZS_TYPE_DN.value,
            'zs_type_xd': Config.ZS_TYPE_DN.value,
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
        支持增量更新

        Args:
            klines: K线数据DataFrame，需包含date,high,low,open,close,volume列

        Returns:
            self: 返回自身以支持链式调用
        """
        if klines is None or len(klines) == 0:
            return self

        # 数据预处理
        klines = self._preprocess_klines(klines)

        # 转换为内部格式
        new_klines = self._convert_to_klines(klines)

        # 增量处理：只处理新增的K线
        if len(new_klines) == 0:
            return self

        # 更新原始K线数据
        self._update_src_klines(new_klines)

        # 处理缠论K线（包含处理）
        self._process_cl_klines()

        # --- 步骤 2: 调用独立模块进行计算 ---
        # 计算技术指标
        self.idx = calculate_indicators(self.cl_klines)
        # 识别分型
        self.fxs = identify_fractals(self.cl_klines)
        # 计算笔
        self.bis = calculate_bis(self.fxs)
        # 计算线段
        self.xds = calculate_xds(self.bis, self.config)
        # 计算走势段和趋势段
        self.zsds, self.qsds = calculate_trends(self.xds)
        # 计算中枢
        self.bi_zss = calculate_zss(self.bis, self.config)
        self.xd_zss = calculate_zss(self.xds, self.config)
        # 计算买卖点和背驰
        calculate_line_signals(self, self.bis, self.bi_zss)
        calculate_line_signals(self, self.xds, self.xd_zss)

        return self

    def _preprocess_klines(self, klines: pd.DataFrame) -> pd.DataFrame:
        """预处理K线数据"""
        klines = klines.copy()

        # 确保date列是datetime类型
        if 'date' in klines.columns and not pd.api.types.is_datetime64_any_dtype(klines['date']):
            klines['date'] = pd.to_datetime(klines['date'])

        # 确保数值列是float类型
        numeric_cols = ['high', 'low', 'open', 'close', 'volume']
        for col in numeric_cols:
            if col in klines.columns:
                klines[col] = pd.to_numeric(klines[col], errors='coerce')

        # 排序
        klines = klines.sort_values('date').reset_index(drop=True)

        # 过滤开始时间
        if self.start_datetime:
            klines = klines[klines['date'] >= self.start_datetime]

        return klines

    def _convert_to_klines(self, df: pd.DataFrame) -> List[Kline]:
        """将DataFrame转换为Kline对象列表"""
        klines = []
        start_index = len(self.src_klines)

        for i, row in df.iterrows():
            kline = Kline(
                index=start_index + i,
                date=row['date'],
                h=float(row['high']),
                l=float(row['low']),
                o=float(row['open']),
                c=float(row['close']),
                a=float(row['volume']) if 'volume' in row else 0.0
            )
            klines.append(kline)

        return klines

    def _update_src_klines(self, new_klines: List[Kline]):
        """更新原始K线数据"""
        if len(new_klines) == 0:
            return

        # 如果有重叠，更新最后一根K线，添加新的K线
        if len(self.src_klines) > 0:
            # 检查是否有重叠
            last_date = self.src_klines[-1].date
            for i, kline in enumerate(new_klines):
                if kline.date == last_date:
                    # 更新最后一根K线
                    self.src_klines[-1] = kline
                    # 添加后续新K线
                    self.src_klines.extend(new_klines[i + 1:])
                    break
                elif kline.date > last_date:
                    # 添加所有新K线
                    self.src_klines.extend(new_klines[i:])
                    break
        else:
            # 第一次处理
            self.src_klines = new_klines

    def _process_cl_klines(self):
        """
        遍历原始K线，进行包含关系处理。
        Iterate through the original K-lines to handle inclusion relationships.
        """
        if len(self.src_klines) == 0:
            return

        # 逐根处理原始K线
        for i in range(len(self.src_klines)):
            current_k = self.src_klines[i]

            # 将原始K线包装成临时的缠论K线对象
            cl_k = CLKline(
                k_index=current_k.index, date=current_k.date, h=current_k.h, l=current_k.l,
                o=current_k.o, c=current_k.c, a=current_k.a, klines=[current_k],
                index=len(self.cl_klines), _n=1
            )

            # 如果是第一根K线，直接放入结果列表
            if not self.cl_klines:
                self.cl_klines.append(cl_k)
                continue

            last_cl_k = self.cl_klines[-1]

            # 检查是否有缺口，有缺口则不进行包含处理
            has_gap = cl_k.l > last_cl_k.h or cl_k.h < last_cl_k.l
            if has_gap:
                cl_k.q = True
                self.cl_klines.append(cl_k)
                continue

            # 判断是否需要与前一根处理过的K线合并
            if self._need_merge(last_cl_k, cl_k):
                # 1. 确定合并方向 (1. Determine the merge direction)
                direction = 'up'  # 默认向上 (Default to 'up')
                if last_cl_k.up_qs is not None:
                    # 如果上一根合并K线已经有方向，则继承该方向
                    direction = last_cl_k.up_qs
                elif len(self.cl_klines) >= 2:
                    # 标准情况：比较最后两根已处理K线的高点来定方向
                    prev_prev_k = self.cl_klines[-2]
                    if last_cl_k.h < prev_prev_k.h:
                        direction = 'down'
                else:
                    # 边缘情况：第一次发生包含关系，根据两根K线的高低点关系来确定初始方向
                    # 规则：比较高点，高点相同则比较低点。这反映了价格的最新“努力”方向。
                    if cl_k.h > last_cl_k.h:
                        direction = 'up'
                    elif cl_k.h < last_cl_k.h:
                        direction = 'down'
                    else:  # 高点相同
                        if cl_k.l > last_cl_k.l:
                            direction = 'up'
                        else:  # 低点相同或更低
                            direction = 'down'

                # 2. 调用合并函数，并传入确定的方向
                merged_k = self._merge_klines(last_cl_k, cl_k, direction)
                self.cl_klines[-1] = merged_k  # 替换掉最后一根K线
            else:
                # 如果没有包含关系，直接添加新K线
                self.cl_klines.append(cl_k)

    def _need_merge(self, k1: CLKline, k2: CLKline) -> bool:
        """
        判断两根缠论K线是否存在包含关系
        (Determine if an inclusion relationship exists between two Chanlun K-lines)
        """
        k1_contains_k2 = k1.h >= k2.h and k1.l <= k2.l
        k2_contains_k1 = k2.h >= k1.h and k2.l <= k1.l
        return k1_contains_k2 or k2_contains_k1

    def _merge_klines(self, k1: CLKline, k2: CLKline, direction: str) -> CLKline:
        if direction == 'up':
            h, l = max(k1.h, k2.h), max(k1.l, k2.l)
            date, k_index = (k1.date, k1.k_index) if k1.h > k2.h else (k2.date, k2.k_index)
        else:
            h, l = min(k1.h, k2.h), min(k1.l, k2.l)
            date, k_index = (k1.date, k1.k_index) if k1.l < k2.l else (k2.date, k2.k_index)
        merged = CLKline(
            k_index=k_index, date=date, h=h, l=l, o=h, c=l, a=k1.a + k2.a,
            klines=k1.klines + k2.klines, index=k1.index, _n=k1.n + k2.n, _q=k1.q
        )
        merged.up_qs = direction
        return merged

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
        return self.src_klines

    def get_klines(self) -> List[Kline]:
        """返回K线列表"""
        if self.config.get('kline_type') == Config.KLINE_TYPE_CHANLUN.value:
            # 返回缠论K线对应的原始K线
            result = []
            for cl_k in self.cl_klines:
                result.append(cl_k)
            return result
        else:
            return self.src_klines

    def get_cl_klines(self) -> List[CLKline]:
        """返回缠论K线列表"""
        return self.cl_klines

    def get_idx(self) -> dict:
        """返回技术指标数据"""
        return self.idx

    def get_fxs(self) -> List[FX]:
        """返回分型列表"""
        return self.fxs

    def get_bis(self) -> List[BI]:
        """返回笔列表"""
        return self.bis

    def get_xds(self) -> List[XD]:
        """返回线段列表"""
        return self.xds

    def get_zsds(self) -> List[XD]:
        """返回走势段列表"""
        return self.zsds

    def get_qsds(self) -> List[XD]:
        """返回趋势段列表"""
        return self.qsds

    def get_bi_zss(self, zs_type: str = None) -> List[ZS]:
        """返回笔中枢列表"""
        if zs_type is None:
            zs_type = self.config.get('zs_type_bi', Config.ZS_TYPE_DN.value)
        return self.bi_zss.get(zs_type, [])

    def get_xd_zss(self, zs_type: str = None) -> List[ZS]:
        """返回线段中枢列表"""
        if zs_type is None:
            zs_type = self.config.get('zs_type_xd', Config.ZS_TYPE_DN.value)
        return self.xd_zss.get(zs_type, [])

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
        create_dn_zs(zs_type, lines)