# -*- coding: utf-8 -*-
"""
缠论分析核心实现
基于缠论技术分析理论的量化分析实现

主要功能：
1. K线数据处理和合并
2. 分型识别和标记
3. 笔、线段、走势段的计算
4. 中枢识别和分析
5. 买卖点标记
6. 背驰分析

作者：重构版本
版本：1.0.0
"""

import datetime
import copy
from typing import Dict, List, Union, Tuple
import pandas as pd
import numpy as np
from talib import abstract

from chanlun.cl_interface import (
    ICL, Kline, CLKline, FX, BI, XD, ZS, MMD, BC, TZXL, XLFX,
    Config, LINE, MACD_INFOS,
    query_macd_ld, compare_ld_beichi, user_custom_mmd
)


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
        self.klines: List[Kline] = []  # 处理后的K线
        self.cl_klines: List[CLKline] = []  # 缠论K线
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
            'kline_type': Config.KLINE_TYPE_CHANLUN.value,
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

            # 其他配置
            'cal_last_zs': True,  # 是否计算最后中枢
            'use_macd_ld': True,  # 是否使用MACD力度
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

        # 计算技术指标
        self._calculate_indicators()

        # 识别分型
        self._identify_fractals()

        # 计算笔
        self._calculate_bis()

        # 计算线段
        self._calculate_xds()

        # 计算走势段和趋势段
        self._calculate_zsds_and_qsds()

        # 计算中枢
        self._calculate_zss()

        # 计算买卖点和背驰
        self._calculate_mmds_and_bcs()

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
        """处理缠论K线（包含处理）"""
        if len(self.src_klines) < 2:
            return

        # 重新计算缠论K线
        self.cl_klines = []

        i = 0
        while i < len(self.src_klines):
            current_k = self.src_klines[i]

            # 创建缠论K线
            cl_k = CLKline(
                k_index=current_k.index,
                date=current_k.date,
                h=current_k.h,
                l=current_k.l,
                o=current_k.o,
                c=current_k.c,
                a=current_k.a,
                klines=[current_k],
                index=len(self.cl_klines),
                _n=1
            )

            # 包含处理
            if len(self.cl_klines) > 0:
                last_cl_k = self.cl_klines[-1]

                # 判断是否需要合并
                if self._need_merge(last_cl_k, cl_k):
                    # 合并K线
                    merged_k = self._merge_klines(last_cl_k, cl_k)
                    self.cl_klines[-1] = merged_k
                    i += 1
                    continue

            self.cl_klines.append(cl_k)
            i += 1

    def _need_merge(self, k1: CLKline, k2: CLKline) -> bool:
        """判断两根缠论K线是否需要合并"""
        # 包含关系判断
        k1_contains_k2 = k1.h >= k2.h and k1.l <= k2.l
        k2_contains_k1 = k2.h >= k1.h and k2.l <= k1.l

        return k1_contains_k2 or k2_contains_k1

    def _merge_klines(self, k1: CLKline, k2: CLKline) -> CLKline:
        """合并两根缠论K线"""
        # 根据趋势方向合并
        if k1.up_qs == 'up' or (k1.up_qs is None and k2.c > k2.o):
            # 向上趋势，取高高低高
            h = max(k1.h, k2.h)
            l = max(k1.l, k2.l)
        else:
            # 向下趋势，取高低低低
            h = min(k1.h, k2.h)
            l = min(k1.l, k2.l)

        merged = CLKline(
            k_index=k2.k_index,
            date=k2.date,
            h=h,
            l=l,
            o=k1.o,
            c=k2.c,
            a=k1.a + k2.a,
            klines=k1.klines + k2.klines,
            index=k1.index,
            _n=k1.n + 1
        )

        # 设置趋势
        if k1.up_qs:
            merged.up_qs = k1.up_qs
        elif k2.c > k1.o:
            merged.up_qs = 'up'
        else:
            merged.up_qs = 'down'

        return merged

    def _calculate_indicators(self):
        """计算技术指标"""
        if len(self.cl_klines) < 26:
            return

        # 提取价格数据
        close_prices = np.array([k.c for k in self.cl_klines])

        # 计算MACD
        macd_result = abstract.MACD(close_prices)

        self.idx['macd'] = {
            'dif': macd_result[0].tolist(),
            'dea': macd_result[1].tolist(),
            'hist': macd_result[2].tolist()
        }

    def _identify_fractals(self):
        """识别分型"""
        if len(self.cl_klines) < 3:
            return

        self.fxs = []

        for i in range(1, len(self.cl_klines) - 1):
            prev_k = self.cl_klines[i - 1]
            curr_k = self.cl_klines[i]
            next_k = self.cl_klines[i + 1]

            # 顶分型判断
            if (curr_k.h > prev_k.h and curr_k.h > next_k.h and
                    curr_k.l > prev_k.l and curr_k.l > next_k.l):

                fx = FX(
                    _type='ding',
                    k=curr_k,
                    klines=[prev_k, curr_k, next_k],
                    val=curr_k.h,
                    index=len(self.fxs),
                    done=True
                )
                self.fxs.append(fx)

            # 底分型判断
            elif (curr_k.h < prev_k.h and curr_k.h < next_k.h and
                  curr_k.l < prev_k.l and curr_k.l < next_k.l):

                fx = FX(
                    _type='di',
                    k=curr_k,
                    klines=[prev_k, curr_k, next_k],
                    val=curr_k.l,
                    index=len(self.fxs),
                    done=True
                )
                self.fxs.append(fx)

    def _calculate_bis(self):
        """计算笔"""
        if len(self.fxs) < 2:
            return

        self.bis = []

        # 处理相邻分型成笔
        for i in range(len(self.fxs) - 1):
            start_fx = self.fxs[i]
            end_fx = self.fxs[i + 1]

            # 不同类型的分型才能成笔
            if start_fx.type == end_fx.type:
                continue

            # 判断笔的方向
            bi_type = 'up' if start_fx.type == 'di' else 'down'

            bi = BI(
                start=start_fx,
                end=end_fx,
                _type=bi_type,
                index=len(self.bis),
                default_zs_type=self.config.get('zs_type_bi')
            )

            # 设置笔的高低点
            if bi_type == 'up':
                bi.high = end_fx.val
                bi.low = start_fx.val
            else:
                bi.high = start_fx.val
                bi.low = end_fx.val

            # 设置中枢相关的高低点
            bi.zs_high = bi.high
            bi.zs_low = bi.low

            self.bis.append(bi)

    def _calculate_xds(self):
        """计算线段"""
        if len(self.bis) < 3:
            return

        self.xds = []

        # 使用特征序列方法计算线段
        tzxls = self._calculate_tzxls()
        xlfxs = self._calculate_xlfxs(tzxls)

        # 根据序列分型构造线段
        for i in range(len(xlfxs) - 1):
            start_xlfx = xlfxs[i]
            end_xlfx = xlfxs[i + 1]

            if start_xlfx.type == end_xlfx.type:
                continue

            # 确定线段方向
            xd_type = 'up' if start_xlfx.type == 'di' else 'down'

            # 找到对应的笔
            start_line = self._find_line_by_xlfx(start_xlfx)
            end_line = self._find_line_by_xlfx(end_xlfx)

            if not start_line or not end_line:
                continue

            xd = XD(
                start=start_line.start if xd_type == 'up' else start_line.end,
                end=end_line.end if xd_type == 'up' else end_line.start,
                start_line=start_line,
                end_line=end_line,
                _type=xd_type,
                ding_fx=end_xlfx if xd_type == 'up' else start_xlfx,
                di_fx=start_xlfx if xd_type == 'up' else end_xlfx,
                index=len(self.xds),
                default_zs_type=self.config.get('zs_type_xd')
            )

            # 设置线段高低点
            if xd_type == 'up':
                xd.high = end_line.high
                xd.low = start_line.low
            else:
                xd.high = start_line.high
                xd.low = end_line.low

            xd.zs_high = xd.high
            xd.zs_low = xd.low
            xd.done = True

            self.xds.append(xd)

    def _calculate_tzxls(self) -> List[TZXL]:
        """计算特征序列"""
        if len(self.bis) < 3:
            return []

        tzxls = []

        # 处理包含关系，形成特征序列
        i = 0
        while i < len(self.bis) - 1:
            current_bi = self.bis[i]
            next_bi = self.bis[i + 1]

            # 同向笔处理包含关系
            if current_bi.type == next_bi.type:
                if current_bi.type == 'up':
                    bh_direction = 'up'
                else:
                    bh_direction = 'down'

                tzxl = TZXL(
                    bh_direction=bh_direction,
                    line=next_bi,
                    pre_line=current_bi,
                    line_bad=False,
                    done=True
                )

                # 处理包含
                if bh_direction == 'up':
                    tzxl.lines = [current_bi, next_bi]
                else:
                    tzxl.lines = [current_bi, next_bi]

                tzxl.update_maxmin()
                tzxls.append(tzxl)
                i += 2
            else:
                i += 1

        return tzxls

    def _calculate_xlfxs(self, tzxls: List[TZXL]) -> List[XLFX]:
        """计算序列分型"""
        if len(tzxls) < 3:
            return []

        xlfxs = []

        for i in range(1, len(tzxls) - 1):
            prev_xl = tzxls[i - 1]
            curr_xl = tzxls[i]
            next_xl = tzxls[i + 1]

            # 顶分型
            if (curr_xl.max > prev_xl.max and curr_xl.max > next_xl.max):
                xlfx = XLFX(
                    _type='ding',
                    xl=curr_xl,
                    xls=[prev_xl, curr_xl, next_xl],
                    done=True
                )
                xlfxs.append(xlfx)

            # 底分型
            elif (curr_xl.min < prev_xl.min and curr_xl.min < next_xl.min):
                xlfx = XLFX(
                    _type='di',
                    xl=curr_xl,
                    xls=[prev_xl, curr_xl, next_xl],
                    done=True
                )
                xlfxs.append(xlfx)

        return xlfxs

    def _find_line_by_xlfx(self, xlfx: XLFX) -> Union[BI, None]:
        """根据序列分型找到对应的笔"""
        if not xlfx.xl.lines:
            return None

        # 返回特征序列中最相关的笔
        if xlfx.type == 'ding':
            return max(xlfx.xl.lines, key=lambda x: x.high)
        else:
            return min(xlfx.xl.lines, key=lambda x: x.low)

    def _calculate_zsds_and_qsds(self):
        """计算走势段和趋势段"""
        # 暂时简化：走势段等同于线段
        self.zsds = self.xds.copy()
        self.qsds = self.xds.copy()

    def _calculate_zss(self):
        """计算中枢"""
        # 计算笔中枢
        self._calculate_bi_zss()

        # 计算线段中枢
        self._calculate_xd_zss()

        # 计算走势段和趋势段中枢
        self.zsd_zss = []
        self.qsd_zss = []

    def _calculate_bi_zss(self):
        """计算笔中枢"""
        zs_type = self.config.get('zs_type_bi', Config.ZS_TYPE_DN.value)

        if zs_type not in self.bi_zss:
            self.bi_zss[zs_type] = []

        # 使用段内中枢方法
        if len(self.bis) >= 3:
            zss = self.create_dn_zs('bi', self.bis)
            self.bi_zss[zs_type] = zss

    def _calculate_xd_zss(self):
        """计算线段中枢"""
        zs_type = self.config.get('zs_type_xd', Config.ZS_TYPE_DN.value)

        if zs_type not in self.xd_zss:
            self.xd_zss[zs_type] = []

        # 使用段内中枢方法
        if len(self.xds) >= 3:
            zss = self.create_dn_zs('xd', self.xds)
            self.xd_zss[zs_type] = zss

    def _calculate_mmds_and_bcs(self):
        """计算买卖点和背驰"""
        # 计算笔的买卖点和背驰
        for bi in self.bis:
            self._calculate_bi_mmd_bc(bi)

        # 计算线段的买卖点和背驰
        for xd in self.xds:
            self._calculate_xd_mmd_bc(xd)

    def _calculate_bi_mmd_bc(self, bi: BI):
        """计算笔的买卖点和背驰"""
        if len(self.bi_zss) == 0:
            return

        # 获取相关中枢
        for zs_type, zss in self.bi_zss.items():
            if len(zss) == 0:
                continue

            # 找到与当前笔相关的中枢
            relevant_zs = None
            for zs in reversed(zss):
                if bi.index >= zs.lines[0].index:
                    relevant_zs = zs
                    break

            if not relevant_zs:
                continue

            # 判断买卖点类型
            self._identify_mmd(bi, relevant_zs, zs_type)

            # 判断背驰
            self._identify_bc(bi, relevant_zs, zs_type)

    def _calculate_xd_mmd_bc(self, xd: XD):
        """计算线段的买卖点和背驰"""
        if len(self.xd_zss) == 0:
            return

        # 获取相关中枢
        for zs_type, zss in self.xd_zss.items():
            if len(zss) == 0:
                continue

            # 找到与当前线段相关的中枢
            relevant_zs = None
            for zs in reversed(zss):
                if xd.index >= zs.lines[0].index:
                    relevant_zs = zs
                    break

            if not relevant_zs:
                continue

            # 判断买卖点类型
            self._identify_mmd(xd, relevant_zs, zs_type)

            # 判断背驰
            self._identify_bc(xd, relevant_zs, zs_type)

    def _identify_mmd(self, line: Union[BI, XD], zs: ZS, zs_type: str):
        """识别买卖点"""
        if line.type == 'down':
            # 下跌线段，可能的买点
            if line.low < zs.zd:
                # 跌破中枢，三类买点
                line.add_mmd('3buy', zs, zs_type, '跌破中枢下沿')
            elif line.low > zs.zg:
                # 回调不跌破中枢上沿，一类买点
                line.add_mmd('1buy', zs, zs_type, '回调不破中枢上沿')
            else:
                # 在中枢内部，二类买点
                line.add_mmd('2buy', zs, zs_type, '中枢内部买点')

        elif line.type == 'up':
            # 上涨线段，可能的卖点
            if line.high > zs.zg:
                # 突破中枢，三类卖点
                line.add_mmd('3sell', zs, zs_type, '突破中枢上沿')
            elif line.high < zs.zd:
                # 反弹不破中枢下沿，一类卖点
                line.add_mmd('1sell', zs, zs_type, '反弹不破中枢下沿')
            else:
                # 在中枢内部，二类卖点
                line.add_mmd('2sell', zs, zs_type, '中枢内部卖点')

    def _identify_bc(self, line: Union[BI, XD], zs: ZS, zs_type: str):
        """识别背驰"""
        if len(zs.lines) < 2:
            return

        # 找到比较的线段
        compare_line = None
        for zs_line in reversed(zs.lines[:-1]):
            if zs_line.type == line.type:
                compare_line = zs_line
                break

        if not compare_line:
            return

        # 力度比较
        line_ld = line.get_ld(self)
        compare_ld = compare_line.get_ld(self)

        is_bc = compare_ld_beichi(compare_ld, line_ld, line.type)

        if is_bc:
            line.add_bc('bi' if isinstance(line, BI) else 'xd',
                        zs, compare_line, [compare_line], True, zs_type)

    # ICL接口实现方法

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
                result.extend(cl_k.klines)
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

    def create_dn_zs(
            self,
            zs_type: str,
            lines: List[LINE],
            max_line_num: int = 999,
            zs_include_last_line: bool = True,
    ) -> List[ZS]:
        """
        创建段内中枢

        Args:
            zs_type: 中枢类型 ('bi' 笔中枢, 'xd' 线段中枢)
            lines: 线的列表
            max_line_num: 中枢最大线段数量
            zs_include_last_line: 中枢是否包含最后一线

        Returns:
            中枢列表
        """
        if len(lines) < 3:
            return []

        zss = []
        i = 0

        while i <= len(lines) - 3:
            # 寻找可能的中枢起始位置
            zs_lines = []
            j = i

            # 至少需要3段来形成中枢
            while j < len(lines) and len(zs_lines) < max_line_num:
                zs_lines.append(lines[j])

                # 检查是否能形成中枢
                if len(zs_lines) >= 3 and self._can_form_zs(zs_lines):
                    # 创建中枢
                    zs = self._create_zs_from_lines(zs_type, zs_lines)
                    if zs:
                        zss.append(zs)
                        i = j + 1
                        break

                j += 1
            else:
                i += 1

        return zss

    def _can_form_zs(self, lines: List[LINE]) -> bool:
        """判断线段列表是否能形成中枢"""
        if len(lines) < 3:
            return False

        # 检查前三段是否有重叠
        overlaps = []
        for i in range(len(lines) - 1):
            line1 = lines[i]
            line2 = lines[i + 1]

            # 计算重叠区间
            overlap_high = min(line1.zs_high, line2.zs_high)
            overlap_low = max(line1.zs_low, line2.zs_low)

            if overlap_high > overlap_low:
                overlaps.append((overlap_low, overlap_high))
            else:
                return False

        # 检查是否有公共重叠区间
        if len(overlaps) < 2:
            return False

        common_low = max(overlap[0] for overlap in overlaps[:2])
        common_high = min(overlap[1] for overlap in overlaps[:2])

        return common_high > common_low

    def _create_zs_from_lines(self, zs_type: str, lines: List[LINE]) -> Union[ZS, None]:
        """从线段列表创建中枢"""
        if not self._can_form_zs(lines):
            return None

        # 计算中枢区间
        zg = float('-inf')
        zd = float('inf')
        gg = float('-inf')
        dd = float('inf')

        # 前三段确定中枢区间
        first_three = lines[:3]
        for line in first_three:
            zg = max(zg, min(line.zs_high, first_three[0].zs_high))
            zd = min(zd, max(line.zs_low, first_three[0].zs_low))
            gg = max(gg, line.zs_high)
            dd = min(dd, line.zs_low)

        # 重新计算精确的中枢区间
        overlaps = []
        for i in range(len(first_three) - 1):
            line1 = first_three[i]
            line2 = first_three[i + 1]
            overlap_high = min(line1.zs_high, line2.zs_high)
            overlap_low = max(line1.zs_low, line2.zs_low)
            overlaps.append((overlap_low, overlap_high))

        zd = max(overlap[0] for overlap in overlaps)
        zg = min(overlap[1] for overlap in overlaps)

        # 判断中枢类型
        if lines[0].type == 'up' and lines[-1].type == 'down':
            zs_direction = 'down'
        elif lines[0].type == 'down' and lines[-1].type == 'up':
            zs_direction = 'up'
        else:
            zs_direction = 'zd'  # 震荡

        # 创建中枢对象
        zs = ZS(
            zs_type=zs_type,
            start=lines[0].start,
            end=lines[-1].end,
            zg=zg,
            zd=zd,
            gg=gg,
            dd=dd,
            _type=zs_direction,
            index=0,  # 会在后续调整
            line_num=len(lines),
            level=0
        )

        # 添加线段到中枢
        for line in lines:
            zs.add_line(line)

        zs.done = True
        zs.real = True

        return zs

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