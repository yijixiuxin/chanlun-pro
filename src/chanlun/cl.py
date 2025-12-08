# -*- coding: utf-8 -*-
import datetime
import copy
from typing import List, Union, Dict, Tuple

import numpy as np
import pandas as pd
import talib

from chanlun import cl_interface
from chanlun import cl_utils
from chanlun.cl_interface import (
    ICL, Config, Kline, CLKline, FX, BI, XD, ZS, MMD, BC, LINE, TZXL, XLFX,
    query_macd_ld, compare_ld_beichi, user_custom_mmd
)
from chanlun.cl_utils import cal_macd_bis_is_bc


class CL(ICL):
    def __init__(
        self,
        code: str,
        frequency: str,
        config: Union[dict, None] = None,
        start_datetime: datetime.datetime = None,
    ):
        self.code = code
        self.frequency = frequency
        self.config = cl_utils.query_cl_chart_config("common", code)
        if config:
            self.config.update(config)
        self.start_datetime = start_datetime

        self.klines: List[Kline] = []
        self.cl_klines: List[CLKline] = []
        self.idx: Dict[str, Dict] = {"macd": {"dif": [], "dea": [], "hist": []}}

        self.fxs: List[FX] = []
        self.bis: List[BI] = []
        self.xds: List[XD] = []
        self.zsds: List[XD] = []
        self.qsds: List[XD] = []

        self.bi_zss: Dict[str, List[ZS]] = {}
        self.xd_zss: Dict[str, List[ZS]] = {}
        self.zsd_zss: List[ZS] = []
        self.qsd_zss: List[ZS] = []

        self.last_bi_zs: Union[ZS, None] = None
        self.last_xd_zs: Union[ZS, None] = None

    def get_code(self) -> str:
        return self.code

    def get_frequency(self) -> str:
        return self.frequency

    def get_config(self) -> dict:
        return self.config

    def get_src_klines(self) -> List[Kline]:
        return self.klines

    def get_klines(self) -> List[Kline]:
        if self.config["kline_type"] == Config.KLINE_TYPE_CHANLUN.value:
            return self.cl_klines
        return self.klines

    def get_cl_klines(self) -> List[CLKline]:
        return self.cl_klines

    def get_idx(self) -> dict:
        return self.idx

    def get_fxs(self) -> List[FX]:
        return self.fxs

    def get_bis(self) -> List[BI]:
        return self.bis

    def get_xds(self) -> List[XD]:
        return self.xds

    def get_zsds(self) -> List[XD]:
        return self.zsds

    def get_qsds(self) -> List[XD]:
        return self.qsds

    def get_bi_zss(self, zs_type: str = None) -> List[ZS]:
        if zs_type is None:
            zs_type = self.config["zs_bi_type"][0]
        return self.bi_zss.get(zs_type, [])

    def get_xd_zss(self, zs_type: str = None) -> List[ZS]:
        if zs_type is None:
            zs_type = self.config["zs_xd_type"][0]
        return self.xd_zss.get(zs_type, [])

    def get_zsd_zss(self) -> List[ZS]:
        return self.zsd_zss

    def get_qsd_zss(self) -> List[ZS]:
        return self.qsd_zss

    def get_last_bi_zs(self) -> Union[ZS, None]:
        return self.last_bi_zs

    def get_last_xd_zs(self) -> Union[ZS, None]:
        return self.last_xd_zs

    def process_klines(self, klines: pd.DataFrame):
        if len(klines) == 0:
            return

        new_klines = []
        if "date" not in klines.columns:
             if isinstance(klines.index, pd.DatetimeIndex):
                 klines = klines.reset_index()
                 klines.rename(columns={"index": "date"}, inplace=True)

        for _, row in klines.iterrows():
            date_val = row["date"]
            if isinstance(date_val, str):
                date_val = pd.to_datetime(date_val)
            
            if self.start_datetime and date_val < self.start_datetime:
                continue

            # Handle different volume column names
            volume = 0
            if "volume" in row:
                volume = float(row["volume"])
            elif "vol" in row:
                volume = float(row["vol"])
                
            k = Kline(
                index=len(self.klines) + len(new_klines),
                date=date_val,
                h=float(row["high"]),
                l=float(row["low"]),
                o=float(row["open"]),
                c=float(row["close"]),
                a=volume,
            )
            new_klines.append(k)

        if len(new_klines) == 0:
            return

        if len(self.klines) > 0:
            last_k = self.klines[-1]
            nk_dt = new_klines[0].date
            lk_dt = last_k.date
            if isinstance(nk_dt, pd.Timestamp) and isinstance(lk_dt, pd.Timestamp):
                if (nk_dt.tz is not None and lk_dt.tz is None):
                    nk_cmp = nk_dt.tz_localize(None)
                    lk_cmp = lk_dt
                elif (nk_dt.tz is None and lk_dt.tz is not None):
                    nk_cmp = nk_dt
                    lk_cmp = lk_dt.tz_localize(None)
                else:
                    nk_cmp = nk_dt
                    lk_cmp = lk_dt
            else:
                nk_cmp = nk_dt
                lk_cmp = lk_dt
            if nk_cmp == lk_cmp:
                self.klines[-1] = new_klines[0]
                new_klines = new_klines[1:]
            elif nk_cmp < lk_cmp:
                idx = -1
                for i, k in enumerate(self.klines):
                    kd = k.date
                    if isinstance(kd, pd.Timestamp) and isinstance(nk_dt, pd.Timestamp):
                        if (kd.tz is not None and nk_dt.tz is None):
                            kd_cmp = kd.tz_localize(None)
                            nkd_cmp = nk_dt
                        elif (kd.tz is None and nk_dt.tz is not None):
                            kd_cmp = kd
                            nkd_cmp = nk_dt.tz_localize(None)
                        else:
                            kd_cmp = kd
                            nkd_cmp = nk_dt
                    else:
                        kd_cmp = kd
                        nkd_cmp = nk_dt
                    if kd_cmp >= nkd_cmp:
                        idx = i
                        break
                if idx != -1:
                    self.klines = self.klines[:idx]
                    for i, k in enumerate(self.klines):
                        k.index = i
                    for i, k in enumerate(new_klines):
                        k.index = len(self.klines) + i
        
        self.klines.extend(new_klines)
        self._cal_idx()
        self._cal_cl_klines()
        self._cal_fx()
        self._cal_bi()
        self._cal_xd()
        self._cal_zsd()
        self._cal_zs()
        self._cal_mmd_bc()

    def _cal_idx(self):
        close_prices = np.array([k.c for k in self.klines])
        if len(close_prices) < 35:
             self.idx["macd"]["dif"] = [0] * len(close_prices)
             self.idx["macd"]["dea"] = [0] * len(close_prices)
             self.idx["macd"]["hist"] = [0] * len(close_prices)
             return

        dif, dea, hist = talib.MACD(
            close_prices,
            fastperiod=int(self.config["idx_macd_fast"]),
            slowperiod=int(self.config["idx_macd_slow"]),
            signalperiod=int(self.config["idx_macd_signal"]),
        )
        np.nan_to_num(dif, copy=False)
        np.nan_to_num(dea, copy=False)
        np.nan_to_num(hist, copy=False)

        self.idx["macd"]["dif"] = dif
        self.idx["macd"]["dea"] = dea
        self.idx["macd"]["hist"] = hist * 2

    def _cal_cl_klines(self):
        """
        严格按照时间优先级原则处理包含关系
        """
        self.cl_klines = []
        if len(self.klines) == 0:
            return

        k0 = self.klines[0]
        ck0 = CLKline(
            k_index=k0.index,
            date=k0.date,
            h=k0.h,
            l=k0.l,
            o=k0.o,
            c=k0.c,
            a=k0.a,
            klines=[k0],
            index=0,
            _n=1
        )
        self.cl_klines.append(ck0)

        direction = None  # 初始方向未知

        for i in range(1, len(self.klines)):
            k = self.klines[i]
            last_ck = self.cl_klines[-1]

            is_included = False
            if (k.h >= last_ck.h and k.l <= last_ck.l) or (last_ck.h >= k.h and last_ck.l <= k.l):
                is_included = True
            
            if is_included:
                # 缠论时间优先级原则：包含处理方向由前一个非包含K线决定
                if direction is None:
                    # 根据前一根K线的收盘价关系确定方向
                    direction = "up" if k.c > last_ck.c else "down"
                
                new_h = 0
                new_l = 0
                if direction == "up":
                    new_h = max(last_ck.h, k.h)
                    new_l = max(last_ck.l, k.l)  # 向上处理：取高高
                else:
                    new_h = min(last_ck.h, k.h)  # 向下处理：取低低
                    new_l = min(last_ck.l, k.l)
                
                last_ck.h = new_h
                last_ck.l = new_l
                last_ck.k_index = k.index
                last_ck.date = k.date
                last_ck.a += k.a
                last_ck.klines.append(k)
                last_ck.n += 1
            else:
                # 修复：方向判断基于时间优先级
                if k.h > last_ck.h and k.l > last_ck.l:
                    direction = "up"
                elif k.h < last_ck.h and k.l < last_ck.l:
                    direction = "down"
                else:
                    # 非标准情况，根据收盘价决定方向
                    direction = "up" if k.c > last_ck.c else "down"
                
                new_ck = CLKline(
                    k_index=k.index,
                    date=k.date,
                    h=k.h,
                    l=k.l,
                    o=k.o,
                    c=k.c,
                    a=k.a,
                    klines=[k],
                    index=len(self.cl_klines),
                    _n=1
                )
                new_ck.up_qs = direction
                self.cl_klines.append(new_ck)

    def _cal_fx(self):
        """
        严格按照缠论理论识别分型
        """
        self.fxs = []
        if len(self.cl_klines) < 3:
            return

        fx_qj = self.config["fx_qj"]
        fx_bh = self.config["fx_bh"]
        
        # 临时存储所有候选分型
        candidate_fxs = []
        
        # 分型识别应在无包含K线序列上进行
        for i in range(1, len(self.cl_klines) - 1):
            k1 = self.cl_klines[i-1]
            k2 = self.cl_klines[i]
            k3 = self.cl_klines[i+1]
            
            # 检查是否为有效的分型
            fx_type = None
            if k2.h > k1.h and k2.h > k3.h:
                fx_type = "ding"
            elif k2.l < k1.l and k2.l < k3.l:
                fx_type = "di"
            
            if fx_type:
                val = 0
                if fx_type == "ding":
                    if fx_qj == Config.FX_QJ_CK.value:
                        val = k2.h
                    else:
                        val = max([_k.h for _k in k2.klines])
                else:
                    if fx_qj == Config.FX_QJ_CK.value:
                        val = k2.l
                    else:
                        val = min([_k.l for _k in k2.klines])
                
                fx = FX(
                    _type=fx_type,
                    k=k2,
                    klines=[k1, k2, k3],
                    val=val,
                    index=len(candidate_fxs),
                    done=True
                )
                candidate_fxs.append(fx)
        
        # 处理候选分型，确保顶底交替
        if not candidate_fxs:
            return
            
        # 从第一个分型开始
        self.fxs.append(candidate_fxs[0])
        
        for i in range(1, len(candidate_fxs)):
            current_fx = candidate_fxs[i]
            last_fx = self.fxs[-1]
            
            # 检查是否为同类型分型
            if current_fx.type == last_fx.type:
                # 同类型分型，保留更极值的
                if current_fx.type == "ding" and current_fx.val > last_fx.val:
                    self.fxs[-1] = current_fx
                elif current_fx.type == "di" and current_fx.val < last_fx.val:
                    self.fxs[-1] = current_fx
            else:
                # 不同类型分型，直接添加
                self.fxs.append(current_fx)

    def check_bi_valid(self, start_fx: FX, end_fx: FX, bi_type: str) -> bool:
        """
        检查笔的有效性，严格按照缠论理论
        """
        # 顶底必须交替
        if start_fx.type == end_fx.type:
            return False

        # 方向必须满足：顶->低 新低；底->顶 新高
        if start_fx.type == "ding" and end_fx.val >= start_fx.val:
            return False
        if start_fx.type == "di" and end_fx.val <= start_fx.val:
            return False

        # 缠论K线中心索引差值（用于判断是否存在独立缠论K线）
        ck_diff = end_fx.k.index - start_fx.k.index

        # 原始K线数量（用于新笔/简单笔判断）
        src_k_num = 0
        for ck in self.cl_klines[start_fx.k.index : end_fx.k.index + 1]:
            src_k_num += len(ck.klines)

        if bi_type == Config.BI_TYPE_OLD.value:
            # 老笔：分型不共用缠论K线，分型之间至少有一根独立缠论K线
            return ck_diff >= 2
        elif bi_type == Config.BI_TYPE_NEW.value:
            # 新笔：分型之间至少5根原始K线，且不共用缠论K线（至少一根独立缠论K线）
            return src_k_num >= 5 and ck_diff >= 2
        elif bi_type == Config.BI_TYPE_JDB.value:
            # 简单笔：至少5根原始K线即可
            return src_k_num >= 5
        elif bi_type == Config.BI_TYPE_DD.value:
            # 顶底成笔：出现相邻顶底即可
            return True
        else:
            return ck_diff >= 2

    def _bi_high_low(self, start_fx: FX, end_fx: FX) -> Tuple[float, float]:
        """
        计算笔的高低点
        """
        qy = self.config.get("fx_qy", Config.FX_QY_THREE.value)
        qj_ck = Config.FX_QJ_CK.value
        qj_k = Config.FX_QJ_K.value
        bi_qj = self.config.get("bi_qj", Config.BI_QJ_DD.value)
        
        if bi_qj == Config.BI_QJ_DD.value:
            high = max(start_fx.val, end_fx.val)
            low = min(start_fx.val, end_fx.val)
        elif bi_qj == Config.BI_QJ_CK.value:
            high = max(start_fx.high(qj_ck, qy), end_fx.high(qj_ck, qy))
            low = min(start_fx.low(qj_ck, qy), end_fx.low(qj_ck, qy))
        elif bi_qj == Config.BI_QJ_K.value:
            high = max(start_fx.high(qj_k, qy), end_fx.high(qj_k, qy))
            low = min(start_fx.low(qj_k, qy), end_fx.low(qj_k, qy))
        else:
            high = max(start_fx.val, end_fx.val)
            low = min(start_fx.val, end_fx.val)
        return high, low

    def _cal_bi(self):
        """
        严格按照缠论理论划分笔
        """
        self.bis = []
        if len(self.fxs) < 2:
            return

        bi_type = self.config["bi_type"]
        
        # 1. 找到第一个有效的笔
        start_fx = self.fxs[0]
        idx = 1
        
        while idx < len(self.fxs):
            end_fx = self.fxs[idx]
            
            if self.check_bi_valid(start_fx, end_fx, bi_type):
                bi = BI(
                    start=start_fx,
                    end=end_fx,
                    _type="down" if start_fx.type == "ding" else "up",
                    index=len(self.bis)
                )
                bi.high, bi.low = self._bi_high_low(start_fx, end_fx)
                self.bis.append(bi)
                break
            
            # 更新起始分型（同类型且更极值）
            if start_fx.type == end_fx.type:
                if start_fx.type == "ding" and end_fx.val >= start_fx.val:
                    start_fx = end_fx
                elif start_fx.type == "di" and end_fx.val <= start_fx.val:
                    start_fx = end_fx
            
            idx += 1
            
        if not self.bis:
            return
            
        # 2. 继续从最后一笔的结束点开始
        curr_fx_idx = self.bis[-1].end.index + 1
        
        while curr_fx_idx < len(self.fxs):
            # 总是参考最后一笔的结束点
            last_bi = self.bis[-1]
            start_fx = last_bi.end
            next_fx = self.fxs[curr_fx_idx]
            
            # 检查1：同类型延伸（合并逻辑）
            if next_fx.type == start_fx.type:
                updated = False
                if start_fx.type == "ding" and next_fx.val >= start_fx.val:
                    updated = True
                elif start_fx.type == "di" and next_fx.val <= start_fx.val:
                    updated = True
                    
                if updated:
                    # 延伸最后一笔
                    last_bi.end = next_fx
                    last_bi.high, last_bi.low = self._bi_high_low(last_bi.start, next_fx)
                
                curr_fx_idx += 1
                continue

            # 检查2：不同类型（潜在新笔）
            if self.check_bi_valid(start_fx, next_fx, bi_type):
                bi = BI(
                    start=start_fx,
                    end=next_fx,
                    _type="down" if start_fx.type == "ding" else "up",
                    index=len(self.bis)
                )
                bi.high, bi.low = self._bi_high_low(start_fx, next_fx)
                self.bis.append(bi)
            else:
                # 无效新笔，忽略当前分型
                pass
                
            curr_fx_idx += 1

    def _check_xd_basic_overlap(self, start_bi: BI, end_bi: BI) -> bool:
        """
        检查线段基本重叠条件：第1笔与第3笔必须有价格重叠
        """
        if start_bi.type == "up":
            # 向上线段：第3笔低点 <= 第1笔高点
            return end_bi.low <= start_bi.high
        else:
            # 向下线段：第3笔高点 >= 第1笔低点
            return end_bi.high >= start_bi.low

    def _get_xd_direction(self, start_bi: BI, start_idx: int) -> Union[str, None]:
        """
        确定线段方向，必须与上一线段反向（如果存在）
        """
        if len(self.xds) == 0:
            # 第一个线段，方向由起始笔决定
            return start_bi.type
        
        last_xd = self.xds[-1]
        required_dir = "down" if last_xd.type == "up" else "up"
        
        if start_bi.type == required_dir:
            return required_dir
        # 方向相同，检查是否延伸上一线段
        if last_xd.type == "up" and start_bi.high > last_xd.high:
            last_xd.high = start_bi.high
            last_xd.end = start_bi.end
            last_xd.end_line = start_bi
            return None
        if last_xd.type == "down" and start_bi.low < last_xd.low:
            last_xd.low = start_bi.low
            last_xd.end = start_bi.end
            last_xd.end_line = start_bi
            return None
        # 同向但未延伸，跳过
        return None

    def _find_first_same_dir_overlap_index(self, start_idx: int, xd_dir: str) -> Union[int, None]:
        """
        寻找与起始笔同向且与起始笔满足重叠条件的最早笔索引
        线段最短三笔成立：首笔与最后一个同向笔区间有交集
        """
        start_bi = self.bis[start_idx]
        for j in range(start_idx + 2, len(self.bis)):
            if self.bis[j].type != xd_dir:
                continue
            if self._check_xd_basic_overlap(start_bi, self.bis[j]):
                return j
        return None

    def _create_feature_sequence_element(self, bi: BI, xd_dir: str) -> TZXL:
        """
        创建特征序列元素
        修复：根据缠论理论，特征序列的处理方向与线段方向相反
        - 向上线段：特征序列为下笔，处理方向为"down"（取低低）
        - 向下线段：特征序列为上笔，处理方向为"up"（取高高）
        """
        # 特征序列方向与线段方向相反
        bh_direction = "down" if xd_dir == "up" else "up"
        
        tzxl = TZXL(
            bh_direction=bh_direction,
            line=bi,
            pre_line=None,
            line_bad=False,
            done=True
        )
        tzxl.max = bi.high
        tzxl.min = bi.low
        tzxl.lines = [bi]
        return tzxl

    def _is_feature_sequence_included(self, tzxl: TZXL, last_tzxl: TZXL, xd_dir: str) -> bool:
        """
        判断两个特征序列元素是否存在包含关系
        严格按照缠论理论：特征序列包含关系判断
        """
        # 检查是否存在包含关系
        return (tzxl.max <= last_tzxl.max and tzxl.min >= last_tzxl.min) or \
               (last_tzxl.max <= tzxl.max and last_tzxl.min >= tzxl.min)

    def _process_feature_sequence_inclusion(self, tzxl: TZXL, last_tzxl: TZXL, xd_dir: str):
        """
        处理特征序列包含关系
        严格按照缠论理论进行包含处理
        """
        # 根据特征序列的处理方向进行包含处理
        if last_tzxl.bh_direction == "up":
            # 向上包含：取高高
            last_tzxl.max = max(last_tzxl.max, tzxl.max)
            last_tzxl.min = max(last_tzxl.min, tzxl.min)
        else:
            # 向下包含：取低低
            last_tzxl.max = min(last_tzxl.max, tzxl.max)
            last_tzxl.min = min(last_tzxl.min, tzxl.min)
        
        # 更新包含的笔列表
        last_tzxl.lines.append(tzxl.line)
        # 更新最大值最小值
        last_tzxl.update_maxmin()

    def _check_feature_sequence_fracture(self, feature_sequence: List[TZXL], xd_dir: str) -> Union[Tuple[str, TZXL, bool], None]:
        """
        检查特征序列是否形成分型
        严格按照缠论理论：特征序列分型识别
        修复：正确处理缺口判断，返回分型类型、中心元素、是否有缺口
        """
        if len(feature_sequence) < 3:
            return None
            
        # 收集所有可能的候选分型，类似笔分型的处理方式
        candidate_fxs = []
        
        for i in range(2, len(feature_sequence)):
            t1, t2, t3 = feature_sequence[i-2], feature_sequence[i-1], feature_sequence[i]
            
            # 检查是否形成分型
            fx_type = None
            if xd_dir == "up":
                # 向上线段，特征序列(下笔)找顶分型
                # 严格缠论分型条件：中间元素最高，两边元素较低
                if t2.max >= t1.max and t2.max >= t3.max:
                    fx_type = "ding"
            else:
                # 向下线段，特征序列(上笔)找底分型
                # 严格缠论分型条件：中间元素最低，两边元素较高
                if t2.min <= t1.min and t2.min <= t3.min:
                    fx_type = "di"
            
            if fx_type:
                # 检查第一与第二元素是否无重叠（缺口）
                # 缺口判断：两个特征序列元素的价格区间没有重叠
                a_low, a_high = t1.min, t1.max
                b_low, b_high = t2.min, t2.max
                has_gap = (b_low > a_high) or (a_low > b_high)
                candidate_fxs.append((fx_type, t2, i-1, has_gap))
        
        if not candidate_fxs:
            return None
        
        # 处理候选分型，确保同类型相邻分型保留极值
        valid_fxs = [candidate_fxs[0]]
        
        for i in range(1, len(candidate_fxs)):
            current_fx = candidate_fxs[i]
            last_valid_fx = valid_fxs[-1]
            
            # 检查是否为同类型分型
            if current_fx[0] == last_valid_fx[0]:
                # 同类型分型，保留更极值的
                if current_fx[0] == "ding" and current_fx[1].max > last_valid_fx[1].max:
                    valid_fxs[-1] = current_fx
                elif current_fx[0] == "di" and current_fx[1].min < last_valid_fx[1].min:
                    valid_fxs[-1] = current_fx
            else:
                # 不同类型分型，直接添加
                valid_fxs.append(current_fx)
        
        # 返回最后一个有效分型（最新的）
        if valid_fxs:
            fx_type, center_tzxl, _, has_gap = valid_fxs[-1]
            return (fx_type, center_tzxl, has_gap)
        
        return None

    def _create_xlfx(self, fx_type: str, center_tzxl: TZXL, xls: List[TZXL]) -> XLFX:
        """
        创建序列分型对象
        """
        xlfx = XLFX(fx_type, center_tzxl, xls, True)
        
        if len(xls) >= 2:
            t1, t2 = xls[0], xls[1]
            a_low, a_high = t1.min, t1.max
            b_low, b_high = t2.min, t2.max
            xlfx.qk = (b_low > a_high) or (a_low > b_high)
        
        return xlfx

    def _get_xd_end_bi_index(self, fx_tzxl: TZXL, xd_dir: str) -> int:
        """
        获取线段结束的笔索引
        严格按照缠论理论：线段结束于特征序列分型确认的前一笔
        修复：正确找到与线段同向的最后一笔
        """
        # 分型中心对应的笔是反向笔，需要找到前一笔同向笔作为结束点
        center_bi = fx_tzxl.line
        
        # 根据缠论理论：
        # 1. 向上线段结束于顶分型，结束点是顶分型前一笔（上笔）
        # 2. 向下线段结束于底分型，结束点是底分型前一笔（下笔）
        
        # 找到分型中心笔在全局笔序列中的位置
        center_idx = center_bi.index
        
        if xd_dir == "up":
            # 向上线段，需要找到最后一个上笔（在顶分型之前）
            # 从分型中心笔向前找，找到第一个上笔
            for i in range(center_idx - 1, -1, -1):
                if self.bis[i].type == "up":
                    return i
        else:
            # 向下线段，需要找到最后一个下笔（在底分型之前）
            for i in range(center_idx - 1, -1, -1):
                if self.bis[i].type == "down":
                    return i
        
        # 如果没找到，返回分型中心笔的前一笔
        return max(0, center_idx - 1)

    def _find_xd_end_by_feature_sequence(self, start_idx: int, xd_dir: str) -> Union[Tuple[int, XLFX, XLFX], None]:
        """
        使用特征序列方法寻找线段结束点
        修复：正确处理缺口情形，严格按照缠论理论
        引入双特征序列处理逻辑，处理有缺口分型的验证与破坏
        返回: (结束笔索引, 顶分型, 底分型) 或 None
        """
        feature_sequence = []  # 原线段特征序列 (序列A)
        
        # 待确认的有缺口分型信息
        # 结构: {
        #   'fx_type': str, 
        #   'fx_tzxl': TZXL, 
        #   'xls': List[TZXL], 
        #   'end_bi_idx': int, 
        #   'check_sequence': List[TZXL] # 用于验证的反向特征序列 (序列B)
        # }
        pending_gap_fx = None
        
        # 仅在形成最短线段（三笔）之后，开始收集特征序列
        # 注意：这里我们遍历所有笔，根据笔的方向决定归属
        for i in range(start_idx + 1, len(self.bis)):
            bi = self.bis[i]
            
            # --- 情况1：笔是原线段特征序列元素（反向笔） ---
            if bi.type != xd_dir:
                # 创建特征序列元素
                tzxl = self._create_feature_sequence_element(bi, xd_dir)
                
                # 特征序列包含处理 (序列A)
                if feature_sequence:
                    last_tzxl = feature_sequence[-1]
                    if self._is_feature_sequence_included(tzxl, last_tzxl, xd_dir):
                        self._process_feature_sequence_inclusion(tzxl, last_tzxl, xd_dir)
                    else:
                        feature_sequence.append(tzxl)
                else:
                    feature_sequence.append(tzxl)
                
                # 如果当前没有待确认的分型，检查序列A是否形成分型
                if pending_gap_fx is None:
                    if len(feature_sequence) >= 3:
                        fx_result = self._check_feature_sequence_fracture(feature_sequence, xd_dir)
                        if fx_result:
                            fx_type, fx_tzxl, has_gap = fx_result
                            end_bi_idx = self._get_xd_end_bi_index(fx_tzxl, xd_dir)
                            
                            # 必须确保分型位置在 start_idx 之后
                            if end_bi_idx > start_idx:
                                if not has_gap:
                                    # 无缺口，直接确认结束
                                    return self._create_xd_end_result(fx_type, fx_tzxl, feature_sequence[-3:], end_bi_idx)
                                else:
                                    # 有缺口，进入待确认模式
                                    pending_gap_fx = {
                                        'fx_type': fx_type,
                                        'fx_tzxl': fx_tzxl,
                                        'xls': feature_sequence[-3:].copy(),
                                        'end_bi_idx': end_bi_idx,
                                        'check_sequence': [] # 初始化序列B
                                    }
            
            # --- 情况2：笔是原线段同向笔 ---
            else:
                # 只有在有待确认分型时，同向笔才重要
                if pending_gap_fx:
                    # 1. 检查是否破坏了待确认分型（创新高/新低）
                    # 向下线段(xd_dir='down')，待确认底分型，如果新下笔创新低，则破坏
                    # 向上线段(xd_dir='up')，待确认顶分型，如果新上笔创新高，则破坏
                    
                    is_broken = False
                    end_val = self.bis[pending_gap_fx['end_bi_idx']].end.val
                    
                    if xd_dir == "down":
                        # 向下线段，看是否跌破底分型结束点
                        if bi.low < end_val:
                            is_broken = True
                    else:
                        # 向上线段，看是否突破顶分型结束点
                        if bi.high > end_val:
                            is_broken = True
                    
                    if is_broken:
                        # 分型被破坏，重置待确认状态，继续寻找新的分型
                        pending_gap_fx = None
                        continue
                    
                    # 2. 如果未破坏，将此同向笔加入序列B（反向线段的特征序列）
                    # 注意：反向线段的方向与 xd_dir 相反
                    # 向上线段的反向是向下，特征序列是上笔（即当前的 bi）
                    # 向下线段的反向是向上，特征序列是下笔（即当前的 bi）
                    
                    check_xd_dir = "down" if xd_dir == "up" else "up"
                    
                    # 创建序列B元素
                    tzxl_b = self._create_feature_sequence_element(bi, check_xd_dir)
                    
                    # 序列B包含处理
                    check_seq = pending_gap_fx['check_sequence']
                    if check_seq:
                        last_tzxl_b = check_seq[-1]
                        if self._is_feature_sequence_included(tzxl_b, last_tzxl_b, check_xd_dir):
                            self._process_feature_sequence_inclusion(tzxl_b, last_tzxl_b, check_xd_dir)
                        else:
                            check_seq.append(tzxl_b)
                    else:
                        check_seq.append(tzxl_b)
                    
                    # 3. 检查序列B是否形成分型
                    # 向上线段(xd_dir='up') -> 待确认顶分型 -> 反向向下 -> 序列B(上笔)找底分型
                    # 向下线段(xd_dir='down') -> 待确认底分型 -> 反向向上 -> 序列B(下笔)找顶分型
                    
                    if len(check_seq) >= 3:
                        # 注意：这里check的特征序列方向是 check_xd_dir
                        fx_result_b = self._check_feature_sequence_fracture(check_seq, check_xd_dir)
                        if fx_result_b:
                            # 序列B出现了分型，确认原线段结束！
                            # 结束点就是 pending_gap_fx 中记录的点
                            return self._create_xd_end_result(
                                pending_gap_fx['fx_type'],
                                pending_gap_fx['fx_tzxl'],
                                pending_gap_fx['xls'],
                                pending_gap_fx['end_bi_idx']
                            )
        
        return None

    def _create_xd_end_result(self, fx_type, fx_tzxl, xls, end_bi_idx):
        """辅助方法：创建线段结束返回结果"""
        if fx_type == "ding":
            ding_fx = self._create_xlfx("ding", fx_tzxl, xls)
            di_fx = None
        else:
            ding_fx = None
            di_fx = self._create_xlfx("di", fx_tzxl, xls)
        return (end_bi_idx, ding_fx, di_fx)

    def _handle_unfinished_xd(self):
        """
        处理未完成线段
        """
        if len(self.xds) > 0:
            last_xd = self.xds[-1]
            # 如果最后有剩余笔，创建未完成线段
            if last_xd.end_line.index < len(self.bis) - 1:
                start_bi_idx = last_xd.end_line.index + 1
                if start_bi_idx < len(self.bis):
                    start_bi = self.bis[start_bi_idx]
                    end_bi = self.bis[-1]
                    
                    xd_dir = "down" if last_xd.type == "up" else "up"
                    
                    xd = XD(
                        start=start_bi.start,
                        end=end_bi.end,
                        start_line=start_bi,
                        end_line=end_bi,
                        _type=xd_dir,
                        ding_fx=None,
                        di_fx=None,
                        index=len(self.xds)
                    )
                    
                    if xd_dir == "up":
                        xd.high = max([bi.high for bi in self.bis[start_bi_idx:]])
                        xd.low = start_bi.start.val
                    else:
                        xd.high = start_bi.start.val
                        xd.low = min([bi.low for bi in self.bis[start_bi_idx:]])
                    
                    xd.done = False
                    self.xds.append(xd)
        else:
            # 没有线段，创建第一个未完成线段
            if len(self.bis) > 0:
                xd = XD(
                    start=self.bis[0].start,
                    end=self.bis[-1].end,
                    start_line=self.bis[0],
                    end_line=self.bis[-1],
                    _type=self.bis[0].type,
                    ding_fx=None,
                    di_fx=None,
                    index=0
                )
                
                if xd.type == "up":
                    xd.high = max([bi.high for bi in self.bis])
                    xd.low = self.bis[0].start.val
                else:
                    xd.high = self.bis[0].start.val
                    xd.low = min([bi.low for bi in self.bis])
                
                xd.done = False
                self.xds.append(xd)

    def _cal_xd(self):
        """
        严格按照缠论理论划分线段，基于特征序列分型
        修复：添加基本重叠条件检查，修正线段结束点判断
        """
        self.xds = []
        if len(self.bis) < 3:
            return

        # 严格按照缠论理论进行线段划分
        i = 0
        while i < len(self.bis) - 2:
            # 确定线段起始点
            start_bi = self.bis[i]
            
            # 确定线段方向：必须与上一线段反向（如果存在）
            xd_dir = self._get_xd_direction(start_bi, i)
            if xd_dir is None:
                i += 1
                continue
            
            # 寻找与起始笔同向且满足重叠的最早笔索引（三笔最短线段成立）
            first_overlap_idx = self._find_first_same_dir_overlap_index(i, xd_dir)
            if first_overlap_idx is None:
                i += 1
                continue
            
            # 使用特征序列方法寻找线段结束
            xd_end_info = self._find_xd_end_by_feature_sequence(first_overlap_idx, xd_dir)
            if xd_end_info is None:
                # 没有找到线段结束，继续向后寻找
                i += 1
                continue
            
            end_bi_idx, ding_fx, di_fx = xd_end_info
            end_bi = self.bis[end_bi_idx]
            
            # 创建线段
            xd = XD(
                start=start_bi.start,
                end=end_bi.end,
                start_line=start_bi,
                end_line=end_bi,
                _type=xd_dir,
                ding_fx=ding_fx,
                di_fx=di_fx,
                index=len(self.xds)
            )
            
            # 设置线段高低点
            xd_bis = self.bis[i:end_bi_idx+1]
            if xd_dir == "up":
                xd.high = max([bi.high for bi in xd_bis])
                xd.low = min([bi.low for bi in xd_bis])
            else:
                xd.high = max([bi.high for bi in xd_bis])
                xd.low = min([bi.low for bi in xd_bis])
            
            xd.done = True
            self.xds.append(xd)
            
            # 下一段从结束笔的下一笔开始
            i = end_bi_idx + 1
        
        # 处理未完成线段
        self._handle_unfinished_xd()

    def _cal_zsd(self):
        """
        走势段使用线段相同的逻辑
        """
        self.zsds = self.xds

    def _cal_zs(self):
        """
        计算中枢
        """
        # Clear existing ZSs
        self.bi_zss = {}
        self.xd_zss = {}

        # Helper: 以三线重叠计算中枢，并向后延伸直到离开（含离开段）
        def _calc_zss_by_lines(_lines: List[LINE], _zs_type: str) -> List[ZS]:
            zss: List[ZS] = []
            if len(_lines) < 3:
                return zss
            i = 0
            while i <= len(_lines) - 3:
                l1, l2, l3 = _lines[i], _lines[i + 1], _lines[i + 2]
                zg = min(l1.high, l2.high, l3.high)
                zd = max(l1.low, l2.low, l3.low)
                if zg > zd:
                    zs = ZS(
                        zs_type=_zs_type,
                        start=l1.start,
                        end=l3.end,
                        zg=zg,
                        zd=zd,
                        gg=max(l1.high, l2.high, l3.high),
                        dd=min(l1.low, l2.low, l3.low),
                        index=len(zss),
                        line_num=3,
                        _type="up" if l1.type == "down" else "down",
                    )
                    zs.lines = [l1, l2, l3]
                    zs.real = True
                    j = i + 3
                    # 段内延伸（含离开段）
                    while j < len(_lines):
                        ln = _lines[j]
                        if not (ln.high < zd or ln.low > zg):
                            zs.lines.append(ln)
                            zs.end = ln.end
                            if ln.high > zs.gg:
                                zs.gg = ln.high
                            if ln.low < zs.dd:
                                zs.dd = ln.low
                            j += 1
                        else:
                            zs.lines.append(ln)  # 计入离开段以确定右边界
                            zs.end = ln.end
                            if ln.high > zs.gg:
                                zs.gg = ln.high
                            if ln.low < zs.dd:
                                zs.dd = ln.low
                            j += 1
                            break
                    zss.append(zs)
                    i = j - 1
                else:
                    i += 1
            return zss

        # --- 计算笔中枢（支持标准/段内） ---
        zs_bi_types = self.config.get("zs_bi_type", [])
        if not isinstance(zs_bi_types, list):
            zs_bi_types = [zs_bi_types]

        for zs_type in zs_bi_types:
            self.bi_zss[zs_type] = []
            if len(self.bis) < 3:
                continue

            if zs_type == Config.ZS_TYPE_DN.value and len(self.xds) > 0:
                # 段内中枢：每个线段内独立计算中枢，从线段起点开始重算
                for xd in self.xds:
                    start_idx = xd.start_line.index
                    end_idx = xd.end_line.index if xd.end_line is not None else self.bis[-1].index
                    sub_lines = [bi for bi in self.bis if start_idx <= bi.index <= end_idx]
                    if len(sub_lines) < 3:
                        continue
                    zss_dn = _calc_zss_by_lines(sub_lines, zs_type)
                    # 修正中枢索引为全局顺序
                    for zs in zss_dn:
                        zs.index = len(self.bi_zss[zs_type])
                        self.bi_zss[zs_type].append(zs)
            else:
                # 标准中枢/其他类型：在全体笔上计算
                self.bi_zss[zs_type] = _calc_zss_by_lines(self.bis, zs_type)

        # --- 计算线段中枢（支持标准/段内） ---
        zs_xd_types = self.config.get("zs_xd_type", [])
        if not isinstance(zs_xd_types, list):
            zs_xd_types = [zs_xd_types]

        for zs_xd_type in zs_xd_types:
            self.xd_zss[zs_xd_type] = []
            if len(self.xds) < 3:
                continue

            if zs_xd_type == Config.ZS_TYPE_DN.value:
                # 段内中枢：按走势段内线段进行局部计算（此处线段本身已是特征序列产物，直接全量计算即可）
                self.xd_zss[zs_xd_type] = _calc_zss_by_lines(self.xds, zs_xd_type)
            else:
                # 标准中枢/其他类型：同样按全量线段计算
                self.xd_zss[zs_xd_type] = _calc_zss_by_lines(self.xds, zs_xd_type)

    def _cal_mmd_bc(self):
        """
        严格按照缠论理论识别买卖点和背驰
        """
        # --- 1. 笔的买卖点和背驰识别 ---
        zs_type = self.config.get("zs_bi_type", ["common"])[0]
        zss = self.bi_zss.get(zs_type, [])
        
        # 配置项
        check_1buy = self.config.get("mmd_1buy_bc", True)
        check_1sell = self.config.get("mmd_1sell_bc", True)
        check_2buy = self.config.get("mmd_2buy_bc", True)
        check_2sell = self.config.get("mmd_2sell_bc", True)
        check_3buy = self.config.get("mmd_3buy_bc", True)
        check_3sell = self.config.get("mmd_3sell_bc", True)
        check_l2buy = self.config.get("mmd_l2buy_bc", True)
        check_l2sell = self.config.get("mmd_l2sell_bc", True)
        check_l3buy = self.config.get("mmd_l3buy_bc", True)
        check_l3sell = self.config.get("mmd_l3sell_bc", True)

        for i in range(len(self.bis)):
            bi = self.bis[i]
            
            # 1. 基本背驰判断（笔背驰）
            if i >= 2:
                prev_bi = self.bis[i-2]
                if prev_bi.type == bi.type:
                    # 检查是否创新高/新低且力度减弱
                    if ((bi.type == "up" and bi.high > prev_bi.high) or 
                        (bi.type == "down" and bi.low < prev_bi.low)):
                        # 比较力度
                        ld1 = bi.get_ld(self)["macd"]
                        ld2 = prev_bi.get_ld(self)["macd"]
                        if compare_ld_beichi(ld2, ld1, bi.type):
                            bi.add_bc("bi", None, prev_bi, [prev_bi], True, zs_type)
            
            # 2. 基于中枢的买卖点识别
            for zs in zss:
                if not zs.real:
                    continue
                
                # 第三类买卖点：离开中枢后回抽不进入中枢
                if i >= 1:
                    prev_bi = self.bis[i-1]
                    # 检查是否离开中枢
                    if prev_bi.start.index == zs.end.index:
                        if check_3buy and bi.type == "down" and bi.low > zs.zg:
                            bi.add_mmd("3buy", zs, zs_type)
                        if check_3sell and bi.type == "up" and bi.high < zs.zd:
                            bi.add_mmd("3sell", zs, zs_type)
                
                # 盘整背驰：离开段与进入段比较
                is_pz, compare_line = self.beichi_pz(zs, bi)
                if is_pz:
                    bi.add_bc("pz", zs, compare_line, [compare_line], True, zs_type)
                
                # 趋势背驰：需要至少两个中枢形成趋势
                is_qs, compare_lines = self.beichi_qs(self.bis, zss, bi)
                if is_qs and zs.lines[-1].index < bi.index:
                    bi.add_bc("qs", zs, None, compare_lines, True, zs_type)
                    
                    # 第一类买卖点：趋势背驰
                    if check_1buy and bi.type == "down" and bi.low < zs.zd:
                        bi.add_mmd("1buy", zs, zs_type)
                    if check_1sell and bi.type == "up" and bi.high > zs.zg:
                        bi.add_mmd("1sell", zs, zs_type)
            
            # 3. 第二类买卖点识别
            if i >= 2:
                prev_bi_2 = self.bis[i-2]
                
                # 检查是否有第一类买卖点
                is_1buy = any(m.name == "1buy" for m in prev_bi_2.get_mmds(zs_type))
                is_1sell = any(m.name == "1sell" for m in prev_bi_2.get_mmds(zs_type))
                
                if check_2buy and bi.type == "down":
                    # 情况1：一类买点后，不创新低
                    if is_1buy and bi.low > prev_bi_2.low:
                        target_zs = prev_bi_2.get_mmds(zs_type)[0].zs
                        bi.add_mmd("2buy", target_zs, zs_type)
                    
                    # 情况2：趋势背驰后，不创新低
                    for bc in prev_bi_2.get_bcs(zs_type):
                        if bc.type in ["qs", "pz"] and bc.zs is not None:
                            if bi.low > prev_bi_2.low and compare_ld_beichi(prev_bi_2.get_ld(self), bi.get_ld(self), "down"):
                                bi.add_mmd("2buy", bc.zs, zs_type)
                                break
                
                if check_2sell and bi.type == "up":
                    # 情况1：一类卖点后，不创新高
                    if is_1sell and bi.high < prev_bi_2.high:
                        target_zs = prev_bi_2.get_mmds(zs_type)[0].zs
                        bi.add_mmd("2sell", target_zs, zs_type)
                    
                    # 情况2：趋势背驰后，不创新高
                    for bc in prev_bi_2.get_bcs(zs_type):
                        if bc.type in ["qs", "pz"] and bc.zs is not None:
                            if bi.high < prev_bi_2.high and compare_ld_beichi(prev_bi_2.get_ld(self), bi.get_ld(self), "up"):
                                bi.add_mmd("2sell", bc.zs, zs_type)
                                break
            
            # 4. 类买卖点识别
            if i >= 2:
                prev_bi_2 = self.bis[i-2]
                prev_bi = self.bis[i-1]
                
                if bi.type == "down":
                    # 类二买：前一同向线段出现二买，当前有重叠
                    if check_l2buy:
                        has_2buy_mmds = [m for m in prev_bi_2.get_mmds(zs_type) if m.name == "2buy"]
                        if has_2buy_mmds:
                            # 检查是否有重叠形成中枢
                            zg = min(prev_bi_2.high, prev_bi.high, bi.high)
                            if zg > bi.low and bi.low > prev_bi_2.low:
                                target_zs = has_2buy_mmds[0].zs
                                bi.add_mmd("l2buy", target_zs, zs_type)
                    
                    # 类三买：前一同向线段出现三买，当前有重叠
                    if check_l3buy:
                        has_3buy_mmds = [m for m in prev_bi_2.get_mmds(zs_type) if m.name == "3buy"]
                        if has_3buy_mmds:
                            zg = min(prev_bi_2.high, prev_bi.high, bi.high)
                            if zg > bi.low and bi.low > prev_bi_2.low:
                                target_zs = has_3buy_mmds[0].zs
                                bi.add_mmd("l3buy", target_zs, zs_type)
                
                if bi.type == "up":
                    # 类二卖
                    if check_l2sell:
                        has_2sell_mmds = [m for m in prev_bi_2.get_mmds(zs_type) if m.name == "2sell"]
                        if has_2sell_mmds:
                            zd = max(prev_bi_2.low, prev_bi.low, bi.low)
                            if bi.high > zd and bi.high < prev_bi_2.high:
                                target_zs = has_2sell_mmds[0].zs
                                bi.add_mmd("l2sell", target_zs, zs_type)
                    
                    # 类三卖
                    if check_l3sell:
                        has_3sell_mmds = [m for m in prev_bi_2.get_mmds(zs_type) if m.name == "3sell"]
                        if has_3sell_mmds:
                            zd = max(prev_bi_2.low, prev_bi.low, bi.low)
                            if bi.high > zd and bi.high < prev_bi_2.high:
                                target_zs = has_3sell_mmds[0].zs
                                bi.add_mmd("l3sell", target_zs, zs_type)
        
        # 用户自定义买卖点
        if len(self.bis) > 0:
            user_custom_mmd(self, self.bis[-1], self.bis, zs_type, zss)
        
        # --- 2. 线段的买卖点和背驰识别 ---
        zs_xd_type = self.config.get("zs_xd_type", ["common"])[0]
        xd_zss = self.xd_zss.get(zs_xd_type, [])
        
        if len(self.xds) > 0:
            for i in range(len(self.xds)):
                xd = self.xds[i]
                
                # 基本背驰判断（线段背驰）
                if i >= 2:
                    prev_xd = self.xds[i-2]
                    if prev_xd.type == xd.type:
                        if ((xd.type == "up" and xd.high > prev_xd.high) or 
                            (xd.type == "down" and xd.low < prev_xd.low)):
                            ld1 = xd.get_ld(self)["macd"]
                            ld2 = prev_xd.get_ld(self)["macd"]
                            if compare_ld_beichi(ld2, ld1, xd.type):
                                xd.add_bc("xd", None, prev_xd, [prev_xd], True, zs_xd_type)
                
                # 基于中枢的买卖点
                if not xd_zss:
                    continue
                
                # 第二类买卖点
                if i >= 2:
                    prev_xd_2 = self.xds[i-2]
                    is_1buy = any(m.name == "1buy" for m in prev_xd_2.get_mmds(zs_xd_type))
                    is_1sell = any(m.name == "1sell" for m in prev_xd_2.get_mmds(zs_xd_type))
                    
                    if xd.type == "down":
                        if is_1buy and xd.low > prev_xd_2.low:
                            target_zs = prev_xd_2.get_mmds(zs_xd_type)[0].zs
                            xd.add_mmd("2buy", target_zs, zs_xd_type)
                        
                        for bc in prev_xd_2.get_bcs(zs_xd_type):
                            if bc.type in ["qs", "pz"] and bc.zs is not None:
                                if xd.low > prev_xd_2.low and compare_ld_beichi(prev_xd_2.get_ld(self), xd.get_ld(self), "down"):
                                    xd.add_mmd("2buy", bc.zs, zs_xd_type)
                                    break
                    
                    if xd.type == "up":
                        if is_1sell and xd.high < prev_xd_2.high:
                            target_zs = prev_xd_2.get_mmds(zs_xd_type)[0].zs
                            xd.add_mmd("2sell", target_zs, zs_xd_type)
                        
                        for bc in prev_xd_2.get_bcs(zs_xd_type):
                            if bc.type in ["qs", "pz"] and bc.zs is not None:
                                if xd.high < prev_xd_2.high and compare_ld_beichi(prev_xd_2.get_ld(self), xd.get_ld(self), "up"):
                                    xd.add_mmd("2sell", bc.zs, zs_xd_type)
                                    break
                
                # 第三类买卖点
                for zs in xd_zss:
                    if not zs.real:
                        continue
                    
                    if i >= 1:
                        prev_xd = self.xds[i-1]
                        if prev_xd.start.index == zs.end.index:
                            if xd.type == "down" and xd.low > zs.zg:
                                xd.add_mmd("3buy", zs, zs_xd_type)
                            if xd.type == "up" and xd.high < zs.zd:
                                xd.add_mmd("3sell", zs, zs_xd_type)
                    
                    # 第一类买卖点（趋势背驰）
                    is_qs = any(b.type == "qs" and b.zs.index == zs.index for b in xd.get_bcs(zs_xd_type))
                    if is_qs:
                        if xd.type == "down" and xd.low < zs.zd:
                            xd.add_mmd("1buy", zs, zs_xd_type)
                        if xd.type == "up" and xd.high > zs.zg:
                            xd.add_mmd("1sell", zs, zs_xd_type)
                    
                    # 第二类买卖点
                    if i >= 3:
                        prev_xd_2 = self.xds[i-2]
                        prev_xd_3 = self.xds[i-3]
                        is_1buy = any(m.name == "1buy" for m in prev_xd_2.get_mmds(zs_xd_type))
                        
                        if xd.type == "down":
                            has_3sell_prev = any(m.name == "3sell" and m.zs.index == zs.index for m in prev_xd_3.get_mmds(zs_xd_type))
                            if has_3sell_prev and not is_1buy and xd.low > prev_xd_2.low:
                                xd.add_mmd("2buy", zs, zs_xd_type)
                        
                        if xd.type == "up":
                            has_3buy_prev = any(m.name == "3buy" and m.zs.index == zs.index for m in prev_xd_3.get_mmds(zs_xd_type))
                            if has_3buy_prev and not any(m.name == "1sell" for m in prev_xd_2.get_mmds(zs_xd_type)) and xd.high < prev_xd_2.high:
                                xd.add_mmd("2sell", zs, zs_xd_type)

    def beichi_pz(self, zs: ZS, now_line: LINE) -> Tuple[bool, Union[LINE, None]]:
        """
        判断盘整背驰
        """
        if len(zs.lines) < 1:
            return False, None

        lines = self.bis if isinstance(now_line, BI) else (self.xds if isinstance(now_line, XD) else None)
        if lines is None:
            return False, None

        first = zs.lines[0]
        entering_idx = first.index - 1
        if entering_idx < 0:
            return False, None
        entering = lines[entering_idx]

        if now_line.start.index != zs.end.index:
            return False, None

        group = [entering] + zs.lines + [now_line]
        if now_line.type == "up":
            enter_extreme = entering.high == max(l.high for l in group)
            leave_extreme = now_line.high == max(l.high for l in group)
        else:
            enter_extreme = entering.low == min(l.low for l in group)
            leave_extreme = now_line.low == min(l.low for l in group)

        if not (enter_extreme and leave_extreme):
            return False, None

        ld1 = entering.get_ld(self)
        ld2 = now_line.get_ld(self)
        if compare_ld_beichi(ld1, ld2, now_line.type):
            return True, entering
        return False, None

    def beichi_qs(self, lines: List[LINE], zss: List[ZS], now_line: LINE) -> Tuple[bool, List[LINE]]:
        """
        判断趋势背驰
        """
        if len(zss) < 2:
            return False, []

        zs1 = zss[-2]
        zs2 = zss[-1]
        trend_type, _ = self.zss_is_qs(zs1, zs2)
        if not trend_type:
            return False, []

        if trend_type == "up" and now_line.type != "up":
            return False, []
        if trend_type == "down" and now_line.type != "down":
            return False, []

        prev_same = [l for l in lines if l.index > zs1.lines[-1].index and l.index < now_line.index and l.type == now_line.type]
        if not prev_same:
            return False, []
        ref = prev_same[-1]

        if now_line.type == "up" and not (now_line.high > ref.high):
            return False, []
        if now_line.type == "down" and not (now_line.low < ref.low):
            return False, []

        ld1 = ref.get_ld(self)
        ld2 = now_line.get_ld(self)
        if compare_ld_beichi(ld1, ld2, now_line.type):
            return True, [ref]
        return False, []

    def zss_is_qs(self, one_zs: ZS, two_zs: ZS) -> Tuple[str, None]:
        """
        判断两个中枢是否形成趋势
        """
        wzgx = self.config.get("zs_wzgx", Config.ZS_WZGX_ZGD.value)
        
        # Up Trend
        is_up = False
        if wzgx == Config.ZS_WZGX_ZGD.value:
            if two_zs.zd > one_zs.zg: is_up = True
        elif wzgx == Config.ZS_WZGX_ZGGDD.value:
            if two_zs.zd > one_zs.gg: is_up = True
        elif wzgx == Config.ZS_WZGX_GD.value:
            if two_zs.dd > one_zs.gg: is_up = True
            
        if is_up:
            return "up", None
            
        # Down Trend
        is_down = False
        if wzgx == Config.ZS_WZGX_ZGD.value:
            if two_zs.zg < one_zs.zd: is_down = True
        elif wzgx == Config.ZS_WZGX_ZGGDD.value:
            if two_zs.zg < one_zs.dd: is_down = True
        elif wzgx == Config.ZS_WZGX_GD.value:
            if two_zs.gg < one_zs.dd: is_down = True
            
        if is_down:
            return "down", None
            
        return None, None

    def create_dn_zs(self, zs_type: str, lines: List[LINE], max_line_num: int = 999, zs_include_last_line=True) -> List[ZS]:
        """
        根据给定的线列表创建中枢
        """
        zss = []
        if len(lines) < 3:
            return zss
            
        i = 0
        while i <= len(lines) - 3:
            l1 = lines[i]
            l2 = lines[i+1]
            l3 = lines[i+2]
            
            zg = min(l1.high, l2.high, l3.high)
            zd = max(l1.low, l2.low, l3.low)
            
            if zg > zd:
                zs = ZS(
                    zs_type=zs_type,
                    start=l1.start,
                    end=l3.end,
                    zg=zg,
                    zd=zd,
                    gg=max(l1.high, l2.high, l3.high),
                    dd=min(l1.low, l2.low, l3.low),
                    index=len(zss),
                    line_num=3,
                    _type="up" if l1.type == "down" else "down"
                )
                zs.lines = [l1, l2, l3]
                zs.real = True
                zss.append(zs)
                
                j = i + 3
                while j < len(lines):
                    ln = lines[j]
                    if not zs_include_last_line and j == len(lines) - 1:
                        break

                    if not (ln.high < zd or ln.low > zg):
                        if len(zs.lines) >= max_line_num:
                            break
                        zs.lines.append(ln)
                        zs.end = ln.end
                        if ln.high > zs.gg: zs.gg = ln.high
                        if ln.low < zs.dd: zs.dd = ln.low
                        j += 1
                    else:
                        break
                i = j - 1
            else:
                i += 1
        return zss


def create_cl(code: str, frequency: str, config: Union[dict, None] = None) -> CL:
    return CL(code, frequency, config)
