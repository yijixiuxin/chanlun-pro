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
        if len(self.klines) == 0:
            self.cl_klines = []
            return

        start_idx = 0
        direction = None

        if len(self.cl_klines) > 0:
            # 增量更新：弹出最后一个缠论K线，重新计算
            last_ck = self.cl_klines.pop()
            start_idx = last_ck.klines[0].index
            # 恢复之前的方向
            if len(self.cl_klines) > 0:
                direction = self.cl_klines[-1].up_qs
        else:
            # 全量计算：初始化第一个K线
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
            start_idx = 1

        for i in range(start_idx, len(self.klines)):
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
        if len(self.cl_klines) < 3:
            self.fxs = []
            return

        fx_qj = self.config["fx_qj"]
        fx_bh = self.config["fx_bh"]
        
        # 增量更新：清理可能失效的分型
        # 最后一个CL_Kline可能发生了变化，所以涉及最后几个CL_Kline的分型都需要重算
        while len(self.fxs) > 0 and self.fxs[-1].k.index >= len(self.cl_klines) - 2:
            self.fxs.pop()
            
        start_idx = 1
        if len(self.fxs) > 0:
            start_idx = self.fxs[-1].k.index + 1
        
        # 临时存储所有候选分型
        candidate_fxs = []
        
        # 分型识别应在无包含K线序列上进行
        for i in range(start_idx, len(self.cl_klines) - 1):
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
                        # 修正 K 线坐标
                        high_k = None
                        for _k in k2.klines:
                            if abs(_k.h - val) < 1e-9:
                                high_k = _k
                                break
                        if high_k:
                            k2.k_index = high_k.index
                            k2.date = high_k.date
                    else:
                        high_k = max(k2.klines, key=lambda _k: _k.h)
                        val = high_k.h
                        k2.k_index = high_k.index
                        k2.date = high_k.date
                else:
                    if fx_qj == Config.FX_QJ_CK.value:
                        val = k2.l
                        # 修正 K 线坐标
                        low_k = None
                        for _k in k2.klines:
                            if abs(_k.l - val) < 1e-9:
                                low_k = _k
                                break
                        if low_k:
                            k2.k_index = low_k.index
                            k2.date = low_k.date
                    else:
                        low_k = min(k2.klines, key=lambda _k: _k.l)
                        val = low_k.l
                        k2.k_index = low_k.index
                        k2.date = low_k.date
                
                fx = FX(
                    _type=fx_type,
                    k=k2,
                    klines=[k1, k2, k3],
                    val=val,
                    index=len(self.fxs) + len(candidate_fxs),
                    done=True
                )
                candidate_fxs.append(fx)
        
        # 处理候选分型，确保顶底交替且不包含
        if not candidate_fxs:
            return
            
        # 如果 self.fxs 为空，初始化第一个
        if not self.fxs:
            self.fxs.append(candidate_fxs[0])
            candidate_fxs = candidate_fxs[1:]
            
        for i in range(len(candidate_fxs)):
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
                # 不同类型分型，检查是否包含
                if not self._is_fx_included(last_fx, current_fx):
                    # 不包含，可以添加
                    # 重新设置 index
                    current_fx.index = len(self.fxs)
                    self.fxs.append(current_fx)
                else:
                    # 包含关系，保留更重要的分型（通常保留前一个）
                    # 这里可以根据具体策略选择保留哪个分型
                    pass

    def _check_bi_len_valid(self, start_fx: FX, end_fx: FX, bi_type: str) -> bool:
        """
        检查笔的长度是否有效
        """
        # 缠论K线中心索引差值（用于判断是否存在独立缠论K线）
        ck_diff = end_fx.k.index - start_fx.k.index

        # 原始K线数量（用于新笔/简单笔判断）
        src_k_num = 0
        for ck in self.cl_klines[start_fx.k.index : end_fx.k.index + 1]:
            src_k_num += len(ck.klines)

        if bi_type == Config.BI_TYPE_OLD.value:
            # 老笔：分型不共用缠论K线，分型之间至少有一根独立缠论K线
            return src_k_num >= 5 and ck_diff >= 4
        elif bi_type == Config.BI_TYPE_NEW.value:
            # 新笔：分型之间至少5根原始K线，且不共用缠论K线（至少一根独立缠论K线）
            return src_k_num >= 5 and ck_diff >= 3
        elif bi_type == Config.BI_TYPE_JDB.value:
            # 简单笔：至少5根原始K线即可
            return src_k_num >= 5
        elif bi_type == Config.BI_TYPE_DD.value:
            # 顶底成笔：出现相邻顶底即可
            return True
        else:
            return ck_diff >= 4

    def check_bi_valid(self, start_fx: FX, end_fx: FX, bi_type: str) -> bool:
        """
        检查笔的有效性，严格按照缠论理论
        增加分型有效性确认：只有被反向笔确认的分型才可作为笔的转折点
        """
        # 顶底必须交替
        if start_fx.type == end_fx.type:
            return False

        # 方向必须满足：顶->低 新低；底->顶 新高
        if start_fx.type == "ding" and end_fx.val >= start_fx.val:
            return False
        if start_fx.type == "di" and end_fx.val <= start_fx.val:
            return False

        # 检查分型有效性：只有被反向笔确认的分型才有效
        if not self._is_fx_confirmed_by_subsequent_movement(start_fx, end_fx, bi_type):
            return False

        return self._check_bi_len_valid(start_fx, end_fx, bi_type)

    def _is_fx_included(self, fx1: FX, fx2: FX) -> bool:
        """
        检查两个分型是否存在包含关系
        根据缠论理论：相邻顶底分型中间K线的高低点不能包含
        
        包含关系定义：
        一个分型的最高价 >= 另一个分型的最高价 且
        一个分型的最低价 <= 另一个分型的最低价
        """
        # 获取分型的极值范围
        fx1_high = fx1.val if fx1.type == "ding" else fx1.k.h
        fx1_low = fx1.val if fx1.type == "di" else fx1.k.l
        
        fx2_high = fx2.val if fx2.type == "ding" else fx2.k.h
        fx2_low = fx2.val if fx2.type == "di" else fx2.k.l
        
        # 检查包含关系
        if (fx1_high >= fx2_high and fx1_low <= fx2_low) or \
           (fx2_high >= fx1_high and fx2_low <= fx1_low):
            return True
        
        return False

    def _is_fx_confirmed_by_subsequent_movement(self, start_fx: FX, end_fx: FX, bi_type: str) -> bool:
        """
        检查分型是否可以作为有效的笔结束点
        根据缠论理论，只有被后续走势确认的分型才可作为笔的转折点
        
        核心规则：
        1. 如果后续出现同类型但更极值的分型，则当前分型无效（趋势延续）
        2. 如果后续出现反向分型并形成有效确认，则当前分型有效
        3. 保守处理：在没有明确反证时，认为分型有效
        """
        end_fx_idx = end_fx.index
        
        # 0. 检查分型是否被后续K线直接破坏（针对最后一笔未完成情况）
        # 如果分型被后续K线突破（顶分型后有更高High，底分型后有更低Low），则分型无效
        # 只需要检查到下一个分型之前的K线即可，或者如果没有下一个分型，则检查所有后续K线
        # 注意：只针对最后一个分型进行此检查，防止历史分型被未来数据破坏
        if end_fx_idx == len(self.fxs) - 1:
            check_start_k_idx = end_fx.k.index + 1
            for i in range(check_start_k_idx, len(self.cl_klines)):
                ck = self.cl_klines[i]
                if end_fx.type == "ding" and ck.h > end_fx.val:
                    return False
                if end_fx.type == "di" and ck.l < end_fx.val:
                    return False

        # 检查后续分型
        for i in range(end_fx_idx + 1, len(self.fxs)):
            next_fx = self.fxs[i]
            
            # 同类型分型检查：趋势延续
            if next_fx.type == end_fx.type:
                if end_fx.type == "ding" and next_fx.val > end_fx.val:
                    # 后续出现更高的顶分型，说明趋势在延续，当前顶分型无效
                    return False
                elif end_fx.type == "di" and next_fx.val < end_fx.val:
                    # 后续出现更低的底分型，说明趋势在延续，当前底分型无效
                    return False
            else:
                # 反向分型检查：趋势可能反转
                # 只有当反向分型能够形成有效的一笔时，才算真正确认
                if self._check_bi_len_valid(end_fx, next_fx, bi_type):
                    if end_fx.type == "ding" and next_fx.type == "di" and next_fx.val < end_fx.val:
                        # 顶分型被更低的底分型确认，有效
                        return True
                    elif end_fx.type == "di" and next_fx.type == "ding" and next_fx.val > end_fx.val:
                        # 底分型被更高的顶分型确认，有效
                        return True
                else:
                    # 反向分型虽然方向正确，但距离不够，不能作为确认信号
                    # 继续向后寻找，防止因伪反向分型导致误判
                    continue
        
        # 如果没有后续分型或无法明确判断
        # 检查是否被后续K线破坏（兜底检查）
        check_start_k_idx = end_fx.k.index + 1
        for i in range(check_start_k_idx, len(self.cl_klines)):
            ck = self.cl_klines[i]
            if end_fx.type == "ding" and ck.h > end_fx.val:
                return False
            if end_fx.type == "di" and ck.l < end_fx.val:
                return False

        # 保守处理：认为分型有效
        return True

    def _is_trend_continuation(self, last_bi: BI, next_fx: FX) -> bool:
        """
        检查趋势是否在延续
        防止在趋势延续过程中过早结束当前笔
        
        规则：
        1. 如果当前是向上笔，且后续出现更高的顶分型，说明趋势在延续
        2. 如果当前是向下笔，且后续出现更低的底分型，说明趋势在延续
        3. 这种情况下不应该结束当前笔
        """
        if last_bi.type == "up" and next_fx.type == "ding":
            # 向上笔趋势中，出现更高的顶分型
            # 检查后续是否还有更高的顶分型
            for i in range(next_fx.index + 1, len(self.fxs)):
                future_fx = self.fxs[i]
                if future_fx.type == "ding" and future_fx.val > next_fx.val:
                    # 后续出现更高的顶分型，说明趋势在延续
                    return True
        elif last_bi.type == "down" and next_fx.type == "di":
            # 向下笔趋势中，出现更低的底分型
            # 检查后续是否还有更低的底分型
            for i in range(next_fx.index + 1, len(self.fxs)):
                future_fx = self.fxs[i]
                if future_fx.type == "di" and future_fx.val < next_fx.val:
                    # 后续出现更低的底分型，说明趋势在延续
                    return True
        
        return False

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
        if len(self.fxs) < 2:
            self.bis = []
            return

        bi_type = self.config["bi_type"]

        # 增量更新：清理失效的笔
        # 笔依赖于分型，如果分型被清理了，对应的笔也要清理
        last_fx_index = self.fxs[-1].index
        while len(self.bis) > 0 and self.bis[-1].end.index > last_fx_index:
             self.bis.pop()
        
        # 检查最后一笔是否被破坏（针对最后一笔结束分型被后续K线破坏的情况）
        if len(self.bis) > 0:
            last_bi = self.bis[-1]
            if not self.check_bi_valid(last_bi.start, last_bi.end, bi_type):
                self.bis.pop()

        # 1. 如果没有笔，则全量计算
        if len(self.bis) == 0:
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
                # 额外检查：趋势延续判断
                # 如果当前笔是向上笔且后续出现更高的顶分型，或向下笔且后续出现更低的底分型
                # 说明趋势在延续，不应该结束当前笔
                if not self._is_trend_continuation(last_bi, next_fx):
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

        # 2025-01-05 Update: 移除“尝试添加未完成的笔”的手动逻辑
        # 根据用户要求：历史走势和当前走势逻辑一致，必须有分型才能生成笔。
        # 原有的“虚拟笔”逻辑（无分型直接连接当前极值）被移除。
        # 主循环已经能够处理最后一个 valid fractal 形成的笔。

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
        - 向上线段：特征序列为下笔，处理方向为"up"（取高高，因为向上线段中下笔应该越来越高）
        - 向下线段：特征序列为上笔，处理方向为"down"（取低低，因为向下线段中上笔应该越来越低）
        """
        # 特征序列方向与线段方向相反
        # 2025修订：修正包含处理方向
        # 向上线段(xd_dir='up') -> 特征序列(下笔) -> 期望上涨 -> 包含处理取up (High-High)
        # 向下线段(xd_dir='down') -> 特征序列(上笔) -> 期望下跌 -> 包含处理取down (Low-Low)
        bh_direction = "up" if xd_dir == "up" else "down"
        
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
        只检查序列末尾的三个元素是否构成分型，避免重复检测旧分型
        """
        if len(feature_sequence) < 3:
            return None
            
        # 只检查最后三个元素
        t1, t2, t3 = feature_sequence[-3], feature_sequence[-2], feature_sequence[-1]
        
        fx_type = None
        if xd_dir == "up":
            # 向上线段，特征序列(下笔)找顶分型
            # 严格缠论分型条件：中间元素最高，两边元素较低
            # 2025-12-26 修正：线段的序列分型要求K线与左右2边比，顶分型高点最高低点也是最高
            if t2.max > t1.max and t2.max > t3.max and t2.min > t1.min and t2.min > t3.min:
                fx_type = "ding"
        else:
            # 向下线段，特征序列(上笔)找底分型
            # 严格缠论分型条件：中间元素最低，两边元素较高
            # 2025-12-26 修正：线段的序列分型要求K线与左右2边比，底分型低点最低高点也是最低
            if t2.min < t1.min and t2.min < t3.min and t2.max < t1.max and t2.max < t3.max:
                fx_type = "di"
        
        if fx_type:
            # 2025-12-26 修正：特征序列的分型必须是线段的极值，不能只比较3笔
            # 对于向下线段，分型必须是所有特征序列元素的最低点
            # 对于向上线段，分型必须是所有特征序列元素的最高点
            if fx_type == "ding":
                # 顶分型：必须是全局最高
                if any(tx.max > t2.max for tx in feature_sequence):
                    return None
            elif fx_type == "di":
                # 底分型：必须是全局最低
                if any(tx.min < t2.min for tx in feature_sequence):
                    return None

            # 检查第一与第二元素是否无重叠（缺口）
            # 标准：(b_low > a_high) or (a_low > b_high)
            a_low, a_high = t1.min, t1.max
            b_low, b_high = t2.min, t2.max
            has_gap = (b_low > a_high) or (a_low > b_high)
            return (fx_type, t2, has_gap)
        
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
        修复：
        1. 确保该笔对应特征序列分型的极值点（还原包含处理前的真实极值）
        2. 返回该极值笔在时间序列上的前一笔（即线段方向的最后一笔）
        """
        # fx_tzxl 是分型的中间元素（特征序列元素，即反向笔）
        # 如果发生了包含处理，fx_tzxl.lines 中可能包含多个原始反向笔
        # 我们需要找到构成极值的那一笔反向笔（target_bi）
        
        target_bi = None
        if xd_dir == "up":
            # 向上线段结束于顶分型（由下笔构成的特征序列的顶分型）
            # 我们需要找到中间元素中，起始点（High）最高的那个下笔
            # 这个下笔的起点，就是向上线段的最高点（终点）
            target_bi = max(fx_tzxl.lines, key=lambda b: b.high)
        else:
            # 向下线段结束于底分型（由上笔构成的特征序列的底分型）
            # 我们需要找到中间元素中，起始点（Low）最低的那个上笔
            # 这个上笔的起点，就是向下线段的最低点（终点）
            target_bi = min(fx_tzxl.lines, key=lambda b: b.low)
            
        # 找到这个 target_bi 在全局 bis 列表中的索引
        center_idx = target_bi.index
        
        # 线段结束于 target_bi 的前一笔
        # target_bi 是特征序列元素（反向笔，如X3）
        # 它的前一笔（如S3）是线段方向的笔，且 S3.end == X3.start
        # 所以返回 center_idx - 1，即 S3 的索引
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
                # 0. 优先检查：笔破坏 + 3笔重叠 (文档修正规则)
                # 场景：D(破坏), E, F(当前) 构成3笔重叠，且D破坏了B(前一特征笔)
                # 这是一种特殊的线段结束确认模式，不需要标准特征序列分型
                if i >= start_idx + 4:
                    f_bi = bi
                    e_bi = self.bis[i-1]
                    d_bi = self.bis[i-2]
                    
                    # 检查3笔重叠 (D, E, F)
                    is_overlap = False
                    # 重叠定义：max(lows) <= min(highs)
                    highs = [d_bi.high, e_bi.high, f_bi.high]
                    lows = [d_bi.low, e_bi.low, f_bi.low]
                    if max(lows) <= min(highs):
                        is_overlap = True
                    
                    if is_overlap:
                        # 增加方向性检查：反向线段的第二笔不应创新高/新低
                        # 如果是向上线段结束（寻找向下线段），中间的上笔E不应高于第一笔D的高点
                        if xd_dir == "up" and e_bi.high > d_bi.high:
                            is_overlap = False
                        # 如果是向下线段结束（寻找向上线段），中间的下笔E不应低于第一笔D的低点
                        elif xd_dir == "down" and e_bi.low < d_bi.low:
                            is_overlap = False

                    if is_overlap:
                        # 检查笔破坏：D 跌破 B (对于向上线段)
                        # B 是 bis[i-4] (上一个特征序列笔)
                        # 注意：这里直接比较原始笔，未经过包含处理，符合“笔破坏”定义
                        b_bi = self.bis[i-4]
                        
                        is_break = False
                        if xd_dir == "up":
                            if d_bi.low < b_bi.low:
                                is_break = True
                        else:
                            if d_bi.high > b_bi.high:
                                is_break = True
                                
                        if is_break:
                            # 确认线段结束于 C (D的起点)
                            # C 是 bis[i-3]
                            end_idx = i - 3

                            # 修复：笔破坏场景下，如果被破坏的前一特征笔(B)的极值点比破坏笔(D)的起点更极值，
                            # 则线段应该结束于 B 的起点（即 i-5），而不是 C (i-3)。
                            # 场景：向下线段，B(Up) Start=Low1, D(Up) Start=Low2. Low1 < Low2.
                            # D 破坏 B，确认反转，但反转点（最低点）是 Low1。
                            if i - 5 > start_idx:
                                if xd_dir == "up": # 向上线段，找最高点
                                    # b_bi.start.val (High1) vs d_bi.start.val (High2)
                                    if b_bi.start.val > d_bi.start.val:
                                        end_idx = i - 5
                                else: # 向下线段，找最低点
                                    # b_bi.start.val (Low1) vs d_bi.start.val (Low2)
                                    if b_bi.start.val < d_bi.start.val:
                                        end_idx = i - 5
                            
                            # 修复：如果有待确认的更极值的分型，应该优先使用待确认的分型作为结束点
                            # 场景：最高点A -> 回调 -> 次高点B -> 破位下跌(D)
                            # 此时 D 破坏了前面的结构，确认线段结束。
                            # 但线段真正的结束点应该是 A，而不是 B (D的起点)。
                            if pending_gap_fx:
                                use_pending = False
                                if xd_dir == "up" and pending_gap_fx['fx_tzxl'].max > self.bis[end_idx].end.val:
                                    use_pending = True
                                elif xd_dir == "down" and pending_gap_fx['fx_tzxl'].min < self.bis[end_idx].end.val:
                                    use_pending = True
                                
                                if use_pending:
                                    return self._create_xd_end_result(
                                        pending_gap_fx['fx_type'],
                                        pending_gap_fx['fx_tzxl'],
                                        pending_gap_fx['xls'],
                                        pending_gap_fx['end_bi_idx']
                                    )
                            
                            # 构造虚拟的分型结果用于返回
                            # 使用 D 作为分型极值点（虽然它是破坏笔，但在逻辑上它终结了线段）
                            # 或者更准确地，线段结束于 C。
                            # 我们创建一个基于 C 点前后的虚拟分型结构
                            
                            # 构造 TZXL 元素用于创建 XLFX
                            # 注意：这里主要是为了满足返回类型，核心是 end_idx
                            tzxl_d = self._create_feature_sequence_element(d_bi, xd_dir)
                            
                            # 递归构造结果
                            return self._create_xd_end_result(
                                "ding" if xd_dir == "up" else "di",
                                tzxl_d, # 使用 D 作为特征分型元素
                                [tzxl_d], # 虚拟列表
                                end_idx
                            )

                # 检查是否满足无缺口分型的提前确认条件 (3笔破坏)
                # 场景：Pending分型(U1) -> CheckSeq(D1) -> Current(U2)
                # 修改：取消对 has_gap 的限制，允许有缺口分型也通过此逻辑确认（强分型+3笔确认）
                if pending_gap_fx and pending_gap_fx['has_gap']:
                    check_seq = pending_gap_fx['check_sequence']
                    if len(check_seq) >= 1:
                        # 此时构成了 U1-D1-U2 的三笔结构
                        # 只要 U2 不创新低(对于底分型) 或 不创新高(对于顶分型)，则反向线段成立
                        is_valid_reverse = False
                        pen3 = bi
                        
                        # 获取新线段的第一笔（即原分型的最后一笔）
                        # pending_gap_fx['xls'][-1] 是特征序列元素，取其包含的最后一笔
                        pen1 = pending_gap_fx['xls'][-1].lines[-1]

                        if xd_dir == "down": # 原向下，新线段向上
                             # 底分型，要求 U2.low >= Fractal_Min (不创新低)
                             # 且要求 pen1 和 pen3 有重叠（构成线段基本条件）
                             if pen3.low >= pending_gap_fx['fx_tzxl'].min and pen3.low <= pen1.high:
                                 is_valid_reverse = True
                        else: # 原向上，新线段向下
                             # 顶分型，要求 U2.high <= Fractal_Max (不创新高)
                             # 且要求 pen1 和 pen3 有重叠
                             if pen3.high <= pending_gap_fx['fx_tzxl'].max and pen3.high >= pen1.low:
                                 is_valid_reverse = True
                        
                        if is_valid_reverse:
                            return self._create_xd_end_result(
                                pending_gap_fx['fx_type'],
                                pending_gap_fx['fx_tzxl'],
                                pending_gap_fx['xls'],
                                pending_gap_fx['end_bi_idx']
                            )

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
                # 修改：无论是否有待确认分型，都检查是否有新的分型形成，并进行择优替换
                if len(feature_sequence) >= 3:
                    fx_result = self._check_feature_sequence_fracture(feature_sequence, xd_dir)
                    if fx_result:
                        fx_type, fx_tzxl, has_gap = fx_result
                        end_bi_idx = self._get_xd_end_bi_index(fx_tzxl, xd_dir)
                        
                        # 必须确保分型位置在 start_idx 之后
                        if end_bi_idx > start_idx:
                            # 判断是否需要更新 pending_gap_fx
                            should_update = False
                            if pending_gap_fx is None:
                                should_update = True
                            else:
                                # 比较新分型与当前 pending 分型的极值
                                # 向下线段(xd_dir='down') -> 找底分型 -> 更低更好
                                if xd_dir == "down":
                                    if fx_tzxl.min < pending_gap_fx['fx_tzxl'].min:
                                        should_update = True
                                # 向上线段(xd_dir='up') -> 找顶分型 -> 更高更好
                                else:
                                    if fx_tzxl.max > pending_gap_fx['fx_tzxl'].max:
                                        should_update = True
                            
                            if should_update:
                                # 更新待确认分型，并重置验证序列
                                pending_gap_fx = {
                                    'fx_type': fx_type,
                                    'fx_tzxl': fx_tzxl,
                                    'xls': feature_sequence[-3:].copy(),
                                    'end_bi_idx': end_bi_idx,
                                    'check_sequence': [], # 初始化序列B
                                    'has_gap': has_gap # 记录是否有缺口
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
        
        # 修复：如果在数据末尾有待确认的分型，尝试直接确认
        # 场景：数据结束时，虽然SequenceB没有形成分型，但Pending分型已经形成且是目前最优解
        # 对于实时分析，展示这个分型比一直显示“未完成”更有价值
        # 2025-01-05 Update: 恢复此逻辑，并确保主循环正确处理
        # 用户要求逻辑一致，只要有特征序列分型即可，不需要未来线段确认。
        # 这里的 pending_gap_fx 就是已经形成的特征序列分型（只是没被后续反向线段确认）。
        # 对于当前线段，这就是它的结束点。
        if pending_gap_fx:
            return self._create_xd_end_result(
                pending_gap_fx['fx_type'],
                pending_gap_fx['fx_tzxl'],
                pending_gap_fx['xls'],
                pending_gap_fx['end_bi_idx']
            )

        return None

    def _check_fractal_for_unfinished_xd(self, start_idx: int, end_bi_idx: int, xd_dir: str) -> Union[Tuple[XLFX, XLFX], None]:
        """
        检查未完成线段的结束点是否构成了特征序列分型
        """
        feature_sequence = []
        target_end_idx = end_bi_idx
        
        # 我们需要扫描到 end_bi_idx 之后的笔，以构建特征序列
        # 特征序列元素必须包含 end_bi_idx 之后的反向笔
        # 比如向上线段，结束于 end_bi (Up)，我们需要检查 end_bi 之后的 Down 笔是否构成特征序列顶分型
        
        # 从线段起点开始构建特征序列，直到覆盖到 end_bi_idx 后面
        scan_end_idx = min(len(self.bis), end_bi_idx + 5) # 只需要往后多看几笔
        
        for i in range(start_idx + 1, scan_end_idx):
            bi = self.bis[i]
            
            # 只关心特征序列笔 (反向笔)
            if bi.type != xd_dir:
                tzxl = self._create_feature_sequence_element(bi, xd_dir)
                
                if feature_sequence:
                    last_tzxl = feature_sequence[-1]
                    if self._is_feature_sequence_included(tzxl, last_tzxl, xd_dir):
                        self._process_feature_sequence_inclusion(tzxl, last_tzxl, xd_dir)
                    else:
                        feature_sequence.append(tzxl)
                else:
                    feature_sequence.append(tzxl)
                
                # 检查分型
                if len(feature_sequence) >= 3:
                    fx_result = self._check_feature_sequence_fracture(feature_sequence, xd_dir)
                    if fx_result:
                        fx_type, fx_tzxl, _ = fx_result
                        
                        # 检查这个分型是否对应我们的 end_bi_idx
                        # 分型的中间元素 fx_tzxl 应该对应 end_bi_idx 之后的那个反向笔
                        # fx_tzxl.lines 包含了原始笔。我们需要找到其中的关键笔。
                        # 对于向上线段，特征序列顶分型，中间元素是下笔。
                        # 这个下笔的起点，应该是线段的最高点。
                        # 也就是该下笔的前一笔，应该是 end_bi。
                        
                        target_bi = None
                        if xd_dir == "up":
                            # 顶分型，取最高的下笔 (Start High)
                            target_bi = max(fx_tzxl.lines, key=lambda b: b.high)
                        else:
                            # 底分型，取最低的上笔 (Start Low)
                            target_bi = min(fx_tzxl.lines, key=lambda b: b.low)
                            
                        # target_bi 的前一笔的索引
                        calc_end_idx = target_bi.index - 1
                        
                        if calc_end_idx == end_bi_idx:
                            # 匹配成功！
                            if fx_type == "ding":
                                return (self._create_xlfx("ding", fx_tzxl, feature_sequence[-3:]), None)
                            else:
                                return (None, self._create_xlfx("di", fx_tzxl, feature_sequence[-3:]))
        
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
        修复：支持多段未完成线段的处理，确保线段方向交替，并且正确处理数据末端的破坏笔
        """
        # 防御性编程：如果已经有未完成的线段，不再添加
        if len(self.xds) > 0 and not self.xds[-1].done:
            return

        start_bi_idx = 0
        current_type = None

        if len(self.xds) > 0:
            last_xd = self.xds[-1]
            if last_xd.end_line.index < len(self.bis) - 1:
                start_bi_idx = last_xd.end_line.index + 1
                current_type = "down" if last_xd.type == "up" else "up"
            else:
                return
        else:
            if len(self.bis) > 0:
                start_bi_idx = 0
                current_type = self.bis[0].type
            else:
                return

        while start_bi_idx < len(self.bis):
            # 寻找当前方向的极值笔
            end_bi_idx = start_bi_idx
            
            check_bis = self.bis[start_bi_idx:]
            if current_type == "up":
                # 向上线段，找最高点（且笔的方向必须为up）
                # 注意：bis是交替的，所以 check_bis[0], check_bis[2]... 是 up
                # 我们需要在这些 up 笔中找 high 最大的
                max_high = -float('inf')
                for i, bi in enumerate(check_bis):
                    if bi.type == "up":
                        if bi.high >= max_high:
                            max_high = bi.high
                            end_bi_idx = start_bi_idx + i
            else:
                # 向下线段，找最低点（且笔的方向必须为down）
                min_low = float('inf')
                for i, bi in enumerate(check_bis):
                    if bi.type == "down":
                        if bi.low <= min_low:
                            min_low = bi.low
                            end_bi_idx = start_bi_idx + i
            
            start_bi = self.bis[start_bi_idx]
            end_bi = self.bis[end_bi_idx]
            
            xd = XD(
                start=start_bi.start,
                end=end_bi.end,
                start_line=start_bi,
                end_line=end_bi,
                _type=current_type,
                ding_fx=None,
                di_fx=None,
                index=len(self.xds)
            )
            
            # 计算高低点
            # 注意：线段的高低点范围覆盖了 start_bi 到 end_bi 之间的所有笔
            segment_bis = self.bis[start_bi_idx : end_bi_idx + 1]
            if current_type == "up":
                xd.high = max([b.high for b in segment_bis])
                xd.low = start_bi.start.val
            else:
                xd.high = start_bi.start.val
                xd.low = min([b.low for b in segment_bis])
                
            # 尝试检查是否存在特征序列分型，以支持 MMD 计算
            # 注意：这不会改变 xd.done 状态，仅用于 MMD
            fractal_check = self._check_fractal_for_unfinished_xd(start_bi_idx, end_bi_idx, current_type)
            if fractal_check:
                xd.ding_fx, xd.di_fx = fractal_check
            
            xd.done = False
            self.xds.append(xd)
            
            # 准备下一轮
            start_bi_idx = end_bi_idx + 1
            current_type = "down" if current_type == "up" else "up"

    def _cal_xd(self):
        """
        严格按照缠论理论划分线段，基于特征序列分型
        修复：添加基本重叠条件检查，修正线段结束点判断
        """
        if len(self.bis) < 3:
            self.xds = []
            return

        # 增量更新：清理未完成的线段和失效线段
        while len(self.xds) > 0 and not self.xds[-1].done:
            self.xds.pop()
            
        # 检查线段是否有效（依赖的笔是否被清理）
        while len(self.xds) > 0 and self.xds[-1].end_line.index >= len(self.bis):
             self.xds.pop()

        # 严格按照缠论理论进行线段划分
        i = 0
        if len(self.xds) > 0:
             i = self.xds[-1].end_line.index + 1
             
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
            # 注意：必须从线段起始位置开始扫描特征序列，否则会遗漏前面的特征元素，导致无法识别早期的分型
            xd_end_info = self._find_xd_end_by_feature_sequence(i, xd_dir)
            if xd_end_info is None:
                # 没有找到线段结束，继续向后寻找
                i += 1
                continue
            
            end_bi_idx, ding_fx, di_fx = xd_end_info
            
            # 确保结束点在基本重叠结构之后（线段定义：至少三笔）
            if end_bi_idx < first_overlap_idx:
                i += 1
                continue
                
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
            
            # 2025-01-05 Update: 标记是否为"已完成"
            # 严格来说，最后一个线段如果没有被反向线段确认，它在 Chanlun 意义上是"未完成"的
            # 但它有完整的特征序列分型。
            # 我们可以通过判断它是否是最后一个线段，且结束于数据末尾来标记 done=False?
            # 或者，如果 xd_end_info 是由 fallback (pending_gap_fx) 返回的，它就是未确认的。
            # 由于 _find_xd_end_by_feature_sequence 没有返回 flag，我们这里做个简单的判断：
            # 如果 end_bi 是 bis 列表中的倒数第几个，且后面没有足够的笔形成反向分型，那它就是 provisional.
            # 不过，cl_interface 定义 done 为 "是否完成"，通常指分型是否完成。
            # 特征序列分型已经完成，所以 done=True 是合理的 (structurally complete)。
            # 用户抱怨的是 "Figure 2 marked Done is wrong" (Bi case, no fractal).
            # 这里是有 Fractal 的，所以 mark Done 应该是对的。
            # 之前的问题是 "Virtual Pen" 没有 Fractal 也 mark Done。
            
            self.xds.append(xd)
            
            # 下一段从结束笔的下一笔开始
            i = end_bi_idx + 1
        
        # 处理未完成线段
        # 2025-01-05 Update: 移除手动处理未完成线段的逻辑
        # 主循环已经能够处理以特征序列分型结束的线段。
        # 如果没有特征序列分型，则不生成线段。
        # self._handle_unfinished_xd()

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
            if len(_lines) < 5:
                return zss
            i = 1
            while i <= len(_lines) - 3:
                l1, l2, l3 = _lines[i], _lines[i + 1], _lines[i + 2]
                zg = min(l1.high, l2.high, l3.high)
                zd = max(l1.low, l2.low, l3.low)
                if zg > zd:
                    # Check overlap ratio > 1/3 of fluctuation range
                    overlap_height = zg - zd
                    total_range = max(l1.high, l2.high, l3.high) - min(l1.low, l2.low, l3.low)
                    if total_range > 0 and overlap_height <= total_range * (1/3):
                        i += 1
                        continue

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
                    
                    has_leaving = False
                    # 段内延伸
                    while j < len(_lines):
                        ln = _lines[j]
                        
                        # Check if line leaves the [zd, zg] range
                        # If it ends outside the range, it is considered leaving
                        is_leaving = False
                        if ln.type == "up":
                            if ln.high > zg:
                                is_leaving = True
                        else:
                            if ln.low < zd:
                                is_leaving = True

                        if not is_leaving:
                            # 检查是否可以作为延伸
                            # 用户要求：如果不完全重叠或重叠很小，不应算作延伸
                            # 增加一个检查：如果线段大部分在区间外，则不算延伸
                            
                            # 计算重叠部分长度
                            overlap_high = min(ln.high, zg)
                            overlap_low = max(ln.low, zd)
                            overlap_len = max(0, overlap_high - overlap_low)
                            ln_len = ln.high - ln.low
                            
                            # 如果重叠长度小于线段长度的一半，认为重叠不足，不算延伸
                            # 这将导致循环中断，has_leaving 为 False，从而丢弃该中枢
                            if ln_len > 0 and overlap_len < ln_len * 0.5:
                                break

                            zs.lines.append(ln)
                            zs.end = ln.end
                            if ln.high > zs.gg:
                                zs.gg = ln.high
                            if ln.low < zs.dd:
                                zs.dd = ln.low
                            j += 1
                        else:
                            has_leaving = True
                            break
                    
                    # 必须要有离开段，才算有效中枢
                    if not has_leaving:
                         # 如果没有离开段，且已经到了最后，说明中枢未完成，或者不是中枢
                         # 根据需求 "应是从中枢离开才叫中枢"，这里我们不保存未完成的中枢
                         i += 1
                         continue

                    # 检查是否满足5笔条件 (进入1 + 中枢3+ + 离开1)
                    # 虽然i从1开始已经保证了进入段，这里再确认一下
                    # has_leaving保证了离开段
                    # zs.lines至少3笔
                    # 所以总数至少 1 + 3 + 1 = 5
                    
                    zss.append(zs)
                    # 下一个中枢寻找，从离开段的下一段开始（或者离开段作为可能的进入段）
                    # j 是离开段的索引。离开段是 _lines[j]
                    # 下一次寻找中枢，i 应该指向 _lines[j+1] (作为新中枢的第一笔，lines[j]作为进入段)
                    i = j + 1
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
        # 1. 第一类买卖点
        # 趋势背驰
        check_qs_1mmd = str(self.config.get("cl_mmd_cal_qs_1mmd", "1")) == "1"
        # 非趋势，3买卖点后，新高/新低且背驰
        check_not_qs_3mmd_1mmd = str(self.config.get("cl_mmd_cal_not_qs_3mmd_1mmd", "1")) == "1"
        # 趋势，3买卖点后，新高/新低且背驰
        check_qs_3mmd_1mmd = str(self.config.get("cl_mmd_cal_qs_3mmd_1mmd", "1")) == "1"

        # 2. 第二类买卖点
        check_2buy = self.config.get("mmd_2buy_bc", True)
        check_2sell = self.config.get("mmd_2sell_bc", True)
        
        # 3. 第三类买卖点
        check_3buy = self.config.get("mmd_3buy_bc", True)
        check_3sell = self.config.get("mmd_3sell_bc", True)
        
        # 4. 类买卖点
        check_l2buy = self.config.get("mmd_l2buy_bc", True)
        check_l2sell = self.config.get("mmd_l2sell_bc", True)
        check_l3buy = self.config.get("mmd_l3buy_bc", True)
        check_l3sell = self.config.get("mmd_l3sell_bc", True)

        for i in range(len(self.bis)):
            bi = self.bis[i]
            
            # 检查笔是否有效：未完成的笔，如果没有形成 valid 的分型（end.klines < 3），不进行买卖点计算
            # 只有当笔的分型构造完成后（至少3根K线），才认为具备判断买卖点的基础
            # 2025-01-05 Update: 增加严格的几何形态检查，确保未完成笔的端点具备分型结构
            if not bi.is_done():
                if not bi.end.klines or len(bi.end.klines) < 3:
                    continue
                # 严格检查分型几何形态
                k1, k2, k3 = bi.end.klines[0], bi.end.klines[1], bi.end.klines[2]
                if bi.end.type == "ding":
                    # 顶分型：中间高点最高
                    if not (k2.h > k1.h and k2.h > k3.h):
                        continue
                elif bi.end.type == "di":
                    # 底分型：中间低点最低
                    if not (k2.l < k1.l and k2.l < k3.l):
                        continue

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

            # 1.1 无背驰加速赶顶/赶底识别 (New Feature)
            check_no_qs_1buy_acc = str(self.config.get("cl_mmd_cal_no_qs_1buy_acc", "1")) == "1"
            check_no_qs_1sell_acc = str(self.config.get("cl_mmd_cal_no_qs_1sell_acc", "1")) == "1"
            
            # 只要有分型即可，不需要等待笔确认 (bi.is_done)
            if bi.end and len(bi.end.klines) >= 3:
                fx_k1 = bi.end.klines[0]
                fx_k2 = bi.end.klines[1]
                fx_k3 = bi.end.klines[2]

                # 辅助函数：计算斜率
                def cal_slope(k_start, k_end, mode="high"):
                    if not k_start or not k_end: return 0
                    idx_diff = abs(k_end.index - k_start.index)
                    if idx_diff == 0: return 0
                    
                    val_diff = 0
                    if mode == "high":
                        val_diff = abs(k_end.h - k_start.h)
                    elif mode == "low":
                        val_diff = abs(k_end.l - k_start.l)
                    else: # close
                        val_diff = abs(k_end.c - k_start.c)
                    
                    return val_diff / idx_diff

                # 1. 加速赶顶无背驰一卖
                if check_no_qs_1sell_acc and bi.type == "up":
                    # 条件1：强顶分型
                    k3_close = fx_k3.klines[-1].c if fx_k3.klines else fx_k3.c
                    is_strong_top = fx_k3.l < fx_k1.l and k3_close <= fx_k1.l

                    # 条件2：顶分型后K线斜率 > 前K线斜率
                    # 分型中间K线前3-5根 vs 后3-5根
                    is_slope_acc = False
                    
                    slope_pre_max = 0
                    slope_post_max = 0
                    
                    # Pre-check (3 to 5 bars back)
                    for offset in range(3, 6):
                        pre_idx = fx_k2.index - offset
                        if pre_idx >= 0 and pre_idx < len(self.cl_klines):
                            k_pre = self.cl_klines[pre_idx]
                            s = cal_slope(k_pre, fx_k2, "high")
                            if s > slope_pre_max: slope_pre_max = s
                    
                    # Post-check (3 to 5 bars forward)
                    current_max_idx = len(self.cl_klines) - 1
                    for offset in range(3, 6):
                        post_idx = fx_k2.index + offset
                        if post_idx <= current_max_idx:
                            k_post = self.cl_klines[post_idx]
                            s = cal_slope(fx_k2, k_post, "low")
                            if s > slope_post_max: slope_post_max = s
                    
                    if slope_post_max > slope_pre_max and slope_post_max > 0:
                        is_slope_acc = True

                    # 1 OR 2 满足其一
                    if is_strong_top or is_slope_acc:
                        valid_zss = [z for z in zss if z.lines and z.lines[-1].index < bi.index]
                        target_zs = valid_zss[-1] if valid_zss else None
                        if target_zs:
                            msg = "加速无背驰一卖"
                            if is_strong_top: msg += "(强顶分)"
                            if is_slope_acc: msg += "(斜率加速)"
                            bi.add_mmd("1sell", target_zs, zs_type, msg)

                # 2. 加速赶底无背驰一买
                if check_no_qs_1buy_acc and bi.type == "down":
                    # 条件1：强底分型
                    is_strong_bottom = fx_k3.h > fx_k1.h
                    
                    # 条件2：底分型后K线斜率 > 前K线斜率
                    is_slope_acc = False
                    slope_pre_max = 0
                    slope_post_max = 0
                    
                    for offset in range(3, 6):
                        pre_idx = fx_k2.index - offset
                        if pre_idx >= 0 and pre_idx < len(self.cl_klines):
                            k_pre = self.cl_klines[pre_idx]
                            s = cal_slope(k_pre, fx_k2, "low")
                            if s > slope_pre_max: slope_pre_max = s
                            
                    current_max_idx = len(self.cl_klines) - 1
                    for offset in range(3, 6):
                        post_idx = fx_k2.index + offset
                        if post_idx <= current_max_idx:
                            k_post = self.cl_klines[post_idx]
                            s = cal_slope(fx_k2, k_post, "high")
                            if s > slope_post_max: slope_post_max = s

                    if slope_post_max > slope_pre_max and slope_post_max > 0:
                        is_slope_acc = True

                    if is_strong_bottom or is_slope_acc:
                        valid_zss = [z for z in zss if z.lines and z.lines[-1].index < bi.index]
                        target_zs = valid_zss[-1] if valid_zss else None
                        if target_zs and bi.low < target_zs.zd:
                            msg = "加速无背驰一买"
                            if is_strong_bottom: msg += "(强底分)"
                            if is_slope_acc: msg += "(斜率加速)"
                            bi.add_mmd("1buy", target_zs, zs_type, msg)

            # 2. 基于中枢的买卖点识别
            for zs in zss:
                if not zs.real:
                    continue
                
                # 判断当前中枢是否是趋势中枢
                valid_zss = [z for z in zss if z.index <= zs.index]
                is_qs_trend = False
                if len(valid_zss) >= 2:
                    trend_type = self.zss_is_qs(valid_zss[-2], valid_zss[-1])
                    if trend_type:
                        is_qs_trend = True

                # 第三类买卖点：离开中枢后回抽不进入中枢
                # 检查是否离开中枢
                if i >= 1:
                    prev_bi = self.bis[i-1]
                    if prev_bi.start.index == zs.end.index:
                        if check_3buy and bi.type == "down" and bi.low > zs.zg and bi.low > zs.lines[-1].high:
                            bi.add_mmd("3buy", zs, zs_type)
                        if check_3sell and bi.type == "up" and bi.high < zs.zd and bi.high < zs.lines[-1].low:
                            bi.add_mmd("3sell", zs, zs_type)
                
                # 盘整背驰：离开段与进入段比较
                is_pz, compare_line = self.beichi_pz(zs, bi)
                if is_pz:
                    bi.add_bc("pz", zs, compare_line, [compare_line], True, zs_type)
                
                # 趋势背驰：需要至少两个中枢形成趋势
                is_qs, compare_lines = self.beichi_qs(self.bis, valid_zss, bi)
                if is_qs and zs.lines[-1].index < bi.index:
                    bi.add_bc("qs", zs, None, compare_lines, True, zs_type)
                    
                    # 第一类买卖点：趋势背驰
                    if check_qs_1mmd:
                        if bi.type == "down" and bi.low < zs.zd:
                            bi.add_mmd("1buy", zs, zs_type)
                        if bi.type == "up" and bi.high > zs.zg:
                            bi.add_mmd("1sell", zs, zs_type)
            
            # 补充：基于3买卖点后的背驰产生的1买卖点
            if i >= 2:
                prev_bi = self.bis[i-1]
                prev_bi_2 = self.bis[i-2]
                
                # 检查前一笔是否有3买卖点
                prev_mmds = prev_bi.get_mmds(zs_type)
                
                # 3卖后背驰 -> 1买
                has_3sell = [m for m in prev_mmds if m.name == "3sell"]
                if has_3sell and bi.type == "down" and bi.low < prev_bi_2.low:
                    # 检查力度背驰
                    ld_prev = prev_bi_2.get_ld(self)["macd"]
                    ld_now = bi.get_ld(self)["macd"]
                    if compare_ld_beichi(ld_prev, ld_now, "down"):
                        # 确定是趋势还是非趋势
                        # 获取3卖对应的中枢
                        zs = has_3sell[0].zs
                        # 判断该中枢是否形成趋势
                        valid_zss = [z for z in zss if z.index <= zs.index]
                        is_qs_trend = False
                        if len(valid_zss) >= 2:
                            t_type = self.zss_is_qs(valid_zss[-2], valid_zss[-1])
                            if t_type: is_qs_trend = True
                        
                        if (is_qs_trend and check_qs_3mmd_1mmd) or (not is_qs_trend and check_not_qs_3mmd_1mmd):
                            bi.add_mmd("1buy", zs, zs_type)
                            # 也可以标记为QS背驰，虽然定义上可能略有不同，但为了图表展示一致性
                            bi.add_bc("qs", zs, None, [prev_bi_2], True, zs_type)

                # 3买后背驰 -> 1卖
                has_3buy = [m for m in prev_mmds if m.name == "3buy"]
                if has_3buy and bi.type == "up" and bi.high > prev_bi_2.high:
                    # 检查力度背驰：当前段与3买前的离开段比较 (Trend Divergence logic)
                    ld_prev = prev_bi_2.get_ld(self)["macd"]
                    ld_now = bi.get_ld(self)["macd"]
                    bc_trend = compare_ld_beichi(ld_prev, ld_now, "up")
                    
                    # 检查盘整背驰：当前段与中枢进入段比较 (PZ BC logic)
                    zs = has_3buy[0].zs
                    is_pz, _ = self.beichi_pz(zs, bi)
                    
                    if bc_trend or is_pz:
                        valid_zss = [z for z in zss if z.index <= zs.index]
                        is_qs_trend = False
                        if len(valid_zss) >= 2:
                            t_type = self.zss_is_qs(valid_zss[-2], valid_zss[-1])
                            if t_type: is_qs_trend = True
                            
                        if (is_qs_trend and check_qs_3mmd_1mmd) or (not is_qs_trend and check_not_qs_3mmd_1mmd):
                            bi.add_mmd("1sell", zs, zs_type)
                            bi.add_bc("qs", zs, None, [prev_bi_2], True, zs_type)

            # 3. 第二类买卖点识别
            if i >= 2:
                prev_bi_2 = self.bis[i-2]
                
                # 检查是否有第一类买卖点
                is_1buy = any(m.name == "1buy" for m in prev_bi_2.get_mmds(zs_type))
                is_1sell = any(m.name == "1sell" for m in prev_bi_2.get_mmds(zs_type))
                
                # 检查是否有趋势背驰 (即使没有标记为1buy)
                has_qs_bc = any(b.type == "qs" for b in prev_bi_2.get_bcs(zs_type))

                if check_2buy and bi.type == "down":
                    # 情况1：一类买点后，不创新低
                    if is_1buy and bi.low > prev_bi_2.low:
                        target_zs = prev_bi_2.get_mmds(zs_type)[0].zs
                        bi.add_mmd("2buy", target_zs, zs_type)
                    
                    # 情况2：趋势背驰后，不创新低 (即使没有1buy标记)
                    elif has_qs_bc and bi.low > prev_bi_2.low:
                         for bc in prev_bi_2.get_bcs(zs_type):
                            if bc.type == "qs" and bc.zs is not None:
                                # 再次确认力度衰竭 (可选，但通常二买意味着回调不破低)
                                if compare_ld_beichi(prev_bi_2.get_ld(self), bi.get_ld(self), "down"):
                                    bi.add_mmd("2buy", bc.zs, zs_type)
                                    break
                
                if check_2sell and bi.type == "up":
                    # 情况1：一类卖点后，不创新高
                    if is_1sell and bi.high < prev_bi_2.high:
                        target_zs = prev_bi_2.get_mmds(zs_type)[0].zs
                        bi.add_mmd("2sell", target_zs, zs_type)
                    
                    # 情况2：趋势背驰后，不创新高
                    elif has_qs_bc and bi.high < prev_bi_2.high:
                        for bc in prev_bi_2.get_bcs(zs_type):
                            if bc.type == "qs" and bc.zs is not None:
                                if compare_ld_beichi(prev_bi_2.get_ld(self), bi.get_ld(self), "up"):
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
            check_no_qs_1buy_acc = str(self.config.get("cl_mmd_cal_no_qs_1buy_acc", "1")) == "1"
            check_no_qs_1sell_acc = str(self.config.get("cl_mmd_cal_no_qs_1sell_acc", "1")) == "1"

            # 辅助函数：计算斜率
            def cal_slope(k_start, k_end, mode="high"):
                if not k_start or not k_end: return 0
                idx_diff = abs(k_end.index - k_start.index)
                if idx_diff == 0: return 0
                
                val_diff = 0
                if mode == "high":
                    val_diff = abs(k_end.h - k_start.h)
                elif mode == "low":
                    val_diff = abs(k_end.l - k_start.l)
                else: # close
                    val_diff = abs(k_end.c - k_start.c)
                
                return val_diff / idx_diff

            for i in range(len(self.xds)):
                xd = self.xds[i]
                
                # 检查线段是否有效：未完成的线段，如果没有形成特征序列分型（ding_fx/di_fx 为空），不进行买卖点计算
                # 只有当线段具备特征序列分型或一笔破坏结构时，才认为具备判断买卖点的基础
                if xd.ding_fx is None and xd.di_fx is None:
                    continue

                # 线段的无背驰加速赶顶/赶底识别
                # 线段的买卖点只要有特征序列分型或一笔破坏就可以 (这里简化为只要有分型，即 XD 存在即可)
                # 因为 XD 对象本身就是由特征序列分型确认生成的
                if xd.end and len(xd.end.klines) >= 3:
                    fx_k1 = xd.end.klines[0]
                    fx_k2 = xd.end.klines[1]
                    fx_k3 = xd.end.klines[2]
                    
                    # 1. 加速赶顶无背驰一卖
                    if check_no_qs_1sell_acc and xd.type == "up":
                        # 条件1：强顶分型
                        k3_close = fx_k3.klines[-1].c if fx_k3.klines else fx_k3.c
                        is_strong_top = fx_k3.l < fx_k1.l and k3_close <= fx_k1.l

                        # 条件2：斜率加速
                        is_slope_acc = False
                        slope_pre_max = 0
                        slope_post_max = 0
                        
                        # Pre-check (3 to 5 bars back)
                        for offset in range(3, 6):
                            pre_idx = fx_k2.index - offset
                            if pre_idx >= 0 and pre_idx < len(self.cl_klines):
                                k_pre = self.cl_klines[pre_idx]
                                s = cal_slope(k_pre, fx_k2, "high")
                                if s > slope_pre_max: slope_pre_max = s
                        
                        # Post-check (3 to 5 bars forward)
                        current_max_idx = len(self.cl_klines) - 1
                        for offset in range(3, 6):
                            post_idx = fx_k2.index + offset
                            if post_idx <= current_max_idx:
                                k_post = self.cl_klines[post_idx]
                                s = cal_slope(fx_k2, k_post, "low")
                                if s > slope_post_max: slope_post_max = s
                        
                        if slope_post_max > slope_pre_max and slope_post_max > 0:
                            is_slope_acc = True

                        if is_strong_top or is_slope_acc:
                            valid_zss = [z for z in xd_zss if z.lines and z.lines[-1].index < xd.index]
                            target_zs = valid_zss[-1] if valid_zss else None
                            if target_zs:
                                msg = "加速无背驰一卖"
                                if is_strong_top: msg += "(强顶分)"
                                if is_slope_acc: msg += "(斜率加速)"
                                xd.add_mmd("1sell", target_zs, zs_xd_type, msg)
                    
                    # 2. 加速赶底无背驰一买
                    if check_no_qs_1buy_acc and xd.type == "down":
                        # 条件1：强底分型
                        is_strong_bottom = fx_k3.h > fx_k1.h
                        
                        # 条件2：斜率加速
                        is_slope_acc = False
                        slope_pre_max = 0
                        slope_post_max = 0
                        
                        for offset in range(3, 6):
                            pre_idx = fx_k2.index - offset
                            if pre_idx >= 0 and pre_idx < len(self.cl_klines):
                                k_pre = self.cl_klines[pre_idx]
                                s = cal_slope(k_pre, fx_k2, "low")
                                if s > slope_pre_max: slope_pre_max = s
                                
                        current_max_idx = len(self.cl_klines) - 1
                        for offset in range(3, 6):
                            post_idx = fx_k2.index + offset
                            if post_idx <= current_max_idx:
                                k_post = self.cl_klines[post_idx]
                                s = cal_slope(fx_k2, k_post, "high")
                                if s > slope_post_max: slope_post_max = s
        
                        if slope_post_max > slope_pre_max and slope_post_max > 0:
                            is_slope_acc = True

                        if is_strong_bottom or is_slope_acc:
                            valid_zss = [z for z in xd_zss if z.lines and z.lines[-1].index < xd.index]
                            target_zs = valid_zss[-1] if valid_zss else None
                            if target_zs and xd.low < target_zs.zd:
                                msg = "加速无背驰一买"
                                if is_strong_bottom: msg += "(强底分)"
                                if is_slope_acc: msg += "(斜率加速)"
                                xd.add_mmd("1buy", target_zs, zs_xd_type, msg)
                
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
                    
                    has_qs_bc = any(b.type == "qs" for b in prev_xd_2.get_bcs(zs_xd_type))

                    if xd.type == "down":
                        if is_1buy and xd.low > prev_xd_2.low:
                            target_zs = prev_xd_2.get_mmds(zs_xd_type)[0].zs
                            xd.add_mmd("2buy", target_zs, zs_xd_type)
                        
                        elif has_qs_bc and xd.low > prev_xd_2.low:
                            for bc in prev_xd_2.get_bcs(zs_xd_type):
                                if bc.type == "qs" and bc.zs is not None:
                                    if compare_ld_beichi(prev_xd_2.get_ld(self), xd.get_ld(self), "down"):
                                        xd.add_mmd("2buy", bc.zs, zs_xd_type)
                                        break
                    
                    if xd.type == "up":
                        if is_1sell and xd.high < prev_xd_2.high:
                            target_zs = prev_xd_2.get_mmds(zs_xd_type)[0].zs
                            xd.add_mmd("2sell", target_zs, zs_xd_type)
                        
                        elif has_qs_bc and xd.high < prev_xd_2.high:
                            for bc in prev_xd_2.get_bcs(zs_xd_type):
                                if bc.type == "qs" and bc.zs is not None:
                                    if compare_ld_beichi(prev_xd_2.get_ld(self), xd.get_ld(self), "up"):
                                        xd.add_mmd("2sell", bc.zs, zs_xd_type)
                                        break
                
                # 第三类买卖点
                for zs in xd_zss:
                    if not zs.real:
                        continue
                    
                    # 检查是否离开中枢
                    if i >= 1:
                        prev_xd = self.xds[i-1]
                        if prev_xd.start.index == zs.end.index:
                            if check_3buy and xd.type == "down" and xd.low > zs.zg and xd.low > zs.lines[-1].high:
                                xd.add_mmd("3buy", zs, zs_xd_type)
                            if check_3sell and xd.type == "up" and xd.high < zs.zd and xd.high < zs.lines[-1].low:
                                xd.add_mmd("3sell", zs, zs_xd_type)
                    
                    # 盘整背驰
                    is_pz, compare_line = self.beichi_pz(zs, xd)
                    if is_pz:
                        xd.add_bc("pz", zs, compare_line, [compare_line], True, zs_xd_type)

                    # 趋势背驰
                    valid_zss = [z for z in xd_zss if z.index <= zs.index]
                    is_qs, compare_lines = self.beichi_qs(self.xds, valid_zss, xd)
                    if is_qs and zs.lines[-1].index < xd.index:
                        xd.add_bc("qs", zs, None, compare_lines, True, zs_xd_type)

                    # 第一类买卖点（趋势背驰）
                    is_qs = any(b.type == "qs" and b.zs.index == zs.index for b in xd.get_bcs(zs_xd_type))
                    if is_qs:
                        if check_qs_1mmd:
                            if xd.type == "down" and xd.low < zs.zd:
                                xd.add_mmd("1buy", zs, zs_xd_type)
                            if xd.type == "up" and xd.high > zs.zg:
                                xd.add_mmd("1sell", zs, zs_xd_type)
            
            # 补充：基于3买卖点后的背驰产生的1买卖点
                if i >= 2:
                    prev_xd = self.xds[i-1]
                    prev_xd_2 = self.xds[i-2]
                    
                    prev_mmds = prev_xd.get_mmds(zs_xd_type)
                    
                # 3卖后背驰 -> 1买
                if i >= 2:
                    has_3sell = [m for m in prev_mmds if m.name == "3sell"]
                    if has_3sell and xd.type == "down" and xd.low < prev_xd_2.low:
                        # 检查力度背驰：当前段与3卖前的离开段比较
                        ld_prev = prev_xd_2.get_ld(self)["macd"]
                        ld_now = xd.get_ld(self)["macd"]
                        bc_trend = compare_ld_beichi(ld_prev, ld_now, "down")

                        # 检查盘整背驰：当前段与中枢进入段比较
                        zs = has_3sell[0].zs
                        is_pz, _ = self.beichi_pz(zs, xd)

                        if bc_trend or is_pz:
                            valid_zss = [z for z in xd_zss if z.index <= zs.index]
                            is_qs_trend = False
                            if len(valid_zss) >= 2:
                                t_type = self.zss_is_qs(valid_zss[-2], valid_zss[-1])
                                if t_type: is_qs_trend = True
                            
                            if (is_qs_trend and check_qs_3mmd_1mmd) or (not is_qs_trend and check_not_qs_3mmd_1mmd):
                                xd.add_mmd("1buy", zs, zs_xd_type)
                                xd.add_bc("qs", zs, None, [prev_xd_2], True, zs_xd_type)

                    # 3买后背驰 -> 1卖
                    has_3buy = [m for m in prev_mmds if m.name == "3buy"]
                    if has_3buy and xd.type == "up" and xd.high > prev_xd_2.high:
                        # 检查力度背驰
                        ld_prev = prev_xd_2.get_ld(self)["macd"]
                        ld_now = xd.get_ld(self)["macd"]
                        bc_trend = compare_ld_beichi(ld_prev, ld_now, "up")
                        
                        # 检查盘整背驰
                        zs = has_3buy[0].zs
                        is_pz, _ = self.beichi_pz(zs, xd)
                        
                        if bc_trend or is_pz:
                            valid_zss = [z for z in xd_zss if z.index <= zs.index]
                            is_qs_trend = False
                            if len(valid_zss) >= 2:
                                t_type = self.zss_is_qs(valid_zss[-2], valid_zss[-1])
                                if t_type: is_qs_trend = True
                            
                            if (is_qs_trend and check_qs_3mmd_1mmd) or (not is_qs_trend and check_not_qs_3mmd_1mmd):
                                xd.add_mmd("1sell", zs, zs_xd_type)
                                xd.add_bc("qs", zs, None, [prev_xd_2], True, zs_xd_type)
                    
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

                # 4. 类买卖点识别
                if i >= 2:
                    prev_xd_2 = self.xds[i-2]
                    prev_xd = self.xds[i-1]
                    
                    if xd.type == "down":
                        # 类二买
                        if check_l2buy:
                            has_2buy_mmds = [m for m in prev_xd_2.get_mmds(zs_xd_type) if m.name == "2buy"]
                            if has_2buy_mmds:
                                zg = min(prev_xd_2.high, prev_xd.high, xd.high)
                                if zg > xd.low and xd.low > prev_xd_2.low:
                                    target_zs = has_2buy_mmds[0].zs
                                    xd.add_mmd("l2buy", target_zs, zs_xd_type)
                        
                        # 类三买
                        if check_l3buy:
                            has_3buy_mmds = [m for m in prev_xd_2.get_mmds(zs_xd_type) if m.name == "3buy"]
                            if has_3buy_mmds:
                                zg = min(prev_xd_2.high, prev_xd.high, xd.high)
                                if zg > xd.low and xd.low > prev_xd_2.low:
                                    target_zs = has_3buy_mmds[0].zs
                                    xd.add_mmd("l3buy", target_zs, zs_xd_type)
                    
                    if xd.type == "up":
                        # 类二卖
                        if check_l2sell:
                            has_2sell_mmds = [m for m in prev_xd_2.get_mmds(zs_xd_type) if m.name == "2sell"]
                            if has_2sell_mmds:
                                zd = max(prev_xd_2.low, prev_xd.low, xd.low)
                                if xd.high > zd and xd.high < prev_xd_2.high:
                                    target_zs = has_2sell_mmds[0].zs
                                    xd.add_mmd("l2sell", target_zs, zs_xd_type)
                        
                        # 类三卖
                        if check_l3sell:
                            has_3sell_mmds = [m for m in prev_xd_2.get_mmds(zs_xd_type) if m.name == "3sell"]
                            if has_3sell_mmds:
                                zd = max(prev_xd_2.low, prev_xd.low, xd.low)
                                if xd.high > zd and xd.high < prev_xd_2.high:
                                    target_zs = has_3sell_mmds[0].zs
                                    xd.add_mmd("l3sell", target_zs, zs_xd_type)

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
        trend_type = self.zss_is_qs(zs1, zs2)
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

    def zss_is_qs(self, one_zs: ZS, two_zs: ZS) -> Union[str, None]:
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
            return "up"
            
        # Down Trend
        is_down = False
        if wzgx == Config.ZS_WZGX_ZGD.value:
            if two_zs.zg < one_zs.zd: is_down = True
        elif wzgx == Config.ZS_WZGX_ZGGDD.value:
            if two_zs.zg < one_zs.dd: is_down = True
        elif wzgx == Config.ZS_WZGX_GD.value:
            if two_zs.gg < one_zs.dd: is_down = True
            
        if is_down:
            return "down"
            
        return None

    def create_dn_zs(self, zs_type: str, lines: List[LINE], max_line_num: int = 999, zs_include_last_line=True) -> List[ZS]:
        """
        根据给定的线列表创建中枢
        """
        zss = []
        if len(lines) < 5:
            return zss
            
        i = 1
        while i <= len(lines) - 3:
            l1 = lines[i]
            l2 = lines[i+1]
            l3 = lines[i+2]
            
            zg = min(l1.high, l2.high, l3.high)
            zd = max(l1.low, l2.low, l3.low)
            
            if zg > zd:
                # Check overlap ratio > 2/3 of fluctuation range
                overlap_height = zg - zd
                total_range = max(l1.high, l2.high, l3.high) - min(l1.low, l2.low, l3.low)
                if total_range > 0 and overlap_height <= total_range * (2/3):
                    i += 1
                    continue

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
                
                j = i + 3
                has_leaving = False
                while j < len(lines):
                    ln = lines[j]
                    if not zs_include_last_line and j == len(lines) - 1:
                        break

                    is_leaving = False
                    if ln.type == "up":
                        if ln.high > zg:
                            is_leaving = True
                    else:
                        if ln.low < zd:
                            is_leaving = True

                    if not is_leaving:
                        # 检查是否可以作为延伸
                        # 用户要求：如果不完全重叠或重叠很小，不应算作延伸
                        # 增加一个检查：如果线段大部分在区间外，则不算延伸
                        
                        # 计算重叠部分长度
                        overlap_high = min(ln.high, zg)
                        overlap_low = max(ln.low, zd)
                        overlap_len = max(0, overlap_high - overlap_low)
                        ln_len = ln.high - ln.low
                        
                        # 如果重叠长度小于线段长度的一半，认为重叠不足，不算延伸
                        # 这将导致循环中断，has_leaving 为 False，从而丢弃该中枢
                        if ln_len > 0 and overlap_len < ln_len * 0.5:
                            break

                        if len(zs.lines) >= max_line_num:
                            break
                        zs.lines.append(ln)
                        zs.end = ln.end
                        if ln.high > zs.gg: zs.gg = ln.high
                        if ln.low < zs.dd: zs.dd = ln.low
                        j += 1
                    else:
                        has_leaving = True
                        break
                
                if not has_leaving:
                    i += 1
                    continue

                zss.append(zs)
                i = j + 1
            else:
                i += 1
        return zss


def create_cl(code: str, frequency: str, config: Union[dict, None] = None) -> CL:
    return CL(code, frequency, config)
