# -*- coding: utf-8 -*-
import datetime
import math
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Tuple, Union

import numpy as np
import pandas as pd

"""
CL_*** 配置项，可以在调用缠论计算时，通过传递 config 变量进行变更，如 config['CL_BI_FX_STRICT'] = True
"""


class Config(Enum):
    """
    缠论配置项
    """

    # K 线类型
    KLINE_TYPE_DEFAULT = "kline_default"  # 默认K线
    KLINE_TYPE_CHANLUN = "kline_chanlun"  # 包含处理后的缠论K线
    # K 线缺口定义 (个人定制，不清楚的使用默认 none 配置)
    KLINE_QK_NONE = "none"
    KLINE_QK_CK = "ck"
    # 分型配置项
    FX_QY_MIDDLE = "fx_qy_middle"  # 分型区间所算的区域，使用分型中间的k线作为分型区间
    FX_QY_THREE = "fx_qy_three"  # 分型区间所算的区域，使用分型三根缠论k线作为区间
    FX_QJ_CK = "fx_qj_ck"  # 用顶底的缠论K线，获取分型区间
    FX_QJ_K = "fx_qj_k"  # 用顶底的原始k线，获取分型区间
    FX_BH_YES = "fx_bh_yes"  # 不判断顶底关系，即接受所有关系
    FX_BH_DINGDI = "fx_bh_dingdi"  # 顶不可以在底中，但底可以在顶中
    FX_BH_DIDING = "fx_bh_diding"  # 底不可以在顶中，但顶可以在底中
    FX_BH_NO_QBH = "fx_bh_no_qbh"  # 不允许前一个分型包含后一个分型
    FX_BH_NO_HBQ = "fx_bh_no_hbq"  # 不允许后一个分型包含前一个分型
    FX_BH_NO = "fx_bh_no"  # 顶不可以在底中，底不可以在顶中
    FX_CD_NO = "fx_cd_no"  # 顶底分型不可重叠

    # 笔配置项
    BI_TYPE_OLD = "bi_type_old"  # 笔类型，使用老笔规则
    BI_TYPE_NEW = "bi_type_new"  # 笔类型，使用新笔规则
    BI_TYPE_JDB = "bi_type_jdb"  # 笔类型，简单笔
    BI_TYPE_DD = "bi_type_dd"  # 笔类型，使用顶底成笔规则
    BI_BZH_NO = "bi_bzh_no"  # 笔标准化，不进行标准化
    BI_BZH_YES = "bi_bzh_yes"  # 笔标准化，进行标准化，画在最高最低上
    BI_QJ_DD = "bi_qj_dd"  # 笔区间，使用起止的顶底点作为区间
    BI_QJ_CK = "bi_qj_ck"  # 笔区间，使用缠论K线的最高最低价作为区间
    BI_QJ_K = "bi_qj_k"  # 笔区间，使用原始K线的最高最低价作为区间
    BI_FX_CHD_YES = "bi_fx_cgd_yes"  # 笔内分型，次高低可以成笔
    BI_FX_CHD_NO = "bi_fx_cgd_no"  # 笔内分型，次高低不可以成笔

    # 线段配置项
    XD_QJ_DD = "xd_qj_dd"  # 线段区间，使用线段的顶底点作为区间
    XD_QJ_CK = "xd_qj_ck"  # 线段区间，使用线段中缠论K线的最高最低作为区间
    XD_QJ_K = "xd_qj_k"  # 线段区间，使用线段中原始K线的最高最低作为区间
    ### 笔破坏定义：线段的结束转折笔超过或低过线段的起始位置
    XD_BI_POHUAI_NO = "no"  # 线段不支持笔破坏
    XD_BI_POHUAI_YES = "yes"  # 线段支持笔破坏
    XD_BI_POHUAI_YES_QK = "yes_qk"  # 线段支持笔破坏（笔内必须有缺口）

    # 走势段配置项
    ZSD_BZH_NO = "zsd_bzh_no"  # TODO 移除配置。走势段不进行标准化
    ZSD_BZH_YES = "zsd_bzh_yes"  # TODO 移除配置。走势段进行标准化
    ZSD_QJ_DD = "zsd_qj_dd"  # 走势段区间，使用线段的顶底点作为区间
    ZSD_QJ_CK = "zsd_qj_ck"  # 走势段区间，使用线段中缠论K线的最高最低作为区间
    ZSD_QJ_K = "zsd_qj_k"  # 走势段区间，使用线段中原始K线的最高最低作为区间

    # 中枢配置项
    ZS_TYPE_BZ = "zs_type_bz"  # 计算的中枢类型，标准中枢，中枢维持的方法
    ZS_TYPE_DN = "zs_type_dn"  # 计算中枢的类型，段内中枢，形成线段内的中枢
    ZS_TYPE_FX = "zs_type_fx"  # 计算中枢的类型，方向中枢，进入与离开线的方向相反，严格的分为上涨与下跌中枢
    ZS_TYPE_FL = "zs_type_fl"  # 计算中枢的类型，分类中枢，段内中枢的优化，包括的在线段转折的中阴中枢
    ZS_QJ_DD = "zs_qj_dd"  # 中枢区间，使用线段的顶底点作为区间
    ZS_QJ_CK = "zs_qj_ck"  # 中枢区间，使用线段中缠论K线的最高最低作为区间
    ZS_QJ_K = "zs_qj_k"  # 中枢区间，使用线段中原始K线的最高最低作为区间
    ZS_CD_THREE = "zs_cd_three"  # 中枢重叠区间依据：中枢重叠区间取前三段的重叠区域
    ZS_CD_MORE = "zs_cd_more"  # 中枢重叠区间依据：中枢重叠区间取中枢所有线段的重叠区域
    ZS_WZGX_ZGD = "zs_wzgx_zgd"  # 判断两个中枢的位置关系，比较方式，zg与zd 宽松比较
    # 判断两个中枢的位置关系，比较方式，zg与dd zd与gg 较为宽松比较
    ZS_WZGX_ZGGDD = "zs_wzgx_zggdd"
    ZS_WZGX_GD = "zs_wzgx_gd"  # 判断两个中枢的位置关系，比较方式，gg与dd 严格比较


class Kline:
    """
    原始K线对象
    """

    def __init__(
        self,
        index: int,
        date: datetime.datetime,
        h: float,
        l: float,
        o: float,
        c: float,
        a: float,
    ):
        self.index: int = index
        self.date: datetime.datetime = date
        self.h: float = h
        self.l: float = l
        self.o: float = o
        self.c: float = c
        self.a: float = a

    def __str__(self):
        return f"index: {self.index} date: {self.date} h: {self.h} l: {self.l} o: {self.o} c:{self.c} a:{self.a}"


class CLKline:
    """
    缠论K线对象
    """

    def __init__(
        self,
        k_index: int,
        date: datetime,
        h: float,
        l: float,
        o: float,
        c: float,
        a: float,
        klines: List[Kline] = None,
        index: int = 0,
        _n: int = 0,
        _q: bool = False,
    ):
        if klines is None:
            klines = []
        self.k_index: int = k_index
        self.date: datetime = date
        self.h: float = h
        self.l: float = l
        self.o: float = o
        self.c: float = c
        self.a: float = a
        self.klines: List[Kline] = klines  # 其中包含K线对象
        self.index: int = index
        self.n: int = _n  # 记录包含的K线数量
        self.q: bool = _q  # 是否有缺口
        self.up_qs = None  # 合并时之前的趋势

    def __str__(self):
        return f"index: {self.index} k_index:{self.k_index} date: {self.date} h: {self.h} l: {self.l} _n:{self.n} _q:{self.q} up_qs:{self.up_qs}"


class FX:
    """
    分型对象
    """

    def __init__(
        self,
        _type: str,
        k: CLKline,
        klines: List[CLKline],
        val: float,
        index: int = 0,
        done: bool = True,
    ):
        self.type: str = _type  # 分型类型 （ding 顶分型 di 底分型）
        self.k: CLKline = k
        self.klines: List[CLKline] = klines
        self.val: float = val
        self.index: int = index
        self.done: bool = done  # 分型是否完成

    def ld(self) -> int:
        """
        分型力度值，数值越大表示分型力度越大
        根据第三根K线与前两根K线的位置关系决定
        """
        ld = 0
        one_k = self.klines[0]
        two_k = self.klines[1]
        three_k = self.klines[2]
        if three_k is None:
            return ld
        if self.klines[0].k_index == -1 or self.klines[-1].k_index == -1:
            return ld
        if self.type == "ding":
            # 第三个缠论K线要一根单阴线
            if len(three_k.klines) > 1:
                return ld
            if three_k.klines[0].c > three_k.klines[0].o:
                return ld
            # 第三个K线的高点，低于第二根的 50% 以下
            if three_k.h < (two_k.h - ((two_k.h - two_k.l) * 0.5)):
                ld += 1
            # 第三个最低点是三根中最低的
            if three_k.l < one_k.l and three_k.l < two_k.l:
                ld += 1
            # 第三根的K线的收盘价要低于前两个K线
            if three_k.klines[0].c < one_k.l and three_k.klines[0].c < two_k.l:
                ld += 1
            # 第三个缠论K线的实体，要大于第二根缠论K线
            if (three_k.h - three_k.l) > (two_k.h - two_k.l):
                ld += 1
            # 第三个K线不能有太大的下影线
            if (three_k.klines[0].h - three_k.klines[0].l) != 0 and (
                three_k.klines[0].c - three_k.klines[0].l
            ) / (three_k.klines[0].h - three_k.klines[0].l) < 0.3:
                ld += 1
        elif self.type == "di":
            # 第三个缠论K线要一根单阳线
            if len(three_k.klines) > 1:
                return ld
            if three_k.klines[0].c < three_k.klines[0].o:
                return ld
            # 第三个K线的低点，高于第二根的 50% 之上
            if three_k.l > (two_k.l + ((two_k.h - two_k.l) * 0.5)):
                ld += 1
            # 第三个最高点是三根中最高的
            if three_k.h > one_k.h and three_k.h > two_k.h:
                ld += 1
            # 第三根的K线的收盘价要高于前两个K线
            if three_k.klines[0].c > one_k.h and three_k.klines[0].c > two_k.h:
                ld += 1
            # 第三个缠论K线的实体，要大于第二根缠论K线
            if (three_k.h - three_k.l) > (two_k.h - two_k.l):
                ld += 1
            # 第三个K线不能有太大的上影线
            if (three_k.klines[0].h - three_k.klines[0].l) != 0 and (
                three_k.klines[0].h - three_k.klines[0].c
            ) / (three_k.klines[0].h - three_k.klines[0].l) < 0.3:
                ld += 1
        return ld

    def high(self, qj_type: str, qy_type: str) -> float:
        """
        获取分型最高点
        """
        if qj_type == Config.FX_QJ_CK.value:
            # （获取缠论K线的最高点）
            if qy_type == Config.FX_QY_MIDDLE.value:
                return self.k.h
            return max([_ck.h for _ck in self.klines if _ck is not None])
        elif qj_type == Config.FX_QJ_K.value:
            # （获取原始K线的最高点）
            if qy_type == Config.FX_QY_MIDDLE.value:
                return max([_k.h for _k in self.k.klines])
            return max(
                [_k.h for _ck in self.klines if _ck is not None for _k in _ck.klines]
            )
        else:
            raise Exception(f"获取分型高点的区间类型错误 {qj_type}")

    def low(self, qj_type: str, qy_type: str) -> float:
        """
        获取分型的最低点（取原始K线的最低点）
        """
        if qj_type == Config.FX_QJ_CK.value:
            if qy_type == Config.FX_QY_MIDDLE.value:
                return self.k.l
            return min([_ck.l for _ck in self.klines if _ck is not None])
        elif qj_type == Config.FX_QJ_K.value:
            if qy_type == Config.FX_QY_MIDDLE.value:
                return min([_k.l for _k in self.k.klines])
            return min(
                [_k.l for _ck in self.klines if _ck is not None for _k in _ck.klines]
            )
        else:
            raise Exception(f"获取分型低点的区间类型错误 {qj_type}")

    def fx_k_nums(self) -> int:
        # 分型内原始K线的数量
        k_nums = 0
        for _ck in self.klines:
            if _ck is None:
                continue
            k_nums += len(_ck.klines)
        return k_nums

    def get_start_src_k(self) -> Kline:
        return self.klines[0].klines[0]

    def get_end_src_k(self) -> Kline:
        return (
            self.klines[-1].klines[-1]
            if self.klines[-1] is not None
            else self.klines[-2].klines[-1]
        )

    def __str__(self):
        return f"index: {self.index} type: {self.type} date : {self.k.date} val: {self.val} done: {self.done}"


class LINE:
    """
    线的基本定义，笔和线段继承此对象
    """

    def __init__(self, start: FX, end: FX, _type: str, index: int):
        self.start: FX = start  # 线的起始位置，以分型来记录
        self.end: FX = end  # 线的结束位置，以分型来记录

        # 根据缠论配置（笔/段区间），得来的高低点（顶底高低 或 缠论K线高低 或 原始K线高低）
        self.high: float = 0
        self.low: float = 0
        # 根据缠论配置（中枢区间），得来的高低点（zs_qj_dd ZS_QJ_CK ZS_QJ_K）
        self.zs_high: float = 0
        self.zs_low: float = 0
        self.type: str = _type  # 线的方向类型 （up 上涨  down 下跌）
        self.index: int = index  # 线的索引，后续查找方便

    @abstractmethod
    def is_done(self):
        """
        判断线是否结束
        """
        return False

    def get_ld(self, cl) -> dict:
        """
        返回线的力度信息
        """
        return {"macd": query_macd_ld(cl, self.start, self.end)}

    def ding_high(self) -> float:
        return self.end.val if self.type == "up" else self.start.val

    def di_low(self) -> float:
        return self.end.val if self.type == "down" else self.start.val

    def jiaodu(self) -> float:
        """
        计算线段与坐标轴呈现的角度（正为上，负为下）

        弧度 = dy / dx
            dy = 终点与起点的差值比例
            dx = 线段之间的k线数量
        """
        if self.end.val == self.start.val:
            return 0

        dy = (self.end.val - self.start.val) / self.start.val * 100
        dx = self.end.k.k_index - self.start.k.k_index
        # 弧度
        k = math.atan2(dy, dx)
        # 弧度转角度
        j = math.degrees(k)
        return j if self.end.val > self.start.val else -j


class ZS:
    """
    中枢对象（笔中枢，线段中枢）
    """

    def __init__(
        self,
        zs_type: str,
        start: FX,
        end: FX = None,
        zg: float = None,
        zd: float = None,
        gg: float = None,
        dd: float = None,
        _type: str = None,
        index: int = 0,
        line_num: int = 0,
        level: int = 0,
    ):
        self.zs_type: str = zs_type  # 标记中枢类型 bi 笔中枢 xd 线段中枢 zsd 走势段中枢
        self.start: FX = start
        self.lines: List[Union[BI, XD, LINE]] = (
            []
        )  # 中枢，记录中枢的线（笔 or 线段）对象
        self.end: FX = end

        self.zg: float = zg
        self.zd: float = zd
        self.gg: float = gg
        self.dd: float = dd
        self.type: str = _type  # 中枢类型（up 上涨中枢  down 下跌中枢  zd 震荡中枢）
        self.index: int = index
        self.line_num: int = line_num  # 中枢包含的 笔或线段 个数
        self.level: int = level  # 中枢级别 0 本级别 1 上一级别 ...

        self.done = False  # 记录中枢是否完成
        self.real = True  # 记录是否是有效中枢

    def add_line(self, line: LINE) -> bool:
        """
        添加 笔 or 线段
        """
        self.lines.append(line)
        return True

    def zf(self) -> float:
        """
        中枢振幅
        中枢重叠区间占整个中枢区间的百分比，越大说明中枢重叠区域外的波动越小
        """
        zgzd = self.zg - self.zd
        if zgzd == 0.0:
            return 0
        return (zgzd / (self.gg - self.dd)) * 100

    def zs_mmds(self, zs_type="|") -> List[str]:
        """
        获取中枢内线的所有买点列表
        """
        mmds = []
        for _l in self.lines:
            mmds += _l.line_mmds(zs_type)
        return mmds

    def zs_up_bcs(self, zs_type="|") -> List[str]:
        """
        获取中枢内，向上线段的背驰列表
        """
        bcs = []
        for _l in self.lines:
            if _l.type == "up":
                bcs += _l.line_bcs(zs_type)
        return bcs

    def zs_down_bcs(self, zs_type="|") -> List[str]:
        """
        获取中枢内，向上线段的背驰列表
        """
        bcs = []
        for _l in self.lines:
            if _l.type == "down":
                bcs += _l.line_bcs(zs_type)
        return bcs

    def __str__(self):
        return f"index: {self.index} zs_type: {self.zs_type} level: {self.level} FX: ({self.start.k.date}-{self.end.k.date}) type: {self.type} zg: {self.zg} zd: {self.zd} gg: {self.gg} dd: {self.dd} done: {self.done} real: {self.real} "


class MMD:
    """
    买卖点对象
    """

    def __init__(self, name: str, zs: ZS):
        self.name: str = name  # 买卖点名称
        self.zs: ZS = zs  # 买卖点对应的中枢对象
        self.msg: str = ""  # 买卖点信息

    def __str__(self):
        return f"MMD: {self.name} ZS: {self.zs} MSG: {self.msg}"


class BC:
    """
    背驰对象
    """

    def __init__(
        self,
        _type: str,
        zs: Union[ZS, None],
        compare_line: LINE,
        compare_lines: List[LINE],
        bc: bool,
    ):
        self.type: str = (
            _type  # 背驰类型 （bi 笔背驰 xd 线段背驰 zsd 走势段背驰 pz 盘整背驰 qs 趋势背驰）
        )
        self.zs: Union[ZS, None] = zs  # 背驰对应的中枢
        self.compare_line: LINE = (
            compare_line  # 比较的笔 or 线段， 在 笔背驰、线段背驰、盘整背驰有用
        )
        self.compare_lines: List[LINE] = compare_lines  # 在趋势背驰的时候使用
        self.bc = bc  # 是否背驰

    def __str__(self):
        return f"BC type: {self.type} bc: {self.bc} zs: {self.zs}"


class BI(LINE):
    """
    笔对象
    """

    def __init__(
        self,
        start: FX,
        end: FX = None,
        _type: str = None,
        index: int = 0,
        default_zs_type: str = None,
    ):
        super().__init__(start, end, _type, index)
        self.mmds: List[MMD] = []  # 买卖点
        self.bcs: List[BC] = []  # 背驰信息

        self.default_zs_type: str = default_zs_type
        # 记录不同中枢下的背驰和买卖点
        self.zs_type_mmds: Dict[str, List[MMD]] = {}
        self.zs_type_bcs: Dict[str, List[BC]] = {}

        # 记录是否是拆分笔
        self.is_split = ""

    @property
    def td(self):
        """
        笔是否停顿
        弃用，请使用 Strategy.bi_td 方法进行判断
        """
        return False

    def __str__(self):
        return f"index: {self.index} type: {self.type} FX: ({self.start.k.date} - {self.end.k.date}) high: {self.high} low: {self.low} done: {self.is_done()}"

    def is_done(self) -> bool:
        """
        返回笔是否完成
        """
        return self.end.done

    def fx_num(self) -> int:
        """
        包含的分型数量
        """
        return self.end.index - self.start.index

    def get_mmds(self, zs_type: str = None) -> List[MMD]:
        # 返回买卖点，需要检查买点的中枢是否有效
        if zs_type is None:
            return [_m for _m in self.mmds if _m.zs.real]
        if zs_type not in self.zs_type_mmds.keys():
            return []
        return [_m for _m in self.zs_type_mmds[zs_type] if _m.zs.real]

    def get_bcs(self, zs_type: str = None) -> List[BC]:
        # 需要检查买点的中枢是否有效
        if zs_type is None:
            return [_b for _b in self.bcs if _b.bc and (_b.zs is None or _b.zs.real)]
        if zs_type not in self.zs_type_bcs.keys():
            return []
        return [
            _b
            for _b in self.zs_type_bcs[zs_type]
            if _b.bc and (_b.zs is None or _b.zs.real)
        ]

    def add_mmd(self, name: str, zs: ZS, zs_type: str, msg: str = "") -> bool:
        """
        添加买卖点
        """
        mmd_obj = MMD(name, zs)
        mmd_obj.msg = msg
        if zs_type == self.default_zs_type:
            self.mmds.append(mmd_obj)

        if zs_type not in self.zs_type_mmds.keys():
            self.zs_type_mmds[zs_type] = []
        self.zs_type_mmds[zs_type].append(mmd_obj)
        return True

    def add_bc(
        self,
        _type: str,
        zs: Union[ZS, None],
        compare_line: Union[LINE, None],
        compare_lines: List[LINE],
        bc: bool,
        zs_type: str,
    ) -> bool:
        """
        添加背驰点
        """
        bc_obj = BC(_type, zs, compare_line, compare_lines, bc)
        if zs_type == self.default_zs_type:
            self.bcs.append(bc_obj)
        if zs_type not in self.zs_type_bcs.keys():
            self.zs_type_bcs[zs_type] = []
        self.zs_type_bcs[zs_type].append(bc_obj)

        return True

    def line_mmds(self, zs_type: Union[str, None] = None) -> list:
        """
        返回当前线所有买卖点名称

        zs_type 如果等于  | ，获取当前笔所有中枢的买卖点 合集
        zs_type 如果等于  & ，获取当前笔所有中枢的买卖点 交集

        """
        if zs_type is None:
            return [m.name for m in self.mmds if m.zs.real]

        if zs_type == "|":
            mmds = []
            for zs_type in self.zs_type_mmds.keys():
                mmds += self.line_mmds(zs_type)
            return list(set(mmds))
        if zs_type == "&":
            mmds = self.line_mmds()
            for zs_type in self.zs_type_mmds.keys():
                mmds = set(mmds) & set(self.line_mmds(zs_type))
            return list(mmds)

        if zs_type not in self.zs_type_mmds.keys():
            return []
        return [m.name for m in self.zs_type_mmds[zs_type] if m.zs.real]

    def line_bcs(self, zs_type: Union[str, None] = None) -> list:
        """
        返回当前线所有的背驰类型

        zs_type 如果等于  | ，获取当前笔所有中枢的买卖点 合集
        zs_type 如果等于  & ，获取当前笔所有中枢的买卖点 交集
        """
        if zs_type is None:
            return [
                _bc.type
                for _bc in self.bcs
                if _bc.bc and (_bc.zs is None or _bc.zs.real)
            ]

        if zs_type == "|":
            bcs = []
            for zs_type in self.zs_type_bcs.keys():
                bcs += self.line_bcs(zs_type)
            return list(set(bcs))
        if zs_type == "&":
            bcs = self.line_bcs()
            for zs_type in self.zs_type_bcs.keys():
                bcs = set(bcs) & set(self.line_bcs(zs_type))
            return list(bcs)

        if zs_type not in self.zs_type_bcs.keys():
            return []
        return [
            _bc.type
            for _bc in self.zs_type_bcs[zs_type]
            if _bc.bc and (_bc.zs is None or _bc.zs.real)
        ]

    def mmd_exists(self, check_mmds: list, zs_type: Union[str, None] = None) -> bool:
        """
        检查当前笔是否包含指定的买卖点的一个
        """
        mmds = self.line_mmds(zs_type)
        return len(set(check_mmds) & set(mmds)) > 0

    def bc_exists(self, bc_types: list, zs_type: Union[str, None] = None) -> bool:
        """
        检查是否有背驰的情况
        """
        bcs = self.line_bcs(zs_type)
        return len(set(bc_types) & set(bcs)) > 0


class TZXL:
    """
    特征序列
    """

    def __init__(
        self,
        bh_direction: str,
        line: Union[LINE, None],
        pre_line: LINE,
        line_bad: bool,
        done: bool,
    ):
        self.bh_direction: str = (
            bh_direction  # 特征序列包含的方向 up 向上包含，取高高，down 向下包含，取低低
        )
        self.line: Union[LINE, None] = line
        self.pre_line: LINE = pre_line
        self.line_bad: bool = line_bad
        self.is_up_line: bool = False
        self.lines: List[LINE] = [line]
        self.done: bool = done

        self.max: float = 0
        self.min: float = 0
        self.update_maxmin()

    def __str__(self):
        return f"done {self.done} max {self.max} min {self.min} line_bad {self.line_bad} line {self.line} pre_line {self.pre_line} num {len(self.lines)}"

    def update_maxmin(self):
        if self.bh_direction == "up":
            self.max = max([_l.high for _l in self.lines])
            self.min = max([_l.low for _l in self.lines])
        else:
            self.max = min([_l.high for _l in self.lines])
            self.min = min([_l.low for _l in self.lines])

    def get_start_fx(self):
        if self.bh_direction == "up":
            sort_lines = sorted(self.lines, key=lambda _l: _l.high, reverse=True)
        else:
            sort_lines = sorted(self.lines, key=lambda _l: _l.low, reverse=False)
        return sort_lines[0].start

    def get_end_fx(self):
        if self.bh_direction == "up":
            sort_lines = sorted(self.lines, key=lambda _l: _l.low, reverse=True)
        else:
            sort_lines = sorted(self.lines, key=lambda _l: _l.high, reverse=False)
        return sort_lines[0].end


class XLFX:
    """
    序列分型
    """

    def __init__(self, _type: str, xl: TZXL, xls: List[TZXL], done: bool = True):
        self.type: str = _type
        self.xl: TZXL = xl
        self.xls: List[TZXL] = xls

        self.qk = False  # 分型是否有缺口
        self.is_line_bad = False  # 是否是一笔破坏分型
        self.fx_high = max(_xl.max for _xl in self.xls if _xl is not None)
        self.fx_low = min(_xl.min for _xl in self.xls if _xl is not None)

        self.done = done  # 序列分型是否完成

        self.bh_type = None

    @property
    def high(self):
        return self.xl.max

    @property
    def low(self):
        return self.xl.min

    def get_last_xl(self) -> TZXL:
        return [_xl for _xl in self.xls if _xl is not None][-1]

    def __str__(self):
        return f"XLFX type : {self.type} done : {self.done} qk : {self.qk} high : {self.high} low : {self.low} xl : {self.xl}"


class XD(LINE):
    """
    线段对象
    """

    def __init__(
        self,
        start: FX,
        end: FX,
        start_line: LINE,
        end_line: LINE = None,
        _type: str = None,
        ding_fx: XLFX = None,
        di_fx: XLFX = None,
        index: int = 0,
        default_zs_type: str = None,
    ):
        super().__init__(start, end, _type, index)

        self.start_line: Union[LINE, BI, XD] = start_line  # 线段起始笔
        self.end_line: Union[LINE, BI, XD] = end_line  # 线段结束笔
        self.mmds: List[MMD] = []  # 买卖点
        self.bcs: List[BC] = []  # 背驰信息
        self.ding_fx: XLFX = ding_fx
        self.di_fx: XLFX = di_fx
        self.tzxls: List[TZXL] = []  # 特征序列列表
        self.done: bool = False  # 标记线段是否完成

        # 是否是拆分后的线段，如果是，这里会写明原因
        self.is_split: str = ""

        self.default_zs_type: str = default_zs_type
        # 记录不同中枢下的背驰和买卖点
        self.zs_type_mmds: Dict[str, List[MMD]] = {}
        self.zs_type_bcs: Dict[str, List[BC]] = {}

        self.default_zs_type: str = default_zs_type
        # 记录不同中枢下的背驰和买卖点
        self.zs_type_mmds: Dict[str, List[MMD]] = {}
        self.zs_type_bcs: Dict[str, List[BC]] = {}

        self.not_del: bool = False  # 计算过程中，不允许删除重新计算
        self.not_yx: bool = False  # 计算过程中，不允许进行延续计算

    def is_qk(self) -> bool:
        """
        成线段的分型是否有缺口
        """
        return self.ding_fx.qk if self.type == "up" else self.di_fx.qk

    def fx_is_done(self) -> bool:
        """
        返回构成线段的结束特征序列分型是否完成
        """
        return self.ding_fx.done if self.type == "up" else self.di_fx.done

    def fx_is_bad_line(self) -> bool:
        """
        返回构成线段的结束特征序列分型是否是有笔包含的情况
        """
        return self.ding_fx.is_line_bad if self.type == "up" else self.di_fx.is_line_bad

    def is_done(self) -> bool:
        return self.done

    def get_mmds(self, zs_type: str = None) -> List[MMD]:
        # 需要检查买点的中枢是否有效
        if zs_type is None:
            return [_m for _m in self.mmds if _m.zs.real]
        if zs_type not in self.zs_type_mmds.keys():
            return []
        return [_m for _m in self.zs_type_mmds[zs_type] if _m.zs.real]

    def get_bcs(self, zs_type: str = None) -> List[BC]:
        # 需要检查买点的中枢是否有效
        if zs_type is None:
            return [bc for bc in self.bcs if bc.bc and (bc.zs is None or bc.zs.real)]
        if zs_type not in self.zs_type_bcs.keys():
            return []
        return [
            bc
            for bc in self.zs_type_bcs[zs_type]
            if bc.bc and (bc.zs is None or bc.zs.real)
        ]

    def add_mmd(self, name: str, zs: ZS, zs_type: str, msg: str = "") -> bool:
        """
        添加买卖点
        """
        mmd_obj = MMD(name, zs)
        mmd_obj.msg = msg
        if zs_type == self.default_zs_type:
            self.mmds.append(mmd_obj)
        if zs_type not in self.zs_type_mmds.keys():
            self.zs_type_mmds[zs_type] = []
        self.zs_type_mmds[zs_type].append(mmd_obj)
        return True

    def add_bc(
        self,
        _type: str,
        zs: Union[ZS, None],
        compare_line: LINE,
        compare_lines: List[LINE],
        bc: bool,
        zs_type: str,
    ) -> bool:
        """
        添加背驰点
        """
        bc_obj = BC(_type, zs, compare_line, compare_lines, bc)
        if zs_type == self.default_zs_type:
            self.bcs.append(bc_obj)
        if zs_type not in self.zs_type_bcs.keys():
            self.zs_type_bcs[zs_type] = []
        self.zs_type_bcs[zs_type].append(bc_obj)
        return True

    def line_mmds(self, zs_type: Union[str, None] = None) -> list:
        """
        返回当前线所有买卖点名称

        zs_type 如果等于  | ，获取当前笔所有中枢的买卖点 合集
        zs_type 如果等于  & ，获取当前笔所有中枢的买卖点 交集

        """
        if zs_type is None:
            return [m.name for m in self.mmds if m.zs.real]

        if zs_type == "|":
            mmds = []
            for zs_type in self.zs_type_mmds.keys():
                mmds += self.line_mmds(zs_type)
            return list(set(mmds))
        if zs_type == "&":
            mmds = self.line_mmds()
            for zs_type in self.zs_type_mmds.keys():
                mmds = set(mmds) & set(self.line_mmds(zs_type))
            return list(mmds)

        if zs_type not in self.zs_type_mmds.keys():
            return []
        return [m.name for m in self.zs_type_mmds[zs_type] if m.zs.real]

    def line_bcs(self, zs_type: Union[str, None] = None) -> list:
        """
        返回当前线所有的背驰类型

        zs_type 如果等于  | ，获取当前笔所有中枢的买卖点 合集
        zs_type 如果等于  & ，获取当前笔所有中枢的买卖点 交集
        """
        if zs_type is None:
            return [
                _bc.type
                for _bc in self.bcs
                if _bc.bc and (_bc.zs is None or _bc.zs.real)
            ]

        if zs_type == "|":
            bcs = []
            for zs_type in self.zs_type_bcs.keys():
                bcs += self.line_bcs(zs_type)
            return list(set(bcs))
        if zs_type == "&":
            bcs = self.line_bcs()
            for zs_type in self.zs_type_bcs.keys():
                bcs = set(bcs) & set(self.line_bcs(zs_type))
            return list(bcs)

        if zs_type not in self.zs_type_bcs.keys():
            return []
        return [
            _bc.type
            for _bc in self.zs_type_bcs[zs_type]
            if _bc.bc and (_bc.zs is None or _bc.zs.real)
        ]

    def mmd_exists(self, check_mmds: list, zs_type: Union[str, None] = None) -> bool:
        """
        检查当前笔是否包含指定的买卖点的一个
        """
        mmds = self.line_mmds(zs_type)
        return len(set(check_mmds) & set(mmds)) > 0

    def bc_exists(self, bc_types: list, zs_type: Union[str, None] = None) -> bool:
        """
        检查是否有背驰的情况
        """
        bcs = self.line_bcs(zs_type)
        return len(set(bc_types) & set(bcs)) > 0

    def __str__(self):
        return f"XD index: {self.index} type: {self.type} start: {self.start_line.start.k.date} end: {self.end_line.end.k.date} high: {self.high} low: {self.low} is_qk: {self.is_qk()} done: {self.is_done()} ({self.is_split})"


@dataclass
class LOW_LEVEL_QS:
    zss: List[ZS]  # 低级别线构成的中枢列表
    lines: List[Union[LINE, BI, XD]]  # 包含的低级别线
    zs_num: int = 0
    line_num: int = 0
    bc_line: Union[LINE, None] = None  # 背驰的线
    last_line: Union[LINE, BI, XD, None] = None  # 最后一个线
    qs: bool = False  # 是否形成趋势
    pz: bool = False  # 是否形成盘整
    line_bc: bool = False  # 是否形成（笔、线段）背驰
    qs_bc: bool = False  # 是否趋势背驰
    pz_bc: bool = False  # 是否盘整背驰

    def __str__(self):
        return f"低级别信息：中枢 {self.zs_num} 线 {self.line_num} 趋势 {self.qs} 盘整 {self.pz} 线背驰 {self.line_bc} 盘整背驰 {self.pz_bc} 趋势背驰 {self.qs_bc}"


@dataclass
class MACD_INFOS:
    # 记录中枢内，macd 的变化情况
    dif_up_cross_num = 0  # dif 线上穿零轴的次数
    dea_up_cross_num = 0  # dea 线上穿令咒的次数
    dif_down_cross_num = 0  # dif 线下穿零轴的次数
    dea_down_cross_num = 0  # dea 线下穿零轴的次数
    gold_cross_num = 0  # 金叉次数
    die_cross_num = 0  # 死叉次数
    last_dif = 0
    last_dea = 0


@dataclass
class LINE_FORM_INFOS:
    # 组成形态的线列表
    lines: List[Union[LINE, BI, XD]]
    # 方向
    direction: str
    # 线的数量
    line_num: int
    # 线的形态描述
    form_type: str
    # 线组成的中枢信息
    zss: Union[None, List[ZS]] = None
    # 最后线是否背驰段
    is_bc_line: bool = False
    # 形态级别
    form_level: float = 0
    # 形态趋势
    form_qs: str = ""
    # 其他信息
    infos: dict = None

    def __str__(self):
        msg = f'{"向上" if self.direction == "up" else "向下"} {self.form_type} ({self.form_level}) {"进入背驰段" if self.is_bc_line else "无背驰"}'
        if self.infos is not None:
            if "zs_pre_line_num" in self.infos.keys():
                msg += f'  中枢前 {self.infos["zs_pre_line_num"]} 段 / '
            if "zs_next_line_num" in self.infos.keys():
                msg += f'  中枢后 {self.infos["zs_next_line_num"]} 段 / '
            if "zs_pre_level" in self.infos.keys():
                msg += f'  前中枢 {self.infos["zs_pre_level"]} 级别 / '
            if "zs_next_level" in self.infos.keys():
                msg += f'  后中枢 {self.infos["zs_next_level"]} 级别 / '
        return msg.strip(" / ")


@dataclass
class BW_LINE_QS_INFOS:
    """
    倒推线段的趋势信息
    """

    # 线段的组成
    lines: List[Union[LINE, BI, XD]]
    # 中枢列表
    zss: List[ZS]
    # 中枢类型拼接字符串
    zss_str = ""
    # 走势类型描述
    zoushi_type_str = ""

    def __str__(self):
        return f"低级别信息：中枢 {self.zs_num} 线 {self.line_num} 趋势 {self.qs} 盘整 {self.pz} 线背驰 {self.line_bc} 盘整背驰 {self.pz_bc} 趋势背驰 {self.qs_bc}"


class ICL(metaclass=ABCMeta):
    """
    缠论数据分析接口定义
    """

    @abstractmethod
    def __init__(
        self,
        code: str,
        frequency: str,
        config: Union[dict, None] = None,
        start_datetime: datetime.datetime = None,
    ):
        """
        缠论计算
        :param code: 代码
        :param frequency: 周期
        :param config: 计算缠论依赖的配置项
        :param start_datetime: 开始分析的时间，不设置则分析计算所有
        """

    @abstractmethod
    def process_klines(self, klines: pd.DataFrame):
        """
        计算k线缠论数据
        传递 pandas 数据，需要包括以下列：
            date  时间日期  datetime 格式，对于在 DataFrame 中 date 不是日期格式的，需要执行 pd.to_datetime 方法转换下
            high  最高价
            low   最低价
            open  开盘价
            close  收盘价
            volume  成交量

        可增量多次调用，重复已计算的会自动跳过，最后一个 bar 会进行更新
        """
        pass

    # @abstractmethod
    # def check_bi_inside_bc(self, bi: BI):
    #     """
    #
    #     """

    @abstractmethod
    def get_code(self) -> str:
        """
        返回计算的标的代码
        """
        pass

    @abstractmethod
    def get_frequency(self) -> str:
        """
        返回计算的周期参数
        """
        pass

    def get_config(self) -> dict:
        """
        返回计算时使用的缠论配置项
        """
        pass

    @abstractmethod
    def get_src_klines(self) -> List[Kline]:
        """
        返回原始K线列表
        """
        pass

    @abstractmethod
    def get_klines(self) -> List[Kline]:
        """
        返回K线列表
        如果 kline_type == kline_default 则返回原始 K 线数据
        如果 kline_type == kline_chanlun 则返回缠论 K 线数据
        如需获取原始K线数据，使用 get_src_klines 方法
        """
        pass

    @abstractmethod
    def get_cl_klines(self) -> List[CLKline]:
        """
        返回合并后的缠论K线列表
        """
        pass

    @abstractmethod
    def get_idx(self) -> dict:
        """
        返回计算的指标数据
        """
        pass

    @abstractmethod
    def get_fxs(self) -> List[FX]:
        """
        返回缠论分型列表
        """
        pass

    @abstractmethod
    def get_bis(self) -> List[BI]:
        """
        返回计算缠论笔列表
        """
        pass

    @abstractmethod
    def get_xds(self) -> List[XD]:
        """
        返回计算缠论线段列表
        """
        pass

    @abstractmethod
    def get_zsds(self) -> List[XD]:
        """
        返回计算缠论走势段列表
        """
        pass

    @abstractmethod
    def get_qsds(self) -> List[XD]:
        """
        返回计算缠论趋势段列表
        """
        pass

    @abstractmethod
    def get_bi_zss(self, zs_type: str = None) -> List[ZS]:
        """
        返回计算缠论笔中枢列表
        """
        pass

    @abstractmethod
    def get_xd_zss(self, zs_type: str = None) -> List[ZS]:
        """
        返回计算缠论线段中枢（走势中枢）
        """
        pass

    @abstractmethod
    def get_zsd_zss(self) -> List[ZS]:
        """
        返回走势段中枢
        """
        pass

    @abstractmethod
    def get_qsd_zss(self) -> List[ZS]:
        """
        返回趋势段中枢
        """
        pass

    @abstractmethod
    def get_last_bi_zs(self) -> Union[ZS, None]:
        """
        返回最后的笔中枢，根据最后几笔倒推出的笔中枢，和 self.get_bi_zss()[-1] 方式获取的中枢不一定一致
        """
        pass

    @abstractmethod
    def get_last_xd_zs(self) -> Union[ZS, None]:
        """
        返回最后的线段中枢，需要 CL_CAL_LAST_ZS 设置为 True 才会有中枢
        """
        pass

    @abstractmethod
    def create_dn_zs(
        self,
        zs_type: str,
        lines: List[LINE],
        max_line_num: int = 999,
        zs_include_last_line=True,
    ) -> List[ZS]:
        """
        创建段内中枢
        @param zs_type: 中枢类型：bi 笔中枢 xd 线段中枢
        @param lines: 线的列表
        @param max_line_num: 中枢最大线段数量
        @param zs_include_last_line: 如果中枢最后一笔不是最高 or 最低，在中枢内部，中枢区间是否包含最后一笔，默认包含
        """
        pass

    @abstractmethod
    def beichi_pz(self, zs: ZS, now_line: LINE) -> Tuple[bool, Union[LINE, None]]:
        """
        判断中枢与指定线是否构成盘整背驰
        @param zs: 中枢
        @param now_line: 需要对比的线
        """

    @abstractmethod
    def beichi_qs(
        self, lines: List[LINE], zss: List[ZS], now_line: LINE
    ) -> Tuple[bool, List[LINE]]:
        """
        判断指定线与之前的中枢，是否形成了趋势背驰

        @parma lines: 线的列表，用来查找之前的线
        @param zss：中枢列表，用来获取最后两个中枢，判断配置关系
        @param now_line: 最后一个线
        """

    @abstractmethod
    def zss_is_qs(self, one_zs: ZS, two_zs: ZS) -> Tuple[str, None]:
        """
        判断两个中枢是否形成趋势（根据设置的位置关系配置，来判断两个中枢是否有重叠）
        返回  up 是向上趋势， down 是向下趋势 ，None 则没有趋势
        """


def query_macd_ld(cd: ICL, start_fx: FX, end_fx: FX):
    """
    计算分型区间 macd 力度
    实际比较力度是根据 hist 的 up_sum 和 down_sum 进行比较
    向上线段，比较 up_sum 红柱子总和
    向下线段，比较 down_sum 绿柱子总和
    """
    if start_fx.index > end_fx.index:
        raise Exception(
            "%s - %s - %s 计算力度，开始分型不可以大于结束分型"
            % (cd.get_code(), cd.get_frequency(), cd.get_klines()[-1].date)
        )

    dea = np.array(
        cd.get_idx()["macd"]["dea"][start_fx.k.k_index : end_fx.k.k_index + 1]
    )
    dif = np.array(
        cd.get_idx()["macd"]["dif"][start_fx.k.k_index : end_fx.k.k_index + 1]
    )
    hist = np.array(
        cd.get_idx()["macd"]["hist"][start_fx.k.k_index : end_fx.k.k_index + 1]
    )
    if len(hist) == 0:
        hist = np.array([0])
    if len(dea) == 0:
        dea = np.array([0])
    if len(dif) == 0:
        dif = np.array([0])

    hist_abs = abs(hist)
    hist_up = np.array([_i for _i in hist if _i > 0])
    hist_down = np.array([_i for _i in hist if _i < 0])
    hist_max = np.max(hist)
    hist_min = np.min(hist)
    hist_sum = hist_abs.sum()
    hist_up_sum = hist_up.sum()
    hist_down_sum = abs(hist_down.sum())
    end_dea = dea[-1]
    end_dif = dif[-1]
    end_hist = hist[-1]
    return {
        "dea": {"end": end_dea, "max": np.max(dea), "min": np.min(dea)},
        "dif": {"end": end_dif, "max": np.max(dif), "min": np.min(dif)},
        "hist": {
            "sum": hist_sum,
            "up_sum": hist_up_sum,
            "down_sum": hist_down_sum,
            "max": hist_max,
            "min": hist_min,
            "end": end_hist,
        },
    }


def compare_ld_beichi(one_ld: dict, two_ld: dict, line_direction: str):
    """
    比较两个力度，后者小于前者，返回 True
    :param one_ld:
    :param two_ld:
    :param line_direction: [up down] 比较线的方向，向上看macd红柱子之和，向下看macd绿柱子之和
    :return:
    """
    hist_key = "sum"
    if line_direction == "up":
        hist_key = "up_sum"
    elif line_direction == "down":
        hist_key = "down_sum"
    if "macd" not in two_ld.keys() or "macd" not in one_ld.keys():
        return False
    if two_ld["macd"]["hist"][hist_key] < one_ld["macd"]["hist"][hist_key]:
        return True
    else:
        return False


def user_custom_mmd(
    cd: ICL,
    line: Union[BI, XD],
    lines: List[Union[BI, XD]],
    zs_type: str,
    zss: List[ZS],
):
    """
    用户可自定义买卖点
    每次笔or线段更新，计算完系统默认买卖点后，会执行这个方法，用户可按照自己的规则，给当前线段增加买卖点

    这里示例增加类二类三类买卖点

    @param cd: 缠论数据对象
    @param line: 要计算买卖点的线
    @param lines：要计算线类型的列表
    @param zs_type: 中枢类型 ，参考中枢类型配置项 取值 zs_type_bz、zs_type_dn、zs_type_fx
    """
    # 清空买卖点与背驰情况，重新计算

    if len(lines) < 4 or len(zss) == 0:
        return False

    # 类二类买卖点，如果前一笔同向的线段出现二类买卖点，当前与二类买卖点笔有重叠（形成中枢），不创前一笔的高点或低点，增加类二类买卖点
    pre_same_line = lines[line.index - 2]
    for mmd in pre_same_line.get_mmds(zs_type):
        if line.type == "down" and mmd.name == "2buy" and line.low > pre_same_line.low:
            new_zss = cd.create_dn_zs("", lines[-4:])  # 自一类买卖点后形成的中枢
            if len(new_zss) != 1:
                continue
            line.add_mmd(
                "l2buy",
                new_zss[0],
                zs_type,
                msg="二买后，重叠中枢后，不创二买低点，形成类二买",
            )
        if line.type == "up" and mmd.name == "2sell" and line.high < pre_same_line.high:
            new_zss = cd.create_dn_zs("", lines[-4:])  # 自一类买卖点后形成的中枢
            if len(new_zss) != 1:
                continue
            line.add_mmd(
                "l2sell",
                new_zss[0],
                zs_type,
                msg="二卖后，重叠中枢后，不创二卖高点，形成类二卖",
            )

    # 类三类买卖点，如果前一笔同向的线段出现三类买卖点，当前与三类买卖点笔有重叠（形成中枢），不创前中枢的高低点，并且力度笔三类买卖点小，增加类三类买卖点
    for mmd in pre_same_line.get_mmds(zs_type):
        if (
            line.type == "down"
            and mmd.name == "3buy"
            and line.low > mmd.zs.zg
            and compare_ld_beichi(pre_same_line.get_ld(cd), line.get_ld(cd), "down")
        ):
            new_zss = cd.create_dn_zs(
                "", lines[-4:]
            )  # 从三买的前一段到现在一段，共4段形成的中枢
            if len(new_zss) != 1:
                continue
            line.add_mmd(
                "l3buy",
                new_zss[0],
                zs_type,
                msg="三买后，重叠中枢后，不创三买低点，形成类三买",
            )
        if (
            line.type == "up"
            and mmd.name == "3sell"
            and line.high < mmd.zs.zd
            and compare_ld_beichi(pre_same_line.get_ld(cd), line.get_ld(cd), "up")
        ):
            new_zss = cd.create_dn_zs(
                "", lines[-4:]
            )  # 从三卖的前一段到现在一段，共4段形成的中枢
            if len(new_zss) != 1:
                continue
            line.add_mmd(
                "l3sell",
                new_zss[0],
                zs_type,
                msg="三卖后，重叠中枢后，不创三卖高点，形成类三卖",
            )

    return True
