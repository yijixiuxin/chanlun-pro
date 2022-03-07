import datetime
from typing import List, Dict

import numpy as np
import pandas as pd
import talib as ta


class Kline:
    """
    原始K线对象
    """

    def __init__(self, index: int, date: datetime, h: float, l: float, o: float, c: float, a: float):
        self.index: int = index
        self.date: datetime = date
        self.h: float = h
        self.l: float = l
        self.o: float = o
        self.c: float = c
        self.a: float = a

    def __str__(self):
        return "index: %s date: %s h: %s l: %s o: %s c:%s a:%s" % \
               (self.index, self.date, self.h, self.l, self.o, self.c, self.a)


class CLKline:
    """
    缠论K线对象
    """

    def __init__(self, k_index: int, date: datetime, h: float, l: float, o: float, c: float, a: float,
                 klines: List[Kline] = [], index: int = 0, _n: int = 0, _q: bool = False):
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
        return "index: %s k_index:%s date: %s h: %s l: %s _n:%s _q:%s" % \
               (self.index, self.k_index, self.date, self.h, self.l, self.n, self.q)


class FX:
    """
    分型对象
    """

    def __init__(self, _type: str, k: CLKline, klines: List[CLKline], val: float,
                 index: int = 0, tk: bool = False, real: bool = True, done: bool = True):
        self.type: str = _type  # 分型类型 （ding 顶分型 di 底分型）
        self.k: CLKline = k
        self.klines: List[CLKline] = klines
        self.val: float = val
        self.index: int = index
        self.tk: bool = tk  # 记录是否跳空
        self.real: bool = real  # 是否是有效的分型
        self.done: bool = done  # 分型是否完成

    def __str__(self):
        return 'index: %s type: %s real: %s date : %s val: %s done: %s' % (
            self.index, self.type, self.real, self.k.date, self.val, self.done)


class ZS:
    """
    中枢对象
    """

    def __init__(self, start: FX, end: FX = None, zg: float = None, zd: float = None,
                 gg: float = None, dd: float = None, _type: str = None, index: int = 0, bi_num: int = 0,
                 level: int = 0, is_high_kz: bool = False, max_ld: dict = None):
        self.start: FX = start
        self.bis: List[BI] = []
        self.end: FX = end
        self.zg: float = zg
        self.zd: float = zd
        self.gg: float = gg
        self.dd: float = dd
        self.type: str = _type  # 中枢类型（up 上涨中枢  down 下跌中枢  zd 震荡中枢）
        self.index: int = index
        self.bi_num: int = bi_num  # 中枢包含的笔数
        self.level: int = level  # 中枢级别 0 本级别 1 上一级别 ...
        self.is_high_kz: bool = is_high_kz  # 中枢是否是高级别的扩展
        self.max_ld: dict = max_ld  # 记录中枢中最大笔力度
        self.done = False  # 记录中枢是否完成
        self.real = True  # 记录是否是有效中枢

    def add_bi(self, bi):
        """
        添加笔，检查是否有重复，重复则不添加
        """
        # bi_indexs = [_bi.index for _bi in self.bis]
        # if bi.index in bi_indexs:
        #     return True
        self.bis.append(bi)
        # self.bis = sorted(self.bis, key=lambda b: b.index, reverse=False)
        return True

    def __str__(self):
        return 'index: %s level: %s FX: (%s-%s) type: %s zg: %s zd: %s gg: %s dd: %s done: %s' % \
               (self.index, self.level, self.start.index, self.end.index, self.type, self.zg, self.zd, self.gg, self.dd,
                self.done)


class MMD:
    """
    买卖点对象
    """

    def __init__(self, name: str, zs: ZS):
        self.name: str = name  # 买卖点名称
        self.zs: ZS = zs  # 买卖点对应的中枢对象

    def __str__(self):
        return 'MMD: %s ZS: %s' % (self.name, self.zs)


class BC:
    """
    背驰对象
    """

    def __init__(self, _type: str, zs: ZS, pre_bi, bc: bool):
        self.type: str = _type  # 背驰类型 （bi 笔背驰 pz 盘整背驰 qs 趋势背驰）
        self.zs: ZS = zs  # 背驰对应的中枢
        self.pre_bi: BI = pre_bi  # 比较的笔
        self.bc = bc  # 是否背驰

    def __str__(self):
        return 'BC type: %s bc: %s zs: %s' % (self.type, self.bc, self.zs)


class BI:
    """
    笔对象
    """

    def __init__(self, start: FX, end: FX = None, high: float = None, low: float = None, _type: str = None,
                 ld: dict = None, done: bool = None, index: int = 0, td: bool = False, fx_num: int = 0):
        self.start: FX = start  # 起始分型
        self.end: FX = end  # 结束分型
        self.high: float = high
        self.low: float = low
        self.type: str = _type  # 笔类型 （up 上涨笔  down 下跌笔）
        self.ld: dict = ld  # 记录笔的力度信息
        self.done: bool = done  # 笔是否完成
        self.mmds: List[MMD] = []  # 买卖点
        self.index: int = index
        self.bcs: List[BC] = []  # 背驰信息
        self.td: bool = td  # 笔是否停顿
        self.fx_num: int = fx_num  # 包含的分型数量

    def __str__(self):
        return 'index: %s type: %s FX: (%s - %s) high: %s low: %s done: %s' % \
               (self.index, self.type, self.start.index, self.end.index, self.high, self.low, self.done)

    def add_mmd(self, name: str, zs: ZS):
        """
        添加买卖点
        """
        self.mmds.append(MMD(name, zs))
        return True

    def bi_mmds(self):
        """
        返回当前笔所有买卖点名称
        """
        return list(set([m.name for m in self.mmds]))

    def mmd_exists(self, check_mmds: list):
        """
        检查当前笔是否包含指定的买卖点的一个
        """
        mmds = self.bi_mmds()
        return len(set(check_mmds) & set(mmds)) > 0

    def bc_exists(self, bc_type: list):
        """
        检查是否有背驰的情况
        """
        bc = False
        for _bc in self.bcs:
            if _bc.type in bc_type and _bc.bc:
                bc = True
                break
        return bc

    def add_bc(self, _type: str, zs: [ZS, None], pre_bi, bc: bool):
        """
        添加背驰点
        """
        self.bcs.append(BC(_type, zs, pre_bi, bc))
        return True


class XD:
    """
    线段对象
    """

    def __init__(self, start: BI, end: BI = None, _type: str = None, high: float = None, low: float = None,
                 index: int = 0):
        self.start: BI = start  # 线段起始笔
        self.end: BI = end  # 线段结束笔
        self.type: str = _type  # 线段类型 （up 上涨线段 down 下跌线段）
        self.high: float = high
        self.low: float = low
        self.index: int = index
        self.done: bool = True

    def __str__(self):
        return 'XD index: %s type: %s start: % end: %s high: %s low: %s done: %s' % (
            self.index, self.type, self.start.index, self.end.index, self.high, self.low, self.done
        )


class QS:
    """
    趋势对象
    """

    def __init__(self, start_bi: BI, end_bi: BI = None, zss: List[ZS] = [], _type='zd'):
        self.start_bi: BI = start_bi  # 趋势的起始笔
        self.end_bi: BI = end_bi  # 趋势的结束笔
        self.zss: List[ZS] = zss  # 趋势包含的中枢列表
        self.type: str = _type  # 趋势类型 （up 上涨趋势 zd 震荡趋势 down 下跌趋势）

    def __str__(self):
        return 'QS Type %s start: %s end: %s ZSS %s' % (
            self.type, self.start_bi.start.index, self.end_bi.end.index, len(self.zss))


class XLFX:
    """
    线段序列分型
    """

    def __init__(self, _type: str, high: float, low: float, bi: BI):
        self.type = _type
        self.high = high
        self.low = low
        self.bi = bi

    def __str__(self):
        return "XLFX type : %s high : %s low : %s bi : %s" % (self.type, self.high, self.low, self.bi)


class CL:
    """
    行情数据缠论分析
    """

    def __init__(self, code: str, frequency: str, config: dict = None):
        """
        缠论计算
        :param code: 代码
        :param frequency: 周期
        :param config: 配置
        """
        self.code = code
        self.frequency = frequency
        self.config = config

        # 计算后保存的值
        self.klines: List[Kline] = []  # 整理后的原始K线
        self.cl_klines: List[CLKline] = []  # 缠论K线
        self.idx: dict = {}  # 各种行情指标
        self.fxs: List[FX] = []  # 分型列表
        self.bis: List[BI] = []  # 笔列表
        self.xds: List[XD] = []  # 线段列表
        self.zss: List[ZS] = []  # 中枢列表
        self.qss: List[QS] = []  # 趋势列表

        # 用于保存计算线段的序列分型
        self.__xl_ding = []
        self.__xl_di = []
        self.__xlfx_ding = []
        self.__xlfx_di = []

        self.use_time = 0  # debug 调试用时

    def process_klines(self, klines: pd.DataFrame):
        """
        计算k线缠论数据
        传递 pandas 数据，需要包括以下列：
            date  时间日期  datetime 格式
            high  最高价
            low   最低价
            open  开盘价
            close  收盘价
            volume  成交量

        可增量多次调用，重复已计算的会自动跳过，最后一个 bar 会进行更新
        """
        k_index = len(self.klines)
        for _k in klines.iterrows():
            k = _k[1]
            if len(self.klines) == 0:
                nk = Kline(index=k_index, date=k['date'], h=float(k['high']), l=float(k['low']),
                           o=float(k['open']), c=float(k['close']), a=float(k['volume']))
                self.klines.append(nk)
                k_index += 1
                continue
            if self.klines[-1].date > k['date']:
                continue
            if self.klines[-1].date == k['date']:
                self.klines[-1].h = float(k['high'])
                self.klines[-1].l = float(k['low'])
                self.klines[-1].o = float(k['open'])
                self.klines[-1].c = float(k['close'])
                self.klines[-1].a = float(k['volume'])
            else:
                nk = Kline(index=k_index, date=k['date'], h=float(k['high']), l=float(k['low']),
                           o=float(k['open']), c=float(k['close']), a=float(k['volume']))
                self.klines.append(nk)
                k_index += 1
            self.process_idx()
            self.process_cl_kline()
            self.process_fx()
            if self.process_bi():
                # 有新笔产生，才更新以下数据
                self.process_xd()
                self.process_zs()
                self.process_qs()
                self.process_mmd()

        return self

    def process_idx(self):
        """
        计算指标
        TODO 全量更新，后续可改为增量更新
        """
        prices = [k.c for k in self.klines]
        # 计算 macd
        macd_dif, macd_dea, macd_hist = ta.MACD(np.array(prices), fastperiod=12, slowperiod=26, signalperiod=9)
        macd = {'dea': macd_dea, 'dif': macd_dif, 'hist': macd_hist}

        # 计算 BOLL 指标
        boll_up, boll_mid, boll_low = ta.BBANDS(np.array(prices), timeperiod=20)

        self.idx = {
            'macd': macd,
            'boll': {'up': boll_up, 'mid': boll_mid, 'low': boll_low}
        }

    def process_cl_kline(self):
        """
        根据最后一个 k 线，检查包含关系，生成缠论K线
        """
        k = self.klines[-1]  # 最后一根K线对象
        if len(self.cl_klines) == 0:
            cl_kline = CLKline(date=k.date, k_index=k.index, h=k.h, l=k.l, o=k.o, c=k.c, a=k.a, klines=[k], _n=1,
                               _q=False, index=0)
            self.cl_klines.append(cl_kline)
            return True

        # 传递之前两个的缠论K线，用来判断趋势（不包括最后两个）
        up_cl_klines = self.cl_klines[-4:-2]

        # 最后两个缠论K线，重新进行包含处理
        cl_kline_1 = self.cl_klines[-1]
        cl_kline_2 = self.cl_klines[-2] if len(self.cl_klines) >= 2 else None
        klines = cl_kline_2.klines if cl_kline_2 else []
        klines += cl_kline_1.klines
        if k.date != klines[-1].date:
            klines.append(k)

        cl_klines = self.klines_baohan(klines, up_cl_klines)
        if (len(cl_klines) >= 2 and cl_kline_2) or (len(cl_klines) == 1 and cl_kline_2):
            # 重新给缠论k线附新值
            cl_kline_2.k_index = cl_klines[0].k_index
            cl_kline_2.date = cl_klines[0].date
            cl_kline_2.h = cl_klines[0].h
            cl_kline_2.l = cl_klines[0].l
            cl_kline_2.o = cl_klines[0].o
            cl_kline_2.c = cl_klines[0].c
            cl_kline_2.a = cl_klines[0].a
            cl_kline_2.klines = cl_klines[0].klines
            cl_kline_2.n = cl_klines[0].n
            cl_kline_2.q = cl_klines[0].q
            cl_kline_2.up_qs = cl_klines[0].up_qs

            if len(cl_klines) == 1:
                # 之前有两个缠论K线，新合并的只有一个了，之前最后一个删除
                cl_kline_1 = None
                del (self.cl_klines[-1])
            del (cl_klines[0])

        if cl_kline_1 and len(cl_klines) > 0:
            cl_kline_1.k_index = cl_klines[0].k_index
            cl_kline_1.date = cl_klines[0].date
            cl_kline_1.h = cl_klines[0].h
            cl_kline_1.l = cl_klines[0].l
            cl_kline_1.o = cl_klines[0].o
            cl_kline_1.c = cl_klines[0].c
            cl_kline_1.a = cl_klines[0].a
            cl_kline_1.klines = cl_klines[0].klines
            cl_kline_1.n = cl_klines[0].n
            cl_kline_1.q = cl_klines[0].q
            cl_kline_1.up_qs = cl_klines[0].up_qs
            del (cl_klines[0])

        for ck in cl_klines:
            ck.index = self.cl_klines[-1].index + 1
            self.cl_klines.append(ck)

        return True

    def process_fx(self):
        """
        根据最后一个缠论K线，计算分型
        """
        if len(self.cl_klines) < 3:
            return False

        up_k = self.cl_klines[-3]
        now_k = self.cl_klines[-2]
        end_k = self.cl_klines[-1]
        fx = None
        if (up_k.h < now_k.h and now_k.h > end_k.h) and (up_k.l < now_k.l and now_k.l > end_k.l):
            tiaokong = True if (up_k.h < now_k.l or now_k.l > end_k.h) else False
            fx = FX(index=0, _type='ding', k=now_k, klines=[up_k, now_k, end_k], val=now_k.h, tk=tiaokong, real=True,
                    done=True)
        if (up_k.h > now_k.h and now_k.h < end_k.h) and (up_k.l > now_k.l and now_k.l < end_k.l):
            tiaokong = True if (up_k.l > now_k.h or now_k.h < end_k.l) else False
            fx = FX(index=0, _type='di', k=now_k, klines=[up_k, now_k, end_k], val=now_k.l, tk=tiaokong, real=True,
                    done=True)

        if fx is None:
            # 检测未完成符合条件的分型
            up_k = self.cl_klines[-2]
            now_k = self.cl_klines[-1]
            end_k = None
            if now_k.h > up_k.h:
                fx = FX(index=0, _type='ding', k=now_k, klines=[up_k, now_k, end_k], val=now_k.h, tk=False, real=True,
                        done=False)
            elif now_k.l < up_k.l:
                fx = FX(index=0, _type='di', k=now_k, klines=[up_k, now_k, end_k], val=now_k.l, tk=False, real=True,
                        done=False)
            else:
                return False

        if len(self.fxs) == 0 and fx.done is False:
            return False
        elif len(self.fxs) == 0 and fx.done is True:
            self.fxs.append(fx)
            return True

        # 检查和上个分型是否是一个，是就重新算
        is_update = False  # 标识本次是否是更新分型
        end_fx = self.fxs[-1]
        if fx.k.index == end_fx.k.index:
            end_fx.k = fx.k
            end_fx.klines = fx.klines
            end_fx.val = fx.val
            end_fx.tk = fx.tk
            end_fx.done = fx.done
            end_fx.real = fx.real
            fx = end_fx
            is_update = True

        # 检查分型有效性，根据上一个有效分型，进行检查
        up_fx = None
        # 记录区间中无效分型的最大最小值
        fx_qj_high = None
        fx_qj_low = None
        for _fx in self.fxs[::-1]:
            if is_update and _fx.index == fx.index:
                continue
            fx_qj_high = _fx.val if fx_qj_low is None else max(fx_qj_high, _fx.val)
            fx_qj_low = _fx.val if fx_qj_low is None else min(fx_qj_low, _fx.val)
            if _fx.real:
                up_fx = _fx
                break

        if up_fx is None:
            return False

        if fx.type == 'ding' and up_fx.type == 'ding' and up_fx.k.h <= fx.k.h:
            # 连续两个顶分型，前面的低于后面的，只保留后面的，前面的去掉
            up_fx.real = False
        elif fx.type == 'di' and up_fx.type == 'di' and up_fx.k.l >= fx.k.l:
            # 连续两个底分型，前面的高于后面的，只保留后面的，前面的去掉
            up_fx.real = False
        elif fx.type == up_fx.type:
            # 相邻的性质，必然前顶不能低于后顶，前底不能高于后底，遇到相同的，只保留第一个
            fx.real = False
        elif fx.type == 'ding' and up_fx.type == 'di' \
                and (fx.k.h <= up_fx.k.l or fx.k.l <= up_fx.k.h or fx.val < fx_qj_high):
            # 当前分型 顶，上一个分型 底，当 顶 低于 底， 或者 当前分型不是区间中最高的，是个无效的顶，跳过
            fx.real = False
        elif fx.type == 'di' and up_fx.type == 'ding' \
                and (fx.k.l >= up_fx.k.h or fx.k.h >= up_fx.k.l or fx.val > fx_qj_low):
            # 当前分型 底，上一个分型 顶 ，当 底 高于 顶，或者 当前分型不是区间中最低的，是个无效顶底，跳过
            fx.real = False
        else:
            # 顶与底之间缠论 K线 数量大于1
            if fx.k.index - up_fx.k.index >= 4:
                pass
            else:
                fx.real = False

        if is_update is False:
            fx.index = self.fxs[-1].index + 1
            self.fxs.append(fx)
        return True

    def process_bi(self):
        """
        根据最后的分型，找到对应的笔
        """
        if len(self.fxs) == 0:
            return False

        # 检查最后一笔的起始分型是否有效，无效则删除笔
        if len(self.bis) > 0 and self.bis[-1].start.real is False:
            del (self.bis[-1])

        bi = self.bis[-1] if len(self.bis) > 0 else None

        # 如果笔存在，检查是否有笔分型停顿
        if bi:
            close = self.klines[-1].c
            if bi.done and bi.type == 'up' and close < bi.end.klines[-1].l:
                bi.td = True
            elif bi.done and bi.type == 'down' and close > bi.end.klines[-1].h:
                bi.td = True
            else:
                bi.td = False

        if bi is None:
            real_fx = [_fx for _fx in self.fxs if _fx.real]
            if len(real_fx) < 2:
                return False
            for fx in real_fx:
                if bi is None:
                    bi = BI(start=fx, index=0)
                    continue
                if bi.start.type == fx.type:
                    continue
                bi.end = fx
                bi.type = 'up' if bi.start.type == 'di' else 'down'
                bi.high = max(bi.start.val, bi.end.val)
                bi.low = min(bi.start.val, bi.end.val)
                bi.done = fx.done
                bi.td = False
                bi.fx_num = bi.end.index - bi.start.index
                self.process_bi_ld(bi)  # 计算笔力度
                self.bis.append(bi)
                return True

        # 确定最后一个有效分型
        end_real_fx = None
        for _fx in self.fxs[::-1]:
            if _fx.real:
                end_real_fx = _fx
                break
        if bi.end.real is False and bi.end.type == end_real_fx.type:
            bi.end = end_real_fx
            bi.high = max(bi.start.val, bi.end.val)
            bi.low = min(bi.start.val, bi.end.val)
            bi.done = end_real_fx.done
            bi.fx_num = bi.end.index - bi.start.index
            self.process_bi_ld(bi)  # 计算笔力度
            return True

        if bi.end.index < end_real_fx.index and bi.end.type != end_real_fx.type:
            # 新笔产生了
            new_bi = BI(start=bi.end, end=end_real_fx)
            new_bi.index = self.bis[-1].index + 1
            new_bi.type = 'up' if new_bi.start.type == 'di' else 'down'
            new_bi.high = max(new_bi.start.val, new_bi.end.val)
            new_bi.low = min(new_bi.start.val, new_bi.end.val)
            new_bi.done = end_real_fx.done
            new_bi.td = False
            new_bi.fx_num = new_bi.end.index - new_bi.start.index
            self.process_bi_ld(new_bi)  # 计算笔力度
            self.bis.append(new_bi)
            return True
        return False

    def process_xd(self):
        """
        根据最后笔，生成特征序列，计算线段
        """

        if len(self.xds) == 0:
            dings = self.cal_xd_xlfx(self.bis, 'ding')
            dis = self.cal_xd_xlfx(self.bis, 'di')
            if len(dings) <= 0 or len(dis) <= 0:
                return False
            xlfxs = dings + dis
            xlfxs.sort(key=lambda xl: xl.bi.index)
            xlfxs = self.merge_xd_xlfx(xlfxs)
            if len(xlfxs) < 2:
                return False
            xd = XD(xlfxs[0].bi, xlfxs[1].bi, _type='up' if xlfxs[0].type == 'di' else 'down',
                    high=max(xlfxs[0].high, xlfxs[1].high), low=min(xlfxs[0].low, xlfxs[0].low))
            self.xds.append(xd)
            return True

        up_xd = self.xds[-1]
        dings = self.cal_xd_xlfx(self.bis[up_xd.end.index:], 'ding')
        dis = self.cal_xd_xlfx(self.bis[up_xd.end.index:], 'di')
        if len(dings) == 0 and len(dis) == 0:
            return False

        if up_xd.type == 'up' and len(dis) > 0:
            # 上一个线段是向上的，找底分型
            for di in dis:
                if di.bi.index - up_xd.end.index >= 2 and di.low < up_xd.high:
                    self.xds.append(
                        XD(up_xd.end, di.bi, _type='down', high=up_xd.high, low=di.low, index=up_xd.index + 1)
                    )
                    return True
        if up_xd.type == 'down' and len(dings) > 0:
            # 上一个线段是向下的，找顶分型
            for ding in dings:
                if ding.bi.index - up_xd.end.index >= 2 and ding.high > up_xd.low:
                    self.xds.append(
                        XD(up_xd.end, ding.bi, _type='up', high=ding.high, low=up_xd.low, index=up_xd.index + 1)
                    )
                    return True

        if up_xd.type == 'up' and len(dings) > 0:
            # 上一个线段是向上的，之后又出现了顶分型，进行线段的修正
            for ding in dings:
                if ding.high >= up_xd.high:
                    up_xd.end = ding.bi
                    up_xd.high = ding.high
            return True

        if up_xd.type == 'down' and len(dis) > 0:
            # 上一个线段是向下的，之后有出现了底分型，进行线段的修正
            for di in dis:
                if di.low <= up_xd.low:
                    up_xd.end = di.bi
                    up_xd.low = di.low
            return True

        return False

    def process_zs(self):
        """
        根据最后一笔，计算中枢
        """
        if len(self.bis) < 4:
            return False
        if len(self.zss) == 0:
            bis = self.bis[-4:]
            zs = self.create_zs(None, bis)
            if zs:
                # print('INIT ZS %s \n' % zs)
                self.zss.append(zs)
            return True

        bi = self.bis[-1]
        # 获取所有未完成的中枢，依次根据最新的笔进行重新计算
        for zs in self.zss:
            if zs.done:
                continue
            if zs.end.index == bi.end.index:
                continue
            # 调用创建中枢，属于更新一次中枢属性
            self.create_zs(zs, self.bis[zs.bis[0].index:bi.index + 1])
            # print('UPDATE ZS %s \n BI %s \n\n' % (zs, bi))
            # 如当前笔与中枢最后一笔格了一笔，则中枢已完成
            if bi.index - zs.bis[-1].index > 1:
                zs.done = True
                if len(zs.bis) < 5:  # 中枢笔小于5笔为无效中枢  进入一笔 + 3 + 离开一笔
                    zs.real = False

        # 以新笔为基础，创建中枢
        zs = self.create_zs(None, self.bis[-4:])
        if zs:
            # 检查是否已经有这个中枢了
            is_exists = False
            for _zs in self.zss[::-1]:
                if _zs.start.index == zs.start.index:
                    is_exists = True
                    break
            if is_exists is False:
                zs.index = self.zss[-1].index + 1
                self.zss.append(zs)
                # print('ADD ZS %s \n BI %s \n\n' % (zs, bi))

        return True

    def process_qs(self):
        """
        计算当前趋势（已经成型的）
        """
        if len(self.zss) == 0:
            return True
        bi = self.bis[-1]
        # 最后一个趋势是否有延续
        if len(self.qss) > 0:

            end_qs = self.qss[-1]
            zss = [_zs for _zs in self.zss if
                   (end_qs.start_bi.start.index <= _zs.start.index and _zs.end.index <= bi.end.index)]
            _qss = self.find_zs_qss(zss)
            for _qs in _qss:
                if _qs.type == end_qs.type and \
                        _qs.start_bi.start.index == end_qs.start_bi.start.index and \
                        _qs.end_bi.end.index != end_qs.end_bi.index:
                    end_qs.zss = _qs.zss
                    end_qs.end_bi = _qs.end_bi

        # 计算之后的中枢
        start_bi = self.qss[-1].end_bi if len(self.qss) > 0 else self.bis[0]
        zss = [_zs for _zs in self.zss if
               (start_bi.start.index <= _zs.start.index and _zs.end.index <= bi.end.index)]
        _qss = self.find_zs_qss(zss)
        for _qs in _qss:
            if _qs.type == 'up' or _qs.type == 'down':
                # 有新的趋势中枢，上一个走势与当前之间算震荡
                if len(self.qss) > 0:
                    up_qs = self.qss[-1]
                    qj_zss = [_zs for _zs in self.zss if
                              (up_qs.end_bi.end.index <= _zs.start.index and _zs.end.index <= _qs.start_bi.start.index)]
                    if len(qj_zss) > 0:
                        self.qss.append(
                            QS(start_bi=qj_zss[0].bis[0], end_bi=qj_zss[-1].bis[-1], zss=qj_zss, _type='zd'))
                self.qss.append(_qs)

        return True

    def process_mmd(self):
        """
        计算背驰与买卖点
        """
        if len(self.zss) == 0:
            return True

        bi = self.bis[-1]
        # 清空买卖点与背驰情况，重新计算
        bi.bcs = []
        bi.mmds = []

        # 笔背驰添加
        bi.add_bc('bi', None, self.bis[-3], self.beichi_bi(self.bis[-3], bi))
        # 查找所有以当前笔结束的中枢
        bi_zss = [_zs for _zs in self.zss if (_zs.bis[-1].index == bi.index and _zs.real and _zs.level == 0)]
        for zs in bi_zss:
            bi.add_bc('pz', zs, zs.bis[0], self.beichi_pz(zs, bi))
            bi.add_bc('qs', zs, zs.bis[0], self.beichi_qs(zs, bi))

        # 买卖点的判断
        # 一类买卖点，有趋势背驰，记为一类买卖点
        for bc in bi.bcs:
            if bc.type == 'qs' and bc.bc:
                if bi.type == 'up':
                    bi.add_mmd('1sell', bc.zs)
                if bi.type == 'down':
                    bi.add_mmd('1buy', bc.zs)

        # 二类买卖点，同向的前一笔突破，再次回拉不破，或者背驰，即为二类买卖点
        for zs in bi_zss:
            if len(zs.bis) < 7:
                continue
            tx_bi = zs.bis[-3]
            if zs.bis[0].type == 'up' and bi.type == 'up':
                if tx_bi.high == zs.gg and (tx_bi.high > bi.high or tx_bi.bc_exists(['pz', 'qs'])):
                    bi.add_mmd('2sell', zs)
            if zs.bis[0].type == 'down' and bi.type == 'down':
                if tx_bi.low == zs.dd and (tx_bi.low < bi.low or tx_bi.bc_exists(['pz', 'qs'])):
                    bi.add_mmd('2buy', zs)

        # 类二买卖点，当前中枢的第一笔是二类买卖点，并且离开中枢的力度比进入的力度弱，则为类二买卖点
        for zs in bi_zss:
            # 如果中枢笔中包含反方向买卖点或者背驰，则不在出类二买卖点了
            have_buy = False
            have_sell = False
            have_bc = False
            for _bi in zs.bis[:-1]:
                # 不包括当前笔
                if _bi.mmd_exists(['1buy', '2buy', 'l2buy', '3buy', 'l3buy']):
                    have_buy = True
                if _bi.mmd_exists(['1sell', '2sell', 'l2sell', '3sell', 'l3sell']):
                    have_sell = True
                if _bi.bc_exists(['pz', 'qs']):
                    have_bc = True
            if '2buy' in zs.bis[1].bi_mmds() and bi.type == 'down':
                if have_sell is False and have_bc is False and self.compare_ld_beichi(zs.bis[1].ld, bi.ld):
                    bi.add_mmd('l2buy', zs)
            if '2sell' in zs.bis[1].bi_mmds() and bi.type == 'up':
                if have_buy is False and have_bc is False and self.compare_ld_beichi(zs.bis[1].ld, bi.ld):
                    bi.add_mmd('l2sell', zs)

        # 三类买卖点，需要找中枢结束笔是前一笔的中枢
        bi_3mmd_zss = [_zs for _zs in self.zss if (_zs.bis[-1].index == bi.index - 1 and _zs.real and _zs.level == 0)]
        for zs in bi_3mmd_zss:
            if len(zs.bis) < 5:
                continue
            if bi.type == 'up' and bi.high < zs.zd:
                bi.add_mmd('3sell', zs)
            if bi.type == 'down' and bi.low > zs.zg:
                bi.add_mmd('3buy', zs)

        # 类三类买卖点，同类二买卖点差不多
        for zs in bi_zss:
            # 如果中枢笔中包含反方向买卖点或者背驰，则不在出类三买卖点了
            have_buy = False
            have_sell = False
            have_bc = False
            for _bi in zs.bis[:-1]:
                # 不包括当前笔
                if _bi.mmd_exists(['1buy', '2buy', 'l2buy', '3buy', 'l3buy']):
                    have_buy = True
                if _bi.mmd_exists(['1sell', '2sell', 'l2sell', '3sell', 'l3sell']):
                    have_sell = True
                if _bi.bc_exists(['pz', 'qs']):
                    have_bc = True
            for mmd in zs.bis[1].mmds:
                if mmd.name == '3buy':
                    if have_sell is False and have_bc is False and bi.type == 'down' \
                            and bi.low > mmd.zs.zg \
                            and self.compare_ld_beichi(zs.bis[0].ld, bi.ld):
                        bi.add_mmd('l3buy', mmd.zs)
                if mmd.name == '3sell':
                    if have_buy is False and have_bc is False and bi.type == 'up' \
                            and bi.high < mmd.zs.zd \
                            and self.compare_ld_beichi(zs.bis[0].ld, bi.ld):
                        bi.add_mmd('l3sell', mmd.zs)

        return True

    def beichi_bi(self, pre_bi: BI, now_bi: BI):
        """
        计算两笔之间是否有背驰，两笔必须是同方向的，最新的笔创最高最低记录
        背驰 返回 True，否则返回 False
        """
        if pre_bi.type != now_bi.type:
            return False
        if pre_bi.type == 'up' and now_bi.high < pre_bi.high:
            return False
        if pre_bi.type == 'down' and now_bi.low > pre_bi.low:
            return False

        return self.compare_ld_beichi(pre_bi.ld, now_bi.ld)

    def beichi_pz(self, zs: ZS, now_bi: BI):
        """
        判断中枢是否有盘整背驰，中枢最后一笔要创最高最低才可比较

        """
        if zs.bis[-1].index != now_bi.index:
            return False
        if zs.type not in ['up', 'down']:
            return False

        return self.compare_ld_beichi(zs.bis[0].ld, now_bi.ld)

    def beichi_qs(self, zs: ZS, now_bi: BI):
        """
        判断是否是趋势背驰，首先需要看之前是否有不重合的同向中枢，在进行背驰判断
        """
        if len(self.qss) == 0:
            # 无趋势无背驰
            return False
        if zs.bis[-1].index != now_bi.index:
            return False
        # 趋势最后一个中枢，是否和当前中枢是一个（根据 index 判断）
        end_qs = self.qss[-1]
        if end_qs.zss[-1].index != zs.index or end_qs.zss[-1].bis[-1].index != now_bi.index:
            return False
        # 最后两个中枢的级别要一致
        if end_qs.zss[-1].level != end_qs.zss[-2].level:
            return False
        # 趋势中枢与笔方向是否一致
        if end_qs.zss[-1].type != now_bi.type:
            return False
        # 两个趋势之间的力度，与离开中枢的力度做对比
        qj_ld = self.query_macd_ld(end_qs.zss[-2].end, end_qs.zss[-1].start)
        qj_ld = {'macd': qj_ld}
        return self.compare_ld_beichi(qj_ld, now_bi.ld)

    def create_zs(self, zs: [ZS, None], bis: List[BI]) -> [ZS, None]:
        """
        根据笔，获取是否有共同的中枢区间
        bis 中，第一笔是进入中枢的，不计算高低，最后一笔不一定是最后一个出中枢的，如果是最后一个出中枢的，则不需要计算高低点
        """
        if len(bis) < 3:
            return None

        # 进入段要笔中枢第一段高或低
        if bis[0].type == 'up' and bis[0].low > bis[1].low:
            return None
        if bis[0].type == 'down' and bis[0].high < bis[1].high:
            return None

        if zs is None:
            zs = ZS(start=bis[1].start, _type='zd')
        zs.bis = []
        zs.add_bi(bis[0])

        zs_fanwei = [bis[1].high, bis[1].low]
        zs_gg = bis[1].high
        zs_dd = bis[1].low
        for i in range(1, len(bis)):
            # 当前笔的交叉范围
            bi = bis[i]
            cross_fanwei = self.cross_qujian(zs_fanwei, [bi.high, bi.low])
            if cross_fanwei is None:
                break

            # 下一笔的交叉范围
            if i < len(bis) - 1:
                next_bi = bis[i + 1]
                next_fanwei = self.cross_qujian(zs_fanwei, [next_bi.high, next_bi.low])
            else:
                next_fanwei = True

            if next_fanwei:
                zs_gg = max(zs_gg, bi.high)
                zs_dd = min(zs_dd, bi.low)
                if i <= 2:
                    zs_fanwei = [cross_fanwei['max'], cross_fanwei['min']]
                # 根据笔数量，计算级别
                zs.bi_num = len(zs.bis)  # TODO 是否包含前后两笔？
                zs.level = int(zs.bi_num / 9)
                zs.end = bi.end
                # 记录中枢中，最大的笔力度
                if zs.max_ld is None:
                    zs.max_ld = bi.ld
                elif bi.ld:
                    zs.max_ld = zs.max_ld if self.compare_ld_beichi(zs.max_ld, bi.ld) else bi.ld
            zs.add_bi(bi)

        # 看看中枢笔数是否足够
        if len(zs.bis) < 4:
            return None

        zs.zg = zs_fanwei[0]
        zs.zd = zs_fanwei[1]
        zs.gg = zs_gg
        zs.dd = zs_dd

        # 计算中枢方向
        if zs.bis[0].type == zs.bis[-1].type:
            if zs.bis[0].type == 'up' and zs.bis[0].low < zs.dd and zs.bis[-1].high >= zs.gg:
                zs.type = zs.bis[0].type
            elif zs.bis[0].type == 'down' and zs.bis[0].high > zs.gg and zs.bis[-1].low <= zs.dd:
                zs.type = zs.bis[0].type
            else:
                zs.type = 'zd'
        else:
            zs.type = 'zd'

        return zs

    def process_bi_ld(self, bi: BI):
        """
        处理并计算笔的力度
        """
        bi.ld = {
            'macd': self.query_macd_ld(bi.start, bi.end)
        }
        return True

    def query_macd_ld(self, start_fx: FX, end_fx: FX):
        """
        计算分型区间 macd 力度
        """
        if start_fx.index > end_fx.index:
            raise Exception('%s - %s - %s 计算力度，开始分型不可以大于结束分型' % (self.code, self.frequency, self.klines[-1].date))

        dea = self.idx['macd']['dea'][start_fx.k.k_index:end_fx.k.k_index + 1]
        dif = self.idx['macd']['dif'][start_fx.k.k_index:end_fx.k.k_index + 1]
        hist = self.idx['macd']['hist'][start_fx.k.k_index:end_fx.k.k_index + 1]
        if len(hist) == 0:
            hist = np.array([0])
        if len(dea) == 0:
            dea = np.array([0])
        if len(dif) == 0:
            dif = np.array([0])

        hist_abs = abs(hist)
        hist_up = np.array([_i for _i in hist if _i > 0])
        hist_down = np.array([_i for _i in hist if _i < 0])
        hist_sum = hist_abs.sum()
        hist_up_sum = hist_up.sum()
        hist_down_sum = hist_down.sum()
        end_dea = dea[-1]
        end_dif = dif[-1]
        end_hist = hist[-1]
        return {
            'dea': {'end': end_dea, 'max': np.max(dea), 'min': np.min(dea)},
            'dif': {'end': end_dif, 'max': np.max(dif), 'min': np.min(dif)},
            'hist': {'sum': hist_sum, 'up_sum': hist_up_sum, 'down_sum': hist_down_sum, 'end': end_hist},
        }

    @staticmethod
    def cal_xd_xlfx(bis: List[BI], fx_type='ding'):
        """
        计算线段序列分型
        """
        xulie = []
        for bi in bis:
            if (fx_type == 'ding' and bi.type == 'down') or (fx_type == 'di' and bi.type == 'up'):
                now_xl = {'max': bi.high, 'min': bi.low, 'bi': bi}
                if len(xulie) == 0:
                    xulie.append(now_xl)
                    continue
                qs = 'up' if fx_type == 'ding' else 'down'
                up_xl = xulie[-1]
                if (up_xl['max'] >= now_xl['max'] and up_xl['min'] <= now_xl['min']) \
                        or (up_xl['max'] <= now_xl['max'] and up_xl['min'] >= now_xl['min']):
                    if qs == 'up':
                        now_xl['bi'] = now_xl['bi'] if now_xl['max'] >= up_xl['max'] else up_xl['bi']
                        now_xl['max'] = max(up_xl['max'], now_xl['max'])
                        now_xl['min'] = max(up_xl['min'], now_xl['min'])
                    else:
                        now_xl['bi'] = now_xl['bi'] if now_xl['min'] <= up_xl['min'] else up_xl['bi']
                        now_xl['max'] = min(up_xl['max'], now_xl['max'])
                        now_xl['min'] = min(up_xl['min'], now_xl['min'])

                    del (xulie[-1])
                    xulie.append(now_xl)
                else:
                    xulie.append(now_xl)

        xlfxs: List[XLFX] = []
        for i in range(1, len(xulie) - 1):
            up_xl = xulie[i - 1]
            now_xl = xulie[i]
            next_xl = xulie[i + 1]
            if fx_type == 'ding' and up_xl['max'] <= now_xl['max'] and now_xl['max'] >= next_xl['max']:
                now_xl['type'] = 'ding'
                xlfxs.append(
                    XLFX('ding', now_xl['max'], now_xl['min'], now_xl['bi'])
                )
            if fx_type == 'di' and up_xl['min'] >= now_xl['min'] and now_xl['min'] <= next_xl['min']:
                now_xl['type'] = 'di'
                xlfxs.append(
                    XLFX('di', now_xl['max'], now_xl['min'], now_xl['bi'])
                )

        return xlfxs

    @staticmethod
    def merge_xd_xlfx(xlfxs: List[XLFX]):
        """
        合并线段的顶底序列分型，过滤掉无效的分型
        """
        real_xl_fxs = []
        for xl in xlfxs:
            if len(real_xl_fxs) == 0:
                real_xl_fxs.append(xl)
                continue
            up_xl = real_xl_fxs[-1]
            if up_xl.type == 'ding' and xl.type == 'ding':
                # 两个顶序列分型
                if xl.high > up_xl.high:
                    del (real_xl_fxs[-1])
                    real_xl_fxs.append(xl)
            elif up_xl.type == 'di' and xl.type == 'di':
                # 两个低序列分型
                if xl.low < up_xl.low:
                    del (real_xl_fxs[-1])
                    real_xl_fxs.append(xl)
            elif up_xl.type == 'ding' and xl.type == 'di' and up_xl.high < xl.low:
                continue
            elif up_xl.type == 'di' and xl.type == 'ding' and up_xl.low > xl.high:
                continue
            elif xl.bi.index - up_xl.bi.index < 3:  # 线段不足3笔
                continue
            else:
                real_xl_fxs.append(xl)

        return real_xl_fxs

    @staticmethod
    def compare_ld_beichi(one_ld: dict, two_ld: dict):
        """
        比较两个力度，后者小于前者，返回 True
        :param one_ld:
        :param two_ld:
        :return:
        """
        hist_key = 'sum'
        # if hist_type == 'up':
        #     hist_key = 'up_sum'
        # if hist_type == 'down':
        #     hist_key = 'down_sum'
        if two_ld['macd']['hist'][hist_key] < one_ld['macd']['hist'][hist_key]:
            return True
        else:
            return False

    @staticmethod
    def cross_qujian(qj_one, qj_two):
        """
        计算两个范围相交部分区间
        :param qj_one:
        :param qj_two:
        :return:
        """
        # 判断线段是否与范围值内有相交
        max_one = max(qj_one[0], qj_one[1])
        min_one = min(qj_one[0], qj_one[1])
        max_two = max(qj_two[0], qj_two[1])
        min_two = min(qj_two[0], qj_two[1])

        cross_max_val = min(max_two, max_one)
        cross_min_val = max(min_two, min_one)

        if cross_max_val >= cross_min_val:
            return {'max': cross_max_val, 'min': cross_min_val}
        else:
            return None

    @staticmethod
    def klines_baohan(klines: List[Kline], up_cl_klines: List[CLKline]) -> List[CLKline]:
        """
        k线包含处理，返回缠论k线对象
        """
        cl_klines = []
        cl_kline = CLKline(k_index=klines[0].index, date=klines[0].date,
                           h=klines[0].h, l=klines[0].l, o=klines[0].o, c=klines[0].c, a=klines[0].a,
                           klines=[klines[0]])
        cl_klines.append(cl_kline)
        up_cl_klines.append(cl_kline)

        # if klines[0].date >= datetime.datetime.strptime('2021-06-03 00:00:00', '%Y-%m-%d %H:%M:%S'):
        #     a = 1

        for i in range(1, len(klines)):
            cl_k = cl_klines[-1]
            k = klines[i]
            if (cl_k.h >= k.h and cl_k.l <= k.l) or (k.h >= cl_k.h and k.l <= cl_k.l):
                qushi = 'up' if len(up_cl_klines) >= 2 and up_cl_klines[-2].h < cl_k.h else 'down'
                if qushi == 'up':  # 趋势上涨，向上合并
                    cl_k.k_index = cl_k.k_index if cl_k.h > k.h else k.index
                    cl_k.date = cl_k.date if cl_k.h > k.h else k.date
                    cl_k.h = max(cl_k.h, k.h)
                    cl_k.l = max(cl_k.l, k.l)
                    cl_k.a += k.a  # 交易量累加
                    cl_k.up_qs = 'up'
                else:
                    cl_k.k_index = cl_k.k_index if cl_k.l < k.l else k.index
                    cl_k.date = cl_k.date if cl_k.l < k.l else k.date
                    cl_k.h = min(cl_k.h, k.h)
                    cl_k.l = min(cl_k.l, k.l)
                    cl_k.a += k.a
                    cl_k.up_qs = 'down'
                cl_k.klines.append(k)
                cl_k.n += 1
            else:
                cl_kline = CLKline(k_index=k.index, date=k.date, h=k.h, l=k.l, o=k.o, c=k.c, a=k.a, klines=[k])
                cl_klines.append(cl_kline)
                up_cl_klines.append(cl_kline)

        return cl_klines

    @staticmethod
    def find_zs_qss(zss: List[ZS]) -> List[QS]:
        """
        查找中枢列表 两两互不关联的中枢
        """
        qss = []
        zss = sorted(zss, key=lambda z: z.index, reverse=False)

        def copy_zs(_zs: ZS) -> ZS:
            """
            复制一个新的中枢对象
            """
            new_zs = ZS(
                start=_zs.start, end=_zs.end, zg=_zs.zg, zd=_zs.zd, gg=_zs.gg, dd=_zs.dd, _type=_zs.type,
                index=_zs.index, bi_num=_zs.bi_num, level=_zs.level
            )
            new_zs.bis = _zs.bis
            return new_zs

        for zs in zss:
            if zs.type not in ['up', 'down']:
                continue
            qs = None
            start_zs = zs
            for next_zs in zss:
                if next_zs.type != start_zs.type:
                    continue
                if next_zs.index <= start_zs.index:
                    continue
                if (start_zs.gg < next_zs.dd or start_zs.dd > next_zs.gg) and (
                        next_zs.bis[0].index - start_zs.bis[-1].index <= 2):
                    if qs is None:
                        qs = QS(start_bi=start_zs.bis[0], end_bi=start_zs.bis[-1], zss=[copy_zs(start_zs)],
                                _type=start_zs.type)
                    qs.zss.append(copy_zs(next_zs))
                    qs.end_bi = next_zs.bis[-1]
                    start_zs = next_zs

            if qs and len(qs.zss) >= 2:
                qss.append(qs)

        return qss


def batch_cls(code, klines: Dict[str, pd.DataFrame]) -> List[CL]:
    """
    批量计算并获取 缠论 数据
    :param code:
    :param klines:
    :return:
    """
    cls = []
    for f in klines.keys():
        cls.append(CL(code, f).process_klines(klines[f]))
    return cls


class MultiLevelAnalyse(object):
    """
    缠论多级别分析
    """

    def __init__(self, up_cd: CL, low_cd: CL):
        self.up_cd: CL = up_cd
        self.low_cd: CL = low_cd

    def low_level_qs_by_bi(self, up_bi: BI):
        """
        根据高级别笔，获取其低级别笔的趋势信息
        """
        low_bis = self.__query_low_bis_by_bi(up_bi)
        low_zss = self.__query_low_zss_by_bi(up_bi)

        # 低级别最后一个与高级笔同向的笔
        low_last_bi = low_bis[-1] if len(low_bis) > 0 else None

        # 是否包含至少一个同向的中枢
        same_type_zs = len([_zs for _zs in low_zss if (_zs.type == up_bi.type)])

        qs_done = False
        if len(low_zss) > 0 and low_last_bi and low_last_bi.bc_exists(['pz', 'qs']):
            qs_done = True

        return {
            'zss': low_zss,
            'zs_num': len(low_zss),
            'same_type_zs': same_type_zs,
            'bis': low_bis,
            'bi_num': len(low_bis),
            'last_bi_pz_bc': low_last_bi.bc_exists(['pz']) if low_last_bi is not None else False,
            'last_bi_qs_bc': low_last_bi.bc_exists(['qs']) if low_last_bi is not None else False,
            'last_bi_bi_bc': low_last_bi.bc_exists(['bi']) if low_last_bi is not None else False,
            'low_last_bi': low_last_bi,
            'qs_done': qs_done,
        }

    def up_bi_low_level_qs(self):
        """
        高级别笔，最后一笔的低级别趋势信息
        """
        last_bi = self.up_cd.bis[-1]
        return self.low_level_qs_by_bi(last_bi)

    def __query_low_bis_by_bi(self, up_bi: BI):
        """
        查询高级别笔中包含的低级别的笔，根据高级的笔来查找
        """
        start_date = up_bi.start.k.date
        end_date = up_bi.end.k.date

        low_bis: List[BI] = []
        for _bi in self.low_cd.bis:
            if _bi.start.k.date < start_date:
                continue
            if end_date is not None and _bi.start.k.date > end_date:
                break
            if len(low_bis) == 0 and _bi.type != up_bi.type:
                continue
            low_bis.append(_bi)
        if len(low_bis) > 0 and low_bis[-1].type != up_bi.type:
            del (low_bis[-1])

        return low_bis

    def __query_low_zss_by_bi(self, up_bi: BI):
        """
        查询高级别笔中包含的低级别的中枢，根据高级的笔来查找
        """
        start_date = up_bi.start.k.date
        end_date = up_bi.end.k.date

        low_zss: List[ZS] = []
        for _zs in self.low_cd.zss:
            if start_date <= _zs.start.k.date <= end_date:
                low_zss.append(_zs)

        return low_zss
