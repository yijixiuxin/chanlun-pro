import datetime
import time
from typing import List, Tuple, Dict

import numpy as np
import pandas as pd
import talib as ta


# 聚宽平台 python 3.6，不支持 dataclass

class Kline:
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
        self._n: int = _n  # 记录包含的K线数量
        self._q: bool = _q  # 是否有缺口

    def __str__(self):
        return "index: %s k_index:%s date: %s h: %s l: %s _n:%s _q:%s" % \
               (self.index, self.k_index, self.date, self.h, self.l, self._n, self._q)


class FX:
    def __init__(self, _type: str, k: CLKline, klines: List[CLKline], val: float,
                 index: int = 0, tk: bool = False, real: bool = True, done: bool = True):
        self.type: str = _type
        self.k: CLKline = k
        self.klines: List[CLKline] = klines
        self.val: float = val
        self.index: int = index
        self.tk: bool = tk  # 记录是否跳空
        self.real: bool = real
        self.done: bool = done


class BI:
    def __init__(self, start: FX, end: FX = None, high: float = None, low: float = None, _type: str = None,
                 ld: dict = None, done: bool = None, mmds: list = [], index: int = 0, qs_beichi: bool = False,
                 pz_beichi: bool = False, td: bool = False, fx_num: int = 0):
        self.start: FX = start
        self.end: FX = end
        self.high: float = high
        self.low: float = low
        self.type: str = _type
        self.ld: dict = ld
        self.done: bool = done  # 笔是否完成
        self.mmds: list = mmds  # 买卖点
        self.index: int = index
        self.qs_beichi: bool = qs_beichi  # 趋势背驰
        self.pz_beichi: bool = pz_beichi  # 盘整背驰
        self.td: bool = td  # 笔是否停顿
        self.fx_num: int = fx_num  # 包含的分型数量


class XD:
    def __init__(self, start: FX, end: FX = None, _type: str = None, high: float = None, low: float = None,
                 index: int = 0):
        self.start: FX = start
        self.end: FX = end
        self.type: str = _type
        self.high: float = high
        self.low: float = low
        self.index: int = index


class ZS:
    def __init__(self, start: FX, bis: List[BI], end: FX = None, zg: float = None, zd: float = None,
                 gg: float = None, dd: float = None, _type: str = None, index: int = 0, bi_num: int = 0,
                 level: int = 0, is_high_kz: bool = False, max_ld: dict = None):
        self.start: FX = start
        self.bis: List[BI] = bis
        self.end: FX = end
        self.zg: float = zg
        self.zd: float = zd
        self.gg: float = gg
        self.dd: float = dd
        self.type: str = _type
        self.index: int = index
        self.bi_num: int = bi_num
        self.level: int = level  # 中枢级别 0 本级别 1 上一级别 ...
        self.is_high_kz: bool = is_high_kz  # 中枢是否是高级别的扩展
        self.max_ld: dict = max_ld  # 记录中枢中最大笔力度


class CL:
    """
    缠论类
    """

    def __init__(self, code: str, klines: pd.DataFrame, frequency: str):
        """
        缠论计算
        :param klines:
        :param frequency:
        """
        self.code = code
        self.frequency = frequency

        # 计算后保存的值
        self.klines: List[Kline] = []
        self.cl_klines: List[CLKline] = []  # 缠论K线
        self.idx: dict = {}  # ta-lib 各种指标
        self.fxs: List[FX] = []  # 分型列表
        self.bis: List[BI] = []  # 笔列表
        self.xds: List[XD] = []  # 线段列表
        self.zss: List[ZS] = []  # 中枢列表

        # 初始处理 缠论K线
        self.process_cl_kline(klines)
        # 处理 缠论数据
        self.process_cl_datas()

    def process_cl_datas(self):
        """
        根据缠论 K 线，计算缠论的 分型、笔、线段、中枢、买卖点
        :return:
        """
        self.idx = self.process_idx()  # 计算技术指标
        self.fxs = self.process_fx()
        self.bis = self.process_bi()
        self.process_ld()  # 处理笔的力度
        self.xds = self.process_xd()
        self.zss = self.process_zs(self.bis)
        self.process_mmds()

    def process_cl_kline(self, klines: pd.DataFrame):
        """
        将初始 K 线整理为缠论包含关系的 k 线
        :return:
        """
        for i in range(len(klines)):
            nk = klines.iloc[i]
            date = self.__process_date(nk['date'])
            k = Kline(index=i, date=date,
                      h=float(nk['high']), l=float(nk['low']),
                      o=float(nk['open']), c=float(nk['close']), a=float(nk['volume']))
            self.klines.append(k)

            is_new, cl_k = self.__create_cl_kline(k)
            if is_new:
                self.cl_klines.append(cl_k)
            else:
                self.cl_klines[-1] = cl_k

        # 重新编号
        for i in range(len(self.cl_klines)):
            self.cl_klines[i].index = i  # 重新编号

        return

    def increment_process_kline(self, klines: pd.DataFrame):
        """
        增量添加并处理 Kline
        :param klines:
        :return:
        """

        # 根据最后一个 缠论K线，进行增量更新处理（保证相同时间K线的数据进行更新）
        last_cl_kline = self.cl_klines[-1]
        start_kline = self.klines[last_cl_kline.klines[0].index - 1]

        # 删除后续的 kline 数据
        del (self.cl_klines[-1])
        for i in range(self.klines[-1].index - start_kline.index):
            del (self.klines[-1])

        run_klines = klines[klines['date'] >= start_kline.date]
        for i in range(len(run_klines)):
            nk = run_klines.iloc[i]
            if nk['date'] <= start_kline.date:
                # 已经处理的进行跳过
                continue

            k = Kline(index=start_kline.index + 1, date=nk['date'],
                      h=float(nk['high']), l=float(nk['low']),
                      o=float(nk['open']), c=float(nk['close']), a=float(nk['volume']))
            self.klines.append(k)
            start_kline = k

            is_new, cl_k = self.__create_cl_kline(k)
            if is_new:
                self.cl_klines.append(cl_k)
            else:
                self.cl_klines[-1] = cl_k

        # 重新编号
        for i in range(len(self.cl_klines)):
            self.cl_klines[i].index = i  # 重新编号

        # 重新计算缠论数据
        self.process_cl_datas()

    def process_idx(self) -> map:
        """
        计算所需要的技术指标
        :return:
        """
        prices = []
        for k in self.klines:
            prices.append(k.c)
        # 计算 macd
        macd_dif, macd_dea, macd_hist = ta.MACD(np.array(prices), fastperiod=12, slowperiod=26, signalperiod=9)
        macd = {'dea': macd_dea, 'dif': macd_dif, 'hist': macd_hist}

        # 计算 BOLL 指标
        boll_up, boll_mid, boll_low = ta.BBANDS(np.array(prices), timeperiod=20)

        return {
            'macd': macd,
            'boll': {'up': boll_up, 'mid': boll_mid, 'low': boll_low}
        }

    def process_fx(self) -> List[FX]:
        """
        找出图形中的 分型
        :return:
        """
        fxs = []
        for i in range(1, len(self.cl_klines) - 1):
            up_k = self.cl_klines[i - 1]
            now_k = self.cl_klines[i]
            next_k = self.cl_klines[i + 1]
            fx = None
            if (up_k.h < now_k.h and now_k.h > next_k.h) \
                    and (up_k.l < now_k.l and now_k.l > next_k.l):
                tiaokong = True if (up_k.h < now_k.l or now_k.l > next_k.h) else False
                fx = FX(_type='ding', k=now_k, klines=[up_k, now_k, next_k], val=now_k.h, tk=tiaokong,
                        real=True, done=True)
            if (up_k.h > now_k.h and now_k.h < next_k.h) \
                    and (up_k.l > now_k.l and now_k.l < next_k.l):
                tiaokong = True if (up_k.l > now_k.h or now_k.h < next_k.l) else False
                fx = FX(_type='di', k=now_k, klines=[up_k, now_k, next_k], val=now_k.l, tk=tiaokong,
                        real=True, done=True)

            if fx is not None:
                if len(fxs) == 0:
                    fxs.append(fx)
                    continue

                up_fx = self.__get_up_real_fx(fxs)
                if up_fx is False:
                    continue

                if fx.type == 'ding' and up_fx.type == 'ding' and up_fx.k.h <= fx.k.h:
                    # 连续两个顶分型，前面的低于后面的，只保留后面的，前面的去掉
                    up_fx.real = False
                    fxs.append(fx)
                elif fx.type == 'di' and up_fx.type == 'di' and up_fx.k.l >= fx.k.l:
                    # 连续两个底分型，前面的高于后面的，只保留后面的，前面的去掉
                    up_fx.real = False
                    fxs.append(fx)
                elif fx.type == up_fx.type:
                    # 相邻的性质，必然前顶不能低于后顶，前底不能高于后底，遇到相同的，只保留第一个
                    fx.real = False
                    fxs.append(fx)
                elif fx.type == 'ding' and up_fx.type == 'di' \
                        and (fx.k.h <= up_fx.k.l or fx.k.l <= up_fx.k.h):
                    # 当前分型 顶，上一个分型 底，当 顶 低于 底，是个无效的顶，跳过
                    fx.real = False
                    fxs.append(fx)
                elif fx.type == 'di' and up_fx.type == 'ding' \
                        and (fx.k.l >= up_fx.k.h or fx.k.h >= up_fx.k.l):
                    # 当前分型 底，上一个分型 顶 ，当 底 高于 顶，是个无效顶底，跳过
                    fx.real = False
                    fxs.append(fx)
                else:
                    # 顶与底直接底非包含关系的 K线 数量大于等于3
                    if fx.k.index - up_fx.k.index >= 4:
                        fxs.append(fx)
                    else:
                        fx.real = False
                        fxs.append(fx)

        # 计算最后一段未完成的笔，如果符合随时可完成的状态，记录分型
        up_fx = self.__get_up_real_fx(fxs)
        if up_fx:
            end_k = self.cl_klines[-1]
            # 顶分型 突破新高，将笔画到新高点
            if up_fx.type == 'ding' and end_k.h > up_fx.k.h:
                up_fx.real = False
                fxs.append(
                    FX(_type='ding', k=end_k, klines=[self.cl_klines[-2], end_k, None], val=end_k.h, tk=False,
                       real=True, done=False)
                )
            elif up_fx.type == 'ding' and end_k.index - up_fx.k.index >= 4 \
                    and end_k.l < up_fx.k.l and end_k.h < up_fx.k.h \
                    and up_fx.k.index != fxs[-1].k.index:
                fxs.append(
                    FX(_type='di', k=end_k, klines=[self.cl_klines[-2], end_k, None], val=end_k.l, tk=False,
                       real=True, done=False)
                )
            # 底分型 突破新低，将笔画到新低点
            elif up_fx.type == 'di' and end_k.l < up_fx.k.l:
                up_fx.real = False
                fxs.append(
                    FX(_type='di', k=end_k, klines=[self.cl_klines[-2], end_k, None], val=end_k.l, tk=False,
                       real=True, done=False)
                )
            elif up_fx.type == 'di' and end_k.index - up_fx.k.index >= 4 \
                    and end_k.h > up_fx.k.h and end_k.l > up_fx.k.l \
                    and up_fx.k.index != fxs[-1].k.index:
                fxs.append(
                    FX(_type='ding', k=end_k, klines=[self.cl_klines[-2], end_k, None], val=end_k.h, tk=False,
                       real=True, done=False)
                )

        # 给分型编号，后续好直接查找
        for i in range(len(fxs)):
            fxs[i].index = i
        return fxs

    def process_bi(self) -> List[BI]:
        """
        找出顶分型与低分型连接的 笔
        :return:
        """
        bis = []
        bi = None
        # 记录一笔中，无效的顶底分型数量
        _dd_total = 0
        for fx in self.fxs:
            if not fx.real:
                _dd_total += 1
                continue

            if bi is None:
                bi = BI(start=fx)
                continue

            if bi.start.type == fx.type:
                # 相同分型跳过
                continue

            bi.end = fx
            bi.type = 'up' if bi.start.type == 'di' else 'down'
            bi.high = max(bi.start.val, bi.end.val)
            bi.low = min(bi.start.val, bi.end.val)
            bi.qs_beichi = False
            bi.pz_beichi = False
            bi.mmds = []
            bi.done = fx.done
            bi.td = False
            bi.fx_num = _dd_total
            bis.append(bi)
            # 重置技术
            bi = BI(start=fx)
            _dd_total = 0

        # 确认最后笔分型是否停顿(只计算最后指定几笔的，之前的没必要算了)
        if len(bis) > 0:
            loop_num = min(6, len(bis) - 1)
            for i in range(1, loop_num):
                last_bi = bis[-i]
                if last_bi.done and len(self.cl_klines) > last_bi.end.k.index + 1:
                    cl_kline_next = self.cl_klines[last_bi.end.k.index + 1]
                    for i in range(last_bi.end.k.k_index + 1, len(self.klines)):
                        if last_bi.type == 'up' and self.klines[i].c < cl_kline_next.l:
                            last_bi.td = True
                            break
                        if last_bi.type == 'down' and self.klines[i].c > cl_kline_next.h:
                            last_bi.td = True
                            break

        # 给 笔 编号，后续好直接找
        for i in range(0, len(bis)):
            bis[i].index = i

        # 从新计算高低值，包含其中的K线
        for bi in bis:
            for i in range(bi.start.k.k_index, bi.end.k.k_index + 1):
                bi.high = max(bi.high, self.klines[i].h)
                bi.low = min(bi.low, self.klines[i].l)

        return bis

    def process_ld(self):
        """
        处理计算 笔 力度
        :return:
        """
        # 计算 线段 笔的 macd 力度
        for bi in self.bis:
            macd_ld = self.__query_macd_ld(bi.start, bi.end)
            bi.ld = {'macd': macd_ld}

        return

    def process_xd(self) -> List[XD]:
        """
        根据笔，生成线段
        :return:
        """
        xds = []
        # 顶底分型合并 并 排序
        dings = self.__find_bi_xulie_fx('ding')
        dis = self.__find_bi_xulie_fx('di')
        xl_fxs = dings + dis
        xl_fxs.sort(key=lambda xl: xl['bi'].start.index)

        if len(xl_fxs) == 0:
            return xds

        real_xl_fxs = [xl_fxs[0]]
        for i in range(1, len(xl_fxs)):
            up_xl = real_xl_fxs[-1]
            now_xl = xl_fxs[i]
            if up_xl['type'] == 'ding' and now_xl['type'] == 'ding':
                # 两个顶序列分型
                if now_xl['max'] > up_xl['max']:
                    del (real_xl_fxs[-1])
                    real_xl_fxs.append(now_xl)
            elif up_xl['type'] == 'di' and now_xl['type'] == 'di':
                # 两个低序列分型
                if now_xl['min'] < up_xl['min']:
                    del (real_xl_fxs[-1])
                    real_xl_fxs.append(now_xl)
            elif now_xl['bi'].index - up_xl['bi'].index < 3:  # 线段不足3笔
                continue
            else:
                real_xl_fxs.append(now_xl)

        xd = None
        for fx in real_xl_fxs:
            if xd is None:
                xd = XD(start=fx['bi'].start)
                xd.type = 'up' if fx['type'] == 'di' else 'down'
                continue
            if (xd.type == 'up' and fx['type'] == 'ding') \
                    or (xd.type == 'down' and fx['type'] == 'di'):
                xd.end = fx['bi'].end
                xds.append(xd)
                xd = XD(start=fx['bi'].start)
                xd.type = 'up' if fx['type'] == 'di' else 'down'

        for i in range(len(xds)):
            xd = xds[i]
            xd.high = max(xd.start.val, xd.end.val)
            xd.low = min(xd.start.val, xd.end.val)
            xd.index = i
        return xds

    def process_zs(self, bis: List[BI]) -> List[ZS]:
        """
        处理中枢 (分解中枢)
        :return:
        """
        zss = []
        if len(bis) <= 3:
            return zss

        start = 1
        end = start + 3

        _run_zs = {'zd': None, 'up': None, 'down': None}

        while True:
            if end >= len(bis):
                if _run_zs['up'] is not None:
                    zss.append(_run_zs['up'])
                    _run_zs = {'zd': None, 'up': None, 'down': None}
                    start = zss[-1].bis[-1].index + 1
                    if len(bis) - start >= 5:
                        end = start + 3
                    else:
                        break
                elif _run_zs['down'] is not None:
                    zss.append(_run_zs['down'])
                    _run_zs = {'zd': None, 'up': None, 'down': None}
                    start = zss[-1].bis[-1].index + 1
                    if len(bis) - start >= 5:
                        end = start + 3
                    else:
                        break
                elif len(bis) - start > 3:
                    start += 1
                    end = start + 3
                elif _run_zs['zd'] is not None:
                    zss.append(_run_zs['zd'])
                    break
                else:
                    break
            # print('start : %s end : %s' % (start, end))
            _bis = bis[start:end]
            zs = self.__create_zs(_bis)

            if zs is not None:
                _run_zs[zs.type] = zs
                self.__compare_zs_ld(zss, bis, zs, end)
                end += 1
            elif zs is None:
                if _run_zs['up'] is not None:
                    zss.append(_run_zs['up'])
                    _run_zs = {'zd': None, 'up': None, 'down': None}
                    start = zss[-1].bis[-1].index + 1
                    end = start + 3
                elif _run_zs['down'] is not None:
                    zss.append(_run_zs['down'])
                    _run_zs = {'zd': None, 'up': None, 'down': None}
                    start = zss[-1].bis[-1].index + 1
                    end = start + 3
                else:
                    start += 1
                    end = start + 3

        # 编号
        for i in range(len(zss)):
            zss[i].index = i
            if i > 0:
                # 如果中枢与前一个中枢有重合，则扩展为一个高级别的中枢扩展
                if self.__cross_qujian([zss[i - 1].zg, zss[i - 1].zd], [zss[i].zg, zss[i].zd]) is not None:
                    zss[i].is_high_kz = True

        return zss

    def process_mmds(self):
        """
        处理买卖点
        :return:
        """
        # 检查买卖点
        for bi in self.bis:
            bi.mmds = []  # process_mmds 这个方法会多次运行，避免 mmds 一直增加，初始化一下

            if bi.qs_beichi:
                # 只要是趋势背驰，那就是第一买卖点
                if bi.type == 'up':
                    bi.mmds.append('1sell')
                elif bi.type == 'down':
                    bi.mmds.append('1buy')

            zs = self.__find_bi_zs(bi)
            if zs is None:  # 没有中枢则没有买卖点
                continue

            zs_one_bi = zs.bis[0]
            zs_end_bi = zs.bis[-1]

            up_same_bi = self.bis[bi.index - 2]

            # 检查 2 类买卖点
            if '1buy' in up_same_bi.mmds:
                if bi.type == 'down' and bi.low > up_same_bi.low:
                    bi.mmds.append('2buy')
            if '1sell' in up_same_bi.mmds:
                if bi.type == 'up' and bi.high < up_same_bi.high:
                    bi.mmds.append('2sell')

            # 离开中枢一笔（新高、新低），后续一笔回拉不突破前高、前低，就是二类买卖点
            if bi.type == 'up' and up_same_bi.high >= zs.gg \
                    and zs.bis[-1].end.index == up_same_bi.start.index and bi.high < up_same_bi.high:
                bi.mmds.append('2sell')
            if bi.type == 'down' and up_same_bi.low <= zs.dd \
                    and zs.bis[-1].end.index == up_same_bi.start.index and bi.low > up_same_bi.low:
                bi.mmds.append('2buy')

            # 检查类2买卖点
            if '2buy' in up_same_bi.mmds:
                if bi.type == 'down' and self.__compare_ld_beichi(up_same_bi.ld, bi.ld):
                    bi.mmds.append('l2buy')
            if '2sell' in up_same_bi.mmds:
                if bi.type == 'up' and self.__compare_ld_beichi(up_same_bi.ld, bi.ld):
                    bi.mmds.append('l2sell')

            # 查找三类买卖点
            if bi.type == 'down' and bi.low > zs.zg and bi.index - zs_end_bi.index == 2:
                bi.mmds.append('3buy')
            if bi.type == 'up' and bi.high < zs.zd and bi.index - zs_end_bi.index == 2:
                bi.mmds.append('3sell')

        return

    def __process_date(self, d):
        """
        将时间统一处理
        :param d:
        :return:
        """
        d = str(d)
        d = d.replace('+08:00', '')
        if len(d) == 10:
            return datetime.datetime.strptime(d, '%Y-%m-%d')
        else:
            return datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')

    def __create_cl_kline(self, k: Kline) -> Tuple[bool, CLKline]:
        """
        根据 K线 生成 缠论K线，处理其中的包含关系
        :param k:
        :return: (true 为新增缠论Kline，false 为包含缠论Kline)
        """
        cl_k = CLKline(date=k.date, k_index=k.index, h=k.h, l=k.l, o=k.o, c=k.c, a=k.a, klines=[k], _n=1, _q=False)
        # 判断是否有跳空缺口，有跳空缺口，补一根 缠论K线
        if len(self.klines) > 2:
            up_kline = self.klines[-2]
            if up_kline.l > k.h or up_kline.h < k.l:
                # print('有跳空', len(self.klines), up_kline, k)
                self.cl_klines.append(
                    CLKline(date=k.date, k_index=k.index,
                            h=min(k.l, self.klines[k.index - 1].l),
                            l=min(k.h, self.klines[k.index - 1].h), o=k.o, c=k.c, a=k.a, klines=[], _n=1, _q=True)
                )

        if k.index <= 1:
            # 前两根 K线 直接返回
            return True, cl_k

        upup_k = self.cl_klines[-2]
        up_k = self.cl_klines[-1]
        # 判断趋势
        qushi = 'up' if up_k.h > upup_k.h else 'down'

        if (up_k.h >= cl_k.h and up_k.l <= cl_k.l) or (cl_k.h >= up_k.h and cl_k.l <= up_k.l):
            if qushi == 'up':  # 趋势上涨，向上合并
                up_k.k_index = up_k.k_index if up_k.h > cl_k.h else cl_k.k_index
                up_k.date = up_k.date if up_k.h > cl_k.h else cl_k.date
                up_k.h = max(up_k.h, cl_k.h)
                up_k.l = max(up_k.l, cl_k.l)
                up_k.a += cl_k.a  # 交易量累加
            else:
                up_k.k_index = up_k.k_index if up_k.l < cl_k.l else cl_k.k_index
                up_k.date = up_k.date if up_k.l < cl_k.l else cl_k.date
                up_k.h = min(up_k.h, cl_k.h)
                up_k.l = min(up_k.l, cl_k.l)
                up_k.a += cl_k.a
            up_k.klines.append(k)
            up_k._n += 1
            return False, up_k

        return True, cl_k

    def __get_up_real_fx(self, fxs: List[FX]) -> FX:
        """
        查找上一个确认的分型
        :return:
        """
        for i in range(1, len(fxs) + 1):
            f = fxs[-i]
            if f.real:
                return f
        return False

    def __find_bi_xulie_fx(self, fx_type='ding'):
        """
        查找笔序列分型
        :param fx_type:
        :return:
        """
        xulie = []
        for i in range(1, len(self.bis)):
            bi = self.bis[i]
            if (fx_type == 'ding' and bi.type == 'down') or (fx_type == 'di' and bi.type == 'up'):
                now_xl = {'max': bi.high, 'min': bi.low, 'bi': bi}
                if len(xulie) == 0:
                    xulie.append(now_xl)
                    continue
                qs = 'up' if fx_type == 'ding' else 'down'
                up_xl = xulie[-1]
                if (up_xl['max'] >= now_xl['max'] and up_xl['min'] <= now_xl['min']) \
                        or (up_xl['max'] <= now_xl['max'] and up_xl['min'] >= now_xl['min']):
                    del (xulie[-1])
                    if qs == 'up':
                        now_xl['max'] = max(up_xl['max'], now_xl['max'])
                        now_xl['min'] = max(up_xl['min'], now_xl['min'])
                    else:
                        now_xl['max'] = min(up_xl['max'], now_xl['max'])
                        now_xl['min'] = min(up_xl['min'], now_xl['min'])
                    xulie.append(now_xl)
                else:
                    xulie.append(now_xl)

        xl_fxs = []
        for i in range(1, len(xulie) - 1):
            up_xl = xulie[i - 1]
            now_xl = xulie[i]
            next_xl = xulie[i + 1]
            if fx_type == 'ding' and up_xl['max'] <= now_xl['max'] and now_xl['max'] >= next_xl['max']:
                now_xl['type'] = 'ding'
                xl_fxs.append(now_xl)
            if fx_type == 'di' and up_xl['min'] >= now_xl['min'] and now_xl['min'] <= next_xl['min']:
                now_xl['type'] = 'di'
                xl_fxs.append(now_xl)

        return xl_fxs

    def __cross_qujian(self, qj_one, qj_two):
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

    def __create_zs(self, bis: List[BI]) -> [ZS, None]:
        """
        创建中枢
        :param bis:
        :return:
        """
        if len(bis) < 3:
            return None
        zs = ZS(start=bis[0].start, bis=[bis[0]])

        zs_fanwei = [bis[0].high, bis[0].low]
        zs_gg = bis[0].high
        zs_dd = bis[0].low
        for i in range(1, len(bis)):
            bi = bis[i]
            bi_fanwei = [bi.high, bi.low]
            cross_fanwei = self.__cross_qujian(zs_fanwei, bi_fanwei)
            if cross_fanwei is None:
                return None
            zs_gg = max(zs_gg, bi.high)
            zs_dd = min(zs_dd, bi.low)
            if i <= 2:
                zs_fanwei = [cross_fanwei['max'], cross_fanwei['min']]
            zs.bis.append(bi)
            # 根据笔数量，计算级别
            zs.bi_num = len(zs.bis)
            zs.level = (zs.bi_num % 3) - 1
            zs.end = bi.end
            # 记录中枢中，最大的笔力度
            if zs.max_ld is None:
                zs.max_ld = bi.ld
            else:
                zs.max_ld = zs.max_ld if self.__compare_ld_beichi(zs.max_ld, bi.ld) else bi.ld

        zs.zg = zs_fanwei[0]
        zs.zd = zs_fanwei[1]
        zs.gg = zs_gg
        zs.dd = zs_dd

        zs_pre_bi = self.bis[bis[0].index - 1]
        zs_next_bi = None if self.bis[-1].index == bis[-1].index else self.bis[bis[-1].index + 1]
        zs_type = 'up' if bis[0].type == 'down' else 'down'

        if zs_next_bi is None:
            zs.type = 'zd'
        elif zs_pre_bi.type == zs_next_bi.type:  # 进去中枢笔同向，中枢才有方向
            if zs_type == 'up' and zs_pre_bi.low < zs.dd and zs_next_bi.high > zs.gg:
                zs.type = 'up'
            elif zs_type == 'down' and zs_pre_bi.high > zs.gg and zs_next_bi.low < zs.dd:
                zs.type = 'down'
            else:
                zs.type = 'zd'
        else:
            zs.type = 'zd'
        return zs

    def __compare_zs_ld(self, zss: List[ZS], bis: List[BI], end_zs: ZS, end_bi_index: int):
        """
        对比中枢前后力度
        :param zss:
        :param bis:
        :param end_zs:
        :param end_bi_index:
        :return:
        """
        if end_bi_index >= len(bis):
            return

        # 对比 趋势/盘整 背驰
        pre_bi = bis[end_zs.bis[0].index - 1]
        end_bi = bis[end_bi_index]
        pre_zs = zss[-1] if len(zss) > 0 else None

        if pre_zs and end_zs.type in ['up', 'down'] \
                and pre_zs.type == end_zs.type \
                and end_zs.bis[0].index - pre_zs.bis[-1].index <= 3:
            # 趋势背驰判断（有前中枢并且同类型的，并且新高新低的，才有趋势背驰）
            if (end_zs.type == 'up' and end_zs.zd > pre_zs.zg) \
                    or (end_zs.type == 'down' and end_zs.zg < pre_zs.zd):
                if end_bi.qs_beichi is False:
                    if self.__zs_call_back_boll(end_zs) and self.__compare_ld_beichi(pre_bi.ld, end_bi.ld):
                        end_bi.qs_beichi = True
        else:
            # 盘整背驰判断
            if end_bi.pz_beichi is False:
                # 中枢内新高或新低，判断力度，出盘整背驰
                if end_bi.high > end_zs.gg or end_bi.low < end_zs.dd:
                    if self.__compare_ld_beichi(pre_bi.ld, end_bi.ld) and self.__compare_ld_beichi(end_zs.max_ld,
                                                                                                   end_bi.ld):
                        end_bi.pz_beichi = True

        return

    def __compare_ld_beichi(self, one_ld: dict, two_ld: dict):
        """
        比较两个力度，后者小于前者，返回 True
        :param one_ld:
        :param two_ld:
        :return:
        """
        if two_ld['macd']['hist']['sum'] < one_ld['macd']['hist']['sum']:
            return True
        else:
            return False

    def __macd_beichi(self, one_qj: List[FX], zs_qj: List[FX], two_qj: List[FX], beichi_type: str = 'qs') -> bool:
        """
        计算 给定的两端的 macd 是否背驰
        :param one_qj:
        :param zs_qj:
        :param two_qj:
        :param beichi_type:
        :return:
        """
        back_zero = False
        if zs_qj is not None:
            back_zero = self.__macd_call_back_zero(zs_qj[0], zs_qj[1])
        one_ld = self.__query_macd_ld(one_qj[0], one_qj[1])
        two_ld = self.__query_macd_ld(two_qj[0], two_qj[1])

        if beichi_type == 'qs':  # 趋势背驰判断
            if back_zero and two_ld['hist']['sum'] < one_ld['hist']['sum']:
                return True

        elif beichi_type == 'pz':  # 盘整背驰判断
            if two_ld['hist']['sum'] < one_ld['hist']['sum']:
                return True
        return False

    def __macd_call_back_zero(self, start_fx: FX, end_fx: FX):
        """
        检查是否有回调 0 轴
        :param start_fx:
        :param end_fx:
        :return:
        """
        # 确认区间是否回抽0轴
        start_index = start_fx.index
        for bi in self.bis:
            if bi.start.index == start_index:
                if (bi.ld['macd']['dea']['min'] <= 3.1415926 and bi.ld['macd']['dea']['max'] >= -3.1415926) \
                        or (bi.ld['macd']['dif']['min'] <= 3.1415926 and bi.ld['macd']['dif']['max'] >= -3.1415926):
                    return True
                start_index = bi.end.index
            if start_index >= end_fx.index:
                break

        return False

    def __zs_call_back_boll(self, zs: ZS):
        """
        中枢是否回拉boll中轨，替换 macd 回拉零轴判断
        :param zs:
        :return:
        """
        zs_qj = [zs.zg, zs.zd]
        zs_k_index = [zs.bis[0].start.k.k_index, zs.bis[-1].end.k.k_index]
        boll_mid = self.idx['boll']['mid'][zs_k_index[0]:zs_k_index[1]]
        boll_qj = [max(boll_mid), min(boll_mid)]
        cross = self.__cross_qujian(zs_qj, boll_qj)
        if cross is not None:
            return True
        return False

    def __query_macd_ld(self, start_fx: FX, end_fx: FX):
        """
        计算指定位置内的 macd 力度
        :param start_fx:
        :param end_fx:
        :return:
        """
        if start_fx.index > end_fx.index:
            raise Exception('计算力度，开始分型不可以大于结束分型')

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
        hist_sum = hist_abs.sum()
        end_dea = dea[-1]
        end_dif = dif[-1]
        end_hist = hist[-1]

        return {
            'dea': {'end': end_dea, 'max': dea.max(), 'min': dea.min()},
            'dif': {'end': end_dif, 'max': dif.max(), 'min': dif.min()},
            'hist': {'sum': hist_sum, 'end': end_hist},
        }

    def __find_bi_zs(self, bi: BI) -> [None, ZS]:
        """
        查询笔所在的中枢，不包括以此笔起始的中枢
        :param bi:
        :return:
        """
        bi_zs = None
        for zs in self.zss:
            if zs.type in ['up', 'down'] and zs.start.index < bi.start.index:
                bi_zs = zs
            else:
                break
        return bi_zs


def batch_cls(code, klines: Dict[str, pd.DataFrame]) -> List[CL]:
    """
    批量计算并获取 缠论 数据
    :param code:
    :param klines:
    :return:
    """
    cls = []
    for f in klines.keys():
        cls.append(CL(code, klines[f], f))
    return cls
