"""缠论数据获取模块

提供缠论结构化数据的获取接口
"""

from typing import Dict, List, Optional, Any
import pandas as pd

from chanlun.base import Market
from chanlun.exchange import get_exchange
from chanlun.cl_utils import web_batch_get_cl_datas


def get_cl_data(
    market: str,
    code: str,
    frequency: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    cl_config: Optional[dict] = None,
):
    """
    获取单个标的的缠论数据

    Args:
        market: 市场标识，如 'a', 'hk', 'futures' 等
        code: 标的代码
        frequency: 周期
        start_date: 开始日期
        end_date: 结束日期
        cl_config: 缠论配置，None使用默认配置

    Returns:
        ICL 缠论数据对象

    Example:
        >>> cd = get_cl_data('a', 'SH.600519', 'd', '2024-01-01')
        >>> print(f"笔数量: {len(cd.get_bis())}")
    """
    # 获取行情数据
    ex = get_exchange(_get_market_enum(market))
    klines = ex.klines(code=code, frequency=frequency, start_date=start_date, end_date=end_date)

    if klines is None or len(klines) == 0:
        raise ValueError(f"无法获取 {code} 的K线数据")

    # 计算缠论数据
    cls = web_batch_get_cl_datas(
        market=market,
        code=code,
        klines={frequency: klines},
        cl_config=cl_config,
    )

    return cls[0] if cls else None


def get_cl_structured_data(
    market: str,
    code: str,
    frequency: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    cl_config: Optional[dict] = None,
) -> Dict[str, Any]:
    """
    获取缠论结构化数据（适合AI处理）

    Args:
        market: 市场标识
        code: 标的代码
        frequency: 周期
        start_date: 开始日期
        end_date: 结束日期
        cl_config: 缠论配置

    Returns:
        Dict 包含结构化的缠论数据：
        {
            "code": str,
            "frequency": str,
            "klines_count": int,
            "latest_price": float,
            "latest_date": str,
            "bis": List[笔信息],
            "xds": List[线段信息],
            "zss": List[中枢信息],
            "mmds": List[买卖点信息],
            "bcs": List[背驰信息],
        }

    Example:
        >>> data = get_cl_structured_data('a', 'SH.600519', 'd')
        >>> print(f"最新价格: {data['latest_price']}")
        >>> for bi in data['bis'][-3:]:
        ...     print(f"笔: {bi['start_date']} -> {bi['end_date']} {bi['type']}")
    """
    cd = get_cl_data(market, code, frequency, start_date, end_date, cl_config)

    if cd is None:
        return {}

    klines = cd.get_src_klines()
    latest_k = klines[-1] if klines else None

    def _format_k(k):
        """格式化K线数据"""
        if k is None:
            return None
        return {
            'date': str(k.date) if hasattr(k, 'date') else None,
            'open': k.o,
            'high': k.h,
            'low': k.l,
            'close': k.c,
            'volume': k.v,
        }

    def _format_bi(bi):
        """格式化笔数据"""
        return {
            'start_date': str(bi.start.k.date) if bi.start and bi.start.k else None,
            'end_date': str(bi.end.k.date) if bi.end and bi.end.k else None,
            'start_val': bi.start.val if bi.start else None,
            'end_val': bi.end.val if bi.end else None,
            'type': bi.type,
            'is_done': bi.is_done(),
            'high': bi.high,
            'low': bi.low,
            'mmds': [m.type for m in bi.mmds] if hasattr(bi, 'mmds') else [],
            'bcs': [b.type for b in bi.bcs] if hasattr(bi, 'bcs') else [],
        }

    def _format_xd(xd):
        """格式化线段数据"""
        return {
            'start_date': str(xd.start.k.date) if xd.start and xd.start.k else None,
            'end_date': str(xd.end.k.date) if xd.end and xd.end.k else None,
            'start_val': xd.start.val if xd.start else None,
            'end_val': xd.end.val if xd.end else None,
            'type': xd.type,
            'is_done': xd.is_done(),
            'high': xd.high,
            'low': xd.low,
            'mmds': [m.type for m in xd.mmds] if hasattr(xd, 'mmds') else [],
            'bcs': [b.type for b in xd.bcs] if hasattr(xd, 'bcs') else [],
        }

    def _format_zs(zs):
        """格式化中枢数据"""
        return {
            'start_date': str(zs.start.k.date) if zs.start and zs.start.k else None,
            'end_date': str(zs.end.k.date) if zs.end and zs.end.k else None,
            'type': zs.type,
            'zs_type': getattr(zs, 'zs_type', 'standard'),  # 中枢类型
            'gg': zs.gg,  # 高高点
            'zg': zs.zg,  # 中枢高点
            'zd': zs.zd,  # 中枢低点
            'dd': zs.dd,  # 低低点
            'level': getattr(zs, 'level', 1),  # 中枢级别
            'line_num': getattr(zs, 'line_num', 0),  # 构成中枢的线段数量
        }

    def _format_mmd(mmd):
        """格式化买卖点数据"""
        return {
            'type': mmd.type,
            'name': getattr(mmd, 'name', mmd.type),
            'price': getattr(mmd, 'price', None),
            'index': getattr(mmd, 'index', None),
        }

    def _format_bc(bc):
        """格式化背驰数据"""
        return {
            'type': bc.type,
            'name': getattr(bc, 'name', bc.type),
            'bis': [_format_bi(b) for b in bc.bis] if hasattr(bc, 'bis') else [],
        }

    return {
        'code': cd.get_code(),
        'frequency': cd.get_frequency(),
        'klines_count': len(klines),
        'latest_price': latest_k.c if latest_k else None,
        'latest_date': str(latest_k.date) if latest_k else None,
        'bis': [_format_bi(bi) for bi in cd.get_bis()],
        'xds': [_format_xd(xd) for xd in cd.get_xds()],
        'zss': [_format_zs(zs) for zs in cd.get_zss()],
        'mmds': [_format_mmd(mmd) for mmd in cd.get_mmds()],
        'bcs': [_format_bc(bc) for bc in cd.get_bcs()],
    }


def batch_get_cl_data(
    market: str,
    codes: List[str],
    frequency: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    cl_config: Optional[dict] = None,
) -> List[Dict[str, Any]]:
    """
    批量获取多个标的的缠论结构化数据

    Args:
        market: 市场标识
        codes: 标的代码列表
        frequency: 周期
        start_date: 开始日期
        end_date: 结束日期
        cl_config: 缠论配置

    Returns:
        List[Dict]，每个元素对应一个标的的结构化缠论数据

    Example:
        >>> results = batch_get_cl_data('a', ['SH.600519', 'SH.601398'], 'd')
        >>> for r in results:
        ...     print(f"{r['code']}: {r['klines_count']} K线, {len(r['bis'])} 笔")
    """
    results = []
    for code in codes:
        try:
            data = get_cl_structured_data(market, code, frequency, start_date, end_date, cl_config)
            results.append(data)
        except Exception as e:
            print(f"获取 {code} 缠论数据失败: {e}")
            results.append({'code': code, 'error': str(e)})
    return results


def _get_market_enum(market: str) -> Market:
    """将市场字符串转换为Market枚举"""
    market_map = {
        'a': Market.A,
        'hk': Market.HK,
        'futures': Market.FUTURES,
        'ny_futures': Market.NY_FUTURES,
        'currency': Market.CURRENCY,
        'currency_spot': Market.CURRENCY_SPOT,
        'us': Market.US,
        'fx': Market.FX,
    }
    if market not in market_map:
        raise ValueError(f"不支持的市场: {market}")
    return market_map[market]
