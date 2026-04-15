"""缠论数据获取模块

提供缠论结构化数据的获取接口
"""

from typing import Any, Dict, List

from chanlun.base import Market
from chanlun.cl_utils import query_cl_chart_config, web_batch_get_cl_datas
from chanlun.exchange import get_exchange


def get_cl_data(
    market: str,
    code: str,
    frequency: str,
):
    """
    获取单个标的的缠论数据

    Args:
        market: 市场标识，如 'a', 'hk', 'futures' 等
        code: 标的代码
        frequency: 周期

    Returns:
        ICL 缠论数据对象

    Example:
        >>> cd = get_cl_data('a', 'SH.600519', 'd')
        >>> print(f"笔数量: {len(cd.get_bis())}")
    """
    # 获取行情数据
    ex = get_exchange(_get_market_enum(market))
    klines = ex.klines(code=code, frequency=frequency)

    if klines is None or len(klines) == 0:
        raise ValueError(f"无法获取 {code} 的K线数据")

    # 计算缠论数据
    cl_config = query_cl_chart_config(market, code)
    cls = web_batch_get_cl_datas(
        market=market,
        code=code,
        klines={frequency: klines},
        cl_config=cl_config,
    )

    return cls[0] if cls else None


def get_cl_structured_data(market: str, code: str, frequency: str) -> Dict[str, Any]:
    """
    获取缠论结构化数据（适合AI处理）

    Args:
        market: 市场标识
        code: 标的代码
        frequency: 周期

    Returns:
        Dict 包含结构化的缠论数据：
        {
            "code": str,
            "frequency": str,
            "klines_count": int,
            "latest_price": float,
            "latest_date": str,
            "stock_info": Dict 股票基本信息,
            "bis": List[笔信息],
            "xds": List[线段信息],
            "zss": Dict 不同类型中枢信息,
            "mmds": List[买卖点信息],
            "bcs": List[背驰信息],
            "zs_relationships": List 中枢位置关系,
        }

    Example:
        >>> data = get_cl_structured_data('a', 'SH.600519', 'd')
        >>> print(f"最新价格: {data['latest_price']}")
        >>> for bi in data['bis'][-3:]:
        ...     print(f"笔: {bi['start_date']} -> {bi['end_date']} {bi['type']}")
    """
    cd = get_cl_data(market, code, frequency)

    if cd is None:
        return {}

    # 获取股票信息
    ex = get_exchange(_get_market_enum(market))
    stock_info = ex.stock_info(code) or {}

    klines = cd.get_src_klines()
    latest_k = klines[-1] if klines else None

    def _format_bi(bi):
        """格式化笔数据"""
        return {
            "start_date": str(bi.start.k.date) if bi.start and bi.start.k else None,
            "end_date": str(bi.end.k.date) if bi.end and bi.end.k else None,
            "start_val": bi.start.val if bi.start else None,
            "end_val": bi.end.val if bi.end else None,
            "type": bi.type,
            "is_done": bi.is_done(),
            "high": bi.high,
            "low": bi.low,
            "mmds": [m for m in bi.get_mmds("|")],
            "bcs": [b for b in bi.get_bcs("|")],
            "length": abs(bi.end.val - bi.start.val) if bi.start and bi.end else None,
        }

    def _format_xd(xd):
        """格式化线段数据"""
        return {
            "start_date": str(xd.start.k.date) if xd.start and xd.start.k else None,
            "end_date": str(xd.end.k.date) if xd.end and xd.end.k else None,
            "start_val": xd.start.val if xd.start else None,
            "end_val": xd.end.val if xd.end else None,
            "type": xd.type,
            "is_done": xd.is_done(),
            "high": xd.high,
            "low": xd.low,
            "mmds": [m for m in xd.get_mmds("|")],
            "bcs": [b for b in xd.get_bcs("|")],
            "length": abs(xd.end.val - xd.start.val) if xd.start and xd.end else None,
        }

    def _format_zs(zs):
        """格式化中枢数据"""
        return {
            "start_date": str(zs.start.k.date) if zs.start and zs.start.k else None,
            "end_date": str(zs.end.k.date) if zs.end and zs.end.k else None,
            "type": zs.type,
            "zs_type": getattr(zs, "zs_type", "standard"),  # 中枢类型
            "gg": zs.gg,  # 高高点
            "zg": zs.zg,  # 中枢高点
            "zd": zs.zd,  # 中枢低点
            "dd": zs.dd,  # 低低点
            "level": getattr(zs, "level", 1),  # 中枢级别
            "line_num": getattr(zs, "line_num", 0),  # 构成中枢的线段数量
            "range": zs.zg - zs.zd
            if hasattr(zs, "zg") and hasattr(zs, "zd")
            else None,  # 中枢范围
        }

    # 获取不同类型的中枢数据
    zss_by_type = {}
    zs_relationships = []
    for zs_type in cd.get_config().get("zs_bi_type", ["zs_type_bz"]):
        zss = cd.get_bi_zss(zs_type)
        if zss:
            zss_by_type[zs_type] = [_format_zs(zs) for zs in zss[-3:]]
            # 分析中枢位置关系
            if len(zss) >= 2:
                zs_direction = cd.zss_is_qs(zss[-2], zss[-1])
                zs_relationships.append(
                    {
                        "zs_type": zs_type,
                        "direction": zs_direction,
                        "current_zs": _format_zs(zss[-1]),
                        "previous_zs": _format_zs(zss[-2]),
                    }
                )

    # 获取所有笔的买卖点和背驰
    all_mmds = []
    all_bcs = []
    for bi in cd.get_bis()[-9:]:
        all_mmds.extend([m for m in bi.get_mmds("|")])
        all_bcs.extend([b for b in bi.get_bcs("|")])

    return {
        "code": cd.get_code(),
        "frequency": cd.get_frequency(),
        "klines_count": len(klines),
        "latest_price": latest_k.c if latest_k else None,
        "latest_date": str(latest_k.date) if latest_k else None,
        "stock_info": stock_info,
        "bis": [_format_bi(bi) for bi in cd.get_bis()[-9:]],
        "xds": [_format_xd(xd) for xd in cd.get_xds()[-3:]],
        "zss": zss_by_type,
        "mmds": list(set(all_mmds)),
        "bcs": list(set(all_bcs)),
        "zs_relationships": zs_relationships,
    }


def batch_get_cl_data(
    market: str, codes: List[str], frequency: str
) -> List[Dict[str, Any]]:
    """
    批量获取多个标的的缠论结构化数据

    Args:
        market: 市场标识
        codes: 标的代码列表
        frequency: 周期

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
            data = get_cl_structured_data(market, code, frequency)
            results.append(data)
        except Exception as e:
            print(f"获取 {code} 缠论数据失败: {e}")
            results.append({"code": code, "error": str(e)})
    return results


def _get_market_enum(market: str) -> Market:
    """将市场字符串转换为Market枚举"""
    market_map = {
        "a": Market.A,
        "hk": Market.HK,
        "futures": Market.FUTURES,
        "ny_futures": Market.NY_FUTURES,
        "currency": Market.CURRENCY,
        "currency_spot": Market.CURRENCY_SPOT,
        "us": Market.US,
        "fx": Market.FX,
    }
    if market not in market_map:
        raise ValueError(f"不支持的市场: {market}")
    return market_map[market]


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="缠论数据获取工具")
    parser.add_argument("--fun", type=str, required=True, help="要调用的方法名")
    parser.add_argument("--market", type=str, help="市场标识")
    parser.add_argument("--code", type=str, help="标的代码")
    parser.add_argument("--codes", type=str, help="标的代码列表，逗号分隔")
    parser.add_argument("--frequency", type=str, help="周期")

    args = parser.parse_args()

    # 构建参数字典
    kwargs = {}
    if args.market:
        kwargs["market"] = args.market
    if args.code:
        kwargs["code"] = args.code
    if args.codes:
        kwargs["codes"] = args.codes.split(",")
    if args.frequency:
        kwargs["frequency"] = args.frequency

    # 调用指定方法
    fun_map = {
        "get_cl_data": get_cl_data,
        "get_cl_structured_data": get_cl_structured_data,
        "batch_get_cl_data": batch_get_cl_data,
    }

    if args.fun not in fun_map:
        print(f"不支持的方法: {args.fun}")
        exit(1)

    try:
        result = fun_map[args.fun](**kwargs)
        # 对于 get_cl_data 返回的对象，需要特殊处理
        if args.fun == "get_cl_data" and result:
            # 转换为结构化数据以便输出
            structured_result = get_cl_structured_data(
                args.market, args.code, args.frequency
            )
            print(json.dumps(structured_result, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"执行出错: {e}")
        import traceback

        traceback.print_exc()
