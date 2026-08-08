import pandas as pd


def klines_to_heikin_ashi_klines(ks: pd.DataFrame) -> pd.DataFrame:
    """
    将缠论数据的普通K线，转换成平均K线数据，返回格式 pd.DataFrame
    """
    # s_time = time.time()
    cd_klines = ks.to_dict(orient="records")
    # print(f"转换成列表数据格式耗时: {time.time() - s_time:.2f}s")

    # s_time = time.time()
    mean_klines: list = []
    for i in range(len(cd_klines)):
        if i == 0:
            mean_klines.append(cd_klines[i])
            continue
        mk = mean_klines[i - 1]
        nk = cd_klines[i]
        # 开盘价 =（前一根烛台的开盘价+ 前一根烛台的收盘价）/2
        # 收盘价 =（当前烛台的开盘价 + 最高价 + 最低价 + 收盘价）/4
        # 最大值（或最高价）= 当前周期的最高价、当前周期的平均 K 线图开盘价或收盘价中的最大值。
        # 最小值（或最低价）= 当前周期的最低价、当前周期的平均 K 线图开盘价或收盘价中的最小值
        _open = (mk["open"] + mk["close"]) / 2
        _close = (nk["open"] + nk["high"] + nk["low"] + nk["close"]) / 4
        _high = max(nk["high"], _open, _close)
        _low = min(nk["low"], _open, _close)
        _volume = nk["volume"]
        mean_klines.append(
            {
                "code": nk["code"],
                "date": nk["date"],
                "high": _high,
                "open": _open,
                "low": _low,
                "close": _close,
                "volume": _volume,
            }
        )
    # print(f"转换成平均K线数据格式耗时: {time.time() - s_time:.2f}s")

    # s_time = time.time()
    df = pd.DataFrame(mean_klines)
    # print(f"转换成 pd.DataFrame 数据格式耗时: {time.time() - s_time:.2f}s")

    return df
