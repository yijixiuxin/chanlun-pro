import pandas as pd


def klines_to_heikin_ashi_klines(ks: pd.DataFrame, smooth_num: int = 1) -> pd.DataFrame:
    """
    将缠论数据的普通K线，转换成平均K线数据，返回格式 pd.DataFrame

    :param ks: 原始K线数据
    :param smooth_num: 平滑次数，取值范围 [1, 5]，1 表示标准 Heikin-Ashi
    :return: 平均K线 DataFrame
    """
    # 参数校验
    if not isinstance(smooth_num, int) or not (1 <= smooth_num <= 5):
        raise ValueError(f"smooth_num 必须是 1~5 之间的整数，当前值: {smooth_num}")

    # 单次 Heikin-Ashi 计算
    def _single_pass(src_df: pd.DataFrame) -> pd.DataFrame:
        cd_klines = src_df.to_dict(orient="records")
        mean_klines: list = []
        for i in range(len(cd_klines)):
            if i == 0:
                mean_klines.append(cd_klines[i])
                continue
            mk = mean_klines[i - 1]
            nk = cd_klines[i]
            # 开盘价 =（前一根烛台的开盘价 + 前一根烛台的收盘价）/2
            # 收盘价 =（当前烛台的开盘价 + 最高价 + 最低价 + 收盘价）/4
            # 最高价 = 当前周期的最高价、当前平均K线的开盘价或收盘价中的最大值
            # 最低价 = 当前周期的最低价、当前平均K线的开盘价或收盘价中的最小值
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
        return pd.DataFrame(mean_klines)

    # 迭代 smooth_num 次
    result_df = ks
    for _ in range(smooth_num):
        result_df = _single_pass(result_df)

    return result_df
