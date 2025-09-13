# -*- coding: utf-8 -*-
"""
技术指标计算模块
负责根据输入的K线数据计算如MACD等技术指标。
"""
from typing import List, Dict
import numpy as np
from talib import abstract

from chanlun.core.cl_interface import CLKline


def calculate_indicators(cl_klines: List[CLKline]) -> Dict:
    """
    计算技术指标

    Args:
        cl_klines: 缠论K线列表

    Returns:
        包含指标数据的字典
    """
    if len(cl_klines) < 26:
        return {'macd': {'dif': [], 'dea': [], 'hist': []}}

    # 提取价格数据
    close_prices = np.array([k.c for k in cl_klines])

    # 计算MACD
    macd_result = abstract.MACD(close_prices)

    return {
        'macd': {
            'dif': macd_result[0].tolist(),
            'dea': macd_result[1].tolist(),
            'hist': macd_result[2].tolist()
        }
    }
