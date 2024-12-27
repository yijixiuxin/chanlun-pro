# 期货合约信息
# https://www.jiaoyixingqiu.com/shouxufei
# http://www.hongyuanqh.com/download/20241213/%E4%BF%9D%E8%AF%81%E9%87%91%E6%A0%87%E5%87%8620241213.pdf
# 手续费分为 百分比 和 每手固定金额，小于1的就是百分比设置，大于1的就是固定金额的
feature_contracts = {
    # 上海期货交易所手续费一览表
    "SHFE.RB": {  # 螺纹钢
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.07,  # 买开 保证金率
        "margin_rate_short": 0.07,  # 卖开 保证金率
        "fee_rate_open": 0.0001,  # 开仓 手续费
        "fee_rate_close": 0.0001,  # 平仓 手续费
        "fee_rate_close_today": 0.0001,  # 平今 手续费
    },
    "SHFE.FU": {  # 燃油
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.09,  # 买开 保证金率
        "margin_rate_short": 0.09,  # 卖开 保证金率
        "fee_rate_open": 0.00001,  # 开仓 手续费
        "fee_rate_close": 0.00001,  # 平仓 手续费
        "fee_rate_close_today": 0,  # 平今 手续费
    },
    "SHFE.RU": {  # 橡胶
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.08,  # 买开 保证金率
        "margin_rate_short": 0.08,  # 卖开 保证金率
        "fee_rate_open": 3.01,  # 开仓 手续费
        "fee_rate_close": 3.01,  # 平仓 手续费
        "fee_rate_close_today": 0,  # 平今 手续费
    },
    "SHFE.HC": {  # 热卷
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.07,  # 买开 保证金率
        "margin_rate_short": 0.07,  # 卖开 保证金率
        "fee_rate_open": 3.01,  # 开仓 手续费
        "fee_rate_close": 3.01,  # 平仓 手续费
        "fee_rate_close_today": 0,  # 平今 手续费
    },
    "SHFE.AU": {  # 黄金
        "symbol_size": 20,  # 每手数量
        "margin_rate_long": 0.12,  # 买开 保证金率
        "margin_rate_short": 0.12,  # 卖开 保证金率
        "fee_rate_open": 10.01,  # 开仓 手续费
        "fee_rate_close": 10.01,  # 平仓 手续费
        "fee_rate_close_today": 0,  # 平今 手续费
    },
    "SHFE.AG": {  # 白银
        "symbol_size": 15,  # 每手数量
        "margin_rate_long": 0.12,  # 买开 保证金率
        "margin_rate_short": 0.12,  # 卖开 保证金率
        "fee_rate_open": 0.000005,  # 开仓 手续费
        "fee_rate_close": 0.000005,  # 平仓 手续费
        "fee_rate_close_today": 0.000005,  # 平今 手续费
    },
    "SHFE.BU": {  # 沥青
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.09,  # 买开 保证金率
        "margin_rate_short": 0.09,  # 卖开 保证金率
        "fee_rate_open": 0.000005,  # 开仓 手续费
        "fee_rate_close": 0.000005,  # 平仓 手续费
        "fee_rate_close_today": 0,  # 平今 手续费
    },
    # 郑州商品交易所手续费一览表
    "CZCE.RM": {  # 菜籽粕
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.07,  # 买开 保证金率
        "margin_rate_short": 0.07,  # 卖开 保证金率
        "fee_rate_open": 0.00001,  # 开仓 手续费
        "fee_rate_close": 0.00001,  # 平仓 手续费
        "fee_rate_close_today": 0.00001,  # 平今 手续费
    },
    "CZCE.FG": {  # 玻璃
        "symbol_size": 20,  # 每手数量
        "margin_rate_long": 0.12,  # 买开 保证金率
        "margin_rate_short": 0.12,  # 卖开 保证金率
        "fee_rate_open": 6.01,  # 开仓 手续费
        "fee_rate_close": 6.01,  # 平仓 手续费
        "fee_rate_close_today": 6.01,  # 平今 手续费
    },
    "CZCE.OI": {  # 菜籽油
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.07,  # 买开 保证金率
        "margin_rate_short": 0.07,  # 卖开 保证金率
        "fee_rate_open": 2,  # 开仓 手续费
        "fee_rate_close": 2,  # 平仓 手续费
        "fee_rate_close_today": 2,  # 平今 手续费
    },
    "CZCE.MA": {  # 甲醇
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.08,  # 买开 保证金率
        "margin_rate_short": 0.08,  # 卖开 保证金率
        "fee_rate_open": 0.0001,  # 开仓 手续费
        "fee_rate_close": 0.0001,  # 平仓 手续费
        "fee_rate_close_today": 0.0001,  # 平今 手续费
    },
    "CZCE.SA": {  # 纯碱
        "symbol_size": 20,  # 每手数量
        "margin_rate_long": 0.12,  # 买开 保证金率
        "margin_rate_short": 0.12,  # 卖开 保证金率
        "fee_rate_open": 0.0002,  # 开仓 手续费
        "fee_rate_close": 0.0002,  # 平仓 手续费
        "fee_rate_close_today": 0.0002,  # 平今 手续费
    },
    "CZCE.TA": {  # PTA
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.07,  # 买开 保证金率
        "margin_rate_short": 0.07,  # 卖开 保证金率
        "fee_rate_open": 3.01,  # 开仓 手续费
        "fee_rate_close": 3.01,  # 平仓 手续费
        "fee_rate_close_today": 0,  # 平今 手续费
    },
    # 大连商品交易所手续费一览表
    "DCE.M": {  # 豆粕
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.07,  # 买开 保证金率
        "margin_rate_short": 0.07,  # 卖开 保证金率
        "fee_rate_open": 1.51,  # 开仓 手续费
        "fee_rate_close": 1.51,  # 平仓 手续费
        "fee_rate_close_today": 1.51,  # 平今 手续费
    },
    "DCE.P": {  # 棕榈油
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.08,  # 买开 保证金率
        "margin_rate_short": 0.08,  # 卖开 保证金率
        "fee_rate_open": 2.5,  # 开仓 手续费
        "fee_rate_close": 2.5,  # 平仓 手续费
        "fee_rate_close_today": 2.5,  # 平今 手续费
    },
    "DCE.Y": {  # 豆油
        "symbol_size": 20,  # 每手数量
        "margin_rate_long": 0.07,  # 买开 保证金率
        "margin_rate_short": 0.07,  # 卖开 保证金率
        "fee_rate_open": 2.5,  # 开仓 手续费
        "fee_rate_close": 2.5,  # 平仓 手续费
        "fee_rate_close_today": 2.5,  # 平今 手续费
    },
    "DCE.V": {  # PVC
        "symbol_size": 5,  # 每手数量
        "margin_rate_long": 0.07,  # 买开 保证金率
        "margin_rate_short": 0.07,  # 卖开 保证金率
        "fee_rate_open": 1.01,  # 开仓 手续费
        "fee_rate_close": 1.01,  # 平仓 手续费
        "fee_rate_close_today": 1.01,  # 平今 手续费
    },
    "DCE.C": {  # 玉米
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.07,  # 买开 保证金率
        "margin_rate_short": 0.07,  # 卖开 保证金率
        "fee_rate_open": 1.21,  # 开仓 手续费
        "fee_rate_close": 1.21,  # 平仓 手续费
        "fee_rate_close_today": 1.21,  # 平今 手续费
    },
    "DCE.I": {  # 塑料
        "symbol_size": 5,  # 每手数量
        "margin_rate_long": 0.07,  # 买开 保证金率
        "margin_rate_short": 0.07,  # 卖开 保证金率
        "fee_rate_open": 1.01,  # 开仓 手续费
        "fee_rate_close": 1.01,  # 平仓 手续费
        "fee_rate_close_today": 1.01,  # 平今 手续费
    },
}


if __name__ == "__main__":
    print(feature_contracts.keys())
