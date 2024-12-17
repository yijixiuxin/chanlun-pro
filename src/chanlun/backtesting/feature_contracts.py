# 期货合约信息
# https://www.jiaoyixingqiu.com/shouxufei
# http://www.hongyuanqh.com/download/20241213/%E4%BF%9D%E8%AF%81%E9%87%91%E6%A0%87%E5%87%8620241213.pdf
# 手续费分为 百分比 和 每手固定金额，小于1的就是百分比设置，大于1的就是固定金额的
feature_contracts = {
    "SHFE.RB": {  # 螺纹钢
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.07,  # 买开 保证金率
        "margin_rate_short": 0.07,  # 卖开 保证金率
        "fee_rate_open": 0.0001,  # 开仓 手续费
        "fee_rate_close": 0.0001,  # 平仓 手续费
        "fee_rate_close_today": 0.0001,  # 平今 手续费
    },
    "CZCE.RM": {  # 菜籽粕
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.07,  # 买开 保证金率
        "margin_rate_short": 0.07,  # 卖开 保证金率
        "fee_rate_open": 1.51,  # 开仓 手续费
        "fee_rate_close": 1.51,  # 平仓 手续费
        "fee_rate_close_today": 1.51,  # 平今 手续费
    },
    "CZCE.MA": {  # 甲醇
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.08,  # 买开 保证金率
        "margin_rate_short": 0.08,  # 卖开 保证金率
        "fee_rate_open": 0.0001,  # 开仓 手续费
        "fee_rate_close": 0.0001,  # 平仓 手续费
        "fee_rate_close_today": 0.0001,  # 平今 手续费
    },
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
    "DCE.V": {  # PVC
        "symbol_size": 5,  # 每手数量
        "margin_rate_long": 0.07,  # 买开 保证金率
        "margin_rate_short": 0.07,  # 卖开 保证金率
        "fee_rate_open": 1.01,  # 开仓 手续费
        "fee_rate_close": 1.01,  # 平仓 手续费
        "fee_rate_close_today": 1.01,  # 平今 手续费
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
}
