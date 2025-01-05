# 期货合约信息
# https://www.jiaoyixingqiu.com/shouxufei
# http://www.hongyuanqh.com/download/20241213/%E4%BF%9D%E8%AF%81%E9%87%91%E6%A0%87%E5%87%8620241213.pdf
# 手续费分为 百分比 和 每手固定金额，小于1的就是百分比设置，大于1的就是固定金额的
# 可根据自己开户期货券商的标准进行调整
futures_contracts = {
    # 上海期货交易所手续费一览表
    "SHFE.RB": {  # 螺纹钢
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.11,  # 买开 保证金率
        "margin_rate_short": 0.11,  # 卖开 保证金率
        "fee_rate_open": 0.00010107,  # 开仓 手续费
        "fee_rate_close": 0.00010107,  # 平仓 手续费
        "fee_rate_close_today": 0.00010107,  # 平今 手续费
    },
    "SHFE.FU": {  # 燃油
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.16,  # 买开 保证金率
        "margin_rate_short": 0.16,  # 卖开 保证金率
        "fee_rate_open": 0.00005107,  # 开仓 手续费
        "fee_rate_close": 0.00005107,  # 平仓 手续费
        "fee_rate_close_today": 0.00000007,  # 平今 手续费
    },
    "SHFE.RU": {  # 橡胶
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.13,  # 买开 保证金率
        "margin_rate_short": 0.13,  # 卖开 保证金率
        "fee_rate_open": 3.01,  # 开仓 手续费
        "fee_rate_close": 3.01,  # 平仓 手续费
        "fee_rate_close_today": 18.01,  # 平今 手续费
    },
    "SHFE.HC": {  # 热卷
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.11,  # 买开 保证金率
        "margin_rate_short": 0.11,  # 卖开 保证金率
        "fee_rate_open": 0.00010107,  # 开仓 手续费
        "fee_rate_close": 0.00010107,  # 平仓 手续费
        "fee_rate_close_today": 0.00010107,  # 平今 手续费
    },
    "SHFE.AU": {  # 黄金
        "symbol_size": 20,  # 每手数量
        "margin_rate_long": 0.17,  # 买开 保证金率
        "margin_rate_short": 0.17,  # 卖开 保证金率
        "fee_rate_open": 10.01,  # 开仓 手续费
        "fee_rate_close": 10.01,  # 平仓 手续费
        "fee_rate_close_today": 0,  # 平今 手续费
    },
    "SHFE.AG": {  # 白银
        "symbol_size": 15,  # 每手数量
        "margin_rate_long": 0.18,  # 买开 保证金率
        "margin_rate_short": 0.18,  # 卖开 保证金率
        "fee_rate_open": 0.00005107,  # 开仓 手续费
        "fee_rate_close": 0.00005107,  # 平仓 手续费
        "fee_rate_close_today": 0.00005107,  # 平今 手续费
    },
    "SHFE.BU": {  # 沥青
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.16,  # 买开 保证金率
        "margin_rate_short": 0.16,  # 卖开 保证金率
        "fee_rate_open": 0.00005107,  # 开仓 手续费
        "fee_rate_close": 0.00005107,  # 平仓 手续费
        "fee_rate_close_today": 0,  # 平今 手续费
    },
    # 郑州商品交易所手续费一览表
    "CZCE.RM": {  # 菜籽粕
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.12,  # 买开 保证金率
        "margin_rate_short": 0.12,  # 卖开 保证金率
        "fee_rate_open": 5.01,  # 开仓 手续费
        "fee_rate_close": 5.01,  # 平仓 手续费
        "fee_rate_close_today": 5.01,  # 平今 手续费
    },
    "CZCE.FG": {  # 玻璃
        "symbol_size": 20,  # 每手数量
        "margin_rate_long": 0.18,  # 买开 保证金率
        "margin_rate_short": 0.18,  # 卖开 保证金率
        "fee_rate_open": 10.01,  # 开仓 手续费
        "fee_rate_close": 10.01,  # 平仓 手续费
        "fee_rate_close_today": 10.01,  # 平今 手续费
    },
    "CZCE.OI": {  # 菜籽油
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.11,  # 买开 保证金率
        "margin_rate_short": 0.11,  # 卖开 保证金率
        "fee_rate_open": 5.01,  # 开仓 手续费
        "fee_rate_close": 5.01,  # 平仓 手续费
        "fee_rate_close_today": 5.01,  # 平今 手续费
    },
    "CZCE.MA": {  # 甲醇
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.13,  # 买开 保证金率
        "margin_rate_short": 0.13,  # 卖开 保证金率
        "fee_rate_open": 0.00010107,  # 开仓 手续费
        "fee_rate_close": 0.00010107,  # 平仓 手续费
        "fee_rate_close_today": 0.00010107,  # 平今 手续费
    },
    "CZCE.SA": {  # 纯碱
        "symbol_size": 20,  # 每手数量
        "margin_rate_long": 0.18,  # 买开 保证金率
        "margin_rate_short": 0.18,  # 卖开 保证金率
        "fee_rate_open": 0.00040107,  # 开仓 手续费
        "fee_rate_close": 0.00040107,  # 平仓 手续费
        "fee_rate_close_today": 0.00040107,  # 平今 手续费
    },
    "CZCE.TA": {  # PTA
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.12,  # 买开 保证金率
        "margin_rate_short": 0.12,  # 卖开 保证金率
        "fee_rate_open": 3.01,  # 开仓 手续费
        "fee_rate_close": 3.01,  # 平仓 手续费
        "fee_rate_close_today": 0,  # 平今 手续费
    },
    # 大连商品交易所手续费一览表
    "DCE.M": {  # 豆粕
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.12,  # 买开 保证金率
        "margin_rate_short": 0.12,  # 卖开 保证金率
        "fee_rate_open": 1.51,  # 开仓 手续费
        "fee_rate_close": 1.51,  # 平仓 手续费
        "fee_rate_close_today": 1.51,  # 平今 手续费
    },
    "DCE.P": {  # 棕榈油
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.14,  # 买开 保证金率
        "margin_rate_short": 0.14,  # 卖开 保证金率
        "fee_rate_open": 2.51,  # 开仓 手续费
        "fee_rate_close": 2.51,  # 平仓 手续费
        "fee_rate_close_today": 2.51,  # 平今 手续费
    },
    "DCE.Y": {  # 豆油
        "symbol_size": 20,  # 每手数量
        "margin_rate_long": 0.11,  # 买开 保证金率
        "margin_rate_short": 0.11,  # 卖开 保证金率
        "fee_rate_open": 2.51,  # 开仓 手续费
        "fee_rate_close": 2.51,  # 平仓 手续费
        "fee_rate_close_today": 2.51,  # 平今 手续费
    },
    "DCE.V": {  # PVC
        "symbol_size": 5,  # 每手数量
        "margin_rate_long": 0.12,  # 买开 保证金率
        "margin_rate_short": 0.12,  # 卖开 保证金率
        "fee_rate_open": 1.01,  # 开仓 手续费
        "fee_rate_close": 1.01,  # 平仓 手续费
        "fee_rate_close_today": 1.01,  # 平今 手续费
    },
    "DCE.C": {  # 玉米
        "symbol_size": 10,  # 每手数量
        "margin_rate_long": 0.11,  # 买开 保证金率
        "margin_rate_short": 0.11,  # 卖开 保证金率
        "fee_rate_open": 1.21,  # 开仓 手续费
        "fee_rate_close": 1.21,  # 平仓 手续费
        "fee_rate_close_today": 1.21,  # 平今 手续费
    },
    "DCE.L": {  # 塑料
        "symbol_size": 5,  # 每手数量
        "margin_rate_long": 0.12,  # 买开 保证金率
        "margin_rate_short": 0.12,  # 卖开 保证金率
        "fee_rate_open": 1.01,  # 开仓 手续费
        "fee_rate_close": 1.01,  # 平仓 手续费
        "fee_rate_close_today": 1.01,  # 平今 手续费
    },
    "DCE.I": {  # 铁矿石
        "symbol_size": 50,  # 每手数量
        "margin_rate_long": 0.16,  # 买开 保证金率
        "margin_rate_short": 0.16,  # 卖开 保证金率
        "fee_rate_open": 0.00010107,  # 开仓 手续费
        "fee_rate_close": 0.00010107,  # 平仓 手续费
        "fee_rate_close_today": 0.00010107,  # 平今 手续费
    },
}


if __name__ == "__main__":
    print(futures_contracts.keys())
