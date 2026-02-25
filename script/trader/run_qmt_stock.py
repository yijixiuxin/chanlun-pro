# -*- coding: utf-8 -*-
import time
import traceback
from chanlun import fun
from chanlun.cl_interface import Config
from chanlun.trader.online_market_datas import OnlineMarketDatas
from chanlun.strategy.strategy_custom_level_mla import StrategyCustomLevelMLA

# 导入 QMT 交易所和交易类
from chanlun.exchange.exchange_qmt import ExchangeQMTStock
from chanlun.trader.trader_qmt_stock import QMTTraderStock

"""
QMT 实盘/模拟交易运行脚本 - 股票(Stock)
"""

logger = fun.get_logger("trader_qmt_stock.log")

# ================= 配置区域 =================

# 1. 定义监控的代码列表
# 股票示例: 'SH.600519', 'SZ.000001'
run_codes = ['SH.600519', 'SZ.000001']
market_type = 'a'

# 2. 运行周期
frequencys = ['5m', '30m']

# 3. 缠论配置
cl_config = {
    "fx_qj": Config.FX_QJ_K.value,
    "fx_bh": Config.FX_BH_YES.value,
    "bi_type": Config.BI_TYPE_NEW.value,
    "bi_bzh": Config.BI_BZH_YES.value,
    "bi_fx_cgd": Config.BI_FX_CHD_NO.value,
    "bi_qj": Config.BI_QJ_DD.value,
    "xd_qj": Config.XD_QJ_DD.value,
    "zsd_bzh": Config.ZSD_BZH_NO.value,
    "zsd_qj": Config.ZSD_QJ_DD.value,
    "zs_bi_type": Config.ZS_TYPE_DN.value,
    "zs_xd_type": Config.ZS_TYPE_DN.value,
    "zs_qj": Config.ZS_QJ_CK.value,
    "zs_wzgx": Config.ZS_WZGX_ZGD.value,
}

# 4. 策略对象 (请修改为您自己的策略类)
STR = StrategyCustomLevelMLA()

# ===========================================

def run_trader():
    logger.info("启动 QMT 股票交易程序")
    logger.info(f"监控代码: {run_codes}")

    # 初始化交易所 (QMT)
    ex = ExchangeQMTStock()

    # 初始化交易对象
    TR = QMTTraderStock(name="QMT_Stock_Trader", log=logger.info)

    # 初始化行情数据对象
    Data = OnlineMarketDatas(market_type, frequencys, ex, cl_config)

    # 加载历史数据 (如果有保存)
    TR.load_from_pkl("trader_qmt_stock")

    # 设置策略和数据
    TR.set_strategy(STR)
    TR.set_data(Data)

    while True:
        try:
            # 每 5 秒检查一次，避免过于频繁
            time.sleep(5)
            
            # 检查是否在交易时间
            # if ex.now_trading() is False:
            #     continue

            # 增加当前持仓中的代码到监控列表
            current_codes = TR.position_codes()
            monitor_codes = list(set(run_codes + current_codes))

            for code in monitor_codes:
                try:
                    # 执行策略检查
                    # is_filter=False 表示直接执行，不进入缓冲区
                    TR.run(code, is_filter=False)
                except Exception as e:
                    logger.error(f"运行代码 {code} 出错: {traceback.format_exc()}")

            # 清空数据缓存，防止内存溢出，并确保获取最新数据
            Data.clear_cache()

            # 保存交易状态
            TR.save_to_pkl("trader_qmt_stock")

        except Exception as e:
            logger.error(f"主循环出错: {traceback.format_exc()}")
            time.sleep(10)

if __name__ == '__main__':
    try:
        run_trader()
    except Exception as e:
        logger.error(traceback.format_exc())
    finally:
        logger.info("程序退出")
