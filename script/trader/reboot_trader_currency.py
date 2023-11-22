#:  -*- coding: utf-8 -*-
import time
import traceback

from chanlun import fun
from chanlun.cl_interface import Config
from chanlun.exchange.exchange_binance import ExchangeBinance
from chanlun.strategy.strategy_demo import StrategyDemo
from chanlun.trader.online_market_datas import OnlineMarketDatas
from chanlun.trader.trader_currency import TraderCurrency

logger = fun.get_logger('./logs/trader_currency.log')

logger.info('数字货币自动化交易程序')

try:
    ex = ExchangeBinance()
    run_num = 30
    run_codes = ex.ticker24HrRank(run_num)
    frequencys = ['30m']

    cl_config = {
        # 分型默认配置
        'fx_qj': Config.FX_QJ_K.value,
        'fx_bh': Config.FX_BH_YES.value,
        # 笔默认配置
        'bi_type': Config.BI_TYPE_NEW.value,
        'bi_bzh': Config.BI_BZH_YES.value,
        'bi_fx_cgd': Config.BI_FX_CHD_NO.value,
        'bi_qj': Config.BI_QJ_DD.value,
        # 线段默认配置
        'xd_bzh': Config.XD_BZH_NO.value,
        'xd_qj': Config.XD_QJ_DD.value,
        # 走势段默认配置
        'zsd_bzh': Config.ZSD_BZH_NO.value,
        'zsd_qj': Config.ZSD_QJ_DD.value,
        # 中枢默认配置
        'zs_bi_type': Config.ZS_TYPE_DN.value,  # 笔中枢类型
        'zs_xd_type': Config.ZS_TYPE_DN.value,  # 走势中枢类型
        'zs_qj': Config.ZS_QJ_CK.value,
        'zs_wzgx': Config.ZS_WZGX_ZGD.value,
    }

    p_redis_key = 'trader_currency'

    # 交易对象
    TR = TraderCurrency('Currency', log=logger.info)
    # 从 Redis 中加载数据
    TR.load_from_redis(p_redis_key)
    # 数据对象
    Data = OnlineMarketDatas('currency', frequencys, ex, cl_config)
    # 设置使用的策略
    STR = StrategyDemo()

    # 将策略与数据对象加入到交易对象中
    TR.set_strategy(STR)
    TR.set_data(Data)

    logger.info('Run symbols: %s' % run_codes)

    while True:
        try:
            seconds = int(time.time())

            if seconds % (60 * 60) == 0:
                # 每一个小时，更新 24 小时交易量排行代码
                run_codes = ex.ticker24HrRank(run_num)
                logger.info('Run symbols: %s' % run_codes)

            if seconds % (5 * 60) != 0:
                time.sleep(1)
                continue

            # 增加当前持仓中的交易对儿
            run_codes = TR.position_codes() + run_codes
            run_codes = list(set(run_codes))

            for code in run_codes:
                try:
                    TR.run(code)
                except Exception as e:
                    logger.error(traceback.format_exc())

            # 清空之前获取的k线缓存，避免后续无法获取最新数据
            Data.clear_cache()

            # 保存交易数据到 Redis 中
            TR.save_to_redis(p_redis_key)

        except Exception as e:
            logger.error(traceback.format_exc())

except Exception as e:
    logger.error(traceback.format_exc())
finally:
    logger.info('Done')
