#:  -*- coding: utf-8 -*-
import time
import traceback

from chanlun import fun, zixuan
from chanlun.core.cl_interface import Config
from chanlun.exchange.exchange_tq import ExchangeTq
from chanlun.strategy import strategy_demo
from chanlun.trader.online_market_datas import OnlineMarketDatas
from chanlun.trader.trader_futures import TraderFutures

logger = fun.get_logger("trader_futures.log")

logger.info("期货自动化交易程序")

try:
    zx = zixuan.ZiXuan("futures")
    ex = ExchangeTq(use_account=True)
    # 执行的 标的与周期 设置
    frequencys = ["10s"]
    cl_config = {
        # 分型默认配置
        "fx_qj": Config.FX_QJ_K.value,
        "fx_bh": Config.FX_BH_YES.value,
        # 笔默认配置
        "bi_type": Config.BI_TYPE_NEW.value,
        "bi_bzh": Config.BI_BZH_YES.value,
        "bi_fx_cgd": Config.BI_FX_CHD_NO.value,
        "bi_qj": Config.BI_QJ_DD.value,
        # 线段默认配置
        "xd_bzh": Config.XD_BZH_NO.value,
        "xd_qj": Config.XD_QJ_DD.value,
        # 走势段默认配置
        "zsd_bzh": Config.ZSD_BZH_NO.value,
        "zsd_qj": Config.ZSD_QJ_DD.value,
        # 中枢默认配置
        "zs_bi_type": Config.ZS_TYPE_DN.value,  # 笔中枢类型
        "zs_xd_type": Config.ZS_TYPE_DN.value,  # 走势中枢类型
        "zs_qj": Config.ZS_QJ_CK.value,
        "zs_wzgx": Config.ZS_WZGX_ZGD.value,
    }

    p_redis_key = "trader_futures"

    # 交易对象
    TR = TraderFutures("futures", log=logger.info)
    # 从Redis 中加载交易数据
    TR.load_from_pkl(p_redis_key)
    # 数据对象
    Data = OnlineMarketDatas("futures", frequencys, ex, cl_config)
    # 设置使用的策略
    STR = strategy_demo.StrategyDemo()

    # 将策略与数据对象加入到交易对象中
    TR.set_strategy(STR)
    TR.set_data(Data)

    cl_config = {
        # 分型默认配置
        "fx_qj": Config.FX_QJ_K.value,
        "fx_bh": Config.FX_BH_YES.value,
        # 笔默认配置
        "bi_type": Config.BI_TYPE_NEW.value,
        "bi_bzh": Config.BI_BZH_YES.value,
        "bi_fx_cgd": Config.BI_FX_CHD_NO.value,
        "bi_qj": Config.BI_QJ_DD.value,
        # 线段默认配置
        "xd_bzh": Config.XD_BZH_NO.value,
        "xd_qj": Config.XD_QJ_DD.value,
        # 走势类型默认配置
        "zslx_bzh": Config.ZSLX_BZH_NO.value,
        "zslx_qj": Config.ZSLX_QJ_DD.value,
        # 中枢默认配置
        "zs_bi_type": Config.ZS_TYPE_DN.value,  # 笔中枢类型
        "zs_xd_type": Config.ZS_TYPE_DN.value,  # 走势中枢类型
        "zs_qj": Config.ZS_QJ_CK.value,
        "zs_wzgx": Config.ZS_WZGX_ZGD.value,
    }

    cl_config = {
        # 分型默认配置
        "fx_qj": Config.FX_QJ_K.value,
        "fx_bh": Config.FX_BH_YES.value,
        # 笔默认配置
        "bi_type": Config.BI_TYPE_NEW.value,
        "bi_bzh": Config.BI_BZH_YES.value,
        "bi_fx_cgd": Config.BI_FX_CHD_NO.value,
        "bi_qj": Config.BI_QJ_DD.value,
        # 线段默认配置
        "xd_bzh": Config.XD_BZH_NO.value,
        "xd_qj": Config.XD_QJ_DD.value,
        # 走势类型默认配置
        "zslx_bzh": Config.ZSLX_BZH_NO.value,
        "zslx_qj": Config.ZSLX_QJ_DD.value,
        # 中枢默认配置
        "zs_bi_type": Config.ZS_TYPE_DN.value,  # 笔中枢类型
        "zs_xd_type": Config.ZS_TYPE_DN.value,  # 走势中枢类型
        "zs_qj": Config.ZS_QJ_CK.value,
        "zs_wzgx": Config.ZS_WZGX_ZGD.value,
    }

    p_redis_key = "trader_futures"

    # 交易对象
    TR = TraderFutures("futures", log=logger.info)
    # 从Redis 中加载交易数据
    TR.load_from_pkl(p_redis_key)
    # 数据对象
    Data = OnlineMarketDatas("futures", frequencys, ex, cl_config)
    # 设置使用的策略
    STR = strategy_demo.StrategyDemo()

    # 将策略与数据对象加入到交易对象中
    TR.set_strategy(STR)
    TR.set_data(Data)

    while True:
        try:
            seconds = int(time.time())

            # 每 5 分钟执行一次
            if seconds % (10) != 0:
                time.sleep(1)
                continue

            if ex.now_trading() is False:
                continue

            # 增加当前持仓中的交易对儿
            stocks = zx.zx_stocks("我的持仓")
            run_codes = [_s["code"] for _s in stocks]
            run_codes = TR.position_codes() + run_codes
            run_codes = list(set(run_codes))

            # print('Run Codes %s' % run_codes)

            for code in run_codes:
                try:
                    TR.run(code)
                except Exception as e:
                    logger.error(traceback.format_exc())

            # 清空之前获取的k线缓存，避免后续无法获取最新数据
            Data.clear_cache()
            # 保存交易数据到 Redis 中
            TR.save_to_pkl(p_redis_key)

        except Exception as e:
            logger.error(traceback.format_exc())

except Exception as e:
    logger.error(traceback.format_exc())
finally:
    logger.info("Done")
