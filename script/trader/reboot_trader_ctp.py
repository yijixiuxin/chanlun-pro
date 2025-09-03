import time
import traceback

from chanlun import fun
from chanlun.cl_interface import Config
from chanlun.exchange.exchange_ctp import MarketCTP
from chanlun.strategy.strategy_demo import StrategyDemo
from chanlun.trader.online_market_datas import OnlineMarketDatas
from chanlun.trader.trader_ctp import CTPTrader

logger = fun.get_logger("trader_ctp.log")

logger.info("期货自动化交易程序")

try:
    # 初始化CTP行情接口
    market = MarketCTP()

    # 交易品种列表
    run_codes = ["rb2405", "IF2403", "IC2403", "au2406"]  # 可根据需要修改
    frequencys = ["15m", "5m"]  # 使用的K线周期

    # 缠论配置
    cl_config = {
        "fx_qj": Config.FX_QJ_K.value,
        "fx_bh": Config.FX_BH_YES.value,
        "bi_type": Config.BI_TYPE_NEW.value,
        "bi_bzh": Config.BI_BZH_YES.value,
        "bi_fx_cgd": Config.BI_FX_CHD_NO.value,
        "bi_qj": Config.BI_QJ_DD.value,
        "zsd_bzh": Config.ZSD_BZH_NO.value,
        "zsd_qj": Config.ZSD_QJ_DD.value,
        "zs_bi_type": Config.ZS_TYPE_DN.value,
        "zs_seg_type": Config.ZS_TYPE_DN.value,
        "zs_qj": Config.ZS_QJ_CK.value,
        "zs_wzgx": Config.ZS_WZGX_ZGD.value,
    }

    p_strategy_key = "trader_ctp"

    # 初始化交易对象
    TR = CTPTrader("CTP", log=logger.info)
    # 从Redis加载数据
    TR.load_from_pkl(p_strategy_key)
    # 数据对象
    Data = OnlineMarketDatas("futures", frequencys, market, cl_config)
    # 设置策略
    STR = StrategyDemo()

    # 设置策略和数据
    TR.set_strategy(STR)
    TR.set_data(Data)

    logger.info(f"交易品种: {run_codes}")

    while True:
        try:
            # 判断是否是交易时间
            if not market.now_trading():
                time.sleep(10)
                continue

            seconds = int(time.time())

            # 每5分钟运行一次策略
            if seconds % (5 * 60) != 0:
                time.sleep(1)
                continue

            # 合并持仓品种和监控品种
            run_codes = TR.position_codes() + run_codes
            run_codes = list(set(run_codes))

            for code in run_codes:
                try:
                    TR.run(code)
                except Exception:
                    logger.error(f"{code} 策略运行异常: {traceback.format_exc()}")

            # 清空K线缓存
            Data.clear_cache()

            # 保存交易数据
            TR.save_to_pkl(p_strategy_key)

            # 风控检查
            if seconds % (60 * 5) == 0:  # 每5分钟检查一次
                positions = TR.get_positions()
                for pos in positions:
                    # 检查止损
                    if TR.check_stop_loss(pos):
                        TR.force_close(pos.code, pos, "触发止损")
                    # 检查持仓时间
                    if TR.check_position_time(pos):
                        TR.force_close(pos.code, pos, "持仓时间过长")

        except Exception:
            logger.error(f"主循环异常: {traceback.format_exc()}")
            time.sleep(10)

except Exception:
    logger.error(f"程序异常退出: {traceback.format_exc()}")
finally:
    logger.info("程序结束")
    if "TR" in locals():
        TR.close()
