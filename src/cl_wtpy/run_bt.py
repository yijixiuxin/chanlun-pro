from wtpy import WtBtEngine, EngineType
from wtpy.apps import WtBtAnalyst

from cl_wtpy.strategy.base_strategy import BaseStrategy
from chanlun.strategy.strategy_xd_mmd import StrategyXDMMD

if __name__ == "__main__":
    # 创建一个运行环境，并加入策略
    engine = WtBtEngine(EngineType.ET_CTA, logCfg='logcfgbt.json', isFile=False, bDumpCfg=True)
    engine.init('./common/', "configbt.json")
    engine.configBacktest(201909100930, 201912011500)
    engine.configBTStorage(mode="csv", path="./storage/")
    engine.commitBTConfig()

    straInfo = BaseStrategy(
        name='pydt_IF',
        strategy=StrategyXDMMD(),
        code="CFFEX.IF.HOT",
        period='m5'
    )
    engine.set_cta_strategy(straInfo)

    engine.run_backtest()

    analyst = WtBtAnalyst()
    analyst.add_strategy("pydt_IF", folder="./outputs_bt/pydt_IF/", init_capital=500000, rf=0.02,
                         annual_trading_days=240)
    analyst.run()

    kw = input('press any key to exit\n')
    engine.release_backtest()
