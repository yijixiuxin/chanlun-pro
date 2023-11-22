from vnpy.trader.constant import Interval
from vnpy_ctabacktester.engine import BacktestingEngine
from datetime import datetime

from cl_vnpy.strategies.chanlun_xdmmd_strategy import ChanlunXdmmdStrategy

engine = BacktestingEngine()

engine.set_parameters(
    vt_symbol='ag2206.SHFE',
    interval=Interval.MINUTE,
    start=datetime(2022, 1, 10),
    end=datetime(2022, 3, 1),
    rate=0.3 / 10000,
    slippage=0.2,
    size=300,
    pricetick=0.2,
    capital=1_000_000,
)

engine.add_strategy(BaseStrategy, {})
engine.load_data()
engine.run_backtesting()
df = engine.calculate_result()
print(df)

engine.calculate_statistics()
engine.show_chart()
