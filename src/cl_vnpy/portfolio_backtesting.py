from datetime import datetime

from vnpy_ctabacktester.engine import BacktestingEngine

from cl_vnpy.strategies.chanlun_xdmmd_strategy import ChanlunXdmmdStrategy


def run_backtesting(strategy_class, setting, vt_symbol, interval, start, end, rate, slippage, size, pricetick, capital):
    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol=vt_symbol,
        interval=interval,
        start=start,
        end=end,
        rate=rate,
        slippage=slippage,
        size=size,
        pricetick=pricetick,
        capital=capital,
    )
    engine.add_strategy(strategy_class, setting)
    engine.load_data()
    engine.run_backtesting()
    df = engine.calculate_result()
    return df

def show_portafolio(df):
    engine = BacktestingEngine()
    engine.calculate_statistics(df)
    engine.show_chart(df)

df1 = run_backtesting(
    strategy_class=ChanlunXdmmdStrategy,
    setting={'fixed_size': 90},
    vt_symbol="MA99.CZCE",
    interval="1m",
    start=datetime(2020, 1, 1),
    end=datetime(2022, 9, 30),
    rate=0.3 / 10000,
    slippage=1,
    size=10,
    pricetick=1,
    capital=1_000_000,
    )

df2 = run_backtesting(
    strategy_class=ChanlunXdmmdStrategy,
    setting={'fixed_size': 60},
    vt_symbol="RB99.SHFE",
    interval="1m",
    start=datetime(2020, 1, 1),
    end=datetime(2022, 9, 30),
    rate=0.3 / 10000,
    slippage=1,
    size=10,
    pricetick=1,
    capital=1_000_000,
    )

df3 = run_backtesting(
    strategy_class=ChanlunXdmmdStrategy,
    setting={'fixed_size': 50},
    vt_symbol="SA99.CZCE",
    interval="1m",
    start=datetime(2020, 1, 1),
    end=datetime(2022, 9, 30),
    rate=0.3 / 10000,
    slippage=1,
    size=20,
    pricetick=1,
    capital=1_000_000,
    )

dfp = df1 + df2 + df3
dfp =dfp.dropna() 
show_portafolio(dfp)