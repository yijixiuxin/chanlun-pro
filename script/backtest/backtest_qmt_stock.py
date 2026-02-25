import sys
import pathlib
import datetime

# Add src to path
sys.path.append(str(pathlib.Path(__file__).parent.parent.parent / "src"))

from chanlun.backtesting.backtest import BackTest
from chanlun.strategy.strategy_demo import StrategyDemo
from chanlun.cl_utils import query_cl_chart_config

def run_backtest():
    # 策略配置
    strategy = StrategyDemo()
    
    # 回测配置
    config = {
        "mode": "signal", # signal 信号模式, trade 实盘模式(模拟)
        "market": "a", # 股票市场
        "base_code": "SH.600519", 
        "codes": ["SH.600519", "SZ.000001"], # 回测的代码列表，需确保数据库中有数据
        "frequencys": ["30m", "5m"], # 回测周期，需确保数据库中有对应的周期数据
        "start_datetime": "2023-01-01 00:00:00", 
        "end_datetime": "2023-12-31 00:00:00",
        "init_balance": 100000, # 初始资金
        "fee_rate": 0.0005, # 手续费
        "max_pos": 2, # 最大持仓数
        "cl_config": query_cl_chart_config("a", "30m"), # 缠论配置
        "strategy": strategy, # 策略对象
        "save_file": "./data/backtest_stock_qmt.pkl", # 结果保存文件
    }
    
    print("Start Backtest Stock QMT")
    bt = BackTest(config)
    # 单进程运行，方便调试
    bt.run_process(max_workers=1) 
    
    # 打印结果
    print("Done")
    # bt.info() # BackTest.info() prints log

if __name__ == "__main__":
    run_backtest()
