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
        "market": "futures", # 期权使用期货市场配置（支持做空和T+0）
        "base_code": "SH.10004268", # 示例期权代码
        "codes": ["SH.10004268"], # 回测的代码列表，需确保数据库中有数据
        "frequencys": ["5m", "1m"], # 回测周期
        "start_datetime": "2023-01-01 00:00:00", 
        "end_datetime": "2023-12-31 00:00:00",
        "init_balance": 100000, # 初始资金
        "fee_rate": 0.0001, # 手续费
        "max_pos": 2, # 最大持仓数
        "cl_config": query_cl_chart_config("futures", "5m"), # 缠论配置
        "strategy": strategy, # 策略对象
        "save_file": "./data/backtest_option_qmt.pkl", # 结果保存文件
    }
    
    print("Start Backtest Option QMT")
    bt = BackTest(config)
    # 单进程运行
    bt.run_process(max_workers=1) 
    
    print("Done")

if __name__ == "__main__":
    run_backtest()
