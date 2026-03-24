# 回测使用指南

## WTPy 集成

chanlun-pro 支持通过 WTPy 进行策略回测。

### 基本流程

1. **准备历史数据**
2. **编写策略**
3. **执行回测**
4. **分析结果**

### 数据准备

```python
from chanlun.exchange.exchange_db import ExchangeDB
from chanlun.exchange import get_exchange
from chanlun.base import Market

db_ex = ExchangeDB("a")

# 插入K线数据
klines = db_ex.klines('SH.600519', 'd', '2020-01-01', '2024-12-31')
```

## 策略编写

### 基础策略结构

```python
from chanlun.cl_interface import Strategy

class MyStrategy(Strategy):
    def __init__(self):
        super().__init__()
        self.name = "my_strategy"

    def on_bar(self, code: str, market: str, frequency: str, cd):
        """
        每根K线触发一次
        """
        # 获取最新买卖点
        mmds = cd.get_mmds()

        for mmd in mmds:
            if mmd.type == '1buy':
                self.buy(code, mmd.price, 100)
            elif mmd.type == '1sell':
                self.sell(code, mmd.price, 100)
```

### 缠论信号

```python
# 笔买卖点
cd.get_bi_mmds()   # 笔级别买卖点
cd.get_xd_mmds()   # 线段级别买卖点

# 背驰信号
cd.get_bcs()

# 中枢震荡信号
cd.get_zs_zdzd_mmds()  # 中枢震荡买卖点
```

## 参考示例

参考文档：
- `cookbook/docs/缠论回测与交易指南.md`
- `cookbook/docs/策略缠论参数优化.md`
- `cookbook/docs/多中枢类型相同买卖点策略.md`
