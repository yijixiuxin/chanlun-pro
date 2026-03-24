# 策略编写指南

## 策略基类

```python
from chanlun.cl_interface import Strategy

class MyStrategy(Strategy):
    def __init__(self):
        super().__init__()
        self.name = "my_strategy"
        self.positions = {}  # 持仓

    def on_bar(self, code: str, market: str, frequency: str, cd):
        """K线更新时调用"""
        pass

    def on_trade(self, trade):
        """成交时调用"""
        pass
```

## 买卖点信号

### 标准买卖点

| 类型 | 说明 |
|------|------|
| 1buy | 第一买点 |
| 2buy | 第二买点 |
| 3buy | 第三买点 |
| l2buy | 类二买点 |
| l3buy | 类三买点 |
| 1sell | 第一卖点 |
| 2sell | 第二卖点 |
| 3sell | 第三卖点 |

### 获取买卖点

```python
# 笔级别买卖点
bi_mmds = cd.get_bi_mmds()

# 线段级别买卖点
xd_mmds = cd.get_xd_mmds()

# 中枢震荡买卖点
zs_mmds = cd.get_zs_zdzd_mmds()
```

## 实盘对接

### 支持的交易接口

参考 `cookbook/docs/实盘.md`：
- 掘金量化
- VNPY
- WTPy

### 示例：订单发送

```python
def send_order(code, direction, price, volume):
    """发送订单"""
    if direction == 'buy':
        # 调用实盘接口买入
        pass
    elif direction == 'sell':
        # 调用实盘接口卖出
        pass
```

## 参考文档

- `cookbook/docs/策略编写与运行.md`
- `cookbook/docs/基于线段的中枢震荡策略.md`
- `cookbook/docs/多中枢类型相同买卖点策略.md`
- `cookbook/docs/缠论买卖点和背驰规则.md`
- `cookbook/docs/缠论数据对象与方法.md`
