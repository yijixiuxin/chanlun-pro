# 行情数据获取指南

## 核心接口

通过 `chanlun.exchange.get_exchange()` 获取交易所对象：

```python
from chanlun.exchange import get_exchange
from chanlun.base import Market

# A股
ex = get_exchange(Market.A)

# 港股
ex = get_exchange(Market.HK)

# 期货
ex = get_exchange(Market.FUTURES)
```

## 获取K线数据

### 基本用法

```python
ex = get_exchange(Market.A)

# 获取日线数据
klines = ex.klines('SH.600519', 'd', '2024-01-01', '2024-12-31')

# 获取分钟数据
klines_5m = ex.klines('SH.600519', '5m', '2024-03-01')
klines_30m = ex.klines('SH.600519', '30m', '2024-03-01')
```

### 返回格式

返回 `pandas.DataFrame`，包含列：
- `date` - 日期时间
- `open` - 开盘价
- `high` - 最高价
- `low` - 最低价
- `close` - 收盘价
- `volume` - 成交量

### 支持的周期

| 周期 | 说明 |
|------|------|
| 1m | 1分钟 |
| 5m | 5分钟 |
| 15m | 15分钟 |
| 30m | 30分钟 |
| 60m | 60分钟 |
| d | 日线 |
| w | 周线 |
| m | 月线 |

## 标的代码格式

### A股
- 上交所：`SH.600519`（贵州茅台）
- 深交所：`SZ.000001`（平安银行）

### 期货
- 中金所：`CFFEX.IF2504`
- 大商所：`DCE.m2504`
- 上期所：`SHFE.cu2504`
- 原油：`INE.sc2504`

### 数字货币
- 币安合约：`BTC.USDT`（永续）

### 美股
- 指数：`US.IXIC`（纳斯达克）
- 股票：`US.AAPL`

## 历史数据下载

### A股历史数据

参考脚本：`script/crontab/reboot_sync_a_klines.py`

```python
from chanlun.exchange.exchange_baostock import ExchangeBaostock
from chanlun.exchange.exchange_db import ExchangeDB

db_ex = ExchangeDB("a")
line_ex = ExchangeBaostock()

# 获取所有股票
stocks = line_ex.all_stocks()

# 下载历史数据
f_start_datetime = {
    "m": "1990-01-01",
    "w": "1990-01-01",
    "d": "2000-01-01",
    "30m": "2015-01-01",
    "5m": "2015-01-01",
}

# 同步单个标的
last_dt = db_ex.query_last_datetime('SH.600519', 'd')
klines = line_ex.klines('SH.600519', 'd', start_date=last_dt)
db_ex.insert_klines('SH.600519', 'd', klines)
```

### 数字货币历史数据

参考脚本：`script/crontab/reboot_sync_currency_klines.py`

```python
from chanlun.exchange.exchange_binance import ExchangeBinance

ex = ExchangeBinance()

# 获取所有合约
contracts = ex.all_stocks()

# 下载历史K线
klines = ex.klines('BTC.USDT', 'd', '2020-01-01')
```