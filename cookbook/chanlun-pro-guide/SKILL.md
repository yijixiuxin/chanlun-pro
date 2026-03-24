---
name: chanlun-pro-guide
description: "指导 AI 基于 chanlun-pro 项目进行二次开发，包含安装配置、行情获取、缠论计算、回测、选股和策略编写"
---

# Chanlun-Pro Guide

本指南用于指导 AI 基于 chanlun-pro 项目进行二次开发。

## 环境准备

### 1. 安装项目

```bash
# 克隆项目
git clone https://github.com/your-repo/chanlun-pro.git
cd chanlun-pro

# 设置 PYTHONPATH
export PYTHONPATH="/path/to/chanlun-pro/src:$PYTHONPATH"
```

### 2. 配置文件

项目配置文件位于 `src/chanlun/config.py`，主要配置项：

- `EXCHANGE_A` - A股数据源：tdx/futu/baostock/db/qmt
- `EXCHANGE_HK` - 港股数据源：tdx_hk/futu/db
- `EXCHANGE_FUTURES` - 期货数据源：tq/tdx_futures/db
- `EXCHANGE_CURRENCY` - 数字货币数据源：binance/db
- `EXCHANGE_US` - 美股数据源：alpaca/polygon/ib/tdx_us/db

## 模块索引

### 行情数据
- `src/chanlun/exchange/__init__.py` - 交易所接口
- `src/chanlun/exchange/exchange.py` - Exchange 抽象类
- `cookbook/chanlun-pro-guide/docs/market_data.md` - 详细指南

### 缠论计算
- `src/chanlun/cl_utils.py` - 缠论工具函数
- `cookbook/chanlun-pro-guide/docs/cl_calculation.md` - 详细指南

### 回测
- `cookbook/chanlun-pro-guide/docs/backtest.md` - 详细指南

### 选股
- `cookbook/chanlun-pro-guide/docs/xuangu.md` - 详细指南

### 策略开发
- `cookbook/chanlun-pro-guide/docs/strategy_dev.md` - 详细指南

## 快速参考

### 获取行情数据

```python
from chanlun.exchange import get_exchange
from chanlun.base import Market

ex = get_exchange(Market.A)
klines = ex.klines('SH.600519', 'd')
```

### 计算缠论

```python
from chanlun.cl_utils import web_batch_get_cl_datas

cls = web_batch_get_cl_datas('a', 'SH.600519', {'d': klines})
cd = cls[0]
```

### 访问缠论数据

```python
bis = cd.get_bis()   # 笔
xds = cd.get_xds()   # 线段
zss = cd.get_zss()   # 中枢
mmds = cd.get_mmds() # 买卖点
bcs = cd.get_bcs()   # 背驰
```

## 参考文档

详细文档见 `cookbook/chanlun-pro-guide/docs/` 目录：
- [安装配置](docs/installation.md)
- [行情数据获取](docs/market_data.md)
- [缠论计算](docs/cl_calculation.md)
- [回测使用](docs/backtest.md)
- [选股操作](docs/xuangu.md)
- [策略编写](docs/strategy_dev.md)
