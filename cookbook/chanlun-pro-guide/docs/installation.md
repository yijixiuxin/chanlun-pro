# 安装配置指南

## 环境要求

- Python 3.8+
- 依赖包：pandas, numpy, requests, tqdm 等（见 requirements.txt）

## 安装步骤

### 1. 克隆项目

```bash
git clone https://github.com/your-repo/chanlun-pro.git
cd chanlun-pro
```

### 2. 设置 PYTHONPATH

将 `src` 目录添加到 `PYTHONPATH` 环境变量：

**Linux/Mac:**
```bash
export PYTHONPATH="/path/to/chanlun-pro/src:$PYTHONPATH"
```

**Windows:**
```cmd
set PYTHONPATH=C:\path\to\chanlun-pro\src;%PYTHONPATH%
```

**Python 代码中设置:**
```python
import sys
sys.path.insert(0, '/path/to/chanlun-pro/src')
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

## 配置文件

项目配置位于 `src/chanlun/config.py`，主要配置项：

| 配置项 | 说明 | 可选值 |
|--------|------|--------|
| EXCHANGE_A | A股数据源 | tdx, futu, baostock, db, qmt |
| EXCHANGE_HK | 港股数据源 | tdx_hk, futu, db |
| EXCHANGE_FUTURES | 期货数据源 | tq, tdx_futures, db |
| EXCHANGE_NY_FUTURES | 美股期货 | tdx_ny_futures, db |
| EXCHANGE_FX | 外汇数据源 | tdx_fx, db |
| EXCHANGE_CURRENCY | 数字货币 | binance, db |
| EXCHANGE_US | 美股数据源 | alpaca, polygon, ib, tdx_us, db |

### 配置示例

```python
from chanlun import config

# 使用东方财富数据源
config.EXCHANGE_A = 'futu'

# 使用天勤期货数据源
config.EXCHANGE_FUTURES = 'tq'
```

## 数据源说明

| 数据源 | 说明 | 特点 |
|--------|------|------|
| tdx | 同花顺 | 需安装同花顺软件 |
| futu | 富途 | 实时行情，支持港美股 |
| baostock | Baostock | 免费，分钟数据有限 |
| db | 数据库 | 需自行导入数据 |
| tq | 天勤 | 期货专业数据 |
| binance | 币安 | 数字货币数据 |
