---
name: chanlun-pro-data
description: "获取 chanlun-pro 项目的行情数据与缠论结构化数据，支持多市场多周期"
---

# Chanlun-Pro Data Retrieval Skill

在使用本技能前，确保项目已安装并配置好环境变量：
```bash
export PYTHONPATH="/path/to/chanlun-pro/src:$PYTHONPATH"
```

## 核心功能

### 1. 行情数据获取

**使用模块：** `chanlun.exchange`

**主要接口：**
```python
from chanlun.exchange import get_exchange
from chanlun.base import Market

# 获取交易所对象
ex = get_exchange(Market.A)  # A股市场

# 获取K线数据
klines = ex.klines(code='SH.600519', frequency='d', start_date='2024-01-01', end_date='2024-12-31')
```

**支持的市场 (Market)：**
- `Market.A` - 沪深A股
- `Market.HK` - 港股
- `Market.FUTURES` - 国内期货
- `Market.NY_FUTURES` - 美股期货
- `Market.CURRENCY` - 数字货币合约
- `Market.CURRENCY_SPOT` - 数字货币现货
- `Market.US` - 美股
- `Market.FX` - 外汇

**支持的周期：** 1m/5m/15m/30m/60m/d/w/m

### 2. 缠论数据获取

**使用模块：** `chanlun.cl_utils`

**主要接口：**
```python
from chanlun.cl_utils import web_batch_get_cl_datas
from chanlun.exchange import get_exchange
from chanlun.base import Market

# 获取K线数据
ex = get_exchange(Market.A)
klines = ex.klines(code='SH.600519', frequency='d')

# 计算缠论数据
cls = web_batch_get_cl_datas(
    market='a',
    code='SH.600519',
    klines={'d': klines},
    cl_config=None  # 可传入缠论配置
)

cd = cls[0]  # 获取计算结果

# 访问缠论数据
bis = cd.get_bis()        # 笔列表
xds = cd.get_xds()        # 线段列表
zss = cd.get_zss()        # 中枢列表
mmds = cd.get_mmds()      # 买卖点列表
bcs = cd.get_bcs()        # 背驰列表
```

### 3. AI缠论结构化数据

**使用模块：** `chanlun.tools.ai_analyse`

**主要接口：**
```python
from chanlun.tools.ai_analyse import AiAnalyser

analyser = AiAnalyser()
prompt = analyser.prompt(cd=cl_data)  # 生成AI分析prompt

# 返回结构化Markdown格式的缠论分析数据
```

## 快速调用示例

```python
# 完整示例：获取A股日线缠论数据
from chanlun.exchange import get_exchange
from chanlun.base import Market
from chanlun.cl_utils import web_batch_get_cl_datas

# 1. 获取行情
ex = get_exchange(Market.A)
klines = ex.klines('SH.600519', 'd', '2024-01-01')

# 2. 计算缠论
cls = web_batch_get_cl_datas('a', 'SH.600519', {'d': klines})
cd = cls[0]

# 3. 访问结果
print(f"笔数量: {len(cd.get_bis())}")
print(f"线段数量: {len(cd.get_xds())}")
print(f"中枢数量: {len(cd.get_zss())}")
print(f"买卖点: {cd.get_mmds()}")
```
