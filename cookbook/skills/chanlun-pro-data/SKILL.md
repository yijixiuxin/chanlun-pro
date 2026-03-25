---
name: chanlun-pro-data
description: "获取 chanlun-pro 项目的行情数据与缠论结构化数据，支持多市场多周期"
metadata:
  project_path: "<设置为你本地的 chanlun-pro 项目路径>"
  execution: "uv run"
---

# Chanlun-Pro Data Retrieval Skill

## 项目路径

**chanlun-pro 项目路径：** `<CHANLUN_PRO_PATH>` （使用前请在配置中设置为你本地的项目路径）

## 脚本执行方式

使用 `uv run` 在 chanlun-pro 目录下执行 scripts 脚本：

```bash
cd <CHANLUN_PRO_PATH>
uv run python scripts/xxx.py
```

或使用 `uv run` 直接执行模块：

```bash
cd <CHANLUN_PRO_PATH>
uv run python -m cookbook.skills.chanlun-pro-data.scripts.get_market_data
```

在使用本技能前，确保项目已安装并配置好环境变量：

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
klines = ex.klines(code='SH.600519', frequency='d')
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
from chanlun.cl_utils import web_batch_get_cl_datas, query_cl_chart_config
from chanlun.exchange import get_exchange
from chanlun.base import Market

# 获取K线数据
ex = get_exchange(Market.A)
klines = ex.klines(code='SH.600519', frequency='d')

# 计算缠论数据
cl_config = query_cl_chart_config('a', 'SH.600519')
cls = web_batch_get_cl_datas(
    market='a',
    code='SH.600519',
    klines={'d': klines},
    cl_config=cl_config
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

### 4. 便捷脚本模块（推荐使用）

`scripts/` 目录提供了更易用的封装模块，可直接导入使用：

```python
# 行情数据获取
from cookbook.skills.chanlun-pro-data.scripts.get_market_data import (
    get_market_data,
    get_multiple_market_data,
    list_supported_markets,
    list_supported_frequencies,
    list_all_market_frequencies,
)

# 缠论数据获取
from cookbook.skills.chanlun-pro-data.scripts.get_cl_data import (
    get_cl_data,
    get_cl_structured_data,
    batch_get_cl_data,
)
```

**行情数据脚本：**

```python
# 查看支持的市场
print(list_supported_markets())   # {'a': '沪深A股', 'hk': '港股', ...}

# 查看周期（不指定市场时返回所有通用周期别名）
print(list_supported_frequencies())
# {'1m': '1分钟', '5m': '5分钟', '15m': '15分钟', '30m': '30分钟', '60m': '60分钟', 'd': '日线', 'w': '周线', 'm': '月线'}

# 查看某市场实际支持的周期（通过 Exchange.support_frequencys 获取）
print(list_supported_frequencies('a'))   # A股，如 {'d': 'Day', 'w': 'Week', 'm': 'Month'}
print(list_supported_frequencies('hk'))  # 港股

# 查看所有市场各自的周期支持情况
print(list_all_market_frequencies())
# {'a': {'d': 'Day', 'w': 'Week', 'm': 'Month'}, 'hk': {...}, ...}

# 获取单个标的行情
df = get_market_data(market='a', code='SH.600519', frequency='d')
# 返回 DataFrame，含 date, open, high, low, close, volume 列

# 批量获取多个标的行情
data = get_multiple_market_data('a', ['SH.600519', 'SH.601398'], 'd')
# 返回 Dict[code, DataFrame]
```

**缠论数据脚本：**

```python
# 获取缠论数据对象
cd = get_cl_data(market='a', code='SH.600519', frequency='d')
bis = cd.get_bis()        # 笔列表
xds = cd.get_xds()        # 线段列表
zss = cd.get_zss()        # 中枢列表
mmds = cd.get_mmds()      # 买卖点列表
bcs = cd.get_bcs()        # 背驰列表

# 获取结构化数据（适合AI处理，返回字典）
data = get_cl_structured_data(market='a', code='SH.600519', frequency='d')
# 返回结构：
# {
#     "code": str,
#     "frequency": str,
#     "klines_count": int,
#     "latest_price": float,
#     "latest_date": str,
#     "bis": List[笔信息],
#     "xds": List[线段信息],
#     "zss": List[中枢信息],
#     "mmds": List[买卖点信息],
#     "bcs": List[背驰信息],
# }

# 批量获取多个标的缠论数据
results = batch_get_cl_data(market='a', codes=['SH.600519', 'SH.601398'], frequency='d')
# 返回 List[Dict]，每个元素对应一个标的的结构化数据
```

## 快速调用示例

### 方式一：使用便捷脚本（推荐）

```python
# 获取A股日线缠论数据
from cookbook.skills.chanlun-pro-data.scripts.get_cl_data import (
    get_cl_structured_data,
)

data = get_cl_structured_data(market='a', code='SH.600519', frequency='d')
print(f"笔数量: {len(data['bis'])}")
print(f"线段数量: {len(data['xds'])}")
print(f"中枢数量: {len(data['zss'])}")
print(f"买卖点: {data['mmds']}")
```

### 方式二：直接使用底层 API

```python
# 完整示例：获取A股日线缠论数据
from chanlun.exchange import get_exchange
from chanlun.base import Market
from chanlun.cl_utils import web_batch_get_cl_datas, query_cl_chart_config

# 1. 获取行情
ex = get_exchange(Market.A)
klines = ex.klines('SH.600519', 'd')

# 2. 计算缠论
cl_config = query_cl_chart_config('a', 'SH.600519')
cls = web_batch_get_cl_datas('a', 'SH.600519', {'d': klines}, cl_config)
cd = cls[0]

# 3. 访问结果
print(f"笔数量: {len(cd.get_bis())}")
print(f"线段数量: {len(cd.get_xds())}")
print(f"中枢数量: {len(cd.get_zss())}")
print(f"买卖点: {cd.get_mmds()}")
```
