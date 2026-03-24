# 缠论计算指南

## 核心接口

### web_batch_get_cl_datas

批量计算缠论数据：

```python
from chanlun.cl_utils import web_batch_get_cl_datas
from chanlun.exchange import get_exchange
from chanlun.base import Market

# 1. 获取K线数据
ex = get_exchange(Market.A)
klines = ex.klines('SH.600519', 'd', '2024-01-01')

# 2. 计算缠论
cls = web_batch_get_cl_datas(
    market='a',
    code='SH.600519',
    klines={'d': klines},  # 传入多周期：{'d': klines_d, '30m': klines_30m}
    cl_config=None  # 可传入缠论配置
)

# 3. 获取结果
cd = cls[0]  # d 周期结果
```

## 访问缠论数据

### 笔 (BI)

```python
bis = cd.get_bis()

for bi in bis:
    print(f"""
    笔: {bi.type}
    起始: {bi.start.k.date} @ {bi.start.val}
    结束: {bi.end.k.date} @ {bi.end.val}
    完成: {bi.is_done()}
    高点: {bi.high}
    低点: {bi.low}
    """)
```

### 线段 (XD)

```python
xds = cd.get_xds()

for xd in xds:
    print(f"""
    线段: {xd.type}
    起始: {xd.start.k.date}
    结束: {xd.end.k.date}
    完成: {xd.is_done()}
    """)
```

### 中枢 (ZS)

```python
# 获取标准中枢
zss = cd.get_zss()

# 获取特定类型中枢
bi_zss = cd.get_bi_zss('bi')  # 笔中枢
xd_zss = cd.get_xd_zss('xd')  # 线段中枢

for zs in zss:
    print(f"""
    中枢类型: {zs.type}
    级别: {zs.level}
    高点: {zs.zg} 低点: {zs.zd}
    最高: {zs.gg} 最低: {zs.dd}
    """)
```

### 买卖点 (MMD)

```python
mmds = cd.get_mmds()

for mmd in mmds:
    print(f"买卖点: {mmd.type} @ {mmd.price}")
```

### 背驰 (BC)

```python
bcs = cd.get_bcs()

for bc in bcs:
    print(f"背驰: {bc.type}")
```

## 缠论配置

```python
cl_config = {
    'fx_qj': 5,           # 分型区间幅度
    'bi_zj': True,        # 笔是否包含中枢
    'zs_bi_type': ['bi'], # 中枢类型：bi/xd
    'zs_xd_type': ['xd'],
    'fx_bh': True,        # 分型包含笔
    'xd_bzh': True,      # 线段笔破坏
    'xd_px': True,       # 线段方向
}
```

## 增量更新

```python
# 追加新K线后，增量更新
new_klines = ex.klines('SH.600519', 'd', start_date='2024-12-01')

# 使用新K线重新计算
cls = web_batch_get_cl_datas('a', 'SH.600519', {'d': new_klines}, cl_config)
```

## AI分析数据

```python
from chanlun.tools.ai_analyse import AiAnalyser

analyser = AiAnalyser()
prompt = analyser.prompt(cd=cl_data)

# prompt 包含Markdown格式的缠论分析数据
```
