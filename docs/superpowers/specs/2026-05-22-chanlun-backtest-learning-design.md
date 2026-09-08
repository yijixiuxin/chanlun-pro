# 缠论回测学习页面设计

## 概述

在现有 Flask Web 应用中新增一个缠论回测学习页面（路由 `/backtest`），用户可以随机抽取一只 A 股股票，回放历史 K 线走势，观察缠论运行过程，并进行模拟交易练习。

## 页面布局

左右分栏布局：

- **左侧 (70%宽度)**：两个 TradingView 图表，上下 7/3 分割
  - 上方：小级别图表（如 30m）
  - 下方：大级别图表（如 d）
- **右侧 (30%宽度)**：操作控制区

## 路由设计

| 方法 | 路由 | 用途 |
|---|---|---|
| `GET` | `/backtest` | 页面入口，返回 HTML |
| `POST` | `/backtest/start` | 随机选股、拉取行情、计算初始缠论数据，返回初始状态 |
| `POST` | `/backtest/step` | 回放前进一根 K 线，返回两个周期更新后的 TV chart 数据 |
| `POST` | `/backtest/stop` | 结束回放，清理 session |
| `GET` | `/backtest/tv/config` | TV 图表配置 |
| `GET` | `/backtest/tv/history` | TV 图表历史数据（仅回传到当前回放位置） |

## 后端逻辑

### 1. 随机选股（`/backtest/start`）

- 从通达信全量股票列表（60/00/30 开头）中随机选取一只
- 随机选择一组周期对：`(d, 30m)`、`(60m, 15m)`、`(30m, 5m)` — 前者为大级别，后者为小级别
- 调用 `ExchangeTDX.klines()` 获取小级别 K 线数据
- 若数据不足 4000 根，重新随机选股
- 从倒数第 1000 根 K 线开始回放

### 2. 股票匿名化

- 后端随机选股后，对前端隐藏真实股票代码和名称
- 前端只显示匿名标识，如 "股票 #1" 或随机别名
- 后端 session 中保留真实代码用于行情获取，前端始终使用匿名标识

### 3. 高周期转换（无未来数据）

- 每次小级别数据更新后，用 `convert_stock_kline_frequency(small_klines[0:current_pos], high_freq)` 做周期转换
- 确保高周期不会包含回放位置之后的 K 线数据

### 4. 缠论计算

- 使用 `cl.CL(code, freq, cl_config).process_klines(klines)` 计算
- 每次调用都传入**完整的当前 K 线数据** `klines[0:current_pos]`（不是只传新增部分）
- `process_klines` 内部自行处理增量逻辑，跳过已处理的 bar，只更新新增或变化的 bar
- 计算结果缓存在内存（session 级别），不需要文件缓存

### 5. TV 数据转换

- 使用 `cl_data_to_tv_chart(cd, config)` 生成 TradingView 图表数据

### 6. 回放状态（内存 session）

```python
replay_sessions = {}  # key: session_id

{
    "code": "SH.600001",              # 真实代码（不传给前端）
    "name": "上证指数",                # 真实名称（不传给前端）
    "display_id": "股票 #1",          # 匿名标识（传给前端）
    "small_freq": "30m",
    "high_freq": "d",
    "all_klines": pd.DataFrame,      # 全部小级别 K 线
    "current_pos": int,               # 当前回放到的 index
    "start_pos": int,                 # 起始位置
    "cl_small": ICL,                  # 小级别 CL 对象
    "cl_high": ICL,                   # 大级别 CL 对象
    "cl_config": dict,                # 缠论配置
    "speed": int,                     # 回放速度（秒/根）
    "running": bool,                  # 是否正在回放
}
```

### 7. TV 数据馈送

自定义 JS datafeed，不直接使用 TV 自带的 UDF。因为需要通过 `/backtest/tv/history` 限定 K 线只到当前回放位置。前端使用 `Datafeeds.UDFCompatibleDatafeed` 但指向 `/backtest/tv/` 前缀。

## 前端设计

### 左侧图表区

- 两个 TV 图表 widget，用 `Widget` 构造函数创建
- 上方图表：symbol 设为 `small`，下方图表：symbol 设为 `high`
- 每次 `/backtest/step` 返回数据后：
  - 调用 `chart_small.setData(data_small)` 更新小级别图表（需使用 TV 的 `real-time` 模式推送新 bar）
  - 或用 `datafeed` 的 callback 推送新数据
- 缠论绘制：参照 `charts.js` 中 `ChartUtils` 的**绘制逻辑**（分型、笔、线段、中枢的形状构造），在 `backtest.js` 中独立实现，不直接复用 `ChartUtils`，因为不需要其中的按钮事件、标记管理等交互功能，只需要纯绘制

### 右侧操作区（纯 HTML + JS）

```
┌─────────────────────┐
│      操作控制        │
│ [开始] [暂停] [继续] │
│ 回放速度: [滑块] 1s  │
├─────────────────────┤
│ 股票: #1             │
│ 周期: 30m / d       │
├─────────────────────┤
│ 初始资金: ¥100,000  │
│ 当前资金: ¥98,500   │
│ 浮动盈亏: -1.5%     │
├─────────────────────┤
│    当前持仓          │
│  多: 1000股 @10.50  │
│  空: 无              │
├─────────────────────┤
│    交易操作          │
│ 当前价格: ¥10.52    │
│ 数量: [输入框]       │
│ [买入] [卖出] [平仓] │
├─────────────────────┤
│    持仓历史          │
│ (交易记录表格)       │
└─────────────────────┘
```

### 模拟交易逻辑（前端 JS）

- 初始资金：100,000
- 做多：买入用当前 K 线收盘价，扣减资金 = 价格 × 数量
- 做空：卖出用当前 K 线收盘价，扣减资金 = 价格 × 数量
- 平仓：计算盈亏，归还资金
- 当前市值 = 现金 + 持仓市值（多仓为正，空仓为负）
- 状态仅存于当前页面，刷新即丢失

## 文件清单

| 文件 | 变更类型 | 说明 |
|---|---|---|
| `web/chanlun_chart/cl_app/__init__.py` | 修改 | 添加 `/backtest` 路由 |
| `web/chanlun_chart/cl_app/templates/backtest.html` | 新增 | 回测页面模板 |
| `web/chanlun_chart/cl_app/static/js/backtest.js` | 新增 | 前端交互逻辑 + TV 图表 + 缠论绘制 |
| `web/chanlun_chart/cl_app/static/css/backtest.css` | 新增 | 页面样式 |
| `web/chanlun_chart/cl_app/templates/index.html` | 修改 | 导航菜单添加回测入口 |

不需要修改 `src/chanlun/` 下的任何文件，复用现有 API。

## 已确认

- 初始资金 100,000
- 回放速度默认 2 秒/根，范围 0.5s ~ 10s
- 交易手续费暂不考虑
- 股票代码和名称匿名化处理
- 缠论计算每次传入完整 klines，由 `process_klines` 内部处理增量
- 缠论绘制参照 `ChartUtils` 但独立实现，不引入冗余的交互功能
