# 大QMT桥接使用

---

本项目支持通过 Redis Pub/Sub 与「大QMT」客户端通信，获取沪深行情并执行交易，替代原先 miniQMT 的 `xtdata` 服务。

桥接分为两部分：

- **服务端**：`script\bigqmt_server\big_qmt_redis_bridge.py`（GBK 版），运行在大QMT 内置 Python 中，订阅 Redis 频道并处理请求；
- **客户端**：`src\chanlun\tools\qmt_bridge_client.py`（由 `get_client()` 获取），本项目通过它向桥接服务发起行情 / 交易请求。

> 大QMT 侧使用原生代码格式（`600519.SH`），本项目统一转换为通达信格式（`SH.600519`）。

---

### 前提条件

1. 安装并启动 **Redis**；
2. 正确配置 Redis 的 `ip / port` 等参数（见下）；
3. 如需交易，需将 **accountid** 设置正确；
4. 在 QMT 中运行策略时，需打开 **实盘模式**。

---

### 1. 配置

#### Redis 配置

客户端侧使用项目配置文件 `src\chanlun\config.py` 中的 Redis 设置：

    # Redis 配置
    REDIS_HOST = "127.0.0.1"
    REDIS_PORT = 6379

桥接通信使用独立的 Redis **db=1**，避免与应用自身数据冲突（若你的 Redis 设了密码，请对应配置到服务端）。服务端侧由环境变量读取，常用配置如下：

| 环境变量 | 说明 | 默认值 |
| --- | --- | --- |
| `BIG_QMT_REDIS_HOST` | Redis 地址 | `127.0.0.1` |
| `BIG_QMT_REDIS_PORT` | Redis 端口 | `6379` |
| `BIG_QMT_REDIS_DB` | Redis 库号 | `1` |
| `BIG_QMT_REDIS_PASSWORD` | Redis 密码（可选） | 空 |
| `BIG_QMT_ACCOUNT_ID` | 交易账号ID（必填，用于交易） | 空 |
| `BIG_QMT_DEBUG_LOG` | 详细日志开关，`1` 开启 | `1` |

交易账号也可以在初始化时通过 `ExchangeQMT(account_id=...)` 传入，或在程序里用环境变量 `BIG_QMT_ACCOUNT_ID` 指定。

---

### 2. 部署桥接服务到 大QMT 客户端

1. 将 `script\bigqmt_server\big_qmt_redis_bridge.py`（**GBK 版本**）复制到大QMT 客户端，作为策略运行，**切勿使用 UTF-8 版**（`big_qmt_redis_bridge_utf8.py`）；
2. 在大QMT 内置 Python 中运行该策略（依赖 `redis` 模块，QMT 内置已支持直接 `import redis`）；
3. 若需交易，请务必确认：
   - 已设置正确的 `BIG_QMT_ACCOUNT_ID`（交易请求按账号频道隔离）；
   - 大QMT 已打开**实盘模式**。

> 部署版由 `to_gbk.py` 生成：`script\bigqmt_server\big_qmt_redis_bridge.py`（GBK）。
> 一般只需改 UTF-8 主版，再重新生成即可：`python script\bigqmt_server\to_gbk.py`
> （使用项目 venv 的 python：`.venv\Scripts\python.exe`）。

---

### 3. 使用  `exchange_qmt.py` 调用

在保证桥接服务已运行于大QMT 的前提下，直接在项目中使用 `src\chanlun\exchange\exchange_qmt.py` 调用即可。

#### 基础示例

    from chanlun.exchange.exchange_qmt import ExchangeQMT

    # 传入交易账号（不传则使用环境变量 BIG_QMT_ACCOUNT_ID）
    ex = ExchangeQMT(account_id="8888888888")

    # 获取行情
    klines = ex.klines("SH.000001", "30m")
    print(klines)

    # 获取账户资金
    balance = ex.balance()
    print(balance)

    # 获取持仓
    positions = ex.positions()
    print(len(positions))
    for p in positions:
        print(p)

    # 下单买入
    order = ex.order("SH.515880", "buy", 100)
    print(order)

    # 查询订单详情
    detail = ex.order_detail("635052213")
    print(detail)

    # 撤销订单
    res = ex.order_cancel("2164")
    print(res)

#### 常见接口

- `ex.klines(code, frequency)`：获取 K 线（`code` 用通达信格式，如 `SH.600519`）；
- `ex.balance()`：查询账户资金（返回 `total_money/cash/market_value/total_assets` 等字段）；
- `ex.positions()`：查询持仓（返回 `code/name/amount/can_sell_amount/price/profit` 等字段）；
- `ex.order(code, opt, amount, price=0)`：下单（`opt`：`buy`/`sell`）；
- `ex.order_detail(order_id)`：查询订单详情；
- `ex.order_cancel(order_id)`：撤销订单。

#### 直接调用桥接客户端（可选）

如需更底层、更灵活的调用，可绕过 `exchange_qmt` 直接使用桥接客户端：

    from chanlun.tools.qmt_bridge_client import get_client

    client = get_client()
    client.balance("8888888888")      # 交易接口自动按账号频道请求
    client.positions("8888888888")

桥接客户端暴露的接口包括：`health / all_stocks / all_ticks / tick / instrument_detail / history_data / divid_factors / balance / positions / order / order_detail / order_cancel`。

---

### 常见问题

- **订单返回 `done:False` / 持仓为 0**：多数情况是桥接侧账号未设置或未在 QMT 中打开实盘模式，请检查 `BIG_QMT_ACCOUNT_ID` 与实盘开关；
- **客户端提示 Redis 不可用**：请确认 Redis 已启动，且 `config.py` 的 `REDIS_HOST / REDIS_PORT` 与桥接服务端一致；
- **行情能取到但交易无响应**：交易请求按账号频道 `qmt_bridge:req:<account_id>` 隔离，只有设置了相同 `ACCOUNT_ID` 的大QMT 实例才会处理。

---
