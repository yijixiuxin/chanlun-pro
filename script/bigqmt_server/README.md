# 大QMT桥接服务（bigqmt_server，Redis Pub/Sub 模式）

本目录下的大QMT侧桥接服务脚本，用于替代原先 miniQMT 的 `xtdata` 服务，
它在大QMT内置 Python 中运行
（依赖 `redis` 模块，可直接 `import redis`），通过 **Redis** 与应用通信。

## 通信方式（请求/响应）

| 方向 | 机制 | 说明 |
|------|------|------|
| 请求 | **Pub/Sub 订阅（共享频道）** | 项目 `PUBLISH qmt_bridge:req`，所有大QMT实例订阅同一频道；请求 JSON 中带 `result_key`（结果写入的 Redis key） |
| 结果 | **Redis key** | 大QMT处理完 `SETEX` 写入 `result_key`（TTL 300 秒），项目轮询在该 key 中取结果，取完自动删除 |
| 存活 | 心跳 | 大QMT每 5 秒 `SETEX qmt_bridge:heartbeat`（TTL 10 秒），任一实例存活即刷新，仅用于 health 诊断 |

**支持批量启动多个大QMT实例**：所有实例订阅同一共享频道 `qmt_bridge:req`，
谁拿到谁处理（竞争消费，天然负载分担），**无需实例ID**。实例正忙（如 all_stocks 慢请求）时
其它空闲实例会自动接管；重复处理无副作用（同一 result_key 后写覆盖）。

## ⚠️ 编码说明（务必阅读）

大QMT内置 Python 的编辑器/导入链路**按 GBK 处理脚本内容**：

- **`big_qmt_redis_bridge.py`（GBK 编码，声明 `# -*- coding: gbk -*-`）** —— 这是要复制进大QMT运行的文件；
- `big_qmt_redis_bridge_utf8.py`（UTF-8 编码）—— 仓库维护用的主版本，**不要**把它复制进大QMT，
  否则编辑器把中文转成 GBK 后与 utf-8 编码声明不匹配，会报 `UnicodeDecodeError` 导致策略启动后自动关闭；
- 修改主版本后，用 `python to_gbk.py` 重新生成 GBK 部署文件。

## 一、前置条件

- 一台 Redis（与应用共用即可，默认 `127.0.0.1:6379`，db=1）；
- 大QMT内置 Python 环境（可 `import redis`）。

## 二、部署（可批量多个实例）

### 第 1 步：准备脚本

将 `big_qmt_redis_bridge.py`（GBK 版）复制到大QMT内置 Python 环境
（新建 Python 策略后粘贴代码，或按大QMT支持的导入方式导入该文件）。
确认编辑器内中文注释无乱码。

### 第 2 步：启动实例

每个大QMT客户端运行同一份脚本即可，**无需设置实例ID**；可批量启动多个实例，
全部订阅共享频道 `qmt_bridge:req`，谁拿到谁处理（竞争消费，负载分担）。

启动成功日志：

```text
big_qmt_app redis 桥接已启动: redis=127.0.0.1:6379/1 订阅频道=qmt_bridge:req 心跳=qmt_bridge:heartbeat
```

### 第 3 步：项目配置

客户端发布到共享频道，无需配置实例列表；只需保证 Redis 配置一致：

```text
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
```

> 注意：大QMT侧与 cl_fastapi 侧必须连接**同一个 Redis**（host/port/db 一致）。

## 三、大QMT侧环境变量

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `BIG_QMT_REDIS_HOST` | `127.0.0.1` | 通信 Redis 地址 |
| `BIG_QMT_REDIS_PORT` | `6379` | 通信 Redis 端口 |
| `BIG_QMT_REDIS_DB` | `1` | 通信 Redis db |
| `BIG_QMT_REDIS_PASSWORD` | 空 | 通信 Redis 密码 |
| `BIG_QMT_DEBUG_LOG` | `0` | 设为 `1` 输出详细日志（如 all_stocks 每 100 只进度、心跳计数） |

## 四、接口（请求 JSON 中的 path 字段）

请求 JSON 结构：`{"id": "<请求ID>", "path": "/xxx", "params": {...}, "result_key": "qmt_bridge:resp:<请求ID>"}`

| path | 用途 |
|------|------|
| `/health` | 健康检查（运行时是否就绪、共享频道、Redis 信息） |
| `/all_stocks` | 获取全部证券列表（QMT 格式 code/name/price_tick；已用 `is_typed_stock` 过滤，仅股票/ETF/指数） |
| `/all_ticks` | 获取全市场 tick 快照（同上，仅股票/ETF/指数） |
| `/tick` | 获取指定证券 tick 五档行情（`codes=600519.SH,000001.SZ` 可多个） |
| `/instrument_detail` | 获取证券合约详情（名称、价格精度） |
| `/history_data` | 获取历史/实时行情（period/start_time/end_time/count/dividend_type/fields；每次请求先 `download_history_data` 下载） |
| `/divid_factors` | 获取股票除权除息信息 |

代码格式：全部使用大QMT原生格式（`600519.SH`/`000001.SZ`/`830779.BJ`）；
类型判定由大QMT侧 `is_typed_stock(100003/100004/100005)` 完成（股票/ETF/指数），
通达信格式转换（`SZ.000001`）在 cl_fastapi 项目 `app/exchange/exchange_qmt.py` 中完成。

## 五、日志说明（排查用）

服务端日志输出到大QMT策略日志面板，关键节点：

```text
[时间] big_qmt_app redis 桥接已启动: redis=... 订阅频道=qmt_bridge:req 心跳=qmt_bridge:heartbeat
[时间] 收到请求 id=xxx path=/all_stocks result_key=qmt_bridge:resp:xxx
[时间] all_stocks: 获取证券代码 N 个
[时间][DEBUG] all_stocks: 已处理 100/N 只（当前 600519.SH）    # 每100只进度（需 BIG_QMT_DEBUG_LOG=1）
[时间] all_stocks: 完成 共 N 只，耗时 X.Xs
[时间] 请求处理完成 id=xxx path=/all_stocks 耗时 X.Xs
[时间] 结果已写入 key=qmt_bridge:resp:xxx                     # 项目即可取到结果
```

- 看到“收到请求”但长时间没有后续日志，说明卡在具体的行情/详情调用上（逐只进度会显示到哪只）；
- 如果项目侧一直超时且本服务没有“收到请求”，说明消息被其它实例消费或订阅未建立；
  项目会重试发布，空闲实例自动接管。

## 六、注意事项

- 大QMT侧只依赖 `redis` 模块与 Python 标准库；
- 大QMT内置 Python 为 py3.6，脚本避免使用 3.7+ 语法；
- 监听与心跳由 `run_time` 定时器驱动（无线程）；请求在实例内串行处理，
  慢请求（如 all_stocks）会暂时阻塞该实例，可批量启动多个实例共同消费分担压力；
- 访问日志与异常会写入脚本所在目录的 `qmt-bridge-debug.ndjson`；
- 不要用 UTF-8 编辑器直接修改 GBK 部署文件（会出现乱码），请改 UTF-8 主版本后重新运行 `python to_gbk.py`。

---
