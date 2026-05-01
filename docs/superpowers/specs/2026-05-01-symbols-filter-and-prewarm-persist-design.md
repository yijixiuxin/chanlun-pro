# 标的列表只展示股票 + 预热进度持久化 设计

- **日期**：2026-05-01
- **作者**：Claude Code（缠论-pro 项目协作）
- **范围**：`web/chanlun_chart/cl_app/blueprints/symbols.py`、`src/chanlun/exchange/exchange_qmt.py`、`src/chanlun/exchange/exchange_baostock.py`，新增 `prewarm_status/` 持久化目录
- **不在范围**：港股、美股的 type 字段补齐（数据源限制，见"调研结论"）；图表数据 `chart_data_cache` 已有持久化机制，不动

---

## 背景

两个相互独立的功能性需求：

1. **标的列表只展示股票**：当前 `/symbols` 页面把 A 股的指数 / ETF / 个股全部混杂展示，用户希望只看到个股。
2. **预热进度持久化**：当前 `/symbols/prewarm` 任务进度仅在内存（`PrewarmManager._tasks`），重启后清零；用户重新点击全量预热会把已预热的 cache_key 全部重做一遍，浪费时间和上游 HTTP 配额。

## 调研结论

**A 股 tdx 适配器**返回的 `all_stocks()` 已带 `type` 字段（`stock_cn` / `index_cn` / `etf_cn`）。其他适配器：

| 适配器 | type 字段 | 数据源能否识别 |
|--------|----------|----------------|
| `exchange_qmt` (A 股) | ❌ | ✅ xtdata 已按 stock/etf/index 过滤入选，源头加一行赋值即可 |
| `exchange_baostock` (A 股) | ❌ | ✅ 用 code 前缀规则识别 |
| `exchange_tdx_hk` (港股) | ❌ | ⚠️ category 不区分 stock/REIT/ETF，要识别需额外 SDK 调用 |
| `exchange_futu` (港股) | ❌ | ⚠️ 取自单一 plate，没分类信息 |
| `exchange_tdx_us` (美股) | ❌ | ⚠️ category=13/market=74 不细分 |
| `exchange_polygon` / `exchange_alpaca` (美股) | ❌ | ❌ csv 数据源完全不带类型 |

**结论**：港股 / 美股的"是否个股"在当前数据源下识别成本极高（甚至 polygon/alpaca 完全无能为力），本期不动；后续若需要可单独立项。

`chart_data_cache` 已经做了"RAM 热层 + 磁盘冷层"（`fdb.set/get_chart_cache`），重启后磁盘数据仍在，按需 lazy load 进 RAM——这是预热持久化能"借力"的关键基础。

---

## 需求 1：标的列表只展示股票

### 数据层：A 股 3 个适配器补 `type` 字段

#### `src/chanlun/exchange/exchange_qmt.py`

`all_stocks()`（约 line 156）append dict 时，根据已查到的 `_stock_type` 赋值：

```python
if _stock_type.get("stock"):
    sym_type = "stock_cn"
elif _stock_type.get("etf"):
    sym_type = "etf_cn"
else:
    sym_type = "index_cn"
all_stocks.append({
    "code": tdx_code,
    "name": stock_detail["InstrumentName"],
    "type": sym_type,
    "precision": fun.reverse_decimal_to_power_of_ten(stock_detail["PriceTick"]),
})
```

#### `src/chanlun/exchange/exchange_baostock.py`

`all_stocks()`（约 line 37）按 code 前缀规则：

```python
def _infer_baostock_type(code: str) -> str:
    """根据 baostock code 前缀粗略判定个股 vs ETF。
    - sh.6xxxxx / sz.0xxxxx / sz.3xxxxx (创业板) / bj.4|8xxxxx → stock_cn
    - sh.5xxxxx / sz.1xxxxx → etf_cn
    - 其他保留 unknown 让上层兜底放行。
    """
    if "." not in code:
        return "unknown"
    market_part, num_part = code.split(".", 1)
    if not num_part:
        return "unknown"
    head = num_part[0]
    market_part = market_part.lower()
    if market_part == "sh":
        return "stock_cn" if head == "6" else ("etf_cn" if head == "5" else "unknown")
    if market_part == "sz":
        return "stock_cn" if head in ("0", "3") else ("etf_cn" if head == "1" else "unknown")
    if market_part == "bj":
        return "stock_cn" if head in ("4", "8") else "unknown"
    return "unknown"

# 在 append 时：
__all_stocks.append({"code": row[0], "name": row[2], "type": _infer_baostock_type(row[0])})
```

#### `src/chanlun/exchange/exchange_tdx.py`

已有 `type`，无需改。

### API 层：`symbols.py` 加白名单过滤

文件顶部新增常量：

```python
# 标的列表"仅显示个股"的 type 白名单。市场不在表中则不过滤。
# 港股/美股因数据源不返回 type 字段，本期不过滤；期货/外汇/数字货币本无个股概念。
STOCK_ONLY_TYPES_BY_MARKET: Dict[str, set] = {
    "a": {"stock_cn"},
}
```

`symbols_list` 视图函数中，在 query 模糊匹配**之前**应用：

```python
allow_types = STOCK_ONLY_TYPES_BY_MARKET.get(market)
if allow_types is not None:
    def _keep(s):
        # 未带 type 字段或 type='unknown' 的条目默认放行，避免把"识别失败"误删。
        t = s.get("type", "unknown")
        return t in allow_types or t == "unknown"
    all_stocks = [s for s in all_stocks if _keep(s)]
```

### 兼容性

- `g_all_stocks` 是适配器进程内单例：进程重启后才会重新拉到带 type 的列表。这是预期行为。
- `tv.py` 的图表查询、自选导入、提醒等其他用途不读 `type` 字段，不受影响。
- 如果用户某市场配的适配器没补 type（如港股 futu），`STOCK_ONLY_TYPES_BY_MARKET` 表里没有该 market，过滤逻辑短路返回，列表完整。

---

## 需求 2：预热进度持久化 + 已 done 跳过 (2B)

### 已 done 跳过（核心机制 / 复用现有持久化）

`PrewarmManager._prewarm_one_code` 在循环每个 interval 时，先 check 现有缓存：

```python
cache_key = _build_cache_key(market, code, interval, cl_config)
existing = _get_chart_cache_entry(cache_key)  # 自动 fallback 到 disk
if existing is not None and existing.get("cl_chart_data"):
    # 复用磁盘冷层；本次跳过 compute，记 succeeded
    LogUtil.debug(f"[prewarm] cache hit, skip compute {market}/{code} {interval}")
    return True
# 否则继续走 compute_and_cache_chart_data ...
```

→ 利用 `fdb.get_chart_cache` 已有的"按需 load disk"能力，不需要新增持久化层。
重启后只要 disk pkl 还在（`<data>/chart_cache/*.pkl`），跳过自然生效。

### Task 进度持久化

| 维度 | 设计 |
|------|------|
| **存储位置** | `<config.get_data_path()>/prewarm_status/<market>.json`，每市场一个文件 |
| **写盘时机** | (1) 进度推进时，每 50 个标的写一次（`if task.done % 50 == 0`）；(2) 终态（finished/cancelled/error/aborted）写一次 |
| **写盘内容** | `task.to_dict()` 已经把 `to_dict()` 序列化为可 json 的 dict |
| **写盘方式** | 临时文件 → atomic rename，避免崩溃留半截文件；写失败仅 LogUtil.warning |
| **启动恢复** | `PrewarmManager.__init__` 扫描 `prewarm_status/*.json`，json.load 后用 `PrewarmTask.from_dict` 还原对象 |
| **`from_dict`** | 新增 classmethod；只恢复展示性字段，**不恢复** `cancel_event` / `_worker_thread`（不可序列化且无意义） |
| **中断处理** | 启动时若发现某 task `status == "running"`（说明上次进程被杀），改写为 `"aborted"` 并落盘一次；UI 显示"上次中断在 X/Y" |
| **再次预热** | 用户再点全量预热时，新建一个 task 覆盖旧 task；worker 在循环里走"已 done 跳过"逻辑，进度照常推进，但实际算的少很多 |

### `PrewarmTask` 数据迁移

- `to_dict()` 已存在，直接复用
- 新增：

```python
@classmethod
def from_dict(cls, d: dict) -> "PrewarmTask":
    """从持久化 json 还原任务对象（仅恢复展示字段）。"""
    t = cls(market=d["market"], total=d["total"])
    t.task_id = d.get("task_id", t.task_id)
    t.done = d.get("done", 0)
    t.succeeded = d.get("succeeded", 0)
    t.failed = d.get("failed", 0)
    t.current_code = d.get("current_code", "")
    t.current_name = d.get("current_name", "")
    t.status = d.get("status", "aborted")
    t.started_at = d.get("started_at", time.time())
    t.finished_at = d.get("finished_at")
    t.error_msg = d.get("error_msg", "")
    return t
```

### 边界处理

- prewarm_status 目录首次写入时 mkdir；权限失败仅 warning（不影响正常预热）
- 单个 json 损坏：load 时 try/except，损坏文件 unlink + warning，按"该市场无历史 task"处理
- 中途写盘失败：warning，不影响内存进度推进
- 启动恢复阶段失败（如目录不存在、权限拒绝）：warning，按"无历史 task"启动

---

## 测试要点

| 项 | 验证方式 |
|----|----------|
| qmt all_stocks 含 type 字段 | 启动后访问 `/symbols/list?market=a`，items 中至少看到一个非 stock_cn 的条目（说明源头识别正常）后再切到本期实现，列表里只剩 stock_cn |
| baostock all_stocks 含 type 字段 | 切到 baostock 配置下访问 `/symbols/list?market=a`，验证 ETF 被滤除 |
| 港股 / 美股不受影响 | `/symbols/list?market=hk` / `?market=us` 列表数量与改动前一致 |
| 预热前 disk-cache 跳过 | 第一次跑 `/symbols/prewarm`（A 股），等少数标的 done 后 cancel；再次跑应在 done 计数同样数量但实际计算极快 |
| Task 进度持久化 | 跑预热到 done > 50；杀进程；重启；调 `/symbols/prewarm/status?market=a` 应能拿到上次进度（status="aborted"） |
| 中断后再次预热 | 上一步的 aborted 状态下点重新预热，新 task 启动；上次已 done 的标的本次走 cache hit 跳过 |
| json 损坏自愈 | 手工把 `prewarm_status/a.json` 写成乱字符；重启服务；该市场恢复为"无历史 task"，并有 warning 日志 |

---

## 不影响的部分

- `chart_data_cache` / `fdb.set_chart_cache` / `fdb.get_chart_cache` 持久化机制完全不动
- 预热的 worker 线程模型、信号量、用户活跃度让位等并发控制不动
- TradingView 协议字段、登录、CSRF、安全响应头等不动
- 其他蓝图（`alert.py` / `xuangu.py` / `setting.py` / `ai.py` 等）不动
