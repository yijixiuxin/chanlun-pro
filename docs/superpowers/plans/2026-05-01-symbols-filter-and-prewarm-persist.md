# 标的列表过滤 + 预热进度持久化 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 spec `2026-05-01-symbols-filter-and-prewarm-persist-design.md` 中的两条独立需求：A 股标的列表只显示个股（按 type 字段过滤），以及 `/symbols/prewarm` 任务进度持久化到磁盘并在重启时恢复。

**Architecture:** 需求 1 在数据层（A 股 3 个适配器）补 type 字段、在 API 层（`symbols.py`）按白名单过滤；需求 2 在 `PrewarmManager` 加写盘 / 启动恢复逻辑，复用 `<data>/prewarm_status/<market>.json` 文件。`_prewarm_one_code` 中已有"磁盘冷层命中跳过"逻辑，无需改造。

**Tech Stack:** Python 3.10+（项目最低版本），Flask（视图层），现有 `chanlun.config.get_data_path()`，标准库 `json` / `tempfile` / `os`。无需引入新依赖。

**Test approach:** 项目无 pytest 框架，沿用之前 `_verify_csrf_security.py` 同模式 — 每个 phase 后用一次性 verify 脚本通过 `importlib.util` 直接加载模块验证纯函数 / 数据结构正确性，集成验证靠手工烟雾测试。

---

## File Map

| 文件 | 改动类型 | 责任 |
|------|----------|------|
| `src/chanlun/exchange/exchange_qmt.py` | Modify | A 股 qmt 适配器 `all_stocks()` 补 `type` 字段（line ~156） |
| `src/chanlun/exchange/exchange_baostock.py` | Modify | A 股 baostock 适配器 `all_stocks()` 补 `type` 字段 + 辅助函数 `_infer_baostock_type` |
| `web/chanlun_chart/cl_app/blueprints/symbols.py` | Modify | (a) 顶部加 `STOCK_ONLY_TYPES_BY_MARKET` 常量；(b) `symbols_list` 视图加过滤逻辑；(c) `PrewarmTask` 加 `from_dict` 类方法；(d) `PrewarmManager` 加写盘 / 启动恢复 / 中断处理 |

无新增模块文件、无新增测试目录。验证脚本临时落到 `_verify_*.py` 用完即删。

---

## Phase 1：A 股 3 个适配器补 type 字段 + 标的列表过滤

### Task 1：`exchange_qmt.py` 补 `type` 字段

**Files:**
- Modify: `src/chanlun/exchange/exchange_qmt.py:136-160`

- [ ] **Step 1: 编辑 all_stocks() 中 append dict 的位置**

`Read` 当前 line 132-163 确认上下文，然后用 Edit 把：

```python
                    all_stocks.append({
                        "code": tdx_code,
                        "name": stock_detail["InstrumentName"],
                        "precision": fun.reverse_decimal_to_power_of_ten(stock_detail["PriceTick"]),
                    })
```

替换为：

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

注：line 138-140 已经查到 `_stock_type = xtdata.get_instrument_type(_c)` 并用其过滤入选条件，这里直接复用变量。

- [ ] **Step 2: ast 语法校验**

Run:
```bash
cd D:/project/chanlun-pro && python -c "import ast; ast.parse(open('src/chanlun/exchange/exchange_qmt.py', encoding='utf-8').read())"
```

Expected: 无输出（无错即通过）

- [ ] **Step 3: Commit**

```bash
git add src/chanlun/exchange/exchange_qmt.py
git commit -m "feat(exchange/qmt): all_stocks 返回 type 字段（stock_cn/etf_cn/index_cn）

为标的列表"只显示股票"过滤提供数据基础。复用已查得的 _stock_type
（xtdata.get_instrument_type）赋值，不增加新的 native 调用。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2：`exchange_baostock.py` 补 type 字段

**Files:**
- Modify: `src/chanlun/exchange/exchange_baostock.py:37-57`

- [ ] **Step 1: 在 `all_stocks` 之前新增辅助函数 `_infer_baostock_type`**

用 Edit 把：

```python
    def all_stocks(self):
        """
        获取支持的所有股票列表
        :return:
        """
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        # TODO 节假日兼容
        day = "2022-04-18"

        rs = bs.query_all_stock(day=day)
        __all_stocks = []
        while (rs.error_code == "0") & rs.next():
            # 获取一条记录，将记录合并在一起
            row = rs.get_row_data()
            if row[0][:6] in ["sz.399", "sh.000"]:
                continue
            __all_stocks.append({"code": row[0], "name": row[2]})
        self.g_all_stocks = __all_stocks
        return self.g_all_stocks
```

替换为：

```python
    @staticmethod
    def _infer_baostock_type(code: str) -> str:
        """根据 baostock code 前缀推断个股 / ETF。
        - sh.6xxxxx / sz.0xxxxx / sz.3xxxxx (创业板) / bj.4|8xxxxx → stock_cn
        - sh.5xxxxx / sz.1xxxxx → etf_cn
        - 其他保留 unknown，由上层"未识别默认放行"兜底。
        """
        if "." not in code:
            return "unknown"
        market_part, num_part = code.split(".", 1)
        if not num_part:
            return "unknown"
        head = num_part[0]
        market_part = market_part.lower()
        if market_part == "sh":
            if head == "6":
                return "stock_cn"
            if head == "5":
                return "etf_cn"
            return "unknown"
        if market_part == "sz":
            if head in ("0", "3"):
                return "stock_cn"
            if head == "1":
                return "etf_cn"
            return "unknown"
        if market_part == "bj":
            if head in ("4", "8"):
                return "stock_cn"
            return "unknown"
        return "unknown"

    def all_stocks(self):
        """
        获取支持的所有股票列表
        :return:
        """
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        # TODO 节假日兼容
        day = "2022-04-18"

        rs = bs.query_all_stock(day=day)
        __all_stocks = []
        while (rs.error_code == "0") & rs.next():
            # 获取一条记录，将记录合并在一起
            row = rs.get_row_data()
            if row[0][:6] in ["sz.399", "sh.000"]:
                continue
            __all_stocks.append({
                "code": row[0],
                "name": row[2],
                "type": self._infer_baostock_type(row[0]),
            })
        self.g_all_stocks = __all_stocks
        return self.g_all_stocks
```

- [ ] **Step 2: ast 语法校验**

Run:
```bash
cd D:/project/chanlun-pro && python -c "import ast; ast.parse(open('src/chanlun/exchange/exchange_baostock.py', encoding='utf-8').read())"
```

Expected: 无输出

- [ ] **Step 3: Commit**

```bash
git add src/chanlun/exchange/exchange_baostock.py
git commit -m "feat(exchange/baostock): all_stocks 返回 type 字段（按 code 前缀推断）

为标的列表"只显示股票"过滤提供数据基础。前缀规则：
- sh.6 / sz.0 / sz.3 / bj.4|8 → stock_cn
- sh.5 / sz.1 → etf_cn
- 其他 → unknown（上层兜底放行）

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3：`symbols.py` 加白名单过滤

**Files:**
- Modify: `web/chanlun_chart/cl_app/blueprints/symbols.py:64-66`（顶部加常量）
- Modify: `web/chanlun_chart/cl_app/blueprints/symbols.py:127-136`（视图过滤）

- [ ] **Step 1: 加常量到 `DEFAULT_PAGE_SIZE` 后面**

用 Edit 把：

```python
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 500
```

替换为：

```python
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 500

# 标的列表"仅显示个股"的 type 白名单。市场不在表中则不过滤。
# 港股/美股因数据源不返回 type 字段，本期不过滤；期货/外汇/数字货币本无个股概念。
STOCK_ONLY_TYPES_BY_MARKET: Dict[str, set] = {
    "a": {"stock_cn"},
}
```

- [ ] **Step 2: 在 `symbols_list` 视图函数里 query 模糊匹配前加过滤**

用 Edit 把：

```python
    if query:
        filtered = [
            s
            for s in all_stocks
            if query in s.get("code_lower", "")
            or query in s.get("name_lower", "")
            or query in s.get("pinyin_initials", "")
        ]
    else:
        filtered = all_stocks
```

替换为：

```python
    # 应用 type 白名单过滤（仅对配置了的市场生效）
    allow_types = STOCK_ONLY_TYPES_BY_MARKET.get(market)
    if allow_types is not None:
        def _keep(s):
            # 未带 type 字段或 type='unknown' 的条目默认放行，避免把"识别失败"误删。
            t = s.get("type", "unknown")
            return t in allow_types or t == "unknown"
        all_stocks = [s for s in all_stocks if _keep(s)]

    if query:
        filtered = [
            s
            for s in all_stocks
            if query in s.get("code_lower", "")
            or query in s.get("name_lower", "")
            or query in s.get("pinyin_initials", "")
        ]
    else:
        filtered = all_stocks
```

- [ ] **Step 3: ast 语法校验**

Run:
```bash
cd D:/project/chanlun-pro && python -c "import ast; ast.parse(open('web/chanlun_chart/cl_app/blueprints/symbols.py', encoding='utf-8').read())"
```

Expected: 无输出

- [ ] **Step 4: Commit**

```bash
git add web/chanlun_chart/cl_app/blueprints/symbols.py
git commit -m "feat(symbols): A 股标的列表按 type 白名单只显示个股

新增 STOCK_ONLY_TYPES_BY_MARKET = {'a': {'stock_cn'}}；symbols_list
视图在 query 匹配前先按白名单过滤。未识别 type（'unknown'）默认放行
避免误删；其他市场未配置即不过滤。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 4：Phase 1 verify 脚本

**Files:**
- Create: `_verify_phase1.py`（临时验证脚本，验完即删）

- [ ] **Step 1: 写 verify 脚本**

Write `_verify_phase1.py`：

```python
"""Phase 1 验证：baostock type 推断 + symbols.py 过滤逻辑（纯函数级）。"""
import ast, sys, importlib.util, pathlib
ROOT = pathlib.Path(__file__).parent

# 1. 验证 _infer_baostock_type 规则
spec = importlib.util.spec_from_file_location("baostock_iso", ROOT / "src/chanlun/exchange/exchange_baostock.py")
# 不能直接 exec_module（要 import baostock 三方包），改用 ast 抽出函数源码并 exec 进独立命名空间
src = (ROOT / "src/chanlun/exchange/exchange_baostock.py").read_text(encoding="utf-8")
tree = ast.parse(src)
func_src = None
for node in ast.walk(tree):
    if isinstance(node, ast.FunctionDef) and node.name == "_infer_baostock_type":
        func_src = ast.unparse(node)
        break
assert func_src, "_infer_baostock_type not found"
ns = {}
exec(func_src, ns)
infer = ns["_infer_baostock_type"]

cases = [
    ("sh.600519", "stock_cn"), ("sh.601318", "stock_cn"),
    ("sh.510300", "etf_cn"), ("sh.588000", "etf_cn"),  # 5 开头都判 etf_cn
    ("sz.000001", "stock_cn"), ("sz.300750", "stock_cn"),
    ("sz.159915", "etf_cn"),
    ("bj.430047", "stock_cn"), ("bj.832000", "stock_cn"),
    ("xx.123456", "unknown"), ("invalidcode", "unknown"),
    ("sh.", "unknown"),
]
for code, expected in cases:
    got = infer(code)
    assert got == expected, f"infer({code!r}): expected {expected}, got {got}"
print(f"OK  baostock infer 规则 {len(cases)} 项全通过")

# 2. 验证 symbols.py 顶部常量与过滤逻辑
sym_src = (ROOT / "web/chanlun_chart/cl_app/blueprints/symbols.py").read_text(encoding="utf-8")
assert 'STOCK_ONLY_TYPES_BY_MARKET' in sym_src, "常量未添加"
assert '"a": {"stock_cn"}' in sym_src, "A 股白名单未配置"
assert "allow_types = STOCK_ONLY_TYPES_BY_MARKET.get(market)" in sym_src, "过滤入口未添加"
assert "if allow_types is not None:" in sym_src, "过滤条件未添加"
assert "t == \"unknown\"" in sym_src, "unknown 兜底未添加"
print("OK  symbols.py 过滤代码片段全部存在")

# 3. 验证 exchange_qmt.py append 已带 type
qmt_src = (ROOT / "src/chanlun/exchange/exchange_qmt.py").read_text(encoding="utf-8")
assert '"type": sym_type' in qmt_src, "qmt append type 未添加"
assert "_stock_type.get(\"stock\")" in qmt_src, "qmt type 推断未添加"
print("OK  exchange_qmt.py type 字段已注入")

print("\nPhase 1 ALL PASS")
```

- [ ] **Step 2: 用项目环境跑 verify 脚本**

Run:
```bash
C:/Users/lc/miniconda3/envs/chanlun-pro/python.exe D:/project/chanlun-pro/_verify_phase1.py
```

Expected: 末尾输出 `Phase 1 ALL PASS`，全部 `OK` 行

- [ ] **Step 3: 删除验证脚本**

Run:
```bash
rm D:/project/chanlun-pro/_verify_phase1.py
```

Expected: 文件被删除（不 commit）

---

## Phase 2：预热 Task 进度持久化

### Task 5：`PrewarmTask` 加 `from_dict` 类方法

**Files:**
- Modify: `web/chanlun_chart/cl_app/blueprints/symbols.py`（在 `to_dict` 方法之后约 line 283）

- [ ] **Step 1: 在 `to_dict` 方法后加 `from_dict` 类方法**

用 Edit 把：

```python
    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "market": self.market,
            "total": self.total,
            "done": self.done,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "current_code": self.current_code,
            "current_name": self.current_name,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "elapsed": (self.finished_at or time.time()) - self.started_at,
            "error_msg": self.error_msg,
        }
```

替换为：

```python
    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "market": self.market,
            "total": self.total,
            "done": self.done,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "current_code": self.current_code,
            "current_name": self.current_name,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "elapsed": (self.finished_at or time.time()) - self.started_at,
            "error_msg": self.error_msg,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PrewarmTask":
        """从持久化 json 还原任务对象（仅恢复展示字段，不恢复 cancel_event 等运行时对象）。"""
        t = cls(market=d["market"], total=int(d.get("total", 0)))
        t.task_id = d.get("task_id", t.task_id)
        t.done = int(d.get("done", 0))
        t.succeeded = int(d.get("succeeded", 0))
        t.failed = int(d.get("failed", 0))
        t.current_code = d.get("current_code", "")
        t.current_name = d.get("current_name", "")
        t.status = d.get("status", "aborted")
        t.started_at = float(d.get("started_at", time.time()))
        finished = d.get("finished_at")
        t.finished_at = float(finished) if finished is not None else None
        t.error_msg = d.get("error_msg", "")
        return t
```

- [ ] **Step 2: ast 语法校验**

Run:
```bash
cd D:/project/chanlun-pro && python -c "import ast; ast.parse(open('web/chanlun_chart/cl_app/blueprints/symbols.py', encoding='utf-8').read())"
```

Expected: 无输出

- [ ] **Step 3: Commit**

```bash
git add web/chanlun_chart/cl_app/blueprints/symbols.py
git commit -m "feat(symbols): PrewarmTask 加 from_dict 类方法

为预热任务进度从磁盘 json 恢复做准备；只恢复展示字段，不恢复
cancel_event / _worker_thread 等运行时对象（这些在 __init__ 中重建）。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 6：`PrewarmManager` 加写盘辅助方法 + 进度推进时调用

**Files:**
- Modify: `web/chanlun_chart/cl_app/blueprints/symbols.py`（在 `PrewarmManager.__init__` 附近 line 295-303 加常量；新增 `_persist_task_locked` 方法；`_run_task` 内 `_process_one` 计数后调用）

- [ ] **Step 1: 在 PREWARM_USER_RECENT_TRACK_SECONDS 常量后加持久化目录配置**

用 Edit 把：

```python
PREWARM_USER_RECENT_TRACK_SECONDS = 600  # 10 分钟内看过的算"用户关注"
```

替换为：

```python
PREWARM_USER_RECENT_TRACK_SECONDS = 600  # 10 分钟内看过的算"用户关注"

# 任务进度持久化目录与写盘频率
_PREWARM_PERSIST_DIRNAME = "prewarm_status"
# 写盘频率：每完成 N 个标的写一次（额外终态时强制写一次）。
# 50 是经验值：典型 11k 标的预热 220 次写盘，IO 开销 < 1%；同时崩溃后丢失进度 < 50 个。
_PREWARM_PERSIST_EVERY_N_DONE = 50
```

- [ ] **Step 2: 在 `PrewarmManager.__init__` 后追加持久化辅助方法**

用 Edit 把：

```python
    def __init__(self):
        self._lock = threading.Lock()
        # market -> 最近一次任务（无论是否已完成）
        self._tasks: Dict[str, PrewarmTask] = {}
        # 是否有任务正在运行（全局互斥）
        self._global_running: bool = False
        # worker 线程引用（仅用于调试，不主动 join）
        self._worker_thread: Optional[threading.Thread] = None

    # ---------------- 公开 API ----------------
```

替换为：

```python
    def __init__(self):
        self._lock = threading.Lock()
        # market -> 最近一次任务（无论是否已完成）
        self._tasks: Dict[str, PrewarmTask] = {}
        # 是否有任务正在运行（全局互斥）
        self._global_running: bool = False
        # worker 线程引用（仅用于调试，不主动 join）
        self._worker_thread: Optional[threading.Thread] = None

    # ---------------- 持久化 ----------------

    def _persist_dir(self) -> "pathlib.Path":
        """惰性获取持久化目录；首次调用时创建。失败返回 None 由调用方降级。"""
        from chanlun.config import get_data_path
        try:
            d = get_data_path() / _PREWARM_PERSIST_DIRNAME
            d.mkdir(parents=True, exist_ok=True)
            return d
        except OSError as e:
            LogUtil.warning(f"[prewarm] persist dir create failed: {e}")
            return None

    def _persist_task(self, task: "PrewarmTask") -> None:
        """把单个 task 状态原子写到 <data>/prewarm_status/<market>.json。
        写失败仅 warning，不影响内存进度。
        多 worker 可能并发调用（_process_one 中按 done % 50 触发），tmp 名
        带 uuid 避免互相覆盖；最终 rename 到同一目标，后到者覆盖前者，符合
        "保留最新一次写入"语义。
        """
        d = self._persist_dir()
        if d is None:
            return
        path = d / f"{task.market}.json"
        tmp = d / f"{task.market}.json.tmp.{uuid.uuid4().hex}"
        try:
            data = task.to_dict()
            tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            # Windows 上 Path.replace 是原子的（同卷），避免半截文件。
            tmp.replace(path)
        except OSError as e:
            LogUtil.warning(f"[prewarm] persist task failed market={task.market}: {e}")
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass

    # ---------------- 公开 API ----------------
```

- [ ] **Step 3: 在文件顶部 import 区加 `import json` 与 `import pathlib`**

`Read` 文件 line 26-32 确认现有 import，然后用 Edit 把：

```python
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
```

替换为：

```python
import json
import pathlib
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
```

- [ ] **Step 4: `_run_task` 内 `_process_one` 计数后按频率写盘**

`Read` `_process_one` 内 done 计数那段（line 446-458），找到：

```python
            with counter_lock:
                if code_ok:
                    task.succeeded += 1
                else:
                    task.failed += 1
                task.done += 1
                done_now = task.done

            if done_now % 100 == 0:
                LogUtil.info(
                    f"[prewarm] progress market={market} "
                    f"{done_now}/{task.total} succeeded={task.succeeded} failed={task.failed}"
                )
```

用 Edit 替换为：

```python
            with counter_lock:
                if code_ok:
                    task.succeeded += 1
                else:
                    task.failed += 1
                task.done += 1
                done_now = task.done

            if done_now % 100 == 0:
                LogUtil.info(
                    f"[prewarm] progress market={market} "
                    f"{done_now}/{task.total} succeeded={task.succeeded} failed={task.failed}"
                )
            # 周期性持久化任务进度（崩溃后最多丢失 _PREWARM_PERSIST_EVERY_N_DONE 个标的的进度）
            if done_now % _PREWARM_PERSIST_EVERY_N_DONE == 0:
                self._persist_task(task)
```

- [ ] **Step 5: ast 语法校验**

Run:
```bash
cd D:/project/chanlun-pro && python -c "import ast; ast.parse(open('web/chanlun_chart/cl_app/blueprints/symbols.py', encoding='utf-8').read())"
```

Expected: 无输出

- [ ] **Step 6: Commit**

```bash
git add web/chanlun_chart/cl_app/blueprints/symbols.py
git commit -m "feat(symbols): PrewarmManager 加进度持久化（每 50 标的落盘一次）

新增 _persist_dir / _persist_task 辅助方法，写到
<data>/prewarm_status/<market>.json。原子 write+rename，写失败 warning
但不阻塞预热推进。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 7：终态写盘（finished / cancelled / error）

**Files:**
- Modify: `web/chanlun_chart/cl_app/blueprints/symbols.py`（`_run_task` 终态分支约 line 500-540 区间）

- [ ] **Step 1: 找到 _run_task 末尾设置 status / finished_at 的位置**

`Read` `_run_task` 倒数 30 行（约 line 500-540），定位三处终态赋值（finished / cancelled / error），分别在每处 `task.status = ...` 与 `task.finished_at = ...` 之后追加 `self._persist_task(task)`。

- [ ] **Step 2: 找到 `mark_batch_prewarm_active(market, False)` 之前的 finally 块**

`Read` `_run_task` 函数尾部，定位 `finally` 块。在 `finally` 块开头追加：

```python
        finally:
            # 终态强制持久化一次（保证崩溃 / 取消都能落盘最后状态）
            try:
                self._persist_task(task)
            except Exception as e:
                LogUtil.warning(f"[prewarm] final persist failed: {e}")
```

注意：要替换原 `finally:` 行的整段写法。具体编辑前用 Read 查看 line 500-560 的完整 finally 实现，仅在已有 finally 块的开头处插入持久化调用，不要重写无关逻辑。

- [ ] **Step 3: ast 语法校验**

Run:
```bash
cd D:/project/chanlun-pro && python -c "import ast; ast.parse(open('web/chanlun_chart/cl_app/blueprints/symbols.py', encoding='utf-8').read())"
```

Expected: 无输出

- [ ] **Step 4: Commit**

```bash
git add web/chanlun_chart/cl_app/blueprints/symbols.py
git commit -m "feat(symbols): PrewarmManager 终态强制写盘（finally 块）

任务结束（finished/cancelled/error）时强制 persist 一次，
保证 UI 显示与磁盘状态一致。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 8：启动恢复 + 中断检测

**Files:**
- Modify: `web/chanlun_chart/cl_app/blueprints/symbols.py`（`PrewarmManager.__init__` 末尾加 `_load_persisted_tasks` 调用 + 新增方法）

- [ ] **Step 1: 在 `__init__` 末尾追加加载调用，新增 `_load_persisted_tasks` 方法**

用 Edit 把：

```python
    def __init__(self):
        self._lock = threading.Lock()
        # market -> 最近一次任务（无论是否已完成）
        self._tasks: Dict[str, PrewarmTask] = {}
        # 是否有任务正在运行（全局互斥）
        self._global_running: bool = False
        # worker 线程引用（仅用于调试，不主动 join）
        self._worker_thread: Optional[threading.Thread] = None

    # ---------------- 持久化 ----------------
```

替换为：

```python
    def __init__(self):
        self._lock = threading.Lock()
        # market -> 最近一次任务（无论是否已完成）
        self._tasks: Dict[str, PrewarmTask] = {}
        # 是否有任务正在运行（全局互斥）
        self._global_running: bool = False
        # worker 线程引用（仅用于调试，不主动 join）
        self._worker_thread: Optional[threading.Thread] = None
        # 启动恢复：从磁盘载入历史 task；进程内首次构造时执行一次。
        self._load_persisted_tasks()

    # ---------------- 持久化 ----------------

    def _load_persisted_tasks(self) -> None:
        """启动时扫描 prewarm_status/*.json 还原 _tasks。
        - 损坏文件：warning + unlink + 跳过；
        - status 仍为 "running"：说明上次进程异常退出，改写为 "aborted" 并落盘。
        """
        d = self._persist_dir()
        if d is None:
            return
        try:
            files = list(d.glob("*.json"))
        except OSError as e:
            LogUtil.warning(f"[prewarm] list persist dir failed: {e}")
            return
        for path in files:
            try:
                raw = path.read_text(encoding="utf-8")
                data = json.loads(raw)
                task = PrewarmTask.from_dict(data)
            except (OSError, ValueError, KeyError) as e:
                LogUtil.warning(f"[prewarm] load persisted task corrupt path={path} err={e}, 删除")
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
                continue
            if task.status == "running":
                # 上次进程异常退出，改为 aborted 并立即写回
                task.status = "aborted"
                if task.finished_at is None:
                    task.finished_at = time.time()
                try:
                    path.write_text(
                        json.dumps(task.to_dict(), ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                except OSError as e:
                    LogUtil.warning(f"[prewarm] write back aborted state failed: {e}")
            self._tasks[task.market] = task
            LogUtil.info(
                f"[prewarm] restored task market={task.market} status={task.status} "
                f"{task.done}/{task.total}"
            )
```

- [ ] **Step 2: ast 语法校验**

Run:
```bash
cd D:/project/chanlun-pro && python -c "import ast; ast.parse(open('web/chanlun_chart/cl_app/blueprints/symbols.py', encoding='utf-8').read())"
```

Expected: 无输出

- [ ] **Step 3: Commit**

```bash
git add web/chanlun_chart/cl_app/blueprints/symbols.py
git commit -m "feat(symbols): PrewarmManager 启动时恢复历史 task + 中断检测

__init__ 末尾调 _load_persisted_tasks 扫描 prewarm_status/*.json
还原到 _tasks。损坏文件 unlink；status='running' 视为上次进程被杀，
改写为 'aborted' 并回写磁盘，UI 上能显示"上次中断在 X/Y"。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 9：Phase 2 verify 脚本

**Files:**
- Create: `_verify_phase2.py`

- [ ] **Step 1: 写 verify 脚本**

Write `_verify_phase2.py`：

```python
"""Phase 2 验证：from_dict / 持久化文件读写 / 启动恢复 + 中断检测（隔离环境）。"""
import json, sys, tempfile, pathlib, types, importlib.util, ast
ROOT = pathlib.Path(__file__).parent
sys.path.insert(0, str(ROOT / "src"))

# 用临时 data path 隔离测试，避免污染真实数据目录
TMP_DATA = pathlib.Path(tempfile.mkdtemp(prefix="prewarm_verify_"))

# 注入 stub config（避免拉真 config.py 引发的依赖）
stub = types.ModuleType("chanlun.config")
stub.get_data_path = lambda: TMP_DATA
sys.modules["chanlun.config"] = stub
import chanlun
chanlun.config = stub

# 直接按文件路径加载 symbols.py：跳过 cl_app.__init__（拉 pytz/scheduler 等）。
# 但 symbols.py 自己 import 了 chanlun.cl_utils 等，需要 stub 一些。
sys.modules.setdefault("chanlun.cl_utils", types.ModuleType("chanlun.cl_utils"))
sys.modules["chanlun.cl_utils"].query_cl_chart_config = lambda *a, **kw: {}
sys.modules.setdefault("chanlun.tools.log_util", types.ModuleType("chanlun.tools.log_util"))
class _Log:
    @staticmethod
    def info(*a, **kw): pass
    @staticmethod
    def warning(*a, **kw): pass
    @staticmethod
    def error(*a, **kw): pass
    @staticmethod
    def debug(*a, **kw): pass
    @staticmethod
    def exception(*a, **kw): pass
sys.modules["chanlun.tools.log_util"].LogUtil = _Log

# stub services.constants 与 .tv（symbols.py 依赖）
sys.modules.setdefault("cl_app", types.ModuleType("cl_app"))
sys.modules.setdefault("cl_app.services", types.ModuleType("cl_app.services"))
sys.modules.setdefault("cl_app.services.constants", types.ModuleType("cl_app.services.constants"))
sys.modules["cl_app.services.constants"].market_types = {"a":"a", "hk":"hk"}
sys.modules["cl_app.services.constants"].resolution_maps = {"1":"1m","5":"5m","30":"30m","1D":"d"}
sys.modules.setdefault("cl_app.blueprints", types.ModuleType("cl_app.blueprints"))
tv_stub = types.ModuleType("cl_app.blueprints.tv")
tv_stub._build_cache_key = lambda *a, **kw: ""
tv_stub._get_chart_cache_entry = lambda *a, **kw: None
tv_stub._get_last_user_request_time = lambda: 0.0
tv_stub._get_user_recent_codes = lambda m: []
tv_stub.cache_lock = __import__("threading").RLock()
tv_stub.chart_data_cache = {}
tv_stub.compute_and_cache_chart_data = lambda *a, **kw: True
tv_stub.get_cached_processed_stocks = lambda *a, **kw: []
tv_stub.mark_batch_prewarm_active = lambda *a, **kw: None
sys.modules["cl_app.blueprints.tv"] = tv_stub

# 加载 symbols.py
spec = importlib.util.spec_from_file_location(
    "_symbols_iso", ROOT / "web/chanlun_chart/cl_app/blueprints/symbols.py",
)
mod = importlib.util.module_from_spec(spec)
# Flask import 会需要 flask 装好，已经在 conda 环境中
spec.loader.exec_module(mod)

PrewarmTask = mod.PrewarmTask
PrewarmManager = mod.PrewarmManager

# 1. from_dict 往返
t1 = PrewarmTask("a", 100)
t1.done = 30
t1.succeeded = 25
t1.failed = 5
t1.status = "running"
t1.current_code = "SH.600519"
d = t1.to_dict()
t2 = PrewarmTask.from_dict(d)
assert t2.market == "a" and t2.total == 100 and t2.done == 30
assert t2.succeeded == 25 and t2.failed == 5
assert t2.status == "running" and t2.current_code == "SH.600519"
print("OK  PrewarmTask.from_dict 往返一致")

# 2. _persist_task 写盘 + 文件存在
mgr = PrewarmManager()  # __init__ 会触发 _load_persisted_tasks（首次空目录无事）
mgr._persist_task(t1)
expected_path = TMP_DATA / "prewarm_status" / "a.json"
assert expected_path.exists(), f"persist 文件未创建: {expected_path}"
with open(expected_path, encoding="utf-8") as fp:
    saved = json.load(fp)
assert saved["market"] == "a" and saved["done"] == 30
print("OK  _persist_task 原子写盘 + 内容正确")

# 3. 启动恢复 + 中断检测：把 status 改为 "running"，新建一个 manager 应自动改 aborted
saved["status"] = "running"
expected_path.write_text(json.dumps(saved), encoding="utf-8")
mgr2 = PrewarmManager()
restored = mgr2._tasks.get("a")
assert restored is not None, "未恢复 task"
assert restored.status == "aborted", f"中断检测失败：status={restored.status}"
# 验证写回了磁盘
with open(expected_path, encoding="utf-8") as fp:
    again = json.load(fp)
assert again["status"] == "aborted"
print("OK  启动恢复 + 中断检测 (running→aborted) 正常")

# 4. 损坏文件自愈
expected_path.write_text("{not valid json", encoding="utf-8")
mgr3 = PrewarmManager()
assert mgr3._tasks.get("a") is None, "损坏文件不应恢复出 task"
assert not expected_path.exists(), "损坏文件应被删除"
print("OK  json 损坏文件自愈（unlink + 跳过）")

print("\nPhase 2 ALL PASS")
```

- [ ] **Step 2: 跑 verify 脚本**

Run:
```bash
C:/Users/lc/miniconda3/envs/chanlun-pro/python.exe D:/project/chanlun-pro/_verify_phase2.py
```

Expected: 末尾 `Phase 2 ALL PASS`，所有 OK 行

- [ ] **Step 3: 删除验证脚本**

Run:
```bash
rm D:/project/chanlun-pro/_verify_phase2.py
```

Expected: 文件被删除

---

## Phase 3：手工烟雾测试 + 收尾

### Task 10：启动服务跑端到端冒烟

**Files:** 无修改

- [ ] **Step 1: 重启 Web 服务**

在你的 PyCharm 或 Anaconda Prompt 跑：

```bash
C:/Users/lc/miniconda3/envs/chanlun-pro/python.exe D:/project/chanlun-pro/web/chanlun_chart/app.py nobrowser
```

观察日志：
- 启动成功
- 若 `<data>/prewarm_status/` 已有历史文件，应看到 `[prewarm] restored task market=...` 日志

- [ ] **Step 2: 验证标的列表过滤**

浏览器打开 `http://127.0.0.1:9900/symbols?market=a`，肉眼检查：
- 列表里应**只见个股**（如 `SH.600519 贵州茅台`），不见指数（`SH.000001 上证指数`）和 ETF（`SH.510300 沪深300ETF`）
- 切到 `market=hk` 或 `market=us` 应看到完整列表（不受过滤影响）

- [ ] **Step 3: 验证预热持久化**

浏览器打开 `http://127.0.0.1:9900/symbols?market=a`，点击"预热"按钮（或手工 POST `/symbols/prewarm`）。等到进度 > 50（看 LogUtil.info 日志 `[prewarm] progress ... 50/...`）。

检查 `<data>/prewarm_status/a.json` 已生成且 `done >= 50`。

- [ ] **Step 4: 验证中断恢复**

杀掉服务进程（Ctrl+C 或任务管理器），重启服务。

调 `http://127.0.0.1:9900/symbols/prewarm/status?market=a`，响应应有：
```json
{"ok": true, "task": {"market": "a", "status": "aborted", "done": 50, ...}}
```

- [ ] **Step 5: 验证再次预热跳过已 done**

再次启动预热（POST `/symbols/prewarm`）。观察日志：
- 已 done 的标的应有 `[prewarm] cache hit ...`（debug 级，需打开 debug 日志才能看到；info 级仅会看到推进非常快）
- 总耗时显著小于第一次

- [ ] **Step 6: 总结性 commit（无代码改动则跳过）**

如果烟雾测试中发现需调整的细节（如文案、日志级别），修后 commit；否则跳过。

---

## Self-Review

按照 writing-plans 技能要求，对计划做一次 fresh-eyes review：

**1. Spec 覆盖：**
- ✅ 需求 1 数据层 qmt → Task 1
- ✅ 需求 1 数据层 baostock → Task 2
- ✅ 需求 1 API 层过滤 → Task 3
- ✅ 需求 2 已 done 跳过 → 已存在于 `_prewarm_one_code` line 640-642，无需做
- ✅ 需求 2 from_dict → Task 5
- ✅ 需求 2 写盘频率 → Task 6
- ✅ 需求 2 终态写盘 → Task 7
- ✅ 需求 2 启动恢复 + 中断检测 → Task 8
- ✅ 损坏文件自愈 → Task 8 (`_load_persisted_tasks` try/except + unlink)

**2. Placeholder scan：**
- 无 TBD/TODO/"实现后" 类占位
- Task 7 Step 2 描述较抽象（"在 finally 块开头追加"），但已要求执行者先 Read 看清结构再做精准 Edit；这是因为该位置在原文件中位置可能因 Phase 2 Task 6 改动而漂移。

**3. Type 一致性：**
- `_PREWARM_PERSIST_DIRNAME` / `_PREWARM_PERSIST_EVERY_N_DONE` 在 Task 6 定义，Task 6 / 7 / 8 都使用，命名一致
- `_persist_dir` / `_persist_task` / `_load_persisted_tasks` 命名风格统一
- `from_dict` / `to_dict` 配对

**4. 风险点：**
- Task 6 / 7 / 8 都修同一个 `symbols.py` 文件，分别 commit。按顺序执行不会冲突。
- `_load_persisted_tasks` 在 `__init__` 中无锁调用，因为此时 manager 还未对外暴露，无并发；安全。
- `_persist_task` 在 `_process_one` 内被多 worker 并发调用：用同一个临时文件名 `<market>.json.tmp` 会竞态。改进意见：把 tmp 文件改用 uuid 后缀。

→ 修一下 Task 6 Step 2 的 `_persist_task`，把 tmp 名加 uuid。

</parameter>
