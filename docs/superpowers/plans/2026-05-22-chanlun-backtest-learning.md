# 缠论回测学习页面 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在现有 Flask web 应用中新增 `/backtest` 页面，实现历史 K 线回放、双周期缠论图表展示和模拟交易功能。

**Architecture:** 后端在 `__init__.py` 中新增 6 个路由，用全局 dict 管理回放 session。前端使用两个 TradingView 图表（自定义 datafeed）+ 右侧操作面板。回放时通过 `setInterval` 定时调用 `/backtest/step`，用 `onRealtimeCallback` 推送新 K 线，$.ajax 获取缠论数据后重新绘制。

**Tech Stack:** Flask (Python), TradingView Charting Library (JS), jQuery, layui CSS

---

### Task 1: 添加后端回放路由

**Files:**
- Modify: `web/chanlun_chart/cl_app/__init__.py`

- [ ] **Step 1: 在 `create_app()` 函数开头的 import 区域添加新 import**

在文件顶部现有的 `from chanlun import config, fun` 之后添加：

```python
import random
import uuid
from chanlun.exchange.exchange import convert_stock_kline_frequency
from chanlun import cl
```

- [ ] **Step 2: 在 `create_app()` 内、`frequency_maps` 定义之后添加回放 session 管理 dict**

在 `resolution_maps = dict(zip(frequency_maps.values(), frequency_maps.keys()))` 之后添加：

```python
# 回测回放 session 存储
_replay_sessions: dict = {}
```

- [ ] **Step 3: 添加 `GET /backtest` 页面路由**

在 `index_show()` 路由之后添加：

```python
@app.route("/backtest")
@login_required
def backtest_page():
    """回测学习页面"""
    return render_template("backtest.html")
```

- [ ] **Step 4: 添加 `GET /backtest/tv/config` 路由**

```python
@app.route("/backtest/tv/config")
@login_required
def backtest_tv_config():
    """回测 TV 图表配置"""
    frequencys = list(
        set(market_frequencys["a"])
        | set(market_frequencys["hk"])
        | set(market_frequencys["us"])
        | set(market_frequencys["futures"])
        | set(market_frequencys["currency"])
    )
    supportedResolutions = [v for k, v in frequency_maps.items() if k in frequencys]
    return {
        "supports_search": False,
        "supports_group_request": False,
        "supported_resolutions": supportedResolutions,
        "supports_marks": False,
        "supports_timescale_marks": False,
        "supports_time": False,
        "exchanges": [
            {"value": "backtest", "name": "回测", "desc": "回测学习"},
        ],
    }
```

- [ ] **Step 5: 添加 `GET /backtest/tv/symbols` 路由**

（TV 图表初始化时需要解析 symbol）

```python
@app.route("/backtest/tv/symbols")
@login_required
def backtest_tv_symbols():
    """回测 symbol 解析"""
    symbol = request.args.get("symbol")
    freq_key = symbol  # symbol 就是 'small' 或 'high'
    session_id = session.get("bt_session_id")
    rs = _replay_sessions.get(session_id)
    if rs is None:
        return {"name": symbol, "ticker": symbol, "description": symbol,
                "exchange": "backtest", "type": "stock", "session": "24x7",
                "timezone": "Asia/Shanghai", "pricescale": 100,
                "minmov": 1, "minmov2": 0, "has_intraday": True,
                "has_daily": True, "has_weekly_and_monthly": True,
                "supported_resolutions": ["1", "5", "15", "30", "60", "1D"],
                "intraday_multipliers": ["1", "5", "15", "30", "60"],
                "seconds_multipliers": [],
                "daily_multipliers": ["1"],
                "visible_plots_set": "ohlcv"}
    freq = rs["small_freq"] if symbol == "small" else rs["high_freq"]
    return {"name": symbol, "ticker": symbol, "description": symbol,
            "exchange": "backtest", "type": "stock", "session": "24x7",
            "timezone": "Asia/Shanghai", "pricescale": 100,
            "minmov": 1, "minmov2": 0, "has_intraday": True,
            "has_daily": True, "has_weekly_and_monthly": True,
            "supported_resolutions": [frequency_maps.get(freq, "1D")],
            "intraday_multipliers": ["1", "5", "15", "30", "60"],
            "seconds_multipliers": [],
            "daily_multipliers": ["1"],
            "visible_plots_set": "ohlcv"}
```

- [ ] **Step 6: 添加 `GET /backtest/tv/history` 路由**

```python
@app.route("/backtest/tv/history")
@login_required
def backtest_tv_history():
    """回测 TV 历史数据（仅返回到 current_pos）"""
    symbol = request.args.get("symbol")
    _from = request.args.get("from")
    _to = request.args.get("to")
    resolution = request.args.get("resolution")
    firstDataRequest = request.args.get("firstDataRequest", "true")

    session_id = session.get("bt_session_id")
    rs = _replay_sessions.get(session_id)
    if rs is None:
        return {"s": "no_data"}

    if symbol == "small":
        cd = rs["cl_small"]
    else:
        cd = rs["cl_high"]

    cl_chart_data = cl_data_to_tv_chart(cd, rs["cl_config"])
    if cl_chart_data is None:
        return {"s": "no_data"}

    # 只返回到 current_pos 的数据
    current_pos = rs["current_pos"]
    small_klines = rs["all_klines"]
    if current_pos < len(small_klines):
        cutoff_time = fun.datetime_to_int(small_klines.iloc[current_pos]["date"])
    else:
        cutoff_time = cl_chart_data["t"][-1] if cl_chart_data["t"] else 0

    # 截取所有数据到 current_pos
    def _slice(data_list, times, cutoff):
        """根据时间截取数据"""
        if not data_list:
            return data_list
        if not times:
            return data_list
        result = []
        for item in data_list:
            pts = item.get("points")
            if pts:
                if isinstance(pts, dict):
                    if pts.get("time", 0) <= cutoff:
                        result.append(item)
                elif isinstance(pts, list) and len(pts) > 0:
                    if pts[0].get("time", 0) <= cutoff:
                        result.append(item)
            else:
                result.append(item)
        return result

    t = [v for v in cl_chart_data["t"] if v <= cutoff_time]
    idx = len(t)
    c = cl_chart_data["c"][:idx]
    o = cl_chart_data["o"][:idx]
    h = cl_chart_data["h"][:idx]
    l = cl_chart_data["l"][:idx]
    v = cl_chart_data["v"][:idx]

    fxs = _slice(cl_chart_data["fxs"], cl_chart_data["t"], cutoff_time)
    bis = _slice(cl_chart_data["bis"], cl_chart_data["t"], cutoff_time)
    xds = _slice(cl_chart_data["xds"], cl_chart_data["t"], cutoff_time)
    zsds = _slice(cl_chart_data["zsds"], cl_chart_data["t"], cutoff_time)
    bi_zss = _slice(cl_chart_data["bi_zss"], cl_chart_data["t"], cutoff_time)
    xd_zss = _slice(cl_chart_data["xd_zss"], cl_chart_data["t"], cutoff_time)
    zsd_zss = _slice(cl_chart_data["zsd_zss"], cl_chart_data["t"], cutoff_time)
    bcs = _slice(cl_chart_data["bcs"], cl_chart_data["t"], cutoff_time)
    mmds = _slice(cl_chart_data["mmds"], cl_chart_data["t"], cutoff_time)

    if firstDataRequest == "false":
        t = t[-10:]
        c = c[-10:]
        o = o[-10:]
        h = h[-10:]
        l = l[-10:]
        v = v[-10:]
        fxs = fxs[-5:]
        bis = bis[-5:]
        xds = xds[-5:]
        zsds = zsds[-5:]
        bi_zss = bi_zss[-5:]
        xd_zss = xd_zss[-5:]
        zsd_zss = zsd_zss[-5:]
        bcs = bcs[-5:]
        mmds = mmds[-5:]

    return {
        "s": "ok",
        "t": t, "c": c, "o": o, "h": h, "l": l, "v": v,
        "fxs": fxs, "bis": bis, "xds": xds, "zsds": zsds,
        "bi_zss": bi_zss, "xd_zss": xd_zss, "zsd_zss": zsd_zss,
        "bcs": bcs, "mmds": mmds,
    }
```

- [ ] **Step 7: 添加 `POST /backtest/start` 路由**

```python
@app.route("/backtest/start", methods=["POST"])
@login_required
def backtest_start():
    """随机选股，初始计算，开始回放"""
    from chanlun.exchange import get_exchange
    from chanlun.base import Market

    ex = get_exchange(Market.A)
    all_stocks = ex.all_stocks()

    # 过滤：只要 60/00/30 开头的股票
    valid_stocks = [s for s in all_stocks
                    if s["code"].split(".")[-1][:2] in ["60", "00", "30"]]

    # 随机周期对
    freq_pairs = [("d", "30m"), ("60m", "15m"), ("30m", "5m")]

    max_attempts = 50
    for _ in range(max_attempts):
        stock = random.choice(valid_stocks)
        high_freq, small_freq = random.choice(freq_pairs)
        klines = ex.klines(stock["code"], small_freq)
        if klines is not None and len(klines) >= 4000:
            break
    else:
        return {"ok": False, "msg": "无法找到满足条件的股票，请重试"}

    # 从倒数 1000 根开始回放
    start_pos = len(klines) - 1000

    cl_config = query_cl_chart_config("a", stock["code"])

    # 初始计算小级别
    cd_small = cl.CL(stock["code"], small_freq, cl_config)
    cd_small.process_klines(klines.iloc[:start_pos + 1])

    # 初始计算大级别
    high_klines = convert_stock_kline_frequency(
        klines.iloc[:start_pos + 1].copy(), high_freq
    )
    cd_high = cl.CL(stock["code"], high_freq, cl_config)
    cd_high.process_klines(high_klines)

    # 生成 session
    session_id = str(uuid.uuid4())
    session["bt_session_id"] = session_id

    _replay_sessions[session_id] = {
        "code": stock["code"],
        "name": stock["name"],
        "display_id": f"股票 #{random.randint(1, 9999)}",
        "small_freq": small_freq,
        "high_freq": high_freq,
        "all_klines": klines,
        "current_pos": start_pos,
        "start_pos": start_pos,
        "cl_small": cd_small,
        "cl_high": cd_high,
        "cl_config": cl_config,
        "running": True,
    }

    # 返回初始价格（当前 K 线收盘价）
    current_bar = klines.iloc[start_pos]
    return {
        "ok": True,
        "display_id": _replay_sessions[session_id]["display_id"],
        "small_freq": small_freq,
        "high_freq": high_freq,
        "current_price": float(current_bar["close"]),
        "current_time": fun.datetime_to_str(current_bar["date"]),
    }
```

- [ ] **Step 8: 添加 `POST /backtest/step` 路由**

```python
@app.route("/backtest/step", methods=["POST"])
@login_required
def backtest_step():
    """回放前进一步，返回新的 K 线和缠论数据"""
    session_id = session.get("bt_session_id")
    rs = _replay_sessions.get(session_id)
    if rs is None or not rs["running"]:
        return {"ok": False, "msg": "回放未开始或已结束"}

    rs["current_pos"] += 1
    pos = rs["current_pos"]
    klines = rs["all_klines"]

    if pos >= len(klines):
        rs["running"] = False
        return {"ok": False, "msg": "回放已结束", "finished": True}

    # 增量更新小级别 CL
    rs["cl_small"].process_klines(klines.iloc[:pos + 1])

    # 增量更新大级别 CL
    high_klines = convert_stock_kline_frequency(
        klines.iloc[:pos + 1].copy(), rs["high_freq"]
    )
    rs["cl_high"].process_klines(high_klines)

    # 生成 TV 数据
    cl_small_data = cl_data_to_tv_chart(rs["cl_small"], rs["cl_config"])
    cl_high_data = cl_data_to_tv_chart(rs["cl_high"], rs["cl_config"])

    current_bar = klines.iloc[pos]
    new_bar = {
        "time": fun.datetime_to_int(current_bar["date"]),
        "close": float(current_bar["close"]),
        "open": float(current_bar["open"]),
        "high": float(current_bar["high"]),
        "low": float(current_bar["low"]),
        "volume": float(current_bar["volume"]),
    }

    # 检查大级别是否有新 bar
    new_high_bar = None
    if len(cl_high_data["t"]) > 0:
        last_high_time = cl_high_data["t"][-1]
        # 如果最后一个 bar 时间戳 > 之前返回的
        if len(cl_high_data["t"]) >= 2:
            new_high_idx = len(cl_high_data["t"]) - 1
            prev_high_idx = new_high_idx - 1
            prev_high_time = cl_high_data["t"][prev_high_idx]
            if last_high_time != prev_high_time:
                new_high_bar = {
                    "time": last_high_time,
                    "close": cl_high_data["c"][-1],
                    "open": cl_high_data["o"][-1],
                    "high": cl_high_data["h"][-1],
                    "low": cl_high_data["l"][-1],
                    "volume": cl_high_data["v"][-1],
                }
        elif len(cl_high_data["t"]) == 1:
            new_high_bar = {
                "time": last_high_time,
                "close": cl_high_data["c"][-1],
                "open": cl_high_data["o"][-1],
                "high": cl_high_data["h"][-1],
                "low": cl_high_data["l"][-1],
                "volume": cl_high_data["v"][-1],
            }

    return {
        "ok": True,
        "current_pos": pos,
        "total_bars": len(klines),
        "new_bar": new_bar,
        "new_high_bar": new_high_bar,
        "cl_small": cl_small_data,
        "cl_high": cl_high_data,
        "current_price": float(current_bar["close"]),
    }
```

- [ ] **Step 9: 添加 `POST /backtest/stop` 路由**

```python
@app.route("/backtest/stop", methods=["POST"])
@login_required
def backtest_stop():
    """停止回放并清理 session"""
    session_id = session.get("bt_session_id")
    if session_id and session_id in _replay_sessions:
        del _replay_sessions[session_id]
    session.pop("bt_session_id", None)
    return {"ok": True}
```

- [ ] **Step 10: 在 `create_app()` 顶部添加 Flask session 支持**

在文件顶部 import 区域确认已有 `from flask import Flask, redirect, render_template, request, send_file, session`（添加 `session`）。

- [ ] **Step 11: 提交**

```bash
git add web/chanlun_chart/cl_app/__init__.py
git commit -m "feat: add backtest replay routes for chanlun learning page"
```

---

### Task 2: 创建 HTML 模板

**Files:**
- Create: `web/chanlun_chart/cl_app/templates/backtest.html`

- [ ] **Step 1: 创建 `backtest.html`**

```html
<!doctype html>
<html lang="zh">
<head>
  <title>缠论回测学习</title>
  <link rel="shortcut icon" href="{{ url_for('static', filename='favicon.ico') }}" />
  <link rel="stylesheet" href="{{ url_for('static', filename='css/layui.css') }}" />
  <link rel="stylesheet" href="{{ url_for('static', filename='css/backtest.css') }}" />
  {% include 'dark.html' %}
</head>
<body>
  <div class="bt-container">
    <!-- 左侧图表区 -->
    <div class="bt-charts" id="bt-charts">
      <div class="bt-chart-small" id="bt-chart-small">
        <div id="tv_chart_small" style="width:100%;height:100%;"></div>
      </div>
      <div class="bt-chart-high" id="bt-chart-high">
        <div id="tv_chart_high" style="width:100%;height:100%;"></div>
      </div>
    </div>
    <!-- 右侧操作区 -->
    <div class="bt-panel" id="bt-panel">
      <div class="bt-panel-section">
        <h3>操作控制</h3>
        <div class="bt-btn-group">
          <button class="layui-btn layui-btn-normal" id="bt-btn-start">开始</button>
          <button class="layui-btn" id="bt-btn-pause">暂停</button>
          <button class="layui-btn layui-btn-warm" id="bt-btn-stop">结束</button>
        </div>
        <div class="bt-speed">
          <label>回放速度: <span id="bt-speed-label">2s</span></label>
          <input type="range" id="bt-speed-slider" min="1" max="20" value="4" step="1" />
          <div class="bt-speed-range">
            <span>0.5s</span><span>10s</span>
          </div>
        </div>
      </div>
      <hr>
      <div class="bt-panel-section">
        <h3>股票信息</h3>
        <div>股票: <span id="bt-stock-id">--</span></div>
        <div>周期: <span id="bt-freqs">--</span></div>
        <div>进度: <span id="bt-progress">--</span></div>
        <div>当前时间: <span id="bt-current-time">--</span></div>
      </div>
      <hr>
      <div class="bt-panel-section">
        <h3>资金状况</h3>
        <div>初始资金: <span id="bt-init-capital">¥100,000</span></div>
        <div>当前资金: <span id="bt-current-capital">¥100,000</span></div>
        <div>持仓市值: <span id="bt-position-value">¥0</span></div>
        <div>浮动盈亏: <span id="bt-pnl">0.00%</span></div>
      </div>
      <hr>
      <div class="bt-panel-section">
        <h3>当前持仓</h3>
        <div id="bt-position-display">无持仓</div>
      </div>
      <hr>
      <div class="bt-panel-section">
        <h3>交易操作</h3>
        <div>当前价格: <span id="bt-current-price">--</span></div>
        <div class="bt-trade-form">
          <label>数量(股): <input type="number" id="bt-trade-qty" value="100" min="100" step="100" /></label>
          <div class="bt-trade-btns">
            <button class="layui-btn layui-btn-danger" id="bt-btn-buy">买入（做多）</button>
            <button class="layui-btn layui-btn-danger" id="bt-btn-sell">卖出（做空）</button>
            <button class="layui-btn" id="bt-btn-close">平仓</button>
          </div>
        </div>
      </div>
      <hr>
      <div class="bt-panel-section">
        <h3>交易记录</h3>
        <table class="layui-table" id="bt-trade-records" style="font-size:12px;">
          <thead><tr><th>时间</th><th>方向</th><th>价格</th><th>数量</th><th>盈亏</th></tr></thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
  </div>

  <script src="{{ url_for('static', filename='jquery-3.7.0.min.js') }}"></script>
  <script src="{{ url_for('static', filename='layui.js') }}"></script>
  <script src="{{ url_for('static', filename='charting_library/charting_library.standalone.js') }}?version=1.0.0"></script>
  <script src="{{ url_for('static', filename='js/backtest.js') }}"></script>
</body>
</html>
```

- [ ] **Step 2: 提交**

```bash
git add web/chanlun_chart/cl_app/templates/backtest.html
git commit -m "feat: add backtest page HTML template"
```

---

### Task 3: 创建 CSS 样式

**Files:**
- Create: `web/chanlun_chart/cl_app/static/css/backtest.css`

- [ ] **Step 1: 创建 `backtest.css`**

```css
* { margin: 0; padding: 0; box-sizing: border-box; }

.bt-container {
  display: flex;
  height: 100vh;
  overflow: hidden;
}

.bt-charts {
  flex: 7;
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.bt-chart-small {
  flex: 7;
  position: relative;
  min-height: 0;
}

.bt-chart-high {
  flex: 3;
  position: relative;
  min-height: 0;
  border-top: 2px solid #e6e6e6;
}

.bt-panel {
  flex: 3;
  min-width: 280px;
  max-width: 380px;
  overflow-y: auto;
  padding: 12px;
  background: #f9f9f9;
  border-left: 1px solid #e6e6e6;
}

.bt-panel-section {
  margin-bottom: 8px;
}

.bt-panel-section h3 {
  font-size: 14px;
  margin-bottom: 6px;
}

.bt-btn-group { margin-bottom: 8px; }
.bt-btn-group button { margin-right: 4px; }

.bt-speed { margin: 10px 0; }
.bt-speed label { display: block; margin-bottom: 4px; font-size: 13px; }
.bt-speed input[type="range"] { width: 100%; }
.bt-speed-range { display: flex; justify-content: space-between; font-size: 11px; color: #999; }

.bt-trade-form { margin-top: 6px; }
.bt-trade-form label { display: block; margin-bottom: 6px; font-size: 13px; }
.bt-trade-form input { width: 100px; padding: 2px 4px; }
.bt-trade-btns { margin-top: 4px; }
.bt-trade-btns button { margin-right: 4px; margin-bottom: 4px; }

#bt-pnl.positive { color: #e74c3c; }
#bt-pnl.negative { color: #2ecc71; }

#bt-trade-records { width: 100%; }
#bt-trade-records td { padding: 2px 4px; }

/* dark theme support */
.dark .bt-panel { background: #1a1a1a; border-left-color: #333; color: #ccc; }
.dark .bt-chart-high { border-top-color: #333; }
.dark .bt-panel hr { border-color: #333; }
.dark .layui-table { background: #1a1a1a; color: #ccc; }
```

- [ ] **Step 2: 提交**

```bash
git add web/chanlun_chart/cl_app/static/css/backtest.css
git commit -m "feat: add backtest page CSS styles"
```

---

### Task 4: 创建前端 JS - ChartUtils 和 BacktestApp

**Files:**
- Create: `web/chanlun_chart/cl_app/static/js/backtest.js`

- [ ] **Step 1: 写入常量定义和 ChartUtils**

```javascript
// === 常量 ===
const BT_CONFIG = {
  INITIAL_CAPITAL: 100000,
  COLORS: {
    DING: "#FA8072",
    DI: "#1E90FF",
    BI: "#708090",
    XD: "#00BFFF",
    ZSD: "#FFA710",
    BI_ZSS: "#708090",
    XD_ZSS: "#00BFFF",
    ZSD_ZSS: "#FFA710",
    BCS: "#D1D4DC",
    BC_TEXT: "#fccbcd",
    MMD_UP: "#FA8072",
    MMD_DOWN: "#1E90FF",
  },
};

// === ChartUtils（简化版，仅绘制，无交互） ===
const BTChartUtils = {
  createShape(chart, points, options = {}) {
    const defaults = {
      lock: true, disableSelection: true, disableSave: true,
      disableUndo: true, showInObjectsTree: false, overrides: {},
    };
    const config = { ...defaults, ...options };
    return config.shape === "trend_line" || config.shape === "rectangle"
      ? chart.createMultipointShape(points, config)
      : chart.createShape(points, config);
  },

  createFxShape(chart, fx) {
    const color = fx.text === "ding" ? BT_CONFIG.COLORS.DING : BT_CONFIG.COLORS.DI;
    return this.createShape(chart, fx.points, {
      shape: "circle",
      overrides: { backgroundColor: color, color: color, linewidth: 4 },
    });
  },

  createLineShape(chart, line, color) {
    return this.createShape(chart, line.points, {
      shape: "trend_line",
      overrides: {
        linestyle: parseInt(line.linestyle) || 0,
        linewidth: 1, linecolor: color,
      },
    });
  },

  createZhongshuShape(chart, zs, color) {
    return this.createShape(chart, zs.points, {
      shape: "rectangle",
      overrides: {
        linestyle: parseInt(zs.linestyle) || 0,
        linewidth: 1, linecolor: color,
        backgroundColor: color, transparency: 95,
        fillBackground: true, filled: true,
      },
    });
  },

  createMmdShape(chart, mmd) {
    const isBuy = mmd.text.includes("B");
    const color = isBuy ? BT_CONFIG.COLORS.MMD_UP : BT_CONFIG.COLORS.MMD_DOWN;
    const shape = isBuy ? "arrow_up" : "arrow_down";
    return this.createShape(chart, mmd.points, {
      shape, text: mmd.text,
      overrides: {
        markerColor: color, backgroundColor: color,
        color: color, fontsize: 12, transparency: 80,
      },
    });
  },

  createBcShape(chart, bc) {
    return this.createShape(chart, bc.points, {
      shape: "balloon", text: bc.text,
      overrides: {
        markerColor: BT_CONFIG.COLORS.BCS,
        backgroundColor: BT_CONFIG.COLORS.BCS,
        textColor: BT_CONFIG.COLORS.BC_TEXT,
        transparency: 70, fontsize: 12,
      },
    });
  },
};
```

- [ ] **Step 2: 写入自定义 Datafeed**

```javascript
// === 自定义 Datafeed ===
function createBacktestDatafeed(chartKey) {
  return {
    _realtimeCallback: null,
    _bars: {},

    onReady(callback) {
      $.getJSON("/backtest/tv/config", callback);
    },

    searchSymbols(userInput, exchange, symbolType, onResult) {
      onResult([]);
    },

    resolveSymbol(symbolName, onResult, onError) {
      $.getJSON("/backtest/tv/symbols", { symbol: symbolName }, function (data) {
        onResult(data);
      }).fail(onError);
    },

    getBars(symbolInfo, resolution, from, to, onResult, onError, firstDataRequest) {
      const self = this;
      $.getJSON("/backtest/tv/history", {
        symbol: symbolInfo.name,
        resolution: resolution,
        from: from,
        to: to,
        firstDataRequest: firstDataRequest ? "true" : "false",
      }, function (data) {
        if (data.s === "ok") {
          const bars = [];
          for (let i = 0; i < data.t.length; i++) {
            bars.push({
              time: data.t[i] * 1000,
              close: data.c[i],
              open: data.o[i],
              high: data.h[i],
              low: data.l[i],
              volume: data.v[i],
            });
          }
          // 存储缠论数据以便重绘
          self._bars = data;
          onResult(bars, { noData: bars.length === 0 });
        } else {
          onResult([], { noData: true });
        }
      }).fail(function () { onResult([], { noData: true }); });
    },

    subscribeBars(symbolInfo, resolution, onRealtimeCallback, subscriberUID) {
      this._realtimeCallback = onRealtimeCallback;
    },

    unsubscribeBars(subscriberUID) {
      this._realtimeCallback = null;
    },

    pushBar(bar) {
      if (this._realtimeCallback) {
        const tvBar = {
          time: bar.time * 1000,
          close: bar.close,
          open: bar.open,
          high: bar.high,
          low: bar.low,
          volume: bar.volume,
        };
        this._realtimeCallback(tvBar);
      }
    },
  };
}
```

- [ ] **Step 3: 写入 BacktestApp 框架**

```javascript
// === BacktestApp ===
const BacktestApp = {
  widgetSmall: null,
  widgetHigh: null,
  chartSmall: null,
  chartHigh: null,
  datafeedSmall: null,
  datafeedHigh: null,
  timerId: null,
  speedMs: 2000,
  running: false,
  paused: false,

  // 交易状态
  capital: BT_CONFIG.INITIAL_CAPITAL,
  position: { type: null, qty: 0, price: 0 },  // type: 'long'|'short'|null
  tradeRecords: [],
  drawnShapeIds: { small: [], high: [] },

  init() {
    const winHeight = window.innerHeight;
    const smallHeight = winHeight * 0.7;
    const highHeight = winHeight * 0.3;

    this.datafeedSmall = createBacktestDatafeed("small");
    this.datafeedHigh = createBacktestDatafeed("high");

    // 创建小级别图表
    this.widgetSmall = new TradingView.widget({
      debug: false,
      autosize: true,
      fullscreen: false,
      container: "tv_chart_small",
      symbol: "small",
      interval: "1D",
      datafeed: this.datafeedSmall,
      library_path: "static/charting_library/",
      theme: "Light",
      timezone: "Asia/Shanghai",
      locale: "zh",
      disabled_features: ["go_to_date", "header_symbol_search", "header_compare",
        "display_market_status", "symbol_info", "volume_force_overlay"],
      enabled_features: [],
      time_frames: [],
      charts_storage_url: "/backtest/tv",
      charts_storage_api_version: "1.1",
      client_id: "bt_small",
    });

    // 创建大级别图表
    this.widgetHigh = new TradingView.widget({
      debug: false,
      autosize: true,
      fullscreen: false,
      container: "tv_chart_high",
      symbol: "high",
      interval: "1D",
      datafeed: this.datafeedHigh,
      library_path: "static/charting_library/",
      theme: "Light",
      timezone: "Asia/Shanghai",
      locale: "zh",
      disabled_features: ["go_to_date", "header_symbol_search", "header_compare",
        "display_market_status", "symbol_info", "volume_force_overlay",
        "header_widget_dom_node", "header_interval_dialog_button"],
      enabled_features: [],
      time_frames: [],
      charts_storage_url: "/backtest/tv",
      charts_storage_api_version: "1.1",
      client_id: "bt_high",
    });

    const self = this;
    this.widgetSmall.onChartReady(() => {
      self.chartSmall = self.widgetSmall.activeChart();
      self.chartSmall.onDataLoaded().subscribe(null, () => self.redrawShapes("small"));
    });
    this.widgetHigh.onChartReady(() => {
      self.chartHigh = self.widgetHigh.activeChart();
      self.chartHigh.onDataLoaded().subscribe(null, () => self.redrawShapes("high"));
    });

    this.bindEvents();
  },

  redrawShapes(chartKey) {
    const datafeed = chartKey === "small" ? this.datafeedSmall : this.datafeedHigh;
    const chart = chartKey === "small" ? this.chartSmall : this.chartHigh;
    const bars = datafeed._bars;
    if (!bars || !chart) return;

    // 清除旧形状
    this.drawnShapeIds[chartKey].forEach(id => {
      try { id.then(i => chart.removeEntity(i)); } catch (e) {}
    });
    this.drawnShapeIds[chartKey] = [];

    const ids = this.drawnShapeIds[chartKey];

    // 绘制分型
    (bars.fxs || []).forEach(fx => {
      ids.push({ id: BTChartUtils.createFxShape(chart, fx) });
    });
    // 绘制笔
    (bars.bis || []).forEach(bi => {
      ids.push({ id: BTChartUtils.createLineShape(chart, bi, BT_CONFIG.COLORS.BI) });
    });
    // 绘制线段
    (bars.xds || []).forEach(xd => {
      ids.push({ id: BTChartUtils.createLineShape(chart, xd, BT_CONFIG.COLORS.XD) });
    });
    // 绘制走势段
    (bars.zsds || []).forEach(zsd => {
      ids.push({ id: BTChartUtils.createLineShape(chart, zsd, BT_CONFIG.COLORS.ZSD) });
    });
    // 绘制笔中枢
    (bars.bi_zss || []).forEach(zs => {
      ids.push({ id: BTChartUtils.createZhongshuShape(chart, zs, BT_CONFIG.COLORS.BI_ZSS) });
    });
    // 绘制线段中枢
    (bars.xd_zss || []).forEach(zs => {
      ids.push({ id: BTChartUtils.createZhongshuShape(chart, zs, BT_CONFIG.COLORS.XD_ZSS) });
    });
    // 绘制走势段中枢
    (bars.zsd_zss || []).forEach(zs => {
      ids.push({ id: BTChartUtils.createZhongshuShape(chart, zs, BT_CONFIG.COLORS.ZSD_ZSS) });
    });
    // 绘制背驰
    (bars.bcs || []).forEach(bc => {
      ids.push({ id: BTChartUtils.createBcShape(chart, bc) });
    });
    // 绘制买卖点
    (bars.mmds || []).forEach(mmd => {
      ids.push({ id: BTChartUtils.createMmdShape(chart, mmd) });
    });
  },
```

- [ ] **Step 4: 写入按钮事件绑定**

```javascript
  bindEvents() {
    const self = this;

    $("#bt-btn-start").click(() => self.startReplay());
    $("#bt-btn-pause").click(() => self.togglePause());
    $("#bt-btn-stop").click(() => self.stopReplay());
    $("#bt-btn-buy").click(() => self.trade("buy"));
    $("#bt-btn-sell").click(() => self.trade("sell"));
    $("#bt-btn-close").click(() => self.trade("close"));

    $("#bt-speed-slider").on("input", function () {
      const val = parseInt($(this).val());
      self.speedMs = val * 500;  // 1-20 映射到 0.5s-10s
      $("#bt-speed-label").text((self.speedMs / 1000).toFixed(1) + "s");
    });
  },

  startReplay() {
    const self = this;
    $.post("/backtest/start", function (res) {
      if (!res.ok) { layer.msg(res.msg); return; }

      // 更新 UI
      $("#bt-stock-id").text(res.display_id);
      $("#bt-freqs").text(res.small_freq + " / " + res.high_freq);
      $("#bt-current-price").text("¥" + res.current_price.toFixed(2));
      $("#bt-current-time").text(res.current_time);
      $("#bt-btn-start").addClass("layui-btn-disabled").attr("disabled", true);

      // 重置交易状态
      self.capital = BT_CONFIG.INITIAL_CAPITAL;
      self.position = { type: null, qty: 0, price: 0 };
      self.tradeRecords = [];
      self.updateCapitalDisplay();
      self.updatePositionDisplay();
      $("#bt-trade-records tbody").empty();

      // 重新加载图表数据
      self.widgetSmall.activeChart().resetData();
      self.widgetHigh.activeChart().resetData();

      self.running = true;

      // 等待 1 秒让图表初始化完成后开始回放
      setTimeout(() => self.startTimer(), 1000);
    });
  },

  startTimer() {
    const self = this;
    this.timerId = setInterval(() => self.stepForward(), this.speedMs);
  },

  stepForward() {
    const self = this;
    $.post("/backtest/step", function (res) {
      if (res.finished) {
        self.stopTimer();
        layer.msg("回放已结束");
        return;
      }
      if (!res.ok) return;

      // 更新 UI
      $("#bt-current-price").text("¥" + res.current_price.toFixed(2));
      const progress = ((res.current_pos / res.total_bars) * 100).toFixed(1);
      $("#bt-progress").text(progress + "%");

      // 推送新 bar 到小级别图表
      if (res.new_bar) {
        self.datafeedSmall.pushBar(res.new_bar);
      }

      // 推送新 bar 到大级别图表
      if (res.new_high_bar) {
        self.datafeedHigh.pushBar(res.new_high_bar);
      }

      // 存储缠论数据并重绘
      if (res.cl_small) {
        self.datafeedSmall._bars = res.cl_small;
        self.redrawShapes("small");
      }
      if (res.cl_high) {
        self.datafeedHigh._bars = res.cl_high;
        self.redrawShapes("high");
      }

      // 更新持仓市值
      self.updateCapitalDisplay();
    });
  },

  togglePause() {
    if (!this.running) return;
    if (this.paused) {
      this.startTimer();
      this.paused = false;
      $("#bt-btn-pause").text("暂停");
    } else {
      this.stopTimer();
      this.paused = true;
      $("#bt-btn-pause").text("继续");
    }
  },

  stopTimer() {
    if (this.timerId) {
      clearInterval(this.timerId);
      this.timerId = null;
    }
  },

  stopReplay() {
    this.stopTimer();
    this.running = false;
    this.paused = false;
    $.post("/backtest/stop");
    $("#bt-btn-start").removeClass("layui-btn-disabled").attr("disabled", false);
    $("#bt-btn-pause").text("暂停");
    layer.msg("回放已结束");
  },
```

- [ ] **Step 5: 写入交易逻辑**

```javascript
  trade(action) {
    if (!this.running || this.paused) {
      layer.msg("请先开始回放"); return;
    }

    const priceText = $("#bt-current-price").text().replace("¥", "");
    const price = parseFloat(priceText);
    if (isNaN(price)) return;

    const qty = parseInt($("#bt-trade-qty").val()) || 0;
    if (qty <= 0) { layer.msg("请输入有效数量"); return; }

    if (action === "buy") {
      if (this.position.type === "short") { layer.msg("请先平掉空仓"); return; }
      const cost = price * qty;
      if (cost > this.capital) { layer.msg("资金不足"); return; }
      this.capital -= cost;
      if (this.position.type === "long") {
        // 加仓
        const totalQty = this.position.qty + qty;
        const avgPrice = (this.position.price * this.position.qty + price * qty) / totalQty;
        this.position.qty = totalQty;
        this.position.price = avgPrice;
      } else {
        this.position = { type: "long", qty: qty, price: price };
      }
      this.addTradeRecord("买入", price, qty);
    } else if (action === "sell") {
      if (this.position.type === "long") { layer.msg("请先平掉多仓"); return; }
      const cost = price * qty;
      if (cost > this.capital) { layer.msg("资金不足"); return; }
      this.capital -= cost;
      if (this.position.type === "short") {
        const totalQty = this.position.qty + qty;
        const avgPrice = (this.position.price * this.position.qty + price * qty) / totalQty;
        this.position.qty = totalQty;
        this.position.price = avgPrice;
      } else {
        this.position = { type: "short", qty: qty, price: price };
      }
      this.addTradeRecord("卖出", price, qty);
    } else if (action === "close") {
      if (!this.position.type) { layer.msg("无持仓可平"); return; }
      let pnl = 0;
      if (this.position.type === "long") {
        pnl = (price - this.position.price) * this.position.qty;
      } else if (this.position.type === "short") {
        pnl = (this.position.price - price) * this.position.qty;
      }
      this.capital += this.position.price * this.position.qty + pnl;
      this.addTradeRecord("平仓", price, this.position.qty, pnl);
      this.position = { type: null, qty: 0, price: 0 };
    }

    this.updateCapitalDisplay();
    this.updatePositionDisplay();
  },

  addTradeRecord(direction, price, qty, pnl) {
    const time = $("#bt-current-time").text();
    this.tradeRecords.push({ time, direction, price, qty, pnl: pnl || 0 });
    const row = `<tr>
      <td>${time}</td>
      <td>${direction}</td>
      <td>¥${price.toFixed(2)}</td>
      <td>${qty}</td>
      <td>${pnl !== undefined ? "¥" + pnl.toFixed(2) : "--"}</td>
    </tr>`;
    $("#bt-trade-records tbody").prepend(row);
  },

  updateCapitalDisplay() {
    const priceText = $("#bt-current-price").text().replace("¥", "");
    const price = parseFloat(priceText);
    let positionValue = 0;
    if (this.position.type === "long" && !isNaN(price)) {
      positionValue = this.position.qty * price;
    } else if (this.position.type === "short" && !isNaN(price)) {
      positionValue = -this.position.qty * price;
    }

    const totalValue = this.capital + positionValue;
    const pnl = totalValue - BT_CONFIG.INITIAL_CAPITAL;
    const pnlPct = ((pnl / BT_CONFIG.INITIAL_CAPITAL) * 100).toFixed(2);

    $("#bt-current-capital").text("¥" + totalValue.toFixed(2));
    $("#bt-position-value").text("¥" + positionValue.toFixed(2));
    const pnlEl = $("#bt-pnl");
    pnlEl.text((pnl >= 0 ? "+" : "") + pnlPct + "%");
    pnlEl.removeClass("positive negative");
    pnlEl.addClass(pnl >= 0 ? "positive" : "negative");
  },

  updatePositionDisplay() {
    if (!this.position.type || this.position.qty === 0) {
      $("#bt-position-display").text("无持仓");
    } else {
      const typeLabel = this.position.type === "long" ? "做多" : "做空";
      $("#bt-position-display").text(
        `${typeLabel} ${this.position.qty}股 @¥${this.position.price.toFixed(2)}`
      );
    }
  },
};
```

- [ ] **Step 6: 写入页面启动代码**

```javascript
// === 页面启动 ===
$(function () {
  layui.use("layer", function () {
    window.layer = layui.layer;
    BacktestApp.init();
  });
});
```

- [ ] **Step 7: 提交**

```bash
git add web/chanlun_chart/cl_app/static/js/backtest.js
git commit -m "feat: add backtest frontend logic with TV charts and trading simulation"
```

---

### Task 5: 修改导航菜单添加入口

**Files:**
- Modify: `web/chanlun_chart/cl_app/templates/index.html`

- [ ] **Step 1: 在下拉菜单中添加回测入口**

在 `index.html` 中，找到 `dropdown.render` 里的 `data` 数组，在 `id: 8`（切换主题）之前插入：

```javascript
{
  title: "缠论回测",
  templet:
    '<div><i class="layui-icon layui-icon-chart-screen"></i> <span>缠论回测学习</span></div>',
  id: 9,
},
```

- [ ] **Step 2: 在 `click` 回调中添加跳转处理**

在 `dropdown.render` 的 `click` 函数中，在 `else if (item.title === "系统设置")` 之前添加：

```javascript
} else if (item.title === "缠论回测") {
  window.open("/backtest", "_blank");
```

- [ ] **Step 3: 提交**

```bash
git add web/chanlun_chart/cl_app/templates/index.html
git commit -m "feat: add backtest entry in navigation menu"
```

---

### Task 6: 验证测试

- [ ] **Step 1: 启动 Flask 服务并验证页面可访问**

```bash
python web/chanlun_chart/app.py
```

打开浏览器访问 `http://localhost:9900/backtest`，确认页面加载正常（两个 TV 图表 + 右侧面板）。

- [ ] **Step 2: 点击"开始"按钮，验证回放流程**

观察：
- 股票信息显示（匿名标识）
- 图表加载 K 线和缠论数据
- 右侧面板显示当前价格、当前时间
- K 线逐根推进

- [ ] **Step 3: 测试交易功能**

- 输入买入数量，点击买入 → 确认持仓更新、资金扣减
- 点击平仓 → 确认盈亏计算、资金归还
- 测试做空流程
- 确认暂停/继续功能正常

- [ ] **Step 4: 测试边界情况**

- 回放结束后能否正常停止
- 未开始回放时点击交易按钮是否有提示
- 资金不足时买入是否有提示

- [ ] **Step 5: 提交如有修复**

```bash
git add -A
git commit -m "fix: backtest page bug fixes from testing"
```
