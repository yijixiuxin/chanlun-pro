// -----------------------------------------------------------------------
// 文件名: macd_stats.js
// 功能: 在 TradingView 图表上统计任意区间的 MACD / MACD_HTF
//       红绿柱高低点与面积，并提供区间选择交互与侧边面板。
// 依赖: charts.js (ChartManager / ChanlunTVRegistry / GlobalTVDatafeeds)
//       layui (用于面板 UI，可选；未加载时降级为原生 div)
// -----------------------------------------------------------------------

var MacdStats = (function () {

    // ===================== 常量 =====================
    const PANEL_ID = 'macd-stats-panel';
    const RANGE_SHAPE_TAG = 'macd-stats-range';
    const MARKER_SHAPE_TAG = 'macd-stats-marker';

    const COLOR_POS = '#ef5350'; // 红柱
    const COLOR_NEG = '#26a69a'; // 绿柱
    const COLOR_RANGE_BG = '#FFD54F';

    // ===================== 工具：时间二分查找 =====================
    // 复制自 chart_idx_macd_backend.js 的 smartSearch，
    // 避免跨文件耦合，后续可统一抽到 utils.js。
    function smartSearch(times, target, intervalStr) {
        if (target === undefined || target === null || isNaN(target)) return -1;
        if (!times || times.length === 0) return -1;

        const isSeconds = target < 10000000000;
        let tolerance = isSeconds ? 3600 : 3600000;
        const ivs = String(intervalStr || '').toLowerCase();
        if (ivs.includes('w')) tolerance = isSeconds ? 432000 : 432000000;
        else if (ivs.includes('d') || ivs === '1440') tolerance = isSeconds ? 172800 : 172800000;

        let left = 0, right = times.length - 1, idx = -1;
        while (left <= right) {
            const mid = Math.floor((left + right) / 2);
            if (times[mid] >= target) { idx = mid; right = mid - 1; }
            else { left = mid + 1; }
        }

        let bestIdx = -1, minDiff = Infinity;
        if (idx !== -1) {
            const diff = Math.abs(times[idx] - target);
            if (diff <= tolerance && diff < minDiff) { minDiff = diff; bestIdx = idx; }
        }
        const prevIdx = (idx === -1) ? times.length - 1 : idx - 1;
        if (prevIdx >= 0) {
            const diff = Math.abs(times[prevIdx] - target);
            if (diff <= tolerance && diff < minDiff) { minDiff = diff; bestIdx = prevIdx; }
        }
        return bestIdx;
    }

    // ===================== 工具：定位 barsResult =====================
    function findBarsResult(targetCode, targetInterval) {
        const datafeeds = [];
        if (window.GlobalTVDatafeeds && window.GlobalTVDatafeeds.length > 0) {
            for (const df of window.GlobalTVDatafeeds) datafeeds.push(df);
        }
        if (window.tvDatafeed && !datafeeds.includes(window.tvDatafeed)) datafeeds.push(window.tvDatafeed);

        const code = String(targetCode || '').toLowerCase();
        const itv = String(targetInterval || '').toLowerCase();
        const mappings = { 'd': '1d', '1d': 'd', 'w': '1w', '1w': 'w', 'm': '1m', '1m': 'm', '1440': '1d', '240': '4h' };

        for (const df of datafeeds) {
            if (!df || !df._historyProvider || !df._historyProvider.bars_result) continue;
            const barsMap = df._historyProvider.bars_result;
            for (const key of barsMap.keys()) {
                const k = String(key);
                if (!k.toLowerCase().includes(code)) continue;
                let match = false;
                if (k.endsWith(itv)) match = true;
                else if (mappings[itv] && k.endsWith(mappings[itv])) match = true;
                else if (/^\d+$/.test(itv) && k.endsWith(itv + 'm')) match = true;
                if (match) return barsMap.get(key);
            }
        }
        return null;
    }

    // ===================== 核心算法：区间统计 =====================
    /**
     * 在指定区间内统计 MACD 红绿柱信息。
     * @param {number[]} times - bar 时间戳数组
     * @param {number[]} hist  - 对应的 hist 数组 (macd_hist 或 higher_macd_hist)
     * @param {number} startIdx - 区间起始索引（含）
     * @param {number} endIdx   - 区间结束索引（含）
     * @param {object} opts
     *   - htfDedup: boolean，true 时按 hist 值变化分段，每段只算一次（用于 MACD_HTF）
     *   - excludeLast: boolean，true 时排除区间末尾未收盘的最后一根
     * @returns {object} 统计结果
     */
    function computeStats(times, hist, startIdx, endIdx, opts) {
        opts = opts || {};
        const result = {
            startIdx, endIdx,
            barCount: 0,
            posArea: 0, negArea: 0, netArea: 0,
            posMax: 0, posMaxTime: null, posMaxIdx: -1,
            negMin: 0, negMinTime: null, negMinIdx: -1,
            segmentCount: 0,
            posSegments: [], // [{startIdx,endIdx,area,peak,peakIdx}]
            negSegments: [],
            excludedLast: false,
        };
        if (!times || !hist || startIdx < 0 || endIdx < startIdx) return result;
        if (endIdx >= times.length) endIdx = times.length - 1;

        let realEnd = endIdx;
        if (opts.excludeLast && realEnd > startIdx) {
            realEnd = realEnd - 1;
            result.excludedLast = true;
            result.endIdx = realEnd;
        }

        // HTF 去重：按 hist 值变化分段，重复值只取一次
        // 普通 MACD：每根独立累加
        let prevHistVal = null;
        let curSeg = null; // {startIdx, endIdx, area, peak, peakIdx, sign}

        const flushSeg = () => {
            if (!curSeg) return;
            if (curSeg.sign > 0) result.posSegments.push(curSeg);
            else if (curSeg.sign < 0) result.negSegments.push(curSeg);
            curSeg = null;
        };

        for (let i = startIdx; i <= realEnd; i++) {
            const v = Number(hist[i]);
            if (!isFinite(v)) continue;

            // HTF 模式：值未变化则跳过累加
            const isDup = opts.htfDedup && prevHistVal !== null && v === prevHistVal;
            prevHistVal = v;

            const sign = v > 0 ? 1 : (v < 0 ? -1 : 0);

            // 段切换检测
            if (!curSeg || curSeg.sign !== sign) {
                flushSeg();
                if (sign !== 0) {
                    curSeg = {
                        startIdx: i, endIdx: i,
                        area: 0, peak: v, peakIdx: i, sign: sign,
                    };
                }
            } else {
                curSeg.endIdx = i;
            }

            if (!isDup) {
                result.barCount++;
                if (v > 0) {
                    result.posArea += v;
                    if (v > result.posMax) {
                        result.posMax = v;
                        result.posMaxTime = times[i];
                        result.posMaxIdx = i;
                    }
                } else if (v < 0) {
                    result.negArea += Math.abs(v);
                    if (v < result.negMin) {
                        result.negMin = v;
                        result.negMinTime = times[i];
                        result.negMinIdx = i;
                    }
                }

                if (curSeg) {
                    curSeg.area += Math.abs(v);
                    if (sign > 0 && v > curSeg.peak) { curSeg.peak = v; curSeg.peakIdx = i; }
                    if (sign < 0 && v < curSeg.peak) { curSeg.peak = v; curSeg.peakIdx = i; }
                }
            }
        }
        flushSeg();

        result.netArea = result.posArea - result.negArea;
        result.segmentCount = result.posSegments.length + result.negSegments.length;
        return result;
    }

    /**
     * 对比当前段与上一同色段，给出简单的背驰提示
     */
    function buildDivergenceHint(stats, source) {
        const lastPos = stats.posSegments[stats.posSegments.length - 1];
        const prevPos = stats.posSegments[stats.posSegments.length - 2];
        const lastNeg = stats.negSegments[stats.negSegments.length - 1];
        const prevNeg = stats.negSegments[stats.negSegments.length - 2];

        const hints = [];
        if (lastPos && prevPos) {
            const peakDown = lastPos.peak < prevPos.peak;
            const areaDown = lastPos.area < prevPos.area;
            if (peakDown && areaDown) hints.push(`红柱顶背驰倾向（峰值 ${prevPos.peak.toFixed(4)} → ${lastPos.peak.toFixed(4)}，面积 ${prevPos.area.toFixed(4)} → ${lastPos.area.toFixed(4)}）`);
        }
        if (lastNeg && prevNeg) {
            const peakUp = lastNeg.peak > prevNeg.peak; // peak 是负值，绝对值变小即"上升"
            const areaDown = lastNeg.area < prevNeg.area;
            if (peakUp && areaDown) hints.push(`绿柱底背驰倾向（谷值 ${prevNeg.peak.toFixed(4)} → ${lastNeg.peak.toFixed(4)}，面积 ${prevNeg.area.toFixed(4)} → ${lastNeg.area.toFixed(4)}）`);
        }
        if (hints.length === 0) hints.push(`无明显背驰信号（基于 ${source}）`);
        return hints;
    }

    // ===================== 控制器：每个 ChartManager 一个 =====================
    class MacdStatsController {
        constructor(chartManager) {
            this.cm = chartManager;
            this.startTime = null;
            this.endTime = null;
            this.startTimeRaw = null; // 原始单位（秒/毫秒），用于 createShape
            this.endTimeRaw = null;
            this.pickMode = null; // null | 'start' | 'end'
            this.shapeIds = []; // 当前绘制的标记 ids
            this.contextMenuUnsub = null;
            this._toolbarBtn = null;
            this._snapshots = []; // 历史区间快照
        }

        // 注入工具栏按钮（点击进入"取点模式"）
        attachToolbarButton() {
            if (!this.cm.widget || typeof this.cm.widget.headerReady !== 'function') return;
            this.cm.widget.headerReady().then(() => {
                try {
                    const btn = this.cm.widget.createButton({ align: 'right', useTradingViewStyle: false });
                    if (!btn) return;
                    btn.setAttribute('title', 'MACD 区间统计：点击后依次单击两根 K 线');
                    btn.style.cssText = 'cursor:pointer;padding:0 10px;color:#FFD54F;font-weight:bold;';
                    btn.innerHTML = '📊 MACD 区间';
                    btn.addEventListener('click', () => this.beginPickRange());
                    this._toolbarBtn = btn;
                } catch (e) {
                    console.warn('[MacdStats] createButton failed', e);
                }
            }).catch(e => console.warn('[MacdStats] headerReady failed', e));
        }

        // 注入右键菜单
        attachContextMenu() {
            if (!this.cm.chart || typeof this.cm.chart.onContextMenu !== 'function') return;
            try {
                this.cm.chart.onContextMenu((unixTime, price) => {
                    return [
                        { position: 'top', text: '-' },
                        {
                            position: 'top',
                            text: '📊 MACD: 设为统计起点',
                            click: () => this.setStartTime(unixTime),
                        },
                        {
                            position: 'top',
                            text: '📊 MACD: 设为统计终点并计算',
                            click: () => this.setEndTime(unixTime, true),
                        },
                        {
                            position: 'top',
                            text: '📊 MACD: 清除区间',
                            click: () => this.clearRange(),
                        },
                    ];
                });
            } catch (e) {
                console.warn('[MacdStats] onContextMenu failed', e);
            }
        }

        beginPickRange() {
            this.startTime = null;
            this.endTime = null;
            this.startTimeRaw = null;
            this.endTimeRaw = null;
            this.pickMode = 'start';
            this._showToast('请单击图表第一根 K 线作为【起点】(可按 ESC 取消)');
            this._installCrosshairPicker();
        }

        _installCrosshairPicker() {
            if (!this.cm.chart || !this.cm.widget) {
                console.warn('[MacdStats] chart/widget not ready');
                return;
            }

            // 清理上一次未完成的 picker
            if (this._pickerCleanup) {
                try { this._pickerCleanup(); } catch (e) { /* ignore */ }
                this._pickerCleanup = null;
            }

            let lastTime = null;
            const installedAt = Date.now();

            // 订阅 crosshair 移动，获取鼠标悬停的 K 线时间
            let crosshairSub = null;
            const crosshairHandler = (params) => {
                if (params && params.time !== undefined && params.time !== null) {
                    lastTime = params.time;
                }
            };
            try {
                if (typeof this.cm.chart.crossHairMoved === 'function') {
                    crosshairSub = this.cm.chart.crossHairMoved();
                    if (crosshairSub && typeof crosshairSub.subscribe === 'function') {
                        crosshairSub.subscribe(null, crosshairHandler);
                    }
                }
            } catch (e) {
                console.warn('[MacdStats] subscribe crossHairMoved failed', e);
            }

            // ✅ 用 TV 官方的 widget.subscribe('mouse_down') 事件
            // 这是 TV 提供的标准 API，专门用于监听图表内的鼠标按下，不会被 canvas 吞掉
            const onMouseDown = (params) => {
                // 防止按钮点击的同一次事件被立即捕获
                if (Date.now() - installedAt < 250) return;

                let t = lastTime;
                // 兜底：取可见区间右端
                if (t === null || t === undefined) {
                    try {
                        const range = this.cm.chart.getVisibleRange && this.cm.chart.getVisibleRange();
                        if (range && range.to) t = range.to;
                    } catch (e) { /* ignore */ }
                }
                if (t === null || t === undefined) {
                    this._showToast('未能识别 K 线位置，请先在 K 线上方移动鼠标');
                    return;
                }

                if (this.pickMode === 'start') {
                    this.setStartTime(t);
                    this.pickMode = 'end';
                    this._showToast('已设起点，请单击【终点】K 线');
                } else if (this.pickMode === 'end') {
                    this.setEndTime(t, true);
                    this.pickMode = null;
                    cleanup();
                }
            };

            const onKey = (ev) => {
                if (ev.key === 'Escape') {
                    this._showToast('已取消区间选择');
                    this.pickMode = null;
                    cleanup();
                }
            };

            // 注意：TV widget.subscribe 返回 undefined，不返回订阅对象，
            // 取消订阅要用 widget.unsubscribe(eventName, handler)
            try {
                this.cm.widget.subscribe('mouse_down', onMouseDown);
            } catch (e) {
                console.warn('[MacdStats] widget.subscribe(mouse_down) failed', e);
                this._showToast('当前 TV 版本不支持 mouse_down 事件，请改用右键菜单选区间');
                return;
            }

            const cleanup = () => {
                try { this.cm.widget.unsubscribe('mouse_down', onMouseDown); } catch (e) { /* ignore */ }
                document.removeEventListener('keydown', onKey, true);
                if (crosshairSub && typeof crosshairSub.unsubscribe === 'function') {
                    try { crosshairSub.unsubscribe(null, crosshairHandler); } catch (e) { /* ignore */ }
                }
                this._pickerCleanup = null;
            };

            this._pickerCleanup = cleanup;
            document.addEventListener('keydown', onKey, true);
        }

        setStartTime(t) {
            this.startTimeRaw = t;
            this.startTime = t;
            this._showToast(`起点已设：${this._fmtTime(t)}`);
        }

        setEndTime(t, autoCompute) {
            this.endTimeRaw = t;
            this.endTime = t;
            this._showToast(`终点已设：${this._fmtTime(t)}`);
            if (autoCompute) this.computeAndRender();
        }

        clearRange() {
            this.startTime = this.endTime = null;
            this.startTimeRaw = this.endTimeRaw = null;
            this.pickMode = null;
            this._removeShapes();
            this._hidePanel();
        }

        // 核心：计算并渲染
        computeAndRender() {
            if (this.startTime === null || this.endTime === null) {
                this._showToast('请先选择起点和终点');
                return;
            }
            const symbolInterval = this.cm.widget && this.cm.widget.symbolInterval && this.cm.widget.symbolInterval();
            if (!symbolInterval) return;

            const code = String(symbolInterval.symbol || '').toLowerCase();
            const interval = String(symbolInterval.interval || '').toLowerCase();
            const barsResult = findBarsResult(code, interval);
            if (!barsResult || !barsResult.times || barsResult.times.length === 0) {
                console.warn('[MacdStats] findBarsResult failed', { code, interval });
                this._showToast('未找到当前图表数据，请稍后重试');
                return;
            }

            // 时间单位对齐
            const times = barsResult.times;
            const dataInSec = times[times.length - 1] < 10000000000;
            let s = this.startTime, e = this.endTime;
            if (dataInSec && s > 10000000000) s = Math.floor(s / 1000);
            if (dataInSec && e > 10000000000) e = Math.floor(e / 1000);
            // 如果 startTime 是毫秒、数据是秒，反之亦然，统一对齐
            if (!dataInSec && s < 10000000000) s = s * 1000;
            if (!dataInSec && e < 10000000000) e = e * 1000;
            if (s > e) { const tmp = s; s = e; e = tmp; }

            const firstT = times[0];
            const lastT = times[times.length - 1];

            // 夹紧到 K 线数据范围内：用户可能点在右侧未来空白区或左侧
            let startIdx = smartSearch(times, s, interval);
            let endIdx = smartSearch(times, e, interval);

            // 兜底：超出右边界 → 夹到最后一根
            if (startIdx === -1) {
                if (s > lastT) startIdx = times.length - 1;
                else if (s < firstT) startIdx = 0;
            }
            if (endIdx === -1) {
                if (e > lastT) endIdx = times.length - 1;
                else if (e < firstT) endIdx = 0;
            }

            if (startIdx === -1 || endIdx === -1) {
                console.warn('[MacdStats] smartSearch failed even after clamp', { s, e, firstT, lastT });
                this._showToast('时间无法对齐到 K 线，请重新选择');
                return;
            }
            if (startIdx > endIdx) { const tmp = startIdx; startIdx = endIdx; endIdx = tmp; }
            if (startIdx === endIdx) {
                this._showToast('起点和终点是同一根 K 线，请重新选择');
                return;
            }

            const hasHigher = barsResult.higher_macd_hist
                && barsResult.higher_macd_hist.length > 0
                && !barsResult.higher_macd_hist.every(v => isNaN(v) || v === null);

            // 同时计算 MACD 与 MACD_HTF
            const statsLocal = computeStats(times, barsResult.macd_hist || [], startIdx, endIdx, {
                htfDedup: false, excludeLast: true,
            });
            const statsHtf = hasHigher ? computeStats(times, barsResult.higher_macd_hist, startIdx, endIdx, {
                htfDedup: true, excludeLast: true,
            }) : null;

            // 绘制区间背景
            this.cm.markDrawingMutationStart('macd-stats');
            try {
                this._removeShapes();
                // this._drawRangeRect(times, startIdx, endIdx);
                // this._drawMarkers(times, statsLocal, statsHtf);
            } finally {
                this.cm.markDrawingMutationEnd('macd-stats');
            }

            // 渲染面板
            this._renderPanel({
                code, interval,
                startTime: times[startIdx], endTime: times[endIdx],
                barCount: endIdx - startIdx + 1,
                statsLocal, statsHtf, hasHigher,
            });
        }

        // -------------- 绘图 --------------
        _drawRangeRect(times, startIdx, endIdx) {
            try {
                const t1 = times[startIdx];
                const t2 = times[endIdx];
                // 用 hline + vline 模拟区间高亮（rectangle 需要价格坐标，MACD 面板没有可靠的价格范围）
                // 改为在主图上画两条垂直线
                if (typeof this.cm.chart.createMultipointShape === 'function') {
                    const id1 = this.cm.chart.createMultipointShape(
                        [{ time: t1 }],
                        {
                            shape: 'vertical_line', lock: true, disableSelection: true,
                            disableSave: true, disableUndo: true, showInObjectsTree: false,
                            overrides: { linecolor: COLOR_RANGE_BG, linewidth: 2, linestyle: 2 },
                        }
                    );
                    const id2 = this.cm.chart.createMultipointShape(
                        [{ time: t2 }],
                        {
                            shape: 'vertical_line', lock: true, disableSelection: true,
                            disableSave: true, disableUndo: true, showInObjectsTree: false,
                            overrides: { linecolor: COLOR_RANGE_BG, linewidth: 2, linestyle: 2 },
                        }
                    );
                    if (id1) this.shapeIds.push(id1);
                    if (id2) this.shapeIds.push(id2);
                }
            } catch (e) { console.warn('[MacdStats] draw range failed', e); }
        }

        _drawMarkers(times, statsLocal, statsHtf) {
            const place = (idx, text, up) => {
                if (idx < 0 || idx >= times.length) return;
                try {
                    const id = this.cm.chart.createShape(
                        { time: times[idx] },
                        {
                            shape: up ? 'arrow_down' : 'arrow_up',
                            text: text,
                            lock: true, disableSelection: true, disableSave: true, disableUndo: true,
                            showInObjectsTree: false,
                            overrides: {
                                color: up ? COLOR_POS : COLOR_NEG,
                                backgroundColor: up ? COLOR_POS : COLOR_NEG,
                                fontsize: 11, transparency: 30,
                            },
                        }
                    );
                    if (id) this.shapeIds.push(id);
                } catch (e) { /* ignore */ }
            };
            // 仅在主图标记，副图（MACD pane）不易通过公开 API 精准定位
            if (statsLocal.posMaxIdx >= 0) place(statsLocal.posMaxIdx, `MACD红峰 ${statsLocal.posMax.toFixed(4)}`, true);
            if (statsLocal.negMinIdx >= 0) place(statsLocal.negMinIdx, `MACD绿谷 ${statsLocal.negMin.toFixed(4)}`, false);
            if (statsHtf) {
                if (statsHtf.posMaxIdx >= 0) place(statsHtf.posMaxIdx, `HTF红峰 ${statsHtf.posMax.toFixed(4)}`, true);
                if (statsHtf.negMinIdx >= 0) place(statsHtf.negMinIdx, `HTF绿谷 ${statsHtf.negMin.toFixed(4)}`, false);
            }
        }

        _removeShapes() {
            if (!this.cm.chart) return;
            for (const id of this.shapeIds) {
                try { this.cm.chart.removeEntity(id); } catch (e) { /* ignore */ }
            }
            this.shapeIds = [];
        }

        // -------------- 面板 UI --------------
        _ensurePanel() {
            let panel = document.getElementById(PANEL_ID);
            if (panel) return panel;
            panel = document.createElement('div');
            panel.id = PANEL_ID;
            panel.style.cssText = [
                'position:fixed', 'top:80px', 'right:20px', 'width:340px', 'max-height:80vh',
                'overflow-y:auto', 'background:#1e222d', 'color:#d1d4dc',
                'border:1px solid #363c4e', 'border-radius:6px',
                'box-shadow:0 4px 16px rgba(0,0,0,0.4)', 'z-index:9999',
                'font-size:12px', 'font-family:Consolas,Monaco,monospace',
                'padding:12px', 'display:none',
            ].join(';');
            document.body.appendChild(panel);
            // 拖拽支持
            this._makeDraggable(panel);
            return panel;
        }

        _makeDraggable(panel) {
            let isDown = false, ox = 0, oy = 0;
            panel.addEventListener('mousedown', (ev) => {
                if (ev.target.closest('.macd-stats-no-drag')) return;
                isDown = true;
                ox = ev.clientX - panel.offsetLeft;
                oy = ev.clientY - panel.offsetTop;
            });
            document.addEventListener('mousemove', (ev) => {
                if (!isDown) return;
                panel.style.left = (ev.clientX - ox) + 'px';
                panel.style.top = (ev.clientY - oy) + 'px';
                panel.style.right = 'auto';
            });
            document.addEventListener('mouseup', () => { isDown = false; });
        }

        _hidePanel() {
            const p = document.getElementById(PANEL_ID);
            if (p) p.style.display = 'none';
        }

        _renderPanel(payload) {
            const panel = this._ensurePanel();
            panel.style.display = 'block';

            const fmt = (n) => (n === null || n === undefined || isNaN(n)) ? '-' : Number(n).toFixed(4);
            const renderBlock = (title, s, source) => {
                if (!s) return `<div style="opacity:.6;margin:6px 0;">[${title}] 无数据</div>`;
                const hints = buildDivergenceHint(s, source);
                return `
                <div style="margin:8px 0;padding:8px;background:#262b3a;border-radius:4px;">
                  <div style="font-weight:bold;color:#FFD54F;margin-bottom:6px;">${title}</div>
                  <div>📊 柱数: <b>${s.barCount}</b> ${s.excludedLast ? '<span style="color:#888">(已排除末根)</span>' : ''}</div>
                  <div style="color:${COLOR_POS}">🔴 红柱面积: <b>${fmt(s.posArea)}</b> | 峰值: <b>${fmt(s.posMax)}</b> @ ${this._fmtTime(s.posMaxTime)}</div>
                  <div style="color:${COLOR_NEG}">🟢 绿柱面积: <b>${fmt(s.negArea)}</b> | 谷值: <b>${fmt(s.negMin)}</b> @ ${this._fmtTime(s.negMinTime)}</div>
                  <div>⚖️ 净面积(红-绿): <b style="color:${s.netArea >= 0 ? COLOR_POS : COLOR_NEG}">${fmt(s.netArea)}</b></div>
                  <div>🔁 段数: 红 ${s.posSegments.length} / 绿 ${s.negSegments.length}</div>
                  <div style="margin-top:6px;font-size:11px;color:#9aa3b8;">${hints.map(h => '· ' + h).join('<br>')}</div>
                </div>`;
            };

            panel.innerHTML = `
              <div class="macd-stats-no-drag" style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;cursor:default;">
                <div style="font-weight:bold;color:#fff;font-size:13px;">📊 MACD 区间统计</div>
                <div>
                  <button class="macd-stats-no-drag" data-act="snapshot" style="background:#2962FF;border:0;color:#fff;padding:2px 8px;border-radius:3px;cursor:pointer;margin-right:4px;">快照</button>
                  <button class="macd-stats-no-drag" data-act="close" style="background:#444;border:0;color:#fff;padding:2px 8px;border-radius:3px;cursor:pointer;">×</button>
                </div>
              </div>
              <div style="font-size:11px;color:#9aa3b8;margin-bottom:6px;">
                ${payload.code} · ${payload.interval} · ${payload.barCount} 根 K 线<br>
                ${this._fmtTime(payload.startTime)} → ${this._fmtTime(payload.endTime)}
              </div>
              ${renderBlock('MACD (本周期)', payload.statsLocal, '本周期')}
              ${payload.hasHigher ? renderBlock('MACD_HTF (跨周期)', payload.statsHtf, '跨周期') : '<div style="opacity:.5;font-size:11px;">未启用 MACD_HTF 跨周期数据</div>'}
              ${this._renderSnapshots()}
            `;

            panel.querySelector('[data-act="close"]').addEventListener('click', () => this.clearRange());
            panel.querySelector('[data-act="snapshot"]').addEventListener('click', () => {
                this._snapshots.unshift({
                    label: `${payload.interval} ${this._fmtTime(payload.startTime)} ~ ${this._fmtTime(payload.endTime)}`,
                    posArea: payload.statsLocal.posArea,
                    negArea: payload.statsLocal.negArea,
                    netArea: payload.statsLocal.netArea,
                });
                if (this._snapshots.length > 5) this._snapshots.length = 5;
                this._renderPanel(payload);
            });
        }

        _renderSnapshots() {
            if (!this._snapshots || this._snapshots.length === 0) return '';
            const fmt = (n) => Number(n).toFixed(4);
            const rows = this._snapshots.map((s, i) => `
              <tr>
                <td style="padding:2px 4px;">${i + 1}</td>
                <td style="padding:2px 4px;">${s.label}</td>
                <td style="padding:2px 4px;color:${COLOR_POS}">${fmt(s.posArea)}</td>
                <td style="padding:2px 4px;color:${COLOR_NEG}">${fmt(s.negArea)}</td>
                <td style="padding:2px 4px;color:${s.netArea >= 0 ? COLOR_POS : COLOR_NEG}">${fmt(s.netArea)}</td>
              </tr>
            `).join('');
            return `
              <div style="margin-top:10px;">
                <div style="font-weight:bold;color:#FFD54F;margin-bottom:4px;">历史快照 (本周期)</div>
                <table style="width:100%;border-collapse:collapse;font-size:11px;">
                  <thead><tr style="opacity:.6;"><th>#</th><th>区间</th><th>红</th><th>绿</th><th>净</th></tr></thead>
                  <tbody>${rows}</tbody>
                </table>
              </div>`;
        }

        // -------------- 工具 --------------
        _fmtTime(t) {
            if (t === null || t === undefined || isNaN(t)) return '-';
            const ms = t < 10000000000 ? t * 1000 : t;
            const d = new Date(ms);
            const pad = (n) => String(n).padStart(2, '0');
            return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
        }

        _showToast(msg) {
            let toast = document.getElementById('macd-stats-toast');
            if (!toast) {
                toast = document.createElement('div');
                toast.id = 'macd-stats-toast';
                toast.style.cssText = [
                    'position:fixed', 'top:60px', 'left:50%', 'transform:translateX(-50%)',
                    'background:rgba(41,98,255,0.95)', 'color:#fff', 'padding:8px 16px',
                    'border-radius:4px', 'z-index:10000', 'font-size:12px',
                    'transition:opacity .3s', 'pointer-events:none',
                ].join(';');
                document.body.appendChild(toast);
            }
            toast.textContent = msg;
            toast.style.opacity = '1';
            clearTimeout(this._toastTimer);
            this._toastTimer = setTimeout(() => { toast.style.opacity = '0'; }, 2200);
        }

        dispose() {
            this._removeShapes();
            this._hidePanel();
            const panel = document.getElementById(PANEL_ID);
            if (panel) panel.remove();
        }
    }

    // ===================== 对外接口 =====================
    return {
        attach(chartManager) {
            if (!chartManager) return null;
            if (chartManager._macdStats) return chartManager._macdStats;
            const ctrl = new MacdStatsController(chartManager);
            chartManager._macdStats = ctrl;
            ctrl.attachToolbarButton();
            ctrl.attachContextMenu();
            return ctrl;
        },
        // 便于调试
        _internal: { computeStats, smartSearch, findBarsResult },
    };
})();

window.MacdStats = MacdStats;