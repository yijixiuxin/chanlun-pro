(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports) :
    typeof define === 'function' && define.amd ? define(['exports'], factory) :
    (global = typeof globalThis !== 'undefined' ? globalThis : global || self, factory(global.Datafeeds = {}));
})(this, (function (exports) { 'use strict';

    function logMessage(message) {}
    function getErrorMessage(error) {
        if (error === undefined) return '';
        else if (typeof error === 'string') return error;
        return error.message;
    }

    class QuotesProvider {
        constructor(datafeedUrl, requester) {
            this._datafeedUrl = datafeedUrl;
            this._requester = requester;
        }
        getQuotes(symbols) {
            return new Promise((resolve, reject) => {
                this._requester.sendRequest(this._datafeedUrl, 'quotes', { symbols: symbols })
                    .then((response) => {
                    if (response.s === 'ok') resolve(response.d);
                    else reject(response.errmsg);
                }).catch((error) => {
                    reject(`network error: ${getErrorMessage(error)}`);
                });
            });
        }
    }

    class Requester {
        constructor(headers) {
            if (headers) this._headers = headers;
        }
        sendRequest(datafeedUrl, urlPath, params) {
            if (params !== undefined) {
                const paramKeys = Object.keys(params);
                if (paramKeys.length !== 0) urlPath += '?';
                urlPath += paramKeys.map((key) => {
                    return `${encodeURIComponent(key)}=${encodeURIComponent(params[key].toString())}`;
                }).join('&');
            }
            const options = { credentials: 'same-origin' };
            if (this._headers !== undefined) options.headers = this._headers;
            return fetch(`${datafeedUrl}/${urlPath}`, options)
                .then((response) => response.text())
                .then((responseTest) => JSON.parse(responseTest));
        }
    }

    class HistoryProvider {
        constructor(datafeedUrl, requester, limitedServerResponse) {
            this._datafeedUrl = datafeedUrl;
            this._requester = requester;
            this._limitedServerResponse = limitedServerResponse;
            this.bars_result = new Map();
        }
        getBars(symbolInfo, resolution, periodParams) {
            const requestParams = {
                symbol: symbolInfo.ticker || "",
                resolution: resolution,
                from: periodParams.from,
                to: periodParams.to,
            };
            if (periodParams.countBack !== undefined) requestParams.countback = periodParams.countBack;
            if (periodParams.firstDataRequest !== undefined) requestParams.firstDataRequest = periodParams.firstDataRequest;
            if (symbolInfo.currency_code !== undefined) requestParams.currencyCode = symbolInfo.currency_code;
            if (symbolInfo.unit_id !== undefined) requestParams.unitId = symbolInfo.unit_id;
            return new Promise(async (resolve, reject) => {
                try {
                    const initialResponse = await this._requester.sendRequest(this._datafeedUrl, "history", requestParams);
                    const result = this._processHistoryResponse(initialResponse, requestParams);
                    if (this._limitedServerResponse) {
                        await this._processTruncatedResponse(result, requestParams);
                    }
                    resolve(result);
                }
                catch (e) {
                    if (e instanceof Error || typeof e === "string") {
                        const reasonString = getErrorMessage(e);
                        console.warn(`HistoryProvider: getBars() failed, error=${reasonString}`);
                        reject(reasonString);
                    }
                }
            });
        }
        async _processTruncatedResponse(result, requestParams) {
            let lastResultLength = result.bars.length;
            try {
                while (this._limitedServerResponse &&
                    this._limitedServerResponse.maxResponseLength > 0 &&
                    this._limitedServerResponse.maxResponseLength === lastResultLength &&
                    requestParams.from < requestParams.to) {
                    if (requestParams.countback) requestParams.countback = requestParams.countback - lastResultLength;
                    if (this._limitedServerResponse.expectedOrder === "earliestFirst") {
                        requestParams.from = Math.round(result.bars[result.bars.length - 1].time / 1000);
                    } else {
                        requestParams.to = Math.round(result.bars[0].time / 1000);
                    }
                    const followupResponse = await this._requester.sendRequest(this._datafeedUrl, "history", requestParams);
                    const followupResult = this._processHistoryResponse(followupResponse, requestParams);
                    lastResultLength = followupResult.bars.length;
                    if (this._limitedServerResponse.expectedOrder === "earliestFirst") {
                        if (followupResult.bars[0].time === result.bars[result.bars.length - 1].time) followupResult.bars.shift();
                        result.bars.push(...followupResult.bars);
                    } else {
                        if (followupResult.bars[followupResult.bars.length - 1].time === result.bars[0].time) followupResult.bars.pop();
                        result.bars.unshift(...followupResult.bars);
                    }
                }
            }
            catch (e) {
                if (e instanceof Error || typeof e === "string") {
                    const reasonString = getErrorMessage(e);
                    console.warn(`HistoryProvider: getBars() warning during followup request, error=${reasonString}`);
                }
            }
        }
        
        _processHistoryResponse(response, requestParams) {
            if (response.s !== "ok" && response.s !== "no_data") {
                throw new Error(response.errmsg);
            }
            const bars = [];
            const meta = { noData: false };
            
            if (response.s === "no_data") {
                meta.noData = true;
                meta.nextTime = response.nextTime;
            } else {
                const volumePresent = response.v !== undefined;
                const ohlPresent = response.o !== undefined;
                for (let i = 0; i < response.t.length; ++i) {
                    const barValue = {
                        time: response.t[i] * 1000, // K线强制转毫秒
                        close: response.c[i],
                        open: response.c[i],
                        high: response.c[i],
                        low: response.c[i],
                    };
                    if (ohlPresent) {
                        barValue.open = response.o[i];
                        barValue.high = response.h[i];
                        barValue.low = response.l[i];
                    }
                    if (volumePresent) {
                        barValue.volume = response.v[i];
                    }
                    bars.push(barValue);
                }

                const res_key = requestParams["symbol"].toString().toLowerCase() +
                    requestParams["resolution"].toString().toLowerCase();

                let obj_res = this.bars_result.get(res_key);

                const raw_times = (response.t || []).map(t => t * 1000);
                const macd_dif = response.macd_dif || [];
                const macd_dea = response.macd_dea || [];
                const macd_hist = response.macd_hist || [];
                const macd_area = response.macd_area || [];

                // [DEBUG LOG] 1. 打印接收到的原始数据情况
                const bisLen = response.bis ? response.bis.length : 0;
                const fxsLen = response.fxs ? response.fxs.length : 0;
                console.log(`[DEBUG-BUNDLE] <Recv> Key: ${res_key} | Bars: ${bars.length} | Bis: ${bisLen} | Fxs: ${fxsLen}`);

                // [DEBUG LOG] 2. 检查 Chanlun 数据的时间戳样例（第一笔）
                if (bisLen > 0) {
                    console.log(`[DEBUG-BUNDLE] Sample Bi Time: ${response.bis[0].points[0].time} (Raw)`);
                }

                const mergeAlignedArrays = (existingTimes = [], existingArr = [], newTimes = [], newArr = []) => {
                    const map = new Map();
                    existingTimes.forEach((t, i) => {
                        map.set(t, existingArr[i] !== undefined ? existingArr[i] : NaN);
                    });
                    newTimes.forEach((t, i) => {
                        let val = NaN;
                        if (newArr && i < newArr.length) val = newArr[i];
                        if (val === null || val === undefined) val = NaN;
                        map.set(t, val);
                    });
                    const allTimes = Array.from(new Set([...existingTimes, ...newTimes])).sort((a, b) => a - b);
                    return {
                        times: allTimes,
                        values: allTimes.map(t => {
                            const v = map.get(t);
                            return (v === undefined || v === null) ? NaN : v;
                        })
                    };
                };

                // 定义合并函数
                const updateTextPoints = (existingPoints, newPoints) => {
                    if (!newPoints || newPoints.length === 0) return existingPoints || [];
                    if (!existingPoints || existingPoints.length === 0) return newPoints;
                    const combined = [...existingPoints];
                    for (const p of newPoints) {
                        const exists = combined.some(ep => JSON.stringify(ep.points) === JSON.stringify(p.points) && ep.text === p.text);
                        if (!exists) combined.push(p);
                    }
                    const getPointTime = (point) => (Array.isArray(point.points) ? point.points[0].time : point.points.time);
                    return combined.sort((a, b) => getPointTime(a) - getPointTime(b));
                };

                const updateLineSegments = (existingSegments, newSegments) => {
                    if (!newSegments || newSegments.length === 0) return existingSegments || [];
                    if (!existingSegments || existingSegments.length === 0) return newSegments;
                    const combined = [...existingSegments];
                    for (const s of newSegments) {
                        const exists = combined.some(es => JSON.stringify(es.points) === JSON.stringify(s.points));
                        if (!exists) combined.push(s);
                    }
                    return combined.sort((a, b) => {
                        if (!a.points || a.points.length === 0) return -1;
                        if (!b.points || b.points.length === 0) return 1;
                        return a.points[0].time - b.points[0].time;
                    });
                };

                if (obj_res === undefined) {
                    const difObj = mergeAlignedArrays([], [], raw_times, macd_dif);
                    const deaObj = mergeAlignedArrays([], [], raw_times, macd_dea);
                    const histObj = mergeAlignedArrays([], [], raw_times, macd_hist);
                    const areaObj = mergeAlignedArrays([], [], raw_times, macd_area);

                    const newCache = {
                        bars: bars,
                        meta: meta,
                        times: difObj.times,
                        macd_dif: difObj.values,
                        macd_dea: deaObj.values,
                        macd_hist: histObj.values,
                        macd_area: areaObj.values,
                        fxs: response.fxs || [],
                        bis: response.bis || [],
                        xds: response.xds || [],
                        zsds: response.zsds || [],
                        bi_zss: response.bi_zss || [],
                        xd_zss: response.xd_zss || [],
                        zsd_zss: response.zsd_zss || [],
                        bcs: response.bcs || [],
                        mmds: response.mmds || [],
                        chart_color: response.chart_color,
                    };
                    this.bars_result.set(res_key, newCache);
                    console.log(`[DEBUG-BUNDLE] <Init> Cache created for ${res_key}. Bis: ${newCache.bis.length}`);
                }
                else {
                    const oldTimes = obj_res.times || [];

                    // Bars 合并
                    const oldBars = obj_res.bars || [];
                    const barMap = new Map();
                    oldBars.forEach(b => barMap.set(b.time, b));
                    bars.forEach(b => barMap.set(b.time, b));
                    const mergedBars = Array.from(barMap.values()).sort((a, b) => a.time - b.time);
                    obj_res.bars = mergedBars;

                    // MACD 合并
                    const difObj = mergeAlignedArrays(oldTimes, obj_res.macd_dif, raw_times, macd_dif);
                    const deaObj = mergeAlignedArrays(oldTimes, obj_res.macd_dea, raw_times, macd_dea);
                    const histObj = mergeAlignedArrays(oldTimes, obj_res.macd_hist, raw_times, macd_hist);
                    const areaObj = mergeAlignedArrays(oldTimes, obj_res.macd_area, raw_times, macd_area);

                    obj_res.times = difObj.times;
                    obj_res.macd_dif = difObj.values;
                    obj_res.macd_dea = deaObj.values;
                    obj_res.macd_hist = histObj.values;
                    obj_res.macd_area = areaObj.values;

                    // Chanlun 数据合并
                    obj_res.fxs = updateTextPoints(obj_res.fxs, response.fxs);
                    obj_res.bis = updateLineSegments(obj_res.bis, response.bis);
                    obj_res.xds = updateLineSegments(obj_res.xds, response.xds);
                    obj_res.zsds = updateLineSegments(obj_res.zsds, response.zsds);
                    obj_res.bi_zss = updateLineSegments(obj_res.bi_zss, response.bi_zss);
                    obj_res.xd_zss = updateLineSegments(obj_res.xd_zss, response.xd_zss);
                    obj_res.zsd_zss = updateLineSegments(obj_res.zsd_zss, response.zsd_zss);
                    obj_res.bcs = updateTextPoints(obj_res.bcs, response.bcs);
                    obj_res.mmds = updateTextPoints(obj_res.mmds, response.mmds);
                    obj_res.chart_color = response.chart_color;

                    this.bars_result.set(res_key, obj_res);
                    console.log(`[DEBUG-BUNDLE] <Merge> Cache updated for ${res_key}. Total Bars: ${obj_res.bars.length}, Total Bis: ${obj_res.bis.length}`);
                }
            }

            const result = {
                bars: bars,
                meta: meta,
                fxs: response.fxs,
                bis: response.bis,
                xds: response.xds,
                zsds: response.zsds,
                bi_zss: response.bi_zss,
                xd_zss: response.xd_zss,
                zsd_zss: response.zsd_zss,
                bcs: response.bcs,
                mmds: response.mmds,
                chart_color: response.chart_color,
            };
            return result;
        }
    }

    class DataPulseProvider {
        constructor(historyProvider, updateFrequency) {
            this._subscribers = {};
            this._requestsPending = 0;
            this._historyProvider = historyProvider;
            setInterval(this._updateData.bind(this), updateFrequency);
        }
        subscribeBars(symbolInfo, resolution, newDataCallback, listenerGuid) {
            if (this._subscribers.hasOwnProperty(listenerGuid)) return;
            this._subscribers[listenerGuid] = {
                lastBarTime: null,
                listener: newDataCallback,
                resolution: resolution,
                symbolInfo: symbolInfo,
            };
        }
        unsubscribeBars(listenerGuid) {
            delete this._subscribers[listenerGuid];
        }
        _updateData() {
            if (this._requestsPending > 0) return;
            this._requestsPending = 0;
            for (const listenerGuid in this._subscribers) {
                this._requestsPending += 1;
                this._updateDataForSubscriber(listenerGuid)
                    .then(() => { this._requestsPending -= 1; })
                    .catch((reason) => { this._requestsPending -= 1; });
            }
        }
        _updateDataForSubscriber(listenerGuid) {
            const subscriptionRecord = this._subscribers[listenerGuid];
            const rangeEndTime = parseInt((Date.now() / 1000).toString());
            const rangeStartTime = rangeEndTime - periodLengthSeconds(subscriptionRecord.resolution, 10);
            return this._historyProvider.getBars(subscriptionRecord.symbolInfo, subscriptionRecord.resolution, {
                from: rangeStartTime,
                to: rangeEndTime,
                countBack: 2,
                firstDataRequest: false,
            }).then((result) => {
                this._onSubscriberDataReceived(listenerGuid, result);
            });
        }
        _onSubscriberDataReceived(listenerGuid, result) {
            if (!this._subscribers.hasOwnProperty(listenerGuid)) return;
            const bars = result.bars;
            if (bars.length === 0) return;
            const lastBar = bars[bars.length - 1];
            const subscriptionRecord = this._subscribers[listenerGuid];
            if (subscriptionRecord.lastBarTime !== null && lastBar.time < subscriptionRecord.lastBarTime) return;
            const isNewBar = subscriptionRecord.lastBarTime !== null && lastBar.time > subscriptionRecord.lastBarTime;
            if (isNewBar) {
                if (bars.length < 2) throw new Error('Not enough bars in history for proper pulse update. Need at least 2.');
                const previousBar = bars[bars.length - 2];
                subscriptionRecord.listener(previousBar);
            }
            subscriptionRecord.lastBarTime = lastBar.time;
            subscriptionRecord.listener(lastBar);
        }
    }

    function periodLengthSeconds(resolution, requiredPeriodsCount) {
        let daysCount = 0;
        if (resolution === 'D' || resolution === '1D') daysCount = requiredPeriodsCount;
        else if (resolution === 'M' || resolution === '1M') daysCount = 31 * requiredPeriodsCount;
        else if (resolution === 'W' || resolution === '1W') daysCount = 7 * requiredPeriodsCount;
        else daysCount = requiredPeriodsCount * parseInt(resolution) / (24 * 60);
        return daysCount * 24 * 60 * 60;
    }

    class QuotesPulseProvider {
        constructor(quotesProvider) {
            this._subscribers = {};
            this._requestsPending = 0;
            this._timers = null;
            this._quotesProvider = quotesProvider;
        }
        subscribeQuotes(symbols, fastSymbols, onRealtimeCallback, listenerGuid) {
            this._subscribers[listenerGuid] = {
                symbols: symbols,
                fastSymbols: fastSymbols,
                listener: onRealtimeCallback,
            };
            this._createTimersIfRequired();
        }
        unsubscribeQuotes(listenerGuid) {
            delete this._subscribers[listenerGuid];
            if (Object.keys(this._subscribers).length === 0) {
                this._destroyTimers();
            }
        }
        _createTimersIfRequired() {
            if (this._timers === null) {
                const fastTimer = window.setInterval(this._updateQuotes.bind(this, 1), 10000);
                const generalTimer = window.setInterval(this._updateQuotes.bind(this, 0), 60000);
                this._timers = { fastTimer, generalTimer };
            }
        }
        _destroyTimers() {
            if (this._timers !== null) {
                clearInterval(this._timers.fastTimer);
                clearInterval(this._timers.generalTimer);
                this._timers = null;
            }
        }
        _updateQuotes(updateType) {
            if (this._requestsPending > 0) return;
            for (const listenerGuid in this._subscribers) {
                this._requestsPending++;
                const subscriptionRecord = this._subscribers[listenerGuid];
                this._quotesProvider.getQuotes(updateType === 1 ? subscriptionRecord.fastSymbols : subscriptionRecord.symbols)
                    .then((data) => {
                    this._requestsPending--;
                    if (!this._subscribers.hasOwnProperty(listenerGuid)) return;
                    subscriptionRecord.listener(data);
                }).catch((reason) => {
                    this._requestsPending--;
                });
            }
        }
    }

    function extractField$1(data, field, arrayIndex, valueIsArray) {
        if (!(field in data)) return undefined;
        const value = data[field];
        if (Array.isArray(value) && (!valueIsArray || Array.isArray(value[0]))) return value[arrayIndex];
        return value;
    }
    function symbolKey(symbol, currency, unit) {
        return symbol + (currency !== undefined ? '_%|#|%_' + currency : '') + (unit !== undefined ? '_%|#|%_' + unit : '');
    }
    
    class SymbolsStorage {
        constructor(datafeedUrl, datafeedSupportedResolutions, requester) {
            this._exchangesList = ['NYSE', 'FOREX', 'AMEX'];
            this._symbolsInfo = {};
            this._symbolsList = [];
            this._datafeedUrl = datafeedUrl;
            this._datafeedSupportedResolutions = datafeedSupportedResolutions;
            this._requester = requester;
            this._readyPromise = this._init();
            this._readyPromise.catch((error) => {
                console.error(`SymbolsStorage: Cannot init, error=${error.toString()}`);
            });
        }
        resolveSymbol(symbolName, currencyCode, unitId) {
            return this._readyPromise.then(() => {
                const symbolInfo = this._symbolsInfo[symbolKey(symbolName, currencyCode, unitId)];
                if (symbolInfo === undefined) return Promise.reject('invalid symbol');
                return Promise.resolve(symbolInfo);
            });
        }
        searchSymbols(searchString, exchange, symbolType, maxSearchResults) {
            return this._readyPromise.then(() => {
                const weightedResult = [];
                const queryIsEmpty = searchString.length === 0;
                searchString = searchString.toUpperCase();
                for (const symbolName of this._symbolsList) {
                    const symbolInfo = this._symbolsInfo[symbolName];
                    if (symbolInfo === undefined) continue;
                    if (symbolType.length > 0 && symbolInfo.type !== symbolType) continue;
                    if (exchange && exchange.length > 0 && symbolInfo.exchange !== exchange) continue;
                    const positionInName = symbolInfo.name.toUpperCase().indexOf(searchString);
                    const positionInDescription = symbolInfo.description.toUpperCase().indexOf(searchString);
                    if (queryIsEmpty || positionInName >= 0 || positionInDescription >= 0) {
                        const alreadyExists = weightedResult.some((item) => item.symbolInfo === symbolInfo);
                        if (!alreadyExists) {
                            const weight = positionInName >= 0 ? positionInName : 8000 + positionInDescription;
                            weightedResult.push({ symbolInfo: symbolInfo, weight: weight });
                        }
                    }
                }
                const result = weightedResult
                    .sort((item1, item2) => item1.weight - item2.weight)
                    .slice(0, maxSearchResults)
                    .map((item) => {
                    const symbolInfo = item.symbolInfo;
                    return {
                        symbol: symbolInfo.name,
                        full_name: `${symbolInfo.exchange}:${symbolInfo.name}`,
                        description: symbolInfo.description,
                        exchange: symbolInfo.exchange,
                        params: [],
                        type: symbolInfo.type,
                        ticker: symbolInfo.name,
                    };
                });
                return Promise.resolve(result);
            });
        }
        _init() {
            const promises = [];
            const alreadyRequestedExchanges = {};
            for (const exchange of this._exchangesList) {
                if (alreadyRequestedExchanges[exchange]) continue;
                alreadyRequestedExchanges[exchange] = true;
                promises.push(this._requestExchangeData(exchange));
            }
            return Promise.all(promises).then(() => { this._symbolsList.sort(); });
        }
        _requestExchangeData(exchange) {
            return new Promise((resolve, reject) => {
                this._requester.sendRequest(this._datafeedUrl, 'symbol_info', { group: exchange })
                    .then((response) => {
                    try { this._onExchangeDataReceived(exchange, response); }
                    catch (error) { reject(error instanceof Error ? error : new Error(`SymbolsStorage: Unexpected exception ${error}`)); return; }
                    resolve();
                }).catch((reason) => { resolve(); });
            });
        }
        _onExchangeDataReceived(exchange, data) {
             let symbolIndex = 0;
             try {
                const symbolsCount = data.symbol.length;
                const tickerPresent = data.ticker !== undefined;
                for (; symbolIndex < symbolsCount; ++symbolIndex) {
                    const symbolName = data.symbol[symbolIndex];
                    const listedExchange = extractField$1(data, 'exchange-listed', symbolIndex);
                    const tradedExchange = extractField$1(data, 'exchange-traded', symbolIndex);
                    const exchangeListedName = extractField$1(data, 'exchange_listed_name', symbolIndex);
                    const currencyCode = extractField$1(data, 'currency-code', symbolIndex);
                    const unitId = extractField$1(data, 'unit-id', symbolIndex);
                    const ticker = tickerPresent ? extractField$1(data, 'ticker', symbolIndex) : symbolName;
                    const symbolInfo = {
                        ticker: ticker,
                        name: symbolName,
                        base_name: [listedExchange + ':' + symbolName],
                        listed_exchange: listedExchange,
                        exchange: exchangeListedName || listedExchange,
                        currency_code: currencyCode,
                        original_currency_code: extractField$1(data, 'original-currency-code', symbolIndex),
                        unit_id: unitId,
                        original_unit_id: extractField$1(data, 'original-unit-id', symbolIndex),
                        unit_conversion_types: extractField$1(data, 'unit-conversion-types', symbolIndex, true),
                        description: extractField$1(data, 'description', symbolIndex),
                        has_intraday: definedValueOrDefault(extractField$1(data, 'has-intraday', symbolIndex), false),
                        visible_plots_set: definedValueOrDefault(extractField$1(data, 'visible-plots-set', symbolIndex), undefined),
                        minmov: extractField$1(data, 'minmovement', symbolIndex) || extractField$1(data, 'minmov', symbolIndex) || 0,
                        minmove2: extractField$1(data, 'minmove2', symbolIndex) || extractField$1(data, 'minmov2', symbolIndex),
                        fractional: extractField$1(data, 'fractional', symbolIndex),
                        pricescale: extractField$1(data, 'pricescale', symbolIndex),
                        type: extractField$1(data, 'type', symbolIndex),
                        session: extractField$1(data, 'session-regular', symbolIndex),
                        session_holidays: extractField$1(data, 'session-holidays', symbolIndex),
                        corrections: extractField$1(data, 'corrections', symbolIndex),
                        timezone: extractField$1(data, 'timezone', symbolIndex),
                        supported_resolutions: definedValueOrDefault(extractField$1(data, 'supported-resolutions', symbolIndex, true), this._datafeedSupportedResolutions),
                        has_daily: definedValueOrDefault(extractField$1(data, 'has-daily', symbolIndex), true),
                        intraday_multipliers: definedValueOrDefault(extractField$1(data, 'intraday-multipliers', symbolIndex, true), ['1', '5', '15', '30', '60']),
                        has_weekly_and_monthly: extractField$1(data, 'has-weekly-and-monthly', symbolIndex),
                        has_empty_bars: extractField$1(data, 'has-empty-bars', symbolIndex),
                        volume_precision: definedValueOrDefault(extractField$1(data, 'volume-precision', symbolIndex), 0),
                        format: 'price',
                    };
                    this._symbolsInfo[ticker] = symbolInfo;
                    this._symbolsInfo[symbolName] = symbolInfo;
                    if (currencyCode !== undefined || unitId !== undefined) {
                        this._symbolsInfo[symbolKey(ticker, currencyCode, unitId)] = symbolInfo;
                        this._symbolsInfo[symbolKey(symbolName, currencyCode, unitId)] = symbolInfo;
                    }
                    this._symbolsList.push(symbolName);
                }
             } catch(error) { throw new Error(`SymbolsStorage: API error: ${Object(error).message}`); }
        }
    }
    function definedValueOrDefault(value, defaultValue) {
        return value !== undefined ? value : defaultValue;
    }

    function extractField(data, field, arrayIndex) {
        const value = data[field];
        return Array.isArray(value) ? value[arrayIndex] : value;
    }

    class UDFCompatibleDatafeedBase {
        constructor(datafeedURL, quotesProvider, requester, updateFrequency = 10 * 1000, limitedServerResponse) {
            this._configuration = defaultConfiguration();
            this._symbolsStorage = null;
            this._datafeedURL = datafeedURL;
            this._requester = requester;
            this._historyProvider = new HistoryProvider(datafeedURL, this._requester, limitedServerResponse);
            this._quotesProvider = quotesProvider;
            this._dataPulseProvider = new DataPulseProvider(this._historyProvider, updateFrequency);
            this._quotesPulseProvider = new QuotesPulseProvider(this._quotesProvider);
            this._configurationReadyPromise = this._requestConfiguration()
                .then((configuration) => {
                if (configuration === null) configuration = defaultConfiguration();
                this._setupWithConfiguration(configuration);
            });
        }
        onReady(callback) {
            this._configurationReadyPromise.then(() => { callback(this._configuration); });
        }
        getQuotes(symbols, onDataCallback, onErrorCallback) {
            this._quotesProvider.getQuotes(symbols).then(onDataCallback).catch(onErrorCallback);
        }
        subscribeQuotes(symbols, fastSymbols, onRealtimeCallback, listenerGuid) {
            this._quotesPulseProvider.subscribeQuotes(symbols, fastSymbols, onRealtimeCallback, listenerGuid);
        }
        unsubscribeQuotes(listenerGuid) {
            this._quotesPulseProvider.unsubscribeQuotes(listenerGuid);
        }
        getMarks(symbolInfo, from, to, onDataCallback, resolution) {
            if (!this._configuration.supports_marks) return;
            const requestParams = {
                symbol: symbolInfo.ticker || '',
                from: from,
                to: to,
                resolution: resolution,
            };
            this._send('marks', requestParams).then((response) => {
                if (!Array.isArray(response)) {
                    const result = [];
                    for (let i = 0; i < response.id.length; ++i) {
                        result.push({
                            id: extractField(response, 'id', i),
                            time: extractField(response, 'time', i),
                            color: extractField(response, 'color', i),
                            text: extractField(response, 'text', i),
                            label: extractField(response, 'label', i),
                            labelFontColor: extractField(response, 'labelFontColor', i),
                            minSize: extractField(response, 'minSize', i),
                            borderWidth: extractField(response, 'borderWidth', i),
                            hoveredBorderWidth: extractField(response, 'hoveredBorderWidth', i),
                            imageUrl: extractField(response, 'imageUrl', i),
                            showLabelWhenImageLoaded: extractField(response, 'showLabelWhenImageLoaded', i),
                        });
                    }
                    response = result;
                }
                onDataCallback(response);
            }).catch((error) => { onDataCallback([]); });
        }
        getTimescaleMarks(symbolInfo, from, to, onDataCallback, resolution) {
            if (!this._configuration.supports_timescale_marks) return;
            const requestParams = {
                symbol: symbolInfo.ticker || '',
                from: from,
                to: to,
                resolution: resolution,
            };
            this._send('timescale_marks', requestParams).then((response) => {
                if (!Array.isArray(response)) {
                    const result = [];
                    for (let i = 0; i < response.id.length; ++i) {
                        result.push({
                            id: extractField(response, 'id', i),
                            time: extractField(response, 'time', i),
                            color: extractField(response, 'color', i),
                            label: extractField(response, 'label', i),
                            tooltip: extractField(response, 'tooltip', i),
                            imageUrl: extractField(response, 'imageUrl', i),
                            showLabelWhenImageLoaded: extractField(response, 'showLabelWhenImageLoaded', i),
                        });
                    }
                    response = result;
                }
                onDataCallback(response);
            }).catch((error) => { onDataCallback([]); });
        }
        getServerTime(callback) {
            if (!this._configuration.supports_time) return;
            this._send('time').then((response) => {
                const time = parseInt(response);
                if (!isNaN(time)) callback(time);
            }).catch((error) => { });
        }
        searchSymbols(userInput, exchange, symbolType, onResult) {
             if (this._configuration.supports_search) {
                const params = {
                    limit: 30,
                    query: userInput.toUpperCase(),
                    type: symbolType,
                    exchange: exchange,
                };
                this._send('search', params).then((response) => {
                    if (response.s !== undefined) { onResult([]); return; }
                    onResult(response);
                }).catch((reason) => { onResult([]); });
            } else {
                if (this._symbolsStorage === null) throw new Error('UdfCompatibleDatafeed: inconsistent configuration (symbols storage)');
                this._symbolsStorage.searchSymbols(userInput, exchange, symbolType, 30).then(onResult).catch(onResult.bind(null, []));
            }
        }
        resolveSymbol(symbolName, onResolve, onError, extension) {
            const currencyCode = extension && extension.currencyCode;
            const unitId = extension && extension.unitId;
            function onResultReady(symbolInfo) { onResolve(symbolInfo); }
            if (!this._configuration.supports_group_request) {
                const params = { symbol: symbolName };
                if (currencyCode !== undefined) params.currencyCode = currencyCode;
                if (unitId !== undefined) params.unitId = unitId;
                this._send('symbols', params).then((response) => {
                    if (response.s !== undefined) onError('unknown_symbol');
                    else {
                         const symbol = response.name;
                        const listedExchange = response.listed_exchange ?? response['exchange-listed'];
                        const tradedExchange = response.exchange ?? response['exchange-traded'];
                        const result = {
                            ...response,
                            name: symbol,
                            base_name: [listedExchange + ':' + symbol],
                            listed_exchange: listedExchange,
                            exchange: tradedExchange,
                            ticker: response.ticker,
                            currency_code: response.currency_code ?? response['currency-code'],
                            original_currency_code: response.original_currency_code ?? response['original-currency-code'],
                            unit_id: response.unit_id ?? response['unit-id'],
                            original_unit_id: response.original_unit_id ?? response['original-unit-id'],
                            unit_conversion_types: response.unit_conversion_types ?? response['unit-conversion-types'],
                            has_intraday: response.has_intraday ?? response['has-intraday'] ?? false,
                            visible_plots_set: response.visible_plots_set ?? response['visible-plots-set'],
                            minmov: response.minmovement ?? response.minmov ?? 0,
                            minmove2: response.minmovement2 ?? response.minmove2,
                            session: response.session ?? response['session-regular'],
                            session_holidays: response.session_holidays ?? response['session-holidays'],
                            supported_resolutions: response.supported_resolutions ?? response['supported-resolutions'] ?? this._configuration.supported_resolutions ?? [],
                            has_daily: response.has_daily ?? response['has-daily'] ?? true,
                            intraday_multipliers: response.intraday_multipliers ?? response['intraday-multipliers'] ?? ['1', '5', '15', '30', '60'],
                            has_weekly_and_monthly: response.has_weekly_and_monthly ?? response['has-weekly-and-monthly'],
                            has_empty_bars: response.has_empty_bars ?? response['has-empty-bars'],
                            volume_precision: response.volume_precision ?? response['volume-precision'],
                            format: response.format ?? 'price',
                        };
                        onResultReady(result);
                    }
                }).catch((reason) => { onError('unknown_symbol'); });
            } else {
                if (this._symbolsStorage === null) throw new Error('UdfCompatibleDatafeed: inconsistent configuration (symbols storage)');
                this._symbolsStorage.resolveSymbol(symbolName, currencyCode, unitId).then(onResultReady).catch(onError);
            }
        }
        getBars(symbolInfo, resolution, periodParams, onResult, onError) {
            this._historyProvider.getBars(symbolInfo, resolution, periodParams).then((result) => {
                onResult(result.bars, result.meta);
            }).catch(onError);
        }
        subscribeBars(symbolInfo, resolution, onTick, listenerGuid, _onResetCacheNeededCallback) {
            this._dataPulseProvider.subscribeBars(symbolInfo, resolution, onTick, listenerGuid);
        }
        unsubscribeBars(listenerGuid) {
            this._dataPulseProvider.unsubscribeBars(listenerGuid);
        }
        _requestConfiguration() {
            return this._send('config').catch((reason) => { return null; });
        }
        _send(urlPath, params) {
            return this._requester.sendRequest(this._datafeedURL, urlPath, params);
        }
        _setupWithConfiguration(configurationData) {
            this._configuration = configurationData;
            if (configurationData.exchanges === undefined) configurationData.exchanges = [];
            if (!configurationData.supports_search && !configurationData.supports_group_request) throw new Error('Unsupported datafeed configuration. Must either support search, or support group request');
            if (configurationData.supports_group_request || !configurationData.supports_search) {
                this._symbolsStorage = new SymbolsStorage(this._datafeedURL, configurationData.supported_resolutions || [], this._requester);
            }
        }
    }
    function defaultConfiguration() {
        return {
            supports_search: false,
            supports_group_request: true,
            supported_resolutions: ['1', '5', '15', '30', '60', '1D', '1W', '1M'],
            supports_marks: false,
            supports_timescale_marks: false,
        };
    }

    class UDFCompatibleDatafeed extends UDFCompatibleDatafeedBase {
        constructor(datafeedURL, updateFrequency = 10 * 1000, limitedServerResponse) {
            const requester = new Requester();
            const quotesProvider = new QuotesProvider(datafeedURL, requester);
            super(datafeedURL, quotesProvider, requester, updateFrequency, limitedServerResponse);
        }
    }

    exports.UDFCompatibleDatafeed = UDFCompatibleDatafeed;

}));