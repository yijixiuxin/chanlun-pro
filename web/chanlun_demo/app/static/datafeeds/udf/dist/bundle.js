(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports) :
    typeof define === 'function' && define.amd ? define(['exports'], factory) :
    (global = typeof globalThis !== 'undefined' ? globalThis : global || self, factory(global.Datafeeds = {}));
}(this, (function (exports) { 'use strict';

    /**
     * If you want to enable logs from datafeed set it to `true`
     */
    function logMessage(message) {
    }
    function getErrorMessage(error) {
        if (error === undefined) {
            return '';
        }
        else if (typeof error === 'string') {
            return error;
        }
        return error.message;
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
                symbol: symbolInfo.ticker || '',
                resolution: resolution,
                from: periodParams.from,
                to: periodParams.to,
            };
            if (periodParams.countBack !== undefined) {
                requestParams.countback = periodParams.countBack;
            }
            if (periodParams.firstDataRequest !== undefined) {
                requestParams.firstDataRequest = periodParams.firstDataRequest;
            }
            if (symbolInfo.currency_code !== undefined) {
                requestParams.currencyCode = symbolInfo.currency_code;
            }
            if (symbolInfo.unit_id !== undefined) {
                requestParams.unitId = symbolInfo.unit_id;
            }
            return new Promise(async (resolve, reject) => {
                try {
                    const initialResponse = await this._requester.sendRequest(this._datafeedUrl, 'history', requestParams);
                    const result = this._processHistoryResponse(initialResponse, requestParams);
                    if (this._limitedServerResponse) {
                        await this._processTruncatedResponse(result, requestParams);
                    }
                    resolve(result);
                }
                catch (e) {
                    if (e instanceof Error || typeof e === 'string') {
                        const reasonString = getErrorMessage(e);
                        // tslint:disable-next-line:no-console
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
                    // adjust request parameters for follow-up request
                    if (requestParams.countback) {
                        requestParams.countback = requestParams.countback - lastResultLength;
                    }
                    if (this._limitedServerResponse.expectedOrder === 'earliestFirst') {
                        requestParams.from = Math.round(result.bars[result.bars.length - 1].time / 1000);
                    }
                    else {
                        requestParams.to = Math.round(result.bars[0].time / 1000);
                    }
                    const followupResponse = await this._requester.sendRequest(this._datafeedUrl, 'history', requestParams);
                    const followupResult = this._processHistoryResponse(followupResponse, requestParams);
                    lastResultLength = followupResult.bars.length;
                    // merge result with results collected so far
                    if (this._limitedServerResponse.expectedOrder === 'earliestFirst') {
                        if (followupResult.bars[0].time === result.bars[result.bars.length - 1].time) {
                            // Datafeed shouldn't include a value exactly matching the `to` timestamp but in case it does
                            // we will remove the duplicate.
                            followupResult.bars.shift();
                        }
                        result.bars.push(...followupResult.bars);
                    }
                    else {
                        if (followupResult.bars[followupResult.bars.length - 1].time === result.bars[0].time) {
                            // Datafeed shouldn't include a value exactly matching the `to` timestamp but in case it does
                            // we will remove the duplicate.
                            followupResult.bars.pop();
                        }
                        result.bars.unshift(...followupResult.bars);
                    }
                }
            }
            catch (e) {
                /**
                 * Error occurred during followup request. We won't reject the original promise
                 * because the initial response was valid so we will return what we've got so far.
                 */
                if (e instanceof Error || typeof e === 'string') {
                    const reasonString = getErrorMessage(e);
                    // tslint:disable-next-line:no-console
                    console.warn(`HistoryProvider: getBars() warning during followup request, error=${reasonString}`);
                }
            }
        }
        _processHistoryResponse(response, requestParams) {
            if (response.s !== 'ok' && response.s !== 'no_data') {
                throw new Error(response.errmsg);
            }
            const bars = [];
            const meta = {
                noData: false,
            };
            let fxs = [];
            let bis = [];
            let xds = [];
            let zsds = [];
            let bi_zss = [];
            let xd_zss = [];
            let zsd_zss = [];
            let bcs = [];
            let mmds = [];
            let result = {
                bars: bars,
                meta: meta,
                fxs: fxs,
                bis: bis,
                xds: xds,
                zsds: zsds,
                bi_zss: bi_zss,
                xd_zss: xd_zss,
                zsd_zss: zsd_zss,
                bcs: bcs,
                mmds: mmds,
            };
            if (response.s === 'no_data') {
                meta.noData = true;
                meta.nextTime = response.nextTime;
            }
            else {
                const volumePresent = response.v !== undefined;
                const ohlPresent = response.o !== undefined;
                fxs = response.fxs;
                bis = response.bis;
                xds = response.xds;
                zsds = response.zsds;
                bi_zss = response.bi_zss;
                xd_zss = response.xd_zss;
                zsd_zss = response.zsd_zss;
                bcs = response.bcs;
                mmds = response.mmds;
                for (let i = 0; i < response.t.length; ++i) {
                    const barValue = {
                        time: response.t[i] * 1000,
                        close: parseFloat(response.c[i]),
                        open: parseFloat(response.c[i]),
                        high: parseFloat(response.c[i]),
                        low: parseFloat(response.c[i]),
                    };
                    if (ohlPresent) {
                        barValue.open = parseFloat(response.o[i]);
                        barValue.high = parseFloat(response.h[i]);
                        barValue.low = parseFloat(response.l[i]);
                    }
                    if (volumePresent) {
                        barValue.volume = parseFloat(response.v[i]);
                    }
                    bars.push(barValue);
                }
                let result = {
                    bars: bars,
                    meta: meta,
                    fxs: fxs,
                    bis: bis,
                    xds: xds,
                    zsds: zsds,
                    bi_zss: bi_zss,
                    xd_zss: xd_zss,
                    zsd_zss: zsd_zss,
                    bcs: bcs,
                    mmds: mmds,
                };
                let obj_res = this.bars_result.get(requestParams['symbol'].toString().toLowerCase());
                if (obj_res == undefined) {
                    let obj_res = new Map();
                    obj_res.set(requestParams['resolution'].toString().toLowerCase(), result);
                    this.bars_result.set(requestParams['symbol'].toString().toLowerCase(), obj_res);
                }
                else {
                    obj_res.set(requestParams['resolution'].toString().toLowerCase(), result);
                    this.bars_result.set(requestParams['symbol'].toString().toLowerCase(), obj_res);
                }
            }
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
            if (this._subscribers.hasOwnProperty(listenerGuid)) {
                return;
            }
            this._subscribers[listenerGuid] = {
                lastBarTime: null,
                listener: newDataCallback,
                resolution: resolution,
                symbolInfo: symbolInfo,
            };
            logMessage(`DataPulseProvider: subscribed for #${listenerGuid} - {${symbolInfo.name}, ${resolution}}`);
        }
        unsubscribeBars(listenerGuid) {
            delete this._subscribers[listenerGuid];
        }
        _updateData() {
            if (this._requestsPending > 0) {
                return;
            }
            this._requestsPending = 0;
            // eslint-disable-next-line guard-for-in
            for (const listenerGuid in this._subscribers) {
                this._requestsPending += 1;
                this._updateDataForSubscriber(listenerGuid)
                    .then(() => {
                    this._requestsPending -= 1;
                    logMessage(`DataPulseProvider: data for #${listenerGuid} updated successfully, pending=${this._requestsPending}`);
                })
                    .catch((reason) => {
                    this._requestsPending -= 1;
                    logMessage(`DataPulseProvider: data for #${listenerGuid} updated with error=${getErrorMessage(reason)}, pending=${this._requestsPending}`);
                });
            }
        }
        _updateDataForSubscriber(listenerGuid) {
            const subscriptionRecord = this._subscribers[listenerGuid];
            const rangeEndTime = parseInt((Date.now() / 1000).toString());
            // BEWARE: please note we really need 2 bars, not the only last one
            // see the explanation below. `10` is the `large enough` value to work around holidays
            const rangeStartTime = rangeEndTime - periodLengthSeconds(subscriptionRecord.resolution, 10);
            return this._historyProvider.getBars(subscriptionRecord.symbolInfo, subscriptionRecord.resolution, {
                from: rangeStartTime,
                to: rangeEndTime,
                countBack: 2,
                firstDataRequest: false,
            })
                .then((result) => {
                this._onSubscriberDataReceived(listenerGuid, result);
            });
        }
        _onSubscriberDataReceived(listenerGuid, result) {
            // means the subscription was cancelled while waiting for data
            if (!this._subscribers.hasOwnProperty(listenerGuid)) {
                return;
            }
            const bars = result.bars;
            if (bars.length === 0) {
                return;
            }
            const lastBar = bars[bars.length - 1];
            const subscriptionRecord = this._subscribers[listenerGuid];
            if (subscriptionRecord.lastBarTime !== null && lastBar.time < subscriptionRecord.lastBarTime) {
                return;
            }
            const isNewBar = subscriptionRecord.lastBarTime !== null && lastBar.time > subscriptionRecord.lastBarTime;
            // Pulse updating may miss some trades data (ie, if pulse period = 10 secods and new bar is started 5 seconds later after the last update, the
            // old bar's last 5 seconds trades will be lost). Thus, at fist we should broadcast old bar updates when it's ready.
            if (isNewBar) {
                if (bars.length < 2) {
                    throw new Error('Not enough bars in history for proper pulse update. Need at least 2.');
                }
                const previousBar = bars[bars.length - 2];
                subscriptionRecord.listener(previousBar);
            }
            subscriptionRecord.lastBarTime = lastBar.time;
            subscriptionRecord.listener(lastBar);
        }
    }
    function periodLengthSeconds(resolution, requiredPeriodsCount) {
        let daysCount = 0;
        if (resolution === 'D' || resolution === '1D') {
            daysCount = requiredPeriodsCount;
        }
        else if (resolution === 'M' || resolution === '1M') {
            daysCount = 31 * requiredPeriodsCount;
        }
        else if (resolution === 'W' || resolution === '1W') {
            daysCount = 7 * requiredPeriodsCount;
        }
        else {
            daysCount = requiredPeriodsCount * parseInt(resolution) / (24 * 60);
        }
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
                const fastTimer = window.setInterval(this._updateQuotes.bind(this, 1 /* SymbolsType.Fast */), 10000 /* UpdateTimeouts.Fast */);
                const generalTimer = window.setInterval(this._updateQuotes.bind(this, 0 /* SymbolsType.General */), 60000 /* UpdateTimeouts.General */);
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
            if (this._requestsPending > 0) {
                return;
            }
            // eslint-disable-next-line guard-for-in
            for (const listenerGuid in this._subscribers) {
                this._requestsPending++;
                const subscriptionRecord = this._subscribers[listenerGuid];
                this._quotesProvider.getQuotes(updateType === 1 /* SymbolsType.Fast */ ? subscriptionRecord.fastSymbols : subscriptionRecord.symbols)
                    .then((data) => {
                    this._requestsPending--;
                    if (!this._subscribers.hasOwnProperty(listenerGuid)) {
                        return;
                    }
                    subscriptionRecord.listener(data);
                    logMessage(`QuotesPulseProvider: data for #${listenerGuid} (${updateType}) updated successfully, pending=${this._requestsPending}`);
                })
                    .catch((reason) => {
                    this._requestsPending--;
                    logMessage(`QuotesPulseProvider: data for #${listenerGuid} (${updateType}) updated with error=${getErrorMessage(reason)}, pending=${this._requestsPending}`);
                });
            }
        }
    }

    function extractField(data, field, arrayIndex, valueIsArray) {
        const value = data[field];
        if (Array.isArray(value) && (!valueIsArray || Array.isArray(value[0]))) {
            return value[arrayIndex];
        }
        return value;
    }
    function symbolKey(symbol, currency, unit) {
        // here we're using a separator that quite possible shouldn't be in a real symbol name
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
                // seems it is impossible
                // tslint:disable-next-line:no-console
                console.error(`SymbolsStorage: Cannot init, error=${error.toString()}`);
            });
        }
        // BEWARE: this function does not consider symbol's exchange
        resolveSymbol(symbolName, currencyCode, unitId) {
            return this._readyPromise.then(() => {
                const symbolInfo = this._symbolsInfo[symbolKey(symbolName, currencyCode, unitId)];
                if (symbolInfo === undefined) {
                    return Promise.reject('invalid symbol');
                }
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
                    if (symbolInfo === undefined) {
                        continue;
                    }
                    if (symbolType.length > 0 && symbolInfo.type !== symbolType) {
                        continue;
                    }
                    if (exchange && exchange.length > 0 && symbolInfo.exchange !== exchange) {
                        continue;
                    }
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
                        full_name: symbolInfo.full_name,
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
                if (alreadyRequestedExchanges[exchange]) {
                    continue;
                }
                alreadyRequestedExchanges[exchange] = true;
                promises.push(this._requestExchangeData(exchange));
            }
            return Promise.all(promises)
                .then(() => {
                this._symbolsList.sort();
            });
        }
        _requestExchangeData(exchange) {
            return new Promise((resolve, reject) => {
                this._requester.sendRequest(this._datafeedUrl, 'symbol_info', { group: exchange })
                    .then((response) => {
                    try {
                        this._onExchangeDataReceived(exchange, response);
                    }
                    catch (error) {
                        reject(error instanceof Error ? error : new Error(`SymbolsStorage: Unexpected exception ${error}`));
                        return;
                    }
                    resolve();
                })
                    .catch((reason) => {
                    logMessage(`SymbolsStorage: Request data for exchange '${exchange}' failed, reason=${getErrorMessage(reason)}`);
                    resolve();
                });
            });
        }
        _onExchangeDataReceived(exchange, data) {
            let symbolIndex = 0;
            try {
                const symbolsCount = data.symbol.length;
                const tickerPresent = data.ticker !== undefined;
                for (; symbolIndex < symbolsCount; ++symbolIndex) {
                    const symbolName = data.symbol[symbolIndex];
                    const listedExchange = extractField(data, 'exchange-listed', symbolIndex);
                    const tradedExchange = extractField(data, 'exchange-traded', symbolIndex);
                    const fullName = tradedExchange + ':' + symbolName;
                    const currencyCode = extractField(data, 'currency-code', symbolIndex);
                    const unitId = extractField(data, 'unit-id', symbolIndex);
                    const ticker = tickerPresent ? extractField(data, 'ticker', symbolIndex) : symbolName;
                    const symbolInfo = {
                        ticker: ticker,
                        name: symbolName,
                        base_name: [listedExchange + ':' + symbolName],
                        full_name: fullName,
                        listed_exchange: listedExchange,
                        exchange: tradedExchange,
                        currency_code: currencyCode,
                        original_currency_code: extractField(data, 'original-currency-code', symbolIndex),
                        unit_id: unitId,
                        original_unit_id: extractField(data, 'original-unit-id', symbolIndex),
                        unit_conversion_types: extractField(data, 'unit-conversion-types', symbolIndex, true),
                        description: extractField(data, 'description', symbolIndex),
                        has_intraday: definedValueOrDefault(extractField(data, 'has-intraday', symbolIndex), false),
                        visible_plots_set: definedValueOrDefault(extractField(data, 'visible-plots-set', symbolIndex), undefined),
                        minmov: extractField(data, 'minmovement', symbolIndex) || extractField(data, 'minmov', symbolIndex) || 0,
                        minmove2: extractField(data, 'minmove2', symbolIndex) || extractField(data, 'minmov2', symbolIndex),
                        fractional: extractField(data, 'fractional', symbolIndex),
                        pricescale: extractField(data, 'pricescale', symbolIndex),
                        type: extractField(data, 'type', symbolIndex),
                        session: extractField(data, 'session-regular', symbolIndex),
                        session_holidays: extractField(data, 'session-holidays', symbolIndex),
                        corrections: extractField(data, 'corrections', symbolIndex),
                        timezone: extractField(data, 'timezone', symbolIndex),
                        supported_resolutions: definedValueOrDefault(extractField(data, 'supported-resolutions', symbolIndex, true), this._datafeedSupportedResolutions),
                        has_daily: definedValueOrDefault(extractField(data, 'has-daily', symbolIndex), true),
                        intraday_multipliers: definedValueOrDefault(extractField(data, 'intraday-multipliers', symbolIndex, true), ['1', '5', '15', '30', '60']),
                        has_weekly_and_monthly: extractField(data, 'has-weekly-and-monthly', symbolIndex),
                        has_empty_bars: extractField(data, 'has-empty-bars', symbolIndex),
                        volume_precision: definedValueOrDefault(extractField(data, 'volume-precision', symbolIndex), 0),
                        format: 'price',
                    };
                    this._symbolsInfo[ticker] = symbolInfo;
                    this._symbolsInfo[symbolName] = symbolInfo;
                    this._symbolsInfo[fullName] = symbolInfo;
                    if (currencyCode !== undefined || unitId !== undefined) {
                        this._symbolsInfo[symbolKey(ticker, currencyCode, unitId)] = symbolInfo;
                        this._symbolsInfo[symbolKey(symbolName, currencyCode, unitId)] = symbolInfo;
                        this._symbolsInfo[symbolKey(fullName, currencyCode, unitId)] = symbolInfo;
                    }
                    this._symbolsList.push(symbolName);
                }
            }
            catch (error) {
                throw new Error(`SymbolsStorage: API error when processing exchange ${exchange} symbol #${symbolIndex} (${data.symbol[symbolIndex]}): ${Object(error).message}`);
            }
        }
    }
    function definedValueOrDefault(value, defaultValue) {
        return value !== undefined ? value : defaultValue;
    }

    function extractField$1(data, field, arrayIndex) {
        const value = data[field];
        return Array.isArray(value) ? value[arrayIndex] : value;
    }
    /**
     * This class implements interaction with UDF-compatible datafeed.
     * See [UDF protocol reference](@docs/connecting_data/UDF)
     */
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
                if (configuration === null) {
                    configuration = defaultConfiguration();
                }
                this._setupWithConfiguration(configuration);
            });
        }
        onReady(callback) {
            this._configurationReadyPromise.then(() => {
                callback(this._configuration);
            });
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
            if (!this._configuration.supports_marks) {
                return;
            }
            const requestParams = {
                symbol: symbolInfo.ticker || '',
                from: from,
                to: to,
                resolution: resolution,
            };
            this._send('marks', requestParams)
                .then((response) => {
                if (!Array.isArray(response)) {
                    const result = [];
                    for (let i = 0; i < response.id.length; ++i) {
                        result.push({
                            id: extractField$1(response, 'id', i),
                            time: extractField$1(response, 'time', i),
                            color: extractField$1(response, 'color', i),
                            text: extractField$1(response, 'text', i),
                            label: extractField$1(response, 'label', i),
                            labelFontColor: extractField$1(response, 'labelFontColor', i),
                            minSize: extractField$1(response, 'minSize', i),
                            borderWidth: extractField$1(response, 'borderWidth', i),
                            hoveredBorderWidth: extractField$1(response, 'hoveredBorderWidth', i),
                            imageUrl: extractField$1(response, 'imageUrl', i),
                            showLabelWhenImageLoaded: extractField$1(response, 'showLabelWhenImageLoaded', i),
                        });
                    }
                    response = result;
                }
                onDataCallback(response);
            })
                .catch((error) => {
                logMessage(`UdfCompatibleDatafeed: Request marks failed: ${getErrorMessage(error)}`);
                onDataCallback([]);
            });
        }
        getTimescaleMarks(symbolInfo, from, to, onDataCallback, resolution) {
            if (!this._configuration.supports_timescale_marks) {
                return;
            }
            const requestParams = {
                symbol: symbolInfo.ticker || '',
                from: from,
                to: to,
                resolution: resolution,
            };
            this._send('timescale_marks', requestParams)
                .then((response) => {
                if (!Array.isArray(response)) {
                    const result = [];
                    for (let i = 0; i < response.id.length; ++i) {
                        result.push({
                            id: extractField$1(response, 'id', i),
                            time: extractField$1(response, 'time', i),
                            color: extractField$1(response, 'color', i),
                            label: extractField$1(response, 'label', i),
                            tooltip: extractField$1(response, 'tooltip', i),
                            imageUrl: extractField$1(response, 'imageUrl', i),
                            showLabelWhenImageLoaded: extractField$1(response, 'showLabelWhenImageLoaded', i),
                        });
                    }
                    response = result;
                }
                onDataCallback(response);
            })
                .catch((error) => {
                logMessage(`UdfCompatibleDatafeed: Request timescale marks failed: ${getErrorMessage(error)}`);
                onDataCallback([]);
            });
        }
        getServerTime(callback) {
            if (!this._configuration.supports_time) {
                return;
            }
            this._send('time')
                .then((response) => {
                const time = parseInt(response);
                if (!isNaN(time)) {
                    callback(time);
                }
            })
                .catch((error) => {
                logMessage(`UdfCompatibleDatafeed: Fail to load server time, error=${getErrorMessage(error)}`);
            });
        }
        searchSymbols(userInput, exchange, symbolType, onResult) {
            if (this._configuration.supports_search) {
                const params = {
                    limit: 30 /* Constants.SearchItemsLimit */,
                    query: userInput.toUpperCase(),
                    type: symbolType,
                    exchange: exchange,
                };
                this._send('search', params)
                    .then((response) => {
                    if (response.s !== undefined) {
                        logMessage(`UdfCompatibleDatafeed: search symbols error=${response.errmsg}`);
                        onResult([]);
                        return;
                    }
                    onResult(response);
                })
                    .catch((reason) => {
                    logMessage(`UdfCompatibleDatafeed: Search symbols for '${userInput}' failed. Error=${getErrorMessage(reason)}`);
                    onResult([]);
                });
            }
            else {
                if (this._symbolsStorage === null) {
                    throw new Error('UdfCompatibleDatafeed: inconsistent configuration (symbols storage)');
                }
                this._symbolsStorage.searchSymbols(userInput, exchange, symbolType, 30 /* Constants.SearchItemsLimit */)
                    .then(onResult)
                    .catch(onResult.bind(null, []));
            }
        }
        resolveSymbol(symbolName, onResolve, onError, extension) {
            const currencyCode = extension && extension.currencyCode;
            const unitId = extension && extension.unitId;
            function onResultReady(symbolInfo) {
                onResolve(symbolInfo);
            }
            if (!this._configuration.supports_group_request) {
                const params = {
                    symbol: symbolName,
                };
                if (currencyCode !== undefined) {
                    params.currencyCode = currencyCode;
                }
                if (unitId !== undefined) {
                    params.unitId = unitId;
                }
                this._send('symbols', params)
                    .then((response) => {
                    var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m, _o, _p, _q, _r, _s, _t, _u, _v, _w, _x, _y, _z, _0, _1, _2;
                    if (response.s !== undefined) {
                        onError('unknown_symbol');
                    }
                    else {
                        const symbol = response.name;
                        const listedExchange = (_a = response.listed_exchange) !== null && _a !== void 0 ? _a : response['exchange-listed'];
                        const tradedExchange = (_b = response.exchange) !== null && _b !== void 0 ? _b : response['exchange-traded'];
                        const fullName = (_c = response.full_name) !== null && _c !== void 0 ? _c : `${tradedExchange}:${symbol}`;
                        const result = {
                            ...response,
                            name: symbol,
                            base_name: [listedExchange + ':' + symbol],
                            full_name: fullName,
                            listed_exchange: listedExchange,
                            exchange: tradedExchange,
                            currency_code: (_d = response.currency_code) !== null && _d !== void 0 ? _d : response['currency-code'],
                            original_currency_code: (_e = response.original_currency_code) !== null && _e !== void 0 ? _e : response['original-currency-code'],
                            unit_id: (_f = response.unit_id) !== null && _f !== void 0 ? _f : response['unit-id'],
                            original_unit_id: (_g = response.original_unit_id) !== null && _g !== void 0 ? _g : response['original-unit-id'],
                            unit_conversion_types: (_h = response.unit_conversion_types) !== null && _h !== void 0 ? _h : response['unit-conversion-types'],
                            has_intraday: (_k = (_j = response.has_intraday) !== null && _j !== void 0 ? _j : response['has-intraday']) !== null && _k !== void 0 ? _k : false,
                            visible_plots_set: (_l = response.visible_plots_set) !== null && _l !== void 0 ? _l : response['visible-plots-set'],
                            minmov: (_o = (_m = response.minmovement) !== null && _m !== void 0 ? _m : response.minmov) !== null && _o !== void 0 ? _o : 0,
                            minmove2: (_p = response.minmovement2) !== null && _p !== void 0 ? _p : response.minmove2,
                            session: (_q = response.session) !== null && _q !== void 0 ? _q : response['session-regular'],
                            session_holidays: (_r = response.session_holidays) !== null && _r !== void 0 ? _r : response['session-holidays'],
                            supported_resolutions: (_u = (_t = (_s = response.supported_resolutions) !== null && _s !== void 0 ? _s : response['supported-resolutions']) !== null && _t !== void 0 ? _t : this._configuration.supported_resolutions) !== null && _u !== void 0 ? _u : [],
                            has_daily: (_w = (_v = response.has_daily) !== null && _v !== void 0 ? _v : response['has-daily']) !== null && _w !== void 0 ? _w : true,
                            intraday_multipliers: (_y = (_x = response.intraday_multipliers) !== null && _x !== void 0 ? _x : response['intraday-multipliers']) !== null && _y !== void 0 ? _y : ['1', '5', '15', '30', '60'],
                            has_weekly_and_monthly: (_z = response.has_weekly_and_monthly) !== null && _z !== void 0 ? _z : response['has-weekly-and-monthly'],
                            has_empty_bars: (_0 = response.has_empty_bars) !== null && _0 !== void 0 ? _0 : response['has-empty-bars'],
                            volume_precision: (_1 = response.volume_precision) !== null && _1 !== void 0 ? _1 : response['volume-precision'],
                            format: (_2 = response.format) !== null && _2 !== void 0 ? _2 : 'price',
                        };
                        onResultReady(result);
                    }
                })
                    .catch((reason) => {
                    logMessage(`UdfCompatibleDatafeed: Error resolving symbol: ${getErrorMessage(reason)}`);
                    onError('unknown_symbol');
                });
            }
            else {
                if (this._symbolsStorage === null) {
                    throw new Error('UdfCompatibleDatafeed: inconsistent configuration (symbols storage)');
                }
                this._symbolsStorage.resolveSymbol(symbolName, currencyCode, unitId).then(onResultReady).catch(onError);
            }
        }
        getBars(symbolInfo, resolution, periodParams, onResult, onError) {
            this._historyProvider.getBars(symbolInfo, resolution, periodParams)
                .then((result) => {
                onResult(result.bars, result.meta);
            })
                .catch(onError);
        }
        subscribeBars(symbolInfo, resolution, onTick, listenerGuid, _onResetCacheNeededCallback) {
            this._dataPulseProvider.subscribeBars(symbolInfo, resolution, onTick, listenerGuid);
        }
        unsubscribeBars(listenerGuid) {
            this._dataPulseProvider.unsubscribeBars(listenerGuid);
        }
        _requestConfiguration() {
            return this._send('config')
                .catch((reason) => {
                logMessage(`UdfCompatibleDatafeed: Cannot get datafeed configuration - use default, error=${getErrorMessage(reason)}`);
                return null;
            });
        }
        _send(urlPath, params) {
            return this._requester.sendRequest(this._datafeedURL, urlPath, params);
        }
        _setupWithConfiguration(configurationData) {
            this._configuration = configurationData;
            if (configurationData.exchanges === undefined) {
                configurationData.exchanges = [];
            }
            if (!configurationData.supports_search && !configurationData.supports_group_request) {
                throw new Error('Unsupported datafeed configuration. Must either support search, or support group request');
            }
            if (configurationData.supports_group_request || !configurationData.supports_search) {
                this._symbolsStorage = new SymbolsStorage(this._datafeedURL, configurationData.supported_resolutions || [], this._requester);
            }
            logMessage(`UdfCompatibleDatafeed: Initialized with ${JSON.stringify(configurationData)}`);
        }
    }
    function defaultConfiguration() {
        return {
            supports_search: false,
            supports_group_request: true,
            supported_resolutions: [
                '1',
                '5',
                '15',
                '30',
                '60',
                '1D',
                '1W',
                '1M',
            ],
            supports_marks: false,
            supports_timescale_marks: false,
        };
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
                    if (response.s === 'ok') {
                        resolve(response.d);
                    }
                    else {
                        reject(response.errmsg);
                    }
                })
                    .catch((error) => {
                    const errorMessage = getErrorMessage(error);
                    reject(`network error: ${errorMessage}`);
                });
            });
        }
    }

    class Requester {
        constructor(headers) {
            if (headers) {
                this._headers = headers;
            }
        }
        sendRequest(datafeedUrl, urlPath, params) {
            if (params !== undefined) {
                const paramKeys = Object.keys(params);
                if (paramKeys.length !== 0) {
                    urlPath += '?';
                }
                urlPath += paramKeys.map((key) => {
                    return `${encodeURIComponent(key)}=${encodeURIComponent(params[key].toString())}`;
                }).join('&');
            }
            // Send user cookies if the URL is on the same origin as the calling script.
            const options = { credentials: 'same-origin' };
            if (this._headers !== undefined) {
                options.headers = this._headers;
            }
            // eslint-disable-next-line no-restricted-globals
            return fetch(`${datafeedUrl}/${urlPath}`, options)
                .then((response) => response.text())
                .then((responseTest) => JSON.parse(responseTest));
        }
    }

    class UDFCompatibleDatafeed extends UDFCompatibleDatafeedBase {
        constructor(datafeedURL, updateFrequency = 10 * 1000, limitedServerResponse) {
            const requester = new Requester();
            const quotesProvider = new QuotesProvider(datafeedURL, requester);
            super(datafeedURL, quotesProvider, requester, updateFrequency, limitedServerResponse);
        }
    }

    exports.UDFCompatibleDatafeed = UDFCompatibleDatafeed;

    Object.defineProperty(exports, '__esModule', { value: true });

})));
