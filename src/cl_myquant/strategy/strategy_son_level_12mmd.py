from __future__ import print_function, absolute_import

from chanlun.exchange.exchange import convert_stock_kline_frequency
from chanlun.strategy import strategy_son_level_1mmd
from cl_myquant.base import *

import time
import traceback

# 定义全局变量
market_data: MyQuantData  # 数据对象
trader: MyQuantTrader  # 交易对象
strategy: Strategy  # 策略对象
# 使用的缠论配置
cl_config = {
    'fx_bh': 'fx_bh_yes', 'fx_qj': 'fx_qj_k',
    'bi_type': 'bi_type_old', 'bi_bzh': 'bi_bzh_yes', 'bi_qj': 'bi_qj_dd', 'bi_fx_cgd': 'bi_fx_cgd_no',
    'xd_bzh': 'xd_bzh_yes', 'zsd_bzh': 'zsd_bzh_yes', 'xd_qj': 'xd_qj_dd', 'zsd_qj': 'zsd_qj_dd',
    'zs_bi_type': 'zs_type_fx', 'zs_xd_type': 'zs_type_dn',
    'zs_wzgx': 'zs_wzgx_zggdd', 'zs_qj': 'zs_qj_ck'
}


def init(context):
    global market_data, trader, strategy
    # 初始化数据对象、策略、交易对象
    market_data = MyQuantData(context, ['1d', '1800s', '300s'], cl_config)
    strategy = strategy_son_level_1mmd.StrategySonLevel1MMD()
    trader = MyQuantTrader('a', context)
    trader.set_data(market_data)
    trader.set_strategy(strategy)

    # 每月执行一遍选股
    schedule(schedule_func=xuangu_macd, date_rule='1m', time_rule='9:20:00')


def xuangu_macd(context: Context):
    """
    根据月线MACD 柱子是红的选择
    """
    global market_data

    s_time = time.time()

    # 查询所有股票标的
    instruments = get_instruments(
        symbols=None,
        exchanges=['SHSE', 'SZSE'],
        sec_types=SEC_TYPE_STOCK,
        names=None, skip_suspended=True, skip_st=True, fields=None, df=False
    )
    # 上市日期要大于 1 年
    symbols = [
        _i['symbol'] for _i in instruments
        if _i['listed_date'] < context.now - datetime.timedelta(days=364)
    ]
    print(f'获取所有上市大于1年的股票标的数量{len(symbols)}')

    # 查获取市值大于100亿的股票列表
    fundamentals = get_fundamentals_n(
        'trading_derivative_indicator', symbols,
        context.now,
        fields='TOTMKTCAP', filter=None, count=1,
        df=False)

    fundamentals = sorted(fundamentals, key=lambda f: f['TOTMKTCAP'], reverse=True)
    symbols = [_f['symbol'] for _f in fundamentals if _f['TOTMKTCAP'] > 10000000000]
    print(f'获取市值大于100亿的股票列表：{len(symbols)}')

    # 计算月线MACD 是红柱子的股票
    macd_up_symbols = []
    for symbol in symbols:
        try:
            _macd_s_time = time.time()
            klines = history_n(symbol=symbol, frequency='1d', count=5000, end_time=context.now,
                               fields='symbol,eob,open,high,low,close,cum_volume',
                               adjust=ADJUST_PREV, df=True)
            klines.loc[:, 'code'] = klines['symbol']
            klines.loc[:, 'date'] = pd.to_datetime(klines['eob'])
            klines.loc[:, 'volume'] = klines['cum_volume']
            klines = klines[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]
            klines = convert_stock_kline_frequency(klines, 'm')
            closes = klines['close'].tolist()
            if len(closes) <= 30:
                continue
            macd_dif, macd_dea, macd_hist = talib.MACD(np.array(closes), fastperiod=12, slowperiod=26, signalperiod=9)
            if macd_hist[-1] is not None and macd_hist[-1] > 0:
                macd_up_symbols.append(symbol)
            # print(f'{symbol} cal macd use time : {(time.time() - _macd_s_time)}')
        except Exception as e:
            print(f'{symbol} - 异常： {e}')
            print(traceback.format_exc())

    print('Macd 红柱子股票数量：', len(macd_up_symbols))

    # 取合集
    symbols = list(set(symbols) & set(macd_up_symbols))
    print('最终获取股票合集：', len(symbols))

    # 查询当前持仓
    positions = context.account().positions()
    pos_symbols = [_p['symbol'] for _p in positions if _p['amount'] > 0]
    print(f'当前持仓股票列表：{pos_symbols}')

    symbols = symbols + pos_symbols
    symbols = list(set(symbols))

    # 进行重新订阅（取消之前的订阅）
    subscribe(symbols=symbols, frequency='1d', count=2000)
    subscribe(symbols=symbols, frequency='1800s', count=2000)
    subscribe(symbols=symbols, frequency='300s', count=2000)

    # 初始化缠论数据
    market_data.init_cl_datas(symbols, ['1d', '1800s', '300s'])

    print('选股总耗时：', time.time() - s_time)

    return symbols


def on_bar(context, bars):
    global market_data, trader
    # print(f"on bar : {bars[0]['symbol']} {bars[0]['eob']}")
    # 更新缠论数据
    market_data.update_bars(bars)

    # 执行策略
    trader.run(bars[0]['symbol'])
