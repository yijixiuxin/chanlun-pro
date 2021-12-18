from django.conf.urls import url
from . import views
from . import views_trader
from . import views_stock
from . import views_currency
from . import views_back

urlpatterns = [

    url(r'^search/$', views_stock.SearchStocks, name='search'),

    # 股票系统
    url(r'^stock_index/$', views_stock.stock_index_show, name='stock'),
    url(r'^stock_kline/$', views_stock.stock_kline_show, name='stock'),
    url(r'^stock_zixuan/$', views_stock.stock_my_zixuan, name='stock'),
    url(r'^stock_jhs/$', views_stock.stock_jhs, name='stock'),
    url(r'^stock_plate/$', views_stock.stock_plate, name='stock'),
    url(r'^plate_stocks/$', views_stock.plate_stocks, name='stock'),
    url(r'^dl_ranks_show/$', views_stock.dl_ranks_show, name='stock'),
    url(r'^dl_hy_ranks_save/$', views_stock.dl_hy_ranks_save, name='stock'),
    url(r'^dl_gn_ranks_save/$', views_stock.dl_gn_ranks_save, name='stock'),

    # 数字货币
    url(r'^currency_index/$', views_currency.currency_index_view, name='currency'),
    url(r'^currency_kline/$', views_currency.currency_kline_show, name='currency'),
    url(r'^currency_trade_open/$', views_currency.currency_trade_open, name='currency'),
    url(r'^currency_trade_close/$', views_currency.currency_trade_close, name='currency'),
    url(r'^currency_jhs/$', views_currency.currency_jhs, name='currency'),
    url(r'^currency_opt_records/$', views_currency.currency_opt_records, name='currency'),
    url(r'^currency_positions/$', views_currency.currency_positions, name='currency'),
    url(r'^currency_pos_loss_price_save/$', views_currency.currency_pos_loss_price_save, name='currency'),
    url(r'^currency_pos_profit_rate_save/$', views_currency.currency_pos_profit_rate_save, name='currency'),
    url(r'^currency_pos_check_set_save/$', views_currency.currency_pos_check_set_save, name='currency'),
    url(r'^currency_pos_check_set_del/$', views_currency.currency_pos_check_set_del, name='currency'),
    url(r'^currency_open_buysell/$', views_currency.currency_open_buysell, name='currency'),
    url(r'^currency_open_buysell_del/$', views_currency.currency_open_buysell_del, name='currency'),
    url(r'^currency_open_buysell_save/$', views_currency.currency_open_buysell_save, name='currency'),

    # 回放系统
    url(r'^re_record/$', views_back.ReRecordKlineIndexView, name='rerecords'),
    url(r'^re_record/klines$', views_back.ReRecordKlines, name='rerecords'),

    # 多图表
    url(r'^more/charts$', views.MoreChartsView, name='mores'),
    url(r'^more/kline$', views.ChartKline, name='mores'),

    # 自定义时间 Kline 图表
    url(r'^custom/index$', views.CustomKlineDateView, name='custom'),
    url(r'^custom/kline$', views.CustomDateKline, name='custom'),

    # 交易对象视图
    url(r'^trader/index$', views_trader.trader_view, name='trader'),
    url(r'^strategy_back$', views_trader.StrategyBackIndex, name='strategy'),
    url(r'^strategy_back/kline$', views_trader.StragegyBackKline, name='strategy'),
]

