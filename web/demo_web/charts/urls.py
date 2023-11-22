from django.urls import path

from . import views
from . import views_tv

urlpatterns = [

    # 股票相关url配置
    path('', views.index_show),
    path('stock/kline', views.kline_chart),

    # TradingView 行情图表
    path('tv_chart', views_tv.index_show),
    path('tv/config', views_tv.config),
    path('tv/symbol_info', views_tv.symbol_info),
    path('tv/symbols', views_tv.symbols),
    path('tv/search', views_tv.search),
    path('tv/history', views_tv.history),
    path('tv/time', views_tv.time),
    path('tv/1.1/charts', views_tv.charts),
    path('tv/1.1/study_templates', views_tv.study_templates),

]
