from django.conf.urls import url
from . import views

urlpatterns = [

    url(r'^$', views.stock_index_show, name='demo'),
    url(r'^currency/$', views.currency_index_show, name='demo'),
    url(r'^cl_klines/$', views.query_cl_klines, name='demo'),

    url(r'^search_code/$', views.search_stock_code, name='demo'),

]

