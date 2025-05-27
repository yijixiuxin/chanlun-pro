## 支持的交易市场

---

### 最优个市场交易数据平台

1. 沪深A股，推荐使用 QMT 数据源；
2. 期货数据，推荐使用天勤 tqsdk 数据源；
3. 美股数据，推荐自费购买无延迟的行情数据；


### 新增自定义交易市场

项目中支持的交易市场，在 `src/chanlun/exchange` 目录，新增 自己的交易市场（数据源）；

只需要继承 `Exchange` 基类，并实现其中的

> `default_code` : 在WEB展示时，默认展示的代码
>
> `support_frequencys` ： 返回交易所支持的周期；例如 ：{'d': 'Day'} 内部使用的周期是 d，在web端展示 Day
>
> `all_stocks` : 获取市场支持的所有标的
>
> `klines` : 获取标的指定周期的行情数据
>
> `ticks` : 获取标的的最新价格与涨跌幅

五个方法，即可进行基础的使用。

### ExchangeBaostock

* 市场：沪深A股
* 数据源: http://baostock.com/
* 交易：不支持
* 文件：`src/chanlun/exchange/exchange_baostock.py`

证券宝 www.baostock.com 是一个免费、开源的证券数据平台，优点是有完整的历史行情数据，缺点是无实时数据，无指数分钟数据；

主要用于历史行情下载；

### ExchangeTDX

* 市场：沪深A股
* 数据源：通达信
* 交易：不支持
* 文件：`src/chanlun/exchange/exchange_tdx.py`

基于 pytdx 包获取 通达信 行情数据，优点是快速的实时数据，缺点是获取历史行情数据比较困难，自己计算的复权数据，与平台会有差异。

### ExchangeQMT

* 市场：沪深A股
* 数据源：MiniQMT
* 交易：支持
* 文件：`src/chanlun/exchange/exchange_qmt.py`

需要券商开通权限；

文档：http://dict.thinktrader.net/nativeApi/start_now.html

使用前提：下载 xtquant，解压后， 并放入 chanlun-pro/src 目录下，并打开 QMT交易端（极简模式）

使用提示：第一次访问行情，系统会下载该标的的历史行情，第一次比较慢，尤其是低级别数据比较多，需要多等待，后续再次访问就快了。


### ExchangeTDXFutures

* 市场：国内期货
* 数据源：通达信
* 交易：不支持
* 文件：`src/chanlun/exchange/exchange_tdx_futures.py`

基于 pytdx 包获取 通达信 国内期货行情数据，实时数据


### ExchangeTDXNYFutures

* 市场：纽约期货
* 数据源：通达信
* 交易：不支持
* 文件：`src/chanlun/exchange/exchange_tdx_ny_futures.py`

基于 pytdx 包获取 通达信 纽约期货行情数据，10分钟延迟数据


### ExchangeTDXHK

* 市场：港股
* 数据源：通达信
* 交易：不支持
* 文件：`src/chanlun/exchange/exchange_tdx_hk.py`

基于 pytdx 包获取 通达信 港股行情数据，有 15 分钟延迟。

### ExchangeTDXUS

* 市场：美股
* 数据源：通达信
* 交易：不支持
* 文件：`src/chanlun/exchange/exchange_tdx_us.py`

基于 pytdx 包获取 通达信 美股行情数据，有 15 分钟延迟。

### ExchangeTDXFX

* 市场：外汇
* 数据源：通达信
* 交易：不支持
* 文件：`src/chanlun/exchange/exchange_tdx_fx.py`

基于 pytdx 包获取 通达信 外汇行情数据


### ExchangeFutu

* 市场：沪深A股、港股
* 数据源：富途
* 交易：支持港股交易
* 文件：`src/chanlun/exchange/exchange_futu.py`

富途开发的 API 接口，优点实时数据与历史数据，可复权，可实盘交易港股；缺点是限制账户订阅最大标的数量。

### ExchangeTq

* 市场：期货
* 数据源：天勤
* 交易：支持期货交易
* 文件：`src/chanlun/exchange/exchange_tq.py`

天勤开源的 期货API 接口；优点实时的期货数据，可实盘交易；缺点是历史行情需要专业版才可获取。

### ExchangePolygon

* 市场：美股
* 数据源：Polygon
* 交易：不支持
* 文件：`src/chanlun/exchange/exchange_polygon.py`

美股市场行情服务，用的不多，可自行到官网了解：https://polygon.io/

### ExchangeAlpaca

* 市场：美股
* 数据源：Alpaca
* 交易：不支持
* 文件：`src/chanlun/exchange/exchange_alpaca.py`

美股市场行情服务，用的不多，可自行到官网了解：https://alpaca.markets/

### ExchangeIB

* 市场：美股
* 数据源：盈透证券
* 交易：支持
* 文件：`src/chanlun/exchange/exchange_ib.py`

盈透证券 TWS API 行情服务，需要开户入金，获取行情需要付费订阅。

### ExchangeBinance

* 市场：数字货币
* 数据源：Binance
* 交易：支持 USDT 永续合约
* 文件：`src/chanlun/exchange/exchange_binance.py`

可获取 Binance 交易所 USDT 永续合约行情

### ExchangeZB

* 市场：数字货币
* 数据源：ZB
* 交易：支持现货
* 文件：`src/chanlun/exchange/exchange_zb.py`

ZB 交易所现货行情接口

### ExchangeDB

* 市场：全市场
* 数据源：数据库
* 交易：不支持
* 文件：`src/chanlun/exchange/exchange_db.py`

将 API 获取到的历史行情数据写入到数据库，并提供查询服务