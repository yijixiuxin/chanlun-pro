# 缠论市场分析工具

---

本项目是基于 缠中说禅 所分享的缠论知识，结合个人理解，用于学习和研究市场行情的分析工具；  
因为个人能力有限，理解可能会有偏差的地方，导致项目所实现的缠论，必然有错误之处；  
所以决定开源出来，希望大家一起学习并加以完善。

[【在线Demo展示】](http://www.chanlun-trader.com/)


缠论图表
![缠论图表demo](img/chanlun_demo.png)

回测结果展示  
![回测结果展示](img/strategy_back_demo.png)
![回测结果展示](img/strategy_back_kline.png)

多图表展示  
![多图表展示](img/more_charts.png)

行情数据回放练习  
![行情数据回放练习](img/hf_back_kline.png)

### 特别说明

项目原本是个人学习与研究缠论所用，并且对于 Python 了解不多，代码用的比较简单，基本没有用到什么高深技术，对于初学 Python 的人来说相对友好，花点时间都能看懂；  
其中 WEB 输入参数，并没有多加校验，完全凭借人之本善加以约束，在使用中还请注意；  
如有问题，可在 issues 中进行提问；  
也可添加我的微信和微信群进行交流（添加请备注：缠论@姓名）

![微信](img/wx.jpg) ![群](img/wx_qun.jpg)


### 项目当前功能

* 缠论图表展示
* 行情数据下载（A股、港股、数字货币）
* 行情回放练习
* 自定义缠论策略进行回测
* 实盘策略交易

[项目更新记录](update.md)

### 安装使用

> 前提：需要事先安装 MySQL、Redis （PS：单纯的研究环境则不太需要）

1. 克隆项目到本地 （git clone https://github.com/yijixiuxin/chanlun.git）
2. 安装 [Anaconda](https://www.anaconda.com/) ，创建运行环境，Python 版本 3.7
3. pip 安装项目依赖包 ( pip install -r requirements.txt )
4. 使用 Anaconda 安装 JupyterLab，用于本地进行研究使用
5. 在 cl_v2 目录，复制拷贝 config.py.demo 文件为 config.py ，用于配置整个项目的配置参数
6. 到 web 目录，运行 python manage.py runserver 0.0.0.0:8000 启动 web 服务
7. 浏览器访问 http://127.0.0.1:8000/charts/stock_index/ 即可显示缠论主页

> 默认 exchange 交易行情，使用的 [baostock](http://baostock.com/baostock/index.php/%E9%A6%96%E9%A1%B5) 服务  
> 可直接创建 exchange 对象进行使用，无需配置 api 等参数
> 缺点是在交易日无当日行情更新，好处是可获取很久之前的历史行情数据
> 在研究环境使用还是很好用的


### Docker 使用说明
1. 平台安装 Docker [下载](https://www.docker.com/products/docker-desktop)
2. 启动 Docker 服务
3. 执行以下命令启动容器；project_code_path 需要替换为项目代码地址

```
    docker run -itd -p 8000:8000 -p 8888:8888 -p 3306:3306 -v /project_code_path:/root/app yijixiuxin/chanlun
```

4. 访问以下地址进行访问

```
    http://127.0.0.1:8000/charts/stock_index  项目主页    
    http://127.0.0.1:8888   研究环境 (token：262468670f9a00b51e3f93b0955a0bdfdcba7ba3e8b821c5)
```

### 配置文件说明

* 代理服务器配置  
如果需要使用数字货币行情，则需要进行配置；   
项目中实现了 币安交易所的 USDT 永续合约行情与交易，可直接配置 API 进行使用；   
数字货币行情与交易，基于 [ccxt](https://github.com/ccxt/ccxt) 包实现，可以很方便进行其他交易所的实现；  
如没有好用的 vpn，推荐我自己使用的 [V2free](https://w1.ddnsgo.xyz/auth/register?code=RFb5) 服务 （PS：使用链接注册，我可以获得返利） 
   
* 富途 API 配置  
我自己主要使用的行情服务，同时也可以进行 港股 的自动化交易，推荐使用；  
[OpenAPI](https://www.futunn.com/download/OpenAPI?lang=zh-CN)  
不足之处就是获取股票 K 线有限制 [API文档](https://openapi.futunn.com/futu-api-doc/intro/authority.html)  
开户用户每月只能获取 100 只股票K线（可多次获取），总资产达1万港币则为 300   
我的用法为：在聚宽平台研究环境筛选符合条件的股票，在使用 富途API 获取行情精选查看，这样 300 个已经够用了。

* Redis、Mysql 配置   
K线 同步到本地，使用 Mysql 进行保存，程序会自动创建表，需要给表创建的权限；  
Redis 用于保存非结构化的一些数据，用于实现一些信息的保存；如 股票盯盘结果查看、动量排行、实盘实例保存等；

* 钉钉消息配置  
用于发送推送消息 [API 文档](https://open.dingtalk.com/document/robots/custom-robot-access)

### 研究环境

克隆代码到本地，配置好相应的配置，开启本地 JupyterLab，浏览器访问 ： http://127.0.0.1:8888/lab/tree   

项目根目录有写好的一些实例，可直接运行查看，并按需要修改测试

### 聚宽平台

缠论计算类 cl.py 可以在聚宽平台直接使用，可以复制代码到聚宽的研究环境中使用；   
项目目录 joinquant 有相应的实例，以及行业、概念板块的动量排行计算；  
可直接上传目录（joinquant）中文件到研究环境中运行：  

> ps: 这里的 cl.py 与项目中 cl_v2/cl.py 是同一个文件

### WEB 网页

到 web 目录，运行 python manage.py runserver 0.0.0.0:8000 启动 web 服务   
访问：http://127.0.0.1:8000/charts/stock_index/

> 本地 Demo 可直接访问  http://127.0.0.1:8000，依赖少，无需 Redis、Mysql 和配置 行情服务器即可运行

> PS：页面只根据我自己的显示屏进行了适配，不保证所有分辨率大小下都能显示正常；   
> 如有显示异常，可自行尝试修改；  
> 如有前端大神愿意优化，也很欢迎提交代码，感激不尽！

web 服务中，股票行情使用 富途API 实现，其中股票列表是基于在富途的自选股列表实现
![股票列表](img/my_stocks_code.png)  
可根据自己需求进行修改，行情服务也可以自定义去实现，继承 Exchange 类并实现相应方法即可； 

页面左上角输入框，支撑股票搜索，输入关键字即可

![股票搜索](img/stock_search.png) 

按钮 "导入" 功能，则可以导入自定义的股票列表到页面中，格式为 JSON 数据，示例：
> [{"code": "SH.600711", "name": "盛屯矿业"}, {"code": "SZ.300363", "name": "博腾股份"}, {"code": "SZ.002192", "name": "融捷股份"}, {"code": "SH.603259", "name": "药明康德"}, {"code": "SH.688315", "name": "诺禾致源"}, {"code": "SH.600392", "name": "盛和资源"}, {"code": "SH.600111", "name": "北方稀土"}, {"code": "SZ.002010", "name": "传化智联"}]

页面中行情默认不自动刷新，有按钮 "开启自动刷新" 可手动开启，Javascript 会轮训方式请求行情并刷新。

动量排行页面，需要使用 聚宽平台 运行出的结果，复制后手动新增到项目中，才可正常进行展示；  
（聚宽平台代码在项目 joinquant 目录中）

交易者页面，显示当前实盘对象的运行情况，按需使用。  

其他功能自行摸索吧，有特别说明的后续在补充......


### 行情数据同步到本地

支持将行情保存到本地 MySQL 数据库，实例可查看根目录中的 [同步行情到数据库.ipynb](同步行情到数据库.ipynb)  
同时也已经实现了 自动化脚本：
* [cl_v2/scripts/reboot_sync_currency_klines.py](cl_v2/scripts/reboot_sync_currency_klines.py) 同步数字货币到数据库
* [cl_v2/scripts/reboot_sync_stock_klines.py](cl_v2/scripts/reboot_sync_stock_klines.py) 同步港股股票数据到数据库

可自行查看具体代码实现

### 策略回测

回测行情数据基于本地 MySQL 中的数据，所以在回测前需要确认本地是否有回测时间段的行情数据

回测的类为 : [cl_v2/trader.py](cl_v2/trader.py)  
策略的类为 : [cl_v2/strategy.py](cl_v2/strategy.py)

自己写的策略，需要继承 Strategy 类，并实现其中的 look、stare 方法
Demo 实例参考 ： [cl_v2/my/strategy_demo.py](cl_v2/my/strategy_demo.py)

回测运行实例参考：[数字货币策略回测.ipynb](数字货币策略回测.ipynb)


### 实盘策略交易

> 当前项目还处于不断完善的过程，其中核心的 cl.py 文件肯定会有变动，这样会导致之前回测的信号发生变化；  
> 如果用于实盘，代码要与研究环境分开，避免因为代码更新，导致实盘信号与之前回测有偏差，导致亏损。


策略经过回测验证，满足自己要求，即可接入实盘进行交易；  
实盘的交易对像需要继承 [cl_v2/trader.py](cl_v2/trader.py)    
实现其中的 open_buy/open_sell/close_buy/close_sell 交易方法即可  
参看：[trader_currency.py](cl_v2/my/trader_currency.py) 数字货币 [trader_hk_stock.py](/cl_v2/my/trader_hk_stock.py) 港股交易

实盘运行脚本在 strategy_trader 目录下   
参看：  
[reboot_trader_currency.py](cl_v2/strategy_trader/reboot_trader_currency.py) 数字货币自动交易者

[reboot_trader_currency.py](cl_v2/strategy_trader/reboot_trader_currency.py) 数字货币自动交易者


### 赞助
开发维护不易，如果觉得项目对你有帮助，还请多多支持

![微信支付](img/wx_pay.jpg)

BTC : 36Rc4vuVL6ogJrw6SMsjPMqEHGmY1SBxYi  
ETH : 0xEae26eaaa3b7ce6E7b0B991Ca14C8Ed744ECd21f
