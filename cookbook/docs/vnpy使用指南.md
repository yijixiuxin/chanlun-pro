## VNPY 使用指南

---

> 不建议直接修改项目提供的策略    
> 
> 写自己的策略，以 my_ 开头来命名自己的策略文件名称（不会被 git 添加到仓库）

### 安装相关包

        pip install vnpy
        pip install vnpy_ctp
        pip install vnpy_ctastrategy
        pip install vnpy_ctabacktester
        pip install vnpy_portfoliostrategy
        pip install vnpy_datamanager
        pip install vnpy_tushare
        pip install vnpy_udata
        pip install vnpy_tqsdk
        pip install vnpy_mysql

### 设置环境变量

在 `conda` 环境，启动 vnpy 窗口，需要设置的环境变量，`${anaconda3}` 替换成自己 `anaconda3` 的地址

        QT_QPA_PLATFORM_PLUGIN_PATH
        ${anaconda3}\envs\chanlun\Lib\site-packages\PySide6\plugins\platforms

### VNPY 配置修改

修改 `chanlun_pro\src\cl_vnpy\.vntrader\vt_setting.json` 配置文件

修改 `C:\Users\${电脑用户名}\.vntrader\vt_setting.json` 配置文件 （使用 `Jupyterlab` 执行读取的是这个配置）

*没有以上目录和文件，执行下 `run_app.py`，在窗口进行配置并保存，也可以完成配置文件的变更*

    ### 数据源配置
    ### https://www.vnpy.com/docs/cn/datafeed.html
    "datafeed.name": "tqsdk",
    "datafeed.username": "******",
    "datafeed.password": "******",

    ### 数据库配置
    ###　https://www.vnpy.com/docs/cn/database.html
    "database.name": "mysql",
    "database.database": "vnpy",
    "database.host": "127.0.0.1",
    "database.port": 3306,
    "database.user": "root",
    "database.password": "123456"

### 导入或下载历史数据

* 通过配置的数据服务，通过窗口自行下载所需数据
* 通过 QQ群 文件，下载 vnpy 数据，并导入到数据库 [历史行情下载](历史行情下载?id=qq群下载并导入历史行情数据)

### 策略编写

策略文件放在 `src/cl_vnpy/strategies` 目录下

* 使用 vnpy 原生的方法，继承 `CtaTemplate` 进行策略的编写，教程参考 vnpy 官网文档
* 使用 chanlun 中的策略，通过 vnpy 进行执行，通过继承 `BaseStrategy` 来编写

继承 `BaseStrategy` ，基本只需要重写 `__init__` 方法即可；

重要的是设置以下类成员：

        self.cl_config : 缠论计算的配置
        self.STR ：缠论的策略类
        self.intervals : 运行的周期，通过 1m （实盘是 tick） 合成特定周期的bar，并调用回调方法进行数据更新

其他的具体看代码吧

#### TODO List：

* 执行效率还需要优化

### 策略的执行

* 使用可视化窗口进行策略回测，执行 `cl_vnpy/run_app.py`，启动 vnpy 并进行回测
* 脚本执行，执行 `cl_vnpy/run_bt.py`，进行回测
* JupyterLab 进行回测，文件 `noteboot/VNPY_期货回测.ipynb`

### 实盘策略执行

参考 vnpy 官网文档吧

https://www.vnpy.com/docs/cn/gateway.html#id3

Simnow 网站：https://www.simnow.com.cn/product.action