# 安装文档

---

> ### 前期准备：
>
> 富途账号（使用其获取股票列表、行业概念板块信息、自选股等功能）开户即可，不需要入金；   
> 项目地址：https://www.futunn.com/OpenAPI
>
> 
> 天勤期货账号（使用期期货行情数据），免费版只能获取期货数据，并且不可以查看历史数据，注册账户即可使用；  
> 
> 官网地址：https://www.shinnytech.com/   
> 文档地址：https://doc.shinnytech.com/tqsdk/latest/quickstart.html
> 
> 
> 美股行情服务：（二选一即可）
> 
> https://polygon.io/   
> https://alpaca.markets/
>
> 
> 钉钉账号（使用其接收消息）  
> 官网地址：https://www.dingtalk.com/  
> 自定义机器人接入文档：https://open.dingtalk.com/document/robots/custom-robot-access
>
> 
> VPN 工具（用于获取数字货币行情数据）  
> 推荐工具：[V2free](https://w1.ddnsgo.xyz/auth/register?code=RFb5)

1. 安装 [Anaconda](https://www.anaconda.com/products/individual)
   or  [miniconda](https://docs.conda.io/en/latest/miniconda.html) ，创建运行环境，Python 版本 3.7

``` 
# 下载 Anaconda 并安装，打开 Anaconda Shell 并执行以下命令  
conda create -y -n chanlun python=3.7
```

2. 安装 MySQL、Redis 数据库

```
# Mac OS
brew install redis
brew install mariadb

# Ubuntu
apt-get install redis-server
apt-get install mariadb-server
    
# Windows
自行百度搜索吧
Redis 官网：https://redis.io/download
MariaDB 官网：https://mariadb.org/download/

```

3. 创建 MySQL 数据库，并创建项目使用的用户

```
CREATE DATABASE chanlun_klines;
CREATE USER 'chanlun'@'127.0.0.1' IDENTIFIED BY '123456';
GRANT ALL ON chanlun_klines.* TO 'chanlun'@'127.0.0.1';
flush privileges;
```

4. 克隆项目到本地

```
git clone https://github.com/yijixiuxin/chanlun-pro.git

# 进入项目根目录
cd chanlun-pro

```

5. 设置 PYTHONPATH 环境变量

```
# Linux & Mac

vim ~/.bashrc
# 在最后一行增加  project_path 替换成项目所在的目录
export PYTHONPATH=$PYTHONPATH:/project_path/chanlun-pro/src:

source ~/.bashrc

# Windows

我的电脑 -> 右键菜单选“属性” -> 高级系统设置 -> 高级 -> 环境变量 -> 系统变量 -> 新建

# 系统变量信息，project_path 需要替换成项目所在目录
变量名：PYTHONPATH
变量值：project_path\chanlun-pro\src

设置完成后，重启终端 ，输入命令 $env:PYTHONPATH  查看是否设置成功

```

6. pip 安装项目依赖包

```
    # 切换到新创建的 chanlun 环境  
    conda activate chanlun
    
    # conda 安装相关的库会比较方便
    conda install -y pandas requests numpy redis matplotlib pymysql  
    conda install -y -c conda-forge ta-lib  ipywidgets  
    
    # PIP 安装项目依赖包
    pip3 config set global.index-url https://mirrors.aliyun.com/pypi/simple/
    pip3 install -r requirements.txt
    
    # 安装 pytdx 包
    pip3 install wheel
    pip3 install package/pytdx-1.72r2-py3-none-any.whl
```

7. 使用 Anaconda 安装 JupyterLab，用于本地进行研究使用（如果只使用 WEB，则不需要安装）

```
    conda install -y -c conda-forge jupyterlab jupyterlab_widgets jupyterlab-lsp jupyterlab_execute_time jupyterlab-language-pack-zh-CN
    jupyter-lab  # 启动
```

8. 开启 FutuOpenD 并登录，以便项目调用其中的富途 API，用于请求股票信息

9. 在 src/chanlun 目录， 复制拷贝 config.py.demo 文件为 config.py ，用于配置整个项目的配置参数；并修改其中的配置项为自己的

10. 执行以下命令获取本机 mac 地址，并发送给作者，获取授权许可文件，并放置在项目中的 src/pytransform 目录下

```
    pip install pyarmor==7.7.4
    pyarmor hdinfo
    # 将 Default Mac address: "****"  内容发送给作者，获取授权文件
```

12. 到 web/chanlun_web 目录，启动 web 服务

```
    cd web/chanlun_web
    python manage.py runserver 0.0.0.0:8000
```

8. 浏览器访问 http://127.0.0.1:8000/ 即可显示缠论解缠主页

### ~~Docker 使用说明~~

暂时不维护了

> Docker 相关命令文档 https://www.runoob.com/docker/docker-command-manual.html

1. 平台安装 Docker [下载](https://www.docker.com/products/docker-desktop)
2. 启动 Docker 服务
3. 构建 Docker 镜像

```
# 进入项目中的 docker 目录，执行以下命令
docker build -t chanlun
```

4. 根据镜像启动容器；project_code_path 需要替换为项目代码地址

```
    docker run -itd -p 8000:8000 -p 8888:8888 -p 3306:3306 -v /project_code_path:/root/app chanlun-pro
```

4. 访问以下地址进行访问

```
    http://127.0.0.1:8000  项目主页    
    http://127.0.0.1:8888   研究环境 (token：262468670f9a00b51e3f93b0955a0bdfdcba7ba3e8b821c5)
```

### 研究环境

克隆代码到本地，配置好相应的配置，开启本地 JupyterLab  
浏览器访问 ： http://127.0.0.1:8888/lab/tree

项目中 notebook 中有写好的一些实例，可直接运行查看，并按需要修改测试

### WEB 网页

到 web/chanlun_web 目录，运行 python manage.py runserver 0.0.0.0:8000 启动 web 服务   
访问：http://127.0.0.1:8000/

期货行情使用 天勤SDK 获取；

数字货币使用 ccxt 包，默认获取的是 Binance 的 USDT永续合约 行情；

按钮 "导入股票代码" 功能，则可以导入自定义的股票列表到页面中，格式为 JSON 数据，示例：
> [{"code": "SH.600711", "name": "盛屯矿业"}, {"code": "SZ.300363", "name": "博腾股份"}, {"code": "SZ.002192", "name": "融捷股份"}, {"code": "SH.603259", "name": "药明康德"}, {"code": "SH.688315", "name": "诺禾致源"}, {"code": "SH.600392", "name": "盛和资源"}, {"code": "SH.600111", "name": "北方稀土"}, {"code": "SZ.002010", "name": "传化智联"}]

页面中行情默认不自动刷新，有按钮 "开启自动刷新" 可手动开启，Javascript 会轮训方式请求行情并刷新。

动量排行页面，需要使用 聚宽平台 运行出的结果，复制后手动新增到项目中，才可正常进行展示；  
（聚宽平台代码在项目 joinquant 目录中）
