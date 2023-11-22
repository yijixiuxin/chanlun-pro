# 掘金量化使用文档

---

官网：https://www.myquant.cn/

文档：https://www.myquant.cn/docs/python/43

### 安装

下载掘金量化 3.0 客户端并安装（目前仅支持windows，可远程）

打开 掘金量化 -> 系统设置

在SDK语言，Python 语言找到项目创建的 3.10 的 Python 版本，点击安装

之后会列出当前已安装的所有 Python 环境列表，其中找到 3.10 版本，路径为 `chanlun\python.exe` 一行，点击后面 一键安装，并设置为
默认解释器

安装完成就可以使用了，在使用过程中要一直开启 掘金量化，不可关闭

### 使用

1. 在掘金量化客户端中的 量化研究 中进行策略编写与执行
2. 在项目中的 `src/cl_myquant` 目录编写策略并执行，在 掘金量化 客户端查看策略直接结果

> 在 config.py 配置中，设置 掘金的 token

        # 掘金设置 https://www.myquant.cn/docs2/faq/#%E5%A6%82%E4%BD%95%E4%BD%BF%E7%94%A8-linux-%E7%89%88%E6%9C%AC%E7%9A%84-python-sdk
        GM_SERVER_ADDR = '127.0.0.1:7001'
        GM_TOKEN = '******'