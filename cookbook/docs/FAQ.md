## FAQ 常见问题

---

### 如何获取当前网卡地址？

通过 getmac / ifconfig 等命令获取网卡地址会获取多个，不清楚到底哪一个是正确的，可使用一下命令获取

    pip install pyarmor
    python -m pyarmor.cli.hdinfo

将输出内容发送给作者，获取授权文件

### 运行报错 License is not for this machine

许可文件绑定的网卡地址错误，可通过以上获取网卡地址的命令，重新获取网卡地址进行授权即可。

### 沪深A股的 行业/概念 板块更新

> 如果有配置 config.py 中的 TDX_PATH 配置项，则通过读取通达信本地文件获取 行业/概念 信息，无需执行以下脚本 

项目中的行业/概念板块，采用同花顺的板块与概念数据，数据通过脚本采集更新，并保存在本地 json 文件中，需要手动定时去更新

    Python 文件 ：src/chanlun/exchange/stocks_bkgn.py
    JSON 文件：src/chanlun/exchange/stocks_bkgn.json
    
    # 更新方式：python  stocks_bkgn.py 即可

### 如何获取 v2rayN 的 HTTP 代理 IP 和端口？

1. 打开 v2rayN 客户端
2. 在系统托盘找到 v2rayN 图标，双击打开
3. 在顶部菜单选择 "设置" -> "参数设置" -> "基础设置"
4. 在界面中，找到 "本地监听端口" 部分，进行端口设置
5. 在主界面左下角，有如下显示：本地:[stocks:10808] [http(系统代理):10809]...
6. 默认的代理地址通常是：
   - IP: 127.0.0.1
   - 端口: 10809  

注意：如果修改过默认端口，请使用您设置的实际端口号；使用 http 端口，不要用 stocks 端口。