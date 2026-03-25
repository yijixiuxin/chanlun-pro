### Mac 安装文档

---

> Python 版本支持 3.9、3.10、3.11 ，不然运行会报 RuntimeError 错误
> 前置条件
> 已经安装 git、MySQL、Redis

### 方式一：一键安装 (推荐)

使用 uv 自动管理依赖，更简单快捷。

```bash
# 克隆项目
git clone https://gitee.com/wang-student/chanlun-pro.git
cd chanlun-pro

# 运行安装脚本
chmod +x install_mac.sh
./install_mac.sh
```

安装完成后，启动服务：
```bash
uv run web/chanlun_chart/app.py
```

---

### 方式二：Conda 手动安装

> ### pytdx 必须使用项目目录 package 下提供的包进行安装
> ### pytdx 必须使用项目目录 package 下提供的包进行安装
> ### pytdx 必须使用项目目录 package 下提供的包进行安装

#### 1. 克隆项目到本地

        git clone https://github.com/yijixiuxin/chanlun-pro.git
        # gitee 国内地址
        # git clone https://gitee.com/wang-student/chanlun-pro.git
        cd chanlun-pro

### 2. pip 安装项目依赖包

         # 创建项目运行 Python 环境
         conda create -y -n chanlun python=3.11
         # 切换到新创建的 chanlun 环境  
         conda activate chanlun
             
         # conda 安装相关的库会比较方便
         conda install -y -c conda-forge ta-lib  
             
         # PIP 安装项目依赖包
         pip3 config set global.index-url https://mirrors.aliyun.com/pypi/simple/
         pip3 install -r requirements.txt
             
         # 安装 pytdx 包
         pip3 install wheel
         pip3 install package/pytdx-1.72r2-py3-none-any.whl

### 3. 设置 `PYTHONPATH` 环境变量

         vim ~/.bashrc
         # 在最后一行增加  project_path 替换成项目所在的目录
         export PYTHONPATH=$PYTHONPATH:/project_path/chanlun-pro/src:
         source ~/.bashrc

### 4. 在 `src/chanlun` 目录， 复制拷贝 `config.py.demo` 文件为 `config.py` 并修改其中的 [配置项](配置文件说明.md)

### 5. 加作者微信，获取授权许可文件，并放置在项目中的 `src/pyarmor_runtime_005445` 目录下

### 6. 到 `web/chanlun_chart` 目录，启动 web 服务

         # 使用 conda 环境
         conda activate chanlun
         python web/chanlun_chart/app.py



---

> ## 针对于 M系列芯片 的处理  
> 
> 如果运行程序，被系统 kill 掉，启动不了，可以按照以下操作进行处理


1. 打开“钥匙串访问”，菜单栏选择“证书助手”，“创建证书”，名称输入 “chanlun”，证书类型选择“代码签名”，然后创建；

2. 在“命令行”中，输入 `security find-identity`，查看刚刚创建的证书（后面跟着 chanlun 这个名字）；

3. 使用 codesign 进行签名，命令：
        
        # 进入程序目录下的 pyarmor_run_time_005445/darwin_aarch64 文件夹
        cd /你的项目目录/chanlun-pro/src/pyarmor_run_time_005445/darwin_aarch64

        # 使用 condesign 进行签名，`6DCA**替换你的证书ID**9ADF` 替换成 `security find-identity` 命令输出中的证书 ID
        codesign -s "6DCA**替换你的证书ID**9ADF" pyarmor_runtime.cpython-311-darwin.so
        
