### Mac 安装文档

---

### AI 自动安装（复制提示词给 AI）

配套 AI 安装文档：<https://gitee.com/wang-student/chanlun-pro/raw/master/AI-INSTALL.md>

把下面整段提示词复制给具备终端与文件操作能力的 AI（如 Claude Code、Cursor、Codex），它会自动完成安装、启动与验收：

```text
请阅读并严格遵守以下安装文档，在我的机器上安装并运行 chanlun-pro：
https://gitee.com/wang-student/chanlun-pro/raw/master/AI-INSTALL.md

我的环境：macOS（Apple Silicon 或 Intel）

执行要求：
1. 先完整阅读文档，按 Gate 0 → Gate 6 顺序执行，每道 Gate 验收通过后再继续；
2. 开工前先向我确认文档 §2.4 的 5 个问题（授权文件、访问范围、端口、数据库、是否常驻）；
3. 授权文件 src/pyarmor_runtime_005445/pyarmor.rkey 必须由我提供；缺失时停下来向我要，不要伪造或绕过；
4. 安装完成后按文档 §5 验收，并把访问地址、启动命令、日志位置、验收结果发给我；
5. 遇到文档 §7 排查表解决不了的问题，把完整报错和已执行命令发给我；不要修改核心加密代码。
```

> AI 唯一无法自动完成的是**授权文件**（`pyarmor.rkey`）：请先按[授权方式](licensing.md)申请，把文件交给 AI 后，其余步骤均可自动完成。默认使用 SQLite，本页下方“前置条件”里的 MySQL/Redis 不是必须的。M 系列芯片若启动被系统 kill，按 AI 文档 §4.4 做本地签名。

> Python 版本支持 3.11 3.12 3.13 3.14 ，不然运行会报 RuntimeError 错误
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

        # gitee 国内地址
        git clone https://gitee.com/wang-student/chanlun-pro.git
        # Github 国外地址
        git clone https://github.com/yijixiuxin/chanlun-pro.git
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

### 4. 在 `src/chanlun` 目录， 复制拷贝 `config.py.demo` 文件为 `config.py` 并修改其中的 [配置项](config-file-explained.md)

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
        
