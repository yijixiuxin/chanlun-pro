### Ubuntu 安装文档

---

### AI 自动安装（复制提示词给 AI）

配套 AI 安装文档：<https://gitee.com/wang-student/chanlun-pro/raw/master/AI-INSTALL.md>

把下面整段提示词复制给具备终端与文件操作能力的 AI（如 Claude Code、Cursor、Codex），它会自动完成安装、启动与验收：

```text
请阅读并严格遵守以下安装文档，在我的机器上安装并运行 chanlun-pro：
https://gitee.com/wang-student/chanlun-pro/raw/master/AI-INSTALL.md

我的环境：Ubuntu/Debian Linux x86_64（glibc，非 ARM）

执行要求：
1. 先完整阅读文档，按 Gate 0 → Gate 6 顺序执行，每道 Gate 验收通过后再继续；
2. 开工前先向我确认文档 §2.4 的 5 个问题（授权文件、访问范围、端口、数据库、是否常驻）；
3. 授权文件 src/pyarmor_runtime_005445/pyarmor.rkey 必须由我提供；缺失时停下来向我要，不要伪造或绕过；
4. 安装完成后按文档 §5 验收，并把访问地址、启动命令、日志位置、验收结果发给我；
5. 遇到文档 §7 排查表解决不了的问题，把完整报错和已执行命令发给我；不要修改核心加密代码。
```

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
chmod +x install_linux.sh
./install_linux.sh
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

