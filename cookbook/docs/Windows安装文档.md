### Windows 安装文档

---

### [B站安装教程](https://www.bilibili.com/video/BV1XH4y1K7VM/)

### Chanlun-PRO Windows 启动器 下载与教程

#### 建议下载项目，用双击 bat 文件进行安装

> 启动器针对于不会 Python 与部署的小伙伴，鼠标点点即可进行使用。    
> 视频：https://www.bilibili.com/video/BV1tbsheQEWs    
> 网盘下载：    
>     链接: https://pan.quark.cn/s/43af02e8fdd6     
 
 
---

> Python 版本支持 3.8、3.9、3.10、3.11 ，不然运行会报 RuntimeError 错误  

> ### pytdx 必须使用项目目录 package 下提供的包进行安装
> ### 如果提示 “Tushare内置的pytdx版本和最新的pytdx 版本不同...” 无需理睬，等待即可


## 加作者微信，获取授权许可文件，并放置在项目中的 `src/pyarmor_runtime_005445` 目录下


### 1. 通过 GitHub Desktop 克隆项目到本地 或者 直接打包下载到本地 

> 两个地址项目是同步的，国内的可选 Gitee 国外的可选 GitHub （请帮忙给一个 Star）

      # GitHub 地址
      https://github.com/yijixiuxin/chanlun-pro
      # Gitee 地址
      https://gitee.com/wang-student/chanlun-pro

### 2. 在 `chanlun-pro` 目录，双击 `windows_install.bat` 文件进行安装

### 3. 在 `chanlun-pro` 目录，双击 `windows_run.bat` 运行项目


---
---
---

### 通过以上方式可以正常运行，就不用看下面了
### 以下是手动安装的命令


### 设置 PYTHONPATH 环境变量

         # 我的电脑 -> 右键菜单选“属性” -> 高级系统设置 -> 高级 -> 环境变量 -> 系统变量 -> 新建
         # 系统变量信息，project_path 需要替换成项目所在目录
         变量名：PYTHONPATH
         变量值：\你的项目代码路径\chanlun-pro\src
         
         设置完成后，重启终端 ，输入命令 $env:PYTHONPATH  查看是否设置成功

### 在 `src/chanlun` 目录， 复制拷贝 `config.py.demo` 文件为 `config.py` 并修改其中的 [配置项](配置文件说明.md)

UV 和 Conda 任选一个自己喜欢的就行

### UV 安装命令

      # 安装 uv
      powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
      
      # 进入 chanlun-pro 目录，创建虚拟环境，安装依赖包
      cd \你的项目代码路径\chanlun-pro
      
      uv venv --python=3.11 .venv
      .venv\Scripts\activate
      uv pip install package/pytdx-1.72r2-py3-none-any.whl
      uv pip install package/ta_lib-0.4.25-cp311-cp311-win_amd64.whl
      uv pip install -r requirements.txt

      # 检查环境
      uv run check_env.py

      # 运行 Web 服务
      uv run web/chanlun_chart/app.py

### Conda 安装命令

      # 创建项目运行 Python 环境
      conda create -y -n chanlun python=3.11
      # 切换到新创建的 chanlun 环境  
      conda activate chanlun

      # PIP 安装项目依赖包
      pip3 config set global.index-url https://mirrors.aliyun.com/pypi/simple/
      pip3 install package/pytdx-1.72r2-py3-none-any.whl
      pip3 install package/ta_lib-0.4.25-cp311-cp311-win_amd64.whl
      pip3 install -r requirements.txt

      # 检查环境
      python check_env.py

      # 运行 Web 服务
      python web/chanlun_chart/app.py
      