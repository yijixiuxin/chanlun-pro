### Windows 安装文档

---

### [B站安装教程](https://www.bilibili.com/video/BV1XH4y1K7VM/)

### Chanlun-PRO Windows 启动器 下载与教程
> 启动器针对于不会 Python 与部署的小伙伴，鼠标点点即可进行使用。    
> 视频：https://www.bilibili.com/video/BV1tbsheQEWs    
> 百度网盘下载：    
>     链接: https://pan.baidu.com/s/1U0F8GCC6emruMiV6xJ6Tfg    
>     提取码: dfv8    
> QQ 群文件下载： 723118857    

---

> Python 版本支持 3.8、3.9、3.10、3.11 ，不然运行会报 RuntimeError 错误  
> 前置条件  
> 已经安装 GitHub Desktop、 Anaconda、MySQL、Redis  
> ### pytdx 必须使用项目目录 package 下提供的包进行安装
> ### pytdx 必须使用项目目录 package 下提供的包进行安装
> ### pytdx 必须使用项目目录 package 下提供的包进行安装

### 1. 通过 GitHub Desktop 克隆项目到本地

      # GitHub 地址
      https://github.com/yijixiuxin/chanlun-pro.git
      # Gitee 地址
      https://gitee.com/wang-student/chanlun-pro

### 2. 在 `chanlun-pro` 目录，双击 `install_windows.bat` 文件进行安装

      # 如果 Anaconda 没有安装到默认目录，可修改 install_windows.bat 中的 conda_path 目录（GBK编码打开和保存）
      # 或者 搜索 Anaconda Prompt 程序，并打开，手动执行以下命令，进行安装
      
      cd \你的项目代码路径\chanlun-pro
      conda create -y -n chanlun python=3.11
      conda activate chanlun
      pip install package/pytdx-1.72r2-py3-none-any.whl
      pip install package/ta_lib-0.4.25-cp311-cp311-win_amd64.whl
      pip install -r requirements.txt


### 3. 设置 PYTHONPATH 环境变量

         # 我的电脑 -> 右键菜单选“属性” -> 高级系统设置 -> 高级 -> 环境变量 -> 系统变量 -> 新建
         # 系统变量信息，project_path 需要替换成项目所在目录
         变量名：PYTHONPATH
         变量值：\你的项目代码路径\chanlun-pro\src
         
         设置完成后，重启终端 ，输入命令 $env:PYTHONPATH  查看是否设置成功

### 4. 在 `src/chanlun` 目录， 复制拷贝 `config.py.demo` 文件为 `config.py` 并修改其中的 [配置项](配置文件说明.md)

### 5. 运行项目根目录中的 `check_env.py` 文件，检查环境是否OK，如果输出 “环境OK”，则可以继续进行，如果有错误，则安装提示进行修复

         conda activate chanlun
         python check_env.py

### 6. 加作者微信，获取授权许可文件，并放置在项目中的 `src/pyarmor_runtime_005445` 目录下

### 7. 在 `web/chanlun_chart` 目录，双击  `run.bat` 启动

      # 如果报错，找不到 conda 命令等信息，可以使用 python 直接执行 app.py 进行启动
      # 搜索 Anaconda Prompt 程序，并打开，手动执行以下命令，进行启动

      conda activate chanlun
      cd \你的项目代码路径\chanlun-pro\web\chanlun_chart
      python app.py