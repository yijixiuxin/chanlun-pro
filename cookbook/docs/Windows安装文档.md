### Windows 安装文档

---

> Python 版本支持 3.7、3.8、3.9、3.10 （建议 3.10），不然运行会报 RuntimeError 错误  
> 前置条件  
> 已经安装 GitHub Desktop、 Anaconda、MySQL、Redis  

### 1. 通过 GitHub Desktop 克隆项目到本地

   https://github.com/yijixiuxin/chanlun-pro.git

### 2. 在 `chanlun-pro` 目录，双击 `install_windows.bat` 文件进行安装

### 3. 执行以下命令获取本机 mac 地址，并发送给作者，获取授权许可文件，并放置在项目中的 `src/pytransform` 目录下

          pip install pyarmor==7.7.4
          pyarmor hdinfo
          # 将 Default Mac address: "****"  内容发送给作者，获取授权文件

### 4. 设置 PYTHONPATH 环境变量

         # 我的电脑 -> 右键菜单选“属性” -> 高级系统设置 -> 高级 -> 环境变量 -> 系统变量 -> 新建
         # 系统变量信息，project_path 需要替换成项目所在目录
         变量名：PYTHONPATH
         变量值：project_path\chanlun-pro\src
         
         设置完成后，重启终端 ，输入命令 $env:PYTHONPATH  查看是否设置成功

### 5. 在 `src/chanlun` 目录， 复制拷贝 `config.py.demo` 文件为 `config.py` 并修改其中的 [配置项](配置文件说明.md)

### 6. 运行项目根目录中的 `check_env.py` 文件，检查环境是否OK，如果输出 “环境OK”，则可以继续进行，如果有错误，则安装提示进行修复

         conda activate chanlun
         python check_env.py

### 7. （旧版）~~在 `web/chanlun_web` 目录，双击  `run.bat` 启动，浏览器访问 http://127.0.0.1:8000/ 即可显示缠论解缠主页~~

### 8. （新版）在 `web/chanlun_chart` 目录，双击  `run.bat` 启动

    