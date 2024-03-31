@echo off

rem anaconda 的安装目录 
set conda_path=%USERPROFILE%\anaconda3
rem 设置创建的环境名称
set conda_env_name=chanlun
rem 设置 conda.exe 可执行文件的路径
set conda_exe=%conda_path%\Scripts\conda.exe
rem 创建环境后的 pip 地址
set conda_pip=%conda_path%\envs\%conda_env_name%\Scripts\pip.exe

rem 检查文件是否存在
if not exist %conda_path% (
  echo 错误:未找到 conda 安装目录，可编辑此文件，修改 conda_path 为 Anaconda 的实际安装路径
  echo %conda_path%
  set /p dummy=
) else (
   echo Anaconda 安装路径：%conda_path%
   echo 创建环境名称：%conda_env_name%

   rem 删除环境
   %conda_exe% remove -n %conda_env_name% -y --all
    
   echo 创建 %conda_env_name% 环境并安装依赖
   %conda_exe% create -y -n %conda_env_name% python=3.10
   %conda_exe% activate %conda_env_name%
   %conda_exe% install -y -c conda-forge ta-lib
   %conda_pip% config set global.index-url https://mirrors.aliyun.com/pypi/simple/
   %conda_pip% install -r requirements.txt
   %conda_pip% install wheel
   %conda_pip% install package/pytdx-1.72r2-py3-none-any.whl

   echo 脚本执行完成,按 Enter 键退出...
   set /p dummy=
)