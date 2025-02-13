@echo off
setlocal enabledelayedexpansion

:: 通过检查 PATH 环境变量查找 Anaconda 路径
for %%a in ("%PATH:;=" "%") do (
    set "current_path=%%~a"
    :: 检查路径中是否包含 Anaconda/Miniconda 相关特征文件
    if exist "!current_path!\conda.exe" (
        set "conda_path=!current_path!"
        goto found
    )
    if exist "!current_path!\conda.bat" (
        set "conda_path=!current_path!"
        goto found
    )
)

:: 如果常规路径找不到，尝试注册表查询（针对所有用户安装的情况）
for /f "skip=2 tokens=2,*" %%a in ('reg query "HKLM\SOFTWARE\Python\ContinuumAnalytics\Anaconda3" /v InstallPath 2^>nul') do (
    set "conda_path=%%b"
    goto found
)

:not_found
echo Anaconda 未找到，请确认已安装或已添加至系统环境变量
pause
exit /b 1

:found
rem anaconda 的安装目录 
:: 提取主安装路径（去除 Scripts/condabin 子目录）
set "conda_path=!conda_path:\Scripts=!"
set "conda_path=!conda_path:\condabin=!"
echo 检测到 Anaconda 安装路径为: %conda_path%

rem 设置创建的环境名称
set conda_env_name=chanlun
rem 设置 conda.exe 可执行文件的路径
set conda_exe=%conda_path%\Scripts\conda.exe
rem 创建环境后的 pip 地址
set conda_pip=%conda_path%\envs\%conda_env_name%\Scripts\pip.exe

echo Anaconda 安装路径：%conda_path%
echo 创建环境名称：%conda_env_name%

%conda_exe% init

rem 删除环境
%conda_exe% remove -n %conda_env_name% -y --all

echo 创建 %conda_env_name% 环境并安装依赖
%conda_exe% create -y -n %conda_env_name% python=3.11
%conda_exe% activate %conda_env_name%
%conda_pip% config set global.index-url https://mirrors.aliyun.com/pypi/simple/
%conda_pip% install package/ta_lib-0.4.25-cp311-cp311-win_amd64.whl
%conda_pip% install package/pytdx-1.72r2-py3-none-any.whl
%conda_pip% install -r requirements.txt

echo 脚本执行完成,按 Enter 键退出...
set /p dummy=
