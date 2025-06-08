@echo off
setlocal enabledelayedexpansion

REM 获取脚本所在目录（项目根目录）
set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%"

REM ROOT_DIR 的值
echo ROOT_DIR: %ROOT_DIR%

echo 1. uv 目录
set "UV_DIR=%ROOT_DIR%script\bin\uv.exe"
echo UV_DIR: %UV_DIR%

echo 2. 创建虚拟环境
%UV_DIR% sync

echo 3. 检查配置文件
if not exist "%ROOT_DIR%src\chanlun\config.py" (
    echo 创建配置文件...
    copy "%ROOT_DIR%src\chanlun\config.py.demo" "%ROOT_DIR%src\chanlun\config.py" >nul
)

echo 4. 设置环境变量
set "PYTHONPATH=%ROOT_DIR%src"
echo 设置PYTHONPATH: !PYTHONPATH!

echo 5. 运行环境检查脚本
if exist "%ROOT_DIR%check_env.py" (
    %UV_DIR% run "%ROOT_DIR%check_env.py"
)

echo 环境配置完成！
pause
