@echo off
setlocal enabledelayedexpansion

REM 获取脚本所在目录（项目根目录）
set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%"

REM ROOT_DIR 的值
echo ROOT_DIR: %ROOT_DIR%

echo 激活虚拟环境
call .venv\Scripts\activate.bat

echo 设置环境变量
set "PYTHONPATH=%ROOT_DIR%src"
echo 设置PYTHONPATH: !PYTHONPATH!

echo 运行 WEB 服务
if exist "%ROOT_DIR%web/chanlun_chart/app.py" (
    uv run "%ROOT_DIR%web/chanlun_chart/app.py"
)
