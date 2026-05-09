@echo off
setlocal enabledelayedexpansion

REM 获取脚本所在目录（项目根目录）
set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%"

REM ROOT_DIR 的值
echo ROOT_DIR: %ROOT_DIR%

echo uv 目录
set "UV_DIR=%ROOT_DIR%script\bin\uv.exe"
echo UV_DIR: %UV_DIR%

echo 运行 WEB 服务
if exist "%ROOT_DIR%web/chanlun_chart/app.py" (
    %UV_DIR% run "%ROOT_DIR%web/chanlun_chart/app.py"
)
