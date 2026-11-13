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

REM 启动项目前，统一同步数据库表结构
REM 脚本可重复执行：表或列已存在会自动跳过，不会修改业务数据
echo 同步数据库表结构
"%UV_DIR%" run "%ROOT_DIR%script\db_change_schema.py"
if errorlevel 1 (
    echo 数据库结构同步失败，请检查 config.py 中的数据库配置后重试
    pause
    exit /b 1
)

echo 运行 WEB 服务
if exist "%ROOT_DIR%web/chanlun_chart/app.py" (
    %UV_DIR% run "%ROOT_DIR%web/chanlun_chart/app.py"
)