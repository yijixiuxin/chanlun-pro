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

echo 2. 获取机器信息
%UV_DIR% run -m pyarmor.cli.hdinfo

echo 复制以上信息，发送给作者，用于生成授权码
pause