@echo off
setlocal enabledelayedexpansion

REM ��ȡ�ű�����Ŀ¼����Ŀ��Ŀ¼��
set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%"

REM ROOT_DIR ��ֵ
echo ROOT_DIR: %ROOT_DIR%

echo 1. uv Ŀ¼
set "UV_DIR=%ROOT_DIR%script\bin\uv.exe"
echo UV_DIR: %UV_DIR%

echo 2. �������⻷��
%UV_DIR% python install 3.11
%UV_DIR% venv --python=3.11 .venv
%UV_DIR% sync

echo 3. ��ȡ������Ϣ
%UV_DIR% run -m pyarmor.cli.hdinfo

echo ����������Ϣ�����͸����ߣ�����������Ȩ��
pause