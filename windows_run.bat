@echo off
setlocal enabledelayedexpansion

REM ��ȡ�ű�����Ŀ¼����Ŀ��Ŀ¼��
set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%"

REM ROOT_DIR ��ֵ
echo ROOT_DIR: %ROOT_DIR%

echo uv Ŀ¼
set "UV_DIR=%ROOT_DIR%script\bin\uv.exe"
echo UV_DIR: %UV_DIR%

echo �������⻷��
call .venv\Scripts\activate.bat

echo ���û�������
set "PYTHONPATH=%ROOT_DIR%src"
echo ����PYTHONPATH: !PYTHONPATH!

echo ���� WEB ����
if exist "%ROOT_DIR%web/chanlun_chart/app.py" (
    %UV_DIR% run "%ROOT_DIR%web/chanlun_chart/app.py"
)
