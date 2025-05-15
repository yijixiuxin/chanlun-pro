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

echo 2. ���Python 3.11���⻷��
if not exist ".venv\" (
    echo ����Python 3.11���⻷��...
    %UV_DIR% venv --python=3.11 .venv
    if errorlevel 1 (
        echo �������⻷��ʧ��
        exit /b 1
    )
)

echo �������⻷��
call .venv\Scripts\activate.bat

echo 3. ��װ���ذ�
echo ��װ����������...
@REM pip install wheel
%UV_DIR% pip install "%ROOT_DIR%package/pytdx-1.72r2-py3-none-any.whl"
%UV_DIR% pip install "%ROOT_DIR%package/ta_lib-0.4.25-cp311-cp311-win_amd64.whl"

echo 4. ��������ļ�
if not exist "%ROOT_DIR%src\chanlun\config.py" (
    echo ���������ļ�...
    copy "%ROOT_DIR%src\chanlun\config.py.demo" "%ROOT_DIR%src\chanlun\config.py" >nul
)

echo 5. ��װrequirements.txt����
if exist "%ROOT_DIR%requirements.txt" (
    echo ��װ��Ŀ����...
    %UV_DIR% pip install -r "%ROOT_DIR%requirements.txt"
)

echo REM 6. ���û�������
set "PYTHONPATH=%ROOT_DIR%src"
echo ����PYTHONPATH: !PYTHONPATH!

echo 7. ���л������ű�
if exist "%ROOT_DIR%check_env.py" (
    %UV_DIR% run "%ROOT_DIR%check_env.py"
)

echo ����������ɣ�
pause
