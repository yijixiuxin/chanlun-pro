@echo off
setlocal enabledelayedexpansion

REM 获取脚本所在目录（项目根目录）
set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%"

REM ROOT_DIR 的值
echo ROOT_DIR: %ROOT_DIR%

echo 1. 检查并安装uv
where uv >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing uv...
    powershell -Command "irm https://astral.sh/uv/install.ps1 | iex"
    if errorlevel 1 (
        echo 安装uv失败，请手动安装后重试
        exit /b 1
    )
)

echo 2. 检查Python 3.11虚拟环境
if not exist ".venv\" (
    echo 创建Python 3.11虚拟环境...
    uv venv --python=3.11 .venv
    if errorlevel 1 (
        echo 创建虚拟环境失败
        exit /b 1
    )
)

echo 激活虚拟环境
call .venv\Scripts\activate.bat

echo 3. 安装本地包
echo 安装本地依赖包...
@REM pip install wheel
uv pip install "%ROOT_DIR%package/pytdx-1.72r2-py3-none-any.whl"
uv pip install "%ROOT_DIR%package/ta_lib-0.4.25-cp311-cp311-win_amd64.whl"

echo 4. 检查配置文件
if not exist "%ROOT_DIR%src\chanlun\config.py" (
    echo 创建配置文件...
    copy "%ROOT_DIR%src\chanlun\config.py.demo" "%ROOT_DIR%src\chanlun\config.py" >nul
)

echo 5. 安装requirements.txt依赖
if exist "%ROOT_DIR%requirements.txt" (
    echo 安装项目依赖...
    uv pip install -r "%ROOT_DIR%requirements.txt"
)

echo REM 6. 设置环境变量
set "PYTHONPATH=%ROOT_DIR%src"
echo 设置PYTHONPATH: !PYTHONPATH!

echo 7. 运行环境检查脚本
if exist "%ROOT_DIR%check_env.py" (
    uv run "%ROOT_DIR%check_env.py"
)

echo 环境配置完成！
pause
