@echo off
setlocal enabledelayedexpansion

:: 第一阶段 - 查找Anaconda安装路径
echo 正在定位Anaconda安装路径...

:: 通过检查 PATH 环境变量查找 Anaconda 路径
for %%a in ("%PATH:;=" "%") do (
    set "current_path=%%~a"
    :: 检查路径中是否包含 Anaconda/Miniconda 相关特征文件
    if exist "!current_path!\conda.exe" (
        set "conda_path=!current_path!"
        goto validate_conda_path
    )
    if exist "!current_path!\conda.bat" (
        set "conda_path=!current_path!"
        goto validate_conda_path
    )
)

:: 如果常规路径找不到，尝试注册表查询（针对所有用户安装的情况）
for /f "skip=2 tokens=2,*" %%a in ('reg query "HKLM\SOFTWARE\Python\ContinuumAnalytics\Anaconda3" /v InstallPath 2^>nul') do (
    set "conda_path=%%b"
    goto validate_conda_path
)

:: 方法3：检查用户目录下的常见安装位置
if exist "%USERPROFILE%\Anaconda3\conda.exe" (
    set "conda_path=%USERPROFILE%\Anaconda3"
    goto validate_conda_path
)

echo 错误：未找到Anaconda安装，请确认已正确安装
pause
exit /b 1

:validate_conda_path
set "conda_path=!conda_path:\conda.exe=!"
set "conda_path=!conda_path:\Scripts=!"
set "conda_path=!conda_path:\condabin=!"
echo 检测到Anaconda根目录：!conda_path!

:: 第二阶段 - 查找目标环境
echo 正在搜索 [chanlun] 环境...
set "env_found=false"

:: 查找方式1：标准环境目录
set "env_path=!conda_path!\envs\chanlun"
if exist "!env_path!\python.exe" (
    set "env_found=true"
    goto found_env
)

:: 查找方式2：用户自定义环境目录
for /f "tokens=*" %%e in ('dir /s /b "%USERPROFILE%\.conda\envs\chanlun\python.exe" 2^>nul') do (
    set "env_path=%%~dpe\.."
    set "env_found=true"
    goto found_env
)

:: 查找方式3：通过conda配置信息
if exist "!conda_path!\condabin\conda.bat" (
    for /f "delims=" %%i in ('""!conda_path!\condabin\conda.bat" env list --json"') do (
        set "json_output=%%i"
    )
    for /f "tokens=2 delims=:," %%j in ('echo !json_output! ^| findstr /C:"\"name\": \"chanlun\""') do (
        set "env_path=%%~j"
        set "env_path=!env_path:\"/=!"
        set "env_path=!env_path:"=!"
        if exist "!env_path!\python.exe" (
            set "env_found=true"
            goto found_env
        )
    )
)

if "!env_found!" == "false" (
    echo 错误：未找到 [chanlun] 虚拟环境，请确认：
    echo 1. 环境名称是否正确
    echo 2. 环境是否已创建
    pause
    exit /b 2
)

:found_env
echo 找到环境路径：!env_path!

:: 第三阶段 - 执行Python脚本
set "python_exe=!env_path!\python.exe"
if not exist "!python_exe!" (
    echo 错误：Python解释器未找到 [!python_exe!]
    pause
    exit /b 3
)

echo 正在使用 [!python_exe!] 执行 app.py...
"!python_exe!" "app.py"

if !errorlevel! neq 0 (
    echo 错误：执行app.py时发生错误 (错误码：!errorlevel!)
    pause
    exit /b 4
)

echo 执行完成
pause