# Chanlun-PRO 安装与运行说明（面向 AI 执行者）

> **文档版本**：2026-10-22；适用代码：`master` 分支（本版基于提交 `371d9774`；本文档本身会再产生一个提交，功能代码不变）
> **本文网络地址**（发给 AI 的就是这个）：<https://gitee.com/wang-student/chanlun-pro/raw/master/AI-INSTALL.md>
> **读者**：具备 Shell / 文件编辑能力的 AI Agent。
> **目标**：在一台用户机器上完成安装，启动 Web 服务，并按第 5 节完成验收与交付。

---

## 0. 先看结论（边界与硬约束）

- 这是一个 Python + Flask/Tornado 的本地 Web 应用，**没有 Docker 方案**，需要裸机安装。
- 全流程只有 **1 件 AI 无法独立完成的事：授权文件 `pyarmor.rkey`**（见 §2.3）。它必须先由人类提供，否则服务一定启动失败。
- 依赖必须用 **uv + `pyproject.toml`** 安装。**不要**执行 `pip install -r requirements.txt`，也不要按旧文档用 conda 手工装依赖（ta-lib、pytdx 是平台相关本地 wheel，手工装极易失败）。
- 默认使用 **SQLite**，不需要 MySQL / Redis / Node.js / Docker；这些只在用户明确要求时才配置。
- Web 服务默认端口 **9900**（代码写死），默认只监听配置里的 `WEB_HOST`。

---

## 1. 执行协议（AI 必读）

1. 本文是 **Runbook**：按 Gate 0 → 6 顺序执行；每道 Gate 都有「检查命令 + 通过标准」，**未通过不得进入下一道**。
2. 开始前一次性向用户问清 §2.4 的 5 个问题，不要边做边问。
3. 所有命令默认在**项目根目录**（含 `pyproject.toml` 的目录）执行。
4. 失败时先读真实报错，再查 §7 排查表；同一个错误连续 2 次解决不了，停止并把**完整报错 + 已执行命令**交给用户。
5. 禁止事项：
   - 不要解密、修改、反编译 `src/chanlun/core/**` 和 `src/pyarmor_runtime_*`；
   - 不要伪造授权文件，也不要把一台机器的 `pyarmor.rkey` 复制到另一台机器；
   - 不要把 `src/chanlun/config.py`、`pyarmor.rkey` 提交进 git；
   - 不要在没有 `LOGIN_PWD` 的情况下把服务暴露到公网；
   - 不要用 `windows_install.bat` / `windows_run.bat` 做自动化：前者会 `pause` 等按键并清空 `.venv`，后者在前台运行、不便后台管理。
6. 若本文与仓库代码不一致，以代码和实际输出为准，并在最终报告中指出文档过期点。

---

## 2. Gate 0：开工前检查

### 2.1 平台支持矩阵

核心引擎是加密二进制，只提供以下平台（`src/chanlun/cl.py` 会校验）：

| 平台 | 是否支持 | 说明 |
|---|---|---|
| Windows 10/11 x86_64 | ✅ | 主支持平台；仓库自带 Windows 版 uv |
| macOS x86_64 | ✅ | ta-lib wheel 要求 macOS ≥ 10.15 |
| macOS Apple Silicon（arm64） | ✅ | ta-lib wheel 要求 macOS ≥ 14；若启动进程被系统直接 kill，需要对 pyarmor 运行时做本地签名（见 §4.4） |
| Linux x86_64（glibc：Ubuntu/Debian/CentOS 等） | ✅ | 也适用于 WSL2 |
| Windows ARM64 | ❌ | 核心引擎没有 windows_arm64，运行会报不支持平台 |
| Linux ARM64（含 aarch64 云主机/树莓派） | ❌ | 核心引擎只提供 linux_x86_64 |
| Alpine / musl Linux | ❌ | 只提供 glibc 构建（pyarmor 运行时无 musl 版本，ta-lib 只映射 manylinux wheel） |

检查命令：

```powershell
# Windows
[System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
```

```bash
# macOS / Linux
uname -srm
```

### 2.2 资源与网络

- 磁盘：**≥ 5 GB**（克隆后工作区约 0.4 GB + `.git` 约 0.3 GB + 虚拟环境约 1.1 GB + 行情缓存）。
- 内存：≥ 2 GB。
- 网络必须能访问：
  - 代码源：`gitee.com`（国内推荐）或 `github.com`；
  - Python 下载：uv 会下载 Python 3.11；
  - PyPI：默认走阿里云镜像 `mirrors.aliyun.com`（`uv.lock` 已固定）；
  - 运行时：通达信行情服务器（首次启动会探测，属正常行为）。
- 不需要：MySQL、Redis、Node.js、Docker、conda（默认 SQLite）。
- **本机必须没有指向其它 chanlun 项目 `src` 的 `PYTHONPATH`**：`web/chanlun_chart/app.py` 只是把本项目 `src` 追加到 `sys.path` 末尾，若系统里有另一个 chanlun 的 `src` 在 `PYTHONPATH` 中且排在前面，就会 import 到另一份代码（表现为配置不生效、连到别的数据库，甚至“假成功”）。检查并清空：

  ```powershell
  $env:PYTHONPATH        # 应输出空或与当前项目无关
  ```

  ```bash
  echo "$PYTHONPATH"
  ```

  若包含其它 `chanlun-pro/src`：本会话执行 `$env:PYTHONPATH=''`（Windows）/ `unset PYTHONPATH`（macOS/Linux），并建议用户从系统环境变量中移除。

### 2.3 硬阻塞：授权文件 `pyarmor.rkey`

- 必须放在：`<项目根目录>/src/pyarmor_runtime_005445/pyarmor.rkey`（文件名必须完全一致）。
- 仓库出于授权原因**不包含**该文件；缺失时启动会报：

  ```
  RuntimeError: missing license key to run the script (1:10672)
  ```

- 获取方式（**必须由人类完成，AI 不要尝试绕过**）：
  - **试用授权**（免费 20 天，不需要机器信息）：按 <https://chanlun-pro.readthedocs.io/licensing/> 联系作者微信获取；
  - **正式授权**（与本机绑定）：先取本机机器信息发给作者，再换取 `pyarmor.rkey`：

    ```powershell
    .\script\bin\uv.exe run python -m pyarmor.cli.hdinfo
    ```

    ```bash
    uv run python -m pyarmor.cli.hdinfo
    ```

    输出包含 `Machine ID`、网卡地址等，原样发送给作者即可。
    注意：该命令依赖 uv 与项目依赖，**需先完成 §3.2** 再执行（Windows 可直接用 `.\script\bin\uv.exe`，mac/Linux 需先装 uv）。
  - 如果安装完成后报 `License is not for this machine`，说明授权文件与本机不匹配，按上面步骤重新获取。

### 2.4 开工前一次性问用户（5 个问题）

1. **授权文件**：`pyarmor.rkey` 在哪里？如果还没有，请用户先去申请（这是阻塞项；可先做 §3.1–§3.3，等文件到位再启动）。
2. **访问范围**：只在本机用，还是要局域网/公网访问？（决定 `WEB_HOST` 和 `LOGIN_PWD`）
3. **端口**：9900 是否可用？如果被占用，是否同意停掉占用进程？
4. **数据库**：是否必须用 MySQL？（默认 SQLite，零配置）
5. **是否常驻/开机自启**：默认只启动一个后台进程，不做系统服务。

---

## 3. 安装

### 3.1 获取代码（Gate 1）

**Windows（PowerShell）：**

```powershell
$PROJ = Join-Path $env:USERPROFILE 'chanlun-pro'
git clone --depth 1 https://gitee.com/wang-student/chanlun-pro.git $PROJ
Set-Location $PROJ
git log -1 --oneline
```

**macOS / Linux：**

```bash
git clone --depth 1 https://gitee.com/wang-student/chanlun-pro.git ~/chanlun-pro
cd ~/chanlun-pro
git log -1 --oneline
```

**通过标准**：目录下能看到 `pyproject.toml`、`uv.lock`、`web/chanlun_chart/app.py`、`src/chanlun/`，且 `git log` 输出一条提交记录。

说明：

- 国内优先用 Gitee；国外可用 GitHub：`https://github.com/yijixiuxin/chanlun-pro.git`。
- `--depth 1` 只为加速；后续要 `git pull` 更新也没问题。
- 机器没有 git 时：Windows/macOS 安装 Git；或直接下载 GitHub zip：`https://github.com/yijixiuxin/chanlun-pro/archive/refs/heads/master.zip`（**不要**用 Gitee 的 archive 链接，它返回 HTML 页面）。
- 仓库约 400 MB，加上 `.git` 克隆后约 0.7 GB；克隆 1–5 分钟属正常。

### 3.2 安装 uv、Python 与依赖（Gate 2）

**Windows：仓库自带 uv，无需安装**

```powershell
.\script\bin\uv.exe --version          # 预期：uv 0.12.x
.\script\bin\uv.exe python install 3.11
.\script\bin\uv.exe sync
```

**macOS / Linux：先安装 uv**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv --version
uv python install 3.11
uv sync
```

> 在中国大陆若 `astral.sh` 访问困难：macOS 可用 `brew install uv`；通用可用
> `python3 -m pip install uv -i https://mirrors.aliyun.com/pypi/simple/`，然后直接用 `uv`。
> 若 uv 下载 Python 失败：配置代理后重试，或设置 Python 安装镜像（示例，按可用镜像替换）：
> `export UV_PYTHON_INSTALL_MIRROR="https://mirrors.aliyun.com/python-build-standalone/"`。

**通过标准：**

- `uv sync` 退出码为 0，最后几行类似 `Installed 181 packages in xx s`；
- 项目根目录出现 `.venv/`（约 1.1 GB）；
- 耗时：冷缓存（首次下载 Python + 181 个包）约 5–15 分钟；uv 缓存已热时可 < 1 分钟，两种都正常。

```powershell
# Windows 依赖自检
.\script\bin\uv.exe run python -c "import talib, pytdx, pandas, flask, tornado; print('deps ok')"
```

```bash
# macOS / Linux 依赖自检
uv run python -c "import talib, pytdx, pandas, flask, tornado; print('deps ok')"
```

预期输出 `deps ok`（后面若出现 `Uninstalled/Installed 1 package` 的 pytdx 提示，属正常现象，可忽略）。

### 3.3 生成并最小化配置（Gate 3）

配置文件 `src/chanlun/config.py` 已被 `.gitignore` 忽略，不会污染仓库。

**复制模板（仅当文件不存在时）：**

```powershell
if (-not (Test-Path .\src\chanlun\config.py)) {
    Copy-Item .\src\chanlun\config.py.demo .\src\chanlun\config.py
}
```

```bash
[ -f src/chanlun/config.py ] || cp src/chanlun/config.py.demo src/chanlun/config.py
```

**用你的文件编辑工具修改这 4 行**（其余保持默认即可）：

> ⚠️ 模板 `config.py.demo` 的默认值是 `WEB_HOST = '0.0.0.0'` + `LOGIN_PWD = ''`——这是最危险的组合
> （同网络任何人都能访问）。本机自用**必须**按上表改成 `127.0.0.1`，不要直接沿用模板默认值。

| 配置项 | 本机自用（默认推荐） | 需要局域网/公网访问 |
|---|---|---|
| `WEB_HOST` | `'127.0.0.1'` | `'0.0.0.0'` |
| `LOGIN_PWD` | `''` | 至少 12 位的强密码字符串 |
| `DB_TYPE` | `"sqlite"` | 用户要求 MySQL 时才改 `"mysql"` |
| `REDIS_HOST` | `''` | `''`（本项目基本不用 Redis） |

> 安全规则：只要 `WEB_HOST = '0.0.0.0'` 或填了本机局域网 IP，就**必须**同时设置非空 `LOGIN_PWD`，
> 否则同网络任何人都能访问你的行情与交易配置。

**通过标准：**

```powershell
Select-String .\src\chanlun\config.py -Pattern '^WEB_HOST|^LOGIN_PWD|^DB_TYPE|^REDIS_HOST|^DATA_PATH'
```

```bash
grep -nE '^(WEB_HOST|LOGIN_PWD|DB_TYPE|REDIS_HOST|DATA_PATH)' src/chanlun/config.py
```

输出与上表一致即可。`DATA_PATH` 默认 `.chanlun_pro`（解读为 `~/.chanlun_pro`，即 Windows 的
`C:\Users\<用户名>\.chanlun_pro`）；如用户要求指定位置，可改成绝对路径（目录需可写）。

### 3.4 放置授权文件并自检（Gate 4）

```powershell
# Windows：把用户提供的文件复制到固定路径
Copy-Item 'C:\Users\<用户名>\Downloads\pyarmor.rkey' '.\src\pyarmor_runtime_005445\pyarmor.rkey' -Force
```

```bash
# macOS / Linux
cp ~/Downloads/pyarmor.rkey src/pyarmor_runtime_005445/pyarmor.rkey
```

**自检命令（必须通过）：**

```powershell
.\script\bin\uv.exe run python -c "import sys;sys.path.insert(0,'src');import chanlun.cl;print('LICENSE OK')"
```

```bash
uv run python -c "import sys;sys.path.insert(0,'src');import chanlun.cl;print('LICENSE OK')"
```

**通过标准**：**退出码 0 且最后一行输出 `LICENSE OK`**（主判据）。控制台上方还会打印启动横幅
（含 `运行环境: Python: 3.11 (CL Fast)` 和 `授权信息: <授权名>`）；Windows 控制台若把中文显示成乱码，
不影响判定，可忽略乱码只看 `LICENSE OK` 与退出码。

若报 `RuntimeError: missing license key to run the script ...`：授权文件没放对位置或文件名不对（见 §2.3）。

---

## 4. 启动与访问（Gate 5）

### 4.1 前台启动（先验证能跑起来）

```powershell
# Windows
.\script\bin\uv.exe run python -u web\chanlun_chart\app.py nobrowser
```

```bash
# macOS / Linux
uv run python -u web/chanlun_chart/app.py nobrowser
```

参数说明：

- `nobrowser`：**必须带**。否则程序会尝试打开本机浏览器（服务器/无桌面环境会报错或挂起）。
- `-u`：让日志实时输出，便于判断启动进度。
- 端口 9900 是**代码写死**的；本项目没有 `--port` 参数。
  如果 9900 被占用且无法停掉占用进程（或用户指定了别的端口），只能改代码：
  把 `web/chanlun_chart/app.py` 中 `s.bind(9900, config.WEB_HOST)` 的 `9900` 改成目标端口
  （同文件的 `webbrowser.open("http://127.0.0.1:9900")` 建议一并改，仅影响自动打开浏览器），
  然后把后文所有验收命令里的 9900 同步替换。注意：升级代码时该改动可能被覆盖。

**启动过程预期：**

1. 先打印授权横幅（`Chanlun-Pro 缠论数据计算` … `授权信息: ...`）；
2. 首次/换数据目录时，会探测通达信行情服务器，可能打印若干行 `BAD RESPONSE x.x.x.x` / `GOOD RESPONSE ...`，属正常；
3. 打印 **`启动成功`** 后开始监听；
4. 启动耗时：首次约 **20–90 秒**（含通达信服务器探测，网络差时可能到 2 分钟），已有数据目录时约 **10–20 秒**。等待超时建议给到 **180 秒**，不要过早判定失败。

**失败时的行为**：程序会打印完整 traceback，然后执行 `input("出现异常，按回车键退出")`。
自动化场景请**关闭/重定向 stdin**，让失败直接以非 0 退出，而不是等待回车：

```powershell
# Windows PowerShell（stdin 置空）
$null | .\script\bin\uv.exe run python -u web\chanlun_chart\app.py nobrowser
```

```bash
# macOS / Linux
uv run python -u web/chanlun_chart/app.py nobrowser < /dev/null
```

### 4.2 后台常驻（验证通过后再用）

**Windows（推荐，输出分开重定向）：**

```powershell
$logs = Join-Path $env:USERPROFILE '.chanlun_pro\logs'
New-Item -ItemType Directory -Force -Path $logs | Out-Null
$stdin = Join-Path $env:TEMP 'chanlun-empty.stdin'
New-Item -ItemType File -Force -Path $stdin | Out-Null
$uv = (Resolve-Path '.\script\bin\uv.exe').Path
$p = Start-Process -FilePath $uv `
    -ArgumentList @('run','python','-u','web\chanlun_chart\app.py','nobrowser') `
    -WorkingDirectory (Get-Location) -WindowStyle Hidden `
    -RedirectStandardInput  $stdin `
    -RedirectStandardOutput (Join-Path $logs 'web.out.log') `
    -RedirectStandardError  (Join-Path $logs 'web.err.log') `
    -PassThru
"launcher pid=$($p.Id)"
```

> `-RedirectStandardInput` 指向空文件：即使启动失败，程序也不会卡在「按回车键退出」。
> 注意：`$p.Id` 是 uv 启动器的 PID。真正的服务进程要看 9900 端口占用者（见下方停止命令）。

**macOS / Linux：**

```bash
mkdir -p ~/.chanlun_pro/logs
nohup uv run python -u web/chanlun_chart/app.py nobrowser \
  > ~/.chanlun_pro/logs/web.out.log 2>&1 &
echo $! > ~/.chanlun_pro/web.pid
```

**停止服务**（按端口找真实进程，最稳）：

```powershell
$owner = (Get-NetTCPConnection -LocalPort 9900 -State Listen -ErrorAction SilentlyContinue).OwningProcess
if ($owner) { Stop-Process -Id $owner -Force }
# 兜底（上面查不到时）：
# netstat -ano | findstr ':9900'   → 取最后一列 PID → Stop-Process -Id <PID> -Force
```

```bash
kill $(lsof -ti:9900)                  # 或：pkill -f 'web/chanlun_chart/app.py'
```

> 只启动**一个**实例。多个实例同时使用默认 SQLite 数据目录会互相锁库。

### 4.3 访问地址

- 仅本机（`WEB_HOST = '127.0.0.1'`）：<http://127.0.0.1:9900>
- 局域网/公网（`WEB_HOST = '0.0.0.0'`）：`http://<机器IP>:9900`
  - Windows 首次监听 `0.0.0.0` 可能弹防火墙授权，选「专用网络」允许；
  - 公网访问请确认已在路由器/安全组放行端口，且 `LOGIN_PWD` 非空。
- 登录逻辑：`LOGIN_PWD` 为空时访问 `/` 会自动跳转 `/login` 并自动登录，浏览器直接可用；设置了密码则先输入密码。
- WSL2：Windows 浏览器通常可直接访问 `http://127.0.0.1:9900`；若不通，把 `WEB_HOST` 改为 `0.0.0.0`，用 `hostname -I` 得到的 WSL IP 访问。

### 4.4 macOS Apple Silicon 补充（进程被系统 kill 时）

M 系列芯片上如果启动后进程很快被系统杀掉（`Killed: 9`、`zsh: killed`，日志里没有 Python 异常），
是 macOS 对加密运行时 `.so` 的签名限制，需要对运行时做一次本地签名（来自官方 Mac 安装文档）：

1. 打开「钥匙串访问」→ 菜单「证书助手」→「创建证书」，名称填 `chanlun`，证书类型选「代码签名」，创建；
2. `security find-identity -v -p codesigning` 查到刚创建的证书 ID；
3. 在项目根目录执行（按实际 Python 版本替换 `.cpython-311-` 中的版本号）：

   ```bash
   cd src/pyarmor_runtime_005445/darwin_aarch64
   codesign --force -s "<上一步的证书ID>" pyarmor_runtime.cpython-311-darwin.so
   ```

4. 重新启动服务。

---

## 5. Gate 6：验收清单（逐项通过才算完成）

在项目根目录、服务运行状态下执行：

```powershell
# 1) 端口在监听（查不到时用兜底命令）
Get-NetTCPConnection -LocalPort 9900 -State Listen
# 兜底：netstat -ano | findstr ':9900'

# 2) 静态资源可访问（无需登录，最可靠的存活探针）
Invoke-WebRequest 'http://127.0.0.1:9900/static/favicon.ico' -UseBasicParsing -TimeoutSec 10 |
    Select-Object StatusCode          # 预期 200

# 3) 首页未登录会 302 到 /login（PowerShell 会把 302 当异常，属正常）
try { Invoke-WebRequest 'http://127.0.0.1:9900/' -UseBasicParsing -MaximumRedirection 0 | Out-Null }
catch { $_.Exception.Response.StatusCode.value__ }     # 预期 302

# 4) 带登录态的首页：先访问 /login（空密码会自动登录并跟随重定向建立会话），再取首页
$s = New-Object Microsoft.PowerShell.Commands.WebRequestSession
Invoke-WebRequest 'http://127.0.0.1:9900/login' -WebSession $s -UseBasicParsing | Out-Null
$r = Invoke-WebRequest 'http://127.0.0.1:9900/' -WebSession $s -UseBasicParsing -TimeoutSec 15
$r.StatusCode
[regex]::Match($r.Content, '<title>(.*?)</title>').Groups[1].Value
```

> 第 4 步如果刚启动就失败/仍是 302，等 2–3 秒重试；服务刚起时首次请求偶发未建立会话。
> 如果 `LOGIN_PWD` 非空，先按下面方式带密码登录，再做第 4 步；或直接用浏览器验证：
>
> ```powershell
> $s = New-Object Microsoft.PowerShell.Commands.WebRequestSession
> Invoke-WebRequest 'http://127.0.0.1:9900/login' -Method Post -Body @{password='<登录密码>'} -WebSession $s -UseBasicParsing | Out-Null
> ```

```bash
# macOS / Linux 对应检查
curl -s -o /dev/null -w 'favicon=%{http_code}\n' http://127.0.0.1:9900/static/favicon.ico
curl -s -c   /tmp/cl.cookies -o /dev/null http://127.0.0.1:9900/login
curl -s -b   /tmp/cl.cookies http://127.0.0.1:9900/ | grep -o '<title>[^<]*</title>'
```

**全部满足才算验收通过：**

- [ ] `uv sync` 退出码 0，`.venv/` 存在；
- [ ] 授权自检输出 `LICENSE OK`；
- [ ] 启动日志包含 `启动成功`，进程 30 秒后仍存活；
- [ ] 9900 端口处于 Listen；
- [ ] `/static/favicon.ico` 返回 200；
- [ ] 带登录态访问 `/` 返回 200，标题含「缠论解盘」；
- [ ] 已按用户要求确认访问范围（本机 / 局域网 / 公网）与登录密码策略。

---

## 6. 日常运维

| 操作 | 命令 / 位置 |
|---|---|
| 查看启动日志 | 前台：终端输出；后台：`~/.chanlun_pro/logs/web.out.log`、`web.err.log` |
| 应用自身日志 | `~/.chanlun_pro/logs/*.log`（如 `stocks_bkgn.log`、`check_chart_loss_alert.log`） |
| 数据目录 | `~/.chanlun_pro/`（可用 `config.DATA_PATH` 改）：`db/chanlun_klines.sqlite`、`klines/`、`secret_key` 等 |
| 重启 | 停进程 → 再执行 §4.2 启动命令 |
| 更新代码 | `git pull --ff-only` → `uv sync` → 重启（`config.py` 不会被覆盖） |
| 备份 | 停服务后复制整个 `~/.chanlun_pro/`（含 SQLite 与监控/回测记录） |
| 卸载 | 停服务 → 删除项目目录；确认后删除 `~/.chanlun_pro/` |

---

## 7. 故障排查

### 7.1 症状 → 原因 → 处理

| 症状 | 原因 | 处理 |
|---|---|---|
| `RuntimeError: missing license key to run the script` | 缺 `pyarmor.rkey` 或路径/文件名不对 | 放到 `src/pyarmor_runtime_005445/pyarmor.rkey`，重跑 §3.4 |
| `License is not for this machine` | 授权文件与本机不匹配 | `uv run python -m pyarmor.cli.hdinfo` 取机器信息，重新向作者申请 |
| `ImportError: chanlun 不支持当前平台：...` | 平台不受支持（Windows ARM64 / Linux ARM64 / Alpine） | 换 Windows x64、Linux x86_64(glibc)、macOS |
| `ModuleNotFoundError: No module named 'talib'` / `'ta_lib'` | 绕开 uv 用了 pip / conda 装依赖 | 删掉 `.venv`，回到 `uv sync` |
| macOS arm64 上 `uv sync` 报 ta-lib wheel 平台不匹配 | 系统版本低于 macOS 14 | 升级系统，或改用 Intel Mac / x86_64 环境 |
| `uv: command not found`（mac/linux） | uv 未安装或不在 PATH | 重新执行安装命令，并 `export PATH="$HOME/.local/bin:$PATH"` |
| `uv sync` 长时间卡住/超时 | 阿里云镜像或 Python 下载不通 | 见 §7.2 |
| Linux 启动报 `ImportError: libGL.so.1: cannot open shared object file` 或 `libglib-2.0.so.0` 缺失 | 无桌面环境的服务器缺图形库（opencv 依赖） | Ubuntu/Debian：`sudo apt-get install -y libgl1 libglib2.0-0`，再重启服务 |
| `OSError: [WinError 10048]` / `Address already in use` | 9900 被占用 | `Get-NetTCPConnection -LocalPort 9900` / `lsof -i:9900` 找到并停掉占用进程；确认不是另一个本服务实例 |
| `sqlite3.OperationalError: no such column: cl_alert_task.check_bi_status` | 机器上有旧版本遗留的 SQLite 库，schema 过旧 | 停服务 → 备份后删除 `~/.chanlun_pro/db/chanlun_klines.sqlite` → 重启重建；或改用 MySQL |
| 启动后立即退出并打印 `出现异常，按回车键退出` | 真正原因在同一条 traceback 上方 | 读完整报错，对照本表 |
| 启动日志大量 `BAD RESPONSE x.x.x.x` | 通达信服务器探测失败 | 正常现象，等待即可；若全部 BAD 且行情打不开，检查防火墙/网络 |
| `uv run` 每次打印 `Uninstalled/Installed 1 package`（pytdx） | 本仓库 wheel 元数据特性 | 正常，忽略 |
| 控制台中文乱码 | Windows 控制台编码 | 执行 `chcp 65001`，或直接看日志文件/浏览器页面，不影响运行 |
| 页面 302 跳来跳去打不开 | 设置了 `LOGIN_PWD` 但未登录 | 访问 `/login` 输入密码 |
| 自己写的 Python 脚本报 `No module named 'chanlun'` | 项目不是 pip 安装的包 | 脚本开头加 `sys.path.insert(0, '<项目根>/src')` |
| 改了 `config.py` 不生效，或日志/数据库指向别的目录 | 本机 `PYTHONPATH` 指向另一个 chanlun 项目的 `src`，实际 import 的是那份代码 | 清空 `PYTHONPATH` 后重启（见 §2.2） |
| 启动报 `database is locked` | 多个实例共用同一个 SQLite | 只保留一个实例，或给每个实例配置不同的 `DATA_PATH` |
| 启动报 `PermissionError` / 无法创建 `~/.chanlun_pro` | 服务账号的 HOME 不可写，或 `DATA_PATH` 指到了无权限目录 | 把 `DATA_PATH` 改成服务账号有写权限的绝对路径 |
| 远程机器上端口在监听但浏览器打不开 | `WEB_HOST` 仍为 `127.0.0.1`，或防火墙/安全组未放行 | 改为 `0.0.0.0` + 设置 `LOGIN_PWD`，并放行 9900 |

### 7.2 阿里云镜像不可用时（仅当 §3.2 下载失败）

```powershell
# Windows
.\script\bin\uv.exe sync --default-index https://pypi.org/simple
```

```bash
# macOS / Linux
uv sync --default-index https://pypi.org/simple
```

说明：该命令会重新解析依赖，**可能改写 `uv.lock` 中的镜像地址**（之后会固定走 PyPI）。
安装完成后如希望恢复仓库原样：`git checkout -- uv.lock`。海外网络环境通常不需要这一步。

---

## 8. 速查表

| 项目 | 值 |
|---|---|
| 代码仓库 | `https://gitee.com/wang-student/chanlun-pro.git`（备：`https://github.com/yijixiuxin/chanlun-pro.git`） |
| 项目根目录 | 含 `pyproject.toml` 的目录（示例：`C:\Users\<用户名>\chanlun-pro`） |
| Python | 3.11（`.python-version` 指定；uv 自动安装；支持 3.11–3.14） |
| 依赖安装 | `uv sync`（**不要**用 pip/conda 装本项目依赖） |
| 配置文件 | `src/chanlun/config.py`（由 `config.py.demo` 复制，已 gitignore） |
| 授权文件 | `src/pyarmor_runtime_005445/pyarmor.rkey` |
| 启动命令 | `uv run python -u web/chanlun_chart/app.py nobrowser` |
| 端口 | 9900（代码写死） |
| 访问地址 | `http://127.0.0.1:9900` |
| 存活探针 | `GET /static/favicon.ico` → 200 |
| 数据目录 | `~/.chanlun_pro/`（`DATA_PATH` 可改） |
| 日志 | `~/.chanlun_pro/logs/`，后台启动另有 `web.out.log` / `web.err.log` |
| Windows 自带 uv | `.\script\bin\uv.exe` |
| 停止服务 | 结束监听 9900 的进程（§4.2） |

---

## 9. 给用户的交付模板（AI 最终回复用）

```
安装已完成，验收通过。

- 项目目录：<绝对路径>
- 访问地址：http://127.0.0.1:9900    （如开启局域网：http://<IP>:9900，登录密码：<密码或“未设置”>）
- 启动命令：<实际执行的命令>
- 日志位置：~/.chanlun_pro/logs/web.out.log 与 web.err.log
- 授权状态：已加载（授权信息：<授权名>）
- 验收结果：uv sync ✅  LICENSE ✅  端口 9900 ✅  favicon 200 ✅  首页 200 ✅

需要你处理/知晓：
- <如授权文件尚未提供，或其他待确认事项>
```

---

## 10. 参考链接

- 授权方式：<https://chanlun-pro.readthedocs.io/licensing/>
- Windows 安装（人工版）：<https://chanlun-pro.readthedocs.io/windows-installation/>
- 配置文件说明：<https://chanlun-pro.readthedocs.io/config-file-explained/>
- 常见问题：<https://chanlun-pro.readthedocs.io/FAQ/>
- Gitee 仓库：<https://gitee.com/wang-student/chanlun-pro>
- GitHub 仓库：<https://github.com/yijixiuxin/chanlun-pro>
