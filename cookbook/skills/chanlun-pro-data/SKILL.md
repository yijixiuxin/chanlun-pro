---
name: chanlun-pro-data
description: "获取 chanlun-pro 项目的行情数据与缠论结构化数据，支持多市场多周期"
metadata:
  project_path: "<设置为你本地的 chanlun-pro 项目路径>"
  execution: "script\bin\uv.exe run"
---

# Chanlun-Pro Data Retrieval Skill

## 项目路径

**chanlun-pro 项目路径：** `<CHANLUN_PRO_PATH>` （使用前请在配置中设置为你本地的项目路径）

## 脚本执行方式

必须先 cd 到 `<CHANLUN_PRO_PATH>` 目录，使用目录下的 `script\bin\uv.exe` 执行命令：

```bash
cd <CHANLUN_PRO_PATH>
script\bin\uv.exe run python src\chanlun\tools\skill\get_cl_data.py --fun batch_get_cl_data --market a --codes SH.000001
```

## 核心功能

### 1. 缠论数据获取脚本

**脚本路径：** `src\chanlun\tools\skill\get_cl_data.py`

**支持的方法：**

#### get_cl_data
- **功能：** 获取单个标的的缠论数据
- **参数：**
  - `market`：市场标识，如 'a', 'hk', 'futures' 等
  - `code`：标的代码
  - `frequency`：周期
- **示例：**
  ```bash
  script\bin\uv.exe run python src\chanlun\tools\skill\get_cl_data.py --fun get_cl_data --market a --code SH.600519 --frequency d
  ```

#### get_cl_structured_data
- **功能：** 获取缠论结构化数据（适合AI处理）
- **参数：**
  - `market`：市场标识
  - `code`：标的代码
  - `frequency`：周期
- **示例：**
  ```bash
  script\bin\uv.exe run python src\chanlun\tools\skill\get_cl_data.py --fun get_cl_structured_data --market a --code SH.600519 --frequency d
  ```

#### batch_get_cl_data
- **功能：** 批量获取多个标的的缠论结构化数据
- **参数：**
  - `market`：市场标识
  - `codes`：标的代码列表，逗号分隔
  - `frequency`：周期
- **示例：**
  ```bash
  script\bin\uv.exe run python src\chanlun\tools\skill\get_cl_data.py --fun batch_get_cl_data --market a --codes SH.600519,SH.601398 --frequency d
  ```

### 2. 行情数据获取脚本

**脚本路径：** `src\chanlun\tools\skill\get_market_data.py`

**支持的方法：**

#### list_supported_markets
- **功能：** 返回支持的市场列表
- **参数：** 无
- **示例：**
  ```bash
  script\bin\uv.exe run python src\chanlun\tools\skill\get_market_data.py --fun list_supported_markets
  ```

#### list_supported_frequencies
- **功能：** 返回支持的周期列表
- **参数：**
  - `market`：市场标识（可选）
- **示例：**
  ```bash
  script\bin\uv.exe run python src\chanlun\tools\skill\get_market_data.py --fun list_supported_frequencies
  script\bin\uv.exe run python src\chanlun\tools\skill\get_market_data.py --fun list_supported_frequencies --market a
  ```

#### list_all_market_frequencies
- **功能：** 返回所有市场各自支持的周期列表
- **参数：** 无
- **示例：**
  ```bash
  script\bin\uv.exe run python src\chanlun\tools\skill\get_market_data.py --fun list_all_market_frequencies
  ```

#### get_market_data
- **功能：** 获取单个标的的行情数据
- **参数：**
  - `market`：市场标识
  - `code`：标的代码
  - `frequency`：周期
- **示例：**
  ```bash
  script\bin\uv.exe run python src\chanlun\tools\skill\get_market_data.py --fun get_market_data --market a --code SH.600519 --frequency d
  ```

#### get_multiple_market_data
- **功能：** 获取多个标的的行情数据
- **参数：**
  - `market`：市场标识
  - `codes`：标的代码列表，逗号分隔
  - `frequency`：周期
- **示例：**
  ```bash
  script\bin\uv.exe run python src\chanlun\tools\skill\get_market_data.py --fun get_multiple_market_data --market a --codes SH.600519,SH.601398 --frequency d
  ```

## 支持的市场

- `a` - 沪深A股
- `hk` - 港股
- `futures` - 国内期货
- `ny_futures` - 美股期货
- `currency` - 数字货币合约
- `currency_spot` - 数字货币现货
- `us` - 美股
- `fx` - 外汇

## 支持的周期

- 1m（1分钟）
- 5m（5分钟）
- 15m（15分钟）
- 30m（30分钟）
- 60m（60分钟）
- d（日线）
- w（周线）
- m（月线）

## 输出格式

所有脚本执行结果均以 JSON 格式输出，方便后续处理和分析。
