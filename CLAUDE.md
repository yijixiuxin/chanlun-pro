# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**chanlun-pro** is a 缠论 (Chan Lun) market analysis platform for Chinese financial markets. It implements the "缠中说禅" technical analysis theory for analyzing stocks, futures, and cryptocurrency markets.

Supported markets: 沪深A股, 港股, 美股, 国内期货, 纽约期货, 外汇, 数字货币

## Setup Commands

```bash
# Install dependencies (uv preferred)
uv sync

# Run web application (port 9900)
uv run  web/chanlun_chart/app.py
```

**Environment requirements:**
- Python 3.11+
- PYTHONPATH must be set to `src` directory: `export PYTHONPATH=/path/to/src`
- PyArmor license file required at `src/pyarmor_runtime_005445/pyarmor.rkey`
- Copy `src/chanlun/config.py.demo` to `src/chanlun/config.py` and configure

## Architecture

```
src/chanlun/           # Core缠论 library
├── exchange/          # Market data adapters (get_exchange(market) factory)
├── backtesting/       # Backtesting engine
├── strategy/          # Strategy implementations
├── trader/            # Live trading adapters
├── xuangu/            # Stock screening
├── cl.py              # Core缠论 calculation (PyArmor encrypted - DO NOT MODIFY)
├── cl_analyse.py      # Analysis tools
└── config.py          # Configuration (gitignored, use config.py.demo)

web/chanlun_chart/     # Flask web UI
├── app.py             # Entry point (runs on port 9900)
└── cl_app/            # Flask application with TradingView charts

notebook/              # Jupyter notebooks for backtesting
cookbook/docs/         # Documentation (MkDocs)
```

## Key Patterns

- **Market enum**: `from chanlun.base import Market` - defines markets like `Market.A`, `Market.HK`, `Market.FUTURES`
- **Exchange factory**: `from chanlun.exchange import get_exchange` - creates exchange adapters
- **Core缠论**: `from chanlun import cl_interface` - main interface to缠论 calculations
- **Incremental kline processing**: `process_klines()` method for efficient incremental updates

## Configuration

`src/chanlun/config.py` is gitignored. Copy `config.py.demo` as template. Key settings:
- `EXCHANGE_*` - market data adapters (tdx, baostock, futu, binance, etc.)
- `DB_TYPE` - mysql or sqlite
- `DATA_PATH` - where to store market data
- Exchange API keys as needed (FUTU_HOST, BINANCE_APIKEY, etc.)
