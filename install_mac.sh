#!/bin/bash
# chanlun-pro 一键安装脚本 (Mac)

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "=== chanlun-pro Mac 安装脚本 ==="
echo "项目目录: $PROJECT_DIR"

# 1. 安装 uv
echo ""
echo "[1/5] 检查并安装 uv..."
if command -v uv &> /dev/null; then
    echo "uv 已安装，版本: $(uv --version)"
else
    echo "正在安装 uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source "$HOME/.local/bin/env" 2>/dev/null || true
    export PATH="$HOME/.local/bin:$PATH"
fi

# 2. 创建虚拟环境并同步依赖
echo ""
echo "[2/5] 创建虚拟环境并安装依赖..."
uv sync

# 3. 复制配置文件
echo ""
echo "[3/5] 检查配置文件..."
if [ ! -f "src/chanlun/config.py" ]; then
    echo "正在复制 config.py.demo -> config.py..."
    cp src/chanlun/config.py.demo src/chanlun/config.py
    echo "请编辑 src/chanlun/config.py 配置相关参数"
else
    echo "config.py 已存在，跳过"
fi

# 4. 检查授权文件
echo ""
echo "[4/5] 检查 PyArmor 授权文件..."
if [ -f "src/pyarmor_runtime_005445/pyarmor.rkey" ]; then
    echo "授权文件已存在"
else
    echo "授权文件不存在，正在获取机器信息..."
    echo ""
    echo "请将以下信息发送给作者，获取正式授权文件（试用授权不需要机器信息）:"
    echo "================================"
    uv run -m pyarmor.cli.hdinfo
    echo "================================"
    echo ""
    echo "获取授权文件后，请放置到 src/pyarmor_runtime_005445/ 目录下"
fi

echo ""
echo "=== 安装完成! ==="
echo ""
echo "启动服务:"
echo "  uv run web/chanlun_chart/app.py"
echo ""
echo "启动后访问 http://localhost:9900"
