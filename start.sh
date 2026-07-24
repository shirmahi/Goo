#!/bin/bash
set -e

echo "=== Route Panel - Setup ==="

# Install Python dependencies
echo "[1/3] Installing dependencies..."
pip install -q fastapi uvicorn aiosqlite bcrypt httpx 2>/dev/null

# Download routing core if not present
CORE_BIN="/usr/local/bin/proxy_core"
if [ ! -f "$CORE_BIN" ]; then
  echo "[2/3] Downloading routing core..."
  curl -sL "https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-64.zip" -o /tmp/core.zip
  cd /tmp && unzip -o core.zip xray -d /tmp/ 2>/dev/null
  if [ -f /tmp/xray ]; then
    mv /tmp/xray "$CORE_BIN"
    chmod +x "$CORE_BIN"
    echo "     Core installed at $CORE_BIN"
  else
    echo "     WARNING: Core download failed. Panel will run without routing."
  fi
  rm -f /tmp/core.zip
else
  echo "[2/3] Routing core already installed"
fi

# Start the panel
echo "[3/3] Starting panel..."
cd "$(dirname "$0")"
python main.py
