#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CORE_BIN="$HOME/.local/bin/xray"

echo "=== Route Panel — Cloud Shell Edition ==="

# Install Python dependencies
echo "[1/3] Installing dependencies..."
pip install -q fastapi uvicorn aiosqlite bcrypt websockets httpx 2>/dev/null

# Download Xray if not present
mkdir -p "$HOME/.local/bin"
if [ ! -f "$CORE_BIN" ]; then
  echo "[2/3] Downloading Xray core..."
  curl -sL "https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-64.zip" -o /tmp/xray.zip
  cd /tmp && unzip -o xray.zip xray -d /tmp/ 2>/dev/null
  if [ -f /tmp/xray ]; then
    mv /tmp/xray "$CORE_BIN"
    chmod +x "$CORE_BIN"
    echo "     Xray installed at $CORE_BIN"
  else
    echo "     WARNING: Xray download failed. Panel will run without proxy."
  fi
  rm -f /tmp/xray.zip /tmp/geoip.dat /tmp/geosite.dat
else
  echo "[2/3] Xray already installed"
fi

# Detect Cloud Shell Web Preview host
if [ -n "$DEVSHELL_HOSTNAME" ]; then
  export SHELL_HOST="8080-$DEVSHELL_HOSTNAME"
  echo "     Host: $SHELL_HOST"
fi

# Start the panel
echo "[3/3] Starting panel..."
cd "$SCRIPT_DIR"
python main_ssr.py
