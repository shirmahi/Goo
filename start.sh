#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Route Panel - Setup ==="

# Install Python dependencies
echo "[1/2] Installing dependencies..."
pip install -q fastapi uvicorn aiosqlite bcrypt 2>/dev/null

# Start the panel
echo "[2/2] Starting panel..."
cd "$SCRIPT_DIR"
python main_ssr.py
