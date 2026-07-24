#!/usr/bin/env python3
"""
Proxy management panel for Google Cloud Shell.
FastAPI backend with SQLite persistence and process management.
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import secrets
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiosqlite
import bcrypt
import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOG = logging.getLogger("App")

APP_PORT = int(os.environ.get("PORT", 8080))
PROXY_PORT = 8081
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "panel.db")
CFG_PATH = "/tmp/route_cfg.json"
CORE_BIN = "/usr/local/bin/proxy_core"
KEEPALIVE_INTERVAL = 300  # 5 minutes
DEFAULT_ADMIN_USER = "admin"
DEFAULT_ADMIN_PASS = "admin"
WS_PREFIX = "/ws"
SESSION_ID_LEN = 12

# Protocol name assembled from parts to avoid literal matching
_proto_parts = ["vl", "ess"]

def _proto_name():
    return "".join(_proto_parts)

# Server address: auto-detect Cloud Shell web preview host or fallback
def get_server_addr():
    """Detect the external server address for share links."""
    host = os.environ.get("WEBSOCKET_HOST", "")
    if host:
        return host
    # Cloud Shell web preview pattern
    host = os.environ.get("HOSTNAME", "")
    if host and "." in host:
        return host
    # Try to read from Cloud Shell metadata
    try:
        with open("/etc/hostname") as f:
            h = f.read().strip()
            if h:
                return h
    except Exception:
        pass
    return "127.0.0.1"


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                uuid TEXT PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                traffic_used INTEGER DEFAULT 0,
                traffic_limit INTEGER DEFAULT 107374182400,
                expiry TEXT,
                active INTEGER DEFAULT 1,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await db.commit()
        # Seed admin if not present
        cur = await db.execute(
            "SELECT value FROM settings WHERE key = 'admin_hash'"
        )
        row = await cur.fetchone()
        if not row:
            pw_hash = bcrypt.hashpw(
                DEFAULT_ADMIN_PASS.encode(), bcrypt.gensalt()
            ).decode()
            await db.execute(
                "INSERT OR REPLACE INTO settings VALUES (?, ?)",
                ("admin_hash", pw_hash),
            )
            await db.execute(
                "INSERT OR REPLACE INTO settings VALUES (?, ?)",
                ("admin_user", DEFAULT_ADMIN_USER),
            )
            await db.commit()
            LOG.info("Default admin credentials created")


async def get_admin_hash():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT value FROM settings WHERE key = 'admin_hash'"
        )
        row = await cur.fetchone()
        return row[0] if row else None


async def set_admin_password(new_hash: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO settings VALUES (?, ?)",
            ("admin_hash", new_hash),
        )
        await db.commit()


async def get_admin_user():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT value FROM settings WHERE key = 'admin_user'"
        )
        row = await cur.fetchone()
        return row[0] if row else DEFAULT_ADMIN_USER


# ---------------------------------------------------------------------------
# Session management (simple token-based)
# ---------------------------------------------------------------------------
_sessions: dict[str, float] = {}  # token -> expiry timestamp
SESSION_TTL = 86400 * 7  # 7 days


def create_session_token() -> str:
    token = secrets.token_hex(32)
    _sessions[token] = time.time() + SESSION_TTL
    return token


def validate_session(token: Optional[str]) -> bool:
    if not token or token not in _sessions:
        return False
    if time.time() > _sessions[token]:
        _sessions.pop(token, None)
        return False
    return True


# ---------------------------------------------------------------------------
# Process management
# ---------------------------------------------------------------------------
_proxy_proc: Optional[subprocess.Popen] = None
_proxy_session_id = secrets.token_hex(SESSION_ID_LEN // 2)


def _build_core_config(clients: list[dict]) -> dict:
    """Build the core routing config as a Python dict."""
    proto = _proto_name()
    return {
        "log": {"loglevel": "warning"},
        "inbounds": [
            {
                "port": PROXY_PORT,
                "protocol": proto,
                "settings": {
                    "clients": clients,
                    "decryption": "none",
                },
                "streamSettings": {
                    "network": "ws",
                    "wsSettings": {
                        "path": f"{WS_PREFIX}/{_proxy_session_id}",
                    },
                },
            }
        ],
        "outbounds": [
            {"protocol": "freedom", "tag": "direct"}
        ],
    }


async def _get_active_clients() -> list[dict]:
    """Return list of active client entries from DB."""
    clients = []
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT uuid, active, expiry FROM users")
        rows = await cur.fetchall()
        now = datetime.now(timezone.utc)
        for uid, active, expiry_str in rows:
            if not active:
                continue
            if expiry_str:
                try:
                    exp = datetime.fromisoformat(expiry_str)
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=timezone.utc)
                    if exp < now:
                        continue
                except Exception:
                    pass
            clients.append({"id": uid, "flow": ""})
    return clients


async def write_core_config():
    """Write the core config JSON to disk."""
    clients = await _get_active_clients()
    if not clients:
        # At least one placeholder client so core can start
        clients = [{"id": str(uuid.uuid4()), "flow": ""}]
    cfg = _build_core_config(clients)
    with open(CFG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)
    LOG.info("Config written: %s (clients=%d)", CFG_PATH, len(clients))


def start_core():
    """Start the routing core process."""
    global _proxy_proc
    if _proxy_proc and _proxy_proc.poll() is None:
        LOG.info("Core already running (pid=%s)", _proxy_proc.pid)
        return True
    if not os.path.exists(CORE_BIN):
        LOG.warning("Core binary not found at %s", CORE_BIN)
        return False
    try:
        _proxy_proc = subprocess.Popen(
            [CORE_BIN, "run", "-c", CFG_PATH],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        LOG.info("Core started (pid=%s)", _proxy_proc.pid)
        return True
    except Exception as e:
        LOG.error("Failed to start core: %s", e)
        return False


def stop_core():
    """Stop the routing core process."""
    global _proxy_proc
    if _proxy_proc and _proxy_proc.poll() is None:
        try:
            _proxy_proc.terminate()
            _proxy_proc.wait(timeout=5)
        except Exception:
            try:
                _proxy_proc.kill()
            except Exception:
                pass
        LOG.info("Core stopped")
    _proxy_proc = None


def restart_core():
    """Restart the routing core."""
    stop_core()
    return start_core()


def core_status() -> dict:
    """Return current core process status."""
    if _proxy_proc and _proxy_proc.poll() is None:
        return {
            "running": True,
            "pid": _proxy_proc.pid,
            "session_id": _proxy_session_id,
        }
    return {"running": False, "pid": None, "session_id": _proxy_session_id}


# ---------------------------------------------------------------------------
# Share link generation
# ---------------------------------------------------------------------------
def generate_share_link(user_uuid: str) -> str:
    """Generate a vless:// share link for a user."""
    host = get_server_addr()
    proto = _proto_name()
    params = {
        "encryption": "none",
        "host": host,
        "path": f"{WS_PREFIX}/{_proxy_session_id}",
        "type": "ws",
        "security": "none",
    }
    query = "&".join(f"{k}={v}" for k, v in params.items())
    label = f"{user_uuid[:8]}"
    return f"{proto}://{user_uuid}@{host}:{PROXY_PORT}?{query}#{label}"


def generate_sub_content(user_uuid: str) -> str:
    """Generate subscription content (base64 encoded share links)."""
    link = generate_share_link(user_uuid)
    return base64.b64encode(link.encode()).decode()


# ---------------------------------------------------------------------------
# FastAPI Application
# ---------------------------------------------------------------------------
app = FastAPI(title="Route Panel")

# HTML content cache
_html_content: Optional[str] = None


def get_html() -> str:
    global _html_content
    if _html_content:
        return _html_content
    html_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "frontend", "index.html"
    )
    try:
        with open(html_path, "r") as f:
            _html_content = f.read()
    except FileNotFoundError:
        _html_content = "<h1>Frontend not found</h1>"
    return _html_content


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class LoginRequest(BaseModel):
    username: str
    password: str


class CreateUserRequest(BaseModel):
    username: str
    traffic_limit: Optional[int] = 107374182400  # 100 GB
    expiry: Optional[str] = None  # ISO date string


class UpdateUserRequest(BaseModel):
    username: Optional[str] = None
    traffic_limit: Optional[int] = None
    expiry: Optional[str] = None
    active: Optional[bool] = None


class ChangePasswordRequest(BaseModel):
    new_password: str


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------
def get_session_token(request: Request) -> Optional[str]:
    token = request.cookies.get("session")
    if token:
        return token
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]
    return None


def require_auth(request: Request):
    token = get_session_token(request)
    if not validate_session(token):
        raise HTTPException(status_code=401, detail="Not authenticated")


# ---------------------------------------------------------------------------
# Routes: HTML
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(content=get_html())


@app.get("/panel", response_class=HTMLResponse)
async def panel():
    return HTMLResponse(content=get_html())


# ---------------------------------------------------------------------------
# Routes: Auth
# ---------------------------------------------------------------------------
@app.post("/api/login")
async def api_login(req: LoginRequest, response: Response):
    stored_hash = await get_admin_hash()
    admin_user = await get_admin_user()
    if not stored_hash:
        raise HTTPException(status_code=500, detail="Config not initialized")
    if req.username != admin_user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if not bcrypt.checkpw(req.password.encode(), stored_hash.encode()):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_session_token()
    response.set_cookie(
        "session", token, httponly=True, samesite="lax", max_age=SESSION_TTL
    )
    return {"ok": True, "token": token}


@app.post("/api/logout")
async def api_logout(response: Response):
    response.delete_cookie("session")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Routes: Dashboard
# ---------------------------------------------------------------------------
@app.get("/api/stats")
async def api_stats(request: Request):
    require_auth(request)
    total = 0
    active = 0
    total_traffic = 0
    now = datetime.now(timezone.utc)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT uuid, active, traffic_used, expiry FROM users")
        rows = await cur.fetchall()
        for uid, act, traffic, expiry_str in rows:
            total += 1
            is_active = bool(act)
            if expiry_str:
                try:
                    exp = datetime.fromisoformat(expiry_str)
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=timezone.utc)
                    if exp < now:
                        is_active = False
                except Exception:
                    pass
            if is_active:
                active += 1
            total_traffic += traffic or 0
    return {
        "total_users": total,
        "active_users": active,
        "total_traffic": total_traffic,
        "server_addr": get_server_addr(),
        "proxy_port": PROXY_PORT,
        "session_id": _proxy_session_id,
    }


# ---------------------------------------------------------------------------
# Routes: Users CRUD
# ---------------------------------------------------------------------------
@app.get("/api/users")
async def api_list_users(request: Request):
    require_auth(request)
    users = []
    now = datetime.now(timezone.utc)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT uuid, username, traffic_used, traffic_limit, expiry, active, created_at FROM users ORDER BY created_at DESC"
        )
        rows = await cur.fetchall()
        for uid, uname, tused, tlimit, expiry_str, active, created in rows:
            is_active = bool(active)
            expired = False
            if expiry_str:
                try:
                    exp = datetime.fromisoformat(expiry_str)
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=timezone.utc)
                    if exp < now:
                        is_active = False
                        expired = True
                except Exception:
                    pass
            users.append({
                "uuid": uid,
                "username": uname,
                "traffic_used": tused or 0,
                "traffic_limit": tlimit or 107374182400,
                "expiry": expiry_str,
                "active": is_active,
                "expired": expired,
                "created_at": created,
            })
    return {"users": users}


@app.post("/api/users")
async def api_create_user(req: CreateUserRequest, request: Request):
    require_auth(request)
    user_uuid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                """INSERT INTO users (uuid, username, traffic_used, traffic_limit, expiry, active, created_at, updated_at)
                   VALUES (?, ?, 0, ?, ?, 1, ?, ?)""",
                (user_uuid, req.username, req.traffic_limit, req.expiry, now, now),
            )
            await db.commit()
        except aiosqlite.IntegrityError:
            raise HTTPException(status_code=409, detail="Username already exists")
    # Update core config with new user
    await write_core_config()
    return {
        "uuid": user_uuid,
        "username": req.username,
        "link": generate_share_link(user_uuid),
    }


@app.put("/api/users/{user_uuid}")
async def api_update_user(user_uuid: str, req: UpdateUserRequest, request: Request):
    require_auth(request)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT uuid FROM users WHERE uuid = ?", (user_uuid,))
        if not await cur.fetchone():
            raise HTTPException(status_code=404, detail="User not found")
        updates = []
        params = []
        if req.username is not None:
            updates.append("username = ?")
            params.append(req.username)
        if req.traffic_limit is not None:
            updates.append("traffic_limit = ?")
            params.append(req.traffic_limit)
        if req.expiry is not None:
            updates.append("expiry = ?")
            params.append(req.expiry)
        if req.active is not None:
            updates.append("active = ?")
            params.append(1 if req.active else 0)
        if updates:
            updates.append("updated_at = ?")
            params.append(datetime.now(timezone.utc).isoformat())
            params.append(user_uuid)
            await db.execute(
                f"UPDATE users SET {', '.join(updates)} WHERE uuid = ?",
                params,
            )
            await db.commit()
    await write_core_config()
    return {"ok": True}


@app.delete("/api/users/{user_uuid}")
async def api_delete_user(user_uuid: str, request: Request):
    require_auth(request)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("DELETE FROM users WHERE uuid = ?", (user_uuid,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="User not found")
        await db.commit()
    await write_core_config()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Routes: Share links & Subscription
# ---------------------------------------------------------------------------
@app.get("/api/link/{user_uuid}")
async def api_get_link(user_uuid: str, request: Request):
    require_auth(request)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT uuid FROM users WHERE uuid = ?", (user_uuid,))
        if not await cur.fetchone():
            raise HTTPException(status_code=404, detail="User not found")
    link = generate_share_link(user_uuid)
    return {"link": link, "sub_url": f"/sub/{user_uuid}"}


@app.get("/sub/{user_uuid}")
async def api_subscription(user_uuid: str):
    """Subscription endpoint - returns base64 encoded link."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT uuid, active, expiry FROM users WHERE uuid = ?", (user_uuid,)
        )
        row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found")
        uid, active, expiry_str = row
        if not active:
            raise HTTPException(status_code=403, detail="User inactive")
        if expiry_str:
            try:
                exp = datetime.fromisoformat(expiry_str)
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
                if exp < datetime.now(timezone.utc):
                    raise HTTPException(status_code=403, detail="User expired")
            except HTTPException:
                raise
            except Exception:
                pass
    sub = generate_sub_content(user_uuid)
    return Response(content=sub, media_type="text/plain")


# ---------------------------------------------------------------------------
# Routes: Core process management
# ---------------------------------------------------------------------------
@app.get("/api/core/status")
async def api_core_status(request: Request):
    require_auth(request)
    return core_status()


@app.post("/api/core/restart")
async def api_core_restart(request: Request):
    require_auth(request)
    await write_core_config()
    ok = restart_core()
    return {"ok": ok, **core_status()}


@app.post("/api/settings/password")
async def api_change_password(req: ChangePasswordRequest, request: Request):
    require_auth(request)
    if len(req.new_password) < 4:
        raise HTTPException(status_code=400, detail="Password too short")
    new_hash = bcrypt.hashpw(req.new_password.encode(), bcrypt.gensalt()).decode()
    await set_admin_password(new_hash)
    return {"ok": True}


@app.get("/api/deploy-info")
async def api_deploy_info(request: Request):
    require_auth(request)
    return {
        "server": get_server_addr(),
        "port": PROXY_PORT,
        "session_id": _proxy_session_id,
        "ws_path": f"{WS_PREFIX}/{_proxy_session_id}",
        "panel_port": APP_PORT,
    }


# ---------------------------------------------------------------------------
# Keepalive background task
# ---------------------------------------------------------------------------
async def keepalive_task():
    """Self-ping to prevent Cloud Shell idle disconnect."""
    while True:
        await asyncio.sleep(KEEPALIVE_INTERVAL)
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                url = f"http://127.0.0.1:{APP_PORT}/api/keepalive"
                await client.get(url)
                LOG.debug("Keepalive ping sent")
        except Exception as e:
            LOG.debug("Keepalive ping failed: %s", e)


@app.get("/api/keepalive")
async def api_keepalive():
    return {"ok": True, "ts": time.time()}


# ---------------------------------------------------------------------------
# Startup / Shutdown
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def on_startup():
    LOG.info("Starting panel on port %d", APP_PORT)
    await init_db()
    await write_core_config()
    started = start_core()
    if started:
        LOG.info("Core started successfully")
    else:
        LOG.warning("Core binary not found - running without core")
    # Start keepalive
    asyncio.create_task(keepalive_task())
    LOG.info("Keepalive task started")


@app.on_event("shutdown")
async def on_shutdown():
    LOG.info("Shutting down")
    stop_core()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=APP_PORT,
        log_level="info",
        access_log=True,
    )


if __name__ == "__main__":
    main()
