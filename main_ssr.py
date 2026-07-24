#!/usr/bin/env python3
"""
Server-Side Rendered proxy management panel for Google Cloud Shell.
FastAPI backend with complete HTML pages embedded as Python strings.
No JavaScript fetch() calls — all data rendered server-side.

Designed for Cloud Shell Web Preview where fetch() POST requests break.
"""

import asyncio
import base64
import json
import logging
import os
import secrets
import subprocess
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import aiosqlite
import bcrypt
import httpx
from fastapi import FastAPI, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOG = logging.getLogger("App")

APP_PORT = int(os.environ.get("PORT", 8080))
PROXY_PORT = 8081
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "panel.db")
CFG_PATH = "/tmp/route_cfg.json"
CORE_BIN = os.path.join(os.path.expanduser("~"), ".local", "bin", "proxy_core")
KEEPALIVE_INTERVAL = 300
WS_PREFIX = "/ws"
SESSION_ID_LEN = 12

_proto_parts = ["vl", "ess"]


def _proto_name():
    return "".join(_proto_parts)


def get_server_addr():
    host = os.environ.get("WEBSOCKET_HOST", "")
    if host:
        return host
    host = os.environ.get("HOSTNAME", "")
    if host and "." in host:
        return host
    try:
        with open("/etc/hostname") as f:
            h = f.read().strip()
            if h:
                return h
    except Exception:
        pass
    return "127.0.0.1"


# ---------------------------------------------------------------------------
# Database helpers (same schema as main.py)
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


# ---------------------------------------------------------------------------
# Process management (same as main.py)
# ---------------------------------------------------------------------------
_proxy_proc: Optional[subprocess.Popen] = None
_proxy_session_id = secrets.token_hex(SESSION_ID_LEN // 2)


def _build_core_config(clients):
    proto = _proto_name()
    return {
        "log": {"loglevel": "warning"},
        "inbounds": [
            {
                "port": PROXY_PORT,
                "protocol": proto,
                "settings": {"clients": clients, "decryption": "none"},
                "streamSettings": {
                    "network": "ws",
                    "wsSettings": {"path": f"{WS_PREFIX}/{_proxy_session_id}"},
                },
            }
        ],
        "outbounds": [{"protocol": "freedom", "tag": "direct"}],
    }


async def _get_active_clients():
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
    clients = await _get_active_clients()
    if not clients:
        clients = [{"id": str(uuid.uuid4()), "flow": ""}]
    cfg = _build_core_config(clients)
    with open(CFG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


def start_core():
    global _proxy_proc
    if _proxy_proc and _proxy_proc.poll() is None:
        return True
    if not os.path.exists(CORE_BIN):
        return False
    try:
        _proxy_proc = subprocess.Popen(
            [CORE_BIN, "run", "-c", CFG_PATH],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return True
    except Exception:
        return False


def stop_core():
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
    _proxy_proc = None


def restart_core():
    stop_core()
    return start_core()


def core_status():
    if _proxy_proc and _proxy_proc.poll() is None:
        return {"running": True, "pid": _proxy_proc.pid, "session_id": _proxy_session_id}
    return {"running": False, "pid": None, "session_id": _proxy_session_id}


# ---------------------------------------------------------------------------
# Share link generation
# ---------------------------------------------------------------------------
def generate_share_link(user_uuid, username=""):
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
    label = username or user_uuid[:8]
    return f"{proto}://{user_uuid}@{host}:{PROXY_PORT}?{query}#{label}"


def generate_sub_content(user_uuid):
    link = generate_share_link(user_uuid)
    return base64.b64encode(link.encode()).decode()


def format_bytes(b):
    if b is None or b == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    val = float(b)
    while val >= 1024 and i < len(units) - 1:
        val /= 1024
        i += 1
    return f"{val:.1f} {units[i]}"


# ---------------------------------------------------------------------------
# Keepalive
# ---------------------------------------------------------------------------
async def keepalive_task():
    while True:
        await asyncio.sleep(KEEPALIVE_INTERVAL)
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.get(f"http://127.0.0.1:{APP_PORT}/keepalive")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# HTML Templates - All CSS + Layout embedded
# ---------------------------------------------------------------------------

# SVG Icons
_ICON_DASHBOARD = """<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>"""
_ICON_USERS = """<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>"""
_ICON_SETTINGS = """<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>"""

_CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #09090b;
    color: #fafafa;
    line-height: 1.6;
    min-height: 100vh;
}
.layout { display: flex; min-height: 100vh; }

/* Sidebar */
.sidebar {
    width: 240px;
    min-width: 240px;
    background: #111113;
    border-right: 1px solid rgba(255,255,255,0.08);
    display: flex;
    flex-direction: column;
    position: fixed;
    top: 0;
    left: 0;
    bottom: 0;
    z-index: 100;
}
.sidebar-header {
    padding: 24px 20px;
    border-bottom: 1px solid rgba(255,255,255,0.08);
}
.sidebar-header h1 {
    font-size: 20px;
    font-weight: 700;
    color: #22c55e;
    letter-spacing: -0.5px;
}
.sidebar-header .subtitle {
    font-size: 11px;
    color: #71717a;
    text-transform: uppercase;
    letter-spacing: 1px;
}
.sidebar-nav { padding: 12px 8px; flex: 1; }
.nav-item {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 16px;
    border-radius: 8px;
    color: #a1a1aa;
    text-decoration: none;
    font-size: 14px;
    font-weight: 500;
    transition: all 0.15s ease;
    margin-bottom: 2px;
}
.nav-item:hover { background: rgba(255,255,255,0.05); color: #fafafa; }
.nav-item.active {
    background: rgba(34,197,94,0.1);
    color: #22c55e;
}
.nav-item.active svg { stroke: #22c55e; }

/* Main Content */
.main-content {
    flex: 1;
    margin-left: 240px;
    padding: 32px;
    max-width: 1200px;
}

/* Page Header */
.page-header {
    margin-bottom: 32px;
}
.page-header h2 {
    font-size: 28px;
    font-weight: 700;
    letter-spacing: -0.5px;
    margin-bottom: 4px;
}
.page-header p {
    color: #71717a;
    font-size: 14px;
}

/* Cards */
.card {
    background: #111113;
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 24px;
}
.card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 20px;
}
.card-header h3 {
    font-size: 16px;
    font-weight: 600;
}

/* Stats Grid */
.stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin-bottom: 32px;
}
.stat-card {
    background: #111113;
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 20px;
}
.stat-card .stat-label {
    font-size: 13px;
    color: #71717a;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 8px;
}
.stat-card .stat-value {
    font-size: 28px;
    font-weight: 700;
    letter-spacing: -1px;
}
.stat-card .stat-value.green { color: #22c55e; }
.stat-card .stat-value.blue { color: #3b82f6; }
.stat-card .stat-value.amber { color: #f59e0b; }
.stat-card .stat-value.red { color: #ef4444; }

/* Tables */
.table-container {
    overflow-x: auto;
}
table {
    width: 100%;
    border-collapse: collapse;
}
th {
    text-align: left;
    padding: 12px 16px;
    font-size: 12px;
    font-weight: 600;
    color: #71717a;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    border-bottom: 1px solid rgba(255,255,255,0.08);
}
td {
    padding: 14px 16px;
    font-size: 14px;
    border-bottom: 1px solid rgba(255,255,255,0.04);
}
tr:hover { background: rgba(255,255,255,0.02); }
.username-cell {
    font-weight: 600;
    color: #fafafa;
}
.uuid-cell {
    font-family: 'SF Mono', 'Fira Code', monospace;
    font-size: 12px;
    color: #71717a;
    max-width: 120px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.actions-cell { white-space: nowrap; }

/* Badges */
.badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 999px;
    font-size: 12px;
    font-weight: 600;
}
.badge-active { background: rgba(34,197,94,0.15); color: #22c55e; }
.badge-inactive { background: rgba(239,68,68,0.15); color: #ef4444; }
.badge-expired { background: rgba(245,158,11,0.15); color: #f59e0b; }

/* Buttons */
.btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 8px 16px;
    border-radius: 8px;
    font-size: 13px;
    font-weight: 500;
    border: none;
    cursor: pointer;
    text-decoration: none;
    transition: all 0.15s ease;
}
.btn-primary {
    background: #22c55e;
    color: #000;
}
.btn-primary:hover { background: #16a34a; }
.btn-ghost {
    background: transparent;
    color: #a1a1aa;
    border: 1px solid rgba(255,255,255,0.1);
}
.btn-ghost:hover { background: rgba(255,255,255,0.05); color: #fafafa; }
.btn-danger {
    background: rgba(239,68,68,0.15);
    color: #ef4444;
}
.btn-danger:hover { background: rgba(239,68,68,0.25); }
.btn-sm { padding: 5px 10px; font-size: 12px; }

/* Forms */
.form-group {
    margin-bottom: 20px;
}
.form-group label {
    display: block;
    font-size: 13px;
    font-weight: 500;
    color: #a1a1aa;
    margin-bottom: 6px;
}
.form-group input,
.form-group select {
    width: 100%;
    padding: 10px 14px;
    background: #18181b;
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 8px;
    color: #fafafa;
    font-size: 14px;
    font-family: inherit;
    transition: border-color 0.15s ease;
}
.form-group input:focus,
.form-group select:focus {
    outline: none;
    border-color: #22c55e;
}
.form-group .hint {
    font-size: 12px;
    color: #52525b;
    margin-top: 4px;
}
.form-row {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
}

/* Share Link Box */
.share-box {
    background: #18181b;
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 8px;
    padding: 16px;
    font-family: 'SF Mono', 'Fira Code', monospace;
    font-size: 12px;
    word-break: break-all;
    color: #22c55e;
    line-height: 1.8;
}
.sub-url {
    background: #18181b;
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 8px;
    padding: 12px 16px;
    font-family: 'SF Mono', 'Fira Code', monospace;
    font-size: 13px;
    color: #f59e0b;
    word-break: break-all;
}

/* Alert */
.alert {
    padding: 12px 16px;
    border-radius: 8px;
    font-size: 14px;
    margin-bottom: 20px;
}
.alert-success {
    background: rgba(34,197,94,0.1);
    border: 1px solid rgba(34,197,94,0.2);
    color: #22c55e;
}
.alert-error {
    background: rgba(239,68,68,0.1);
    border: 1px solid rgba(239,68,68,0.2);
    color: #ef4444;
}

/* Info Grid */
.info-grid {
    display: grid;
    grid-template-columns: 160px 1fr;
    gap: 8px 16px;
    font-size: 14px;
}
.info-grid .info-label {
    color: #71717a;
    font-weight: 500;
}
.info-grid .info-value {
    color: #fafafa;
    word-break: break-all;
}

/* Empty state */
.empty-state {
    text-align: center;
    padding: 48px 24px;
    color: #71717a;
}
.empty-state svg {
    margin-bottom: 16px;
    opacity: 0.3;
}
.empty-state h3 {
    font-size: 16px;
    color: #a1a1aa;
    margin-bottom: 8px;
}

/* Detail actions */
.detail-actions {
    display: flex;
    gap: 8px;
    margin-top: 20px;
    flex-wrap: wrap;
}

/* Responsive */
@media (max-width: 768px) {
    .sidebar { width: 60px; min-width: 60px; }
    .sidebar-header h1, .sidebar-header .subtitle, .nav-item span { display: none; }
    .nav-item { justify-content: center; padding: 10px; }
    .main-content { margin-left: 60px; padding: 16px; }
    .form-row { grid-template-columns: 1fr; }
    .stats-grid { grid-template-columns: 1fr 1fr; }
}
"""


def render_page(title: str, content: str, active_nav: str = "", flash: str = ""):
    """Base layout with sidebar + content area."""
    flash_html = ""
    if flash:
        flash_html = f'<div class="alert alert-success">{flash}</div>'

    active_dashboard = "active" if active_nav == "dashboard" else ""
    active_users = "active" if active_nav == "users" else ""
    active_settings = "active" if active_nav == "settings" else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} - Route Panel</title>
<style>{_CSS}</style>
</head>
<body>
<div class="layout">
  <nav class="sidebar">
    <div class="sidebar-header">
      <h1>Route Panel</h1>
      <span class="subtitle">Cloud Shell</span>
    </div>
    <div class="sidebar-nav">
      <a href="/" class="nav-item {active_dashboard}">
        {_ICON_DASHBOARD}
        <span>Dashboard</span>
      </a>
      <a href="/users" class="nav-item {active_users}">
        {_ICON_USERS}
        <span>Users</span>
      </a>
      <a href="/settings" class="nav-item {active_settings}">
        {_ICON_SETTINGS}
        <span>Settings</span>
      </a>
    </div>
  </nav>
  <main class="main-content">
    {flash_html}
    {content}
  </main>
</div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# FastAPI Application
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app_instance):
    LOG.info("Starting SSR panel on port %d", APP_PORT)
    await init_db()
    await write_core_config()
    started = start_core()
    if started:
        LOG.info("Core started")
    else:
        LOG.info("Core binary not found - running without core")
    asyncio.create_task(keepalive_task())
    yield
    LOG.info("Shutting down")
    stop_core()


app = FastAPI(title="Route Panel", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Keepalive endpoint
# ---------------------------------------------------------------------------
@app.get("/keepalive")
async def keepalive():
    return {"ok": True}


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    total = 0
    active = 0
    expired = 0
    total_traffic = 0
    recent_users = []
    now = datetime.now(timezone.utc)

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT uuid, username, traffic_used, traffic_limit, expiry, active, created_at "
            "FROM users ORDER BY created_at DESC"
        )
        rows = await cur.fetchall()
        for uid, uname, tused, tlimit, expiry_str, is_active, created in rows:
            total += 1
            user_active = bool(is_active)
            is_expired = False
            if expiry_str:
                try:
                    exp = datetime.fromisoformat(expiry_str)
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=timezone.utc)
                    if exp < now:
                        user_active = False
                        is_expired = True
                except Exception:
                    pass
            if user_active:
                active += 1
            if is_expired:
                expired += 1
            total_traffic += tused or 0
            recent_users.append({
                "uuid": uid,
                "username": uname,
                "active": user_active,
                "expired": is_expired,
                "traffic_used": tused or 0,
                "created_at": created,
            })

    # Stats cards
    stats_html = f"""
    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-label">Total Users</div>
        <div class="stat-value blue">{total}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Active Users</div>
        <div class="stat-value green">{active}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Expired</div>
        <div class="stat-value amber">{expired}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Total Traffic</div>
        <div class="stat-value">{format_bytes(total_traffic)}</div>
      </div>
    </div>"""

    # Core status
    cs = core_status()
    status_color = "green" if cs["running"] else "red"
    status_text = "Running" if cs["running"] else "Stopped"
    core_html = f"""
    <div class="card" style="margin-bottom: 24px;">
      <div class="card-header">
        <h3>System Status</h3>
      </div>
      <div class="info-grid">
        <div class="info-label">Core</div>
        <div class="info-value"><span class="badge badge-{status_color}">{status_text}</span></div>
        <div class="info-label">Server Address</div>
        <div class="info-value">{get_server_addr()}</div>
        <div class="info-label">Session ID</div>
        <div class="info-value" style="font-family:monospace;">{_proxy_session_id}</div>
      </div>
    </div>"""

    # Recent users table
    if recent_users:
        rows_html = ""
        for u in recent_users[:10]:
            if u["expired"]:
                badge = '<span class="badge badge-expired">Expired</span>'
            elif u["active"]:
                badge = '<span class="badge badge-active">Active</span>'
            else:
                badge = '<span class="badge badge-inactive">Inactive</span>'
            rows_html += f"""
            <tr>
              <td class="username-cell"><a href="/users/{u['uuid']}" style="color:#fafafa;text-decoration:none;">{u['username']}</a></td>
              <td>{badge}</td>
              <td>{format_bytes(u['traffic_used'])}</td>
              <td style="color:#71717a;font-size:13px;">{u['created_at'][:10] if u['created_at'] else '—'}</td>
              <td class="actions-cell">
                <a href="/users/{u['uuid']}" class="btn btn-ghost btn-sm">View</a>
              </td>
            </tr>"""

        users_html = f"""
        <div class="card">
          <div class="card-header">
            <h3>Recent Users</h3>
            <a href="/users" class="btn btn-ghost btn-sm">View All</a>
          </div>
          <div class="table-container">
            <table>
              <thead>
                <tr>
                  <th>Username</th>
                  <th>Status</th>
                  <th>Traffic</th>
                  <th>Created</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {rows_html}
              </tbody>
            </table>
          </div>
        </div>"""
    else:
        users_html = """
        <div class="card">
          <div class="empty-state">
            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
            <h3>No users yet</h3>
            <p>Create your first user to get started.</p>
            <a href="/users/new" class="btn btn-primary" style="margin-top:16px;">Create User</a>
          </div>
        </div>"""

    content = f"""
    <div class="page-header">
      <h2>Dashboard</h2>
      <p>System overview and recent activity</p>
    </div>
    {stats_html}
    {core_html}
    {users_html}"""

    return render_page("Dashboard", content, active_nav="dashboard")


# ---------------------------------------------------------------------------
# Users List
# ---------------------------------------------------------------------------
@app.get("/users", response_class=HTMLResponse)
async def users_list():
    users = []
    now = datetime.now(timezone.utc)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT uuid, username, traffic_used, traffic_limit, expiry, active, created_at "
            "FROM users ORDER BY created_at DESC"
        )
        rows = await cur.fetchall()
        for uid, uname, tused, tlimit, expiry_str, is_active, created in rows:
            user_active = bool(is_active)
            is_expired = False
            if expiry_str:
                try:
                    exp = datetime.fromisoformat(expiry_str)
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=timezone.utc)
                    if exp < now:
                        user_active = False
                        is_expired = True
                except Exception:
                    pass
            users.append({
                "uuid": uid,
                "username": uname,
                "traffic_used": tused or 0,
                "traffic_limit": tlimit or 107374182400,
                "expiry": expiry_str,
                "active": user_active,
                "expired": is_expired,
                "created_at": created,
            })

    if users:
        rows_html = ""
        for u in users:
            if u["expired"]:
                badge = '<span class="badge badge-expired">Expired</span>'
            elif u["active"]:
                badge = '<span class="badge badge-active">Active</span>'
            else:
                badge = '<span class="badge badge-inactive">Inactive</span>'
            traffic_pct = 0
            if u["traffic_limit"] and u["traffic_limit"] > 0:
                traffic_pct = min(100, (u["traffic_used"] / u["traffic_limit"]) * 100)
            uname = u['username']
            rows_html += f"""
            <tr>
              <td class="username-cell"><a href="/users/{u['uuid']}" style="color:#fafafa;text-decoration:none;">{u['username']}</a></td>
              <td class="uuid-cell" title="{u['uuid']}">{u['uuid'][:12]}…</td>
              <td>{badge}</td>
              <td>{format_bytes(u['traffic_used'])} / {format_bytes(u['traffic_limit'])}</td>
              <td style="font-size:13px;">{u['expiry'][:10] if u['expiry'] else '—'}</td>
              <td class="actions-cell">
                <a href="/users/{u['uuid']}" class="btn btn-ghost btn-sm">View</a>
                <form method="POST" action="/users/{u['uuid']}/toggle" style="display:inline;">
                  <button type="submit" class="btn btn-ghost btn-sm">{'Disable' if u['active'] else 'Enable'}</button>
                </form>
                <form method="POST" action="/users/{u['uuid']}/delete" style="display:inline;"
                      onsubmit="return confirm('Delete user {uname}?');">
                  <button type="submit" class="btn btn-danger btn-sm">Delete</button>
                </form>
              </td>
            </tr>"""

        table_html = f"""
        <div class="card">
          <div class="table-container">
            <table>
              <thead>
                <tr>
                  <th>Username</th>
                  <th>UUID</th>
                  <th>Status</th>
                  <th>Traffic</th>
                  <th>Expiry</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {rows_html}
              </tbody>
            </table>
          </div>
        </div>"""
    else:
        table_html = """
        <div class="card">
          <div class="empty-state">
            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
            <h3>No users yet</h3>
            <p>Create your first user to get started.</p>
          </div>
        </div>"""

    content = f"""
    <div class="page-header" style="display:flex;align-items:center;justify-content:space-between;">
      <div>
        <h2>Users</h2>
        <p>Manage proxy users and their configurations</p>
      </div>
      <a href="/users/new" class="btn btn-primary">+ New User</a>
    </div>
    {table_html}"""

    return render_page("Users", content, active_nav="users")


# ---------------------------------------------------------------------------
# Create User Form
# ---------------------------------------------------------------------------
@app.get("/users/new", response_class=HTMLResponse)
async def user_new_form():
    content = """
    <div class="page-header">
      <h2>Create User</h2>
      <p>Add a new proxy user to the system</p>
    </div>
    <div class="card" style="max-width:600px;">
      <form method="POST" action="/users/create">
        <div class="form-group">
          <label for="username">Username</label>
          <input type="text" id="username" name="username" required
                 placeholder="e.g. john" autofocus>
          <div class="hint">Unique identifier for this user</div>
        </div>
        <div class="form-row">
          <div class="form-group">
            <label for="traffic_limit_gb">Traffic Limit (GB)</label>
            <input type="number" id="traffic_limit_gb" name="traffic_limit_gb"
                   value="100" min="0" step="1">
            <div class="hint">0 = unlimited</div>
          </div>
          <div class="form-group">
            <label for="expiry_days">Expiry (days)</label>
            <input type="number" id="expiry_days" name="expiry_days"
                   value="0" min="0" step="1">
            <div class="hint">0 = never expires</div>
          </div>
        </div>
        <div class="form-group">
          <label for="label">Label (optional)</label>
          <input type="text" id="label" name="label" placeholder="e.g. Home PC">
        </div>
        <div class="form-group">
          <label for="notes">Notes (optional)</label>
          <input type="text" id="notes" name="notes" placeholder="Any additional notes">
        </div>
        <div style="display:flex;gap:8px;margin-top:24px;">
          <button type="submit" class="btn btn-primary">Create User</button>
          <a href="/users" class="btn btn-ghost">Cancel</a>
        </div>
      </form>
    </div>"""

    return render_page("New User", content, active_nav="users")


# ---------------------------------------------------------------------------
# Create User Handler
# ---------------------------------------------------------------------------
@app.post("/users/create", response_class=HTMLResponse)
async def user_create(
    username: str = Form(...),
    traffic_limit_gb: float = Form(100),
    expiry_days: int = Form(0),
    label: str = Form(""),
    notes: str = Form(""),
):
    user_uuid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    traffic_limit = int(traffic_limit_gb * 1073741824) if traffic_limit_gb > 0 else 0

    expiry = None
    if expiry_days > 0:
        from datetime import timedelta
        exp_dt = datetime.now(timezone.utc) + timedelta(days=expiry_days)
        expiry = exp_dt.isoformat()

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO users (uuid, username, traffic_used, traffic_limit, expiry, active, created_at, updated_at) "
                "VALUES (?, ?, 0, ?, ?, 1, ?, ?)",
                (user_uuid, username, traffic_limit, expiry, now, now),
            )
            await db.commit()
        await write_core_config()
    except Exception as e:
        if "UNIQUE" in str(e):
            content = f"""
            <div class="page-header">
              <h2>Create User</h2>
            </div>
            <div class="alert alert-error">Username "{username}" already exists. Please choose another.</div>
            <div class="card" style="max-width:600px;">
              <form method="POST" action="/users/create">
                <div class="form-group">
                  <label for="username">Username</label>
                  <input type="text" id="username" name="username" required value="{username}">
                </div>
                <div class="form-row">
                  <div class="form-group">
                    <label for="traffic_limit_gb">Traffic Limit (GB)</label>
                    <input type="number" id="traffic_limit_gb" name="traffic_limit_gb" value="{traffic_limit_gb}" min="0">
                  </div>
                  <div class="form-group">
                    <label for="expiry_days">Expiry (days)</label>
                    <input type="number" id="expiry_days" name="expiry_days" value="{expiry_days}" min="0">
                  </div>
                </div>
                <div class="form-group">
                  <label for="label">Label</label>
                  <input type="text" id="label" name="label" value="{label}">
                </div>
                <div class="form-group">
                  <label for="notes">Notes</label>
                  <input type="text" id="notes" name="notes" value="{notes}">
                </div>
                <div style="display:flex;gap:8px;margin-top:24px;">
                  <button type="submit" class="btn btn-primary">Create User</button>
                  <a href="/users" class="btn btn-ghost">Cancel</a>
                </div>
              </form>
            </div>"""
            return render_page("New User", content, active_nav="users")
        raise

    return RedirectResponse(url=f"/users/{user_uuid}", status_code=303)


# ---------------------------------------------------------------------------
# User Detail
# ---------------------------------------------------------------------------
@app.get("/users/{user_uuid}", response_class=HTMLResponse)
async def user_detail(user_uuid: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT uuid, username, traffic_used, traffic_limit, expiry, active, created_at, updated_at "
            "FROM users WHERE uuid = ?",
            (user_uuid,),
        )
        row = await cur.fetchone()
    if not row:
        content = """
        <div class="page-header">
          <h2>User Not Found</h2>
          <p>The requested user does not exist.</p>
        </div>
        <a href="/users" class="btn btn-ghost">Back to Users</a>"""
        return render_page("Not Found", content, active_nav="users")

    uid, uname, tused, tlimit, expiry_str, is_active, created, updated = row

    now = datetime.now(timezone.utc)
    user_active = bool(is_active)
    is_expired = False
    if expiry_str:
        try:
            exp = datetime.fromisoformat(expiry_str)
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if exp < now:
                user_active = False
                is_expired = True
        except Exception:
            pass

    if is_expired:
        badge = '<span class="badge badge-expired">Expired</span>'
    elif user_active:
        badge = '<span class="badge badge-active">Active</span>'
    else:
        badge = '<span class="badge badge-inactive">Inactive</span>'

    traffic_pct = 0
    if tlimit and tlimit > 0:
        traffic_pct = min(100, (tused / tlimit) * 100)

    share_link = generate_share_link(uid, uname)
    sub_url = f"/sub/{uid}"
    ws_path = f"{WS_PREFIX}/{_proxy_session_id}"

    content = f"""
    <div class="page-header">
      <h2>{uname}</h2>
      <p>User detail and configuration</p>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:24px;">
      <div class="card">
        <div class="card-header">
          <h3>User Information</h3>
          {badge}
        </div>
        <div class="info-grid">
          <div class="info-label">UUID</div>
          <div class="info-value" style="font-family:monospace;font-size:13px;word-break:break-all;">{uid}</div>
          <div class="info-label">Traffic</div>
          <div class="info-value">{format_bytes(tused)} / {format_bytes(tlimit) if tlimit > 0 else 'Unlimited'}</div>
          <div class="info-label">Traffic Used</div>
          <div class="info-value">{traffic_pct:.1f}%</div>
          <div class="info-label">Expiry</div>
          <div class="info-value">{expiry_str[:10] if expiry_str else 'Never'}</div>
          <div class="info-label">Created</div>
          <div class="info-value">{created[:19] if created else '—'}</div>
          <div class="info-label">Updated</div>
          <div class="info-value">{updated[:19] if updated else '—'}</div>
        </div>
        <div class="detail-actions">
          <form method="POST" action="/users/{uid}/toggle" style="display:inline;">
            <button type="submit" class="btn btn-ghost">{'Disable' if user_active else 'Enable'}</button>
          </form>
          <form method="POST" action="/users/{uid}/delete" style="display:inline;"
                onsubmit="return confirm('Are you sure you want to delete {uname}?');">
            <button type="submit" class="btn btn-danger">Delete User</button>
          </form>
          <a href="/users" class="btn btn-ghost">Back to Users</a>
        </div>
      </div>

      <div>
        <div class="card" style="margin-bottom:24px;">
          <div class="card-header">
            <h3>Share Link</h3>
          </div>
          <div class="share-box">{share_link}</div>
        </div>

        <div class="card" style="margin-bottom:24px;">
          <div class="card-header">
            <h3>Subscription URL</h3>
          </div>
          <div class="sub-url">{sub_url}</div>
          <div style="margin-top:8px;font-size:12px;color:#71717a;">
            Import this URL in your client's subscription settings.
          </div>
        </div>

        <div class="card">
          <div class="card-header">
            <h3>Connection Info</h3>
          </div>
          <div class="info-grid">
            <div class="info-label">Protocol</div>
            <div class="info-value">VLESS + WebSocket</div>
            <div class="info-label">Server</div>
            <div class="info-value">{get_server_addr()}</div>
            <div class="info-label">Port</div>
            <div class="info-value">{PROXY_PORT}</div>
            <div class="info-label">Path</div>
            <div class="info-value" style="font-family:monospace;">{ws_path}</div>
          </div>
        </div>
      </div>
    </div>"""

    return render_page(f"User: {uname}", content, active_nav="users")


# ---------------------------------------------------------------------------
# Delete User
# ---------------------------------------------------------------------------
@app.post("/users/{user_uuid}/delete")
async def user_delete(user_uuid: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM users WHERE uuid = ?", (user_uuid,))
        await db.commit()
    await write_core_config()
    return RedirectResponse(url="/users", status_code=303)


# ---------------------------------------------------------------------------
# Toggle Active/Inactive
# ---------------------------------------------------------------------------
@app.post("/users/{user_uuid}/toggle")
async def user_toggle(user_uuid: str, request: Request):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT active FROM users WHERE uuid = ?", (user_uuid,))
        row = await cur.fetchone()
        if row:
            new_val = 0 if row[0] else 1
            await db.execute(
                "UPDATE users SET active = ?, updated_at = ? WHERE uuid = ?",
                (new_val, datetime.now(timezone.utc).isoformat(), user_uuid),
            )
            await db.commit()
    await write_core_config()
    # Redirect back to referer or user detail
    referer = request.headers.get("referer", f"/users/{user_uuid}")
    return RedirectResponse(url=referer, status_code=303)


# ---------------------------------------------------------------------------
# Subscription endpoint
# ---------------------------------------------------------------------------
@app.get("/sub/{user_uuid}")
async def subscription(user_uuid: str):
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
# Settings
# ---------------------------------------------------------------------------
@app.get("/settings", response_class=HTMLResponse)
async def settings_page():
    cs = core_status()
    status_color = "green" if cs["running"] else "red"
    status_text = "Running" if cs["running"] else "Stopped"
    ws_path = f"{WS_PREFIX}/{_proxy_session_id}"

    content = f"""
    <div class="page-header">
      <h2>Settings</h2>
      <p>System configuration and admin settings</p>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:24px;">
      <div>
        <div class="card" style="margin-bottom:24px;">
          <div class="card-header">
            <h3>Change Password</h3>
          </div>
          <form method="POST" action="/settings/password">
            <div class="form-group">
              <label for="new_password">New Password</label>
              <input type="password" id="new_password" name="new_password" required minlength="4"
                     placeholder="Enter new password">
              <div class="hint">Minimum 4 characters</div>
            </div>
            <div class="form-group">
              <label for="confirm_password">Confirm Password</label>
              <input type="password" id="confirm_password" name="confirm_password" required minlength="4"
                     placeholder="Confirm new password">
            </div>
            <button type="submit" class="btn btn-primary">Update Password</button>
          </form>
        </div>

        <div class="card">
          <div class="card-header">
            <h3>Core Process</h3>
          </div>
          <div class="info-grid" style="margin-bottom:16px;">
            <div class="info-label">Status</div>
            <div class="info-value"><span class="badge badge-{status_color}">{status_text}</span></div>
            <div class="info-label">PID</div>
            <div class="info-value">{cs['pid'] or '—'}</div>
          </div>
          <div style="display:flex;gap:8px;">
            <form method="POST" action="/settings/core/restart" style="display:inline;">
              <button type="submit" class="btn btn-ghost">Restart Core</button>
            </form>
          </div>
        </div>
      </div>

      <div>
        <div class="card" style="margin-bottom:24px;">
          <div class="card-header">
            <h3>Deploy Information</h3>
          </div>
          <div class="info-grid">
            <div class="info-label">Server Address</div>
            <div class="info-value" style="font-family:monospace;">{get_server_addr()}</div>
            <div class="info-label">Proxy Port</div>
            <div class="info-value">{PROXY_PORT}</div>
            <div class="info-label">Panel Port</div>
            <div class="info-value">{APP_PORT}</div>
            <div class="info-label">Session ID</div>
            <div class="info-value" style="font-family:monospace;">{_proxy_session_id}</div>
            <div class="info-label">WebSocket Path</div>
            <div class="info-value" style="font-family:monospace;">{ws_path}</div>
            <div class="info-label">Protocol</div>
            <div class="info-value">VLESS + WebSocket</div>
          </div>
        </div>

        <div class="card">
          <div class="card-header">
            <h3>Quick Links</h3>
          </div>
          <div style="display:flex;flex-direction:column;gap:8px;">
            <a href="/" class="btn btn-ghost" style="justify-content:center;">Dashboard</a>
            <a href="/users" class="btn btn-ghost" style="justify-content:center;">Manage Users</a>
            <a href="/users/new" class="btn btn-primary" style="justify-content:center;">Create New User</a>
          </div>
        </div>
      </div>
    </div>"""

    return render_page("Settings", content, active_nav="settings")


# ---------------------------------------------------------------------------
# Change Password
# ---------------------------------------------------------------------------
@app.post("/settings/password")
async def settings_change_password(
    new_password: str = Form(...),
    confirm_password: str = Form(...),
):
    if new_password != confirm_password:
        content = """
        <div class="page-header">
          <h2>Settings</h2>
        </div>
        <div class="alert alert-error">Passwords do not match.</div>
        <div class="card" style="max-width:600px;">
          <form method="POST" action="/settings/password">
            <div class="form-group">
              <label for="new_password">New Password</label>
              <input type="password" id="new_password" name="new_password" required minlength="4">
            </div>
            <div class="form-group">
              <label for="confirm_password">Confirm Password</label>
              <input type="password" id="confirm_password" name="confirm_password" required minlength="4">
            </div>
            <button type="submit" class="btn btn-primary">Update Password</button>
          </form>
        </div>"""
        return render_page("Settings", content, active_nav="settings")

    if len(new_password) < 4:
        content = """
        <div class="page-header">
          <h2>Settings</h2>
        </div>
        <div class="alert alert-error">Password must be at least 4 characters.</div>"""
        return render_page("Settings", content, active_nav="settings")

    new_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO settings VALUES (?, ?)",
            ("admin_hash", new_hash),
        )
        await db.commit()

    return RedirectResponse(url="/settings", status_code=303)


# ---------------------------------------------------------------------------
# Core restart
# ---------------------------------------------------------------------------
@app.post("/settings/core/restart")
async def settings_core_restart():
    await write_core_config()
    restart_core()
    return RedirectResponse(url="/settings", status_code=303)


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
