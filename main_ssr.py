#!/usr/bin/env python3
"""
Route Panel — Cloud Shell Edition
Server-side rendered panel + Xray VLESS proxy on single port (8080).
WebSocket /ws/* → Xray (port 8081)
Everything else → Panel HTML
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
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiosqlite
import bcrypt
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LOG = logging.getLogger("App")
APP_PORT = int(os.environ.get("PORT", 8080))
PROXY_PORT = 8081
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "panel.db")
CFG_PATH = "/tmp/xray_cfg.json"
CORE_BIN = os.path.expanduser("~/.local/bin/xray")
SESSION_ID_LEN = 12
WS_PREFIX = "/ws"

# Deploy fingerprint
DEPLOY_ID = hashlib.sha256(f"{time.time()}{os.urandom(8).hex()}".encode()).hexdigest()[:12]
_proxy_session_id = secrets.token_hex(SESSION_ID_LEN // 2)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                uuid TEXT PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                label TEXT DEFAULT '',
                active INTEGER DEFAULT 1,
                traffic_limit_gb REAL DEFAULT 0,
                traffic_used_gb REAL DEFAULT 0,
                expiry_days INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                expires_at TEXT,
                notes TEXT DEFAULT ''
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        await db.commit()

async def get_setting(key: str) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = await cur.fetchone()
        return row[0] if row else ""

async def set_setting(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        await db.commit()

# ---------------------------------------------------------------------------
# Xray process management
# ---------------------------------------------------------------------------
_xray_proc: Optional[subprocess.Popen] = None

def _build_xray_config():
    """Build Xray config JSON with all active users"""
    import asyncio
    loop = asyncio.get_event_loop()
    clients = []
    # We'll build this synchronously for now, async version in start_xray
    return {
        "log": {"loglevel": "warning"},
        "inbounds": [{
            "port": PROXY_PORT,
            "protocol": "vless",
            "settings": {
                "clients": [],  # filled dynamically
                "decryption": "none"
            },
            "streamSettings": {
                "network": "ws",
                "wsSettings": {"path": f"{WS_PREFIX}/{_proxy_session_id}"}
            }
        }],
        "outbounds": [{
            "protocol": "freedom",
            "tag": "direct"
        }]
    }

async def _get_active_clients():
    """Get active user UUIDs from DB for Xray config"""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT uuid FROM users WHERE active=1")
        rows = await cur.fetchall()
        return [r[0] for r in rows]

async def write_xray_config():
    """Write Xray config with current active users"""
    clients = await _get_active_clients()
    config = {
        "log": {"loglevel": "warning"},
        "inbounds": [{
            "port": PROXY_PORT,
            "protocol": "vless",
            "settings": {
                "clients": [{"id": u, "flow": ""} for u in clients],
                "decryption": "none"
            },
            "streamSettings": {
                "network": "ws",
                "wsSettings": {"path": f"{WS_PREFIX}/{_proxy_session_id}"}
            }
        }],
        "outbounds": [{
            "protocol": "freedom",
            "tag": "direct"
        }]
    }
    Path(CFG_PATH).write_text(json.dumps(config, indent=2))
    LOG.info("Config written: %s (%d clients)", CFG_PATH, len(clients))
    return config

async def start_xray():
    global _xray_proc
    if not Path(CORE_BIN).exists():
        LOG.warning("Xray binary not found at %s", CORE_BIN)
        return False
    await write_xray_config()
    try:
        if _xray_proc and _xray_proc.poll() is None:
            _xray_proc.terminate()
            _xray_proc.wait(timeout=5)
    except Exception:
        pass
    _xray_proc = subprocess.Popen(
        [CORE_BIN, "run", "-c", CFG_PATH],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    LOG.info("Xray started (pid=%d)", _xray_proc.pid)
    return True

async def restart_xray():
    return await start_xray()

def xray_status():
    if _xray_proc and _xray_proc.poll() is None:
        return {"running": True, "pid": _xray_proc.pid}
    return {"running": False, "pid": None}

# ---------------------------------------------------------------------------
# HTML templates
# ---------------------------------------------------------------------------
def _css():
    return """
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#09090b;--bg2:#111113;--bg3:#18181b;--bg4:#1f1f23;
  --fg:#fafafa;--fg2:#a1a1aa;--fg3:#71717a;
  --accent:#22c55e;--accent2:#16a34a;--accent-dim:rgba(34,197,94,.12);
  --red:#ef4444;--red-dim:rgba(239,68,68,.12);
  --yellow:#eab308;--yellow-dim:rgba(234,179,8,.12);
  --border:rgba(255,255,255,.08);
  --radius:10px;--radius-sm:6px;
  --sidebar-w:240px;
  --font:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
}
html,body{height:100%;font-family:var(--font);background:var(--bg);color:var(--fg);font-size:14px;line-height:1.5}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline}
button{cursor:pointer;font-family:var(--font);font-size:13px;border:none;border-radius:var(--radius-sm);padding:8px 16px;transition:all .15s}
input,select,textarea{font-family:var(--font);font-size:13px;background:var(--bg);border:1px solid var(--border);border-radius:var(--radius-sm);padding:8px 12px;color:var(--fg);outline:none;width:100%;transition:border-color .15s}
input:focus{border-color:var(--accent)}
.btn{display:inline-flex;align-items:center;gap:6px;text-decoration:none}
.btn-primary{background:var(--accent);color:#000;font-weight:600}
.btn-primary:hover{background:var(--accent2)}
.btn-ghost{background:transparent;color:var(--fg2);border:1px solid var(--border)}
.btn-ghost:hover{background:var(--bg3);color:var(--fg)}
.btn-danger{background:var(--red-dim);color:var(--red);border:1px solid rgba(239,68,68,.2)}
.btn-danger:hover{background:rgba(239,68,68,.2)}
.btn-sm{padding:5px 10px;font-size:12px}
.layout{display:flex;height:100vh}
.sidebar{width:var(--sidebar-w);background:var(--bg2);border-right:1px solid var(--border);display:flex;flex-direction:column;position:fixed;top:0;left:0;bottom:0;z-index:100}
.sidebar-header{padding:20px;border-bottom:1px solid var(--border)}
.sidebar-header h1{font-size:16px;font-weight:700;color:var(--accent);letter-spacing:-.5px}
.sidebar-header .subtitle{font-size:11px;color:var(--fg3);margin-top:2px}
.sidebar-nav{flex:1;padding:12px 8px;display:flex;flex-direction:column;gap:2px}
.nav-item{display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:var(--radius-sm);color:var(--fg2);font-size:13px;font-weight:500;cursor:pointer;transition:all .15s;text-decoration:none}
.nav-item:hover{background:var(--bg3);color:var(--fg);text-decoration:none}
.nav-item.active{background:var(--accent-dim);color:var(--accent)}
.nav-item svg{width:18px;height:18px;flex-shrink:0}
.main-content{margin-left:var(--sidebar-w);flex:1;overflow-y:auto;padding:32px;min-height:100vh}
.card{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:24px}
.card-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:20px}
.card-title{font-size:16px;font-weight:600}
.stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:24px}
.stat-card{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:20px}
.stat-card .label{font-size:12px;color:var(--fg3);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px}
.stat-card .value{font-size:28px;font-weight:700;letter-spacing:-1px}
.stat-card .value.green{color:var(--accent)}
.stat-card .value.blue{color:#3b82f6}
table{width:100%;border-collapse:collapse}
th{text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.5px;color:var(--fg3);padding:12px 16px;border-bottom:1px solid var(--border);font-weight:600}
td{padding:12px 16px;border-bottom:1px solid var(--border);font-size:13px}
tr:hover td{background:var(--bg3)}
.badge{display:inline-flex;align-items:center;gap:5px;font-size:11px;font-weight:600;padding:3px 8px;border-radius:20px}
.badge-active{background:var(--accent-dim);color:var(--accent)}
.badge-inactive{background:var(--red-dim);color:var(--red)}
.dot{width:6px;height:6px;border-radius:50%;display:inline-block}
.dot-green{background:var(--accent)}
.dot-red{background:var(--red)}
.form-group{margin-bottom:16px}
.form-group label{display:block;font-size:12px;color:var(--fg3);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px}
.form-actions{display:flex;gap:12px;margin-top:20px}
.toast{position:fixed;top:20px;right:20px;padding:12px 20px;border-radius:var(--radius-sm);font-size:13px;font-weight:500;z-index:999;opacity:0;transition:opacity .3s}
.toast.show{opacity:1}
.toast.success{background:var(--accent-dim);color:var(--accent);border:1px solid rgba(34,197,94,.3)}
.toast.error{background:var(--red-dim);color:var(--red);border:1px solid rgba(239,68,68,.3)}
.link-box{background:var(--bg);border:1px solid var(--border);border-radius:var(--radius-sm);padding:12px;font-family:monospace;font-size:12px;word-break:break-all;color:var(--accent);margin:12px 0}
.empty{text-align:center;padding:48px;color:var(--fg3)}
.empty svg{margin-bottom:12px;opacity:.5}
@media(max-width:768px){.sidebar{display:none}.main-content{margin-left:0;padding:16px}}
</style>"""

def _sidebar(active=""):
    items = [
        ("dashboard", "/", '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/></svg>', "Dashboard"),
        ("users", "/users", '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>', "Users"),
        ("settings", "/settings", '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>', "Settings"),
    ]
    nav_html = ""
    for key, href, icon, label in items:
        cls = "nav-item active" if key == active else "nav-item"
        nav_html += f'<a href="{href}" class="{cls}">{icon} {label}</a>'
    return f"""
    <nav class="sidebar">
      <div class="sidebar-header">
        <h1>Route Panel</h1>
        <span class="subtitle">Cloud Shell Edition</span>
      </div>
      <div class="sidebar-nav">{nav_html}</div>
    </nav>"""

def render_page(title, content, active_nav=""):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} - Route Panel</title>
{_css()}
</head>
<body>
<div class="layout">
  {_sidebar(active_nav)}
  <main class="main-content">{content}</main>
</div>
</body>
</html>"""

def format_bytes(b):
    if not b: return "0 B"
    for unit in ["B", "KB", "MB", "GB"]:
        if b < 1024: return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app_instance):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    await init_db()
    LOG.info("Panel started on port %d", APP_PORT)
    # Start Xray
    ok = await start_xray()
    if ok:
        LOG.info("Xray started successfully")
    else:
        LOG.warning("Xray not available — panel-only mode")
    # Keepalive
    asyncio.create_task(_keepalive())
    yield
    if _xray_proc and _xray_proc.poll() is None:
        _xray_proc.terminate()

async def _keepalive():
    while True:
        await asyncio.sleep(300)
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                await client.get(f"http://localhost:{APP_PORT}/", timeout=10)
        except Exception:
            pass

app = FastAPI(title="Dashboard", lifespan=lifespan)

# ---------------------------------------------------------------------------
# WebSocket proxy: /ws/* → Xray on port 8081
# ---------------------------------------------------------------------------
@app.websocket(f"{WS_PREFIX}/{{path_param:path}}")
async def ws_proxy(websocket: WebSocket, path_param: str):
    """Proxy WebSocket connections to Xray for VLESS relay"""
    await websocket.accept()
    full_path = f"{WS_PREFIX}/{path_param}"
    LOG.info("WS proxy: %s", full_path)
    try:
        import websockets
        async with websockets.connect(f"ws://127.0.0.1:{PROXY_PORT}{full_path}") as upstream:
            async def ws_to_upstream():
                while True:
                    data = await websocket.receive_bytes()
                    await upstream.send(data)
            async def upstream_to_ws():
                async for msg in upstream:
                    if isinstance(msg, bytes):
                        await websocket.send_bytes(msg)
                    else:
                        await websocket.send_text(msg)
            done, pending = await asyncio.wait(
                [asyncio.create_task(ws_to_upstream()), asyncio.create_task(upstream_to_ws())],
                return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
    except Exception as e:
        LOG.error("WS proxy error: %s", e)
        try:
            await websocket.close()
        except Exception:
            pass

# WebSocket proxy: /ws/* → Xray on port 8081



# ---------------------------------------------------------------------------
# Routes: Pages
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def page_dashboard():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT * FROM users ORDER BY created_at DESC")
        cols = [d[0] for d in cur.description]
        users = [dict(zip(cols, r)) for r in await cur.fetchall()]
    total = len(users)
    active = sum(1 for u in users if u["active"])
    total_traffic = sum(u.get("traffic_used_gb", 0) or 0 for u in users)
    xs = xray_status()

    stats = f"""
    <h2 style="font-size:20px;font-weight:700;margin-bottom:24px;">Dashboard</h2>
    <div class="stat-grid">
      <div class="stat-card"><div class="label">Total Users</div><div class="value green">{total}</div></div>
      <div class="stat-card"><div class="label">Active</div><div class="value green">{active}</div></div>
      <div class="stat-card"><div class="label">Traffic</div><div class="value blue">{total_traffic:.1f} GB</div></div>
      <div class="stat-card">
        <div class="label">Proxy Status</div>
        <div class="value {'green' if xs['running'] else ''}" style="color:{'var(--accent)' if xs['running'] else 'var(--red)'}">
          {'Running' if xs['running'] else 'Stopped'}
        </div>
        <form method="POST" action="/proxy/restart" style="margin-top:8px;">
          <button type="submit" class="btn btn-ghost btn-sm">Restart Proxy</button>
        </form>
      </div>
    </div>"""

    recent = ""
    if users:
        rows = ""
        for u in users[:5]:
            badge = '<span class="badge badge-active"><span class="dot dot-green"></span>Active</span>' if u["active"] else '<span class="badge badge-inactive"><span class="dot dot-red"></span>Inactive</span>'
            rows += f"""<tr>
              <td><a href="/users/{u['uuid']}" style="color:var(--fg)">{u['username']}</a></td>
              <td>{badge}</td>
              <td>{format_bytes(u.get('traffic_used_gb',0)*1024*1024*1024)}</td>
            </tr>"""
        recent = f"""
        <div class="card" style="margin-top:24px;">
          <div class="card-header"><span class="card-title">Recent Users</span><a href="/users" class="btn btn-ghost btn-sm">View All</a></div>
          <table><thead><tr><th>Username</th><th>Status</th><th>Traffic</th></tr></thead><tbody>{rows}</tbody></table>
        </div>"""

    return render_page("Dashboard", stats + recent, "dashboard")

@app.get("/users", response_class=HTMLResponse)
async def page_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT * FROM users ORDER BY created_at DESC")
        cols = [d[0] for d in cur.description]
        users = [dict(zip(cols, r)) for r in await cur.fetchall()]

    if not users:
        content = f"""
        <div class="card-header" style="margin-bottom:24px;">
          <h2 style="font-size:20px;font-weight:700;">Users</h2>
          <a href="/users/new" class="btn btn-primary">+ Add User</a>
        </div>
        <div class="card empty">
          <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
          <p>No users yet. Create your first user to get started.</p>
        </div>"""
        return render_page("Users", content, "users")

    rows = ""
    for u in users:
        badge = '<span class="badge badge-active"><span class="dot dot-green"></span>Active</span>' if u["active"] else '<span class="badge badge-inactive"><span class="dot dot-red"></span>Inactive</span>'
        uname = u['username']
        rows += f"""<tr>
          <td><a href="/users/{u['uuid']}" style="color:var(--fg)">{u['username']}</a></td>
          <td style="color:var(--fg3)">{u['uuid'][:12]}…</td>
          <td>{badge}</td>
          <td>{format_bytes((u.get('traffic_used_gb') or 0)*1024*1024*1024)}</td>
          <td>
            <a href="/users/{u['uuid']}" class="btn btn-ghost btn-sm">View</a>
            <form method="POST" action="/users/{u['uuid']}/toggle" style="display:inline">
              <button type="submit" class="btn btn-ghost btn-sm">{'Disable' if u['active'] else 'Enable'}</button>
            </form>
            <form method="POST" action="/users/{u['uuid']}/delete" style="display:inline" onsubmit="return confirm('Delete {uname}?')">
              <button type="submit" class="btn btn-danger btn-sm">Delete</button>
            </form>
          </td>
        </tr>"""

    content = f"""
    <div class="card-header" style="margin-bottom:24px;">
      <h2 style="font-size:20px;font-weight:700;">Users ({len(users)})</h2>
      <a href="/users/new" class="btn btn-primary">+ Add User</a>
    </div>
    <div class="card">
      <table>
        <thead><tr><th>Username</th><th>UUID</th><th>Status</th><th>Traffic</th><th>Actions</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>"""
    return render_page("Users", content, "users")

@app.get("/users/new", response_class=HTMLResponse)
async def page_new_user():
    content = f"""
    <h2 style="font-size:20px;font-weight:700;margin-bottom:24px;">New User</h2>
    <div class="card" style="max-width:500px;">
      <form method="POST" action="/users/create">
        <div class="form-group">
          <label>Username</label>
          <input type="text" name="username" required placeholder="e.g. user1">
        </div>
        <div class="form-group">
          <label>Label (optional)</label>
          <input type="text" name="label" placeholder="e.g. Mobile">
        </div>
        <div class="form-group">
          <label>Traffic Limit (GB, 0 = unlimited)</label>
          <input type="number" name="traffic_limit_gb" value="0" step="0.1">
        </div>
        <div class="form-group">
          <label>Expiry Days (0 = never)</label>
          <input type="number" name="expiry_days" value="0">
        </div>
        <div class="form-actions">
          <button type="submit" class="btn btn-primary">Create User</button>
          <a href="/users" class="btn btn-ghost">Cancel</a>
        </div>
      </form>
    </div>"""
    return render_page("New User", content, "users")

@app.get("/users/{user_uuid}", response_class=HTMLResponse)
async def page_user_detail(user_uuid: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT * FROM users WHERE uuid=?", (user_uuid,))
        cols = [d[0] for d in cur.description]
        row = await cur.fetchone()
    if not row:
        return render_page("Not Found", '<div class="card empty"><p>User not found</p><a href="/users">← Back to Users</a></div>', "users")
    u = dict(zip(cols, row))
    badge = '<span class="badge badge-active"><span class="dot dot-green"></span>Active</span>' if u["active"] else '<span class="badge badge-inactive"><span class="dot dot-red"></span>Inactive</span>'

    # Generate share link
    host = os.environ.get("SHELL_HOST", "localhost")
    port = "443"
    ws_path = f"{WS_PREFIX}/{_proxy_session_id}"
    sni = host
    share_link = f"vless://{u['uuid']}@{host}:{port}?encryption=none&type=ws&host={host}&path={ws_path}&security=tls&sni={sni}&fp=chrome&alpn=h2,http/1.1#{u['username']}"

    content = f"""
    <div style="display:flex;align-items:center;gap:16px;margin-bottom:24px;">
      <a href="/users" class="btn btn-ghost btn-sm">← Back</a>
      <h2 style="font-size:20px;font-weight:700;">{u['username']}</h2>
      {badge}
    </div>
    <div class="stat-grid">
      <div class="stat-card"><div class="label">UUID</div><div style="font-family:monospace;font-size:12px;word-break:break-all;color:var(--fg2)">{u['uuid']}</div></div>
      <div class="stat-card"><div class="label">Traffic</div><div class="value blue">{format_bytes((u.get('traffic_used_gb') or 0)*1024*1024*1024)}</div></div>
      <div class="stat-card"><div class="label">Expires</div><div style="font-size:14px;color:var(--fg2)">{u['expires_at'][:10] if u.get('expires_at') else 'Never'}</div></div>
    </div>
    <div class="card" style="margin-top:24px;">
      <div class="card-header"><span class="card-title">Share Link</span></div>
      <p style="font-size:12px;color:var(--fg3);margin-bottom:8px;">Copy this link and import it in your V2Ray/Xray client:</p>
      <div class="link-box" id="shareLink">{share_link}</div>
      <div style="display:flex;gap:8px;margin-top:12px;">
        <button onclick="navigator.clipboard.writeText(document.getElementById('shareLink').textContent).then(()=>this.textContent='Copied!')" class="btn btn-primary btn-sm">Copy Link</button>
        <a href="https://api.qrserver.com/v1/create-qr-code/?size=200x200&data={share_link}" target="_blank" class="btn btn-ghost btn-sm">QR Code</a>
      </div>
    </div>
    <div class="card" style="margin-top:24px;">
      <div class="card-header"><span class="card-title">Subscription</span></div>
      <p style="font-size:12px;color:var(--fg3);margin-bottom:8px;">Add this URL as subscription in your client:</p>
      <div class="link-box">{host}/sub/{u['uuid']}</div>
    </div>"""
    return render_page(f"User: {u['username']}", content, "users")

@app.get("/settings", response_class=HTMLResponse)
async def page_settings():
    xs = xray_status()
    status_color = "var(--accent)" if xs["running"] else "var(--red)"
    status_text = "Running" if xs["running"] else "Stopped"
    content = f"""
    <h2 style="font-size:20px;font-weight:700;margin-bottom:24px;">Settings</h2>
    <div class="card" style="max-width:500px;margin-bottom:24px;">
      <div class="card-header"><span class="card-title">Proxy Status</span></div>
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;">
        <span style="color:{status_color};font-weight:600;">● {status_text}</span>
        <span style="color:var(--fg3);font-size:12px;">PID: {xs.get('pid', '—')}</span>
      </div>
      <div style="display:flex;gap:8px;">
        <form method="POST" action="/proxy/restart"><button type="submit" class="btn btn-primary btn-sm">Restart</button></form>
      </div>
    </div>
    <div class="card" style="max-width:500px;margin-bottom:24px;">
      <div class="card-header"><span class="card-title">Deploy Info</span></div>
      <p style="font-size:12px;color:var(--fg3);margin-bottom:8px;">Session ID: <code style="color:var(--accent)">{_proxy_session_id}</code></p>
      <p style="font-size:12px;color:var(--fg3);">Deploy ID: <code style="color:var(--accent)">{DEPLOY_ID}</code></p>
    </div>"""
    return render_page("Settings", content, "settings")

# ---------------------------------------------------------------------------
# Routes: Actions (POST)
# ---------------------------------------------------------------------------
@app.post("/users/create")
async def action_create_user(request: Request):
    form = await request.form()
    username = form.get("username", "").strip()
    if not username:
        raise HTTPException(400, "Username required")
    label = form.get("label", "").strip()
    traffic_limit = float(form.get("traffic_limit_gb", 0) or 0)
    expiry_days = int(form.get("expiry_days", 0) or 0)
    user_uuid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=expiry_days)).isoformat() if expiry_days > 0 else None
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO users (uuid, username, label, traffic_limit_gb, expiry_days, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (user_uuid, username, label, traffic_limit, expiry_days, now, expires_at)
            )
            await db.commit()
        except Exception as e:
            return RedirectResponse("/users/new?error=exists", status_code=302)
    # Update Xray config
    await write_xray_config()
    return RedirectResponse("/users", status_code=302)

@app.post("/users/{user_uuid}/delete")
async def action_delete_user(user_uuid: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM users WHERE uuid=?", (user_uuid,))
        await db.commit()
    await write_xray_config()
    return RedirectResponse("/users", status_code=302)

@app.post("/users/{user_uuid}/toggle")
async def action_toggle_user(user_uuid: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT active FROM users WHERE uuid=?", (user_uuid,))
        row = await cur.fetchone()
        if row:
            new_val = 0 if row[0] else 1
            await db.execute("UPDATE users SET active=? WHERE uuid=?", (new_val, user_uuid))
            await db.commit()
    await write_xray_config()
    return RedirectResponse(f"/users/{user_uuid}", status_code=302)

@app.post("/proxy/restart")
async def action_restart_proxy():
    await restart_xray()
    return RedirectResponse("/", status_code=302)

# ---------------------------------------------------------------------------
# Routes: API (subscription)
# ---------------------------------------------------------------------------
@app.get("/sub/{user_uuid}")
async def api_subscription(user_uuid: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT username FROM users WHERE uuid=?", (user_uuid,))
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "User not found")
    host = os.environ.get("SHELL_HOST", "localhost")
    port = "443"
    ws_path = f"{WS_PREFIX}/{_proxy_session_id}"
    sni = host
    link = f"vless://{user_uuid}@{host}:{port}?encryption=none&type=ws&host={host}&path={ws_path}&security=tls&sni={sni}&fp=chrome&alpn=h2,http/1.1#{row[0]}"
    return Response(content=base64.b64encode(link.encode()).decode(), media_type="text/plain")

@app.get("/api/ping")
async def api_ping():
    return {"ok": True, "session": _proxy_session_id}

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)
