# Route Panel - Google Cloud Shell Edition

A lightweight proxy management panel designed to run on Google Cloud Shell (free tier). Provides a web UI to manage VLESS+WebSocket users and generates share links for easy client configuration.

## Features

- **Web-based Management Panel** - Dark theme, responsive SPA
- **User Management** - Create, edit, delete users with traffic limits and expiry dates
- **Share Link Generation** - Valid VLESS:// links with QR codes
- **Subscription Endpoint** - Base64-encoded subscription for auto-import
- **Auto Config** - Automatically generates and applies routing configuration
- **Keepalive** - Prevents Cloud Shell idle disconnect (20-min timeout)
- **Password Protection** - Bcrypt-hashed admin authentication
- **No Dependencies** - Pure HTML/CSS/JS frontend, no CDN

## Quick Start on Cloud Shell

### Step 1: Open Cloud Shell
Go to [Google Cloud Shell](https://shell.cloud.google.com)

### Step 2: Clone or Create the Project
```bash
# Option A: If you have a git repo
git clone <your-repo-url> gcloud-panel
cd gcloud-panel

# Option B: Create manually
mkdir gcloud-panel && cd gcloud-panel
# Copy the project files here
```

### Step 3: Start the Panel
```bash
bash start.sh
```

This will:
1. Install Python dependencies (fastapi, uvicorn, etc.)
2. Download the routing core binary if needed
3. Start the panel on port 8080

### Step 4: Open Web Preview
In Cloud Shell, click **"Web Preview"** (icon in top-right) → **"Preview on port 8080"**

### Step 5: Login
- **Username:** `admin`
- **Password:** `admin`

> **Important:** Change the default password immediately in Settings!

### Step 6: Create Users
1. Go to **Users** tab
2. Click **"New User"**
3. Enter a username and set traffic/expiry limits
4. Copy the generated share link
5. Import the link in your client app

## Client Apps

Import the share link in any of these apps:

| Platform | App |
|----------|-----|
| Android | V2RayNG, Nekobox, Drony |
| iOS | Shadowrocket, Stash, Quantumult |
| Windows | Nekoray, V2RayN |
| macOS | V2RayU, Nekoray |

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/login` | Authenticate |
| GET | `/api/stats` | Dashboard statistics |
| GET | `/api/users` | List all users |
| POST | `/api/users` | Create user |
| PUT | `/api/users/{uuid}` | Update user |
| DELETE | `/api/users/{uuid}` | Delete user |
| GET | `/api/link/{uuid}` | Get share link |
| GET | `/sub/{uuid}` | Subscription (base64) |
| GET | `/api/core/status` | Service status |
| POST | `/api/core/restart` | Restart service |

## File Structure

```
gcloud-panel/
├── main.py              # FastAPI backend (single file)
├── frontend/
│   └── index.html       # Web UI (SPA, dark theme)
├── start.sh             # One-click startup script
├── requirements.txt     # Python dependencies
└── README.md            # This file
```

## Notes

- Cloud Shell provides 50 hours/week of free usage
- The panel uses port 8080 for web UI and port 8081 for the routing service
- A keepalive task runs every 5 minutes to prevent idle disconnect
- All user data is stored in `panel.db` (SQLite)
- The routing config is generated automatically from active users

## Troubleshooting

**Panel won't start:**
- Make sure you're in the correct directory
- Try: `pip install -r requirements.txt` then `python main.py`

**Can't access Web Preview:**
- Ensure port 8080 is selected in Web Preview settings
- Check that the panel is running (look for "Started server process" in terminal)

**Users can't connect:**
- Check if the routing core is running (Dashboard → System Status)
- Verify the share link matches your server address
- Some Cloud Shell instances may block non-standard ports

## License

MIT
