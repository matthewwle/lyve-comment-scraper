import asyncio
import json
import os
from collections import deque
from datetime import datetime, timezone

from dotenv import load_dotenv
from fastapi import FastAPI, Request, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sse_starlette.sse import EventSourceResponse
from supabase import create_client, Client

from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent, DisconnectEvent

load_dotenv()

# ── Supabase ───────────────────────────────────────────────────────────────────
SUPABASE_URL      = os.environ["SUPABASE_URL"]
SUPABASE_ANON_KEY = os.environ["SUPABASE_ANON_KEY"]
ALLOWED_DOMAIN    = "lyvestudio.com"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/assets", StaticFiles(directory="assets"), name="assets")

# ── Per-user sessions ──────────────────────────────────────────────────────────
# Keyed by user email. Each user gets their own independent scraper session.
sessions: dict[str, dict] = {}


def get_session(user: str) -> dict:
    """Return existing session for user, or create a fresh one."""
    if user not in sessions:
        sessions[user] = {
            "client":    None,
            "task":      None,
            "comments":  deque(maxlen=200),
            "limit":     200,
            "status":    "idle",
            "error_msg": "",
            "clients":   set(),   # asyncio.Queue per SSE browser tab
        }
    return sessions[user]


# ── Auth ───────────────────────────────────────────────────────────────────────
security = HTTPBearer(auto_error=False)


def require_auth(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    token: str = Query(default=None),
) -> str:
    """Accept token from Bearer header OR ?token= query param (needed for EventSource)."""
    raw = credentials.credentials if credentials else token
    if not raw:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        user = supabase.auth.get_user(raw)
        return user.user.email
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


# ── Public routes ──────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    with open("static/index.html") as f:
        return f.read()


@app.post("/login")
async def login(payload: dict) -> JSONResponse:
    email    = payload.get("email", "").strip().lower()
    password = payload.get("password", "")

    if not email.endswith(f"@{ALLOWED_DOMAIN}"):
        raise HTTPException(status_code=403, detail=f"Email must be @{ALLOWED_DOMAIN}")

    try:
        res = supabase.auth.sign_in_with_password({"email": email, "password": password})
        return JSONResponse({
            "token":         res.session.access_token,
            "refresh_token": res.session.refresh_token,
            "email":         res.user.email,
        })
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid email or password")


@app.post("/refresh")
async def refresh_token(payload: dict) -> JSONResponse:
    refresh_token = payload.get("refresh_token", "")
    if not refresh_token:
        raise HTTPException(status_code=400, detail="refresh_token required")
    try:
        res = supabase.auth.refresh_session(refresh_token)
        return JSONResponse({
            "token":         res.session.access_token,
            "refresh_token": res.session.refresh_token,
        })
    except Exception:
        raise HTTPException(status_code=401, detail="Could not refresh session")


@app.post("/logout")
async def logout() -> JSONResponse:
    return JSONResponse({"ok": True})


# ── Protected routes (all scoped to the authenticated user) ────────────────────

@app.get("/status")
async def get_status(user: str = Depends(require_auth)) -> JSONResponse:
    session = get_session(user)
    return JSONResponse({
        "status":        session["status"],
        "error":         session["error_msg"],
        "comment_count": len(session["comments"]),
    })


@app.post("/start")
async def start_scraper(payload: dict, user: str = Depends(require_auth)) -> JSONResponse:
    url   = payload.get("url", "").strip()
    limit = max(10, min(2000, int(payload.get("limit", 200))))

    session = get_session(user)
    await _stop_current(session)

    try:
        unique_id = extract_unique_id(url)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    session["limit"]    = limit
    session["comments"] = deque(maxlen=limit)
    session["status"]   = "connecting"

    client = TikTokLiveClient(unique_id=unique_id, sign_api_key=os.environ.get("SIGN_API_KEY"))
    session["client"] = client

    @client.on(ConnectEvent)
    async def on_connect(event: ConnectEvent) -> None:
        session["status"] = "live"
        await _broadcast(session, {"type": "status", "status": "live", "user": unique_id})

    @client.on(CommentEvent)
    async def on_comment(event: CommentEvent) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        entry = {
            "type": "comment",
            "ts":   ts,
            "user": event.user.nickname,
            "text": event.comment,
        }
        session["comments"].append(entry)
        await _broadcast(session, entry)

    @client.on(DisconnectEvent)
    async def on_disconnect(event: DisconnectEvent) -> None:
        session["status"] = "idle"
        await _broadcast(session, {"type": "status", "status": "disconnected"})

    session["task"] = asyncio.create_task(_run_client(session, client))
    return JSONResponse({"ok": True, "user": unique_id})


@app.post("/stop")
async def stop_scraper(user: str = Depends(require_auth)) -> JSONResponse:
    session = get_session(user)
    await _stop_current(session)
    await _broadcast(session, {"type": "status", "status": "idle"})
    return JSONResponse({"ok": True})


@app.get("/stream")
async def stream(request: Request, user: str = Depends(require_auth)) -> EventSourceResponse:
    session = get_session(user)
    queue: asyncio.Queue = asyncio.Queue(maxsize=500)
    session["clients"].add(queue)

    # Replay current rolling window immediately to this new tab
    for comment in list(session["comments"]):
        await queue.put(comment)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield {"data": json.dumps(data)}
                except asyncio.TimeoutError:
                    yield {"data": json.dumps({"type": "heartbeat"})}
        finally:
            session["clients"].discard(queue)

    return EventSourceResponse(event_generator())


# ── Internal helpers ───────────────────────────────────────────────────────────

def extract_unique_id(url: str) -> str:
    from urllib.parse import urlparse
    path_parts = urlparse(url.strip()).path.strip("/").split("/")
    if not path_parts or not path_parts[0].startswith("@"):
        raise ValueError(f"Cannot parse @username from URL: {url}")
    return path_parts[0]


async def _broadcast(session: dict, data: dict) -> None:
    dead = set()
    for q in session["clients"]:
        try:
            q.put_nowait(data)
        except asyncio.QueueFull:
            dead.add(q)
    session["clients"] -= dead


async def _run_client(session: dict, client: TikTokLiveClient) -> None:
    try:
        await client.start(fetch_live_check=True)
    except Exception as exc:
        session["status"]    = "error"
        session["error_msg"] = str(exc)
        await _broadcast(session, {"type": "status", "status": "error", "msg": str(exc)})


async def _stop_current(session: dict) -> None:
    client = session.get("client")
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass
    task = session.get("task")
    if task and not task.done():
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
    session["client"]    = None
    session["task"]      = None
    session["status"]    = "idle"
    session["error_msg"] = ""
