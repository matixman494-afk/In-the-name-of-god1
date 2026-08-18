# main.py
# ══════════════════════════════════════════════════════════════════════════════
# Matix — نسخه‌ی پایه و ساده‌ی یک پنل VLESS-over-WebSocket با پشتیبانی ربات
# تلگرام. طراحی شده برای اینکه راحت قابل فهم و توسعه باشه — بدون فروشگاه،
# کیف پول، رفرال یا هر منطق تجاری اضافه؛ فقط هسته‌ی کار: ساخت/مدیریت لینک +
# تونل واقعی + یک پنل وب ساده + یک ربات تلگرام ساده برای مدیریت از راه دور.
# ══════════════════════════════════════════════════════════════════════════════

import asyncio
import hashlib
import json
import logging
import os
import secrets
import time
import uuid as uuid_lib
from datetime import datetime, timedelta
from pathlib import Path

import aiofiles
from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
import uvicorn

from vless_relay import run_tunnel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Matix")

app = FastAPI(title="Matix", docs_url=None, redoc_url=None)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                    allow_methods=["*"], allow_headers=["*"])

# ── تنظیمات پایه ──────────────────────────────────────────────────────────────
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
STATE_FILE = DATA_DIR / "state.json"
SECRET_FILE = DATA_DIR / "secret.key"
SAVE_LOCK = asyncio.Lock()

DEFAULT_PORT = 443
DEFAULT_FINGERPRINT = "chrome"
DEFAULT_ALPN = "http/1.1"

SESSION_COOKIE = "basevpn_session"
SESSION_TTL = 60 * 60 * 24 * 365  # یک سال


def _load_or_create_secret() -> str:
    env_secret = os.environ.get("SECRET_KEY")
    if env_secret:
        return env_secret
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        if SECRET_FILE.exists():
            existing = SECRET_FILE.read_text(encoding="utf-8").strip()
            if existing:
                return existing
        new_secret = secrets.token_urlsafe(32)
        SECRET_FILE.write_text(new_secret, encoding="utf-8")
        return new_secret
    except Exception as e:
        logger.warning(f"secret دائمی ذخیره نشد: {e}")
        return secrets.token_urlsafe(32)


SECRET = _load_or_create_secret()


def hash_password(pw: str) -> str:
    return hashlib.sha256(f"{pw}{SECRET}".encode()).hexdigest()


# ── State (در حافظه + ذخیره روی دیسک به‌صورت JSON) ─────────────────────────
LINKS: dict = {}            # uuid(str) -> {label, active, limit_bytes, used_bytes, expires_at, created_at}
AUTH = {"password_hash": hash_password(os.environ.get("ADMIN_PASSWORD", "admin123"))}
SESSIONS: dict = {}
TELEGRAM_SETTINGS = {
    "bot_token": os.environ.get("TELEGRAM_BOT_TOKEN", "").strip(),
    "admin_ids": [int(x) for x in os.environ.get("TELEGRAM_ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()],
    "required_channels": [],   # [{"id": "@channel یا -100123..", "title": "...", "url": "https://t.me/..."}]
}


async def load_state():
    global LINKS
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        if STATE_FILE.exists():
            async with aiofiles.open(STATE_FILE, "r", encoding="utf-8") as f:
                raw = await f.read()
            data = json.loads(raw)
            LINKS.update(data.get("links", {}))
            if "password_hash" in data and not os.environ.get("ADMIN_PASSWORD"):
                AUTH["password_hash"] = data["password_hash"]
            if "telegram" in data:
                TELEGRAM_SETTINGS.update(data["telegram"])
            logger.info(f"state لود شد: {len(LINKS)} لینک")
    except Exception as e:
        logger.warning(f"state لود نشد: {e}")


async def save_state():
    async with SAVE_LOCK:
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            data = {
                "links": LINKS,
                "password_hash": AUTH["password_hash"],
                "telegram": TELEGRAM_SETTINGS,
                "saved_at": datetime.now().isoformat(),
            }
            tmp = STATE_FILE.with_suffix(".tmp")
            async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))
            tmp.replace(STATE_FILE)
        except Exception as e:
            logger.warning(f"state ذخیره نشد: {e}")


@app.on_event("startup")
async def startup():
    await load_state()
    from telegram_bot import start_bot
    await start_bot()


@app.on_event("shutdown")
async def shutdown():
    await save_state()
    from telegram_bot import stop_bot
    await stop_bot()


# ── کمک‌کننده‌ها ───────────────────────────────────────────────────────────────
def get_host(request: Request | None = None) -> str:
    """دامنه‌ی عمومی رو ترجیحاً از خود درخواست می‌گیره؛ برای جاهایی که درخواست
    نداریم (مثل ربات تلگرام) از env می‌خونیم، نه از یک مقدار کش‌شده‌ی قدیمی."""
    if request is not None:
        h = request.headers.get("x-forwarded-host") or request.headers.get("host")
        if h:
            return h.split(":")[0]
    manual = os.environ.get("PUBLIC_DOMAIN", "").strip()
    if manual:
        return manual.replace("https://", "").replace("http://", "").split("/")[0]
    return os.environ.get("RAILWAY_PUBLIC_DOMAIN", "localhost")


def fmt_bytes(n: int) -> str:
    n = float(n)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def parse_size_to_bytes(value: float, unit: str) -> int:
    mult = {"MB": 1024**2, "GB": 1024**3, "KB": 1024}.get(unit.upper(), 1024**3)
    return int(value * mult)


def is_link_allowed(link: dict) -> bool:
    if not link.get("active", True):
        return False
    exp = link.get("expires_at")
    if exp and datetime.fromisoformat(exp) < datetime.now():
        return False
    limit = link.get("limit_bytes", 0)
    if limit and link.get("used_bytes", 0) >= limit:
        return False
    return True


def vless_link_for(uid: str, host: str, label: str = "Matix") -> str:
    from urllib.parse import quote
    params = (
        f"encryption=none&security=tls&sni={host}&fp={DEFAULT_FINGERPRINT}"
        f"&alpn={quote(DEFAULT_ALPN)}&type=ws&host={host}&path=%2Fws%2F{uid}"
    )
    return f"vless://{uid}@{host}:{DEFAULT_PORT}?{params}#{quote(label)}"


async def make_link(label: str, limit_bytes: int = 0, expires_days: int = 0) -> tuple[str, dict]:
    uid = str(uuid_lib.uuid4())
    expires_at = (datetime.now() + timedelta(days=expires_days)).isoformat() if expires_days > 0 else None
    link = {
        "label": label[:60] or "کانفیگ جدید",
        "active": True,
        "limit_bytes": limit_bytes,
        "used_bytes": 0,
        "expires_at": expires_at,
        "created_at": datetime.now().isoformat(),
    }
    LINKS[uid] = link
    asyncio.create_task(save_state())
    return uid, link


async def remove_link(uid: str) -> str | None:
    link = LINKS.pop(uid, None)
    if link is None:
        return None
    asyncio.create_task(save_state())
    return link["label"]


# ── Auth ──────────────────────────────────────────────────────────────────────
async def create_session() -> str:
    token = secrets.token_urlsafe(32)
    SESSIONS[token] = time.time() + SESSION_TTL
    return token


def is_valid_session(token: str | None) -> bool:
    if not token:
        return False
    exp = SESSIONS.get(token)
    if exp is None:
        return False
    if exp < time.time():
        SESSIONS.pop(token, None)
        return False
    return True


async def require_auth(request: Request):
    if not is_valid_session(request.cookies.get(SESSION_COOKIE)):
        raise HTTPException(status_code=401, detail="unauthorized")


# ── API: احراز هویت ────────────────────────────────────────────────────────────
@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    password = str(body.get("password", ""))
    if hash_password(password) != AUTH["password_hash"]:
        raise HTTPException(status_code=401, detail="رمز اشتباهه")
    token = await create_session()
    resp = JSONResponse({"ok": True})
    resp.set_cookie(SESSION_COOKIE, token, max_age=SESSION_TTL, httponly=True, samesite="lax")
    return resp


@app.post("/api/logout")
async def api_logout(request: Request):
    SESSIONS.pop(request.cookies.get(SESSION_COOKIE), None)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE)
    return resp


@app.post("/api/change-password")
async def api_change_password(request: Request, _=Depends(require_auth)):
    body = await request.json()
    new_pw = str(body.get("new_password", ""))
    if len(new_pw) < 4:
        raise HTTPException(status_code=400, detail="رمز خیلی کوتاهه")
    AUTH["password_hash"] = hash_password(new_pw)
    await save_state()
    return {"ok": True}


# ── API: تنظیمات ربات تلگرام (قابل تغییر از خود پنل، بدون نیاز به redeploy) ──
@app.get("/api/settings/telegram")
async def api_get_telegram_settings(_=Depends(require_auth)):
    return {
        "bot_token": TELEGRAM_SETTINGS.get("bot_token", ""),
        "admin_ids": TELEGRAM_SETTINGS.get("admin_ids", []),
        "bot_active": bool(TELEGRAM_SETTINGS.get("bot_token")),
    }


@app.post("/api/settings/telegram")
async def api_set_telegram_settings(request: Request, _=Depends(require_auth)):
    from telegram_bot import reset_offset
    body = await request.json()
    new_token = str(body.get("bot_token", "")).strip()
    admin_ids_raw = str(body.get("admin_ids", ""))
    admin_ids = [int(x) for x in admin_ids_raw.replace(" ", "").split(",") if x.lstrip("-").isdigit()]

    token_changed = new_token != TELEGRAM_SETTINGS.get("bot_token", "")
    TELEGRAM_SETTINGS["bot_token"] = new_token
    TELEGRAM_SETTINGS["admin_ids"] = admin_ids
    if token_changed:
        reset_offset()  # آفست قبلی مال یه ربات دیگه بوده، برای توکن جدید بی‌معنیه
    await save_state()
    return {"ok": True}


# ── API: کانال‌های عضویت اجباری ────────────────────────────────────────────────
@app.get("/api/settings/channels")
async def api_get_channels(_=Depends(require_auth)):
    return {"channels": TELEGRAM_SETTINGS.get("required_channels", [])}


@app.post("/api/settings/channels")
async def api_add_channel(request: Request, _=Depends(require_auth)):
    body = await request.json()
    cid = str(body.get("id", "")).strip()
    title = str(body.get("title", "")).strip()[:60]
    url = str(body.get("url", "")).strip()
    if not (cid.startswith("@") or cid.startswith("-100")):
        raise HTTPException(status_code=400, detail="آیدی باید با @ یا -100 شروع بشه")
    if not title:
        raise HTTPException(status_code=400, detail="اسم نمایشی رو وارد کن")
    if not url:
        url = f"https://t.me/{cid[1:]}" if cid.startswith("@") else ""
    if not url.startswith("http"):
        raise HTTPException(status_code=400, detail="لینک عضویت معتبر نیست")
    TELEGRAM_SETTINGS.setdefault("required_channels", []).append({"id": cid, "title": title, "url": url})
    await save_state()
    return {"ok": True}


@app.delete("/api/settings/channels/{index}")
async def api_delete_channel(index: int, _=Depends(require_auth)):
    channels = TELEGRAM_SETTINGS.get("required_channels", [])
    if not (0 <= index < len(channels)):
        raise HTTPException(status_code=404, detail="پیدا نشد")
    channels.pop(index)
    await save_state()
    return {"ok": True}


# ── API: مدیریت لینک‌ها ─────────────────────────────────────────────────────
@app.get("/api/links")
async def api_list_links(request: Request, _=Depends(require_auth)):
    host = get_host(request)
    out = []
    for uid, link in LINKS.items():
        out.append({
            "uuid": uid,
            **link,
            "allowed": is_link_allowed(link),
            "used_fmt": fmt_bytes(link.get("used_bytes", 0)),
            "limit_fmt": "∞" if not link.get("limit_bytes") else fmt_bytes(link["limit_bytes"]),
            "vless_link": vless_link_for(uid, host, link["label"]),
        })
    out.sort(key=lambda x: x["created_at"], reverse=True)
    return {"links": out}


@app.post("/api/links")
async def api_create_link(request: Request, _=Depends(require_auth)):
    body = await request.json()
    label = str(body.get("label", "کانفیگ جدید"))
    limit_gb = float(body.get("limit_gb", 0) or 0)
    days = int(body.get("expires_days", 0) or 0)
    limit_bytes = parse_size_to_bytes(limit_gb, "GB") if limit_gb > 0 else 0
    uid, link = await make_link(label, limit_bytes, days)
    host = get_host(request)
    return {"ok": True, "uuid": uid, "vless_link": vless_link_for(uid, host, link["label"])}


@app.patch("/api/links/{uid}")
async def api_update_link(uid: str, request: Request, _=Depends(require_auth)):
    body = await request.json()
    link = LINKS.get(uid)
    if not link:
        raise HTTPException(status_code=404, detail="پیدا نشد")
    if "active" in body:
        link["active"] = bool(body["active"])
    if "label" in body:
        link["label"] = str(body["label"])[:60]
    if "reset_usage" in body and body["reset_usage"]:
        link["used_bytes"] = 0
    if "limit_gb" in body:
        v = float(body.get("limit_gb") or 0)
        link["limit_bytes"] = parse_size_to_bytes(v, "GB") if v > 0 else 0
    if "expires_days" in body:
        d = int(body.get("expires_days") or 0)
        link["expires_at"] = (datetime.now() + timedelta(days=d)).isoformat() if d > 0 else None
    asyncio.create_task(save_state())
    return {"ok": True}


@app.delete("/api/links/{uid}")
async def api_delete_link(uid: str, _=Depends(require_auth)):
    label = await remove_link(uid)
    if label is None:
        raise HTTPException(status_code=404, detail="پیدا نشد")
    return {"ok": True}


# ── WebSocket: تونل واقعی VLESS ─────────────────────────────────────────────
def _is_uuid_allowed(client_uuid: str) -> bool:
    link = LINKS.get(client_uuid)
    return bool(link) and is_link_allowed(link)


def _on_bytes(client_uuid: str, n: int):
    link = LINKS.get(client_uuid)
    if link:
        link["used_bytes"] = link.get("used_bytes", 0) + n


@app.websocket("/ws/{uid}")
async def websocket_tunnel(websocket: WebSocket, uid: str):
    # uid توی مسیر فقط برای خوانایی/لاگه؛ UUID واقعی از داخل هدر VLESS پارس می‌شه
    await run_tunnel(websocket, is_uuid_allowed=_is_uuid_allowed, on_bytes=_on_bytes)


# ── صفحات HTML ────────────────────────────────────────────────────────────────
from pages import LOGIN_HTML, DASHBOARD_HTML


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if is_valid_session(request.cookies.get(SESSION_COOKIE)):
        return RedirectResponse(url="/dashboard")
    return HTMLResponse(content=LOGIN_HTML)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not is_valid_session(request.cookies.get(SESSION_COOKIE)):
        return RedirectResponse(url="/login")
    return HTMLResponse(content=DASHBOARD_HTML)


@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(content="<script>location.href='/dashboard'</script>")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info", workers=1)
