# telegram_bot.py
# ══════════════════════════════════════════════════════════════════════════════
# ربات تلگرام ساده — فقط برای مدیریت لینک‌ها توسط ادمین(ها). هیچ منطق
# فروشگاه/کیف‌پول/رفرالی نداره؛ فقط: ساخت لینک، لیست، فعال/غیرفعال، ریست
# مصرف، حذف. با long-polling کار می‌کنه (بدون نیاز به webhook/دامنه‌ی جدا).
# ══════════════════════════════════════════════════════════════════════════════

import asyncio
import logging

import httpx

logger = logging.getLogger("telegram_bot")

API_BASE = "https://api.telegram.org/bot{token}/{method}"
POLL_TIMEOUT = 30

_client: httpx.AsyncClient | None = None
_poll_task: asyncio.Task | None = None
_offset = 0
_pending: dict[int, dict] = {}   # chat_id -> {"action": "...", "step": "...", "data": {...}}


def _token() -> str:
    import main
    return main.TELEGRAM_SETTINGS.get("bot_token", "")


def _admin_ids() -> list[int]:
    import main
    return main.TELEGRAM_SETTINGS.get("admin_ids", [])


async def _api(method: str, **params):
    token = _token()
    if not token:
        return None
    url = API_BASE.format(token=token, method=method)
    try:
        r = await _client.post(url, json=params, timeout=POLL_TIMEOUT + 10)
        return r.json()
    except Exception as e:
        logger.warning(f"telegram api error ({method}): {e}")
        return None


async def _send(chat_id: int, text: str, kb: dict | None = None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb:
        payload["reply_markup"] = kb
    await _api("sendMessage", **payload)


async def _edit(chat_id: int, message_id: int, text: str, kb: dict | None = None):
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text,
               "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb:
        payload["reply_markup"] = kb
    res = await _api("editMessageText", **payload)
    if res is None or not res.get("ok"):
        await _send(chat_id, text, kb)


async def _answer_cb(cb_id: str, text: str = "", alert: bool = False):
    await _api("answerCallbackQuery", callback_query_id=cb_id, text=text, show_alert=alert)


# ── عضویت اجباری در کانال‌ها ──────────────────────────────────────────────────
async def _is_member(channel_id: str, user_id: int) -> bool | None:
    """True=عضوه، False=عضو نیست، None=مشخص نشد (مثلاً ربات ادمین کانال نیست)."""
    res = await _api("getChatMember", chat_id=channel_id, user_id=user_id)
    if not res or not res.get("ok"):
        logger.warning(f"عدم موفقیت در چک عضویت کانال {channel_id}: {res}")
        return None
    status = res.get("result", {}).get("status")
    return status in ("member", "administrator", "creator")


async def _missing_channels(user_id: int) -> list[dict]:
    import main
    channels = main.TELEGRAM_SETTINGS.get("required_channels", [])
    missing = []
    for ch in channels:
        ok = await _is_member(ch["id"], user_id)
        if ok is False:  # فقط وقتی مطمئنیم عضو نیست؛ خطای نامشخص رو نادیده می‌گیریم تا کل ربات قفل نشه
            missing.append(ch)
    return missing


def _join_gate_kb(missing: list[dict]) -> dict:
    rows = [[{"text": f"📢 عضویت در {ch['title']}", "url": ch["url"]}] for ch in missing]
    rows.append([{"text": "✅ عضو شدم، بررسی کن", "callback_data": "checkjoin"}])
    return {"inline_keyboard": rows}


def _join_gate_text(missing: list[dict]) -> str:
    names = "\n".join(f"• {ch['title']}" for ch in missing)
    return (
        "🔒 برای استفاده از ربات باید اول توی کانال(های) زیر عضو بشی:\n\n"
        f"{names}\n\n"
        "بعد از عضویت روی دکمه‌ی «✅ عضو شدم» بزن."
    )


# ── منوها ────────────────────────────────────────────────────────────────────
def _main_menu_kb():
    return {"inline_keyboard": [
        [{"text": "➕ ساخت لینک جدید", "callback_data": "new"}],
        [{"text": "📋 لیست لینک‌ها", "callback_data": "list:0"}],
        [{"text": "⚙️ تنظیمات", "callback_data": "settings"}],
    ]}


def _settings_menu_kb():
    return {"inline_keyboard": [
        [{"text": "📢 کانال‌های عضویت اجباری", "callback_data": "channels"}],
        [{"text": "🔙 بازگشت", "callback_data": "home"}],
    ]}


def _channels_menu_kb():
    import main
    channels = main.TELEGRAM_SETTINGS.get("required_channels", [])
    rows = [[{"text": f"❌ حذف: {ch['title']}", "callback_data": f"delchannel:{i}"}] for i, ch in enumerate(channels)]
    rows.append([{"text": "➕ افزودن کانال جدید", "callback_data": "addchannel"}])
    rows.append([{"text": "🔙 بازگشت", "callback_data": "settings"}])
    return {"inline_keyboard": rows}


def _channels_menu_text() -> str:
    import main
    channels = main.TELEGRAM_SETTINGS.get("required_channels", [])
    if not channels:
        return "📢 <b>کانال‌های عضویت اجباری</b>\n\nهنوز هیچ کانالی اضافه نکردی. کاربرها بدون هیچ محدودیتی می‌تونن از ربات استفاده کنن."
    lines = "\n".join(f"{i+1}. {ch['title']} — <code>{ch['id']}</code>" for i, ch in enumerate(channels))
    return f"📢 <b>کانال‌های عضویت اجباری</b>\n\n{lines}\n\n⚠️ ربات باید توی همه‌ی این کانال‌ها ادمین باشه تا بتونه عضویت رو چک کنه."


def _link_kb(uid: str, active: bool):
    return {"inline_keyboard": [
        [{"text": "🔴 غیرفعال کردن" if active else "🟢 فعال کردن", "callback_data": f"toggle:{uid}"}],
        [{"text": "♻️ ریست مصرف", "callback_data": f"reset:{uid}"}],
        [{"text": "🗑 حذف", "callback_data": f"del:{uid}"}],
        [{"text": "🔙 بازگشت به لیست", "callback_data": "list:0"}],
    ]}


def _link_detail_text(uid: str, link: dict, vless_link: str) -> str:
    import main
    status = "🟢 فعال" if main.is_link_allowed(link) else "🔴 غیرفعال/منقضی/تمام‌شده"
    exp = link.get("expires_at")
    exp_txt = exp.split("T")[0] if exp else "نامحدود"
    limit_txt = "نامحدود" if not link.get("limit_bytes") else main.fmt_bytes(link["limit_bytes"])
    return (
        f"🔗 <b>{link['label']}</b>\n"
        f"وضعیت: {status}\n"
        f"مصرف: {main.fmt_bytes(link.get('used_bytes', 0))} / {limit_txt}\n"
        f"انقضا: {exp_txt}\n\n"
        f"<code>{vless_link}</code>"
    )


# ── هندلر پیام‌ها ────────────────────────────────────────────────────────────
async def _handle_message(msg: dict):
    import main
    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()
    is_admin = chat_id in _admin_ids()

    if text == "/start":
        _pending.pop(chat_id, None)
        if is_admin:
            await _send(chat_id, "👋 به پنل مدیریت Matix خوش اومدی.", _main_menu_kb())
            return
        missing = await _missing_channels(chat_id)
        if missing:
            await _send(chat_id, _join_gate_text(missing), _join_gate_kb(missing))
            return
        await _send(chat_id, "👋 به Matix خوش اومدی!\nبرای دریافت کانفیگ با ادمین در ارتباط باش.")
        return

    if not is_admin:
        return  # کاربر عادی فقط /start می‌تونه بزنه؛ بقیه‌ی منطق دست ادمینه

    pending = _pending.get(chat_id)
    if not pending:
        return

    if pending["action"] == "new_link":
        step = pending["step"]
        data = pending["data"]
        if step == "label":
            data["label"] = text[:60] or "کانفیگ جدید"
            pending["step"] = "limit"
            await _send(chat_id, "📦 چند گیگ حجم داشته باشه؟ (برای نامحدود بنویس 0)")
        elif step == "limit":
            try:
                data["limit_gb"] = float(text.replace(",", "."))
            except ValueError:
                await _send(chat_id, "❗️ فقط عدد بفرست (مثلاً 20 یا 0):")
                return
            pending["step"] = "days"
            await _send(chat_id, "⏳ چند روز اعتبار داشته باشه؟ (برای نامحدود بنویس 0)")
        elif step == "days":
            if not text.isdigit():
                await _send(chat_id, "❗️ فقط عدد صحیح بفرست:")
                return
            data["days"] = int(text)
            uid, link = await main.make_link(data["label"], main.parse_size_to_bytes(data["limit_gb"], "GB") if data["limit_gb"] > 0 else 0, data["days"])
            host = main.get_host()
            vless_link = main.vless_link_for(uid, host, link["label"])
            _pending.pop(chat_id, None)
            await _send(chat_id, "✅ لینک ساخته شد:\n\n" + _link_detail_text(uid, link, vless_link), _link_kb(uid, link["active"]))
        return

    if pending["action"] == "add_channel":
        step = pending["step"]
        data = pending["data"]
        if step == "id":
            cid = text.strip()
            if not (cid.startswith("@") or cid.startswith("-100")):
                await _send(chat_id, "❗️ آیدی باید با @ شروع بشه (کانال عمومی) یا با -100 (کانال خصوصی). دوباره بفرست:")
                return
            data["id"] = cid
            pending["step"] = "title"
            await _send(chat_id, "🏷 یک اسم نمایشی برای این کانال بفرست (مثلاً «کانال اطلاع‌رسانی»):")
        elif step == "title":
            data["title"] = text[:60] or data["id"]
            pending["step"] = "url"
            default_url = f"https://t.me/{data['id'][1:]}" if data["id"].startswith("@") else ""
            hint = f"\n(برای استفاده از {default_url} فقط بنویس -)" if default_url else ""
            await _send(chat_id, f"🔗 لینک عضویت (join link) کانال رو بفرست.{hint}")
        elif step == "url":
            url = text.strip()
            if url == "-" and data["id"].startswith("@"):
                url = f"https://t.me/{data['id'][1:]}"
            if not url.startswith("http"):
                await _send(chat_id, "❗️ لینک باید با https:// شروع بشه. دوباره بفرست:")
                return
            data["url"] = url
            main.TELEGRAM_SETTINGS.setdefault("required_channels", []).append(
                {"id": data["id"], "title": data["title"], "url": data["url"]}
            )
            asyncio.create_task(main.save_state())
            _pending.pop(chat_id, None)
            await _send(chat_id, f"✅ کانال «{data['title']}» به لیست عضویت اجباری اضافه شد.\n\n"
                                  f"⚠️ یادت نره ربات رو توی این کانال ادمین کنی، وگرنه نمی‌تونه عضویت رو چک کنه.",
                        _channels_menu_kb())
        return


async def _handle_callback(cb: dict):
    import main
    chat_id = cb["message"]["chat"]["id"]
    message_id = cb["message"]["message_id"]
    data = cb["data"]
    cb_id = cb["id"]
    is_admin = chat_id in _admin_ids()

    if not is_admin:
        if data == "checkjoin":
            missing = await _missing_channels(chat_id)
            if missing:
                await _answer_cb(cb_id, "هنوز عضو همه‌ی کانال‌ها نشدی ❗️", alert=True)
                await _edit(chat_id, message_id, _join_gate_text(missing), _join_gate_kb(missing))
            else:
                await _answer_cb(cb_id, "عضویت تایید شد ✅")
                await _edit(chat_id, message_id, "✅ عضویتت تایید شد!\nبرای دریافت کانفیگ با ادمین در ارتباط باش.")
            return
        await _answer_cb(cb_id, "⛔️ اجازه نداری")
        return

    await _answer_cb(cb_id)

    if data == "home":
        await _edit(chat_id, message_id, "👋 به پنل مدیریت Matix خوش اومدی.", _main_menu_kb())
        return

    if data == "settings":
        await _edit(chat_id, message_id, "⚙️ تنظیمات", _settings_menu_kb())
        return

    if data == "channels":
        await _edit(chat_id, message_id, _channels_menu_text(), _channels_menu_kb())
        return

    if data == "addchannel":
        _pending[chat_id] = {"action": "add_channel", "step": "id", "data": {}}
        await _edit(chat_id, message_id,
            "📢 آیدی عددی یا یوزرنیم کانال رو بفرست:\n"
            "• کانال عمومی: <code>@channel</code>\n"
            "• کانال خصوصی: <code>-1001234567890</code>\n\n"
            "⚠️ قبلش حتماً ربات رو توی اون کانال ادمین کن.")
        return

    if data.startswith("delchannel:"):
        idx = int(data.split(":", 1)[1])
        channels = main.TELEGRAM_SETTINGS.get("required_channels", [])
        if 0 <= idx < len(channels):
            removed = channels.pop(idx)
            asyncio.create_task(main.save_state())
            await _edit(chat_id, message_id, f"🗑 کانال «{removed['title']}» حذف شد.\n\n" + _channels_menu_text(), _channels_menu_kb())
        return

    if data == "new":
        _pending[chat_id] = {"action": "new_link", "step": "label", "data": {}}
        await _edit(chat_id, message_id, "🏷 یک اسم برای این کانفیگ بفرست:")
        return

    if data.startswith("list:"):
        page = int(data.split(":")[1])
        items = sorted(main.LINKS.items(), key=lambda x: x[1]["created_at"], reverse=True)
        page_size = 8
        chunk = items[page * page_size:(page + 1) * page_size]
        if not chunk and page == 0:
            await _edit(chat_id, message_id, "هنوز هیچ لینکی نساختی.", _main_menu_kb())
            return
        rows = []
        for uid, link in chunk:
            mark = "🟢" if main.is_link_allowed(link) else "🔴"
            rows.append([{"text": f"{mark} {link['label']}", "callback_data": f"view:{uid}"}])
        nav = []
        if page > 0:
            nav.append({"text": "◀️ قبلی", "callback_data": f"list:{page-1}"})
        if len(items) > (page + 1) * page_size:
            nav.append({"text": "بعدی ▶️", "callback_data": f"list:{page+1}"})
        if nav:
            rows.append(nav)
        rows.append([{"text": "➕ ساخت لینک جدید", "callback_data": "new"}])
        await _edit(chat_id, message_id, f"📋 لینک‌ها ({len(items)} عدد):", {"inline_keyboard": rows})
        return

    if data.startswith("view:"):
        uid = data.split(":", 1)[1]
        link = main.LINKS.get(uid)
        if not link:
            await _edit(chat_id, message_id, "این لینک دیگه وجود نداره.", _main_menu_kb())
            return
        host = main.get_host()
        vless_link = main.vless_link_for(uid, host, link["label"])
        await _edit(chat_id, message_id, _link_detail_text(uid, link, vless_link), _link_kb(uid, link["active"]))
        return

    if data.startswith("toggle:"):
        uid = data.split(":", 1)[1]
        link = main.LINKS.get(uid)
        if link:
            link["active"] = not link.get("active", True)
            asyncio.create_task(main.save_state())
            host = main.get_host()
            vless_link = main.vless_link_for(uid, host, link["label"])
            await _edit(chat_id, message_id, _link_detail_text(uid, link, vless_link), _link_kb(uid, link["active"]))
        return

    if data.startswith("reset:"):
        uid = data.split(":", 1)[1]
        link = main.LINKS.get(uid)
        if link:
            link["used_bytes"] = 0
            asyncio.create_task(main.save_state())
            host = main.get_host()
            vless_link = main.vless_link_for(uid, host, link["label"])
            await _edit(chat_id, message_id, "♻️ مصرف ریست شد.\n\n" + _link_detail_text(uid, link, vless_link), _link_kb(uid, link["active"]))
        return

    if data.startswith("del:"):
        uid = data.split(":", 1)[1]
        label = await main.remove_link(uid)
        if label:
            await _edit(chat_id, message_id, f"🗑 لینک «{label}» حذف شد.", _main_menu_kb())
        return


# ── حلقه‌ی long-polling ─────────────────────────────────────────────────────
async def _poll_loop():
    global _offset
    while True:
        try:
            if not _token():
                await asyncio.sleep(5)
                continue
            res = await _api("getUpdates", offset=_offset, timeout=POLL_TIMEOUT, allowed_updates=["message", "callback_query"])
            if not res or not res.get("ok"):
                await asyncio.sleep(3)
                continue
            for update in res.get("result", []):
                _offset = update["update_id"] + 1
                if "message" in update:
                    await _handle_message(update["message"])
                elif "callback_query" in update:
                    await _handle_callback(update["callback_query"])
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"poll loop error: {e}")
            await asyncio.sleep(3)


async def start_bot():
    global _client, _poll_task
    _client = httpx.AsyncClient()
    if _token():
        logger.info("ربات تلگرام روشن شد (long polling)")
    else:
        logger.info("TELEGRAM_BOT_TOKEN تنظیم نشده — ربات خاموش می‌مونه")
    _poll_task = asyncio.create_task(_poll_loop())


def reset_offset():
    """وقتی توکن ربات از پنل عوض می‌شه، آفست قدیمی برای ربات جدید بی‌معنیه."""
    global _offset
    _offset = 0


async def stop_bot():
    global _poll_task, _client
    if _poll_task:
        _poll_task.cancel()
    if _client:
        await _client.aclose()
