# telegram_bot.py
# ══════════════════════════════════════════════════════════════════════════════
# ربات Matix — فروشگاه + مدیریت کانفیگ.
#   • ادمین‌ها (TELEGRAM_ADMIN_IDS): مدیریت کامل، ساخت دستی کانفیگ، تایید/رد سفارش‌ها،
#     تنظیمات قیمت و تست رایگان.
#   • بقیه‌ی کاربرها (مشتری): خرید کانفیگ با حجم/مدت دلخواه (ارسال رسید → تایید ادمین
#     → تحویل خودکار کانفیگ)، دریافت یک کانفیگ تست رایگان، مشاهده‌ی کانفیگ‌های خودشون.
# با long polling کار می‌کنه، نیازی به دامنه/webhook نداره.
# ══════════════════════════════════════════════════════════════════════════════

import asyncio
import io
import json
import os
import random
import re
import time

import aiofiles
import httpx

try:
    import qrcode
    _QR_AVAILABLE = True
except Exception:
    qrcode = None
    _QR_AVAILABLE = False

from datetime import datetime, timedelta
from urllib.parse import quote

from main import (
    LINKS,
    make_link,
    remove_link,
    set_link_active,
    vless_link_for_link,
    get_host,
    fmt_bytes,
    is_link_allowed,
    logger,
    log_activity,
    save_state,
    DATA_DIR,
    PROTOCOLS,
    DEFAULT_PROTOCOL,
    FINGERPRINTS,
    DEFAULT_FINGERPRINT,
    DEFAULT_ALPN_BY_PROTOCOL,
    DEFAULT_PORT,
    DEFAULT_SPEED_LIMIT,
    MIN_PORT,
    MAX_PORT,
    parse_size_to_bytes,
    parse_speed_to_bytes,
    TELEGRAM_SETTINGS,
)

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
_admin_ids_raw = os.environ.get("TELEGRAM_ADMIN_IDS", "").strip()
ADMIN_IDS = {int(x) for x in _admin_ids_raw.replace(" ", "").split(",") if x.isdigit()} if _admin_ids_raw else set()

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
PAGE_SIZE = 6
BOT_NAME = "Matix"
V2BOX_URL = "https://play.google.com/store/apps/details?id=dev.hexasoftware.v2box"

# ── فروشگاه: قیمت‌گذاری، سفارش‌ها، مصرف‌کنندگان تست رایگان ────────────────────
SHOP_FILE = DATA_DIR / "matix_shop.json"
SHOP_LOCK = asyncio.Lock()
SHOP: dict = {
    "price_per_gb": 15000,      # تومان به‌ازای هر گیگابایت
    "price_per_day": 2000,      # تومان به‌ازای هر روز اعتبار
    "min_price": 20000,         # حداقل مبلغ قابل‌قبول سفارش
    "trial_volume_gb": 0.2,     # حجم کانفیگ تست رایگان (گیگابایت)
    "trial_days": 1,            # مدت اعتبار کانفیگ تست رایگان (روز)
    "card_number": "",          # شماره کارت برای واریز (نمایش به مشتری)
    "card_owner": "",           # نام صاحب کارت
    "trial_used": [],           # لیست chat_id هایی که تست رایگان گرفته‌اند
    "orders": {},               # order_id(str) -> order dict
    "order_seq": 0,
    "discount_codes": {},       # CODE(str) -> {"type":"percent"|"fixed","value":float,"active":bool,"max_uses":int,"used_count":int,"expires_at":str|None}
    "gift_codes": {},           # CODE(str) -> {"amount":int,"active":bool,"max_uses":int,"used_count":int,"redeemed_by":[chat_id,...]}
    "announce_channel": "",     # آیدی عددی یا یوزرنیم کانال اعلان خرید، مثلاً @mychannel یا -100123456789
    "wallets": {},               # str(chat_id) -> موجودی کیف پول (تومان)
    "wallet_topups": {},         # topup_id(str) -> {"chat_id","amount","status","created_at","receipt_message_id"}
    "wallet_topup_seq": 0,
    "wallet_topup_presets": [50000, 100000, 200000, 500000],  # مبالغ پیشنهادی شارژ کیف پول (تومان) — از پنل ادمین قابل تغییره
    "referrals": {},              # str(referred_chat_id) -> referrer_chat_id
    "referral_bonus": 10000,      # پاداش معرفی (تومان) به معرف، بعد از اولین خرید موفق زیرمجموعه
    "known_users": [],            # لیست همه‌ی chat_id هایی که تا حالا با ربات تعامل داشته‌اند
    "required_channel": "",       # آیدی عددی یا یوزرنیم کانالی که عضویت توش اجباریه (خالی = بدون محدودیت)
    "required_channel_url": "",   # لینک عضویت در همون کانال (برای دکمه)
    "support_username": "",       # یوزرنیم پشتیبانی برای دکمه‌ی «پشتیبانی» (بدون @)
    "channels": [],                # [{"name": "کانال اصلی", "url": "https://t.me/..."}]
    "support_messages": [],        # [{"id":int,"chat_id":,"username":,"text":,"at":iso,"read":bool,"replies":[{"from":"admin"|"user","text":,"at":}]}]
    "support_next_id": 1,
    "join_log": [],                 # [{"chat_id":,"username":,"at":iso}] — آخرین ۵۰۰ ورود
    "spins": {},                   # str(chat_id) -> تاریخ آخرین چرخش گردونه‌ی شانس (ISO date)
    "spin_min": 2000,              # حداقل جایزه‌ی گردونه‌ی شانس (تومان)
    "spin_max": 20000,             # حداکثر جایزه‌ی گردونه‌ی شانس (تومان)
}

async def _load_shop():
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        if SHOP_FILE.exists():
            async with aiofiles.open(SHOP_FILE, "r", encoding="utf-8") as f:
                raw = await f.read()
            data = json.loads(raw)
            SHOP.update(data)
    except Exception as e:
        logger.warning(f"Matix shop state could not be loaded: {e}")

async def _save_shop():
    async with SHOP_LOCK:
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            tmp = SHOP_FILE.with_suffix(".tmp")
            async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
                await f.write(json.dumps(SHOP, ensure_ascii=False, indent=2))
            tmp.replace(SHOP_FILE)
        except Exception as e:
            logger.warning(f"Matix shop state could not be saved: {e}")

def _next_order_id() -> str:
    SHOP["order_seq"] = int(SHOP.get("order_seq", 0)) + 1
    return str(SHOP["order_seq"])

def _price_for(volume_gb: float, days: int) -> int:
    price = volume_gb * SHOP.get("price_per_gb", 0) + days * SHOP.get("price_per_day", 0)
    return max(int(price), int(SHOP.get("min_price", 0)))

# ── کدهای تخفیف ───────────────────────────────────────────────────────────
def _find_discount(code: str) -> dict | None:
    if not code:
        return None
    return SHOP.get("discount_codes", {}).get(code.strip().upper())

def _discount_valid(d: dict | None) -> bool:
    if not d or not d.get("active", True):
        return False
    max_uses = d.get("max_uses", 0)
    if max_uses and d.get("used_count", 0) >= max_uses:
        return False
    exp = d.get("expires_at")
    if exp:
        try:
            if datetime.now() > datetime.fromisoformat(exp):
                return False
        except ValueError:
            pass
    return True

def _apply_discount(price: int, d: dict) -> int:
    if d.get("type") == "percent":
        price = price - int(price * float(d.get("value", 0)) / 100)
    else:
        price = price - int(d.get("value", 0))
    return max(price, 0)

def _discount_desc(d: dict) -> str:
    if d.get("type") == "percent":
        return f"{d.get('value')}٪ تخفیف"
    return f"{int(d.get('value', 0)):,} تومان تخفیف"

# ── کدهای هدیه (شارژ مستقیم کیف پول با مبلغ دلخواه) ─────────────────────────
def _find_gift(code: str) -> dict | None:
    if not code:
        return None
    return SHOP.get("gift_codes", {}).get(code.strip().upper())

def _gift_valid_for(d: dict | None, chat_id: int) -> tuple[bool, str]:
    if not d:
        return False, "این کد هدیه وجود نداره."
    if not d.get("active", True):
        return False, "این کد هدیه غیرفعال شده."
    if chat_id in d.get("redeemed_by", []):
        return False, "قبلاً این کد رو استفاده کردی."
    max_uses = d.get("max_uses", 0)
    if max_uses and d.get("used_count", 0) >= max_uses:
        return False, "ظرفیت استفاده از این کد تموم شده."
    return True, ""

def _gift_desc(d: dict) -> str:
    max_uses = d.get("max_uses", 0) or "نامحدود"
    return f"{int(d.get('amount', 0)):,} تومان — {d.get('used_count', 0)}/{max_uses} استفاده"

# ── کیف پول ───────────────────────────────────────────────────────────────
def _wallet_balance(chat_id: int) -> int:
    return int(SHOP.get("wallets", {}).get(str(chat_id), 0))

def _wallet_add(chat_id: int, amount: int):
    w = SHOP.setdefault("wallets", {})
    w[str(chat_id)] = int(w.get(str(chat_id), 0)) + int(amount)

def _wallet_sub(chat_id: int, amount: int) -> bool:
    if _wallet_balance(chat_id) < amount:
        return False
    w = SHOP.setdefault("wallets", {})
    w[str(chat_id)] = int(w.get(str(chat_id), 0)) - int(amount)
    return True

def _next_topup_id() -> str:
    SHOP["wallet_topup_seq"] = int(SHOP.get("wallet_topup_seq", 0)) + 1
    return str(SHOP["wallet_topup_seq"])

# ── رفرال ─────────────────────────────────────────────────────────────────
def _register_referral(chat_id: int, referrer_id: int):
    refs = SHOP.setdefault("referrals", {})
    key = str(chat_id)
    if key in refs or referrer_id == chat_id:
        return
    refs[key] = referrer_id

async def _reward_referrer_if_first_order(order: dict):
    """اگه این اولین سفارشِ تایید‌شده‌ی این کاربر باشه و از طریق رفرال اومده باشه، به معرفش پاداش می‌ده."""
    ref_chat = SHOP.get("referrals", {}).get(str(order["chat_id"]))
    if not ref_chat:
        return
    prior_approved = [
        o for o in SHOP.get("orders", {}).values()
        if o.get("chat_id") == order["chat_id"] and o.get("status") == "approved" and o.get("id") != order.get("id")
    ]
    if prior_approved:
        return
    bonus = int(SHOP.get("referral_bonus", 0))
    if bonus <= 0:
        return
    _wallet_add(ref_chat, bonus)
    await _save_shop()
    await _send(ref_chat, f"🎉 یکی از دوستایی که دعوت کردی خرید کرد! <b>{bonus:,} تومان</b> به کیف پولت اضافه شد.")

# ── عضویت اجباری در کانال ────────────────────────────────────────────────
async def _passes_membership_gate(chat_id: int) -> bool:
    required = SHOP.get("required_channel")
    if not required or _is_admin(chat_id):
        return True
    res = await _call("getChatMember", chat_id=required, user_id=chat_id)
    status = ((res or {}).get("result") or {}).get("status")
    return status in ("member", "administrator", "creator")

def _join_gate_kb(chat_id: int):
    url = SHOP.get("required_channel_url") or f"https://t.me/{str(SHOP.get('required_channel', '')).lstrip('@')}"
    return {"inline_keyboard": [
        [{"text": "📣 عضویت در کانال", "url": url}],
        [{"text": "✅ عضو شدم، ادامه بده", "callback_data": "checkjoin"}],
    ]}

# ── آمار ادمین ────────────────────────────────────────────────────────────
def _apply_renewal(uid: str, extra_gb: float, extra_days: int):
    """حجم/روز اضافه رو به یه کانفیگ موجود اعمال می‌کنه و لینک بروزشده رو برمی‌گردونه."""
    link = LINKS[uid]
    if extra_gb > 0:
        extra_bytes = parse_size_to_bytes(extra_gb, "GB")
        current_limit = int(link.get("limit_bytes", 0) or 0)
        if current_limit > 0:
            link["limit_bytes"] = current_limit + extra_bytes
    if extra_days > 0:
        now = datetime.now()
        current_exp = link.get("expires_at")
        try:
            base = datetime.fromisoformat(current_exp) if current_exp else now
        except ValueError:
            base = now
        if base < now:
            base = now
        link["expires_at"] = (base + timedelta(days=extra_days)).isoformat()
    link["active"] = True
    return uid, link

def _admin_stats_text() -> str:
    total_links = len(LINKS)
    active_links = sum(1 for l in LINKS.values() if is_link_allowed(l))
    orders = list(SHOP.get("orders", {}).values())
    approved = [o for o in orders if o.get("status") == "approved"]
    pending = [o for o in orders if o.get("status") == "pending"]
    rejected = [o for o in orders if o.get("status") == "rejected"]
    revenue = sum(int(o.get("price", 0)) for o in approved)
    today = datetime.now().date().isoformat()
    today_orders = [o for o in approved if str(o.get("created_at", ""))[:10] == today]
    today_rev = sum(int(o.get("price", 0)) for o in today_orders)
    users = len(SHOP.get("known_users", []))
    wallets_total = sum(int(v) for v in SHOP.get("wallets", {}).values())
    return (
        "📊 <b>آمار و داشبورد Matix</b>\n\n"
        f"👥 کاربران آشنا با ربات: <b>{users}</b>\n"
        f"📦 کل کانفیگ‌ها: <b>{total_links}</b>  (فعال: {active_links})\n\n"
        f"🧾 سفارش‌ها — کل: <b>{len(orders)}</b>\n"
        f"   ✅ تایید‌شده: {len(approved)}   ⏳ در انتظار: {len(pending)}   ❌ رد‌شده: {len(rejected)}\n\n"
        f"💰 کل درآمد (تایید‌شده): <b>{revenue:,} تومان</b>\n"
        f"📅 امروز: {len(today_orders)} سفارش — <b>{today_rev:,} تومان</b>\n\n"
        f"👛 مجموع موجودی کیف‌پول‌ها: {wallets_total:,} تومان"
    )

# ── زبان (فارسی / English) ─────────────────────────────────────────────────
_lang: dict = {}  # chat_id -> "fa" | "en"

def _lg(chat_id: int) -> str:
    return _lang.get(chat_id, "fa")

def _toggle_lang(chat_id: int) -> str:
    _lang[chat_id] = "en" if _lg(chat_id) == "fa" else "fa"
    return _lang[chat_id]

T = {
    "admin_home_title": {"fa": "💠 پنل مدیریت Matix", "en": "💠 Matix Control Room"},
    "admin_home_sub": {
        "fa": "به کنترل‌روم Matix خوش اومدی، کاپیتان 🧭\nاز میان گزینه‌های زیر انتخاب کن:",
        "en": "Welcome to the Matix control room, captain 🧭\nPick an option below:",
    },
    "cust_home_title": {"fa": "<b>🕊️ به نام خدا</b>", "en": "<b>🕊️ In the name of God</b>"},
    "cust_home_sub": {
        "fa": (
            "به <b>Matix</b> خوش اومدی ✨\n"
            "<b>اینترنتِ آزاد، پرسرعت و پایدار</b> — بدون قطعی، بدون دردسر 🚀\n\n"
            "🔹 هر حجم و هر مدتی که بخوای، با چند لمس بساز\n"
            "🔹 تحویل کاملاً خودکار و آنی بعد از تایید پرداخت\n"
            "🔹 پیش از خرید، یک کانفیگ تست رایگان بگیر\n\n"
            "<b>از منوی زیر شروع کن 👇</b>"
        ),
        "en": (
            "Welcome to <b>Matix</b> ✨\n"
            "<b>Free, fast and stable internet</b> — no drops, no hassle 🚀\n\n"
            "🔹 Any volume, any duration, built in a few taps\n"
            "🔹 Fully automatic, instant delivery after payment approval\n"
            "🔹 Grab a free trial before you buy\n\n"
            "<b>Start from the menu below 👇</b>"
        ),
    },
    "cust_home_sub_start": {
        "fa": (
            "به <b>Matix</b> خوش اومدی ✨\n"
            "اینترنتِ آزاد، پرسرعت و پایدار، همیشه در دسترسته 🚀\n"
            "از منوی رنگی زیر هر کاری که لازم داری رو با یه لمس انجام بده 👇"
        ),
        "en": (
            "Welcome back to <b>Matix</b> ✨\n"
            "Free, fast and stable internet, always ready for you 🚀\n"
            "Pick anything you need from the colorful menu below 👇"
        ),
    },
    "cust_home_sub_return": {
        "fa": (
            "<b>به صفحه‌ی اصلی برگشتی</b> 🏠\n"
            "هر کانفیگ، خرید، تمدید یا شارژ کیف پولی که لازم داری از همینجا در دسترسه؛\n"
            "از منوی زیر ادامه بده 👇"
        ),
        "en": (
            "<b>Back to the home menu</b> 🏠\n"
            "Every config, purchase, renewal, or wallet action is right here;\n"
            "continue from the menu below 👇"
        ),
    },
    "btn_buy": {"fa": "🛒 خرید کانفیگ", "en": "🛒 Buy a Config"},
    "btn_trial": {"fa": "🎁 دریافت تست رایگان", "en": "🎁 Get Free Trial"},
    "btn_myconfigs": {"fa": "📦 کانفیگ‌های من", "en": "📦 My Configs"},
    "btn_help": {"fa": "❔ راهنما", "en": "❔ Help"},
    "btn_share": {"fa": "📤 معرفی به دوستان", "en": "📤 Share with Friends"},
    "btn_open_sub": {"fa": "🌐 باز کردن صفحه‌ی اتصال", "en": "🌐 Open Connection Page"},
    "btn_check_usage": {"fa": "📊 استعلام حجم کانفیگ", "en": "📊 Check Config Usage"},
    "usage_prompt": {
        "fa": "📊 <b>استعلام حجم کانفیگ</b>\n\nلینک vless یا لینک ساب کانفیگت رو همین‌جا بفرست تا مصرف و باقی‌مانده‌ی حجمشو برات بگم:",
        "en": "📊 <b>Check Config Usage</b>\n\nSend your config's vless link or subscription link here and I'll tell you the usage and remaining volume:",
    },
    "usage_not_found": {"fa": "❗️ کانفیگی با این لینک/UUID پیدا نشد. دوباره امتحان کن یا از منو یکی از «کانفیگ‌های من» رو انتخاب کن.", "en": "❗️ No config found for that link/UUID. Try again or pick one from \"My Configs\"."},
    "btn_admin_manage": {"fa": "🗂 مدیریت کانفیگ‌ها", "en": "🗂 Manage Configs"},
    "btn_admin_new": {"fa": "➕ ساخت دستی", "en": "➕ Manual Create"},
    "btn_admin_orders": {"fa": "📥 سفارش‌های در انتظار", "en": "📥 Pending Orders"},
    "btn_admin_settings": {"fa": "⚙️ تنظیمات فروش", "en": "⚙️ Shop Settings"},
    "btn_admin_discounts": {"fa": "🏷 کدهای تخفیف", "en": "🏷 Discount Codes"},
    "btn_admin_gifts": {"fa": "🎁 کدهای هدیه", "en": "🎁 Gift Codes"},
    "btn_admin_stats": {"fa": "📊 آمار و گزارش", "en": "📊 Stats & Reports"},
    "btn_admin_broadcast": {"fa": "📢 پیام همگانی", "en": "📢 Broadcast"},
    "btn_wallet": {"fa": "💰 کیف پول", "en": "💰 Wallet"},
    "btn_referral": {"fa": "🤝 دعوت دوستان", "en": "🤝 Invite Friends"},
    "btn_renew": {"fa": "🔄 تمدید کانفیگ", "en": "🔄 Renew Config"},
    "btn_pay_wallet": {"fa": "💰 پرداخت از کیف پول", "en": "💰 Pay from Wallet"},
    "btn_pay_card": {"fa": "💳 پرداخت کارت‌به‌کارت", "en": "💳 Card Payment"},
    "btn_discount_apply": {"fa": "🏷 اعمال کد تخفیف", "en": "🏷 Apply Discount Code"},
    "btn_discount_new": {"fa": "➕ کد تخفیف جدید", "en": "➕ New Discount Code"},
    "btn_refresh": {"fa": "🔄 رفرش", "en": "🔄 Refresh"},
    "btn_lang": {"fa": "🌐 English", "en": "🌐 فارسی"},
    "btn_prev": {"fa": "◀ قبلی", "en": "◀ Prev"},
    "btn_next": {"fa": "بعدی ▶", "en": "Next ▶"},
    "btn_back_home": {"fa": "🏠 صفحه‌ی اصلی", "en": "🏠 Home"},
    "btn_back_list": {"fa": "⬅ بازگشت به لیست", "en": "⬅ Back to List"},
    "btn_show_link": {"fa": "🔗 نمایش لینک اتصال", "en": "🔗 Show Link"},
    "btn_disable": {"fa": "⛔ غیرفعال‌سازی", "en": "⛔ Disable"},
    "btn_enable": {"fa": "✅ فعال‌سازی", "en": "✅ Enable"},
    "btn_delete": {"fa": "🗑 حذف کانفیگ", "en": "🗑 Delete Config"},
    "btn_confirm_delete": {"fa": "✅ بله، حذف کن", "en": "✅ Yes, delete"},
    "btn_cancel": {"fa": "❌ انصراف", "en": "❌ Cancel"},
    "btn_make_config": {"fa": "✅ ساخت کانفیگ", "en": "✅ Create Config"},
    "btn_pay_sent": {"fa": "📎 رسید رو فرستادم", "en": "📎 I've sent the receipt"},
    "btn_approve": {"fa": "✅ تایید و ارسال خودکار", "en": "✅ Approve & Auto-deliver"},
    "btn_reject": {"fa": "❌ رد سفارش", "en": "❌ Reject Order"},
    "no_access": {"fa": "⛔ این بخش فقط برای ادمین‌هاست.", "en": "⛔ Admins only."},
    "cancelled": {"fa": "لغو شد.", "en": "Cancelled."},
    "list_empty": {"fa": "هنوز هیچ کانفیگی ساخته نشده.", "en": "No configs created yet."},
    "list_header": {"fa": "🗂 لیست کانفیگ‌ها ({n} مورد):", "en": "🗂 Config List ({n} items):"},
    "not_exist": {"fa": "این کانفیگ دیگه وجود نداره.", "en": "This config no longer exists."},
    "confirm_delete_q": {"fa": "❗️ از حذف «{label}» مطمئنی؟ این عمل برگشت‌ناپذیره.", "en": "❗️ Delete \"{label}\"? This can't be undone."},
    "deleted": {"fa": "🗑 کانفیگ «{label}» حذف شد.", "en": "🗑 Config \"{label}\" deleted."},
    "already_deleted": {"fa": "این کانفیگ قبلاً حذف شده بود.", "en": "This config was already deleted."},
    "invalid_btn": {"fa": "این دکمه دیگه معتبر نیست.", "en": "This button is no longer valid."},
    "step_invalid": {"fa": "این مرحله دیگه معتبر نیست، از منو دوباره شروع کن.", "en": "This step is no longer valid, start again from the menu."},
    "gen_cancelled": {"fa": "عملیات لغو شد.", "en": "Operation cancelled."},
    "use_menu": {"fa": "از دکمه‌های زیر استفاده کن:", "en": "Use the buttons below:"},
    "no_access_cb": {"fa": "⛔ دسترسی نداری", "en": "⛔ Access denied"},
    "link_msg": {"fa": ("🔗 کانفیگ «{label}»:\n\n<code>{vless}</code>\n\n"
                         "برای اتصال، متن بالا رو کپی کن و توی اپلیکیشنت وارد کن، یا از دکمه‌های زیر استفاده کن 👇"),
                 "en": ("🔗 Config for \"{label}\":\n\n<code>{vless}</code>\n\n"
                        "Copy the text above into your app, or use the buttons below 👇")},
    "link_not_found": {"fa": "کانفیگ پیدا نشد", "en": "Config not found"},
    "created_msg": {"fa": "✅ کانفیگ ساخته شد.\n\n{detail}", "en": "✅ Config created.\n\n{detail}"},
    "help_text": {
        "fa": ("❔ <b>راهنمای Matix</b>\n\n"
               "🛒 <b>خرید کانفیگ</b>: حجم و مدت دلخواهت رو وارد کن، قیمت محاسبه می‌شه، "
               "مبلغ رو واریز کن و رسیدشو بفرست. به محض تایید ادمین، کانفیگت خودکار برات ارسال می‌شه.\n\n"
               "🎁 <b>تست رایگان</b>: یک‌بار برای هر کاربر، بدون نیاز به پرداخت یا تایید.\n\n"
               "📦 <b>کانفیگ‌های من</b>: همه‌ی کانفیگ‌هایی که خریدی یا تست گرفتی، یک‌جا.\n\n"
               "📊 <b>اعلام حجم سریع</b>: کافیه لینک vless یا لینک ساب کانفیگت رو همین‌جا برام بفرستی، "
               "بلافاصله میزان مصرف و باقی‌مانده‌ی حجمشو برات می‌گم.\n\n"
               "📱 <b>چجوری کانفیگ رو وارد کنم؟</b>\n"
               "۱. اول اپلیکیشن <b>v2Box</b> رو با دکمه‌ی زیر نصب کن.\n"
               "۲. کانفیگت رو از «کانفیگ‌های من» باز کن و لینکش رو کپی کن.\n"
               "۳. توی v2Box، روی «+» بالای صفحه بزن و «Import from Clipboard» رو انتخاب کن.\n"
               "۴. کانفیگ به لیستت اضافه می‌شه؛ روش بزن تا وصل بشه، بعد کلید اتصال (پایین صفحه) رو روشن کن. 🚀"),
        "en": ("❔ <b>Matix Help</b>\n\n"
               "🛒 <b>Buy a Config</b>: enter your desired volume and duration, the price is "
               "calculated, pay and send the receipt. Once an admin approves, your config is delivered automatically.\n\n"
               "🎁 <b>Free Trial</b>: one per user, no payment or approval needed.\n\n"
               "📦 <b>My Configs</b>: every config you bought or claimed, in one place.\n\n"
               "📊 <b>Quick Usage Check</b>: just send me your config's vless link or "
               "subscription link, and I'll instantly reply with how much data you've used and how much is left."),
    },
}

def _t(chat_id: int, key: str, **kw) -> str:
    s = T.get(key, {}).get(_lg(chat_id), key)
    return s.format(**kw) if kw else s

# ── انیمیشن‌های ورود ────────────────────────────────────────────────────────
_ENTRY_FRAMES = {
    "fa": ["⭐ در حال همگام‌سازی با Matix", "⭐ در حال همگام‌سازی با Matix.", "⭐ در حال همگام‌سازی با Matix..", "⭐ در حال همگام‌سازی با Matix..."],
    "en": ["⭐ Syncing with Matix", "⭐ Syncing with Matix.", "⭐ Syncing with Matix..", "⭐ Syncing with Matix..."],
}
_GEN_FRAMES = {
    "fa": ["🧊 در حال رمزنگاری کانال...", "🌊 در حال تخصیص مسیر اختصاصی...", "💠 در حال پیکربندی سرور...", "✨ در حال نهایی‌سازی کانفیگ..."],
    "en": ["🧊 Encrypting the channel...", "🌊 Allocating a dedicated route...", "💠 Configuring the server...", "✨ Finalizing your config..."],
}
_PAY_FRAMES = {
    "fa": ["💳 در حال ثبت سفارش...", "📨 در حال اطلاع‌رسانی به تیم پشتیبانی...", "✅ سفارش ثبت شد!"],
    "en": ["💳 Placing your order...", "📨 Notifying the support team...", "✅ Order placed!"],
}

async def _play_frames(chat_id: int, message_id: int, frames: list[str], final_text: str | None = None, final_kb: dict | None = None):
    for frame in frames:
        await _edit(chat_id, message_id, frame)
        await asyncio.sleep(0.4)
    if final_text is not None:
        await _edit(chat_id, message_id, final_text, final_kb)

async def _play_entry_animation(chat_id: int, is_new: bool = False):
    """کاربر /start می‌زنه و بلافاصله منوی اصلی با همه‌ی دکمه‌ها میاد، بدون انیمیشن/تاخیر."""
    home_title, home_sub, kb = _home_view(chat_id, mode="first" if is_new else "start")
    await _send(chat_id, _join_ts(home_title, home_sub), kb)

_client: httpx.AsyncClient | None = None
_poll_task: asyncio.Task | None = None
_running = False
_pending: dict = {}   # chat_id -> {"action": "...", "step": "...", "data": {...}}
_bot_username: str | None = None

# ── استیکرهای بامزه‌ی Matix ──────────────────────────────────────────────────
# مقدار هر کلید باید file_id یک استیکر واقعی تلگرام باشه. برای گرفتن file_id،
# استیکر دلخواه رو برای @RawDataBot (یا @JsonDumpBot) فوروارد کن و مقدار
# "file_id" رو از جواب کپی کن؛ بعد این‌جا جایگزین کن یا با متغیر محیطی زیر ست کن.
STICKERS = {
    "welcome": os.environ.get("MATIX_STICKER_WELCOME", "").strip(),  # 👋 خوش‌آمدگویی
    "gift":    os.environ.get("MATIX_STICKER_GIFT", "").strip(),     # 🎁 تست رایگان / کانفیگ آماده شد
    "success": os.environ.get("MATIX_STICKER_SUCCESS", "").strip(),  # 🎉 تایید سفارش
    "sad":     os.environ.get("MATIX_STICKER_SAD", "").strip(),      # 😔 رد سفارش / خطا
}

async def _send_sticker(chat_id: int, key: str):
    """اگه file_id استیکر برای این کلید تنظیم شده باشه می‌فرستدش؛ در غیر این صورت بی‌صدا رد می‌شه."""
    file_id = STICKERS.get(key)
    if not file_id:
        return
    await _call("sendSticker", chat_id=chat_id, sticker=file_id)

def _share_url() -> str | None:
    """لینک اشتراک‌گذاری ربات Matix رو برای دکمه‌ی «Share» تلگرام می‌سازه (نیاز به یوزرنیم ربات داره)."""
    if not _bot_username:
        return None
    text = quote("🚀 اینترنت آزاد، پرسرعت و پایدار با Matix — همین الان امتحانش کن!")
    return f"https://t.me/share/url?url=https://t.me/{_bot_username}&text={text}"

async def _announce_purchase(order: dict):
    """بعد از تایید سفارش، یه اعلان توی کانال تنظیم‌شده می‌فرسته؛ هویت خریدار فقط نصفه (نصف آیدی/یوزرنیم) نشون داده می‌شه."""
    channel = SHOP.get("announce_channel")
    if not channel:
        return
    await _ensure_username_cached()
    masked = _mask_identity(order.get("chat_id"), order.get("username"))
    text = (
        "🎉 <b>یک خرید تازه در Matix ثبت شد!</b>\n\n"
        f"🙈 خریدار: <code>{masked}</code>\n"
        f"📦 حجم: <b>{order['volume_gb']} GB</b>\n"
        f"📅 مدت اعتبار: <b>{order['days']} روز</b>\n\n"
        "تو هم می‌تونی همین الان اینترنت آزاد و پرسرعتتو با چند لمس بسازی 👇"
    )
    kb = None
    if _bot_username:
        kb = {"inline_keyboard": [[_btn("🛒 من هم می‌خوام بخرم", url=f"https://t.me/{_bot_username}?start=buy", style="success")]]}
    await _call("sendMessage", chat_id=channel, text=text, parse_mode="HTML",
                disable_web_page_preview=True, reply_markup=kb)

def _rebuild_api_base():
    global API_BASE
    API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

def configure(token: str, admin_ids):
    """توکن و لیست آیدی ادمین‌ها رو به‌صورت داینامیک (مثلاً از پنل وب) ست می‌کنه.
    این فقط متغیرهای ماژول رو آپدیت می‌کنه؛ برای اعمال واقعی باید ربات
    متوقف و دوباره start بشه (به restart_bot نگاه کن)."""
    global BOT_TOKEN, ADMIN_IDS, _bot_username
    BOT_TOKEN = (token or "").strip()
    ADMIN_IDS = {int(a) for a in admin_ids} if admin_ids else set()
    _bot_username = None
    _rebuild_api_base()

def get_current_config() -> dict:
    """تنظیمات فعلی (چه از env و چه از پنل ست شده باشه) رو برمی‌گردونه."""
    return {"bot_token": BOT_TOKEN, "admin_ids": sorted(ADMIN_IDS)}

async def _ensure_username_cached():
    global _bot_username
    if _bot_username or not _running or not BOT_TOKEN:
        return
    data = await _call("getMe")
    if data and data.get("ok"):
        _bot_username = data["result"].get("username")

async def get_status() -> dict:
    """وضعیت اتصال ربات برای نمایش در پنل (متصل/قطع + یوزرنیم ربات)."""
    await _ensure_username_cached()
    return {"connected": bool(_running and BOT_TOKEN), "username": _bot_username}

# ── Admin manual-create wizard ────────────────────────────────────────────────
WIZARD_STEPS = ["label", "protocol", "fingerprint", "alpn", "port", "volume", "speed", "iplimit", "days"]

PROTOCOL_LABELS = {
    "vless-ws": "VLESS + WebSocket",
    "xhttp": "XHTTP (mode: auto)",
}

def _protocol_label(p: str) -> str:
    return PROTOCOL_LABELS.get(p, p)

def _fp_label(fp: str) -> str:
    return fp.capitalize()

_VOLUME_RE = re.compile(r"^([\d.]+)\s*(GB|MB|KB)?$", re.IGNORECASE)
_SPEED_RE = re.compile(r"^([\d.]+)\s*(MBIT|MBPS|MB|KB)?$", re.IGNORECASE)

def _parse_volume_text(text: str):
    m = _VOLUME_RE.match(text.strip())
    if not m:
        return None
    try:
        value = float(m.group(1))
    except ValueError:
        return None
    if value <= 0:
        return 0
    unit = (m.group(2) or "GB").upper()
    return parse_size_to_bytes(value, unit)

def _parse_speed_text(text: str):
    m = _SPEED_RE.match(text.strip())
    if not m:
        return None
    try:
        value = float(m.group(1))
    except ValueError:
        return None
    if value <= 0:
        return 0
    unit_raw = (m.group(2) or "MBIT").upper()
    unit = "MBIT" if unit_raw in ("MBIT", "MBPS") else unit_raw
    return parse_speed_to_bytes(value, unit)

def _parse_nonneg_int(text: str):
    try:
        n = int(text.strip())
    except ValueError:
        return None
    return max(0, n)

def _parse_positive_float(text: str):
    try:
        v = float(text.strip().replace(",", "."))
    except ValueError:
        return None
    if v <= 0:
        return None
    return v

# ── Telegram API helpers ────────────────────────────────────────────────────
async def _call(method: str, **params):
    if _client is None:
        return None
    try:
        r = await _client.post(f"{API_BASE}/{method}", json=params, timeout=40)
        data = r.json()
        if not data.get("ok"):
            logger.warning(f"Telegram API {method} failed: {data}")
        return data
    except Exception as e:
        logger.warning(f"Telegram API {method} error: {e}")
        return None

async def _send(chat_id: int, text: str, kb: dict | None = None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb:
        payload["reply_markup"] = kb
    return await _call("sendMessage", **payload)

async def _edit(chat_id: int, message_id: int, text: str, kb: dict | None = None):
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb:
        payload["reply_markup"] = kb
    res = await _call("editMessageText", **payload)
    if res is None or not res.get("ok"):
        await _send(chat_id, text, kb)

def _qr_png(data: str) -> bytes | None:
    """یه تصویر QR-Code از یه رشته (مثلاً لینک ساب) می‌سازه و بایت‌های PNG رو برمی‌گردونه.
    اگه کتابخونه‌ی qrcode روی سرور نصب نباشه، None برمی‌گردونه (بدون کرش کردن ربات)."""
    if not _QR_AVAILABLE:
        return None
    try:
        img = qrcode.make(data, box_size=9, border=3)
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except Exception as e:
        logger.warning(f"QR generation failed: {e}")
        return None

async def _send_photo_bytes(chat_id: int, photo_bytes: bytes, caption: str = "", kb: dict | None = None):
    """آپلود مستقیم یه عکس (بایت‌های PNG) به تلگرام، برای ارسال QR-Code کانفیگ."""
    if _client is None:
        return None
    data = {"chat_id": str(chat_id), "caption": caption, "parse_mode": "HTML"}
    if kb:
        data["reply_markup"] = json.dumps(kb)
    files = {"photo": ("config.png", photo_bytes, "image/png")}
    try:
        r = await _client.post(f"{API_BASE}/sendPhoto", data=data, files=files, timeout=40)
        res = r.json()
        if not res.get("ok"):
            logger.warning(f"Telegram API sendPhoto failed: {res}")
            await _send(chat_id, caption, kb)
        return res
    except Exception as e:
        logger.warning(f"Telegram API sendPhoto error: {e}")
        await _send(chat_id, caption, kb)
        return None

async def _answer_cb(cb_id: str, text: str = "", alert: bool = False):
    await _call("answerCallbackQuery", callback_query_id=cb_id, text=text, show_alert=alert)

async def _forward(to_chat_id: int, from_chat_id: int, message_id: int):
    return await _call("forwardMessage", chat_id=to_chat_id, from_chat_id=from_chat_id, message_id=message_id)

def _is_admin(chat_id: int) -> bool:
    return chat_id in ADMIN_IDS

async def _notify_admins(text: str, kb: dict | None = None):
    for aid in ADMIN_IDS:
        await _send(aid, text, kb)

# ── Keyboards: Home (چیدمان کارتی/شبکه‌ای — دوستونه) ─────────────────────────
def _join_ts(title: str, sub: str) -> str:
    """تایتل و زیرمتن رو به هم می‌چسبونه؛ اگه تایتل خالی باشه، فقط زیرمتن برمی‌گرده."""
    return f"{title}\n\n{sub}" if title else sub

def _home_view(chat_id: int, mode: str = "home"):
    """mode: 'first' (اولین /start عمری کاربر، متن کامل)، 'start' (استارت‌های بعدی، کوتاه)،
    'home' (بازگشت از یه زیرمنو با دکمه‌ی بازگشت، کوتاه‌ترین حالت، بدون تکرار بسم‌الله)."""
    if _is_admin(chat_id):
        title = _t(chat_id, "admin_home_title")
        sub = _t(chat_id, "admin_home_sub")
        n_pending = sum(1 for o in SHOP["orders"].values() if o["status"] == "pending")
        orders_label = _t(chat_id, "btn_admin_orders") + (f" ({n_pending})" if n_pending else "")
        n_unread = _support_unread_count()
        support_label = f"📨 پیام‌های پشتیبانی" + (f" ({n_unread} جدید)" if n_unread else "")
        kb = {"inline_keyboard": [
            [_btn(_t(chat_id, "btn_admin_manage"), "list:0", style="primary"),
             _btn(_t(chat_id, "btn_admin_new"), "newcfg", style="success")],
            [_btn(orders_label, "orders:0", style="danger" if n_pending else "primary"),
             _btn(_t(chat_id, "btn_admin_settings"), "settings", style="primary")],
            [_btn(_t(chat_id, "btn_check_usage"), "usage:start", style="primary"),
             _btn(_t(chat_id, "btn_admin_stats"), "stats:home", style="primary")],
            [_btn(support_label, "supportmsgs:0", style="danger" if n_unread else "primary"),
             _btn(f"👥 کاربران ({len(SHOP.get('known_users', []))})", "joins:0", style="primary")],
            [_btn("💎 مدیریت کیف پول کاربران", "adminwallet:start", style="success"),
             _btn("👑 افزودن ادمین", "admin:addstart", style="danger")],
            [_btn(_t(chat_id, "btn_admin_broadcast"), "bcast:start", style="primary"),
             _btn("✉️ پیام به یک کاربر", "dm:start", style="primary")],
            [_btn(_t(chat_id, "btn_refresh"), "home", style="success"),
             _btn(_t(chat_id, "btn_lang"), "lang:toggle", style="primary")],
        ]}
    else:
        title = _t(chat_id, "cust_home_title")
        if mode == "first":
            sub = _t(chat_id, "cust_home_sub")
        elif mode == "start":
            sub = _t(chat_id, "cust_home_sub_start")
        else:
            sub = _t(chat_id, "cust_home_sub_return")
        wallet_label = "💎 کیف پول" + f" ({_wallet_balance(chat_id):,}ت)"
        rows = [
            [_btn("⚡️ خرید اشتراک", "buy:start", style="success"),
             _btn("📡 کانال‌های ما", "channels:home", style="success")],
            [_btn("🧪 اکانت تست رایگان", "trial:claim", style="success"),
             _btn("🎰 گردونه شانس", "spin:go", style="success")],
            [_btn("📦 سرویس‌های من", "mine:0", style="danger"),
             _btn(wallet_label, "wallet:home", style="success")],
            [_btn("🤝 زیرمجموعه‌گیری", "ref:home", style="primary"),
             _btn("🎓 آموزش", "help", style="primary")],
            [_btn("🎧 پشتیبانی", "support:home", style="primary"),
             _btn("👑 درخواست نمایندگی", "reseller:request", style="success")],
        ]
        kb = {"inline_keyboard": rows}
    return title, sub, kb


def _links_list_kb(chat_id: int, page: int):
    items = sorted(LINKS.items(), key=lambda kv: kv[1].get("created_at", ""), reverse=True)
    total = len(items)
    start = page * PAGE_SIZE
    chunk = items[start:start + PAGE_SIZE]
    rows = []
    for uid, l in chunk:
        dot = "🟢" if is_link_allowed(l) else "🔴"
        src = {"shop": "🛒", "trial": "🎁"}.get(l.get("source"), "")
        rows.append([{"text": f"{dot}{src} {l.get('label','?')[:26]}", "callback_data": f"view:{uid}"}])
    nav = []
    if start > 0:
        nav.append({"text": _t(chat_id, "btn_prev"), "callback_data": f"list:{page-1}"})
    if start + PAGE_SIZE < total:
        nav.append({"text": _t(chat_id, "btn_next"), "callback_data": f"list:{page+1}"})
    if nav:
        rows.append(nav)
    rows.append([{"text": _t(chat_id, "btn_admin_new"), "callback_data": "newcfg"}])
    rows.append([{"text": _t(chat_id, "btn_back_home"), "callback_data": "home"}])
    return {"inline_keyboard": rows}

def _mine_list_kb(chat_id: int, page: int):
    items = sorted(
        [(uid, l) for uid, l in LINKS.items() if l.get("owner_chat_id") == chat_id],
        key=lambda kv: kv[1].get("created_at", ""), reverse=True,
    )
    total = len(items)
    start = page * PAGE_SIZE
    chunk = items[start:start + PAGE_SIZE]
    rows = []
    for uid, l in chunk:
        dot = "🟢" if is_link_allowed(l) else "🔴"
        rows.append([{"text": f"{dot} {l.get('label','?')[:28]}", "callback_data": f"view:{uid}"}])
    nav = []
    if start > 0:
        nav.append({"text": _t(chat_id, "btn_prev"), "callback_data": f"mine:{page-1}"})
    if start + PAGE_SIZE < total:
        nav.append({"text": _t(chat_id, "btn_next"), "callback_data": f"mine:{page+1}"})
    if nav:
        rows.append(nav)
    rows.append([{"text": _t(chat_id, "btn_buy"), "callback_data": "buy:start"}])
    rows.append([{"text": _t(chat_id, "btn_back_home"), "callback_data": "home"}])
    return {"inline_keyboard": rows}, total

def _link_detail_kb(chat_id: int, uid: str, active: bool):
    rows = [[{"text": _t(chat_id, "btn_show_link"), "callback_data": f"link:{uid}"}]]
    rows.append([{"text": "📲 دانلود v2Box", "url": V2BOX_URL}])
    rows.append([{"text": _t(chat_id, "btn_renew"), "callback_data": f"renew:start:{uid}"}])
    if _is_admin(chat_id):
        rows.append([{"text": (_t(chat_id, "btn_disable") if active else _t(chat_id, "btn_enable")), "callback_data": f"toggle:{uid}"}])
        rows.append([{"text": _t(chat_id, "btn_delete"), "callback_data": f"del:{uid}"}])
        rows.append([{"text": _t(chat_id, "btn_back_list"), "callback_data": "list:0"}])
    else:
        rows.append([{"text": _t(chat_id, "btn_back_list"), "callback_data": "mine:0"}])
    return {"inline_keyboard": rows}

def _confirm_delete_kb(chat_id: int, uid: str):
    return {"inline_keyboard": [
        [{"text": _t(chat_id, "btn_confirm_delete"), "callback_data": f"delok:{uid}"},
         {"text": _t(chat_id, "btn_cancel"), "callback_data": f"view:{uid}"}],
    ]}

# ── Wizard keyboards (admin manual create) ───────────────────────────────────
def _wizard_cancel_kb(chat_id: int):
    return {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "w:cancel"}]]}

def _wizard_protocol_kb(chat_id: int):
    rows = [[{"text": _protocol_label(p), "callback_data": f"w:proto:{p}"}] for p in PROTOCOLS]
    rows.append([{"text": _t(chat_id, "btn_cancel"), "callback_data": "w:cancel"}])
    return {"inline_keyboard": rows}

def _wizard_fp_kb(chat_id: int):
    rows, row = [], []
    for fp in FINGERPRINTS:
        row.append({"text": _fp_label(fp), "callback_data": f"w:fp:{fp}"})
        if len(row) == 3:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([{"text": _t(chat_id, "btn_cancel"), "callback_data": "w:cancel"}])
    return {"inline_keyboard": rows}

def _wizard_skip_kb(chat_id: int, step_key: str, label: str):
    return {"inline_keyboard": [
        [{"text": label, "callback_data": f"w:skip:{step_key}"}],
        [{"text": _t(chat_id, "btn_cancel"), "callback_data": "w:cancel"}],
    ]}

ALPN_PRESET_MAP = {"p1": "http/1.1", "p2": "h2,http/1.1", "p3": "h2"}

def _wizard_alpn_kb(chat_id: int):
    return {"inline_keyboard": [
        [{"text": "🔤 http/1.1 (پیشنهادی)", "callback_data": "w:alpnpreset:p1"}],
        [{"text": "🔤 h2,http/1.1", "callback_data": "w:alpnpreset:p2"}],
        [{"text": "🔤 h2", "callback_data": "w:alpnpreset:p3"}],
        [{"text": "⏭ پیش‌فرض پروتکل", "callback_data": "w:skip:alpn"}],
        [{"text": _t(chat_id, "btn_cancel"), "callback_data": "w:cancel"}],
    ]}

def _wizard_unlimited_kb(chat_id: int, step_key: str):
    return _wizard_skip_kb(chat_id, step_key, "♾ نامحدود")

def _wizard_confirm_kb(chat_id: int):
    return {"inline_keyboard": [
        [{"text": _t(chat_id, "btn_make_config"), "callback_data": "w:confirm"}],
        [{"text": _t(chat_id, "btn_cancel"), "callback_data": "w:cancel"}],
    ]}

def _wizard_prompt(step: str, data: dict) -> str:
    n = WIZARD_STEPS.index(step) + 1 if step in WIZARD_STEPS else len(WIZARD_STEPS)
    head = f"💠 ساخت دستی کانفیگ — مرحله {n}/{len(WIZARD_STEPS)}\n\n"
    if step == "label":
        return head + "✏️ اسم/برچسب کانفیگ رو بفرست:"
    if step == "protocol":
        return head + "🌐 پروتکل رو از دکمه‌های زیر انتخاب کن:"
    if step == "fingerprint":
        return head + "🖐 Fingerprint (uTLS) رو انتخاب کن:"
    if step == "alpn":
        return head + ("🔤 ALPN رو از دکمه‌های زیر انتخاب کن (پیشنهادی: <code>http/1.1</code>)\n"
                        "یا خودت هر مقدار دلخواهی رو تایپ و ارسال کن (مثلاً h2,http/1.1):")
    if step == "port":
        return head + f"🔌 شماره پورت (بین {MIN_PORT} تا {MAX_PORT}) رو بفرست\nیا پیش‌فرض ({DEFAULT_PORT}) رو انتخاب کن:"
    if step == "volume":
        return head + "📦 محدودیت حجم مصرفی رو بفرست، مثلاً:\n<code>10GB</code> یا <code>500MB</code>\nیا دکمه‌ی نامحدود رو بزن:"
    if step == "speed":
        return head + "🚀 محدودیت سرعت رو به مگابیت‌بر‌ثانیه بفرست، مثلاً <code>20</code>\nیا دکمه‌ی نامحدود رو بزن:"
    if step == "iplimit":
        return head + "👥 حداکثر تعداد آی‌پی/کاربر هم‌زمان مجاز رو بفرست\nیا دکمه‌ی نامحدود رو بزن:"
    if step == "days":
        return head + "📅 تعداد روزهای اعتبار کانفیگ رو بفرست\nیا دکمه‌ی نامحدود (بدون انقضا) رو بزن:"
    return head

def _wizard_summary(data: dict) -> str:
    limit = "نامحدود" if not data.get("limit_bytes") else fmt_bytes(data["limit_bytes"])
    speed = "نامحدود" if not data.get("speed_limit_bytes") else f"{data['speed_limit_bytes']*8/1024/1024:.1f} Mbps"
    iplim = data.get("ip_limit", 0) or "نامحدود"
    days = data.get("expires_days", 0)
    days_txt = "بدون انقضا" if not days else f"{days} روز"
    proto = data.get("protocol", DEFAULT_PROTOCOL)
    alpn = data.get("alpn") or f"پیش‌فرض ({DEFAULT_ALPN_BY_PROTOCOL.get(proto, 'http/1.1')})"
    return (
        "💠 خلاصه‌ی کانفیگ جدید — تایید کن:\n\n"
        f"برچسب: <b>{data.get('label','?')}</b>\n"
        f"پروتکل: {_protocol_label(proto)}\n"
        f"Fingerprint: {_fp_label(data.get('fingerprint', DEFAULT_FINGERPRINT))}\n"
        f"ALPN: {alpn}\n"
        f"پورت: {data.get('port', DEFAULT_PORT)}\n"
        f"محدودیت حجم: {limit}\n"
        f"محدودیت سرعت: {speed}\n"
        f"محدودیت آی‌پی: {iplim}\n"
        f"انقضا: {days_txt}"
    )

# ── View builders ────────────────────────────────────────────────────────────
def _format_detail(uid: str, l: dict) -> str:
    status = "🟢 فعال" if is_link_allowed(l) else "🔴 غیرفعال/منقضی"
    limit = "نامحدود" if not l.get("limit_bytes") else fmt_bytes(l["limit_bytes"])
    speed = "نامحدود" if not l.get("speed_limit_bytes") else f"{l['speed_limit_bytes']*8/1024/1024:.1f} Mbps"
    exp = l.get("expires_at")
    exp_txt = exp.split("T")[0] if exp else "بدون انقضا"
    proto = l.get("protocol", DEFAULT_PROTOCOL)
    alpn = l.get("alpn") or f"پیش‌فرض ({DEFAULT_ALPN_BY_PROTOCOL.get(proto, 'http/1.1')})"
    src = {"shop": "🛒 خریداری‌شده", "trial": "🎁 تست رایگان", "admin": "💠 ساخت دستی"}.get(l.get("source"), "")
    src_line = f"منبع: {src}\n" if src else ""
    return (
        f"<b>{l.get('label','?')}</b>\n"
        f"وضعیت: {status}\n"
        f"{src_line}"
        f"مصرف: {fmt_bytes(l.get('used_bytes',0))} / {limit}\n"
        f"محدودیت سرعت: {speed}\n"
        f"محدودیت آی‌پی: {l.get('ip_limit',0) or 'نامحدود'}\n"
        f"پروتکل: {_protocol_label(proto)}\n"
        f"Fingerprint: {_fp_label(l.get('fingerprint', DEFAULT_FINGERPRINT))}\n"
        f"ALPN: {alpn}\n"
        f"پورت: {l.get('port', DEFAULT_PORT)}\n"
        f"انقضا: {exp_txt}\n"
        f"UUID: <code>{uid}</code>"
    )

def _format_detail_simple(l: dict, vless: str = "") -> str:
    """نمای ساده‌ی کانفیگ برای مشتری — بدون UUID/پورت/پروتکل، فقط اطلاعات ضروری + متن vless قابل کپی."""
    status = "🟢 فعال" if is_link_allowed(l) else "🔴 غیرفعال/منقضی"
    limit = "نامحدود" if not l.get("limit_bytes") else fmt_bytes(l["limit_bytes"])
    exp = l.get("expires_at")
    exp_txt = exp.split("T")[0] if exp else "بدون انقضا"
    txt = (
        f"<b>{l.get('label','?')}</b>\n"
        f"وضعیت: {status}\n"
        f"مصرف: {fmt_bytes(l.get('used_bytes',0))} / {limit}\n"
        f"انقضا: {exp_txt}"
    )
    if vless:
        txt += (
            "\n\n📎 متن اتصال (روی متن بزن تا کپی بشه، بعد توی v2Box وارد کن):\n"
            f"<code>{vless}</code>\n\n"
            "یا کد QR بالا رو مستقیم با v2Box اسکن کن 📱🙏"
        )
    return txt

def _link_detail_kb_customer(chat_id: int, uid: str):
    return {"inline_keyboard": [
        [_btn("📲 دانلود v2Box", url=V2BOX_URL, style="success")],
        [_btn(_t(chat_id, "btn_renew"), f"renew:start:{uid}", style="primary")],
        [_btn(_t(chat_id, "btn_back_list"), "mine:0", style="primary")],
    ]}

async def _countdown_edit(chat_id: int, message_id: int, base: str):
    """یه افکت شمارش سریع ۱ تا ۳ روی پیام انیمیشن (همزمان با آماده شدن QR در پس‌زمینه)."""
    for i in range(1, 4):
        await _edit(chat_id, message_id, f"{base}\n\n⏳ {i}{'.'*i}")
        await asyncio.sleep(0.32)

async def _deliver_config(chat_id: int, message_id: int | None, uid: str, l: dict, prefix: str = ""):
    """کانفیگ رو برای مشتری، با یه افکت شمارش ۱-۲-۳ کوتاه، QR-Code از خودِ vless (نه لینک ساب)
    + متن vless قابل کپی و دکمه‌های رنگی ارسال می‌کنه. شمارش و آماده‌سازی QR همزمان انجام می‌شن
    تا معطلی نداشته باشه."""
    host = get_host()
    vless = vless_link_for_link(l, uid, host)
    caption = (f"{prefix}\n\n" if prefix else "") + _format_detail_simple(l, vless)
    kb = _link_detail_kb_customer(chat_id, uid)
    photo = _qr_png(vless)
    send_coro = _send_photo_bytes(chat_id, photo, caption, kb) if photo else _send(chat_id, caption, kb)
    if message_id:
        await asyncio.gather(send_coro, _countdown_edit(chat_id, message_id, prefix or "✅ کانفیگت آماده شد!"))
    else:
        await send_coro

_UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")

def _extract_uid_from_text(text: str) -> str | None:
    """از متن ارسالی کاربر (لینک vless، لینک ساب/صفحه‌ی اختصاصی یا خودِ UUID) شناسه‌ی کانفیگ رو پیدا می‌کنه."""
    if not text:
        return None
    m = _UUID_RE.search(text)
    if m and m.group(0) in LINKS:
        return m.group(0)
    for uid in LINKS:
        if uid in text:
            return uid
    return None

def _usage_report(uid: str, l: dict) -> str:
    """گزارش کوتاه و خوانا از حجم مصرفی یک کانفیگ — برای قابلیت «اعلام حجم»."""
    limit = l.get("limit_bytes", 0)
    used = l.get("used_bytes", 0)
    limit_txt = "♾ نامحدود" if not limit else fmt_bytes(limit)
    remain_txt = "♾ نامحدود" if not limit else fmt_bytes(max(limit - used, 0))
    pct_txt = f" ({min(used/limit*100, 100):.1f}٪ مصرف‌شده)" if limit else ""
    status = "🟢 فعال" if is_link_allowed(l) else "🔴 غیرفعال/منقضی"
    exp = l.get("expires_at")
    exp_txt = exp.split("T")[0] if exp else "بدون انقضا"
    return (
        f"📊 <b>گزارش حجم — Matix</b>\n\n"
        f"🏷 برچسب: <b>{l.get('label','?')}</b>\n"
        f"وضعیت: {status}\n\n"
        f"🔹 مصرف‌شده: <b>{fmt_bytes(used)}</b>\n"
        f"🔹 باقی‌مانده: <b>{remain_txt}</b>\n"
        f"🔹 سقف مجاز: {limit_txt}{pct_txt}\n"
        f"📅 انقضا: {exp_txt}"
    )

def _buy_summary_text(data: dict) -> str:
    code = data.get("discount_code")
    lines = [
        "🧊 خلاصه‌ی سفارش:\n",
        f"حجم: <b>{data['volume_gb']} GB</b>",
        f"مدت: <b>{data['days']} روز</b>",
    ]
    if code:
        base = _price_for(data["volume_gb"], data["days"])
        lines.append(f"کد تخفیف: <b>{code}</b> (قیمت پایه: {base:,} تومان)")
    lines.append(f"مبلغ قابل پرداخت: <b>{data['price']:,} تومان</b>")
    lines.append("\nبرای ادامه، تایید کن تا اطلاعات پرداخت رو ببینی.")
    return "\n".join(lines)

def _buyer_line(chat_id: int, username: str | None) -> str:
    """یک خط شناسایی مشتری با نام/یوزرنیم، آیدی عددی، و لینک مستقیم به پروفایلش برای ادمین."""
    looks_like_username = bool(username) and " " not in username and username.replace("_", "").isalnum()
    display = f"@{username}" if looks_like_username else (username or str(chat_id))
    return f"👤 {display} — <code>{chat_id}</code> — <a href=\"tg://user?id={chat_id}\">مشاهده پروفایل</a>"

def _mask_identity(chat_id: int, username: str | None) -> str:
    """هویت خریدار رو نصفه‌پنهون می‌کنه (برای گزارش‌های عمومی کانال) — نیمی از آیدی/یوزرنیم معلومه، بقیه ستاره."""
    looks_like_username = bool(username) and " " not in username and username.replace("_", "").isalnum()
    if looks_like_username:
        keep = max(1, len(username) // 2)
        return "@" + username[:keep] + "•" * max(1, len(username) - keep)
    s = str(chat_id)
    keep = max(2, len(s) // 2)
    return s[:keep] + "•" * (len(s) - keep)

def _btn(text: str, cb: str | None = None, url: str | None = None, style: str | None = None) -> dict:
    """یه دکمه‌ی این‌لاین می‌سازه؛ style یکی از success/primary/destructive (Bot API 9.4+) برای دکمه‌ی رنگی."""
    b: dict = {"text": text}
    if cb is not None:
        b["callback_data"] = cb
    if url is not None:
        b["url"] = url
    if style:
        b["style"] = style
    return b

def _order_summary(order: dict) -> str:
    code_line = f"کد تخفیف: {order.get('discount_code')}\n" if order.get("discount_code") else ""
    return (
        f"🧾 سفارش #{order['id']}\n"
        f"مشتری: {_buyer_line(order['chat_id'], order.get('username'))}\n"
        f"حجم: {order['volume_gb']} GB\n"
        f"مدت: {order['days']} روز\n"
        f"{code_line}"
        f"مبلغ: {order['price']:,} تومان\n"
        f"وضعیت: {order['status']}"
    )

# ── Buy wizard keyboards ──────────────────────────────────────────────────────
def _buy_cancel_kb(chat_id: int):
    return {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "buy:cancel"}]]}

def _buy_confirm_kb(chat_id: int):
    return {"inline_keyboard": [
        [{"text": _t(chat_id, "btn_discount_apply"), "callback_data": "buy:discount"}],
        [{"text": "✅ تایید و مشاهده‌ی مبلغ", "callback_data": "buy:confirm"}],
        [{"text": _t(chat_id, "btn_cancel"), "callback_data": "buy:cancel"}],
    ]}

def _order_admin_kb(order_id: str):
    return {"inline_keyboard": [
        [{"text": "✅ تایید و ارسال خودکار", "callback_data": f"ord:appr:{order_id}"},
         {"text": "❌ رد سفارش", "callback_data": f"ord:rej:{order_id}"}],
    ]}

def _orders_list_kb(chat_id: int, page: int):
    pending = sorted(
        [o for o in SHOP["orders"].values() if o["status"] == "pending"],
        key=lambda o: o["created_at"], reverse=True,
    )
    total = len(pending)
    start = page * PAGE_SIZE
    chunk = pending[start:start + PAGE_SIZE]
    rows = [[{"text": f"🧾 #{o['id']} — {o['volume_gb']}GB/{o['days']}d — {o['price']:,}ت", "callback_data": f"ordview:{o['id']}"}] for o in chunk]
    nav = []
    if start > 0:
        nav.append({"text": _t(chat_id, "btn_prev"), "callback_data": f"orders:{page-1}"})
    if start + PAGE_SIZE < total:
        nav.append({"text": _t(chat_id, "btn_next"), "callback_data": f"orders:{page+1}"})
    if nav:
        rows.append(nav)
    rows.append([{"text": _t(chat_id, "btn_back_home"), "callback_data": "home"}])
    return {"inline_keyboard": rows}, total

def _settings_kb(chat_id: int):
    return {"inline_keyboard": [
        [{"text": f"💰 هر گیگ: {SHOP['price_per_gb']:,}ت", "callback_data": "set:price_per_gb"},
         {"text": f"📅 هر روز: {SHOP['price_per_day']:,}ت", "callback_data": "set:price_per_day"}],
        [{"text": f"🔻 حداقل مبلغ: {SHOP['min_price']:,}ت", "callback_data": "set:min_price"},
         {"text": f"🎁 حجم تست: {SHOP['trial_volume_gb']}GB", "callback_data": "set:trial_volume_gb"}],
        [{"text": f"🎁 مدت تست: {SHOP['trial_days']} روز", "callback_data": "set:trial_days"},
         {"text": f"💳 کارت: {SHOP['card_number'] or '—'}", "callback_data": "set:card_number"}],
        [{"text": f"👤 صاحب کارت: {SHOP['card_owner'] or '—'}", "callback_data": "set:card_owner"}],
        [{"text": f"📣 کانال اعلان خرید: {SHOP.get('announce_channel') or '—'}", "callback_data": "set:announce_channel"}],
        [{"text": f"🔒 کانال اجباری: {SHOP.get('required_channel') or '—'}", "callback_data": "set:required_channel"},
         {"text": f"🔗 لینک عضویت", "callback_data": "set:required_channel_url"}],
        [{"text": f"🤝 پاداش رفرال: {SHOP.get('referral_bonus', 0):,}ت", "callback_data": "set:referral_bonus"}],
        [{"text": f"☎️ یوزرنیم پشتیبانی: {SHOP.get('support_username') or '—'}", "callback_data": "set:support_username"}],
        [{"text": f"📢 کانال‌های ما ({len(SHOP.get('channels', []))} مورد)", "callback_data": "set:channels"}],
        [{"text": f"🎲 جایزه گردونه: {SHOP.get('spin_min', 0):,}–{SHOP.get('spin_max', 0):,}ت", "callback_data": "set:spin_range"}],
        [{"text": f"💰 مبالغ پیشنهادی شارژ کیف پول: {', '.join(f'{int(a):,}' for a in SHOP.get('wallet_topup_presets', []))}", "callback_data": "set:wallet_topup_presets"}],
        [{"text": _t(chat_id, "btn_admin_discounts"), "callback_data": "discounts:0"},
         {"text": _t(chat_id, "btn_admin_gifts"), "callback_data": "giftcodes:0"}],
        [{"text": _t(chat_id, "btn_back_home"), "callback_data": "home"}],
    ]}

# ── کدهای تخفیف: کیبوردها و متن‌ها ────────────────────────────────────────
def _discounts_list_kb(chat_id: int, page: int):
    codes = sorted(SHOP.get("discount_codes", {}).items())
    total = len(codes)
    start = page * PAGE_SIZE
    chunk = codes[start:start + PAGE_SIZE]
    rows = []
    for code, d in chunk:
        dot = "🟢" if _discount_valid(d) else "🔴"
        rows.append([{"text": f"{dot} {code} — {_discount_desc(d)}", "callback_data": f"disc:view:{code}"}])
    nav = []
    if start > 0:
        nav.append({"text": _t(chat_id, "btn_prev"), "callback_data": f"discounts:{page-1}"})
    if start + PAGE_SIZE < total:
        nav.append({"text": _t(chat_id, "btn_next"), "callback_data": f"discounts:{page+1}"})
    if nav:
        rows.append(nav)
    rows.append([{"text": _t(chat_id, "btn_discount_new"), "callback_data": "disc:new"}])
    rows.append([{"text": _t(chat_id, "btn_back_home"), "callback_data": "settings"}])
    return {"inline_keyboard": rows}, total

# ── کدهای هدیه: کیبوردها و متن‌ها ──────────────────────────────────────────
def _giftcodes_list_kb(chat_id: int, page: int):
    codes = sorted(SHOP.get("gift_codes", {}).items())
    total = len(codes)
    start = page * PAGE_SIZE
    chunk = codes[start:start + PAGE_SIZE]
    rows = []
    for code, d in chunk:
        dot = "🟢" if d.get("active", True) else "🔴"
        rows.append([{"text": f"{dot} {code} — {_gift_desc(d)}", "callback_data": f"gift:view:{code}"}])
    nav = []
    if start > 0:
        nav.append({"text": _t(chat_id, "btn_prev"), "callback_data": f"giftcodes:{page-1}"})
    if start + PAGE_SIZE < total:
        nav.append({"text": _t(chat_id, "btn_next"), "callback_data": f"giftcodes:{page+1}"})
    if nav:
        rows.append(nav)
    rows.append([{"text": "➕ کد هدیه جدید", "callback_data": "gift:new"}])
    rows.append([{"text": _t(chat_id, "btn_back_home"), "callback_data": "settings"}])
    return {"inline_keyboard": rows}, total

def _giftcode_detail_kb(chat_id: int, code: str, active: bool):
    return {"inline_keyboard": [
        [{"text": ("⛔ غیرفعال‌سازی" if active else "✅ فعال‌سازی"), "callback_data": f"gift:toggle:{code}"}],
        [{"text": "🗑 حذف کد", "callback_data": f"gift:del:{code}"}],
        [{"text": "⬅ بازگشت به لیست", "callback_data": "giftcodes:0"}],
    ]}

def _giftcode_detail_text(code: str, d: dict) -> str:
    status = "🟢 فعال" if d.get("active", True) else "🔴 غیرفعال"
    max_uses = d.get("max_uses", 0) or "نامحدود"
    return (
        f"🎁 <b>{code}</b>\n"
        f"وضعیت: {status}\n"
        f"مبلغ: <b>{int(d.get('amount', 0)):,} تومان</b>\n"
        f"دفعات استفاده: {d.get('used_count', 0)} / {max_uses}"
    )

def _giftcode_unlimited_kb(chat_id: int):
    return {"inline_keyboard": [
        [{"text": "♾ نامحدود", "callback_data": "gift:skip:maxuses"}],
        [{"text": _t(chat_id, "btn_cancel"), "callback_data": "gift:cancel"}],
    ]}

def _giftcode_confirm_kb(chat_id: int):
    return {"inline_keyboard": [
        [{"text": "✅ ساخت کد هدیه", "callback_data": "gift:confirm"}],
        [{"text": _t(chat_id, "btn_cancel"), "callback_data": "gift:cancel"}],
    ]}

def _giftcode_summary(data: dict) -> str:
    max_uses = data.get("max_uses", 0) or "نامحدود"
    return (
        "🎁 خلاصه‌ی کد هدیه — تایید کن:\n\n"
        f"کد: <b>{data.get('code')}</b>\n"
        f"مبلغ: <b>{int(data.get('amount', 0)):,} تومان</b>\n"
        f"حداکثر تعداد استفاده: {max_uses}"
    )

# ── صندوق پیام‌های پشتیبانی (ادمین) ──────────────────────────────────────────
def _support_unread_count() -> int:
    return sum(1 for m in SHOP.get("support_messages", []) if not m.get("read"))

def _support_list_kb(chat_id: int, page: int):
    msgs = sorted(SHOP.get("support_messages", []), key=lambda m: m["id"], reverse=True)
    total = len(msgs)
    start = page * PAGE_SIZE
    chunk = msgs[start:start + PAGE_SIZE]
    rows = []
    for m in chunk:
        dot = "🆕" if not m.get("read") else "✅"
        uname = f"@{m['username']}" if m.get("username") and " " not in m["username"] else (m.get("username") or str(m["chat_id"]))
        snippet = (m.get("text") or "")[:24]
        rows.append([{"text": f"{dot} #{m['id']} {uname} — {snippet}", "callback_data": f"supportmsg:view:{m['id']}"}])
    nav = []
    if start > 0:
        nav.append({"text": _t(chat_id, "btn_prev"), "callback_data": f"supportmsgs:{page-1}"})
    if start + PAGE_SIZE < total:
        nav.append({"text": _t(chat_id, "btn_next"), "callback_data": f"supportmsgs:{page+1}"})
    if nav:
        rows.append(nav)
    rows.append([{"text": _t(chat_id, "btn_back_home"), "callback_data": "home"}])
    return {"inline_keyboard": rows}, total

def _support_detail_kb(chat_id: int, sid: int):
    return {"inline_keyboard": [
        [_btn("↩️ پاسخ به این تیکت", f"supportmsg:reply:{sid}", style="success")],
        [_btn("⬅ بازگشت به لیست", "supportmsgs:0", style="primary")],
    ]}

def _support_detail_text(m: dict) -> str:
    uname = f"@{m['username']}" if m.get("username") and " " not in m["username"] else (m.get("username") or "—")
    when = (m.get("at") or "")[:16].replace("T", " ")
    lines = [
        f"📨 <b>تیکت #{m['id']}</b>\n",
        f"👤 {uname} — <code>{m['chat_id']}</code>",
        f"🕒 {when}\n",
        f"💬 {m.get('text','')}",
    ]
    for r in m.get("replies", []):
        who = "👤 مشتری" if r.get("from") == "user" else "☎️ پشتیبانی"
        rwhen = (r.get("at") or "")[:16].replace("T", " ")
        lines.append(f"\n— {who} ({rwhen}):\n{r.get('text','')}")
    return "\n".join(lines)


def _discount_detail_kb(chat_id: int, code: str, active: bool):
    return {"inline_keyboard": [
        [{"text": ("⛔ غیرفعال‌سازی" if active else "✅ فعال‌سازی"), "callback_data": f"disc:toggle:{code}"}],
        [{"text": "🗑 حذف کد", "callback_data": f"disc:del:{code}"}],
        [{"text": "⬅ بازگشت به لیست", "callback_data": "discounts:0"}],
    ]}

def _discount_detail_text(code: str, d: dict) -> str:
    status = "🟢 فعال" if _discount_valid(d) else "🔴 غیرفعال/غیرمعتبر"
    max_uses = d.get("max_uses", 0) or "نامحدود"
    return (
        f"🏷 <b>{code}</b>\n"
        f"وضعیت: {status}\n"
        f"مقدار: {_discount_desc(d)}\n"
        f"دفعات استفاده: {d.get('used_count', 0)} / {max_uses}"
    )

def _discount_type_kb(chat_id: int):
    return {"inline_keyboard": [
        [{"text": "٪ درصدی", "callback_data": "disc:type:percent"},
         {"text": "💵 مبلغ ثابت", "callback_data": "disc:type:fixed"}],
        [{"text": _t(chat_id, "btn_cancel"), "callback_data": "disc:cancel"}],
    ]}

def _discount_unlimited_kb(chat_id: int):
    return {"inline_keyboard": [
        [{"text": "♾ نامحدود", "callback_data": "disc:skip:maxuses"}],
        [{"text": _t(chat_id, "btn_cancel"), "callback_data": "disc:cancel"}],
    ]}

def _discount_confirm_kb(chat_id: int):
    return {"inline_keyboard": [
        [{"text": "✅ ساخت کد تخفیف", "callback_data": "disc:confirm"}],
        [{"text": _t(chat_id, "btn_cancel"), "callback_data": "disc:cancel"}],
    ]}

def _discount_summary(data: dict) -> str:
    unit = "٪" if data.get("type") == "percent" else "تومان"
    max_uses = data.get("max_uses", 0) or "نامحدود"
    return (
        "🏷 خلاصه‌ی کد تخفیف — تایید کن:\n\n"
        f"کد: <b>{data.get('code')}</b>\n"
        f"نوع: {'درصدی' if data.get('type') == 'percent' else 'مبلغ ثابت'}\n"
        f"مقدار: {data.get('value')} {unit}\n"
        f"حداکثر تعداد استفاده: {max_uses}"
    )

# ── Update handling ──────────────────────────────────────────────────────────
async def _handle_message(msg: dict):
    chat_id = msg.get("chat", {}).get("id")
    text = (msg.get("text") or "").strip()
    photo = msg.get("photo")
    username = msg.get("from", {}).get("username") or msg.get("from", {}).get("first_name", "")
    if chat_id is None:
        return

    known = SHOP.setdefault("known_users", [])
    is_new_user = chat_id not in known
    if is_new_user:
        known.append(chat_id)
        joins = SHOP.setdefault("join_log", [])
        joins.append({"chat_id": chat_id, "username": username, "at": datetime.now().isoformat()})
        if len(joins) > 500:
            del joins[:-500]
        await _save_shop()
        who = f"@{username}" if username and " " not in username else (username or str(chat_id))
        log_activity("telegram", f"👤 کاربر جدید وارد ربات شد: {who} ({chat_id}) — مجموع کاربران: {len(known)}", "ok")

    if text.startswith("/start"):
        _pending.pop(chat_id, None)
        await _ensure_username_cached()
        parts = text.split(maxsplit=1)
        payload = parts[1].strip() if len(parts) > 1 else ""
        if payload.startswith("ref_"):
            ref_id_txt = payload[4:]
            if ref_id_txt.lstrip("-").isdigit():
                ref_id = int(ref_id_txt)
                is_new_referral = str(chat_id) not in SHOP.get("referrals", {}) and ref_id != chat_id
                _register_referral(chat_id, ref_id)
                await _save_shop()
                if is_new_referral:
                    who = f"@{username}" if username else str(chat_id)
                    await _send(ref_id, f"👥 یکی از دوستات ({who}) با لینک دعوت تو وارد Matix شد!\nبعد از اولین خریدش، پاداش رفرال به کیف پولت اضافه می‌شه 🎁")
            payload = ""
        if not await _passes_membership_gate(chat_id):
            await _send(chat_id, "🔒 برای استفاده از ربات، اول باید عضو کانال ما بشی:", _join_gate_kb(chat_id))
            return
        await _send_sticker(chat_id, "welcome")
        if payload == "buy":
            _pending[chat_id] = {"action": "buy", "step": "volume", "data": {}}
            await _send(chat_id, "📦 چند گیگابایت حجم می‌خوای؟ فقط عدد رو بفرست، مثلاً <code>20</code>:", _buy_cancel_kb(chat_id))
            return
        await _play_entry_animation(chat_id, is_new=is_new_user)
        return

    if not await _passes_membership_gate(chat_id):
        await _send(chat_id, "🔒 برای استفاده از ربات، اول باید عضو کانال ما بشی:", _join_gate_kb(chat_id))
        return

    if text == "/menu":
        _pending.pop(chat_id, None)
        await _ensure_username_cached()
        await _send_sticker(chat_id, "welcome")
        await _play_entry_animation(chat_id, is_new=False)
        return

    if text == "/cancel":
        _pending.pop(chat_id, None)
        await _send(chat_id, _t(chat_id, "cancelled"))
        title, sub, kb = _home_view(chat_id)
        await _send(chat_id, _join_ts(title, sub), kb)
        return

    pending = _pending.get(chat_id)

    # ── مراحل استعلام حجم (دکمه‌ی جدا) ──────────────────────────────────────
    if pending and pending.get("action") == "usage_check":
        cancel_kb = {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "usage:cancel"}]]}
        uid = _extract_uid_from_text(text)
        l = LINKS.get(uid) if uid else None
        if not l:
            await _send(chat_id, _t(chat_id, "usage_not_found"), cancel_kb)
            return
        if not _is_admin(chat_id) and l.get("owner_chat_id") != chat_id:
            await _send(chat_id, "⛔ این کانفیگ متعلق به شما نیست.", cancel_kb)
            return
        _pending.pop(chat_id, None)
        kb = {"inline_keyboard": [
            [{"text": _t(chat_id, "btn_show_link"), "callback_data": f"link:{uid}"}],
            [{"text": _t(chat_id, "btn_back_home"), "callback_data": "home"}],
        ]}
        await _send(chat_id, _usage_report(uid, l), kb)
        return

    # ── مراحل خرید (مشتری) ──────────────────────────────────────────────────
    if pending and pending.get("action") == "buy":
        step = pending["step"]
        data = pending["data"]

        if step == "volume" and text:
            v = _parse_positive_float(text)
            if v is None:
                await _send(chat_id, "❗️ یه عدد معتبر بفرست، مثلاً <code>20</code> (گیگابایت):", _buy_cancel_kb(chat_id))
                return
            data["volume_gb"] = v
            pending["step"] = "days"
            await _send(chat_id, "📅 حالا مدت اعتبار (روز) رو بفرست، مثلاً <code>30</code>:", _buy_cancel_kb(chat_id))
            return

        if step == "days" and text:
            n = _parse_nonneg_int(text)
            if n is None or n <= 0:
                await _send(chat_id, "❗️ یه عدد صحیح و مثبت بفرست (تعداد روز):", _buy_cancel_kb(chat_id))
                return
            data["days"] = n
            data["price"] = _price_for(data["volume_gb"], data["days"])
            data.pop("discount_code", None)
            pending["step"] = "confirm"
            await _send(chat_id, _buy_summary_text(data), _buy_confirm_kb(chat_id))
            return

        if step == "discount" and text:
            d = _find_discount(text)
            if not _discount_valid(d):
                await _send(chat_id, "❗️ این کد تخفیف معتبر نیست یا منقضی شده. یه کد دیگه بفرست یا لغو کن:", _buy_cancel_kb(chat_id))
                return
            base_price = _price_for(data["volume_gb"], data["days"])
            data["price"] = _apply_discount(base_price, d)
            data["discount_code"] = text.strip().upper()
            pending["step"] = "confirm"
            await _send(chat_id, "✅ کد تخفیف اعمال شد!\n\n" + _buy_summary_text(data), _buy_confirm_kb(chat_id))
            return

        if step == "receipt":
            if photo or text:
                data["receipt_message_id"] = msg.get("message_id")
                data["receipt_from_chat"] = chat_id
                order_id = _next_order_id()
                order = {
                    "id": order_id,
                    "chat_id": chat_id,
                    "username": username,
                    "volume_gb": data["volume_gb"],
                    "days": data["days"],
                    "price": data["price"],
                    "discount_code": data.get("discount_code"),
                    "status": "pending",
                    "receipt_message_id": msg.get("message_id"),
                    "created_at": datetime.now().isoformat(),
                    "uid": None,
                }
                SHOP["orders"][order_id] = order
                await _save_shop()
                _pending.pop(chat_id, None)

                r = await _send(chat_id, "💳 در حال ثبت سفارش...")
                mid = (r or {}).get("result", {}).get("message_id")
                if mid:
                    await _play_frames(chat_id, mid, _PAY_FRAMES["fa" if _lg(chat_id) == "fa" else "en"][1:])

                for aid in ADMIN_IDS:
                    await _forward(aid, chat_id, msg.get("message_id"))
                    await _send(aid, _order_summary(order), _order_admin_kb(order_id))

                await _send(
                    chat_id,
                    "✅ رسیدت دریافت شد و سفارشت برای تیم پشتیبانی ارسال شد.\n"
                    "به محض تایید، کانفیگت به‌صورت خودکار همینجا برات ارسال می‌شه 🚀",
                )
                return
            await _send(chat_id, "📎 لطفاً عکس رسید یا کد پیگیری واریز رو بفرست:", _buy_cancel_kb(chat_id))
            return

    # ── مراحل تمدید کانفیگ ───────────────────────────────────────────────────
    if pending and pending.get("action") == "renew":
        step = pending["step"]
        rdata = pending["data"]
        renew_cancel_kb = {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "renew:cancel"}]]}

        if step == "volume" and text:
            if text.strip() == "0":
                v = 0.0
            else:
                v = _parse_positive_float(text)
            if v is None:
                await _send(chat_id, "❗️ یه عدد معتبر بفرست (یا ۰ برای رد کردن این مرحله):", renew_cancel_kb)
                return
            rdata["extra_gb"] = v
            pending["step"] = "days"
            await _send(chat_id, "📅 چند روز اعتبار اضافه می‌خوای؟ (اگه نمی‌خوای، عدد ۰ رو بفرست):", renew_cancel_kb)
            return

        if step == "days" and text:
            n = _parse_nonneg_int(text)
            if n is None:
                await _send(chat_id, "❗️ یه عدد صحیح بفرست (یا ۰ برای رد کردن این مرحله):", renew_cancel_kb)
                return
            rdata["extra_days"] = n
            if rdata.get("extra_gb", 0) <= 0 and n <= 0:
                await _send(chat_id, "❗️ حداقل یکی از حجم یا مدت باید بیشتر از صفر باشه. دوباره حجم رو بفرست:", renew_cancel_kb)
                pending["step"] = "volume"
                return
            price = _price_for(rdata.get("extra_gb", 0), n)
            rdata["price"] = price
            pending["step"] = "confirm"
            balance = _wallet_balance(chat_id)
            kb_rows = []
            if price > 0 and balance >= price:
                kb_rows.append([{"text": f"{_t(chat_id,'btn_pay_wallet')} ({balance:,}ت)", "callback_data": "renew:pay:wallet"}])
            kb_rows.append([{"text": _t(chat_id, "btn_pay_card"), "callback_data": "renew:pay:card"}])
            kb_rows.append([{"text": _t(chat_id, "btn_cancel"), "callback_data": "renew:cancel"}])
            summary = (
                "🔄 <b>خلاصه‌ی تمدید</b>\n\n"
                f"حجم اضافه: {rdata.get('extra_gb', 0)} GB\n"
                f"مدت اضافه: {n} روز\n"
                f"مبلغ قابل پرداخت: <b>{price:,} تومان</b>\n\nروش پرداخت رو انتخاب کن:"
            )
            await _send(chat_id, summary, {"inline_keyboard": kb_rows})
            return

        if step == "receipt":
            if photo or text:
                order_id = _next_order_id()
                order = {
                    "id": order_id, "chat_id": chat_id, "username": username,
                    "volume_gb": rdata.get("extra_gb", 0), "days": rdata.get("extra_days", 0),
                    "price": rdata["price"], "discount_code": None, "status": "pending",
                    "type": "renew", "renew_uid": rdata["uid"],
                    "receipt_message_id": msg.get("message_id"),
                    "created_at": datetime.now().isoformat(), "uid": None,
                }
                SHOP["orders"][order_id] = order
                await _save_shop()
                _pending.pop(chat_id, None)
                for aid in ADMIN_IDS:
                    await _forward(aid, chat_id, msg.get("message_id"))
                    await _send(aid, "🔄 درخواست تمدید کانفیگ:\n\n" + _order_summary(order), _order_admin_kb(order_id))
                await _send(chat_id, "✅ رسیدت دریافت شد؛ به محض تایید، کانفیگت تمدید می‌شه 🚀")
                return
            await _send(chat_id, "📎 لطفاً عکس رسید یا کد پیگیری واریز رو بفرست:", renew_cancel_kb)
            return

    # ── مراحل شارژ کیف پول ───────────────────────────────────────────────────
    if pending and pending.get("action") == "wallet_topup":
        step = pending["step"]
        wdata = pending["data"]
        wallet_cancel_kb = {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "wallet:cancel"}]]}

        if step == "amount" and text:
            n = _parse_nonneg_int(text)
            if n is None or n <= 0:
                await _send(chat_id, "❗️ یه عدد صحیح و مثبت بفرست (تومان):", wallet_cancel_kb)
                return
            wdata["amount"] = n
            pending["step"] = "receipt"
            card = SHOP.get("card_number") or "—"
            owner = SHOP.get("card_owner") or "—"
            await _send(
                chat_id,
                f"💳 مبلغ <b>{n:,} تومان</b> رو به شماره کارت زیر واریز کن:\n\n<code>{card}</code>\nبه نام: {owner}\n\n"
                f"بعد از واریز، عکس رسید یا کد پیگیری رو همینجا بفرست 📎",
                wallet_cancel_kb,
            )
            return

        if step == "receipt":
            if photo or text:
                topup_id = _next_topup_id()
                topup = {
                    "id": topup_id, "chat_id": chat_id, "username": username,
                    "amount": wdata["amount"], "status": "pending",
                    "created_at": datetime.now().isoformat(),
                }
                SHOP.setdefault("wallet_topups", {})[topup_id] = topup
                await _save_shop()
                _pending.pop(chat_id, None)
                kb = {"inline_keyboard": [[
                    {"text": "✅ تایید شارژ", "callback_data": f"wt:appr:{topup_id}"},
                    {"text": "❌ رد", "callback_data": f"wt:rej:{topup_id}"},
                ]]}
                for aid in ADMIN_IDS:
                    await _forward(aid, chat_id, msg.get("message_id"))
                    await _send(aid, f"💰 درخواست شارژ کیف پول از:\n{_buyer_line(chat_id, username)}\nمبلغ: {wdata['amount']:,} تومان", kb)
                await _send(chat_id, "✅ رسیدت دریافت شد؛ به محض تایید، کیف پولت شارژ می‌شه 🚀")
                return
            await _send(chat_id, "📎 لطفاً عکس رسید یا کد پیگیری واریز رو بفرست:", wallet_cancel_kb)
            return

    # ── ارسال پیام همگانی (ادمین) ────────────────────────────────────────────
    if pending and pending.get("action") == "broadcast" and _is_admin(chat_id):
        if photo or text:
            targets = list(SHOP.get("known_users", []))
            _pending.pop(chat_id, None)
            r = await _send(chat_id, f"📢 در حال ارسال به {len(targets)} کاربر...")
            sent, failed = 0, 0
            for uid_ in targets:
                res = None
                if photo:
                    res = await _forward(uid_, chat_id, msg.get("message_id"))
                else:
                    res = await _send(uid_, text)
                if res and res.get("ok"):
                    sent += 1
                else:
                    failed += 1
                await asyncio.sleep(0.05)
            await _send(chat_id, f"✅ پیام همگانی ارسال شد.\nموفق: {sent}   ناموفق: {failed}")
            return
        await _send(chat_id, "📢 متن پیامی که می‌خوای برای همه‌ی کاربرا ارسال بشه رو بفرست:",
                    {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "bcast:cancel"}]]})
        return

    # ── مراحل ساخت کد تخفیف (ادمین) ─────────────────────────────────────────
    if pending and pending.get("action") == "discount_wizard" and text:
        if not _is_admin(chat_id):
            _pending.pop(chat_id, None)
        else:
            step = pending["step"]
            wdata = pending["data"]

            if step == "code":
                code = text.strip().upper().replace(" ", "")
                if not code or len(code) > 30:
                    await _send(chat_id, "❗️ یه کد معتبر بفرست (حداکثر ۳۰ کاراکتر):")
                    return
                if code in SHOP.get("discount_codes", {}):
                    await _send(chat_id, "❗️ این کد قبلاً وجود داره. یه کد دیگه بفرست:")
                    return
                wdata["code"] = code
                pending["step"] = "type"
                await _send(chat_id, "🏷 نوع تخفیف رو انتخاب کن:", _discount_type_kb(chat_id))
                return

            if step == "type":
                await _send(chat_id, "لطفاً از دکمه‌های بالا یکی رو انتخاب کن 👆", _discount_type_kb(chat_id))
                return

            if step == "value":
                v = _parse_positive_float(text)
                if v is None or (wdata.get("type") == "percent" and v > 100):
                    hint = " (حداکثر ۱۰۰٪)" if wdata.get("type") == "percent" else ""
                    await _send(chat_id, f"❗️ یه عدد معتبر بفرست{hint}:")
                    return
                wdata["value"] = v
                pending["step"] = "maxuses"
                await _send(chat_id, "🔢 حداکثر تعداد دفعات استفاده رو بفرست (عدد صحیح)، یا نامحدود رو بزن:", _discount_unlimited_kb(chat_id))
                return

            if step == "maxuses":
                n = _parse_nonneg_int(text)
                if n is None:
                    await _send(chat_id, "❗️ یه عدد صحیح بفرست:", _discount_unlimited_kb(chat_id))
                    return
                wdata["max_uses"] = n
                pending["step"] = "confirm"
                await _send(chat_id, _discount_summary(wdata), _discount_confirm_kb(chat_id))
                return

    # ── ساخت کد هدیه (ادمین) ────────────────────────────────────────────────
    if pending and pending.get("action") == "gift_wizard" and text:
        if not _is_admin(chat_id):
            _pending.pop(chat_id, None)
        else:
            step = pending["step"]
            wdata = pending["data"]

            if step == "code":
                code = text.strip().upper().replace(" ", "")
                if not code or len(code) > 30:
                    await _send(chat_id, "❗️ یه کد معتبر بفرست (حداکثر ۳۰ کاراکتر):")
                    return
                if code in SHOP.get("gift_codes", {}):
                    await _send(chat_id, "❗️ این کد قبلاً وجود داره. یه کد دیگه بفرست:")
                    return
                wdata["code"] = code
                pending["step"] = "amount"
                await _send(chat_id, "💰 مبلغ هدیه رو به تومان بفرست (هر عددی که بخوای):")
                return

            if step == "amount":
                n = _parse_nonneg_int(text)
                if n is None or n <= 0:
                    await _send(chat_id, "❗️ یه عدد صحیح و بزرگ‌تر از صفر بفرست:")
                    return
                wdata["amount"] = n
                pending["step"] = "maxuses"
                await _send(chat_id, "🔢 حداکثر تعداد دفعات استفاده رو بفرست (عدد صحیح)، یا نامحدود رو بزن:", _giftcode_unlimited_kb(chat_id))
                return

            if step == "maxuses":
                n = _parse_nonneg_int(text)
                if n is None:
                    await _send(chat_id, "❗️ یه عدد صحیح بفرست:", _giftcode_unlimited_kb(chat_id))
                    return
                wdata["max_uses"] = n
                pending["step"] = "confirm"
                await _send(chat_id, _giftcode_summary(wdata), _giftcode_confirm_kb(chat_id))
                return

    # ── دریافت کد هدیه (مشتری) ────────────────────────────────────────────
    if pending and pending.get("action") == "gift_redeem" and text:
        _pending.pop(chat_id, None)
        code = text.strip().upper().replace(" ", "")
        d = _find_gift(code)
        ok, reason = _gift_valid_for(d, chat_id)
        if not ok:
            await _send(chat_id, f"❌ {reason}", {"inline_keyboard": [[{"text": _t(chat_id, "btn_back_home"), "callback_data": "wallet:home"}]]})
            return
        amount = int(d["amount"])
        wallets = SHOP.setdefault("wallets", {})
        wallets[str(chat_id)] = int(wallets.get(str(chat_id), 0)) + amount
        d["used_count"] = d.get("used_count", 0) + 1
        d.setdefault("redeemed_by", []).append(chat_id)
        await _save_shop()
        await _send(chat_id, f"🎉 کد هدیه با موفقیت اعمال شد! <b>{amount:,} تومان</b> به کیف پولت اضافه شد.\n\n💰 موجودی فعلی: <b>{_wallet_balance(chat_id):,} تومان</b>",
                    {"inline_keyboard": [[{"text": _t(chat_id, "btn_back_home"), "callback_data": "wallet:home"}]]})
        return

    # ── مراحل ساخت دستی (ادمین) ─────────────────────────────────────────────
    if pending and pending.get("action") == "admin_wizard" and text:
        if not _is_admin(chat_id):
            _pending.pop(chat_id, None)
        else:
            step = pending["step"]
            data = pending["data"]

            if step == "label":
                data["label"] = text[:60] or "کانفیگ جدید"
                pending["step"] = "protocol"
                await _send(chat_id, _wizard_prompt("protocol", data), _wizard_protocol_kb(chat_id))
                return

            if step in ("protocol", "fingerprint"):
                kb = _wizard_protocol_kb(chat_id) if step == "protocol" else _wizard_fp_kb(chat_id)
                await _send(chat_id, "لطفاً از دکمه‌های بالا یکی رو انتخاب کن 👆", kb)
                return

            if step == "alpn":
                data["alpn"] = text.strip()[:100]
                pending["step"] = "port"
                await _send(chat_id, _wizard_prompt("port", data), _wizard_skip_kb(chat_id, "port", f"⏭ پیش‌فرض ({DEFAULT_PORT})"))
                return

            if step == "port":
                try:
                    p = int(text.strip())
                except ValueError:
                    p = None
                if p is None or not (MIN_PORT <= p <= MAX_PORT):
                    await _send(chat_id, f"❗️ عدد پورت نامعتبره. یه عدد بین {MIN_PORT} تا {MAX_PORT} بفرست:", _wizard_skip_kb(chat_id, "port", f"⏭ پیش‌فرض ({DEFAULT_PORT})"))
                    return
                data["port"] = p
                pending["step"] = "volume"
                await _send(chat_id, _wizard_prompt("volume", data), _wizard_unlimited_kb(chat_id, "volume"))
                return

            if step == "volume":
                parsed = _parse_volume_text(text)
                if parsed is None:
                    await _send(chat_id, "❗️ فرمت درست نیست. مثلاً بفرست: <code>10GB</code> یا <code>500MB</code>", _wizard_unlimited_kb(chat_id, "volume"))
                    return
                data["limit_bytes"] = parsed
                pending["step"] = "speed"
                await _send(chat_id, _wizard_prompt("speed", data), _wizard_unlimited_kb(chat_id, "speed"))
                return

            if step == "speed":
                parsed = _parse_speed_text(text)
                if parsed is None:
                    await _send(chat_id, "❗️ فرمت درست نیست. یه عدد بفرست، مثلاً <code>20</code> (Mbps)", _wizard_unlimited_kb(chat_id, "speed"))
                    return
                data["speed_limit_bytes"] = parsed
                pending["step"] = "iplimit"
                await _send(chat_id, _wizard_prompt("iplimit", data), _wizard_unlimited_kb(chat_id, "iplimit"))
                return

            if step == "iplimit":
                n = _parse_nonneg_int(text)
                if n is None:
                    await _send(chat_id, "❗️ یه عدد صحیح بفرست:", _wizard_unlimited_kb(chat_id, "iplimit"))
                    return
                data["ip_limit"] = n
                pending["step"] = "days"
                await _send(chat_id, _wizard_prompt("days", data), _wizard_unlimited_kb(chat_id, "days"))
                return

            if step == "days":
                n = _parse_nonneg_int(text)
                if n is None:
                    await _send(chat_id, "❗️ یه عدد صحیح بفرست (تعداد روز):", _wizard_unlimited_kb(chat_id, "days"))
                    return
                data["expires_days"] = n
                pending["step"] = "confirm"
                await _send(chat_id, _wizard_summary(data), _wizard_confirm_kb(chat_id))
                return

    # ── ورودی تنظیمات فروش (ادمین) ──────────────────────────────────────────
    if pending and pending.get("action") == "support_msg" and text:
        _pending.pop(chat_id, None)
        sid = SHOP.get("support_next_id", 1)
        SHOP["support_next_id"] = sid + 1
        SHOP.setdefault("support_messages", []).append({
            "id": sid, "chat_id": chat_id, "username": username,
            "text": text, "at": datetime.now().isoformat(), "read": False, "replies": [],
        })
        await _save_shop()
        log_activity("telegram", f"📨 پیام پشتیبانی جدید (#{sid}) از {username or chat_id}", "info")
        await _send(chat_id, f"✅ پیامت ثبت شد (شماره تیکت #{sid})؛ به‌زودی جواب می‌گیری.")
        return

    # ── پاسخ ادمین به یه تیکت پشتیبانی ──────────────────────────────────────
    if pending and pending.get("action") == "support_reply" and text and _is_admin(chat_id):
        sid = pending.get("sid")
        msg_obj = next((m for m in SHOP.get("support_messages", []) if m["id"] == sid), None)
        _pending.pop(chat_id, None)
        if not msg_obj:
            await _send(chat_id, "این تیکت دیگه پیدا نشد.")
            return
        msg_obj.setdefault("replies", []).append({"from": "admin", "text": text, "at": datetime.now().isoformat()})
        msg_obj["read"] = True
        await _save_shop()
        await _send(msg_obj["chat_id"], f"☎️ <b>پاسخ پشتیبانی</b> (تیکت #{sid}):\n\n{text}")
        await _send(chat_id, f"✅ پاسخت برای تیکت #{sid} ارسال شد.", _support_detail_kb(chat_id, sid))
        return

    # ── انتقال کیف پول به کاربر دیگر (مشتری) ──────────────────────────────
    if pending and pending.get("action") == "wallet_transfer" and text:
        step = pending["step"]
        wdata = pending["data"]
        cancel_kb = {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "wallet:home"}]]}

        if step == "target":
            target = _parse_nonneg_int(text)
            if target is None or target <= 0:
                await _send(chat_id, "❗️ یه آیدی عددی معتبر بفرست:", cancel_kb)
                return
            if target == chat_id:
                await _send(chat_id, "❗️ نمی‌تونی به کیف پول خودت انتقال بدی. یه آیدی دیگه بفرست:", cancel_kb)
                return
            wdata["target"] = target
            pending["step"] = "amount"
            await _send(chat_id, f"💰 چقدر تومان می‌خوای به آیدی <code>{target}</code> بفرستی؟", cancel_kb)
            return

        if step == "amount":
            n = _parse_nonneg_int(text)
            if n is None or n <= 0:
                await _send(chat_id, "❗️ یه عدد صحیح و بزرگ‌تر از صفر بفرست:", cancel_kb)
                return
            if n > _wallet_balance(chat_id):
                await _send(chat_id, f"❗️ موجودیت کافی نیست. موجودی فعلی: {_wallet_balance(chat_id):,} تومان", cancel_kb)
                return
            wdata["amount"] = n
            pending["step"] = "confirm"
            confirm_kb = {"inline_keyboard": [
                [{"text": "✅ تایید و ارسال", "callback_data": "wallet:transfer:confirm"}],
                [{"text": _t(chat_id, "btn_cancel"), "callback_data": "wallet:home"}],
            ]}
            await _send(chat_id, f"💸 مبلغ <b>{n:,} تومان</b> به آیدی <code>{wdata['target']}</code> منتقل بشه؟", confirm_kb)
            return

    # ── مدیریت کیف پول کاربران توسط ادمین (شارژ/کسر دستی) ──────────────────
    if pending and pending.get("action") == "admin_wallet" and text and _is_admin(chat_id):
        step = pending["step"]
        wdata = pending["data"]
        cancel_kb = {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "home"}]]}

        if step == "target":
            target = _parse_nonneg_int(text)
            if target is None or target <= 0:
                await _send(chat_id, "❗️ یه آیدی عددی معتبر بفرست:", cancel_kb)
                return
            wdata["target"] = target
            pending["step"] = "amount"
            await _send(chat_id, f"💰 چقدر تومان به کیف پول <code>{target}</code> اضافه/کم بشه؟ (برای کسر، عدد منفی بفرست، مثلاً <code>-5000</code>)", cancel_kb)
            return

        if step == "amount":
            try:
                n = int(text.strip().replace(",", ""))
            except Exception:
                await _send(chat_id, "❗️ یه عدد صحیح بفرست (می‌تونه منفی هم باشه):", cancel_kb)
                return
            target = wdata["target"]
            _wallet_add(target, n)
            await _save_shop()
            _pending.pop(chat_id, None)
            new_balance = _wallet_balance(target)
            sign = "➕" if n >= 0 else "➖"
            await _send(chat_id, f"✅ {sign} {abs(n):,} تومان برای <code>{target}</code> اعمال شد.\nموجودی جدید: <b>{new_balance:,} تومان</b>")
            try:
                await _send(target, f"💎 موجودی کیف پولت توسط پشتیبانی {sign} <b>{abs(n):,} تومان</b> تغییر کرد.\nموجودی فعلی: <b>{new_balance:,} تومان</b>")
            except Exception:
                pass
            return

    # ── افزودن ادمین جدید ────────────────────────────────────────────────
    if pending and pending.get("action") == "add_admin" and text and _is_admin(chat_id):
        _pending.pop(chat_id, None)
        new_id = _parse_nonneg_int(text)
        if new_id is None or new_id <= 0:
            await _send(chat_id, "❗️ آیدی عددی نامعتبر بود. از منو دوباره امتحان کن.")
            return
        if new_id in ADMIN_IDS:
            await _send(chat_id, "این آیدی از قبل ادمینه.")
            return
        ADMIN_IDS.add(new_id)
        TELEGRAM_SETTINGS["admin_ids"] = sorted(ADMIN_IDS)
        await save_state()
        log_activity("telegram", f"👑 ادمین جدید اضافه شد: {new_id}", "ok")
        await _send(chat_id, f"✅ آیدی <code>{new_id}</code> به‌عنوان ادمین اضافه شد.")
        try:
            await _send(new_id, "👑 تبریک! تو الان یکی از ادمین‌های ربات Matix هستی. برای دیدن پنل مدیریت /start رو بزن.")
        except Exception:
            pass
        return

    # ── ارسال پیام مستقیم ادمین به یک کاربر خاص ─────────────────────────────
    if pending and pending.get("action") == "dm_send" and text and _is_admin(chat_id):
        step = pending["step"]
        wdata = pending["data"]
        cancel_kb = {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "home"}]]}

        if step == "target":
            target = _parse_nonneg_int(text)
            if target is None or target <= 0:
                await _send(chat_id, "❗️ یه آیدی عددی معتبر بفرست:", cancel_kb)
                return
            wdata["target"] = target
            pending["step"] = "text"
            await _send(chat_id, f"✉️ متن پیامی که می‌خوای برای <code>{target}</code> ارسال بشه رو بفرست:", cancel_kb)
            return

        if step == "text":
            wdata["text"] = text
            pending["step"] = "confirm"
            preview = text if len(text) <= 300 else text[:300] + "…"
            confirm_kb = {"inline_keyboard": [
                [{"text": "✅ ارسال", "callback_data": "dm:confirm"}],
                [{"text": _t(chat_id, "btn_cancel"), "callback_data": "home"}],
            ]}
            await _send(chat_id, f"✉️ این پیام برای <code>{wdata['target']}</code> ارسال بشه؟\n\n«{preview}»", confirm_kb)
            return

    if pending and pending.get("action") == "set_value" and text and _is_admin(chat_id):
        field = pending["field"]
        if field in ("price_per_gb", "price_per_day", "min_price"):
            n = _parse_nonneg_int(text)
            if n is None:
                await _send(chat_id, "❗️ یه عدد صحیح (تومان) بفرست:")
                return
            SHOP[field] = n
        elif field in ("trial_volume_gb",):
            v = _parse_positive_float(text)
            if v is None:
                await _send(chat_id, "❗️ یه عدد معتبر بفرست (گیگابایت):")
                return
            SHOP[field] = v
        elif field == "trial_days":
            n = _parse_nonneg_int(text)
            if n is None:
                await _send(chat_id, "❗️ یه عدد صحیح (روز) بفرست:")
                return
            SHOP[field] = n
        elif field in ("card_number", "card_owner"):
            SHOP[field] = text.strip()[:60]
        elif field == "announce_channel":
            SHOP[field] = text.strip()
        elif field == "required_channel":
            SHOP[field] = "" if text.strip() == "-" else text.strip()
        elif field == "required_channel_url":
            SHOP[field] = text.strip()
        elif field == "referral_bonus":
            n = _parse_nonneg_int(text)
            if n is None:
                await _send(chat_id, "❗️ یه عدد صحیح (تومان) بفرست:")
                return
            SHOP[field] = n
        elif field == "support_username":
            SHOP[field] = text.strip().lstrip("@")[:64]
        elif field == "channels":
            chans = []
            for line in text.splitlines():
                line = line.strip()
                if not line or "|" not in line:
                    continue
                name, url = line.split("|", 1)
                name, url = name.strip(), url.strip()
                if name and url.startswith("http"):
                    chans.append({"name": name[:40], "url": url})
            if not chans:
                await _send(chat_id, "❗️ فرمت درست نبود. هر خط باید «اسم | لینک» باشه:")
                return
            SHOP[field] = chans
        elif field == "spin_range":
            parts = [p.strip() for p in re.split(r"[,\n]+", text) if p.strip()]
            if len(parts) != 2:
                await _send(chat_id, "❗️ دو عدد با کاما بفرست، مثال: <code>2000,20000</code>")
                return
            lo, hi = _parse_nonneg_int(parts[0]), _parse_nonneg_int(parts[1])
            if lo is None or hi is None or lo <= 0 or hi < lo:
                await _send(chat_id, "❗️ مقادیر نامعتبرن. مثال: <code>2000,20000</code>")
                return
            SHOP["spin_min"], SHOP["spin_max"] = lo, hi
        elif field == "wallet_topup_presets":
            parts = [p.strip() for p in re.split(r"[,\n]+", text) if p.strip()]
            amounts = []
            for p in parts:
                n = _parse_nonneg_int(p)
                if n is None or n <= 0:
                    await _send(chat_id, "❗️ فقط عدد بفرست، با کاما جدا کن. مثال: <code>50000,100000,200000,500000</code>")
                    return
                amounts.append(n)
            if not amounts:
                await _send(chat_id, "❗️ حداقل یک مبلغ بفرست.")
                return
            SHOP[field] = amounts
        await _save_shop()
        _pending.pop(chat_id, None)
        await _send(chat_id, "✅ ذخیره شد.", _settings_kb(chat_id))
        return

    # ── قابلیت «اعلام حجم»: اگه کاربر لینک vless، لینک ساب یا UUID کانفیگ رو بفرسته ─
    if text:
        uid = _extract_uid_from_text(text)
        if uid:
            l = LINKS.get(uid)
            if l and (_is_admin(chat_id) or l.get("owner_chat_id") == chat_id):
                kb = {"inline_keyboard": [
                    [{"text": _t(chat_id, "btn_show_link"), "callback_data": f"link:{uid}"}],
                    [{"text": _t(chat_id, "btn_back_home"), "callback_data": "home"}],
                ]}
                await _send(chat_id, _usage_report(uid, l), kb)
                return
            if l:
                await _send(chat_id, "⛔ این کانفیگ متعلق به شما نیست.")
                return

    # پیام ناشناخته → صفحه‌ی اصلی رو نشون بده
    title, sub, kb = _home_view(chat_id)
    await _send(chat_id, _join_ts(title, sub), kb)

async def _handle_callback(cb: dict):
    chat_id = cb.get("message", {}).get("chat", {}).get("id")
    message_id = cb.get("message", {}).get("message_id")
    data = cb.get("data", "")
    cb_id = cb.get("id")
    username = cb.get("from", {}).get("username") or cb.get("from", {}).get("first_name", "")

    if chat_id is None:
        return
    await _answer_cb(cb_id)

    if data == "checkjoin":
        if await _passes_membership_gate(chat_id):
            title, sub, kb = _home_view(chat_id)
            await _edit(chat_id, message_id, f"✅ عضویت تایید شد!\n\n" + _join_ts(title, sub), kb)
        else:
            await _answer_cb(cb_id, "هنوز عضو کانال نشدی 🙁", alert=True)
        return

    if not await _passes_membership_gate(chat_id):
        await _edit(chat_id, message_id, "🔒 برای استفاده از ربات، اول باید عضو کانال ما بشی:", _join_gate_kb(chat_id))
        return

    if data == "lang:toggle":
        _toggle_lang(chat_id)
        title, sub, kb = _home_view(chat_id)
        await _edit(chat_id, message_id, _join_ts(title, sub), kb)
        return

    if data == "home":
        _pending.pop(chat_id, None)
        title, sub, kb = _home_view(chat_id)
        await _edit(chat_id, message_id, _join_ts(title, sub), kb)
        return

    if data == "help":
        v2box_kb = {"inline_keyboard": [
            [{"text": "📲 دانلود اپلیکیشن v2Box", "url": V2BOX_URL}],
            [{"text": _t(chat_id, "btn_back_home"), "callback_data": "home"}],
        ]}
        await _edit(chat_id, message_id, _t(chat_id, "help_text"), v2box_kb)
        return

    # ── گردونه‌ی شانس (یک‌بار در روز، جایزه‌ی نقدی به کیف پول) ──────────────
    if data == "spin:go":
        today = datetime.now().date().isoformat()
        spins = SHOP.setdefault("spins", {})
        if spins.get(str(chat_id)) == today:
            await _answer_cb(cb_id, "🎲 امروز یه بار چرخوندی! فردا دوباره سر بزن 🌟", alert=True)
            return
        lo, hi = int(SHOP.get("spin_min", 2000)), int(SHOP.get("spin_max", 20000))
        step = max(1, (hi - lo) // 1000)
        prize = lo + random.randint(0, step) * 1000
        spins[str(chat_id)] = today
        wallets = SHOP.setdefault("wallets", {})
        wallets[str(chat_id)] = int(wallets.get(str(chat_id), 0)) + prize
        await _save_shop()
        for frame in ["🎡 در حال چرخش گردونه", "🎡 در حال چرخش گردونه.", "🎡 در حال چرخش گردونه..", "🎡 در حال چرخش گردونه..."]:
            await _edit(chat_id, message_id, frame)
            await asyncio.sleep(0.35)
        title, sub, kb = _home_view(chat_id)
        await _edit(chat_id, message_id,
                     f"🎉 تبریک! گردونه <b>{prize:,} تومان</b> جایزه داد و به کیف پولت اضافه شد!\n\n" + _join_ts(title, sub), kb)
        return

    # ── تعرفه‌ی اشتراک‌ها (چند نمونه‌ی محاسبه‌شده از فرمول قیمت‌گذاری) ──────────
    if data == "tariff:home":
        pergb, perday, minp = SHOP.get("price_per_gb", 0), SHOP.get("price_per_day", 0), SHOP.get("min_price", 0)
        lines = [
            "💰 <b>تعرفه‌ی اشتراک‌های Matix</b>\n",
            f"نرخ پایه: هر گیگ <b>{pergb:,}</b> تومان + هر روز <b>{perday:,}</b> تومان (حداقل سفارش {minp:,} تومان)\n",
        ]
        for gb, days in [(10, 30), (20, 30), (30, 30), (50, 60), (100, 90)]:
            lines.append(f"📦 {gb} گیگ / {days} روز — <b>{_price_for(gb, days):,} تومان</b>")
        lines.append("\nهر ترکیب دلخواهی از حجم و مدت رو هم می‌تونی از «خرید اشتراک» بسازی 👇")
        kb = {"inline_keyboard": [
            [_btn(_t(chat_id, "btn_buy"), "buy:start", style="success")],
            [_btn(_t(chat_id, "btn_back_home"), "home")],
        ]}
        await _edit(chat_id, message_id, "\n".join(lines), kb)
        return

    # ── پشتیبانی ───────────────────────────────────────────────────────────
    if data == "support:home":
        uname = (SHOP.get("support_username") or "").lstrip("@")
        _pending[chat_id] = {"action": "support_msg"}
        txt = "🎧 <b>پشتیبانی Matix</b>\n\nپیامت رو بنویس و بفرست؛ مستقیم توی صندوق پشتیبانی ثبت می‌شه و به‌زودی جواب می‌گیری."
        rows = []
        if uname:
            rows.append([_btn("💬 گفتگوی مستقیم با پشتیبانی", url=f"https://t.me/{uname}", style="primary")])
        rows.append([_btn(_t(chat_id, "btn_back_home"), "home", style="success")])
        await _edit(chat_id, message_id, txt, {"inline_keyboard": rows})
        return

    # ── صندوق پیام‌های پشتیبانی (ادمین) ──────────────────────────────────────
    if data.startswith("supportmsgs:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        page = int(data.split(":", 1)[1] or 0)
        kb, total = _support_list_kb(chat_id, page)
        n_unread = _support_unread_count()
        await _edit(chat_id, message_id, f"📨 صندوق پشتیبانی ({total} تیکت، {n_unread} خونده‌نشده):", kb)
        return

    if data.startswith("supportmsg:view:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        sid = int(data.split(":", 2)[2])
        m = next((x for x in SHOP.get("support_messages", []) if x["id"] == sid), None)
        if not m:
            kb, total = _support_list_kb(chat_id, 0)
            await _edit(chat_id, message_id, "این تیکت دیگه پیدا نشد.", kb)
            return
        if not m.get("read"):
            m["read"] = True
            await _save_shop()
        await _edit(chat_id, message_id, _support_detail_text(m), _support_detail_kb(chat_id, sid))
        return

    if data.startswith("supportmsg:reply:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        sid = int(data.split(":", 2)[2])
        _pending[chat_id] = {"action": "support_reply", "sid": sid}
        await _edit(chat_id, message_id, f"↩️ پاسخت به تیکت #{sid} رو بنویس:",
                    {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": f"supportmsg:view:{sid}"}]]})
        return

    # ── درخواست نمایندگی ──────────────────────────────────────────────────
    if data == "reseller:request":
        await _notify_admins(f"🧑‍💼 <b>درخواست نمایندگی جدید!</b>\n\n{_buyer_line(chat_id, username)}\n\nبرای بررسی و هماهنگی باهاش در تماس باش.")
        await _answer_cb(cb_id, "✅ درخواستت برای تیم ادمین ارسال شد؛ به‌زودی باهات تماس می‌گیریم.", alert=True)
        return

    # ── ورودهای اخیر ربات (فقط ادمین) ────────────────────────────────────────
    if data.startswith("joins:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        page = int(data.split(":", 1)[1] or 0)
        joins = list(reversed(SHOP.get("join_log", [])))
        total = len(joins)
        start = page * PAGE_SIZE
        chunk = joins[start:start + PAGE_SIZE]
        lines = [f"👥 <b>کاربران ربات: {len(SHOP.get('known_users', []))} نفر</b>\n"]
        for j in chunk:
            uname = f"@{j['username']}" if j.get("username") and " " not in j["username"] else (j.get("username") or "—")
            when = (j.get("at") or "")[:16].replace("T", " ")
            lines.append(f"🟢 {uname} — <code>{j['chat_id']}</code> — {when}")
        if not chunk:
            lines.append("هنوز ورودی ثبت نشده.")
        nav = []
        if start > 0:
            nav.append({"text": _t(chat_id, "btn_prev"), "callback_data": f"joins:{page-1}"})
        if start + PAGE_SIZE < total:
            nav.append({"text": _t(chat_id, "btn_next"), "callback_data": f"joins:{page+1}"})
        rows = [nav] if nav else []
        rows.append([_btn(_t(chat_id, "btn_back_home"), "home", style="success")])
        await _edit(chat_id, message_id, "\n".join(lines), {"inline_keyboard": rows})
        return

    # ── استعلام حجم کانفیگ (دکمه‌ی جدا) ─────────────────────────────────────
    if data == "usage:start":
        _pending[chat_id] = {"action": "usage_check"}
        kb = {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "usage:cancel"}]]}
        await _edit(chat_id, message_id, _t(chat_id, "usage_prompt"), kb)
        return

    if data == "usage:cancel":
        _pending.pop(chat_id, None)
        title, sub, kb = _home_view(chat_id)
        await _edit(chat_id, message_id, f"{_t(chat_id,'gen_cancelled')}\n\n" + _join_ts(title, sub), kb)
        return

    # ── مشاهده‌ی جزئیات یک کانفیگ (مشترک بین ادمین/مشتری، با محدودیت دسترسی) ──
    if data.startswith("view:"):
        uid = data.split(":", 1)[1]
        l = LINKS.get(uid)
        if not l:
            title, sub, kb = _home_view(chat_id)
            await _edit(chat_id, message_id, _t(chat_id, "not_exist"), kb)
            return
        if not _is_admin(chat_id) and l.get("owner_chat_id") != chat_id:
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        if _is_admin(chat_id):
            await _edit(chat_id, message_id, _format_detail(uid, l), _link_detail_kb(chat_id, uid, l["active"]))
        else:
            await _deliver_config(chat_id, message_id, uid, l, "📦 جزئیات کانفیگت:")
        return

    if data.startswith("link:"):
        uid = data.split(":", 1)[1]
        l = LINKS.get(uid)
        if not l:
            await _answer_cb(cb_id, _t(chat_id, "link_not_found"))
            return
        if not _is_admin(chat_id) and l.get("owner_chat_id") != chat_id:
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        host = get_host()
        vless = vless_link_for_link(l, uid, host)
        sub_url = f"https://{host}/sub/{uid}"
        public_url = f"https://{host}/p/{uid}"
        msg = _t(chat_id, "link_msg", label=l.get('label'), vless=vless, sub_url=sub_url, public_url=public_url)
        kb_rows = [[{"text": _t(chat_id, "btn_open_sub"), "url": public_url}]]
        share_url = _share_url()
        if share_url:
            kb_rows.append([{"text": _t(chat_id, "btn_share"), "url": share_url}])
        kb_rows.append([{"text": _t(chat_id, "btn_back_home"), "callback_data": "home"}])
        await _send(chat_id, msg, {"inline_keyboard": kb_rows})
        return

    # ── بخش مشتری: کانفیگ‌های من ─────────────────────────────────────────────
    if data.startswith("mine:"):
        page = int(data.split(":", 1)[1] or 0)
        kb, total = _mine_list_kb(chat_id, page)
        if total == 0:
            await _edit(chat_id, message_id, "📦 هنوز کانفیگی نداری. از دکمه‌ی زیر یکی بگیر 👇", kb)
            return
        await _edit(chat_id, message_id, f"📦 کانفیگ‌های من ({total} مورد):", kb)
        return

    # ── بخش مشتری: تست رایگان ───────────────────────────────────────────────
    if data == "trial:claim":
        if chat_id in SHOP.get("trial_used", []):
            await _answer_cb(cb_id, "🎁 قبلاً تست رایگانت رو گرفتی.", alert=True)
            return
        await _edit(chat_id, message_id, "🎁 در حال ساخت کانفیگ تست رایگانت...")
        await _play_frames(chat_id, message_id, _GEN_FRAMES[_lg(chat_id)])
        vol_bytes = parse_size_to_bytes(SHOP.get("trial_volume_gb", 0.2), "GB")
        days = int(SHOP.get("trial_days", 1))
        expires_at = (datetime.now() + timedelta(days=days)).isoformat() if days > 0 else None
        uid, link = await make_link(
            label=f"🎁 تست رایگان - {username or chat_id}",
            limit_bytes=vol_bytes,
            expires_at=expires_at,
        )
        link["owner_chat_id"] = chat_id
        link["owner_username"] = username
        link["source"] = "trial"
        SHOP.setdefault("trial_used", []).append(chat_id)
        await _save_shop()
        await save_state()
        await _deliver_config(chat_id, message_id, uid, link, "🎉 کانفیگ تست رایگانت آماده شد!")
        await _send_sticker(chat_id, "gift")
        return

    # ── بخش مشتری: شروع خرید ────────────────────────────────────────────────
    if data == "buy:start":
        _pending[chat_id] = {"action": "buy", "step": "volume", "data": {}}
        await _edit(chat_id, message_id, "📦 چند گیگابایت حجم می‌خوای؟ فقط عدد رو بفرست، مثلاً <code>20</code>:", _buy_cancel_kb(chat_id))
        return

    if data == "buy:cancel":
        _pending.pop(chat_id, None)
        title, sub, kb = _home_view(chat_id)
        await _edit(chat_id, message_id, f"{_t(chat_id,'gen_cancelled')}\n\n" + _join_ts(title, sub), kb)
        return

    if data == "buy:discount":
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "buy" or pending.get("step") != "confirm":
            await _answer_cb(cb_id, _t(chat_id, "step_invalid"), alert=True)
            return
        pending["step"] = "discount"
        await _edit(chat_id, message_id, "🏷 کد تخفیفتو بفرست:", _buy_cancel_kb(chat_id))
        return

    if data == "buy:confirm":
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "buy" or pending.get("step") != "confirm":
            await _answer_cb(cb_id, _t(chat_id, "step_invalid"), alert=True)
            return
        price = pending["data"]["price"]
        balance = _wallet_balance(chat_id)
        kb_rows = []
        if price > 0 and balance >= price:
            kb_rows.append([{"text": f"{_t(chat_id,'btn_pay_wallet')} ({balance:,}ت)", "callback_data": "buy:pay:wallet"}])
        kb_rows.append([{"text": _t(chat_id, "btn_pay_card"), "callback_data": "buy:pay:card"}])
        kb_rows.append([{"text": _t(chat_id, "btn_cancel"), "callback_data": "buy:cancel"}])
        await _edit(chat_id, message_id, f"💳 مبلغ قابل پرداخت: <b>{price:,} تومان</b>\n\nروش پرداخت رو انتخاب کن:", {"inline_keyboard": kb_rows})
        return

    if data == "buy:pay:card":
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "buy":
            await _answer_cb(cb_id, _t(chat_id, "step_invalid"), alert=True)
            return
        pending["step"] = "receipt"
        card = SHOP.get("card_number") or "—"
        owner = SHOP.get("card_owner") or "—"
        price = pending["data"]["price"]
        pay_text = (
            f"💳 مبلغ <b>{price:,} تومان</b> رو به شماره کارت زیر واریز کن:\n\n"
            f"<code>{card}</code>\n"
            f"به نام: {owner}\n\n"
            f"بعد از واریز، عکس رسید یا کد پیگیری رو همینجا بفرست 📎"
        )
        await _edit(chat_id, message_id, pay_text, _buy_cancel_kb(chat_id))
        return

    if data == "buy:pay:wallet":
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "buy":
            await _answer_cb(cb_id, _t(chat_id, "step_invalid"), alert=True)
            return
        wdata = pending["data"]
        price = wdata["price"]
        if not _wallet_sub(chat_id, price):
            await _answer_cb(cb_id, "موجودی کیف پولت کافی نیست", alert=True)
            return
        vol_bytes = parse_size_to_bytes(wdata["volume_gb"], "GB")
        expires_at = (datetime.now() + timedelta(days=wdata["days"])).isoformat() if wdata["days"] > 0 else None
        uid, link = await make_link(label=f"🛒 {username or chat_id}", limit_bytes=vol_bytes, expires_at=expires_at)
        link["owner_chat_id"] = chat_id
        link["owner_username"] = username
        link["source"] = "shop"
        order_id = _next_order_id()
        order = {
            "id": order_id, "chat_id": chat_id, "username": username,
            "volume_gb": wdata["volume_gb"], "days": wdata["days"], "price": price,
            "discount_code": wdata.get("discount_code"), "status": "approved",
            "payment_method": "wallet", "created_at": datetime.now().isoformat(), "uid": uid,
        }
        SHOP["orders"][order_id] = order
        code = wdata.get("discount_code")
        if code and code in SHOP.get("discount_codes", {}):
            SHOP["discount_codes"][code]["used_count"] = SHOP["discount_codes"][code].get("used_count", 0) + 1
        await save_state()
        await _save_shop()
        _pending.pop(chat_id, None)
        await _deliver_config(chat_id, message_id, uid, link, "✅ مبلغ از کیف پولت کسر شد و کانفیگت آماده شد!")
        await _send_sticker(chat_id, "success")
        await _announce_purchase(order)
        await _reward_referrer_if_first_order(order)
        return

    # ── کانال‌های ما ─────────────────────────────────────────────────────────
    if data == "channels:home":
        chans = SHOP.get("channels", [])
        if not chans:
            txt = "📢 فعلاً کانالی ثبت نشده."
            rows = []
        else:
            txt = "📢 <b>کانال‌های Matix</b>\n\nحتماً عضو بشو تا از اخبار و آفرها جا نمونی 👇"
            rows = [[_btn(c["name"], url=c["url"], style="primary")] for c in chans]
        rows.append([_btn(_t(chat_id, "btn_back_home"), "home", style="success")])
        await _edit(chat_id, message_id, txt, {"inline_keyboard": rows})
        return

    # ── کیف پول ──────────────────────────────────────────────────────────────
    if data == "wallet:home":
        balance = _wallet_balance(chat_id)
        txt = (
            f"💎 <b>کیف پول Matix</b>\n\n"
            f"موجودی فعلی: <b>{balance:,} تومان</b>\n"
            f"🆔 آیدی کیف پولت: <code>{chat_id}</code>\n"
            f"(این آیدی رو به بقیه بده تا بتونن براش پول بفرستن)"
        )
        kb = {"inline_keyboard": [
            [_btn("➕ شارژ کیف پول", "wallet:topup:start", style="success")],
            [_btn("💸 انتقال به آیدی دیگر", "wallet:transfer:start", style="success")],
            [_btn("🎁 کد هدیه دارم", "gift:redeem:start", style="success")],
            [_btn(_t(chat_id, "btn_back_home"), "home", style="primary")],
        ]}
        await _edit(chat_id, message_id, txt, kb)
        return

    if data == "wallet:transfer:start":
        _pending[chat_id] = {"action": "wallet_transfer", "step": "target", "data": {}}
        await _edit(chat_id, message_id, "💸 آیدی عددی کیف پول مقصد رو بفرست:",
                    {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "wallet:home"}]]})
        return

    if data == "wallet:transfer:confirm":
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "wallet_transfer" or pending.get("step") != "confirm":
            await _answer_cb(cb_id, _t(chat_id, "step_invalid"), alert=True)
            return
        wdata = pending["data"]
        target, amount = wdata["target"], wdata["amount"]
        if not _wallet_sub(chat_id, amount):
            _pending.pop(chat_id, None)
            await _edit(chat_id, message_id, "❗️ موجودیت کافی نیست.", {"inline_keyboard": [[{"text": _t(chat_id, "btn_back_home"), "callback_data": "wallet:home"}]]})
            return
        _wallet_add(target, amount)
        await _save_shop()
        _pending.pop(chat_id, None)
        await _edit(chat_id, message_id, f"✅ مبلغ <b>{amount:,} تومان</b> به آیدی <code>{target}</code> منتقل شد.\nموجودی فعلی تو: <b>{_wallet_balance(chat_id):,} تومان</b>",
                    {"inline_keyboard": [[{"text": _t(chat_id, "btn_back_home"), "callback_data": "wallet:home"}]]})
        try:
            await _send(target, f"💎 مبلغ <b>{amount:,} تومان</b> از یه کاربر دیگه به کیف پولت واریز شد!\nموجودی فعلی: <b>{_wallet_balance(target):,} تومان</b>")
        except Exception:
            pass
        return

    # ── مدیریت کیف پول کاربران (ادمین) ─────────────────────────────────────
    if data == "adminwallet:start":
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        _pending[chat_id] = {"action": "admin_wallet", "step": "target", "data": {}}
        await _edit(chat_id, message_id, "💎 آیدی عددی کیف پولی که می‌خوای شارژ/کسر کنی رو بفرست:",
                    {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "home"}]]})
        return

    # ── افزودن ادمین جدید ────────────────────────────────────────────────
    if data == "admin:addstart":
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        _pending[chat_id] = {"action": "add_admin"}
        await _edit(chat_id, message_id, "👑 آیدی عددی تلگرام شخصی که می‌خوای ادمین بشه رو بفرست:\n(برای گرفتن آیدی عددی، طرف باید یه بار به @userinfobot پیام بده)",
                    {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "home"}]]})
        return

    if data == "wallet:topup:start":
        _pending[chat_id] = {"action": "wallet_topup", "step": "amount", "data": {}}
        presets = SHOP.get("wallet_topup_presets") or []
        rows = []
        row = []
        for amt in presets:
            row.append({"text": f"{int(amt):,} ت", "callback_data": f"wallet:topup:preset:{int(amt)}"})
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([{"text": _t(chat_id, "btn_cancel"), "callback_data": "wallet:cancel"}])
        kb = {"inline_keyboard": rows}
        await _edit(chat_id, message_id, "➕ چه مبلغی (تومان) می‌خوای شارژ کنی؟\nیکی از مبالغ زیر رو انتخاب کن، یا خودت یه عدد دلخواه بفرست:", kb)
        return

    if data.startswith("wallet:topup:preset:"):
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "wallet_topup":
            await _answer_cb(cb_id, _t(chat_id, "step_invalid"), alert=True)
            return
        try:
            amount = int(data.split(":", 3)[3])
        except (ValueError, IndexError):
            await _answer_cb(cb_id, _t(chat_id, "invalid_btn"))
            return
        pending["data"]["amount"] = amount
        card = SHOP.get("card_number") or "—"
        owner = SHOP.get("card_owner") or "—"
        kb = {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "wallet:cancel"}]]}
        await _edit(chat_id, message_id,
            f"💳 مبلغ <b>{amount:,} تومان</b> رو به شماره کارت زیر واریز کن:\n\n<code>{card}</code>\nبه نام: {owner}\n\n"
            f"بعد از واریز، عکس رسید یا کد پیگیری رو همینجا بفرست 📎",
            kb)
        pending["step"] = "receipt"
        return

    if data == "wallet:cancel":
        _pending.pop(chat_id, None)
        title, sub, kb = _home_view(chat_id)
        await _edit(chat_id, message_id, f"{_t(chat_id,'gen_cancelled')}\n\n" + _join_ts(title, sub), kb)
        return

    if data.startswith("wt:appr:") or data.startswith("wt:rej:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        topup_id = data.split(":", 2)[2]
        topup = SHOP.get("wallet_topups", {}).get(topup_id)
        if not topup:
            await _answer_cb(cb_id, "این درخواست پیدا نشد", alert=True)
            return
        if data.startswith("wt:appr:"):
            topup["status"] = "approved"
            _wallet_add(topup["chat_id"], topup["amount"])
            await _save_shop()
            await _send(topup["chat_id"], f"✅ کیف پولت با <b>{topup['amount']:,} تومان</b> شارژ شد! 🎉")
            await _send_sticker(topup["chat_id"], "success")
            await _edit(chat_id, message_id, f"✅ شارژ کیف پول کاربر <code>{topup['chat_id']}</code> تایید و اعمال شد.")
        else:
            topup["status"] = "rejected"
            await _save_shop()
            await _send(topup["chat_id"], "❌ متاسفانه درخواست شارژ کیف پولت تایید نشد. برای پیگیری با پشتیبانی تماس بگیر.")
            await _edit(chat_id, message_id, f"❌ شارژ کیف پول کاربر <code>{topup['chat_id']}</code> رد شد.")
        return

    # ── رفرال ────────────────────────────────────────────────────────────────
    if data == "ref:home":
        await _ensure_username_cached()
        count = sum(1 for v in SHOP.get("referrals", {}).values() if v == chat_id)
        link = f"https://t.me/{_bot_username}?start=ref_{chat_id}" if _bot_username else "—"
        bonus = SHOP.get("referral_bonus", 0)
        txt = (
            "🤝 <b>دعوت دوستان</b>\n\n"
            f"به‌ازای هر دوستی که با لینک زیر بیاد و اولین خریدشو انجام بده، "
            f"<b>{bonus:,} تومان</b> به کیف پولت اضافه می‌شه!\n\n"
            f"لینک اختصاصی تو:\n<code>{link}</code>\n\n"
            f"👥 تعداد دعوت‌شده‌ها: <b>{count}</b>"
        )
        kb_rows = []
        if _bot_username:
            share_text = quote("بیا با Matix اینترنت آزاد و پرسرعت بگیر 🚀")
            kb_rows.append([{"text": "📤 اشتراک‌گذاری لینک دعوت", "url": f"https://t.me/share/url?url={link}&text={share_text}"}])
        kb_rows.append([{"text": _t(chat_id, "btn_back_home"), "callback_data": "home"}])
        await _edit(chat_id, message_id, txt, {"inline_keyboard": kb_rows})
        return

    # ── تمدید کانفیگ ─────────────────────────────────────────────────────────
    if data.startswith("renew:start:"):
        uid = data.split(":", 2)[2]
        l = LINKS.get(uid)
        if not l:
            await _answer_cb(cb_id, _t(chat_id, "link_not_found"), alert=True)
            return
        if not _is_admin(chat_id) and l.get("owner_chat_id") != chat_id:
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        _pending[chat_id] = {"action": "renew", "step": "volume", "data": {"uid": uid}}
        kb = {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "renew:cancel"}]]}
        await _edit(chat_id, message_id, "🔄 چند گیگابایت حجم اضافه می‌خوای؟ (اگه نمی‌خوای حجم اضافه کنی، عدد ۰ رو بفرست):", kb)
        return

    if data == "renew:cancel":
        _pending.pop(chat_id, None)
        title, sub, kb = _home_view(chat_id)
        await _edit(chat_id, message_id, f"{_t(chat_id,'gen_cancelled')}\n\n" + _join_ts(title, sub), kb)
        return

    if data == "renew:pay:wallet" or data == "renew:pay:card":
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "renew" or pending.get("step") != "confirm":
            await _answer_cb(cb_id, _t(chat_id, "step_invalid"), alert=True)
            return
        rdata = pending["data"]
        price = rdata["price"]
        if data == "renew:pay:wallet":
            if not _wallet_sub(chat_id, price):
                await _answer_cb(cb_id, "موجودی کیف پولت کافی نیست", alert=True)
                return
            uid, link = _apply_renewal(rdata["uid"], rdata["extra_gb"], rdata["extra_days"])
            await save_state()
            await _save_shop()
            _pending.pop(chat_id, None)
            await _deliver_config(chat_id, message_id, uid, link, "✅ مبلغ از کیف پولت کسر شد و کانفیگت تمدید شد!")
            await _send_sticker(chat_id, "success")
            return
        pending["step"] = "receipt"
        card = SHOP.get("card_number") or "—"
        owner = SHOP.get("card_owner") or "—"
        pay_text = (
            f"💳 مبلغ <b>{price:,} تومان</b> رو به شماره کارت زیر واریز کن:\n\n"
            f"<code>{card}</code>\nبه نام: {owner}\n\n"
            f"بعد از واریز، عکس رسید یا کد پیگیری رو همینجا بفرست 📎"
        )
        await _edit(chat_id, message_id, pay_text, {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "renew:cancel"}]]})
        return

    # ── آمار ادمین ───────────────────────────────────────────────────────────
    if data == "stats:home":
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        kb = {"inline_keyboard": [
            [{"text": "🔄 بروزرسانی", "callback_data": "stats:home"}],
            [{"text": _t(chat_id, "btn_back_home"), "callback_data": "home"}],
        ]}
        await _edit(chat_id, message_id, _admin_stats_text(), kb)
        return

    # ── ارسال پیام همگانی ────────────────────────────────────────────────────
    if data == "bcast:start":
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        _pending[chat_id] = {"action": "broadcast", "step": "text"}
        kb = {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "bcast:cancel"}]]}
        await _edit(chat_id, message_id, "📢 متن پیامی که می‌خوای برای همه‌ی کاربرا ارسال بشه رو بفرست:", kb)
        return

    if data == "bcast:cancel":
        _pending.pop(chat_id, None)
        title, sub, kb = _home_view(chat_id)
        await _edit(chat_id, message_id, f"{_t(chat_id,'gen_cancelled')}\n\n" + _join_ts(title, sub), kb)
        return

    # ── ارسال پیام مستقیم به یک کاربر خاص ────────────────────────────────────
    if data == "dm:start":
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        _pending[chat_id] = {"action": "dm_send", "step": "target", "data": {}}
        await _edit(chat_id, message_id, "✉️ آیدی عددی کاربری که می‌خوای بهش پیام بدی رو بفرست:",
                    {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "home"}]]})
        return

    if data == "dm:confirm":
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "dm_send" or pending.get("step") != "confirm" or not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "step_invalid"), alert=True)
            return
        wdata = pending["data"]
        target, dm_text = wdata["target"], wdata["text"]
        _pending.pop(chat_id, None)
        try:
            r = await _send(target, f"☎️ <b>پیام از تیم Matix</b>:\n\n{dm_text}")
            ok = bool(r and r.get("ok"))
        except Exception:
            ok = False
        if ok:
            await _edit(chat_id, message_id, f"✅ پیام برای <code>{target}</code> ارسال شد.", {"inline_keyboard": [[{"text": _t(chat_id, "btn_back_home"), "callback_data": "home"}]]})
        else:
            await _edit(chat_id, message_id, f"❗️ ارسال پیام به <code>{target}</code> ناموفق بود (شاید کاربر ربات رو بلاک کرده یا هنوز /start نزده).", {"inline_keyboard": [[{"text": _t(chat_id, "btn_back_home"), "callback_data": "home"}]]})
        return

    # ── بخش ادمین: لیست کامل کانفیگ‌ها ──────────────────────────────────────
    if data.startswith("list:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        page = int(data.split(":", 1)[1] or 0)
        if not LINKS:
            title, sub, kb = _home_view(chat_id)
            await _edit(chat_id, message_id, _t(chat_id, "list_empty"), kb)
            return
        await _edit(chat_id, message_id, _t(chat_id, "list_header", n=len(LINKS)), _links_list_kb(chat_id, page))
        return

    if data == "newcfg":
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        _pending[chat_id] = {"action": "admin_wizard", "step": "label", "data": {}}
        await _edit(chat_id, message_id, _wizard_prompt("label", {}), _wizard_cancel_kb(chat_id))
        return

    if data.startswith("toggle:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        uid = data.split(":", 1)[1]
        l = await set_link_active(uid, not LINKS.get(uid, {}).get("active", True))
        if not l:
            title, sub, kb = _home_view(chat_id)
            await _edit(chat_id, message_id, _t(chat_id, "not_exist"), kb)
            return
        await _edit(chat_id, message_id, _format_detail(uid, l), _link_detail_kb(chat_id, uid, l["active"]))
        return

    if data.startswith("del:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        uid = data.split(":", 1)[1]
        l = LINKS.get(uid)
        if not l:
            title, sub, kb = _home_view(chat_id)
            await _edit(chat_id, message_id, _t(chat_id, "not_exist"), kb)
            return
        await _edit(chat_id, message_id, _t(chat_id, "confirm_delete_q", label=l.get('label')), _confirm_delete_kb(chat_id, uid))
        return

    if data.startswith("delok:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        uid = data.split(":", 1)[1]
        label = await remove_link(uid)
        title, sub, kb = _home_view(chat_id)
        if label is None:
            await _edit(chat_id, message_id, _t(chat_id, "already_deleted"), kb)
        else:
            await _edit(chat_id, message_id, _t(chat_id, "deleted", label=label), kb)
        return

    # ── بخش ادمین: سفارش‌های در انتظار ─────────────────────────────────────
    if data.startswith("orders:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        page = int(data.split(":", 1)[1] or 0)
        kb, total = _orders_list_kb(chat_id, page)
        if total == 0:
            await _edit(chat_id, message_id, "📭 سفارش در انتظاری وجود نداره.", kb)
            return
        await _edit(chat_id, message_id, f"📥 سفارش‌های در انتظار ({total} مورد):", kb)
        return

    if data.startswith("ordview:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        order_id = data.split(":", 1)[1]
        order = SHOP["orders"].get(order_id)
        if not order:
            await _answer_cb(cb_id, "سفارش پیدا نشد", alert=True)
            return
        await _edit(chat_id, message_id, _order_summary(order), _order_admin_kb(order_id))
        return

    if data.startswith("ord:appr:") or data.startswith("ord:rej:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        order_id = data.rsplit(":", 1)[1]
        order = SHOP["orders"].get(order_id)
        if not order:
            await _edit(chat_id, message_id, "این سفارش دیگه وجود نداره.")
            return
        if order["status"] != "pending":
            await _answer_cb(cb_id, f"این سفارش قبلاً «{order['status']}» شده.", alert=True)
            return

        if data.startswith("ord:appr:"):
            order["status"] = "approved"
            if order.get("type") == "renew" and order.get("renew_uid") in LINKS:
                uid, link = _apply_renewal(order["renew_uid"], order.get("volume_gb", 0), order.get("days", 0))
                order["uid"] = uid
                await save_state()
                await _save_shop()
                await _deliver_config(order["chat_id"], None, uid, link, "✅ تمدید تایید شد و کانفیگت بروزرسانی شد!")
                await _send_sticker(order["chat_id"], "success")
                await _edit(chat_id, message_id, "✅ تمدید تایید و اعمال شد.\n\n" + _order_summary(order))
                return
            vol_bytes = parse_size_to_bytes(order["volume_gb"], "GB")
            expires_at = (datetime.now() + timedelta(days=order["days"])).isoformat() if order["days"] > 0 else None
            uid, link = await make_link(
                label=f"🛒 {order.get('username') or order['chat_id']}",
                limit_bytes=vol_bytes,
                expires_at=expires_at,
            )
            link["owner_chat_id"] = order["chat_id"]
            link["owner_username"] = order.get("username")
            link["source"] = "shop"
            order["uid"] = uid
            code = order.get("discount_code")
            if code and code in SHOP.get("discount_codes", {}):
                SHOP["discount_codes"][code]["used_count"] = SHOP["discount_codes"][code].get("used_count", 0) + 1
            await save_state()
            await _save_shop()

            r = await _send(order["chat_id"], "🎉 سفارشت تایید شد! در حال آماده‌سازی کانفیگت...")
            mid = (r or {}).get("result", {}).get("message_id")
            if mid:
                await _play_frames(order["chat_id"], mid, _GEN_FRAMES[_lg(order["chat_id"])])
            await _deliver_config(order["chat_id"], None, uid, link, "✅ کانفیگت آماده شد!")
            await _send_sticker(order["chat_id"], "success")
            await _announce_purchase(order)
            await _reward_referrer_if_first_order(order)
            await _edit(chat_id, message_id, "✅ تایید شد و کانفیگ به‌صورت خودکار برای مشتری ارسال شد.\n\n" + _order_summary(order))
        else:
            order["status"] = "rejected"
            await _save_shop()
            await _send(order["chat_id"], "❌ متاسفانه سفارشت تایید نشد. برای پیگیری با پشتیبانی در ارتباط باش.")
            await _send_sticker(order["chat_id"], "sad")
            await _edit(chat_id, message_id, "❌ سفارش رد شد و به مشتری اطلاع داده شد.\n\n" + _order_summary(order))
        return

    # ── بخش ادمین: تنظیمات فروش ────────────────────────────────────────────
    if data == "settings":
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        await _edit(chat_id, message_id, "⚙️ تنظیمات فروش — روی هرکدوم بزن تا تغییرش بدی:", _settings_kb(chat_id))
        return

    if data.startswith("set:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        field = data.split(":", 1)[1]
        _pending[chat_id] = {"action": "set_value", "field": field}
        prompts = {
            "price_per_gb": "💰 قیمت هر گیگابایت (تومان) رو بفرست:",
            "price_per_day": "📅 قیمت هر روز اعتبار (تومان) رو بفرست:",
            "min_price": "🔻 حداقل مبلغ قابل‌قبول سفارش (تومان) رو بفرست:",
            "trial_volume_gb": "🎁 حجم کانفیگ تست رایگان (گیگابایت) رو بفرست، مثلاً 0.2:",
            "trial_days": "🎁 مدت اعتبار کانفیگ تست رایگان (روز) رو بفرست:",
            "card_number": "💳 شماره کارت رو بفرست:",
            "card_owner": "👤 نام صاحب کارت رو بفرست:",
            "announce_channel": (
                "📣 آیدی عددی یا یوزرنیم کانال اعلان خرید رو بفرست، مثلاً <code>@mychannel</code> یا <code>-1001234567890</code>.\n"
                "⚠️ حتماً ربات باید ادمین همون کانال باشه تا بتونه پیام بفرسته."
            ),
            "required_channel": (
                "🔒 آیدی عددی یا یوزرنیم کانالی که عضویتش اجباریه رو بفرست، مثلاً <code>@mychannel</code> یا <code>-1001234567890</code>.\n"
                "⚠️ ربات باید ادمین همون کانال باشه تا بتونه عضویت رو چک کنه. برای حذف محدودیت، یه خط تیره (-) بفرست."
            ),
            "required_channel_url": "🔗 لینک عضویت در کانال اجباری رو بفرست (مثلاً https://t.me/mychannel):",
            "referral_bonus": "🤝 مبلغ پاداش رفرال (تومان) که بعد از اولین خرید هر زیرمجموعه به معرفش داده می‌شه رو بفرست:",
            "support_username": "☎️ یوزرنیم تلگرام پشتیبانی رو بفرست (بدون @)، مثلاً <code>matix_support</code>:",
            "channels": "📢 لیست کانال‌ها رو بفرست، هر خط یک کانال به فرم «اسم | لینک»، مثلاً:\n<code>کانال اصلی | https://t.me/matix_channel\nپشتیبانی | https://t.me/matix_support</code>",
            "spin_range": "🎲 بازه‌ی جایزه‌ی گردونه‌ی شانس (تومان) رو با کاما بفرست، مثلاً <code>2000,20000</code>:",
            "wallet_topup_presets": (
                "💰 مبالغ پیشنهادی شارژ کیف پول (تومان) رو با کاما جدا کن و بفرست.\n"
                "مثال: <code>50000,100000,200000,500000</code>"
            ),
        }
        await _edit(chat_id, message_id, prompts.get(field, "مقدار جدید رو بفرست:"))
        return

    # ── بخش ادمین: مدیریت کدهای تخفیف ──────────────────────────────────────
    if data.startswith("discounts:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        page = int(data.split(":", 1)[1] or 0)
        kb, total = _discounts_list_kb(chat_id, page)
        txt = f"🏷 کدهای تخفیف ({total} مورد):" if total else "🏷 هنوز کد تخفیفی نساختی."
        await _edit(chat_id, message_id, txt, kb)
        return

    if data == "disc:new":
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        _pending[chat_id] = {"action": "discount_wizard", "step": "code", "data": {}}
        await _edit(chat_id, message_id, "🏷 اسم/کد تخفیف رو بفرست (مثلاً OFF20):",
                    {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "disc:cancel"}]]})
        return

    if data == "disc:cancel":
        _pending.pop(chat_id, None)
        kb, total = _discounts_list_kb(chat_id, 0)
        await _edit(chat_id, message_id, f"{_t(chat_id,'gen_cancelled')}\n\n🏷 کدهای تخفیف ({total} مورد):", kb)
        return

    if data.startswith("disc:type:"):
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "discount_wizard" or pending.get("step") != "type":
            await _answer_cb(cb_id, _t(chat_id, "step_invalid"), alert=True)
            return
        pending["data"]["type"] = data.split(":", 2)[2]
        pending["step"] = "value"
        unit = "٪" if pending["data"]["type"] == "percent" else "تومان"
        await _edit(chat_id, message_id, f"🔢 مقدار تخفیف رو به {unit} بفرست:",
                    {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "disc:cancel"}]]})
        return

    if data == "disc:skip:maxuses":
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "discount_wizard" or pending.get("step") != "maxuses":
            await _answer_cb(cb_id, _t(chat_id, "step_invalid"), alert=True)
            return
        pending["data"]["max_uses"] = 0
        pending["step"] = "confirm"
        await _edit(chat_id, message_id, _discount_summary(pending["data"]), _discount_confirm_kb(chat_id))
        return

    if data == "disc:confirm":
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "discount_wizard" or pending.get("step") != "confirm":
            await _answer_cb(cb_id, _t(chat_id, "step_invalid"), alert=True)
            return
        d = pending["data"]
        code = d["code"]
        SHOP.setdefault("discount_codes", {})[code] = {
            "type": d["type"], "value": d["value"], "active": True,
            "max_uses": d.get("max_uses", 0), "used_count": 0, "expires_at": None,
        }
        await _save_shop()
        _pending.pop(chat_id, None)
        await _edit(chat_id, message_id, f"✅ کد تخفیف «{code}» ساخته شد.", _discount_detail_kb(chat_id, code, True))
        return

    if data.startswith("disc:view:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        code = data.split(":", 2)[2]
        d = SHOP.get("discount_codes", {}).get(code)
        if not d:
            kb, total = _discounts_list_kb(chat_id, 0)
            await _edit(chat_id, message_id, "این کد دیگه وجود نداره.", kb)
            return
        await _edit(chat_id, message_id, _discount_detail_text(code, d), _discount_detail_kb(chat_id, code, d.get("active", True)))
        return

    if data.startswith("disc:toggle:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        code = data.split(":", 2)[2]
        d = SHOP.get("discount_codes", {}).get(code)
        if not d:
            await _answer_cb(cb_id, "کد پیدا نشد", alert=True)
            return
        d["active"] = not d.get("active", True)
        await _save_shop()
        await _edit(chat_id, message_id, _discount_detail_text(code, d), _discount_detail_kb(chat_id, code, d["active"]))
        return

    if data.startswith("disc:delok:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        code = data.split(":", 2)[2]
        SHOP.get("discount_codes", {}).pop(code, None)
        await _save_shop()
        kb, total = _discounts_list_kb(chat_id, 0)
        await _edit(chat_id, message_id, f"🗑 کد «{code}» حذف شد.", kb)
        return

    if data.startswith("disc:del:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        code = data.split(":", 2)[2]
        if code not in SHOP.get("discount_codes", {}):
            kb, total = _discounts_list_kb(chat_id, 0)
            await _edit(chat_id, message_id, "این کد دیگه وجود نداره.", kb)
            return
        kb = {"inline_keyboard": [
            [{"text": "✅ بله، حذف کن", "callback_data": f"disc:delok:{code}"},
             {"text": "❌ انصراف", "callback_data": f"disc:view:{code}"}],
        ]}
        await _edit(chat_id, message_id, f"❗️ از حذف کد «{code}» مطمئنی؟", kb)
        return

    # ── کدهای هدیه (ادمین) ────────────────────────────────────────────────
    if data.startswith("giftcodes:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        page = int(data.split(":", 1)[1] or 0)
        kb, total = _giftcodes_list_kb(chat_id, page)
        await _edit(chat_id, message_id, f"🎁 کدهای هدیه ({total} مورد):", kb)
        return

    if data == "gift:new":
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        _pending[chat_id] = {"action": "gift_wizard", "step": "code", "data": {}}
        await _edit(chat_id, message_id, "🎁 اسم/کد هدیه رو بفرست (مثلاً WELCOME50):",
                    {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "gift:cancel"}]]})
        return

    if data == "gift:cancel":
        _pending.pop(chat_id, None)
        kb, total = _giftcodes_list_kb(chat_id, 0)
        await _edit(chat_id, message_id, f"{_t(chat_id,'gen_cancelled')}\n\n🎁 کدهای هدیه ({total} مورد):", kb)
        return

    if data == "gift:skip:maxuses":
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "gift_wizard" or pending.get("step") != "maxuses":
            await _answer_cb(cb_id, _t(chat_id, "step_invalid"), alert=True)
            return
        pending["data"]["max_uses"] = 0
        pending["step"] = "confirm"
        await _edit(chat_id, message_id, _giftcode_summary(pending["data"]), _giftcode_confirm_kb(chat_id))
        return

    if data == "gift:confirm":
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "gift_wizard" or pending.get("step") != "confirm":
            await _answer_cb(cb_id, _t(chat_id, "step_invalid"), alert=True)
            return
        d = pending["data"]
        code = d["code"]
        SHOP.setdefault("gift_codes", {})[code] = {
            "amount": d["amount"], "active": True,
            "max_uses": d.get("max_uses", 0), "used_count": 0, "redeemed_by": [],
        }
        await _save_shop()
        _pending.pop(chat_id, None)
        await _edit(chat_id, message_id, f"✅ کد هدیه «{code}» به مبلغ {d['amount']:,} تومان ساخته شد.", _giftcode_detail_kb(chat_id, code, True))
        return

    if data.startswith("gift:view:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        code = data.split(":", 2)[2]
        d = SHOP.get("gift_codes", {}).get(code)
        if not d:
            kb, total = _giftcodes_list_kb(chat_id, 0)
            await _edit(chat_id, message_id, "این کد دیگه وجود نداره.", kb)
            return
        await _edit(chat_id, message_id, _giftcode_detail_text(code, d), _giftcode_detail_kb(chat_id, code, d.get("active", True)))
        return

    if data.startswith("gift:toggle:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        code = data.split(":", 2)[2]
        d = SHOP.get("gift_codes", {}).get(code)
        if not d:
            await _answer_cb(cb_id, "کد پیدا نشد", alert=True)
            return
        d["active"] = not d.get("active", True)
        await _save_shop()
        await _edit(chat_id, message_id, _giftcode_detail_text(code, d), _giftcode_detail_kb(chat_id, code, d["active"]))
        return

    if data.startswith("gift:delok:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        code = data.split(":", 2)[2]
        SHOP.get("gift_codes", {}).pop(code, None)
        await _save_shop()
        kb, total = _giftcodes_list_kb(chat_id, 0)
        await _edit(chat_id, message_id, f"🗑 کد «{code}» حذف شد.", kb)
        return

    if data.startswith("gift:del:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        code = data.split(":", 2)[2]
        if code not in SHOP.get("gift_codes", {}):
            kb, total = _giftcodes_list_kb(chat_id, 0)
            await _edit(chat_id, message_id, "این کد دیگه وجود نداره.", kb)
            return
        kb = {"inline_keyboard": [
            [{"text": "✅ بله، حذف کن", "callback_data": f"gift:delok:{code}"},
             {"text": "❌ انصراف", "callback_data": f"gift:view:{code}"}],
        ]}
        await _edit(chat_id, message_id, f"❗️ از حذف کد «{code}» مطمئنی؟", kb)
        return

    # ── دریافت کد هدیه (مشتری) ───────────────────────────────────────────
    if data == "gift:redeem:start":
        _pending[chat_id] = {"action": "gift_redeem"}
        await _edit(chat_id, message_id, "🎁 کد هدیه‌ت رو بفرست:",
                    {"inline_keyboard": [[{"text": _t(chat_id, "btn_cancel"), "callback_data": "wallet:home"}]]})
        return

    # ── ساخت دستی (ادمین) ───────────────────────────────────────────────────
    if data == "w:cancel":
        _pending.pop(chat_id, None)
        title, sub, kb = _home_view(chat_id)
        await _edit(chat_id, message_id, f"{_t(chat_id, 'gen_cancelled')}\n\n" + _join_ts(title, sub), kb)
        return

    if data.startswith("w:"):
        if not _is_admin(chat_id):
            await _answer_cb(cb_id, _t(chat_id, "no_access_cb"), alert=True)
            return
        pending = _pending.get(chat_id)
        if not pending or pending.get("action") != "admin_wizard":
            title, sub, kb = _home_view(chat_id)
            await _edit(chat_id, message_id, _t(chat_id, "step_invalid"), kb)
            return

        step = pending["step"]
        wdata = pending["data"]

        if data.startswith("w:proto:") and step == "protocol":
            proto = data.split(":", 2)[2]
            wdata["protocol"] = proto if proto in PROTOCOLS else DEFAULT_PROTOCOL
            pending["step"] = "fingerprint"
            await _edit(chat_id, message_id, _wizard_prompt("fingerprint", wdata), _wizard_fp_kb(chat_id))
            return

        if data.startswith("w:fp:") and step == "fingerprint":
            fp = data.split(":", 2)[2]
            wdata["fingerprint"] = fp if fp in FINGERPRINTS else DEFAULT_FINGERPRINT
            pending["step"] = "alpn"
            await _edit(chat_id, message_id, _wizard_prompt("alpn", wdata), _wizard_alpn_kb(chat_id))
            return

        if data.startswith("w:alpnpreset:") and step == "alpn":
            code = data.split(":", 2)[2]
            wdata["alpn"] = ALPN_PRESET_MAP.get(code, "")
            pending["step"] = "port"
            await _edit(chat_id, message_id, _wizard_prompt("port", wdata), _wizard_skip_kb(chat_id, "port", f"⏭ پیش‌فرض ({DEFAULT_PORT})"))
            return

        if data == "w:skip:alpn" and step == "alpn":
            wdata["alpn"] = ""
            pending["step"] = "port"
            await _edit(chat_id, message_id, _wizard_prompt("port", wdata), _wizard_skip_kb(chat_id, "port", f"⏭ پیش‌فرض ({DEFAULT_PORT})"))
            return

        if data == "w:skip:port" and step == "port":
            wdata["port"] = DEFAULT_PORT
            pending["step"] = "volume"
            await _edit(chat_id, message_id, _wizard_prompt("volume", wdata), _wizard_unlimited_kb(chat_id, "volume"))
            return

        if data == "w:skip:volume" and step == "volume":
            wdata["limit_bytes"] = 0
            pending["step"] = "speed"
            await _edit(chat_id, message_id, _wizard_prompt("speed", wdata), _wizard_unlimited_kb(chat_id, "speed"))
            return

        if data == "w:skip:speed" and step == "speed":
            wdata["speed_limit_bytes"] = 0
            pending["step"] = "iplimit"
            await _edit(chat_id, message_id, _wizard_prompt("iplimit", wdata), _wizard_unlimited_kb(chat_id, "iplimit"))
            return

        if data == "w:skip:iplimit" and step == "iplimit":
            wdata["ip_limit"] = 0
            pending["step"] = "days"
            await _edit(chat_id, message_id, _wizard_prompt("days", wdata), _wizard_unlimited_kb(chat_id, "days"))
            return

        if data == "w:skip:days" and step == "days":
            wdata["expires_days"] = 0
            pending["step"] = "confirm"
            await _edit(chat_id, message_id, _wizard_summary(wdata), _wizard_confirm_kb(chat_id))
            return

        if data == "w:confirm" and step == "confirm":
            await _play_frames(chat_id, message_id, _GEN_FRAMES[_lg(chat_id)])
            expires_days = wdata.get("expires_days", 0)
            expires_at = (datetime.now() + timedelta(days=expires_days)).isoformat() if expires_days > 0 else None
            uid, link = await make_link(
                label=wdata.get("label") or "کانفیگ جدید",
                limit_bytes=wdata.get("limit_bytes", 0),
                expires_at=expires_at,
                protocol=wdata.get("protocol", DEFAULT_PROTOCOL),
                fingerprint=wdata.get("fingerprint", DEFAULT_FINGERPRINT),
                alpn=wdata.get("alpn", ""),
                port=wdata.get("port", DEFAULT_PORT),
                ip_limit=wdata.get("ip_limit", 0),
                speed_limit_bytes=wdata.get("speed_limit_bytes", 0),
            )
            link["source"] = "admin"
            _pending.pop(chat_id, None)
            await _edit(chat_id, message_id, _t(chat_id, "created_msg", detail=_format_detail(uid, link)), _link_detail_kb(chat_id, uid, link["active"]))
            return

        await _answer_cb(cb_id, _t(chat_id, "invalid_btn"))
        return

# ── Polling loop ─────────────────────────────────────────────────────────────
async def _poll_loop():
    global _running
    offset = 0
    logger.info(f"⭐ Matix bot polling started (admins: {len(ADMIN_IDS)})")
    while _running:
        try:
            res = await _call("getUpdates", offset=offset, timeout=30, allowed_updates=["message", "callback_query"])
            if not res or not res.get("ok"):
                await asyncio.sleep(3)
                continue
            for upd in res.get("result", []):
                offset = upd["update_id"] + 1
                try:
                    if "message" in upd:
                        await _handle_message(upd["message"])
                    elif "callback_query" in upd:
                        await _handle_callback(upd["callback_query"])
                except Exception as e:
                    logger.warning(f"Telegram update handling error: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Telegram poll loop error: {e}")
            await asyncio.sleep(3)

# ── Lifecycle ────────────────────────────────────────────────────────────────
async def start_bot():
    global _client, _poll_task, _running
    if not BOT_TOKEN:
        logger.info("Matix bot: TELEGRAM_BOT_TOKEN تنظیم نشده، ربات غیرفعاله.")
        return
    if not ADMIN_IDS:
        logger.warning("Matix bot: TELEGRAM_ADMIN_IDS تنظیم نشده، هیچ‌کس اجازه‌ی مدیریت نداره.")
    await _load_shop()
    _client = httpx.AsyncClient(timeout=httpx.Timeout(40.0, connect=10.0))
    _running = True
    _poll_task = asyncio.create_task(_poll_loop())

async def stop_bot():
    global _running, _client, _poll_task
    _running = False
    if _poll_task:
        _poll_task.cancel()
        try:
            await _poll_task
        except asyncio.CancelledError:
            pass
        _poll_task = None
    if _client:
        await _client.aclose()
        _client = None

async def restart_bot(token: str, admin_ids) -> tuple[bool, str | None]:
    """ربات رو با توکن/آیدی‌های ادمین جدید (که از پنل وب اومدن) عوض می‌کنه."""
    global _bot_username
    token = (token or "").strip()
    if not token:
        return False, "توکن نمی‌تواند خالی باشد"
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=8.0)) as c:
            r = await c.post(f"https://api.telegram.org/bot{token}/getMe")
            data = r.json()
    except Exception as e:
        return False, f"اتصال به تلگرام ناموفق بود: {e}"
    if not data.get("ok"):
        return False, data.get("description") or "توکن ربات نامعتبر است"
    username = data["result"].get("username")

    await stop_bot()
    configure(token, admin_ids)
    _bot_username = username
    await start_bot()
    return True, None
