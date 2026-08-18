# vless_relay.py
# ══════════════════════════════════════════════════════════════════════════════
# هسته‌ی تونل: پارس کردن هدر پروتکل VLESS و رله‌ی دوطرفه‌ی بایت‌ها بین
# WebSocket (کلاینت) و یک اتصال TCP واقعی (مقصد). این فایل کاملاً مستقل از
# پنل/دیتابیس/تلگرامه — هر پروژه‌ای می‌تونه همینو import کنه.
#
# ساختار هدر VLESS (نسخه‌ی 0، همونی که همه‌ی کلاینت‌ها مثل v2rayNG/NekoBox/
# Streisand می‌فرستن):
#   1 بایت   ورژن (باید 0 باشه)
#   16 بایت  UUID کاربر
#   1 بایت   طول addons (M) — معمولاً 0
#   M بایت   addons (نادیده گرفته می‌شه)
#   1 بایت   دستور: 1 = TCP، 2 = UDP (ما فقط TCP رو پشتیبانی می‌کنیم)
#   2 بایت   پورت مقصد (big-endian)
#   1 بایت   نوع آدرس: 1 = IPv4، 2 = دامنه، 3 = IPv6
#   N بایت   آدرس مقصد
#   ...      بقیه‌ی همون پیام = اولین تکه‌ی payload واقعی (اختیاری)
#
# پاسخ سرور (اولین پیام باینری که برمی‌گردونیم): 1 بایت ورژن + 1 بایت طول
# addons پاسخ (0) — از اونجا به بعد فقط بایت خام رد و بدل می‌شه.
# ══════════════════════════════════════════════════════════════════════════════

import asyncio
import struct
import uuid as uuid_lib
import logging

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger("vless_relay")

RELAY_BUF = 32 * 1024        # سایز بافر رله (بایت)
CONNECT_TIMEOUT = 10          # ثانیه، برای باز کردن اتصال TCP به مقصد


class VlessHeaderError(Exception):
    pass


def parse_vless_header(data: bytes):
    """هدر VLESS رو پارس می‌کنه و (uuid_str, target_host, target_port, remaining_payload) برمی‌گردونه."""
    if len(data) < 18:
        raise VlessHeaderError("داده خیلی کوتاهه")

    version = data[0]
    if version != 0:
        raise VlessHeaderError(f"نسخه‌ی پروتکل پشتیبانی نمی‌شه: {version}")

    raw_uuid = data[1:17]
    client_uuid = str(uuid_lib.UUID(bytes=raw_uuid))

    pos = 17
    addons_len = data[pos]
    pos += 1 + addons_len

    if len(data) < pos + 1:
        raise VlessHeaderError("هدر ناقصه (بعد از addons)")

    command = data[pos]
    pos += 1
    if command != 1:
        raise VlessHeaderError(f"فقط TCP (command=1) پشتیبانی می‌شه، دریافت شد: {command}")

    if len(data) < pos + 2:
        raise VlessHeaderError("هدر ناقصه (پورت)")
    port = struct.unpack(">H", data[pos:pos + 2])[0]
    pos += 2

    if len(data) < pos + 1:
        raise VlessHeaderError("هدر ناقصه (نوع آدرس)")
    addr_type = data[pos]
    pos += 1

    if addr_type == 1:  # IPv4
        if len(data) < pos + 4:
            raise VlessHeaderError("هدر ناقصه (IPv4)")
        host = ".".join(str(b) for b in data[pos:pos + 4])
        pos += 4
    elif addr_type == 2:  # دامنه
        if len(data) < pos + 1:
            raise VlessHeaderError("هدر ناقصه (طول دامنه)")
        dlen = data[pos]
        pos += 1
        if len(data) < pos + dlen:
            raise VlessHeaderError("هدر ناقصه (دامنه)")
        host = data[pos:pos + dlen].decode("utf-8", errors="ignore")
        pos += dlen
    elif addr_type == 3:  # IPv6
        if len(data) < pos + 16:
            raise VlessHeaderError("هدر ناقصه (IPv6)")
        raw6 = data[pos:pos + 16]
        host = ":".join(f"{raw6[i]:02x}{raw6[i+1]:02x}" for i in range(0, 16, 2))
        pos += 16
    else:
        raise VlessHeaderError(f"نوع آدرس نامعتبر: {addr_type}")

    remaining_payload = data[pos:]
    return client_uuid, host, port, remaining_payload


def build_vless_response_header() -> bytes:
    """اولین پیام باینری که بعد از قبول اتصال به کلاینت برمی‌گردونیم."""
    return bytes([0, 0])  # ورژن 0، طول addons پاسخ = 0


async def relay_ws_to_tcp(ws: WebSocket, writer: asyncio.StreamWriter, on_bytes=None):
    """هر چی از WebSocket میاد رو به سوکت TCP مقصد می‌ریزه."""
    try:
        while True:
            msg = await ws.receive_bytes()
            if not msg:
                continue
            writer.write(msg)
            await writer.drain()
            if on_bytes:
                on_bytes(len(msg))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug(f"ws->tcp relay ended: {e}")
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def relay_tcp_to_ws(ws: WebSocket, reader: asyncio.StreamReader, on_bytes=None):
    """هر چی از سوکت TCP مقصد میاد رو به WebSocket کلاینت می‌فرسته."""
    try:
        while True:
            chunk = await reader.read(RELAY_BUF)
            if not chunk:
                break
            await ws.send_bytes(chunk)
            if on_bytes:
                on_bytes(len(chunk))
    except Exception as e:
        logger.debug(f"tcp->ws relay ended: {e}")
    finally:
        try:
            await ws.close()
        except Exception:
            pass


async def run_tunnel(ws: WebSocket, is_uuid_allowed, on_bytes=None):
    """
    یک تونل کامل VLESS-over-WebSocket رو اجرا می‌کنه.

    is_uuid_allowed(uuid_str) -> bool | باید synchronous باشه و سریع (فقط چک روی دیکشنری در حافظه).
    on_bytes(uuid_str, n) -> اختیاری، برای ثبت آمار مصرف صدا زده می‌شه.
    """
    await ws.accept()
    try:
        first = await ws.receive_bytes()
    except WebSocketDisconnect:
        return

    try:
        client_uuid, host, port, remaining = parse_vless_header(first)
    except VlessHeaderError as e:
        logger.warning(f"هدر VLESS نامعتبر: {e}")
        await ws.close(code=4000)
        return

    if not is_uuid_allowed(client_uuid):
        logger.info(f"UUID رد شد (غیرمجاز/غیرفعال/منقضی): {client_uuid}")
        await ws.close(code=4001)
        return

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=CONNECT_TIMEOUT
        )
    except Exception as e:
        logger.warning(f"اتصال به مقصد {host}:{port} شکست خورد: {e}")
        await ws.close(code=4002)
        return

    await ws.send_bytes(build_vless_response_header())
    if remaining:
        writer.write(remaining)
        await writer.drain()

    def _count(n: int):
        if on_bytes:
            on_bytes(client_uuid, n)

    await asyncio.gather(
        relay_ws_to_tcp(ws, writer, on_bytes=_count),
        relay_tcp_to_ws(ws, reader, on_bytes=_count),
        return_exceptions=True,
    )
