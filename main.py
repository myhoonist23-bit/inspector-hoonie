import asyncio
import hashlib
import html
import json
import logging
import os
import re
import signal
import tempfile
from datetime import datetime, timezone

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import CommandStart
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    ChatJoinRequest,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from dotenv import load_dotenv

try:
    from PIL import Image
    import pytesseract

    HAS_OCR = True
except Exception:
    Image = None
    pytesseract = None
    HAS_OCR = False

# ================= CONFIG =================

load_dotenv()

BOT_TOKEN = (
    os.getenv("BOT_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("TELEGRAM_TOKEN")
    or ""
).strip()
GROUP_ID = int(os.getenv("GROUP_ID", "0"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip()
}
EXTRA_CHECK_CHAT_IDS = [
    int(x.strip())
    for x in os.getenv("CHECK_CHAT_IDS", "").split(",")
    if x.strip() and x.strip() != "0"
]
DB_PATH = os.getenv("DB_PATH", "applications.db")
TESSERACT_CMD = os.getenv("TESSERACT_CMD", "").strip()
if HAS_OCR and TESSERACT_CMD:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is missing")

if GROUP_ID == 0:
    raise ValueError("GROUP_ID is missing")

if not ADMIN_IDS:
    raise ValueError("ADMIN_IDS is missing")

QUESTIONS = [
    "1/5 — Group join ရတဲ့အကြောင်းအရင်း",
    "2/5 — Engene ဖြစ်တာဘယ်လောက်ကြာပြီလဲ?",
    "3/5 — Bias နဲ့ Bias wrecker ဘယ်သူလဲ?",
    "4/5 — Fav Enha song နဲ့အဲထဲက Lyric တစ်ကြောင်း",
    "5/5 - Group Link ဘယ်ကရခဲ့တာလဲ? gp ထဲရှိတဲ့တစ်ယောက်ယောက်က invite ခဲ့တယ်ဆို invite တဲ့သူရဲ့ tg username ပြောပေးပါ",
]
TOTAL_TEXT_QUESTIONS = len(QUESTIONS)
MNET_ACC_STEP = TOTAL_TEXT_QUESTIONS          # index 5: Mnet Plus Account Name
CERT_PHOTO_STEP = TOTAL_TEXT_QUESTIONS + 1    # index 6: 2026 MAMA 2nd round voting certificate Photo
TOTAL_STEPS = TOTAL_TEXT_QUESTIONS + 2        # 0..6

ACTIVE_STATUSES = ("asking", "awaiting_review")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

dp = Dispatcher()

# ================= DATABASE =================


async def get_db():
    return await aiosqlite.connect(DB_PATH)


async def init_db():
    db = await get_db()
    try:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS applications (
            user_id INTEGER PRIMARY KEY,
            chat_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT NOT NULL,
            first_name TEXT,
            last_name TEXT,
            phone_number TEXT,
            name_history TEXT DEFAULT '[]',
            username_history TEXT DEFAULT '[]',
            language_code TEXT,
            is_premium INTEGER DEFAULT 0,
            profile_photos_count INTEGER DEFAULT 0,
            profile_photo_file_id TEXT,
            invite_link_info TEXT,
            bio TEXT,
            personal_chat TEXT,
            group_status TEXT,
            channel_status TEXT,
            checked_chats_json TEXT DEFAULT '{}',
            status TEXT NOT NULL,
            question_index INTEGER DEFAULT 0,
            answer_1 TEXT,
            answer_2 TEXT,
            answer_3 TEXT,
            answer_4 TEXT,
            answer_5 TEXT,
            mnet_account TEXT,
            certificate_photo_file_id TEXT,
            certificate_no TEXT,
            certificate_date TEXT,
            certificate_sha256 TEXT,
            duplicate_warning TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS used_certificates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            certificate_no TEXT,
            image_sha256 TEXT NOT NULL,
            first_user_id INTEGER NOT NULL,
            first_mnet_account TEXT,
            certificate_date TEXT,
            created_at TEXT NOT NULL
        )
        """)

        cursor = await db.execute("PRAGMA table_info(applications)")
        existing_cols = {row[1] for row in await cursor.fetchall()}
        extra_columns = {
            "first_name": "TEXT",
            "last_name": "TEXT",
            "phone_number": "TEXT",
            "name_history": "TEXT DEFAULT '[]'",
            "username_history": "TEXT DEFAULT '[]'",
            "language_code": "TEXT",
            "is_premium": "INTEGER DEFAULT 0",
            "profile_photos_count": "INTEGER DEFAULT 0",
            "profile_photo_file_id": "TEXT",
            "invite_link_info": "TEXT",
            "bio": "TEXT",
            "personal_chat": "TEXT",
            "group_status": "TEXT",
            "channel_status": "TEXT",
            "checked_chats_json": "TEXT DEFAULT '{}'",
            "answer_5": "TEXT",
            "mnet_account": "TEXT",
            "certificate_photo_file_id": "TEXT",
            "certificate_no": "TEXT",
            "certificate_date": "TEXT",
            "certificate_sha256": "TEXT",
            "duplicate_warning": "TEXT",
        }
        for col_name, col_type in extra_columns.items():
            if col_name not in existing_cols:
                await db.execute(
                    f"ALTER TABLE applications ADD COLUMN {col_name} {col_type}"
                )

        await db.commit()
    finally:
        await db.close()


def current_time():
    return datetime.now(timezone.utc).isoformat()


def _parse_history(raw_json: str | None) -> list[str]:
    if not raw_json:
        return []
    try:
        data = json.loads(raw_json)
        return [str(x) for x in data if x]
    except Exception:
        return []


def _parse_dict(raw_json: str | None) -> dict:
    if not raw_json:
        return {}
    try:
        data = json.loads(raw_json)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _append_unique_history(history: list[str], new_value: str | None) -> list[str]:
    if not new_value:
        return history
    cleaned = new_value.strip()
    if not cleaned:
        return history
    if cleaned not in history:
        history.append(cleaned)
    return history


def _get_step_prompt(index: int) -> str:
    if index < TOTAL_TEXT_QUESTIONS:
        return QUESTIONS[index]
    if index == MNET_ACC_STEP:
        return "Mnet Plus acc name ပို့ပေးပါ"
    return "2026 MAMA 2nd round voting certificate ပြပေးပါ"


def parse_certificate_info(image_path: str) -> dict:
    with open(image_path, "rb") as f:
        img_bytes = f.read()
    sha256_hash = hashlib.sha256(img_bytes).hexdigest()

    cert_no = None
    cert_date = None
    raw_text = ""

    if HAS_OCR:
        try:
            with Image.open(image_path) as img:
                raw_text = pytesseract.image_to_string(img.convert("RGB")) or ""
        except Exception as error:
            logging.debug("OCR skipped/failed: %s", error)

    if raw_text:
        no_match = re.search(
            r"\b(?:No\.?|Certificate\s*No\.?|Cert\s*No\.?)\s*[:\-]?\s*([A-Z0-9]{6,30})\b",
            raw_text,
            re.IGNORECASE,
        )
        if no_match:
            cert_no = no_match.group(1).upper().strip()

        date_match = re.search(
            r"\b(20\d{2}[./-]\d{1,2}[./-]\d{1,2}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)\b",
            raw_text,
        )
        if date_match:
            cert_date = date_match.group(1).strip()

    if not cert_no:
        cert_no = f"IMG-{sha256_hash[:12].upper()}"

    if not cert_date:
        cert_date = datetime.now(timezone.utc).strftime("%Y.%m.%d %H:%M")

    return {
        "sha256": sha256_hash,
        "certificate_no": cert_no,
        "certificate_date": cert_date,
    }


async def check_and_record_certificate(
    user_id: int,
    mnet_account: str | None,
    cert_no: str | None,
    cert_date: str | None,
    sha256_hash: str,
) -> str | None:
    warnings: list[str] = []
    db = await get_db()
    try:
        db.row_factory = aiosqlite.Row

        if cert_no:
            cursor = await db.execute(
                """
                SELECT first_user_id, first_mnet_account
                FROM used_certificates
                WHERE UPPER(certificate_no) = UPPER(?)
                  AND first_user_id != ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (cert_no, user_id),
            )
            dup_by_no = await cursor.fetchone()
            if dup_by_no:
                warnings.append(
                    f"Cert No ({cert_no}) already used by User ID {dup_by_no['first_user_id']}"
                    + (
                        f" [{dup_by_no['first_mnet_account']}]"
                        if dup_by_no["first_mnet_account"]
                        else ""
                    )
                )

        cursor = await db.execute(
            """
            SELECT first_user_id, first_mnet_account, certificate_no
            FROM used_certificates
            WHERE image_sha256 = ?
              AND first_user_id != ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (sha256_hash, user_id),
        )
        dup_by_hash = await cursor.fetchone()
        if dup_by_hash:
            warnings.append(
                f"Same image already used by User ID {dup_by_hash['first_user_id']}"
                + (
                    f" [{dup_by_hash['first_mnet_account']}]"
                    if dup_by_hash["first_mnet_account"]
                    else ""
                )
            )

        cursor = await db.execute(
            "SELECT id FROM used_certificates WHERE first_user_id = ? ORDER BY id ASC LIMIT 1",
            (user_id,),
        )
        own_rec = await cursor.fetchone()
        now = current_time()
        if own_rec:
            await db.execute(
                """
                UPDATE used_certificates
                SET certificate_no = COALESCE(?, certificate_no),
                    image_sha256 = ?,
                    first_mnet_account = COALESCE(?, first_mnet_account),
                    certificate_date = COALESCE(?, certificate_date),
                    created_at = ?
                WHERE id = ?
                """,
                (cert_no, sha256_hash, mnet_account, cert_date, now, own_rec["id"]),
            )
        else:
            await db.execute(
                """
                INSERT INTO used_certificates (
                    certificate_no, image_sha256, first_user_id,
                    first_mnet_account, certificate_date, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (cert_no, sha256_hash, user_id, mnet_account, cert_date, now),
            )

        await db.commit()
    finally:
        await db.close()

    return " | ".join(warnings) if warnings else None


async def _check_single_chat_membership(bot: Bot, chat_id: int, user_id: int) -> str:
    label = str(chat_id)
    status = "unknown"
    try:
        chat_obj = await bot.get_chat(chat_id)
        title = getattr(chat_obj, "title", None)
        uname = getattr(chat_obj, "username", None)
        if title and uname:
            label = f"{title} (@{uname})"
        elif title:
            label = f"{title} ({chat_id})"
    except TelegramBadRequest:
        pass

    try:
        member = await bot.get_chat_member(chat_id, user_id)
        status = getattr(member, "status", "unknown")
    except TelegramBadRequest as error:
        status = f"check_failed ({error.message})"
        logging.debug(
            "get_chat_member failed for chat %s user %s: %s",
            chat_id,
            user_id,
            error,
        )

    return f"{label} -> {status}"


async def fetch_extra_user_info(
    bot: Bot, user, event: ChatJoinRequest | None = None
) -> dict:
    user_id = user.id
    first_name = getattr(user, "first_name", None)
    last_name = getattr(user, "last_name", None)
    full_name = (
        getattr(user, "full_name", None)
        or " ".join([p for p in (first_name, last_name) if p])
        or str(user_id)
    )
    username = getattr(user, "username", None)
    language_code = getattr(user, "language_code", None)
    is_premium = 1 if getattr(user, "is_premium", False) else 0

    bio = None
    personal_chat = None
    group_status = "unknown"
    channel_status = "n/a"
    profile_photos_count = 0
    profile_photo_file_id = None
    invite_link_info = None
    checked_chats: dict[str, str] = {}

    if event and getattr(event, "invite_link", None):
        inv = event.invite_link
        parts = []
        if getattr(inv, "name", None):
            parts.append(f"Name: {inv.name}")
        if getattr(inv, "invite_link", None):
            parts.append(inv.invite_link)
        if getattr(inv, "creator", None):
            creator_name = inv.creator.full_name
            creator_u = f" (@{inv.creator.username})" if inv.creator.username else ""
            parts.append(f"By: {creator_name}{creator_u} [{inv.creator.id}]")
        invite_link_info = " | ".join(parts) if parts else None

    try:
        chat = await bot.get_chat(user_id)
        bio = getattr(chat, "bio", None)
        if getattr(chat, "username", None) is not None:
            username = chat.username
        if getattr(chat, "first_name", None):
            first_name = chat.first_name
        if getattr(chat, "last_name", None) is not None:
            last_name = chat.last_name
        updated_full = " ".join([p for p in (first_name, last_name) if p]).strip()
        if updated_full:
            full_name = updated_full
        p_chat = getattr(chat, "personal_chat", None)
        if p_chat:
            if p_chat.username:
                personal_chat = f"@{p_chat.username}"
            else:
                personal_chat = f"{p_chat.title or 'Channel'} ({p_chat.id})"
    except TelegramBadRequest as error:
        logging.debug("get_chat failed for %s: %s", user_id, error)

    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
        profile_photos_count = getattr(photos, "total_count", 0) or 0
        if photos.photos and len(photos.photos) > 0 and len(photos.photos[0]) > 0:
            profile_photo_file_id = photos.photos[0][-1].file_id
    except TelegramBadRequest as error:
        logging.debug("get_user_profile_photos failed for %s: %s", user_id, error)

    try:
        member = await bot.get_chat_member(GROUP_ID, user_id)
        group_status = member.status
    except TelegramBadRequest as error:
        logging.debug("get_chat_member (group) failed for %s: %s", user_id, error)

    if CHANNEL_ID != 0:
        try:
            ch_member = await bot.get_chat_member(CHANNEL_ID, user_id)
            channel_status = ch_member.status
        except TelegramBadRequest as error:
            logging.debug("get_chat_member (channel) failed for %s: %s", user_id, error)

    chats_to_check = []
    for cid in [GROUP_ID, CHANNEL_ID, *EXTRA_CHECK_CHAT_IDS]:
        if cid != 0 and cid not in chats_to_check:
            chats_to_check.append(cid)

    for cid in chats_to_check:
        checked_chats[str(cid)] = await _check_single_chat_membership(bot, cid, user_id)

    return {
        "full_name": full_name,
        "username": username,
        "first_name": first_name,
        "last_name": last_name,
        "language_code": language_code,
        "is_premium": is_premium,
        "profile_photos_count": profile_photos_count,
        "profile_photo_file_id": profile_photo_file_id,
        "invite_link_info": invite_link_info,
        "bio": bio,
        "personal_chat": personal_chat,
        "group_status": group_status,
        "channel_status": channel_status,
        "checked_chats": checked_chats,
    }


async def notify_admins_profile_change(
    bot: Bot,
    user_id: int,
    old_name: str,
    new_name: str,
    old_username: str | None,
    new_username: str | None,
):
    old_u_text = f"@{old_username}" if old_username else "None"
    new_u_text = f"@{new_username}" if new_username else "None"

    lines = [
        "<b>USER PROFILE CHANGE DETECTED</b>",
        "--------------------",
        f"<b>User ID:</b> <code>{user_id}</code>",
    ]
    if old_name != new_name:
        lines.append(
            f"<b>Acc Name Changed:</b> {html.escape(old_name)} -> {html.escape(new_name)}"
        )
    if (old_username or "") != (new_username or ""):
        lines.append(
            f"<b>Username Changed:</b> {html.escape(old_u_text)} -> {html.escape(new_u_text)}"
        )

    text = "\n".join(lines)
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except (TelegramForbiddenError, TelegramBadRequest) as error:
            logging.warning(
                "Failed to send profile change alert to admin %s: %s",
                admin_id,
                error,
            )


async def notify_admins_member_left(
    bot: Bot,
    user,
    new_status: str,
):
    full_name = (
        getattr(user, "full_name", None)
        or " ".join(
            [
                p
                for p in (
                    getattr(user, "first_name", None),
                    getattr(user, "last_name", None),
                )
                if p
            ]
        )
        or str(user.id),
    )
    username_text = f"@{user.username}" if getattr(user, "username", None) else "Not set"
    action_label = "Kicked / Removed" if new_status == "kicked" else "Left Group"

    text = (
        "<b>GROUP MEMBER LEFT ALERT</b>\n"
        "--------------------\n"
        f"<b>Acc Name:</b> {html.escape(full_name)}\n"
        f"<b>Username:</b> {html.escape(username_text)}\n"
        f"<b>User ID:</b> <code>{user.id}</code>\n"
        f"<b>Group ID:</b> <code>{GROUP_ID}</code>\n"
        f"<b>Status:</b> {html.escape(action_label)} ({html.escape(new_status)})"
    )

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except (TelegramForbiddenError, TelegramBadRequest) as error:
            logging.warning(
                "Failed to send member left alert to admin %s: %s",
                admin_id,
                error,
            )


async def record_seen_user(
    bot: Bot,
    user,
    event: ChatJoinRequest | None = None,
    shared_phone: str | None = None,
):
    info = await fetch_extra_user_info(bot, user, event)
    now = current_time()
    db = await get_db()
    try:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM applications WHERE user_id=?",
            (user.id,),
        )
        existing = await cursor.fetchone()

        name_hist = _parse_history(existing["name_history"]) if existing else []
        user_hist = _parse_history(existing["username_history"]) if existing else []

        name_changed = False
        username_changed = False
        old_name = existing["full_name"] if existing else ""
        old_username = existing["username"] if existing else None

        if existing:
            if existing["full_name"] and existing["full_name"] != info["full_name"]:
                name_changed = True
            if (existing["username"] or "") != (info["username"] or ""):
                username_changed = True

            name_hist = _append_unique_history(name_hist, existing["full_name"])
            if existing["username"]:
                user_hist = _append_unique_history(user_hist, f"@{existing['username']}")

        name_hist = _append_unique_history(name_hist, info["full_name"])
        if info["username"]:
            user_hist = _append_unique_history(user_hist, f"@{info['username']}")

        checked_json = json.dumps(info.get("checked_chats", {}), ensure_ascii=False)
        phone_to_store = shared_phone or (
            existing["phone_number"]
            if existing and "phone_number" in existing.keys()
            else None
        )

        if not existing:
            await db.execute(
                """
                INSERT INTO applications (
                    user_id, chat_id, username, full_name,
                    first_name, last_name, phone_number,
                    name_history, username_history,
                    language_code, is_premium, profile_photos_count, profile_photo_file_id,
                    invite_link_info, bio, personal_chat, group_status, channel_status,
                    checked_chats_json, status, question_index,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'idle', 0, ?, ?)
                """,
                (
                    user.id,
                    user.id,
                    info["username"],
                    info["full_name"],
                    info["first_name"],
                    info["last_name"],
                    phone_to_store,
                    json.dumps(name_hist, ensure_ascii=False),
                    json.dumps(user_hist, ensure_ascii=False),
                    info["language_code"],
                    info["is_premium"],
                    info["profile_photos_count"],
                    info["profile_photo_file_id"],
                    info["invite_link_info"],
                    info["bio"],
                    info["personal_chat"],
                    info["group_status"],
                    info["channel_status"],
                    checked_json,
                    now,
                    now,
                ),
            )
        else:
            await db.execute(
                """
                UPDATE applications
                SET username=?, full_name=?, first_name=?, last_name=?,
                    phone_number=COALESCE(?, phone_number),
                    name_history=?, username_history=?,
                    language_code=?, is_premium=?, profile_photos_count=?,
                    profile_photo_file_id=COALESCE(?, profile_photo_file_id),
                    invite_link_info=COALESCE(?, invite_link_info),
                    bio=?, personal_chat=?, group_status=?, channel_status=?,
                    checked_chats_json=?, updated_at=?
                WHERE user_id=?
                """,
                (
                    info["username"],
                    info["full_name"],
                    info["first_name"],
                    info["last_name"],
                    phone_to_store,
                    json.dumps(name_hist, ensure_ascii=False),
                    json.dumps(user_hist, ensure_ascii=False),
                    info["language_code"],
                    info["is_premium"],
                    info["profile_photos_count"],
                    info["profile_photo_file_id"],
                    info["invite_link_info"],
                    info["bio"],
                    info["personal_chat"],
                    info["group_status"],
                    info["channel_status"],
                    checked_json,
                    now,
                    user.id,
                ),
            )

        await db.commit()

        if existing and (name_changed or username_changed):
            await notify_admins_profile_change(
                bot=bot,
                user_id=user.id,
                old_name=old_name,
                new_name=info["full_name"],
                old_username=old_username,
                new_username=info["username"],
            )

        return info
    finally:
        await db.close()


async def get_application(user_id: int):
    db = await get_db()
    try:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM applications WHERE user_id=?",
            (user_id,),
        )
        return await cursor.fetchone()
    finally:
        await db.close()


async def create_application(user, chat_id: int, extra_info: dict):
    now = current_time()
    db = await get_db()
    try:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM applications WHERE user_id=?",
            (user.id,),
        )
        old = await cursor.fetchone()

        if old and old["status"] in ACTIVE_STATUSES:
            return False

        name_hist = _parse_history(old["name_history"]) if old else []
        user_hist = _parse_history(old["username_history"]) if old else []
        existing_phone = (
            old["phone_number"]
            if old and "phone_number" in old.keys()
            else None
        )

        if old:
            name_hist = _append_unique_history(name_hist, old["full_name"])
            if old["username"]:
                user_hist = _append_unique_history(user_hist, f"@{old['username']}")

        name_hist = _append_unique_history(name_hist, extra_info["full_name"])
        if extra_info["username"]:
            user_hist = _append_unique_history(user_hist, f"@{extra_info['username']}")

        checked_json = json.dumps(extra_info.get("checked_chats", {}), ensure_ascii=False)

        await db.execute(
            """
            INSERT INTO applications (
                user_id, chat_id, username, full_name,
                first_name, last_name, phone_number,
                name_history, username_history,
                language_code, is_premium,
                profile_photos_count, profile_photo_file_id, invite_link_info,
                bio, personal_chat, group_status, channel_status, checked_chats_json,
                status, question_index,
                answer_1, answer_2, answer_3, answer_4, answer_5,
                mnet_account, certificate_photo_file_id, certificate_no,
                certificate_date, certificate_sha256, duplicate_warning,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'asking', 0,
                    NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)

            ON CONFLICT(user_id) DO UPDATE SET
                chat_id=excluded.chat_id,
                username=excluded.username,
                full_name=excluded.full_name,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                phone_number=COALESCE(excluded.phone_number, applications.phone_number),
                name_history=excluded.name_history,
                username_history=excluded.username_history,
                language_code=excluded.language_code,
                is_premium=excluded.is_premium,
                profile_photos_count=excluded.profile_photos_count,
                profile_photo_file_id=COALESCE(excluded.profile_photo_file_id, applications.profile_photo_file_id),
                invite_link_info=excluded.invite_link_info,
                bio=excluded.bio,
                personal_chat=excluded.personal_chat,
                group_status=excluded.group_status,
                channel_status=excluded.channel_status,
                checked_chats_json=excluded.checked_chats_json,
                status='asking',
                question_index=0,
                answer_1=NULL,
                answer_2=NULL,
                answer_3=NULL,
                answer_4=NULL,
                answer_5=NULL,
                mnet_account=NULL,
                certificate_photo_file_id=NULL,
                certificate_no=NULL,
                certificate_date=NULL,
                certificate_sha256=NULL,
                duplicate_warning=NULL,
                created_at=excluded.created_at,
                updated_at=excluded.updated_at
            """,
            (
                user.id,
                chat_id,
                extra_info.get("username"),
                extra_info.get("full_name"),
                extra_info.get("first_name"),
                extra_info.get("last_name"),
                existing_phone,
                json.dumps(name_hist, ensure_ascii=False),
                json.dumps(user_hist, ensure_ascii=False),
                extra_info.get("language_code"),
                extra_info.get("is_premium", 0),
                extra_info.get("profile_photos_count", 0),
                extra_info.get("profile_photo_file_id"),
                extra_info.get("invite_link_info"),
                extra_info.get("bio"),
                extra_info.get("personal_chat"),
                extra_info.get("group_status"),
                extra_info.get("channel_status"),
                checked_json,
                now,
                now,
            ),
        )

        await db.commit()
        return True
    finally:
        await db.close()


async def save_text_step(user_id: int, index: int, value: str):
    db = await get_db()
    try:
        if index < TOTAL_TEXT_QUESTIONS:
            column = f"answer_{index + 1}"
            cursor = await db.execute(
                f"""
                UPDATE applications
                SET {column}=?, question_index=?, updated_at=?
                WHERE user_id=?
                  AND status='asking'
                  AND question_index=?
                """,
                (value, index + 1, current_time(), user_id, index),
            )
        elif index == MNET_ACC_STEP:
            cursor = await db.execute(
                """
                UPDATE applications
                SET mnet_account=?, question_index=?, updated_at=?
                WHERE user_id=?
                  AND status='asking'
                  AND question_index=?
                """,
                (value, index + 1, current_time(), user_id, index),
            )
        else:
            return False

        await db.commit()
        return cursor.rowcount == 1
    finally:
        await db.close()


async def save_certificate_step(
    user_id: int,
    photo_file_id: str,
    cert_no: str | None,
    cert_date: str | None,
    sha256_hash: str,
    dup_warning: str | None,
):
    db = await get_db()
    try:
        cursor = await db.execute(
            """
            UPDATE applications
            SET certificate_photo_file_id=?,
                certificate_no=?,
                certificate_date=?,
                certificate_sha256=?,
                duplicate_warning=?,
                question_index=?,
                status='awaiting_review',
                updated_at=?
            WHERE user_id=?
              AND status='asking'
              AND question_index=?
            """,
            (
                photo_file_id,
                cert_no,
                cert_date,
                sha256_hash,
                dup_warning,
                TOTAL_STEPS,
                current_time(),
                user_id,
                CERT_PHOTO_STEP,
            ),
        )
        await db.commit()
        return cursor.rowcount == 1
    finally:
        await db.close()


async def update_status(user_id: int, status: str):
    db = await get_db()
    try:
        await db.execute(
            """
            UPDATE applications
            SET status=?, updated_at=?
            WHERE user_id=?
            """,
            (status, current_time(), user_id),
        )
        await db.commit()
    finally:
        await db.close()


# ================= KEYBOARDS =================


def admin_keyboard(user_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="APPROVE",
                    callback_data=f"app:approve:{user_id}",
                ),
                InlineKeyboardButton(
                    text="DECLINE",
                    callback_data=f"app:decline:{user_id}",
                ),
            ]
        ]
    )


# ================= APPLICATION CARD (NO EMOJI) =================


def make_application_card(app):
    username = f"@{html.escape(app['username'])}" if app["username"] else "Not set"
    phone_val = (
        app["phone_number"]
        if "phone_number" in app.keys() and app["phone_number"]
        else None
    )
    phone_text = (
        html.escape(phone_val)
        if phone_val
        else "Not available via Bot API (unless shared)"
    )
    personal_chat = (
        html.escape(app["personal_chat"]) if app["personal_chat"] else "None"
    )

    checked_dict = _parse_dict(app["checked_chats_json"])
    if checked_dict:
        group_lines = "\n".join(
            f"- {html.escape(str(v))}" for v in checked_dict.values()
        )
    else:
        group_lines = f"- Group ({GROUP_ID}) -> {html.escape(app['group_status'] or 'unknown')}"

    mnet_acc = (
        html.escape(app["mnet_account"])
        if "mnet_account" in app.keys() and app["mnet_account"]
        else "Not provided"
    )
    cert_no = (
        html.escape(app["certificate_no"])
        if "certificate_no" in app.keys() and app["certificate_no"]
        else "N/A"
    )
    cert_date = (
        html.escape(app["certificate_date"])
        if "certificate_date" in app.keys() and app["certificate_date"]
        else datetime.now(timezone.utc).strftime("%Y.%m.%d %H:%M")
    )
    dup_warn = (
        app["duplicate_warning"]
        if "duplicate_warning" in app.keys() and app["duplicate_warning"]
        else None
    )
    dup_line = (
        f"<b>Duplicate Alert:</b> {html.escape(dup_warn)}\n"
        if dup_warn
        else "<b>Duplicate Alert:</b> None\n"
    )

    return (
        "<b>NEW GROUP APPLICATION (5 QUESTIONS)</b>\n"
        "--------------------\n"
        f"<b>Acc Name:</b> {html.escape(app['full_name'])}\n"
        f"<b>Username:</b> {username}\n"
        f"<b>User ID:</b> <code>{app['user_id']}</code>\n"
        f"<b>Phone Number:</b> {phone_text}\n"
        f"<b>Personal Channel:</b> {personal_chat}\n"
        f"<b>Current Groups:</b>\n{group_lines}\n"
        "--------------------\n"
        f"<b>Q1.</b> {html.escape(app['answer_1'] or '')}\n\n"
        f"<b>Q2.</b> {html.escape(app['answer_2'] or '')}\n\n"
        f"<b>Q3.</b> {html.escape(app['answer_3'] or '')}\n\n"
        f"<b>Q4.</b> {html.escape(app['answer_4'] or '')}\n\n"
        f"<b>Q5.</b> {html.escape(app['answer_5'] or '')}\n"
        "--------------------\n"
        f"<b>Mnet Plus Acc:</b> {mnet_acc}\n"
        f"<b>Certificate No.:</b> <code>{cert_no}</code>\n"
        f"<b>Participation Date:</b> {cert_date}\n"
        f"{dup_line}"
        "--------------------\n"
        "Please review this application."
    )


async def send_to_admins(bot: Bot, user_id: int):
    app = await get_application(user_id)
    if not app:
        return

    card = make_application_card(app)
    profile_photo_id = app["profile_photo_file_id"]
    cert_photo_id = (
        app["certificate_photo_file_id"]
        if "certificate_photo_file_id" in app.keys()
        else None
    )

    for admin_id in ADMIN_IDS:
        try:
            if profile_photo_id:
                await bot.send_photo(
                    admin_id,
                    photo=profile_photo_id,
                    caption=f"Profile Photo - User ID: {user_id}",
                )
            if cert_photo_id:
                await bot.send_photo(
                    admin_id,
                    photo=cert_photo_id,
                    caption=f"2026 MAMA 2nd round voting certificate - User ID: {user_id}",
                )
            await bot.send_message(
                admin_id,
                card,
                reply_markup=admin_keyboard(user_id),
            )
        except (TelegramForbiddenError, TelegramBadRequest) as error:
            logging.warning("Admin DM failed: %s | %s", admin_id, error)


# ================= START =================


@dp.message(CommandStart())
async def start_handler(message: Message, bot: Bot):
    user_id = message.from_user.id
    await record_seen_user(bot, message.from_user)
    app = await get_application(user_id)

    if app:
        status = app["status"]

        if status == "asking":
            index = app["question_index"]
            if index < TOTAL_STEPS:
                await message.answer(
                    "Your application is in progress.\n\n" + _get_step_prompt(index)
                )
                return

        if status == "awaiting_review":
            await message.answer(
                "Your application is waiting for admin approval."
            )
            return

        if status == "approved":
            await message.answer(
                "Your application has already been approved."
            )
            return

    await message.answer(
        "Application bot မှကြိုဆိုပါတယ်!\n\n"
        "Group Join Request Apply လုပ်ရန်:\n"
        "1. bot ကို start လုပ်ပေးပါ.\n"
        "2. invite link မှတစ်ဆင့် join request ပို့ပေးပါ.\n"
        "3. မေးခွန်းများကို အမှန်အတိုင်းဖြေဆိုပေးပါ.\n"
        "4. admin ထံမှ approval စောင့်ပေးပါ.\n\n"
    )


# ================= SHARED CONTACT (IF ANY) =================


@dp.message(F.chat.type == "private", F.contact)
async def contact_handler(message: Message, bot: Bot):
    if message.contact and message.contact.phone_number:
        await record_seen_user(
            bot,
            message.from_user,
            shared_phone=message.contact.phone_number,
        )
        await message.answer("Phone number saved.")


# ================= GROUP MEMBER LEFT HANDLER =================


@dp.chat_member(F.chat.id == GROUP_ID)
async def group_member_left_handler(event: ChatMemberUpdated, bot: Bot):
    user = event.new_chat_member.user
    if user.is_bot:
        return

    old_status = event.old_chat_member.status
    new_status = event.new_chat_member.status

    was_in_group = old_status in ("member", "administrator", "restricted")
    now_left = new_status in ("left", "kicked")

    if not (was_in_group and now_left):
        return

    await record_seen_user(bot, user)
    await update_status(user.id, "left_group")
    await notify_admins_member_left(bot, user, new_status)

    try:
        await bot.send_message(
            user.id,
            "Group ထဲမှ ထွက်သွားတဲ့အတွက် ပြန်ဝင်လိုပါက group join request "
            "ပြန်ပို့ပေးပါ။ Join request ပို့ပြီးတာနဲ့ မေးခွန်း ၅ ခုကို "
            "အစကနေ ပြန်ဖြေပေးရပါမယ်။",
        )
    except (TelegramForbiddenError, TelegramBadRequest):
        pass


# ================= JOIN REQUEST =================


@dp.chat_join_request(F.chat.id == GROUP_ID)
async def join_request_handler(event: ChatJoinRequest, bot: Bot):
    user = event.from_user
    app = await get_application(user.id)

    if app and app["status"] == "approved":
        try:
            await bot.approve_chat_join_request(GROUP_ID, user.id)
        except TelegramBadRequest:
            pass
        return

    extra_info = await fetch_extra_user_info(bot, user, event)
    created = await create_application(user, event.chat.id, extra_info)

    if not created:
        try:
            await bot.send_message(
                user.id,
                "application စပြီးပါပြီ "
                "request ထပ်မလုပ်မီ ပြီးဆုံးအောင်ဖြေဆိုပေးပါ.",
            )
        except (TelegramForbiddenError, TelegramBadRequest):
            pass
        return

    prefix = ""
    if app and app["status"] == "declined":
        prefix = "ယခင် request ပယ်ချခံထားရတဲ့အတွက် မေးခွန်းများကို အစကနေ ပြန်ဖြေပေးပါ။\n\n"
    elif app and app["status"] == "left_group":
        prefix = "Group ထဲ ပြန်ဝင်ရန် မေးခွန်းများကို အစကနေ ပြန်ဖြေပေးပါ။\n\n"

    try:
        await bot.send_message(
            user.id,
            prefix + "မေးခွန်း ၅ ခုဖြေပေးပါ!\n\n" + _get_step_prompt(0),
        )
    except (TelegramForbiddenError, TelegramBadRequest):
        await update_status(user.id, "dm_unavailable")
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    "Applicant cannot receive bot messages.\n"
                    f"User ID: <code>{user.id}</code>\n\n"
                    "Ask them to press /start and submit a new join request.",
                )
            except Exception:
                pass


# ================= ANSWERS (5 QUESTIONS + MNET ACC NAME) =================


@dp.message(
    F.chat.type == "private",
    F.text,
    ~F.text.startswith("/"),
)
async def answer_handler(message: Message, bot: Bot):
    user_id = message.from_user.id
    await record_seen_user(bot, message.from_user)
    app = await get_application(user_id)

    if not app or app["status"] != "asking":
        return

    index = app["question_index"]
    if index >= TOTAL_STEPS:
        return

    if index == CERT_PHOTO_STEP:
        await message.answer("2026 MAMA 2nd round voting certificate ပြပေးပါ")
        return

    answer = message.text.strip()
    if not answer:
        await message.answer("Please enter an answer.")
        return

    if len(answer) > 1500:
        await message.answer("Please keep your answer under 1500 characters.")
        return

    saved = await save_text_step(user_id, index, answer)
    if not saved:
        await message.answer("Could not save your answer. Please try again.")
        return

    await message.answer(_get_step_prompt(index + 1))


# ================= CERTIFICATE PHOTO =================


@dp.message(
    F.chat.type == "private",
    F.photo,
)
async def certificate_photo_handler(message: Message, bot: Bot):
    user_id = message.from_user.id
    await record_seen_user(bot, message.from_user)
    app = await get_application(user_id)

    if not app or app["status"] != "asking":
        return

    index = app["question_index"]
    if index != CERT_PHOTO_STEP:
        return

    photo = message.photo[-1]
    photo_file_id = photo.file_id

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            tmp_path = tmp.name

        await bot.download(photo, destination=tmp_path)
        cert_info = await asyncio.to_thread(parse_certificate_info, tmp_path)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    mnet_account = app["mnet_account"] if "mnet_account" in app.keys() else None
    dup_warning = await check_and_record_certificate(
        user_id=user_id,
        mnet_account=mnet_account,
        cert_no=cert_info["certificate_no"],
        cert_date=cert_info["certificate_date"],
        sha256_hash=cert_info["sha256"],
    )

    saved = await save_certificate_step(
        user_id=user_id,
        photo_file_id=photo_file_id,
        cert_no=cert_info["certificate_no"],
        cert_date=cert_info["certificate_date"],
        sha256_hash=cert_info["sha256"],
        dup_warning=dup_warning,
    )
    if not saved:
        await message.answer("Could not save your certificate. Please try again.")
        return

    await message.answer(
        "admin ထံ request ပို့ပြီးပါပြီ.\n\n"
        "စောင့်ဆိုင်းပေးပါ။"
    )
    await send_to_admins(bot, user_id)


# ================= RELAY OTHER USER DM TO ADMINS =================


@dp.message(
    F.chat.type == "private",
    ~F.text.startswith("/"),
)
async def relay_other_dm_to_admins(message: Message, bot: Bot):
    if not message.from_user:
        return

    user = message.from_user
    if user.id in ADMIN_IDS:
        return

    await record_seen_user(bot, user)
    app = await get_application(user.id)

    # မေးခွန်း ၅ ခု / certificate ဖြေဆိုနေချိန်ဆိုရင် သက်ဆိုင်ရာ handler က ကိုင်တွယ်ပြီးသားဖြစ်ပါတယ်
    if app and app["status"] == "asking":
        return

    full_name = getattr(user, "full_name", None) or str(user.id)
    username_text = f"@{user.username}" if user.username else "Not set"
    header = (
        "<b>USER MESSAGE TO BOT</b>\n"
        "--------------------\n"
        f"<b>Acc Name:</b> {html.escape(full_name)}\n"
        f"<b>Username:</b> {html.escape(username_text)}\n"
        f"<b>User ID:</b> <code>{user.id}</code>"
    )
    if message.text:
        header += f"\n<b>Message:</b>\n{html.escape(message.text)}"

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, header)
            if not message.text:
                await bot.copy_message(
                    chat_id=admin_id,
                    from_chat_id=message.chat.id,
                    message_id=message.message_id,
                )
        except (TelegramForbiddenError, TelegramBadRequest) as error:
            logging.warning(
                "Failed to relay user DM to admin %s: %s",
                admin_id,
                error,
            )


# ================= ADMIN APPROVAL =================


@dp.callback_query(F.data.startswith("app:"))
async def approval_handler(callback: CallbackQuery, bot: Bot):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("You are not authorized.", show_alert=True)
        return

    try:
        _, action, user_id_text = callback.data.split(":")
        user_id = int(user_id_text)
    except (ValueError, AttributeError):
        await callback.answer("Invalid button.", show_alert=True)
        return

    app = await get_application(user_id)
    if not app:
        await callback.answer("Application not found.", show_alert=True)
        return

    if app["status"] != "awaiting_review":
        await callback.answer(
            f"Application status: {app['status']}",
            show_alert=True,
        )
        return

    if action == "approve":
        try:
            await bot.approve_chat_join_request(GROUP_ID, user_id)
        except TelegramBadRequest as error:
            logging.warning("Approve failed for %s: %s", user_id, error)
            await callback.answer(
                "Could not approve. Join request may have expired.",
                show_alert=True,
            )
            return

        await update_status(user_id, "approved")
        await callback.answer("Approved!")
        if callback.message:
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except TelegramBadRequest:
                pass
        try:
            await bot.send_message(
                user_id,
                "Group Member ဖြစ်သွားပါပြီ",
            )
        except (TelegramForbiddenError, TelegramBadRequest):
            pass

    elif action == "decline":
        try:
            await bot.decline_chat_join_request(GROUP_ID, user_id)
        except TelegramBadRequest:
            pass

        await update_status(user_id, "declined")
        await callback.answer("Declined.")
        if callback.message:
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except TelegramBadRequest:
                pass
        try:
            await bot.send_message(
                user_id,
                "Join Request ပယ်ချခံရသည်။",
            )
        except (TelegramForbiddenError, TelegramBadRequest):
            pass

    else:
        await callback.answer("Unknown action.", show_alert=True)


# ================= MAIN =================


def _install_signal_handlers():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(dp.stop_polling()),
            )
        except (NotImplementedError, RuntimeError):
            pass


async def main():
    print("Initializing database...", flush=True)
    await init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    await bot.set_my_commands([
        BotCommand(command="start", description="Start application"),
    ])

    _install_signal_handlers()

    print("Starting bot polling...", flush=True)
    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            handle_signals=False,
        )
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        logging.exception("Fatal error in bot")
