"""
SaveDeleted - перехватчик удалённых, изменённых и самоуничтожающихся сообщений

Модуль для Heroku UserBot 2.0.0.
Сохраняет все входящие сообщения в SQLite, медиа — в StorageChat.
Уведомляет владельца об удалениях, изменениях, TTL-сообщениях и историях.
Поддерживает WhiteList/BlackList, массовые удаления, retention-очистку.
"""

__version__ = (1, 0, 0)

# meta developer: @Mr4epTuk
# requires: aiosqlite aiohttp
# scope: hikka_only

import aiohttp
import aiosqlite
import asyncio
import json
import logging
import os
import time

from .. import loader, utils
from herokutl.types import Message
from herokutl.tl.types import (
    UpdateDeleteChannelMessages,
    UpdateDeleteMessages,
    UpdateEditChannelMessage,
    UpdateEditMessage,
    UpdateStory,
)

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    msg_id INTEGER NOT NULL,
    chat_id INTEGER NOT NULL,
    sender_id INTEGER,
    date INTEGER,
    text TEXT,
    media_type TEXT DEFAULT 'text',
    storage_msg_id INTEGER,
    is_ttl INTEGER DEFAULT 0,
    ttl_period INTEGER,
    forward_info TEXT,
    created_at INTEGER
);

CREATE TABLE IF NOT EXISTS chats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL UNIQUE,
    title TEXT,
    type TEXT DEFAULT 'user',
    is_whitelist INTEGER DEFAULT 0,
    is_blacklist INTEGER DEFAULT 0,
    save_messages INTEGER DEFAULT 1,
    save_media INTEGER DEFAULT 1,
    save_stories INTEGER DEFAULT 0,
    save_profile_photo INTEGER DEFAULT 0,
    media_types TEXT DEFAULT '["photo","video","file","voice","sticker","gif"]'
);

CREATE TABLE IF NOT EXISTS stories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    story_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    date INTEGER,
    media_type TEXT,
    storage_msg_id INTEGER,
    created_at INTEGER
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE INDEX IF NOT EXISTS idx_messages_chat_msg ON messages(chat_id, msg_id);
CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date);
CREATE INDEX IF NOT EXISTS idx_chats_chat_id ON chats(chat_id);
"""

DEFAULT_SETTINGS = {
    "is_setup_done": "0",
    "notify_via": "builtin",
    "notify_sound": "0",
    "default_pm": "1",
    "default_groups": "0",
    "default_channels": "0",
    "default_bots": "0",
    "track_deletions": "1",
    "track_edits": "1",
    "track_ttl": "1",
    "show_diff": "0",
    "whitelist_enabled": "0",
    "retention_days": "7",
    "save_messages": "1",
    "save_media": "1",
    "save_stories": "0",
    "save_photos": "0",
    "media_photo": "1",
    "media_video": "1",
    "media_files": "1",
    "media_voice": "1",
    "media_stickers": "1",
}

MEDIA_TYPE_NAMES_RU = {
    "photo": "фото",
    "video": "видео",
    "audio": "аудио",
    "voice": "голосовое",
    "round": "кружок",
    "sticker": "стикер",
    "gif": "GIF",
    "file": "файл",
    "geo": "геолокацию",
    "poll": "опрос",
    "text": "сообщение",
}


def _get_media_type(message: Message) -> str:
    if getattr(message, "photo", None):
        return "photo"
    if getattr(message, "video", None):
        return "video"
    if getattr(message, "audio", None):
        return "audio"
    if getattr(message, "voice", None):
        return "voice"
    if getattr(message, "video_note", None):
        return "round"
    if getattr(message, "sticker", None):
        return "sticker"
    if getattr(message, "gif", None):
        return "gif"
    if getattr(message, "document", None):
        return "file"
    if getattr(message, "geo", None):
        return "geo"
    if getattr(message, "poll", None):
        return "poll"
    return "text"


@loader.tds
class SaveDeletedMod(loader.Module):
    """Перехватчик удалённых, изменённых и TTL-сообщений"""

    strings = {
        "name": "SaveDeleted",
        "no_args": "❌ Specify @username, ID or link",
        "entity_not_found": "❌ Chat/user not found",
        "wl_added": "✅ <b>{}</b> added to WhiteList",
        "bl_added": "🚫 <b>{}</b> added to BlackList",
        "wl_already": "ℹ️ <b>{}</b> already in WhiteList",
        "bl_already": "ℹ️ <b>{}</b> already in BlackList",
        "deleted_msg": "<a href=\"{url}\">{name}</a> deleted a message.",
        "deleted_media": "<a href=\"{url}\">{name}</a> deleted {media_name}.",
        "deleted_forward": "<a href=\"{url}\">{name}</a> deleted a forwarded message from <a href=\"{fwd_url}\">{fwd_name}</a>.",
        "deleted_group": "<a href=\"{url}\">{name}</a> deleted a message in <a href=\"{chat_url}\">{chat_title}</a>.",
        "deleted_channel": "In channel <a href=\"{chat_url}\">{chat_title}</a> a message was deleted.",
        "edited_msg": "<a href=\"{url}\">{name}</a> edited a message.",
        "edited_caption": "<a href=\"{url}\">{name}</a> added text to {media_name}.",
        "edited_group": "<a href=\"{url}\">{name}</a> edited a message in <a href=\"{chat_url}\">{chat_title}</a>.",
        "ttl_msg": "<a href=\"{url}\">{name}</a> sent a self-destructing {media_name}.",
        "story_deleted": "<a href=\"{url}\">{name}</a> deleted a story.",
        "mass_same": "In chat <a href=\"{chat_url}\">{chat_title}</a> {count} identical messages were deleted.",
        "mass_different": "In chat <a href=\"{chat_url}\">{chat_title}</a> {count} messages were deleted.",
        "blockquote_old_new": "Old text: {old}<br><br>New text: {new}",
        "msg_text": "{text}",
        "msg_caption": "{caption}",
    }

    strings_ru = {
        "name": "SaveDeleted",
        "no_args": "❌ Укажите @username, ID или ссылку",
        "entity_not_found": "❌ Чат/пользователь не найден",
        "wl_added": "✅ <b>{}</b> добавлен в WhiteList",
        "bl_added": "🚫 <b>{}</b> добавлен в BlackList",
        "wl_already": "ℹ️ <b>{}</b> уже в WhiteList",
        "bl_already": "ℹ️ <b>{}</b> уже в BlackList",
        "deleted_msg": "<a href=\"{url}\">{name}</a> удалил сообщение.",
        "deleted_media": "<a href=\"{url}\">{name}</a> удалил {media_name}.",
        "deleted_forward": "<a href=\"{url}\">{name}</a> удалил пересланное сообщение от <a href=\"{fwd_url}\">{fwd_name}</a>.",
        "deleted_group": "<a href=\"{url}\">{name}</a> удалил сообщение в группе <a href=\"{chat_url}\">{chat_title}</a>.",
        "deleted_channel": "В канале <a href=\"{chat_url}\">{chat_title}</a> удалили сообщение.",
        "edited_msg": "<a href=\"{url}\">{name}</a> изменил сообщение.",
        "edited_caption": "<a href=\"{url}\">{name}</a> добавил текст к {media_name}.",
        "edited_group": "<a href=\"{url}\">{name}</a> изменил сообщение в группе <a href=\"{chat_url}\">{chat_title}</a>.",
        "ttl_msg": "<a href=\"{url}\">{name}</a> отправил одноразовое {media_name}.",
        "story_deleted": "<a href=\"{url}\">{name}</a> удалил историю.",
        "mass_same": "В чате <a href=\"{chat_url}\">{chat_title}</a> удалили {count} одинаковых сообщений.",
        "mass_different": "В чате <a href=\"{chat_url}\">{chat_title}</a> было удалено {count} сообщений.",
        "blockquote_old_new": "Старый текст: {old}<br><br>Новый текст: {new}",
        "msg_text": "{text}",
        "msg_caption": "{caption}",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "is_setup_done",
                False,
                "Whether the module has been set up",
                validator=loader.validators.Boolean(),
            ),
        )

    # --- Lifecycle ---

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._me_id = self._tg_id

        try:
            base = utils.get_base_dir()
        except Exception:
            base = os.path.dirname(os.path.abspath(__file__))
        self._sqlite_path = os.path.join(base, "save_deleted.sqlite")
        self._conn = None
        self._deletion_buffers = {}

        try:
            await self._init_db()
        except Exception as e:
            logger.warning("Primary DB init failed (%s), trying fallback path", e)
            self._sqlite_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)) or "/tmp",
                "save_deleted.sqlite",
            )
            try:
                await self._init_db()
            except Exception as e2:
                logger.exception("DB init completely failed: %s", e2)
                self._conn = None

        if self._conn:
            await self._ensure_defaults()
            await self._ensure_storage_chat()
            logger.info("SaveDeleted core initialized")
        else:
            logger.warning("SaveDeleted running without DB — no persistence")

    async def on_unload(self):
        for buf in list(self._deletion_buffers.values()):
            if buf.get("timer"):
                buf["timer"].cancel()
        self._deletion_buffers.clear()
        if self._conn:
            await self._conn.close()
            self._conn = None
        logger.info("SaveDeleted unloaded")

    # --- Database ---

    async def _init_db(self):
        logger.debug("Opening SQLite at %s", self._sqlite_path)
        self._conn = await aiosqlite.connect(self._sqlite_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()

    async def _ensure_defaults(self):
        for key, val in DEFAULT_SETTINGS.items():
            await self._set_setting(key, val, insert_only=True)

    async def _ensure_storage_chat(self):
        sid = await self._get_setting("storage_chat_id", "")
        if sid:
            try:
                await self._client.get_entity(int(sid))
                return
            except Exception:
                pass

        try:
            from herokutl.tl.functions.channels import CreateChannelRequest

            result = await self._client(
                CreateChannelRequest(
                    title="SaveDeleted Storage",
                    about="Media storage for SaveDeleted module",
                    megagroup=True,
                )
            )
            chat_id = result.chats[0].id
            await self._set_setting("storage_chat_id", str(chat_id))

            try:
                await self._client.edit_folder(chat_id, folder=1)
            except Exception:
                logger.warning("Failed to archive storage chat")
        except Exception as e:
            logger.exception("Failed to create storage chat: %s", e)

    async def _get_setting(self, key: str, default: str = "") -> str:
        if not self._conn:
            return default
        try:
            async with self._conn.execute(
                "SELECT value FROM settings WHERE key = ?", (key,)
            ) as cursor:
                row = await cursor.fetchone()
                return row["value"] if row else default
        except Exception as e:
            logger.error("_get_setting(%s) error: %s", key, e)
            return default

    async def _set_setting(self, key: str, value: str, insert_only: bool = False):
        if not self._conn:
            return
        try:
            if insert_only:
                await self._conn.execute(
                    "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                    (key, value),
                )
            else:
                await self._conn.execute(
                    "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                    (key, value),
                )
            await self._conn.commit()
        except Exception as e:
            logger.error("_set_setting(%s) error: %s", key, e)

    async def _db_fetchone(self, query: str, params: tuple = ()):
        if not self._conn:
            return None
        try:
            async with self._conn.execute(query, params) as cursor:
                return await cursor.fetchone()
        except Exception as e:
            logger.error("_db_fetchone error: %s", e)
            return None

    async def _db_fetchall(self, query: str, params: tuple = ()):
        if not self._conn:
            return []
        try:
            async with self._conn.execute(query, params) as cursor:
                return await cursor.fetchall()
        except Exception as e:
            logger.error("_db_fetchall error: %s", e)
            return []

    async def _db_execute(self, query: str, params: tuple = ()):
        if not self._conn:
            return
        try:
            await self._conn.execute(query, params)
            await self._conn.commit()
        except Exception as e:
            logger.error("_db_execute error: %s", e)

    # --- Storage Chat ---

    async def _upload_media(self, message: Message) -> int:
        sid = await self._get_setting("storage_chat_id", "")
        if not sid:
            return 0
        try:
            path = await message.download_media()
            if not path:
                return 0
            try:
                sent = await self._client.send_file(
                    int(sid), path, caption=message.text or ""
                )
                return sent.id if hasattr(sent, "id") else 0
            finally:
                try:
                    os.unlink(path)
                except Exception:
                    pass
        except Exception as e:
            logger.error("_upload_media error: %s", e)
            return 0

    # --- Tracking logic ---

    async def _should_track(self, chat_id: int) -> bool:
        row = await self._db_fetchone(
            "SELECT is_blacklist, is_whitelist FROM chats WHERE chat_id = ?",
            (chat_id,),
        )
        if row and row["is_blacklist"]:
            return False

        wl_enabled = (await self._get_setting("whitelist_enabled", "0")) == "1"
        if wl_enabled and (not row or not row["is_whitelist"]):
            return False

        try:
            entity = await self._client.get_entity(chat_id)
        except Exception:
            return True

        from herokutl.tl.types import (
            Channel,
            Chat,
            User,
        )

        if isinstance(entity, User):
            if entity.bot:
                return (await self._get_setting("default_bots", "0")) == "1"
            return (await self._get_setting("default_pm", "1")) == "1"
        if isinstance(entity, Chat):
            return (await self._get_setting("default_groups", "0")) == "1"
        if isinstance(entity, Channel):
            if entity.broadcast:
                return (await self._get_setting("default_channels", "0")) == "1"
            return (await self._get_setting("default_groups", "0")) == "1"
        return True

    async def _should_track_msg(self, message: Message) -> bool:
        if not await self._should_track(message.chat_id):
            return False
        if (await self._get_setting("save_messages", "1")) != "1":
            return False
        row = await self._db_fetchone(
            "SELECT save_messages FROM chats WHERE chat_id = ?",
            (message.chat_id,),
        )
        if row and not row["save_messages"]:
            return False
        return True

    async def _should_track_media(self, message: Message, media_type: str) -> bool:
        if (await self._get_setting("save_media", "1")) != "1":
            return False
        if media_type == "text":
            return True

        row = await self._db_fetchone(
            "SELECT save_media, media_types FROM chats WHERE chat_id = ?",
            (message.chat_id,),
        )
        if row is not None and not row["save_media"]:
            return False
        if row and row["media_types"]:
            try:
                allowed_types = json.loads(row["media_types"])
                if media_type not in allowed_types:
                    return False
            except Exception:
                pass

        key_map = {
            "photo": "media_photo",
            "video": "media_video",
            "audio": "media_files",
            "voice": "media_voice",
            "round": "media_voice",
            "sticker": "media_stickers",
            "gif": "media_stickers",
            "file": "media_files",
            "geo": "media_files",
            "poll": "media_files",
        }
        setting_key = key_map.get(media_type, "media_files")
        return (await self._get_setting(setting_key, "1")) == "1"

    async def _should_track_deletions(self) -> bool:
        return (await self._get_setting("track_deletions", "1")) == "1"

    async def _should_track_edits(self) -> bool:
        return (await self._get_setting("track_edits", "1")) == "1"

    async def _should_track_ttl(self) -> bool:
        return (await self._get_setting("track_ttl", "1")) == "1"

    # --- Entity resolution ---

    async def _resolve_entity(self, arg: str, message: Message = None):
        arg = arg.strip() if arg else ""
        if not arg and message:
            reply = await message.get_reply_message()
            if reply:
                return await self._client.get_entity(reply.sender_id)
            return await self._client.get_entity(message.chat_id)

        try:
            entity_id = int(arg)
            if entity_id < 0:
                entity_id = int(str(entity_id).replace("-100", ""))
            return await self._client.get_entity(entity_id)
        except ValueError:
            pass
        except Exception:
            return None

        try:
            return await self._client.get_entity(arg)
        except Exception:
            return None

    @staticmethod
    def _get_display_name(entity) -> str:
        if hasattr(entity, 'first_name'):
            name = entity.first_name or ''
            if getattr(entity, 'last_name', None):
                name += ' ' + entity.last_name
            return name.strip()
        if hasattr(entity, 'title'):
            return entity.title or ''
        return str(getattr(entity, 'id', 'Unknown'))

    @staticmethod
    def _get_entity_type(entity) -> str:
        from herokutl.tl.types import Channel, Chat, User

        if isinstance(entity, User):
            return "bot" if entity.bot else "user"
        if isinstance(entity, Chat):
            return "group"
        if isinstance(entity, Channel):
            return "channel" if entity.broadcast else "supergroup"
        return "user"

    async def _get_entity_info(self, entity_id: int) -> dict:
        row = await self._db_fetchone(
            "SELECT title, type FROM chats WHERE chat_id = ?", (entity_id,)
        )
        if row:
            return {"title": row["title"] or str(entity_id), "type": row["type"]}

        try:
            entity = await self._client.get_entity(entity_id)
            title = self._get_display_name(entity)
            etype = self._get_entity_type(entity)
            return {"title": title, "type": etype}
        except Exception:
            return {"title": str(entity_id), "type": "user"}

    async def _format_entity_link(self, entity_id: int) -> str:
        row = await self._db_fetchone(
            "SELECT type FROM chats WHERE chat_id = ?", (entity_id,)
        )
        if row and row["type"] in ("channel", "supergroup"):
            try:
                entity = await self._client.get_entity(entity_id)
                if hasattr(entity, "username") and entity.username:
                    return f"https://t.me/{entity.username}"
            except Exception:
                pass
        return f"tg://user?id={entity_id}"

    async def _cache_entity(self, entity_id: int):
        existing = await self._db_fetchone(
            "SELECT chat_id FROM chats WHERE chat_id = ?", (entity_id,)
        )
        if existing:
            return
        try:
            entity = await self._client.get_entity(entity_id)
            title = self._get_display_name(entity)
            etype = self._get_entity_type(entity)
            await self._conn.execute(
                "INSERT OR IGNORE INTO chats (chat_id, title, type) VALUES (?, ?, ?)",
                (entity_id, title, etype),
            )
            await self._conn.commit()
        except Exception:
            pass

    # --- Message storage ---

    async def _store_message(self, message: Message):
        if not await self._should_track_msg(message):
            return

        now = int(time.time())
        media_type = _get_media_type(message)
        text = message.text or ""

        if getattr(message, "sender_id", None):
            await self._cache_entity(message.sender_id)

        if getattr(message, "chat_id", None):
            await self._cache_entity(message.chat_id)

        forward_info = None
        if getattr(message, "fwd_from", None):
            try:
                fi = {}
                fwd = message.fwd_from
                if getattr(fwd, "from_id", None):
                    fwd_entity = await self._client.get_entity(fwd.from_id)
                    fi["name"] = self._get_display_name(fwd_entity)
                    fi["id"] = fwd.from_id.user_id if hasattr(fwd.from_id, "user_id") else str(fwd.from_id)
                forward_info = json.dumps(fi, ensure_ascii=False)
            except Exception:
                pass

        storage_msg_id = 0
        if media_type != "text" and await self._should_track_media(message, media_type):
            try:
                storage_msg_id = await self._upload_media(message)
            except Exception as e:
                logger.error("Failed to store media: %s", e)

        ttl_period = getattr(message, "ttl_period", 0) or 0
        is_ttl = 1 if ttl_period and ttl_period > 0 else 0

        try:
            await self._conn.execute(
                """INSERT INTO messages
                   (msg_id, chat_id, sender_id, date, text, media_type,
                    storage_msg_id, is_ttl, ttl_period, forward_info, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    message.id,
                    message.chat_id,
                    message.sender_id,
                    getattr(message, "date", None),
                    text,
                    media_type,
                    storage_msg_id,
                    is_ttl,
                    ttl_period,
                    forward_info,
                    now,
                ),
            )
            await self._conn.commit()
        except Exception as e:
            logger.error("_store_message: %s", e)

        if is_ttl and await self._should_track_ttl():
            await self._send_ttl_notification(message, media_type)

    async def _get_cached_messages(self, chat_id: int, msg_ids: list) -> list:
        if not msg_ids:
            return []
        placeholders = ",".join("?" * len(msg_ids))
        rows = await self._db_fetchall(
            f"SELECT * FROM messages WHERE chat_id = ? AND msg_id IN ({placeholders})",
            (chat_id, *msg_ids),
        )
        return rows if rows else []

    # --- Event watchers ---

    @loader.watcher(in_=True, only_messages=True)
    async def watcher_save(self, message: Message):
        """Save all incoming messages to DB"""
        if message.sender_id in (777000, 489000):
            return
        await self._store_message(message)

    @loader.raw_handler(UpdateDeleteMessages, UpdateDeleteChannelMessages)
    async def raw_deletions(self, update):
        """Handle deleted messages"""
        if not await self._should_track_deletions():
            return

        if isinstance(update, UpdateDeleteChannelMessages):
            chat_id = getattr(update, "channel_id", 0)
            msg_ids = list(getattr(update, "messages", []))
            if not chat_id or not msg_ids:
                return
            await self._buffer_deletions(chat_id, msg_ids)
        else:
            msg_ids = list(getattr(update, "messages", []))
            if not msg_ids:
                return
            await self._buffer_deletions_by_msgs(msg_ids)

    async def _find_chat_id_by_msg(self, msg_id: int) -> int:
        row = await self._db_fetchone(
            "SELECT chat_id FROM messages WHERE msg_id = ? LIMIT 1", (msg_id,)
        )
        return row["chat_id"] if row else 0

    async def _buffer_deletions_by_msgs(self, msg_ids: list):
        for msg_id in msg_ids:
            chat_id = await self._find_chat_id_by_msg(msg_id)
            if chat_id:
                await self._buffer_deletions(chat_id, [msg_id])

    @loader.raw_handler(UpdateEditMessage, UpdateEditChannelMessage)
    async def raw_edits(self, update):
        """Handle edited messages"""
        if not await self._should_track_edits():
            return

        message = getattr(update, "message", None)
        if not message:
            return

        if getattr(message, "sender_id", 0) in (777000, 489000):
            return

        old_rows = await self._get_cached_messages(message.chat_id, [message.id])
        if not old_rows:
            return

        old_row = old_rows[0]
        old_text = old_row["text"] or ""
        new_text = message.text or ""
        is_same = old_text == new_text

        if is_same:
            return

        old_media = old_row["media_type"] or "text"

        sender_id = message.sender_id or 0
        if sender_id:
            sender_info = await self._get_entity_info(sender_id)
            sender_name = utils.escape_html(sender_info["title"])
            sender_url = await self._format_entity_link(sender_id)
        else:
            sender_name = "Unknown"
            sender_url = "#"

        chat_info = await self._get_entity_info(message.chat_id)
        chat_title = utils.escape_html(chat_info["title"])
        chat_url = await self._format_entity_link(message.chat_id)

        is_private = chat_info["type"] == "user"

        show_diff = (await self._get_setting("show_diff", "0")) == "1"

        if old_media != "text" and not old_text and new_text:
            header = self.strings("edited_caption").format(
                url=sender_url,
                name=sender_name,
                media_name=MEDIA_TYPE_NAMES_RU.get(old_media, old_media),
            )
            body = self.strings("msg_caption").format(
                caption=f"<blockquote expandable>{utils.escape_html(new_text)}</blockquote>"
            )
        else:
            header = self.strings("edited_msg").format(
                url=sender_url, name=sender_name
            )
            if show_diff:
                body = self.strings("blockquote_old_new").format(
                    old=utils.escape_html(old_text),
                    new=utils.escape_html(new_text),
                )
                body = f"<blockquote expandable>{body}</blockquote>"
            else:
                body = self.strings("msg_text").format(
                    text=f"<blockquote expandable>{utils.escape_html(new_text)}</blockquote>"
                )

        if not is_private:
            header = self.strings("edited_group").format(
                url=sender_url,
                name=sender_name,
                chat_url=chat_url,
                chat_title=chat_title,
            )

        text = f"{header}\n{body}"
        await self._send_notification(text)

        await self._store_message(message)

    @loader.raw_handler(UpdateStory)
    async def raw_stories(self, update):
        story = getattr(update, "story", None)
        peer = getattr(update, "peer", None)
        if not story or not peer:
            return

        user_id = getattr(peer, "user_id", 0)
        if not user_id:
            return

        now = int(time.time())
        deleted = bool(getattr(story, "deleted", False) or getattr(story, "expired", False))

        if deleted:
            rows = await self._db_fetchall(
                "SELECT * FROM stories WHERE story_id = ? AND user_id = ?",
                (story.id, user_id),
            )
            if rows:
                user_info = await self._get_entity_info(user_id)
                user_name = utils.escape_html(user_info["title"])
                user_url = await self._format_entity_link(user_id)

                text = self.strings("story_deleted").format(
                    url=user_url, name=user_name
                )
                await self._send_notification(text)

                await self._db_execute(
                    "DELETE FROM stories WHERE story_id = ? AND user_id = ?",
                    (story.id, user_id),
                )
            return

        media_type = "photo"
        sid = await self._get_setting("storage_chat_id", "")
        storage_msg_id = 0

        if (await self._get_setting("save_stories", "0")) == "1" and sid:
            try:
                story_media = getattr(story, "media", None)
                if story_media:
                    await self._client.send_file(int(sid), story_media)
            except Exception as e:
                logger.error("Failed to save story: %s", e)

        await self._conn.execute(
            """INSERT OR REPLACE INTO stories
               (story_id, user_id, date, media_type, storage_msg_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (story.id, user_id, getattr(story, "date", now), media_type, storage_msg_id, now),
        )
        await self._conn.commit()

    # --- Mass deletion buffer ---

    async def _buffer_deletions(self, chat_id: int, msg_ids: list):
        if chat_id not in self._deletion_buffers:
            self._deletion_buffers[chat_id] = {"messages": [], "timer": None}

        buf = self._deletion_buffers[chat_id]
        buf["messages"].extend(msg_ids)

        if buf["timer"]:
            buf["timer"].cancel()
        buf["timer"] = asyncio.ensure_future(self._flush_deletions(chat_id))

    async def _flush_deletions(self, chat_id: int):
        await asyncio.sleep(0.5)
        buf = self._deletion_buffers.pop(chat_id, None)
        if not buf:
            return
        msg_ids = list(set(buf["messages"]))
        if not msg_ids:
            return

        if not await self._should_track(chat_id):
            return

        rows = await self._get_cached_messages(chat_id, msg_ids)
        if not rows:
            return

        if len(rows) > 10:
            await self._handle_mass_deletion(chat_id, rows)
        else:
            for row in rows:
                await self._handle_single_deletion(chat_id, row)

    async def _handle_mass_deletion(self, chat_id: int, rows: list):
        chat_info = await self._get_entity_info(chat_id)
        chat_title = utils.escape_html(chat_info["title"])
        chat_url = await self._format_entity_link(chat_id)

        texts = [r["text"] for r in rows if r["text"]]
        unique_texts = list(set(texts))

        if len(unique_texts) == 1 and unique_texts[0]:
            text = self.strings("mass_same").format(
                chat_url=chat_url,
                chat_title=chat_title,
                count=len(rows),
            )
            text += f"\n<blockquote expandable>{utils.escape_html(unique_texts[0])}</blockquote>"
        else:
            text = self.strings("mass_different").format(
                chat_url=chat_url,
                chat_title=chat_title,
                count=len(rows),
            )

        await self._send_notification(text)

        media_rows = [r for r in rows if r["storage_msg_id"]]
        if media_rows:
            sid = await self._get_setting("storage_chat_id", "")
            if sid:
                for chunk in utils.chunks(media_rows, 10):
                    album = []
                    for mr in chunk:
                        try:
                            msg = await self._client.get_messages(int(sid), ids=mr["storage_msg_id"])
                            if msg:
                                album.append(msg)
                        except Exception:
                            pass
                    if album:
                        try:
                            await self._client.send_file(
                                self._me_id,
                                [m.media for m in album if m.media],
                                caption=f"Media from mass deletion in {chat_title}",
                            )
                        except Exception as e:
                            logger.error("Failed to send media album: %s", e)

        all_text = "\n\n".join(
            f"[{r['msg_id']}] {r['text']}" for r in rows if r["text"]
        )
        if len(all_text) > 8000:
            import tempfile

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", delete=False, encoding="utf-8"
            ) as f:
                f.write(all_text)
                tmp_path = f.name
            try:
                await self._client.send_file(
                    self._me_id,
                    tmp_path,
                    caption=f"Deleted log ({len(rows)} messages)",
                )
            except Exception as e:
                logger.error("Failed to send log file: %s", e)
            finally:
                os.unlink(tmp_path)

    async def _handle_single_deletion(self, chat_id: int, row):
        sender_id = row["sender_id"] or 0
        text_content = row["text"] or ""
        media_type = row["media_type"] or "text"
        forward_info = row["forward_info"]
        storage_msg_id = row["storage_msg_id"]

        if sender_id:
            sender_info = await self._get_entity_info(sender_id)
            sender_name = utils.escape_html(sender_info["title"])
            sender_url = await self._format_entity_link(sender_id)
        else:
            sender_name = "Unknown"
            sender_url = "#"

        chat_info = await self._get_entity_info(chat_id)
        chat_title = utils.escape_html(chat_info["title"])
        chat_url = await self._format_entity_link(chat_id)
        chat_type = chat_info["type"]

        reply_to = None
        if storage_msg_id and media_type != "text":
            sid = await self._get_setting("storage_chat_id", "")
            if sid:
                try:
                    stored = await self._client.get_messages(int(sid), ids=storage_msg_id)
                    if stored and stored.media:
                        sent = await self._client.send_file(
                            self._me_id, stored.media, caption=stored.text or ""
                        )
                        reply_to = sent.id if hasattr(sent, "id") else None
                except Exception as e:
                    logger.error("Failed to re-send media: %s", e)

        if chat_type == "channel":
            header = self.strings("deleted_channel").format(
                chat_url=chat_url, chat_title=chat_title
            )
        elif chat_type not in ("user", "bot"):
            header = self.strings("deleted_group").format(
                url=sender_url,
                name=sender_name,
                chat_url=chat_url,
                chat_title=chat_title,
            )
        elif forward_info:
            try:
                fi = json.loads(forward_info)
                fwd_name = utils.escape_html(fi.get("name", "Unknown"))
                fwd_url = f"tg://user?id={fi.get('id', 0)}"
                header = self.strings("deleted_forward").format(
                    url=sender_url,
                    name=sender_name,
                    fwd_url=fwd_url,
                    fwd_name=fwd_name,
                )
            except Exception:
                header = self.strings("deleted_msg").format(
                    url=sender_url, name=sender_name
                )
        elif media_type != "text":
            header = self.strings("deleted_media").format(
                url=sender_url,
                name=sender_name,
                media_name=MEDIA_TYPE_NAMES_RU.get(media_type, media_type),
            )
        else:
            header = self.strings("deleted_msg").format(
                url=sender_url, name=sender_name
            )

        body_parts = []
        if text_content:
            body_parts.append(
                f"<blockquote expandable>{utils.escape_html(text_content)}</blockquote>"
            )

        text = header
        if body_parts:
            text += "\n" + "\n".join(body_parts)

        await self._send_notification(text, reply_to=reply_to if reply_to else None)

    async def _send_ttl_notification(self, message: Message, media_type: str):
        sender_id = getattr(message, "sender_id", 0)
        if not sender_id:
            return
        sender_info = await self._get_entity_info(sender_id)
        sender_name = utils.escape_html(sender_info["title"])
        sender_url = await self._format_entity_link(sender_id)

        media_name = MEDIA_TYPE_NAMES_RU.get(media_type, media_type)
        header = self.strings("ttl_msg").format(
            url=sender_url, name=sender_name, media_name=media_name
        )

        text = message.text or ""
        body = ""
        if text:
            body = f"\n<blockquote expandable>{utils.escape_html(text)}</blockquote>"

        await self._send_notification(header + body)

    # --- Notification delivery ---

    async def _send_notification(self, text: str, reply_to: int = None):
        if not text:
            return

        from herokutl.extensions import html

        if len(text) > 4096:
            chunks = list(utils.smart_split(*html.parse(text), limit=4000))
        else:
            chunks = [text]

        try:
            notify_via = await self._get_setting("notify_via", "builtin")
            sound = (await self._get_setting("notify_sound", "0")) == "1"

            for i, chunk in enumerate(chunks):
                reply = reply_to if i == 0 else None
                if notify_via == "builtin":
                    await self._send_via_builtin(chunk, sound, reply)
                elif notify_via == "token":
                    await self._send_via_token(chunk, sound, reply)
                elif notify_via == "group":
                    await self._send_via_storage(chunk, reply)
                elif notify_via == "forum":
                    await self._send_via_forum(chunk, reply)
                else:
                    await self._send_via_builtin(chunk, sound, reply)
        except Exception as e:
            logger.error("_send_notification: %s", e)

    async def _send_via_builtin(self, text: str, sound: bool, reply_to: int = None):
        try:
            kwargs = {
                "chat_id": self._me_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_notification": not sound,
            }
            if reply_to:
                kwargs["reply_to_message_id"] = reply_to
            await self.inline.bot.send_message(**kwargs)
        except Exception:
            try:
                kwargs2 = {
                    "entity": self._me_id,
                    "message": text,
                    "parse_mode": "html",
                    "silent": not sound,
                }
                if reply_to:
                    kwargs2["reply_to"] = reply_to
                await self._client.send_message(**kwargs2)
            except Exception as e:
                logger.error("_send_via_builtin fallback: %s", e)

    async def _send_via_token(self, text: str, sound: bool, reply_to: int = None):
        token = await self._get_setting("bot_token", "")
        if not token:
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": self._me_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_notification": not sound,
        }
        if reply_to:
            payload["reply_to_message_id"] = reply_to
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()
                    if not data.get("ok"):
                        logger.warning("Token bot API error: %s", data)
            except Exception as e:
                logger.error("_send_via_token: %s", e)

    async def _send_via_storage(self, text: str, reply_to: int = None):
        sid = await self._get_setting("storage_chat_id", "")
        if not sid:
            return
        try:
            kwargs = {"entity": int(sid), "message": text, "parse_mode": "html"}
            if reply_to:
                kwargs["reply_to"] = reply_to
            await self._client.send_message(**kwargs)
        except Exception as e:
            logger.error("_send_via_storage: %s", e)

    async def _send_via_forum(self, text: str, reply_to: int = None):
        sid = await self._get_setting("storage_chat_id", "")
        if not sid:
            return
        topic_id = await self._get_setting("forum_topic_id", "")
        try:
            kwargs = {"entity": int(sid), "message": text, "parse_mode": "html"}
            if topic_id:
                kwargs["reply_to"] = int(topic_id)
            elif reply_to:
                kwargs["reply_to"] = reply_to
            await self._client.send_message(**kwargs)
        except Exception as e:
            logger.error("_send_via_forum: %s", e)

    # --- Commands ---

    @loader.command(
        ru_doc="<@username | ID | ссылка> — Добавить в WhiteList",
        en_doc="<@username | ID | link> — Add to WhiteList",
    )
    async def sdwlcmd(self, message: Message):
        """Add chat/user to WhiteList"""
        args = utils.get_args_raw(message)
        if not args:
            await utils.answer(message, self.strings("no_args"))
            return

        entity = await self._resolve_entity(args, message)
        if not entity:
            await utils.answer(message, self.strings("entity_not_found"))
            return

        entity_id = entity.id
        title = self._get_display_name(entity)
        etype = self._get_entity_type(entity)

        existing = await self._db_fetchone(
            "SELECT is_whitelist FROM chats WHERE chat_id = ?", (entity_id,)
        )
        if existing and existing["is_whitelist"]:
            await utils.answer(
                message, self.strings("wl_already").format(utils.escape_html(title))
            )
            return

        await self._conn.execute(
            """INSERT INTO chats (chat_id, title, type, is_whitelist, is_blacklist,
               save_messages, save_media, save_stories, save_profile_photo)
               VALUES (?, ?, ?, 1, 0, 1, 1, 1, 1)
               ON CONFLICT(chat_id) DO UPDATE SET
               is_whitelist=1, is_blacklist=0,
               save_messages=1, save_media=1, save_stories=1, save_profile_photo=1""",
            (entity_id, title, etype),
        )
        await self._conn.commit()

        await utils.answer(
            message, self.strings("wl_added").format(utils.escape_html(title))
        )

    @loader.command(
        ru_doc="<@username | ID | ссылка> — Добавить в BlackList",
        en_doc="<@username | ID | link> — Add to BlackList",
    )
    async def sdblcmd(self, message: Message):
        """Add chat/user to BlackList"""
        args = utils.get_args_raw(message)
        if not args:
            await utils.answer(message, self.strings("no_args"))
            return

        entity = await self._resolve_entity(args, message)
        if not entity:
            await utils.answer(message, self.strings("entity_not_found"))
            return

        entity_id = entity.id
        title = self._get_display_name(entity)
        etype = self._get_entity_type(entity)

        existing = await self._db_fetchone(
            "SELECT is_blacklist FROM chats WHERE chat_id = ?", (entity_id,)
        )
        if existing and existing["is_blacklist"]:
            await utils.answer(
                message, self.strings("bl_already").format(utils.escape_html(title))
            )
            return

        await self._conn.execute(
            """INSERT INTO chats (chat_id, title, type, is_whitelist, is_blacklist,
               save_messages, save_media, save_stories, save_profile_photo)
               VALUES (?, ?, ?, 0, 1, 0, 0, 0, 0)
               ON CONFLICT(chat_id) DO UPDATE SET
               is_whitelist=0, is_blacklist=1,
               save_messages=0, save_media=0, save_stories=0, save_profile_photo=0""",
            (entity_id, title, etype),
        )
        await self._conn.commit()

        await utils.answer(
            message, self.strings("bl_added").format(utils.escape_html(title))
        )

    # --- Cleanup loop ---

    @loader.loop(interval=3600, autostart=True, wait_before=True)
    async def cleanup_loop(self):
        retention = await self._get_setting("retention_days", "7")
        if not retention or retention.lower() == "forever":
            return

        try:
            days = int(retention)
        except ValueError:
            return

        cutoff = int(time.time()) - days * 86400

        await self._db_execute("DELETE FROM messages WHERE created_at < ?", (cutoff,))
        await self._db_execute("DELETE FROM stories WHERE created_at < ?", (cutoff,))

        logger.debug("Cleanup: removed entries older than %s days", days)
