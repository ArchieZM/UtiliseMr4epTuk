"""
    SaveDeleted - Сохранение удаленных сообщений

    Автоматически кэширует историю чатов и отправляет уведомления
    о редактированиях, удалениях и одноразовых медиа.

    Поддерживает storage-чат для хранения медиа в Telegram,
    ежечасный бекап БД, миграцию локальных файлов.
"""

__version__ = (0, 3, 0)

# meta developer: @Mr4epTuk
# scope: hikka_only
# requires: aiosqlite
# meta banner: https://raw.githubusercontent.com/ArchieZM/UtiliseMr4epTuk/main/SaveDeleted.png

import os
import re
import json
import shutil
import base64
import asyncio
import difflib
import logging
import aiosqlite
import hashlib
from datetime import datetime
from telethon import events
from telethon.utils import pack_bot_file_id
from telethon.tl.types import Message, MessageEmpty, InputDocument, DocumentAttributeSticker, InputStickerSetID, InputStickerSetEmpty
import telethon.tl.types as types
from .. import loader, utils

logger = logging.getLogger(__name__)


@loader.tds
class SaveDeletedMod(loader.Module):
    """Saver for deleted/edited messages, stickers and one-time media"""

    strings = {
        "name": "SaveDeleted",
        "added_wl": "<b><tg-emoji emoji-id='5870633910337015697'>✅</tg-emoji> {chat} added to Whitelist.</b>",
        "rm_wl": "<b><tg-emoji emoji-id='5870931487146119264'>❗️</tg-emoji> {chat} removed from Whitelist.</b>",
        "added_bl": "<b><tg-emoji emoji-id='5870633910337015697'>✅</tg-emoji> {chat} added to Blacklist.</b>",
        "rm_bl": "<b><tg-emoji emoji-id='5870931487146119264'>❗️</tg-emoji> {chat} removed from Blacklist.</b>",
        "db_cleared": "<b><tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> Database successfully cleared!</b>",
        "chat_cleared": "<b><tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> History for {chat} cleared!</b>",
        "not_found": "<b><tg-emoji emoji-id='5870931487146119264'>❗️</tg-emoji> Chat or user not found.</b>",

        "mass_del": "<b><tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> Mass Deletion ({count} messages)</b>",

        "del_pm": "<tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> {sender} deleted {label}",
        "del_fwd": "<tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> {sender} deleted forwarded {label} from {fwd}",
        "del_group": "<tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> {sender} deleted {label} from group {chat}",
        "del_fwd_group": "<tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> {sender} deleted forwarded {label} from {fwd} from group {chat}",
        "del_channel": "<tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> {chat} deleted {label}",
        "del_fwd_channel": "<tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> {chat} deleted forwarded {label} from {fwd}",

        "edited": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> {sender} {actions}",
        "ed_group": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> {sender} {actions} in group {chat}",
        "ed_channel": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> {chat} {actions}",

        "onetime": "<tg-emoji emoji-id='5256054975389247793'>🔥</tg-emoji> {sender} sent {label_ot}",

        "empty": "<i>empty</i>",
        "old_text": "Old text",
        "new_text": "New text",
        "deleted_text": "Deleted text",
        "diff": "Changes",
        "text": "Text",
        "caption": "Caption",

        "label_photo": "Photo",
        "label_video": "Video",
        "label_voice": "Voice message",
        "label_round": "Video message",
        "label_audio": "Audio",
        "label_document": "File",
        "label_sticker": "Sticker",
        "label_contact": "Contact",
        "label_geo": "Location",
        "label_none": "Message",
        "label_service": "System message",
        "label_poll": "Poll",

        "label_dat_photo": "Photo",
        "label_dat_video": "Video",
        "label_dat_voice": "Voice message",
        "label_dat_round": "Video message",
        "label_dat_audio": "Audio",
        "label_dat_document": "File",
        "label_dat_sticker": "Sticker",
        "label_dat_contact": "Contact",
        "label_dat_geo": "Location",
        "label_dat_none": "Message",
        "label_dat_service": "System message",
        "label_dat_poll": "Poll",
        "label_gen_photo": "Photo",
        "label_gen_video": "Video",
        "label_gen_voice": "Voice message",
        "label_gen_round": "Video message",
        "label_gen_audio": "Audio",
        "label_gen_document": "File",
        "label_gen_sticker": "Sticker",
        "label_gen_contact": "Contact",
        "label_gen_geo": "Location",
        "label_gen_none": "Message",
        "label_gen_service": "System message",
        "label_gen_poll": "Poll",

        "label_ot_photo": "one-time photo",
        "label_ot_video": "one-time video",
        "label_ot_voice": "one-time voice message",
        "label_ot_round": "one-time video message",
        "label_ot_audio": "one-time audio",
        "label_ot_document": "one-time file",
        "label_ot_sticker": "one-time sticker",
        "label_ot_contact": "one-time contact",
        "label_ot_geo": "one-time location",
        "label_ot_none": "one-time message",
        "label_ot_service": "one-time system message",
        "label_ot_poll": "one-time poll",

        "backup_start": "<b>📦 Starting backup... {done}/{total}</b>",
        "backup_done": "<b>✅ Backup complete: {done}/{total} files migrated to storage.</b>",
        "backup_no_storage": "<b>❗ Storage chat not configured. Set storage_chat_id in config.</b>",
        "backup_nothing": "<b>✅ No local files to migrate.</b>",

        "act_added_text": {
            "photo": "added text to {label}",
            "video": "added text to {label}",
            "voice": "added text to {label}",
            "round": "added text to {label}",
            "audio": "added text to {label}",
            "document": "added text to {label}",
            "contact": "added text to {label}",
            "geo": "added text to {label}",
            "service": "added text to {label}",
            "poll": "added text to {label}",
            "none": "added text to {label}",
        },
        "act_removed_text": {
            "photo": "removed text from {label}",
            "video": "removed text from {label}",
            "voice": "removed text from {label}",
            "round": "removed text from {label}",
            "audio": "removed text from {label}",
            "document": "removed text from {label}",
            "contact": "removed text from {label}",
            "geo": "removed text from {label}",
            "service": "removed text from {label}",
            "poll": "removed text from {label}",
            "none": "removed text from {label}",
        },
        "act_edited_text": {
            "photo": "edited {label}",
            "video": "edited {label}",
            "voice": "edited {label}",
            "round": "edited {label}",
            "audio": "edited {label}",
            "document": "edited {label}",
            "contact": "edited {label}",
            "geo": "edited {label}",
            "service": "edited {label}",
            "poll": "edited {label}",
            "none": "edited {label}",
        },
        "act_attached": {
            "photo": "attached {label}",
            "video": "attached {label}",
            "voice": "attached {label}",
            "round": "attached {label}",
            "audio": "attached {label}",
            "document": "attached {label}",
            "sticker": "attached {label}",
            "contact": "attached {label}",
            "geo": "attached {label}",
            "poll": "attached {label}",
            "none": "attached {label}",
        },
        "act_removed_media": {
            "photo": "removed {label}",
            "video": "removed {label}",
            "voice": "removed {label}",
            "round": "removed {label}",
            "audio": "removed {label}",
            "document": "removed {label}",
            "sticker": "removed {label}",
            "contact": "removed {label}",
            "geo": "removed {label}",
            "poll": "removed {label}",
            "none": "removed {label}",
        },
        "act_replaced": {
            "photo": "replaced {label}",
            "video": "replaced {label}",
            "voice": "replaced {label}",
            "round": "replaced {label}",
            "audio": "replaced {label}",
            "document": "replaced {label}",
            "sticker": "replaced {label}",
            "contact": "replaced {label}",
            "geo": "replaced {label}",
            "poll": "replaced {label}",
            "none": "replaced {label}",
        },
        "m_photo": "Photo",
        "m_video": "Video",
        "m_voice": "Voice message",
        "m_round": "Video message",
        "m_audio": "Audio",
        "m_document": "File",
        "m_sticker": "Sticker",
        "m_contact": "Contact",
        "m_geo": "Location",
        "m_none": "Message",
        "m_service": "System message",
        "m_poll": "Poll",
    }

    strings_ru = {
        "name": "SaveDeleted",
        "added_wl": "<b><tg-emoji emoji-id='5870633910337015697'>✅</tg-emoji> {chat} добавлен в Белый список.</b>",
        "rm_wl": "<b><tg-emoji emoji-id='5870931487146119264'>❗️</tg-emoji> {chat} удален из Белого списка.</b>",
        "added_bl": "<b><tg-emoji emoji-id='5870633910337015697'>✅</tg-emoji> {chat} добавлен в Черный список.</b>",
        "rm_bl": "<b><tg-emoji emoji-id='5870931487146119264'>❗️</tg-emoji> {chat} удален из Черного списка.</b>",
        "db_cleared": "<b><tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> База данных полностью очищена!</b>",
        "chat_cleared": "<b><tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> История чата {chat} удалена из БД!</b>",
        "not_found": "<b><tg-emoji emoji-id='5870931487146119264'>❗️</tg-emoji> Чат или пользователь не найден.</b>",

        "mass_del": "<b><tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> Массовое удаление ({count} сообщений)</b>",

        "del_pm": "<tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> {sender} удалил(а) {label}",
        "del_fwd": "<tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> {sender} удалил(а) пересланное {label} от {fwd}",
        "del_group": "<tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> {sender} удалил(а) {label} из группы {chat}",
        "del_fwd_group": "<tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> {sender} удалил(а) пересланное {label} от {fwd} из группы {chat}",
        "del_channel": "<tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> {chat} удалил(а) {label}",
        "del_fwd_channel": "<tg-emoji emoji-id='5879896690210639947'>🗑</tg-emoji> {chat} удалил(а) пересланное {label} от {fwd}",

        "edited": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> {sender} {actions}",
        "ed_group": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> {sender} {actions} в группе {chat}",
        "ed_channel": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> {chat} {actions}",

        "onetime": "<tg-emoji emoji-id='5256054975389247793'>🔥</tg-emoji> {sender} отправил(а) {label_ot}",

        "empty": "<i>пусто</i>",
        "old_text": "Старый текст",
        "new_text": "Новый текст",
        "deleted_text": "Удалённый текст",
        "diff": "Изменилось",
        "text": "Текст",
        "caption": "Подпись",

        "label_photo": "фото",
        "label_video": "видео",
        "label_voice": "голосовое",
        "label_round": "кружок",
        "label_audio": "аудио",
        "label_document": "файл",
        "label_sticker": "стикер",
        "label_contact": "контакт",
        "label_geo": "локация",
        "label_none": "сообщение",
        "label_service": "системное сообщение",
        "label_poll": "опрос",

        "backup_start": "<b>📦 Бекап... {done}/{total}</b>",
        "backup_done": "<b>✅ Бекап завершен: {done}/{total} файлов перенесено в storage.</b>",
        "backup_no_storage": "<b>❗ Storage-чат не настроен. Укажи storage_chat_id в конфиге.</b>",
        "backup_nothing": "<b>✅ Нет локальных файлов для миграции.</b>",

        "act_added_text": {
            "photo": "добавил(а) текст к {label_dat}",
            "video": "добавил(а) текст к {label_dat}",
            "voice": "добавил(а) текст к {label_dat}",
            "round": "добавил(а) текст к {label_dat}",
            "audio": "добавил(а) текст к {label_dat}",
            "document": "добавил(а) текст к {label_dat}",
            "contact": "добавил(а) текст к {label_dat}",
            "geo": "добавил(а) текст к {label_dat}",
            "service": "добавил(а) текст к {label_dat}",
            "poll": "добавил(а) текст к {label_dat}",
            "none": "добавил(а) текст к {label_dat}",
        },
        "act_removed_text": {
            "photo": "убрал(а) текст из {label_gen}",
            "video": "убрал(а) текст из {label_gen}",
            "voice": "убрал(а) текст из {label_gen}",
            "round": "убрал(а) текст из {label_gen}",
            "audio": "убрал(а) текст из {label_gen}",
            "document": "убрал(а) текст из {label_gen}",
            "contact": "убрал(а) текст из {label_gen}",
            "geo": "убрал(а) текст из {label_gen}",
            "service": "убрал(а) текст из {label_gen}",
            "poll": "убрал(а) текст из {label_gen}",
            "none": "убрал(а) текст из {label_gen}",
        },
        "act_edited_text": {
            "photo": "изменил(а) текст {label_gen}",
            "video": "изменил(а) текст {label_gen}",
            "voice": "изменил(а) текст {label_gen}",
            "round": "изменил(а) текст {label_gen}",
            "audio": "изменил(а) текст {label_gen}",
            "document": "изменил(а) подпись к {label_dat}",
            "contact": "изменил(а) текст {label_gen}",
            "geo": "изменил(а) текст {label_gen}",
            "service": "изменил(а) текст {label_gen}",
            "poll": "изменил(а) текст {label_gen}",
            "none": "изменил(а) {label}",
        },
        "act_attached": {
            "photo": "прикрепил(а) {label}",
            "video": "прикрепил(а) {label}",
            "voice": "прикрепил(а) {label}",
            "round": "прикрепил(а) {label}",
            "audio": "прикрепил(а) {label}",
            "document": "прикрепил(а) {label}",
            "sticker": "прикрепил(а) {label}",
            "contact": "прикрепил(а) {label}",
            "geo": "прикрепил(а) {label}",
            "poll": "прикрепил(а) {label}",
            "none": "прикрепил(а) {label}",
        },
        "act_removed_media": {
            "photo": "убрал(а) {label}",
            "video": "убрал(а) {label}",
            "voice": "убрал(а) {label}",
            "round": "убрал(а) {label}",
            "audio": "убрал(а) {label}",
            "document": "убрал(а) {label}",
            "sticker": "убрал(а) {label}",
            "contact": "убрал(а) {label}",
            "geo": "убрал(а) {label}",
            "poll": "убрал(а) {label}",
            "none": "убрал(а) {label}",
        },
        "act_replaced": {
            "photo": "перезалил(а) {label}",
            "video": "перезалил(а) {label}",
            "voice": "перезалил(а) {label}",
            "round": "перезалил(а) {label}",
            "audio": "перезалил(а) {label}",
            "document": "перезалил(а) {label}",
            "sticker": "перезалил(а) {label}",
            "contact": "перезалил(а) {label}",
            "geo": "перезалил(а) {label}",
            "poll": "перезалил(а) {label}",
            "none": "перезалил(а) {label}",
        },
        "label_dat_photo": "фото",
        "label_dat_video": "видео",
        "label_dat_voice": "голосовому",
        "label_dat_round": "кружку",
        "label_dat_audio": "аудио",
        "label_dat_document": "файлу",
        "label_dat_sticker": "стикеру",
        "label_dat_contact": "контакту",
        "label_dat_geo": "локации",
        "label_dat_none": "сообщению",
        "label_dat_service": "системному сообщению",
        "label_dat_poll": "опросу",
        "label_gen_photo": "фото",
        "label_gen_video": "видео",
        "label_gen_voice": "голосового",
        "label_gen_round": "кружка",
        "label_gen_audio": "аудио",
        "label_gen_document": "файла",
        "label_gen_sticker": "стикера",
        "label_gen_contact": "контакта",
        "label_gen_geo": "локации",
        "label_gen_none": "сообщения",
        "label_gen_service": "системного сообщения",
        "label_gen_poll": "опроса",
        "label_ot_photo": "одноразовое фото",
        "label_ot_video": "одноразовое видео",
        "label_ot_voice": "одноразовое голосовое",
        "label_ot_round": "одноразовый кружок",
        "label_ot_audio": "одноразовое аудио",
        "label_ot_document": "одноразовый файл",
        "label_ot_sticker": "одноразовый стикер",
        "label_ot_contact": "одноразовый контакт",
        "label_ot_geo": "одноразовую локацию",
        "label_ot_none": "одноразовое сообщение",
        "label_ot_service": "одноразовое системное сообщение",
        "label_ot_poll": "одноразовый опрос",
        "m_photo": "Фото",
        "m_video": "Видео",
        "m_voice": "Голосовое",
        "m_round": "Кружок",
        "m_audio": "Аудио",
        "m_document": "Файл",
        "m_sticker": "Стикер",
        "m_contact": "Контакт",
        "m_geo": "Локация",
        "m_none": "Сообщение",
        "m_service": "Системное сообщение",
        "m_poll": "Опрос",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("custom_bot_token", "", "Токен кастомного бота / Custom bot token",
                validator=loader.validators.Hidden(loader.validators.String())),
            loader.ConfigValue("storage_chat_id", 0, "ID чата для хранения медиа и бекапов / Storage chat ID (0=выкл)",
                validator=loader.validators.Integer()),
            loader.ConfigValue("show_diff", True, "Показывать изменения / Show diff",
                validator=loader.validators.Boolean()),
            loader.ConfigValue("show_msg_link", True, "Показывать ссылку на сообщение / Show link to message",
                validator=loader.validators.Boolean()),
            loader.ConfigValue("save_pm", True, "Сохранять ЛС / Save PMs",
                validator=loader.validators.Boolean()),
            loader.ConfigValue("save_groups", False, "Сохранять группы / Save Groups",
                validator=loader.validators.Boolean()),
            loader.ConfigValue("save_channels", False, "Сохранять каналы / Save Channels",
                validator=loader.validators.Boolean()),
            loader.ConfigValue("save_bots", True, "Сохранять сообщения от ботов / Save messages from bots",
                validator=loader.validators.Boolean()),
            loader.ConfigValue("save_own", False, "Сохранять свои сообщения / Save own messages",
                validator=loader.validators.Boolean()),
            loader.ConfigValue("save_deleted", True, "Сохранять удалённые / Save deleted",
                validator=loader.validators.Boolean()),
            loader.ConfigValue("save_edited", True, "Сохранять изменения / Save edits",
                validator=loader.validators.Boolean()),
            loader.ConfigValue("save_onetime", True, "Сохранять одноразовые / Save one-time media",
                validator=loader.validators.Boolean()),
            loader.ConfigValue("use_whitelist", False, "Использовать Белый список / Use Whitelist",
                validator=loader.validators.Boolean()),
            loader.ConfigValue("keep_full_history", False, "Безлимитная история и кэш / Unlimited history cache",
                validator=loader.validators.Boolean()),
            loader.ConfigValue("auto_cleanup_days", 0, "Удалять записи старше X дней / Delete older than X days (0=off)",
                validator=loader.validators.Integer(minimum=0)),
        )
        self.db_path = "savedeleted_cache.db"
        self.media_dir = "savedeleted_media"
        self._bg_tasks = set()
        self.cached_chats = set()
        self._db_conn = None
        self._db_lock = None
        self._cache_lock = None
        self._bot_cache = {}
        self._bot_cache_time = {}
        self._custom_bot = None
        self._custom_bot_token = ""

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._tg_id = (await client.get_me()).id

        logging.getLogger("aiosqlite").setLevel(logging.WARNING)

        os.makedirs(self.media_dir, exist_ok=True)

        self._db_lock = asyncio.Lock()
        self._cache_lock = asyncio.Lock()

        self._db_conn = await aiosqlite.connect(self.db_path)
        await self._db_conn.execute("PRAGMA journal_mode=WAL")
        await self._db_conn.execute("PRAGMA synchronous=NORMAL")
        await self._db_conn.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                msg_id INTEGER,
                chat_id INTEGER,
                sender_id INTEGER,
                text TEXT,
                media_path TEXT,
                timestamp REAL,
                PRIMARY KEY (msg_id, chat_id)
            )
        ''')
        await self._db_conn.execute('CREATE TABLE IF NOT EXISTS auto_cached (chat_id INTEGER PRIMARY KEY)')

        cursor = await self._db_conn.execute("PRAGMA table_info(messages)")
        cols = [row[1] for row in await cursor.fetchall()]
        if "media_type" not in cols:
            await self._db_conn.execute("ALTER TABLE messages ADD COLUMN media_type TEXT DEFAULT 'none'")
        if "media_id" not in cols:
            await self._db_conn.execute("ALTER TABLE messages ADD COLUMN media_id TEXT DEFAULT ''")
        if "fwd_info" not in cols:
            await self._db_conn.execute("ALTER TABLE messages ADD COLUMN fwd_info TEXT DEFAULT ''")
        if "bot_file_id" not in cols:
            await self._db_conn.execute("ALTER TABLE messages ADD COLUMN bot_file_id TEXT DEFAULT ''")

        await self._db_conn.execute("CREATE INDEX IF NOT EXISTS idx_msg_chat ON messages(msg_id, chat_id)")
        await self._db_conn.execute("CREATE INDEX IF NOT EXISTS idx_chat ON messages(chat_id)")
        await self._db_conn.execute("CREATE INDEX IF NOT EXISTS idx_ts ON messages(timestamp)")
        await self._db_conn.commit()

        cursor = await self._db_conn.execute("SELECT chat_id FROM auto_cached")
        async for row in cursor:
            self.cached_chats.add(row[0])

        self.whitelist = self.pointer("sd_whitelist", [])
        self.blacklist = self.pointer("sd_blacklist", [])
        self._sticker_meta = self.pointer("sd_stickers", {})

        self._run_in_background(self._cleanup_loop())
        self._run_in_background(self._backup_db_loop())
        self._client.add_event_handler(self.on_deleted, events.MessageDeleted())
        self._client.add_event_handler(self.on_edited, events.MessageEdited())

    async def on_unload(self):
        for task in list(self._bg_tasks):
            task.cancel()
        try:
            self._client.remove_event_handler(self.on_deleted)
            self._client.remove_event_handler(self.on_edited)
        except Exception:
            pass
        await self._close_custom_bot()
        if self._db_conn:
            await self._db_conn.close()

    def _run_in_background(self, coro):
        task = asyncio.ensure_future(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    def _bare_id(self, cid: int) -> str:
        s = str(abs(cid))
        if s.startswith("100") and len(s) > 10:
            return s[3:]
        return s

    def _peer_to_id(self, peer) -> int:
        if isinstance(peer, types.PeerUser):
            return peer.user_id
        if isinstance(peer, types.PeerChat):
            return -peer.chat_id
        if isinstance(peer, types.PeerChannel):
            return -1000000000000 - peer.channel_id
        return 0

    async def _get_custom_bot(self):
        token = self.config["custom_bot_token"]
        if isinstance(token, str):
            token = token.strip()

        if not token:
            if self._custom_bot:
                await self._close_custom_bot()
            return None

        if token != self._custom_bot_token:
            if self._custom_bot:
                await self._close_custom_bot()
            try:
                import aiogram
                self._custom_bot = aiogram.Bot(token=token)
                self._custom_bot_token = token
            except Exception:
                self._custom_bot = None
                self._custom_bot_token = ""

        return self._custom_bot

    async def _close_custom_bot(self):
        if self._custom_bot:
            try:
                import aiogram
                if aiogram.__version__.startswith("3"):
                    await self._custom_bot.session.close()
                else:
                    await self._custom_bot.close()
            except Exception:
                pass
            self._custom_bot = None
            self._custom_bot_token = ""

    async def _is_bot_sender(self, user_id):
        now = datetime.now().timestamp()
        if user_id in self._bot_cache:
            if now - self._bot_cache_time.get(user_id, 0) < 3600:
                return self._bot_cache[user_id]
        try:
            entity = await self._client.get_entity(user_id)
            is_bot = getattr(entity, "bot", False)
        except Exception:
            is_bot = False
        self._bot_cache[user_id] = is_bot
        self._bot_cache_time[user_id] = now
        return is_bot

    async def _should_save(self, message, chat_id, sender_id, whitelisted=False):
        if getattr(message, "out", False):
            return self.config["save_own"]

        if sender_id in (777000, 489000):
            return False

        if not self.config["save_bots"]:
            if await self._is_bot_sender(sender_id):
                return False

        if whitelisted:
            return True

        if getattr(message, "is_private", False):
            return self.config["save_pm"]

        if getattr(message, "is_group", False):
            return self.config["save_groups"]

        if getattr(message, "is_channel", False):
            chat = getattr(message, "chat", None)
            if chat is None:
                try:
                    chat = await self._client.get_entity(chat_id)
                except Exception:
                    pass
            if chat is not None and getattr(chat, "megagroup", False):
                return self.config["save_groups"]
            if chat is not None and hasattr(chat, "broadcast") and not getattr(chat, "megagroup", False):
                return self.config["save_channels"]
            return self.config["save_groups"] or self.config["save_channels"]

        return self.config["save_groups"] or self.config["save_channels"]

    def _serialize_contact(self, contact) -> str:
        if not contact:
            return ""
        return json.dumps({
            "p": getattr(contact, "phone_number", "") or "",
            "f": getattr(contact, "first_name", "") or "",
            "l": getattr(contact, "last_name", "") or "",
            "v": getattr(contact, "vcard", "") or "",
            "u": getattr(contact, "user_id", 0) or 0,
        }, ensure_ascii=False)

    def _deserialize_contact(self, data: str) -> dict:
        if not data:
            return {"p": "", "f": "", "l": "", "v": "", "u": 0}
        try:
            return json.loads(str(data))
        except Exception:
            return {"p": "", "f": "", "l": "", "v": "", "u": 0}

    def _clean_name(self, text: str) -> str:
        if not text:
            return ""
        return re.sub(r'[\u200B-\u200F\u202A-\u202E\u2066-\u2069\uFEFF]', '', str(text)).strip()

    def _get_media_type(self, message) -> str:
        if getattr(message, "contact", None):
            return "contact"
        if getattr(message, "geo", None):
            return "geo"
        if getattr(message, "action", None):
            return "service"
        if getattr(message, "sticker", None):
            return "sticker"
        if getattr(message, "photo", None):
            return "photo"
        if getattr(message, "voice", None):
            return "voice"
        if getattr(message, "video_note", None):
            return "round"
        if getattr(message, "video", None):
            return "video"
        if getattr(message, "audio", None):
            return "audio"
        if getattr(message, "document", None):
            return "document"
        if getattr(message, "poll", None):
            return "poll"
        return "none"

    def _get_media_type_raw(self, msg) -> str:
        media = getattr(msg, "media", None)
        if not media:
            if getattr(msg, "action", None):
                return "service"
            return "none"

        if isinstance(media, types.MessageMediaContact):
            return "contact"
        if isinstance(media, (types.MessageMediaGeo, types.MessageMediaGeoPoint)):
            return "geo"
        if isinstance(media, types.MessageMediaPhoto):
            return "photo"
        if isinstance(media, types.MessageMediaDocument):
            doc = getattr(media, "document", None)
            if doc:
                for attr in getattr(doc, "attributes", []):
                    if isinstance(attr, DocumentAttributeSticker):
                        return "sticker"
                    if isinstance(attr, types.DocumentAttributeAudio):
                        if getattr(attr, "voice", False):
                            return "voice"
                        return "audio"
                    if isinstance(attr, types.DocumentAttributeVideo):
                        if getattr(attr, "round_message", False):
                            return "round"
                        return "video"
            return "document"
        if isinstance(media, types.MessageMediaWebPage):
            return "none"
        return "none"

    def _get_media_id(self, message) -> str:
        if not getattr(message, "media", None):
            return ""
        try:
            if hasattr(message.media, 'photo') and message.media.photo:
                return str(message.media.photo.id)
            if hasattr(message.media, 'document') and message.media.document:
                return str(message.media.document.id)
        except Exception:
            pass
        return ""

    def _get_media_id_raw(self, msg) -> str:
        media = getattr(msg, "media", None)
        if not media:
            return ""
        try:
            if isinstance(media, types.MessageMediaPhoto) and media.photo:
                return str(media.photo.id)
            if isinstance(media, types.MessageMediaDocument) and media.document:
                return str(media.document.id)
        except Exception:
            pass
        return ""

    def _get_sender_id_raw(self, msg) -> int:
        from_id = getattr(msg, "from_id", None)
        if from_id:
            if isinstance(from_id, types.PeerUser):
                return from_id.user_id
            return self._peer_to_id(from_id)
        peer = getattr(msg, "peer_id", None)
        if peer:
            return self._peer_to_id(peer)
        return 0

    def _safe_parse_text(self, message) -> str:
        text = ""
        try:
            text = getattr(message, "message", "") or ""
            if getattr(message, "action", None):
                act = message.action
                if isinstance(act, types.MessageActionPhoneCall):
                    if getattr(act, "video", False):
                        text += "Video call"
                    else:
                        text += "Call"
                    if getattr(act, "duration", 0):
                        text += f" ({act.duration} sec)"
                elif isinstance(act, types.MessageActionPinMessage):
                    text += "pinned a message"
                elif isinstance(act, types.MessageActionChatAddUser):
                    text += "added user"
                elif isinstance(act, types.MessageActionChatDeleteUser):
                    text += "left the group"
                elif isinstance(act, types.MessageActionChatJoinedByLink):
                    text += "joined via link"
                elif isinstance(act, types.MessageActionChatCreate):
                    text += "created group"
                elif isinstance(act, types.MessageActionChannelCreate):
                    text += "created channel"
                elif isinstance(act, types.MessageActionChatEditTitle):
                    text += "changed group name"
                elif isinstance(act, types.MessageActionChatEditPhoto):
                    text += "changed group photo"
                elif isinstance(act, types.MessageActionChatDeletePhoto):
                    text += "removed group photo"
                elif isinstance(act, types.MessageActionGameScore):
                    text += f"scored {act.score}"
                elif isinstance(act, types.MessageActionSetMessagesTTL):
                    text += "set auto-delete timer"
                elif isinstance(act, types.MessageActionScreenshotTaken):
                    text += "took a screenshot"
                elif isinstance(act, types.MessageActionBotAllowed):
                    text += "allowed bot"
                elif isinstance(act, types.MessageActionContactSignUp):
                    text += "joined Telegram"
                elif isinstance(act, types.MessageActionHistoryClear):
                    text += "cleared history"
                elif isinstance(act, types.MessageActionGiftPremium):
                    text += "gifted Premium"
                elif isinstance(act, types.MessageActionTopicCreate):
                    text += "created topic"
                elif isinstance(act, types.MessageActionTopicEdit):
                    text += "edited topic"
                elif isinstance(act, types.MessageActionGroupCall):
                    text += "started group call"
                elif isinstance(act, types.MessageActionWebViewDataSent):
                    text += "sent data via Web App"
                else:
                    text += "service message"
            media = getattr(message, "media", None)
            if isinstance(media, types.MessageMediaPoll):
                poll = getattr(media, "poll", None)
                if poll and hasattr(poll, "question"):
                    text_attr = getattr(poll.question, "text", poll.question) if hasattr(poll.question, "text") else poll.question
                    text += f"\nPoll: {utils.escape_html(str(text_attr))}"
        except Exception:
            pass
        return text.strip()

    def _safe_chunk_text(self, text: str, limit: int = 3000) -> list:
        if len(text) <= limit:
            return [text]

        chunks = []
        while text:
            if len(text) <= limit:
                chunks.append(text)
                break

            cut = limit
            last_lt = text.rfind('<', 0, cut)
            last_gt = text.rfind('>', 0, cut)
            if last_lt > last_gt:
                cut = last_lt

            nl = text.rfind('\n', max(cut // 2, 0), cut)
            if nl > 0:
                cut = nl + 1
            else:
                sp = text.rfind(' ', max(cut // 2, 0), cut)
                if sp > 0:
                    cut = sp + 1

            chunks.append(text[:cut])
            text = text[cut:]

        return chunks

    def _build_message(self, header: str, text_blocks: list) -> list:
        parts = []
        curr = header + "\n"
        for label, text in text_blocks:
            if not text:
                continue
            chunks = self._safe_chunk_text(text)
            for i, chunk in enumerate(chunks):
                prefix = label if i == 0 else ""
                addition = f"{prefix} <blockquote expandable>{chunk}</blockquote>\n" if prefix else f"<blockquote expandable>{chunk}</blockquote>\n"
                if len(curr) + len(addition) > 3800:
                    parts.append(curr)
                    curr = addition
                else:
                    curr += addition
        if curr.strip():
            parts.append(curr.strip())
        return parts

    def _generate_diff(self, old_text: str, new_text: str) -> str:
        old_tokens = [t for t in re.split(r'(\s+)', str(old_text)) if t]
        new_tokens = [t for t in re.split(r'(\s+)', str(new_text)) if t]
        diff = difflib.ndiff(old_tokens, new_tokens)
        result = []
        for token in diff:
            code = token[:2]
            text = token[2:]
            if code == "- ":
                result.append(f"<s>{utils.escape_html(text)}</s>" if not text.isspace() else text)
            elif code == "+ ":
                result.append(f"<b>{utils.escape_html(text)}</b>" if not text.isspace() else text)
            elif code == "  ":
                result.append(utils.escape_html(text))
        return "".join(result)

    def _get_msg_link(self, chat_id: int, msg_id: int, text: str, c_type: str = "") -> str:
        if not self.config["show_msg_link"]:
            return text
        if c_type == "pm" or (chat_id > 0 and not c_type):
            url = f"tg://openmessage?user_id={chat_id}&message_id={msg_id}"
        else:
            url = f"tg://openmessage?chat_id={self._bare_id(chat_id)}&message_id={msg_id}"
        parts = re.split(r'(<tg-emoji[^>]*>.*?</tg-emoji>)', text)
        res = ""
        for part in parts:
            if part.startswith("<tg-emoji"):
                res += part
            elif part.strip():
                res += f'<a href="{url}">{part}</a>'
            else:
                res += part
        return res

    async def _get_fwd_info(self, message) -> str:
        if not getattr(message, "fwd_from", None):
            return ""
        try:
            if message.fwd_from.from_name:
                return utils.escape_html(self._clean_name(message.fwd_from.from_name))
            if message.forward:
                if message.forward.sender:
                    sender = message.forward.sender
                    name = utils.escape_html(self._clean_name(getattr(sender, 'first_name', getattr(sender, 'title', 'User'))))
                    if getattr(sender, 'username', None):
                        return f'<a href="https://t.me/{sender.username}">{name}</a>'
                    return f'<a href="tg://openmessage?user_id={sender.id}">{name}</a>'
                elif message.forward.chat:
                    chat = message.forward.chat
                    name = utils.escape_html(self._clean_name(getattr(chat, 'title', 'Chat')))
                    if getattr(chat, 'username', None):
                        return f'<a href="https://t.me/{chat.username}">{name}</a>'
                    return f'<a href="tg://openmessage?chat_id={self._bare_id(chat.id)}">{name}</a>'
            if message.fwd_from.from_id:
                peer = message.fwd_from.from_id
                try:
                    entity = await self._client.get_entity(peer)
                    if isinstance(entity, types.User):
                        name = utils.escape_html(self._clean_name(getattr(entity, 'first_name', 'User') or 'User'))
                        if getattr(entity, 'username', None):
                            return f'<a href="https://t.me/{entity.username}">{name}</a>'
                        return f'<a href="tg://openmessage?user_id={entity.id}">{name}</a>'
                    elif isinstance(entity, (types.Channel, types.Chat)):
                        name = utils.escape_html(self._clean_name(getattr(entity, 'title', 'Chat') or 'Chat'))
                        if getattr(entity, 'username', None):
                            return f'<a href="https://t.me/{entity.username}">{name}</a>'
                        return f'<a href="tg://openmessage?chat_id={self._bare_id(entity.id)}">{name}</a>'
                except Exception:
                    pass
                if isinstance(peer, types.PeerUser):
                    return f'<a href="tg://openmessage?user_id={peer.user_id}">User {peer.user_id}</a>'
                elif isinstance(peer, types.PeerChannel):
                    return f'<a href="tg://openmessage?chat_id={peer.channel_id}">Channel {peer.channel_id}</a>'
                elif isinstance(peer, types.PeerChat):
                    return f'<a href="tg://openmessage?chat_id={peer.chat_id}">Chat {peer.chat_id}</a>'
        except Exception:
            pass
        return "Unknown"

    async def _get_chat_info(self, chat_id: int) -> str:
        info, _ = await self._get_chat_info_and_type(chat_id)
        return info

    async def _get_chat_info_and_type(self, chat_id: int):
        try:
            chat = await self._client.get_entity(chat_id)
            if getattr(chat, "title", None):
                link = f'<a href="tg://openmessage?chat_id={self._bare_id(chat_id)}">{utils.escape_html(self._clean_name(chat.title))}</a>'
                if isinstance(chat, types.Chat) or getattr(chat, "megagroup", False):
                    ctype = "group"
                else:
                    ctype = "channel"
                return link, ctype
        except Exception:
            pass
        if chat_id < 0:
            return f"Chat {self._bare_id(chat_id)}", "group"
        return "\u041b\u0421 (PM)", "pm"

    async def _get_user_link(self, user_id: int) -> str:
        if not user_id:
            return "Telegram"
        try:
            user = await self._client.get_entity(user_id)
            name = utils.escape_html(self._clean_name(getattr(user, 'first_name', 'User') or 'User'))
            if getattr(user, 'username', None):
                return f'<a href="https://t.me/{user.username}">{name}</a>'
            return f'<a href="tg://openmessage?user_id={user_id}">{name}</a>'
        except Exception:
            return f'<a href="tg://openmessage?user_id={user_id}">User {user_id}</a>'

    async def _save_media_to_storage(self, media_path, storage_id, caption=""):
        if not media_path or not os.path.exists(str(media_path)):
            return media_path

        custom_bot = await self._get_custom_bot()
        bot = custom_bot or getattr(self.inline, "bot", None)

        try:
            if bot:
                try:
                    await utils.invite_inline_bot(self._client, await self._client.get_entity(storage_id))
                except Exception:
                    pass

                import aiogram
                is_aio_3 = aiogram.__version__.startswith("3")
                if is_aio_3:
                    from aiogram.types import FSInputFile
                    media = FSInputFile(media_path)
                else:
                    from aiogram.types import InputFile
                    media = InputFile(media_path)

                sent = await bot.send_document(storage_id, media, caption=caption[:1024] if caption else None)
                return f"tg://{sent.message_id}"
            else:
                sent = await self._client.send_file(storage_id, media_path, force_document=True, caption=caption[:1024] if caption else None)
                return f"tg://{sent.id}"
        except Exception as e:
            logger.warning("Failed to send media to storage: %s", e)
            return media_path

    async def _get_media_from_storage(self, media_path):
        if not media_path:
            return None
        if media_path.startswith("tg://"):
            try:
                msg_id = int(media_path.replace("tg://", ""))
                storage_id = self.config["storage_chat_id"]
                msg = await self._client.get_messages(storage_id, ids=msg_id)
                if msg:
                    return await msg.download_media("/tmp/")
            except Exception:
                pass
            return None
        if os.path.exists(str(media_path)):
            return media_path
        return None

    def _cleanup_media_file(self, media_path, media_type):
        if not media_path or media_path.startswith("tg://"):
            return
        if media_type in ("geo", "contact", "service"):
            return
        try:
            if os.path.exists(media_path):
                os.remove(media_path)
        except Exception:
            pass

    async def _send_to_bot(self, parts: list, media_path: str = None, media_type: str = "none", meta_key: str = ""):
        if not parts:
            return

        temp_media = None
        if media_path and media_path.startswith("tg://"):
            temp_media = await self._get_media_from_storage(media_path)
            media_path = temp_media or media_path

        if media_type == "sticker":
            custom_bot = await self._get_custom_bot()
            bot = custom_bot or getattr(self.inline, "bot", None)
            bot_file_id = self._sticker_meta.get(meta_key, {}).get("bot_file_id", "") if meta_key else ""
            try:
                if bot and bot_file_id:
                    sent_sticker = await bot.send_sticker(self._tg_id, sticker=bot_file_id)
                    import aiogram
                    for part in parts:
                        kwargs = {"chat_id": self._tg_id, "text": part, "parse_mode": "HTML", "reply_to_message_id": sent_sticker.message_id}
                        try:
                            from aiogram.types import LinkPreviewOptions
                            kwargs["link_preview_options"] = LinkPreviewOptions(is_disabled=True)
                        except ImportError:
                            kwargs["disable_web_page_preview"] = True
                        await bot.send_message(**kwargs)
                    if temp_media:
                        self._cleanup_media_file(temp_media, media_type)
                    return
            except Exception as e:
                logger.error("Bot sticker failed: %s, trying MTProto fallback", e)

            result = await self._fallback_send(parts, media_path, media_type, meta_key)
            if temp_media:
                self._cleanup_media_file(temp_media, media_type)
            return result

        custom_bot = await self._get_custom_bot()
        bot = custom_bot or getattr(self.inline, "bot", None)
        if not bot:
            result = await self._fallback_send(parts, media_path, media_type, meta_key)
            if temp_media:
                self._cleanup_media_file(temp_media, media_type)
            return result

        try:
            import aiogram
            is_aio_3 = aiogram.__version__.startswith("3")
            sent_msg = None

            first_part = parts[0]
            caption = first_part if len(first_part) <= 1024 and media_type not in ("round", "contact", "geo", "service", "sticker") else "<b>Report</b>"

            if media_type == "contact" and media_path:
                c = self._deserialize_contact(str(media_path))
                if c["p"] and c["f"]:
                    media = types.MessageMediaContact(phone_number=c["p"], first_name=c["f"], last_name=c["l"], vcard=c["v"], user_id=c["u"])
                    try:
                        sent_msg = await self._client.send_message("me", file=media)
                    except Exception:
                        sent_msg = None
            elif media_type == "geo" and media_path:
                p = str(media_path).split("|")
                if len(p) >= 2:
                    sent_msg = await bot.send_location(self._tg_id, latitude=float(p[0]), longitude=float(p[1]))
            elif media_path and os.path.exists(str(media_path)):
                if os.path.getsize(media_path) < 50 * 1024 * 1024:
                    if is_aio_3:
                        from aiogram.types import FSInputFile
                        media = FSInputFile(media_path)
                    else:
                        from aiogram.types import InputFile
                        media = InputFile(media_path)
                    if media_type == "photo" or (media_type == "none" and media_path.lower().endswith(('.jpg', '.jpeg', '.png'))):
                        sent_msg = await bot.send_photo(self._tg_id, photo=media, caption=caption, parse_mode="HTML")
                    elif media_type == "video":
                        try:
                            sent_msg = await bot.send_video(self._tg_id, video=media, caption=caption, parse_mode="HTML")
                        except Exception:
                            if is_aio_3:
                                from aiogram.types import FSInputFile
                                media = FSInputFile(media_path)
                            else:
                                from aiogram.types import InputFile
                                media = InputFile(media_path)
                            sent_msg = await bot.send_document(self._tg_id, document=media, caption=caption, parse_mode="HTML")
                    elif media_type == "round":
                        try:
                            sent_msg = await bot.send_video_note(self._tg_id, video_note=media)
                        except Exception:
                            sent_msg = None
                    elif media_type == "voice":
                        sent_msg = await bot.send_voice(self._tg_id, voice=media, caption=caption, parse_mode="HTML")
                    elif media_type == "audio":
                        sent_msg = await bot.send_audio(self._tg_id, audio=media, caption=caption, parse_mode="HTML")
                    elif media_type == "sticker":
                        sent_msg = await bot.send_sticker(self._tg_id, sticker=media)
                    else:
                        sent_msg = await bot.send_document(self._tg_id, document=media, caption=caption, parse_mode="HTML")
                else:
                    await bot.send_message(self._tg_id, text="<i>Media too large (>50MB).</i>", parse_mode="HTML")

            to_send = parts[1:] if sent_msg and caption == first_part and media_type not in ("round", "contact", "geo", "service", "sticker") else parts

            reply_id = getattr(sent_msg, "message_id", None)
            for part in to_send:
                kwargs = {"chat_id": self._tg_id, "text": part, "parse_mode": "HTML", "reply_to_message_id": reply_id}
                try:
                    from aiogram.types import LinkPreviewOptions
                    kwargs["link_preview_options"] = LinkPreviewOptions(is_disabled=True)
                except ImportError:
                    kwargs["disable_web_page_preview"] = True
                sent_msg = await bot.send_message(**kwargs)
            reply_id = getattr(sent_msg, "message_id", None) or getattr(sent_msg, "id", None)

        except Exception as e:
            logger.warning("Bot send failed: %s", e)
            await self._fallback_send(parts, media_path if media_path and os.path.exists(str(media_path)) else None, media_type)
        finally:
            if temp_media:
                self._cleanup_media_file(temp_media, media_type)

    async def _fallback_send(self, parts: list, media_path: str = None, media_type: str = "none", meta_key: str = ""):
        if media_type == "sticker":
            sent_sticker = None

            if meta_key and meta_key in self._sticker_meta:
                sm = self._sticker_meta[meta_key]
                try:
                    file_ref = base64.b64decode(sm["file_ref"])
                    input_doc = InputDocument(id=sm["id"], access_hash=sm["access_hash"], file_reference=file_ref)
                    sent_sticker = await self._client.send_file("me", file=input_doc)
                except Exception as e:
                    logger.error("Sticker InputDocument expired, trying file upload: %s", e)

            if not sent_sticker and media_path and os.path.exists(str(media_path)):
                try:
                    kwargs = {"file": media_path}
                    if meta_key and meta_key in self._sticker_meta:
                        sm = self._sticker_meta[meta_key]
                        attrs = [DocumentAttributeSticker(
                            alt=sm.get("alt", ""),
                            stickerset=InputStickerSetID(id=sm["set_id"], access_hash=sm["set_hash"])
                            if "set_id" in sm else InputStickerSetEmpty(),
                        )]
                        kwargs["attributes"] = attrs
                    else:
                        kwargs["attributes"] = [DocumentAttributeSticker(alt="", stickerset=InputStickerSetEmpty())]
                    sent_sticker = await self._client.send_file("me", **kwargs)
                except Exception as e:
                    logger.error("Sticker file upload failed: %s", e)

            if sent_sticker:
                for text in parts:
                    fb_text = text.replace("<tg-emoji emoji-id=", "<emoji document_id=").replace("</tg-emoji>", "</emoji>")
                    await self._client.send_message("me", fb_text, parse_mode="html", link_preview=False, reply_to=sent_sticker.id)
                return

            logger.error("Sticker send completely failed, sending text-only")

        for i, text in enumerate(parts):
            fb_text = text.replace("<tg-emoji emoji-id=", "<emoji document_id=").replace("</tg-emoji>", "</emoji>")
            try:
                if i == 0:
                    if media_type == "contact" and media_path:
                        c = self._deserialize_contact(str(media_path))
                        if c["p"] and c["f"]:
                            try:
                                media = types.MessageMediaContact(phone_number=c["p"], first_name=c["f"], last_name=c["l"], vcard=c["v"], user_id=c["u"])
                                await self._client.send_message("me", file=media)
                            except Exception:
                                media = types.InputMediaContact(phone_number=c["p"], first_name=c["f"], last_name=c["l"], vcard=c["v"])
                                await self._client.send_message("me", file=media)
                            await self._client.send_message("me", fb_text, parse_mode="html", link_preview=False)
                            continue
                    elif media_type == "geo" and media_path:
                        p = str(media_path).split("|")
                        if len(p) >= 2:
                            await self._client.send_message("me", file=types.InputMediaGeoPoint(geo_point=types.InputGeoPoint(lat=float(p[0]), long=float(p[1]), accuracy_radius=0)))
                            await self._client.send_message("me", fb_text, parse_mode="html", link_preview=False)
                            continue
                    if media_path and os.path.exists(str(media_path)):
                        await self._client.send_file("me", file=media_path, caption=fb_text, parse_mode="html")
                    else:
                        await self._client.send_message("me", fb_text, parse_mode="html", link_preview=False)
                else:
                    await self._client.send_message("me", fb_text, parse_mode="html", link_preview=False)
            except Exception as e:
                logger.warning("Fallback send failed: %s", e)

    async def _db_exec(self, sql, params=None):
        async with self._db_lock:
            cursor = await self._db_conn.execute(sql, params or [])
            return await cursor.fetchall()

    async def _db_exec_many(self, sql, rows):
        async with self._db_lock:
            await self._db_conn.executemany(sql, rows)
            await self._db_conn.commit()

    async def _db_run(self, sql, params=None):
        async with self._db_lock:
            await self._db_conn.execute(sql, params or [])
            await self._db_conn.commit()

    def _get_bot_file_id(self, message) -> str:
        try:
            media = getattr(message, "media", None)
            if not media:
                return ""
            doc = getattr(media, "document", None)
            if doc:
                return pack_bot_file_id(doc)
            photo = getattr(media, "photo", None)
            if photo:
                return pack_bot_file_id(photo)
        except Exception:
            pass
        return ""

    async def _save_to_db(self, message, chat_id: int, media_path, media_type, fwd_info: str, bot_file_id: str = ""):
        parsed_text = self._safe_parse_text(message)
        media_id = self._get_media_id(message)
        try:
            await self._db_run(
                "INSERT OR REPLACE INTO messages (msg_id, chat_id, sender_id, text, media_path, timestamp, media_type, media_id, fwd_info, bot_file_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (getattr(message, "id", 0), chat_id, getattr(message, "sender_id", 0) or self._get_sender_id_raw(message), parsed_text, media_path, datetime.now().timestamp(), media_type, media_id, fwd_info, bot_file_id),
            )
        except Exception as e:
            logger.warning("DB save error: %s", e)

    async def _save_media(self, message, media_type, chat_id=0):
        storage_id = self.config["storage_chat_id"]

        if media_type == "contact" and getattr(message, "contact", None):
            return self._serialize_contact(message.contact)
        if media_type == "geo" and getattr(message, "geo", None):
            return f"{message.geo.lat}|{message.geo.long}"

        if getattr(message, "media", None) and not isinstance(getattr(message, "media", None), types.MessageMediaWebPage):
            if storage_id:
                tmp_path = None
                try:
                    tmp_path = await message.download_media("/tmp/")
                    if tmp_path:
                        mid = getattr(message, "id", 0)
                        sid = getattr(message, "sender_id", 0) or self._get_sender_id_raw(message)
                        caption = f"{mid}|{chat_id}|{media_type}|{sid}"
                        media_path = await self._save_media_to_storage(tmp_path, storage_id, caption)
                        if media_type == "sticker":
                            self._save_sticker_meta(chat_id, mid, message)
                        return media_path
                except Exception as e:
                    logger.warning("Storage upload failed: %s", e)
                finally:
                    if tmp_path and os.path.exists(tmp_path):
                        try:
                            os.remove(tmp_path)
                        except Exception:
                            pass
                return None

            try:
                local_path = await message.download_media(self.media_dir + "/")
                if local_path and media_type == "sticker":
                    self._save_sticker_meta(chat_id, message.id, message)
                return local_path
            except Exception:
                pass
        return None

    def _save_sticker_meta(self, chat_id, msg_id, message):
        try:
            media = getattr(message, "media", None)
            if not media:
                return
            doc = getattr(media, "document", None)
            if not doc:
                return
            bot_file_id = ""
            try:
                bot_file_id = pack_bot_file_id(doc)
            except Exception:
                pass
            meta = {
                "id": doc.id,
                "access_hash": doc.access_hash,
                "file_ref": base64.b64encode(doc.file_reference).decode(),
                "bot_file_id": bot_file_id,
            }
            for attr in getattr(doc, "attributes", []):
                if isinstance(attr, DocumentAttributeSticker):
                    meta["alt"] = attr.alt or ""
                    if isinstance(attr.stickerset, InputStickerSetID):
                        meta["set_id"] = attr.stickerset.id
                        meta["set_hash"] = attr.stickerset.access_hash
                    break
            key = f"{chat_id}_{msg_id}"
            self._sticker_meta[key] = meta
        except Exception as e:
            logger.error("Failed to save sticker meta: %s", e)

    async def _should_cache_chat(self, chat_id):
        if self.config["use_whitelist"]:
            return chat_id in self.whitelist
        if chat_id in self.blacklist:
            return False
        try:
            entity = await self._client.get_entity(chat_id)
            if isinstance(entity, types.User):
                return self.config["save_pm"]
            if isinstance(entity, types.Chat):
                return self.config["save_groups"]
            if isinstance(entity, types.Channel):
                if getattr(entity, "megagroup", False):
                    return self.config["save_groups"]
                return self.config["save_channels"]
        except Exception:
            pass
        return False

    async def _auto_cache_chat(self, chat_id: int):
        async with self._cache_lock:
            if chat_id in self.cached_chats:
                return
            self.cached_chats.add(chat_id)

        if not await self._should_cache_chat(chat_id):
            return

        try:
            await self._db_run("INSERT OR IGNORE INTO auto_cached (chat_id) VALUES (?)", (chat_id,))

            limit = None if self.config["keep_full_history"] else 3000
            batch = []
            async for msg in self._client.iter_messages(chat_id, limit=limit):
                if not self.config["save_own"] and getattr(msg, "out", False):
                    continue
                if not hasattr(msg, "id"):
                    continue
                sender_id = getattr(msg, "sender_id", 0)
                if sender_id in (777000, 489000):
                    continue
                if not self.config["save_bots"] and await self._is_bot_sender(sender_id):
                    continue

                m_type = self._get_media_type(msg)
                m_id = self._get_media_id(msg)
                bf_id = self._get_bot_file_id(msg)
                text = self._safe_parse_text(msg)
                fwd_info = await self._get_fwd_info(msg)

                media_data = None
                if m_type == "contact" and getattr(msg, "contact", None):
                    media_data = self._serialize_contact(msg.contact)
                elif m_type == "geo" and msg.geo:
                    media_data = f"{msg.geo.lat}|{msg.geo.long}"
                elif getattr(msg, "media", None) and self.config["keep_full_history"]:
                    try:
                        media_data = await msg.download_media(self.media_dir + "/")
                    except Exception:
                        pass

                batch.append((msg.id, chat_id, sender_id, text, media_data, datetime.now().timestamp(), m_type, m_id, fwd_info, bf_id))
                if len(batch) >= 500:
                    sql = "INSERT OR IGNORE INTO messages (msg_id, chat_id, sender_id, text, media_path, timestamp, media_type, media_id, fwd_info, bot_file_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    await self._db_exec_many(sql, batch)
                    batch.clear()

            if batch:
                sql = "INSERT OR IGNORE INTO messages (msg_id, chat_id, sender_id, text, media_path, timestamp, media_type, media_id, fwd_info, bot_file_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                await self._db_exec_many(sql, batch)
                batch.clear()
        except Exception as e:
            logger.warning("Auto-cache chat %s failed: %s", chat_id, e)

    async def _cleanup_loop(self):
        while True:
            try:
                days = self.config["auto_cleanup_days"]
                if days > 0:
                    cutoff = datetime.now().timestamp() - (days * 86400)
                    rows = await self._db_exec("SELECT media_path, media_type FROM messages WHERE timestamp < ?", (cutoff,))
                    for row in rows:
                        self._cleanup_media_file(row[0], row[1])
                    await self._db_run("DELETE FROM messages WHERE timestamp < ?", (cutoff,))

                if not self.config["keep_full_history"]:
                    rows = await self._db_exec("SELECT chat_id FROM messages GROUP BY chat_id HAVING COUNT(msg_id) > 3000")
                    for row in rows:
                        c_id = row[0]
                        to_delete = await self._db_exec(
                            "SELECT msg_id, media_path, media_type FROM messages WHERE chat_id = ? ORDER BY timestamp DESC LIMIT -1 OFFSET 3000",
                            (c_id,),
                        )
                        for del_row in to_delete:
                            self._cleanup_media_file(del_row[1], del_row[2])
                        if to_delete:
                            ids = [r[0] for r in to_delete]
                            if ids:
                                ph = ",".join("?" for _ in ids)
                                await self._db_run(f"DELETE FROM messages WHERE chat_id = ? AND msg_id IN ({ph})", [c_id] + ids)
            except Exception as e:
                logger.warning("Cleanup error: %s", e)
            await asyncio.sleep(3600)

    async def _backup_db_loop(self):
        await asyncio.sleep(180)
        while True:
            try:
                storage_id = self.config["storage_chat_id"]
                if not storage_id:
                    await asyncio.sleep(10800)
                    continue

                backup_path = f"/tmp/savedeleted_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
                async with self._db_lock:
                    await asyncio.to_thread(shutil.copy2, self.db_path, backup_path)

                with open(backup_path, "rb") as f:
                    file_hash = hashlib.md5(f.read()).hexdigest()[:8]

                final_name = f"/tmp/savedeleted_bkp_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file_hash}.db"
                os.rename(backup_path, final_name)

                await self._save_media_to_storage(final_name, storage_id, f"DB Backup {datetime.now().strftime('%Y-%m-%d %H:%M')} | md5:{file_hash}")
                os.remove(final_name)
            except Exception as e:
                logger.warning("DB backup failed: %s", e)
            await asyncio.sleep(10800)

    async def _resolve_entity(self, message: Message):
        args = utils.get_args_raw(message)
        if args:
            entity = None
            try:
                entity = await self._client.get_entity(args)
            except Exception:
                pass
            if not entity:
                try:
                    n = int(args)
                    if n > 0:
                        entity = await self._client.get_entity(-1000000000000 - n)
                    elif n < -1000000000000:
                        entity = await self._client.get_entity(int(self._bare_id(n)))
                except Exception:
                    pass
            if entity:
                eid = getattr(entity, 'id', 0)
                if eid < 0:
                    eid = int(self._bare_id(eid))
                return eid, getattr(entity, 'title', getattr(entity, 'first_name', str(args)))
            return None, None
        return utils.get_chat_id(message), "Current chat"

    @loader.watcher(no_commands=True)
    async def watcher(self, message):
        if not hasattr(message, "id"):
            return
        if not isinstance(message, (types.Message, types.MessageService)):
            return

        chat_id = utils.get_chat_id(message)
        sender_id = getattr(message, "sender_id", 0)

        if sender_id in (777000, 489000):
            return

        if chat_id in self.blacklist:
            return

        if self.config["use_whitelist"]:
            if chat_id not in self.whitelist:
                return
            if not await self._should_save(message, chat_id, sender_id, whitelisted=True):
                return
        else:
            if not await self._should_save(message, chat_id, sender_id):
                return

        if chat_id not in self.cached_chats:
            self._run_in_background(self._auto_cache_chat(chat_id))

        if getattr(message, "media", None) and getattr(message.media, "ttl_seconds", None) and self.config["save_onetime"]:
            self._run_in_background(self._process_onetime(message, chat_id))
            return

        self._run_in_background(self._process_and_save(message, chat_id))

    async def _process_onetime(self, message: Message, chat_id: int):
        try:
            sender_id = getattr(message, "sender_id", 0)
            if sender_id in (777000, 489000):
                return
            if not self.config["save_bots"] and await self._is_bot_sender(sender_id):
                return

            media_type = self._get_media_type(message)
            parsed_text = self._safe_parse_text(message)
            media_path = await self._save_media(message, media_type, chat_id)
            sender = await self._get_user_link(sender_id)

            label = self.strings("label_ot_" + media_type)
            header = self.strings("onetime").format(sender=sender, label_ot=label)

            blocks = [("", utils.escape_html(parsed_text))] if parsed_text else []
            parts = self._build_message(header, blocks)

            await self._send_to_bot(parts, media_path, media_type)
            self._cleanup_media_file(media_path, media_type)
            await self._save_to_db(message, chat_id, None, media_type, "", self._get_bot_file_id(message))
        except Exception as e:
            logger.warning("Onetime media error: %s", e)

    async def _process_and_save(self, message: Message, chat_id: int):
        fwd_info = await self._get_fwd_info(message)
        media_type = self._get_media_type(message)
        media_path = await self._save_media(message, media_type, chat_id)
        bot_file_id = self._get_bot_file_id(message)
        await self._save_to_db(message, chat_id, media_path, media_type, fwd_info, bot_file_id)

    async def on_deleted(self, event):
        if not self.config["save_deleted"] or not event.deleted_ids:
            return

        deleted_ids = event.deleted_ids
        chat_id = getattr(event, "chat_id", None)
        if chat_id is not None and chat_id < 0:
            chat_id = int(self._bare_id(chat_id))

        if not deleted_ids:
            return

        ph = ",".join("?" for _ in deleted_ids)
        query = f"SELECT msg_id, chat_id, sender_id, text, media_path, timestamp, media_type, fwd_info, bot_file_id FROM messages WHERE msg_id IN ({ph})"
        params = list(deleted_ids)
        if chat_id is not None:
            query += " AND chat_id = ?"
            params.append(chat_id)

        try:
            records = await self._db_exec(query, params)
        except Exception as e:
            logger.warning("on_deleted DB error: %s", e)
            return

        if not records:
            return

        if len(records) >= 30:
            filtered = []
            for row in records:
                c_id = row[1]
                if self.config["use_whitelist"] and c_id not in self.whitelist:
                    continue
                if c_id in self.blacklist:
                    continue
                s_id = row[2]
                if not self.config["save_bots"] and await self._is_bot_sender(s_id):
                    continue
                self._cleanup_media_file(row[4], row[6])
                filtered.append(row)
            records = filtered
            if not records:
                return

            ids = [r[0] for r in records]
            ph2 = ",".join("?" for _ in ids)
            await self._db_run(f"DELETE FROM messages WHERE msg_id IN ({ph2})", ids)

            report = self.strings("mass_del").format(count=len(records))
            for row in records:
                ts_str = datetime.fromtimestamp(row[5]).strftime('%H:%M:%S')
                short_text = (utils.escape_html(str(row[3]))[:50] + '...') if row[3] else self.strings("empty")
                report += f"\n[{ts_str}] <code>{row[2]}</code>: {utils.escape_html(short_text)}"

            await self._send_to_bot([report])
            return

        for row in records:
            msg_id, c_id, sender_id, text, media_path, ts, m_type, fwd_info, bot_file_id = row

            if self.config["use_whitelist"] and c_id not in self.whitelist:
                continue
            if c_id in self.blacklist:
                continue
            if not self.config["save_bots"] and await self._is_bot_sender(sender_id):
                continue

            sender = await self._get_user_link(sender_id)
            c_info, c_type = await self._get_chat_info_and_type(c_id)

            label = self.strings("label_" + m_type)

            if fwd_info:
                if c_type == "channel":
                    key = "del_fwd_channel"
                elif c_type == "group":
                    key = "del_fwd_group"
                else:
                    key = "del_fwd"
                header = self.strings(key).format(sender=sender, label=label, fwd=fwd_info, chat=c_info)
            else:
                if c_type == "channel":
                    key = "del_channel"
                elif c_type == "group":
                    key = "del_group"
                else:
                    key = "del_pm"
                header = self.strings(key).format(sender=sender, label=label, chat=c_info)

            blocks = [(self.strings("text"), utils.escape_html(text))] if text and text.strip() else []
            parts = self._build_message(header, blocks)

            await self._send_to_bot(parts, media_path, m_type, f"{c_id}_{msg_id}" if m_type == "sticker" else "")
            self._cleanup_media_file(media_path, m_type)

            await self._db_run("DELETE FROM messages WHERE msg_id = ? AND chat_id = ?", (msg_id, c_id))

    async def on_edited(self, event):
        if not self.config["save_edited"]:
            return

        message = getattr(event, "message", None)
        if not message:
            return

        msg_id = getattr(message, "id", 0)
        if not msg_id:
            return

        chat_id = utils.get_chat_id(message)
        if not chat_id:
            return

        try:
            rows = await self._db_exec(
                "SELECT sender_id, text, media_path, timestamp, media_type, media_id, fwd_info, bot_file_id FROM messages WHERE msg_id = ? AND chat_id = ?",
                (msg_id, chat_id),
            )
        except Exception as e:
            logger.warning("on_edited DB error: %s", e)
            return

        if not rows:
            return
        sender_id, old_text, old_media_path, ts, old_m_type, old_media_id, fwd_info, old_bot_file_id = rows[0]

        if sender_id in (777000, 489000):
            return
        if not self.config["save_bots"] and await self._is_bot_sender(sender_id):
            return

        if self.config["use_whitelist"] and chat_id not in self.whitelist:
            return
        if chat_id in self.blacklist:
            return

        new_text = self._safe_parse_text(message)
        new_m_type = self._get_media_type(message)
        new_media_id = self._get_media_id(message)
        new_bot_file_id = self._get_bot_file_id(message)

        sender = await self._get_user_link(sender_id)
        c_info, c_type = await self._get_chat_info_and_type(chat_id)

        text_changed = old_text != new_text
        media_added = old_m_type == "none" and new_m_type != "none"
        media_removed = old_m_type != "none" and new_m_type == "none"
        media_changed = old_m_type != "none" and new_m_type != "none" and bool(old_media_id) and bool(new_media_id) and old_media_id != new_media_id

        if not text_changed and not media_added and not media_removed and not media_changed:
            return

        actions = []
        if text_changed:
            mtype = old_m_type
            label = self.strings("label_" + mtype)
            label_dat = self.strings("label_dat_" + mtype)
            label_gen = self.strings("label_gen_" + mtype)
            linked_label = self._get_msg_link(chat_id, msg_id, label, c_type)
            linked_dat = self._get_msg_link(chat_id, msg_id, label_dat, c_type)
            linked_gen = self._get_msg_link(chat_id, msg_id, label_gen, c_type)

            if not (old_text or "").strip() and (new_text or "").strip():
                tpl = self.strings("act_added_text").get(mtype, self.strings("act_added_text")["none"])
                action = tpl.format(label=linked_label, label_dat=linked_dat, label_gen=linked_gen)
            elif (old_text or "").strip() and not (new_text or "").strip():
                tpl = self.strings("act_removed_text").get(mtype, self.strings("act_removed_text")["none"])
                action = tpl.format(label=linked_label, label_dat=linked_dat, label_gen=linked_gen)
            else:
                tpl = self.strings("act_edited_text").get(mtype, self.strings("act_edited_text")["none"])
                action = tpl.format(label=linked_label, label_dat=linked_dat, label_gen=linked_gen)
            actions.append(action)

        if media_added:
            mtype = new_m_type
            label = self.strings("label_" + mtype)
            linked_label = self._get_msg_link(chat_id, msg_id, label, c_type)
            tpl = self.strings("act_attached").get(mtype, self.strings("act_attached")["none"])
            actions.append(tpl.format(label=linked_label))
        if media_removed:
            mtype = old_m_type
            label = self.strings("label_" + mtype)
            linked_label = self._get_msg_link(chat_id, msg_id, label, c_type)
            tpl = self.strings("act_removed_media").get(mtype, self.strings("act_removed_media")["none"])
            actions.append(tpl.format(label=linked_label))
        if media_changed:
            mtype = new_m_type
            label = self.strings("label_" + mtype)
            linked_label = self._get_msg_link(chat_id, msg_id, label, c_type)
            tpl = self.strings("act_replaced").get(mtype, self.strings("act_replaced")["none"])
            actions.append(tpl.format(label=linked_label))

        actions_str = " & ".join(actions)
        if c_type == "channel":
            header = self.strings("ed_channel").format(chat=c_info, sender=sender, actions=actions_str)
        elif c_type == "group":
            header = self.strings("ed_group").format(sender=sender, actions=actions_str, chat=c_info)
        else:
            header = self.strings("edited").format(sender=sender, actions=actions_str)

        blocks = []
        if text_changed:
            if not (old_text or "").strip() and (new_text or "").strip():
                blocks.append((self.strings("text"), utils.escape_html(new_text)))
            elif (old_text or "").strip() and not (new_text or "").strip():
                blocks.append((self.strings("deleted_text"), utils.escape_html(old_text)))
            else:
                blocks.append((self.strings("old_text"), utils.escape_html(old_text)))
                blocks.append((self.strings("new_text"), utils.escape_html(new_text)))
                if self.config["show_diff"]:
                    blocks.append((self.strings("diff"), self._generate_diff(old_text, new_text)))

        new_media_path = old_media_path
        media_to_send = None
        media_type_to_send = "none"

        if media_added:
            new_media_path = await self._save_media(message, new_m_type, chat_id)
            media_to_send, media_type_to_send = new_media_path, new_m_type
        elif media_changed or media_removed:
            media_to_send, media_type_to_send = old_media_path, old_m_type
            if media_changed:
                new_media_path = await self._save_media(message, new_m_type, chat_id)
        elif text_changed and old_m_type != "none":
            media_to_send, media_type_to_send = old_media_path, old_m_type

        parts = self._build_message(header, blocks)
        await self._send_to_bot(parts, media_to_send, media_type_to_send, f"{chat_id}_{msg_id}" if media_type_to_send == "sticker" else "")

        if media_changed or media_removed:
            self._cleanup_media_file(old_media_path, old_m_type)

        try:
            await self._db_run(
                "UPDATE messages SET text = ?, media_path = ?, timestamp = ?, media_type = ?, media_id = ?, bot_file_id = ? WHERE msg_id = ? AND chat_id = ?",
                (new_text, new_media_path, datetime.now().timestamp(), new_m_type, new_media_id, new_bot_file_id, msg_id, chat_id),
            )
        except Exception as e:
            logger.warning("on_edited DB update error: %s", e)

    @loader.command(
        ru_doc="Бекап локальных медиа в storage-чат (по одному, с таймаутом)",
        en_doc="Backup all local media files to storage chat (one by one with timeout)",
    )
    async def sdbackupcmd(self, message: Message):
        """Migrate local media files to storage chat"""
        storage_id = self.config["storage_chat_id"]
        if not storage_id:
            await utils.answer(message, self.strings("backup_no_storage"))
            return

        records = await self._db_exec(
            "SELECT msg_id, chat_id, media_path, media_type FROM messages WHERE media_path IS NOT NULL AND media_path != '' AND media_path NOT LIKE 'tg://%' AND media_type NOT IN ('contact','geo','service')",
        )

        if not records:
            await utils.answer(message, self.strings("backup_nothing"))
            return

        total = len(records)
        status_msg = await utils.answer(message, self.strings("backup_start").format(done=0, total=total))

        done = 0
        for msg_id, chat_id, media_path, media_type in records:
            if not media_path or not os.path.exists(str(media_path)):
                done += 1
                continue

            try:
                new_path = await self._save_media_to_storage(media_path, storage_id, f"sdbackup:{msg_id}:{chat_id}:{media_type}")
                if new_path != media_path and new_path.startswith("tg://"):
                    await self._db_run(
                        "UPDATE messages SET media_path = ? WHERE msg_id = ? AND chat_id = ?",
                        (new_path, msg_id, chat_id),
                    )
                self._cleanup_media_file(media_path, media_type)
                done += 1
            except Exception as e:
                logger.warning("Backup failed for %s: %s", media_path, e)

            if done % 5 == 0:
                try:
                    await status_msg.edit(self.strings("backup_start").format(done=done, total=total))
                except Exception:
                    pass

            await asyncio.sleep(1.5)

        try:
            await status_msg.edit(self.strings("backup_done").format(done=done, total=total))
        except Exception:
            await utils.answer(message, self.strings("backup_done").format(done=done, total=total))

    @loader.command(
        ru_doc="<@юзернейм/ID/ссылка> - Добавить чат в Белый список",
        en_doc="<@username/ID/link> - Add chat/user to Whitelist",
    )
    async def sdwlcmd(self, message: Message):
        chat_id, title = await self._resolve_entity(message)
        if not chat_id:
            return await utils.answer(message, self.strings("not_found"))
        if chat_id in self.whitelist:
            self.whitelist.remove(chat_id)
            await utils.answer(message, self.strings("rm_wl").format(chat=title))
        else:
            self.whitelist.append(chat_id)
            await utils.answer(message, self.strings("added_wl").format(chat=title))

    @loader.command(
        ru_doc="<@юзернейм/ID/ссылка> - Добавить чат в Черный список",
        en_doc="<@username/ID/link> - Add chat/user to Blacklist",
    )
    async def sdblcmd(self, message: Message):
        chat_id, title = await self._resolve_entity(message)
        if not chat_id:
            return await utils.answer(message, self.strings("not_found"))
        if chat_id in self.blacklist:
            self.blacklist.remove(chat_id)
            await utils.answer(message, self.strings("rm_bl").format(chat=title))
        else:
            self.blacklist.append(chat_id)
            await utils.answer(message, self.strings("added_bl").format(chat=title))

    @loader.command(
        ru_doc="[chat_id] - Очистить БД или историю конкретного чата",
        en_doc="[chat_id] - Clear DB or specific chat history",
    )
    async def sdclearcmd(self, message: Message):
        args = utils.get_args_raw(message)
        if args:
            chat_id, title = await self._resolve_entity(message)
            if not chat_id:
                return await utils.answer(message, self.strings("not_found"))
            rows = await self._db_exec("SELECT media_path, media_type FROM messages WHERE chat_id = ?", (chat_id,))
            for row in rows:
                self._cleanup_media_file(row[0], row[1])
            await self._db_run("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
            return await utils.answer(message, self.strings("chat_cleared").format(chat=title))

        await self._db_run("DELETE FROM messages")
        await self._db_run("DELETE FROM auto_cached")
        self.cached_chats.clear()
        if os.path.exists(self.media_dir):
            for file in os.listdir(self.media_dir):
                try:
                    os.remove(os.path.join(self.media_dir, file))
                except Exception:
                    pass
        await utils.answer(message, self.strings("db_cleared"))

    @loader.command(
        ru_doc="Показать статистику модуля",
        en_doc="Show module statistics",
    )
    async def sdstatscmd(self, message: Message):
        try:
            rows = await self._db_exec("SELECT COUNT(*) FROM messages")
            count = rows[0][0] if rows else 0
            rows = await self._db_exec("SELECT COUNT(DISTINCT chat_id) FROM messages")
            chats = rows[0][0] if rows else 0

            db_size = await asyncio.to_thread(os.path.getsize, self.db_path) / (1024 * 1024) if os.path.exists(self.db_path) else 0
            media_size = 0
            if os.path.exists(self.media_dir):

                def _calc_media_size():
                    return sum(
                        os.path.getsize(os.path.join(self.media_dir, f))
                        for f in os.listdir(self.media_dir)
                        if os.path.isfile(os.path.join(self.media_dir, f))
                    ) / (1024 * 1024)

                media_size = await asyncio.to_thread(_calc_media_size)

            storage_id = self.config["storage_chat_id"]
            storage_info = f"\n<b>StorageChat:</b> <code>{storage_id}</code>" if storage_id else "\n<b>Storage:</b> <code>local disk</code>"

            text = (
                f"<b>SaveDeleted Stats:</b>\n\n"
                f"<b>Messages:</b> <code>{count}</code>\n"
                f"<b>Chats:</b> <code>{chats}</code>\n"
                f"<b>DB Size:</b> <code>{db_size:.2f} MB</code>\n"
                f"<b>Media Cache:</b> <code>{media_size:.2f} MB</code>\n"
                f"{storage_info}\n"
                f"<b>Whitelist:</b> <code>{len(self.whitelist)}</code>\n"
                f"<b>Blacklist:</b> <code>{len(self.blacklist)}</code>\n"
            )
            await utils.answer(message, text)
        except Exception as e:
            await utils.answer(message, f"<b>Error:</b> <code>{e}</code>")
