"""
    SaveDeleted - Сохранение удаленных сообщений
    
    Автоматически кэширует историю чатов и отправляет уведомления
    о редактированиях, удалениях и одноразовых медиа.
"""

__version__ = (1, 2, 0)

# meta developer: @Mr4epTuk
# meta name: SaveDeleted
# scope: hikka_only
# requires: aiosqlite

import os
import re
import json
import asyncio
import difflib
import logging
import aiosqlite
import contextvars
from datetime import datetime
from telethon import events
from telethon.tl.types import Message, MessageEmpty
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
        "db_cleared": "<b><tg-emoji emoji-id='5879937509579820068'>🗑</tg-emoji> Database successfully cleared!</b>",
        "chat_cleared": "<b><tg-emoji emoji-id='5879937509579820068'>🗑</tg-emoji> History for {chat} cleared!</b>",
        "not_found": "<b><tg-emoji emoji-id='5870931487146119264'>❗️</tg-emoji> Chat or user not found.</b>",
        
        "mass_del": "<b><tg-emoji emoji-id='5879937509579820068'>🗑</tg-emoji> Mass Deletion ({count} messages)</b>",
        "deleted": "<b><tg-emoji emoji-id='5879937509579820068'>🗑</tg-emoji> Deleted {label}</b>",
        "edited": "<b>{sender} {actions}.</b>",
        "onetime": "<tg-emoji emoji-id='5256054975389247793'>🔥</tg-emoji> User sent one-time {label}!",
        "from": "From: {sender}",
        "fwd_from": "Forwarded from: {sender}",
        "chat": "Chat: {chat}",
        
        "old_text": "Old text:",
        "new_text": "New text:",
        "deleted_text": "Deleted text:",
        "diff": "Changes:",
        "text": "Text:",
        "caption": "Caption:",
        
        "act_added_text": {
            "photo": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> added text to photo", 
            "video": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> added text to video", 
            "voice": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> added text to voice message", 
            "round": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> added text to video message", 
            "audio": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> added text to audio", 
            "document": "<tg-emoji emoji-id='5839380580080293813'>✏️</tg-emoji> added text to file", 
            "contact": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> added text to contact", 
            "geo": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> added text to location", 
            "service": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> added text to system message", 
            "none": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> added text"
        },
        "act_removed_text": {
            "photo": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> removed text from photo", 
            "video": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> removed text from video", 
            "voice": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> removed text from voice message", 
            "round": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> removed text from video message", 
            "audio": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> removed text from audio", 
            "document": "<tg-emoji emoji-id='5839380580080293813'>✏️</tg-emoji> removed text from file", 
            "contact": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> removed text from contact", 
            "geo": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> removed text from location", 
            "service": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> removed text from system message", 
            "none": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> removed text"
        },
        "act_edited_text": {
            "photo": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> edited photo caption", 
            "video": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> edited video caption", 
            "voice": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> edited voice message caption", 
            "round": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> edited video message caption", 
            "audio": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> edited audio caption", 
            "document": "<tg-emoji emoji-id='5839380580080293813'>✏️</tg-emoji> edited file caption", 
            "contact": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> edited contact caption", 
            "geo": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> edited location caption", 
            "service": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> edited system message caption", 
            "none": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> edited message"
        },
        
        "act_attached": {
            "photo": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> attached photo",
            "video": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> attached video",
            "voice": "<tg-emoji emoji-id='5870831513192369918'>🎤</tg-emoji> attached voice message",
            "round": "<tg-emoji emoji-id='5870565671896617027'>📷</tg-emoji> attached video message",
            "audio": "<tg-emoji emoji-id='5870794890006237381'>🎶</tg-emoji> attached audio",
            "document": "<tg-emoji emoji-id='5877495434124988415'>📎</tg-emoji> attached file",
            "sticker": "<tg-emoji emoji-id='5870600285038055496'>📑</tg-emoji> attached sticker",
            "contact": "<tg-emoji emoji-id='5870994129244131212'>👤</tg-emoji> attached contact",
            "geo": "<tg-emoji emoji-id='5870718761710915573'>📍</tg-emoji> attached location",
            "none": "<tg-emoji emoji-id='5877495434124988415'>📎</tg-emoji> attached media"
        },
        "act_removed_media": {
            "photo": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> removed photo",
            "video": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> removed video",
            "voice": "<tg-emoji emoji-id='5870831513192369918'>🎤</tg-emoji> removed voice message",
            "round": "<tg-emoji emoji-id='5870565671896617027'>📷</tg-emoji> removed video message",
            "audio": "<tg-emoji emoji-id='5870794890006237381'>🎶</tg-emoji> removed audio",
            "document": "<tg-emoji emoji-id='5877495434124988415'>📎</tg-emoji> removed file",
            "sticker": "<tg-emoji emoji-id='5870600285038055496'>📑</tg-emoji> removed sticker",
            "contact": "<tg-emoji emoji-id='5870994129244131212'>👤</tg-emoji> removed contact",
            "geo": "<tg-emoji emoji-id='5870718761710915573'>📍</tg-emoji> removed location",
            "none": "<tg-emoji emoji-id='5877495434124988415'>📎</tg-emoji> removed media"
        },
        "act_replaced": {
            "photo": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> replaced photo",
            "video": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> replaced video",
            "voice": "<tg-emoji emoji-id='5870831513192369918'>🎤</tg-emoji> replaced voice message",
            "round": "<tg-emoji emoji-id='5870565671896617027'>📷</tg-emoji> replaced video message",
            "audio": "<tg-emoji emoji-id='5870794890006237381'>🎶</tg-emoji> replaced audio",
            "document": "<tg-emoji emoji-id='5877495434124988415'>📎</tg-emoji> replaced file",
            "sticker": "<tg-emoji emoji-id='5870600285038055496'>📑</tg-emoji> replaced sticker",
            "contact": "<tg-emoji emoji-id='5870994129244131212'>👤</tg-emoji> replaced contact",
            "geo": "<tg-emoji emoji-id='5870718761710915573'>📍</tg-emoji> replaced location",
            "none": "<tg-emoji emoji-id='5877495434124988415'>📎</tg-emoji> replaced media"
        },
        
        "m_photo": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> Photo",
        "m_video": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> Video",
        "m_voice": "<tg-emoji emoji-id='5870831513192369918'>🎤</tg-emoji> Voice message",
        "m_round": "<tg-emoji emoji-id='5870565671896617027'>📷</tg-emoji> Video message",
        "m_audio": "<tg-emoji emoji-id='5870794890006237381'>🎶</tg-emoji> Audio",
        "m_document": "<tg-emoji emoji-id='5873153278023307367'>📄</tg-emoji> File",
        "m_sticker": "<tg-emoji emoji-id='5870600285038055496'>📑</tg-emoji> Sticker",
        "m_contact": "<tg-emoji emoji-id='5870994129244131212'>👤</tg-emoji> Contact",
        "m_geo": "<tg-emoji emoji-id='5870718761710915573'>📍</tg-emoji> Location",
        "m_none": "<tg-emoji emoji-id='5872886929921413168'>💬</tg-emoji> Message",
        "m_service": "<tg-emoji emoji-id='5870982283724328568'>⚙</tg-emoji> System message",
    }

    strings_ru = {
        "added_wl": "<b><tg-emoji emoji-id='5870633910337015697'>✅</tg-emoji> {chat} добавлен в Белый список.</b>",
        "rm_wl": "<b><tg-emoji emoji-id='5870931487146119264'>❗️</tg-emoji> {chat} удален из Белого списка.</b>",
        "added_bl": "<b><tg-emoji emoji-id='5870633910337015697'>✅</tg-emoji> {chat} добавлен в Черный список.</b>",
        "rm_bl": "<b><tg-emoji emoji-id='5870931487146119264'>❗️</tg-emoji> {chat} удален из Черного списка.</b>",
        "db_cleared": "<b><tg-emoji emoji-id='5879937509579820068'>🗑</tg-emoji> База данных полностью очищена!</b>",
        "chat_cleared": "<b><tg-emoji emoji-id='5879937509579820068'>🗑</tg-emoji> История чата {chat} удалена из БД!</b>",
        "not_found": "<b><tg-emoji emoji-id='5870931487146119264'>❗️</tg-emoji> Чат или пользователь не найден.</b>",
        
        "mass_del": "<b><tg-emoji emoji-id='5879937509579820068'>🗑</tg-emoji> Массовое удаление ({count} сообщений)</b>",
        "deleted": "<b><tg-emoji emoji-id='5879937509579820068'>🗑</tg-emoji> Удалено: {label}</b>",
        "edited": "<b>{sender} {actions}.</b>",
        "onetime": "<tg-emoji emoji-id='5256054975389247793'>🔥</tg-emoji> Пользователь отправил одноразовое {label}!",
        "from": "От: {sender}",
        "fwd_from": "Переслано от: {sender}",
        "chat": "Чат: {chat}",
        
        "old_text": "Старый текст:",
        "new_text": "Новый текст:",
        "deleted_text": "Удаленный текст:",
        "diff": "Изменилось:",
        "text": "Текст:",
        "caption": "Подпись:",
        
        "act_added_text": {
            "photo": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> добавил текст к фото", 
            "video": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> добавил текст к видео", 
            "voice": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> добавил текст к голосовому", 
            "round": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> добавил текст к кружку", 
            "audio": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> добавил текст к аудио", 
            "document": "<tg-emoji emoji-id='5839380580080293813'>✏️</tg-emoji> добавил текст к файлу", 
            "contact": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> добавил текст к контакту", 
            "geo": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> добавил текст к локации", 
            "service": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> добавил текст к системному сообщению", 
            "none": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> добавил текст"
        },
        "act_removed_text": {
            "photo": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> удалил текст из фото", 
            "video": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> удалил текст из видео", 
            "voice": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> удалил текст из голосовому", 
            "round": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> удалил текст из кружка", 
            "audio": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> удалил текст из аудио", 
            "document": "<tg-emoji emoji-id='5839380580080293813'>✏️</tg-emoji> удалил текст из файла", 
            "contact": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> удалил текст из контакта", 
            "geo": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> удалил текст из локации", 
            "service": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> удалил текст из системного сообщения", 
            "none": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> удалил текст"
        },
        "act_edited_text": {
            "photo": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> изменил текст фото", 
            "video": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> изменил текст видео", 
            "voice": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> изменил текст голосового", 
            "round": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> изменил текст кружка", 
            "audio": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> изменил текст аудио", 
            "document": "<tg-emoji emoji-id='5839380580080293813'>✏️</tg-emoji> изменил подпись к файлу", 
            "contact": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> изменил текст контакта", 
            "geo": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> изменил текст локации", 
            "service": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> изменил текст системного сообщения", 
            "none": "<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> изменил сообщение"
        },
        
        "act_attached": {
            "photo": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> прикрепил фото",
            "video": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> прикрепил видео",
            "voice": "<tg-emoji emoji-id='5870831513192369918'>🎤</tg-emoji> прикрепил голосовое",
            "round": "<tg-emoji emoji-id='5870565671896617027'>📷</tg-emoji> прикрепил кружок",
            "audio": "<tg-emoji emoji-id='5870794890006237381'>🎶</tg-emoji> прикрепил аудио",
            "document": "<tg-emoji emoji-id='5877495434124988415'>📎</tg-emoji> прикрепил файл",
            "sticker": "<tg-emoji emoji-id='5870600285038055496'>📑</tg-emoji> прикрепил стикер",
            "contact": "<tg-emoji emoji-id='5870994129244131212'>👤</tg-emoji> прикрепил контакт",
            "geo": "<tg-emoji emoji-id='5870718761710915573'>📍</tg-emoji> прикрепил локацию",
            "none": "<tg-emoji emoji-id='5877495434124988415'>📎</tg-emoji> прикрепил медиа"
        },
        "act_removed_media": {
            "photo": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> удалил фото",
            "video": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> удалил видео",
            "voice": "<tg-emoji emoji-id='5870831513192369918'>🎤</tg-emoji> удалил голосовое",
            "round": "<tg-emoji emoji-id='5870565671896617027'>📷</tg-emoji> удалил кружок",
            "audio": "<tg-emoji emoji-id='5870794890006237381'>🎶</tg-emoji> удалил аудио",
            "document": "<tg-emoji emoji-id='5877495434124988415'>📎</tg-emoji> удалил файл",
            "sticker": "<tg-emoji emoji-id='5870600285038055496'>📑</tg-emoji> удалил стикер",
            "contact": "<tg-emoji emoji-id='5870994129244131212'>👤</tg-emoji> удалил контакт",
            "geo": "<tg-emoji emoji-id='5870718761710915573'>📍</tg-emoji> удалил локацию",
            "none": "<tg-emoji emoji-id='5877495434124988415'>📎</tg-emoji> удалил медиа"
        },
        "act_replaced": {
            "photo": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> перезалил фото",
            "video": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> перезалил видео",
            "voice": "<tg-emoji emoji-id='5870831513192369918'>🎤</tg-emoji> перезалил голосовое",
            "round": "<tg-emoji emoji-id='5870565671896617027'>📷</tg-emoji> перезалил кружок",
            "audio": "<tg-emoji emoji-id='5870794890006237381'>🎶</tg-emoji> перезалил аудио",
            "document": "<tg-emoji emoji-id='5877495434124988415'>📎</tg-emoji> перезалил файл",
            "sticker": "<tg-emoji emoji-id='5870600285038055496'>📑</tg-emoji> перезалил стикер",
            "contact": "<tg-emoji emoji-id='5870994129244131212'>👤</tg-emoji> перезалил контакт",
            "geo": "<tg-emoji emoji-id='5870718761710915573'>📍</tg-emoji> перезалил локацию",
            "none": "<tg-emoji emoji-id='5877495434124988415'>📎</tg-emoji> перезалил медиа"
        },
        
        "m_photo": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> Фото",
        "m_video": "<tg-emoji emoji-id='5870782662234346251'>🖼</tg-emoji> Видео",
        "m_voice": "<tg-emoji emoji-id='5870831513192369918'>🎤</tg-emoji> Голосовое",
        "m_round": "<tg-emoji emoji-id='5870565671896617027'>📷</tg-emoji> Кружок",
        "m_audio": "<tg-emoji emoji-id='5870794890006237381'>🎶</tg-emoji> Аудио",
        "m_document": "<tg-emoji emoji-id='5873153278023307367'>📄</tg-emoji> Файл",
        "m_sticker": "<tg-emoji emoji-id='5870600285038055496'>📑</tg-emoji> Стикер",
        "m_contact": "<tg-emoji emoji-id='5870994129244131212'>👤</tg-emoji> Контакт",
        "m_geo": "<tg-emoji emoji-id='5870718761710915573'>📍</tg-emoji> Локация",
        "m_none": "<tg-emoji emoji-id='5872886929921413168'>💬</tg-emoji> Сообщение",
        "m_service": "<tg-emoji emoji-id='5870982283724328568'>⚙</tg-emoji> Системное сообщение",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("custom_bot_token", "", "Токен кастомного бота (пусто = дефолт) / Custom bot token", validator=loader.validators.Hidden()),
            loader.ConfigValue("show_diff", True, "Показывать блок 'Изменилось' (зачеркивания) / Show 'Changes' block", validator=loader.validators.Boolean()),
            loader.ConfigValue("show_msg_link", True, "Показывать ссылку на измененное/удаленное сообщение / Show link to message", validator=loader.validators.Boolean()),
            loader.ConfigValue("save_pm", True, "Логировать ЛС / Log PMs", validator=loader.validators.Boolean()),
            loader.ConfigValue("save_groups", False, "Логировать группы / Log Groups", validator=loader.validators.Boolean()),
            loader.ConfigValue("save_channels", False, "Логировать каналы / Log Channels", validator=loader.validators.Boolean()),
            loader.ConfigValue("save_deleted", True, "Уведомлять об удалении / Notify deletions", validator=loader.validators.Boolean()),
            loader.ConfigValue("save_edited", True, "Уведомлять об изменении / Notify edits", validator=loader.validators.Boolean()),
            loader.ConfigValue("save_onetime", True, "Перехватывать одноразовые / Catch one-time media", validator=loader.validators.Boolean()),
            loader.ConfigValue("use_whitelist", False, "Использовать Белый список / Use Whitelist", validator=loader.validators.Boolean()),
            loader.ConfigValue("keep_full_history", False, "Безлимитная история и кэш старых медиа / Unlimited history & past media cache", validator=loader.validators.Boolean()),
            loader.ConfigValue("auto_cleanup_days", 0, "Удалять записи старше X дней (0 - выкл) / Delete records older than X days", validator=loader.validators.Integer(minimum=0)),
        )
        self.db_path = "savedeleted_cache.db"
        self.media_dir = "savedeleted_media"
        self._bg_tasks = set()
        self.cached_chats = set()

    async def client_ready(self, client, db):
        self._client = client
        self.db = db
        self._tg_id = (await client.get_me()).id

        if not os.path.exists(self.media_dir):
            os.makedirs(self.media_dir)

        async with aiosqlite.connect(self.db_path) as db_conn:
            await db_conn.execute('''
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
            await db_conn.execute('CREATE TABLE IF NOT EXISTS auto_cached (chat_id INTEGER PRIMARY KEY)')
            await db_conn.commit()

            cursor = await db_conn.execute("PRAGMA table_info(messages)")
            cols = [row[1] for row in await cursor.fetchall()]
            if "media_type" not in cols:
                await db_conn.execute("ALTER TABLE messages ADD COLUMN media_type TEXT DEFAULT 'none'")
            if "media_id" not in cols:
                await db_conn.execute("ALTER TABLE messages ADD COLUMN media_id TEXT DEFAULT ''")
            if "fwd_info" not in cols:
                await db_conn.execute("ALTER TABLE messages ADD COLUMN fwd_info TEXT DEFAULT ''")
            await db_conn.commit()

            c_cursor = await db_conn.execute("SELECT chat_id FROM auto_cached")
            for row in await c_cursor.fetchall():
                self.cached_chats.add(row[0])

        self.whitelist = self.pointer("sd_whitelist",[])
        self.blacklist = self.pointer("sd_blacklist",[])

        self._run_in_background(self._cleanup_loop())
        self._client.add_event_handler(self.on_deleted, events.MessageDeleted())
        self._client.add_event_handler(self.on_edited, events.MessageEdited())

    async def on_unload(self):
        for task in self._bg_tasks:
            task.cancel()
        self._client.remove_event_handler(self.on_deleted)
        self._client.remove_event_handler(self.on_edited)

    def _run_in_background(self, coro):
        ctx = contextvars.copy_context()
        task = ctx.run(asyncio.create_task, coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    def _bare_id(self, cid: int) -> str:
        s = str(abs(cid))
        if s.startswith("100") and len(s) > 10:
            return s[3:]
        return s

    def _is_bot_loop(self, chat_id, sender_id):
        bot_id = self.inline.bot.id if getattr(self.inline, "bot", None) else 0
        return sender_id in[self._tg_id, bot_id]

    def _serialize_contact(self, contact) -> str:
        if not contact: return ""
        return json.dumps({
            "p": getattr(contact, "phone_number", "") or "",
            "f": getattr(contact, "first_name", "") or "",
            "l": getattr(contact, "last_name", "") or "",
            "v": getattr(contact, "vcard", "") or "",
            "u": getattr(contact, "user_id", 0) or 0
        }, ensure_ascii=False)

    def _deserialize_contact(self, data: str) -> dict:
        if not data: return {"p": "", "f": "", "l": "", "v": "", "u": 0}
        if str(data).startswith("{"):
            try: return json.loads(str(data))
            except: pass
        parts = str(data).split("|")
        return {"p": parts[0] if len(parts) > 0 else "", "f": parts[1] if len(parts) > 1 else "", "l": parts[2] if len(parts) > 2 else "", "v": "", "u": 0}

    def _clean_name(self, text: str) -> str:
        if not text: return ""
        return re.sub(r'[\u200B-\u200F\u202A-\u202E\u2066-\u2069\uFEFF]', '', str(text)).strip()

    async def _get_fwd_info(self, message: Message) -> str:
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

        except Exception as e:
            logger.debug(f"Error getting fwd info: {e}")
        return "Unknown"

    async def _auto_cache_chat(self, chat_id: int):
        if chat_id in self.cached_chats: return
        self.cached_chats.add(chat_id)
        try:
            async with aiosqlite.connect(self.db_path) as db_conn:
                await db_conn.execute("INSERT OR IGNORE INTO auto_cached (chat_id) VALUES (?)", (chat_id,))
                await db_conn.commit()

            limit = None if self.config["keep_full_history"] else 3000
            batch =[]
            async for msg in self._client.iter_messages(chat_id, limit=limit):
                if getattr(msg, "out", False) or not hasattr(msg, "id"): continue
                m_type = self._get_media_type(msg)
                m_id = self._get_media_id(msg)
                text = self._safe_parse_text(msg)
                fwd_info = await self._get_fwd_info(msg)
                
                media_data = None
                if m_type == "contact" and getattr(msg, "contact", None):
                    media_data = self._serialize_contact(msg.contact)
                elif m_type == "geo" and msg.geo:
                    media_data = f"{msg.geo.lat}|{msg.geo.long}"
                elif getattr(msg, "media", None) and self.config["keep_full_history"]:
                    try: media_data = await msg.download_media(self.media_dir + "/")
                    except: pass

                batch.append((msg.id, chat_id, getattr(msg, "sender_id", 0), text, media_data, datetime.now().timestamp(), m_type, m_id, fwd_info))
                if len(batch) >= 500:
                    async with aiosqlite.connect(self.db_path) as db:
                        await db.executemany("INSERT OR IGNORE INTO messages (msg_id, chat_id, sender_id, text, media_path, timestamp, media_type, media_id, fwd_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", batch)
                        await db.commit()
                    batch.clear()

            if batch:
                async with aiosqlite.connect(self.db_path) as db:
                    await db.executemany("INSERT OR IGNORE INTO messages (msg_id, chat_id, sender_id, text, media_path, timestamp, media_type, media_id, fwd_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", batch)
                    await db.commit()
        except: pass

    async def _cleanup_loop(self):
        while True:
            try:
                days = self.config["auto_cleanup_days"]
                if days > 0:
                    cutoff = datetime.now().timestamp() - (days * 86400)
                    async with aiosqlite.connect(self.db_path) as db_conn:
                        cursor = await db_conn.execute("SELECT media_path, media_type FROM messages WHERE timestamp < ?", (cutoff,))
                        for row in await cursor.fetchall():
                            if row[0] and row[1] not in["geo", "contact", "service"] and os.path.exists(row[0]):
                                os.remove(row[0])
                        await db_conn.execute("DELETE FROM messages WHERE timestamp < ?", (cutoff,))
                        await db_conn.commit()

                if not self.config["keep_full_history"]:
                    async with aiosqlite.connect(self.db_path) as db_conn:
                        cursor = await db_conn.execute("SELECT chat_id FROM messages GROUP BY chat_id HAVING COUNT(msg_id) > 3000")
                        for row in await cursor.fetchall():
                            c_id = row[0]
                            cur2 = await db_conn.execute("SELECT msg_id, media_path, media_type FROM messages WHERE chat_id = ? ORDER BY timestamp DESC LIMIT -1 OFFSET 3000", (c_id,))
                            to_delete = await cur2.fetchall()
                            for del_row in to_delete:
                                if del_row[1] and del_row[2] not in["geo", "contact", "service"] and os.path.exists(del_row[1]):
                                    os.remove(del_row[1])
                            if to_delete:
                                ids = [r[0] for r in to_delete]
                                ph = ",".join("?" for _ in ids)
                                await db_conn.execute(f"DELETE FROM messages WHERE chat_id = ? AND msg_id IN ({ph})",[c_id] + ids)
                                await db_conn.commit()
            except: pass
            await asyncio.sleep(3600)

    def _get_media_type(self, message: Message) -> str:
        if getattr(message, "contact", None): return "contact"
        if getattr(message, "geo", None): return "geo"
        if getattr(message, "action", None): return "service"
        if getattr(message, "sticker", None): return "sticker"
        if getattr(message, "photo", None): return "photo"
        if getattr(message, "voice", None): return "voice"
        if getattr(message, "video_note", None): return "round"
        if getattr(message, "video", None): return "video"
        if getattr(message, "audio", None): return "audio"
        if getattr(message, "document", None): return "document"
        return "none"

    def _get_media_id(self, message: Message) -> str:
        if not getattr(message, "media", None): return ""
        try:
            if hasattr(message.media, 'photo') and message.media.photo: return str(message.media.photo.id)
            if hasattr(message.media, 'document') and message.media.document: return str(message.media.document.id)
        except: pass
        return ""

    def _safe_parse_text(self, message: Message) -> str:
        text = ""
        try:
            text = getattr(message, "message", "") or ""
            if getattr(message, "action", None):
                act = message.action
                if isinstance(act, types.MessageActionPhoneCall):
                    res = "❌ Пропущенный звонок / Missed Call" if getattr(act.video, "video", False) else "📞 Звонок / Call"
                    text += f"[{res} ({act.duration} sec)]"
                elif isinstance(act, types.MessageActionPinMessage):
                    text += "[📌 Сообщение закреплено / Message Pinned]"
                elif isinstance(act, types.MessageActionChatAddUser):
                    text += "[👤 Пользователь добавлен / User Added]"
                elif isinstance(act, types.MessageActionChatDeleteUser):
                    text += "[👤 Пользователь удален/вышел / User Left]"
                elif isinstance(act, types.MessageActionChatJoinedByLink):
                    text += "[🔗 Пользователь зашел по ссылке / User Joined via Link]"
                else:
                    text += "[⚙️ Системное событие / System Event]"
                    
            if getattr(message, "poll", None):
                text += f"\n[📊 Опрос / Poll: {utils.escape_html(message.poll.poll.question)}]"
        except: pass
        return text.strip()

    def _build_message(self, header: str, text_blocks: list) -> list:
        parts =[]
        curr = header + "\n"
        for label, text in text_blocks:
            if not text: continue
            chunks = [text[i:i+3000] for i in range(0, len(text), 3000)]
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
        old_tokens =[t for t in re.split(r'(\s+)', str(old_text)) if t]
        new_tokens =[t for t in re.split(r'(\s+)', str(new_text)) if t]
        diff = difflib.ndiff(old_tokens, new_tokens)
        result =[]
        for token in diff:
            code = token[:2]
            text = token[2:]
            if code == "- ": result.append(f"<s>{utils.escape_html(text)}</s>" if not text.isspace() else text)
            elif code == "+ ": result.append(f"<b>{utils.escape_html(text)}</b>" if not text.isspace() else text)
            elif code == "  ": result.append(utils.escape_html(text))
        return "".join(result)

    async def _get_chat_info(self, chat_id: int) -> str:
        try:
            chat = await self._client.get_entity(chat_id)
            if getattr(chat, "title", None):
                return f'<a href="tg://openmessage?chat_id={self._bare_id(chat_id)}">{utils.escape_html(self._clean_name(chat.title))}</a>'
        except: pass
        return "ЛС (PM)"

    async def _get_user_link(self, user_id: int) -> str:
        if not user_id: return "Unknown"
        try:
            user = await self._client.get_entity(user_id)
            name = utils.escape_html(self._clean_name(getattr(user, 'first_name', 'User') or 'User'))
            if getattr(user, 'username', None):
                return f'<a href="https://t.me/{user.username}">{name}</a>'
            return f'<a href="tg://openmessage?user_id={user_id}">{name}</a>'
        except:
            return f'<a href="tg://openmessage?user_id={user_id}">User {user_id}</a>'

    def _get_msg_link(self, chat_id: int, msg_id: int, text: str) -> str:
        """Делает текст ссылкой (если включено в конфиге), но оставляет <tg-emoji> снаружи <a> тегов"""
        if not self.config["show_msg_link"]:
            return text
            
        if chat_id > 0:
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

    async def _fallback_send(self, parts: list, media_path: str = None, media_type: str = "none"):
        if media_type == "sticker" and media_path and os.path.exists(str(media_path)):
            try:
                sent_sticker = await self._client.send_file("me", file=media_path)
                for text in parts:
                    fb_text = text.replace("<tg-emoji emoji-id=", "<emoji document_id=").replace("</tg-emoji>", "</emoji>")
                    await self._client.send_message("me", fb_text, parse_mode="html", link_preview=False, reply_to=sent_sticker.id)
                return
            except Exception as e:
                logger.error(f"Fallback sticker send failed: {e}")

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
                            except:
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
                logger.error(f"Fallback send failed: {e}")

    async def _send_to_bot(self, parts: list, media_path: str = None, media_type: str = "none"):
        if not parts: return
        
        token = self.config["custom_bot_token"]
        if isinstance(token, str): token = token.strip()
        custom_bot = None
        
        if token:
            try:
                import aiogram
                custom_bot = aiogram.Bot(token=token)
            except: pass
                
        bot = custom_bot or getattr(self.inline, "bot", None)
        if not bot:
            return await self._fallback_send(parts, media_path, media_type)

        try:
            import aiogram
            is_aio_3 = aiogram.__version__.startswith("3")
            sent_msg = None
            
            first_part = parts[0]
            caption = first_part if len(first_part) <= 1024 and media_type not in["round", "contact", "geo", "service", "sticker"] else "<b>📝 Подробный отчет / Report ⬇️</b>"

            if media_type == "contact" and media_path:
                c = self._deserialize_contact(str(media_path))
                if c["p"] and c["f"]:
                    sent_msg = await bot.send_contact(self._tg_id, phone_number=c["p"], first_name=c["f"], last_name=c["l"] if c["l"] else None, vcard=c["v"] if c["v"] else None)
            elif media_type == "geo" and media_path:
                p = str(media_path).split("|")
                if len(p) >= 2:
                    sent_msg = await bot.send_location(self._tg_id, latitude=float(p[0]), longitude=float(p[1]))
            elif media_path and os.path.exists(media_path):
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
                        sent_msg = await bot.send_video(self._tg_id, video=media, caption=caption, parse_mode="HTML")
                    elif media_type == "round":
                        sent_msg = await bot.send_video_note(self._tg_id, video_note=media)
                    elif media_type == "voice":
                        sent_msg = await bot.send_voice(self._tg_id, voice=media, caption=caption, parse_mode="HTML")
                    elif media_type == "audio":
                        sent_msg = await bot.send_audio(self._tg_id, audio=media, caption=caption, parse_mode="HTML")
                    elif media_type == "sticker":
                        sent_msg = await bot.send_sticker(self._tg_id, sticker=media)
                    else:
                        sent_msg = await bot.send_document(self._tg_id, document=media, caption=caption, parse_mode="HTML")
                else:
                    await bot.send_message(self._tg_id, text=f"<i>⚠️ Медиа слишком большое (>50МБ).</i>", parse_mode="HTML")

            to_send = parts[1:] if sent_msg and caption == first_part and media_type not in["round", "contact", "geo", "service", "sticker"] else parts

            reply_id = getattr(sent_msg, "message_id", None)
            for part in to_send:
                kwargs = {"chat_id": self._tg_id, "text": part, "parse_mode": "HTML", "reply_to_message_id": reply_id}
                try:
                    from aiogram.types import LinkPreviewOptions
                    kwargs["link_preview_options"] = LinkPreviewOptions(is_disabled=True)
                except ImportError:
                    kwargs["disable_web_page_preview"] = True
                    
                sent_msg = await bot.send_message(**kwargs)
                reply_id = getattr(sent_msg, "message_id", None)

        except Exception as e:
            logger.error(f"Bot send failed ({e}), using fallback.")
            await self._fallback_send(parts, media_path if os.path.exists(str(media_path)) else None, media_type)
        finally:
            if custom_bot:
                try:
                    import aiogram
                    if aiogram.__version__.startswith("3"): await custom_bot.session.close()
                    else: await custom_bot.close()
                except: pass

    async def _resolve_entity(self, message: Message):
        args = utils.get_args_raw(message)
        if args:
            try:
                entity = await self._client.get_entity(args)
                return utils.get_peer_id(entity), getattr(entity, 'title', getattr(entity, 'first_name', str(args)))
            except: return None, None
        else:
            return utils.get_chat_id(message), "Current chat"

    @loader.watcher()
    async def watcher(self, message):
        if getattr(message, "out", False) or not hasattr(message, "id"): return
        if not isinstance(message, (types.Message, types.MessageService)): return
        
        chat_id = utils.get_chat_id(message)
        sender_id = getattr(message, "sender_id", 0)
        
        if self._is_bot_loop(chat_id, sender_id): return
        
        if getattr(message, "is_private", False) and not self.config["save_pm"]: return
        if getattr(message, "is_group", False) and not self.config["save_groups"]: return
        if getattr(message, "is_channel", False) and not self.config["save_channels"]: return
        if self.config["use_whitelist"] and chat_id not in self.whitelist: return
        if not self.config["use_whitelist"] and chat_id in self.blacklist: return

        if chat_id not in self.cached_chats:
            self._run_in_background(self._auto_cache_chat(chat_id))

        if getattr(message, "media", None) and getattr(message.media, "ttl_seconds", None) and self.config["save_onetime"]:
            self._run_in_background(self._process_onetime(message, chat_id))
            return

        self._run_in_background(self._process_and_save(message, chat_id))

    async def _process_onetime(self, message: Message, chat_id: int):
        try:
            media_type = self._get_media_type(message)
            parsed_text = self._safe_parse_text(message)
            fwd_info = await self._get_fwd_info(message)
            media_path = await message.download_media(self.media_dir + "/")
            sender = await self._get_user_link(message.sender_id)
            c_info = await self._get_chat_info(chat_id)
            
            label = self.strings("m_" + media_type)
            msg_link = f"<b>{self._get_msg_link(chat_id, message.id, self.strings('onetime').format(label=label.lower()))}</b>"
            
            header = f"{msg_link}\n{self.strings('from').format(sender=sender)}"
            if fwd_info: header += f"\n{self.strings('fwd_from').format(sender=fwd_info)}"
            if c_info not in["ЛС", "ЛС (PM)"]: header += f"\n{self.strings('chat').format(chat=c_info)}"
            
            blocks =[("📝", utils.escape_html(parsed_text))] if parsed_text else[]
            parts = self._build_message(header, blocks)
            
            await self._send_to_bot(parts, media_path, media_type)
            if media_path and os.path.exists(media_path): os.remove(media_path)
            
            await self._save_to_db(message, chat_id, None, media_type, fwd_info)
        except Exception as e:
            logger.error(f"Onetime media error: {e}")

    async def _process_and_save(self, message: Message, chat_id: int):
        media_path = None
        media_type = self._get_media_type(message)
        fwd_info = await self._get_fwd_info(message)

        if media_type == "contact" and getattr(message, "contact", None):
            media_path = self._serialize_contact(message.contact)
        elif media_type == "geo" and message.geo:
            media_path = f"{message.geo.lat}|{message.geo.long}"
        elif getattr(message, "media", None):
            try: media_path = await message.download_media(self.media_dir + "/")
            except: pass

        await self._save_to_db(message, chat_id, media_path, media_type, fwd_info)

    async def _save_to_db(self, message: Message, chat_id: int, media_path, media_type, fwd_info: str):
        parsed_text = self._safe_parse_text(message)
        media_id = self._get_media_id(message)
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "INSERT OR REPLACE INTO messages (msg_id, chat_id, sender_id, text, media_path, timestamp, media_type, media_id, fwd_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (message.id, chat_id, getattr(message, "sender_id", 0), parsed_text, media_path, datetime.now().timestamp(), media_type, media_id, fwd_info)
                )
                await db.commit()
        except Exception as e:
            logger.error(f"DB Save error: {e}")

    async def on_deleted(self, event: events.MessageDeleted):
        if not self.config["save_deleted"] or not event.deleted_ids: return
        deleted_ids = event.deleted_ids
        
        chat_id = getattr(event, "chat_id", None)
        
        async with aiosqlite.connect(self.db_path) as db:
            ph = ",".join("?" for _ in deleted_ids)
            query = f"SELECT msg_id, chat_id, sender_id, text, media_path, timestamp, media_type, fwd_info FROM messages WHERE msg_id IN ({ph})"
            params = list(deleted_ids)
            if chat_id:
                query += " AND chat_id = ?"
                params.append(chat_id)
                
            try:
                cursor = await db.execute(query, params)
                records = await cursor.fetchall()
            except: return

        if not records: return

        valid_records =[]
        for row in records:
            try:
                msg_check = await self._client.get_messages(row[1], ids=row[0])
                if msg_check is not None and not isinstance(msg_check, MessageEmpty):
                    continue
            except: pass
            valid_records.append(row)
            
        if not valid_records: return
        records = valid_records

        if len(records) >= 30:
            for row in records:
                if row[4] and row[6] not in["geo", "contact", "service"] and os.path.exists(row[4]): 
                    os.remove(row[4])
            
            ids = [r[0] for r in records]
            ph2 = ",".join("?" for _ in ids)
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(f"DELETE FROM messages WHERE msg_id IN ({ph2})", ids)
                await db.commit()
                
            report = self.strings("mass_del").format(count=len(records))
            for row in records:
                ts_str = datetime.fromtimestamp(row[5]).strftime('%H:%M:%S')
                short_text = (row[3][:50] + '...') if row[3] else self.strings("empty")
                report += f"\n[{ts_str}] <code>{row[2]}</code>: {utils.escape_html(short_text)}"
            
            await self._send_to_bot([report])
            return

        for row in records:
            msg_id, c_id, sender_id, text, media_path, ts, m_type, fwd_info = row
            if self._is_bot_loop(c_id, sender_id): continue

            sender = await self._get_user_link(sender_id)
            c_info = await self._get_chat_info(c_id)
            
            label = self.strings("m_" + m_type)
            header = f"{self.strings('deleted').format(label=label.lower())}\n{self.strings('from').format(sender=sender)}"
            if fwd_info: header += f"\n{self.strings('fwd_from').format(sender=fwd_info)}"
            if c_info not in["ЛС", "ЛС (PM)"]: header += f"\n{self.strings('chat').format(chat=c_info)}"
            
            blocks =[(self.strings("text"), utils.escape_html(text))] if text and text.strip() else[]
            parts = self._build_message(header, blocks)

            await self._send_to_bot(parts, media_path, m_type)

            if media_path and m_type not in ["geo", "contact", "service"] and os.path.exists(media_path): 
                os.remove(media_path)
            
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("DELETE FROM messages WHERE msg_id = ? AND chat_id = ?", (msg_id, c_id))
                await db.commit()

    async def on_edited(self, event: events.MessageEdited):
        if not self.config["save_edited"]: return
        
        msg_id = getattr(event, 'id', getattr(event, 'message_id', None))
        chat_id = getattr(event, 'chat_id', None)
        if not msg_id or not chat_id: return

        async with aiosqlite.connect(self.db_path) as db:
            try:
                cursor = await db.execute("SELECT sender_id, text, media_path, timestamp, media_type, media_id, fwd_info FROM messages WHERE msg_id = ? AND chat_id = ?", (msg_id, chat_id))
                row = await cursor.fetchone()
            except: return

        if not row: return
        sender_id, old_text, old_media_path, ts, old_m_type, old_media_id, fwd_info = row

        if self._is_bot_loop(chat_id, sender_id): return

        message = event.message if hasattr(event, "message") else None
        if not message: return

        new_text = self._safe_parse_text(message)
        new_m_type = self._get_media_type(message)
        new_media_id = self._get_media_id(message)
        
        sender = await self._get_user_link(sender_id)
        c_info = await self._get_chat_info(chat_id)

        text_changed = old_text != new_text
        media_added = old_m_type == "none" and new_m_type != "none"
        media_removed = old_m_type != "none" and new_m_type == "none"
        media_changed = old_m_type != "none" and new_m_type != "none" and old_media_id and new_media_id and old_media_id != new_media_id

        if not text_changed and not media_added and not media_removed and not media_changed: return

        actions =[]
        if text_changed:
            if not old_text.strip() and new_text.strip(): act = self.strings("act_added_text").get(old_m_type, self.strings("act_added_text")["none"])
            elif old_text.strip() and not new_text.strip(): act = self.strings("act_removed_text").get(old_m_type, self.strings("act_removed_text")["none"])
            else: act = self.strings("act_edited_text").get(old_m_type, self.strings("act_edited_text")["none"])
            actions.append(self._get_msg_link(chat_id, msg_id, act))
            
        if media_added: 
            act = self.strings("act_attached").get(new_m_type, self.strings("act_attached")["none"])
            actions.append(self._get_msg_link(chat_id, msg_id, act))
        if media_removed: 
            act = self.strings("act_removed_media").get(old_m_type, self.strings("act_removed_media")["none"])
            actions.append(self._get_msg_link(chat_id, msg_id, act))
        if media_changed: 
            act = self.strings("act_replaced").get(new_m_type, self.strings("act_replaced")["none"])
            actions.append(self._get_msg_link(chat_id, msg_id, act))

        header = f"{self.strings('edited').format(sender=sender, actions=' & '.join(actions))}"
        if fwd_info: header += f"\n{self.strings('fwd_from').format(sender=fwd_info)}"
        if c_info not in["ЛС", "ЛС (PM)"]: header += f"\n{self.strings('chat').format(chat=c_info)}"

        blocks =[]
        if text_changed:
            if not old_text.strip() and new_text.strip():
                blocks.append((self.strings("text"), utils.escape_html(new_text)))
            elif old_text.strip() and not new_text.strip():
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
            if new_m_type == "contact" and getattr(message, "contact", None):
                new_media_path = self._serialize_contact(message.contact)
            elif new_m_type == "geo" and getattr(message, "geo", None):
                new_media_path = f"{message.geo.lat}|{message.geo.long}"
            else:
                try: new_media_path = await message.download_media(self.media_dir + "/")
                except: pass
            media_to_send, media_type_to_send = new_media_path, new_m_type
            
        elif media_changed or media_removed:
            media_to_send, media_type_to_send = old_media_path, old_m_type
            if media_changed:
                if new_m_type == "contact" and getattr(message, "contact", None):
                    new_media_path = self._serialize_contact(message.contact)
                elif new_m_type == "geo" and getattr(message, "geo", None):
                    new_media_path = f"{message.geo.lat}|{message.geo.long}"
                else:
                    try: new_media_path = await message.download_media(self.media_dir + "/")
                    except: pass
                    
        elif text_changed and old_m_type != "none":
            media_to_send, media_type_to_send = old_media_path, old_m_type

        parts = self._build_message(header, blocks)
        await self._send_to_bot(parts, media_to_send, media_type_to_send)

        if (media_changed or media_removed) and old_media_path and old_m_type not in["geo", "contact", "service"] and os.path.exists(old_media_path):
            os.remove(old_media_path)

        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "UPDATE messages SET text = ?, media_path = ?, timestamp = ?, media_type = ?, media_id = ? WHERE msg_id = ? AND chat_id = ?",
                    (new_text, new_media_path, datetime.now().timestamp(), new_m_type, new_media_id, msg_id, chat_id)
                )
                await db.commit()
        except: pass

    @loader.command(
        ru_doc="<@юзернейм/ID/ссылка> - Добавить чат/юзера в Белый список",
        en_doc="<@username/ID/link> - Add chat/user to Whitelist"
    )
    async def sdwlcmd(self, message: Message):
        """Add/remove chat to Whitelist"""
        chat_id, title = await self._resolve_entity(message)
        if not chat_id: return await utils.answer(message, self.strings("not_found"))

        if chat_id in self.whitelist:
            self.whitelist.remove(chat_id)
            await utils.answer(message, self.strings("rm_wl").format(chat=title))
        else:
            self.whitelist.append(chat_id)
            await utils.answer(message, self.strings("added_wl").format(chat=title))

    @loader.command(
        ru_doc="<@юзернейм/ID/ссылка> - Добавить чат/юзера в Черный список",
        en_doc="<@username/ID/link> - Add chat/user to Blacklist"
    )
    async def sdblcmd(self, message: Message):
        """Add/remove chat to Blacklist"""
        chat_id, title = await self._resolve_entity(message)
        if not chat_id: return await utils.answer(message, self.strings("not_found"))

        if chat_id in self.blacklist:
            self.blacklist.remove(chat_id)
            await utils.answer(message, self.strings("rm_bl").format(chat=title))
        else:
            self.blacklist.append(chat_id)
            await utils.answer(message, self.strings("added_bl").format(chat=title))

    @loader.command(
        ru_doc="[chat_id] - Очистить всю базу данных или историю конкретного чата",
        en_doc="[chat_id] - Clear all DB completely or clear history of specific chat"
    )
    async def sdclearcmd(self, message: Message):
        """Clear database"""
        args = utils.get_args_raw(message)
        if args:
            chat_id, title = await self._resolve_entity(message)
            if not chat_id: return await utils.answer(message, self.strings("not_found"))
            
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("SELECT media_path, media_type FROM messages WHERE chat_id = ?", (chat_id,))
                for row in await cursor.fetchall():
                    if row[0] and row[1] not in["geo", "contact", "service"] and os.path.exists(row[0]):
                        os.remove(row[0])
                await db.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
                await db.commit()
            return await utils.answer(message, self.strings("chat_cleared").format(chat=title))

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM messages")
            await db.commit()
        for file in os.listdir(self.media_dir):
            try: os.remove(os.path.join(self.media_dir, file))
            except: pass
        await utils.answer(message, self.strings("db_cleared"))

    @loader.command(
        ru_doc="Показать статистику модуля",
        en_doc="Show module statistics"
    )
    async def sdstatscmd(self, message: Message):
        """Show statistics"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("SELECT COUNT(*) FROM messages")
                count = (await cursor.fetchone())[0]
                cursor = await db.execute("SELECT COUNT(DISTINCT chat_id) FROM messages")
                chats = (await cursor.fetchone())[0]

            db_size = os.path.getsize(self.db_path) / (1024 * 1024) if os.path.exists(self.db_path) else 0
            media_size = sum(os.path.getsize(os.path.join(self.media_dir, f)) for f in os.listdir(self.media_dir) if os.path.isfile(os.path.join(self.media_dir, f))) / (1024 * 1024)

            text = (
                f"<b><tg-emoji emoji-id='5879785854284599288'>ℹ️</tg-emoji> SaveDeleted Stats:</b>\n\n"
                f"<b>Messages:</b> <code>{count}</code>\n"
                f"<b>Chats:</b> <code>{chats}</code>\n"
                f"<b>DB Size:</b> <code>{db_size:.2f} MB</code>\n"
                f"<b>Media Cache:</b> <code>{media_size:.2f} MB</code>\n\n"
                f"<b>Whitelist:</b> <code>{len(self.whitelist)}</code>\n"
                f"<b>Blacklist:</b> <code>{len(self.blacklist)}</code>\n"
            )
            await utils.answer(message, text)
        except Exception as e:
            await utils.answer(message, f"<b>Error:</b> <code>{e}</code>")
