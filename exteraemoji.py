__version__ = (1, 0, 0)

# meta developer: @Mr4epTuk
# scope: hikka_only

import re
import logging
from .. import loader, utils
from herokutl import TelegramClient
from herokutl.types import Message
from herokutl.tl.types import (
    MessageEntityCustomEmoji,
    MessageEntityTextUrl,
    UpdateEditMessage,
    UpdateEditChannelMessage,
)

logger = logging.getLogger(__name__)

_EMOJI_TAG_RE = re.compile(
    r'<emoji\s+document_id=["\x27]?(\d+)["\x27]?[^>]*>([^<]*)</emoji>'
)
_TG_EMOJI_TAG_RE = re.compile(
    r'<tg-emoji\s+emoji-id=["\x27]?(\d+)["\x27]?[^>]*>([^<]*)</tg-emoji>'
)

_BOT_METHODS = (
    "send_message",
    "edit_message_text",
    "send_photo",
    "send_video",
    "send_document",
    "send_animation",
    "send_audio",
    "send_voice",
    "edit_message_caption",
    "edit_message_media",
    "send_media_group",
)

_TEXT_KEYS = ("text", "caption")


@loader.tds
class ExteraEmojiMod(loader.Module):
    """Allows users without Telegram Premium to see premium emojis as clickable tg://emoji links.

    Replaces ALL outgoing premium emojis with tg://emoji?id=... links
    BEFORE they leave the client, so any recipient can tap the emoji
    and see the sticker preview — no Premium required.

    Works in:
    • Regular messages (MTProto TL-layer interception)
    • Edited messages
    • Media captions (photos, videos, documents)
    • Inline bot messages & forms (aiogram Bot-layer interception)
    • Albums / media groups

    ⚠️ Cases where you may want to DISABLE:
    • All chat members already have Telegram Premium
    • Sending many messages per second (extra processing)
    • Clients that don't support tg:// deep links

    Commands:
    .exteraemoji — toggle replacement on/off"""

    strings = {
        "name": "ExteraEmoji",
        "cfg_enabled": "Enable replacement",
        "cfg_enabled_doc": "Enable/disable premium emoji → link conversion globally",
        "cfg_template": "Link template",
        "cfg_template_doc": "URL template ({doc_id} = emoji document ID). Default: tg://emoji?id={doc_id}",
        "cfg_ignored": "Ignored chats",
        "cfg_ignored_doc": "Chat IDs (comma-separated) where replacement is skipped",
        "_cls_doc": "Allows users without Telegram Premium to see premium emojis as clickable tg://emoji links.\n\nReplaces ALL outgoing premium emojis with tg://emoji?id=... links BEFORE they leave the client, so any recipient can tap the emoji and see the sticker preview — no Premium required.\n\nWorks in: regular messages, edits, media captions, inline bot messages, albums.\n\nCases where you may want to DISABLE:\n• All chat members already have Telegram Premium\n• Sending many messages per second (extra processing)\n• Clients that don't support tg:// deep links\n\nCommands:\n.exteraemoji — toggle replacement on/off",
        "toggled_on": "✅ <b>ExteraEmoji is now ON</b>\n\nPremium emojis will be converted to clickable links.",
        "toggled_off": "❌ <b>ExteraEmoji is now OFF</b>\n\nPremium emojis will be sent as-is (only visible to Premium users).",
    }

    strings_ru = {
        "name": "ExteraEmoji",
        "cfg_enabled": "Включить замену",
        "cfg_enabled_doc": "Включить/выключить глобальную замену премиум-эмодзи на ссылки",
        "cfg_template": "Шаблон ссылки",
        "cfg_template_doc": "Шаблон URL ({doc_id} = ID документа эмодзи). По умолчанию: tg://emoji?id={doc_id}",
        "cfg_ignored": "Игнорируемые чаты",
        "cfg_ignored_doc": "ID чатов (через запятую), где замена не производится",
        "_cls_doc": "Позволяет пользователям без Telegram Premium видеть премиум-эмодзи как кликабельные ссылки tg://emoji.\n\nЗаменяет ВСЕ исходящие премиум-эмодзи на tg://emoji?id=... ссылки ДО отправки, так что любой получатель может нажать на эмодзи и увидеть превью стикера — Premium не требуется.\n\nРаботает в: обычных сообщениях, редактировании, подписях к медиа, инлайн-сообщениях бота, альбомах.\n\nСлучаи когда лучше ОТКЛЮЧИТЬ:\n• Все участники чатов уже имеют Telegram Premium\n• Отправка большого количества сообщений в секунду\n• Клиенты не поддерживающие tg:// deep-ссылки\n\nКоманды:\n.exteraemoji — вкл/выкл замену",
        "toggled_on": "✅ <b>ExteraEmoji ВКЛЮЧЕН</b>\n\nПремиум-эмодзи будут заменены на кликабельные ссылки.",
        "toggled_off": "❌ <b>ExteraEmoji ВЫКЛЮЧЕН</b>\n\nПремиум-эмодзи будут отправлены как есть (видны только Premium-пользователям).",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "enabled",
                True,
                lambda: self.strings("cfg_enabled_doc"),
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "link_template",
                "tg://emoji?id={doc_id}",
                lambda: self.strings("cfg_template_doc"),
                validator=loader.validators.String(min_len=1),
            ),
            loader.ConfigValue(
                "ignore_chats",
                [],
                lambda: self.strings("cfg_ignored_doc"),
                validator=loader.validators.Series(loader.validators.Integer()),
            ),
        )

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._processed = set()
        self._hooked_mtproto = False
        self._hooked_bot = False
        self._bot_real = None
        self._bot_orig = {}

        await self._try_hook_mtproto()
        await self._try_hook_inline_bot()

    @loader.command(
        ru_doc="Включить/выключить замену премиум-эмодзи на ссылки",
        en_doc="Toggle premium emoji → link replacement on/off",
    )
    async def exteraemojicmd(self, message: Message):
        """Toggle ExteraEmoji replacement on/off"""
        current = self.config["enabled"]
        self.config["enabled"] = not current
        if current:
            await utils.answer(message, self.strings("toggled_off"))
        else:
            await utils.answer(message, self.strings("toggled_on"))

    def _build_url(self, document_id: int) -> str:
        return self.config["link_template"].format(doc_id=document_id)

    def _replace_html(self, text: str) -> str:
        if not isinstance(text, str) or not text:
            return text

        template = self.config["link_template"]

        text = _EMOJI_TAG_RE.sub(
            lambda m: '<a href="{}">{}</a>'.format(
                template.format(doc_id=m.group(1)),
                m.group(2),
            ),
            text,
        )

        text = _TG_EMOJI_TAG_RE.sub(
            lambda m: '<a href="{}">{}</a>'.format(
                template.format(doc_id=m.group(1)),
                m.group(2),
            ),
            text,
        )

        return text

    def _convert_entities(self, entities: list) -> list:
        if not entities:
            return entities

        changed = False
        result = []

        for entity in entities:
            if isinstance(entity, MessageEntityCustomEmoji):
                result.append(
                    MessageEntityTextUrl(
                        offset=entity.offset,
                        length=entity.length,
                        url=self._build_url(entity.document_id),
                    )
                )
                changed = True
            else:
                result.append(entity)

        return result if changed else entities

    def _process_tl_request(self, request):
        if not self.config["enabled"]:
            return request

        name = type(request).__name__

        if name in ("SendMessageRequest", "EditMessageRequest", "SendMediaRequest"):
            entities = getattr(request, "entities", None)
            if entities:
                converted = self._convert_entities(entities)
                if converted is not entities:
                    request.entities = converted

            msg = getattr(request, "message", None)
            if isinstance(msg, str) and ("<emoji" in msg or "<tg-emoji" in msg):
                request.message = self._replace_html(msg)

        elif name == "SendMultiMediaRequest":
            for media in getattr(request, "multi_media", None) or []:
                ent = getattr(media, "entities", None)
                if ent:
                    converted = self._convert_entities(ent)
                    if converted is not ent:
                        media.entities = converted

                msg = getattr(media, "message", None)
                if isinstance(msg, str) and ("<emoji" in msg or "<tg-emoji" in msg):
                    media.message = self._replace_html(msg)

        return request

    async def _try_hook_mtproto(self):
        module = self

        async def _patched_call(self_client, request, *args, **kwargs):
            request = module._process_tl_request(request)
            return await TelegramClient._prem_orig_call(
                self_client, request, *args, **kwargs
            )

        try:
            if not hasattr(TelegramClient, "_prem_orig_call"):
                TelegramClient._prem_orig_call = TelegramClient.__call__

            TelegramClient.__call__ = _patched_call

            self._hooked_mtproto = True
            logger.info("ExteraEmoji: MTProto __call__ hook active")
        except Exception as e:
            logger.info(
                "ExteraEmoji: MTProto hook blocked (%s), fallback active", e
            )
            self._hooked_mtproto = False

    async def _try_hook_inline_bot(self):
        module = self

        bot = self._find_bot()
        if bot is None:
            logger.info("ExteraEmoji: inline bot not found, skipping aiogram hook")
            return

        real_bot = self._unwrap_proxy(bot)
        self._bot_real = real_bot

        logger.info(
            "ExteraEmoji: found bot (class=%s, unwrapped=%s)",
            type(real_bot).__name__,
            real_bot is not bot,
        )

        def _process_text_args(args, kwargs):
            new_args = list(args)
            new_kwargs = dict(kwargs)
            for i, arg in enumerate(new_args):
                if isinstance(arg, str):
                    new_args[i] = module._replace_html(arg)
            for key in _TEXT_KEYS:
                if key in new_kwargs and isinstance(new_kwargs[key], str):
                    new_kwargs[key] = module._replace_html(new_kwargs[key])
            return new_args, new_kwargs

        def _make_patched(orig):
            async def _patched(*args, **kwargs):
                if not module.config["enabled"]:
                    return await orig(*args, **kwargs)
                new_args, new_kwargs = _process_text_args(args, kwargs)
                return await orig(*new_args, **new_kwargs)
            return _patched

        try:
            for method_name in _BOT_METHODS:
                orig = getattr(real_bot, method_name, None)
                if orig is None:
                    continue
                self._bot_orig[method_name] = orig
                setattr(real_bot, method_name, _make_patched(orig))

            self._hooked_bot = True
            logger.info("ExteraEmoji: bot instance hooks active (%d methods)", len(self._bot_orig))
        except Exception as e:
            logger.info("ExteraEmoji: bot hook blocked (%s)", e)
            self._hooked_bot = False

    def _find_bot(self):
        candidates = []
        try:
            candidates.append(self.inline)
        except Exception:
            pass
        try:
            candidates.append(getattr(self, "_client", None))
        except Exception:
            pass

        for root in candidates:
            if root is None:
                continue
            for attr in ("bot", "_bot", "_dispatcher", "dispatcher"):
                obj = getattr(root, attr, None)
                if obj is None:
                    continue
                if attr in ("_dispatcher", "dispatcher"):
                    bot_candidate = getattr(obj, "bot", None) or getattr(obj, "_bot", None)
                    if bot_candidate is not None and hasattr(bot_candidate, "send_message"):
                        return bot_candidate
                elif hasattr(obj, "send_message"):
                    return obj

        for root in candidates:
            if root is None:
                continue
            try:
                for attr_name in dir(root):
                    if attr_name.startswith("_"):
                        continue
                    obj = getattr(root, attr_name, None)
                    if obj is not None and hasattr(obj, "send_message") and hasattr(obj, "edit_message_text"):
                        return obj
            except Exception:
                pass

        return None

    @staticmethod
    def _unwrap_proxy(obj):
        for key in ("_obj", "__obj", "_target", "__target", "_wrapped", "__wrapped__"):
            val = getattr(obj, key, None)
            if val is not None:
                return val
        for attr in dir(obj):
            if attr.startswith("_SafeProxy__"):
                return getattr(obj, attr)
        return obj

    def _replace_via_entities(self, message: Message) -> str:
        text = message.message or ""
        if not message.entities:
            return text

        entities = sorted(message.entities, key=lambda e: e.offset, reverse=True)
        result = text

        for entity in entities:
            if isinstance(entity, MessageEntityCustomEmoji):
                start = entity.offset
                end = entity.offset + entity.length
                emoji_text = utils.escape_html(result[start:end])
                link_html = '<a href="{}">{}</a>'.format(
                    self._build_url(entity.document_id),
                    emoji_text,
                )
                result = result[:start] + link_html + result[end:]

        return result

    async def _fallback_edit_message(self, message: Message):
        msg_key = (message.chat_id, message.id)
        if msg_key in self._processed:
            self._processed.discard(msg_key)
            return

        raw = getattr(message, "raw_text", None) or ""

        if raw and ("<emoji" in raw or "<tg-emoji" in raw):
            new_text = self._replace_html(raw)
            if new_text != raw:
                self._processed.add(msg_key)
                try:
                    await message.edit(new_text, parse_mode="html")
                except Exception:
                    logger.debug("Fallback HTML edit failed", exc_info=True)
                    self._processed.discard(msg_key)
            return

        entities = message.entities or []
        if any(isinstance(e, MessageEntityCustomEmoji) for e in entities):
            try:
                new_text = self._replace_via_entities(message)
                if new_text != (message.message or ""):
                    self._processed.add(msg_key)
                    await message.edit(new_text, parse_mode="html")
            except Exception:
                logger.debug("Fallback entity edit failed", exc_info=True)
                self._processed.discard(msg_key)

    @loader.watcher(out=True, only_messages=True)
    async def emoji_replacer_watcher(self, message: Message):
        """Fallback watcher for new outgoing messages"""
        if self._hooked_mtproto:
            return
        if not self.config["enabled"]:
            return
        if message.sender_id in (777000, 489000):
            return
        if message.chat_id in self.config["ignore_chats"]:
            return
        await self._fallback_edit_message(message)

    @loader.raw_handler(UpdateEditMessage, UpdateEditChannelMessage)
    async def emoji_replacer_edit_handler(self, event):
        """Fallback handler for outgoing message edits"""
        if self._hooked_mtproto:
            return
        if not self.config["enabled"]:
            return

        message = getattr(event, "message", None)
        if message is None:
            return
        if not getattr(message, "out", False):
            return
        if message.sender_id in (777000, 489000):
            return
        if message.chat_id in self.config["ignore_chats"]:
            return
        await self._fallback_edit_message(message)

    async def on_unload(self):
        if self._hooked_mtproto:
            try:
                TelegramClient.__call__ = TelegramClient._prem_orig_call
                del TelegramClient._prem_orig_call
                logger.info("ExteraEmoji: MTProto hook restored")
            except Exception:
                logger.debug("ExteraEmoji: failed to restore MTProto hook", exc_info=True)

        if self._hooked_bot and self._bot_real is not None:
            for method_name, orig in self._bot_orig.items():
                try:
                    setattr(self._bot_real, method_name, orig)
                except Exception:
                    pass
            logger.info("ExteraEmoji: bot hooks restored")

        self._processed.clear()
