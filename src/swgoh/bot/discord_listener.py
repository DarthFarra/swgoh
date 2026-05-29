# src/swgoh/bot/discord_listener.py
"""
Discord listener: watches a single channel for C3PO TB exports.

Flow:
  1. C3PO posts a `*-tb.json` attachment in the watch channel.
  2. We filter by author (C3PO's user ID) and channel ID.
  3. We download the attachment to memory (no disk write, per design).
  4. We parse it via tb.parse_tb_snapshot().
  5. We update the in-memory cache via tb_cache.set_latest().
  6. We format an auto-summary and post it to the officers' Telegram chat
     via the PTB Bot instance.

Lifecycle:
  - Start: main_bot.py calls start_discord_listener(application) inside
    its post_init hook. We spawn a background task that runs the
    discord.py client on the same asyncio loop as PTB.
  - Shutdown: main_bot.py calls stop_discord_listener(application) in
    post_shutdown. We close the client cleanly.

Telegram routing:
  - TB_AUTO_FORWARD_CHAT_ID: target chat (required).
  - TB_AUTO_FORWARD_THREAD_ID: optional forum-topic id within that chat.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

import discord
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError
from telegram.ext import Application

from ..tb import (
    ParseError,
    format_auto_summary,
    parse_tb_snapshot,
)
from ..tb.formatters import auto_summary_undeployed, format_auto_summary_split

from . import config as bot_cfg
from .services import tb_cache, tb_map_config_cache, tb_targets_cache
from .services.sheets import open_ss, resolve_label_name_by_guild_id
from .services.tb_undeployed_cache import (
    UndeployedMember,
    UndeployedSnapshot,
    set_snapshot as set_undeployed_snapshot,
)


log = logging.getLogger(__name__)


MAX_ATTACHMENT_BYTES: int = 10 * 1024 * 1024
TB_FILENAME_SUFFIX: str = "-tb.json"
_LISTENER_KEY = "__discord_listener__"


# ---------------------------------------------------------------------------
# discord.py client
# ---------------------------------------------------------------------------

class _TBListenerClient(discord.Client):
    """
    Minimal discord.py client. Holds direct references to PTB's bot_data
    (for cache access) and Bot (for outbound Telegram messages), rather
    than the Application itself.
    """

    def __init__(
        self,
        *,
        ptb_bot_data: dict,
        ptb_bot,
        c3po_user_id: int,
        watch_channel_id: int,
        auto_forward_chat_id: int,
        auto_forward_thread_id: int,
        intents: discord.Intents,
    ) -> None:
        super().__init__(intents=intents)
        self._ptb_bot_data = ptb_bot_data
        self._ptb_bot = ptb_bot
        self._c3po_user_id = c3po_user_id
        self._watch_channel_id = watch_channel_id
        self._auto_forward_chat_id = auto_forward_chat_id
        self._auto_forward_thread_id = auto_forward_thread_id

    async def on_ready(self) -> None:
        thread_note = (
            f" thread={self._auto_forward_thread_id}"
            if self._auto_forward_thread_id else ""
        )
        log.info(
            "Discord listener ready: user=%s id=%s watching channel_id=%d "
            "for messages from C3PO (id=%d); forwarding to chat_id=%d%s",
            self.user, self.user.id if self.user else "?",
            self._watch_channel_id, self._c3po_user_id,
            self._auto_forward_chat_id, thread_note,
        )

    async def on_message(self, message: discord.Message) -> None:
        try:
            await self._handle_message(message)
        except Exception:
            log.exception(
                "Unhandled error processing Discord message id=%s author=%s",
                message.id, message.author.id,
            )

    async def _handle_message(self, message: discord.Message) -> None:
        if message.author.id != self._c3po_user_id:
            return
        if message.channel.id != self._watch_channel_id:
            return
        if not message.attachments:
            log.debug(
                "C3PO message id=%s in watch channel had no attachments; "
                "ignoring", message.id,
            )
            return

        for att in message.attachments:
            if not att.filename.endswith(TB_FILENAME_SUFFIX):
                log.debug(
                    "Skipping attachment %r (not a TB export)", att.filename,
                )
                continue
            await self._process_attachment(att, message)

    async def _process_attachment(
        self,
        attachment: discord.Attachment,
        message: discord.Message,
    ) -> None:
        log.info(
            "Processing TB attachment %r (size=%d bytes) from message %s",
            attachment.filename, attachment.size, message.id,
        )

        if attachment.size > MAX_ATTACHMENT_BYTES:
            log.warning(
                "Refusing oversized attachment %r: %d > %d bytes",
                attachment.filename, attachment.size, MAX_ATTACHMENT_BYTES,
            )
            return

        try:
            raw_bytes = await attachment.read()
        except discord.HTTPException as e:
            log.warning("Failed to download attachment %r: %s", attachment.filename, e)
            return

        try:
            text = raw_bytes.decode("utf-8")
            raw = json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            log.warning(
                "Attachment %r is not valid UTF-8 JSON: %s",
                attachment.filename, e,
            )
            await self._notify_officers(
                f"⚠️ Recibí un archivo `{_md_safe(attachment.filename)}` "
                f"de Discord pero no es JSON válido. _Revisa el log._"
            )
            return

        try:
            snapshot = parse_tb_snapshot(raw)
        except ParseError as e:
            log.warning(
                "Attachment %r is not a TB export: %s",
                attachment.filename, e,
            )
            await self._notify_officers(
                f"⚠️ Recibí `{_md_safe(attachment.filename)}` pero no parece "
                f"un export de TB. _{_md_safe(str(e))}_"
            )
            return

        tb_cache.set_latest(
            self._ptb_bot_data,
            snapshot,
            source_filename=attachment.filename,
        )

        try:
            map_config = tb_map_config_cache.get(self._ptb_bot_data)
            tb_targets = tb_targets_cache.get(self._ptb_bot_data)

            # Build the two pieces independently. planet_messages has no
            # buttons; undeployed_message gets the buttons attached (and
            # is what we key the cache by).
            planet_messages, undeployed_message = format_auto_summary_split(
                snap=snapshot,
                map_config=map_config,
                tb_targets=tb_targets,
            )

            # Capture the undeployed list at THIS exact moment, so the
            # buttons act on what the message displays — not a
            # recomputed list. (Empty list = no buttons; still send.)
            undeployed_rows = auto_summary_undeployed(snapshot)

            label, _gname = (None, None)
            try:
                ss = open_ss()
                label, _gname = resolve_label_name_by_guild_id(ss, snapshot.guild_id)
            except Exception:
                log.exception("Sheets lookup failed; sending auto-summary without buttons.")

            await self._send_auto_summary_split(
                planet_messages=planet_messages,
                undeployed_message=undeployed_message,
                guild_id=snapshot.guild_id if undeployed_rows and label else None,
                undeployed_rows=undeployed_rows if label else (),
                guild_name_for_cache=snapshot.guild_name,
            )
            log.info(
                "Auto-forwarded TB summary for instance=%s round=%d "
                "guild=%s undeployed=%d (planet=%d msg, undep=1 msg) to chat_id=%d",
                snapshot.instance_id, snapshot.current_round,
                snapshot.guild_id, len(undeployed_rows),
                len(planet_messages), self._auto_forward_chat_id,
            )
        except Exception:
            log.exception(
                "Failed to format or forward auto-summary; cache was still "
                "updated successfully."
            )

    async def _notify_officers(
        self,
        messages,
    ) -> None:
        """
        Send a simple text-only notification (used for error paths only:
        bad JSON, parse error).

        For the auto-summary itself, use _send_auto_summary_split — it
        handles the planet/undeployed message split and button caching.

        Args:
          messages: str or list[str]. Each non-empty string is sent as
            a separate Telegram message. No buttons, no cache writes.
        """
        if isinstance(messages, str):
            messages = [messages]

        for i, text in enumerate(messages):
            if not text.strip():
                continue

            send_kwargs: dict[str, Any] = {
                "chat_id": self._auto_forward_chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }
            if self._auto_forward_thread_id:
                send_kwargs["message_thread_id"] = self._auto_forward_thread_id

            try:
                await self._ptb_bot.send_message(**send_kwargs)
            except TelegramError as e:
                log.warning(
                    "Telegram send_message failed (chat_id=%d thread_id=%d, msg %d/%d): %s",
                    self._auto_forward_chat_id,
                    self._auto_forward_thread_id,
                    i + 1, len(messages), e,
                )
                continue

    async def _send_auto_summary_split(
        self,
        *,
        planet_messages,
        undeployed_message: str,
        guild_id: Optional[str] = None,
        undeployed_rows: tuple = (),
        guild_name_for_cache: str = "",
    ) -> None:
        """
        Send the auto-summary as TWO separate Telegram messages.

        First: each planet_messages string in order. No buttons.
        Second (and ALWAYS sent — even at 0 undeployed): the undeployed
        message. Buttons attached IF guild_id is non-None AND
        undeployed_rows is non-empty.

        The cache (tb_undeployed_cache) is keyed by the UNDEPLOYED
        message's id, because that's where the buttons live and that's
        what the callback handlers will look up.

        Args:
          planet_messages: list of strings from format_auto_summary_split.
          undeployed_message: single string from format_auto_summary_split.
          guild_id: snap.guild_id (CG id). None = no buttons regardless
            of undeployed_rows.
          undeployed_rows: tuple of _UndeployedRow from auto_summary_undeployed.
            Empty = no buttons even if guild_id set.
          guild_name_for_cache: snap.guild_name, denormalized into the
            cache so callbacks don't need to re-read the snapshot.
        """
        # ---- Step 1: planet messages, no buttons ----
        for i, text in enumerate(planet_messages):
            if not text.strip():
                continue
            send_kwargs: dict[str, Any] = {
                "chat_id": self._auto_forward_chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }
            if self._auto_forward_thread_id:
                send_kwargs["message_thread_id"] = self._auto_forward_thread_id

            try:
                await self._ptb_bot.send_message(**send_kwargs)
            except TelegramError as e:
                log.warning(
                    "Telegram send_message failed for planet msg %d/%d "
                    "(chat_id=%d thread_id=%d): %s",
                    i + 1, len(planet_messages),
                    self._auto_forward_chat_id,
                    self._auto_forward_thread_id, e,
                )
                # Continue — we still want to attempt the undeployed
                # message even if a planet message failed.
                continue

        # ---- Step 2: undeployed message, buttons IF appropriate ----
        attach_buttons = (
            bool(guild_id) and bool(undeployed_rows)
        )
        reply_markup = (
            _build_undeployed_keyboard(guild_id) if attach_buttons else None
        )

        send_kwargs = {
            "chat_id": self._auto_forward_chat_id,
            "text": undeployed_message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
            "reply_markup": reply_markup,
        }
        if self._auto_forward_thread_id:
            send_kwargs["message_thread_id"] = self._auto_forward_thread_id

        try:
            sent = await self._ptb_bot.send_message(**send_kwargs)
        except TelegramError as e:
            log.warning(
                "Telegram send_message failed for undeployed msg "
                "(chat_id=%d thread_id=%d): %s",
                self._auto_forward_chat_id,
                self._auto_forward_thread_id, e,
            )
            return

        # ---- Step 3: cache the undeployed list, keyed by THIS message_id ----
        if attach_buttons and sent is not None:
            cache_members = tuple(
                UndeployedMember(
                    player_id=r.player_id,
                    player_name=r.player_name,
                    deployed_gp=r.deployed_gp,
                    roster_gp=r.roster_gp,
                    missing_gp=r.missing_gp,
                    pct_deployed=r.pct_deployed,
                )
                for r in undeployed_rows
            )
            set_undeployed_snapshot(
                self._ptb_bot_data,
                message_id=sent.message_id,
                snapshot=UndeployedSnapshot(
                    guild_id=guild_id,
                    guild_name=guild_name_for_cache,
                    members=cache_members,
                ),
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_undeployed_keyboard(guild_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "📨 Enviar DMs",
                callback_data=f"tbudm:{guild_id}",
            ),
            InlineKeyboardButton(
                "📢 Publicar en avisos",
                callback_data=f"tbupub:{guild_id}",
            ),
        ],
    ])


def _md_safe(text: str) -> str:
    if not text:
        return ""
    out = text
    for ch in ("*", "_", "`", "["):
        out = out.replace(ch, "\\" + ch)
    return out


def _build_intents() -> discord.Intents:
    intents = discord.Intents.none()
    intents.guilds = True
    intents.guild_messages = True
    intents.message_content = True
    return intents


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def start_discord_listener(application: Application) -> None:
    token = getattr(bot_cfg, "DISCORD_BOT_TOKEN", "") or ""
    c3po_id = getattr(bot_cfg, "DISCORD_C3PO_USER_ID", 0) or 0
    channel_id = getattr(bot_cfg, "DISCORD_WATCH_CHANNEL_ID", 0) or 0
    forward_chat = getattr(bot_cfg, "TB_AUTO_FORWARD_CHAT_ID", 0) or 0
    forward_thread = getattr(bot_cfg, "TB_AUTO_FORWARD_THREAD_ID", 0) or 0

    if not token:
        log.warning("DISCORD_BOT_TOKEN not set; Discord listener disabled.")
        return
    if c3po_id <= 0:
        log.warning(
            "DISCORD_C3PO_USER_ID not set or invalid; Discord listener disabled."
        )
        return
    if channel_id <= 0:
        log.warning(
            "DISCORD_WATCH_CHANNEL_ID not set or invalid; "
            "Discord listener disabled."
        )
        return
    if forward_chat == 0:
        log.warning(
            "TB_AUTO_FORWARD_CHAT_ID not set; Discord listener disabled. "
            "Set it to the Telegram chat that should receive auto-summaries."
        )
        return

    if forward_thread < 0:
        log.warning(
            "TB_AUTO_FORWARD_THREAD_ID=%d is negative; treating as 0.",
            forward_thread,
        )
        forward_thread = 0

    client = _TBListenerClient(
        ptb_bot_data=application.bot_data,
        ptb_bot=application.bot,
        c3po_user_id=c3po_id,
        watch_channel_id=channel_id,
        auto_forward_chat_id=forward_chat,
        auto_forward_thread_id=forward_thread,
        intents=_build_intents(),
    )

    task = asyncio.create_task(
        _run_client(client, token),
        name="discord_listener",
    )

    application.bot_data[_LISTENER_KEY] = (client, task)
    log.info("Discord listener task spawned.")


async def _run_client(client: _TBListenerClient, token: str) -> None:
    try:
        await client.start(token, reconnect=True)
    except discord.LoginFailure:
        log.error(
            "Discord login failed: token is invalid or revoked. "
            "Reset DISCORD_BOT_TOKEN and restart."
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Unexpected error in Discord listener task.")


async def stop_discord_listener(application: Application) -> None:
    entry: Optional[tuple] = application.bot_data.pop(_LISTENER_KEY, None)
    if entry is None:
        return
    client, task = entry

    log.info("Closing Discord listener.")
    try:
        await client.close()
    except Exception:
        log.exception("Error closing Discord client.")

    try:
        await asyncio.wait_for(task, timeout=5.0)
    except asyncio.TimeoutError:
        log.warning("Discord listener task did not exit within 5s; cancelling.")
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
    except (asyncio.CancelledError, Exception):
        pass
