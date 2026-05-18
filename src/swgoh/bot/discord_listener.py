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

Error isolation:
  - Anything raised inside on_message is caught and logged. We never let
    a single bad message crash the listener.
  - Connection failures are handled by discord.py's built-in auto-
    reconnect (configurable via `reconnect=True`, the default).
  - If the bot token is missing or invalid, we log and skip startup
    rather than crashing the whole bot process.

What we deliberately do NOT do:
  - Persist anything to disk. The Discord channel itself is the audit log.
  - Send messages back to Discord. The bot has read-only permissions
    (Send Messages was deliberately not granted).
  - Trust attachment content blindly. We size-check before download and
    validate the JSON parse before touching the cache.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

import discord
from telegram.error import TelegramError
from telegram.ext import Application

from ..tb import (
    ParseError,
    format_auto_summary,
    parse_tb_snapshot,
)
from . import config as bot_cfg
from .services import tb_cache

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

# Hard cap on attachment size we'll download. A typical TB export is
# under 1 MB. 10 MB is plenty of headroom and protects against pathological
# attachments (a maliciously-crafted file, a future C3PO export with
# unexpected expansion, etc.).
MAX_ATTACHMENT_BYTES: int = 10 * 1024 * 1024

# Filename suffix we treat as a TB export. C3PO may produce other JSON
# files (e.g. for raids); this filter keeps the listener focused.
TB_FILENAME_SUFFIX: str = "-tb.json"

# Key under application.bot_data where we stash the listener handle.
# Lets stop_discord_listener find what start_discord_listener created.
_LISTENER_KEY = "__discord_listener__"


# ---------------------------------------------------------------------------
# discord.py client
# ---------------------------------------------------------------------------

class _TBListenerClient(discord.Client):
    """
    Minimal discord.py client. Holds references to the PTB Application
    (for cache access and Telegram sending) and to our config values.

    Why a subclass: discord.py's recommended pattern is event handlers
    as methods on a Client subclass. Could also use the decorator-based
    @client.event pattern, but subclassing keeps state (the PTB
    application reference) cleanly encapsulated.
    """

    def __init__(
        self,
        *,
        application: Application,
        c3po_user_id: int,
        watch_channel_id: int,
        auto_forward_chat_id: int,
        intents: discord.Intents,
    ) -> None:
        super().__init__(intents=intents)
        self._application = application
        self._c3po_user_id = c3po_user_id
        self._watch_channel_id = watch_channel_id
        self._auto_forward_chat_id = auto_forward_chat_id

    # ---- discord.py event handlers ----

    async def on_ready(self) -> None:
        """Logged once after connect / reconnect. Useful for verifying
        the bot is up and in the right server."""
        log.info(
            "Discord listener ready: user=%s id=%s watching channel_id=%d "
            "for messages from C3PO (id=%d)",
            self.user, self.user.id if self.user else "?",
            self._watch_channel_id, self._c3po_user_id,
        )

    async def on_message(self, message: discord.Message) -> None:
        """
        Filter then dispatch. Any exception here is caught at the
        outermost level so a single bad message can't crash the listener.
        """
        try:
            await self._handle_message(message)
        except Exception:
            log.exception(
                "Unhandled error processing Discord message id=%s author=%s",
                message.id, message.author.id,
            )

    # ---- internal handling ----

    async def _handle_message(self, message: discord.Message) -> None:
        # Filter 1: must be from C3PO.
        if message.author.id != self._c3po_user_id:
            return

        # Filter 2: must be in the watch channel.
        if message.channel.id != self._watch_channel_id:
            return

        # Filter 3: must have at least one JSON attachment.
        if not message.attachments:
            log.debug(
                "C3PO message id=%s in watch channel had no attachments; "
                "ignoring", message.id,
            )
            return

        # Process every matching attachment in the message. C3PO
        # typically posts one, but we don't assume.
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
        """
        Download → parse → cache → forward. Each stage logs its own
        outcome. Any failure aborts this attachment but doesn't affect
        others or future messages.
        """
        log.info(
            "Processing TB attachment %r (size=%d bytes) from message %s",
            attachment.filename, attachment.size, message.id,
        )

        # Size guard before download.
        if attachment.size > MAX_ATTACHMENT_BYTES:
            log.warning(
                "Refusing oversized attachment %r: %d > %d bytes",
                attachment.filename, attachment.size, MAX_ATTACHMENT_BYTES,
            )
            return

        # Download in memory. discord.py exposes attachment.read()
        # which returns bytes; no temp file involved.
        try:
            raw_bytes = await attachment.read()
        except discord.HTTPException as e:
            log.warning("Failed to download attachment %r: %s", attachment.filename, e)
            return

        # Decode + JSON-parse.
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

        # Parse into our domain model.
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

        # Update the in-memory cache so /tb_status and friends see it.
        tb_cache.set_latest(
            self._application.bot_data,
            snapshot,
            source_filename=attachment.filename,
        )

        # Auto-forward the summary to the officers' chat.
        try:
            summary = format_auto_summary(snapshot)
            await self._notify_officers(summary)
            log.info(
                "Auto-forwarded TB summary for instance=%s round=%d "
                "to chat_id=%d",
                snapshot.instance_id, snapshot.current_round,
                self._auto_forward_chat_id,
            )
        except Exception:
            log.exception(
                "Failed to format or forward auto-summary; cache was still "
                "updated successfully."
            )

    async def _notify_officers(self, text: str) -> None:
        """
        Send a message to the configured officers' chat via the PTB Bot.
        Any failure is logged but doesn't propagate — losing one
        notification is preferable to crashing the listener.
        """
        try:
            await self._application.bot.send_message(
                chat_id=self._auto_forward_chat_id,
                text=text,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        except TelegramError as e:
            log.warning(
                "Telegram send_message failed (chat_id=%d): %s",
                self._auto_forward_chat_id, e,
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _md_safe(text: str) -> str:
    """
    Light Markdown escape for legacy-Markdown dialect. Used only for
    bot-generated notification text where we embed external strings
    (filenames, error messages). The main `format_auto_summary` and
    friends already handle their own escaping.
    """
    if not text:
        return ""
    out = text
    for ch in ("*", "_", "`", "["):
        out = out.replace(ch, "\\" + ch)
    return out


def _build_intents() -> discord.Intents:
    """
    Minimal intents for a read-only listener.

    Enabled:
      - Guilds          (default; lets us see servers we're in)
      - Guild messages  (so on_message fires for our watch channel)
      - Message content (so we can read attachments; this is the
                         "Message Content Intent" toggle in the
                         Discord developer portal — must also be
                         enabled there or attachments come through
                         as empty).

    Disabled (explicit default):
      - Members, presences, typing, reactions, voice — none of these
        are needed for our use case, and disabling them avoids
        Discord's verification requirements at 100+ guilds.
    """
    intents = discord.Intents.none()
    intents.guilds = True
    intents.guild_messages = True
    intents.message_content = True
    return intents


# ---------------------------------------------------------------------------
# Public API — called from main_bot.py
# ---------------------------------------------------------------------------

async def start_discord_listener(application: Application) -> None:
    """
    Spawn the Discord listener as a background task on PTB's event loop.

    Called from main_bot.py's post_init hook. Returns immediately; the
    actual Discord connection happens asynchronously.

    Behavior on misconfiguration:
      - Missing token → log warning and skip startup. The rest of the
        bot continues working without Discord integration.
      - Invalid IDs (zero) → log warning and skip.

    Why warning-and-skip rather than fail-fast: the project's other
    integrations (Comlink, Sheets) all crash on startup if their
    credentials are missing — they're load-bearing. The Discord listener
    is an optional enhancement; running the bot without it should be
    possible while officers configure or troubleshoot Discord.
    """
    token = getattr(bot_cfg, "DISCORD_BOT_TOKEN", "") or ""
    c3po_id = getattr(bot_cfg, "DISCORD_C3PO_USER_ID", 0) or 0
    channel_id = getattr(bot_cfg, "DISCORD_WATCH_CHANNEL_ID", 0) or 0
    forward_chat = getattr(bot_cfg, "TB_AUTO_FORWARD_CHAT_ID", 0) or 0

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

    client = _TBListenerClient(
        application=application,
        c3po_user_id=c3po_id,
        watch_channel_id=channel_id,
        auto_forward_chat_id=forward_chat,
        intents=_build_intents(),
    )

    # Spawn client.start() as a background task. start() blocks until
    # the connection drops, so we never await it here — we let it run
    # alongside PTB's polling loop.
    task = asyncio.create_task(
        _run_client(client, token),
        name="discord_listener",
    )

    # Stash for shutdown. We keep both the client (so we can .close())
    # and the task (so we can cancel and await it cleanly).
    application.bot_data[_LISTENER_KEY] = (client, task)
    log.info("Discord listener task spawned.")


async def _run_client(client: _TBListenerClient, token: str) -> None:
    """
    Inner runner that handles the discord.py login errors we care about.

    discord.py raises LoginFailure for a bad token. We log and exit
    rather than letting the exception bubble up and crash PTB's loop.
    Any other unexpected exception is logged with a stack trace.
    """
    try:
        await client.start(token, reconnect=True)
    except discord.LoginFailure:
        log.error(
            "Discord login failed: token is invalid or revoked. "
            "Reset DISCORD_BOT_TOKEN and restart."
        )
    except asyncio.CancelledError:
        # Shutdown path — propagate so the task cleans up.
        raise
    except Exception:
        log.exception("Unexpected error in Discord listener task.")


async def stop_discord_listener(application: Application) -> None:
    """
    Close the Discord client cleanly. Called from main_bot.py's
    post_shutdown hook.

    Idempotent: safe to call even if start_discord_listener was a no-op.
    """
    entry: Optional[tuple] = application.bot_data.pop(_LISTENER_KEY, None)
    if entry is None:
        return
    client, task = entry

    log.info("Closing Discord listener.")
    try:
        await client.close()
    except Exception:
        log.exception("Error closing Discord client.")

    # Wait for the task to actually finish, with a short timeout so
    # we don't hang the shutdown.
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
