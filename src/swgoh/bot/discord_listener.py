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
from telegram import InlineKeyboardButton, InlineKeyboardMarkup


from ..tb import (
    ParseError,
    format_auto_summary,
    parse_tb_snapshot,
)

from ..tb.formatters import auto_summary_undeployed

from . import config as bot_cfg
from .services import tb_cache, tb_map_config_cache
from .services.sheets import open_ss, resolve_label_name_by_guild_id
from .services.tb_undeployed_cache import (
    UndeployedMember,
    UndeployedSnapshot,
    set_snapshot as set_undeployed_snapshot,
  )


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
    Minimal discord.py client. Holds direct references to PTB's bot_data
    (for cache access) and Bot (for outbound Telegram messages), rather
    than the Application itself.

    Why not store the Application: discord.py's Client has an `application`
    attribute of its own (auto-populated post-login with the bot's
    AppInfo). Storing PTB's Application as `self.application` would
    collide; storing as `self._application` should be safe, but in
    practice the internals of discord.py have caused this to be
    overwritten in some setups. Storing bot_data + bot avoids the
    question entirely.
    """

    def __init__(
        self,
        *,
        ptb_bot_data: dict,
        ptb_bot,                      # telegram.Bot, type-import-free
        c3po_user_id: int,
        watch_channel_id: int,
        auto_forward_chat_id: int,
        intents: discord.Intents,
    ) -> None:
        super().__init__(intents=intents)
        self._ptb_bot_data = ptb_bot_data
        self._ptb_bot = ptb_bot
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
            self._ptb_bot_data,
            snapshot,
            source_filename=attachment.filename,
        )
        # Auto-forward the summary to the officers' chat.
        try:
          map_config = tb_map_config_cache.get(self._ptb_bot_data)
          messages = format_auto_summary(snapshot, map_config)
          # Capture the undeployed list at THIS exact moment, so the
          # auto-summary's inline buttons act on the same members the
          # message displays — not a recomputed list.
          undeployed_rows = auto_summary_undeployed(snapshot)
        
          # Resolve sheet-side guild label (None if guild not in sheet —
          # in which case we still post the message, just without buttons).
          label, _gname = (None, None)
          try:
            ss = open_ss()
            label, _gname = resolve_label_name_by_guild_id(ss, snapshot.guild_id)
          except Exception:
            # Sheets unreachable. Send the message text-only — better
            # than failing the whole notification.
            log.exception("Sheets lookup failed; sending auto-summary without buttons.")
        
          await self._notify_officers(
            messages,
            guild_id=snapshot.guild_id if undeployed_rows and label else None,
            undeployed_rows=undeployed_rows if label else (),
            guild_name_for_cache=snapshot.guild_name,
          )
          log.info(
            "Auto-forwarded TB summary for instance=%s round=%d "
            "guild=%s undeployed=%d (%d messages) to chat_id=%d",
            snapshot.instance_id, snapshot.current_round,
            snapshot.guild_id, len(undeployed_rows),
            len(messages), self._auto_forward_chat_id,
          )
        except Exception:
          log.exception(
            "Failed to format or forward auto-summary; cache was still "
            "updated successfully."
          )      

# ----------------------------------------------------------------------------
# Replacement of `_notify_officers`
# ----------------------------------------------------------------------------
 
async def _notify_officers(
    self,
    messages,
    *,
    guild_id: Optional[str] = None,
    undeployed_rows: tuple = (),
    guild_name_for_cache: str = "",
) -> None:
    """
    Send one or more messages to the officers' chat via the PTB Bot.
 
    If `guild_id` is provided AND `undeployed_rows` is non-empty, we
    attach inline buttons to the LAST message ("Send DMs", "Publish to
    channel") and cache the undeployed list keyed by the resulting
    Telegram message_id so the callback handlers can act on it.
 
    Buttons are attached only to the last message of a multi-message
    response because:
      - Officers see the buttons in context with the displayed list.
      - Adding buttons to each part would duplicate the action targets.
      - The current minimal format always produces one message, but
        the API contract handles future multi-message cases cleanly.
 
    Args:
      messages: str or list[str] from format_auto_summary.
      guild_id: snap.guild_id (C3PO/CG id). When None, no buttons attached.
      undeployed_rows: tuple of _UndeployedRow from auto_summary_undeployed.
        Empty means no buttons (and no cache write).
      guild_name_for_cache: snap.guild_name, denormalized into the cache
        so callbacks don't need to re-read the snapshot.
    """
    if isinstance(messages, str):
        messages = [messages]
 
    attach_buttons = bool(guild_id) and bool(undeployed_rows)
 
    for i, text in enumerate(messages):
        if not text.strip():
            continue
 
        is_last = (i == len(messages) - 1)
        reply_markup = None
        if attach_buttons and is_last:
            reply_markup = _build_undeployed_keyboard(guild_id)
 
        try:
            sent = await self._ptb_bot.send_message(
                chat_id=self._auto_forward_chat_id,
                text=text,
                parse_mode="Markdown",
                disable_web_page_preview=True,
                reply_markup=reply_markup,
            )
        except TelegramError as e:
            log.warning(
                "Telegram send_message failed (chat_id=%d, msg %d/%d): %s",
                self._auto_forward_chat_id, i + 1, len(messages), e,
            )
            continue
 
        # If this is the last message AND we attached buttons, cache
        # the undeployed list keyed by the sent message_id so the
        # callbacks can act on it later.
        if attach_buttons and is_last and sent is not None:
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
 
 
def _build_undeployed_keyboard(guild_id: str) -> "InlineKeyboardMarkup":
    """
    Two buttons: send DMs, publish to channel. Same row.
 
    callback_data encodes the guild_id so the handler doesn't need
    user_data session state (which would tie buttons to whoever sent
    the auto-summary, but the bot — not a user — sent it).
 
    Callback prefixes are namespaced with `tbu` (TB undeployed) to
    avoid collision with the existing `tickets*` callbacks.
    """
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup  # local import to avoid moving file-level imports
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
        ptb_bot_data=application.bot_data,
        ptb_bot=application.bot,
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
