# src/swgoh/bot/commands/refreshcache.py
"""
Admin command: /refreshcache

Drops all cached sheet reads. The next read of each sheet will hit
the API directly and repopulate the cache with fresh data.

When to use this:
  - After manually editing a ROTE/assignment sheet
  - After modifying OmicronPriorities
  - After /syncdata or /syncguild if the result is needed immediately
    (otherwise TTLs catch up on their own)

Authorisation:
  Limited to chats in SYNC_DATA_ALLOWED_CHATS — same admin set used
  by /syncdata and /syncguild. Non-admin invocations are silently
  ignored so we don't leak the existence of admin commands.
"""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from ... import config as cfg
from ..services import sheets_cache

log = logging.getLogger(__name__)


def _is_admin_chat(chat_id: int) -> bool:
    """Match against the admin chat allowlist (str-typed in config)."""
    allowed_raw = getattr(cfg, "SYNC_DATA_ALLOWED_CHATS", set()) or set()
    allowed_ints = set()
    for x in allowed_raw:
        try:
            allowed_ints.add(int(x))
        except (TypeError, ValueError):
            log.warning("Invalid chat id in SYNC_DATA_ALLOWED_CHATS: %r", x)
    return chat_id in allowed_ints


async def cmd_refreshcache(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Defensive — anonymous/automated updates can have no user/chat.
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    chat_id = update.effective_chat.id
    if not _is_admin_chat(chat_id):
        # Silent no-op for non-admins. Same pattern as /syncdata.
        log.info(
            "refreshcache: non-admin chat %s tried to invoke; ignoring",
            chat_id,
        )
        return

    before = sheets_cache.get_stats()
    dropped = sheets_cache.invalidate_all()

    log.info(
        "refreshcache: invoked by user_id=%s chat_id=%s; "
        "dropped=%d (hits_before=%d misses_before=%d)",
        update.effective_user.id, chat_id, dropped,
        before["hits"], before["misses"],
    )

    # Reminder text is intentional — turning "good code" into "good ops"
    # means making the convention visible every time the command runs.
    await update.message.reply_text(
        f"Caché de hojas vaciada. {dropped} entrada(s) eliminada(s).\n"
        f"Estadísticas previas: {before['hits']} hits, "
        f"{before['misses']} misses.\n\n"
        f"Recordatorio: ejecuta /refreshcache después de:\n"
        f"• Editar manualmente cualquier hoja de asignaciones ROTE\n"
        f"• Modificar OmicronPriorities\n"
        f"• Ejecutar /syncdata o /syncguild si el cambio es urgente"
    )


def get_handlers():
    return [
        CommandHandler("refreshcache", cmd_refreshcache),
    ]
