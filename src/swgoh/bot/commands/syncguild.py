# src/swgoh/bot/commands/syncguild.py
"""
/syncguild command.

Previously routed guild sync through an Apps Script webhook to work around
Railway execution time limits. That dependency has been removed:
  - sync_runner.run_sync_guilds_once() runs the sync directly in a thread
    via asyncio.to_thread(), keeping the event loop unblocked.
  - No external HTTP call to a third-party webhook.
  - No APPS_SCRIPT_WEBHOOK_URL env var required.
"""
from __future__ import annotations

import asyncio
import logging

from telegram import Update
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

from ..services.sheets import open_ss, already_synced_today, resolve_label_name_rote_by_id
from ..services.auth import user_authorized_guilds, user_has_role_in_guild
from ..keyboards.guild_select import make_keyboard_guilds
from ..security import validate_guild_id, CallbackValidationError
from ..services.sync_runner import run_sync_guilds_once

log = logging.getLogger(__name__)


async def cmd_syncguild(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ss   = open_ss()
    opts = user_authorized_guilds(ss, update.effective_user.id)  # [(label, gid)]

    if not opts:
        await update.message.reply_text(
            "No tienes permisos para sincronizar (se requiere rol Lider u Oficial)."
        )
        return

    await update.message.reply_text(
        "Elige el gremio a sincronizar:",
        reply_markup=make_keyboard_guilds(opts, "syncguild"),
    )


async def cb_syncguild(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("syncguild:"):
        return

    raw_gid = data.split(":", 1)[1]
    user_id = q.from_user.id
    ss      = open_ss()

    # Validate guild_id against guilds the user is authorised for.
    authorized = user_authorized_guilds(ss, user_id)
    known_ids  = {gid for _, gid in authorized}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    # Belt-and-suspenders: explicit role check on the resolved guild.
    if not user_has_role_in_guild(ss, user_id, gid):
        await q.edit_message_text("❌ No tienes permisos para sincronizar este gremio.")
        return

    if already_synced_today(ss, gid):
        label, _, _ = resolve_label_name_rote_by_id(ss, gid)
        await q.edit_message_text(f"ℹ️ {label} ya se sincronizó hoy.")
        return

    label, _, _ = resolve_label_name_rote_by_id(ss, gid)
    await q.edit_message_text(
        f"⏳ Sincronizando {label}…\n\n_Esto puede tardar varios minutos._",
        parse_mode="Markdown",
    )

    try:
        await run_sync_guilds_once(gid)
        await q.edit_message_text(f"✅ Sincronización completada para {label}.")

    except asyncio.CancelledError:
        # PTB shutdown during a long sync — let it propagate.
        raise

    except Exception:
        log.exception("Unexpected error in cb_syncguild for guild %s", gid)
        await q.edit_message_text(
            f"❌ Error inesperado sincronizando {label}.\n"
            "Revisa los logs para más detalles."
        )


def get_handlers():
    return [
        CommandHandler("syncguild", cmd_syncguild),
        CallbackQueryHandler(cb_syncguild, pattern=r"^syncguild:"),
    ]
