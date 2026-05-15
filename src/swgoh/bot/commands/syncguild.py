# src/swgoh/bot/commands/syncguild.py
from __future__ import annotations

import os
import asyncio
import logging

import requests
from telegram import Update
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

from ..services.sheets import open_ss, already_synced_today, resolve_label_name_rote_by_id
from ..services.auth import user_authorized_guilds, user_has_role_in_guild
from ..keyboards.guild_select import make_keyboard_guilds
from ..security import (
    rate_limit,
    validate_guild_id,
    CallbackValidationError,
)

log = logging.getLogger(__name__)

APPS_SCRIPT_URL     = os.getenv("APPS_SCRIPT_WEBHOOK_URL")
APPS_SCRIPT_TIMEOUT = int(os.getenv("APPS_SCRIPT_TIMEOUT", "350"))


@rate_limit(cooldown_seconds=30)
async def cmd_syncguild(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def cb_syncguild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("syncguild:"):
        return

    raw_gid = data.split(":", 1)[1]
    user_id = q.from_user.id
    ss      = open_ss()

    # Validate guild_id against guilds the user is actually authorized for
    authorized = user_authorized_guilds(ss, user_id)
    known_ids  = {gid for _, gid in authorized}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    # Belt-and-suspenders: role check on the resolved guild
    if not user_has_role_in_guild(ss, user_id, gid):
        await q.edit_message_text("❌ No tienes permisos para sincronizar este gremio.")
        return

    if already_synced_today(ss, gid):
        label, _, _ = resolve_label_name_rote_by_id(ss, gid)
        await q.edit_message_text(f"ℹ️ {label} ya se sincronizó hoy.")
        return

    label, _, _ = resolve_label_name_rote_by_id(ss, gid)

    if not APPS_SCRIPT_URL:
        await q.edit_message_text("❌ Error: APPS_SCRIPT_WEBHOOK_URL no configurado.")
        log.error("APPS_SCRIPT_WEBHOOK_URL is not set.")
        return

    await q.edit_message_text(
        f"⏳ Sincronizando {label}…\n\n_Esto puede tardar varios minutos._",
        parse_mode="Markdown",
    )

    try:
        result = await _call_apps_script(gid)
        if result.get("status") == "success":
            summary = result.get("result", "Completado")
            await q.edit_message_text(
                f"✅ Sincronización completada para {label}.\n\n`{summary}`",
                parse_mode="Markdown",
            )
        else:
            error_msg = result.get("message", "Error desconocido")
            await q.edit_message_text(f"❌ Error sincronizando {label}.\n\n{error_msg}")

    except asyncio.TimeoutError:
        await q.edit_message_text(
            f"⏱️ {label}: La sincronización está tomando más tiempo del esperado.\n\n"
            "El proceso continúa en segundo plano. Verifica los datos en unos minutos."
        )
        log.warning("Timeout waiting for Apps Script response for guild %s", gid)

    except requests.RequestException as e:
        await q.edit_message_text(f"❌ Error de conexión sincronizando {label}.")
        log.error("Request error calling Apps Script for guild %s: %s", gid, e)

    except Exception as e:
        await q.edit_message_text(f"❌ Error inesperado sincronizando {label}.")
        log.exception("Unexpected error in cb_syncguild for guild %s", gid)


async def _call_apps_script(guild_id: str) -> dict:
    """
    POST to the Apps Script webhook with the guild_id to sync.
    Runs in a thread pool to avoid blocking the asyncio event loop.
    """
    payload = {"action": "sync_guilds", "filterGuildIds": [guild_id]}
    loop    = asyncio.get_event_loop()

    def _sync_request() -> dict:
        response = requests.post(
            APPS_SCRIPT_URL,
            json=payload,
            timeout=APPS_SCRIPT_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()

    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, _sync_request),
            timeout=APPS_SCRIPT_TIMEOUT,
        )
    except asyncio.TimeoutError:
        log.warning("Apps Script timeout for guild %s", guild_id)
        raise
    except requests.Timeout:
        raise asyncio.TimeoutError()


def get_handlers():
    return [
        CommandHandler("syncguild", cmd_syncguild),
        CallbackQueryHandler(cb_syncguild, pattern=r"^syncguild:"),
    ]
