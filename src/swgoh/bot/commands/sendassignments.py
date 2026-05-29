# src/swgoh/bot/commands/sendassignments.py
"""
/sendassignments command.

Allows officers and guild leaders to manually trigger the daily assignment
sender for their guild.

Flow:
  1. Officer runs /sendassignments
  2. Bot checks phase logic — if no active phase today, replies and stops.
  3. Bot shows inline keyboard with guilds the officer belongs to.
  4. Officer selects a guild.
  5. Bot shows confirmation message: phase, guild, estimated player count.
  6. Officer confirms → assignments are sent. Officer cancels → aborted.

Security:
  - Only users with role Lider/Oficial in at least one guild can proceed.
  - Guild selection is validated against the officer's authorised guilds.
  - Belt-and-suspenders role check on the resolved guild before sending.
  - Session stores the confirmed guild_id and phase so the confirmation
    callback cannot be replayed with a different guild.
"""
from __future__ import annotations

import asyncio
import logging

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

from ..services.sheets import (
    open_ss,
    resolve_label_name_rote_by_id,
)
from ..services.sheets_io import read_values_cached
from ..services.auth import user_authorized_guilds, user_has_role_in_guild
from ..keyboards.guild_select import make_keyboard_guilds
from ..security import (
    session_set,
    session_get,
    session_clear,
    validate_guild_id,
    CallbackValidationError,
)
from ...bot.jobs.send_assignments_daily import (
    obtener_fase_actual,
    run as run_send_assignments,
    _read_all_values,
    SHEET_USERS,
    SHEET_GUILDS,
)

log = logging.getLogger(__name__)

# Session keys — namespaced to avoid collisions with other commands
_S_GID   = "sendassign_guild_id"
_S_LABEL = "sendassign_label"
_S_FASE  = "sendassign_fase"


# ---------------------------------------------------------------------------
# Step 1 — /sendassignments entry point
# ---------------------------------------------------------------------------

async def cmd_sendassignments(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id

    # Check phase first — no point asking guild selection if there's nothing to send.
    fase = obtener_fase_actual()
    if not fase:
        await update.message.reply_text(
            "ℹ️ No hay fase activa hoy.\n\n"
            "Las asignaciones solo se envían en semanas ISO pares, de lunes a sábado."
        )
        return

    ss   = open_ss()
    opts = user_authorized_guilds(ss, user_id)  # [(label, gid)]

    if not opts:
        await update.message.reply_text(
            "❌ No tienes permisos para enviar asignaciones "
            "(se requiere rol Lider u Oficial)."
        )
        return

    # Store phase in session so the confirmation callback uses the same value.
    session_set(context, user_id, _S_FASE, fase)

    if len(opts) == 1:
        # Only one guild — skip keyboard, go straight to confirmation.
        label, gid = opts[0]
        session_set(context, user_id, _S_GID,   gid)
        session_set(context, user_id, _S_LABEL, label)
        await update.message.reply_text(
            **_confirmation_message(label, fase, gid),
        )
        return

    await update.message.reply_text(
        f"Fase activa: *{fase}*\n\nElige el gremio al que enviar las asignaciones:",
        parse_mode="Markdown",
        reply_markup=make_keyboard_guilds(opts, "sendassign"),
    )


# ---------------------------------------------------------------------------
# Step 2 — Guild selected → show confirmation
# ---------------------------------------------------------------------------

async def cb_sendassign_guild(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("sendassign:"):
        return

    raw_gid = data.split(":", 1)[1]
    user_id = q.from_user.id
    ss      = open_ss()

    authorized = user_authorized_guilds(ss, user_id)
    known_ids  = {gid for _, gid in authorized}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    if not user_has_role_in_guild(ss, user_id, gid):
        await q.edit_message_text(
            "❌ No tienes permisos para enviar asignaciones en este gremio."
        )
        return

    label, _, _ = resolve_label_name_rote_by_id(ss, gid)
    fase = session_get(context, user_id, _S_FASE)
    if not fase:
        # Session expired between steps — re-check phase.
        fase = obtener_fase_actual()
        if not fase:
            await q.edit_message_text("ℹ️ No hay fase activa hoy.")
            return

    session_set(context, user_id, _S_GID,   gid)
    session_set(context, user_id, _S_LABEL, label)
    session_set(context, user_id, _S_FASE,  fase)

    await q.edit_message_text(**_confirmation_message(label, fase, gid))


# ---------------------------------------------------------------------------
# Step 3a — Confirmed → send assignments
# ---------------------------------------------------------------------------

async def cb_sendassign_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("sendassignconfirm:"):
        return

    raw_gid = data.split(":", 1)[1]
    user_id = q.from_user.id
    ss      = open_ss()

    # Re-validate everything from session — never trust callback data alone.
    stored_gid = session_get(context, user_id, _S_GID)
    label      = session_get(context, user_id, _S_LABEL, "el gremio")
    fase       = session_get(context, user_id, _S_FASE)

    # If session data is missing or mismatched, reject.
    if not stored_gid or stored_gid != raw_gid:
        await q.edit_message_text(
            "❌ Sesión expirada o no válida. Usa /sendassignments de nuevo."
        )
        return

    # Belt-and-suspenders: re-authorise on the live guild list.
    authorized = user_authorized_guilds(ss, user_id)
    known_ids  = {gid for _, gid in authorized}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        session_clear(context, user_id)
        return

    if not user_has_role_in_guild(ss, user_id, gid):
        await q.edit_message_text(
            "❌ No tienes permisos para enviar asignaciones en este gremio."
        )
        session_clear(context, user_id)
        return

    # Re-check phase hasn't changed since the officer started the flow.
    fase_now = obtener_fase_actual()
    if not fase_now:
        await q.edit_message_text("ℹ️ Ya no hay fase activa. No se enviaron asignaciones.")
        session_clear(context, user_id)
        return

    if fase_now != fase:
        await q.edit_message_text(
            f"⚠️ La fase cambió desde que iniciaste el proceso "
            f"(antes: {fase}, ahora: {fase_now}). "
            "Usa /sendassignments de nuevo para confirmar."
        )
        session_clear(context, user_id)
        return

    await q.edit_message_text(
        f"⏳ Enviando asignaciones de fase *{fase}* para *{label}*…",
        parse_mode="Markdown",
    )
    session_clear(context, user_id)

    try:
        sent, skipped = await asyncio.to_thread(
            _run_for_guild, gid, fase
        )
        if sent == 0 and skipped == 0:
            await q.edit_message_text(
                f"ℹ️ No hay jugadores registrados en *{label}* "
                f"con asignaciones para la fase *{fase}*.",
                parse_mode="Markdown",
            )
        else:
            await q.edit_message_text(
                f"✅ Asignaciones enviadas para *{label}* — Fase *{fase}*\n\n"
                f"Enviadas: {sent} · Omitidas: {skipped}",
                parse_mode="Markdown",
            )
    except Exception:
        log.exception(
            "Unexpected error in cb_sendassign_confirm for guild %s phase %s",
            gid, fase,
        )
        await q.edit_message_text(
            f"❌ Error inesperado enviando asignaciones para *{label}*.\n"
            "Revisa los logs para más detalles.",
            parse_mode="Markdown",
        )


# ---------------------------------------------------------------------------
# Step 3b — Cancelled
# ---------------------------------------------------------------------------

async def cb_sendassign_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    if not (q.data or "").startswith("sendassigncancel:"):
        return

    user_id = q.from_user.id
    session_clear(context, user_id)
    await q.edit_message_text("❌ Envío de asignaciones cancelado.")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _confirmation_message(label: str, fase: str, gid: str) -> dict:
    """
    Returns kwargs for reply_text / edit_message_text showing the
    confirmation prompt with Confirm / Cancel buttons.
    """
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "✅ Confirmar envío",
                callback_data=f"sendassignconfirm:{gid}",
            ),
            InlineKeyboardButton(
                "❌ Cancelar",
                callback_data=f"sendassigncancel:{gid}",
            ),
        ]
    ])
    return {
        "text": (
            f"📋 *Confirmar envío de asignaciones*\n\n"
            f"Gremio: *{label}*\n"
            f"Fase: *{fase}*\n\n"
            "Se enviará un mensaje privado a cada jugador registrado "
            "con sus asignaciones para esta fase.\n\n"
            "¿Confirmas el envío?"
        ),
        "parse_mode": "Markdown",
        "reply_markup": kb,
    }


def _run_for_guild(guild_id: str, fase: str) -> tuple[int, int]:
    """
    Sends assignments for a single guild and a specific phase.

    This is the surgical version of send_assignments_daily.run() — it
    targets one guild only instead of all guilds, and uses the phase
    passed in rather than re-computing it (which keeps the behaviour
    consistent with what the officer confirmed in the UI).

    Returns (sent, skipped).
    """
    import time as _time
    from collections import defaultdict
    from ...bot.jobs.send_assignments_daily import (
        _read_all_values,
        _hmap,
        _find_col,
        _gv_by_idx,
        HEADERS_ASSIGN,
        HEADERS_USUARIOS,
        AssignIndex,
        _tg_send_message,
        TELEGRAM_BOT_TOKEN,
        SHEET_USERS,
        SHEET_GUILDS,
    )
    from ... import config as cfg
    from ...sheets import spreadsheet as open_spreadsheet

    ss = open_spreadsheet()

    # Read USUARIOS — filter to this guild only
    u_headers, u_rows = read_values_cached(ss, SHEET_USERS)
    if not u_rows:
        return 0, 0

    uhm       = _hmap(u_headers)
    idx_gname = _find_col(uhm, HEADERS_USUARIOS["guild_name"])
    idx_chat  = _find_col(uhm, HEADERS_USUARIOS["chat_id"])
    idx_uid   = _find_col(uhm, HEADERS_USUARIOS["user_id"])
    idx_alias = _find_col(uhm, HEADERS_USUARIOS["alias"])

    # Read GUILDS to resolve guild_id → guild_name and ROTE sheet
    g_headers, g_rows = read_values_cached(ss, SHEET_GUILDS)
    ghm            = _hmap(g_headers)
    idx_gid_col    = _find_col(ghm, ["Guild Id", "guild_id", "guild id"])
    idx_guild_name = _find_col(ghm, ["Guild Name", "guild_name", "gremio"])
    idx_rote       = _find_col(ghm, ["ROTE"])

    guild_name: str = ""
    sheet_name: str = cfg.DEFAULT_ROTE_SHEET
    for r in g_rows:
        gid_cell = _gv_by_idx(r, idx_gid_col)
        if gid_cell == guild_id:
            guild_name = _gv_by_idx(r, idx_guild_name)
            rote       = _gv_by_idx(r, idx_rote)
            if rote:
                sheet_name = rote
            break

    if not guild_name:
        log.warning("_run_for_guild: guild_id %s not found in Guilds sheet.", guild_id)
        return 0, 0

    # Filter users to this guild
    users = []
    for r in u_rows:
        g  = _gv_by_idx(r, idx_gname)
        ch = _gv_by_idx(r, idx_chat)
        ui = _gv_by_idx(r, idx_uid)
        al = _gv_by_idx(r, idx_alias)
        if g == guild_name and ch and ui:
            users.append((ch, ui, al))

    if not users:
        return 0, 0

    # Read ROTE assignments
try:
        a_headers, a_rows = read_values_cached(ss, sheet_name)
    except Exception as e:
        log.warning("Cannot open ROTE sheet '%s': %s", sheet_name, e)
        return 0, len(users)

    if not a_rows:
        return 0, len(users)

    ahm  = _hmap(a_headers)
    idxs = {
        "fase":      _find_col(ahm, HEADERS_ASSIGN["fase"]),
        "planeta":   _find_col(ahm, HEADERS_ASSIGN["planeta"]),
        "operacion": _find_col(ahm, HEADERS_ASSIGN["operacion"]),
        "personaje": _find_col(ahm, HEADERS_ASSIGN["personaje"]),
        "user_id":   _find_col(ahm, HEADERS_ASSIGN["user_id"]),
        "jugador":   _find_col(ahm, HEADERS_ASSIGN["jugador"]),
    }

    if min(idxs.values()) == -1:
        log.warning("Missing required columns in ROTE sheet '%s'.", sheet_name)
        return 0, len(users)

    assign_index = AssignIndex(sheet_name, idxs, a_rows, fase)

    sent = skipped = 0
    for chat_id, user_id, alias in users:
        try:
            msg = assign_index.build_message_for(guild_name, user_id, alias)
            if not msg:
                skipped += 1
                continue
            _tg_send_message(TELEGRAM_BOT_TOKEN, chat_id, msg, parse_mode="Markdown")
            sent += 1
            _time.sleep(0.05)
        except Exception:
            log.warning("Failed to send to chat %s", chat_id, exc_info=True)
            skipped += 1

    return sent, skipped


# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------

def get_handlers():
    return [
        CommandHandler("sendassignments", cmd_sendassignments),
        CallbackQueryHandler(cb_sendassign_guild,   pattern=r"^sendassign:[^:]+$"),
        CallbackQueryHandler(cb_sendassign_confirm, pattern=r"^sendassignconfirm:"),
        CallbackQueryHandler(cb_sendassign_cancel,  pattern=r"^sendassigncancel:"),
    ]
