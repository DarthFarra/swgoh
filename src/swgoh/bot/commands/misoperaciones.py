# src/swgoh/bot/commands/misoperaciones.py
from __future__ import annotations

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

from ..services.sheets import (
    open_ss,
    usuarios_guilds_for_user,
    resolve_label_name_rote_by_id,
    user_alias_for_guild,
    list_phases_in_rote,
    render_ops_for_alias_phase_grouped,
)
from ..keyboards.guild_select import make_keyboard_guilds
from ..security import (
    session_set,
    session_get,
    validate_guild_id,
    validate_phase,
    CallbackValidationError,
)

# Session keys
_S_GID    = "myops_guild_id"
_S_GNAME  = "myops_guild_name"
_S_LABEL  = "myops_label"
_S_ROTE   = "myops_rote_sheet"
_S_PHASES = "myops_phases"


async def cmd_misoperaciones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss     = open_ss()
    guilds = usuarios_guilds_for_user(ss, update.effective_user.id)
    if not guilds:
        await update.message.reply_text("No estás registrado en ningún gremio.")
        return

    if len(guilds) > 1:
        opts = [(label, gid) for label, gid, _gn in guilds]
        await update.message.reply_text(
            "Elige el gremio para ver tus operaciones:",
            reply_markup=make_keyboard_guilds(opts, "myops"),
        )
        return

    label, gid, gname = guilds[0]
    await _ask_phase_for_guild(update, context, ss, gid, gname, label, via_callback=False)


async def cb_myops(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("myops:"):
        return

    raw_gid = data.split(":", 1)[1]
    ss      = open_ss()
    user_id = q.from_user.id

    # Validate guild_id against the user's own registered guilds
    guilds    = usuarios_guilds_for_user(ss, user_id)
    known_ids = {gid for _, gid, _ in guilds}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    label, gname, rote_sheet = resolve_label_name_rote_by_id(ss, gid)

    alias = user_alias_for_guild(ss, user_id, gname)
    if not alias:
        await q.edit_message_text(f"❌ No encuentro tu alias en '{gname}'. ¿Te has registrado?")
        return

    phases = list_phases_in_rote(ss, rote_sheet)
    if not phases:
        await q.edit_message_text(f"❌ No hay fases en la hoja ROTE de {label}.")
        return

    # Store validated guild context and phase whitelist in session
    session_set(context, user_id, _S_GID,    gid)
    session_set(context, user_id, _S_GNAME,  gname)
    session_set(context, user_id, _S_LABEL,  label)
    session_set(context, user_id, _S_ROTE,   rote_sheet)
    session_set(context, user_id, _S_PHASES, phases)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text=f"Fase {p}", callback_data=f"myopsphase:{gid}:{p}")]
        for p in phases
    ])
    await q.edit_message_text(
        f"Elige la fase para {alias} en {label}:",
        reply_markup=kb,
    )


async def cb_myops_phase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("myopsphase:"):
        return

    try:
        _, raw_gid, raw_phase = data.split(":", 2)
    except ValueError:
        await q.edit_message_text("❌ Datos de fase no válidos.")
        return

    user_id = q.from_user.id
    ss      = open_ss()

    # Validate guild_id
    guilds    = usuarios_guilds_for_user(ss, user_id)
    known_ids = {gid for _, gid, _ in guilds}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    label, gname, rote_sheet = resolve_label_name_rote_by_id(ss, gid)

    # Validate phase against the live list (re-fetch if session expired)
    known_phases = session_get(context, user_id, _S_PHASES) or list_phases_in_rote(ss, rote_sheet)
    try:
        phase = validate_phase(raw_phase, known_phases)
    except CallbackValidationError:
        await q.edit_message_text("❌ Fase no válida.")
        return

    alias = user_alias_for_guild(ss, user_id, gname)
    if not alias:
        await q.edit_message_text(f"❌ No encuentro tu alias en '{gname}'. ¿Te has registrado?")
        return

    title = f"Asignaciones de {alias} — {label} (Fase {phase})"
    body  = render_ops_for_alias_phase_grouped(ss, rote_sheet, alias, phase)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text="Cambiar fase", callback_data=f"myopschoosephase:{gid}")]
    ])
    await q.edit_message_text(f"{title}\n\n{body}", reply_markup=kb)


async def cb_myops_choosephase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("myopschoosephase:"):
        return

    raw_gid = data.split(":", 1)[1]
    user_id = q.from_user.id
    ss      = open_ss()

    guilds    = usuarios_guilds_for_user(ss, user_id)
    known_ids = {gid for _, gid, _ in guilds}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    label, gname, rote_sheet = resolve_label_name_rote_by_id(ss, gid)
    alias = user_alias_for_guild(ss, user_id, gname)
    if not alias:
        await q.edit_message_text(f"❌ No encuentro tu alias en '{gname}'. ¿Te has registrado?")
        return

    phases = list_phases_in_rote(ss, rote_sheet)
    if not phases:
        await q.edit_message_text(f"❌ No hay fases en la hoja ROTE de {label}.")
        return

    session_set(context, user_id, _S_PHASES, phases)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text=f"Fase {p}", callback_data=f"myopsphase:{gid}:{p}")]
        for p in phases
    ])
    await q.edit_message_text(f"Elige la fase para {alias} en {label}:", reply_markup=kb)


async def _ask_phase_for_guild(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    ss,
    gid: str,
    gname: str,
    label: str,
    via_callback: bool,
):
    user_id = update.effective_user.id
    alias   = user_alias_for_guild(ss, user_id, gname)
    if not alias:
        msg = f"❌ No encuentro tu alias en '{gname}'. ¿Te has registrado?"
        if via_callback:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    _, _, rote_sheet = resolve_label_name_rote_by_id(ss, gid)
    phases = list_phases_in_rote(ss, rote_sheet)
    if not phases:
        msg = f"❌ No hay fases en la hoja ROTE de {label}."
        if via_callback:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    session_set(context, user_id, _S_GID,    gid)
    session_set(context, user_id, _S_GNAME,  gname)
    session_set(context, user_id, _S_LABEL,  label)
    session_set(context, user_id, _S_ROTE,   rote_sheet)
    session_set(context, user_id, _S_PHASES, phases)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text=f"Fase {p}", callback_data=f"myopsphase:{gid}:{p}")]
        for p in phases
    ])
    text = f"Elige la fase para {alias} en {label}:"
    if via_callback:
        await update.callback_query.edit_message_text(text, reply_markup=kb)
    else:
        await update.message.reply_text(text, reply_markup=kb)


def get_handlers():
    return [
        CommandHandler("misoperaciones", cmd_misoperaciones),
        CallbackQueryHandler(cb_myops,            pattern=r"^myops:"),
        CallbackQueryHandler(cb_myops_phase,       pattern=r"^myopsphase:"),
        CallbackQueryHandler(cb_myops_choosephase, pattern=r"^myopschoosephase:"),
    ]
