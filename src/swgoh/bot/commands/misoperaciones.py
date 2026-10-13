# src/swgoh/bot/commands/misoperaciones.py
from __future__ import annotations

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

from ..services.sheets import (
    open_ss,
    usuarios_registrations_for_user,
    list_phases_in_rote,
    render_ops_for_alias_phase_grouped,
)
from ..security import (
    session_set,
    validate_phase,
    CallbackValidationError,
)

_S_PHASES = "myops_phases"


def _phase_keyboard(phases):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(text=f"Fase {phase}", callback_data=f"myopsphase:{phase}")]
        for phase in phases
    ])


def _all_phases(ss, registrations):
    phases = set()
    for _label, _gid, _gname, rote_sheet, _alias in registrations:
        phases.update(list_phases_in_rote(ss, rote_sheet))

    def phase_key(value):
        try:
            return (0, int(value))
        except (TypeError, ValueError):
            return (1, str(value).lower())

    return sorted(phases, key=phase_key)


def _operations_for_players(ss, registrations, phase):
    messages = []
    for label, _gid, gname, rote_sheet, alias in registrations:
        body = render_ops_for_alias_phase_grouped(ss, rote_sheet, alias, phase)
        if "No tienes asignaciones" in body:
            continue
        messages.append(f"Asignaciones de *{alias}* — *{label or gname}* (Fase {phase})\n\n{body}")
    return messages


async def _show_phase_picker(target, context, user_id, phases, registrations):
    session_set(context, user_id, _S_PHASES, phases)
    aliases = ", ".join(alias for *_rest, alias in registrations)
    await target.edit_message_text(
        f"Elige la fase para {aliases}:",
        reply_markup=_phase_keyboard(phases),
    )


async def cmd_misoperaciones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss     = open_ss()
    user_id = update.effective_user.id
    registrations = usuarios_registrations_for_user(ss, user_id)
    if not registrations:
        await update.message.reply_text("No estás registrado en ningún gremio.")
        return

    phases = _all_phases(ss, registrations)
    if not phases:
        await update.message.reply_text("❌ No hay fases en tus hojas ROTE.")
        return
    session_set(context, user_id, _S_PHASES, phases)
    aliases = ", ".join(alias for *_rest, alias in registrations)
    await update.message.reply_text(
        f"Elige la fase para {aliases}:",
        reply_markup=_phase_keyboard(phases),
    )


async def cb_myops(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("myops:"):
        return

    ss = open_ss()
    user_id = q.from_user.id
    registrations = usuarios_registrations_for_user(ss, user_id)
    phases = _all_phases(ss, registrations)
    if not phases:
        await q.edit_message_text("❌ No hay fases en tus hojas ROTE.")
        return
    await _show_phase_picker(q, context, user_id, phases, registrations)


async def cb_myops_phase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("myopsphase:"):
        return

    try:
        raw_phase = data.split(":")[-1]
    except ValueError:
        await q.edit_message_text("❌ Datos de fase no válidos.")
        return

    user_id = q.from_user.id
    ss      = open_ss()

    registrations = usuarios_registrations_for_user(ss, user_id)
    known_phases = _all_phases(ss, registrations)
    try:
        phase = validate_phase(raw_phase, known_phases)
    except CallbackValidationError:
        await q.edit_message_text("❌ Fase no válida.")
        return

    if not registrations:
        await q.edit_message_text("❌ No estás registrado en ningún gremio.")
        return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text="Cambiar fase", callback_data="myopschoosephase")]
    ])
    messages = _operations_for_players(ss, registrations, phase)
    if not messages:
        await q.edit_message_text(
            f"No tienes asignaciones en esta fase.", reply_markup=kb
        )
        return

    await q.edit_message_text(messages[0], reply_markup=kb)
    for message in messages[1:]:
        await q.message.reply_text(message, reply_markup=kb)


async def cb_myops_choosephase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("myopschoosephase"):
        return

    user_id = q.from_user.id
    ss      = open_ss()
    registrations = usuarios_registrations_for_user(ss, user_id)
    if not registrations:
        await q.edit_message_text("❌ No estás registrado en ningún gremio.")
        return

    phases = _all_phases(ss, registrations)
    if not phases:
        await q.edit_message_text("❌ No hay fases en tus hojas ROTE.")
        return
    await _show_phase_picker(q, context, user_id, phases, registrations)

def get_handlers():
    return [
        CommandHandler("misoperaciones", cmd_misoperaciones),
        CallbackQueryHandler(cb_myops,            pattern=r"^myops:"),
        CallbackQueryHandler(cb_myops_phase,       pattern=r"^myopsphase:"),
        CallbackQueryHandler(cb_myops_choosephase, pattern=r"^myopschoosephase(?::.*)?$"),
    ]
