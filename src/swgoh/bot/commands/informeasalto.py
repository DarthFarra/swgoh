from __future__ import annotations

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

from ..keyboards.guild_select import make_keyboard_guilds
from ..security import validate_guild_id, CallbackValidationError
from ..services.sheets import (
    open_ss,
    usuarios_guilds_for_user,
    resolve_label_name_rote_by_id,
    user_has_leadership_role,
    get_usernames_for_members,
)
from ..services.raid_report import (
    read_raid_history_rows,
    read_raid_thresholds,
    render_raid_report,
)


def _split_message(message: str, limit: int = 4000) -> list[str]:
    if len(message) <= limit:
        return [message]
    parts: list[str] = []
    remaining = message
    while remaining:
        cut = remaining.rfind("\n", 0, limit + 1)
        if cut <= 0:
            cut = limit
        parts.append(remaining[:cut])
        remaining = remaining[cut:].lstrip("\n")
    return parts


async def _send_report(update: Update, guild_id: str) -> None:
    ss = open_ss()
    _label, guild_name, _rote = resolve_label_name_rote_by_id(ss, guild_id)
    rows = read_raid_history_rows(ss, guild_id, guild_name)
    if not rows:
        await update.message.reply_text(
            "No hay datos suficientes en raid_history para generar el informe."
        )
        return

    names = sorted({row.member_name for row in rows if row.member_name})
    usernames = get_usernames_for_members(ss, guild_name, names)
    try:
        threshold_high, threshold_low = read_raid_thresholds(
            ss, guild_id, guild_name
        )
    except ValueError as exc:
        await update.message.reply_text(f"No se puede generar el informe: {exc}")
        return
    report = render_raid_report(
        guild_name,
        rows,
        usernames,
        threshold_high,
        threshold_low,
    )
    if not report:
        await update.message.reply_text(
            "No hay resultados de asalto registrados para este gremio."
        )
        return

    for part in _split_message(report):
        await update.message.reply_text(part)


async def cmd_informeasalto(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ss = open_ss()
    user_id = update.effective_user.id
    guilds = usuarios_guilds_for_user(ss, user_id)
    leadership = [
        (label, guild_id, guild_name)
        for label, guild_id, guild_name in guilds
        if user_has_leadership_role(ss, user_id, guild_name)
    ]
    if not leadership:
        await update.message.reply_text(
            "No tienes permisos de Oficial o Líder en ningún gremio."
        )
        return

    if len(leadership) == 1:
        _label, guild_id, _guild_name = leadership[0]
        await _send_report(update, guild_id)
        return

    await update.message.reply_text(
        "Elige el gremio para consultar el último asalto:",
        reply_markup=make_keyboard_guilds(
            [(label, guild_id) for label, guild_id, _guild_name in leadership],
            "raidreport",
        ),
    )


async def cb_informeasalto(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    if not data.startswith("raidreport:"):
        return

    raw_guild_id = data.split(":", 1)[1]
    ss = open_ss()
    user_id = query.from_user.id
    guilds = usuarios_guilds_for_user(ss, user_id)
    known_ids = {guild_id for _label, guild_id, _guild_name in guilds}
    try:
        guild_id = validate_guild_id(raw_guild_id, known_ids)
    except CallbackValidationError:
        await query.edit_message_text("❌ Gremio no válido.")
        return

    _label, guild_name, _rote = resolve_label_name_rote_by_id(ss, guild_id)
    if not user_has_leadership_role(ss, user_id, guild_name):
        await query.edit_message_text("❌ No tienes permisos de Oficial o Líder.")
        return

    rows = read_raid_history_rows(ss, guild_id, guild_name)
    if not rows:
        await query.edit_message_text(
            "No hay datos suficientes en raid_history para generar el informe."
        )
        return

    names = sorted({row.member_name for row in rows if row.member_name})
    usernames = get_usernames_for_members(ss, guild_name, names)
    try:
        threshold_high, threshold_low = read_raid_thresholds(
            ss, guild_id, guild_name
        )
    except ValueError as exc:
        await query.edit_message_text(f"No se puede generar el informe: {exc}")
        return
    report = render_raid_report(
        guild_name,
        rows,
        usernames,
        threshold_high,
        threshold_low,
    )
    if not report:
        await query.edit_message_text(
            "No hay resultados de asalto registrados para este gremio."
        )
        return

    parts = _split_message(report)
    await query.edit_message_text(parts[0])
    for part in parts[1:]:
        await context.bot.send_message(chat_id=query.message.chat_id, text=part)


def get_handlers():
    return [
        CommandHandler("informeasalto", cmd_informeasalto),
        CallbackQueryHandler(cb_informeasalto, pattern=r"^raidreport:"),
    ]
