from __future__ import annotations

import asyncio

from telegram import Update
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

from ..keyboards.guild_select import make_keyboard_guilds
from ..security import validate_guild_id, CallbackValidationError
from ..services.gt_prep import (
    evaluate_guild,
    evaluate_player,
    render_guild_prep,
    render_player_prep,
)
from ..services.sheets import (
    open_ss,
    player_id_for_alias,
    resolve_label_name_rote_by_id,
    user_has_leadership_role,
    usuarios_guilds_for_user,
    usuarios_registrations_for_user,
)


def _split_html(message: str, limit: int = 4000) -> list[str]:
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


async def cmd_prepgt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ss = open_ss()
    registrations = usuarios_registrations_for_user(
        ss,
        update.effective_user.id,
    )
    if not registrations:
        await update.message.reply_text(
            "No estás registrado en ningún jugador.",
        )
        return

    preps = []
    for _label, guild_id, guild_name, _rote, alias in registrations:
        player_id = player_id_for_alias(ss, guild_name, alias)
        if not player_id:
            continue
        prep = await asyncio.to_thread(
            evaluate_player,
            ss,
            player_id,
            alias,
        )
        preps.append(prep)

    if not preps:
        await update.message.reply_text(
            "No pude resolver ningún jugador registrado.",
        )
        return

    for prep in preps:
        report = render_player_prep(prep, telegram=True)
        for part in _split_html(report):
            await update.message.reply_text(part, parse_mode="HTML")


async def cmd_prepgtgremio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ss = open_ss()
    guilds = usuarios_guilds_for_user(ss, update.effective_user.id)
    leadership = [
        (label, guild_id, guild_name)
        for label, guild_id, guild_name in guilds
        if user_has_leadership_role(ss, update.effective_user.id, guild_name)
    ]
    if not leadership:
        await update.message.reply_text(
            "Este comando requiere rol Líder u Oficial.",
        )
        return

    if len(leadership) == 1:
        _label, guild_id, guild_name = leadership[0]
        await _send_guild_prep(update, ss, guild_id, guild_name)
        return

    await update.message.reply_text(
        "Elige el gremio:",
        reply_markup=make_keyboard_guilds(
            [(label, guild_id) for label, guild_id, _guild_name in leadership],
            "prepgtgremio",
        ),
    )


async def _send_guild_prep(update, ss, guild_id: str, guild_name: str) -> None:
    preps = await asyncio.to_thread(evaluate_guild, ss, guild_name)
    report = render_guild_prep(guild_name, preps, telegram=True)
    for part in _split_html(report):
        if update.callback_query:
            await update.callback_query.edit_message_text(
                part,
                parse_mode="HTML",
            )
        else:
            await update.message.reply_text(part, parse_mode="HTML")


async def cb_prepgtgremio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    if not data.startswith("prepgtgremio:"):
        return

    raw_guild_id = data.split(":", 1)[1]
    ss = open_ss()
    guilds = usuarios_guilds_for_user(ss, query.from_user.id)
    known_ids = {guild_id for _label, guild_id, _guild_name in guilds}
    try:
        guild_id = validate_guild_id(raw_guild_id, known_ids)
    except CallbackValidationError:
        await query.edit_message_text("❌ Gremio no válido.")
        return

    _label, guild_name, _rote = resolve_label_name_rote_by_id(ss, guild_id)
    if not user_has_leadership_role(ss, query.from_user.id, guild_name):
        await query.edit_message_text("❌ No tienes permisos de Oficial o Líder.")
        return
    await _send_guild_prep(update, ss, guild_id, guild_name)


def get_handlers():
    return [
        CommandHandler("prepgt", cmd_prepgt),
        CommandHandler("prepgtgremio", cmd_prepgtgremio),
        CallbackQueryHandler(
            cb_prepgtgremio,
            pattern=r"^prepgtgremio:",
        ),
    ]
