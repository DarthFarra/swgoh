# src/swgoh/bot/commands/operacionesjugador.py
from __future__ import annotations

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

from ..services.sheets import (
    open_ss,
    usuarios_guilds_for_user,
    resolve_label_name_rote_by_id,
    list_phases_in_rote,
    render_ops_for_alias_phase_grouped,
    user_has_leadership_role,
    list_players_for_guild,
)
from ..keyboards.guild_select import make_keyboard_guilds
from ..keyboards.player_select import make_keyboard_players
from ..security import (
    session_set,
    session_get,
    validate_guild_id,
    validate_player_name,
    validate_phase,
    CallbackValidationError,
)

# Session keys
_S_GID     = "playerops_guild_id"
_S_GNAME   = "playerops_guild_name"
_S_LABEL   = "playerops_label"
_S_ROTE    = "playerops_rote_sheet"
_S_PLAYER  = "playerops_player"
_S_PHASES  = "playerops_phases"
_S_PLAYERS = "playerops_players"  # whitelist of valid player names


async def cmd_operacionesjugador(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss      = open_ss()
    user_id = update.effective_user.id
    guilds  = usuarios_guilds_for_user(ss, user_id)

    if not guilds:
        await update.message.reply_text("No estás registrado en ningún gremio.")
        return

    leadership_guilds = [
        (label, gid, gname)
        for label, gid, gname in guilds
        if user_has_leadership_role(ss, user_id, gname)
    ]

    if not leadership_guilds:
        await update.message.reply_text(
            "No tienes permisos de Oficial o Líder en ningún gremio."
        )
        return

    if len(leadership_guilds) > 1:
        opts = [(label, gid) for label, gid, _ in leadership_guilds]
        await update.message.reply_text(
            "Elige el gremio para ver operaciones de jugadores:",
            reply_markup=make_keyboard_guilds(opts, "playerops"),
        )
        return

    label, gid, gname = leadership_guilds[0]
    await _ask_player_for_guild(update, context, ss, gid, gname, label, via_callback=False)


async def cb_playerops_guild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("playerops:"):
        return

    raw_gid = data.split(":", 1)[1]
    user_id = q.from_user.id
    ss      = open_ss()

    # Validate against guilds where the user has leadership
    guilds    = usuarios_guilds_for_user(ss, user_id)
    known_ids = {gid for _, gid, _ in guilds}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)

    if not user_has_leadership_role(ss, user_id, gname):
        await q.edit_message_text("❌ No tienes permisos de Oficial o Líder en este gremio.")
        return

    await _ask_player_for_guild(update, context, ss, gid, gname, label, via_callback=True)


async def cb_playerops_player(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("playeropsplayer:"):
        return

    try:
        _, raw_gid, raw_player = data.split(":", 2)
    except (ValueError, IndexError):
        await q.edit_message_text("❌ Error al procesar la selección del jugador.")
        return

    user_id = q.from_user.id
    ss      = open_ss()

    # Validate guild
    guilds    = usuarios_guilds_for_user(ss, user_id)
    known_ids = {gid for _, gid, _ in guilds}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    label, gname, rote_sheet = resolve_label_name_rote_by_id(ss, gid)

    if not user_has_leadership_role(ss, user_id, gname):
        await q.edit_message_text("❌ No tienes permisos de Oficial o Líder en este gremio.")
        return

    # Validate player name against the known list (from session or re-fetched)
    known_players_tuples = session_get(context, user_id, _S_PLAYERS) or list_players_for_guild(ss, gname)
    known_player_names   = {name for name, _ in known_players_tuples}
    try:
        player_name = validate_player_name(raw_player, known_player_names)
    except CallbackValidationError:
        await q.edit_message_text("❌ Jugador no válido.")
        return

    phases = list_phases_in_rote(ss, rote_sheet)
    if not phases:
        await q.edit_message_text(f"❌ No hay fases en la hoja ROTE de {label}.")
        return

    session_set(context, user_id, _S_GID,    gid)
    session_set(context, user_id, _S_GNAME,  gname)
    session_set(context, user_id, _S_LABEL,  label)
    session_set(context, user_id, _S_ROTE,   rote_sheet)
    session_set(context, user_id, _S_PLAYER, player_name)
    session_set(context, user_id, _S_PHASES, phases)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text=f"Fase {p}", callback_data=f"playeropsphase:{gid}:{p}")]
        for p in phases
    ])
    await q.edit_message_text(
        f"Elige la fase para {player_name} en {label}:",
        reply_markup=kb,
    )


async def cb_playerops_phase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("playeropsphase:"):
        return

    try:
        _, raw_gid, raw_phase = data.split(":", 2)
    except ValueError:
        await q.edit_message_text("❌ Error al procesar la fase.")
        return

    user_id = q.from_user.id
    ss      = open_ss()

    # Validate guild
    guilds    = usuarios_guilds_for_user(ss, user_id)
    known_ids = {gid for _, gid, _ in guilds}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    label, gname, rote_sheet = resolve_label_name_rote_by_id(ss, gid)

    if not user_has_leadership_role(ss, user_id, gname):
        await q.edit_message_text("❌ No tienes permisos de Oficial o Líder en este gremio.")
        return

    # Validate phase
    known_phases = session_get(context, user_id, _S_PHASES) or list_phases_in_rote(ss, rote_sheet)
    try:
        phase = validate_phase(raw_phase, known_phases)
    except CallbackValidationError:
        await q.edit_message_text("❌ Fase no válida.")
        return

    player_name = session_get(context, user_id, _S_PLAYER)
    if not player_name:
        await q.edit_message_text("❌ No se encontró el jugador seleccionado. Vuelve a empezar.")
        return

    title = f"Asignaciones de {player_name} — {label} (Fase {phase})"
    body  = render_ops_for_alias_phase_grouped(ss, rote_sheet, player_name, phase)

    if not body or "No tienes asignaciones" in body:
        body = f"El jugador no tiene asignaciones para la Fase {phase}."

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text="Cambiar fase",    callback_data=f"playeropschoosephase:{gid}")],
        [InlineKeyboardButton(text="Cambiar jugador", callback_data=f"playeropschooseplayer:{gid}")],
    ])
    await q.edit_message_text(f"{title}\n\n{body}", reply_markup=kb)


async def cb_playerops_choosephase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("playeropschoosephase:"):
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

    if not user_has_leadership_role(ss, user_id, gname):
        await q.edit_message_text("❌ No tienes permisos de Oficial o Líder en este gremio.")
        return

    player_name = session_get(context, user_id, _S_PLAYER)
    if not player_name:
        await q.edit_message_text("❌ No se encontró el jugador seleccionado. Vuelve a empezar.")
        return

    phases = list_phases_in_rote(ss, rote_sheet)
    if not phases:
        await q.edit_message_text(f"❌ No hay fases en la hoja ROTE de {label}.")
        return

    session_set(context, user_id, _S_PHASES, phases)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text=f"Fase {p}", callback_data=f"playeropsphase:{gid}:{p}")]
        for p in phases
    ])
    await q.edit_message_text(
        f"Elige la fase para {player_name} en {label}:",
        reply_markup=kb,
    )


async def cb_playerops_chooseplayer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("playeropschooseplayer:"):
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

    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)

    if not user_has_leadership_role(ss, user_id, gname):
        await q.edit_message_text("❌ No tienes permisos de Oficial o Líder en este gremio.")
        return

    await _ask_player_for_guild(update, context, ss, gid, gname, label, via_callback=True)


async def _ask_player_for_guild(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    ss,
    gid: str,
    gname: str,
    label: str,
    via_callback: bool,
):
    players = list_players_for_guild(ss, gname)
    if not players:
        msg = "❌ No hay jugadores disponibles para el gremio seleccionado."
        if via_callback:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    _, _, rote_sheet = resolve_label_name_rote_by_id(ss, gid)
    user_id = update.effective_user.id

    # Store the player whitelist in session so callbacks can validate against it
    session_set(context, user_id, _S_GID,     gid)
    session_set(context, user_id, _S_GNAME,   gname)
    session_set(context, user_id, _S_LABEL,   label)
    session_set(context, user_id, _S_ROTE,    rote_sheet)
    session_set(context, user_id, _S_PLAYERS, players)

    kb   = make_keyboard_players(players, f"playeropsplayer:{gid}")
    text = f"Selecciona un jugador de {label}:"

    if via_callback:
        await update.callback_query.edit_message_text(text, reply_markup=kb)
    else:
        await update.message.reply_text(text, reply_markup=kb)


def get_handlers():
    return [
        CommandHandler("operacionesjugador", cmd_operacionesjugador),
        CallbackQueryHandler(cb_playerops_guild,        pattern=r"^playerops:"),
        CallbackQueryHandler(cb_playerops_player,       pattern=r"^playeropsplayer:"),
        CallbackQueryHandler(cb_playerops_phase,        pattern=r"^playeropsphase:"),
        CallbackQueryHandler(cb_playerops_choosephase,  pattern=r"^playeropschoosephase:"),
        CallbackQueryHandler(cb_playerops_chooseplayer, pattern=r"^playeropschooseplayer:"),
    ]
