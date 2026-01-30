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

async def cmd_operacionesjugador(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Flujo:
    1) Verifica que el usuario tenga rol de Oficial o Lider.
    2) Detecta gremios del usuario donde tenga ese rol.
    3) Si varios, pide elegir gremio.
    4) Tras elegir gremio, muestra lista de jugadores.
    5) Tras elegir jugador, pide elegir FASE.
    6) Renderiza asignaciones del jugador en esa fase, agrupadas por PLANETA.
    """
    ss = open_ss()
    guilds = usuarios_guilds_for_user(ss, update.effective_user.id)

    if not guilds:
        await update.message.reply_text("No estás registrado en ningún gremio.")
        return

    leadership_guilds = []
    for label, gid, gname in guilds:
        if user_has_leadership_role(ss, update.effective_user.id, gname):
            leadership_guilds.append((label, gid, gname))

    if not leadership_guilds:
        await update.message.reply_text("No tienes permisos de Oficial o Líder en ningún gremio.")
        return

    if len(leadership_guilds) > 1:
        opts = [(label, gid) for (label, gid, _gn) in leadership_guilds]
        kb = make_keyboard_guilds(opts, "playerops")
        await update.message.reply_text("Elige el gremio para ver operaciones de jugadores:", reply_markup=kb)
        return

    label, gid, gname = leadership_guilds[0]
    await _ask_player_for_guild(update, context, ss, gid, gname, label, via_callback=False)

async def cb_playerops_guild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback de selección de gremio para /operacionesjugador."""
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("playerops:"):
        return
    gid = data.split(":", 1)[1]

    ss = open_ss()
    label, gname, _rote_sheet = resolve_label_name_rote_by_id(ss, gid)

    if not user_has_leadership_role(ss, q.from_user.id, gname):
        await q.edit_message_text("❌ No tienes permisos de Oficial o Líder en este gremio.")
        return

    await _ask_player_for_guild(update, context, ss, gid, gname, label, via_callback=True)

async def cb_playerops_player(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback de selección de jugador."""
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("playeropsplayer:"):
        return

    try:
        parts = data.split(":", 2)
        gid = parts[1]
        player_name = parts[2]
    except (ValueError, IndexError):
        await q.edit_message_text("❌ Error al procesar la selección del jugador.")
        return

    ss = open_ss()
    label, gname, rote_sheet = resolve_label_name_rote_by_id(ss, gid)

    context.user_data["selected_player"] = player_name
    context.user_data["selected_guild_id"] = gid
    context.user_data["selected_guild_name"] = gname
    context.user_data["selected_guild_label"] = label
    context.user_data["selected_rote_sheet"] = rote_sheet

    phases = list_phases_in_rote(ss, rote_sheet)
    if not phases:
        await q.edit_message_text(f"❌ No hay fases en la hoja ROTE de {label}.")
        return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text=f"Fase {p}", callback_data=f"playeropsphase:{gid}:{p}")]
        for p in phases
    ])
    await q.edit_message_text(
        f"Elige la fase para {player_name} en {label}:",
        reply_markup=kb,
    )

async def cb_playerops_phase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback de selección de fase."""
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("playeropsphase:"):
        return

    try:
        _, gid, phase = data.split(":", 2)
    except ValueError:
        await q.edit_message_text("❌ Error al procesar la fase.")
        return

    ss = open_ss()

    player_name = context.user_data.get("selected_player")
    label = context.user_data.get("selected_guild_label")
    rote_sheet = context.user_data.get("selected_rote_sheet")

    if not player_name:
        await q.edit_message_text("❌ Error: No se encontró el jugador seleccionado.")
        return

    if phase.strip().lower() == "x":
        phases = list_phases_in_rote(ss, rote_sheet)
        if not phases:
            await q.edit_message_text(f"❌ No hay fases en la hoja ROTE de {label}.")
            return
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(text=f"Fase {p}", callback_data=f"playeropsphase:{gid}:{p}")]
            for p in phases
        ])
        await q.edit_message_text(
            f"Elige la fase para {player_name} en {label}:",
            reply_markup=kb,
        )
        return

    title = f"Asignaciones de {player_name} — {label} (Fase {phase})"
    body = render_ops_for_alias_phase_grouped(ss, rote_sheet, player_name, phase)

    if not body or "No tienes asignaciones" in body:
        body = f"El jugador seleccionado no tiene asignaciones para la Fase {phase}"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text="Cambiar fase", callback_data=f"playeropschoosephase:{gid}")],
        [InlineKeyboardButton(text="Cambiar jugador", callback_data=f"playeropschooseplayer:{gid}")]
    ])

    await q.edit_message_text(f"{title}\n\n{body}", reply_markup=kb)

async def cb_playerops_choosephase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback para volver a mostrar el selector de fases."""
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("playeropschoosephase:"):
        return
    gid = data.split(":", 1)[1]

    ss = open_ss()
    label = context.user_data.get("selected_guild_label")
    rote_sheet = context.user_data.get("selected_rote_sheet")
    player_name = context.user_data.get("selected_player")

    if not player_name:
        await q.edit_message_text("❌ Error: No se encontró el jugador seleccionado.")
        return

    phases = list_phases_in_rote(ss, rote_sheet)
    if not phases:
        await q.edit_message_text(f"❌ No hay fases en la hoja ROTE de {label}.")
        return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text=f"Fase {p}", callback_data=f"playeropsphase:{gid}:{p}")]
        for p in phases
    ])
    await q.edit_message_text(
        f"Elige la fase para {player_name} en {label}:",
        reply_markup=kb,
    )

async def cb_playerops_chooseplayer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback para volver a mostrar el selector de jugadores."""
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("playeropschooseplayer:"):
        return
    gid = data.split(":", 1)[1]

    ss = open_ss()
    label, gname, _rote_sheet = resolve_label_name_rote_by_id(ss, gid)

    players = list_players_for_guild(ss, gname)
    if not players:
        await q.edit_message_text("❌ Jugadores no disponibles para el gremio seleccionado.")
        return

    kb = make_keyboard_players(players, f"playeropsplayer:{gid}")
    await q.edit_message_text(f"Selecciona un jugador de {label}:", reply_markup=kb)

async def _ask_player_for_guild(update: Update, context: ContextTypes.DEFAULT_TYPE, ss, gid: str, gname: str, label: str, via_callback: bool):
    """Utilidad para mostrar la lista de jugadores de un gremio."""
    players = list_players_for_guild(ss, gname)

    if not players:
        msg = "❌ Jugadores no disponibles para el gremio seleccionado."
        if via_callback:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    _, _, rote_sheet = resolve_label_name_rote_by_id(ss, gid)
    context.user_data["selected_guild_id"] = gid
    context.user_data["selected_guild_name"] = gname
    context.user_data["selected_guild_label"] = label
    context.user_data["selected_rote_sheet"] = rote_sheet

    kb = make_keyboard_players(players, f"playeropsplayer:{gid}")
    text = f"Selecciona un jugador de {label}:"

    if via_callback:
        await update.callback_query.edit_message_text(text, reply_markup=kb)
    else:
        await update.message.reply_text(text, reply_markup=kb)

def get_handlers():
    """Retorna todos los handlers del comando /operacionesjugador."""
    return [
        CommandHandler("operacionesjugador", cmd_operacionesjugador),
        CallbackQueryHandler(cb_playerops_guild, pattern=r"^playerops:"),
        CallbackQueryHandler(cb_playerops_player, pattern=r"^playeropsplayer:"),
        CallbackQueryHandler(cb_playerops_phase, pattern=r"^playeropsphase:"),
        CallbackQueryHandler(cb_playerops_choosephase, pattern=r"^playeropschoosephase:"),
        CallbackQueryHandler(cb_playerops_chooseplayer, pattern=r"^playeropschooseplayer:"),
    ]
