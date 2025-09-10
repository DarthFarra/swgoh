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


async def cmd_misoperaciones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Flujo:
      1) Detecta gremios del usuario (por 'Usuarios').
      2) Si varios, pide elegir gremio.
      3) Tras elegir gremio, pide elegir FASE (lee fases reales de la hoja ROTE).
      4) Renderiza asignaciones del alias en esa fase, agrupadas por PLANETA.
    """
    ss = open_ss()
    guilds = usuarios_guilds_for_user(ss, update.effective_user.id)  # [(label, gid, gname)]
    if not guilds:
        await update.message.reply_text("No estás registrado en ningún gremio.")
        return

    if len(guilds) > 1:
        opts = [(label, gid) for (label, gid, _gn) in guilds]
        kb = make_keyboard_guilds(opts, "myops")
        await update.message.reply_text("Elige el gremio para ver tus operaciones:", reply_markup=kb)
        return

    # Solo 1 gremio: saltamos directo a la selección de fase
    label, gid, gname = guilds[0]
    await _ask_phase_for_guild(update, context, ss, gid, gname, label, via_callback=False)


async def cb_myops(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback de selección de gremio para /misoperaciones."""
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("myops:"):
        return
    gid = data.split(":", 1)[1]

    ss = open_ss()
    label, gname, _rote_sheet = resolve_label_name_rote_by_id(ss, gid)
    # Antes de pedir fase, confirmamos que el usuario tiene alias en este gremio
    alias = user_alias_for_guild(ss, q.from_user.id, gname)
    if not alias:
        await q.edit_message_text(f"❌ No encuentro tu alias en '{gname}'. ¿Te has registrado?")
        return

    # Pedir fase (lee las fases reales)
    phases = list_phases_in_rote(ss, _rote_sheet)
    if not phases:
        await q.edit_message_text(f"❌ No hay fases en la hoja ROTE de {label}.")
        return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text=f"Fase {p}", callback_data=f"myopsphase:{gid}:{p}")]
        for p in phases
    ])
    await q.edit_message_text(
        f"Elige la fase para {alias} en {label}:",
        reply_markup=kb,
    )


async def cb_myops_phase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback de selección de fase para /misoperaciones."""
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("myopsphase:"):
        return
    # Formato: myopsphase:<gid>:<fase>
    try:
        _, gid, phase = data.split(":", 2)
    except ValueError:
        return

    ss = open_ss()
    label, gname, rote_sheet = resolve_label_name_rote_by_id(ss, gid)
    alias = user_alias_for_guild(ss, q.from_user.id, gname)
    if not alias:
        await q.edit_message_text(f"❌ No encuentro tu alias en '{gname}'. ¿Te has registrado?")
        return

    # ⛑️ protección por si llega una 'x' desde un callback antiguo
    if phase.strip().lower() == "x":
        # volvemos a mostrar el selector de fases válido
        phases = list_phases_in_rote(ss, rote_sheet)
        if not phases:
            await q.edit_message_text(f"❌ No hay fases en la hoja ROTE de {label}.")
            return
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(text=f"Fase {p}", callback_data=f"myopsphase:{gid}:{p}")]
            for p in phases
        ])
        await q.edit_message_text(
            f"Elige la fase para {alias} en {label}:",
            reply_markup=kb,
        )
        return

    title = f"Asignaciones de {alias} — {label} (Fase {phase})"
    body = render_ops_for_alias_phase_grouped(ss, rote_sheet, alias, phase)

    # Teclado con "Cambiar fase"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text="Cambiar fase", callback_data=f"myopschoosephase:{gid}")]
    ])

    await q.edit_message_text(f"{title}\n\n{body}", reply_markup=kb)


async def cb_myops_choosephase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback para volver a mostrar el selector de fases del mismo gremio."""
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("myopschoosephase:"):
        return
    gid = data.split(":", 1)[1]

    ss = open_ss()
    label, gname, rote_sheet = resolve_label_name_rote_by_id(ss, gid)
    alias = user_alias_for_guild(ss, q.from_user.id, gname)
    if not alias:
        await q.edit_message_text(f"❌ No encuentro tu alias en '{gname}'. ¿Te has registrado?")
        return

    phases = list_phases_in_rote(ss, rote_sheet)
    if not phases:
        await q.edit_message_text(f"❌ No hay fases en la hoja ROTE de {label}.")
        return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text=f"Fase {p}", callback_data=f"myopsphase:{gid}:{p}")]
        for p in phases
    ])
    await q.edit_message_text(
        f"Elige la fase para {alias} en {label}:",
        reply_markup=kb,
    )


async def _ask_phase_for_guild(update: Update, context: ContextTypes.DEFAULT_TYPE, ss, gid: str, gname: str, label: str, via_callback: bool):
    """Utilidad para preguntar la fase cuando hay un solo gremio."""
    # Validar alias
    alias = user_alias_for_guild(ss, update.effective_user.id, gname)
    if not alias:
        if via_callback:
            await update.callback_query.edit_message_text(f"❌ No encuentro tu alias en '{gname}'. ¿Te has registrado?")
        else:
            await update.message.reply_text(f"❌ No encuentro tu alias en '{gname}'. ¿Te has registrado?")
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
        CallbackQueryHandler(cb_myops, pattern=r"^myops:"),
        CallbackQueryHandler(cb_myops_phase, pattern=r"^myopsphase:"),
        CallbackQueryHandler(cb_myops_choosephase, pattern=r"^myopschoosephase:"),
    ]
