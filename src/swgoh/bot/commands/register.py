# src/swgoh/bot/commands/register.py
from __future__ import annotations

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from ..services.sheets import (
    open_ss,
    map_guild_name_to_label_id_rote,
    resolve_label_name_rote_by_id,
    usuarios_already_registered,
    players_find_by_alias,
    players_find_by_ally,
    upsert_usuario,
)
from ..security import (
    session_set,
    session_get,
    session_clear,
    validate_guild_id,
    CallbackValidationError,
)

# Session keys
_S_GUILD_ID   = "reg_guild_id"
_S_GUILD_NAME = "reg_guild_name"
_S_LABEL      = "reg_label"
_S_METHOD     = "reg_method"
_S_WAITING    = "reg_waiting"


async def cmd_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss   = open_ss()
    gmap = map_guild_name_to_label_id_rote(ss)
    if not gmap:
        await update.message.reply_text("No hay gremios configurados.")
        return

    buttons = [
        [InlineKeyboardButton(text=(label or gname), callback_data=f"reg:gid:{gid}")]
        for gname, (label, gid, _rote) in gmap.items()
    ]
    await update.message.reply_text("Elige tu gremio:", reply_markup=InlineKeyboardMarkup(buttons))


async def cb_register_guild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("reg:gid:"):
        return

    raw_gid = data.split(":", 2)[2]
    ss      = open_ss()

    # Validate guild_id against known guilds from Sheets
    gmap = map_guild_name_to_label_id_rote(ss)
    known_ids = {gid for _, (_, gid, _) in gmap.items()}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)
    if not gname:
        await q.edit_message_text("❌ Gremio no encontrado.")
        return

    if usuarios_already_registered(ss, q.from_user.id, gname):
        await q.edit_message_text(f"Ya estás registrado para *{label}*.", parse_mode="Markdown")
        return

    user_id = q.from_user.id
    session_set(context, user_id, _S_GUILD_ID,   gid)
    session_set(context, user_id, _S_GUILD_NAME, gname)
    session_set(context, user_id, _S_LABEL,      label)
    session_clear_waiting(context, user_id)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Registrar por Alias",             callback_data="regm:alias")],
        [InlineKeyboardButton("Registrar por Código de Aliado", callback_data="regm:ally")],
    ])
    await q.edit_message_text(
        f"Gremio: *{label}*\n\n¿Cómo quieres registrarte?",
        reply_markup=kb,
        parse_mode="Markdown",
    )


async def cb_register_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("regm:"):
        return

    method  = data.split(":", 1)[1]
    user_id = q.from_user.id
    label   = session_get(context, user_id, _S_LABEL)

    if not label:
        await q.edit_message_text("Sesión de registro caducada. Usa /register de nuevo.")
        return
    if method not in ("alias", "ally"):
        await q.edit_message_text("Opción no válida.")
        return

    session_set(context, user_id, _S_METHOD,  method)
    session_set(context, user_id, _S_WAITING, True)

    prompt = (
        "Escribe tu *alias de jugador* exactamente como aparece en el juego."
        if method == "alias"
        else "Escribe tu *código de aliado* (puede ser con o sin guiones)."
    )
    await q.edit_message_text(f"Gremio: *{label}*\n\n{prompt}", parse_mode="Markdown")


async def msg_register_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    waiting = session_get(context, user_id, _S_WAITING, False)
    if not waiting:
        return  # not in a registration flow

    ss     = open_ss()
    method = session_get(context, user_id, _S_METHOD)
    gname  = session_get(context, user_id, _S_GUILD_NAME)
    label  = session_get(context, user_id, _S_LABEL)
    value  = (update.message.text or "").strip()

    if not method or not gname:
        session_clear(context, user_id)
        return

    found = (
        players_find_by_ally(ss, gname, value)
        if method == "ally"
        else players_find_by_alias(ss, gname, value)
    )

    if not found:
        kind = "alias" if method == "alias" else "código de aliado"
        await update.message.reply_text(
            f"❌ El {kind} proporcionado no se pudo encontrar en *{label}*.\n"
            "Revisa que coincida exactamente con el del juego.",
            parse_mode="Markdown",
        )
        session_clear(context, user_id)
        return

    upsert_usuario(
        ss,
        found,
        update.effective_user.username or "",
        user_id,
        update.effective_chat.id,
    )
    alias = found.get("alias", "")
    ally  = found.get("allycode", "")
    await update.message.reply_text(
        f"✅ Registrado en *{label}* como *{alias}* (allycode: {ally}).",
        parse_mode="Markdown",
    )
    session_clear(context, user_id)


def session_clear_waiting(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> None:
    """Reset the waiting flag without clearing the whole session."""
    session_set(context, user_id, _S_WAITING, False)


def get_handlers():
    return [
        CommandHandler("register",  cmd_register),
        CommandHandler("registrar", cmd_register),
        CallbackQueryHandler(cb_register_guild,  pattern=r"^reg:gid:"),
        CallbackQueryHandler(cb_register_method, pattern=r"^regm:"),
        MessageHandler(filters.TEXT & ~filters.COMMAND, msg_register_value),
    ]
