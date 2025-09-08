from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters
from ..services.sheets import (
    open_ss, map_guild_name_to_label_id_rote, resolve_label_name_rote_by_id,
    usuarios_already_registered, players_find_by_alias, players_find_by_ally, upsert_usuario
)

# Guardamos estado mínimo en user_data["reg"]
STATE_KEY = "reg"       # dict con {guild_id,guild_name,label,method?}
STATE_WAITING = "waiting_value"

async def cmd_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss = open_ss()
    gmap = map_guild_name_to_label_id_rote(ss)
    if not gmap:
        await update.message.reply_text("No hay gremios configurados.")
        return
    buttons = [[InlineKeyboardButton(text=(label or gname), callback_data=f"reg:gid:{gid}")]
               for gname, (label, gid, _rote) in gmap.items()]
    kb = InlineKeyboardMarkup(buttons)
    await update.message.reply_text("Elige tu gremio:", reply_markup=kb)

async def cb_register_guild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    data = q.data or ""
    if not data.startswith("reg:gid:"):
        return
    gid = data.split(":", 2)[2]
    ss = open_ss()
    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)
    if not gname:
        await q.edit_message_text("❌ Gremio no encontrado.")
        return

    # Ya registrado en ese gremio
    if usuarios_already_registered(ss, q.from_user.id, gname):
        await q.edit_message_text(f"Ya estás registrado para *{label}*.", parse_mode="Markdown")
        return

    context.user_data[STATE_KEY] = {"guild_id": gid, "guild_name": gname, "label": label}

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Registrar por Alias", callback_data="regm:alias")],
        [InlineKeyboardButton("Registrar por Código de Aliado", callback_data="regm:ally")],
    ])
    await q.edit_message_text(f"Gremio: *{label}*\n\n¿Cómo quieres registrarte?", reply_markup=kb, parse_mode="Markdown")

async def cb_register_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    data = q.data or ""
    if not data.startswith("regm:"):
        return
    method = data.split(":", 1)[1]
    st = context.user_data.get(STATE_KEY) or {}
    if not st:
        await q.edit_message_text("Sesión de registro caducada. Usa /register de nuevo.")
        return
    if method not in ("alias","ally"):
        await q.edit_message_text("Opción no válida.")
        return

    st["method"] = method
    st["state"] = STATE_WAITING
    context.user_data[STATE_KEY] = st

    if method == "alias":
        prompt = "Escribe tu *alias de jugador* exactamente como aparece en la hoja (Players → Player Name)."
    else:
        prompt = "Escribe tu *código de aliado* (puede ser con o sin guiones)."

    await q.edit_message_text(f"Gremio: *{st['label']}*\n\n{prompt}", parse_mode="Markdown")

async def msg_register_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st = context.user_data.get(STATE_KEY)
    if not st or st.get("state") != STATE_WAITING:
        return  # ignorar mensajes que no forman parte del registro

    ss = open_ss()
    method = st.get("method")
    gname  = st.get("guild_name")
    value  = (update.message.text or "").strip()

    if method == "ally":
        found = players_find_by_ally(ss, gname, value)
    else:
        found = players_find_by_alias(ss, gname, value)

    if not found:
        kind = "alias" if method == "alias" else "código de aliado"
        await update.message.reply_text(
            f"❌ El {kind} proporcionado no se pudo encontrar en *{st['label']}*.\n"
            f"Revisa que coincida con la hoja *Players*.",
            parse_mode="Markdown"
        )
        context.user_data.pop(STATE_KEY, None)
        return

    upsert_usuario(ss, found, (update.effective_user.username or ""), update.effective_user.id, update.effective_chat.id)
    alias = found.get("alias",""); ally = found.get("allycode","")
    await update.message.reply_text(f"✅ Registrado en *{st['label']}* como *{alias}* (allycode: {ally}).", parse_mode="Markdown")
    context.user_data.pop(STATE_KEY, None)

def get_handlers():
    return [
        CommandHandler("register", cmd_register),
        CommandHandler("registrar", cmd_register),
        CallbackQueryHandler(cb_register_guild, pattern=r"^reg:gid:"),
        CallbackQueryHandler(cb_register_method, pattern=r"^regm:"),
        MessageHandler(filters.TEXT & ~filters.COMMAND, msg_register_value),
    ]
