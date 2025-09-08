from telegram import Update
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes
from ..services.sheets import (
    open_ss, usuarios_guilds_for_user, resolve_label_name_rote_by_id,
    user_alias_for_guild, render_assignments_for_alias
)
from ..keyboards.guil d_select import make_keyboard_guilds

async def cmd_misoperaciones(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    label, gid, gname = guilds[0]
    await _send_my_ops_for_guild(update, context, ss, update.effective_user.id, gid, gname, label)

async def cb_myops(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    data = q.data or ""
    if not data.startswith("myops:"):
        return
    gid = data.split(":", 1)[1]
    ss = open_ss()

    label, gname, rote_sheet = resolve_label_name_rote_by_id(ss, gid)
    alias = user_alias_for_guild(ss, update.effective_user.id, gname)
    if not alias:
        await q.edit_message_text(f"❌ No encuentro tu alias en '{gname}'. ¿Te has registrado?")
        return

    text = render_assignments_for_alias(ss, rote_sheet, alias)
    await q.edit_message_text(f"*{label}* — Operaciones para *{alias}*:\n\n{text}", parse_mode="Markdown")

async def _send_my_ops_for_guild(update, context, ss, user_id: int, guild_id: str, guild_name: str, label: str):
    label2, _gname, rote_sheet = resolve_label_name_rote_by_id(ss, guild_id)
    alias = user_alias_for_guild(ss, user_id, guild_name)
    if not alias:
        await update.message.reply_text(f"❌ No encuentro tu alias en '{guild_name}'. ¿Te has registrado?")
        return
    text = render_assignments_for_alias(ss, rote_sheet, alias)
    await update.message.reply_text(f"*{label2}* — Operaciones para *{alias}*:\n\n{text}", parse_mode="Markdown")

def get_handlers():
    return [
        CommandHandler("misoperaciones", cmd_misoperaciones),
        CallbackQueryHandler(cb_myops, pattern=r"^myops:")
    ]
