from telegram import Update
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes
from ..services.sheets import open_ss, already_synced_today, resolve_label_name_rote_by_id
from ..services.auth import user_authorized_guilds, user_has_role_in_guild
from ..keyboards.guild_select import make_keyboard_guilds
from ..services.sync_runner import run_sync_guilds_once

async def cmd_syncguild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss = open_ss()
    opts = user_authorized_guilds(ss, update.effective_user.id)  # [(label,gid)]
    if not opts:
        await update.message.reply_text("No tienes permisos para sincronizar (se requiere rol Lider u Oficial).")
        return
    kb = make_keyboard_guilds(opts, "syncguild")
    await update.message.reply_text("Elige el gremio a sincronizar:", reply_markup=kb)

async def cb_syncguild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    data = q.data or ""
    if not data.startswith("syncguild:"):
        return
    gid = data.split(":", 1)[1]
    ss = open_ss()

    if not user_has_role_in_guild(ss, q.from_user.id, gid):
        await q.edit_message_text("❌ No tienes permisos para sincronizar este gremio.")
        return

    if already_synced_today(ss, gid):
        label, _, _ = resolve_label_name_rote_by_id(ss, gid)
        await q.edit_message_text(f"ℹ️ {label} ya se sincronizó hoy.")
        return

    label, _, _ = resolve_label_name_rote_by_id(ss, gid)
    await q.edit_message_text(f"⏳ Sincronizando {label}…")
    try:
        _ = await run_sync_guilds_once(gid)
        await q.edit_message_text(f"✅ Sincronización completada para {label}.")
    except Exception as e:
        await q.edit_message_text(f"❌ Error sincronizando {label}.\n{e}")

def get_handlers():
    return [
        CommandHandler("syncguild", cmd_syncguild),
        CallbackQueryHandler(cb_syncguild, pattern=r"^syncguild:")
    ]
