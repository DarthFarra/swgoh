from telegram import Update
from telegram.ext import CommandHandler, ContextTypes
from ..config import SYNC_DATA_ALLOWED_CHATS
from ..services.sync_runner import run_sync_data

async def cmd_syncdata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    if chat_id not in SYNC_DATA_ALLOWED_CHATS:
        await update.message.reply_text("❌ No estás autorizado para ejecutar /syncdata.")
        return

    await update.message.reply_text("⏳ Ejecutando sync de datos globales…")
    try:
        _ = await run_sync_data()
        await update.message.reply_text("✅ Sincronización de datos completada.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error en sync_data.\n{e}")

def get_handlers():
    return [CommandHandler("syncdata", cmd_syncdata)]
