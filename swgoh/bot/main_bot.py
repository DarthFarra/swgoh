import os, logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from ..config import TIMEZONE, SHEET_ASSIGNMENTS, SHEET_USERS
from ..sheets import open_or_create


logging.basicConfig(level=logging.INFO)
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ALLOWED = set([x.strip() for x in os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", "").split(",") if x.strip()])


async def _auth(update: Update) -> bool:
if not ALLOWED:
return True
uid = str(update.effective_user.id)
return uid in ALLOWED


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
if not await _auth(update):
return await update.message.reply_text("No autorizado")
await update.message.reply_text("Hola, soy el bot de Aroa. Usa /operaciones para ver tus asignaciones.")


async def operaciones(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
if not await _auth(update):
return await update.message.reply_text("No autorizado")
ws = open_or_create(SHEET_ASSIGNMENTS)
vals = ws.get_all_values() or []
if len(vals) < 2:
return await update.message.reply_text("No hay asignaciones.")
# Muy básico: listar por jugador (puedes pegar aquí tu formato actual)
header = vals[0]
lines = ["Asignaciones ROTE:"]
for row in vals[1:6]:
if not row: continue
lines.append(" · "+" | ".join(row[:5]))
await update.message.reply_text("\n".join(lines[:20]))


def main():
if not TOKEN:
raise SystemExit("Falta TELEGRAM_BOT_TOKEN")
app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("operaciones", operaciones))
app.run_polling()


if __name__ == "__main__":
main()
