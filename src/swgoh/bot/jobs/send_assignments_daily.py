import os, pytz, datetime as dt
from telegram import Bot
from ...config import TIMEZONE, SHEET_ASSIGNMENTS, SHEET_USERS
from ...sheets import open_or_create


TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


def run():
if not TOKEN:
raise SystemExit("Falta TELEGRAM_BOT_TOKEN")
bot = Bot(TOKEN)
tz = pytz.timezone(TIMEZONE)
now = dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M")
ws_users = open_or_create(SHEET_USERS)
ws_asg = open_or_create(SHEET_ASSIGNMENTS)
users = ws_users.get_all_values() or []
asg = ws_asg.get_all_values() or []


# Envío simple a todos los usuarios con chat_id
header = asg[0] if asg else []
text = "Asignaciones ROTE ("+now+")\n" + "\n".join([" · "+" | ".join(r[:5]) for r in asg[1:6]])
for row in users[1:]:
chat_id = row[0] if row else None
if chat_id:
try:
bot.send_message(chat_id=chat_id, text=text)
except Exception as e:
print("[WARN] fallo enviando a", chat_id, e)


if __name__ == "__main__":
run()
