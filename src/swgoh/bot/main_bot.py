import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler

from .config import BOT_TOKEN

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger(__name__)


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log.info("PING received from user %s", update.effective_user.id)
    await update.message.reply_text("pong")


def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set.")

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("ping", cmd_ping))

    try:
        from .commands import syncguild, misoperaciones, register, syncdata, operacionesjugador
        for handler in (
            syncguild.get_handlers()
            + misoperaciones.get_handlers()
            + register.get_handlers()
            + syncdata.get_handlers()
            + operacionesjugador.get_handlers()
        ):
            app.add_handler(handler)
        log.info("All handlers registered successfully.")
    except Exception as e:
        log.exception("FAILED to register handlers: %s", e)

    log.info("Bot started (polling).")
    app.run_polling()


if __name__ == "__main__":
    main()
