# src/swgoh/bot/main_bot.py
import logging

from telegram.ext import ApplicationBuilder

from .config import BOT_TOKEN
from .commands import syncguild, misoperaciones, register, syncdata, operacionesjugador

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

log = logging.getLogger(__name__)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set.")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    for handler in (
        syncguild.get_handlers()
        + misoperaciones.get_handlers()
        + register.get_handlers()
        + syncdata.get_handlers()
        + operacionesjugador.get_handlers()
    ):
        app.add_handler(handler)

    log.info("Bot started (polling).")
    app.run_polling()


if __name__ == "__main__":
    main()
