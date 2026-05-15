# src/swgoh/bot/main_bot.py
import logging
import traceback

from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes

from .config import BOT_TOKEN
from .commands import syncguild, misoperaciones, register, syncdata, operacionesjugador

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger(__name__)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log all exceptions raised inside handlers so nothing is ever silent."""
    log.error(
        "Exception while handling update %s:\n%s",
        update,
        "".join(traceback.format_exception(
            type(context.error), context.error, context.error.__traceback__
        )),
    )


def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set.")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Global error handler — logs every unhandled exception with full traceback
    app.add_error_handler(error_handler)

    for handler in (
        syncguild.get_handlers()
        + misoperaciones.get_handlers()
        + register.get_handlers()
        + syncdata.get_handlers()
        + operacionesjugador.get_handlers()
    ):
        app.add_handler(handler)

    log.info("All handlers registered successfully.")
    log.info("Bot started (polling).")
    app.run_polling()


if __name__ == "__main__":
    main()
