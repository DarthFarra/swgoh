# src/swgoh/bot/main_bot.py
import logging
import traceback

from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes

from .config import BOT_TOKEN
from .commands import syncguild, misoperaciones, register, syncdata, operacionesjugador


class _RedactTokenFilter(logging.Filter):
    """
    Strips the bot token from log messages before they are written.
    The Telegram API embeds the token in every URL:
      https://api.telegram.org/bot<TOKEN>/method
    This filter replaces the token with '***' in all log records.
    """
    def __init__(self, token: str):
        super().__init__()
        self._token = token

    def filter(self, record: logging.LogRecord) -> bool:
        if self._token:
            record.msg = str(record.msg).replace(self._token, "***")
            record.args = tuple(
                str(a).replace(self._token, "***") if isinstance(a, str) else a
                for a in (record.args or ())
            )
        return True


def _setup_logging(token: str) -> None:
    """
    Configure logging with token redaction applied to every handler.
    Must be called after the token is known so the filter can be seeded.
    """
    fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt)

    redact = _RedactTokenFilter(token)
    # Apply to the root logger so every handler (including httpx) is covered
    root = logging.getLogger()
    for handler in root.handlers:
        handler.addFilter(redact)


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

    _setup_logging(BOT_TOKEN)

    app = ApplicationBuilder().token(BOT_TOKEN).build()
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
