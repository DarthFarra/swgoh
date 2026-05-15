# src/swgoh/bot/main_bot.py
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram.ext import ApplicationBuilder
from .config import BOT_TOKEN
from .commands import syncguild, misoperaciones, register, syncdata, operacionesjugador, tickets
from .jobs.snapshot_tickets import schedule_snapshot_jobs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [main_bot] %(message)s")
log = logging.getLogger(__name__)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Register command handlers
    for h in (
        syncguild.get_handlers()
        + misoperaciones.get_handlers()
        + register.get_handlers()
        + syncdata.get_handlers()
        + operacionesjugador.get_handlers()
        + tickets.get_handlers()
    ):
        app.add_handler(h)

    # Set up APScheduler (runs in the same asyncio event loop as the bot)
    scheduler = AsyncIOScheduler()

    async def _on_startup(application):
        scheduler.start()
        log.info("APScheduler started.")
        # Load guild reset times from spreadsheet and register snapshot jobs
        schedule_snapshot_jobs(scheduler)

    async def _on_shutdown(application):
        if scheduler.running:
            scheduler.shutdown(wait=False)
            log.info("APScheduler stopped.")

    app.post_init = _on_startup
    app.post_shutdown = _on_shutdown

    log.info("Bot iniciado (polling).")
    app.run_polling()


if __name__ == "__main__":
    main()
