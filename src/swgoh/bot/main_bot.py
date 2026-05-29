# src/swgoh/bot/main_bot.py
"""
Bot entry point.

Runs:
  - Telegram polling (PTB)
  - APScheduler for the ticket snapshot jobs (guild-specific, time-driven)
  - PTB JobQueue for the three consolidated batch jobs:
      * send_assignments  — daily at SEND_ASSIGNMENTS_TIME (TIMEZONE)
      * sync_guilds       — on SYNC_GUILDS_CRON schedule
      * sync_data         — on SYNC_DATA_CRON schedule

Previously these were three separate Railway services. They now run in the
same process as the bot, eliminating deployment overhead and cost.

Pi / local deployment:
  Set SEND_ASSIGNMENTS_TIME, SYNC_GUILDS_CRON, SYNC_DATA_CRON in .env.
  The systemd unit file (systemd/swgoh-bot.service) keeps this process alive.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import time as dt_time
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from telegram.ext import ApplicationBuilder

from .config import BOT_TOKEN, SEND_ASSIGNMENTS_TIME, SYNC_GUILDS_CRON, SYNC_DATA_CRON, TIMEZONE, TICKET_REMINDER_LEAD_MINUTES
from .commands import syncguild, misoperaciones, register, syncdata, operacionesjugador, tickets, sendassignments, omicrones, tb, tb_notifications, omicronsummary, refreshcache, tb_reload_targets
from .error_handler import on_error
from .discord_listener import start_discord_listener, stop_discord_listener
from .jobs.snapshot_tickets import schedule_snapshot_jobs, set_bot_for_snapshot_jobs
from .jobs.ticket_reminder import schedule_reminder_jobs
from .jobs.send_assignments_daily import job_send_assignments
from .services.sync_runner import run_sync_guilds_once, run_sync_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

# httpx logs full request URLs at INFO, which for Telegram API calls
# contain the bot token in the path. Silence httpx INFO globally and
# rely on PTB's own debug logging when we need request visibility.
# This is defense-in-depth on top of the redaction filter — even if
# the filter misses a token format, the log line never exists.
logging.getLogger("httpx").setLevel(logging.WARNING)

log = logging.getLogger(__name__)

# Install token-redaction filter on every root handler so secrets
# don't leak through httpx/PTB/discord.py log lines.
from .logging_filters import install_token_redaction  # noqa: E402
install_token_redaction()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_send_time(raw: str, tz: ZoneInfo) -> dt_time:
    """
    Parse SEND_ASSIGNMENTS_TIME (HH:MM) into a timezone-aware datetime.time
    for PTB's run_daily().

    Raises SystemExit on invalid format so the process fails fast at startup.
    """
    try:
        parts = raw.strip().split(":")
        if len(parts) != 2:
            raise ValueError
        hour, minute = int(parts[0]), int(parts[1])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        return dt_time(hour, minute, tzinfo=tz)
    except (ValueError, AttributeError):
        raise SystemExit(
            f"SEND_ASSIGNMENTS_TIME must be in HH:MM format (24h), got: {raw!r}"
        )


# ---------------------------------------------------------------------------
# APScheduler job wrappers
# (APScheduler calls plain functions; we bridge to async via asyncio.run_coroutine_threadsafe
#  or, simpler, by scheduling coroutines directly since we use AsyncIOScheduler)
# ---------------------------------------------------------------------------

async def _job_sync_guilds() -> None:
    """Scheduled weekly guild sync — syncs ALL guilds (no filter)."""
    log.info("[scheduled] sync_guilds triggered.")
    try:
        # Pass empty set → sync_guilds.run() will process all guilds.
        await asyncio.to_thread(
            __import__(
                "swgoh.processing.sync_guilds", fromlist=["run"]
            ).run
        )
        log.info("[scheduled] sync_guilds completed.")
    except Exception:
        log.exception("[scheduled] sync_guilds failed.")


async def _job_sync_data() -> None:
    """Scheduled monthly data sync."""
    log.info("[scheduled] sync_data triggered.")
    try:
        await run_sync_data()
        log.info("[scheduled] sync_data completed.")
    except Exception:
        log.exception("[scheduled] sync_data failed.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("TELEGRAM_BOT_TOKEN is not set.")

    tz = ZoneInfo(TIMEZONE)

    # --- PTB Application ---
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Register all command handlers
    for handler in (
        syncguild.get_handlers()
        + misoperaciones.get_handlers()
        + register.get_handlers()
        + syncdata.get_handlers()
        + operacionesjugador.get_handlers()
        + tickets.get_handlers()
        + sendassignments.get_handlers()
        + omicrones.get_handlers()
        + tb.get_handlers()
        + tb_notifications.get_handlers()
        + tb_reload_targets.get_handlers()
        + omicronsummary.get_handlers()
        + refreshcache.get_handlers()
    ):
        app.add_handler(handler)

    # Global error handler — catches any exception that escapes a
    # registered handler. Without this, PTB logs "No error handlers
    # are registered" and the error context is lost.
    app.add_error_handler(on_error)

    # --- Parse send-assignments time ---
    send_time = _parse_send_time(SEND_ASSIGNMENTS_TIME, tz)
    log.info("send_assignments scheduled at %s %s", send_time.strftime("%H:%M"), TIMEZONE)

    # --- APScheduler (for jobs that need cron expressions) ---
    scheduler = AsyncIOScheduler(timezone=tz)

    async def _on_startup(application) -> None:
      
        # 1. Start APScheduler
        scheduler.start()
        log.info("APScheduler started (timezone=%s).", TIMEZONE)

        # 2. Share bot with snapshot jobs so they can post auto-messages
        # when their async wrapper (run_snapshot_and_publish) fires.
        # Safe to call before schedule_snapshot_jobs — catch-up snapshots
        # deliberately use the sync helper and never post (see below).
        set_bot_for_snapshot_jobs(application.bot)

        # 3. Ticket snapshot jobs (guild-specific times, from spreadsheet)
        schedule_snapshot_jobs(scheduler)

        # 4. Per-guild ticket reminder jobs (reset_time − N minutes)
        schedule_reminder_jobs(scheduler, application, TICKET_REMINDER_LEAD_MINUTES)

      

        # 5. Weekly sync_guilds
        scheduler.add_job(
            _job_sync_guilds,
            trigger=CronTrigger.from_crontab(SYNC_GUILDS_CRON, timezone=tz),
            id="scheduled_sync_guilds",
            name="Weekly guild sync",
            replace_existing=True,
            misfire_grace_time=300,  # 5 min tolerance for a slow cold start
        )
        log.info("sync_guilds scheduled: cron='%s' tz=%s", SYNC_GUILDS_CRON, TIMEZONE)

        # 6. Monthly sync_data
        scheduler.add_job(
            _job_sync_data,
            trigger=CronTrigger.from_crontab(SYNC_DATA_CRON, timezone=tz),
            id="scheduled_sync_data",
            name="Monthly data sync",
            replace_existing=True,
            misfire_grace_time=300,
        )
        log.info("sync_data scheduled: cron='%s' tz=%s", SYNC_DATA_CRON, TIMEZONE)

        # 7. Daily send_assignments via PTB JobQueue
        # (PTB's run_daily handles timezone-aware time natively)
        application.job_queue.run_daily(
            job_send_assignments,
            time=send_time,
            name="daily_send_assignments",
        )
        log.info(
            "send_assignments scheduled daily at %s %s",
            send_time.strftime("%H:%M"), TIMEZONE,
        )

      # Load TB map config from Sheets into bot_data so commands and
        # listener can use it. Fail-soft: missing sheets just degrade
        # the output to generic labels.
        from .services import tb_map_config_cache  # local import to avoid circular
        cfg = tb_map_config_cache.load_into_bot_data(application.bot_data)
        if cfg.is_empty:
            log.warning(
                "TB map config is empty — TB messages will use generic "
                "planet labels (T1, T2...). Check TB_Map_Config tab."
            )
        else:
            log.info(
                "TB map config loaded: %d planets, %d mission names.",
                len(cfg.planets), len(cfg.strike_names),
            )

        # Load TB targets from Sheets into bot_data. Fail-soft: missing
          # sheet just means estimation lines are silently skipped.
          # Officers can refresh without a bot restart via /tb_reload_targets.
          from .services import tb_targets_cache  # local import
          targets = tb_targets_cache.load_into_bot_data(application.bot_data)
          if targets.is_empty:
            log.info(
                 "TB targets sheet is empty or missing — auto-summary will "
                 "skip estimation lines until populated. Use /tb_reload_targets "
                 "after editing the TBTargets sheet."
            )
          else:
            log.info(
              "TB targets loaded: %d entries.",
              len(targets.targets),
            )
      
        # 8. Discord listener (optional; skipped if not configured)
        await start_discord_listener(application)

    async def _on_shutdown(application) -> None:
        await stop_discord_listener(application)
        if scheduler.running:
            scheduler.shutdown(wait=False)
            log.info("APScheduler stopped.")

    app.post_init     = _on_startup
    app.post_shutdown = _on_shutdown

    log.info("Bot starting (polling).")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
