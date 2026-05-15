# src/swgoh/bot/jobs/snapshot_tickets.py
"""
Snapshot job: reads lifetimeValue (ticket contribution type=2) for every
guild member and stores it in the Ticket_Snapshots sheet.

This job is scheduled to run 5 minutes before each guild's configured
reset_time (column 'reset_time', format HH:MM, Madrid/Europe tz).

It is registered by main_bot.py using APScheduler.
"""
from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from ..services.sheets import open_ss, list_guilds_with_reset_time, upsert_ticket_snapshots
from ..services.tickets import fetch_guild_tickets

log = logging.getLogger(__name__)

MADRID_TZ = ZoneInfo("Europe/Madrid")


def run_snapshot_for_guild(guild_id: str, guild_name: str) -> None:
    """
    Fetches current lifetimeValue for all members of a guild and
    writes the snapshot to the spreadsheet.

    Designed to be called by APScheduler; exceptions are caught and
    logged so a single guild failure does not crash the scheduler.
    """
    log.info("Starting ticket snapshot for guild '%s' (id=%s)", guild_name, guild_id)
    try:
        ss = open_ss()
        members = fetch_guild_tickets(guild_id)

        if not members:
            log.warning("No members returned for guild '%s'; skipping snapshot.", guild_name)
            return

        snapshots = {m.player_name: m.lifetime_value for m in members}
        upsert_ticket_snapshots(ss, guild_name, snapshots)

        log.info(
            "Snapshot saved for guild '%s': %d members, date=%s",
            guild_name,
            len(snapshots),
            datetime.now(MADRID_TZ).date().isoformat(),
        )
    except Exception:
        log.exception(
            "Unexpected error during ticket snapshot for guild '%s' (id=%s)",
            guild_name,
            guild_id,
        )


def schedule_snapshot_jobs(scheduler) -> None:
    """
    Reads all guilds with a configured reset_time from the spreadsheet
    and registers a daily CronTrigger job for each, firing 5 min before reset.

    Call this once during bot startup (after the scheduler has started).

    Args:
        scheduler: an APScheduler AsyncIOScheduler instance.
    """
    try:
        ss = open_ss()
        guilds = list_guilds_with_reset_time(ss)
    except Exception:
        log.exception("Could not load guild reset times from spreadsheet; no snapshot jobs scheduled.")
        return

    if not guilds:
        log.warning("No guilds with reset_time configured; snapshot jobs not scheduled.")
        return

    for guild_id, guild_name, reset_time_str in guilds:
        hour, minute = _parse_reset_time(reset_time_str, guild_name)
        if hour is None:
            continue

        # Fire 5 minutes before reset
        snap_minute = minute - 5
        snap_hour = hour
        if snap_minute < 0:
            snap_minute += 60
            snap_hour = (hour - 1) % 24

        job_id = f"snapshot_tickets_{guild_id}"

        # Remove existing job with same id (e.g. on hot-reload) before re-adding
        try:
            scheduler.remove_job(job_id)
        except Exception:
            pass  # Job didn't exist yet — that's fine

        scheduler.add_job(
            run_snapshot_for_guild,
            trigger="cron",
            hour=snap_hour,
            minute=snap_minute,
            timezone=MADRID_TZ,
            id=job_id,
            name=f"Ticket snapshot — {guild_name}",
            kwargs={"guild_id": guild_id, "guild_name": guild_name},
            replace_existing=True,
            misfire_grace_time=120,  # allow up to 2 min late (e.g. cold start)
        )

        log.info(
            "Scheduled ticket snapshot for guild '%s' at %02d:%02d Madrid time (5 min before %s reset)",
            guild_name,
            snap_hour,
            snap_minute,
            reset_time_str,
        )


def _parse_reset_time(reset_time_str: str, guild_name: str):
    """
    Parses a 'HH:MM' string. Returns (hour, minute) as ints, or (None, None)
    on failure so callers can skip gracefully.
    """
    try:
        parts = reset_time_str.strip().split(":")
        if len(parts) != 2:
            raise ValueError(f"Expected HH:MM, got {reset_time_str!r}")
        hour = int(parts[0])
        minute = int(parts[1])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"Out of range: {hour}:{minute}")
        return hour, minute
    except Exception as e:
        log.error(
            "Invalid reset_time %r for guild '%s': %s — skipping snapshot job.",
            reset_time_str,
            guild_name,
            e,
        )
        return None, None
