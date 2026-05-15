# src/swgoh/bot/jobs/snapshot_tickets.py
"""
Snapshot job: reads lifetimeValue (ticket contribution type=2) for every
guild member at reset time and stores it in the Ticket_Snapshots sheet.

Scheduling logic:
  - A CronTrigger job fires at exactly the guild's reset_time (HH:MM Madrid TZ).
  - On bot startup a catch-up check runs: if today's snapshot is missing AND
    the current Madrid time is between reset_time and 23:59, the snapshot is
    taken immediately. This handles Railway restarts that occur after reset.
"""
from __future__ import annotations

import logging
from datetime import datetime, time
from zoneinfo import ZoneInfo

from ..services.sheets import (
    open_ss,
    list_guilds_with_reset_time,
    upsert_ticket_snapshots,
    snapshot_taken_today,
)
from ..services.tickets import fetch_guild_tickets

log = logging.getLogger(__name__)

MADRID_TZ = ZoneInfo("Europe/Madrid")


# ---------------------------------------------------------------------------
# Core snapshot logic
# ---------------------------------------------------------------------------

def run_snapshot_for_guild(guild_id: str, guild_name: str) -> None:
    """
    Fetches current lifetimeValue for all guild members and writes the
    snapshot. Designed to be called by APScheduler; exceptions are caught
    and logged so a single guild failure never crashes the scheduler.
    """
    log.info("Starting ticket snapshot for guild '%s' (id=%s)", guild_name, guild_id)
    try:
        ss      = open_ss()
        members = fetch_guild_tickets(guild_id)

        if not members:
            log.warning(
                "No members returned for guild '%s'; skipping snapshot.", guild_name
            )
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


# ---------------------------------------------------------------------------
# Scheduler registration
# ---------------------------------------------------------------------------

def schedule_snapshot_jobs(scheduler) -> None:
    """
    Reads all guilds with a configured reset_time and registers a daily
    CronTrigger for each, firing at exactly the reset time (Madrid TZ).

    Also performs a catch-up check: if today's snapshot is missing and the
    current time is past the reset, runs the snapshot immediately.

    Call once during bot startup after the scheduler has started.
    """
    try:
        ss     = open_ss()
        guilds = list_guilds_with_reset_time(ss)
    except Exception:
        log.exception(
            "Could not load guild reset times; no snapshot jobs scheduled."
        )
        return

    if not guilds:
        log.warning("No guilds with reset_time configured; snapshot jobs not scheduled.")
        return

    now_madrid = datetime.now(MADRID_TZ)

    for guild_id, guild_name, reset_time_str in guilds:
        hour, minute = _parse_reset_time(reset_time_str, guild_name)
        if hour is None:
            continue

        # Register the daily cron job at exactly reset time
        job_id = f"snapshot_tickets_{guild_id}"
        try:
            scheduler.remove_job(job_id)
        except Exception:
            pass

        scheduler.add_job(
            run_snapshot_for_guild,
            trigger="cron",
            hour=hour,
            minute=minute,
            timezone=MADRID_TZ,
            id=job_id,
            name=f"Ticket snapshot — {guild_name}",
            kwargs={"guild_id": guild_id, "guild_name": guild_name},
            replace_existing=True,
            misfire_grace_time=300,  # 5 min grace for delayed starts
        )

        log.info(
            "Scheduled ticket snapshot for guild '%s' at %02d:%02d Madrid time",
            guild_name, hour, minute,
        )

        # Catch-up: if past reset time today and snapshot not yet taken, run now
        reset_today = now_madrid.replace(
            hour=hour, minute=minute, second=0, microsecond=0
        )
        past_reset   = now_madrid >= reset_today
        before_midnight = now_madrid.time() < time(23, 59)

        if past_reset and before_midnight:
            try:
                already_done = snapshot_taken_today(ss, guild_name)
            except Exception:
                already_done = False

            if not already_done:
                log.info(
                    "Catch-up: running missed snapshot for guild '%s' "
                    "(bot started after %02d:%02d)",
                    guild_name, hour, minute,
                )
                run_snapshot_for_guild(guild_id, guild_name)
            else:
                log.info(
                    "Catch-up not needed for guild '%s': snapshot already taken today.",
                    guild_name,
                )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_reset_time(reset_time_str: str, guild_name: str):
    """
    Parses 'HH:MM'. Returns (hour, minute) as ints or (None, None) on failure.
    """
    try:
        parts = reset_time_str.strip().split(":")
        if len(parts) != 2:
            raise ValueError(f"Expected HH:MM, got {reset_time_str!r}")
        hour   = int(parts[0])
        minute = int(parts[1])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"Out of range: {hour}:{minute}")
        return hour, minute
    except Exception as e:
        log.error(
            "Invalid reset_time %r for guild '%s': %s — skipping.",
            reset_time_str, guild_name, e,
        )
        return None, None
