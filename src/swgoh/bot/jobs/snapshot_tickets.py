# src/swgoh/bot/jobs/snapshot_tickets.py
"""
Snapshot job: reads lifetimeValue (ticket contribution type=2) for every
guild member at reset time and stores it in the Ticket_Snapshots sheet.

Scheduling logic:
  - A CronTrigger job fires at exactly the guild's reset_time (HH:MM Madrid TZ).
  - On bot startup a catch-up check runs: if today's snapshot is missing AND
    the current Madrid time is between reset_time and 23:59, the snapshot is
    taken immediately. This handles Railway restarts that occur after reset.

Missed-deadline auto-post (opt-in via `ticket_missed_post_enabled` column):
  After the snapshot is persisted, if the column is enabled for the guild and
  an announcements channel is configured, post the final "who missed tickets
  today" list to the channel. The list is computed from the SAME members
  fetched for the snapshot, so it represents the exact pre-reset state.

Ordering matters here: the snapshot must persist BEFORE we post, so that a
post-failure can never roll back snapshot data. The reverse order would risk
posting accurate info but losing the snapshot if the sheet write failed.

Async vs sync split:
  - `run_snapshot_for_guild(guild_id, guild_name)` stays synchronous — it's
    callable from any context (scripts, tests, ad-hoc invocations) and never
    touches Telegram. Same contract as before.
  - `run_snapshot_and_publish(...)` is the async coroutine the scheduler uses
    at runtime. It calls the sync snapshot, then awaits the channel post.
    This is what's registered with AsyncIOScheduler so the publish runs on
    the bot's event loop (where PTB's httpx client lives).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time
from typing import Optional
from zoneinfo import ZoneInfo

from telegram.error import Forbidden, BadRequest, TelegramError

from ..config import TIMEZONE
from ..services.sheets import (
    open_ss,
    list_guilds_with_reset_time,
    upsert_ticket_snapshots,
    snapshot_taken_today,
    get_ticket_auto_post_flags,
    get_channel_config_for_guild,
    get_usernames_for_members,
    resolve_label_name_rote_by_id,
)
from ..services.tickets import (
    fetch_guild_tickets,
    publish_tickets_to_channel,
    MemberTickets,
)

log = logging.getLogger(__name__)

MADRID_TZ = ZoneInfo(TIMEZONE)


# ---------------------------------------------------------------------------
# Module-level reference to PTB's bot, set during scheduler registration.
# Needed because AsyncIOScheduler runs coroutines on the bot's loop but the
# coroutine itself receives no PTB context — so we keep a reference here.
#
# Single global is acceptable: one bot per process, lifetime = process.
# ---------------------------------------------------------------------------
_BOT = None


def set_bot_for_snapshot_jobs(bot) -> None:
    """Called once during bot startup so snapshot jobs can post to channels."""
    global _BOT
    _BOT = bot


# ---------------------------------------------------------------------------
# Core snapshot logic — synchronous, no Telegram side-effects
# ---------------------------------------------------------------------------

def run_snapshot_for_guild(guild_id: str, guild_name: str) -> list[MemberTickets]:
    """
    Fetches lifetimeValue + currentValue for all guild members and writes the
    snapshot to the sheet. Returns the fetched members list so a caller that
    also wants to do something with the same data (e.g. publish a message)
    doesn't have to hit the comlink API a second time.

    Designed to be called by APScheduler OR ad-hoc from a script/test.
    Exceptions are caught and logged so a single guild failure never crashes
    the caller; returns an empty list on failure.

    Contract change vs the original: previously returned None. New return type
    is additive — existing callers that ignore the return value still work.
    """
    log.info("Starting ticket snapshot for guild '%s' (id=%s)", guild_name, guild_id)
    try:
        ss      = open_ss()
        members = fetch_guild_tickets(guild_id)

        if not members:
            log.warning(
                "No members returned for guild '%s'; skipping snapshot.", guild_name
            )
            return []

        snapshots = {m.player_name: m.lifetime_value for m in members}
        upsert_ticket_snapshots(ss, guild_name, snapshots)

        log.info(
            "Snapshot saved for guild '%s': %d members, date=%s",
            guild_name,
            len(snapshots),
            datetime.now(MADRID_TZ).date().isoformat(),
        )
        return members
    except Exception:
        log.exception(
            "Unexpected error during ticket snapshot for guild '%s' (id=%s)",
            guild_name,
            guild_id,
        )
        return []


# ---------------------------------------------------------------------------
# Async wrapper — what the scheduler actually invokes
# ---------------------------------------------------------------------------

async def run_snapshot_and_publish(guild_id: str, guild_name: str) -> None:
    """
    Scheduler entry point. Runs the (sync) snapshot in the default thread pool
    so a slow comlink fetch can't block the event loop, then — if auto-post is
    enabled — awaits the channel publish on the loop.

    This is the function registered with AsyncIOScheduler. Keeping the heavy
    sync work off the loop matters when there are many guilds: a 5s comlink
    response time × N guilds firing in the same minute would otherwise stall
    every other scheduled task on the loop.
    """
    loop = asyncio.get_running_loop()
    members = await loop.run_in_executor(
        None, run_snapshot_for_guild, guild_id, guild_name,
    )
    if not members:
        return  # snapshot failed or guild empty — nothing to publish

    await _maybe_publish_missed_post(guild_id, guild_name, members)


async def _maybe_publish_missed_post(
    guild_id: str,
    guild_name: str,
    members: list[MemberTickets],
) -> None:
    """
    Conditionally publish the missed-deadline summary using the `members`
    list already fetched for the snapshot. Avoids a second comlink call AND
    ensures published numbers match the just-written snapshot exactly.

    Gated by:
      - ticket_missed_post_enabled = TRUE in Guilds sheet
      - announcements_channel configured
      - _BOT set (i.e. running inside the main bot process)

    All failures are isolated: this never raises to the caller, because the
    snapshot has already succeeded and a publish failure must not be reported
    as a snapshot failure.
    """
    if _BOT is None:
        log.debug(
            "Snapshot auto-post skipped for '%s': bot reference not set "
            "(running outside the main bot process?).",
            guild_name,
        )
        return

    loop = asyncio.get_running_loop()

    # Sheet reads are blocking I/O — punt them to the executor too.
    try:
        ss = await loop.run_in_executor(None, open_ss)
        flags = await loop.run_in_executor(
            None, get_ticket_auto_post_flags, ss, guild_name,
        )
    except Exception:
        log.exception(
            "[missed_post] could not read auto-post flags for '%s'; skipping.",
            guild_name,
        )
        return

    _, missed_post_enabled = flags
    if not missed_post_enabled:
        log.info(
            "[missed_post] guild '%s' has missed-post disabled; skipping.",
            guild_name,
        )
        return

    try:
        channel_id, thread_id = await loop.run_in_executor(
            None, get_channel_config_for_guild, ss, guild_name,
        )
    except Exception:
        log.exception(
            "[missed_post] could not read channel config for '%s'; skipping.",
            guild_name,
        )
        return

    if not channel_id:
        log.warning(
            "[missed_post] guild '%s' has missed-post enabled but no "
            "announcements_channel configured; skipping.",
            guild_name,
        )
        return

    try:
        label_tuple = await loop.run_in_executor(
            None, resolve_label_name_rote_by_id, ss, guild_id,
        )
        label = (label_tuple[0] if label_tuple else None) or guild_name
    except Exception:
        log.warning(
            "[missed_post] could not resolve label for '%s'; using guild_name.",
            guild_name,
        )
        label = guild_name

    delinquents = [m for m in members if not m.completed_today]

    usernames: dict[str, Optional[str]] = {}
    if delinquents:
        try:
            usernames = await loop.run_in_executor(
                None,
                get_usernames_for_members,
                ss,
                guild_name,
                [m.player_name for m in delinquents],
            )
        except Exception:
            log.exception(
                "[missed_post] username lookup failed for '%s'; using plain names.",
                guild_name,
            )

    # publish_tickets_to_channel uses render_tickets_today_channel internally.
    # That renderer filters to delinquents AND emits the celebratory ✅ when
    # the filtered list is empty. So:
    #   - delinquents non-empty → pass the full `members` (or just delinquents,
    #     it doesn't matter — the filter does the same job) + a header_override
    #     framing this as the deadline post.
    #   - delinquents empty → pass `members`, header_override=None → renderer
    #     emits the standard ✅ message.
    header_override = (
        "Deadline alcanzado — Tickets no completados hoy"
        if delinquents
        else None
    )

    try:
        await publish_tickets_to_channel(
            bot=_BOT,
            channel_id=channel_id,
            members=members,
            usernames=usernames,
            guild_label=label,
            thread_id=thread_id,
            header_override=header_override,
        )
    except Forbidden:
        log.error(
            "[missed_post] bot is not admin of channel %s (guild '%s').",
            channel_id, guild_name,
        )
        return
    except BadRequest as exc:
        log.error(
            "[missed_post] bad channel/thread for '%s' (%s/%s): %s",
            guild_name, channel_id, thread_id, exc,
        )
        return
    except TelegramError:
        log.exception("[missed_post] Telegram error for '%s'.", guild_name)
        return

    log.info(
        "[missed_post] posted for guild '%s' (delinquents=%d).",
        guild_name, len(delinquents),
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

        # Register the daily cron job at exactly reset time.
        # We register the ASYNC wrapper so AsyncIOScheduler runs it on the
        # bot's event loop, where the publish coroutine can talk to Telegram.
        job_id = f"snapshot_tickets_{guild_id}"
        try:
            scheduler.remove_job(job_id)
        except Exception:
            pass

        scheduler.add_job(
            run_snapshot_and_publish,
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

        # Catch-up: if past reset time today and snapshot not yet taken, run now.
        # We deliberately call the SYNC helper here — catch-up snapshots should
        # NOT trigger the auto-post (a "deadline reached" message hours late is
        # noise, not signal). The next real cron fire will post normally.
        reset_today = now_madrid.replace(
            hour=hour, minute=minute, second=0, microsecond=0
        )
        past_reset      = now_madrid >= reset_today
        before_midnight = now_madrid.time() < time(23, 59)

        if past_reset and before_midnight:
            try:
                already_done = snapshot_taken_today(ss, guild_name)
            except Exception:
                already_done = False

            if not already_done:
                log.info(
                    "Catch-up: running missed snapshot for guild '%s' "
                    "(bot started after %02d:%02d) — silent (no auto-post)",
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
