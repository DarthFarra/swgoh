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
  an announcements channel is configured, post the "who missed yesterday's
  600-ticket goal" list to the channel.

  The data shown is computed from the TWO SNAPSHOTS (today's lifetime_d vs
  the previous lifetime_d which becomes today's lifetime_d1). This avoids
  the post-reset race where reading currentValue after reset returns 0 for
  every member — because the CG game server resets currentValue atomically
  at reset time, by the time our cron fires the live data is already wiped.
  The snapshot deltas are immune to this: they compare two monotonically-
  increasing lifetime totals captured 24h apart.

Async vs sync split:
  - `run_snapshot_for_guild(guild_id, guild_name)` stays synchronous — it's
    callable from any context (scripts, tests, catch-up) and never touches
    Telegram. Same contract as before. Returns the members list.
  - `run_snapshot_and_publish(...)` is the async coroutine the scheduler uses
    at runtime. It runs the snapshot AND captures the pre-upsert lifetime_d
    so the publish path has both halves of the delta in memory, without a
    read-after-write of the sheet.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time
from typing import Dict, Optional, Tuple
from zoneinfo import ZoneInfo

from telegram.error import Forbidden, BadRequest, TelegramError

from ..config import TIMEZONE
from ..services.sheets import (
    open_ss,
    list_guilds_with_reset_time,
    upsert_ticket_snapshots,
    snapshot_taken_today,
    read_ticket_snapshot,
    get_ticket_auto_post_flags,
    get_channel_config_for_guild,
    get_usernames_for_members,
    resolve_label_name_rote_by_id,
)
from ..services.tickets import (
    fetch_guild_tickets,
    render_tickets_yesterday_channel,
    DAILY_TICKET_GOAL,
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
    snapshot to the sheet. Returns the fetched members list.

    Designed to be called by APScheduler (catch-up path) OR ad-hoc from a
    script/test. Exceptions are caught and logged so a single guild failure
    never crashes the caller; returns an empty list on failure.

    Does NOT publish to any channel. The publish path lives in
    `run_snapshot_and_publish` which has access to the pre-upsert state
    needed for the delta-based missed-deadline message.
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
    Scheduler entry point. Runs the (sync) snapshot work in the default
    thread pool so a slow comlink fetch can't block the event loop, then
    — if auto-post is enabled — awaits the channel publish on the loop.

    Captures the PREVIOUS lifetime_d BEFORE the upsert so we don't need to
    re-read the sheet after the write. The previous lifetime_d is what
    becomes today's lifetime_d1, which is half of the data the renderer
    needs (the other half — today's lifetime_d — is in the `members` list
    we just fetched).
    """
    loop = asyncio.get_running_loop()

    captured = await loop.run_in_executor(
        None, _snapshot_for_guild_with_capture, guild_id, guild_name,
    )
    if captured is None:
        return

    members, snapshot_d, snapshot_d1 = captured
    await _maybe_publish_missed_post(
        guild_id, guild_name, members, snapshot_d, snapshot_d1,
    )


# ---------------------------------------------------------------------------
# Snapshot with pre-upsert state capture
# ---------------------------------------------------------------------------

def _snapshot_for_guild_with_capture(
    guild_id: str, guild_name: str,
) -> Optional[Tuple[list[MemberTickets], Dict[str, int], Dict[str, int]]]:
    """
    Like run_snapshot_for_guild, but also captures the previous lifetime_d
    state so callers can render the missed-deadline message without a
    read-after-write of the sheet.

    Returns (members, snapshot_d, snapshot_d1) on success, None on failure:
      - members:     the fetched MemberTickets (same as run_snapshot_for_guild)
      - snapshot_d:  name_lower → lifetime_value just written (= new lifetime_d)
      - snapshot_d1: name_lower → previous lifetime_d (= new lifetime_d1)

    Why we read BEFORE upsert (rather than after): the upsert calls
    ws.clear() + ws.update(), which would render any cached read stale.
    The read-then-write order also matches how the data semantically
    flows — "promote the previous value to d1, then write the new d".
    """
    log.info(
        "Starting ticket snapshot+capture for guild '%s' (id=%s)",
        guild_name, guild_id,
    )
    try:
        ss      = open_ss()
        members = fetch_guild_tickets(guild_id)

        if not members:
            log.warning(
                "No members returned for guild '%s'; skipping snapshot.",
                guild_name,
            )
            return None

        # Capture previous snapshot's lifetime_d (= soon-to-be lifetime_d1).
        # Empty dict on a guild's first-ever snapshot — that's fine, the
        # renderer will put every member in the "new members, no judgement"
        # bucket which is the honest interpretation.
        previous_lifetime_d: Dict[str, int] = {}
        previous_snapshot = read_ticket_snapshot(ss, guild_name)
        if previous_snapshot is not None:
            _, prev_d, _ = previous_snapshot   # (date, lifetime_d, lifetime_d1)
            previous_lifetime_d = prev_d

        snapshots = {m.player_name: m.lifetime_value for m in members}
        upsert_ticket_snapshots(ss, guild_name, snapshots)

        log.info(
            "Snapshot saved for guild '%s': %d members, date=%s",
            guild_name,
            len(snapshots),
            datetime.now(MADRID_TZ).date().isoformat(),
        )

        snapshot_d  = {m.player_name.lower(): m.lifetime_value for m in members}
        snapshot_d1 = previous_lifetime_d   # already keyed by name_lower

        return members, snapshot_d, snapshot_d1
    except Exception:
        log.exception(
            "Unexpected error during ticket snapshot for guild '%s' (id=%s)",
            guild_name, guild_id,
        )
        return None


# ---------------------------------------------------------------------------
# Missed-deadline publish — async, runs on the bot's event loop
# ---------------------------------------------------------------------------

async def _maybe_publish_missed_post(
    guild_id: str,
    guild_name: str,
    members: list[MemberTickets],
    snapshot_d: Dict[str, int],
    snapshot_d1: Dict[str, int],
) -> None:
    """
    Conditionally publish the missed-deadline summary using the snapshot
    delta (lifetime_d − lifetime_d1 < 600 → member missed yesterday).

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
            "Missed-post skipped for '%s': bot reference not set "
            "(running outside the main bot process?).",
            guild_name,
        )
        return

    loop = asyncio.get_running_loop()

    try:
        ss = await loop.run_in_executor(None, open_ss)
        _, missed_post_enabled = await loop.run_in_executor(
            None, get_ticket_auto_post_flags, ss, guild_name,
        )
    except Exception:
        log.exception(
            "[missed_post] could not read auto-post flags for '%s'; skipping.",
            guild_name,
        )
        return

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

    # Build the casing map. Renderer needs to display original-cased player
    # names when there's no @username to mention; snapshot dicts are keyed
    # by lowered names. This map is the bridge.
    name_map: Dict[str, str] = {
        m.player_name.lower(): m.player_name for m in members
    }

    # Compute who will be mentioned (delinquents + new members) so we only
    # look up usernames for them — no point fetching usernames for the
    # compliant majority that won't appear in the message.
    will_be_mentioned: list[str] = []
    for name_lower, ld in snapshot_d.items():
        if name_lower not in snapshot_d1:
            will_be_mentioned.append(name_lower)
            continue
        if ld - snapshot_d1[name_lower] < DAILY_TICKET_GOAL:
            will_be_mentioned.append(name_lower)

    usernames: Dict[str, Optional[str]] = {}
    if will_be_mentioned:
        try:
            mentioned_original = [
                name_map.get(n, n) for n in will_be_mentioned
            ]
            usernames = await loop.run_in_executor(
                None,
                get_usernames_for_members,
                ss,
                guild_name,
                mentioned_original,
            )
        except Exception:
            log.exception(
                "[missed_post] username lookup failed for '%s'; using plain names.",
                guild_name,
            )
            usernames = {}

    text = render_tickets_yesterday_channel(
        snapshot_d=snapshot_d,
        snapshot_d1=snapshot_d1,
        name_map=name_map,
        usernames=usernames,
        guild_label=label,
    )

    try:
        kwargs = {
            "chat_id":    channel_id,
            "text":       text,
            "parse_mode": "MarkdownV2",
        }
        if thread_id is not None:
            kwargs["message_thread_id"] = thread_id
        await _BOT.send_message(**kwargs)
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
        "[missed_post] posted for guild '%s' (mentioned=%d, total=%d).",
        guild_name, len(will_be_mentioned), len(members),
    )


# ---------------------------------------------------------------------------
# Scheduler registration
# ---------------------------------------------------------------------------

def schedule_snapshot_jobs(scheduler) -> None:
    """
    Reads all guilds with a configured reset_time and registers a daily
    CronTrigger for each, firing at exactly the reset time (Madrid TZ).

    Also performs a catch-up check: if today's snapshot is missing and the
    current time is past the reset, runs the snapshot immediately. Catch-up
    uses the sync helper and does NOT post — a "deadline reached" message
    hours after the actual reset would be confusing.

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
        log.warning(
            "No guilds with reset_time configured; snapshot jobs not scheduled."
        )
        return

    now_madrid = datetime.now(MADRID_TZ)

    for guild_id, guild_name, reset_time_str in guilds:
        hour, minute = _parse_reset_time(reset_time_str, guild_name)
        if hour is None:
            continue

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

        # Catch-up via SYNC helper — does not post (silent by design).
        reset_today = now_madrid.replace(
            hour=hour, minute=minute, second=0, microsecond=0,
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
    """Parses 'HH:MM'. Returns (hour, minute) or (None, None) on failure."""
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
