# src/swgoh/bot/jobs/ticket_reminder.py
"""
Pre-reset ticket reminder job.

Fires at `reset_time − TICKET_REMINDER_LEAD_MINUTES` (per guild, Madrid TZ) and
posts the current "missing tickets" list to the guild's announcements channel
— same payload as the officer-triggered "Publicar en Avisos" flow.

Opt-in: only runs for guilds with `ticket_reminder_enabled = TRUE/1/yes` in the
Guilds sheet. Missing column → disabled, same as FALSE. Keeps existing guilds
silent until an officer explicitly turns it on.

Reset-time wrap-around (e.g. 00:30 reset → 23:30 reminder the day before) is
handled by computing the reminder time as `reset_datetime − timedelta(minutes=N)`
and then extracting (hour, minute). APScheduler's hour/minute cron fields are
independent of each other and of the date, so the wrap comes out for free.

Catch-up:
  If the bot starts during the reminder window (reminder_time ≤ now < reset_time)
  AND no reminder has fired in this process for today, fire it immediately. The
  "already fired" check is in-memory only (per-process set) — we accept that a
  restart inside the window can fire twice. Persisting state to the sheet for
  this would be overkill given the worst case is a duplicate post 30 min apart.

Blocking I/O:
  Sheet reads (gspread) and comlink HTTP calls are synchronous. We push them
  to the default executor with `run_in_executor` so a slow API can't stall the
  bot's event loop and starve other handlers.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Set
from zoneinfo import ZoneInfo

from telegram.error import Forbidden, BadRequest, TelegramError

from ..config import TIMEZONE
from ..services.sheets import (
    open_ss,
    list_guilds_with_reset_time,
    get_ticket_auto_post_flags,
    get_channel_config_for_guild,
    get_usernames_for_members,
    resolve_label_name_rote_by_id,
)
from ..services.tickets import (
    fetch_guild_tickets,
    publish_tickets_to_channel,
)

log = logging.getLogger(__name__)

MADRID_TZ = ZoneInfo(TIMEZONE)


# In-memory de-dup for catch-up. Keyed by (guild_id, ISO date).
# Cleared on process restart — see module docstring for the trade-off.
_FIRED_TODAY: Set[tuple[str, str]] = set()


# ---------------------------------------------------------------------------
# Core: build + post the reminder for one guild
# ---------------------------------------------------------------------------

async def run_reminder_for_guild(
    bot,
    guild_id: str,
    guild_name: str,
) -> None:
    """
    Reminder job entry point. Registered with AsyncIOScheduler as a coroutine
    so it runs on the bot's event loop (where PTB's httpx client is alive).

    Every blocking I/O call is dispatched via run_in_executor. Every error
    branch logs and returns — one guild's failure must not affect others.
    """
    log.info("[ticket_reminder] firing for guild '%s' (id=%s)", guild_name, guild_id)
    loop = asyncio.get_running_loop()

    try:
        ss = await loop.run_in_executor(None, open_ss)
    except Exception:
        log.exception("[ticket_reminder] could not open spreadsheet; aborting.")
        return

    # Re-check opt-in flag at fire time. Officers may have flipped the column
    # since startup — honor the live value, not the cached one.
    try:
        reminder_enabled, _ = await loop.run_in_executor(
            None, get_ticket_auto_post_flags, ss, guild_name,
        )
    except Exception:
        log.exception(
            "[ticket_reminder] could not read auto-post flags for '%s'; skipping.",
            guild_name,
        )
        return

    if not reminder_enabled:
        log.info(
            "[ticket_reminder] guild '%s' has reminder disabled at fire time; skipping.",
            guild_name,
        )
        return

    try:
        channel_id, thread_id = await loop.run_in_executor(
            None, get_channel_config_for_guild, ss, guild_name,
        )
    except Exception:
        log.exception(
            "[ticket_reminder] could not read channel config for '%s'; skipping.",
            guild_name,
        )
        return

    if not channel_id:
        log.warning(
            "[ticket_reminder] guild '%s' has reminder enabled but no "
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
            "[ticket_reminder] could not resolve label for '%s'; using guild_name.",
            guild_name,
        )
        label = guild_name

    # Live ticket state from comlink. On failure: log loud, post nothing
    # (per design decision). The comlink HTTP client already retries.
    try:
        members = await loop.run_in_executor(
            None, fetch_guild_tickets, guild_id,
        )
    except Exception:
        log.exception(
            "[ticket_reminder] fetch_guild_tickets failed for '%s'; skipping post.",
            guild_name,
        )
        return

    if not members:
        log.warning(
            "[ticket_reminder] no members returned for '%s'; skipping post.",
            guild_name,
        )
        return

    delinquents = [m for m in members if not m.completed_today]
    if not delinquents:
        # Nobody to remind 1h before reset. The missed-deadline job (at reset)
        # owns the celebratory ✅ — duplicating it here would mean two ✅ posts
        # in 1h, which is noise.
        log.info(
            "[ticket_reminder] all members complete for '%s'; nothing to remind.",
            guild_name,
        )
        return

    try:
        usernames: dict[str, Optional[str]] = await loop.run_in_executor(
            None,
            get_usernames_for_members,
            ss,
            guild_name,
            [m.player_name for m in delinquents],
        )
    except Exception:
        log.exception(
            "[ticket_reminder] username lookup failed for '%s'; using plain names.",
            guild_name,
        )
        usernames = {}

    try:
        await publish_tickets_to_channel(
            bot=bot,
            channel_id=channel_id,
            members=delinquents,
            usernames=usernames,
            guild_label=label,
            thread_id=thread_id,
            # No header_override: this is the standard "tickets pendientes hoy"
            # framing, identical to the officer-triggered post. Consistent UX.
        )
    except Forbidden:
        log.error(
            "[ticket_reminder] bot is not admin of channel %s (guild '%s').",
            channel_id, guild_name,
        )
        return
    except BadRequest as exc:
        log.error(
            "[ticket_reminder] bad channel/thread for guild '%s' (%s/%s): %s",
            guild_name, channel_id, thread_id, exc,
        )
        return
    except TelegramError:
        log.exception(
            "[ticket_reminder] Telegram error posting reminder for '%s'.",
            guild_name,
        )
        return

    today_iso = datetime.now(MADRID_TZ).date().isoformat()
    _FIRED_TODAY.add((guild_id, today_iso))
    log.info(
        "[ticket_reminder] posted for guild '%s' (%d delinquents).",
        guild_name, len(delinquents),
    )


# ---------------------------------------------------------------------------
# Scheduler registration
# ---------------------------------------------------------------------------

def schedule_reminder_jobs(scheduler, application, lead_minutes: int) -> None:
    """
    Register the per-guild reminder cron and run the catch-up check.

    `application` is the PTB Application instance; we read `application.bot`
    here so each scheduled coroutine captures a stable bot reference.

    `lead_minutes` is the global config value (TICKET_REMINDER_LEAD_MINUTES).
    Validated upstream; we still defend against pathological values here.
    """
    if lead_minutes <= 0:
        log.warning(
            "TICKET_REMINDER_LEAD_MINUTES=%s is not positive; disabling reminder jobs.",
            lead_minutes,
        )
        return

    if lead_minutes >= 24 * 60:
        log.warning(
            "TICKET_REMINDER_LEAD_MINUTES=%s >= 24h; that's meaningless. Disabling.",
            lead_minutes,
        )
        return

    try:
        ss = open_ss()
        guilds = list_guilds_with_reset_time(ss)
    except Exception:
        log.exception(
            "[ticket_reminder] could not load guild reset times; no jobs scheduled."
        )
        return

    if not guilds:
        log.info(
            "[ticket_reminder] no guilds with reset_time configured; nothing to schedule."
        )
        return

    now_madrid = datetime.now(MADRID_TZ)
    bot = application.bot

    for guild_id, guild_name, reset_time_str in guilds:
        # Opt-in check at startup. If disabled now we skip scheduling — if an
        # officer enables it later, a bot restart picks it up.
        try:
            reminder_enabled, _ = get_ticket_auto_post_flags(ss, guild_name)
        except Exception:
            log.exception(
                "[ticket_reminder] could not read flags for '%s'; skipping.",
                guild_name,
            )
            continue

        if not reminder_enabled:
            log.info(
                "[ticket_reminder] guild '%s' has reminder disabled; not scheduling.",
                guild_name,
            )
            continue

        reset_hm = _parse_hm(reset_time_str, guild_name)
        if reset_hm is None:
            continue
        reset_h, reset_m = reset_hm

        # Compute reminder time by subtracting lead_minutes from "today's reset".
        # The actual date doesn't matter — APScheduler's cron uses hour/minute
        # independently — but timedelta arithmetic handles wrap-around cleanly.
        anchor = now_madrid.replace(
            hour=reset_h, minute=reset_m, second=0, microsecond=0,
        )
        reminder_dt = anchor - timedelta(minutes=lead_minutes)
        rem_h, rem_m = reminder_dt.hour, reminder_dt.minute

        # Wrap the coroutine with bound args. Default-arg binding pattern avoids
        # the classic late-binding closure trap inside a loop.
        async def _job(_gid=guild_id, _gname=guild_name):
            await run_reminder_for_guild(bot, _gid, _gname)

        job_id = f"ticket_reminder_{guild_id}"
        try:
            scheduler.remove_job(job_id)
        except Exception:
            pass

        scheduler.add_job(
            _job,
            trigger="cron",
            hour=rem_h,
            minute=rem_m,
            timezone=MADRID_TZ,
            id=job_id,
            name=f"Ticket reminder — {guild_name}",
            replace_existing=True,
            misfire_grace_time=300,
        )
        log.info(
            "[ticket_reminder] scheduled for guild '%s' at %02d:%02d Madrid "
            "(reset %02d:%02d − %d min)",
            guild_name, rem_h, rem_m, reset_h, reset_m, lead_minutes,
        )

        # Catch-up: did we start inside the reminder window today?
        # Window: [reminder_dt_today, reset_dt_today)
        reset_dt_today = anchor
        reminder_dt_today = reset_dt_today - timedelta(minutes=lead_minutes)

        in_window = reminder_dt_today <= now_madrid < reset_dt_today
        today_iso = now_madrid.date().isoformat()
        already_fired = (guild_id, today_iso) in _FIRED_TODAY

        if in_window and not already_fired:
            log.info(
                "[ticket_reminder] catch-up: firing missed reminder for '%s' "
                "(now %s is inside [%s, %s))",
                guild_name,
                now_madrid.strftime("%H:%M"),
                reminder_dt_today.strftime("%H:%M"),
                reset_dt_today.strftime("%H:%M"),
            )
            # One-shot via the scheduler in 5s — runs on the same loop as the
            # cron fire would. Don't fire immediately: the scheduler may not
            # have processed the cron registration yet, and we want a clean
            # separation from startup activity.
            scheduler.add_job(
                _job,
                trigger="date",
                run_date=now_madrid + timedelta(seconds=5),
                id=f"{job_id}_catchup",
                name=f"Ticket reminder catch-up — {guild_name}",
                replace_existing=True,
                misfire_grace_time=60,
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_hm(reset_time_str: str, guild_name: str):
    """
    Parse 'HH:MM'. Returns (hour, minute) or None on failure.

    Duplicated from snapshot_tickets._parse_reset_time to avoid an import
    cycle between sibling job modules. Both call sites are 8 lines and the
    contract is stable, so duplication wins over a shared helper module.
    """
    try:
        parts = reset_time_str.strip().split(":")
        if len(parts) != 2:
            raise ValueError(f"expected HH:MM, got {reset_time_str!r}")
        h = int(parts[0])
        m = int(parts[1])
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError(f"out of range: {h}:{m}")
        return h, m
    except Exception as e:
        log.error(
            "[ticket_reminder] invalid reset_time %r for guild '%s': %s — skipping.",
            reset_time_str, guild_name, e,
        )
        return None
