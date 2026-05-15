# src/swgoh/bot/services/tickets.py
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from ...comlink import fetch_guild

log = logging.getLogger(__name__)

TICKET_CONTRIBUTION_TYPE = 2
DAILY_TICKET_GOAL = 600


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class MemberTickets:
    """Holds ticket data for a single guild member."""

    __slots__ = ("player_id", "player_name", "current_value", "lifetime_value")

    def __init__(
        self,
        player_id: str,
        player_name: str,
        current_value: int,
        lifetime_value: int,
    ) -> None:
        self.player_id = player_id
        self.player_name = player_name
        self.current_value = current_value
        self.lifetime_value = lifetime_value

    @property
    def missing_today(self) -> int:
        return max(0, DAILY_TICKET_GOAL - self.current_value)

    @property
    def completed_today(self) -> bool:
        return self.current_value >= DAILY_TICKET_GOAL


# ---------------------------------------------------------------------------
# API fetching
# ---------------------------------------------------------------------------

def fetch_guild_tickets(guild_id: str) -> List[MemberTickets]:
    """
    Fetches the guild from comlink and extracts ticket contributions
    (memberContribution type=2) for every member.

    Returns a list of MemberTickets sorted by player name.

    Raises:
        RuntimeError: propagated from comlink on network/HTTP errors.
    """
    if not guild_id:
        raise ValueError("guild_id must not be empty")

    log.info("Fetching guild tickets for guild_id=%s", guild_id)
    gdata = fetch_guild({"guildId": guild_id, "includeRecentGuildActivityInfo": True})

    members: List[Dict[str, Any]] = (
        _safe_get(gdata, ["guild", "member"])
        or _safe_get(gdata, ["member"])
        or []
    )

    result: List[MemberTickets] = []
    for member in members:
        if not isinstance(member, dict):
            continue

        player_id = str(member.get("playerId") or "").strip()
        player_name = str(member.get("playerName") or "").strip()

        if not player_id:
            log.warning("Skipping member with no playerId (name=%r)", player_name)
            continue

        current_val, lifetime_val = _extract_ticket_values(member)

        result.append(
            MemberTickets(
                player_id=player_id,
                player_name=player_name,
                current_value=current_val,
                lifetime_value=lifetime_val,
            )
        )

    result.sort(key=lambda m: m.player_name.lower())
    log.info("Fetched ticket data for %d members (guild_id=%s)", len(result), guild_id)
    return result


def _extract_ticket_values(member: Dict[str, Any]) -> Tuple[int, int]:
    """
    Extracts (currentValue, lifetimeValue) for contribution type=2 (tickets).
    Returns (0, 0) if the contribution entry is absent.
    """
    contributions: List[Dict[str, Any]] = member.get("memberContribution") or []
    for contrib in contributions:
        if not isinstance(contrib, dict):
            continue
        if _to_int(contrib.get("type")) == TICKET_CONTRIBUTION_TYPE:
            return (
                _to_int(contrib.get("currentValue")),
                _to_int(contrib.get("lifetimeValue")),
            )
    return 0, 0


# ---------------------------------------------------------------------------
# Message rendering
# ---------------------------------------------------------------------------

def render_tickets_today(
    members: List[MemberTickets],
    guild_label: str,
) -> str:
    """
    Renders the 'Today (live)' message.
    Only lists members who have NOT yet reached the daily goal.
    All user-supplied and numeric content is escaped for MarkdownV2.
    """
    delinquents = [m for m in members if not m.completed_today]
    label = _escape_md(guild_label)

    if not delinquents:
        total = _escape_md(str(len(members)))
        goal  = _escape_md(str(DAILY_TICKET_GOAL))
        return f"✅ *{label}* — All {total} members have reached their {goal} ticket goal today\\!"

    count_str = _escape_md(f"{len(delinquents)}/{len(members)}")
    lines = [f"🎫 *{label}* — Missing tickets today \\({count_str}\\):\n"]
    for m in sorted(delinquents, key=lambda x: x.current_value):
        name    = _escape_md(m.player_name)
        current = _escape_md(str(m.current_value))
        goal    = _escape_md(str(DAILY_TICKET_GOAL))
        lines.append(f"• {name} \\({current}/{goal}\\)")

    return "\n".join(lines)


def render_tickets_yesterday(
    members_live: List[MemberTickets],
    snapshot: Dict[str, int],  # player_name (lower) -> lifetime_value at snapshot
    guild_label: str,
) -> str:
    """
    Renders the 'Yesterday (missed)' message.

    Logic:
      delta = current_lifetime - snapshot_lifetime
      if delta < DAILY_TICKET_GOAL → missed

    Members not in the snapshot are flagged separately (joined after snapshot).
    All user-supplied and numeric content is escaped for MarkdownV2.
    """
    missed: List[Tuple[str, int]] = []   # (player_name, delta)
    new_members: List[str] = []          # names not in snapshot

    for m in members_live:
        key = m.player_name.lower()
        if key not in snapshot:
            new_members.append(m.player_name)
            continue
        delta = m.lifetime_value - snapshot[key]
        if delta < DAILY_TICKET_GOAL:
            missed.append((m.player_name, max(0, delta)))

    label = _escape_md(guild_label)

    if not missed and not new_members:
        return f"✅ *{label}* — Everyone contributed their tickets yesterday\\!"

    lines = [f"📅 *{label}* — Missed tickets yesterday:\n"]

    if missed:
        missed.sort(key=lambda x: x[1])  # ascending by tickets contributed
        goal = _escape_md(str(DAILY_TICKET_GOAL))
        for name, contributed in missed:
            esc_name        = _escape_md(name)
            esc_contributed = _escape_md(str(contributed))
            lines.append(f"• {esc_name} \\({esc_contributed}/{goal}\\)")

    if new_members:
        lines.append("\n⚠️ *New members \\(no snapshot data\\):*")
        for name in sorted(new_members):
            lines.append(f"• {_escape_md(name)}")

    return "\n".join(lines)


async def publish_tickets_to_channel(
    bot,
    channel_id: str,
    members: List[MemberTickets],
    usernames: Dict[str, Optional[str]],  # player_name_lower -> @handle or None
    guild_label: str,
) -> None:
    """
    Publishes the missing-tickets report to a Telegram channel.

    Raises:
        telegram.error.Forbidden: bot is not an admin of the channel.
        telegram.error.BadRequest: channel_id is invalid.
        Any other telegram.error.*: other delivery failure.
    """
    text = render_tickets_today_channel(members, usernames, guild_label)
    await bot.send_message(
        chat_id=channel_id,
        text=text,
        parse_mode="MarkdownV2",
    )


def render_tickets_today_channel(
    members: List[MemberTickets],
    usernames: Dict[str, Optional[str]],  # player_name_lower -> handle or None
    guild_label: str,
) -> str:
    """
    Renders the channel version of the today report.
    Members with a username get '@handle', others get plain player name.
    Only delinquents are listed.
    """
    delinquents = [m for m in members if not m.completed_today]
    label = _escape_md(guild_label)

    if not delinquents:
        total = _escape_md(str(len(members)))
        goal  = _escape_md(str(DAILY_TICKET_GOAL))
        return (
            f"✅ *{label}* — Todos los {total} miembros han completado "
            f"sus {goal} tickets hoy\\!"
        )

    count_str = _escape_md(f"{len(delinquents)}/{len(members)}")
    lines = [f"🎫 *{label}* — Tickets pendientes hoy \\({count_str}\\):\n"]

    for m in sorted(delinquents, key=lambda x: x.current_value):
        handle = usernames.get(m.player_name.lower())
        if handle:
            # @username mentions work in channels without needing inline links
            name_part = f"@{_escape_md(handle)}"
        else:
            name_part = _escape_md(m.player_name)
        current = _escape_md(str(m.current_value))
        goal    = _escape_md(str(DAILY_TICKET_GOAL))
        lines.append(f"• {name_part} \\({current}/{goal}\\)")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Reminder sending
# ---------------------------------------------------------------------------

REMINDER_TEMPLATE = (
    "⚠️ Recordatorio Tickets\\! "
    "Aun te quedan tickets por hacer, actualmente llevas \\({current}/{goal}\\)"
)


async def send_ticket_reminders(
    bot,
    members: List[MemberTickets],
    chat_ids: Dict[str, int],  # player_name_lower -> chat_id
) -> tuple[int, int]:
    """
    Sends a personalised reminder to each delinquent member who has a chat_id.

    Returns:
        (sent_count, failed_count)

    'failed' covers both members with no chat_id registered AND Telegram
    delivery errors (blocked bot, invalid chat, etc.).
    """
    import asyncio

    delinquents = [m for m in members if not m.completed_today]
    sent = 0
    failed = 0

    for m in delinquents:
        chat_id = chat_ids.get(m.player_name.lower())
        if not chat_id:
            failed += 1
            log.warning("No chat_id for member '%s'; skipping reminder.", m.player_name)
            continue

        text = REMINDER_TEMPLATE.format(
            current=m.current_value,
            goal=DAILY_TICKET_GOAL,
        )
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="MarkdownV2",
            )
            sent += 1
            # Small sleep to respect Telegram's per-second rate limit (30 msg/s global)
            await asyncio.sleep(0.05)
        except Exception as exc:
            log.warning(
                "Failed to send reminder to '%s' (chat_id=%s): %s",
                m.player_name, chat_id, exc,
            )
            failed += 1

    return sent, failed


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_get(d: Any, path: List[Any], default: Any = None) -> Any:
    cur = d
    for key in path:
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return default
    return cur


def _to_int(val: Any, default: int = 0) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _escape_md(text: str) -> str:
    """Escapes special characters for Telegram MarkdownV2."""
    special = r"\_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{ch}" if ch in special else ch for ch in str(text))
