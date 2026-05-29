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
    snapshot_d: Dict[str, int],   # player_name_lower -> lifetimeValue at today's reset
    snapshot_d1: Dict[str, int],  # player_name_lower -> lifetimeValue at yesterday's reset
    guild_label: str,
) -> str:
    """
    Renders the 'Yesterday (missed)' message using the two stored snapshots.

    Logic:
      delta = lifetime_d[member] - lifetime_d1[member]
      if delta < DAILY_TICKET_GOAL → missed yesterday

    Members present in lifetime_d but absent from lifetime_d1 joined after
    yesterday's snapshot and are flagged separately (can't be judged).
    """
    missed: List[Tuple[str, int]] = []   # (player_name, contributed)
    new_members: List[str] = []

    # Iterate snapshot_d (today's roster at reset time)
    for name_lower, ld in snapshot_d.items():
        if name_lower not in snapshot_d1:
            new_members.append(name_lower)
            continue
        delta = ld - snapshot_d1[name_lower]
        if delta < DAILY_TICKET_GOAL:
            missed.append((name_lower, max(0, delta)))

    label = _escape_md(guild_label)

    if not missed and not new_members:
        return f"✅ *{label}* — Todos contribuyeron sus tickets ayer\\!"

    lines = [f"📅 *{label}* — Tickets pendientes ayer:\n"]

    if missed:
        missed.sort(key=lambda x: x[1])
        goal = _escape_md(str(DAILY_TICKET_GOAL))
        for name_lower, contributed in missed:
            esc_name        = _escape_md(name_lower)
            esc_contributed = _escape_md(str(contributed))
            lines.append(f"• {esc_name} \\({esc_contributed}/{goal}\\)")

    if new_members:
        lines.append("\n⚠️ *Nuevos miembros \\(sin datos de ayer\\):*")
        for name in sorted(new_members):
            lines.append(f"• {_escape_md(name)}")

    return "\n".join(lines)

def render_tickets_yesterday_channel(
    snapshot_d:  Dict[str, int],          # player_name_lower -> lifetimeValue today (just written)
    snapshot_d1: Dict[str, int],          # player_name_lower -> lifetimeValue yesterday
    name_map:    Dict[str, str],          # player_name_lower -> player_name (original casing)
    usernames:   Dict[str, Optional[str]],# player_name_lower -> @handle without '@', or None
    guild_label: str,
    header_override: Optional[str] = None,
) -> str:
    """
    Channel-rendered version of render_tickets_yesterday with @mentions.

    Uses the same delta-based logic as the manual /tickets Ayer view, so it's
    immune to the post-reset race that affects live currentValue. Sections:
      • Missed deadline: members where lifetime_d − lifetime_d1 < 600
      • New members:     members in snapshot_d but absent from snapshot_d1
                         (joined after the previous reset; no judgement yet)

    Mentions:
      • If usernames has a non-None @handle for a player → render as @handle
      • Otherwise → render the original-cased player name

    If both sections are empty (everyone hit the goal), emits the celebratory
    ✅ message.

    Args:
      header_override: replaces the default "Deadline alcanzado" header.
                       Provide plain text; caller is responsible for any
                       MarkdownV2 escapes of literal special chars.
    """
    missed: List[Tuple[str, int]] = []   # (name_lower, contributed)
    new_members: List[str] = []          # name_lower

    for name_lower, ld in snapshot_d.items():
        if name_lower not in snapshot_d1:
            new_members.append(name_lower)
            continue
        delta = ld - snapshot_d1[name_lower]
        if delta < DAILY_TICKET_GOAL:
            missed.append((name_lower, max(0, delta)))

    label = _escape_md(guild_label)
    total_members = len(snapshot_d)

    if not missed and not new_members:
        total = _escape_md(str(total_members))
        goal  = _escape_md(str(DAILY_TICKET_GOAL))
        return (
            f"✅ *{label}* — Todos los {total} miembros contribuyeron "
            f"sus {goal} tickets ayer\\!"
        )

    # Header
    if header_override is not None:
        header = f"*{label}* — {header_override}"
    else:
        count_str = _escape_md(f"{len(missed)}/{total_members}")
        header = (
            f"❌ *{label}* — Deadline alcanzado, tickets no completados ayer "
            f"\\({count_str}\\):"
        )

    lines = [header, ""]   # blank line after header for readability

    def _mention_or_name(name_lower: str) -> str:
        handle = usernames.get(name_lower)
        if handle:
            return f"@{_escape_md(handle)}"
        original = name_map.get(name_lower, name_lower)
        return _escape_md(original)

    if missed:
        missed.sort(key=lambda x: x[1])   # least contribution first
        goal = _escape_md(str(DAILY_TICKET_GOAL))
        for name_lower, contributed in missed:
            mention      = _mention_or_name(name_lower)
            contrib_esc  = _escape_md(str(contributed))
            lines.append(f"• {mention} \\({contrib_esc}/{goal}\\)")

    if new_members:
        if missed:
            lines.append("")   # spacer between sections
        lines.append("⚠️ *Nuevos miembros \\(sin datos de ayer\\):*")
        for name_lower in sorted(new_members):
            mention = _mention_or_name(name_lower)
            lines.append(f"• {mention}")

    return "\n".join(lines)

async def publish_tickets_to_channel(
    bot,
    channel_id: str,
    members: List[MemberTickets],
    usernames: Dict[str, Optional[str]],
    guild_label: str,
    thread_id: Optional[int] = None,
    header_override: Optional[str] = None,  # NEW — forwarded to renderer
) -> None:
    """
    Publishes the missing-tickets report to a Telegram channel.

    Args:
        thread_id: if provided, posts into that forum topic (message_thread_id).
                   If None, posts to the channel's general topic.
        header_override: see render_tickets_today_channel docstring.

    Raises:
        telegram.error.Forbidden: bot is not an admin of the channel.
        telegram.error.BadRequest: channel_id or thread_id is invalid.
        Any other telegram.error.*: other delivery failure.
    """
    text = render_tickets_today_channel(
        members, usernames, guild_label, header_override=header_override,
    )
    kwargs = {
        "chat_id": channel_id,
        "text": text,
        "parse_mode": "MarkdownV2",
    }
    if thread_id is not None:
        kwargs["message_thread_id"] = thread_id
    await bot.send_message(**kwargs)


def render_tickets_today_channel(
    members: List[MemberTickets],
    usernames: Dict[str, Optional[str]],  # player_name_lower -> handle or None
    guild_label: str,
    header_override: Optional[str] = None,  # NEW — already-escaped MarkdownV2
) -> str:
    """
    Renders the channel version of the today report.
    Members with a username get '@handle', others get plain player name.
    Only delinquents are listed.

    `header_override` (NEW): if provided, replaces the default
    "🎫 *<label>* — Tickets pendientes hoy (..):" header line. Used by the
    automatic missed-deadline post at reset time to change the framing
    ("Deadline alcanzado") without forking the rendering logic. The body
    (the bullet list of players) is identical.

    The override is inserted as-is; callers are responsible for any
    MarkdownV2 escaping of literal characters they include.
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
    if header_override is not None:
        # Caller-provided header. Still prefix the label so the post is
        # identifiable in a busy channel.
        header = f"*{label}* — {header_override} \\({count_str}\\):\n"
    else:
        header = f"🎫 *{label}* — Tickets pendientes hoy \\({count_str}\\):\n"

    lines = [header]
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
