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
