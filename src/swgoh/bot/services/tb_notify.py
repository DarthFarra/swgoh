# src/swgoh/bot/services/tb_notify.py
"""
TB deployment reminders — DM and channel publish.

Mirrors the structure of services/tickets.py:
  - send_deployment_reminders   → DMs each member individually
  - publish_deployment_to_channel → posts a public list to the channel

Why a separate module from tickets.py:
  Tickets and TB deployment are distinct concerns with separate templates
  and rendering rules. Sharing a "send messages to a list of players"
  helper would over-abstract; the two flows have ~20 lines of shared
  shape and 50 lines of distinct content. Better to repeat the shape.

Officer exclusion (publish only):
  Officers can be excluded from the *public* channel list while still
  receiving DMs. Two intent-revealing reasons:
    1. The DM is private — no social cost to nudging an officer in DM.
    2. The public list is member-facing; publishing officer names there
       has different social dynamics than a generic member callout.
  The exclusion is opt-in per-call (pass `excluded_player_names`); the
  DM function never excludes anyone.
"""
from __future__ import annotations

import logging
from typing import Dict, Optional, Sequence, Tuple

from telegram.error import Forbidden, BadRequest, TelegramError

from .tb_undeployed_cache import UndeployedMember

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DM reminder
# ---------------------------------------------------------------------------

_DM_REMINDER_TEMPLATE = (
    "⚠️ Recordatorio TB\\! "
    "Aún te quedan *{missing}* GP por desplegar "
    "\\(desplegados {deployed} de {roster}, {pct}%\\)\\. "
    "Despliega lo antes posible\\."
)


async def send_deployment_reminders(
    bot,
    members: Sequence[UndeployedMember],
    chat_ids: Dict[str, int],
) -> Tuple[int, int]:
    """
    Send one DM per undeployed member whose chat_id is registered.

    Returns:
      (sent_count, failed_count)

    `failed` covers both "no chat_id on file" and "Telegram delivery error".
    We do NOT retry — Forbidden almost always means the user blocked the bot,
    and a transient network error is rare enough that a single attempt is
    a reasonable trade-off against rate-limit risk.

    Officer policy:
      DMs go to EVERY undeployed member, officers included. This function
      does not know about officer exclusion. That filter lives only in
      the publish path (where the social context is different).
    """
    sent = 0
    failed = 0
    for m in members:
        chat_id = chat_ids.get(m.player_name.lower())
        if not chat_id:
            failed += 1
            log.info(
                "No chat_id for member %r; skipping deployment reminder.",
                m.player_name,
            )
            continue

        text = _format_dm_reminder(m)
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="MarkdownV2",
            )
            sent += 1
        except Forbidden:
            log.warning(
                "Forbidden when DM'ing %r (chat_id=%d); user likely blocked bot.",
                m.player_name, chat_id,
            )
            failed += 1
        except BadRequest as exc:
            log.warning(
                "BadRequest when DM'ing %r (chat_id=%d): %s",
                m.player_name, chat_id, exc,
            )
            failed += 1
        except TelegramError as exc:
            log.warning(
                "TelegramError when DM'ing %r (chat_id=%d): %s",
                m.player_name, chat_id, exc,
            )
            failed += 1
    return sent, failed


def _format_dm_reminder(member: UndeployedMember) -> str:
    """Render the DM reminder for one member, with MarkdownV2 escaping."""
    return _DM_REMINDER_TEMPLATE.format(
        missing=_md2(_fmt_gp(member.missing_gp)),
        deployed=_md2(_fmt_gp(member.deployed_gp)),
        roster=_md2(_fmt_gp(member.roster_gp)),
        pct=_md2(f"{member.pct_deployed * 100:.0f}"),
    )


# ---------------------------------------------------------------------------
# Channel publish
# ---------------------------------------------------------------------------

async def publish_deployment_to_channel(
    bot,
    channel_id: str,
    members: Sequence[UndeployedMember],
    usernames: Dict[str, Optional[str]],
    guild_label: str,
    thread_id: Optional[int] = None,
    excluded_player_names: Optional[set[str]] = None,
) -> int:
    """
    Post the undeployed-list to a Telegram channel.

    Args:
        excluded_player_names: lowercased set of player_names to filter
            OUT of the published list. Used to omit officers from the
            member-facing public message. Caller is responsible for
            normalizing to lowercase. Pass None or empty set for no
            exclusion (default behavior).

        thread_id: if provided, posts into that forum topic.

    Returns:
        The number of members actually included in the post (after
        exclusion). Caller can use this to display "Publicado: N
        miembros" in the success message — the original input may
        have had more before exclusion.

    Raises:
        telegram.error.Forbidden: bot is not an admin of the channel.
        telegram.error.BadRequest: channel_id or thread_id invalid.
        Any other telegram.error.*: other delivery failure.
    """
    # Apply exclusion first — everything downstream operates on the
    # filtered list. Done here (not in caller) to keep the rendering
    # function pure and the contract clear: "publish what I tell you,
    # minus what I tell you to exclude."
    if excluded_player_names:
        filtered = [
            m for m in members
            if m.player_name.lower() not in excluded_player_names
        ]
    else:
        filtered = list(members)

    text = _render_channel_post(filtered, usernames, guild_label)
    kwargs = {
        "chat_id": channel_id,
        "text": text,
        "parse_mode": "MarkdownV2",
    }
    if thread_id is not None:
        kwargs["message_thread_id"] = thread_id
    await bot.send_message(**kwargs)
    return len(filtered)


def _render_channel_post(
    members: Sequence[UndeployedMember],
    usernames: Dict[str, Optional[str]],
    guild_label: str,
) -> str:
    """
    Render the channel post. Mentions players via @handle if available;
    falls back to plain (escaped) name if not.

    The `members` sequence is whatever the caller passed in — after any
    upstream filtering. If empty, we render a positive-spin "all set"
    message rather than a confusing empty list.
    """
    label = _md2(guild_label)
    if not members:
        # Either everyone deployed, or all undeployed members were
        # excluded (e.g. only officers were pending). Either way, a
        # positive framing is correct and avoids exposing "officers
        # only" implicitly.
        return f"✅ *{label}* — Todos han desplegado\\!"

    count = _md2(str(len(members)))
    lines = [f"⚠️ *{label}* — Despliegue TB pendiente \\({count}\\):\n"]

    # Sort by missing GP descending — biggest gaps first.
    sorted_members = sorted(members, key=lambda m: -m.missing_gp)
    for m in sorted_members:
        handle = usernames.get(m.player_name.lower())
        if handle:
            name_part = f"@{_md2(handle)}"
        else:
            name_part = _md2(m.player_name)
        missing = _md2(_fmt_gp(m.missing_gp))
        lines.append(f"• {name_part} — {missing} GP")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_gp(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


_MD2_SPECIAL = r"_*[]()~`>#+-=|{}.!\\"


def _md2(text: str) -> str:
    """Escape every MarkdownV2 special character."""
    if not text:
        return ""
    return "".join(
        f"\\{ch}" if ch in _MD2_SPECIAL else ch
        for ch in str(text)
    )
