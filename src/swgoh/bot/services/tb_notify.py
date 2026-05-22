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

# Template kept module-level so it's discoverable and overridable.
# Uses MarkdownV2 escaping at format-time, not in the template.
_DM_REMINDER_TEMPLATE = (
    "⚠️ Recordatorio TB\\! "
    "Aún te quedan *{missing}* GP por desplegar "
    "\\(desplegados {deployed} de {roster}, {pct}%\\)\\. "
    "Despliega lo antes posible\\."
)


async def send_deployment_reminders(
    bot,
    members: Sequence[UndeployedMember],
    chat_ids: Dict[str, int],   # player_name_lower -> chat_id
) -> Tuple[int, int]:
    """
    Send one DM per undeployed member whose chat_id is registered.

    Returns:
      (sent_count, failed_count)

    `failed` covers both "no chat_id on file" and "Telegram delivery error".
    We do NOT retry — Forbidden almost always means the user blocked the bot,
    and a transient network error is rare enough that a single attempt is
    a reasonable trade-off against rate-limit risk.

    Performance note:
      Sequential awaits, not gather(). A 50-member guild → at most ~5s
      of cumulative latency, which is fine inside a callback handler.
      Parallelism would risk hitting Telegram's per-bot rate limit
      (30 messages/sec) and isn't worth the complexity.
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
            # User blocked the bot, or chat doesn't exist anymore.
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
    usernames: Dict[str, Optional[str]],   # player_name_lower -> @handle or None
    guild_label: str,
    thread_id: Optional[int] = None,
) -> None:
    """
    Post the undeployed-list to a Telegram channel.

    Args:
        thread_id: if provided, posts into that forum topic.
                   If None, posts to the channel's general topic.

    Raises:
        telegram.error.Forbidden: bot is not an admin of the channel.
        telegram.error.BadRequest: channel_id or thread_id invalid.
        Any other telegram.error.*: other delivery failure.

    Caller is responsible for catching these and showing a user-facing
    error — same contract as publish_tickets_to_channel.
    """
    text = _render_channel_post(members, usernames, guild_label)
    kwargs = {
        "chat_id": channel_id,
        "text": text,
        "parse_mode": "MarkdownV2",
    }
    if thread_id is not None:
        kwargs["message_thread_id"] = thread_id
    await bot.send_message(**kwargs)


def _render_channel_post(
    members: Sequence[UndeployedMember],
    usernames: Dict[str, Optional[str]],
    guild_label: str,
) -> str:
    """
    Render the channel post. Mentions players via @handle if available;
    falls back to plain (escaped) name if not.
    """
    label = _md2(guild_label)
    if not members:
        # Defensive: caller should filter empty lists, but cope gracefully.
        return f"✅ *{label}* — Todos han desplegado\\!"

    count = _md2(str(len(members)))
    lines = [f"⚠️ *{label}* — Despliegue TB pendiente \\({count}\\):\n"]

    # Sort by missing GP descending — biggest gaps first, matches the
    # auto-summary ordering and gives officers a single ranked view.
    sorted_members = sorted(members, key=lambda m: -m.missing_gp)
    for m in sorted_members:
        handle = usernames.get(m.player_name.lower())
        if handle:
            name_part = f"@{_md2(handle)}"
        else:
            name_part = _md2(m.player_name)
        missing = _md2(_fmt_gp(m.missing_gp))
        lines.append(f"• {name_part} — {missing} GP por desplegar")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers — kept private to this module so we don't tangle the public surface
# ---------------------------------------------------------------------------

def _fmt_gp(n: int) -> str:
    """Short GP rendering: 13_703_506 -> '13.7M'. Same convention as formatters.py."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


# MarkdownV2 reserved characters per Telegram Bot API docs.
_MD2_SPECIAL = r"_*[]()~`>#+-=|{}.!\\"


def _md2(text: str) -> str:
    """
    Escape every MarkdownV2 special character. Conservative — we escape
    even characters that are usually safe in our outputs, because GP
    values like "13.7M" contain a literal '.' that must be escaped, and
    catching everything is simpler than tracking per-context rules.

    Why MarkdownV2 here when the auto-summary uses legacy Markdown?
      The auto-summary's Markdown dialect is set by the existing
      _notify_officers path. These notifications go to a DIFFERENT
      surface (the user's personal chat or the announcements channel),
      and we follow tickets' precedent of MarkdownV2 there. Mixing
      dialects across surfaces is fine; mixing them within one message
      would not be.
    """
    if not text:
        return ""
    return "".join(
        f"\\{ch}" if ch in _MD2_SPECIAL else ch
        for ch in str(text)
    )
