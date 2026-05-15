# src/swgoh/bot/security.py
"""
Security utilities for the Telegram bot.

Provides:
  - Callback data validation (whitelist-based)
  - Per-user session state stored in bot_data (avoids 64-byte callback_data limit)
  - Per-user command rate limiting (in-process, resets on restart)
"""
from __future__ import annotations

import time
import logging
import functools
from typing import Any, Callable, Coroutine, Optional

from telegram import Update
from telegram.ext import ContextTypes

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

# Minimum seconds between successive command invocations per user.
# Adjust per-command by passing cooldown_seconds to rate_limit().
_DEFAULT_COOLDOWN = 10.0

# bot_data key that stores the rate-limit table: {user_id: {command: last_call_ts}}
_RATE_KEY = "__rate_limits__"


def rate_limit(
    cooldown_seconds: float = _DEFAULT_COOLDOWN,
    message: str = "⏳ Please wait a few seconds before using this command again.",
):
    """
    Decorator for async Telegram command handlers.
    Silently enforces a per-user cooldown; sends `message` if the cooldown
    has not elapsed yet.

    Usage:
        @rate_limit(cooldown_seconds=15)
        async def cmd_syncguild(update, context): ...
    """
    def decorator(handler: Callable[..., Coroutine]) -> Callable[..., Coroutine]:
        command_key = handler.__name__

        @functools.wraps(handler)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            user_id = update.effective_user.id if update.effective_user else None
            if user_id is None:
                # No user context — allow through
                return await handler(update, context, *args, **kwargs)

            now = time.monotonic()
            rate_table: dict = context.bot_data.setdefault(_RATE_KEY, {})
            user_table: dict = rate_table.setdefault(user_id, {})
            last_call: float = user_table.get(command_key, 0.0)

            if now - last_call < cooldown_seconds:
                remaining = cooldown_seconds - (now - last_call)
                log.debug(
                    "Rate limit hit: user=%d command=%s remaining=%.1fs",
                    user_id, command_key, remaining,
                )
                if update.message:
                    await update.message.reply_text(message)
                elif update.callback_query:
                    await update.callback_query.answer(
                        f"Please wait {remaining:.0f}s.", show_alert=False
                    )
                return

            user_table[command_key] = now
            return await handler(update, context, *args, **kwargs)

        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Per-user session state
# ---------------------------------------------------------------------------
# Stores bot-side session data in context.bot_data instead of callback_data,
# which avoids the 64-byte Telegram limit and prevents users from forging state.
#
# Key structure in bot_data:
#   "__sessions__" -> {user_id -> {key -> value}}

_SESSION_KEY = "__sessions__"


def session_set(context: ContextTypes.DEFAULT_TYPE, user_id: int, key: str, value: Any) -> None:
    """Store a value in the per-user session."""
    sessions: dict = context.bot_data.setdefault(_SESSION_KEY, {})
    sessions.setdefault(user_id, {})[key] = value


def session_get(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    key: str,
    default: Any = None,
) -> Any:
    """Retrieve a value from the per-user session."""
    sessions: dict = context.bot_data.get(_SESSION_KEY, {})
    return sessions.get(user_id, {}).get(key, default)


def session_clear(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> None:
    """Clear all session state for a user."""
    sessions: dict = context.bot_data.get(_SESSION_KEY, {})
    sessions.pop(user_id, None)


# ---------------------------------------------------------------------------
# Callback data validation
# ---------------------------------------------------------------------------

class CallbackValidationError(ValueError):
    """Raised when callback data fails whitelist validation."""


def validate_guild_id(guild_id: str, known_guild_ids: set[str]) -> str:
    """
    Assert that `guild_id` is in the known set loaded from Sheets.
    Returns the guild_id unchanged if valid.
    Raises CallbackValidationError if not.
    """
    gid = (guild_id or "").strip()
    if not gid or gid not in known_guild_ids:
        log.warning("Rejected unknown guild_id from callback data: %r", gid)
        raise CallbackValidationError(f"Unknown guild_id: {gid!r}")
    return gid


def validate_player_name(player_name: str, known_players: set[str]) -> str:
    """
    Assert that `player_name` is in the known set loaded from Sheets.
    Returns the player_name unchanged if valid.
    Raises CallbackValidationError if not.
    """
    name = (player_name or "").strip()
    if not name or name not in known_players:
        log.warning("Rejected unknown player_name from callback data: %r", name)
        raise CallbackValidationError(f"Unknown player_name: {name!r}")
    return name


def validate_phase(phase: str, known_phases: list[str]) -> str:
    """
    Assert that `phase` is in the known phases for a guild's ROTE sheet.
    Returns the phase unchanged if valid.
    Raises CallbackValidationError if not.
    """
    p = (phase or "").strip()
    if not p or p not in known_phases:
        log.warning("Rejected unknown phase from callback data: %r", p)
        raise CallbackValidationError(f"Unknown phase: {p!r}")
    return p
