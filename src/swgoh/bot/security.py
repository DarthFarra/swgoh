# src/swgoh/bot/security.py
"""
Security utilities for the Telegram bot.

Provides:
  - Callback data validation (whitelist-based)
  - Per-user session state stored in application.bot_data
  - Per-user command rate limiting (in-process, resets on restart)
"""
from __future__ import annotations

import time
import logging
import functools
from typing import Any, Callable, Coroutine

from telegram import Update
from telegram.ext import ContextTypes

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _bot_data(context: ContextTypes.DEFAULT_TYPE) -> dict:
    """
    Access bot_data reliably via context.application.bot_data.
    This works in PTB v20 regardless of context type configuration,
    unlike context.bot_data which can be unavailable depending on
    how the Application was built.
    """
    return context.application.bot_data

# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

_DEFAULT_COOLDOWN = 10.0
_RATE_KEY = "__rate_limits__"


def rate_limit(
    cooldown_seconds: float = _DEFAULT_COOLDOWN,
    message: str = "⏳ Por favor espera unos segundos antes de usar este comando de nuevo.",
):
    """
    Decorator for async Telegram command handlers.
    Enforces a per-user cooldown. Sends `message` if the cooldown has not elapsed.

    Usage:
        @rate_limit(cooldown_seconds=15)
        async def cmd_syncguild(update, context): ...
    """
    def decorator(handler: Callable[..., Coroutine]) -> Callable[..., Coroutine]:
        command_key = handler.__name__

        @functools.wraps(handler)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
            user_id = update.effective_user.id if update.effective_user else None
            if user_id is None:
                return await handler(update, context)

            now = time.monotonic()
            bot_data = _bot_data(context)
            rate_table: dict = bot_data.setdefault(_RATE_KEY, {})
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
                        f"Por favor espera {remaining:.0f}s.", show_alert=False
                    )
                return

            user_table[command_key] = now
            return await handler(update, context)

        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Per-user session state
# ---------------------------------------------------------------------------
# Session data lives in application.bot_data, keyed by user_id.
# This avoids the 64-byte Telegram callback_data limit and prevents
# users from forging flow state.
#
# Structure: bot_data["__sessions__"] -> {user_id -> {key -> value}}

_SESSION_KEY = "__sessions__"


def session_set(context: ContextTypes.DEFAULT_TYPE, user_id: int, key: str, value: Any) -> None:
    """Store a value in the per-user session."""
    sessions: dict = _bot_data(context).setdefault(_SESSION_KEY, {})
    sessions.setdefault(user_id, {})[key] = value


def session_get(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    key: str,
    default: Any = None,
) -> Any:
    """Retrieve a value from the per-user session."""
    sessions: dict = _bot_data(context).get(_SESSION_KEY, {})
    return sessions.get(user_id, {}).get(key, default)


def session_clear(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> None:
    """Clear all session state for a user."""
    sessions: dict = _bot_data(context).get(_SESSION_KEY, {})
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
