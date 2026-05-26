# src/swgoh/bot/error_handler.py
"""
Global error handler for the PTB Application.

Registered via app.add_error_handler() in main_bot.py. Catches any
exception raised from a handler that wasn't caught locally.

Logging strategy:
  - Known-benign Telegram errors (network blips, user blocked bot,
    stale callback queries) are logged at INFO/WARNING so they don't
    pollute the ERROR stream during manual log review.
  - Everything else is logged at ERROR with a full traceback plus
    the user_id, chat_id, and update type for context.

User-facing behavior:
  - Strict silence. The bot does not send any message on error.
    Rationale: avoids feedback loops where a broken handler spams
    the user, and avoids confusing users on transient errors that
    PTB will retry internally.
"""
from __future__ import annotations

import logging
import traceback

from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import (
    Forbidden,
    BadRequest,
    TimedOut,
    NetworkError,
    RetryAfter,
)

log = logging.getLogger(__name__)


def _classify_update(update: object) -> tuple[str, int | None, int | None]:
    """Extract (update_type, user_id, chat_id) for logging context."""
    if not isinstance(update, Update):
        return type(update).__name__, None, None

    user_id = update.effective_user.id if update.effective_user else None
    chat_id = update.effective_chat.id if update.effective_chat else None

    if update.callback_query:
        # Include the callback data so log review can spot which
        # button family (e.g. 'reg:gid:', 'tbudm:') is failing.
        update_type = f"callback_query[{update.callback_query.data!r}]"
    elif update.message:
        update_type = "message"
    elif update.edited_message:
        update_type = "edited_message"
    elif update.channel_post:
        update_type = "channel_post"
    else:
        update_type = "update"

    return update_type, user_id, chat_id


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Global error handler. Must never raise — any exception here would
    be swallowed by PTB and lost.
    """
    err = context.error
    update_type, user_id, chat_id = _classify_update(update)

    # Known-benign errors: log at lower severity so ERROR stays signal-only.
    if isinstance(err, (TimedOut, NetworkError)):
        log.warning(
            "Transient network error: type=%s user_id=%s err=%r",
            update_type, user_id, err,
        )
        return

    if isinstance(err, RetryAfter):
        log.warning(
            "Telegram rate-limited us: retry_after=%s user_id=%s",
            err.retry_after, user_id,
        )
        return

    if isinstance(err, Forbidden):
        # User blocked the bot, or bot was removed from a chat. Expected.
        log.info(
            "Forbidden: type=%s user_id=%s chat_id=%s err=%r",
            update_type, user_id, chat_id, err,
        )
        return

    if isinstance(err, BadRequest):
        # Most common: "Query is too old", "Message is not modified".
        # Usually not a code bug, but worth knowing about.
        log.warning(
            "Telegram BadRequest: type=%s user_id=%s chat_id=%s err=%r",
            update_type, user_id, chat_id, err,
        )
        return

    # Unknown error — log full traceback at ERROR.
    # We format the traceback manually (rather than exc_info=True) because
    # sys.exc_info() is empty inside an async error handler — PTB already
    # caught the exception, so we read the traceback from err.__traceback__.
    tb = "".join(traceback.format_exception(type(err), err, err.__traceback__))
    log.error(
        "Unhandled exception in handler: type=%s user_id=%s chat_id=%s err=%r\n%s",
        update_type, user_id, chat_id, err, tb,
    )
