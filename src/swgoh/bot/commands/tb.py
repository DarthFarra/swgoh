# src/swgoh/bot/commands/tb.py
"""
Telegram commands for Territory Battle reporting.

Flow:
  Discord listener (separate module) receives a C3PO export, parses it,
  stores the result in bot_data via tb_cache.set_latest(). These
  handlers read from that cache and format responses.

Authorization:
  All TB commands are gated by TB_REPORTS_ALLOWED_CHATS, following the
  same pattern as /syncdata. The default fall-back if officers don't set
  the variable is SYNC_DATA_ALLOWED_CHATS — saves duplicate configuration
  in the common case where the same chats run all admin commands.

Commands provided:
  /tb_status            — current cached snapshot, exception lists,
                          stale-data hint if old
  /tb_failed_specials   — cross-phase post-mortem of failed special missions
  /tb_top [metric] [n]  — top N members by the given metric

Why three commands rather than one with sub-options:
  Telegram users don't enjoy typing arguments. Three commands give each
  query a single-word entry point. Power-users who want /tb_top can pass
  args; everyone else just types the command.
"""
from __future__ import annotations

import logging
from typing import Optional

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from ...tb import (
    format_failed_specials,
    format_no_data,
    format_status,
    format_top_contributors,
)
from ..security import rate_limit
from ..services import tb_cache, tb_map_config_cache
from .. import config as bot_cfg

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Authorization
# ---------------------------------------------------------------------------

def _allowed_chats() -> set[str]:
    """
    Return the set of chat IDs allowed to run TB commands.

    Resolution order:
      1. TB_REPORTS_ALLOWED_CHATS (if set in config).
      2. Fall back to SYNC_DATA_ALLOWED_CHATS — same officer-admin scope.

    Resolved fresh each call so that a config reload (future feature)
    would take effect without a bot restart. The set is tiny so the
    cost is negligible.
    """
    explicit = getattr(bot_cfg, "TB_REPORTS_ALLOWED_CHATS", None)
    if explicit:
        return explicit
    return bot_cfg.SYNC_DATA_ALLOWED_CHATS


async def _check_authorized(update: Update) -> bool:
    """
    Verify the current chat is allowed to run TB commands. Replies with
    an explanation if not, and returns False so the caller can early-out.
    """
    chat_id = str(update.effective_chat.id)
    if chat_id in _allowed_chats():
        return True
    log.info(
        "TB command rejected: chat_id=%s not in allowed list", chat_id,
    )
    await update.message.reply_text(
        "❌ Este chat no está autorizado para ejecutar comandos de TB."
    )
    return False


async def _send_messages(update: Update, messages) -> None:
    """
    Send a list of message strings as separate Telegram messages.

    The formatter returns a list because TB output can exceed Telegram's
    4096-char limit and we split at planet boundaries. Each element is
    a complete, parse-able Markdown message; they're sent in order.

    If a single send fails (network, rate limit), we log and continue
    with the next — losing one message is better than failing the whole
    response.

    Accepts either a list[str] (new formatter contract) or a plain str
    (back-compat for formatters not yet migrated, e.g. format_failed_specials).
    """
    if isinstance(messages, str):
        messages = [messages]
    for i, msg in enumerate(messages):
        if not msg.strip():
            continue
        try:
            await update.message.reply_text(msg, parse_mode="Markdown")
        except Exception:
            log.exception(
                "Failed to send TB message %d/%d (continuing with remaining).",
                i + 1, len(messages),
            )


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

@rate_limit(cooldown_seconds=5)
async def cmd_tb_status(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    /tb_status — show the current cached snapshot.

    Renders the standard status message (progress + exception lists)
    with a stale-data hint if the data is more than a few minutes old.
    If no snapshot has been received yet, renders the no-data message
    with instructions.
    """
    if not await _check_authorized(update):
        return

    entry = tb_cache.get_latest(context.application.bot_data)
    if entry is None:
        await update.message.reply_text(
            format_no_data("no_export_yet"),
            parse_mode="Markdown",
        )
        return

    map_config = tb_map_config_cache.get(context.application.bot_data)
    messages = format_status(
        entry.snapshot,
        map_config=map_config,
        age_minutes=tb_cache.age_minutes(entry),
    )
    await _send_messages(update, messages)


@rate_limit(cooldown_seconds=5)
async def cmd_tb_failed_specials(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    /tb_failed_specials — post-mortem list of special-mission failures
    across every phase in the cached snapshot.
    """
    if not await _check_authorized(update):
        return

    entry = tb_cache.get_latest(context.application.bot_data)
    if entry is None:
        await update.message.reply_text(
            format_no_data("no_export_yet"),
            parse_mode="Markdown",
        )
        return

    msg = format_failed_specials(entry.snapshot)
    await _send_messages(update, msg)


# Metrics we accept as the `by` argument to /tb_top. Kept here (rather
# than reaching into tb.analysis._RANKABLE_METRICS) so the public
# command surface is self-documenting and can evolve independently.
_TOP_METRICS = (
    "summary",
    "power",
    "strike_attempt",
    "covert_complete",
    "unit_donated",
)

_DEFAULT_TOP_METRIC = "summary"
_DEFAULT_TOP_N = 10
_MAX_TOP_N = 25  # cap to avoid 4096-char Telegram message overflow


@rate_limit(cooldown_seconds=5)
async def cmd_tb_top(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    /tb_top [metric] [n] — top N contributors by metric (global).

    Args (both optional, positional):
      metric — one of summary / power / strike_attempt / covert_complete
               / unit_donated. Defaults to 'summary'.
      n      — how many to show, 1..25. Defaults to 10.

    Examples:
      /tb_top
      /tb_top power
      /tb_top covert_complete 5
    """
    if not await _check_authorized(update):
        return

    metric, n, err = _parse_top_args(context.args or [])
    if err:
        await update.message.reply_text(err, parse_mode="Markdown")
        return

    entry = tb_cache.get_latest(context.application.bot_data)
    if entry is None:
        await update.message.reply_text(
            format_no_data("no_export_yet"),
            parse_mode="Markdown",
        )
        return

    msg = format_top_contributors(
        entry.snapshot,
        by=metric,
        n=n,
        phase=None,        # always global for this command
    )
    await _send_messages(update, msg)


def _parse_top_args(args: list[str]) -> tuple[str, int, Optional[str]]:
    """
    Parse `/tb_top [metric] [n]`.

    Returns (metric, n, error_message_or_None). On any validation error
    the error message is a Markdown-formatted user-facing string, and
    metric/n are the defaults (caller short-circuits anyway).

    Why explicit parsing rather than argparse:
      Two optional positional args don't justify the dependency. argparse
      also produces error output formatted for terminals, not Telegram.
    """
    metric = _DEFAULT_TOP_METRIC
    n = _DEFAULT_TOP_N

    if not args:
        return metric, n, None

    metric_raw = args[0].lower()
    if metric_raw not in _TOP_METRICS:
        return (
            metric, n,
            f"❌ Métrica inválida: `{metric_raw}`.\n"
            f"Opciones: {', '.join(_TOP_METRICS)}",
        )
    metric = metric_raw

    if len(args) >= 2:
        try:
            requested = int(args[1])
        except ValueError:
            return metric, n, f"❌ El segundo argumento debe ser un número, no `{args[1]}`."
        if requested < 1 or requested > _MAX_TOP_N:
            return metric, n, f"❌ N debe estar entre 1 y {_MAX_TOP_N}."
        n = requested

    return metric, n, None


# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------

def get_handlers():
    return [
        CommandHandler("tb_status",          cmd_tb_status),
        CommandHandler("tb_failed_specials", cmd_tb_failed_specials),
        CommandHandler("tb_top",             cmd_tb_top),
    ]
