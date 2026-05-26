# src/swgoh/bot/logging_filters.py
"""
Logging filters that scrub sensitive values from log records before
they're emitted.

Currently filters:
  - Telegram bot tokens embedded in URLs (httpx logs the full URL on
    every PTB API call, which would otherwise leak the token to
    Railway/journalctl/wherever).

Design:
  - The filter is installed on the root logger from main_bot.py so it
    runs for *every* log record, regardless of which library emits it.
  - The regex pre-check is a cheap `":" in msg` test before the actual
    regex runs, so the overhead on non-matching messages is one byte
    comparison.
  - We mutate `record.msg` and `record.args` in place rather than
    returning a transformed copy. Logging filters that return False
    suppress the record entirely; we want to keep the record but with
    a scrubbed message.
"""
from __future__ import annotations

import logging
import re

# Telegram bot tokens have a fixed shape: <numeric_id>:<35 base64-ish chars>.
# - The numeric ID is 8-10 digits (varies by when the bot was created).
# - The secret part is 35 characters of A-Z, a-z, 0-9, hyphen, underscore.
#
# We anchor on the colon so we don't match random digit:string patterns.
# The bot prefix ("bot" in URLs) isn't required for the match; we redact
# the secret regardless of how it appears.
_TG_TOKEN_RE = re.compile(
    r"(\d{8,10}):([A-Za-z0-9_-]{35})"
)

# What to put in place of the secret. Keep the bot ID visible — it's
# not a secret, and preserving it helps "which bot made this call?"
# debugging without exposing the auth material.
_REDACTED_SUFFIX = ":***REDACTED***"


def _scrub(text: str) -> str:
    """Replace any Telegram bot token in `text` with its redacted form."""
    if ":" not in text:
        return text
    return _TG_TOKEN_RE.sub(rf"\1{_REDACTED_SUFFIX}", text)


class TokenRedactingFilter(logging.Filter):
    """
    Strip Telegram bot tokens from log records before emission.

    Mutates record.msg and record.args in place. Always returns True so
    the record is still emitted, just with redacted content.

    The args path handles the common case where loggers use lazy
    formatting (e.g. `log.info("URL: %s", url)`). We need to scrub the
    args too because the actual formatting happens after the filter
    runs.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        # Scrub the format string itself. Most loggers don't put tokens
        # here (they use %s placeholders), but httpx in particular logs
        # the full URL as part of the message string.
        if isinstance(record.msg, str):
            record.msg = _scrub(record.msg)

        # Scrub any string args. Tuple args (lazy formatting) is the
        # common case; dict args are also possible.
        if record.args:
            if isinstance(record.args, tuple):
                record.args = tuple(
                    _scrub(a) if isinstance(a, str) else a
                    for a in record.args
                )
            elif isinstance(record.args, dict):
                record.args = {
                    k: _scrub(v) if isinstance(v, str) else v
                    for k, v in record.args.items()
                }

        return True


def install_token_redaction() -> None:
    """
    Add the TokenRedactingFilter to every handler on the root logger.

    Idempotent: calling twice doesn't double-filter (we check for an
    existing instance on each handler first).

    Why handlers and not the root logger itself:
      Python's logging.Filter on a logger only filters records that
      *originate* at that logger. Records emitted by child loggers
      (e.g. httpx, discord, telegram) propagate up to the root and
      are then handed to root's *handlers* — and only the handlers'
      filters run for propagated records. So a filter on root logger
      itself would do nothing for httpx logs. We install on each
      handler instead.

      This is one of Python logging's more counterintuitive corners.
      The official docs spell it out, but it's easy to get wrong.

    Must be called *after* logging.basicConfig() (or whatever installs
    your handlers), since handlers don't exist before then.
    """
    root = logging.getLogger()
    if not root.handlers:
        # basicConfig() hasn't run yet, or someone removed all handlers.
        # Add a handler so there's something to attach the filter to.
        # In practice this branch shouldn't fire because main_bot.py
        # calls basicConfig before us.
        root.addHandler(logging.StreamHandler())

    for handler in root.handlers:
        # Skip if already filtered (idempotent re-install).
        if any(isinstance(f, TokenRedactingFilter) for f in handler.filters):
            continue
        handler.addFilter(TokenRedactingFilter())
    
    # Log how many handlers we attached to, so misconfiguration is
    # visible at startup rather than after a token leaks.
    logging.getLogger(__name__).info(
        "Token redaction filter installed on %d handler(s).",
        len(root.handlers),
    )
