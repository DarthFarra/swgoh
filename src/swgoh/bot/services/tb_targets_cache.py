# src/swgoh/bot/services/tb_targets_cache.py
"""
Storage for the loaded TBTargets inside PTB application.bot_data.

Mirrors tb_map_config_cache: load once at bot startup; refreshable
via the /tb_reload_targets admin command rather than a bot restart.

Failure mode:
  load_tb_targets() never raises. If the sheet is missing, the cached
  value is an empty TBTargets and the formatter skips estimation lines
  silently. Officers see warnings in startup logs.
"""
from __future__ import annotations

import logging

from ...tb.tb_targets import TBTargets, load_tb_targets

log = logging.getLogger(__name__)

_CACHE_KEY = "__tb_targets__"


def load_into_bot_data(bot_data: dict) -> TBTargets:
    """
    Load targets from Sheets and store under a well-known key.

    Returns the loaded targets so the caller can log a summary.
    Idempotent: calling twice replaces the cached value (used by
    /tb_reload_targets).
    """
    targets = load_tb_targets()
    bot_data[_CACHE_KEY] = targets
    return targets


def get(bot_data: dict) -> TBTargets:
    """
    Return the cached TBTargets, or an empty one if startup hasn't run
    yet (defensive — shouldn't happen in normal flow).
    """
    val = bot_data.get(_CACHE_KEY)
    if isinstance(val, TBTargets):
        return val
    if val is not None:
        log.warning(
            "Unexpected type in TB targets cache (%s); using empty targets.",
            type(val).__name__,
        )
    return TBTargets()
