# src/swgoh/bot/services/tb_map_config_cache.py
"""
Storage for the loaded TB MapConfig inside PTB application.bot_data.

Why a dedicated module:
  Same reasoning as tb_cache — multiple modules (Discord listener and
  the three /tb_* commands) need the MapConfig, and putting the
  bot_data key in one place stops them from drifting via a typo.

Lifetime:
  Loaded once at bot startup via load_into_bot_data(). Stays in memory
  for the rest of the process. A bot restart picks up sheet edits.

Failure mode:
  load_map_config() never raises (it's fail-soft). If the sheet is
  missing, the cached value is an empty MapConfig and the formatter
  falls back to generic labels. Officers see warnings in startup logs.
"""
from __future__ import annotations

import logging

from ...tb.map_config import MapConfig, load_map_config

log = logging.getLogger(__name__)

_CACHE_KEY = "__tb_map_config__"


def load_into_bot_data(bot_data: dict) -> MapConfig:
    """
    Load the map config from Sheets and store it under a well-known key.

    Called once during bot startup. Returns the loaded config so the
    caller can log a summary and decide whether to surface "config
    missing" warnings to operators.

    Idempotent: calling twice replaces the cached value. Useful if we
    later add a /tb_reload_config admin command.
    """
    cfg = load_map_config()
    bot_data[_CACHE_KEY] = cfg
    return cfg


def get(bot_data: dict) -> MapConfig:
    """
    Return the cached MapConfig, or an empty one if startup hasn't run
    yet (defensive — shouldn't happen in normal flow).

    Returning an empty MapConfig rather than None keeps the formatter's
    code path simple: it always gets a config object, never has to
    check for None. The formatter already handles the empty case.
    """
    val = bot_data.get(_CACHE_KEY)
    if isinstance(val, MapConfig):
        return val
    if val is not None:
        log.warning(
            "Unexpected type in TB map config cache (%s); using empty config.",
            type(val).__name__,
        )
    return MapConfig()
