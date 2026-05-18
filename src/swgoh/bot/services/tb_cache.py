# src/swgoh/bot/services/tb_cache.py
"""
Latest-TB-snapshot cache, kept in PTB application.bot_data.

Why a dedicated module:
  - The Discord listener writes the cache; the Telegram command handlers
    read it. Putting the cache key in one place stops them from drifting
    out of sync via a typo.
  - Keeps the shape ('what's stored') in one file. If we later add
    fields (e.g. "last C3PO export filename"), there's exactly one
    place to update.

Why bot_data rather than a module-level dict:
  - bot_data is the PTB-blessed home for shared state. It's accessible
    from every handler, scoped to a single Application instance, and
    cleaned up automatically when the bot shuts down.
  - Module-level globals would survive between tests, leak state across
    Application restarts in the same process, and be invisible to PTB's
    introspection.

Persistence: NONE. By design (per the architecture decision). A bot
restart loses the cache; the next C3PO export refills it.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

from ...tb import TBSnapshot

log = logging.getLogger(__name__)


# Single key for the cache entry in bot_data. Underscored to mark it
# as bot-internal state, matching the convention used by security.py
# (_RATE_KEY, _SESSION_KEY).
_CACHE_KEY = "__tb_snapshot__"


@dataclass(frozen=True, slots=True)
class CachedSnapshot:
    """
    What we actually store. Wrapping the snapshot in a small container
    lets us track when it was received (distinct from when the export
    was originally produced) without polluting TBSnapshot itself.

    received_at_monotonic — time.monotonic() value, used for age
      calculations. NOT a wall-clock time, so it's immune to clock
      changes (NTP drift, DST), which is what we want for "how long
      since we received this."

    received_at_wall — UTC datetime for human-facing display. May be
      shown to officers via /tb_status as "received at HH:MM".

    source_filename — the original .json filename from Discord. Useful
      for debugging ("did the bot pick up that 14:23 export?").
    """
    snapshot: TBSnapshot
    received_at_monotonic: float
    received_at_wall: float        # Unix seconds, UTC
    source_filename: str


def set_latest(
    bot_data: dict,
    snapshot: TBSnapshot,
    *,
    source_filename: str = "",
) -> CachedSnapshot:
    """
    Store a freshly-parsed snapshot as the latest. Replaces any prior
    cached value — we don't keep history (per design).

    Called by the Discord listener after a successful parse. Returns the
    CachedSnapshot so the caller can immediately format it for auto-forward
    without an extra get_latest call.
    """
    entry = CachedSnapshot(
        snapshot=snapshot,
        received_at_monotonic=time.monotonic(),
        received_at_wall=time.time(),
        source_filename=source_filename,
    )
    bot_data[_CACHE_KEY] = entry
    log.info(
        "TB cache updated: instance=%s round=%d source=%r",
        snapshot.instance_id, snapshot.current_round, source_filename,
    )
    return entry


def get_latest(bot_data: dict) -> Optional[CachedSnapshot]:
    """Return the cached snapshot, or None if none has been received yet."""
    val = bot_data.get(_CACHE_KEY)
    if val is None:
        return None
    if not isinstance(val, CachedSnapshot):
        # Defensive: someone wrote the wrong type to the cache key.
        # Treat as missing rather than crash the handler.
        log.warning(
            "Unexpected type in TB cache (%s); ignoring.", type(val).__name__
        )
        return None
    return val


def clear(bot_data: dict) -> None:
    """
    Drop the cached snapshot. Not used in normal flow, but useful for
    a hypothetical /tb_clear admin command or for tests.
    """
    bot_data.pop(_CACHE_KEY, None)


def age_minutes(entry: CachedSnapshot) -> int:
    """
    Minutes since the cached snapshot was received. Always >= 0.

    Uses monotonic time so the value is correct even if the system clock
    has been adjusted while the bot was running.
    """
    delta = time.monotonic() - entry.received_at_monotonic
    return max(0, int(delta // 60))
