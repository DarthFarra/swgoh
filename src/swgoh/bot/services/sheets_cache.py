# src/swgoh/bot/services/sheets_cache.py
"""
TTL cache for sheet reads.

Design philosophy:
  Generous TTLs absorb traffic spikes (the original 429 problem).
  Invalidation is treated as an admin override via /refreshcache —
  write paths do NOT invalidate. This codebase doesn't read its own
  writes within a TTL window, and manual sheet edits are rare; TTL
  plus a manual override is sufficient.

  If a future feature needs strict read-your-writes consistency,
  add a targeted invalidate(key) call there. The hooks exist.

Process-local. Single bot instance assumed. If we ever go
multi-instance, swap the dict for Redis with the same get/set/
invalidate interface — no caller changes.

Thread-safe. The protected section never does I/O so lock contention
is irrelevant.

TODO: There are now three caching layers in the codebase
(comlink_player._cache, this module, the open_ss() singleton in
sheets.py). Consider unifying them in a future refactor.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

_lock = threading.Lock()
_cache: Dict[str, Tuple[float, Tuple[List[str], List[List[str]]]]] = {}


@dataclass
class _Stats:
    hits: int = 0
    misses: int = 0
    invalidations: int = 0


_stats = _Stats()

# Used by callers that don't have a TTL profile entry. Conservative.
DEFAULT_TTL_SECS = 300.0  # 5 minutes


def get(key: str) -> Optional[Tuple[List[str], List[List[str]]]]:
    """Return the cached (headers, rows) tuple if present and unexpired."""
    with _lock:
        entry = _cache.get(key)
        if entry is None:
            _stats.misses += 1
            return None
        expires_at, value = entry
        if time.monotonic() >= expires_at:
            del _cache[key]
            _stats.misses += 1
            return None
        _stats.hits += 1
        return value


def set_(
    key: str,
    value: Tuple[List[str], List[List[str]]],
    ttl_seconds: float,
) -> None:
    """Insert or update a cache entry with the given TTL."""
    with _lock:
        _cache[key] = (time.monotonic() + ttl_seconds, value)


def invalidate(key: str) -> None:
    """Drop one entry. Safe for non-existent keys."""
    with _lock:
        if key in _cache:
            del _cache[key]
            _stats.invalidations += 1


def invalidate_all() -> int:
    """
    Drop everything. Returns the count of entries that were dropped.
    Used by the admin /refreshcache command.
    """
    with _lock:
        dropped = len(_cache)
        _cache.clear()
        if dropped:
            _stats.invalidations += 1
        return dropped


def get_stats() -> Dict[str, int]:
    """Snapshot for diagnostics. Useful for an admin /cachestats command."""
    with _lock:
        return {
            "hits": _stats.hits,
            "misses": _stats.misses,
            "invalidations": _stats.invalidations,
            "entries": len(_cache),
        }
