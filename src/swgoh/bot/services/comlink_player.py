# src/swgoh/bot/services/comlink_player.py
"""
Live player data lookup via Comlink for /omicrones (and other future
real-time commands).

Why this module exists:
  - /omicrones needs *current* skill tiers and relic levels. The weekly
    /syncguild snapshot in Player_Skills/Player_Units is too stale —
    a user who upgrades an omicron and immediately runs /omicrones must
    see that reflected.
  - Comlink calls are blocking I/O. We must not block PTB's event loop,
    so the underlying call runs in a thread.
  - Repeated /omicrones calls within a short window (e.g. picking GAC,
    then immediately TW) shouldn't hammer Comlink. A 60s TTL cache is
    a sensible compromise between freshness and load.

Design:
  - One public coroutine: `fetch_player_state(player_id, ...)`.
  - Cache stored in `application.bot_data` so it survives across handler
    invocations within a single bot process. No shared state in module
    globals — that would survive across tests and resists isolation.
  - asyncio.wait_for enforces the timeout regardless of what the lower
    HTTP layer is configured to do.
  - A separate, longer-lived (1h) cache for the Characters/Ships catalog,
    which is rewritten only by /syncdata (monthly).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

# Comlink imports — same try/except as sync_guilds.py uses.
try:
    from ...comlink import fetch_player_by_id  # type: ignore
except ImportError:
    from ...comlink import fetch_player as fetch_player_by_id  # type: ignore

from ...processing import _roster_parse as rp
from .. import config as bot_cfg
from . import sheets as svc_sheets

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cache primitives — generic TTL cache living in bot_data.
#
# Two caches share one dict, keyed by string. This keeps the public surface
# tiny and avoids spreading cache bookkeeping across helpers.
# ---------------------------------------------------------------------------

_CACHE_KEY = "__omi_cache__"
_PLAYER_TTL_SECONDS    = 60.0     # short — user just performed the action
_UNIT_CATALOG_TTL_SECS = 3600.0   # long — rewritten only by /syncdata
_MAX_PLAYER_ENTRIES    = 200      # soft cap to bound memory


def _cache(bot_data: dict) -> Dict[str, Tuple[float, Any, float]]:
    """Return the cache dict, creating it on first access."""
    return bot_data.setdefault(_CACHE_KEY, {})


def _cache_get(bot_data: dict, key: str) -> Optional[Any]:
    """Return cached value if present and not expired; else None."""
    c = _cache(bot_data)
    entry = c.get(key)
    if entry is None:
        return None
    ts, value, ttl = entry
    if time.monotonic() - ts > ttl:
        c.pop(key, None)
        return None
    return value


def _cache_set(bot_data: dict, key: str, value: Any, ttl: float) -> None:
    """Store a value with its TTL. Prunes opportunistically when full."""
    c = _cache(bot_data)
    c[key] = (time.monotonic(), value, ttl)
    # Soft cap: if we're over the limit, drop the oldest entry. This is
    # O(n) but n is small (≤200) and only runs when adding past the cap.
    if len(c) > _MAX_PLAYER_ENTRIES:
        oldest_key = min(c.items(), key=lambda kv: kv[1][0])[0]
        c.pop(oldest_key, None)


# ---------------------------------------------------------------------------
# Public data shape
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PlayerOmicronState:
    """
    Parsed view of one Comlink /player response.

    Both dicts are keyed by *stable IDs* (skill_id, base_id) so the caller
    can map them to whatever display keys it needs. relic_by_base_id
    values follow extract_relic_tiers_by_base_id semantics:
      None  → ship (relics don't apply)
      0..14 → raw Comlink currentTier; convert with rp.relic_level()
    """
    skill_tiers_by_id: Dict[str, int]
    relic_by_base_id:  Dict[str, Optional[int]]


# ---------------------------------------------------------------------------
# Unit catalog (Characters + Ships sheets) — 1h cache
# ---------------------------------------------------------------------------

def load_unit_catalog(
    ss, bot_data: dict
) -> Tuple[Dict[str, str], Dict[str, bool]]:
    """
    Returns (base_id_to_name, is_ship_by_base).

    Cached in bot_data for 1h. The catalog is rewritten only by /syncdata
    (monthly cron), so an hour-stale view is fine; the cost of being
    minutes stale here is "the bot doesn't know about a brand-new
    character" — which a /syncdata-then-/omicrones sequence resolves.
    """
    cached = _cache_get(bot_data, "unit_catalog")
    if cached is not None:
        return cached

    base_to_name: Dict[str, str] = {}
    is_ship: Dict[str, bool] = {}

    def _ingest(sheet_name: str, mark_as_ship: bool) -> None:
        try:
            ws = ss.worksheet(sheet_name)
        except Exception as e:
            log.warning("Cannot open unit catalog sheet %r: %s", sheet_name, e)
            return
        headers, rows = svc_sheets._get_all(ws)
        hl = [h.strip().lower() for h in headers]
        if "base_id" not in hl or "name" not in hl:
            log.warning("Sheet %r missing base_id/Name columns", sheet_name)
            return
        i_b = hl.index("base_id")
        i_n = hl.index("name")
        for r in rows:
            base = (r[i_b] if i_b < len(r) else "").strip()
            name = (r[i_n] if i_n < len(r) else "").strip()
            if base and name:
                base_to_name[base] = name
                is_ship[base] = mark_as_ship

    _ingest(bot_cfg.CHARACTERS_SHEET, mark_as_ship=False)
    _ingest(bot_cfg.SHIPS_SHEET,      mark_as_ship=True)

    data = (base_to_name, is_ship)
    _cache_set(bot_data, "unit_catalog", data, _UNIT_CATALOG_TTL_SECS)
    return data


# ---------------------------------------------------------------------------
# Player state fetch
# ---------------------------------------------------------------------------

async def fetch_player_state(
    *,
    player_id: str,
    bot_data: dict,
    is_ship_by_base: Optional[Dict[str, bool]] = None,
    timeout_seconds: float = 10.0,
    ttl_seconds: float = _PLAYER_TTL_SECONDS,
) -> PlayerOmicronState:
    """
    Fetch + parse one player's roster from Comlink.

    Cached in bot_data for `ttl_seconds`. The cache key is the player_id;
    multiple users sharing a guild who happen to share a player_id
    (impossible but harmless) would share the cache entry.

    Raises:
      asyncio.TimeoutError — if Comlink doesn't respond within
        `timeout_seconds`. Caller should show a "retry" message.
      Exception — anything raised by the underlying HTTP call. Caller
        should catch broadly and show a generic error message.

    Why kwargs-only:
      The argument list will grow over time (auth tokens, locale, etc.).
      Keyword-only locks call sites to be explicit.
    """
    if not player_id:
        raise ValueError("player_id is required")

    cached = _cache_get(bot_data, f"player:{player_id}")
    if cached is not None:
        log.debug("Comlink player cache HIT for %s", player_id)
        return cached
    log.debug("Comlink player cache MISS for %s — fetching", player_id)

    # Run the blocking HTTP call in a thread, with a hard async timeout.
    # asyncio.wait_for cancels the *waiting*, not the underlying request
    # — but the bot returns control to the user immediately, which is
    # what matters for UX. The orphaned request will complete and be
    # discarded.
    resp = await asyncio.wait_for(
        asyncio.to_thread(fetch_player_by_id, player_id),
        timeout=timeout_seconds,
    )

    roster = rp.extract_roster(resp)
    state = PlayerOmicronState(
        skill_tiers_by_id=rp.extract_skill_tiers_by_id(roster),
        relic_by_base_id=rp.extract_relic_tiers_by_base_id(
            roster, is_ship_by_base or {},
        ),
    )
    _cache_set(bot_data, f"player:{player_id}", state, ttl_seconds)
    return state


def invalidate_player_cache(bot_data: dict, player_id: str) -> None:
    """
    Drop a specific player's cached state. Available for future commands
    that perform actions and want the next /omicrones to see fresh data.
    """
    _cache(bot_data).pop(f"player:{player_id}", None)
