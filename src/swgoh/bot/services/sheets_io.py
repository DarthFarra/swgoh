# src/swgoh/bot/services/sheets_io.py
"""
Cached sheet read helper.

Centralises both the cache wiring and the per-sheet TTL profile so
TTLs aren't scattered across handlers. To add a new sheet to the
cached read path, add an entry to SHEET_TTLS or fall through to the
heuristic / default.

All read callers should prefer read_values_cached() over calling
gspread directly. If you genuinely need fresh data, call
sheets_cache.invalidate(...) immediately before — but the answer is
almost always "the TTL is short enough; just use the cached read."

Operator convention:
  After officer-driven sheet edits (ROTE swaps, OmicronPriorities
  tuning, manual fixes to Guilds/Usuarios), run /refreshcache so the
  next read hits the API instead of serving stale cached data.
"""
from __future__ import annotations

import logging
from typing import List, Tuple

from ... import config as cfg
from ...bot.jobs.send_assignments_daily import _read_all_values
from . import sheets_cache

log = logging.getLogger(__name__)


# Per-sheet TTLs in seconds. Centralised so they're easy to tune.
# Sheets not listed here fall through to the heuristic (_ttl_for)
# and ultimately to sheets_cache.DEFAULT_TTL_SECS.
_FIVE_MIN          = 300.0
_THIRTY_MIN        = 1800.0
_ONE_HOUR          = 3600.0
_TWENTY_FOUR_HOURS = 86400.0

SHEET_TTLS = {
    # User registrations — most active write path is /register; eventual
    # consistency is acceptable per design (users don't run commands
    # immediately after registering).
    cfg.SHEET_USERS:               _FIVE_MIN,

    # Guild/player metadata — updated weekly via /syncguild (manual
    # command and scheduled cron). 1-hour TTL absorbs traffic spikes
    # without risking meaningful staleness.
    cfg.SHEET_GUILDS:              _ONE_HOUR,
    cfg.SHEET_PLAYERS:             _ONE_HOUR,
    cfg.SHEET_PLAYER_UNITS:        _ONE_HOUR,
    cfg.SHEET_PLAYER_SKILLS:       _ONE_HOUR,

    # Omicron catalog — rewritten monthly by /syncdata. Very stable;
    # 24h TTL is safe because /syncdata is followed by /refreshcache
    # when the catalog changes meaningfully.
    cfg.SHEET_CHARACTERS_OMICRONS: _TWENTY_FOUR_HOURS,

    # Officer-curated priorities. Manual edits but rare; officers
    # run /refreshcache after tuning.
    cfg.SHEET_OMICRON_PRIORITIES:  _THIRTY_MIN,

    # Ticket snapshots — written once per day by the snapshot job;
    # 1-hour TTL is conservative for what is effectively a daily file.
    cfg.SHEET_TICKET_SNAPSHOTS:    _ONE_HOUR,

    # NOTE: SHEET_CHARACTERS and SHEET_SHIPS are intentionally absent.
    # They're written by /syncdata but not read by the bot at runtime;
    # caching them would waste memory and add invalidation surface for
    # no benefit. If a future command starts reading them, add an
    # entry here.
}

# ROTE / assignment sheet tab names vary per guild and are configured
# in the Guilds sheet. They change only between TBs (member swaps),
# always before TB starts, so 1-hour TTL plus the /refreshcache
# convention is sufficient.
_ROTE_TTL = _ONE_HOUR


def _ttl_for(sheet_name: str) -> float:
    """Resolve the TTL for a sheet name."""
    if sheet_name in SHEET_TTLS:
        return SHEET_TTLS[sheet_name]

    # Heuristic for ROTE/assignment tabs that vary in name per guild.
    # Override an individual tab by adding it explicitly to SHEET_TTLS.
    lowered = sheet_name.lower()
    if lowered.startswith("rote") or "asignaciones" in lowered:
        return _ROTE_TTL

    log.debug("sheets_io: no TTL profile for %r — using default", sheet_name)
    return sheets_cache.DEFAULT_TTL_SECS


def read_values_cached(
    ss, sheet_name: str,
) -> Tuple[List[str], List[List[str]]]:
    """
    Cached version of _read_all_values from send_assignments_daily.

    Returns (headers, rows) for the given sheet. Uses the in-memory
    cache when possible; falls through to a live read on miss/expiry.

    TTL is determined by SHEET_TTLS / _ttl_for(). To force-refresh,
    call sheets_cache.invalidate_all() (the /refreshcache command does
    this).
    """
    key = f"{ss.id}:{sheet_name}"
    hit = sheets_cache.get(key)
    if hit is not None:
        return hit
    value = _read_all_values(ss, sheet_name)
    sheets_cache.set_(key, value, _ttl_for(sheet_name))
    return value
