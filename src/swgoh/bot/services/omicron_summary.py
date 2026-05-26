# src/swgoh/bot/services/omicron_summary.py
"""
Aggregation service for /omicronsummary: per-player omicron counts per
mode, plus per-skill rollups for the inline-button drill-down.

Architecture notes:
  - This module is the *per-guild* roll-up. omicrons.py is the
    *per-player* recommendation engine for /omicrones. They share
    read_omicron_catalog() and OmicronEntry but otherwise serve
    different commands.
  - Player state caching (60s TTL) is handled in comlink_player.py.
    Repeated /omicronsummary invocations within 60s reuse those entries
    automatically — no extra logic here.
  - The aggregated summary itself is cached for ~10 min, keyed by a
    short opaque token, so the mode drill-down buttons don't have to
    re-aggregate or re-fetch.
  - Aggregation is a pure function (`aggregate`) — easy to unit-test
    without touching Comlink or gspread.
"""
from __future__ import annotations

import asyncio
import logging
import secrets
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

from .omicrons import OmicronEntry
from .comlink_player import fetch_player_state, PlayerOmicronState

log = logging.getLogger(__name__)


# Mode abbreviations for the table header. Falls back to the full name
# for any mode not listed here (e.g., a brand-new mode CG adds).
MODE_SHORT: Dict[str, str] = {
    "Territory War":       "TW",
    "Grand Arena":         "GAC",
    "Territory Battles":   "TB",
    "Conquest":            "CQ",
    "Galactic Challenges": "GC",
}


def mode_short(mode_text: str) -> str:
    return MODE_SHORT.get(mode_text, mode_text)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PlayerOmicronCounts:
    """One row in the summary table: a single player's per-mode counts."""
    alias: str
    counts_by_mode: Dict[str, int]
    fetch_ok: bool  # False → row will render '—' for each mode


@dataclass(frozen=True)
class GuildOmicronSummary:
    """
    All the data the renderers (table + drill-down) need.

    guild_id is stored so the drill-down handler can re-check that the
    clicker is still an officer in this guild — defence in depth in case
    the cache token leaks.
    """
    guild_id: str
    guild_name: str
    guild_label: str
    modes: List[str]                                  # alphabetical, stable
    catalog_totals: Dict[str, int]                    # mode → total in catalog
    catalog_by_mode: Dict[str, List[OmicronEntry]]    # mode → entries (for drill-down ordering)
    players: List[PlayerOmicronCounts]                # sorted by alias
    skill_counts_by_mode: Dict[str, Dict[str, int]]   # mode → skill_key → players_with_omicron
    failed_player_aliases: List[str]                  # for footer
    total_players: int                                # convenience: len(players)
    elapsed_seconds: float


# ---------------------------------------------------------------------------
# Drill-down cache (lives in application.bot_data)
# ---------------------------------------------------------------------------

_CACHE_KEY = "__omisum_cache__"
_CACHE_TTL = 600.0  # 10 minutes — long enough to click around, short enough to bound memory


def cache_summary(bot_data: dict, summary: GuildOmicronSummary) -> str:
    """
    Store summary in the cache, return a short opaque token to embed in
    callback_data. The token is generated with `secrets`, so it can't be
    guessed across summaries.
    """
    token = secrets.token_urlsafe(6)  # ~8 chars; well under callback_data 64-byte limit
    cache = bot_data.setdefault(_CACHE_KEY, {})
    cache[token] = (time.monotonic(), summary)

    # Opportunistic pruning. Cache stays small in practice.
    now = time.monotonic()
    expired = [k for k, (ts, _) in cache.items() if now - ts > _CACHE_TTL]
    for k in expired:
        cache.pop(k, None)

    return token


def get_cached_summary(bot_data: dict, token: str) -> Optional[GuildOmicronSummary]:
    """Return cached summary if still valid; else None (caller shows expiry message)."""
    cache = bot_data.setdefault(_CACHE_KEY, {})
    entry = cache.get(token)
    if entry is None:
        return None
    ts, val = entry
    if time.monotonic() - ts > _CACHE_TTL:
        cache.pop(token, None)
        return None
    return val


# ---------------------------------------------------------------------------
# Parallel fetcher with progress reporting
# ---------------------------------------------------------------------------

ProgressCb = Optional[Callable[[int, int], Awaitable[None]]]


async def fetch_all_players(
    *,
    players: List[Tuple[str, str]],
    bot_data: dict,
    is_ship_by_base: Dict[str, bool],
    concurrency: int = 5,
    timeout_seconds: float = 10.0,
    progress: ProgressCb = None,
) -> Dict[str, Optional[PlayerOmicronState]]:
    """
    Fan out fetch_player_state() across `players` with bounded concurrency.

    Returns {player_id: state_or_None}. None means the per-player fetch
    failed — the caller treats it as "no data" and renders that row as
    failed, rather than aborting the whole summary.

    progress(done, total) is awaited after each player completes so the
    caller can update a Telegram message. Errors raised by `progress` are
    swallowed — progress reporting must never abort the fetch.

    Concurrency=5 is conservative; Comlink rate limits have been observed
    around 10-20 req/s. Tune via the caller if needed.
    """
    sem = asyncio.Semaphore(concurrency)

    async def _one(alias: str, pid: str) -> Tuple[str, Optional[PlayerOmicronState]]:
        async with sem:
            try:
                state = await fetch_player_state(
                    player_id=pid,
                    bot_data=bot_data,
                    is_ship_by_base=is_ship_by_base,
                    timeout_seconds=timeout_seconds,
                )
                return pid, state
            except asyncio.TimeoutError:
                log.warning("Comlink timeout for %s (%s)", alias, pid)
                return pid, None
            except Exception:
                log.exception("Comlink fetch failed for %s (%s)", alias, pid)
                return pid, None

    tasks = [asyncio.create_task(_one(a, p)) for a, p in players]
    results: Dict[str, Optional[PlayerOmicronState]] = {}
    total = len(tasks)

    for fut in asyncio.as_completed(tasks):
        pid, state = await fut
        results[pid] = state
        if progress is not None:
            try:
                await progress(len(results), total)
            except Exception:
                log.debug("Progress callback raised", exc_info=True)

    return results


# ---------------------------------------------------------------------------
# Pure aggregation: fetched states + catalog → summary
# ---------------------------------------------------------------------------

def aggregate(
    *,
    guild_id: str,
    guild_name: str,
    guild_label: str,
    players: List[Tuple[str, str]],
    fetched: Dict[str, Optional[PlayerOmicronState]],
    catalog: List[OmicronEntry],
    elapsed_seconds: float,
) -> GuildOmicronSummary:
    """
    Pure function. Build the summary from fetched player states.

    Counts an omicron as 'applied' iff player_tier >= entry.omicron_tier
    (both on the in-game tier scale; see _roster_parse for why).

    For players whose fetch failed, the row is still emitted (so the
    table is dense and aliases are in alphabetical order without gaps);
    counts are zero and fetch_ok=False. The renderer shows '—' for
    these cells and a footer counting them.
    """
    modes = sorted({e.mode_text for e in catalog}, key=str.lower)

    catalog_totals: Dict[str, int] = {m: 0 for m in modes}
    catalog_by_mode: Dict[str, List[OmicronEntry]] = {m: [] for m in modes}
    for e in catalog:
        catalog_totals[e.mode_text] = catalog_totals.get(e.mode_text, 0) + 1
        catalog_by_mode.setdefault(e.mode_text, []).append(e)

    skill_counts_by_mode: Dict[str, Dict[str, int]] = {m: {} for m in modes}
    player_rows: List[PlayerOmicronCounts] = []
    failed: List[str] = []

    for alias, pid in players:
        state = fetched.get(pid)
        if state is None:
            failed.append(alias)
            player_rows.append(PlayerOmicronCounts(
                alias=alias,
                counts_by_mode={m: 0 for m in modes},
                fetch_ok=False,
            ))
            continue

        per_mode = {m: 0 for m in modes}
        for entry in catalog:
            player_tier = state.skill_tiers_by_id.get(entry.skill_id, 0)
            if player_tier >= entry.omicron_tier:
                per_mode[entry.mode_text] = per_mode.get(entry.mode_text, 0) + 1
                bucket = skill_counts_by_mode.setdefault(entry.mode_text, {})
                bucket[entry.skill_key] = bucket.get(entry.skill_key, 0) + 1

        player_rows.append(PlayerOmicronCounts(
            alias=alias, counts_by_mode=per_mode, fetch_ok=True,
        ))

    player_rows.sort(key=lambda p: p.alias.lower())

    return GuildOmicronSummary(
        guild_id=guild_id,
        guild_name=guild_name,
        guild_label=guild_label,
        modes=modes,
        catalog_totals=catalog_totals,
        catalog_by_mode=catalog_by_mode,
        players=player_rows,
        skill_counts_by_mode=skill_counts_by_mode,
        failed_player_aliases=failed,
        total_players=len(player_rows),
        elapsed_seconds=elapsed_seconds,
    )
