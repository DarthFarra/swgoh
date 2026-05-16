# src/swgoh/bot/services/omicrons.py
"""
Omicron recommendation service.

Responsibilities (single-responsibility per function):
  - Read the omicron catalog (`CharactersOmicrons` sheet).
  - Read per-guild/mode priorities (`OmicronPriorities` sheet).
  - Compute, with a pure function, which prioritised omicrons the
    player is still missing for a given mode.

Player skill / relic data does NOT live here anymore — it's fetched
live from Comlink (see services/comlink_player.py) so the user sees the
result of their latest in-game upgrade rather than the last weekly sync.

The catalog tab is fully rewritten by `sync_data.run()` once per month,
so we never write to it from here. The priorities tab is owned by
officers and only auto-created (with headers) if missing — never
overwritten.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .. import config as bot_cfg
from . import sheets as svc_sheets

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sheet / column names
# ---------------------------------------------------------------------------

SHEET_OMICRONS_CATALOG:   str = bot_cfg.CHAR_OMICRONS_SHEET
SHEET_OMICRON_PRIORITIES: str = bot_cfg.OMICRON_PRIORITIES_SHEET

# CharactersOmicrons headers we depend on (written by sync_data.run())
COL_SKILL_ID = "skillid"
COL_OMI_MODE_TEXT = "omicronModeText"
COL_CHAR_NAME = "CharacterName"
COL_CHAR_SKILL = "CharacterName|skill name"  # composite key
COL_TIER = "tier"  # added by the sync_data patch

# OmicronPriorities expected headers (auto-created if sheet missing)
PRIORITIES_HEADERS = ["Guild Name", "Mode", "Skill", "Priority", "Notes"]


# ---------------------------------------------------------------------------
# Data classes — keep the public surface explicit and typed
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OmicronEntry:
    """One catalog row for an omicron skill."""
    skill_id: str
    character: str
    skill_key: str           # "CharacterName|skill name"
    mode_text: str           # e.g. "Grand Arena"
    omicron_tier: int        # 1-based tier index where isOmicronTier=true


@dataclass(frozen=True)
class PriorityEntry:
    """One row from the OmicronPriorities sheet."""
    skill_key: str
    rank: int                # lower number = higher priority
    notes: str = ""


@dataclass(frozen=True)
class Recommendation:
    """One row in the output to the user."""
    rank: int
    character: str
    skill_key: str
    notes: str
    player_relic: Optional[int]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    """Lowercase + collapse whitespace. Used for case-insensitive matching."""
    return " ".join(str(s or "").lower().split())


def _ensure_priorities_sheet(ss):
    """
    Create the OmicronPriorities tab with headers if it doesn't exist.

    Lazy so officers don't have to provision it manually. We never touch
    contents — only create-with-headers if missing, so manual edits are
    always safe.
    """
    try:
        return ss.worksheet(SHEET_OMICRON_PRIORITIES)
    except Exception:
        log.info("Creating missing sheet %r with default headers.", SHEET_OMICRON_PRIORITIES)
        ws = ss.add_worksheet(
            title=SHEET_OMICRON_PRIORITIES,
            rows=200,
            cols=len(PRIORITIES_HEADERS),
        )
        ws.update(range_name="1:1", values=[PRIORITIES_HEADERS])
        return ws


# ---------------------------------------------------------------------------
# Catalog reader (CharactersOmicrons)
# ---------------------------------------------------------------------------

def read_omicron_catalog(ss) -> List[OmicronEntry]:
    """
    Returns every omicron skill row in the catalog.

    Returns [] if the sheet is missing or malformed — logs a warning so
    the operator can spot the issue without the bot dying.
    """
    try:
        ws = ss.worksheet(SHEET_OMICRONS_CATALOG)
    except Exception as e:
        log.warning("Cannot open omicron catalog sheet %r: %s", SHEET_OMICRONS_CATALOG, e)
        return []

    headers, rows = svc_sheets._get_all(ws)
    hl = [h.strip().lower() for h in headers]

    required = [COL_SKILL_ID, COL_OMI_MODE_TEXT, COL_CHAR_NAME, COL_CHAR_SKILL, COL_TIER]
    idx: Dict[str, int] = {}
    for c in required:
        if c.lower() not in hl:
            log.warning(
                "Omicron catalog sheet %r is missing required column %r — "
                "did you run /syncdata after adding the tier column?",
                SHEET_OMICRONS_CATALOG, c,
            )
            return []
        idx[c] = hl.index(c.lower())

    out: List[OmicronEntry] = []
    for r in rows:
        def cell(c: str) -> str:
            i = idx[c]
            return (r[i] if i < len(r) else "").strip()

        skill_id = cell(COL_SKILL_ID)
        mode = cell(COL_OMI_MODE_TEXT)
        char = cell(COL_CHAR_NAME)
        skill_key = cell(COL_CHAR_SKILL)
        tier_raw = cell(COL_TIER)

        if not (skill_id and mode and skill_key):
            continue
        try:
            tier_int = int(tier_raw)
        except (TypeError, ValueError):
            log.debug("Skipping omicron %s — non-integer tier %r", skill_id, tier_raw)
            continue

        out.append(OmicronEntry(
            skill_id=skill_id,
            character=char,
            skill_key=skill_key,
            mode_text=mode,
            omicron_tier=tier_int,
        ))
    return out


def list_omicron_modes(ss) -> List[str]:
    """Distinct mode names found in the catalog, sorted alphabetically."""
    seen: Dict[str, str] = {}  # norm → original casing (first wins)
    for e in read_omicron_catalog(ss):
        k = _norm(e.mode_text)
        if k and k not in seen:
            seen[k] = e.mode_text
    return sorted(seen.values(), key=str.lower)


# ---------------------------------------------------------------------------
# Priorities reader (OmicronPriorities)
# ---------------------------------------------------------------------------

def read_omicron_priorities(ss, guild_name: str, mode_text: str) -> List[PriorityEntry]:
    """
    Read priorities for one (guild, mode). Returns list ordered by Priority asc.

    Tolerant of:
      - Empty Priority cells (skipped silently)
      - Mixed casing in Guild Name / Mode
      - Duplicate Skill rows (last one wins; logged)

    Auto-creates the sheet (with headers only) if missing — never writes
    data, so manual edits are always safe.
    """
    ws = _ensure_priorities_sheet(ss)
    headers, rows = svc_sheets._get_all(ws)
    hl = [h.strip().lower() for h in headers]

    required = ["guild name", "mode", "skill", "priority"]
    for c in required:
        if c not in hl:
            log.warning(
                "OmicronPriorities sheet missing column %r — got headers %r",
                c, headers,
            )
            return []

    i_g = hl.index("guild name")
    i_m = hl.index("mode")
    i_s = hl.index("skill")
    i_p = hl.index("priority")
    i_n = hl.index("notes") if "notes" in hl else None

    target_g = _norm(guild_name)
    target_m = _norm(mode_text)

    by_skill: Dict[str, PriorityEntry] = {}

    for r in rows:
        def cell(i: int) -> str:
            return (r[i] if i < len(r) else "").strip()

        if _norm(cell(i_g)) != target_g:
            continue
        if _norm(cell(i_m)) != target_m:
            continue
        skill_key = cell(i_s)
        prio_raw = cell(i_p)
        if not skill_key or not prio_raw:
            continue
        try:
            rank = int(prio_raw)
        except ValueError:
            log.debug("Skipping priority row — non-integer priority %r for skill %r",
                      prio_raw, skill_key)
            continue
        notes = cell(i_n) if i_n is not None else ""
        norm_key = _norm(skill_key)
        if norm_key in by_skill:
            log.info("Duplicate priority for %r in %s/%s — using last",
                     skill_key, guild_name, mode_text)
        by_skill[norm_key] = PriorityEntry(skill_key=skill_key, rank=rank, notes=notes)

    return sorted(by_skill.values(), key=lambda e: e.rank)


# ---------------------------------------------------------------------------
# Pure recommendation engine
# ---------------------------------------------------------------------------

def compute_recommendations(
    *,
    catalog: List[OmicronEntry],
    priorities: List[PriorityEntry],
    player_skill_tiers: Dict[str, int],
    player_relics: Dict[str, Optional[int]],
    mode_text: str,
    min_relic: int,
    top_n: int,
) -> Tuple[List[Recommendation], Dict[str, int]]:
    """
    Pure function. Given the inputs, returns (recommendations, stats).

    Inputs:
      catalog            — full omicron catalog (any mode).
      priorities         — guild's priority list for the target mode.
      player_skill_tiers — {skill_key_norm: max_tier_int}. Skill keys
                           in the form "CharacterName|skill name", lowercased.
      player_relics      — {character_name_norm: relic_level_int|None}.
                           None = ship (excluded), -1 = <G13, 0..13 = R0..R13.
      mode_text          — mode to compute for; case-insensitive.
      min_relic          — minimum relic level a character must have to
                           be eligible.
      top_n              — max number of recommendations.

    Algorithm:
      1. Restrict catalog to entries matching mode_text.
      2. For each priority (sorted by rank ascending — done here so
         callers can't accidentally break ordering):
         a. Find the corresponding catalog entry.
            No match → counted as "priorities_unmatched".
         b. If player_skill_tiers ≥ omicron_tier → already have, skip.
         c. If character not owned → exclude_not_owned, skip.
         d. If character relic < min_relic → exclude_low_relic, skip.
         e. Otherwise → include in recommendations.
      3. Return top_n.

    Returns recommendations only for omicrons that have a priority entry —
    unranked omicrons are deliberately not recommended (no guild
    direction means no opinion).
    """
    mode_norm = _norm(mode_text)

    catalog_by_key: Dict[str, OmicronEntry] = {
        _norm(e.skill_key): e
        for e in catalog
        if _norm(e.mode_text) == mode_norm
    }

    # Walk priorities in rank order. Sort defensively so callers can't
    # accidentally break the output by passing an unsorted list.
    sorted_priorities = sorted(priorities, key=lambda e: e.rank)

    recs: List[Recommendation] = []
    matched = 0
    already_have = 0
    excluded_low_relic = 0
    excluded_not_owned = 0
    unmatched_priority = 0

    for p in sorted_priorities:
        key = _norm(p.skill_key)
        entry = catalog_by_key.get(key)
        if entry is None:
            unmatched_priority += 1
            continue

        matched += 1
        current_tier = player_skill_tiers.get(key, 0)
        if current_tier >= entry.omicron_tier:
            already_have += 1
            continue

        relic = player_relics.get(_norm(entry.character))
        if relic is None:
            excluded_not_owned += 1
            continue
        if relic < min_relic:
            excluded_low_relic += 1
            continue

        recs.append(Recommendation(
            rank=p.rank,
            character=entry.character,
            skill_key=entry.skill_key,
            notes=p.notes,
            player_relic=relic,
        ))
        if len(recs) >= top_n:
            break

    stats = {
        "priorities_total": len(priorities),
        "priorities_matched_catalog": matched,
        "priorities_unmatched": unmatched_priority,
        "already_have": already_have,
        "excluded_not_owned": excluded_not_owned,
        "excluded_low_relic": excluded_low_relic,
        "recommended": len(recs),
    }
    return recs, stats
