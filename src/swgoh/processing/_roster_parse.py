# src/swgoh/processing/_roster_parse.py
"""
Pure helpers for parsing a Comlink /player response.

Lives in `processing` (not `bot`) because both the batch sync
(`sync_guilds.py`) and the interactive bot consume Comlink player data
and must agree on how skills and relics are read. Duplicating the
parsing in two places invites drift; one shared module fixes both.

Everything here is pure: no I/O, no global state, no logging side
effects. That makes it trivial to unit-test and safe to reuse.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Canonical mapping: Comlink raw `relic.currentTier` integer → display text.
#
# The convention in the codebase is:
#   currentTier = 0  → "<G13"  (sub-relic; gear 12 or lower)
#   currentTier = 1  → "R0"    (just unlocked relics, level 0)
#   currentTier = 14 → "R13"   (max)
#
# Keep this map as the single source of truth. sync_guilds aliases it
# as RELIC_MAP for backwards-compatibility.
# ---------------------------------------------------------------------------

RELIC_DISPLAY: Dict[int, str] = {
    14: "R13", 13: "R12", 12: "R11", 11: "R10", 10: "R9",
    9:  "R8",  8: "R7",  7: "R6",  6: "R5",  5: "R4",
    4:  "R3",  3: "R2",  2: "R1",  1: "R0",  0: "<G13",
}


def relic_display(current_tier: int) -> str:
    """Comlink currentTier → display string ('R5' / '<G13')."""
    return RELIC_DISPLAY.get(current_tier, "<G13")


def relic_level(current_tier: int) -> int:
    """
    Comlink currentTier → relic-level integer.

    Returns:
      -1 for sub-relic ('<G13'),
       0 for R0,
       5 for R5,
      13 for R13.

    Derived from RELIC_DISPLAY so the two functions can't drift.
    """
    s = relic_display(current_tier)
    if s == "<G13":
        return -1
    # s starts with 'R' followed by digits — guaranteed by the map content.
    return int(s[1:])


# ---------------------------------------------------------------------------
# Roster extraction
# ---------------------------------------------------------------------------

def extract_roster(player_response: Any) -> List[Dict[str, Any]]:
    """
    Return the rosterUnit list from a Comlink /player response, trying
    the several envelope shapes the API has used historically.

    Returns [] (never raises) so callers can keep their flow simple.
    """
    if not isinstance(player_response, dict):
        return []
    candidates = (
        ("rosterUnit",),
        ("player", "rosterUnit"),
        ("data", "rosterUnit"),
        ("payload", "rosterUnit"),
    )
    for path in candidates:
        cur: Any = player_response
        for key in path:
            if isinstance(cur, dict):
                cur = cur.get(key)
            else:
                cur = None
                break
        if isinstance(cur, list):
            return cur
    return []


def _base_id(unit: Dict[str, Any]) -> str:
    """Extract BASEID from a unit's definitionId like 'BASEID:...'."""
    defid = str(unit.get("definitionId") or "").strip()
    if not defid:
        return ""
    return defid.split(":", 1)[0]


# ---------------------------------------------------------------------------
# Skills
# ---------------------------------------------------------------------------

def extract_skill_tiers_by_id(roster: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Returns {skill_id: max_tier_int} across all units in the roster.

    If the same skill_id appears under multiple units (shouldn't happen
    in normal data, but we don't trust the wire), the maximum tier wins —
    matching what sync_guilds.py used to do inline.

    Tolerates the several field-name variants the Comlink schema has
    used: `skill` / `skills` / `skillList`, and tier under any of
    `tier` / `currentTier` / `selectedTier` / `tierIndex`.

    No filtering happens here. Callers apply their own filters
    (e.g. sync_guilds excludes skill IDs containing 'SUMMON').
    """
    out: Dict[str, int] = {}
    for unit in roster or []:
        if not isinstance(unit, dict):
            continue
        skills = (
            unit.get("skill")
            or unit.get("skills")
            or unit.get("skillList")
            or []
        )
        if not isinstance(skills, list):
            continue
        for s in skills:
            if not isinstance(s, dict):
                continue
            sid_raw = s.get("id") or s.get("skillId") or s.get("idRef")
            if not sid_raw:
                continue
            sid = str(sid_raw).strip()
            if not sid:
                continue

            tier_raw = s.get("tier")
            if tier_raw is None:
                tier_raw = s.get(
                    "currentTier",
                    s.get("selectedTier", s.get("tierIndex", 0)),
                )
            try:
                tier_int = int(tier_raw)
            except (TypeError, ValueError):
                tier_int = 0

            if tier_int > out.get(sid, 0):
                out[sid] = tier_int
    return out


# ---------------------------------------------------------------------------
# Relics
# ---------------------------------------------------------------------------

def extract_relic_tiers_by_base_id(
    roster: List[Dict[str, Any]],
    is_ship_by_base: Optional[Dict[str, bool]] = None,
) -> Dict[str, Optional[int]]:
    """
    Returns {base_id: comlink_currentTier_int_or_None_if_ship}.

    None means the unit is a ship (relics don't apply).
    Integer 0..14 is the raw Comlink relic.currentTier value (NOT a
    relic level — convert with relic_level() if needed).

    Behaviour notes:
      - Empty/missing relic dict → 0 (sub-relic). Same as sync_guilds.
      - is_ship_by_base is optional. Without it, no unit is marked as a
        ship; callers that don't care about ships can omit it.
    """
    out: Dict[str, Optional[int]] = {}
    is_ship_by_base = is_ship_by_base or {}

    for unit in roster or []:
        if not isinstance(unit, dict):
            continue
        base = _base_id(unit)
        if not base:
            continue
        if is_ship_by_base.get(base, False):
            out[base] = None
            continue
        rel = unit.get("relic")
        if not isinstance(rel, dict):
            out[base] = 0
            continue
        try:
            out[base] = int(rel.get("currentTier") or 0)
        except (TypeError, ValueError):
            out[base] = 0
    return out
