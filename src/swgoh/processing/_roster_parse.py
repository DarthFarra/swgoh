# src/swgoh/processing/_roster_parse.py
"""
Pure helpers for parsing a Comlink /player response.

Lives in `processing` (not `bot`) because both the batch sync
(`sync_guilds.py`) and the interactive bot consume Comlink player data
and must agree on how skills and relics are read. Duplicating the
parsing in two places invites drift; one shared module fixes both.

Everything here is pure: no I/O, no global state, no logging side
effects. That makes it trivial to unit-test and safe to reuse.

----------------------------------------------------------------------
TIER NUMBERING CONVENTION — read before touching tier code below
----------------------------------------------------------------------
The number written to Player_Skills (via this module) and to
CharactersOmicrons.tier (via sync_data.run()) is always the IN-GAME
tier — the number a player sees on their screen in SWGOH.

That's not what Comlink returns natively:
  - Comlink's player skill `tier` field is the 0-indexed position in
    the skill's tiers list.
  - The tiers list itself starts at in-game tier 2 — the base ability
    (in-game tier 1) is not in the list.
  - Net: in_game_tier = comlink_raw_tier + 2.

Likewise the CharactersOmicrons.tier column is written as
`enumerate(tiers, start=1) + 1` in sync_data.run() so it matches.

Picking the in-game number as the canonical convention means humans
debugging the sheet read the same numbers they see in-game. The cost
is one `+2` here and one `+1` in sync_data — both with comments.
----------------------------------------------------------------------
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
# sync_guilds aliases this as RELIC_MAP for backwards-compatibility.
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
# Tier conversion constant
# ---------------------------------------------------------------------------

# Comlink raw tier → in-game tier offset.
# Confirmed empirically at two tier levels:
#   in-game 8 (omicron applied) → comlink raw 6
#   in-game 6 (zeta - 1)        → comlink raw 4
COMLINK_TO_INGAME_TIER_OFFSET = 2


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
    Returns {skill_id: max_in_game_tier} across all units in the roster.

    The returned tier is the IN-GAME tier (what the user sees on their
    screen). See the module docstring for the offset reasoning.

    If the same skill_id appears under multiple units (shouldn't happen
    in normal data, but we don't trust the wire), the maximum tier wins.

    Tolerates the several field-name variants the Comlink schema has
    used: `skill` / `skills` / `skillList`, and tier under any of
    `tier` / `currentTier` / `selectedTier` / `tierIndex`.

    A skill present in the roster with comlink_raw_tier=0 is included
    with in-game-tier=2 (because in-game tier 1 = base ability is
    implicit and never returned). The FIRST occurrence of a skill_id
    must always be recorded, even if the value would be the floor — see
    the sentinel check below.

    No domain filtering happens here. Callers apply their own filters
    (e.g. sync_guilds excludes skill IDs containing 'SUMMON' and
    intersects with the zeta/omicron catalog).
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

            # Convert Comlink's 0-indexed tier-list-position-starting-at-2
            # into the in-game tier number. See module docstring.
            tier_int += COMLINK_TO_INGAME_TIER_OFFSET

            # Sentinel check, NOT "tier_int > existing or 0". The latter
            # would silently drop the first occurrence when the converted
            # tier equals 0, defeating the point of tracking it.
            if sid not in out or tier_int > out[sid]:
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
