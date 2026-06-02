# src/swgoh/tb/parser.py
"""
Parse a C3PO TB export JSON into a TBSnapshot.

Input shape (abbreviated — see a real export for full detail):
    {
      "instanceId":  "TB_EVENT_TB3_MIXED:O1778518800000",
      "definitionId": "t05D",
      "currentRound": 6,
      "currentRoundEndTime": "1779037202000",   # Unix millis, string!
      "mapCompletedEarly": false,
      "profile": {"id": "...", "name": "...", "guildGalacticPower": "..."},
      "member": [ {playerId, playerName, galacticPower, ...}, ... ],
      "currentStat": [ {mapStatId, playerStat: [{memberId, score}, ...]}, ... ],
      "conflictZoneStatus": [ {zoneStatus: {zoneId, zoneState, score}}, ... ],
      "strikeZoneStatus":   [ {playersParticipated, zoneStatus: {...}}, ... ],
      "reconZoneStatus":    [ {platoon, zoneStatus: {...}}, ... ],
      "covertZoneStatus":   [ {zoneStatus, playersParticipated, ...}, ... ],
    }

Key parsing decisions:

* All numeric fields in the JSON are strings (CG's API does this to avoid
  JS number-precision loss for large GP values). We convert exactly once,
  at the boundary, via _to_int. Downstream code never sees a stringified
  number.

* `mapStatId` is the only structured key we have to interpret. It comes
  in three flavors:
      "summary"                                 -> global total
      "summary_round_3"                         -> phase 3 total
      "summary_zone_tb3_mixed_phase04_conflict02" -> per-zone total
  Plus the same patterns for the other five families: power, unit_donated,
  strike_attempt, strike_encounter, covert_attempt, covert_complete, disobey.
  Plus the bonus `covert_complete_mission_<full_mission_id>` which we
  currently ignore (it tells us which specific covert mission was cleared
  but the playerStat array is empty for these; the per-zone breakdown
  covers our needs).

* Zone IDs encode phase + position: "tb3_mixed_phase04_conflict02" -> phase 4.
  We extract phase generically via string ops so this works for TB1/TB2/TB3
  without hardcoded patterns. If the format ever changes, phase falls back
  to 0 rather than crashing.

* Unknown stat families and malformed zone IDs are logged at WARNING and
  skipped. A single bad entry must not crash the whole parse.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .models import (
    CategoryCounts,
    Member,
    PhaseStats,
    Platoon,
    PlatoonSlot,
    PlatoonSquad,
    ReconZone,
    TBSnapshot,
    ZoneStats,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class ParseError(ValueError):
    """Raised when the input JSON is not a recognizable TB export."""


# ---------------------------------------------------------------------------
# Stat family handling
#
# The eight category counters we recognize, mapped from the prefix used in
# mapStatId to the dataclass field name in CategoryCounts.
#
# Order matters for prefix matching: "strike_encounter" must be tried
# before "strike_attempt" would be — except that they don't share a
# prefix in either direction, so order is actually fine. But
# "covert_complete" vs "covert_complete_mission_..." DOES share a prefix,
# so we handle that case explicitly below.
# ---------------------------------------------------------------------------

_CATEGORY_PREFIXES: Tuple[Tuple[str, str], ...] = (
    ("strike_encounter",  "strike_encounter"),
    ("strike_attempt",    "strike_attempt"),
    ("covert_complete",   "covert_complete"),
    ("covert_attempt",    "covert_attempt"),
    ("unit_donated",      "unit_donated"),
    ("disobey",           "disobey"),
    ("summary",           "summary"),
    ("power",             "power"),
)

# Per-mission stat families. Unlike the standard families above, these don't
# have a "_round_N" variant — CG only provides per-mission totals with the
# phase encoded inside the mission ID. We aggregate them by phase ourselves.
#
# Format: "<prefix>_mission_<mission_id>" where the mission_id contains
# "_phaseNN_" somewhere (e.g. "covert_complete_mission_tb3_mixed_phase03_conflict01_covert01").
#
# Only "covert_complete_mission_" is here because CG does NOT provide
# "covert_complete_round_N" totals — these per-mission entries are the
# only source of per-phase covert completion counts.
#
# We deliberately do NOT include "covert_round_attempted_mission_" even
# though it has the same structure: "covert_attempt_round_N" already
# provides per-phase totals, and including both would double-count.
_PER_MISSION_PREFIXES: Tuple[Tuple[str, str], ...] = (
    ("covert_complete_mission_", "covert_complete"),
)


def _category_field(stat_id: str) -> Optional[str]:
    """
    Return the CategoryCounts field name for a mapStatId, or None
    if this stat doesn't map to a tracked category.

    Examples:
      "summary"                                 -> "summary"
      "summary_round_3"                         -> "summary"
      "summary_zone_tb3_mixed_..."              -> "summary"
      "covert_complete"                         -> "covert_complete"
      "covert_complete_mission_tb3_..."         -> "covert_complete" (per-mission)
      "weird_new_thing"                         -> None (unknown family)
    """
    # Per-mission detail first — these don't have round/zone infixes,
    # but they still represent real contributions and must not be silently dropped.
    for prefix, field_name in _PER_MISSION_PREFIXES:
        if stat_id.startswith(prefix):
            return field_name

    for prefix, field_name in _CATEGORY_PREFIXES:
        # Match either exact (global total), "<prefix>_round_N", or
        # "<prefix>_zone_..." — i.e. either the bare prefix or the
        # prefix followed by "_".
        if stat_id == prefix or stat_id.startswith(prefix + "_"):
            return field_name
    return None


def _phase_from_mission_id(stat_id: str) -> Optional[int]:
    """
    Extract the phase number from a per-mission stat_id.
    The mission id is everything after "<prefix>_mission_", and contains
    "_phaseNN_" — we delegate to _phase_from_zone_id which does the same
    extraction for zone ids.

    Example: "covert_complete_mission_tb3_mixed_phase03_conflict01_covert01"
             -> 3
    """
    for prefix, _ in _PER_MISSION_PREFIXES:
        if stat_id.startswith(prefix):
            return _phase_from_zone_id(stat_id[len(prefix):])
    return None


def _stat_granularity(stat_id: str) -> Tuple[str, Optional[int], Optional[str]]:
    """
    Classify a mapStatId by granularity.
    Returns (granularity, phase, zone_id):
      ("total",   None, None)              for "summary"
      ("phase",   3,    None)              for "summary_round_3"
      ("phase",   3,    None)              for "covert_complete_mission_..._phase03_..."
                                             (per-mission entries aggregated into their phase)
      ("zone",    4,    "tb3_mixed_...")   for "summary_zone_tb3_mixed_phase04_..."
      ("unknown", None, None)              otherwise

    Note that the phase number for "zone" and per-mission entries is
    extracted from the embedded id, not the stat-key prefix itself.
    """
    # Per-mission entries: phase comes from the mission id.
    for prefix, _ in _PER_MISSION_PREFIXES:
        if stat_id.startswith(prefix):
            phase = _phase_from_mission_id(stat_id)
            return ("phase", phase, None) if phase is not None else ("unknown", None, None)

    if "_round_" in stat_id:
        # e.g. "summary_round_3" -> phase 3
        try:
            phase_str = stat_id.rsplit("_round_", 1)[1]
            return ("phase", int(phase_str), None)
        except (IndexError, ValueError):
            return ("unknown", None, None)

    if "_zone_" in stat_id:
        # e.g. "summary_zone_tb3_mixed_phase04_conflict02"
        zone_id = stat_id.split("_zone_", 1)[1]
        return ("zone", _phase_from_zone_id(zone_id), zone_id)

    # Bare prefix = global total
    if _category_field(stat_id) is not None:
        return ("total", None, None)

    return ("unknown", None, None)


def _phase_from_zone_id(zone_id: str) -> Optional[int]:
    """
    Extract the phase number from a zone_id like
    "tb3_mixed_phase04_conflict02" -> 4.
    Returns None if no phaseNN segment is found.
    """
    # Split on "_phase" so we get "04_conflict02" (or "04_conflict02_bonus")
    # in the second element. Then the leading digits are the phase number.
    parts = zone_id.split("_phase", 1)
    if len(parts) != 2:
        return None
    tail = parts[1]
    # Read leading digits.
    digits = []
    for ch in tail:
        if ch.isdigit():
            digits.append(ch)
        else:
            break
    if not digits:
        return None
    try:
        return int("".join(digits))
    except ValueError:
        return None


def _zone_type_from_id(zone_id: str) -> str:
    """
    Identify zone type from its id by looking for the segment after phase.

    Examples:
      "tb3_mixed_phase04_conflict02"          -> "conflict"
      "tb3_mixed_phase04_conflict02_strike01" -> "strike"
      "tb3_mixed_phase04_conflict02_covert01" -> "covert"
      "tb3_mixed_phase04_conflict01_bonus"    -> "conflict"  (bonus is a flavor of conflict)
    Recon zones don't appear in conflictZoneStatus, so callers pass type
    explicitly when they know it. This function is the fallback for cases
    where the type must be inferred.
    """
    # Search for the most specific suffix first.
    if "_strike" in zone_id:
        return "strike"
    if "_covert" in zone_id:
        return "covert"
    if "_recon" in zone_id:
        return "recon"
    return "conflict"


# ---------------------------------------------------------------------------
# Primitive converters
# ---------------------------------------------------------------------------

def _to_int(value: Any, default: int = 0) -> int:
    """
    Convert CG's stringified-or-int numeric values to int.
    Returns `default` on missing or unparseable values rather than raising —
    a single bad cell shouldn't fail the whole parse, and 0 is the natural
    identity for the counters we're parsing.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        # bool is a subclass of int; treat True/False as 1/0 explicitly
        # rather than accidentally getting 1 for "True" in counters.
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            try:
                # Some fields may come as "1.0e9"-style scientific notation
                # under unusual conditions. Falling back to float -> int
                # is safe for our value ranges (no precision loss until ~1e15).
                return int(float(value))
            except ValueError:
                return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_dt_utc(millis_value: Any) -> Optional[datetime]:
    """
    Convert a Unix-millis value (string or int) to a UTC datetime.
    Returns None on failure rather than raising — a missing timestamp
    should degrade gracefully (we just can't show "time remaining").
    """
    ms = _to_int(millis_value, default=0)
    if ms <= 0:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Section parsers — each takes one slice of the JSON and returns its
# typed representation. Keeping them separate makes the top-level
# parse_tb_snapshot a readable orchestrator and each piece easy to test.
# ---------------------------------------------------------------------------

def _parse_members(member_list: Any) -> Dict[str, Member]:
    """Build the player_id -> Member dict from the `member` array."""
    if not isinstance(member_list, list):
        log.warning("Expected `member` to be a list, got %s", type(member_list))
        return {}

    result: Dict[str, Member] = {}
    for entry in member_list:
        if not isinstance(entry, dict):
            continue
        player_id = entry.get("playerId") or ""
        if not player_id:
            continue
        result[player_id] = Member(
            player_id=player_id,
            player_name=str(entry.get("playerName", "")),
            galactic_power=_to_int(entry.get("galacticPower")),
            ship_galactic_power=_to_int(entry.get("shipGalacticPower")),
            character_galactic_power=_to_int(entry.get("characterGalacticPower")),
        )
    return result


def _parse_zone_block(
    zone_list: Any,
    fallback_type: str,
) -> Dict[str, ZoneStats]:
    """
    Parse one of the *ZoneStatus arrays into zone_id -> ZoneStats.

    fallback_type names the zone category for entries where the zone_id
    doesn't itself reveal it (e.g. conflictZoneStatus entries have plain
    'tb3_mixed_phase04_conflict02' ids with no type suffix).
    """
    if not isinstance(zone_list, list):
        return {}

    result: Dict[str, ZoneStats] = {}
    for entry in zone_list:
        if not isinstance(entry, dict):
            continue
        zs = entry.get("zoneStatus")
        if not isinstance(zs, dict):
            continue
        zone_id = zs.get("zoneId") or ""
        if not zone_id:
            continue

        # Type comes from the zone_id when discriminable, else fallback.
        inferred = _zone_type_from_id(zone_id)
        zone_type = inferred if inferred != "conflict" else fallback_type

        result[zone_id] = ZoneStats(
            zone_id=zone_id,
            phase=_phase_from_zone_id(zone_id) or 0,
            zone_type=zone_type,
            zone_state=_to_int(zs.get("zoneState")),
            score=_to_int(zs.get("score")),
            players_participated=(
                _to_int(entry["playersParticipated"])
                if "playersParticipated" in entry
                else None
            ),
        )
    return result

def _parse_recon_zones(zone_list: Any) -> Dict[str, "ReconZone"]:
    """
    Parse the reconZoneStatus array into a dict of typed ReconZone entries.
 
    Why a separate function from _parse_zone_block:
      _parse_zone_block reads zoneStatus and produces ZoneStats — the
      generic per-zone metadata. Recon zones carry an additional
      `platoon` array with deeply nested structure (platoon → squad →
      unit), which has no analog in conflict/strike/covert zones.
      Keeping the recon-specific parsing here makes _parse_zone_block
      simpler and surfaces the platoon model in its own dedicated
      function.
 
    Defensive parsing (consistent with the rest of parser.py):
      - Malformed entries are logged at debug level and skipped.
      - A single bad platoon/squad/unit doesn't poison the whole zone.
      - Returns an empty dict (not None) when input is missing —
        downstream consumers can iterate without null checks.
 
    Unit identifier normalization:
      The export's `unitIdentifier` carries a rarity suffix for filled
      slots (e.g. "CAPITALLEVIATHAN:SEVEN_STAR") but only the base id
      for empty slots ("CAPITALLEVIATHAN"). We strip the suffix
      uniformly so downstream consumers see a single canonical id
      regardless of fill state.
    """
    if not isinstance(zone_list, list):
        return {}
 
    result: Dict[str, "ReconZone"] = {}
 
    for entry in zone_list:
        if not isinstance(entry, dict):
            continue
        zs = entry.get("zoneStatus")
        if not isinstance(zs, dict):
            continue
        zone_id = zs.get("zoneId") or ""
        if not zone_id:
            continue
 
        platoon_list = entry.get("platoon")
        if not isinstance(platoon_list, list):
            # Zone exists but has no platoon data — store empty.
            result[zone_id] = ReconZone(zone_id=zone_id, platoons=tuple())
            continue
 
        platoons: list = []
        for p in platoon_list:
            if not isinstance(p, dict):
                continue
            platoon_n = _parse_platoon_number(p.get("id", ""))
            if platoon_n is None:
                log.debug(
                    "Skipping platoon with unparseable id %r in recon zone %r",
                    p.get("id"), zone_id,
                )
                continue
 
            squad_list = p.get("squad")
            if not isinstance(squad_list, list):
                continue
 
            squads: list = []
            for sq in squad_list:
                if not isinstance(sq, dict):
                    continue
                squad_n = _parse_squad_number(sq.get("id", ""))
                if squad_n is None:
                    log.debug(
                        "Skipping squad with unparseable id %r in zone %r platoon %d",
                        sq.get("id"), zone_id, platoon_n,
                    )
                    continue
 
                unit_list = sq.get("unit")
                if not isinstance(unit_list, list):
                    continue
 
                units: list = []
                for u in unit_list:
                    if not isinstance(u, dict):
                        continue
                    unit_id = _normalize_unit_id(u.get("unitIdentifier", ""))
                    member_id = str(u.get("memberId") or "").strip()
                    units.append(PlatoonSlot(
                        unit_id=unit_id,
                        member_id=member_id,
                    ))
 
                squads.append(PlatoonSquad(
                    squad_number=squad_n,
                    units=tuple(units),
                ))
 
            platoons.append(Platoon(
                platoon_number=platoon_n,
                squads=tuple(squads),
            ))
 
        result[zone_id] = ReconZone(
            zone_id=zone_id,
            platoons=tuple(platoons),
        )
 
    return result
 
 
def _parse_platoon_number(platoon_id: str) -> Optional[int]:
    """Parse 'tb3-platoon-6' → 6. Returns None if id doesn't match."""
    prefix = "tb3-platoon-"
    if not platoon_id or not platoon_id.lower().startswith(prefix):
        return None
    tail = platoon_id[len(prefix):]
    try:
        return int(tail)
    except ValueError:
        return None
 
 
def _parse_squad_number(squad_id: str) -> Optional[int]:
    """Parse 'tb3-squad-01' → 1. Returns None if id doesn't match."""
    prefix = "tb3-squad-"
    if not squad_id or not squad_id.lower().startswith(prefix):
        return None
    tail = squad_id[len(prefix):]
    try:
        return int(tail)
    except ValueError:
        return None
 
 
def _normalize_unit_id(raw: str) -> str:
    """
    Strip the rarity suffix from a unit identifier.
 
    "CAPITALLEVIATHAN:SEVEN_STAR" → "CAPITALLEVIATHAN"
    "CAPITALLEVIATHAN"            → "CAPITALLEVIATHAN"
    ""                            → ""
 
    The rarity is meaningful for the game but not for our lookups
    (the Characters sheet keys by base id only).
    """
    if not raw:
        return ""
    colon = raw.find(":")
    return raw[:colon] if colon != -1 else raw

def _parse_current_stat(
    stat_list: Any,
    known_player_ids: set[str],
) -> Tuple[
    Dict[int, PhaseStats],
    Dict[str, CategoryCounts],
    Dict[str, Dict[str, int]],
    Dict[str, Dict[str, int]],
]:
    """
    Process the currentStat array into:
      - phase_stats: phase_number -> PhaseStats (per-member counters per phase)
      - total_stats: player_id    -> CategoryCounts (global totals across the map)
      - zone_member_summary: zone_id -> {player_id -> summary points contributed}
      - zone_member_power: zone_id -> {player_id -> GP deployed}

    For phase rollups: zone-granularity stats are NOT counted into phase totals
    (CG already provides per-phase rollups via `<prefix>_round_N`, and the
    sums equal each other — verified empirically).

    For zone-level data: we capture two families:
      * summary  — used for "contributors per zone" and "platoon math"
      * power    — used for "undeployed GP to active zones" in the header

    Other zone-level families (strike, covert) are skipped to keep memory
    bounded. If a future feature needs them, add parallel dicts here.

    Returns ({}, {}, {}, {}) for malformed input rather than raising —
    same fail-soft principle as elsewhere.
    """
    if not isinstance(stat_list, list):
        log.warning("Expected `currentStat` to be a list, got %s", type(stat_list))
        return {}, {}, {}, {}

    # Mutable accumulators keyed by (phase, player_id) and player_id.
    # We materialize CategoryCounts (frozen) only once at the end.
    phase_accum: Dict[int, Dict[str, Dict[str, int]]] = {}
    total_accum: Dict[str, Dict[str, int]] = {}
    zone_summary_accum: Dict[str, Dict[str, int]] = {}
    zone_power_accum: Dict[str, Dict[str, int]] = {}

    unknown_seen: set[str] = set()

    for stat in stat_list:
        if not isinstance(stat, dict):
            continue
        stat_id = stat.get("mapStatId") or ""
        if not stat_id:
            continue

        field_name = _category_field(stat_id)
        if field_name is None:
            # per-mission detail or future-unknown family — skip silently
            # for per-mission (loud noise otherwise), warn-once for unknown.
            if not stat_id.startswith("covert_complete_mission_") \
                    and stat_id not in unknown_seen:
                log.debug("Skipping unknown mapStatId family: %r", stat_id)
                unknown_seen.add(stat_id)
            continue

        granularity, phase, zone_id = _stat_granularity(stat_id)

        if granularity == "unknown":
            log.debug("Unparseable mapStatId granularity: %r", stat_id)
            continue

        # For zone-level: we care about summary (contributor count, platoon
        # math) and power (undeployed-GP calculation). Skip other zone-level
        # families — they'd grow the dict without a consumer.
        if granularity == "zone" and field_name not in ("summary", "power"):
            continue

        player_stats = stat.get("playerStat") or []
        if not isinstance(player_stats, list):
            continue

        for ps in player_stats:
            if not isinstance(ps, dict):
                continue
            member_id = ps.get("memberId") or ""
            if not member_id or member_id not in known_player_ids:
                # Member referenced in stats but not in `member` list —
                # could be a player who left mid-TB. Skip; including them
                # would mean we have stats with no name to display.
                continue
            score = _to_int(ps.get("score"))
            if score == 0:
                # Zero-score entries are noise — they don't change
                # the all-zero default. Skip to keep dicts small.
                continue

            if granularity == "total":
                bucket = total_accum.setdefault(member_id, {})
                bucket[field_name] = bucket.get(field_name, 0) + score
            elif granularity == "phase":
                assert phase is not None  # narrowed by granularity check
                ph_bucket = phase_accum.setdefault(phase, {}).setdefault(member_id, {})
                ph_bucket[field_name] = ph_bucket.get(field_name, 0) + score
            else:  # "zone" — only field_name in ("summary", "power") reaches here
                assert zone_id is not None
                if field_name == "summary":
                    zone_summary_accum.setdefault(zone_id, {})[member_id] = score
                else:  # "power"
                    zone_power_accum.setdefault(zone_id, {})[member_id] = score

    # Materialize into frozen CategoryCounts.
    phase_stats_out: Dict[int, PhaseStats] = {}
    for phase, members in phase_accum.items():
        by_player = {
            pid: CategoryCounts(**fields)
            for pid, fields in members.items()
        }
        phase_stats_out[phase] = PhaseStats(phase=phase, by_player=by_player)

    total_stats_out: Dict[str, CategoryCounts] = {
        pid: CategoryCounts(**fields)
        for pid, fields in total_accum.items()
    }

    return phase_stats_out, total_stats_out, zone_summary_accum, zone_power_accum


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------

def parse_tb_snapshot(raw: Dict[str, Any]) -> TBSnapshot:
    """
    Parse a decoded C3PO TB export JSON into a TBSnapshot.

    Args:
        raw: the dict obtained from json.load / json.loads on a C3PO
             export file.

    Returns:
        A fully populated TBSnapshot. Missing optional fields default
        sensibly (empty dicts, None timestamps, 0 counters).

    Raises:
        ParseError: if the input does not look like a TB export at all
                    (missing instanceId/profile/member). This is a
                    fail-loud signal that we received the wrong kind of
                    file — distinct from internal field-level issues
                    which fail-soft.
    """
    if not isinstance(raw, dict):
        raise ParseError(f"Expected top-level JSON object, got {type(raw).__name__}")

    instance_id = raw.get("instanceId")
    profile     = raw.get("profile") or {}
    members_raw = raw.get("member")

    if not instance_id or not isinstance(profile, dict) or members_raw is None:
        raise ParseError(
            "Input does not look like a TB export "
            "(missing instanceId, profile, or member)"
        )

    members = _parse_members(members_raw)
    known_ids = set(members.keys())

    phase_stats, total_stats, zone_member_summary, zone_member_power = _parse_current_stat(
        raw.get("currentStat"),
        known_player_ids=known_ids,
    )

    # Merge all zone arrays into one dict, with fallback types per source.
    zones: Dict[str, ZoneStats] = {}
    zones.update(_parse_zone_block(raw.get("conflictZoneStatus"), "conflict"))
    zones.update(_parse_zone_block(raw.get("strikeZoneStatus"),   "strike"))
    zones.update(_parse_zone_block(raw.get("reconZoneStatus"),    "recon"))
    zones.update(_parse_zone_block(raw.get("covertZoneStatus"),   "covert"))

    # Recon zones get a richer parse than _parse_zone_block does — we
    # preserve the platoon → squad → unit nested structure so the
    # platoon-missing feature can analyze fill state.
    recon_zones = _parse_recon_zones(raw.get("reconZoneStatus"))

    snapshot = TBSnapshot(
        instance_id=str(instance_id),
        definition_id=str(raw.get("definitionId", "")),
        guild_id=str(profile.get("id", "")),
        guild_name=str(profile.get("name", "")),
        guild_gp=_to_int(profile.get("guildGalacticPower")),
        current_round=_to_int(raw.get("currentRound")),
        round_end_time_utc=_to_dt_utc(raw.get("currentRoundEndTime")),
        map_completed_early=bool(raw.get("mapCompletedEarly", False)),
        members=members,
        phase_stats=phase_stats,
        total_stats=total_stats,
        zones=zones,
        recon_zones=recon_zones,
        zone_member_summary=zone_member_summary,
        zone_member_power=zone_member_power,
    )

    log.info(
        "Parsed TB snapshot: guild=%r instance=%s round=%d "
        "members=%d phases=%s zones=%d",
        snapshot.guild_name,
        snapshot.instance_id,
        snapshot.current_round,
        snapshot.member_count,
        snapshot.phases_present,
        len(snapshot.zones),
    )
    return snapshot
