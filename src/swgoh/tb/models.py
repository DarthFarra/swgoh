# src/swgoh/tb/models.py
"""
Dataclass models for parsed Territory Battle snapshots.

Design rules:
  - All numeric fields are int — never str. The parser is responsible for
    converting CG's stringified large numbers (galacticPower, score, etc.)
    once at the boundary so downstream code never deals with them.
  - Frozen + slots: snapshots are immutable post-parse. Any "derived"
    views (exception lists, formatted summaries) live in tb/analysis.py
    and return new objects, not mutated ones.
  - Keyed by stable IDs. Members keyed by playerId, zones keyed by zoneId.
    Display names live alongside as fields, but lookups use IDs because
    player names can change mid-TB if someone renames.
  - Per-phase data is sparse on purpose: only phases that appear in the
    JSON are present. Phase 1 of a 6-phase TB is often absent because
    nothing meaningful happened there for this guild.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional


# ---------------------------------------------------------------------------
# Building blocks
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class CategoryCounts:
    """
    The six contribution counters CG tracks per member, per phase,
    and per zone. Not every counter is meaningful at every granularity
    (e.g. unit_donated at the zone level is per-territory deployments;
    at the phase level it's the sum across territories) — but the
    schema is uniform, which keeps queries simple.

    All fields default to 0 so a member who never participated still
    has a valid record. None would force every consumer to do
    null-checks for no benefit.
    """
    summary: int = 0          # total points contributed
    power: int = 0            # GP deployed
    unit_donated: int = 0     # squads/ships placed in territories
    strike_attempt: int = 0   # combat mission attempts
    strike_encounter: int = 0 # combat mission wins
    covert_attempt: int = 0   # special mission attempts
    covert_complete: int = 0  # special mission wins
    disobey: int = 0          # actions against prohibited zones


@dataclass(frozen=True, slots=True)
class Member:
    """
    One guild member's identity + roster GP at the time of the export.
    Per-phase contributions live separately in TBSnapshot.phase_stats
    keyed by player_id — keeping them off Member makes sorting and
    filtering by identity vs by participation independent operations.
    """
    player_id: str
    player_name: str
    galactic_power: int
    ship_galactic_power: int
    character_galactic_power: int


@dataclass(frozen=True, slots=True)
class PhaseStats:
    """
    Per-member contribution counters for a single phase.
    Maps player_id -> CategoryCounts. A member missing from this dict
    contributed nothing to this phase (treat as all-zero).
    """
    phase: int
    by_player: Dict[str, CategoryCounts] = field(default_factory=dict)

    def for_player(self, player_id: str) -> CategoryCounts:
        """Returns the player's counters, or all-zeroes if absent."""
        return self.by_player.get(player_id, CategoryCounts())


@dataclass(frozen=True, slots=True)
class ZoneStats:
    """
    State of a single territory ('zone' in CG's API).
    zone_state encodes the lifecycle:
      1 = locked, 2 = unlocked/active, 3 = completed, 4 = finalized.
    Exact semantics confirmed empirically — CG doesn't document these.
    """
    zone_id: str          # raw, e.g. "tb3_mixed_phase04_conflict02"
    phase: int            # extracted from zone_id; 0 if unparseable
    zone_type: str        # "conflict" | "strike" | "recon" | "covert"
    zone_state: int       # 1..4 (see docstring)
    score: int            # contributions or completion count, type-dependent
    players_participated: Optional[int] = None  # only set for strike/covert


# ---------------------------------------------------------------------------
# Root snapshot
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class TBSnapshot:
    """
    Complete parsed view of one C3PO TB export.

    Identity:
      instance_id   - e.g. "TB_EVENT_TB3_MIXED:O1778518800000"; uniquely
                      identifies a single TB occurrence.
      definition_id - e.g. "t05D"; identifies the map variant (TB3 in this
                      case). Useful for cross-event comparisons.
      guild_id, guild_name - the guild this snapshot is for.
      guild_gp      - total guild GP at export time.

    Progress:
      current_round - 1-based; the phase that's active when this was taken.
                      A finished TB will still have a current_round equal
                      to the final phase (the data doesn't auto-roll over).
      round_end_time_utc - when the current phase ends. Used for the
                      "X hours until phase end" line in officer summaries.
      map_completed_early - guild finished before the timer ran out.

    Data:
      members       - all 50 (or fewer) guild members keyed by player_id.
      phase_stats   - phase_number -> PhaseStats. Sparse by design (see
                      module docstring).
      total_stats   - the global non-round-suffixed counters. Equivalent
                      to summing across phases, but the JSON gives them
                      to us pre-aggregated so we keep them — useful for
                      sanity checks ("phase totals add to global total").
      zones         - all conflict/strike/recon/covert zones keyed by
                      zone_id. The keys preserve the type prefix via the
                      zone_type field on each ZoneStats.

    Meta:
      snapshot_taken_at - when *we* parsed this. NOT when CG produced it
                      (that's not in the JSON). Useful for "data is X
                      minutes old" hints in Telegram.
    """
    # Identity
    instance_id: str
    definition_id: str
    guild_id: str
    guild_name: str
    guild_gp: int

    # Progress
    current_round: int
    round_end_time_utc: Optional[datetime]
    map_completed_early: bool

    # Data
    members: Dict[str, Member]
    phase_stats: Dict[int, PhaseStats]
    total_stats: Dict[str, CategoryCounts]   # keyed by player_id
    zones: Dict[str, ZoneStats]

    # Per-zone per-member summary contribution: zone_id -> {player_id -> points}.
    # Only `summary` is captured here (not all 8 counters) — adding the others
    # would 8x memory for no current consumer. If we later need per-zone strike
    # or covert breakdowns, add them as parallel dicts then.
    # A missing (zone_id, player_id) pair means zero contribution.
    zone_member_summary: Dict[str, Dict[str, int]] = field(default_factory=dict)

    # Per-zone per-member GP deployed: zone_id -> {player_id -> power}.
    # Same structure as zone_member_summary but captures the `power_zone_*`
    # entries from currentStat. Used to compute "GP undeployed to active zones"
    # in the auto-message header. Empty for snapshots parsed by an older
    # version of the parser (degrades the undeployed-GP display gracefully).
    zone_member_power: Dict[str, Dict[str, int]] = field(default_factory=dict)

    # Meta
    snapshot_taken_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # ----- convenience accessors -----

    @property
    def phases_present(self) -> list[int]:
        """Sorted list of phases that have any stats data."""
        return sorted(self.phase_stats.keys())

    @property
    def member_count(self) -> int:
        return len(self.members)

    def member(self, player_id: str) -> Optional[Member]:
        return self.members.get(player_id)
