# src/swgoh/tb/analysis.py
"""
Derived queries on top of a parsed TBSnapshot.

Design rules:
  - Pure functions only. No I/O, no caching, no global state.
    A function takes a snapshot (+ params), returns a value. Trivially
    testable in isolation.
  - Every "exception list" function returns a list (possibly empty).
    Callers compose them; the formatter decides how to present them.
  - Result types are small frozen dataclasses, not raw tuples. This
    keeps formatter code readable (gap.member.player_name) and avoids
    positional-tuple bugs when fields are reordered.
  - `phase` parameter is optional everywhere and defaults to
    snap.current_round. Callers can override to inspect past phases.
  - Thresholds (deployment %, top-N) are parameters with sensible
    defaults — never hardcoded inside the function body.

What's deliberately *not* here:
  - Time-since-phase-start gating. The analysis layer reports facts;
    "should we suppress this if the phase just started" is a policy
    decision that belongs in the auto-forward logic, not here.
  - Cross-TB historical comparisons. Would require a snapshot history
    we don't keep (per your decision to not persist JSONs).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from enum import Enum

from .models import (
    CategoryCounts,
    Member,
    PhaseStats,
    TBSnapshot,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Defaults — exposed as module constants so tests and overrides can find them
# ---------------------------------------------------------------------------

DEFAULT_DEPLOYMENT_THRESHOLD_PCT: float = 0.95
DEFAULT_TOP_N: int = 5


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class DeploymentGap:
    """
    One member's deployment shortfall in a given phase.

    Carries enough denormalized data (member name, raw numbers) that the
    formatter never has to look anything else up. Recomputing pct here
    avoids divide-by-zero scattered across the formatter.
    """
    member: Member
    phase: int
    deployed: int          # power for this phase
    roster: int            # member.galactic_power
    remaining: int         # max(0, roster - deployed)
    pct_deployed: float    # 0..1+; can exceed 1.0 if CG over-credits (rare)


@dataclass(frozen=True, slots=True)
class SpecialFailure:
    """
    A member who attempted specials and didn't complete them all.
    `failed` is attempts minus completes (clamped to >=0 in case CG
    completes-without-attempts ever happens; not seen, but cheap to guard).
    """
    member: Member
    phase: int
    attempted: int
    completed: int
    failed: int


@dataclass(frozen=True, slots=True)
class Contribution:
    """
    Ranking row used by top_contributors / member_phase_breakdown.
    `metric` names which CategoryCounts field is the sort key, so the
    formatter can label the column correctly without re-deriving it.
    """
    member: Member
    phase: Optional[int]   # None means "across the whole map"
    counts: CategoryCounts
    metric: str            # e.g. "summary", "power", "strike_encounter"

    @property
    def value(self) -> int:
        """The metric's value for this row."""
        return getattr(self.counts, self.metric)


@dataclass(frozen=True, slots=True)
class PhaseProgress:
    """
    Aggregate progress for a single phase.

    All values are guild-wide sums across members. Zone-state counts let
    the formatter say "4/6 strikes cleared" without re-querying zones.
    """
    phase: int
    total_summary: int
    total_power: int
    total_unit_donated: int
    total_strike_attempts: int
    total_strike_encounters: int
    total_covert_attempts: int
    total_covert_completes: int
    members_with_any_activity: int      # how many members have summary > 0
    members_total: int


@dataclass(frozen=True, slots=True)
class PlatoonStatus:
    """
    Platoon completion info for one planet.

    Platoons are all-or-nothing: a partially-filled platoon (1-14 of 15
    units donated) gives 0 score. The `completed` count is derived from
    the zone score arithmetic: `(zone_score - member_summary_sum) /
    points_per_platoon`, rounded to nearest int. Small rounding artifacts
    (~0.01% of zone score) appear in real CG data — those are tolerated.

    Fields:
      completed         - how many platoons are fully filled (0..platoon_count)
      total             - total platoons on the planet (typically 6)
      points_per_platoon - bonus points per completed platoon
      points_earned     - completed × points_per_platoon
      points_remaining  - (total − completed) × points_per_platoon
    """
    completed: int
    total: int
    points_per_platoon: int
    points_earned: int
    points_remaining: int

    @property
    def is_complete(self) -> bool:
        return self.completed >= self.total


@dataclass(frozen=True, slots=True)
class StrikeMissionStatus:
    """
    One combat mission's status.

    Fields:
      zone_id              - full strike zone id, e.g. "..._strike01"
      players_participated - attempts so far (max attempts = members_total)
      members_total        - guild size, the denominator in "23/50"
      total_score          - cumulative score earned on this mission
      avg_score            - total_score / participated (0 if no attempts)
      estimated_potential  - avg × (members_total − participated)
      is_complete          - whether the zone is in state 3 or 4
    """
    zone_id: str
    players_participated: int
    members_total: int
    total_score: int
    avg_score: int
    estimated_potential: int
    is_complete: bool

    @property
    def remaining_attempts(self) -> int:
        return max(0, self.members_total - self.players_participated)


@dataclass(frozen=True, slots=True)
class ThresholdGap:
    """
    A single unreached threshold.

    Fields:
      value         - the score threshold (target)
      stars         - stars granted at this threshold (0 for reward-only)
      points_short  - how many points are still needed to reach it
    """
    value: int
    stars: int
    points_short: int

class EstimationState(str, Enum):
    """
    Discriminator for which message variant the formatter should render.
 
    Using str-Enum (rather than plain Enum) so it serializes naturally
    in logs and tests. The string values match what officers might grep
    for in logs.
    """
    NO_TARGET           = "no_target"            # planet has no target configured
    TARGET_MET          = "target_met"           # current score >= target threshold
    PLATOONS_PLUS_RESIDUAL = "platoons+residual" # gap can be filled partly by remaining platoons
    RESIDUAL_ONLY       = "residual_only"        # no platoon help available; pure GP+combat gap
 
 
@dataclass(frozen=True, slots=True)
class EstimationResult:
    """
    Per-planet "Est:" line breakdown.
 
    The formatter dispatches on `state` to pick wording. The numeric
    fields are filled per-state:
 
      NO_TARGET:           all numeric fields = 0; just informational.
      TARGET_MET:          target_threshold and target_stars meaningful;
                           gap_total = 0.
      PLATOONS_PLUS_RESIDUAL: all fields meaningful;
                           platoon_part > 0; residual = gap_total - platoon_part.
      RESIDUAL_ONLY:       platoon_part = 0; residual = gap_total.
 
    Fields:
      state               - which variant to render (see above)
      target_stars        - the configured target (0 if no target)
      target_threshold    - score needed to hit target_stars (0 if no target
                            or if all stars achieved with no threshold above)
      current_score       - planet's current score (echoed for convenience)
      gap_total           - max(0, target_threshold - current_score)
      platoon_part        - how much of gap_total can come from incomplete
                            platoons (capped at gap_total — never overshoots)
      residual            - gap_total - platoon_part; what still needs to
                            come from member deployment + combat + specials
    """
    state: EstimationState
    target_stars: int
    target_threshold: int
    current_score: int
    gap_total: int
    platoon_part: int
    residual: int
 
 


@dataclass(frozen=True, slots=True)
class PlanetReport:
    """
    Complete progress snapshot for one active planet.

    Bundles every value the formatter needs to render a planet block,
    so the formatter is purely presentational and doesn't have to call
    six functions. Designed to be cheap to compute (one pass over the
    snapshot data per planet).

    Fields:
      zone_id                       - full conflict zone id
      zone_state                    - 1=LOCKED 2=ACTIVE 3=OPEN 4=COMPLETED
      score                         - total zone score (includes platoons)
      current_stars                 - stars earned at this score (via config)
      max_stars                     - max stars possible (from config)
      thresholds_remaining          - list of unreached thresholds, ascending
      platoons                      - PlatoonStatus or None if config missing
      missions                      - list of StrikeMissionStatus, in zone_id sort order
      missions_combined_total_est   - sum of mission estimated potentials
      non_participants              - members with 0 summary contribution to this zone
    """
    zone_id: str
    zone_state: int
    score: int
    current_stars: int
    max_stars: int
    thresholds_remaining: List[ThresholdGap]
    platoons: Optional[PlatoonStatus]
    missions: List[StrikeMissionStatus]
    missions_combined_total_est: int
    non_participants: List[Member]


@dataclass(frozen=True, slots=True)
class PhaseProgress:
    """
    Aggregate progress for a single phase.

    All values are guild-wide sums across members. Zone-state counts let
    the formatter say "4/6 strikes cleared" without re-querying zones.
    """
    phase: int
    total_summary: int
    total_power: int
    total_unit_donated: int
    total_strike_attempts: int
    total_strike_encounters: int
    total_covert_attempts: int
    total_covert_completes: int
    members_with_any_activity: int      # how many members have summary > 0
    members_total: int


@dataclass(frozen=True, slots=True)
class TerritoryProgress:
    """
    Progress for a single conflict zone (a 'territory' in user-facing terms).

    Designed for the per-territory line in /tb_status:
      "Territory 2:  257.6M  ·  35/50 contributing  ✓ completed"

    Fields:
      zone_id        - raw, e.g. "tb3_mixed_phase01_conflict02"
      phase, position - derived from zone_id (position is the trailing
                        "conflict_N" number, 1-based).
      zone_state     - 1=locked, 2=open, 3=completed, 4=finalized (see ZoneStats).
      score          - the zone's total score from conflictZoneStatus.
                       This includes deployment bonuses, not just member
                       contributions. It's what counts toward star thresholds.
      contributing_members - number of members with > 0 summary points
                       contributed to this zone.
      members_total  - guild member count for the denominator.
      strikes_completed / strikes_total
                     - count of strike zones inside this conflict by state.
                       strikes_total may be 0 in early phases that have no
                       combat missions.
    """
    zone_id: str
    phase: int
    position: int           # 1-based conflict position within phase
    zone_state: int
    score: int
    contributing_members: int
    members_total: int
    strikes_completed: int
    strikes_total: int

    @property
    def is_completed(self) -> bool:
        """Zone is in a 'done' state (3 or 4)."""
        return self.zone_state in (3, 4)

    @property
    def is_locked(self) -> bool:
        """Zone is still locked (state 1)."""
        return self.zone_state == 1


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _phase_or_current(snap: TBSnapshot, phase: Optional[int]) -> int:
    """Resolve the phase argument: None → snap.current_round."""
    return phase if phase is not None else snap.current_round


def _phase_stats(snap: TBSnapshot, phase: int) -> Optional[PhaseStats]:
    """Look up a phase's stats; return None (not raise) if absent."""
    return snap.phase_stats.get(phase)


def _stable_sort_key_by_name(member: Member) -> str:
    """
    Case-insensitive name sort key. Used for output stability — exception
    lists with the same data should always print in the same order so
    diffs are meaningful and officers' eyes don't have to track movement.
    """
    return (member.player_name or "").lower()


# ---------------------------------------------------------------------------
# Exception-list queries — "who needs nagging in this phase"
# ---------------------------------------------------------------------------

def members_missing_deployment(
    snap: TBSnapshot,
    phase: Optional[int] = None,
    threshold_pct: float = DEFAULT_DEPLOYMENT_THRESHOLD_PCT,
) -> List[DeploymentGap]:
    """
    Return one DeploymentGap per member whose deployed GP is below
    `threshold_pct` of their roster GP for the given phase.

    A member with roster_gp == 0 (shouldn't happen, but defensively) is
    skipped — we can't meaningfully compute a percentage.

    Results sorted by remaining GP descending — the biggest gaps first.
    Ties broken by player name for stability.
    """
    target_phase = _phase_or_current(snap, phase)
    ps = _phase_stats(snap, target_phase)
    if ps is None:
        return []

    gaps: List[DeploymentGap] = []
    for player_id, member in snap.members.items():
        roster = member.galactic_power
        if roster <= 0:
            continue
        deployed = ps.for_player(player_id).power
        pct = deployed / roster
        if pct >= threshold_pct:
            continue
        gaps.append(DeploymentGap(
            member=member,
            phase=target_phase,
            deployed=deployed,
            roster=roster,
            remaining=max(0, roster - deployed),
            pct_deployed=pct,
        ))

    gaps.sort(
        key=lambda g: (-g.remaining, _stable_sort_key_by_name(g.member))
    )
    return gaps


def members_with_no_strikes(
    snap: TBSnapshot,
    phase: Optional[int] = None,
) -> List[Member]:
    """
    Members who have NOT attempted a single combat mission in this phase
    (strike_attempt == 0). The 'zero-only' definition we settled on:
    catches AFK, ignores partial completion.

    Sorted by player name for stability.
    """
    target_phase = _phase_or_current(snap, phase)
    ps = _phase_stats(snap, target_phase)
    if ps is None:
        return []

    out = [
        m for player_id, m in snap.members.items()
        if ps.for_player(player_id).strike_attempt == 0
    ]
    out.sort(key=_stable_sort_key_by_name)
    return out


def members_with_no_summary(
    snap: TBSnapshot,
    phase: Optional[int] = None,
) -> List[Member]:
    """
    Members with zero summary points this phase — i.e. completely AFK,
    didn't even deploy. A superset of "no strikes" usually, but the two
    are conceptually distinct (a player can deploy GP without attempting
    a strike) so we expose both.

    Sorted by player name.
    """
    target_phase = _phase_or_current(snap, phase)
    ps = _phase_stats(snap, target_phase)
    if ps is None:
        return []

    out = [
        m for player_id, m in snap.members.items()
        if ps.for_player(player_id).summary == 0
    ]
    out.sort(key=_stable_sort_key_by_name)
    return out


def members_with_failed_specials(
    snap: TBSnapshot,
    phase: Optional[int] = None,
) -> List[SpecialFailure]:
    """
    Members who attempted special missions and didn't complete them all.

    Per the design: NOT used in auto-summary. Available for officers to
    query post-phase via a dedicated /tb_failed_specials command, since
    it's review-after-the-fact information rather than something to nag
    about live.

    Sorted by failure count descending, then by name.
    """
    target_phase = _phase_or_current(snap, phase)
    ps = _phase_stats(snap, target_phase)
    if ps is None:
        return []

    out: List[SpecialFailure] = []
    for player_id, member in snap.members.items():
        c = ps.for_player(player_id)
        if c.covert_attempt <= c.covert_complete:
            # Either no attempts, or completed everything attempted.
            continue
        out.append(SpecialFailure(
            member=member,
            phase=target_phase,
            attempted=c.covert_attempt,
            completed=c.covert_complete,
            failed=max(0, c.covert_attempt - c.covert_complete),
        ))

    out.sort(
        key=lambda f: (-f.failed, _stable_sort_key_by_name(f.member))
    )
    return out


# ---------------------------------------------------------------------------
# Aggregate / progress queries
# ---------------------------------------------------------------------------

def phase_progress(
    snap: TBSnapshot,
    phase: Optional[int] = None,
) -> Optional[PhaseProgress]:
    """
    Guild-wide aggregate for one phase. Returns None if no data exists
    for that phase (callers can render "no data yet" accordingly).
    """
    target_phase = _phase_or_current(snap, phase)
    ps = _phase_stats(snap, target_phase)
    if ps is None:
        return None

    summary = power = unit_d = 0
    s_attempts = s_encounters = c_attempts = c_completes = 0
    members_active = 0

    for counts in ps.by_player.values():
        if counts.summary > 0:
            members_active += 1
        summary       += counts.summary
        power         += counts.power
        unit_d        += counts.unit_donated
        s_attempts    += counts.strike_attempt
        s_encounters  += counts.strike_encounter
        c_attempts    += counts.covert_attempt
        c_completes   += counts.covert_complete

    return PhaseProgress(
        phase=target_phase,
        total_summary=summary,
        total_power=power,
        total_unit_donated=unit_d,
        total_strike_attempts=s_attempts,
        total_strike_encounters=s_encounters,
        total_covert_attempts=c_attempts,
        total_covert_completes=c_completes,
        members_with_any_activity=members_active,
        members_total=snap.member_count,
    )


# Pattern to recognise conflict zones — i.e. territories — vs strike/covert/recon.
# Conflict zones have IDs like "tb3_mixed_phase01_conflict02" with optionally
# a "_bonus" suffix. They are top-level scoring entities, whereas strike/covert
# zones are nested combat/special missions inside a conflict.
def _is_conflict_zone(zone_id: str) -> bool:
    """
    True if zone_id is a top-level conflict (territory) zone.

    Excludes:
      - Strike/covert/recon sub-zones (they're inside conflicts, not territories).
      - "_specialmission" zones, which appear in strikeZoneStatus but are
        special-mission metadata entries (score=0, no real data) rather than
        actual territories. We discovered these in real exports — they leak
        through the simpler "no _strike/_covert/_recon" check and pollute the
        territory list with empty rows.
    """
    if "_strike" in zone_id or "_covert" in zone_id or "_recon" in zone_id:
        return False
    if zone_id.endswith("_specialmission"):
        return False
    return "_conflict" in zone_id


def _position_from_conflict_id(zone_id: str) -> int:
    """
    Extract the trailing conflict number: "tb3_mixed_phase04_conflict02" -> 2.
    Returns 0 if no number is found (defensive; shouldn't happen in real data).

    Bonus conflicts ("..._conflict03_bonus") share the position of their parent
    (3 in that example) but we treat them as their own territory for display
    purposes. We return the position number; the caller can sort by position.
    """
    # Strip "_bonus" if present so we look at the core conflict number.
    core = zone_id
    if core.endswith("_bonus"):
        core = core[: -len("_bonus")]
    # Find "conflict" followed by digits at the end.
    idx = core.rfind("_conflict")
    if idx == -1:
        return 0
    tail = core[idx + len("_conflict"):]
    digits = []
    for ch in tail:
        if ch.isdigit():
            digits.append(ch)
        else:
            break
    try:
        return int("".join(digits)) if digits else 0
    except ValueError:
        return 0


def territory_progress(
    snap: TBSnapshot,
    phase: Optional[int] = None,
    *,
    include_locked: bool = False,
) -> List[TerritoryProgress]:
    """
    Per-territory progress for a phase, ordered by position.

    A "territory" is one entry in conflictZoneStatus — a top-level
    scoring zone like "tb3_mixed_phase01_conflict02". Strike, covert,
    and recon sub-zones are aggregated into their parent conflict's
    record (strike completion counts), not returned as separate territories.

    Args:
      phase: which phase to report on. None = current_round.
      include_locked: if False (default), zones in state 1 (locked) are
        omitted from the result. Locked zones have no useful progress
        data so showing them just adds noise.

    Returns:
      Empty list if no conflict zones exist for that phase (e.g. caller
      asked about a phase that hasn't been reached). Otherwise sorted
      by position (1, 2, 3, ...).
    """
    target_phase = _phase_or_current(snap, phase)

    # Collect all conflict zones for this phase. We iterate snap.zones once
    # rather than filtering inside conflictZoneStatus, because by the time
    # we have a TBSnapshot the parser has already normalized zones into a
    # single dict keyed by zone_id.
    conflict_zones = [
        z for z in snap.zones.values()
        if z.phase == target_phase and _is_conflict_zone(z.zone_id)
    ]
    if not conflict_zones:
        return []

    # Pre-compute strike zone counts per conflict. Strike zone IDs have the
    # form "<conflict_zone_id>_strike<NN>" — we group by the prefix.
    # We exclude "_specialmission" entries which also live in strikeZoneStatus
    # but aren't real combat missions (metadata zones with score=0).
    strike_counts: Dict[str, Tuple[int, int]] = {}  # conflict_id -> (completed, total)
    for z in snap.zones.values():
        if z.phase != target_phase or z.zone_type != "strike":
            continue
        if "_strike" not in z.zone_id:
            # e.g. "_specialmission" — not a real strike zone.
            continue
        # Derive parent conflict id by stripping "_strikeNN" suffix.
        idx = z.zone_id.rfind("_strike")
        if idx == -1:
            continue
        parent = z.zone_id[:idx]
        comp, total = strike_counts.get(parent, (0, 0))
        total += 1
        if z.zone_state in (3, 4):
            comp += 1
        strike_counts[parent] = (comp, total)

    out: List[TerritoryProgress] = []
    for zone in conflict_zones:
        if not include_locked and zone.zone_state == 1:
            continue
        # Contributing members: those with > 0 summary points in this zone.
        contributing = sum(
            1 for pts in snap.zone_member_summary.get(zone.zone_id, {}).values()
            if pts > 0
        )
        strikes_comp, strikes_total = strike_counts.get(zone.zone_id, (0, 0))
        out.append(TerritoryProgress(
            zone_id=zone.zone_id,
            phase=zone.phase,
            position=_position_from_conflict_id(zone.zone_id),
            zone_state=zone.zone_state,
            score=zone.score,
            contributing_members=contributing,
            members_total=snap.member_count,
            strikes_completed=strikes_comp,
            strikes_total=strikes_total,
        ))

    # Sort by position; "_bonus" zones share the parent position number but
    # come after the parent — secondary sort on the full zone_id puts the
    # base "_conflict03" before "_conflict03_bonus" alphabetically.
    out.sort(key=lambda t: (t.position, t.zone_id))
    return out


def members_not_participating_in_zone(
    snap: TBSnapshot,
    zone_id: str,
) -> List[Member]:
    """
    Return members with zero summary contribution to a specific zone.

    Uses the `zone_member_summary` field captured by the parser. A member
    is "not participating" in a zone if they don't appear in the
    contribution map for that zone (or appear with score 0, but the parser
    drops zero entries).

    The returned list is alphabetically sorted by player name for stable
    display across exports.

    Args:
      zone_id: full conflict zone id, e.g. "tb3_mixed_phase04_conflict03_bonus"

    Returns:
      Empty list if everyone participated, or if the zone has no
      contribution data at all.
    """
    contributors = set(snap.zone_member_summary.get(zone_id, {}).keys())
    non_participants = [
        m for pid, m in snap.members.items()
        if pid not in contributors
    ]
    non_participants.sort(key=_stable_sort_key_by_name)
    return non_participants


def _strike_mission_details(
    snap: TBSnapshot,
    conflict_zone_id: str,
) -> List[StrikeMissionStatus]:
    """
    All strike (combat mission) zones inside a conflict zone, ordered by
    zone_id (so strike01 comes before strike02, etc.).

    Excludes `_specialmission` zones (metadata entries with score=0 that
    show up in strikeZoneStatus but aren't real missions).

    Skips strikes with `_strike` not in their ID — extra defensive filter
    for the same case.

    Returns empty list if no strike zones are found (e.g. for a conflict
    that has no combat missions in this TB type).
    """
    members_total = snap.member_count
    out: List[StrikeMissionStatus] = []

    for zone in snap.zones.values():
        if zone.zone_type != "strike":
            continue
        if "_strike" not in zone.zone_id:
            continue
        if "_specialmission" in zone.zone_id:
            continue
        # Strike zone IDs have the form "<conflict>_strike<NN>". Find
        # the conflict prefix by stripping the "_strike..." suffix.
        idx = zone.zone_id.rfind("_strike")
        if zone.zone_id[:idx] != conflict_zone_id:
            continue

        participated = zone.players_participated
        total_score = zone.score
        avg = total_score // participated if participated > 0 else 0
        remaining = max(0, members_total - participated)
        est_potential = avg * remaining

        out.append(StrikeMissionStatus(
            zone_id=zone.zone_id,
            players_participated=participated,
            members_total=members_total,
            total_score=total_score,
            avg_score=avg,
            estimated_potential=est_potential,
            is_complete=zone.zone_state in (3, 4),
        ))

    out.sort(key=lambda m: m.zone_id)
    return out


def _platoon_status(
    zone_score: int,
    summary_zone_sum: int,
    platoon_count: int,
    points_per_platoon: int,
) -> Optional[PlatoonStatus]:
    """
    Derive platoon completion from the score arithmetic.

    The bonus score on a conflict zone = `zone_score - sum(member contributions)`,
    and that bonus is composed of N × points_per_platoon where N is the
    number of completed platoons (all-or-nothing per platoon).

    Returns None if points_per_platoon is 0 (config missing or zero — we
    can't divide).

    The rounding handles the small (<1%) noise we observe in real CG data
    where summary sums occasionally drift by a few thousand points from
    what arithmetic predicts.
    """
    if points_per_platoon <= 0 or platoon_count <= 0:
        return None

    bonus_score = max(0, zone_score - summary_zone_sum)
    ideal = bonus_score / points_per_platoon
    completed = max(0, min(platoon_count, round(ideal)))

    # Sanity warning if rounding distance is large — could indicate the
    # config has wrong points_per_platoon for this zone.
    error = abs(bonus_score - completed * points_per_platoon)
    error_pct = error / max(1, zone_score)
    if error_pct > 0.05:
        log.warning(
            "Platoon math sanity: zone bonus=%d, expected %d×%d=%d, "
            "off by %d (%.1f%% of zone score). Check points_per_platoon "
            "in TB_Map_Config.",
            bonus_score, completed, points_per_platoon,
            completed * points_per_platoon, error, error_pct * 100,
        )

    return PlatoonStatus(
        completed=completed,
        total=platoon_count,
        points_per_platoon=points_per_platoon,
        points_earned=completed * points_per_platoon,
        points_remaining=(platoon_count - completed) * points_per_platoon,
    )


def planet_report(
    snap: TBSnapshot,
    zone_id: str,
    *,
    platoon_count: Optional[int] = None,
    points_per_platoon: Optional[int] = None,
    thresholds: Optional[List[Tuple[int, int]]] = None,
) -> Optional[PlanetReport]:
    """
    Build the full progress report for one planet (conflict zone).

    Args:
      zone_id: full conflict zone id
      platoon_count: from map_config; None disables platoon section
      points_per_platoon: from map_config; None disables platoon section
      thresholds: list of (value, stars) tuples, in ascending order; from
        map_config. None or empty disables star info.

    Returns None if the zone doesn't exist in the snapshot. (Shouldn't
    happen if the caller filtered first, but defensive.)

    The function reads three things from the snapshot:
      1. The zone's score and state (snap.zones)
      2. Member contributions to this zone (snap.zone_member_summary)
      3. Strike sub-zones for this conflict (filtered from snap.zones)

    All values are computed in one pass — no I/O, no expensive ops.
    """
    zone = snap.zones.get(zone_id)
    if zone is None:
        return None

    score = zone.score

    # Star/threshold calculations (only if config provided thresholds)
    current_stars = 0
    max_stars = 0
    thresholds_remaining: List[ThresholdGap] = []
    if thresholds:
        # Sort thresholds ascending by value to be safe (config loader does
        # this already, but defensive).
        sorted_thresholds = sorted(thresholds, key=lambda t: t[0])
        for value, stars in sorted_thresholds:
            max_stars += stars
            if score >= value:
                current_stars += stars
            else:
                thresholds_remaining.append(ThresholdGap(
                    value=value,
                    stars=stars,
                    points_short=value - score,
                ))

    # Platoon status (only if config provided platoon math)
    summary_zone_sum = sum(snap.zone_member_summary.get(zone_id, {}).values())
    platoons: Optional[PlatoonStatus] = None
    if platoon_count is not None and points_per_platoon is not None:
        platoons = _platoon_status(
            zone_score=score,
            summary_zone_sum=summary_zone_sum,
            platoon_count=platoon_count,
            points_per_platoon=points_per_platoon,
        )

    # Strike missions for this conflict
    missions = _strike_mission_details(snap, zone_id)
    missions_combined_total_est = sum(m.estimated_potential for m in missions)

    # Members with zero contribution
    non_participants = members_not_participating_in_zone(snap, zone_id)

    return PlanetReport(
        zone_id=zone_id,
        zone_state=zone.zone_state,
        score=score,
        current_stars=current_stars,
        max_stars=max_stars,
        thresholds_remaining=thresholds_remaining,
        platoons=platoons,
        missions=missions,
        missions_combined_total_est=missions_combined_total_est,
        non_participants=non_participants,
    )

def estimate_to_target(
    report: "PlanetReport",
    target_stars: Optional[int],
    thresholds: Sequence[Tuple[int, int]],
) -> EstimationResult:
    """
    Compute the "Est:" breakdown for one planet given a target star count.
 
    Pure function; no I/O. Inputs:
      report:        the PlanetReport already built for this planet.
      target_stars:  the configured target (1..3), or None if no target.
      thresholds:    list of (score_value, stars_granted) pairs from
                     MapConfig, in any order. Same data the report was
                     built from — passed explicitly to keep this function
                     decoupled from PlanetConfig.
 
    Returns:
      EstimationResult — see the dataclass docstring for state semantics.
 
    Threshold resolution:
      target_stars maps to "the threshold whose CUMULATIVE star count
      first reaches target_stars." This handles bonus-territory layouts
      where the first thresholds are reward-only (stars=0) and only the
      final threshold grants a star. Setting target_stars=1 on such a
      planet correctly resolves to the star-granting threshold.
 
      If target_stars exceeds the max stars on this planet, we clamp to
      the highest reachable cumulative — silently. Officers typing 4 on
      a 3-star planet get treated as "target = all 3 stars" rather than
      seeing an error.
 
    Platoon math:
      platoon_part = min(report.platoons.points_remaining, gap_total).
      Cap at gap_total because if remaining platoons exceed the gap,
      finishing all of them still only "uses up" the gap — anything
      beyond is overshoot. Showing "120M en platoons + 0M" when the
      gap is only 80M would mislead.
 
      When report.platoons is None (no platoon config) or 0 platoons
      remain, platoon_part = 0 and we drop to RESIDUAL_ONLY.
    """
    # No-target path is the first branch — short-circuit before any math.
    if target_stars is None:
        return EstimationResult(
            state=EstimationState.NO_TARGET,
            target_stars=0,
            target_threshold=0,
            current_score=report.score,
            gap_total=0,
            platoon_part=0,
            residual=0,
        )
 
    if not thresholds:
        # Shouldn't happen — target is set but planet has no threshold
        # config. Treat as NO_TARGET for sane fallback.
        return EstimationResult(
            state=EstimationState.NO_TARGET,
            target_stars=target_stars,
            target_threshold=0,
            current_score=report.score,
            gap_total=0,
            platoon_part=0,
            residual=0,
        )
 
    # Find the threshold at which cumulative stars reaches target_stars.
    # Sort ascending by value so we walk thresholds in score order.
    sorted_t = sorted(thresholds, key=lambda t: t[0])
    cumulative = 0
    target_threshold = 0
    for value, stars in sorted_t:
        cumulative += stars
        if cumulative >= target_stars:
            target_threshold = value
            break
    else:
        # target_stars exceeds max — clamp to last threshold's value.
        # cumulative now equals max_stars. Treat as "target the highest
        # achievable star count" — friendlier than an error.
        target_threshold = sorted_t[-1][0]
 
    # Target met?
    if report.score >= target_threshold:
        return EstimationResult(
            state=EstimationState.TARGET_MET,
            target_stars=target_stars,
            target_threshold=target_threshold,
            current_score=report.score,
            gap_total=0,
            platoon_part=0,
            residual=0,
        )
 
    gap_total = target_threshold - report.score
 
    # Platoon contribution — cap at gap to avoid overshoot in the display.
    platoon_remaining = 0
    if report.platoons is not None:
        platoon_remaining = report.platoons.points_remaining
 
    platoon_part = min(platoon_remaining, gap_total)
    residual = gap_total - platoon_part
 
    if platoon_part > 0:
        state = EstimationState.PLATOONS_PLUS_RESIDUAL
    else:
        state = EstimationState.RESIDUAL_ONLY
 
    return EstimationResult(
        state=state,
        target_stars=target_stars,
        target_threshold=target_threshold,
        current_score=report.score,
        gap_total=gap_total,
        platoon_part=platoon_part,
        residual=residual,
    )
def active_planet_zones(snap: TBSnapshot) -> List[str]:
    """
    All zone IDs that are currently active (state 2 ACTIVE or 3 OPEN).

    These are the planets accepting attacks right now — what officers
    want to see in the auto-message. Excludes LOCKED (state 1, not yet
    available) and COMPLETED (state 4, phase ended, locked in).

    Returns zone IDs sorted by (phase, position) so the output is
    deterministic across exports.
    """
    out: List[Tuple[int, int, str]] = []
    for zone in snap.zones.values():
        if not _is_conflict_zone(zone.zone_id):
            continue
        if zone.zone_state not in (2, 3):
            continue
        position = _position_from_conflict_id(zone.zone_id)
        out.append((zone.phase, position, zone.zone_id))
    out.sort()
    # Bonus zones share parent's position — use full zone_id as tiebreaker
    # so "..._conflict03" sorts before "..._conflict03_bonus" alphabetically.
    out.sort(key=lambda t: (t[0], t[1], t[2]))
    return [zid for _, _, zid in out]


def undeployed_gp_for_active_zones(
    snap: TBSnapshot,
    active_zone_ids: List[str],
) -> Tuple[int, int]:
    """
    Compute (undeployed_gp_total, total_gp) summed across the guild for
    the *currently active* zones.

    Definition (agreed in conversation): "GP not deployed to active zones
    in the current round." Note that in SWGoH, a single member's GP is
    deployable to one (and only one) zone per phase; carry-over zones
    from earlier phases keep their original deployments. So "undeployed
    to active zones" = total GP minus the sum of what each member has
    deployed to any currently-active zone.

    Reads `zone_member_power` (per-zone deployment) populated by the
    parser. If that field is missing or empty (e.g. a snapshot from an
    older parser version), returns (0, total_gp) to make the absence
    visible rather than silently wrong.
    """
    total_gp = sum(m.galactic_power for m in snap.members.values())

    if not snap.zone_member_power:
        # Parser didn't capture power_zone data. Return total_gp and 0
        # undeployed so caller can decide whether to display this section.
        return 0, total_gp

    # For each member, sum their power contribution across all active zones.
    per_member_deployed: Dict[str, int] = {}
    for zone_id in active_zone_ids:
        for pid, power in snap.zone_member_power.get(zone_id, {}).items():
            per_member_deployed[pid] = per_member_deployed.get(pid, 0) + power

    total_deployed = sum(per_member_deployed.values())
    undeployed = max(0, total_gp - total_deployed)
    return undeployed, total_gp


def time_remaining(snap: TBSnapshot) -> Optional[timedelta]:
    """
    Time until the current phase ends.

    Returns:
      - timedelta > 0 if phase is still active.
      - timedelta <= 0 if the timer has already passed (caller may want
        to render "ended N minutes ago" or just "ended").
      - None if the snapshot has no round_end_time_utc (we can't tell).

    We return the raw delta rather than a string so callers control
    formatting. Callers in different contexts (Telegram message vs sheet
    cell) want different formats.
    """
    if snap.round_end_time_utc is None:
        return None
    return snap.round_end_time_utc - datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Ranking / breakdown queries
# ---------------------------------------------------------------------------

# Metrics that are valid sort keys for top_contributors. Restricting to
# this set prevents callers from accidentally passing private attrs or
# typos that would AttributeError at runtime.
_RANKABLE_METRICS = frozenset({
    "summary",
    "power",
    "unit_donated",
    "strike_attempt",
    "strike_encounter",
    "covert_attempt",
    "covert_complete",
    "disobey",
})


def top_contributors(
    snap: TBSnapshot,
    phase: Optional[int] = None,
    n: int = DEFAULT_TOP_N,
    by: str = "summary",
) -> List[Contribution]:
    """
    Return the top N members by a chosen metric.

    Args:
      phase: None → use global total_stats (the whole map).
             int  → use that phase's stats.
      n: number of rows to return. Negative or zero returns [].
      by: which CategoryCounts field to rank by. Must be in
          _RANKABLE_METRICS; ValueError otherwise.

    Ties are broken by player name for stability.

    Returns at most `n` rows; may be fewer if the guild has fewer members
    with that metric > 0 (we exclude zero values — a player who didn't
    contribute isn't a "top contributor"). For an unfiltered ranking
    including zeros, callers can do their own sort.
    """
    if by not in _RANKABLE_METRICS:
        raise ValueError(
            f"top_contributors `by` must be one of {sorted(_RANKABLE_METRICS)}, "
            f"got {by!r}"
        )
    if n <= 0:
        return []

    # Decide source dict: per-phase or global.
    if phase is None:
        # Global totals — keyed by player_id.
        source = snap.total_stats
        chosen_phase: Optional[int] = None
    else:
        ps = _phase_stats(snap, phase)
        if ps is None:
            return []
        source = ps.by_player
        chosen_phase = phase

    rows: List[Contribution] = []
    for player_id, counts in source.items():
        member = snap.member(player_id)
        if member is None:
            continue
        value = getattr(counts, by)
        if value <= 0:
            continue
        rows.append(Contribution(
            member=member,
            phase=chosen_phase,
            counts=counts,
            metric=by,
        ))

    rows.sort(
        key=lambda r: (-r.value, _stable_sort_key_by_name(r.member))
    )
    return rows[:n]


def member_phase_breakdown(
    snap: TBSnapshot,
    player_id: str,
) -> List[Contribution]:
    """
    For one member, return their CategoryCounts for every phase present
    in the snapshot, sorted by phase ascending.

    Useful for "show me this player's progression across the TB" in
    Telegram. Returns [] if the player isn't in the snapshot.

    The `metric` field on each returned Contribution is set to "summary"
    by convention — callers typically display all fields anyway, but
    something has to fill that slot.
    """
    member = snap.member(player_id)
    if member is None:
        return []

    out: List[Contribution] = []
    for phase in snap.phases_present:
        counts = snap.phase_stats[phase].for_player(player_id)
        out.append(Contribution(
            member=member,
            phase=phase,
            counts=counts,
            metric="summary",
        ))
    return out
