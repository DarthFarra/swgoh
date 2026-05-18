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
from typing import List, Optional

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
