#!/usr/bin/env python3
# scripts/inspect_tb.py
"""
Standalone smoke test for the tb parser.

Usage:
    python scripts/inspect_tb.py path/to/c3po-tb-export.json

Prints a human-readable summary of the parsed snapshot so you can
eyeball whether the parser is doing what you expect on real data,
without booting the rest of the bot.

This is intentionally not a unit test — it's an inspection tool.
Real unit tests would go alongside the parser module.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make `src/swgoh/...` importable without installing the package.
_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent / "src"))

from swgoh.tb import (  # noqa: E402
    parse_tb_snapshot,
    ParseError,
    TBSnapshot,
    members_missing_deployment,
    members_with_no_strikes,
    members_with_no_summary,
    members_with_failed_specials,
    phase_progress,
    time_remaining,
    top_contributors,
)


logging.basicConfig(
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("inspect_tb")


def _fmt_int(n: int) -> str:
    """Format an int with thousands separators for readability."""
    return f"{n:,}"


def _fmt_gp(n: int) -> str:
    """Short GP rendering: 14,601,682 -> '14.6M'."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


def _fmt_duration(target: datetime) -> str:
    """'2h 17m' style remaining-time string."""
    now = datetime.now(timezone.utc)
    delta = target - now
    secs = int(delta.total_seconds())
    if secs < 0:
        return f"ended {-secs // 60}m ago"
    h, rem = divmod(secs, 3600)
    m, _ = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


def print_overview(snap: TBSnapshot) -> None:
    print("=" * 70)
    print(f"  Guild:        {snap.guild_name!r}  (id={snap.guild_id})")
    print(f"  Guild GP:     {_fmt_int(snap.guild_gp)}")
    print(f"  Instance:     {snap.instance_id}")
    print(f"  Definition:   {snap.definition_id}")
    print(f"  Current round: {snap.current_round}")
    if snap.round_end_time_utc:
        print(
            f"  Round ends:   {snap.round_end_time_utc.isoformat()} "
            f"({_fmt_duration(snap.round_end_time_utc)} from now)"
        )
    else:
        print("  Round ends:   unknown")
    print(f"  Members:      {snap.member_count}")
    print(f"  Phases seen:  {snap.phases_present}")
    print(f"  Zones:        {len(snap.zones)}")
    print(f"  Completed early: {snap.map_completed_early}")
    print(f"  Parsed at:    {snap.snapshot_taken_at.isoformat()}")


def print_top_contributors(snap: TBSnapshot, top_n: int = 5) -> None:
    """Highest summary-point contributors across the whole map."""
    print()
    print(f"--- Top {top_n} contributors (total summary points) ---")
    ranked = sorted(
        snap.total_stats.items(),
        key=lambda kv: kv[1].summary,
        reverse=True,
    )[:top_n]
    for pid, counts in ranked:
        m = snap.member(pid)
        name = m.player_name if m else f"<unknown {pid}>"
        print(
            f"  {name:<35} "
            f"summary={_fmt_int(counts.summary):>14}  "
            f"power={_fmt_int(counts.power):>14}  "
            f"units={counts.unit_donated:>4}"
        )


def print_phase_breakdown(snap: TBSnapshot) -> None:
    """One row per phase with totals across all members."""
    print()
    print("--- Per-phase totals (summed across members) ---")
    print(
        f"  {'phase':<6} {'summary':>14} {'power':>14} {'units':>6} "
        f"{'strikes':>8} {'covert':>8}"
    )
    for phase in snap.phases_present:
        ps = snap.phase_stats[phase]
        summary = sum(c.summary for c in ps.by_player.values())
        power = sum(c.power for c in ps.by_player.values())
        units = sum(c.unit_donated for c in ps.by_player.values())
        strikes = sum(c.strike_encounter for c in ps.by_player.values())
        coverts = sum(c.covert_complete for c in ps.by_player.values())
        print(
            f"  {phase:<6} {_fmt_int(summary):>14} {_fmt_int(power):>14} "
            f"{units:>6} {strikes:>8} {coverts:>8}"
        )


def print_zone_states_by_phase(snap: TBSnapshot) -> None:
    """How many zones in each state, broken down by phase and type."""
    print()
    print("--- Zone states by phase ---")
    state_names = {1: "locked", 2: "open", 3: "complete", 4: "final"}

    # Collect (phase, zone_type) -> {state: count}
    grid: dict[tuple[int, str], dict[int, int]] = {}
    for zone in snap.zones.values():
        key = (zone.phase, zone.zone_type)
        grid.setdefault(key, {})
        grid[key][zone.zone_state] = grid[key].get(zone.zone_state, 0) + 1

    print(f"  {'phase':<6} {'type':<10} {'states':<60}")
    for (phase, ztype) in sorted(grid.keys()):
        states = grid[(phase, ztype)]
        state_str = ", ".join(
            f"{state_names.get(s, f'state{s}')}={c}"
            for s, c in sorted(states.items())
        )
        print(f"  {phase:<6} {ztype:<10} {state_str}")


def print_potential_slackers(snap: TBSnapshot) -> None:
    """
    Quick sanity check: who is in the guild but has zero summary points
    in the CURRENT round? (i.e. completely AFK this phase.)

    This is exception-list logic similar to what the eventual analysis
    module will produce; printing it here lets us verify the data is
    correct before formalizing it.
    """
    phase = snap.current_round
    if phase not in snap.phase_stats:
        print()
        print(f"--- No phase_stats for current_round={phase}, skipping AFK check ---")
        return

    ps = snap.phase_stats[phase]
    afk = [
        m for pid, m in snap.members.items()
        if ps.for_player(pid).summary == 0
    ]
    print()
    print(f"--- Members with zero points in round {phase} ({len(afk)} of {snap.member_count}) ---")
    for m in sorted(afk, key=lambda x: x.player_name.lower()):
        print(f"  {m.player_name}  (roster {_fmt_gp(m.galactic_power)} GP)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def print_analysis_demo(snap: TBSnapshot) -> None:
    """
    Exercise the analysis module against the snapshot and print results.
    This is intentionally chatty — the point is to verify the analysis
    functions produce sensible output before we wire up formatters.
    """
    print()
    print("=" * 70)
    print("  ANALYSIS DEMO — testing pure-function queries")
    print("=" * 70)

    # Phase progress for current round
    progress = phase_progress(snap)
    if progress:
        print()
        print(f"--- Phase {progress.phase} progress ---")
        print(f"  Active members:     {progress.members_with_any_activity}/{progress.members_total}")
        print(f"  Total summary pts:  {_fmt_int(progress.total_summary)}")
        print(f"  Total power:        {_fmt_int(progress.total_power)}")
        print(f"  Units donated:      {_fmt_int(progress.total_unit_donated)}")
        print(f"  Strike attempts:    {_fmt_int(progress.total_strike_attempts)}")
        print(f"  Strikes completed:  {_fmt_int(progress.total_strike_encounters)}")
        print(f"  Covert attempts:    {_fmt_int(progress.total_covert_attempts)}")
        print(f"  Coverts completed:  {_fmt_int(progress.total_covert_completes)}")

    # Time remaining
    remaining = time_remaining(snap)
    if remaining is not None:
        secs = int(remaining.total_seconds())
        if secs > 0:
            h, rem = divmod(secs, 3600)
            m, _ = divmod(rem, 60)
            print(f"  Time remaining:     {h}h {m}m")
        else:
            print(f"  Time remaining:     phase ended {-secs // 60}m ago")

    # Missing deployment (current phase, 95% threshold)
    gaps = members_missing_deployment(snap)
    print()
    print(f"--- Missing deployment (<95%) in current phase ({len(gaps)} members) ---")
    if not gaps:
        print("  ✓ Everyone has deployed at least 95% of their roster GP.")
    else:
        for gap in gaps[:10]:  # cap to top 10 in this demo
            print(
                f"  {gap.member.player_name:<35} "
                f"deployed {_fmt_gp(gap.deployed)}/{_fmt_gp(gap.roster)} "
                f"({gap.pct_deployed:.0%})  "
                f"remaining: {_fmt_gp(gap.remaining)}"
            )
        if len(gaps) > 10:
            print(f"  ... and {len(gaps) - 10} more")

    # No strikes attempted (current phase)
    no_strikes = members_with_no_strikes(snap)
    print()
    print(f"--- No combat missions attempted in current phase ({len(no_strikes)} members) ---")
    if not no_strikes:
        print("  ✓ Every member attempted at least one combat mission.")
    else:
        for m in no_strikes[:15]:
            print(f"  {m.player_name}")
        if len(no_strikes) > 15:
            print(f"  ... and {len(no_strikes) - 15} more")

    # Fully inactive
    no_summary = members_with_no_summary(snap)
    print()
    print(f"--- Fully AFK in current phase (zero summary points) ({len(no_summary)} members) ---")
    if not no_summary:
        print("  ✓ Every member has contributed at least some points.")
    else:
        for m in no_summary[:15]:
            print(f"  {m.player_name}")

    # Failed specials (across all phases — interesting for post-mortem)
    print()
    print("--- Special-mission failures (post-mortem view across all phases) ---")
    total_failures = 0
    for phase in snap.phases_present:
        failures = members_with_failed_specials(snap, phase=phase)
        if not failures:
            continue
        print(f"  Phase {phase}: {len(failures)} members with failed specials")
        total_failures += len(failures)
        for f in failures[:3]:
            print(
                f"    {f.member.player_name:<35} "
                f"attempted={f.attempted}  completed={f.completed}  failed={f.failed}"
            )
        if len(failures) > 3:
            print(f"    ... and {len(failures) - 3} more in this phase")
    if total_failures == 0:
        print("  ✓ No special-mission failures in any phase.")

    # Top contributors by different metrics
    print()
    print("--- Top contributors (across the whole map) ---")
    for metric in ("summary", "power", "strike_encounter", "covert_complete"):
        top = top_contributors(snap, n=3, by=metric)
        if not top:
            continue
        print(f"  By {metric}:")
        for row in top:
            print(f"    {row.member.player_name:<35} {_fmt_int(row.value):>14}")


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(f"Usage: {argv[0]} <path-to-c3po-tb-export.json>", file=sys.stderr)
        return 2

    path = Path(argv[1])
    if not path.is_file():
        print(f"File not found: {path}", file=sys.stderr)
        return 2

    try:
        with path.open(encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"Failed to read/parse JSON: {e}", file=sys.stderr)
        return 1

    try:
        snap = parse_tb_snapshot(raw)
    except ParseError as e:
        print(f"Not a valid TB export: {e}", file=sys.stderr)
        return 1

    print_overview(snap)
    print_top_contributors(snap)
    print_phase_breakdown(snap)
    print_zone_states_by_phase(snap)
    print_potential_slackers(snap)
    print_analysis_demo(snap)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
