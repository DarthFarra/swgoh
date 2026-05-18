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

from swgoh.tb import parse_tb_snapshot, ParseError, TBSnapshot  # noqa: E402


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
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
