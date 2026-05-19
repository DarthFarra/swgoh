# src/swgoh/tb/formatters.py
"""
Telegram-Markdown formatters for TB snapshots and analysis results.

Design rules:
  - Pure string-builders. No I/O, no Telegram API calls, no escaping
    decisions beyond the legacy-Markdown dialect we target.
  - Each public formatter returns a single string ready for
    `bot.send_message(..., parse_mode="Markdown")`.
  - Length-bounded. Telegram caps messages at 4096 chars; we cap at
    a softer 3500 to leave headroom for the "... and N more" tail.
  - Empty-state safe. Every section handles "no entries" gracefully
    rather than rendering an empty header.

Dialect choice: legacy Markdown (matches send_assignments_daily.py).
We lose strikethrough/spoilers but skip the heavy escaping that
MarkdownV2 demands for names containing punctuation. Player names that
happen to contain '*' or '_' get escaped via _escape_md to prevent
mid-message style breaks; everything else passes through.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Iterable, List, Optional, Sequence

from .analysis import (
    DEFAULT_DEPLOYMENT_THRESHOLD_PCT,
    DeploymentGap,
    PlanetReport,
    SpecialFailure,
    StrikeMissionStatus,
    active_planet_zones,
    members_missing_deployment,
    members_with_failed_specials,
    planet_report,
    territory_progress,
    time_remaining,
    top_contributors,
    undeployed_gp_for_active_zones,
)
from .map_config import MapConfig, PlanetConfig
from .models import Member, TBSnapshot

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tunables — module-level constants so they're discoverable and overridable
# ---------------------------------------------------------------------------

# Telegram hard limit is 4096; we leave 596 chars of headroom for safety.
# In practice no real message gets close to this once truncation kicks in.
SOFT_MESSAGE_CAP: int = 3500

# Max items shown per exception list before truncating with "... and N more".
# Keeps a single section from monopolizing the message budget.
MAX_LIST_ITEMS: int = 12

# Threshold (minutes since update) above which the formatter appends a
# "data is N minutes old" hint. Below this, no hint — the data is fresh.
STALE_HINT_THRESHOLD_MIN: int = 10


# ---------------------------------------------------------------------------
# Low-level formatting helpers
# ---------------------------------------------------------------------------

# Characters that have meaning in legacy Telegram Markdown and could
# accidentally enable/disable a style mid-string. We only escape these
# inside player names (and similar untrusted strings) — surrounding
# markup we control is safe by construction.
_MD_ESCAPE_CHARS = ("*", "_", "`", "[")


def _escape_md(text: str) -> str:
    """
    Escape characters in `text` that would otherwise be interpreted as
    Markdown styling. Used for player names and any other string that
    comes from outside our code.

    We use the legacy-Markdown trick of prepending a backslash; Telegram
    treats `\\*` as a literal asterisk.
    """
    if not text:
        return ""
    out = text
    for ch in _MD_ESCAPE_CHARS:
        out = out.replace(ch, "\\" + ch)
    return out


def _fmt_int(n: int) -> str:
    """Integer with thousands separators: 1234567 -> '1,234,567'."""
    return f"{n:,}"


def _fmt_gp(n: int) -> str:
    """
    Compact GP rendering used for everywhere members eyeball numbers.
      14,601,682 -> '14.6M'
         123,456 -> '123.5k'
              42 -> '42'

    The 'k' threshold is 1,000 not 10,000 so that values like 800k still
    render compactly. Sub-1000 values pass through as plain ints since
    they're already short enough.
    """
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


def _fmt_duration(td: timedelta, *, terse: bool = False) -> str:
    """
    Human-readable duration rendering.

      timedelta(hours=2, minutes=17) -> '2h 17m'
      timedelta(minutes=45)          -> '45m'
      timedelta(seconds=-300)        -> 'ended 5m ago'  (terse=False)
                                     -> '-5m'           (terse=True)

    terse=True is for compact contexts (one-line summaries); the default
    is the friendlier "ended N ago" phrasing for full messages.
    """
    secs = int(td.total_seconds())
    if secs < 0:
        mins = -secs // 60
        return f"-{mins}m" if terse else f"ended {mins}m ago"
    h, rem = divmod(secs, 3600)
    m, _ = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


def _truncate_list(items: Sequence[str], cap: int = MAX_LIST_ITEMS) -> List[str]:
    """
    Truncate a list of lines, appending a "... and N more" line when needed.
    Returns a *new* list (doesn't mutate the input).
    """
    if len(items) <= cap:
        return list(items)
    out = list(items[:cap])
    out.append(f"  _... and {len(items) - cap} more_")
    return out


def _enforce_message_cap(text: str) -> str:
    """
    Final safety net: if a fully-assembled message exceeds the soft cap,
    truncate it and append a notice. This should rarely fire because
    each section is independently bounded by MAX_LIST_ITEMS, but it
    protects against pathological cases (e.g. very long member names).
    """
    if len(text) <= SOFT_MESSAGE_CAP:
        return text
    return text[: SOFT_MESSAGE_CAP - 50].rstrip() + "\n\n_… message truncated_"


def _name(member: Member) -> str:
    """Convenience wrapper: name → escaped name."""
    return _escape_md(member.player_name)


# ---------------------------------------------------------------------------
# Section builders — each returns a list of lines (no trailing newline).
# Empty sections return an empty list, and the assembler skips them.
# ---------------------------------------------------------------------------

def _header_lines(
    snap: TBSnapshot,
    *,
    phase_label: Optional[int] = None,
    omit_phase: bool = False,
) -> List[str]:
    """
    The top-of-message identity block: guild name, phase label, time
    remaining if known. Short.

    phase_label controls what phase number appears in the title:
      None         → use snap.current_round (default for live messages).
      int          → use that specific phase (for past-phase queries).
      <omit_phase> → no phase in the title at all (for cross-phase views).
    """
    name = _escape_md(snap.guild_name) or "Unknown guild"

    if omit_phase:
        title = f"*TB* — {name}"
    else:
        effective_phase = phase_label if phase_label is not None else snap.current_round
        title = f"*TB Phase {effective_phase}* — {name}"

    lines: List[str] = [title]

    td = time_remaining(snap)
    if td is not None:
        lines.append(f"  ⏱  {_fmt_duration(td)}")

    return lines


def _deployment_section(
    snap: TBSnapshot,
    phase: Optional[int],
    threshold: float,
) -> List[str]:
    """
    "Missing deployment" section. Renders nothing if everyone is at or
    above the threshold (handled by the auto-summary's "all clear" path).
    """
    gaps = members_missing_deployment(snap, phase=phase, threshold_pct=threshold)
    if not gaps:
        return []

    lines: List[str] = [
        f"*GP not fully deployed* ({len(gaps)}):"
    ]
    rendered = [_format_gap_line(g) for g in gaps]
    lines.extend(_truncate_list(rendered))
    return lines


def _format_gap_line(gap: DeploymentGap) -> str:
    """One line per missing-deployment entry."""
    return (
        f"  • {_name(gap.member)}: "
        f"{_fmt_gp(gap.remaining)} left "
        f"(deployed {_fmt_gp(gap.deployed)}/{_fmt_gp(gap.roster)}, "
        f"{gap.pct_deployed:.0%})"
    )


def _territories_section(
    snap: TBSnapshot, phase: Optional[int]
) -> List[str]:
    """
    Per-territory progress for the current phase.

    Each territory line:
      "T2: 257.6M  ·  35/50 contributing  ·  Strikes 5/5  ✓"

    Bonus territories ("_bonus" zone IDs) are labeled "T3 Bonus" so officers
    can distinguish them from the base territory at the same position.

    Locked territories are skipped silently — they have no useful progress.
    Strikes line is omitted when the territory has no strike zones (early
    phases sometimes have no combat missions).

    Returns [] if no unlocked territories exist for this phase.
    """
    territories = territory_progress(snap, phase=phase, include_locked=False)
    if not territories:
        return []

    lines: List[str] = ["*Territories:*"]
    for t in territories:
        is_bonus = t.zone_id.endswith("_bonus")
        label = f"T{t.position}{' Bonus' if is_bonus else ''}"

        # Completion glyph. Both "completed" (3) and "finalized" (4) get ✓.
        # "Open but not done" (2) gets nothing — it's the normal in-progress state.
        glyph = " ✓" if t.is_completed else ""

        parts = [
            f"*{label}*: {_fmt_gp(t.score)}",
            f"{t.contributing_members}/{t.members_total} contributing",
        ]
        if t.strikes_total > 0:
            parts.append(f"Strikes {t.strikes_completed}/{t.strikes_total}")
        lines.append("  " + "  ·  ".join(parts) + glyph)

    return lines


def _data_age_footer(snap: TBSnapshot, age_minutes: int) -> List[str]:
    """
    Bottom-of-message hint about how fresh the snapshot is. Suppressed
    when the data is recent (within STALE_HINT_THRESHOLD_MIN) to avoid
    visual noise.
    """
    if age_minutes < STALE_HINT_THRESHOLD_MIN:
        return []
    return [
        "",
        f"_Data is {age_minutes}m old. Run /tb export in Discord to refresh._",
    ]


# ---------------------------------------------------------------------------
# Top-level assembler — composes sections into a final message.
# ---------------------------------------------------------------------------

def _assemble(sections: Iterable[List[str]]) -> str:
    """
    Join section line-lists into one Markdown string with blank-line
    separation between non-empty sections. Empty sections are skipped
    so the output stays tight regardless of which queries had hits.
    """
    blocks = []
    for sec in sections:
        if not sec:
            continue
        blocks.append("\n".join(sec))
    body = "\n\n".join(blocks)
    return _enforce_message_cap(body)


# ---------------------------------------------------------------------------
# Public formatters
# ---------------------------------------------------------------------------

def _planet_header_line(report: PlanetReport, planet_cfg: Optional[PlanetConfig]) -> str:
    """
    First line of a planet block:
      "*Mandalore* — Stars 1/1"
      "*T1* (phase 4) — Stars ?"   (config missing → fallback label)

    Star info is included only when config provides thresholds (max_stars
    > 0). Otherwise we show just the planet name and let the score line
    below carry the progress signal.
    """
    name = planet_cfg.planet_name if planet_cfg else _label_from_zone_id(report.zone_id)
    if report.max_stars > 0:
        return f"*{_escape_md(name)}* — Stars {report.current_stars}/{report.max_stars}"
    return f"*{_escape_md(name)}*"


def _label_from_zone_id(zone_id: str) -> str:
    """
    Fallback display label when no PlanetConfig is available.

    'tb3_mixed_phase04_conflict03'        -> 'Phase 4 T3'
    'tb3_mixed_phase04_conflict03_bonus'  -> 'Phase 4 T3 Bonus'
    """
    # Extract phase number
    phase_match = ""
    for i, c in enumerate(zone_id):
        if zone_id[i:i+5] == "phase":
            j = i + 5
            while j < len(zone_id) and zone_id[j].isdigit():
                phase_match += zone_id[j]
                j += 1
            break
    # Extract conflict position
    pos_match = ""
    idx = zone_id.find("conflict")
    if idx >= 0:
        j = idx + len("conflict")
        while j < len(zone_id) and zone_id[j].isdigit():
            pos_match += zone_id[j]
            j += 1
    is_bonus = zone_id.endswith("_bonus")
    phase_part = f"Phase {int(phase_match)} " if phase_match else ""
    pos_part = f"T{int(pos_match)}" if pos_match else "?"
    bonus_part = " Bonus" if is_bonus else ""
    return f"{phase_part}{pos_part}{bonus_part}"


def _platoon_lines(report: PlanetReport) -> List[str]:
    """
    Render the Platoons section:
      "Platoons: 4/6 — 32.0M points remaining"
      (omitted if all 6 are complete OR if config didn't provide
       platoon math)
    """
    p = report.platoons
    if p is None:
        return []
    if p.is_complete:
        return []  # hidden per spec
    return [
        f"  Platoons: {p.completed}/{p.total} — "
        f"{_fmt_gp(p.points_remaining)} points for completing"
    ]


def _threshold_lines(report: PlanetReport, planet_cfg: Optional[PlanetConfig]) -> List[str]:
    """
    Render "To star N: X points missing" lines for each unreached threshold.

    Hidden thresholds (those already achieved) don't appear, per spec.
    For reward-only thresholds (stars=0), we render "To reward N" instead
    of "To star" — clearer for the Zeffo/Mandalore pattern where the first
    two thresholds are rewards, not stars.

    Threshold indexing is 1-based and consistent with the config sheet's
    t1/t2/t3 columns (which the formatter doesn't see directly, but the
    user thinks in terms of).
    """
    lines: List[str] = []
    if not planet_cfg or not planet_cfg.thresholds:
        return lines

    # Build a mapping from gap value to its 1-based threshold number in
    # the config. This handles the case where some earlier thresholds
    # were achieved (not in thresholds_remaining) — we still want the
    # remaining one to be labelled with its config position.
    threshold_index: dict[int, int] = {
        t.value: i + 1 for i, t in enumerate(planet_cfg.thresholds)
    }

    for gap in report.thresholds_remaining:
        idx = threshold_index.get(gap.value, 0)
        label = f"reward {idx}" if gap.stars == 0 else f"star {idx}"
        lines.append(
            f"  To {label}: {_fmt_gp(gap.points_short)} points missing"
        )

    return lines


def _mission_lines(
    report: PlanetReport,
    strike_name_lookup,
) -> List[str]:
    """
    Render the Combat Missions block:
      "Combat Missions — Est. potential remaining: 288.8M"
      "  Mission 1: 23/50 (avg 1.6M) — Est. potential 43.6M"
      "  ..."

    `strike_name_lookup` is a callable(strike_zone_id) -> Optional[str]
    (the MapConfig.strike_name method). When None, missions display as
    "Mission 1", "Mission 2", etc.

    Skips completely if the planet has no missions (e.g. some TB types
    have planets without combat missions).
    """
    if not report.missions:
        return []

    lines: List[str] = [
        f"  Combat Missions — Est. potential remaining: "
        f"{_fmt_gp(report.missions_combined_total_est)}"
    ]

    for i, mission in enumerate(report.missions, start=1):
        # Try friendly name from config, fall back to "Mission N"
        friendly = strike_name_lookup(mission.zone_id) if strike_name_lookup else None
        name = _escape_md(friendly) if friendly else f"Mission {i}"

        if mission.players_participated == 0:
            # No attempts yet — show 0/N, no avg, no potential extrapolation
            # (extrapolating from zero data is meaningless).
            lines.append(
                f"    • {name}: 0/{mission.members_total} "
                f"(no attempts yet)"
            )
        else:
            lines.append(
                f"    • {name}: {mission.players_participated}/{mission.members_total} "
                f"(avg {_fmt_gp(mission.avg_score)}) — "
                f"Est. potential {_fmt_gp(mission.estimated_potential)}"
            )

    return lines


def _non_participants_lines(report: PlanetReport) -> List[str]:
    """
    Render the "not participated yet" section:
      "Not participated yet (12):"
      "  • PlayerA, PlayerB, PlayerC, ..., and 4 more"

    Empty list → nothing rendered.
    Names are escaped and shown comma-separated on a single line for
    compactness (vs one-per-line which blows up message length).
    """
    non_participants = report.non_participants
    if not non_participants:
        return []

    names = [_escape_md(_name(m)) for m in non_participants]

    # Compact rendering: comma-separated on one line, with "+N more" if
    # we'd exceed MAX_LIST_ITEMS.
    if len(names) <= MAX_LIST_ITEMS:
        names_str = ", ".join(names)
    else:
        head = ", ".join(names[:MAX_LIST_ITEMS])
        names_str = f"{head}, and {len(names) - MAX_LIST_ITEMS} more"

    return [
        f"  Not participated yet ({len(non_participants)}):",
        f"    {names_str}",
    ]


def _format_planet_block(
    report: PlanetReport,
    planet_cfg: Optional[PlanetConfig],
    strike_name_lookup,
) -> List[str]:
    """
    Render one planet's full block: header + thresholds + platoons + missions
    + non-participants.

    Returns a list of lines (no trailing blank line). Caller joins with
    blank-line separators between planets.
    """
    block: List[str] = [_planet_header_line(report, planet_cfg)]
    block.extend(_threshold_lines(report, planet_cfg))
    block.extend(_platoon_lines(report))
    block.extend(_mission_lines(report, strike_name_lookup))
    block.extend(_non_participants_lines(report))
    return block


def _undeployed_gp_lines(snap: TBSnapshot, active_zones: List[str]) -> List[str]:
    """
    Single-line GP summary for the header.

    Only rendered if zone_member_power data is available (modern snapshots).
    Falls back silently if not — better to omit a line than to show a
    misleading "0 undeployed" value.
    """
    undeployed, total = undeployed_gp_for_active_zones(snap, active_zones)
    if not snap.zone_member_power:
        # No power_zone data; skip the line rather than show a wrong "0".
        return []
    deployed = total - undeployed
    return [
        f"  GP: {_fmt_gp(total)} total · "
        f"{_fmt_gp(deployed)} deployed · "
        f"{_fmt_gp(undeployed)} undeployed"
    ]


def format_planet_briefing(
    snap: TBSnapshot,
    map_config: MapConfig,
    *,
    age_minutes: int = 0,
    include_stale_hint: bool = True,
) -> List[str]:
    """
    Build the C3PO-style auto-status / on-demand-status output.

    Returns a LIST of messages (each <= SOFT_MESSAGE_CAP chars). Callers
    that want a single string can ''.join() them, but they'll usually
    send each as a separate Telegram message.

    Layout per planet (active zones only, state in {2, 3}):

      *<Planet name>* — Stars X/Y
        To star N: P points missing      (one per unreached threshold)
        Platoons: X/6 — Y points for completing
        Combat Missions — Est. potential remaining: Z
          • Mission name: A/50 (avg M) — Est. potential P
          • ...
        Not participated yet (N):
          PlayerA, PlayerB, ...

    Args:
      map_config: lookups for planet names, thresholds, strike names.
        Pass an empty MapConfig() if config not loaded — output will
        use generic labels and skip star/threshold/platoon info.
      age_minutes: how stale the underlying export is (for /tb_status).
        0 for auto-forward.
      include_stale_hint: whether to append the "data is N min old" line.
        Auto-forward passes False (the message arrives at age 0).

    Multi-message split:
      If the total content exceeds SOFT_MESSAGE_CAP, we split at planet
      boundaries — never mid-planet. Each output string starts with a
      continuation marker ("...") so officers know it's part 2 of N.
    """
    # Header section: always present.
    header_lines = _header_lines(snap)
    active = active_planet_zones(snap)
    header_lines.extend(_undeployed_gp_lines(snap, active))

    if not active:
        # No active zones — phase might be between rounds, or all zones
        # are LOCKED/COMPLETED. Emit just the header with an explanatory
        # line.
        header_lines.append("")
        header_lines.append("_No active planets at this snapshot._")
        if include_stale_hint:
            header_lines.extend(_data_age_footer(snap, age_minutes))
        return [_enforce_message_cap("\n".join(header_lines))]

    # Build each planet block.
    planet_blocks: List[List[str]] = []
    for zone_id in active:
        planet_cfg = map_config.planet(zone_id) if not map_config.is_empty else None
        # Extract config values for planet_report
        if planet_cfg is not None:
            platoon_count = planet_cfg.platoon_count
            points_per_platoon = planet_cfg.points_per_platoon
            thresholds = [(t.value, t.stars) for t in planet_cfg.thresholds]
        else:
            platoon_count = None
            points_per_platoon = None
            thresholds = None

        report = planet_report(
            snap, zone_id,
            platoon_count=platoon_count,
            points_per_platoon=points_per_platoon,
            thresholds=thresholds,
        )
        if report is None:
            continue
        block = _format_planet_block(
            report,
            planet_cfg,
            map_config.strike_name if not map_config.is_empty else None,
        )
        planet_blocks.append(block)

    # Optional stale-data footer (appended to the LAST message).
    footer_lines: List[str] = []
    if include_stale_hint:
        footer_lines = _data_age_footer(snap, age_minutes)

    # Pack header + planet blocks + footer into messages, splitting at
    # planet boundaries when needed.
    return _pack_into_messages(header_lines, planet_blocks, footer_lines)


def _pack_into_messages(
    header_lines: List[str],
    planet_blocks: List[List[str]],
    footer_lines: List[str],
) -> List[str]:
    """
    Pack content into Telegram-sized messages.

    First message: header + as many planet blocks as fit.
    Subsequent messages: continuation marker + remaining planet blocks.
    Footer: appended to the LAST message only.

    Splits ONLY at planet boundaries — never mid-planet — so each
    planet's info stays together visually.

    Returns at least one string. Always respects SOFT_MESSAGE_CAP.
    """
    messages: List[str] = []
    current_lines: List[str] = list(header_lines)
    used_blank_line_to_planet = False

    def current_size() -> int:
        return len("\n".join(current_lines))

    for block in planet_blocks:
        # Each planet block is preceded by a blank line for visual
        # separation (unless we're at the very start of a message).
        candidate_lines = current_lines + [""] + block
        candidate_size = len("\n".join(candidate_lines))

        if candidate_size > SOFT_MESSAGE_CAP and len(current_lines) > len(header_lines):
            # This planet doesn't fit. Flush the current message and start
            # a new one with this planet at the top.
            messages.append("\n".join(current_lines).rstrip())
            current_lines = ["_(continued)_", ""] + block
        elif candidate_size > SOFT_MESSAGE_CAP:
            # Edge case: the very first planet is too big to fit even
            # in a fresh message. We accept the overflow (better to
            # send a 4500-char message than to lose the planet info).
            # Telegram will accept up to 4096, and our cap is 3500 with
            # 596 chars headroom; a single planet block exceeding 3500
            # would have to be quite elaborate.
            current_lines = candidate_lines
            log.warning(
                "Planet block exceeds SOFT_MESSAGE_CAP (%d chars); "
                "sending anyway. Consider trimming long mission name list.",
                candidate_size,
            )
        else:
            current_lines = candidate_lines

    # Append footer to whatever message we're currently building.
    if footer_lines:
        current_lines.append("")
        current_lines.extend(footer_lines)

    if current_lines:
        messages.append("\n".join(current_lines).rstrip())

    # Final cap enforcement — should be a no-op for well-formed input
    # but defensive.
    return [_enforce_message_cap(m) for m in messages]


def format_auto_summary(
    snap: TBSnapshot,
    map_config: Optional[MapConfig] = None,
) -> List[str]:
    """
    Build the message(s) posted automatically when a new TB export arrives.

    Same content as format_status, minus the data-age hint (which is
    meaningless for a just-received export — age is zero by definition).
    Returns a list because the content can exceed Telegram's 4096-char
    limit and gets split at planet boundaries.

    Args:
      map_config: planet/threshold/strike-name lookups. If None or empty,
        labels fall back to generic identifiers and star info is skipped.

    Returns:
      List of message strings, in order. Usually 1 element; can be 2+
      for late-phase exports with many active planets.
    """
    return format_planet_briefing(
        snap,
        map_config if map_config else MapConfig(),
        age_minutes=0,
        include_stale_hint=False,
    )


def format_status(
    snap: TBSnapshot,
    *,
    map_config: Optional[MapConfig] = None,
    age_minutes: int = 0,
) -> List[str]:
    """
    Build the response to /tb_status.

    Same content as format_auto_summary plus a stale-data footer when the
    cached snapshot is more than a few minutes old. Returns a list of
    messages for the same multi-message reason as format_auto_summary.
    """
    return format_planet_briefing(
        snap,
        map_config if map_config else MapConfig(),
        age_minutes=age_minutes,
        include_stale_hint=True,
    )


def format_failed_specials(snap: TBSnapshot) -> str:
    """
    Build the response to /tb_failed_specials — post-mortem view.

    Iterates every phase present in the snapshot and lists failures.
    Phases with zero failures are skipped entirely (no empty headers).

    Output structure:
      *TB — <guild>*
      *Phase N* (X members with failed specials):
        • Member: tried Y, completed Z, failed W
        • ...
    """
    sections: List[List[str]] = [
        _header_lines(snap, omit_phase=True),
    ]

    any_failures = False
    for phase in snap.phases_present:
        failures = members_with_failed_specials(snap, phase=phase)
        if not failures:
            continue
        any_failures = True
        sections.append(_failed_specials_section(phase, failures))

    if not any_failures:
        sections.append([
            "✅ No special-mission failures in any phase.",
        ])

    return _assemble(sections)


def _failed_specials_section(
    phase: int, failures: Sequence[SpecialFailure]
) -> List[str]:
    """One phase's worth of failed-specials lines."""
    lines: List[str] = [
        f"*Phase {phase}* ({len(failures)}):"
    ]
    rendered = [
        f"  • {_name(f.member)}: "
        f"tried {f.attempted}, completed {f.completed}, "
        f"failed {f.failed}"
        for f in failures
    ]
    lines.extend(_truncate_list(rendered))
    return lines


def format_top_contributors(
    snap: TBSnapshot,
    *,
    by: str = "summary",
    n: int = 10,
    phase: Optional[int] = None,
) -> str:
    """
    Build the response to /tb_top — ranked list of top contributors.

    Args:
      by: metric name (see analysis._RANKABLE_METRICS).
      n: how many to show.
      phase: optional phase filter; None = totals across whole map.
    """
    try:
        rows = top_contributors(snap, phase=phase, n=n, by=by)
    except ValueError as e:
        # Re-raised as a user-facing message rather than a stack trace.
        return f"_Invalid metric: {_escape_md(str(e))}_"

    if not rows:
        scope = f"phase {phase}" if phase is not None else "the map"
        return f"_No contributors to rank for {scope} by {by}._"

    sections: List[List[str]] = [
        _header_lines(
            snap,
            phase_label=phase,
            omit_phase=(phase is None),
        ),
        _top_section(rows_metric=by, rows=rows, phase=phase),
    ]
    return _assemble(sections)


def _top_section(rows_metric: str, rows, phase: Optional[int]) -> List[str]:
    scope = f"Phase {phase}" if phase is not None else "Total"
    lines: List[str] = [
        f"*Top by {rows_metric}* ({scope}):"
    ]
    # Right-aligned numeric column for legibility on monospace clients.
    for i, row in enumerate(rows, start=1):
        # Note: Telegram renders numbers in proportional font; perfect
        # alignment isn't possible, but a leading rank number + space
        # gives reasonable scannability.
        lines.append(
            f"  {i:>2}. {_name(row.member)} — "
            f"{_fmt_int(row.value) if rows_metric in ('summary', 'power') else row.value}"
        )
    return lines


def format_no_data(reason: str = "no_export_yet") -> str:
    """
    Response when /tb_status (or any other command) is called but no
    snapshot is cached. Different reasons get different copy so officers
    can distinguish "bot just started" from "bridge is broken".
    """
    if reason == "no_export_yet":
        return (
            "_No TB data yet._\n"
            "Run `/tb export` in Discord to publish an export "
            "and I'll process it automatically."
        )
    if reason == "bot_restarted":
        return (
            "_TB data was lost when the bot restarted._\n"
            "Run `/tb export` in Discord to re-publish the latest snapshot."
        )
    # Generic fallback.
    return f"_No TB data available ({_escape_md(reason)})._"
