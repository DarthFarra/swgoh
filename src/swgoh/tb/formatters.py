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
from dataclasses import dataclass
from datetime import timedelta
from typing import Iterable, List, Optional, Sequence

from .analysis import (
    DEFAULT_DEPLOYMENT_THRESHOLD_PCT,
    DeploymentGap,
    EstimationResult,
    EstimationState,
    PlanetReport,
    SpecialFailure,
    StrikeMissionStatus,
    active_planet_zones,
    estimate_to_target,
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
from .tb_targets import TBTargets

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

# Threshold below which a member counts as undeployed in the auto-summary.
# 0.95 = "deployed less than 95% of their roster GP". Chosen because the
# real-world distribution is bimodal (≥99% or <20%); any threshold in
# 0.5..0.99 catches the same people.
AUTO_SUMMARY_UNDEPLOYED_THRESHOLD: float = 0.95


# ---------------------------------------------------------------------------
# Low-level formatting helpers
# ---------------------------------------------------------------------------

_MD_ESCAPE_CHARS = ("*", "_", "`", "[")


def _escape_md(text: str) -> str:
    """
    Escape characters in `text` that would otherwise be interpreted as
    Markdown styling. Used for player names and any other string that
    comes from outside our code.
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
    truncate it and append a notice.
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
    remaining if known.
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
    """"Missing deployment" section."""
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
    """Per-territory progress for the current phase."""
    territories = territory_progress(snap, phase=phase, include_locked=False)
    if not territories:
        return []

    lines: List[str] = ["*Territories:*"]
    for t in territories:
        is_bonus = t.zone_id.endswith("_bonus")
        label = f"T{t.position}{' Bonus' if is_bonus else ''}"
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
    """Bottom-of-message hint about how fresh the snapshot is."""
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
    """Join section line-lists into one Markdown string."""
    blocks = []
    for sec in sections:
        if not sec:
            continue
        blocks.append("\n".join(sec))
    body = "\n\n".join(blocks)
    return _enforce_message_cap(body)


# ---------------------------------------------------------------------------
# Planet-block helpers (shared with format_planet_briefing)
# ---------------------------------------------------------------------------

def _planet_header_line(report: PlanetReport, planet_cfg: Optional[PlanetConfig]) -> str:
    """
    First line of a planet block:
      "*Mandalore* — Stars 1/1"
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
    phase_match = ""
    for i, c in enumerate(zone_id):
        if zone_id[i:i+5] == "phase":
            j = i + 5
            while j < len(zone_id) and zone_id[j].isdigit():
                phase_match += zone_id[j]
                j += 1
            break
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
    """
    p = report.platoons
    if p is None:
        return []
    if p.is_complete:
        return []
    return [
        f"  Platoons: {p.completed}/{p.total} — "
        f"{_fmt_gp(p.points_remaining)} points for completing"
    ]


def _threshold_lines(report: PlanetReport, planet_cfg: Optional[PlanetConfig]) -> List[str]:
    """
    Render "To star N: X points missing" lines for each unreached threshold.
    """
    lines: List[str] = []
    if not planet_cfg or not planet_cfg.thresholds:
        return lines

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
    """Render the Combat Missions block."""
    if not report.missions:
        return []

    lines: List[str] = [
        f"  Combat Missions — Est. potential remaining: "
        f"{_fmt_gp(report.missions_combined_total_est)}"
    ]

    for i, mission in enumerate(report.missions, start=1):
        friendly = strike_name_lookup(mission.zone_id) if strike_name_lookup else None
        name = _escape_md(friendly) if friendly else f"Mission {i}"

        if mission.players_participated == 0:
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
    """Render the "not participated yet" section."""
    non_participants = report.non_participants
    if not non_participants:
        return []

    names = [_escape_md(_name(m)) for m in non_participants]

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
    """Render one planet's full block: header + thresholds + platoons + missions + non-participants."""
    block: List[str] = [_planet_header_line(report, planet_cfg)]
    block.extend(_threshold_lines(report, planet_cfg))
    block.extend(_platoon_lines(report))
    block.extend(_mission_lines(report, strike_name_lookup))
    block.extend(_non_participants_lines(report))
    return block


def _undeployed_gp_lines(snap: TBSnapshot, active_zones: List[str]) -> List[str]:
    """Single-line GP summary for the header."""
    undeployed, total = undeployed_gp_for_active_zones(snap, active_zones)
    if not snap.zone_member_power:
        return []
    deployed = total - undeployed
    return [
        f"  GP: {_fmt_gp(total)} total · "
        f"{_fmt_gp(deployed)} deployed · "
        f"{_fmt_gp(undeployed)} undeployed"
    ]


# ---------------------------------------------------------------------------
# format_planet_briefing — full information-dense view (unchanged)
# ---------------------------------------------------------------------------

def format_planet_briefing(
    snap: TBSnapshot,
    map_config: MapConfig,
    *,
    age_minutes: int = 0,
    include_stale_hint: bool = True,
) -> List[str]:
    """
    Build the C3PO-style auto-status / on-demand-status output.

    Returns a LIST of messages (each <= SOFT_MESSAGE_CAP chars).
    """
    header_lines = _header_lines(snap)
    active = active_planet_zones(snap)
    header_lines.extend(_undeployed_gp_lines(snap, active))

    if not active:
        header_lines.append("")
        header_lines.append("_No active planets at this snapshot._")
        if include_stale_hint:
            header_lines.extend(_data_age_footer(snap, age_minutes))
        return [_enforce_message_cap("\n".join(header_lines))]

    planet_blocks: List[List[str]] = []
    for zone_id in active:
        planet_cfg = map_config.planet(zone_id) if not map_config.is_empty else None
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

    footer_lines: List[str] = []
    if include_stale_hint:
        footer_lines = _data_age_footer(snap, age_minutes)

    return _pack_into_messages(header_lines, planet_blocks, footer_lines)


def _pack_into_messages(
    header_lines: List[str],
    planet_blocks: List[List[str]],
    footer_lines: List[str],
) -> List[str]:
    """Pack content into Telegram-sized messages."""
    messages: List[str] = []
    current_lines: List[str] = list(header_lines)

    for block in planet_blocks:
        candidate_lines = current_lines + [""] + block
        candidate_size = len("\n".join(candidate_lines))

        if candidate_size > SOFT_MESSAGE_CAP and len(current_lines) > len(header_lines):
            messages.append("\n".join(current_lines).rstrip())
            current_lines = ["_(continued)_", ""] + block
        elif candidate_size > SOFT_MESSAGE_CAP:
            current_lines = candidate_lines
            log.warning(
                "Planet block exceeds SOFT_MESSAGE_CAP (%d chars); "
                "sending anyway. Consider trimming long mission name list.",
                candidate_size,
            )
        else:
            current_lines = candidate_lines

    if footer_lines:
        current_lines.append("")
        current_lines.extend(footer_lines)

    if current_lines:
        messages.append("\n".join(current_lines).rstrip())

    return [_enforce_message_cap(m) for m in messages]


# ---------------------------------------------------------------------------
# format_auto_summary — minimal push-notification format
# ---------------------------------------------------------------------------

def format_auto_summary(
    snap: TBSnapshot,
    map_config: Optional[MapConfig] = None,
    tb_targets: Optional[TBTargets] = None,
) -> List[str]:
    """
    Build the message posted automatically when a new TB export arrives.

    DELIBERATELY MINIMAL. Officers want two things at a glance:
      1. Per-planet star summary (achieved/missing) + estimation line.
      2. Who hasn't deployed.

    The estimation line ("Est: ...") appears under each planet when
    tb_targets contains a target for that (guild, phase, zone). It
    shows one of four states:
      - "Est: sin objetivo"               — no target configured
      - "Est: Objetivo alcanzado"         — target already met
      - "Est: 50M platoons + 28M"         — gap covered partly by platoons
      - "Est: 100M faltan"                — gap, no platoon contribution

    Args:
      snap: parsed TB snapshot.
      map_config: planet name/threshold lookups. Empty MapConfig if None.
      tb_targets: per-guild per-phase targets. None or empty means no
        estimation lines anywhere (silent — not "sin objetivo" on every
        planet, since "no targets configured at all" is officer-visible
        elsewhere via /tb_reload_targets).
    """
    cfg = map_config if map_config else MapConfig()
    targets = tb_targets if tb_targets else TBTargets()

    lines: List[str] = list(_header_lines(snap))

    active = active_planet_zones(snap)
    if not active:
        lines.append("")
        lines.append("_No active planets at this snapshot._")
        return [_enforce_message_cap("\n".join(lines))]

    lines.append("")
    for zone_id in active:
        planet_cfg = cfg.planet(zone_id) if not cfg.is_empty else None
        if planet_cfg is None or not planet_cfg.thresholds:
            lines.append(_unconfigured_planet_line(zone_id, snap))
        else:
            lines.extend(
                _minimal_planet_block(snap, zone_id, planet_cfg, targets)
            )

    undep_lines = _undeployed_section_lines(
        snap,
        threshold_pct=AUTO_SUMMARY_UNDEPLOYED_THRESHOLD,
    )
    if undep_lines:
        lines.append("")
        lines.extend(undep_lines)

    return [_enforce_message_cap("\n".join(lines).rstrip())]


# ---------------------------------------------------------------------------
# format_auto_summary_split — TWO-message variant
#
# Used by the discord_listener so the planet summary and the undeployed
# list can be sent as separate Telegram messages. The undeployed message
# is the one that carries the inline buttons; the planet message has
# none.
#
# Why a separate function (not "just split format_auto_summary's output"):
#   The original function joins planet text and undeployed text into
#   one rendered message. After joining, there's no clean boundary to
#   split on — the section separator is "blank line then 'Undeployed'",
#   but that's a string-match contract no caller should depend on.
#   Returning the two pieces structurally is much cleaner.
#
# Why keep the original format_auto_summary too:
#   preview_tb_messages.py and test_planet_briefing.py both import and
#   call it. Removing it would break the test scaffolding. The original
#   becomes a thin shim over format_auto_summary_split: render both
#   pieces, join with a blank line. Same output as today.
# ---------------------------------------------------------------------------

def format_auto_summary_split(
    snap: TBSnapshot,
    map_config: Optional[MapConfig] = None,
    tb_targets: Optional[TBTargets] = None,
) -> tuple[List[str], str]:
    """
    Build the auto-summary as TWO independent pieces.

    Returns:
      (planet_messages, undeployed_message)

      planet_messages — list of one or more strings. The first contains
        the header (TB phase + guild + clock); each is bounded by
        SOFT_MESSAGE_CAP. Splits at planet boundaries when long. Usually
        a list of one.

      undeployed_message — single string ready to send. ALWAYS produced,
        even when no members are undeployed; in that case it reads
        "Undeployed (0)" with no list, so officers can confirm the
        export was processed and the answer is "all clear."

    The undeployed_message is what discord_listener attaches buttons to;
    planet_messages get no buttons.
    """
    cfg = map_config if map_config else MapConfig()
    targets = tb_targets if tb_targets else TBTargets()

    # ---- Build the planet message(s) ----
    planet_lines: List[str] = list(_header_lines(snap))

    active = active_planet_zones(snap)
    if not active:
        planet_lines.append("")
        planet_lines.append("_No active planets at this snapshot._")
    else:
        planet_lines.append("")
        for zone_id in active:
            planet_cfg = cfg.planet(zone_id) if not cfg.is_empty else None
            if planet_cfg is None or not planet_cfg.thresholds:
                planet_lines.append(_unconfigured_planet_line(zone_id, snap))
            else:
                planet_lines.extend(
                    _minimal_planet_block(snap, zone_id, planet_cfg, targets)
                )

    planet_message = _enforce_message_cap("\n".join(planet_lines).rstrip())

    # In practice the planet section fits in one message (≤2 KB even
    # for 6 active planets). If it ever overflows, we'd need to apply
    # _pack_into_messages here too. For now, return a one-element list
    # to match the documented signature and future-proof the contract.
    planet_messages = [planet_message]

    # ---- Build the undeployed message ----
    undep_lines = _undeployed_section_lines(
        snap,
        threshold_pct=AUTO_SUMMARY_UNDEPLOYED_THRESHOLD,
    )
    if undep_lines:
        undeployed_message = _enforce_message_cap("\n".join(undep_lines).rstrip())
    else:
        # User explicitly requested: send the message even when nobody
        # is undeployed, so officers see a clear "0 pending" signal.
        # No buttons in this case (the listener handles that based on
        # the row count).
        undeployed_message = "Undeployed (0):"

    return planet_messages, undeployed_message


# ---------------------------------------------------------------------------
# Public helper exposed for the discord_listener
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class _UndeployedRow:
    """
    Internal row type used by both the formatter and the cache writer.
    """
    player_id: str
    player_name: str
    deployed_gp: int
    roster_gp: int
    missing_gp: int
    pct_deployed: float


def auto_summary_undeployed(
    snap: TBSnapshot,
    threshold_pct: float = AUTO_SUMMARY_UNDEPLOYED_THRESHOLD,
) -> List[_UndeployedRow]:
    """
    Return the structured undeployed list using the SAME rule the
    auto-summary message uses to render its "Undeployed (N): ..." section.

    Sorted by missing_gp descending (biggest gaps first).
    """
    gaps = members_missing_deployment(snap, threshold_pct=threshold_pct)
    rows = [
        _UndeployedRow(
            player_id=g.member.player_id,
            player_name=g.member.player_name,
            deployed_gp=g.deployed,
            roster_gp=g.roster,
            missing_gp=g.remaining,
            pct_deployed=g.pct_deployed,
        )
        for g in gaps
    ]
    rows.sort(key=lambda r: (-r.missing_gp, r.player_name.lower()))
    return rows


# ---------------------------------------------------------------------------
# Private helpers for the minimal auto-summary
# ---------------------------------------------------------------------------

def _minimal_planet_block(
    snap: TBSnapshot,
    zone_id: str,
    planet_cfg: PlanetConfig,
    targets: TBTargets,
) -> List[str]:
    """
    One planet's star summary, MINIMAL flavor (auto-summary only).

    Three cases for the header:
      * All stars achieved → header line with ✓ marker.
      * Some stars missing → header + one "X points missing" line per
        unreached threshold.

    Then always: an "Est: ..." line if a target is configured for this
    (guild, phase, zone) triple — even if all stars are met, since
    officers like the confirmation that target was met.

    Uses planet_report from analysis (single source of truth — same math
    the briefing uses, with platoon math enabled so we know how much
    platoon contribution is still available for the estimation).
    """
    # We DO want platoon math here, unlike pre-feature, because the
    # estimation line needs platoons.points_remaining.
    report = planet_report(
        snap,
        zone_id,
        thresholds=[(t.value, t.stars) for t in planet_cfg.thresholds],
        platoon_count=planet_cfg.platoon_count,
        points_per_platoon=planet_cfg.points_per_platoon,
    )
    if report is None:
        return [_unconfigured_planet_line(zone_id, snap)]

    name = planet_cfg.planet_name
    header = (
        f"*{_escape_md(name)}* — Stars "
        f"{report.current_stars}/{report.max_stars}"
    )

    block: List[str] = []
    if report.current_stars >= report.max_stars and report.max_stars > 0:
        # All stars met — just the header with the check mark.
        block.append(f"{header} ✓")
    else:
        block.append(header)
        threshold_index = {
            t.value: i + 1 for i, t in enumerate(planet_cfg.thresholds)
        }
        for gap in report.thresholds_remaining:
            idx = threshold_index.get(gap.value, 0)
            label = f"reward {idx}" if gap.stars == 0 else f"star {idx}"
            block.append(
                f"  To {label}: {_fmt_gp(gap.points_short)} points missing"
            )

    # Estimation line — only included if a target is configured. Skipping
    # silently when targets is empty (the whole feature is off for this
    # guild) is what the docstring promises.
    if not targets.is_empty:
        target_stars = targets.lookup(
            guild_name=snap.guild_name,
            phase=planet_cfg.phase,
            zone_id=zone_id,
        )
        # Compute even when target_stars is None — the function handles
        # NO_TARGET as a state. We render conditionally below.
        est = estimate_to_target(
            report=report,
            target_stars=target_stars,
            thresholds=[(t.value, t.stars) for t in planet_cfg.thresholds],
        )
        block.append(_estimation_line(est))

    return block


def _estimation_line(est: EstimationResult) -> str:
    """
    Render one estimation line per the state machine in EstimationResult.

    Wording matches the design conversation (Spanish, "Est:" prefix).
    Numbers reuse _fmt_gp for consistency with the rest of the message.
    """
    if est.state == EstimationState.NO_TARGET:
        return "  Est: sin objetivo"
    if est.state == EstimationState.TARGET_MET:
        return "  Est: Objetivo alcanzado"
    if est.state == EstimationState.PLATOONS_PLUS_RESIDUAL:
        return (
            f"  Est: {_fmt_gp(est.platoon_part)} platoons + "
            f"{_fmt_gp(est.residual)}"
        )
    # RESIDUAL_ONLY
    return f"  Est: {_fmt_gp(est.residual)} faltan"


def _unconfigured_planet_line(zone_id: str, snap: TBSnapshot) -> str:
    """Fallback for planets not in MapConfig."""
    label = _label_from_zone_id(zone_id)
    zone = snap.zones.get(zone_id)
    score_str = _fmt_gp(zone.score) if zone else "?"
    return f"*{label}* — {score_str} (no stars config)"


def _undeployed_section_lines(
    snap: TBSnapshot,
    threshold_pct: float,
) -> List[str]:
    """
    Render the "Undeployed (N) — TotalMissing:" section as a bulleted
    list with missing-GP per member. Sorted by missing_gp descending.
    """
    rows = auto_summary_undeployed(snap, threshold_pct=threshold_pct)
    if not rows:
        return []

    total_missing = sum(row.missing_gp for row in rows)

    lines: List[str] = [
        f"Undeployed ({len(rows)}) — {_fmt_gp(total_missing)}:"
    ]
    visible_rows = rows[:MAX_LIST_ITEMS]
    for row in visible_rows:
        name = _escape_md(row.player_name)
        missing = _fmt_gp(row.missing_gp)
        lines.append(f"  • {name} — {missing} missing")

    if len(rows) > MAX_LIST_ITEMS:
        lines.append(f"  • …and {len(rows) - MAX_LIST_ITEMS} more")

    return lines


# ---------------------------------------------------------------------------
# Other public formatters (unchanged)
# ---------------------------------------------------------------------------

def format_status(
    snap: TBSnapshot,
    *,
    map_config: Optional[MapConfig] = None,
    age_minutes: int = 0,
) -> List[str]:
    """Response to /tb_status — same as auto-summary plus stale-data footer."""
    return format_planet_briefing(
        snap,
        map_config if map_config else MapConfig(),
        age_minutes=age_minutes,
        include_stale_hint=True,
    )


def format_failed_specials(snap: TBSnapshot) -> str:
    """Response to /tb_failed_specials — post-mortem view."""
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
    """Response to /tb_top — ranked list of top contributors."""
    try:
        rows = top_contributors(snap, phase=phase, n=n, by=by)
    except ValueError as e:
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
    for i, row in enumerate(rows, start=1):
        lines.append(
            f"  {i:>2}. {_name(row.member)} — "
            f"{_fmt_int(row.value) if rows_metric in ('summary', 'power') else row.value}"
        )
    return lines


def format_no_data(reason: str = "no_export_yet") -> str:
    """Response when no snapshot is cached."""
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
    return f"_No TB data available ({_escape_md(reason)})._"
