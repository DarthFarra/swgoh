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
    SpecialFailure,
    members_missing_deployment,
    members_with_failed_specials,
    members_with_no_strikes,
    members_with_no_summary,
    phase_progress,
    time_remaining,
    top_contributors,
)
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


def _no_strikes_section(
    snap: TBSnapshot,
    phase: Optional[int],
) -> List[str]:
    """Members who haven't attempted any combat mission this phase."""
    missing = members_with_no_strikes(snap, phase=phase)
    if not missing:
        return []

    lines: List[str] = [
        f"*No combat missions attempted* ({len(missing)}):"
    ]
    rendered = [f"  • {_name(m)}" for m in missing]
    lines.extend(_truncate_list(rendered))
    return lines


def _afk_section(snap: TBSnapshot, phase: Optional[int]) -> List[str]:
    """
    Fully-AFK members (zero summary points). Often overlaps with
    no-strikes, but a member who deployed GP without doing strikes
    would only appear in the strikes section — and vice-versa for
    someone who did strikes but didn't deploy. Showing both gives
    officers a complete picture.
    """
    afk = members_with_no_summary(snap, phase=phase)
    if not afk:
        return []

    lines: List[str] = [
        f"*Fully inactive this phase* ({len(afk)}):"
    ]
    rendered = [f"  • {_name(m)}" for m in afk]
    lines.extend(_truncate_list(rendered))
    return lines


def _progress_section(snap: TBSnapshot, phase: Optional[int]) -> List[str]:
    """
    Compact aggregate progress for the phase.

    Note on stat semantics:
      * strike_attempt   = number of distinct combat MISSIONS engaged (one
                           per strike zone the player entered).
      * strike_encounter = total wave-battles fought inside those missions
                           (each mission has multiple waves; this is the
                           finer-grained count).
      * covert_attempt / covert_complete are 1:1 (per mission), so the
                           "completed/attempted" framing is correct there.

    For officer purposes we surface strike_attempt (mission engagement) and
    omit strike_encounter — the wave count is mostly noise at this scope.
    Coverts are shown as completed/attempted because that ratio is real.
    """
    prog = phase_progress(snap, phase=phase)
    if prog is None:
        return []

    active_pct = (
        prog.members_with_any_activity / prog.members_total
        if prog.members_total else 0.0
    )

    lines: List[str] = [
        "*Phase progress:*",
        (
            f"  Active: {prog.members_with_any_activity}/{prog.members_total} "
            f"({active_pct:.0%})  ·  "
            f"Summary: {_fmt_gp(prog.total_summary)}  ·  "
            f"Power: {_fmt_gp(prog.total_power)}"
        ),
    ]

    # Strike + covert line. Omit each side independently if all-zero;
    # render only what's meaningful.
    strike_part = (
        f"Strike missions: {prog.total_strike_attempts}"
        if prog.total_strike_attempts else None
    )
    covert_part = (
        f"Coverts: {prog.total_covert_completes}/{prog.total_covert_attempts}"
        if prog.total_covert_attempts else None
    )
    parts = [p for p in (strike_part, covert_part) if p is not None]
    if parts:
        lines.append("  " + "  ·  ".join(parts))

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

def format_auto_summary(
    snap: TBSnapshot,
    *,
    threshold_pct: float = DEFAULT_DEPLOYMENT_THRESHOLD_PCT,
) -> str:
    """
    Build the message posted automatically when a new TB export arrives.

    Concise, exception-focused, current phase only. If nothing is wrong
    (nobody missing deployment, no AFKs, no strike skips), we still post
    a short "all clear" message so officers know the export was received
    and processed.
    """
    sections: List[List[str]] = [
        _header_lines(snap),
        _deployment_section(snap, phase=None, threshold=threshold_pct),
        _no_strikes_section(snap, phase=None),
        _afk_section(snap, phase=None),
    ]

    # If only the header has content, render the all-clear message.
    if all(len(s) == 0 for s in sections[1:]):
        sections.append([
            "✅ All members deployed, attempting strikes, and active this phase.",
        ])

    return _assemble(sections)


def format_status(
    snap: TBSnapshot,
    *,
    age_minutes: int = 0,
    threshold_pct: float = DEFAULT_DEPLOYMENT_THRESHOLD_PCT,
) -> str:
    """
    Build the response to /tb_status.

    Same exception-list content as the auto-summary, plus a one-line
    progress block, plus a data-age hint if the snapshot is stale.

    Args:
      age_minutes: how long since the snapshot was received. The caller
        knows this (it lives next to the cache); we don't compute it
        ourselves because TBSnapshot.snapshot_taken_at is "when we parsed
        it," which can differ from "when the command was issued" by a
        few seconds — irrelevant for officers, but it lets the caller
        decide which clock to use.
    """
    sections: List[List[str]] = [
        _header_lines(snap),
        _progress_section(snap, phase=None),
        _deployment_section(snap, phase=None, threshold=threshold_pct),
        _no_strikes_section(snap, phase=None),
        _afk_section(snap, phase=None),
        _data_age_footer(snap, age_minutes),
    ]
    return _assemble(sections)


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
