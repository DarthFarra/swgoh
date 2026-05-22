# ============================================================================
# PATCH for src/swgoh/tb/formatters.py
#
# 1. Replace the existing `format_auto_summary` (and its body that
#    delegated to format_planet_briefing) with a STANDALONE minimal
#    implementation.
# 2. Add three new private helpers used by the new format.
# 3. Add the `undeployed_members` helper that returns the structured list
#    (the message side + the cache side share this data).
#
# Imports to ADD at the top (next to existing analysis imports):
#   from .analysis import members_missing_deployment  # already there
#
# No new third-party deps.
# ============================================================================

# Constants (add near the existing tunables at module top):

# Threshold below which a member counts as undeployed in the auto-summary.
# 0.95 = "deployed less than 95% of their roster GP". Chosen because the
# real-world distribution is bimodal (≥99% or <20%); any threshold in
# 0.5..0.99 catches the same people. 0.95 errs slightly toward including
# borderline partial deployers.
AUTO_SUMMARY_UNDEPLOYED_THRESHOLD: float = 0.95


# ----------------------------------------------------------------------------
# Public formatter — replaces existing format_auto_summary
# ----------------------------------------------------------------------------

def format_auto_summary(
    snap: TBSnapshot,
    map_config: Optional[MapConfig] = None,
) -> List[str]:
    """
    Build the message posted automatically when a new TB export arrives.

    DELIBERATELY MINIMAL — this is a push notification, not a briefing.
    Officers reading this in Telegram want two things at a glance:

      1. Per-planet star summary (achieved/missing).
      2. Who hasn't deployed.

    Everything else (platoon math, per-strike stats, special missions)
    lives in /tb_briefing via format_planet_briefing.

    Layout:

      *TB Phase 6* — Guild Name
        ⏱  12h 34m

      *Mandalore* — Stars 1/1 ✓
      *Hoth* — Stars 2/3
        To star 3: 100.0M points missing

      Undeployed (2):
        • IMLilliTH — 13.7M missing
        • Nere Nac — 11.9M missing

    Fallbacks:
      - Planet with no MapConfig → raw score with "(no stars config)" note.
      - No active planets → short "no active planets" message.
      - No undeployed members → section omitted.

    Returns:
      A list of message strings — always at least one. Each message is
      bounded by SOFT_MESSAGE_CAP. In practice the minimal format fits
      in one message even for 24 active zones (~500-700 chars total).
    """
    cfg = map_config if map_config else MapConfig()

    # Header — same as format_planet_briefing minus the GP totals line
    # (the undeployed list later carries the actionable per-member info).
    lines: List[str] = list(_header_lines(snap))

    active = active_planet_zones(snap)
    if not active:
        lines.append("")
        lines.append("_No active planets at this snapshot._")
        return [_enforce_message_cap("\n".join(lines))]

    # Per-planet star summary.
    lines.append("")
    for zone_id in active:
        planet_cfg = cfg.planet(zone_id)
        if planet_cfg is None or not planet_cfg.thresholds:
            lines.append(_unconfigured_planet_line(zone_id, snap))
        else:
            lines.extend(_minimal_planet_block(snap, zone_id, planet_cfg))

    # Undeployed members — uses the existing phase-scoped analysis function.
    # threshold=0.95 catches partial deployers (<95% of roster GP).
    undep_lines = _undeployed_section_lines(
        snap,
        threshold_pct=AUTO_SUMMARY_UNDEPLOYED_THRESHOLD,
    )
    if undep_lines:
        lines.append("")
        lines.extend(undep_lines)

    return [_enforce_message_cap("\n".join(lines).rstrip())]


# ----------------------------------------------------------------------------
# Public helper — shared with the auto-forward path so the message and
# the cached "snapshot for buttons" use the SAME data. Single source of truth.
# ----------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class _UndeployedRow:
    """
    Internal row type used by both the formatter and the cache writer.

    Mirrors UndeployedMember (in services/tb_undeployed_cache) but lives
    in the formatter so the formatter has no dependency on the bot
    package. The discord_listener bridges the two by translating one to
    the other.
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

    Exposed publicly so the discord_listener can capture this exact list
    into the cache that backs the auto-summary's inline buttons. Keeping
    one function ensures the message and the action-target list can never
    diverge.

    Sorted by missing_gp descending (biggest gaps first) — same as the
    rendered list. Empty list when everyone is at or above threshold.
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


# ----------------------------------------------------------------------------
# Private helpers
# ----------------------------------------------------------------------------

def _minimal_planet_block(
    snap: TBSnapshot,
    zone_id: str,
    planet_cfg: PlanetConfig,
) -> List[str]:
    """
    One planet's star summary. Two cases:

      * All stars achieved → single header line with ✓ marker.
      * Some stars missing → header + one "X points missing" line per
        unreached threshold.

    Uses `planet_report` from analysis to compute the threshold math
    (single source of truth — same math the briefing uses). We pass
    None for platoon_count/points_per_platoon since we don't render
    platoons here; that branch of planet_report is skipped.
    """
    report = planet_report(
        snap,
        zone_id,
        thresholds=tuple((t.value, t.stars) for t in planet_cfg.thresholds),
        platoon_count=None,
        points_per_platoon=None,
    )
    if report is None:
        # Defensive: caller filtered to active zones, but cope.
        return [_unconfigured_planet_line(zone_id, snap)]

    name = planet_cfg.planet_name
    header = (
        f"*{_escape_md(name)}* — Stars "
        f"{report.current_stars}/{report.max_stars}"
    )
    if report.current_stars >= report.max_stars and report.max_stars > 0:
        return [f"{header} ✓"]

    block: List[str] = [header]
    threshold_index = {
        t.value: i + 1 for i, t in enumerate(planet_cfg.thresholds)
    }
    for gap in report.thresholds_remaining:
        idx = threshold_index.get(gap.value, 0)
        label = f"reward {idx}" if gap.stars == 0 else f"star {idx}"
        block.append(
            f"  To {label}: {_fmt_gp(gap.points_short)} points missing"
        )
    return block


def _unconfigured_planet_line(zone_id: str, snap: TBSnapshot) -> str:
    """
    Fallback for planets not in MapConfig.

    Shows the generic zone label plus the raw score and a "(no stars
    config)" note so officers know to add the planet to the sheet rather
    than wondering why progress data is missing.
    """
    label = _label_from_zone_id(zone_id)
    zone = snap.zones.get(zone_id)
    score_str = _fmt_gp(zone.score) if zone else "?"
    return f"*{label}* — {score_str} (no stars config)"


def _undeployed_section_lines(
    snap: TBSnapshot,
    threshold_pct: float,
) -> List[str]:
    """
    Render the "Undeployed (N):" section as a bulleted list with
    missing-GP per member. Sorted by missing_gp descending — biggest
    gaps first, matches officers' priority order.

    Per-member rendering decision:
      We show "missing GP" (e.g. "13.7M") rather than percentage. Missing
      GP is the actionable unit — it tells officers how much remains to
      be deployed, which is what they want to communicate when chasing
      the member. Percentage is shown only in the DM template, where
      it adds context to the member's own roster size.

    Truncation:
      If the list is longer than MAX_LIST_ITEMS, the tail collapses
      into "and N more". This is defensive — in practice the partial-
      deployer list is small (single digits even in a 50-member guild).
    """
    rows = auto_summary_undeployed(snap, threshold_pct=threshold_pct)
    if not rows:
        return []

    lines: List[str] = [f"Undeployed ({len(rows)}):"]
    visible_rows = rows[:MAX_LIST_ITEMS]
    for row in visible_rows:
        name = _escape_md(row.player_name)
        missing = _fmt_gp(row.missing_gp)
        lines.append(f"  • {name} — {missing} missing")

    if len(rows) > MAX_LIST_ITEMS:
        lines.append(f"  • …and {len(rows) - MAX_LIST_ITEMS} more")

    return lines
