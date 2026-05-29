# src/swgoh/tb/tb_targets.py
"""
Loader for the TBTargets tab.

Provides per-guild, per-phase, per-planet "target_stars" configuration
used by the auto-summary's per-planet estimation line:

    *Hoth* — Stars 2/3
      To star 3: 100M points missing
      Est: 50M en platoons + 50M

Design (matches map_config.py conventions):
  * Load-once at bot startup; refreshable via /tb_reload_targets.
  * Fail-soft: missing tab → empty TBTargets, formatter skips estimation
    lines silently.
  * Validate at load time, log warnings for bad rows.

Sheet structure:
  Tab name: TBTargets
  Columns (case-insensitive, position-independent):
    guild_name      Free-text guild name (must match snap.guild_name).
    phase           Integer 1..6.
    planet_zone_id  Full zone id, e.g. "tb3_mixed_phase04_conflict01".
    target_stars    Integer 0..3 (the number of stars officers want
                    to hit by end of TB).

Why no tb_definition_id column:
  Confirmed in design: this guild runs one TB type. Adding the column
  later is a non-breaking change (extend the lookup key tuple).

Why no global/default fallback:
  Targets are inherently per-guild (different rosters have different
  realistic goals). Officers will configure every guild explicitly.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from ..sheets import try_get_worksheet

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants — sheet/tab and column names
# ---------------------------------------------------------------------------

TARGETS_TAB: str = "TBTargets"

_REQUIRED_COLS = (
    "guild_name",
    "phase",
    "planet_zone_id",
    "target_stars",
)

# Stars must be in [0, _MAX_STARS]. 0 means "no aspiration this phase,
# just measure" — a valid configuration. Out-of-range stars get warned
# and the row is skipped.
_MAX_STARS: int = 3


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------

# Composite key: (guild_name, phase, zone_id) → target_stars.
# Lowercased guild_name so officer-typed casing in the sheet doesn't
# diverge from the snapshot's casing.
_Key = Tuple[str, int, str]


@dataclass(frozen=True, slots=True)
class TBTargets:
    """
    Container for all loaded targets.

    `is_empty` is true when the sheet is missing or contained no valid
    rows — caller (formatter) treats this as "no targets configured"
    and skips the Est line for every planet.
    """
    targets: Dict[_Key, int] = field(default_factory=dict)

    @property
    def is_empty(self) -> bool:
        return not self.targets

    def lookup(
        self,
        guild_name: str,
        phase: int,
        zone_id: str,
    ) -> Optional[int]:
        """
        Return target_stars for a (guild, phase, zone) triple, or None
        if no target is configured. None is the "show 'sin objetivo'"
        path in the formatter.
        """
        key = (guild_name.strip().lower(), phase, zone_id)
        return self.targets.get(key)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_tb_targets() -> TBTargets:
    """
    Read the TBTargets sheet and return a fully-validated TBTargets.

    Never raises. Network errors, missing tab, malformed rows — all
    logged with row-level context; the loader returns the best partial
    state it could build.
    """
    ws = try_get_worksheet(TARGETS_TAB)
    if ws is None:
        log.warning(
            "%s tab not found; TB auto-summary will skip estimation lines.",
            TARGETS_TAB,
        )
        return TBTargets()

    try:
        values = ws.get_all_values()
    except Exception as e:
        log.warning("Could not read %s: %s", TARGETS_TAB, e)
        return TBTargets()

    if not values or len(values) < 2:
        log.info("%s is empty (only header or no rows).", TARGETS_TAB)
        return TBTargets()

    headers = [h.strip().lower() for h in values[0]]
    col = {h: i for i, h in enumerate(headers)}

    missing = [c for c in _REQUIRED_COLS if c not in col]
    if missing:
        log.warning(
            "%s is missing required columns: %s. Skipping all rows.",
            TARGETS_TAB, ", ".join(missing),
        )
        return TBTargets()

    targets: Dict[_Key, int] = {}
    for row_idx, raw_row in enumerate(values[1:], start=2):
        entry = _parse_row(raw_row, col, row_idx)
        if entry is None:
            continue
        key, target_stars = entry
        if key in targets:
            log.warning(
                "%s row %d: duplicate key %r (keeping first occurrence).",
                TARGETS_TAB, row_idx, key,
            )
            continue
        targets[key] = target_stars

    log.info("TB targets loaded: %d entries", len(targets))
    return TBTargets(targets=targets)


def _parse_row(
    raw_row: List[str],
    col: Dict[str, int],
    row_idx: int,
) -> Optional[Tuple[_Key, int]]:
    """
    Parse one row. Returns (key, target_stars) on success, None on
    any unrecoverable error (with warning logged).
    """
    def cell(name: str) -> str:
        idx = col[name]
        return raw_row[idx].strip() if idx < len(raw_row) else ""

    guild_name = cell("guild_name")
    if not guild_name:
        log.warning("%s row %d: empty guild_name; skipping.", TARGETS_TAB, row_idx)
        return None

    zone_id = cell("planet_zone_id")
    if not zone_id:
        log.warning(
            "%s row %d: empty planet_zone_id; skipping.",
            TARGETS_TAB, row_idx,
        )
        return None

    raw_phase = cell("phase")
    try:
        phase = int(raw_phase)
    except ValueError:
        log.warning(
            "%s row %d (zone_id=%r): phase must be integer, got %r; skipping.",
            TARGETS_TAB, row_idx, zone_id, raw_phase,
        )
        return None
    if phase < 1 or phase > 6:
        log.warning(
            "%s row %d (zone_id=%r): phase %d out of range 1..6; skipping.",
            TARGETS_TAB, row_idx, zone_id, phase,
        )
        return None

    raw_stars = cell("target_stars")
    try:
        target_stars = int(raw_stars)
    except ValueError:
        log.warning(
            "%s row %d (zone_id=%r): target_stars must be integer, got %r; "
            "skipping.",
            TARGETS_TAB, row_idx, zone_id, raw_stars,
        )
        return None
    if target_stars < 0 or target_stars > _MAX_STARS:
        log.warning(
            "%s row %d (zone_id=%r): target_stars %d out of range 0..%d; "
            "skipping.",
            TARGETS_TAB, row_idx, zone_id, target_stars, _MAX_STARS,
        )
        return None

    key: _Key = (guild_name.lower(), phase, zone_id)
    return key, target_stars
