# src/swgoh/tb/map_config.py
"""
Loader for the TB_Map_Config and TB_Strike_Names tabs.

Provides per-planet metadata (display name, point thresholds, threshold-star
mapping, platoon point values) and per-strike-mission friendly names that
aren't available in CG's TB export JSON.

Design (per the architecture conversation):
  * Load-once, cache-forever. Sheet rarely changes; a bot restart picks
    up edits. If we later need hot-reload, a `/tb_reload_config`
    command would be a single-line addition.
  * Fail-soft. A missing tab, missing row, or bad cell degrades the
    output (planet shows as "T1" instead of "Mandalore", thresholds
    are skipped) rather than blocking the whole TB pipeline.
  * Validate at load time. Catch typos and column swaps early — bad
    config produces warnings in the bot startup log, not silent errors
    at display time.

Two-tab structure (decided in conversation):

  TB_Map_Config (20 rows for TB3 Mixed; 17 required columns):
    definition_id        e.g. "t05D"
    zone_id              full zone id, e.g. "tb3_mixed_phase04_conflict03_bonus"
    planet_name          friendly display, e.g. "Mandalore"
    phase                phase number (1..6)
    platoon_count        normally 6
    points_per_platoon   e.g. 18480000 for phase 4 zones
    t1_value, t1_stars   threshold 1 value + stars granted (0 = reward-only)
    t2_value, t2_stars   threshold 2 value + stars granted
    t3_value, t3_stars   threshold 3 value + stars granted

  TB_Strike_Names (up to 87 rows; 3 columns):
    Planet               human-only column for officer convenience; ignored
    strike_zone_id       e.g. "tb3_mixed_phase04_conflict03_bonus_strike01"
    mission_name         e.g. "Boarding Action"

Public surface:
  MapConfig         — frozen container with all lookups (planet, strike_name).
  PlanetConfig      — frozen per-planet record.
  load_map_config() — synchronous loader, returns a fully-validated MapConfig.

Threading note: `load_map_config` does I/O via gspread; expect ~1-2s
on first call. Callers (currently the bot startup hook) should run it
during initialization, not on every command.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from ..sheets import try_get_worksheet

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sheet/tab names. Hardcoded rather than env-configurable: these are
# guild-internal officer-facing tabs, not deployment knobs.
# ---------------------------------------------------------------------------
PLANETS_TAB: str = "TB_Map_Config"
STRIKES_TAB: str = "TB_Strike_Names"

# Maximum stars per zone in SWGoH (used for validation only).
_MAX_STARS_PER_ZONE: int = 3


# ---------------------------------------------------------------------------
# Public data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class StarThreshold:
    """
    One scoring threshold for a planet.

    `stars` is the count granted for crossing this threshold (0 for the
    Zeffo/Mandalore-style reward-only thresholds where the first two
    thresholds give rewards but no stars, and only the third gives the
    1 star).

    `value` is the points needed to cross. Always > 0 for valid rows;
    missing/invalid thresholds are simply not added to the planet's
    thresholds list.
    """
    value: int
    stars: int


@dataclass(frozen=True, slots=True)
class PlanetConfig:
    """
    Everything we know about one planet from the config sheet.

    Several fields are "best-effort": if the sheet has a typo we fall
    back to safe defaults (empty thresholds list, default platoon
    count of 6) rather than blocking lookups for the whole planet.
    The `is_complete` property reports whether the row passed every
    validation; the formatter can use this to decide whether to show
    star info or fall back to score-only.
    """
    definition_id: str
    zone_id: str
    planet_name: str
    phase: int
    platoon_count: int
    points_per_platoon: int
    # Thresholds in ascending order. Always 0-3 entries; rows with all
    # three thresholds present are typical, partial rows are tolerated.
    thresholds: Tuple[StarThreshold, ...] = field(default_factory=tuple)

    @property
    def max_platoon_points(self) -> int:
        """Bonus awarded if every platoon on this planet is completed."""
        return self.platoon_count * self.points_per_platoon

    @property
    def max_stars(self) -> int:
        """Total stars achievable via thresholds on this planet (0-3)."""
        return sum(t.stars for t in self.thresholds)

    def stars_for_score(self, score: int) -> int:
        """
        Stars earned at the given score.

        Sums the `stars` value of every threshold whose `value` is <= score.
        Works correctly for reward-only thresholds (which contribute 0) and
        for partial-threshold rows (only some thresholds populated).
        """
        return sum(t.stars for t in self.thresholds if score >= t.value)

    def next_threshold_above(self, score: int) -> Optional[StarThreshold]:
        """
        First threshold the score has not yet crossed, or None if all
        thresholds have been hit. Useful for "points needed for next star"
        display.
        """
        for t in self.thresholds:
            if score < t.value:
                return t
        return None


@dataclass(frozen=True, slots=True)
class MapConfig:
    """
    Fully-loaded TB configuration. Empty (`planets == {} and
    strike_names == {}`) if both sheets are missing or unparseable —
    the formatter checks `is_empty` and falls back to generic labels.
    """
    # Lookup by full zone_id, e.g. "tb3_mixed_phase04_conflict03_bonus"
    planets: Dict[str, PlanetConfig] = field(default_factory=dict)
    # Lookup by full strike zone_id, e.g. "..._strike01"
    strike_names: Dict[str, str] = field(default_factory=dict)

    @property
    def is_empty(self) -> bool:
        """True if no planet config was loaded — formatter should degrade."""
        return not self.planets

    def planet(self, zone_id: str) -> Optional[PlanetConfig]:
        """Return the PlanetConfig for a zone_id, or None if unknown."""
        return self.planets.get(zone_id)

    def strike_name(self, strike_zone_id: str) -> Optional[str]:
        """Return the friendly mission name, or None if unset."""
        return self.strike_names.get(strike_zone_id)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_map_config() -> MapConfig:
    """
    Load both tabs from the configured spreadsheet.

    Never raises: any failure (missing tab, network blip, parse error)
    is logged and the loader returns the best partial config it could
    build. Callers should always get a valid MapConfig object back.

    Returns:
      MapConfig with .is_empty=True if neither tab loaded successfully.
    """
    planets = _load_planets()
    strikes = _load_strike_names()
    log.info(
        "TB map config loaded: %d planets, %d strike names",
        len(planets), len(strikes),
    )
    return MapConfig(planets=planets, strike_names=strikes)


# ---------------------------------------------------------------------------
# Planet rows
# ---------------------------------------------------------------------------

# Required columns in TB_Map_Config. Order doesn't matter; matched by name.
_PLANET_REQUIRED_COLS = (
    "definition_id",
    "zone_id",
    "planet_name",
    "phase",
    "platoon_count",
    "points_per_platoon",
)
# Optional columns (per-threshold). Missing thresholds are tolerated.
_PLANET_THRESHOLD_COLS = (
    ("t1_value", "t1_stars"),
    ("t2_value", "t2_stars"),
    ("t3_value", "t3_stars"),
)


def _load_planets() -> Dict[str, PlanetConfig]:
    """
    Read TB_Map_Config into {zone_id: PlanetConfig}.

    Bad rows are skipped with a warning naming the row number and the
    specific problem. Validation is loud — typos in the sheet shouldn't
    silently degrade the production output.
    """
    ws = try_get_worksheet(PLANETS_TAB)
    if ws is None:
        log.warning(
            "%s tab not found; TB output will fall back to generic labels.",
            PLANETS_TAB,
        )
        return {}

    try:
        values = ws.get_all_values()
    except Exception as e:
        log.warning("Could not read %s: %s", PLANETS_TAB, e)
        return {}

    if not values or len(values) < 2:
        log.warning("%s is empty (or only has a header row).", PLANETS_TAB)
        return {}

    headers = [h.strip().lower() for h in values[0]]
    col = {h: i for i, h in enumerate(headers)}

    missing = [c for c in _PLANET_REQUIRED_COLS if c not in col]
    if missing:
        log.warning(
            "%s is missing required columns: %s. Skipping all rows.",
            PLANETS_TAB, ", ".join(missing),
        )
        return {}

    planets: Dict[str, PlanetConfig] = {}
    for row_idx, raw_row in enumerate(values[1:], start=2):  # +1 for 1-based, +1 for header
        planet = _parse_planet_row(raw_row, col, row_idx)
        if planet is None:
            continue
        if planet.zone_id in planets:
            log.warning(
                "%s row %d: duplicate zone_id %r (keeping first occurrence).",
                PLANETS_TAB, row_idx, planet.zone_id,
            )
            continue
        planets[planet.zone_id] = planet

    return planets


def _parse_planet_row(
    raw_row: List[str],
    col: Dict[str, int],
    row_idx: int,
) -> Optional[PlanetConfig]:
    """
    Parse one row of TB_Map_Config. Returns None on fatal errors
    (required field missing or unparseable). Non-fatal issues
    (e.g. one bad threshold out of three) log a warning and proceed
    with the partial data.
    """
    def cell(name: str) -> str:
        idx = col[name]
        return raw_row[idx].strip() if idx < len(raw_row) else ""

    def int_cell(name: str) -> Optional[int]:
        raw = cell(name)
        if not raw:
            return None
        try:
            # Tolerate thousand-separators (comma or dot). Officers may paste
            # numbers from the game UI as "248,709,636" or "248.709.636".
            cleaned = raw.replace(",", "").replace(".", "").replace(" ", "")
            return int(cleaned)
        except ValueError:
            return None

    # Required string fields.
    zone_id       = cell("zone_id")
    definition_id = cell("definition_id")
    planet_name   = cell("planet_name")

    # Skip rows where zone_id is blank — looks like a placeholder row,
    # not worth a warning.
    if not zone_id:
        return None

    if not definition_id or not planet_name:
        log.warning(
            "%s row %d: missing definition_id or planet_name (zone_id=%r); skipping.",
            PLANETS_TAB, row_idx, zone_id,
        )
        return None

    phase = int_cell("phase")
    if phase is None or phase < 1 or phase > 10:
        log.warning(
            "%s row %d (zone_id=%r): invalid phase %r; skipping.",
            PLANETS_TAB, row_idx, zone_id, cell("phase"),
        )
        return None

    # Sanity check: phase field should agree with the phase encoded in zone_id.
    if f"phase{phase:02d}" not in zone_id:
        log.warning(
            "%s row %d (zone_id=%r): phase column says %d but zone_id "
            "suggests a different phase. Continuing with column value.",
            PLANETS_TAB, row_idx, zone_id, phase,
        )

    platoon_count = int_cell("platoon_count")
    if platoon_count is None or platoon_count <= 0:
        log.warning(
            "%s row %d (zone_id=%r): invalid platoon_count %r; defaulting to 6.",
            PLANETS_TAB, row_idx, zone_id, cell("platoon_count"),
        )
        platoon_count = 6

    points_per_platoon = int_cell("points_per_platoon")
    if points_per_platoon is None or points_per_platoon < 0:
        log.warning(
            "%s row %d (zone_id=%r): invalid points_per_platoon %r; "
            "defaulting to 0 (no platoon bonus shown).",
            PLANETS_TAB, row_idx, zone_id, cell("points_per_platoon"),
        )
        points_per_platoon = 0

    # Parse thresholds — best-effort, skip individual bad ones.
    thresholds = _parse_thresholds(cell, zone_id, row_idx)

    return PlanetConfig(
        definition_id=definition_id,
        zone_id=zone_id,
        planet_name=planet_name,
        phase=phase,
        platoon_count=platoon_count,
        points_per_platoon=points_per_platoon,
        thresholds=thresholds,
    )


def _parse_thresholds(
    cell_fn,
    zone_id: str,
    row_idx: int,
) -> Tuple[StarThreshold, ...]:
    """
    Parse t1..t3 threshold pairs. Empty pairs are skipped silently
    (a planet might genuinely have only 2 thresholds). Validation
    is loud only when something is clearly wrong (typo'd value, stars
    out of range, or thresholds out of ascending order).

    Returns thresholds in ascending order by value.
    """
    parsed: List[StarThreshold] = []

    for value_col, stars_col in _PLANET_THRESHOLD_COLS:
        raw_value = cell_fn(value_col)
        raw_stars = cell_fn(stars_col)

        # Fully empty pair → not configured, skip silently.
        if not raw_value and not raw_stars:
            continue

        # Partially filled → warn (likely an editing mistake).
        if not raw_value or not raw_stars:
            log.warning(
                "%s row %d (zone_id=%r): partial threshold %s/%s "
                "(value=%r, stars=%r); skipping this threshold.",
                PLANETS_TAB, row_idx, zone_id, value_col, stars_col,
                raw_value, raw_stars,
            )
            continue

        # Both filled — try to parse.
        try:
            value = int(raw_value.replace(",", "").replace(".", "").replace(" ", ""))
            stars = int(raw_stars)
        except ValueError:
            log.warning(
                "%s row %d (zone_id=%r): unparseable threshold %s/%s "
                "(value=%r, stars=%r); skipping.",
                PLANETS_TAB, row_idx, zone_id, value_col, stars_col,
                raw_value, raw_stars,
            )
            continue

        if value <= 0:
            log.warning(
                "%s row %d (zone_id=%r): %s must be > 0, got %d; skipping.",
                PLANETS_TAB, row_idx, zone_id, value_col, value,
            )
            continue

        if stars < 0 or stars > _MAX_STARS_PER_ZONE:
            log.warning(
                "%s row %d (zone_id=%r): %s out of range 0..%d, got %d; skipping.",
                PLANETS_TAB, row_idx, zone_id, stars_col,
                _MAX_STARS_PER_ZONE, stars,
            )
            continue

        parsed.append(StarThreshold(value=value, stars=stars))

    # Sort ascending by value for next_threshold_above() to work right.
    parsed.sort(key=lambda t: t.value)

    # Sanity check: total stars across thresholds should be <= 3.
    total_stars = sum(t.stars for t in parsed)
    if total_stars > _MAX_STARS_PER_ZONE:
        log.warning(
            "%s row %d (zone_id=%r): total stars %d exceeds max %d "
            "(check t1_stars/t2_stars/t3_stars columns). "
            "Stars-for-score will still work but the values are likely wrong.",
            PLANETS_TAB, row_idx, zone_id, total_stars, _MAX_STARS_PER_ZONE,
        )

    return tuple(parsed)


# ---------------------------------------------------------------------------
# Strike-name rows
# ---------------------------------------------------------------------------

def _load_strike_names() -> Dict[str, str]:
    """
    Read TB_Strike_Names into {strike_zone_id: mission_name}.

    The "Planet" column in the sheet is an officer-readability aid; we
    ignore it. Lookup is by `strike_zone_id` exactly.

    Missing tab → empty dict (mission names are optional).
    """
    ws = try_get_worksheet(STRIKES_TAB)
    if ws is None:
        log.debug(
            "%s tab not found; mission names will fall back to 'Mission N'.",
            STRIKES_TAB,
        )
        return {}

    try:
        values = ws.get_all_values()
    except Exception as e:
        log.warning("Could not read %s: %s", STRIKES_TAB, e)
        return {}

    if not values or len(values) < 2:
        log.debug("%s is empty (or only has a header row).", STRIKES_TAB)
        return {}

    headers = [h.strip().lower() for h in values[0]]
    col = {h: i for i, h in enumerate(headers)}

    if "strike_zone_id" not in col or "mission_name" not in col:
        log.warning(
            "%s missing required columns 'strike_zone_id' and/or "
            "'mission_name'. Skipping.", STRIKES_TAB,
        )
        return {}

    i_id = col["strike_zone_id"]
    i_nm = col["mission_name"]

    out: Dict[str, str] = {}
    for row_idx, raw in enumerate(values[1:], start=2):
        zid = raw[i_id].strip() if i_id < len(raw) else ""
        nm = raw[i_nm].strip() if i_nm < len(raw) else ""
        if not zid or not nm:
            # Blank rows are normal (officer populating gradually); skip
            # quietly.
            continue
        if zid in out:
            log.warning(
                "%s row %d: duplicate strike_zone_id %r (keeping first).",
                STRIKES_TAB, row_idx, zid,
            )
            continue
        out[zid] = nm

    return out
