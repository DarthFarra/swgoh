#!/usr/bin/env python3
# scripts/test_map_config.py
"""
Smoke test for tb/map_config.py.

Doesn't talk to Google Sheets. Instead, monkey-patches the sheet-fetching
function to return controlled rows, then verifies the loader handles every
documented case correctly:

  * happy path — full row parses into a PlanetConfig
  * partial thresholds — t3 omitted, returns 2 thresholds
  * bad threshold values — logged and skipped, other thresholds preserved
  * out-of-range stars — logged and skipped
  * thousand-separator tolerance — "248,709,636" and "248.709.636" both parse
  * Zeffo/Mandalore pattern — 0/0/1 stars, correctly sums to 1 total
  * stars_for_score / next_threshold_above semantics
  * duplicate zone_ids — first one wins, warning logged
  * blank rows — silently ignored
  * missing required columns — whole tab rejected

Plus the strike-names tab:
  * happy path
  * tab missing — falls back to empty dict, no error
  * extra "Planet" column — ignored without complaint

Exit code: 0 if all assertions pass, 1 on any failure.
"""
from __future__ import annotations

import sys
from pathlib import Path

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent / "src"))


class FakeWorksheet:
    """Minimal stand-in for gspread.Worksheet — only get_all_values()."""
    def __init__(self, values):
        self._values = values

    def get_all_values(self):
        return self._values


def _install_fake_sheets(planet_values, strike_values):
    """
    Replace swgoh.sheets.try_get_worksheet with one that returns our
    fake worksheets keyed by tab name. Must be called before importing
    map_config (so map_config sees the patched function).

    Either argument can be None to simulate a missing tab.
    """
    import swgoh.sheets as sheets_module

    def fake_try_get_worksheet(title):
        if title == "TB_Map_Config":
            return FakeWorksheet(planet_values) if planet_values is not None else None
        if title == "TB_Strike_Names":
            return FakeWorksheet(strike_values) if strike_values is not None else None
        return None

    sheets_module.try_get_worksheet = fake_try_get_worksheet


def _reload_module():
    """Reload map_config so it picks up the patched try_get_worksheet."""
    import importlib
    import swgoh.tb.map_config as mc
    importlib.reload(mc)
    return mc


def test_happy_path():
    print("--- happy path ---")
    planet_rows = [
        # header
        ["definition_id", "zone_id", "planet_name", "phase",
         "platoon_count", "points_per_platoon",
         "t1_value", "t1_stars", "t2_value", "t2_stars",
         "t3_value", "t3_stars"],
        # one well-formed row (TB3 phase 1 conflict 1)
        ["t05D", "tb3_mixed_phase01_conflict01", "Geonosis", "1",
         "6", "10000000",
         "150000000", "1", "250000000", "1",
         "350000000", "1"],
    ]
    strike_rows = [
        ["Planet", "strike_zone_id", "mission_name"],
        ["Geonosis", "tb3_mixed_phase01_conflict01_strike01", "Boarding Action"],
    ]
    _install_fake_sheets(planet_rows, strike_rows)
    mc = _reload_module()
    cfg = mc.load_map_config()

    assert not cfg.is_empty, "config should not be empty"
    planet = cfg.planet("tb3_mixed_phase01_conflict01")
    assert planet is not None, "planet not loaded"
    assert planet.planet_name == "Geonosis"
    assert planet.phase == 1
    assert planet.platoon_count == 6
    assert planet.points_per_platoon == 10_000_000
    assert planet.max_platoon_points == 60_000_000
    assert len(planet.thresholds) == 3
    assert planet.max_stars == 3
    assert planet.stars_for_score(0) == 0
    assert planet.stars_for_score(150_000_000) == 1
    assert planet.stars_for_score(250_000_000) == 2
    assert planet.stars_for_score(350_000_000) == 3
    assert planet.stars_for_score(999_999_999) == 3
    nxt = planet.next_threshold_above(200_000_000)
    assert nxt is not None and nxt.value == 250_000_000

    # Strike name lookup
    assert cfg.strike_name("tb3_mixed_phase01_conflict01_strike01") == "Boarding Action"
    assert cfg.strike_name("tb3_mixed_phase01_conflict01_strike02") is None

    print("  full row → 3 thresholds, max_stars=3, lookups work  ✓")


def test_partial_thresholds():
    print()
    print("--- partial thresholds (only t1 and t2 populated) ---")
    planet_rows = [
        ["definition_id", "zone_id", "planet_name", "phase",
         "platoon_count", "points_per_platoon",
         "t1_value", "t1_stars", "t2_value", "t2_stars",
         "t3_value", "t3_stars"],
        ["t05D", "tb3_mixed_phase01_conflict01", "Geonosis", "1",
         "6", "10000000",
         "100000000", "1", "200000000", "2",
         "", ""],
    ]
    _install_fake_sheets(planet_rows, [])
    mc = _reload_module()
    cfg = mc.load_map_config()
    planet = cfg.planet("tb3_mixed_phase01_conflict01")
    assert planet is not None
    assert len(planet.thresholds) == 2
    assert planet.max_stars == 3  # 1+2
    print("  2 thresholds, max_stars=3 (1+2)  ✓")


def test_zeffo_mandalore_pattern():
    print()
    print("--- Zeffo/Mandalore pattern (0/0/1 stars) ---")
    planet_rows = [
        ["definition_id", "zone_id", "planet_name", "phase",
         "platoon_count", "points_per_platoon",
         "t1_value", "t1_stars", "t2_value", "t2_stars",
         "t3_value", "t3_stars"],
        ["t05D", "tb3_mixed_phase04_conflict03_bonus", "Mandalore", "4",
         "6", "18480000",
         "100000000", "0", "200000000", "0",
         "300000000", "1"],
    ]
    _install_fake_sheets(planet_rows, [])
    mc = _reload_module()
    cfg = mc.load_map_config()
    planet = cfg.planet("tb3_mixed_phase04_conflict03_bonus")
    assert planet is not None
    assert planet.max_stars == 1, f"expected 1 max star, got {planet.max_stars}"
    assert planet.stars_for_score(50_000_000) == 0
    assert planet.stars_for_score(150_000_000) == 0  # crossed t1 but t1 grants 0 stars
    assert planet.stars_for_score(250_000_000) == 0  # crossed t2 still 0
    assert planet.stars_for_score(350_000_000) == 1  # crossed t3, gets the star
    # next_threshold_above at score 250M should still point to t3
    nxt = planet.next_threshold_above(250_000_000)
    assert nxt is not None and nxt.value == 300_000_000
    assert nxt.stars == 1
    print("  reward-only thresholds (0+0+1=1 max star) work correctly  ✓")


def test_thousand_separator_tolerance():
    print()
    print("--- thousand-separator tolerance ---")
    planet_rows = [
        ["definition_id", "zone_id", "planet_name", "phase",
         "platoon_count", "points_per_platoon",
         "t1_value", "t1_stars", "t2_value", "t2_stars",
         "t3_value", "t3_stars"],
        # commas as thousands separators (US style)
        ["t05D", "tb3_mixed_phase01_conflict01", "Geonosis", "1",
         "6", "10,000,000",
         "150,000,000", "1", "", "", "", ""],
        # dots as thousands separators (European style)
        ["t05D", "tb3_mixed_phase01_conflict02", "Sullust", "1",
         "6", "10.000.000",
         "150.000.000", "1", "", "", "", ""],
    ]
    _install_fake_sheets(planet_rows, [])
    mc = _reload_module()
    cfg = mc.load_map_config()
    p1 = cfg.planet("tb3_mixed_phase01_conflict01")
    p2 = cfg.planet("tb3_mixed_phase01_conflict02")
    assert p1 is not None and p1.points_per_platoon == 10_000_000
    assert p2 is not None and p2.points_per_platoon == 10_000_000
    assert p1.thresholds[0].value == 150_000_000
    assert p2.thresholds[0].value == 150_000_000
    print(f"  commas:  {p1.points_per_platoon:,}, threshold {p1.thresholds[0].value:,}  ✓")
    print(f"  dots:    {p2.points_per_platoon:,}, threshold {p2.thresholds[0].value:,}  ✓")


def test_bad_rows():
    print()
    print("--- bad rows (skipped with warnings) ---")
    planet_rows = [
        ["definition_id", "zone_id", "planet_name", "phase",
         "platoon_count", "points_per_platoon",
         "t1_value", "t1_stars", "t2_value", "t2_stars",
         "t3_value", "t3_stars"],
        # good row
        ["t05D", "tb3_mixed_phase01_conflict01", "Geonosis", "1",
         "6", "10000000",
         "100000000", "1", "200000000", "1", "300000000", "1"],
        # bad phase
        ["t05D", "tb3_mixed_phase01_conflict02", "Sullust", "abc",
         "6", "10000000", "", "", "", "", "", ""],
        # bad platoon_count → falls back to 6 (warning, but row kept)
        ["t05D", "tb3_mixed_phase01_conflict03", "Mustafar", "1",
         "-1", "10000000", "", "", "", "", "", ""],
        # bad threshold value
        ["t05D", "tb3_mixed_phase02_conflict01", "Coruscant", "2",
         "6", "11000000",
         "not_a_number", "1", "200000000", "1", "300000000", "1"],
        # out-of-range stars
        ["t05D", "tb3_mixed_phase02_conflict02", "Hoth", "2",
         "6", "11000000",
         "100000000", "5", "200000000", "1", "300000000", "1"],
        # duplicate zone_id (first one wins)
        ["t05D", "tb3_mixed_phase01_conflict01", "DuplicateName", "1",
         "6", "999999999",
         "", "", "", "", "", ""],
        # blank row (silently skipped)
        ["", "", "", "", "", "", "", "", "", "", "", ""],
    ]
    _install_fake_sheets(planet_rows, [])
    mc = _reload_module()
    cfg = mc.load_map_config()
    # Sullust skipped (bad phase)
    assert cfg.planet("tb3_mixed_phase01_conflict02") is None
    # Mustafar kept but with default platoon_count=6
    mustafar = cfg.planet("tb3_mixed_phase01_conflict03")
    assert mustafar is not None and mustafar.platoon_count == 6
    # Coruscant: t1 skipped, t2+t3 preserved
    coruscant = cfg.planet("tb3_mixed_phase02_conflict01")
    assert coruscant is not None and len(coruscant.thresholds) == 2
    # Hoth: t1 skipped (stars out of range), t2+t3 kept
    hoth = cfg.planet("tb3_mixed_phase02_conflict02")
    assert hoth is not None and len(hoth.thresholds) == 2
    # Duplicate: first occurrence kept (real planet name, not "DuplicateName")
    g = cfg.planet("tb3_mixed_phase01_conflict01")
    assert g is not None and g.planet_name == "Geonosis"
    print("  bad phase → skipped, bad platoon_count → defaulted")
    print("  bad threshold → that threshold skipped, others kept")
    print("  duplicate zone_id → first one wins")
    print("  all checks pass  ✓")


def test_missing_required_columns():
    print()
    print("--- missing required columns → tab rejected ---")
    planet_rows = [
        # missing planet_name column
        ["definition_id", "zone_id", "phase",
         "platoon_count", "points_per_platoon"],
        ["t05D", "tb3_mixed_phase01_conflict01", "1", "6", "10000000"],
    ]
    _install_fake_sheets(planet_rows, [])
    mc = _reload_module()
    cfg = mc.load_map_config()
    assert cfg.is_empty, "should reject the whole tab when planet_name missing"
    print("  required column missing → entire tab rejected  ✓")


def test_missing_tabs():
    print()
    print("--- both tabs missing ---")
    _install_fake_sheets(None, None)
    mc = _reload_module()
    cfg = mc.load_map_config()
    assert cfg.is_empty
    assert cfg.planet("anything") is None
    assert cfg.strike_name("anything") is None
    print("  no tabs → empty config, no crash  ✓")


def test_strike_names_extra_column():
    print()
    print("--- TB_Strike_Names with extra 'Planet' column ---")
    # Minimal valid planet sheet
    planet_rows = [
        ["definition_id", "zone_id", "planet_name", "phase",
         "platoon_count", "points_per_platoon",
         "t1_value", "t1_stars", "t2_value", "t2_stars",
         "t3_value", "t3_stars"],
        ["t05D", "tb3_mixed_phase01_conflict01", "Geonosis", "1",
         "6", "10000000", "", "", "", "", "", ""],
    ]
    strike_rows = [
        # The order is: Planet, strike_zone_id, mission_name.
        # We should ignore the "Planet" column entirely.
        ["Planet", "strike_zone_id", "mission_name"],
        ["Geonosis", "tb3_mixed_phase01_conflict01_strike01", "Mission A"],
        ["Geonosis", "tb3_mixed_phase01_conflict01_strike02", "Mission B"],
    ]
    _install_fake_sheets(planet_rows, strike_rows)
    mc = _reload_module()
    cfg = mc.load_map_config()
    assert len(cfg.strike_names) == 2
    assert cfg.strike_name("tb3_mixed_phase01_conflict01_strike01") == "Mission A"
    print("  3-column strike sheet parses correctly, 'Planet' column ignored  ✓")


def main() -> int:
    tests = [
        test_happy_path,
        test_partial_thresholds,
        test_zeffo_mandalore_pattern,
        test_thousand_separator_tolerance,
        test_bad_rows,
        test_missing_required_columns,
        test_missing_tabs,
        test_strike_names_extra_column,
    ]
    for t in tests:
        try:
            t()
        except AssertionError as e:
            print(f"\n❌ {t.__name__} failed: {e}")
            return 1
        except Exception as e:
            print(f"\n❌ {t.__name__} crashed: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return 1
    print()
    print("✅ All smoke tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
