#!/usr/bin/env python3
# scripts/test_planet_briefing.py
"""
Smoke test for format_planet_briefing and the new format_status /
format_auto_summary in formatters.py.

Tests against the real sample JSON, simulating different MapConfig
states:

  * empty config (degraded path, generic labels)
  * full config (planet names, thresholds, platoon math)
  * partial config (some planets in sheet, others not)
  * Zeffo/Mandalore pattern (0/0/1 star thresholds)
  * multi-message split (synthesised long config to force overflow)

Plus structural assertions:
  * returns a List[str], never None or a single string
  * each message is under SOFT_MESSAGE_CAP
  * messages contain "(continued)" marker when split
  * stale-data footer appears only when include_stale_hint=True

Exit code: 0 if all pass, 1 on any failure.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent / "src"))

from swgoh.tb import parse_tb_snapshot  # noqa: E402
from swgoh.tb.formatters import (  # noqa: E402
    SOFT_MESSAGE_CAP,
    format_auto_summary,
    format_status,
    format_planet_briefing,
)
from swgoh.tb.map_config import MapConfig, PlanetConfig, StarThreshold  # noqa: E402


SAMPLE = "/mnt/user-data/uploads/pTtJHHuYQcSMinxQXXZJgA-tb.json"


def _load_sample():
    with open(SAMPLE) as f:
        return parse_tb_snapshot(json.load(f))


def _full_config_for_sample() -> MapConfig:
    """
    Build a MapConfig covering the four active planets in the sample
    snapshot, with realistic-ish thresholds. Used to verify the full
    rendering path.
    """
    return MapConfig(
        planets={
            "tb3_mixed_phase04_conflict03_bonus": PlanetConfig(
                definition_id="t05D",
                zone_id="tb3_mixed_phase04_conflict03_bonus",
                planet_name="Mandalore",
                phase=4, platoon_count=6, points_per_platoon=18_480_000,
                thresholds=(
                    StarThreshold(100_000_000, 0),
                    StarThreshold(200_000_000, 0),
                    StarThreshold(300_000_000, 1),
                ),
            ),
            "tb3_mixed_phase05_conflict01": PlanetConfig(
                definition_id="t05D",
                zone_id="tb3_mixed_phase05_conflict01",
                planet_name="Ring of Kafrene",
                phase=5, platoon_count=6, points_per_platoon=33_264_000,
                thresholds=(
                    StarThreshold(200_000_000, 1),
                    StarThreshold(450_000_000, 1),
                    StarThreshold(700_000_000, 1),
                ),
            ),
            "tb3_mixed_phase05_conflict02": PlanetConfig(
                definition_id="t05D",
                zone_id="tb3_mixed_phase05_conflict02",
                planet_name="Geonosis",
                phase=5, platoon_count=6, points_per_platoon=33_264_000,
                thresholds=(
                    StarThreshold(200_000_000, 1),
                    StarThreshold(450_000_000, 1),
                    StarThreshold(700_000_000, 1),
                ),
            ),
            "tb3_mixed_phase06_conflict03": PlanetConfig(
                definition_id="t05D",
                zone_id="tb3_mixed_phase06_conflict03",
                planet_name="Hoth",
                phase=6, platoon_count=6, points_per_platoon=86_486_400,
                thresholds=(
                    StarThreshold(400_000_000, 1),
                    StarThreshold(700_000_000, 1),
                    StarThreshold(1_000_000_000, 1),
                ),
            ),
        },
        strike_names={
            "tb3_mixed_phase04_conflict03_bonus_strike01": "Boarding Action",
            "tb3_mixed_phase04_conflict03_bonus_strike02": "Cargo Run",
        },
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_returns_list_of_strings():
    print("--- format_auto_summary returns List[str] ---")
    snap = _load_sample()
    result = format_auto_summary(snap, MapConfig())
    assert isinstance(result, list), f"expected list, got {type(result).__name__}"
    assert all(isinstance(m, str) for m in result), "all elements should be strings"
    assert len(result) >= 1, "should return at least one message"
    print(f"  returned {len(result)} message(s)  ✓")


def test_messages_respect_cap():
    print()
    print("--- messages respect SOFT_MESSAGE_CAP ---")
    snap = _load_sample()
    cfg = _full_config_for_sample()
    messages = format_auto_summary(snap, cfg)
    for i, m in enumerate(messages):
        assert len(m) <= SOFT_MESSAGE_CAP, (
            f"message {i+1} is {len(m)} chars, > cap {SOFT_MESSAGE_CAP}"
        )
    print(f"  all {len(messages)} message(s) within {SOFT_MESSAGE_CAP} chars  ✓")


def test_empty_config_uses_generic_labels():
    print()
    print("--- empty MapConfig → generic labels ---")
    snap = _load_sample()
    messages = format_auto_summary(snap, MapConfig())
    full_text = "\n".join(messages)
    # With no config, real planet names shouldn't appear
    assert "Mandalore" not in full_text, "planet name leaked without config"
    assert "Hoth" not in full_text, "planet name leaked without config"
    # And generic labels should appear
    assert "Phase 4 T3" in full_text, f"expected generic label, got: {full_text[:200]}"
    assert "Stars" not in full_text, "shouldn't show star info without config"
    print("  generic labels appear, real names absent, star info hidden  ✓")


def test_full_config_uses_planet_names():
    print()
    print("--- full MapConfig → real planet names and stars ---")
    snap = _load_sample()
    cfg = _full_config_for_sample()
    messages = format_auto_summary(snap, cfg)
    full_text = "\n".join(messages)
    assert "Mandalore" in full_text, "Mandalore name missing"
    assert "Ring of Kafrene" in full_text, "Ring of Kafrene name missing"
    assert "Geonosis" in full_text, "Geonosis name missing"
    assert "Hoth" in full_text, "Hoth name missing"
    assert "Stars 1/1" in full_text, "Mandalore stars 1/1 missing"
    print("  all planet names present, star counts rendered  ✓")


def test_mandalore_zero_star_pattern():
    print()
    print("--- Mandalore-style 0/0/1 thresholds → 1 max star ---")
    snap = _load_sample()
    cfg = _full_config_for_sample()
    messages = format_auto_summary(snap, cfg)
    full_text = "\n".join(messages)
    # Mandalore has 0/0/1 thresholds → max_stars=1. Score 396M > 300M → 1/1.
    # The header line should say "Stars 1/1", NOT "Stars 1/3".
    assert "Stars 1/1" in full_text, "Mandalore should be 1/1 stars"
    assert "Mandalore* — Stars 1/3" not in full_text, "should NOT be 1/3"
    print("  Mandalore renders as 1/1, not 1/3  ✓")


def test_strike_name_lookup_applied():
    print()
    print("--- strike_name lookup replaces 'Mission N' label ---")
    snap = _load_sample()
    cfg = _full_config_for_sample()
    messages = format_auto_summary(snap, cfg)
    full_text = "\n".join(messages)
    # "Boarding Action" is configured for Mandalore strike01
    assert "Boarding Action" in full_text, (
        f"friendly name 'Boarding Action' not rendered; got:\n{full_text[:1000]}"
    )
    assert "Cargo Run" in full_text, "Cargo Run not rendered"
    print("  friendly mission names appear in output  ✓")


def test_unconfigured_planet_falls_back():
    print()
    print("--- partial config: configured planets show names, others use generic ---")
    snap = _load_sample()
    # Config with only Mandalore configured
    partial_cfg = MapConfig(
        planets={
            "tb3_mixed_phase04_conflict03_bonus": PlanetConfig(
                definition_id="t05D",
                zone_id="tb3_mixed_phase04_conflict03_bonus",
                planet_name="Mandalore",
                phase=4, platoon_count=6, points_per_platoon=18_480_000,
                thresholds=(
                    StarThreshold(100_000_000, 0),
                    StarThreshold(200_000_000, 0),
                    StarThreshold(300_000_000, 1),
                ),
            ),
        },
        strike_names={},
    )
    messages = format_auto_summary(snap, partial_cfg)
    full_text = "\n".join(messages)
    assert "Mandalore" in full_text, "configured planet should show name"
    assert "Phase 5 T1" in full_text, "unconfigured planet should use generic label"
    assert "Phase 6 T3" in full_text, "unconfigured planet should use generic label"
    print("  partial config: configured names appear, others fall back  ✓")


def test_stale_data_footer_in_status():
    print()
    print("--- format_status appends data-age footer ---")
    snap = _load_sample()
    cfg = MapConfig()
    messages_with_age = format_status(snap, map_config=cfg, age_minutes=17)
    messages_without_age = format_auto_summary(snap, cfg)
    full_status = "\n".join(messages_with_age)
    full_auto = "\n".join(messages_without_age)
    # Stale hint kicks in at >= STALE_HINT_THRESHOLD_MIN; 17 should trigger
    assert "17m old" in full_status, f"stale hint missing: {full_status[-200:]}"
    assert "17m old" not in full_auto, "auto-summary should not have age hint"
    print("  /tb_status shows '17m old', auto-summary omits it  ✓")


def test_no_active_planets():
    print()
    print("--- empty active list → header + 'no planets' note ---")
    # Hand-craft a snapshot where all zones are locked or completed,
    # by mutating the sample's zone states. Easiest: use a fresh
    # parse and clear active states.
    import dataclasses
    snap = _load_sample()
    # Replace zones with all-locked versions
    from swgoh.tb.models import ZoneStats
    locked_zones = {
        zid: dataclasses.replace(z, zone_state=1)
        for zid, z in snap.zones.items()
    }
    snap_no_active = dataclasses.replace(snap, zones=locked_zones)
    messages = format_auto_summary(snap_no_active, MapConfig())
    full = "\n".join(messages)
    assert "No active planets" in full, f"expected 'No active planets' message, got:\n{full}"
    print("  graceful empty-state message rendered  ✓")


def test_threshold_labels_distinguish_star_vs_reward():
    print()
    print("--- 0-star thresholds labeled 'reward', star thresholds labeled 'star' ---")
    # Synthesize a snapshot where Mandalore hasn't crossed any threshold yet
    # (score < first threshold). Need to mutate the zone score.
    import dataclasses
    from swgoh.tb.models import ZoneStats
    snap = _load_sample()
    cfg = _full_config_for_sample()
    new_zones = dict(snap.zones)
    z = new_zones["tb3_mixed_phase04_conflict03_bonus"]
    new_zones["tb3_mixed_phase04_conflict03_bonus"] = dataclasses.replace(
        z, score=50_000_000, zone_state=2,
    )
    snap_low = dataclasses.replace(snap, zones=new_zones)
    messages = format_auto_summary(snap_low, cfg)
    full = "\n".join(messages)
    # First two thresholds are stars=0 (reward-only), third is star
    assert "To reward 1" in full, f"reward-only threshold missing 'reward' label:\n{full[:1500]}"
    assert "To reward 2" in full, "second reward-only threshold should appear"
    assert "To star 3" in full, "third threshold should appear as star"
    print("  reward thresholds say 'reward', star thresholds say 'star'  ✓")


def test_multi_message_split():
    print()
    print("--- forced overflow splits into multiple messages ---")
    # The real sample fits in one message (~1500 chars). To force a split,
    # I'll temporarily monkey-patch SOFT_MESSAGE_CAP. Better: synthesize a
    # MapConfig with many extra planets... but they have to exist in the
    # snapshot too. Simplest: directly call format_planet_briefing with
    # tiny cap.
    import swgoh.tb.formatters as fmt
    original_cap = fmt.SOFT_MESSAGE_CAP
    fmt.SOFT_MESSAGE_CAP = 800   # force split
    try:
        snap = _load_sample()
        cfg = _full_config_for_sample()
        messages = format_planet_briefing(snap, cfg, age_minutes=0, include_stale_hint=False)
        assert len(messages) >= 2, f"expected >= 2 messages with small cap, got {len(messages)}"
        # Continuation marker appears in messages after the first
        assert "_(continued)_" in messages[1], (
            f"second message missing continuation marker:\n{messages[1][:200]}"
        )
        print(f"  forced cap=800 → {len(messages)} messages, continuation marker present  ✓")
    finally:
        fmt.SOFT_MESSAGE_CAP = original_cap


def main() -> int:
    tests = [
        test_returns_list_of_strings,
        test_messages_respect_cap,
        test_empty_config_uses_generic_labels,
        test_full_config_uses_planet_names,
        test_mandalore_zero_star_pattern,
        test_strike_name_lookup_applied,
        test_unconfigured_planet_falls_back,
        test_stale_data_footer_in_status,
        test_no_active_planets,
        test_threshold_labels_distinguish_star_vs_reward,
        test_multi_message_split,
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
    print("✅ All planet-briefing smoke tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
