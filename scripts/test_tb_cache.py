#!/usr/bin/env python3
# scripts/test_tb_cache.py
"""
Smoke test for tb_cache.

Exercises the cache helpers directly: round-trip, age calculation,
defensive null/wrong-type handling, and clear().

This test does NOT exercise commands/tb.py because that module imports
the `telegram` package, which is a runtime dep of your project (PTB v20)
and not always available in dev sandboxes. The command handlers are
thin wrappers over (a) this cache + (b) the already-tested formatters,
so cache correctness + formatter correctness imply handler correctness.

For real handler validation: deploy the branch and hit the commands
in your Telegram bot.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent / "src"))

from swgoh.tb import parse_tb_snapshot  # noqa: E402
from swgoh.bot.services import tb_cache  # noqa: E402


SAMPLE = "/mnt/user-data/uploads/pTtJHHuYQcSMinxQXXZJgA-tb.json"


def test_cache_round_trip() -> None:
    print("--- cache round-trip ---")
    bot_data: dict = {}

    # Empty cache → None
    assert tb_cache.get_latest(bot_data) is None
    print("  empty cache → None  ✓")

    # Load sample, parse, put in cache
    with open(SAMPLE) as f:
        snap = parse_tb_snapshot(json.load(f))
    entry = tb_cache.set_latest(bot_data, snap, source_filename="test.json")
    assert entry.source_filename == "test.json"
    print("  set_latest returned entry with correct source_filename  ✓")

    # Read it back
    got = tb_cache.get_latest(bot_data)
    assert got is not None
    assert got.snapshot.instance_id == snap.instance_id
    print(f"  get_latest returned entry: instance={got.snapshot.instance_id}  ✓")

    # Age right after set should be 0
    age = tb_cache.age_minutes(got)
    assert age == 0, f"expected 0, got {age}"
    print(f"  age_minutes right after set: {age}m  ✓")


def test_cache_age_progression() -> None:
    print()
    print("--- cache age progression ---")
    bot_data: dict = {}

    with open(SAMPLE) as f:
        snap = parse_tb_snapshot(json.load(f))
    tb_cache.set_latest(bot_data, snap)
    entry = tb_cache.get_latest(bot_data)

    # Simulate an older entry by replacing it with one whose
    # received_at_monotonic is in the past. CachedSnapshot is frozen
    # so we create a new one rather than mutating.
    from swgoh.bot.services.tb_cache import CachedSnapshot
    older = CachedSnapshot(
        snapshot=entry.snapshot,
        received_at_monotonic=time.monotonic() - (47 * 60),
        received_at_wall=entry.received_at_wall,
        source_filename=entry.source_filename,
    )
    bot_data[tb_cache._CACHE_KEY] = older
    age = tb_cache.age_minutes(tb_cache.get_latest(bot_data))
    assert 46 <= age <= 48, f"expected ~47, got {age}"
    print(f"  age after simulated 47-minute gap: {age}m  ✓")


def test_cache_defensive() -> None:
    print()
    print("--- cache defensive paths ---")
    bot_data: dict = {}

    # Wrong type at the cache key → get_latest returns None, not crash
    bot_data[tb_cache._CACHE_KEY] = "not a CachedSnapshot"
    got = tb_cache.get_latest(bot_data)
    assert got is None, "should defensively return None for bad type"
    print("  bad type in cache → None (no crash)  ✓")

    # Now clear it
    tb_cache.clear(bot_data)
    assert tb_cache.get_latest(bot_data) is None
    assert tb_cache._CACHE_KEY not in bot_data
    print("  clear() empties the cache  ✓")


def test_cache_replace() -> None:
    """A new set_latest should replace any prior cached value."""
    print()
    print("--- cache replacement ---")
    bot_data: dict = {}

    with open(SAMPLE) as f:
        snap = parse_tb_snapshot(json.load(f))

    tb_cache.set_latest(bot_data, snap, source_filename="first.json")
    assert tb_cache.get_latest(bot_data).source_filename == "first.json"

    tb_cache.set_latest(bot_data, snap, source_filename="second.json")
    assert tb_cache.get_latest(bot_data).source_filename == "second.json"
    print("  set_latest replaces prior entry  ✓")


def main() -> int:
    try:
        test_cache_round_trip()
        test_cache_age_progression()
        test_cache_defensive()
        test_cache_replace()
    except AssertionError as e:
        print(f"\n❌ Assertion failed: {e}")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n✅ All smoke tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
