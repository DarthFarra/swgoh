#!/usr/bin/env python3
# scripts/preview_tb_messages.py
"""
Render Telegram messages from a sample C3PO export.

Usage:
    python scripts/preview_tb_messages.py path/to/c3po-tb-export.json

Prints each public formatter's output to stdout, separated by clear
banners, so you can eyeball what officers will see in Telegram before
any Discord/Telegram code is involved.

This is purely an inspection tool — the formatters themselves take no
arguments beyond a TBSnapshot and optional tunables.
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent / "src"))

from swgoh.tb import (  # noqa: E402
    parse_tb_snapshot,
    ParseError,
    format_auto_summary,
    format_status,
    format_failed_specials,
    format_top_contributors,
    format_no_data,
)


logging.basicConfig(
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    level=logging.WARNING,  # quiet during preview
)


def _banner(title: str) -> None:
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


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

    _banner("format_auto_summary(snap)  [pushed to Telegram on each new export]")
    print(format_auto_summary(snap))

    _banner("format_status(snap, age_minutes=0)  [response to /tb_status, fresh]")
    print(format_status(snap, age_minutes=0))

    _banner("format_status(snap, age_minutes=45)  [response to /tb_status, stale]")
    print(format_status(snap, age_minutes=45))

    _banner("format_failed_specials(snap)  [response to /tb_failed_specials]")
    print(format_failed_specials(snap))

    _banner("format_top_contributors(snap, by='summary', n=10)")
    print(format_top_contributors(snap, by="summary", n=10))

    _banner("format_top_contributors(snap, by='power', n=5)")
    print(format_top_contributors(snap, by="power", n=5))

    _banner("format_top_contributors(snap, by='strike_encounter', n=5, phase=3)")
    print(format_top_contributors(snap, by="strike_encounter", n=5, phase=3))

    _banner("format_no_data('no_export_yet')")
    print(format_no_data("no_export_yet"))

    _banner("format_no_data('bot_restarted')")
    print(format_no_data("bot_restarted"))

    # Quick statistics on message sizes — useful to catch truncation regressions.
    print()
    print("--- Message lengths (chars) ---")
    for name, msg in (
        ("auto_summary",     format_auto_summary(snap)),
        ("status_fresh",     format_status(snap, age_minutes=0)),
        ("status_stale",     format_status(snap, age_minutes=45)),
        ("failed_specials",  format_failed_specials(snap)),
        ("top_summary_10",   format_top_contributors(snap, by="summary", n=10)),
    ):
        print(f"  {name:<20} {len(msg):>5} chars")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
