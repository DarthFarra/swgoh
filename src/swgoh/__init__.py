# src/swgoh/tb/__init__.py
"""
Territory Battle sub-package.

Parses C3PO TB export JSON files (raw CG game-server data) into a
normalized in-memory representation, and exposes pure-function queries
on top of it. No I/O here — Sheets and Telegram concerns live elsewhere.
"""
from .models import (
    TBSnapshot,
    Member,
    PhaseStats,
    ZoneStats,
    CategoryCounts,
)
from .parser import parse_tb_snapshot, ParseError

__all__ = [
    "TBSnapshot",
    "Member",
    "PhaseStats",
    "ZoneStats",
    "CategoryCounts",
    "parse_tb_snapshot",
    "ParseError",
]
