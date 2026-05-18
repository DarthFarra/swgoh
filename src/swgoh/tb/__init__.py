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
from .analysis import (
    # Result types
    DeploymentGap,
    SpecialFailure,
    Contribution,
    PhaseProgress,
    # Exception-list queries
    members_missing_deployment,
    members_with_no_strikes,
    members_with_no_summary,
    members_with_failed_specials,
    # Aggregate queries
    phase_progress,
    time_remaining,
    # Ranking queries
    top_contributors,
    member_phase_breakdown,
    # Constants
    DEFAULT_DEPLOYMENT_THRESHOLD_PCT,
    DEFAULT_TOP_N,
)

__all__ = [
    # Models
    "TBSnapshot",
    "Member",
    "PhaseStats",
    "ZoneStats",
    "CategoryCounts",
    # Parser
    "parse_tb_snapshot",
    "ParseError",
    # Analysis result types
    "DeploymentGap",
    "SpecialFailure",
    "Contribution",
    "PhaseProgress",
    # Analysis queries
    "members_missing_deployment",
    "members_with_no_strikes",
    "members_with_no_summary",
    "members_with_failed_specials",
    "phase_progress",
    "time_remaining",
    "top_contributors",
    "member_phase_breakdown",
    # Constants
    "DEFAULT_DEPLOYMENT_THRESHOLD_PCT",
    "DEFAULT_TOP_N",
]
