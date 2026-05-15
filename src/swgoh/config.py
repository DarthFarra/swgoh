# src/swgoh/config.py
"""
Single source of truth for all environment variables.

Rules:
- Every env var in the project is declared here, once.
- No other file calls os.getenv() directly (except creds.py, which is
  credentials-specific and intentionally self-contained).
- Defaults are documented inline.
- Validation is fail-fast: missing required vars raise SystemExit at import
  time so the process never starts in a broken state.
"""
from __future__ import annotations

import os
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _require(name: str) -> str:
    """Return the value of a required env var, or raise SystemExit."""
    val = os.getenv(name, "").strip()
    if not val:
        raise SystemExit(
            f"Required environment variable '{name}' is not set. "
            "Check your .env file or Railway/Pi environment."
        )
    return val


def _optional(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(
            f"Environment variable '{name}' must be an integer, got: {raw!r}"
        )


def _float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        raise SystemExit(
            f"Environment variable '{name}' must be a float, got: {raw!r}"
        )


def _str_list(name: str, default: str = "") -> list[str]:
    raw = os.getenv(name, default)
    return [x.strip().upper() for x in raw.split(",") if x.strip()]


def _str_set(name: str, default: str = "") -> set[str]:
    raw = os.getenv(name, default)
    return {s.strip() for s in raw.split(",") if s.strip()}


# ---------------------------------------------------------------------------
# Comlink
# ---------------------------------------------------------------------------

COMLINK_BASE: str = _require("COMLINK_BASE")
if COMLINK_BASE.endswith("/"):
    COMLINK_BASE = COMLINK_BASE[:-1]

COMLINK_HEADERS_JSON: str = _optional("COMLINK_HEADERS_JSON")

# ---------------------------------------------------------------------------
# Google Sheets — identity
# ---------------------------------------------------------------------------

# At least one of these two must be set (validated in sheets.py at open time).
SPREADSHEET_ID: str   = _optional("SPREADSHEET_ID")
SPREADSHEET_NAME: str = _optional("SPREADSHEET_NAME")

if not SPREADSHEET_ID and not SPREADSHEET_NAME:
    raise SystemExit(
        "Set either SPREADSHEET_ID or SPREADSHEET_NAME in your environment."
    )

# ---------------------------------------------------------------------------
# Google Sheets — tab names
# Canonical names used everywhere. Override via env only if your sheet
# uses different tab names.
# ---------------------------------------------------------------------------

SHEET_GUILDS:          str = _optional("SHEET_GUILDS",          "Guilds")
SHEET_PLAYERS:         str = _optional("SHEET_PLAYERS",         "Players")
SHEET_PLAYER_UNITS:    str = _optional("SHEET_PLAYER_UNITS",    "Player_Units")
SHEET_PLAYER_SKILLS:   str = _optional("SHEET_PLAYER_SKILLS",   "Player_Skills")
SHEET_CHARACTERS:      str = _optional("SHEET_CHARACTERS",      "Characters")
SHEET_SHIPS:           str = _optional("SHEET_SHIPS",           "Ships")
SHEET_USERS:           str = _optional("SHEET_USERS",           "Usuarios")
SHEET_ASSIGNMENTS:     str = _optional("SHEET_ASSIGNMENTS",     "Asignaciones ROTE")

# Fallback ROTE sheet name used when a guild has no ROTE column configured.
# Overridable via env if your default sheet has a different name.
DEFAULT_ROTE_SHEET:    str = _optional("DEFAULT_ROTE_SHEET",    "Asignaciones ROTE")
SHEET_TICKET_SNAPSHOTS: str = _optional("SHEET_TICKET_SNAPSHOTS", "Ticket_Snapshots")

# ---------------------------------------------------------------------------
# Telegram bot
# ---------------------------------------------------------------------------

TELEGRAM_BOT_TOKEN: str = _require("TELEGRAM_BOT_TOKEN")

# Chat IDs allowed to run /syncdata (comma-separated)
SYNC_DATA_ALLOWED_CHATS: set[str] = _str_set(
    "SYNC_DATA_ALLOWED_CHATS", "7367477801,30373681"
)

# Chat IDs allowed to run /syncguild (falls back to SYNC_DATA_ALLOWED_CHATS)
_raw_sync_guild = _optional("SYNC_GUILD_ALLOWED_CHATS")
SYNC_GUILD_ALLOWED_CHATS: set[str] = (
    {s.strip() for s in _raw_sync_guild.split(",") if s.strip()}
    if _raw_sync_guild
    else SYNC_DATA_ALLOWED_CHATS
)

# ---------------------------------------------------------------------------
# Timezone
# ---------------------------------------------------------------------------

_tz_name: str = _optional("TIMEZONE", "Europe/Madrid")
try:
    TZ = ZoneInfo(_tz_name)
except ZoneInfoNotFoundError:
    raise SystemExit(
        f"TIMEZONE value {_tz_name!r} is not a valid IANA timezone name. "
        "Example: 'Europe/Madrid', 'Europe/Amsterdam', 'UTC'."
    )
TIMEZONE: str = _tz_name  # raw string, for libs that need it (e.g. pytz, APScheduler)

# ---------------------------------------------------------------------------
# Scheduled jobs
# ---------------------------------------------------------------------------

# Daily send-assignments: time in HH:MM format (24h, in TIMEZONE)
SEND_ASSIGNMENTS_TIME: str = _optional("SEND_ASSIGNMENTS_TIME", "19:10")

# Weekly sync-guilds: full cron expression (runs in TIMEZONE)
SYNC_GUILDS_CRON: str = _optional("SYNC_GUILDS_CRON", "0 3 * * 0")  # Sunday 03:00

# Monthly sync-data: full cron expression (runs in TIMEZONE)
SYNC_DATA_CRON: str = _optional("SYNC_DATA_CRON", "0 2 1 * *")  # 1st of month 02:00

# ---------------------------------------------------------------------------
# Data filters
# ---------------------------------------------------------------------------

# Substrings that, if present in a unit/skill baseId, cause it to be excluded.
EXCLUDE_BASEID_CONTAINS: list[str] = _str_list("EXCLUDE_BASEID_CONTAINS", "")

# Localization locale for comlink data fetches
LOCALE: str = _optional("LOCALE", "ENG_US")

# ---------------------------------------------------------------------------
# HTTP / retry behaviour
# ---------------------------------------------------------------------------

HTTP_RETRIES: int   = _int(  "HTTP_RETRIES",          5)
HTTP_BACKOFF:  float = _float("HTTP_BACKOFF_SECONDS",  1.0)
HTTP_TIMEOUT:  float = _float("HTTP_TIMEOUT_SECONDS",  30.0)

# ---------------------------------------------------------------------------
# Omicron mode map (optional JSON blob)
# ---------------------------------------------------------------------------

OMICRON_MODE_MAP_JSON: str = _optional("OMICRON_MODE_MAP_JSON")
OMICRON_MODE_MAP:      str = _optional("OMICRON_MODE_MAP")
