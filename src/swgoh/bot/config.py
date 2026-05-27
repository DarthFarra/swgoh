# src/swgoh/bot/config.py
"""
Bot-level config shim.

All values come from src/swgoh/config.py (the single source of truth).
This file exists only so bot modules can use a short import path:
    from .config import BOT_TOKEN
instead of reaching up into the core package.

Do NOT add os.getenv() calls here. Add new variables to src/swgoh/config.py
and alias them below.
"""
from __future__ import annotations

from .. import config as _core

# Telegram
BOT_TOKEN: str = _core.TELEGRAM_BOT_TOKEN

# Spreadsheet identity
SPREADSHEET_ID: str   = _core.SPREADSHEET_ID
SPREADSHEET_NAME: str = _core.SPREADSHEET_NAME

# Tab names
GUILDS_SHEET:           str = _core.SHEET_GUILDS
PLAYERS_SHEET:          str = _core.SHEET_PLAYERS
USERS_SHEET:            str = _core.SHEET_USERS
ASSIGNMENTS_SHEET:      str = _core.SHEET_ASSIGNMENTS
DEFAULT_ROTE_SHEET:     str = _core.DEFAULT_ROTE_SHEET
TICKET_SNAPSHOTS_SHEET: str = _core.SHEET_TICKET_SNAPSHOTS

# Auth
SYNC_DATA_ALLOWED_CHATS:  set[str] = _core.SYNC_DATA_ALLOWED_CHATS
SYNC_GUILD_ALLOWED_CHATS: set[str] = _core.SYNC_GUILD_ALLOWED_CHATS

# Timezone
TZ = _core.TZ
TIMEZONE: str = _core.TIMEZONE

# Scheduled job configuration
SEND_ASSIGNMENTS_TIME: str = _core.SEND_ASSIGNMENTS_TIME
SYNC_GUILDS_CRON: str      = _core.SYNC_GUILDS_CRON
SYNC_DATA_CRON: str        = _core.SYNC_DATA_CRON
TICKET_REMINDER_LEAD_MINUTES: int = _core.TICKET_REMINDER_LEAD_MINUTES  # NEW

# Additional tab names used by /omicrones
CHAR_OMICRONS_SHEET:      str = _core.SHEET_CHARACTERS_OMICRONS
CHAR_ZETAS_SHEET:         str = _core.SHEET_CHARACTERS_ZETAS
OMICRON_PRIORITIES_SHEET: str = _core.SHEET_OMICRON_PRIORITIES
 
# Unit catalogs — needed to translate Comlink base_ids to character names.
CHARACTERS_SHEET:         str = _core.SHEET_CHARACTERS
SHIPS_SHEET:              str = _core.SHEET_SHIPS
 
# /omicrones tunables
OMICRON_MIN_RELIC:        int = _core.OMICRON_MIN_RELIC
OMICRON_RECOMMEND_TOP_N:  int = _core.OMICRON_RECOMMEND_TOP_N

# Discord listener (TB export bridge)
DISCORD_BOT_TOKEN:        str = _core.DISCORD_BOT_TOKEN
DISCORD_C3PO_USER_ID:     int = _core.DISCORD_C3PO_USER_ID
DISCORD_WATCH_CHANNEL_ID: int = _core.DISCORD_WATCH_CHANNEL_ID
TB_AUTO_FORWARD_CHAT_ID:  int = _core.TB_AUTO_FORWARD_CHAT_ID
TB_AUTO_FORWARD_THREAD_ID: int = _core.TB_AUTO_FORWARD_THREAD_ID
TB_REPORTS_ALLOWED_CHATS: set[str] = _core.TB_REPORTS_ALLOWED_CHATS
