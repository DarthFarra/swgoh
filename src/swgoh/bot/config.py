# src/swgoh/bot/config.py
import os
from zoneinfo import ZoneInfo

# Reutiliza tu core config
from .. import config as core_cfg

# Token del bot (alias a tu variable actual)
BOT_TOKEN = getattr(core_cfg, "TELEGRAM_BOT_TOKEN", None) or os.getenv("TELEGRAM_BOT_TOKEN")

# Spreadsheet (usas ID en tu core)
SPREADSHEET_ID = getattr(core_cfg, "SPREADSHEET_ID", None) or os.getenv("SPREADSHEET_ID")

# Nombres de pestañas (reutilizamos los tuyos)
USERS_SHEET   = getattr(core_cfg, "SHEET_PLAYERS", None)  # placeholder en caso de que no exista abajo
USERS_SHEET   = getattr(core_cfg, "SHEET_USERS", "Usuarios")  # si tienes SHEET_USERS en tu core, úsalo
GUILDS_SHEET  = getattr(core_cfg, "SHEET_GUILDS", "Guilds")
PLAYERS_SHEET = getattr(core_cfg, "SHEET_PLAYERS", "Players")

# Pestaña por defecto para ROTE (si en Guilds no hay valor en "ROTE")
DEFAULT_ROTE_SHEET = os.getenv("DEFAULT_ROTE_SHEET", "Asignaciones ROTE")

# Chats permitidos para /syncdata (puedes mantenerlo también en env)
_raw = os.getenv("SYNC_DATA_ALLOWED_CHATS", "7367477801,30373681")
SYNC_DATA_ALLOWED_CHATS = {s.strip() for s in _raw.split(",") if s.strip()}

# Zona horaria (reutiliza tu TIMEZONE del core)
TZ = ZoneInfo(getattr(core_cfg, "TIMEZONE", os.getenv("ID_ZONA", "Europe/Amsterdam")))
