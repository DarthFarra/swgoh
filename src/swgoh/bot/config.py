# src/swgoh/bot/config.py
import os
from .. import config as core_cfg  # <- TU config core

# Re-export con fallback a env
BOT_TOKEN = getattr(core_cfg, "TELEGRAM_BOT_TOKEN", os.getenv("TELEGRAM_BOT_TOKEN"))
SPREADSHEET_ID = getattr(core_cfg, "SPREADSHEET_ID", os.getenv("SPREADSHEET_ID"))
SERVICE_ACCOUNT_ENV = getattr(core_cfg, "SERVICE_ACCOUNT_FILE", "SERVICE_ACCOUNT_FILE")

USERS_SHEET   = getattr(core_cfg, "USERS_SHEET",   os.getenv("USERS_SHEET", "Usuarios"))
GUILDS_SHEET  = getattr(core_cfg, "GUILDS_SHEET",  os.getenv("GUILDS_SHEET", "Guilds"))
PLAYERS_SHEET = getattr(core_cfg, "PLAYERS_SHEET", os.getenv("PLAYERS_SHEET", "Players"))
DEFAULT_ROTE_SHEET = getattr(core_cfg, "DEFAULT_ROTE_SHEET", os.getenv("DEFAULT_ROTE_SHEET", "Asignaciones ROTE"))

# Lista de chats permitidos para /syncdata
_raw = getattr(core_cfg, "SYNC_DATA_ALLOWED_CHATS", os.getenv("SYNC_DATA_ALLOWED_CHATS", "7367477801,30373681"))
SYNC_DATA_ALLOWED_CHATS = {s.strip() for s in (_raw.split(",") if isinstance(_raw, str) else list(_raw)) if s.strip()}

# Zona horaria: reusa TZ si existe, si no construye desde env
try:
    TZ = core_cfg.TZ
except Exception:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo(os.getenv("ID_ZONA", "Europe/Amsterdam"))

# Credenciales: reusa helper si existe; si no, define uno pequeño
def load_service_account_info() -> dict:
    if hasattr(core_cfg, "load_service_account_info"):
        return core_cfg.load_service_account_info()
    raw = os.getenv(SERVICE_ACCOUNT_ENV)
    if not raw:
        raise RuntimeError("Falta SERVICE_ACCOUNT_FILE")
    import json, base64
    try:
        return json.loads(raw)
    except Exception:
        try:
            return json.loads(base64.b64decode(raw).decode("utf-8"))
        except Exception:
            with open(raw, "r", encoding="utf-8") as f:
                return json.load(f)
