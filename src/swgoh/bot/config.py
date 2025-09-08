import os
import json
from zoneinfo import ZoneInfo

# Entorno
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_ENV = "SERVICE_ACCOUNT_FILE"

USERS_SHEET   = os.getenv("USERS_SHEET", "Usuarios")
GUILDS_SHEET  = os.getenv("GUILDS_SHEET", "Guilds")
PLAYERS_SHEET = os.getenv("PLAYERS_SHEET", "Players")

DEFAULT_ROTE_SHEET = os.getenv("DEFAULT_ROTE_SHEET", "Asignaciones ROTE")
SYNC_DATA_ALLOWED_CHATS = {
    s.strip() for s in os.getenv("SYNC_DATA_ALLOWED_CHATS", "7367477801,30373681").split(",") if s.strip()
}

TZ = ZoneInfo(os.getenv("ID_ZONA", "Europe/Amsterdam"))

def load_service_account_info() -> dict:
    """
    Lee credenciales desde:
      - JSON en SERVICE_ACCOUNT_FILE
      - Base64 de ese JSON
      - Ruta a fichero JSON
    """
    raw = os.getenv(SERVICE_ACCOUNT_ENV)
    if not raw:
        raise RuntimeError("Falta SERVICE_ACCOUNT_FILE")

    # 1) JSON directo
    try:
        return json.loads(raw)
    except Exception:
        pass

    # 2) Base64
    try:
        import base64
        return json.loads(base64.b64decode(raw).decode("utf-8"))
    except Exception:
        pass

    # 3) Ruta a fichero
    with open(raw, "r", encoding="utf-8") as f:
        return json.load(f)
