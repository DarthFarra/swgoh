import os

# Comlink
COMLINK_BASE = os.getenv("COMLINK_BASE_URL") or os.getenv("COMLINK_BASE") or ""
COMLINK_HEADERS_JSON = os.getenv("COMLINK_HEADERS_JSON")

# Sheets
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")

# Nombres de pestañas
SHEET_GUILDS = os.getenv("SHEET_GUILDS", "Guilds")
SHEET_PLAYERS = os.getenv("SHEET_PLAYERS", "Players")
SHEET_PLAYER_UNITS = os.getenv("SHEET_PLAYER_UNITS", "Player_Units")
SHEET_PLAYER_SKILLS = os.getenv("SHEET_PLAYER_SKILLS", "Player_Skills")
SHEET_CHARACTERS = os.getenv("SHEET_CHARACTERS", "Characters")
SHEET_SHIPS = os.getenv("SHEET_SHIPS", "Ships")
SHEET_USERS = os.getenv("SHEET_USERS", "Usuarios")
SHEET_ASSIGNMENTS = os.getenv("SHEET_ASSIGNMENTS", "Asignaciones ROTE")

# Filtros / otros
EXCLUDE_BASEID_CONTAINS = [x.strip().upper() for x in os.getenv("EXCLUDE_BASEID_CONTAINS", "").split(",") if x.strip()]
TIMEZONE = os.getenv("TIMEZONE", "Europe/Madrid")

# HTTP
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "5"))
HTTP_BACKOFF = float(os.getenv("HTTP_BACKOFF_SECONDS", "1.0"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))
