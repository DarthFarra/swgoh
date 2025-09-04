import os
import sys
import json
import asyncio
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Tuple

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ContextTypes
)

# ========= Config =========
SERVICE_ACCOUNT_ENV = "SERVICE_ACCOUNT_FILE"
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ID_ZONA = os.getenv("ID_ZONA", "Europe/Amsterdam")
TZ = ZoneInfo(ID_ZONA)

SHEET_GUILDS = os.getenv("GUILDS_SHEET", "Guilds")
SHEET_USERS  = os.getenv("USERS_SHEET",  "Usuarios")

# Control de acceso
SYNCDATA_ALLOWED_CHATS = {int(x) for x in os.getenv("SYNC_DATA_ALLOWED_CHATS", "7367477801,30373681").split(",") if x.strip().isdigit()}
SYNCGUILD_ALLOWED_CHATS = {int(x) for x in os.getenv("SYNC_GUILD_ALLOWED_CHATS", "7367477801,30373681").split(",") if x.strip().isdigit()}

# ========= Sheets helpers =========
def _load_service_account_creds():
    raw = os.getenv(SERVICE_ACCOUNT_ENV)
    if not raw:
        raise RuntimeError(f"Falta {SERVICE_ACCOUNT_ENV}")
    def try_json(s: str):
        try: return json.loads(s)
        except Exception: return None
    info = try_json(raw)
    if info is None:
        try:
            info = try_json(__import__("base64").b64decode(raw).decode("utf-8"))
        except Exception:
            info = None
    if info is None:
        with open(raw, "r", encoding="utf-8") as f:
            info = json.load(f)
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive.readonly"]
    return Credentials.from_service_account_info(info, scopes=scopes)

def _open_spreadsheet():
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID")
    gc = gspread.authorize(_load_service_account_creds())
    return gc.open_by_key(SPREADSHEET_ID)

def _get_all(ws):
    vals = ws.get_all_values() or []
    if not vals:
        return [], []
    headers = [h.strip() for h in vals[0]]
    rows = vals[1:] if len(vals) > 1 else []
    return headers, rows

def _is_same_day(iso_str: str) -> bool:
    if not iso_str:
        return False
    try:
        dt = datetime.fromisoformat(iso_str)
    except Exception:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    dt_local = dt.astimezone(TZ)
    now_local = datetime.now(TZ)
    return (dt_local.date() == now_local.date())

def _user_guilds(ss, user_id: int) -> List[str]:
    """Lee 'Usuarios' y devuelve la lista de Guild Name (columna 'guild_name') en los que está registrado el user_id."""
    try:
        ws = ss.worksheet(SHEET_USERS)
    except Exception:
        return []
    headers, rows = _get_all(ws)
    cm = {h.lower(): i for i, h in enumerate(headers)}
    i_uid = cm.get("user_id")
    i_gn = cm.get("guild_name")
    if i_uid is None or i_gn is None:
        return []
    out = []
    for r in rows:
        uid = (r[i_uid] if i_uid < len(r) else "").strip()
        gname = (r[i_gn] if i_gn < len(r) else "").strip()
        if uid and gname and str(user_id) == uid:
            out.append(gname)
    return sorted(set(out))

def _guild_map(ss):
    """Mapea Guild Name -> { short, id, last } desde la hoja Guilds (usa 'nombre abreviado')."""
    ws = ss.worksheet(SHEET_GUILDS)
    headers, rows = _get_all(ws)
    cm = {h.lower(): i for i, h in enumerate(headers)}
    i_name = cm.get("guild name")
    i_short = cm.get("nombre abreviado")
    i_id = cm.get("guild id")
    i_last = cm.get("last update")
    out = {}
    for r in rows:
        name = (r[i_name] if i_name is not None and i_name < len(r) else "").strip()
        if not name:
            continue
        short = (r[i_short] if i_short is not None and i_short < len(r) else "").strip() or name
        gid = (r[i_id] if i_id is not None and i_id < len(r) else "").strip()
        last = (r[i_last] if i_last is not None and i_last < len(r) else "").strip()
        out[name] = {"short": short, "id":
