# src/swgoh/bot/jobs/send_assignments_daily.py
import os
import json
import base64
import time
import random
import datetime
import unicodedata
import urllib.request
from typing import Any, Dict, List, Optional, Tuple, DefaultDict
from collections import defaultdict

import pytz
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

# ==========
# Config env
# ==========
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_ENV = "SERVICE_ACCOUNT_FILE"  # JSON directo / base64 / ruta
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ID_ZONA = os.getenv("ID_ZONA", "Europe/Madrid")  # zona horaria para fase
DEBUG_MODE = os.getenv("DEBUG_ASSIGNMENTS", "").strip().lower() in ("1", "true", "yes", "on")

SHEET_USUARIOS = os.getenv("USUARIOS_SHEET", "Usuarios")
SHEET_GUILDS   = os.getenv("GUILDS_SHEET", "Guilds")

# ==========
# Helpers GS
# ==========
def _load_service_account_creds():
    raw = os.getenv(SERVICE_ACCOUNT_ENV)
    if not raw:
        raise RuntimeError(f"Variable de entorno {SERVICE_ACCOUNT_ENV} no definida")

    def try_json(s: str) -> Optional[dict]:
        try:
            return json.loads(s)
        except Exception:
            return None

    info = try_json(raw)
    if info is None:
        # base64
        try:
            decoded = base64.b64decode(raw).decode("utf-8")
            info = try_json(decoded)
        except Exception:
            info = None

    if info is None:
        # ruta a archivo
        try:
            with open(raw, "r", encoding="utf-8") as f:
                info = json.load(f)
        except Exception as e:
            raise RuntimeError(
                f"No pude interpretar {SERVICE_ACCOUNT_ENV} como JSON/base64/ruta: {e}"
            )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)

def _open_sheet():
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID")
    client = gspread.authorize(_load_service_account_creds())
    return client.open_by_key(SPREADSHEET_ID)

# ===== Reintentos con backoff para lecturas gspread =====
def _with_backoff(fn, *args, **kwargs):
    """
    Ejecuta una función gspread con reintentos en 429/5xx.
    """
    max_attempts = kwargs.pop("_attempts", 6)
    base_sleep = kwargs.pop("_base_sleep", 0.6)
    for attempt in range(1, max_attempts + 1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            # Detecta 429 o 5xx
            status = getattr(getattr(e, "response", None), "status_code", None)
            msg = str(e)
            transient = (status in (429, 500, 502, 503, 504)) or ("429" in msg) or ("Rate Limit" in msg)
            if transient and attempt < max_attempts:
                sleep = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                if DEBUG_MODE:
                    print(f"[DEBUG] gspread retry {attempt}/{max_attempts} tras {status or '??'}; durmiendo {sleep:.2f}s")
                time.sleep(sleep)
                continue
            raise

def _read_all_values(ss, sheet_name: str) -> Tuple[List[str], List[List[str]]]:
    ws = _with_backoff(ss.worksheet, sheet_name)
    vals = _with_backoff(ws.get_all_values)
    headers = [h.strip() for h in (vals[0] if vals else [])]
    rows = vals[1:] if len(vals) > 1 else []
    return headers, rows

# ==========
# Normalización (acento-insensible)
# ==========
def _strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", s or "") if unicodedata.category(ch) != "Mn")

def _slug(s: str) -> str:
    return " ".join(_strip_accents(str(s or "")).lower().split())

def _norm_cell(s: str) -> str:
    return _slug(s)

def _sanitize_ally(s: str) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())

def _hmap(headers: List[str]) -> Dict[str, int]:
    # Mapea slug(header) -> índice (0-based)
    return {_slug(h): i for i, h in enumerate(headers)}

def _find_col(hm: Dict[str, int], aliases: List[str]) -> int:
    for a in aliases:
        idx = hm.get(_slug(a), -1)
        if idx != -1:
            return idx
    return -1

def _gv_by_idx(row: List[str], idx: int) -> str:
    return (row[idx].strip() if 0 <= idx < len(row) else "")

# Alias de cabeceras (ES/EN + variantes con acentos)
HEADERS_ASSIGN = {
    "fase":      ["fase", "phase"],
    "planeta":   ["planeta", "planet"],
    "operacion": ["operacion", "operación", "operation"],
    "personaje": ["personaje", "character", "unit"],
    "jugador":   ["jugador", "player"],
    "user_id":   ["user_id", "userid", "user id", "telegram_user_id"],
}

HEADERS_USUARIOS = {
    "guild_name": ["guild_name", "guild name", "gremio", "nombre de gremio"],
    "chat_id":    ["chat_id", "chat id"],
    "user_id":    ["user_id", "userid", "user id", "telegram_user_id"],
    "alias":      ["alias", "player name", "jugador"],
}

# =========================
# Cálculo de fase (original)
# =========================
def obtener_fase_actual() -> Optional[str]:
    """
    - Si es domingo -> None (no se envía)
    - Si la semana ISO es PAR -> fase = weekday+1 (L=1..S=6)
    - Si la semana es IMPAR -> None (no se envía)
    """
    tz = pytz.timezone(ID_ZONA)
    hoy = datetime.datetime.now(tz)
    if hoy.weekday() >= 6:  # domingo (0=lunes .. 6=domingo)
        if DEBUG_MODE:
            print("[DEBUG] Domingo: no se envía.")
        return None
    semana_par = (hoy.isocalendar()[1] % 2) == 0
    fase = str(hoy.weekday() + 1) if semana_par else None
    if DEBUG_MODE:
        print(f"[DEBUG] Fecha={hoy.isoformat()} semana_par={semana_par} -> fase={fase}")
    return fase

# =========================
# Índice de asignaciones por gremio (una sola lectura por gremio)
# =========================
class AssignIndex:
    def __init__(self, sheet_name: str, idxs: Dict[str, int], rows: List[List[str]], fase: str):
        self.sheet_name = sheet_name
        self.fase = str(fase)
        # índices de columnas
        self.idx_fase    = idxs["fase"]
        self.idx_planeta = idxs["planeta"]
        self.idx_oper    = idxs["operacion"]
        self.idx_pers    = idxs["personaje"]
        self.idx_userid  = idxs["user_id"]
        self.idx_jugador = idxs["jugador"]

        # Estructuras: una pasada
        self.by_uid: DefaultDict[str, List[Tuple[str, str, str]]] = defaultdict(list)
        self.by_alias_norm: DefaultDict[str, List[Tuple[str, str, str]]] = defaultdict(list)

        total_fase = 0
        for r in rows:
            f = _gv_by_idx(r, self.idx_fase)
            if f != self.fase:
                continue
            total_fase += 1

            planeta = _gv_by_idx(r, self.idx_planeta) or "Sin planeta"
            oper    = _gv_by_idx(r, self.idx_oper)    or "Sin operación"
            pers    = _gv_by_idx(r, self.idx_pers)    or "Sin personaje"
            uid     = _gv_by_idx(r, self.idx_userid)
            if uid:
                self.by_uid[uid].append((planeta, oper, pers))
            else:
                jug = _gv_by_idx(r, self.idx_jugador)
                if jug:
                    self.by_alias_norm[_norm_cell(jug)].append((planeta, oper, pers))

        if DEBUG_MODE:
            print(f"[DEBUG] Índice '{sheet_name}' fase={fase}: filas_fase={total_fase} "
                  f"uids={len(self.by_uid)} aliases={len(self.by_alias_norm)}")

    def build_message_for(self, guild_name: str, user_id: str, alias: str) -> Optional[str]:
        items = self.by_uid.get(user_id)
        if not items and alias:
            items = self.by_alias_norm.get(_norm_cell(alias))
        if not items:
            return None

        por_planeta: DefaultDict[str, List[str]] = defaultdict(list)
        for (planeta, oper, pers) in items:
            por_planeta[planeta].append(f"- {pers} ({oper})")

        lines = [f"Asignaciones de *{alias or 'tu usuario'}* — *{guild_name}* (Fase {self.fase})", ""]
        for planeta, asigns in por_planeta.items():
            lines.append(f" {planeta}:")
            lines.extend(asigns)
            lines.append("")
        return "\n".join(lines)

# =========================
# Telegram
# =========================
def _tg_send_message(token: str, chat_id: str | int, text: str, parse_mode: str = "Markdown") -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({
        "chat_id": int(chat_id),
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"content-type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=20) as resp:
        resp.read()  # ignoramos cuerpo

# =========================
# Main
# =========================
def main() -> int:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN")
    ss = _open_sheet()

    fase = obtener_fase_actual()
    if not fase:
        print("[send_assignments_daily] Hoy no se envía (fuera de ventana de fases).")
        return 0
    if DEBUG_MODE:
        print(f"[DEBUG] Fase actual: {fase}")

    # --- Leer USUARIOS una vez ---
    u_headers, u_rows = _read_all_values(ss, SHEET_USUARIOS)
    if not u_rows:
        print("[send_assignments_daily] No hay usuarios registrados.")
        return 0
    uhm = _hmap(u_headers)
    idx_gname = _find_col(uhm, HEADERS_USUARIOS["guild_name"])
    idx_chat  = _find_col(uhm, HEADERS_USUARIOS["chat_id"])
    idx_uid   = _find_col(uhm, HEADERS_USUARIOS["user_id"])
    idx_alias = _find_col(uhm, HEADERS_USUARIOS["alias"])
    if DEBUG_MODE:
        print(f"[DEBUG] Usuarios headers idx: guild_name={idx_gname} chat_id={idx_chat} user_id={idx_uid} alias={idx_alias}")

    users: List[Tuple[str, str, str, str]] = []
    for r in u_rows:
        g  = _gv_by_idx(r, idx_gname)
        ch = _gv_by_idx(r, idx_chat)
        ui = _gv_by_idx(r, idx_uid)
        al = _gv_by_idx(r, idx_alias)
        if g and ch and ui:
            users.append((g, ch, ui, al))
    if DEBUG_MODE:
        print(f"[DEBUG] Usuarios válidos: {len(users)}")

    # --- Leer GUILDS una vez y mapear Guild Name -> ROTE ---
    g_headers, g_rows = _read_all_values(ss, SHEET_GUILDS)
    ghm = _hmap(g_headers)
    idx_guild_name = _find_col(ghm, ["Guild Name", "guild_name", "gremio"])
    idx_rote       = _find_col(ghm, ["ROTE"])

    guild_to_rote: Dict[str, str] = {}
    for r in g_rows:
        gname = _gv_by_idx(r, idx_guild_name)
        rote  = _gv_by_idx(r, idx_rote)
        if gname and rote:
            guild_to_rote[gname] = rote
    if DEBUG_MODE:
        print(f"[DEBUG] Guilds con ROTE configurado: {len(guild_to_rote)}")

    # --- Agrupar usuarios por gremio ---
    per_guild: DefaultDict[str, List[Tuple[str, str, str]]] = defaultdict(list)
    for (g, ch, ui, al) in users:
        per_guild[g].append((ch, ui, al))
    if DEBUG_MODE:
        print(f"[DEBUG] Gremios con usuarios: {len(per_guild)}")

    # --- Construir índice de asignaciones por gremio (una lectura por gremio) ---
    sent = 0
    skipped = 0

    for guild_name, lst in per_guild.items():
        sheet_name = guild_to_rote.get(guild_name) or "Asignaciones ROTE"

        # Leer la pestaña de asignaciones UNA vez
        try:
            a_headers, a_rows = _read_all_values(ss, sheet_name)
        except Exception as e:
            if DEBUG_MODE:
                print(f"[DEBUG] No puedo abrir hoja '{sheet_name}' para '{guild_name}': {e}")
            # No abortamos todo; omitimos usuarios de este gremio
            skipped += len(lst)
            continue

        if not a_rows:
            if DEBUG_MODE:
                print(f"[DEBUG] Hoja '{sheet_name}' vacía para '{guild_name}'")
            skipped += len(lst)
            continue

        ahm = _hmap(a_headers)
        # Resolver índices de columnas con alias
        idxs = {
            "fase":      _find_col(ahm, HEADERS_ASSIGN["fase"]),
            "planeta":   _find_col(ahm, HEADERS_ASSIGN["planeta"]),
            "operacion": _find_col(ahm, HEADERS_ASSIGN["operacion"]),
            "personaje": _find_col(ahm, HEADERS_ASSIGN["personaje"]),
            "user_id":   _find_col(ahm, HEADERS_ASSIGN["user_id"]),
            "jugador":   _find_col(ahm, HEADERS_ASSIGN["jugador"]),
        }
        if DEBUG_MODE:
            print(f"[DEBUG] '{sheet_name}' idxs={idxs}")

        # Si faltan columnas clave, omitimos a los usuarios de este gremio
        if min(idxs.values()) == -1:
            if DEBUG_MODE:
                print(f"[DEBUG] Faltan columnas requeridas en '{sheet_name}' para '{guild_name}'")
            skipped += len(lst)
            continue

        # Crear índice (una pasada)
        assign_index = AssignIndex(sheet_name, idxs, a_rows, fase)

        # Construir y enviar mensajes para los usuarios de este gremio
        for (chat_id, user_id, alias) in lst:
            try:
                msg = assign_index.build_message_for(guild_name, user_id, alias)
                if not msg:
                    skipped += 1
                    if DEBUG_MODE:
                        print(f"[DEBUG] (guild={guild_name}) user_id={user_id} alias='{alias}': sin asignaciones")
                    continue
                _tg_send_message(TELEGRAM_BOT_TOKEN, chat_id, msg, parse_mode="Markdown")
                sent += 1
                # Pequeño respiro para evitar picos (Telegram y logs)
                time.sleep(0.05)
            except Exception as e:
                print(f"[WARN] Fallo enviando a chat {chat_id}: {e}")
                skipped += 1

    print(f"[send_assignments_daily] Fase {fase}: enviados={sent}, omitidos={skipped}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
