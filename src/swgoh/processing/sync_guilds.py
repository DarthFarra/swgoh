# src/swgoh/processing/sync_guilds.py
from __future__ import annotations

import os
import json
import base64
import time
import socket
import logging
import urllib.request
import urllib.error
from urllib.parse import urlparse
from typing import Any, Dict, List, Tuple, Optional

import gspread
from google.oauth2.service_account import Credentials

from ..comlink import fetch_guild
# Compat: si aún no añadiste fetch_player_by_id en comlink.py, usamos fetch_player
try:
    from ..comlink import fetch_player_by_id
except Exception:  # pragma: no cover
    from ..comlink import fetch_player as fetch_player_by_id  # type: ignore

from ..http import COMLINK_BASE  # valida formato al importar

# ==========
# Logging
# ==========
logging.basicConfig(
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("sync_guilds")

# ==========
# Config (env)
# ==========
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_ENV = "SERVICE_ACCOUNT_FILE"  # JSON directo / base64 / ruta

SHEET_GUILDS = os.getenv("GUILDS_SHEET", "Guilds")
SHEET_PLAYERS = os.getenv("PLAYERS_SHEET", "Players")
SHEET_PLAYER_UNITS = os.getenv("PLAYER_UNITS_SHEET", "Player_Units")
SHEET_CHARACTERS = os.getenv("CHARACTERS_SHEET", "Characters")
SHEET_SHIPS = os.getenv("SHIPS_SHEET", "Ships")

# Unidades a excluir por substrings en baseId (coma-separado)
EXCLUDE_BASEID_CONTAINS = [s.strip().upper() for s in os.getenv("EXCLUDE_BASEID_CONTAINS", "").split(",") if s.strip()]

# ==========
# Google Sheets helpers
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
        try:
            decoded = base64.b64decode(raw).decode("utf-8")
            info = try_json(decoded)
        except Exception:
            info = None

    if info is None:
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

def _open_spreadsheet():
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID")
    gc = gspread.authorize(_load_service_account_creds())
    return gc.open_by_key(SPREADSHEET_ID)

def _headers(ws) -> List[str]:
    vals = ws.row_values(1) or []
    return [h.strip() for h in vals]

def ws_update(ws, range_name: str, values: List[List[str]]):
    """Wrapper que usa argumentos nombrados (compat gspread 6.x+)."""
    return ws.update(values=values, range_name=range_name)

def _ensure_headers(ws, required: List[str]) -> Dict[str, int]:
    """
    Asegura columnas; añade al final si faltan (no borra datos).
    Devuelve mapa header_lower -> idx 1-based.
    """
    headers = _headers(ws)
    if not headers:
        ws_update(ws, "A1", [required])
        headers = required[:]
    else:
        low = [h.strip().lower() for h in headers]
        changed = False
        for col in required:
            if col.lower() not in low:
                headers.append(col)
                changed = True
        if changed:
            ws_update(ws, "1:1", [headers])
    return {h.strip().lower(): i for i, h in enumerate(headers, start=1)}

def _get_all(ws) -> Tuple[List[str], List[List[str]]]:
    vals = ws.get_all_values() or []
    if not vals:
        return [], []
    headers = [h.strip() for h in vals[0]]
    rows = vals[1:] if len(vals) > 1 else []
    return headers, rows

# ==========
# Preflight COMLINK (diagnóstico rápido)
# ==========
def preflight_comlink() -> bool:
    base = os.getenv("COMLINK_BASE", "").strip()
    log.info("COMLINK_BASE=%r", base)
    try:
        u = urlparse(base)
        host = u.hostname or ""
        port = u.port or (443 if u.scheme == "https" else 80)
    except Exception as e:
        log.error("URL inválida en COMLINK_BASE: %s", e)
        return False

    # DNS
    try:
        addrs = {ai[4][0] for ai in socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)}
        log.info("DNS %s -> %s", host, ", ".join(sorted(addrs)))
    except Exception as e:
        log.error("No se puede resolver %s: %s", host, e)
        return False

    # POST /metadata mínimo
    try:
        req = urllib.request.Request(
            base.rstrip("/") + "/metadata",
            data=b'{"payload":{}}',
            headers={"content-type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            log.info("Preflight /metadata -> %s %s", r.status, r.reason)
            return True
    except Exception as e:
        log.error("Fallo en preflight /metadata: %s", e)
        return False

# ==========
# Utilidades de parsing y mapeos
# ==========
DIV_MAP = {25: "1", 20: "2", 15: "3", 10: "4", 5: "5"}

RELIC_MAP = {
    11: "R9",
    10: "R8",
    9:  "R7",
    8:  "R6",
    7:  "R5",
    6:  "R4",
    5:  "R3",
    4:  "R2",
    3:  "R1",
    2:  "R0",
    1:  "G12",
    0:  "<G12",
}

def _safe_get(d: Any, path: List[Any], default=None):
    cur = d
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur

def _to_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def _to_compact_json(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return ""

def _parse_last_raid(guild_data: Dict[str, Any]) -> Tuple[str, int]:
    """
    Devuelve (lastRaidId_str, totalPoints)
    Si no existe en la respuesta, devuelve ("", 0).
    """
    arr = _safe_get(guild_data, ["lastRaidPointsSummary"], default=None)
    if arr is None:
        arr = _safe_get(guild_data, ["guild", "lastRaidPointsSummary"], default=[])
    if isinstance(arr, list) and arr:
        first = arr[0] or {}
        ident = first.get("identifier", {})
        pts = _to_int(first.get("totalPoints", 0), 0)
        return _to_compact_json(ident), pts
    return "", 0

def _parse_player_rating(p: Dict[str, Any]) -> str:
    league = _safe_get(p, ["playerRating", "playerRankStatus", "leagueId"], "")
    div_raw = _safe_get(p, ["playerRating", "playerRankStatus", "divisionId"], None)
    div_num = _to_int(div_raw, 0)
    div = DIV_MAP.get(div_num, "")
    if league and div:
        return f"{league} {div}"
    if league:
        return league
    return ""

def _parse_allycode(p: Dict[str, Any]) -> str:
    v = p.get("allycode")
    if v is None:
        v = p.get("allyCode")
    if v is None:
        v = _safe_get(p, ["player", "allyCode"], None)
    s = str(v or "").strip()
    return "".join(ch for ch in s if ch.isdigit())

def _exclude_baseid(base_id: str) -> bool:
    if not EXCLUDE_BASEID_CONTAINS:
        return False
    b = (base_id or "").upper()
    return any(sub in b for sub in EXCLUDE_BASEID_CONTAINS)

# ==========
# Escritura segura en hojas
# ==========
GUILDS_REQUIRED = [
    "Guild Id",
    "Guild Name",
    "Members",
    "GP",
    "Last Raid Id",
    "Last Raid Score",
    # (Preservamos si existen: "ROTE", "nombre abreviado")
]

PLAYERS_REQUIRED = [
    "Player Id",
    "Player Name",
    "Ally code",
    "Guild Name",
    "Role",
    "Level",
    "GP",
    "GAC League",
]

# Player_Units: compat con Apps Script (B=Player Name, C+=unidades).
PLAYER_UNITS_MIN_PREFIX = ["Player Id", "Player Name"]

def upsert_guild_row(ws, colmap: Dict[str, int], row_idx_1b: int, prev_row: List[str], newvals: Dict[str, Any]):
    headers_now = _headers(ws)
    row = prev_row[:] if prev_row else [""] * len(headers_now)

    def should_set(val: Any) -> bool:
        if val is None:
            return False
        # permitir 0 (número), pero evitar strings vacíos
        if isinstance(val, (int, float)):
            return True
        return str(val).strip() != ""

    def setv(colname: str, val: Any):
        if not should_set(val):
            return
        idx = colmap.get(colname.lower())
        if idx:
            row[idx - 1] = str(val)

    for key in ("Guild Name", "Members", "GP", "Last Raid Id", "Last Raid Score"):
        if key in newvals:
            setv(key, newvals[key])

    ws_update(ws, f"{row_idx_1b}:{row_idx_1b}", [row])

def upsert_player_rows(ws, colmap: Dict[str, int], existing_rows: List[List[str]], rows_by_playerid: Dict[str, List[str]]):
    headers_now = _headers(ws)
    idx_pid = colmap.get("player id")
    current_index: Dict[str, int] = {}
    if idx_pid:
        for i, r in enumerate(existing_rows):
            pid = (r[idx_pid - 1] if idx_pid - 1 < len(r) else "").strip()
            if pid:
                current_index[pid] = i

    final_rows = existing_rows[:]
    for pid, newrow in rows_by_playerid.items():
        if pid in current_index:
            i = current_index[pid]
            prev = final_rows[i]
            merged = prev[:] + [""] * (len(headers_now) - len(prev))
            for j in range(min(len(merged), len(newrow))):
                if newrow[j] != "":
                    merged[j] = newrow[j]
            final_rows[i] = merged
        else:
            row = [""] * len(headers_now)
            for j in range(min(len(row), len(newrow))):
                row[j] = newrow[j]
            final_rows.append(row)

    if final_rows != existing_rows:
        ws_update(ws, f"2:{len(final_rows)+1}", final_rows)

# ==========
# Catálogo de unidades (Characters + Ships)
# ==========
def read_unit_catalog(ss) -> Tuple[List[str], Dict[str, str], Dict[str, bool]]:
    """
    Devuelve:
      unit_base_ids (ordenadas por friendly name asc),
      baseId_to_friendly,
      is_ship_by_baseId
    Lee de las hojas Characters y Ships. Aplica EXCLUDE_BASEID_CONTAINS.
    """
    def _read(sheet_name: str) -> Tuple[List[str], List[List[str]]]:
        ws = ss.worksheet(sheet_name)
        vals = ws.get_all_values() or []
        headers = [h.strip() for h in (vals[0] if vals else [])]
        rows = vals[1:] if len(vals) > 1 else []
        return headers, rows

    base_to_name: Dict[str, str] = {}
    is_ship: Dict[str, bool] = {}

    # Characters
    try:
        h, rows = _read(SHEET_CHARACTERS)
        idx_base = h.index("base_id") if "base_id" in h else [i for i, v in enumerate(h) if v.lower() == "base_id"][0]
        idx_name = h.index("Name") if "Name" in h else [i for i, v in enumerate(h) if v.lower() == "name"][0]
        for r in rows:
            base = (r[idx_base] if idx_base < len(r) else "").strip()
            if not base or _exclude_baseid(base):
                continue
            name = (r[idx_name] if idx_name < len(r) else "").strip()
            if name:
                base_to_name[base] = name
                is_ship[base] = False
    except Exception as e:
        log.warning("No se pudo leer Characters: %s", e)

    # Ships
    try:
        h, rows = _read(SHEET_SHIPS)
        idx_base = h.index("base_id") if "base_id" in h else [i for i, v in enumerate(h) if v.lower() == "base_id"][0]
        idx_name = h.index("Name") if "Name" in h else [i for i, v in enumerate(h) if v.lower() == "name"][0]
        for r in rows:
            base = (r[idx_base] if idx_base < len(r) else "").strip()
            if not base or _exclude_baseid(base):
                continue
            name = (r[idx_name] if idx_name < len(r) else "").strip()
            if name:
                base_to_name[base] = name
                is_ship[base] = True
    except Exception as e:
        log.warning("No se pudo leer Ships: %s", e)

    unit_base_ids = sorted(base_to_name.keys(), key=lambda b: base_to_name[b].lower())
    return unit_base_ids, base_to_name, is_ship

def ensure_player_units_headers(ws, unit_base_ids: List[str], base_to_name: Dict[str, str]) -> Tuple[Dict[str, int], Dict[str, int], List[str]]:
    """
    Asegura cabecera en Player_Units:
      - Mantiene lo existente
      - Garantiza que están "Player Id" y "Player Name" (si no estaban, las añade al final; NO reordena)
      - Añade columnas de cada unidad por friendly name (si falta, la añade al final)
    Devuelve:
      colmap_lower -> idx(1-based),
      unit_col_by_friendly -> idx(1-based),
      headers_actualizados
    """
    headers = _headers(ws)
    if not headers:
        headers = PLAYER_UNITS_MIN_PREFIX[:] + [base_to_name[b] for b in unit_base_ids]
        ws_update(ws, "A1", [headers])
    else:
        lower = [h.lower() for h in headers]
        changed = False
        for col in PLAYER_UNITS_MIN_PREFIX:
            if col.lower() not in lower:
                headers.append(col)
                lower.append(col.lower())
                changed = True
        for b in unit_base_ids:
            fname = base_to_name[b]
            if fname.lower() not in lower:
                headers.append(fname)
                lower.append(fname.lower())
                changed = True
        if changed:
            ws_update(ws, "1:1", [headers])

    colmap = {h.strip().lower(): i for i, h in enumerate(headers, start=1)}
    unit_col_by_friendly = {base_to_name[b].strip().lower(): colmap[base_to_name[b].strip().lower()] for b in unit_base_ids if base_to_name[b].strip().lower() in colmap}
    return colmap, unit_col_by_friendly, headers

# ==========
# Player_Units: conversión roster -> valores por columna
# ==========
def roster_to_unit_values(
    roster_units: List[Dict[str, Any]],
    is_ship_by_base: Dict[str, bool],
) -> Dict[str, str]:
    """
    Devuelve: baseId -> valor_celda ("R#" / "G12" / "<G12" / "Nave")
    Usa definitionId antes de ":" para obtener baseId.
    """
    out: Dict[str, str] = {}
    for ru in roster_units or []:
        defid = str(ru.get("definitionId") or "").strip()
        if not defid:
            continue
        base = defid.split(":")[0]
        if not base or _exclude_baseid(base):
            continue

        if is_ship_by_base.get(base, False):
            out[base] = "Nave"
            continue

        relic = 0
        rel_obj = ru.get("relic") or {}
        if isinstance(rel_obj, dict):
            relic = _to_int(rel_obj.get("currentTier"), 0)
        out[base] = RELIC_MAP.get(relic, RELIC_MAP[0])
    return out

# ==========
# Procesado de un gremio
# ==========
def process_guild(
    ss,
    ws_guilds,
    ws_players,
    guild_id: str,
    guild_row_idx_1b: int,
    guild_row_vals: List[str],
) -> Tuple[str, int, Dict[str, Dict[str, Any]]]:
    """
    Procesa un guild:
      - actualiza fila en Guilds
      - devuelve (guild_name, num_miembros_procesados, players_data_by_playerId)
    """
    # 1) /guild  (fetch_guild ya envía {payload:{guildId, includeRecentGuildActivityInfo}, enums:false})
    try:
        gdata = fetch_guild({"guildId": guild_id})
    except Exception as e:
        log.warning("Error en POST /guild: %s", e)
        raise

    guild_obj = gdata.get("guild") if isinstance(gdata.get("guild"), dict) else gdata

    # Nombre de guild: si no viene, conservamos el que ya haya en la fila
    guild_name = _safe_get(guild_obj, ["profile", "name"], "") or guild_obj.get("name", "")
    if not guild_name and guild_row_vals:
        # Mantener el existente en hoja si no viene en respuesta
        try:
            hdrs = _headers(ws_guilds)
            idx_name = hdrs.index("Guild Name")
            guild_name = guild_row_vals[idx_name] if idx_name < len(guild_row_vals) else guild_name
        except Exception:
            pass

    # GP del guild (tolerante)
    guild_gp = _safe_get(guild_obj, ["profile", "guildGalacticPower"], None)
    if guild_gp is None:
        guild_gp = guild_obj.get("galacticPower", 0)

    # *** Miembros según tu esquema: gdata["guild"]["member"] ***
    members_arr = _safe_get(gdata, ["guild", "member"], []) or []
    members_count = len(members_arr)

    last_raid_id, last_raid_points = _parse_last_raid(gdata)

    # 2) Actualizar Guilds (preservando ROTE y nombre abreviado)
    gheaders, _ = _get_all(ws_guilds)
    gcol = {h.lower(): i for i, h in enumerate(gheaders, start=1)}
    newvals = {
        "Guild Name": guild_name,
        "Members": members_count,
        "GP": guild_gp,
        "Last Raid Id": last_raid_id,
        "Last Raid Score": last_raid_points,
    }
    upsert_guild_row(ws_guilds, gcol, guild_row_idx_1b, guild_row_vals, newvals)

    # 3) Obtener detalle por jugador (/player UNA vez por miembro, siempre por playerId)
    players_data: Dict[str, Dict[str, Any]] = {}
    for m in members_arr:
        pid = str(m.get("playerId") or "").strip()
        name_guess = str(m.get("playerName") or "").strip()
        # en tu esquema es "memberLevel"
        role = str(m.get("memberLevel") or "").strip()

        if not pid:
            log.warning("Miembro %r sin playerId; no se puede consultar /player", name_guess)
            continue

        p_resp: Dict[str, Any] = {}
        try:
            p_resp = fetch_player_by_id(pid)
        except Exception as e:
            log.warning("Error /player playerId=%s (%s): %s", pid, name_guess, e)
            p_resp = {}

        name = str(p_resp.get("name") or _safe_get(p_resp, ["player", "name"], "") or name_guess).strip()
        ally = _parse_allycode(p_resp)  # lo sacamos de /player si viene
        level = str(_safe_get(p_resp, ["level"], "") or _safe_get(p_resp, ["player", "level"], ""))
        gp = _safe_get(p_resp, ["galacticPower"], "") or _safe_get(p_resp, ["player", "galacticPower"], "")
        gac = _parse_player_rating(p_resp)
        roster = p_resp.get("rosterUnit") or _safe_get(p_resp, ["player", "rosterUnit"], []) or []

        players_data[pid] = {
            "playerId": pid,
            "name": name,
            "ally": ally,
            "level": level,
            "gp": gp,
            "role": role,
            "gac": gac,
            "roster": roster,
            "guild_name": guild_name,
        }

    return guild_name, members_count, players_data

# ==========
# Main
# ==========
def run() -> str:
    if not preflight_comlink():
        log.error("Abortando: COMLINK_BASE no accesible desde este servicio.")
        return "error: comlink preflight"

    ss = _open_spreadsheet()
    ws_guilds = ss.worksheet(SHEET_GUILDS)
    ws_players = ss.worksheet(SHEET_PLAYERS)
    ws_pu = ss.worksheet(SHEET_PLAYER_UNITS)

    # Asegurar cabeceras base en Guilds y Players
    _ensure_headers(ws_guilds, GUILDS_REQUIRED)
    _ensure_headers(ws_players, PLAYERS_REQUIRED)

    # Leer Guilds
    g_headers, g_rows = _get_all(ws_guilds)
    if not g_rows:
        log.info("No hay filas en Guilds.")
        return "ok: 0 guilds"

    # Índice de columna Guild Id
    try:
        idx_gid = g_headers.index("Guild Id")
    except ValueError:
        low = [h.lower() for h in g_headers]
        if "guild id" in low:
            idx_gid = low.index("guild id")
        else:
            raise RuntimeError("La hoja Guilds no contiene la columna 'Guild Id'")

    # ---- Preparar Player_Units: catálogo de unidades y cabecera ----
    unit_base_ids, base_to_name, is_ship = read_unit_catalog(ss)
    colmap_pu, unit_col_by_friendly, pu_headers = ensure_player_units_headers(ws_pu, unit_base_ids, base_to_name)

    # Índices útiles en Player_Units
    idx_pu_pid = colmap_pu.get("player id")        # puede no existir si la hoja era antigua
    idx_pu_pname = colmap_pu.get("player name")    # Apps Script espera B=Player Name

    # Leer todo Player_Units (para upsert masivo al final)
    _, pu_existing_rows = _get_all(ws_pu)

    # Índices para encontrar filas existentes
    current_by_pid: Dict[str, int] = {}
    current_by_pname: Dict[str, int] = {}
    if idx_pu_pid:
        for i, r in enumerate(pu_existing_rows):
            pid = (r[idx_pu_pid - 1] if idx_pu_pid - 1 < len(r) else "").strip()
            if pid:
                current_by_pid[pid] = i
    if idx_pu_pname:
        for i, r in enumerate(pu_existing_rows):
            pname = (r[idx_pu_pname - 1] if idx_pu_pname - 1 < len(r) else "").strip().lower()
            if pname:
                current_by_pname[pname] = i

    # Plantilla de fila Player_Units
    def new_pu_row() -> List[str]:
        return [""] * len(pu_headers)

    # También prepararemos upsert para Players
    p_headers = _headers(ws_players)
    pcol = {h.lower(): i for i, h in enumerate(p_headers, start=1)}
    _, players_existing_rows = _get_all(ws_players)
    players_index_by_pid: Dict[str, int] = {}
    idx_pid_players = pcol.get("player id")
    if idx_pid_players:
        for i, r in enumerate(players_existing_rows):
            pid = (r[idx_pid_players - 1] if idx_pid_players - 1 < len(r) else "").strip()
            if pid:
                players_index_by_pid[pid] = i

    # Matrices finales (iremos actualizando in-place)
    final_pu_rows = pu_existing_rows[:]  # sin la cabecera
    final_players_rows = players_existing_rows[:]

    processed = 0
    players_upd = 0

    log.info("Procesando %d gremio(s)…", len(g_rows))
    for i, row in enumerate(g_rows, start=2):  # filas 2..N
        gid = (row[idx_gid].strip() if idx_gid < len(row) else "")
        if not gid:
            continue

        # Reintentos alrededor de /guild
        attempts = 4
        delay = 1.2
        last_exc: Optional[Exception] = None
        guild_name = ""
        players_data: Dict[str, Dict[str, Any]] = {}
        for a in range(1, attempts + 1):
            try:
                guild_name, _, players_data = process_guild(
                    ss, ws_guilds, ws_players, gid, i, row
                )
                last_exc = None
                break
            except Exception as e:
                log.warning("Error en POST /guild: %s", e)
                last_exc = e
                time.sleep(delay)
                delay *= 1.6
                continue
        if last_exc:
            log.error("Error obteniendo guildId=%s: %s", gid, last_exc)
            continue

        # Para cada jugador del gremio: actualizar Players y Player_Units
        for pid, pdata in players_data.items():
            pname = pdata.get("name", "") or ""
            ally = pdata.get("ally", "") or ""
            level = pdata.get("level", "") or ""
            gp = pdata.get("gp", "") or ""
            role = pdata.get("role", "") or ""
            gac = pdata.get("gac", "") or ""
            roster = pdata.get("roster", []) or []

            # ---- Players (upsert por Player Id si posible) ----
            if idx_pid_players and pid and pid in players_index_by_pid:
                idx_row = players_index_by_pid[pid]
                prev = final_players_rows[idx_row]
                merged = prev[:] + [""] * (len(p_headers) - len(prev))

                def setp(col: str, val: Any):
                    j = pcol.get(col.lower())
                    if j:
                        merged[j - 1] = "" if val is None else str(val)

                setp("Player Id", pid)
                setp("Player Name", pname)
                setp("Ally code", ally)
                setp("Guild Name", guild_name)
                setp("Role", role)
                setp("Level", level)
                setp("GP", gp)
                setp("GAC League", gac)
                final_players_rows[idx_row] = merged
                players_upd += 1
            else:
                rowp = [""] * len(p_headers)
                def setp(col: str, val: Any):
                    j = pcol.get(col.lower())
                    if j:
                        rowp[j - 1] = "" if val is None else str(val)
                setp("Player Id", pid)
                setp("Player Name", pname)
                setp("Ally code", ally)
                setp("Guild Name", guild_name)
                setp("Role", role)
                setp("Level", level)
                setp("GP", gp)
                setp("GAC League", gac)
                final_players_rows.append(rowp)
                if idx_pid_players and pid:
                    players_index_by_pid[pid] = len(final_players_rows) - 1
                players_upd += 1

            # ---- Player_Units ----
            base_to_val = roster_to_unit_values(roster, is_ship)

            # Encontrar índice de fila existente (por Player Id si existe, si no por Player Name)
            idx_row_pu: Optional[int] = None
            if idx_pu_pid and pid and pid in current_by_pid:
                idx_row_pu = current_by_pid[pid]
            elif idx_pu_pname and pname and pname.lower() in current_by_pname:
                idx_row_pu = current_by_pname[pname.lower()]

            # Asegurar longitud de fila
            if idx_row_pu is not None:
                prev = final_pu_rows[idx_row_pu]
                merged = prev[:] + [""] * (len(pu_headers) - len(prev))
            else:
                merged = [""] * len(pu_headers)
                # registrar nuevo índice
                if idx_pu_pid and pid:
                    current_by_pid[pid] = len(final_pu_rows)
                if idx_pu_pname and pname:
                    current_by_pname[pname.lower()] = len(final_pu_rows)

            # Setear prefijo ("Player Id", "Player Name") si existen
            if idx_pu_pid:
                merged[idx_pu_pid - 1] = pid
            if idx_pu_pname:
                merged[idx_pu_pname - 1] = pname

            # Rellenar unidades por friendly name -> columna
            for base_id, val in base_to_val.items():
                fname = base_to_name.get(base_id)
                if not fname:
                    continue
                col = unit_col_by_friendly.get(fname.strip().lower())
                if not col:
                    continue
                merged[col - 1] = val

            # Guardar en matriz final
            if idx_row_pu is not None:
                final_pu_rows[idx_row_pu] = merged
            else:
                final_pu_rows.append(merged)

        processed += 1

    # ---- Volcado final a Sheets ----
    if final_players_rows != players_existing_rows:
        ws_update(ws_players, f"2:{len(final_players_rows)+1}", final_players_rows)

    if final_pu_rows != pu_existing_rows:
        ws_update(ws_pu, f"2:{len(final_pu_rows)+1}", final_pu_rows)

    if processed == 0:
        log.info("No hay filas nuevas para escribir.")

    return f"ok: guilds={processed}, players_upserted~={players_upd}, player_units_rows={len(final_pu_rows)}"

if __name__ == "__main__":
    print(run())
