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

from ..comlink import fetch_guild, fetch_player
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

def _ensure_headers(ws, required: List[str]) -> Dict[str, int]:
    """
    Asegura que existen las columnas; añade al final si faltan (no borra datos).
    Devuelve mapa header_lower -> idx 1-based.
    """
    headers = _headers(ws)
    if not headers:
        ws.update("A1", [required])
        headers = required[:]
    else:
        low = [h.strip().lower() for h in headers]
        changed = False
        for col in required:
            if col.lower() not in low:
                headers.append(col)
                changed = True
        if changed:
            ws.update("1:1", [headers])
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
# Utilidades de parsing
# ==========
DIV_MAP = {25: "1", 20: "2", 15: "3", 10: "4", 5: "5"}

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
    """
    "GAC League" a partir de:
      playerRating.playerRankStatus.leagueId , divisionId (map 25→1..5→5)
    """
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
        # a veces viene dentro de "player" u "account"
        v = _safe_get(p, ["player", "allyCode"], None)
    s = str(v or "").strip()
    # dejar solo dígitos
    return "".join(ch for ch in s if ch.isdigit())

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
    # columnas que NO tocamos si existen:
    # "ROTE",
    # "nombre abreviado",
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

def upsert_guild_row(ws, colmap: Dict[str, int], row_idx_1b: int, prev_row: List[str], newvals: Dict[str, Any]):
    """
    Reconstruye la fila manteniendo valores previos en columnas no tocadas.
    colmap: header_lower -> idx(1-based)
    """
    headers_now = _headers(ws)
    row = prev_row[:] if prev_row else [""] * len(headers_now)

    def setv(colname: str, val: Any):
        idx = colmap.get(colname.lower())
        if idx:
            # convertir a str plano
            row[idx - 1] = "" if val is None else str(val)

    # Solo actualizamos estos campos
    for key in ("Guild Name", "Members", "GP", "Last Raid Id", "Last Raid Score"):
        if key in newvals:
            setv(key, newvals[key])

    # Escribir fila entera (preserva ROTE y nombre abreviado)
    ws.update(f"{row_idx_1b}:{row_idx_1b}", [row])

def upsert_player_rows(ws, colmap: Dict[str, int], existing_rows: List[List[str]], rows_by_playerid: Dict[str, List[str]]):
    """
    Upsert de Players por 'Player Id'. Preserva filas previas si no hay info nueva.
    """
    headers_now = _headers(ws)
    idx_pid = colmap.get("player id")
    # construir índice actual: Player Id -> row_index (0-based sobre 'existing_rows')
    current_index: Dict[str, int] = {}
    if idx_pid:
        for i, r in enumerate(existing_rows):
            pid = (r[idx_pid - 1] if idx_pid - 1 < len(r) else "").strip()
            if pid:
                current_index[pid] = i

    # Preparar lista final
    final_rows = existing_rows[:]
    for pid, newrow in rows_by_playerid.items():
        if pid in current_index:
            i = current_index[pid]
            # fusionar conservando longitud
            prev = final_rows[i]
            merged = prev[:] + [""] * (len(headers_now) - len(prev))
            for key, val in [
                ("Player Id", newrow[colmap["player id"] - 1] if colmap.get("player id") else ""),
                ("Player Name", newrow[colmap["player name"] - 1] if colmap.get("player name") else ""),
                ("Ally code", newrow[colmap["ally code"] - 1] if colmap.get("ally code") else ""),
                ("Guild Name", newrow[colmap["guild name"] - 1] if colmap.get("guild name") else ""),
                ("Role", newrow[colmap["role"] - 1] if colmap.get("role") else ""),
                ("Level", newrow[colmap["level"] - 1] if colmap.get("level") else ""),
                ("GP", newrow[colmap["gp"] - 1] if colmap.get("gp") else ""),
                ("GAC League", newrow[colmap["gac league"] - 1] if colmap.get("gac league") else ""),
            ]:
                idx = colmap.get(key.lower())
                if idx:
                    merged[idx - 1] = str(val or "")
            final_rows[i] = merged
        else:
            # añadir nuevo
            row = [""] * len(headers_now)
            for key, val in [
                ("Player Id", newrow[colmap["player id"] - 1] if colmap.get("player id") else ""),
                ("Player Name", newrow[colmap["player name"] - 1] if colmap.get("player name") else ""),
                ("Ally code", newrow[colmap["ally code"] - 1] if colmap.get("ally code") else ""),
                ("Guild Name", newrow[colmap["guild name"] - 1] if colmap.get("guild name") else ""),
                ("Role", newrow[colmap["role"] - 1] if colmap.get("role") else ""),
                ("Level", newrow[colmap["level"] - 1] if colmap.get("level") else ""),
                ("GP", newrow[colmap["gp"] - 1] if colmap.get("gp") else ""),
                ("GAC League", newrow[colmap["gac league"] - 1] if colmap.get("gac league") else ""),
            ]:
                idx = colmap.get(key.lower())
                if idx:
                    row[idx - 1] = str(val or "")
            final_rows.append(row)

    # Volcar todo el bloque (sin cabecera)
    if final_rows != existing_rows:
        ws.update(f"2:{len(final_rows)+1}", final_rows)

# ==========
# Core
# ==========
def process_guild(ss, ws_guilds, ws_players, guild_id: str, guild_row_idx_1b: int, guild_row_vals: List[str], players_colmap: Dict[str, int]) -> Tuple[int, int]:
    """
    Procesa un guild:
      - actualiza fila en Guilds
      - devuelve (num_miembros_procesados, num_players_insertados_o_actualizados)
    """
    # 1) Llamada /guild (con includeRecentGuildActivityInfo)
    try:
        gdata = fetch_guild({"guildId": guild_id})
    except Exception as e:
        log.warning("Error en POST /guild: %s", e)
        raise

    # 2) Extraer info del guild
    # Nombre y GP en diferentes estructuras según build de Comlink
    guild_obj = gdata.get("guild") if isinstance(gdata.get("guild"), dict) else gdata
    guild_name = _safe_get(guild_obj, ["profile", "name"], "") or guild_obj.get("name", "")
    guild_gp = _safe_get(guild_obj, ["profile", "guildGalacticPower"], None)
    if guild_gp is None:
        guild_gp = guild_obj.get("galacticPower", 0)
    members_arr = gdata.get("member") or _safe_get(gdata, ["guild", "memberList"], []) or []
    members_count = len(members_arr)

    last_raid_id, last_raid_points = _parse_last_raid(gdata)

    # 3) Actualizar fila en Guilds (preservando ROTE y nombre abreviado)
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

    # 4) Construir filas para Players
    # Asegurar cabeceras en Players
    p_headers = _headers(ws_players)
    if not p_headers:
        ws_players.update("A1", [PLAYERS_REQUIRED])
        p_headers = PLAYERS_REQUIRED[:]
    # ampliar cabecera si faltan columnas
    need = [h for h in PLAYERS_REQUIRED if h.lower() not in (h2.lower() for h2 in p_headers)]
    if need:
        p_headers = p_headers + need
        ws_players.update("1:1", [p_headers])
    pcol = {h.lower(): i for i, h in enumerate(p_headers, start=1)}

    # leer contenido existente
    _, existing_rows = _get_all(ws_players)

    rows_by_playerid: Dict[str, List[str]] = {}
    # Plantilla fila con todas las columnas
    def new_player_row() -> List[str]:
        return [""] * len(p_headers)

    updated = 0

    for m in members_arr:
        # Identidad mínima
        player_id = str(m.get("playerId") or m.get("playerID") or "").strip()
        name = str(m.get("name") or m.get("playerName") or "").strip()
        role = str(m.get("guildMemberLevel") or m.get("role") or "").strip()
        level = str(m.get("level") or m.get("playerLevel") or "")
        gp = m.get("galacticPower") or m.get("gp") or ""

        # /player (preferencia por playerId; si no, intentamos allycode si existiera en m)
        p_resp: Dict[str, Any] = {}
        try:
            identifier: Dict[str, Any]
            if player_id:
                identifier = {"playerId": player_id}
            else:
                ally = m.get("allycode") or m.get("allyCode")
                if ally:
                    identifier = {"allycode": ally}
                else:
                    identifier = {}  # no podemos llamar
            if identifier:
                p_resp = fetch_player(identifier)
        except Exception as e:
            log.warning("Error /player (%s %s): %s", player_id, name, e)
            p_resp = {}

        # Datos finales para Players
        ally = _parse_allycode(p_resp)
        # algunos builds devuelven nombre más “oficial” en /player
        if not name:
            name = str(p_resp.get("name") or _safe_get(p_resp, ["player", "name"], "")).strip()
        # level/gp desde /player si faltaba
        if not level:
            level = str(_safe_get(p_resp, ["level"], "") or _safe_get(p_resp, ["player", "level"], ""))
        if not gp:
            gp = _safe_get(p_resp, ["galacticPower"], "") or _safe_get(p_resp, ["player", "galacticPower"], "")

        gac_league = _parse_player_rating(p_resp)

        # Construir fila destino
        row = new_player_row()
        def set_cell(col: str, val: Any):
            idx = pcol.get(col.lower())
            if idx:
                row[idx - 1] = "" if val is None else str(val)

        set_cell("Player Id", player_id)
        set_cell("Player Name", name)
        set_cell("Ally code", ally)
        set_cell("Guild Name", guild_name)
        set_cell("Role", role)
        set_cell("Level", level)
        set_cell("GP", gp)
        set_cell("GAC League", gac_league)

        if player_id:
            rows_by_playerid[player_id] = row
            updated += 1
        else:
            # si no hay playerId, no podemos indexar bien; añadimos usando un pseudo-id negativo temporal
            rows_by_playerid[f"noid:{name}:{ally}"] = row
            updated += 1

    # Upsert en Players
    upsert_player_rows(ws_players, pcol, existing_rows, rows_by_playerid)

    return members_count, updated

def run() -> str:
    if not preflight_comlink():
        log.error("Abortando: COMLINK_BASE no accesible desde este servicio.")
        return "error: comlink preflight"

    ss = _open_spreadsheet()
    ws_guilds = ss.worksheet(SHEET_GUILDS)
    ws_players = ss.worksheet(SHEET_PLAYERS)

    # Asegurar cabeceras base
    gcol = _ensure_headers(ws_guilds, GUILDS_REQUIRED)
    _ensure_headers(ws_players, PLAYERS_REQUIRED)  # players colmap se recalcula por guild

    # Leer Guilds para obtener IDs a procesar
    g_headers, g_rows = _get_all(ws_guilds)
    if not g_rows:
        log.info("No hay filas en Guilds.")
        return "ok: 0 guilds"

    # Buscar la columna 'Guild Id'
    try:
        idx_gid = g_headers.index("Guild Id")
    except ValueError:
        # tolerancia por mayúsculas/minúsculas
        low = [h.lower() for h in g_headers]
        if "guild id" in low:
            idx_gid = low.index("guild id")
        else:
            raise RuntimeError("La hoja Guilds no contiene la columna 'Guild Id'")

    # Procesar cada fila con Guild Id
    processed = 0
    players_updated_total = 0
    log.info("Procesando %d gremio(s)…", len(g_rows))
    for i, row in enumerate(g_rows, start=2):  # filas 2..N (1 es header)
        gid = (row[idx_gid].strip() if idx_gid < len(row) else "")
        if not gid:
            continue

        # Reintentos básicos alrededor de /guild por si hay cold-start
        attempts = 4
        delay = 1.2
        last_exc: Optional[Exception] = None
        for a in range(1, attempts + 1):
            try:
                m_count, p_upd = process_guild(
                    ss, ws_guilds, ws_players, gid, i, row, {}
                )
                processed += 1
                players_updated_total += p_upd
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

    if processed == 0:
        log.info("No hay filas nuevas para escribir.")
    return f"ok: guilds={processed}, players_upserted~={players_updated_total}"

if __name__ == "__main__":
    print(run())
