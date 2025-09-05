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
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

from ..comlink import fetch_guild
try:
    from ..comlink import fetch_player_by_id
except Exception:
    from ..comlink import fetch_player as fetch_player_by_id  # type: ignore

from ..http import COMLINK_BASE  # valida formato al importar

logging.basicConfig(
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("sync_guilds")

# ----------------- ENV / CONFIG -----------------
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_ENV = "SERVICE_ACCOUNT_FILE"

SHEET_GUILDS = os.getenv("GUILDS_SHEET", "Guilds")
SHEET_PLAYERS = os.getenv("PLAYERS_SHEET", "Players")
SHEET_PLAYER_UNITS = os.getenv("PLAYER_UNITS_SHEET", "Player_Units")
SHEET_PLAYER_SKILLS = os.getenv("PLAYER_SKILLS_SHEET", "Player_Skills")

SHEET_CHARACTERS = os.getenv("CHARACTERS_SHEET", "Characters")
SHEET_SHIPS = os.getenv("SHIPS_SHEET", "Ships")
SHEET_ZETAS = os.getenv("CHAR_ZETAS_SHEET", "CharactersZetas")
SHEET_OMIS  = os.getenv("CHAR_OMICRONS_SHEET", "CharactersOmicrons")

EXCLUDE_BASEID_CONTAINS = [s.strip().upper() for s in os.getenv("EXCLUDE_BASEID_CONTAINS", "").split(",") if s.strip()]

TZ = ZoneInfo(os.getenv("ID_ZONA", "Europe/Amsterdam"))
FILTER_GUILD_IDS = {s.strip() for s in os.getenv("FILTER_GUILD_IDS", "").split(",") if s.strip()}

DIV_MAP  = {25: "1", 20: "2", 15: "3", 10: "4", 5: "5"}
RELIC_MAP = {11:"R9",10:"R8",9:"R7",8:"R6",7:"R5",6:"R4",5:"R3",4:"R2",3:"R1",2:"R0",1:"G12",0:"<G12"}
ROLE_MAP = {2:"Miembro",3:"Oficial",4:"Lider"}

GUILDS_HEADER_SYNONYMS = {"GP": ["GP", "Guild GP"]}
GUILDS_REQUIRED = ["Guild Id","Guild Name","Members","Guild GP","Last Raid Id","Last Raid Score","Last Update"]
PLAYERS_REQUIRED = ["Player Id","Player Name","Ally code","Guild Name","Role","Level","GP","GAC League"]

# Cambio aquí: Player_Units empieza por Guild Name + Player Name
PLAYER_UNITS_MIN_PREFIX = ["Guild Name","Player Name"]

# ----------------- HELPERS -----------------
def now_ts() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")

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
            info = try_json(base64.b64decode(raw).decode("utf-8"))
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

def _headers(ws) -> List[str]:
    vals = ws.row_values(1) or []
    return [h.strip() for h in vals]

def ws_update(ws, range_name: str, values: List[List[str]]):
    return ws.update(values=values, range_name=range_name)

def _get_all(ws) -> Tuple[List[str], List[List[str]]]:
    vals = ws.get_all_values() or []
    if not vals:
        return [], []
    headers = [h.strip() for h in vals[0]]
    rows = vals[1:] if len(vals) > 1 else []
    return headers, rows

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

    try:
        addrs = {ai[4][0] for ai in socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)}
        log.info("DNS %s -> %s", host, ", ".join(sorted(addrs)))
    except Exception as e:
        log.error("No se puede resolver %s: %s", host, e)
        return False

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

def _safe_get(d: Any, path: List[Any], default=None):
    cur = d
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur

def _to_int(x: Any, default: int = 0) -> int:
    try: return int(x)
    except Exception: return default

def _to_compact_json(obj: Any) -> str:
    try: return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    except Exception: return ""

def _parse_last_raid(guild_data: Dict[str, Any]) -> Tuple[str, int]:
    arr = _safe_get(guild_data, ["lastRaidPointsSummary"], None)
    if arr is None:
        arr = _safe_get(guild_data, ["guild","lastRaidPointsSummary"], [])
    if isinstance(arr, list) and arr:
        first = arr[0] or {}
        ident = first.get("identifier", {})
        pts = _to_int(first.get("totalPoints", 0), 0)
        return _to_compact_json(ident), pts
    return "", 0

def _parse_player_rating(p: Dict[str, Any]) -> str:
    league = _safe_get(p, ["playerRating","playerRankStatus","leagueId"], "")
    div_raw = _safe_get(p, ["playerRating","playerRankStatus","divisionId"], None)
    div = DIV_MAP.get(_to_int(div_raw, 0), "")
    return f"{league} {div}".strip()

def _parse_allycode(p: Dict[str, Any]) -> str:
    v = p.get("allycode") or p.get("allyCode") or _safe_get(p, ["player","allyCode"], None)
    s = str(v or "").strip()
    return "".join(ch for ch in s if ch.isdigit())

def _exclude_baseid(base_id: str) -> bool:
    if not EXCLUDE_BASEID_CONTAINS: return False
    b = (base_id or "").upper()
    return any(sub in b for sub in EXCLUDE_BASEID_CONTAINS)

def _exclude_skillid(skill_id: str) -> bool:
    if not EXCLUDE_BASEID_CONTAINS: return False
    s = (skill_id or "").upper()
    return any(sub in s for sub in EXCLUDE_BASEID_CONTAINS)

def map_member_level(val) -> str:
    try: c = int(val)
    except Exception:
        try: c = int(str(val).strip())
        except Exception: c = 0
    return ROLE_MAP.get(c, (str(c) if c else ""))

def write_table_body(ws, headers: List[str], rows: List[List[str]]):
    cols = len(headers) if headers else 1
    target_rows = max(len(rows) + 1, 1)
    ws.resize(target_rows, cols)
    if rows:
        ws_update(ws, "A2", rows)
    else:
        ws.resize(1, cols)

# ----------------- REBUILD ÍNDICES -----------------
def rebuild_pu_index_by_guild_name(rows: List[List[str]], idx_guild_1b: Optional[int], idx_pname_1b: Optional[int]) -> Dict[str,int]:
    """
    Índice por clave compuesta (guild_name | player_name_lower) -> row_index (0-based)
    """
    out: Dict[str,int] = {}
    if not idx_guild_1b or not idx_pname_1b:
        return out
    cg = idx_guild_1b - 1
    cn = idx_pname_1b - 1
    for i, r in enumerate(rows):
        g = (r[cg] if cg < len(r) else "").strip()
        n = (r[cn] if cn < len(r) else "").strip().lower()
        if g and n:
            out[f"{g}|{n}"] = i
    return out

# ----------------- GUILDS / PLAYERS UPSERT -----------------
def _ensure_headers(ws, required: List[str], synonyms: Dict[str, List[str]] | None = None) -> Dict[str, int]:
    headers = _headers(ws)
    if not headers:
        ws_update(ws, "A1", [required]); headers = required[:]
    else:
        existing_lower = [h.strip().lower() for h in headers]
        changed = False
        for req in required:
            req_l = req.lower()
            if req_l in existing_lower: continue
            has_syn = False
            if synonyms and req in synonyms:
                for alt in synonyms[req]:
                    if alt.strip().lower() in existing_lower:
                        has_syn = True; break
            if not has_syn:
                headers.append(req); existing_lower.append(req_l); changed = True
        if changed:
            ws_update(ws, "1:1", [headers])
    return {h.strip().lower(): i for i, h in enumerate(headers, start=1)}

def _resolve_col(colmap: Dict[str,int], name: str, synonyms: Dict[str, List[str]] | None = None) -> Optional[int]:
    key = name.strip().lower()
    if key in colmap: return colmap[key]
    if synonyms and name in synonyms:
        for alt in synonyms[name]:
            k = alt.strip().lower()
            if k in colmap: return colmap[k]
    return None

def upsert_guild_row(ws, colmap: Dict[str,int], row_idx_1b: int, prev_row: List[str], newvals: Dict[str, Any]):
    headers_now = _headers(ws)
    row = prev_row[:] if prev_row else [""] * len(headers_now)
    def should_set(val: Any) -> bool:
        if val is None: return False
        if isinstance(val, (int,float)): return True
        return str(val).strip() != ""
    def setv(colname: str, val: Any):
        if not should_set(val): return
        idx = _resolve_col(colmap, colname, GUILDS_HEADER_SYNONYMS)
        if idx: row[idx-1] = str(val)
    for key in ("Guild Name","Members","GP","Last Raid Id","Last Raid Score"):
        if key in newvals: setv(key, newvals[key])
    setv("Last Update", now_ts())
    ws_update(ws, f"{row_idx_1b}:{row_idx_1b}", [row])

# ----------------- UNIT CATALOG FOR Player_Units -----------------
def read_unit_catalog(ss) -> Tuple[List[str], Dict[str, str], Dict[str, bool]]:
    def _read(sheet_name: str) -> Tuple[List[str], List[List[str]]]:
        ws = ss.worksheet(sheet_name)
        vals = ws.get_all_values() or []
        headers = [h.strip() for h in (vals[0] if vals else [])]
        rows = vals[1:] if len(vals) > 1 else []
        return headers, rows

    base_to_name: Dict[str,str] = {}
    is_ship: Dict[str,bool] = {}

    # Characters
    try:
        h, rows = _read(SHEET_CHARACTERS)
        cm = {v.lower(): i for i, v in enumerate(h)}
        idx_base = cm.get("base_id")
        idx_name = cm.get("name")
        if idx_base is not None and idx_name is not None:
            for r in rows:
                base = (r[idx_base] if idx_base < len(r) else "").strip()
                if not base or _exclude_baseid(base): continue
                name = (r[idx_name] if idx_name < len(r) else "").strip()
                if name: base_to_name[base]=name; is_ship[base]=False
    except Exception as e:
        log.warning("No se pudo leer Characters: %s", e)

    # Ships
    try:
        h, rows = _read(SHEET_SHIPS)
        cm = {v.lower(): i for i, v in enumerate(h)}
        idx_base = cm.get("base_id")
        idx_name = cm.get("name")
        if idx_base is not None and idx_name is not None:
            for r in rows:
                base = (r[idx_base] if idx_base < len(r) else "").strip()
                if not base or _exclude_baseid(base): continue
                name = (r[idx_name] if idx_name < len(r) else "").strip()
                if name: base_to_name[base]=name; is_ship[base]=True
    except Exception as e:
        log.warning("No se pudo leer Ships: %s", e)

    unit_base_ids = sorted(base_to_name.keys(), key=lambda b: base_to_name[b].lower())
    return unit_base_ids, base_to_name, is_ship

def ensure_player_units_headers(ws, unit_base_ids: List[str], base_to_name: Dict[str,str]) -> Tuple[Dict[str,int], Dict[str,int], List[str]]:
    headers = _headers(ws)
    if not headers:
        headers = PLAYER_UNITS_MIN_PREFIX[:] + [base_to_name[b] for b in unit_base_ids]
        ws_update(ws, "A1", [headers])
    else:
        lower = [h.lower() for h in headers]
        changed = False
        for col in PLAYER_UNITS_MIN_PREFIX:
            if col.lower() not in lower:
                headers.append(col); lower.append(col.lower()); changed = True
        for b in unit_base_ids:
            fname = base_to_name[b]
            if fname.lower() not in lower:
                headers.append(fname); lower.append(fname.lower()); changed = True
        if changed:
            ws_update(ws, "1:1", [headers])
    colmap = {h.strip().lower(): i for i, h in enumerate(headers, start=1)}
    unit_col_by_friendly = {base_to_name[b].strip().lower(): colmap[base_to_name[b].strip().lower()] for b in unit_base_ids if base_to_name[b].strip().lower() in colmap}
    return colmap, unit_col_by_friendly, headers

def roster_to_unit_values(roster_units: List[Dict[str,Any]], is_ship_by_base: Dict[str,bool]) -> Dict[str,str]:
    out: Dict[str,str] = {}
    for ru in roster_units or []:
        defid = str(ru.get("definitionId") or "").strip()
        if not defid: continue
        base = defid.split(":")[0]
        if not base or _exclude_baseid(base): continue
        if is_ship_by_base.get(base, False):
            out[base] = "Nave"; continue
        relic = 0
        rel_obj = ru.get("relic") or {}
        if isinstance(rel_obj, dict):
            relic = _to_int(rel_obj.get("currentTier"), 0)
        out[base] = RELIC_MAP.get(relic, RELIC_MAP[0])
    return out

# ----------------- SKILL CATALOG (Zetas + Omicrons) -----------------
def read_skill_catalog(ss) -> Tuple[Dict[str,str], Dict[str,List[str]], List[str]]:
    skill_id_to_name: Dict[str,str] = {}

    def _ingest(sheet_name: str):
        try:
            ws = ss.worksheet(sheet_name)
        except Exception:
            return
        headers, rows = _get_all(ws)
        if not rows: return
        cm = {h.lower(): i for i, h in enumerate(headers)}
        i_sid  = cm.get("skillid")
        i_name = cm.get("skill name")
        if i_sid is None or i_name is None:
            return
        for r in rows:
            sid  = (r[i_sid]  if i_sid  < len(r) else "").strip()
            name = (r[i_name] if i_name < len(r) else "").strip()
            if not sid or not name: continue
            if _exclude_skillid(sid): continue
            skill_id_to_name.setdefault(sid, name)

    _ingest(SHEET_ZETAS)
    _ingest(SHEET_OMIS)

    skill_name_to_ids: Dict[str,List[str]] = {}
    for sid, nm in skill_id_to_name.items():
        skill_name_to_ids.setdefault(nm, []).append(sid)

    skill_names = sorted(skill_name_to_ids.keys(), key=lambda s: s.lower())
    return skill_id_to_name, skill_name_to_ids, skill_names

# ----------------- Player_Skills matriz helpers -----------------
def read_ps_matrix(ws):
    headers, rows = _get_all(ws)
    if not headers:
        return [], [], [], {}
    cmap = {h.lower(): i for i, h in enumerate(headers)}
    i_g = cmap.get("player guild")
    i_n = cmap.get("player name")
    if i_g is None or i_n is None:
        return ["Player Guild","Player Name"], [], [], {}
    skill_names = headers[2:]
    mat = {}
    for r in rows:
        g = (r[i_g] if i_g < len(r) else "").strip()
        n = (r[i_n] if i_n < len(r) else "").strip()
        if not g or not n:
            continue
        key = (g, n)
        d = {}
        for j, sname in enumerate(skill_names, start=2):
            v = r[j] if j < len(r) else ""
            if v:
                d[sname] = v
        mat[key] = d
    return headers, rows, skill_names, mat

def write_ps_matrix(ws, matrix_dict, skill_names):
    headers = ["Player Guild", "Player Name"] + list(skill_names)
    ws.resize(1, max(len(headers), 1))
    ws_update(ws, "1:1", [headers])
    keys_sorted = sorted(matrix_dict.keys(), key=lambda k: (k[0].lower(), k[1].lower()))
    data_rows = []
    for (g, n) in keys_sorted:
        row = [g, n] + ["" for _ in skill_names]
        vals = matrix_dict[(g, n)]
        for idx, sname in enumerate(skill_names, start=0):
            v = vals.get(sname)
            if v is not None:
                row[2 + idx] = v
        data_rows.append(row)
    if data_rows:
        ws.resize(len(data_rows) + 1, len(headers))
        ws_update(ws, "A2", data_rows)
        ws.resize(len(data_rows) + 1, len(headers))
    else:
        ws.resize(1, len(headers))

# ----------------- PROCESO DE GREMIO -----------------
def _parse_last_raid(guild_data: Dict[str, Any]) -> Tuple[str, int]:
    arr = _safe_get(guild_data, ["lastRaidPointsSummary"], None)
    if arr is None:
        arr = _safe_get(guild_data, ["guild","lastRaidPointsSummary"], [])
    if isinstance(arr, list) and arr:
        first = arr[0] or {}
        ident = first.get("identifier", {})
        pts = _to_int(first.get("totalPoints", 0), 0)
        return _to_compact_json(ident), pts
    return "", 0

def process_guild(ss, ws_guilds, ws_players, guild_id: str, guild_row_idx_1b: int, guild_row_vals: List[str]) -> Tuple[str, int, Dict[str, Dict[str, Any]]]:
    try:
        gdata = fetch_guild({"guildId": guild_id, "includeRecentGuildActivityInfo": True})
    except Exception as e:
        log.warning("Error en POST /guild: %s", e)
        raise

    guild_obj = gdata.get("guild") if isinstance(gdata.get("guild"), dict) else gdata

    guild_name = _safe_get(guild_obj, ["profile","name"], "") or guild_obj.get("name", "")
    if not guild_name and guild_row_vals:
        try:
            hdrs = _headers(ws_guilds)
            idx_name = hdrs.index("Guild Name")
            guild_name = guild_row_vals[idx_name] if idx_name < len(guild_row_vals) else guild_name
        except Exception:
            pass

    guild_gp = _safe_get(guild_obj, ["profile","guildGalacticPower"], None)
    if guild_gp is None:
        guild_gp = guild_obj.get("galacticPower", 0)

    members_arr = _safe_get(gdata, ["guild","member"], []) or []
    members_count = len(members_arr)

    last_raid_id, last_raid_points = _parse_last_raid(gdata)

    gheaders, _ = _get_all(ws_guilds)
    gcol = {h.lower(): i for i, h in enumerate(gheaders, start=1)}
    newvals = {"Guild Name": guild_name, "Members": members_count, "GP": guild_gp, "Last Raid Id": last_raid_id, "Last Raid Score": last_raid_points}
    upsert_guild_row(ws_guilds, gcol, guild_row_idx_1b, guild_row_vals, newvals)

    players_data: Dict[str, Dict[str, Any]] = {}
    for m in members_arr:
        pid = str(m.get("playerId") or "").strip()
        name_guess = str(m.get("playerName") or "").strip()
        role_text = map_member_level(m.get("memberLevel"))
        gp_member = _to_int(m.get("galacticPower"), 0)
        if not pid:
            log.warning("Miembro %r sin playerId; no se puede consultar /player", name_guess)
            continue

        p_resp: Dict[str, Any] = {}
        try:
            p_resp = fetch_player_by_id(pid)
        except Exception as e:
            log.warning("Error /player playerId=%s (%s): %s", pid, name_guess, e)
            p_resp = {}

        name = str(p_resp.get("name") or _safe_get(p_resp, ["player","name"], "") or name_guess).strip()
        ally = _parse_allycode(p_resp)
        level = str(_safe_get(p_resp, ["level"], "") or _safe_get(p_resp, ["player","level"], ""))
        gac = _parse_player_rating(p_resp)
        roster = p_resp.get("rosterUnit") or _safe_get(p_resp, ["player","rosterUnit"], []) or []

        players_data[pid] = {
            "playerId": pid,
            "name": name,
            "ally": ally,
            "level": level,
            "gp": gp_member,
            "role": role_text,
            "gac": gac,
            "roster": roster,
            "guild_name": guild_name,
        }

    return guild_name, members_count, players_data

# ----------------- MAIN -----------------
def run() -> str:
    if not preflight_comlink():
        log.error("Abortando: COMLINK_BASE no accesible desde este servicio.")
        return "error: comlink preflight"

    ss = _open_spreadsheet()
    ws_guilds = ss.worksheet(SHEET_GUILDS)
    ws_players = ss.worksheet(SHEET_PLAYERS)
    ws_pu = ss.worksheet(SHEET_PLAYER_UNITS)
    ws_ps = ss.worksheet(SHEET_PLAYER_SKILLS)

    _ensure_headers(ws_guilds, GUILDS_REQUIRED, GUILDS_HEADER_SYNONYMS)
    _ensure_headers(ws_players, PLAYERS_REQUIRED)

    # --- Catálogo de unidades (para Player_Units)
    unit_base_ids, base_to_name, is_ship = read_unit_catalog(ss)
    colmap_pu, unit_col_by_friendly, pu_headers = ensure_player_units_headers(ws_pu, unit_base_ids, base_to_name)
    idx_pu_guild = colmap_pu.get("guild name")
    idx_pu_pname = colmap_pu.get("player name")
    _, pu_existing_rows = _get_all(ws_pu)

    # índice por (guild_name|player_name_lower)
    current_by_guild_player = rebuild_pu_index_by_guild_name(pu_existing_rows, idx_pu_guild, idx_pu_pname)

    # --- Índices Players
    p_headers = _headers(ws_players)
    pcol = {h.lower(): i for i, h in enumerate(p_headers, start=1)}
    _, players_existing_rows = _get_all(ws_players)

    final_pu_rows = pu_existing_rows[:]
    final_players_rows = players_existing_rows[:]

    # --- Player_Skills matriz existente (para merge selectivo) ---
    ps_headers_exist, ps_rows_exist, ps_skill_names_exist, ps_matrix_exist = read_ps_matrix(ws_ps)
    processed_guild_names = set()

    # --- Catálogo de skills (zetas + omicrons)
    skill_id_to_name, skill_name_to_ids, _ = read_skill_catalog(ss)

    # --- Procesar Guilds ---
    g_headers, g_rows = _get_all(ws_guilds)
    if not g_rows:
        log.info("No hay filas en Guilds.")
        ws_update(ws_ps, "1:1", [["Player Guild","Player Name"]])
        ws_ps.resize(1, 2)
        return "ok: 0 guilds"

    try:
        idx_gid = g_headers.index("Guild Id")
    except ValueError:
        low = [h.lower() for h in g_headers]
        if "guild id" in low:
            idx_gid = low.index("guild id")
        else:
            raise RuntimeError("La hoja Guilds no contiene la columna 'Guild Id'")

    processed = 0

    # Matriz recalculada de esta ejecución
    skills_matrix: Dict[Tuple[str,str], Dict[str,str]] = {}

    log.info("Procesando %d gremio(s)…", len(g_rows))
    for i, row in enumerate(g_rows, start=2):
        gid = (row[idx_gid].strip() if idx_gid < len(row) else "")
        if not gid: continue
        if FILTER_GUILD_IDS and gid not in FILTER_GUILD_IDS:
            continue

        attempts = 4
        delay = 1.2
        last_exc: Optional[Exception] = None
        players_data: Dict[str, Dict[str, Any]] = {}
        for _try in range(attempts):
            try:
                _, _, players_data = process_guild(ss, ws_guilds, ws_players, gid, i, row)
                last_exc = None
                break
            except Exception as e:
                log.warning("Error en POST /guild: %s", e)
                last_exc = e
                time.sleep(delay); delay *= 1.6
        if last_exc:
            log.error("Error obteniendo guildId=%s: %s", gid, last_exc)
            continue
        if not players_data:
            continue

        guild_name = next(iter(players_data.values())).get("guild_name", "")
        processed_guild_names.add(guild_name)

        # ---- BORRADO SELECTIVO Player_Units del gremio (por Guild Name)
        if idx_pu_guild:
            final_pu_rows = [r for r in final_pu_rows
                             if not (idx_pu_guild - 1 < len(r) and (r[idx_pu_guild - 1] or "").strip() == guild_name)]

        # >>> RECONSTRUIR ÍNDICES TRAS LOS BORRADOS <<<
        current_by_guild_player = rebuild_pu_index_by_guild_name(final_pu_rows, idx_pu_guild, idx_pu_pname)

        # ---- Reinsertar Player_Units + matriz de skills
        for pid, pdata in players_data.items():
            pname = pdata.get("name","") or ""
            roster= pdata.get("roster",[]) or []

            # Player_Units
            key = f"{guild_name}|{pname.lower()}"
            idx_row_pu: Optional[int] = current_by_guild_player.get(key)
            if idx_row_pu is not None and 0 <= idx_row_pu < len(final_pu_rows):
                prev = final_pu_rows[idx_row_pu]
                merged = prev[:] + [""] * (len(pu_headers) - len(prev))
            else:
                merged = [""] * len(pu_headers)
                if idx_pu_guild: merged[idx_pu_guild-1] = guild_name
                if idx_pu_pname: merged[idx_pu_pname-1] = pname
                current_by_guild_player[key] = len(final_pu_rows)

            if idx_pu_guild: merged[idx_pu_guild-1] = guild_name
            if idx_pu_pname: merged[idx_pu_pname-1] = pname

            base_to_val = roster_to_unit_values(roster, is_ship)
            for base_id, val in base_to_val.items():
                fname = base_to_name.get(base_id)
                if not fname: continue
                col = unit_col_by_friendly.get(fname.strip().lower())
                if not col: continue
                merged[col-1] = val

            if idx_row_pu is not None and 0 <= idx_row_pu < len(final_pu_rows):
                final_pu_rows[idx_row_pu] = merged
            else:
                final_pu_rows.append(merged)

            # Player_Skills matriz por (guild, name)
            if skill_id_to_name:
                rowdict = skills_matrix.setdefault((guild_name, pname), {})
                for ru in roster:
                    skills = (ru.get("skill") or ru.get("skills") or ru.get("skillList") or [])
                    if not isinstance(skills, list): continue
                    for s in skills:
                        if not isinstance(s, dict): continue
                        sid = s.get("id") or s.get("skillId") or s.get("idRef")
                        if not sid: continue
                        sid = str(sid).strip()
                        if sid not in skill_id_to_name:  # limita a zetas/omnis
                            continue
                        if _exclude_skillid(sid):
                            continue
                        tier = s.get("tier")
                        if tier is None:
                            tier = s.get("currentTier", s.get("selectedTier", s.get("tierIndex", 0)))
                        try:
                            tier_int = int(tier)
                        except Exception:
                            tier_int = 0
                        sname = skill_id_to_name[sid]
                        prevv = rowdict.get(sname)
                        if prevv is None or tier_int > _to_int(prevv, 0):
                            rowdict[sname] = str(tier_int)

        processed += 1

    # ---- Volcado final a Sheets ----
    if final_pu_rows != pu_existing_rows:
        write_table_body(ws_pu, _headers(ws_pu), final_pu_rows)

    # ---- MERGE SELECTIVO de Player_Skills ----
    for g in processed_guild_names:
        for key in list(ps_matrix_exist.keys()):
            if key[0] == g:
                del ps_matrix_exist[key]

    for key, vals in skills_matrix.items():
        ps_matrix_exist[key] = vals

    skills_in_use = set()
    for vals in ps_matrix_exist.values():
        for sname, v in vals.items():
            if sname and (v is not None and str(v) != ""):
                skills_in_use.add(sname)

    skill_names_merged = sorted(skills_in_use, key=str.lower)
    write_ps_matrix(ws_ps, ps_matrix_exist, skill_names_merged)

    if processed == 0:
        log.info("No hay filas nuevas para escribir.")

    return f"ok: guilds={processed}, player_units_rows={len(final_pu_rows)}, skill_matrix_rows={len(ps_matrix_exist)}, skill_cols={len(skill_names_merged)}"

if __name__ == "__main__":
    print(run())
