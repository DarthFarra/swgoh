# src/swgoh/processing/sync_guilds.py
from __future__ import annotations

import os
import json
import time
import socket
import logging
import urllib.request
import urllib.error
from urllib.parse import urlparse
from typing import Any, Dict, List, Tuple, Optional, Set

from datetime import datetime
from zoneinfo import ZoneInfo

import gspread

from ..comlink import fetch_guild
try:
    from ..comlink import fetch_player_by_id
except Exception:
    from ..comlink import fetch_player as fetch_player_by_id  # type: ignore

from ..http import COMLINK_BASE
from ..creds import load_credentials          # ← single source of truth
from ..sheets import spreadsheet as open_spreadsheet  # ← single source of truth
from .. import config as cfg
from . import _roster_parse as rp


logging.basicConfig(
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("sync_guilds")

# ----------------- CONFIG (sourced from core — no os.getenv here) -----------------
SHEET_GUILDS        = cfg.SHEET_GUILDS
SHEET_PLAYERS       = cfg.SHEET_PLAYERS
SHEET_PLAYER_UNITS  = cfg.SHEET_PLAYER_UNITS
SHEET_PLAYER_SKILLS = cfg.SHEET_PLAYER_SKILLS
SHEET_CHARACTERS    = cfg.SHEET_CHARACTERS
SHEET_SHIPS         = cfg.SHEET_SHIPS
EXCLUDE_BASEID_CONTAINS = cfg.EXCLUDE_BASEID_CONTAINS
TZ                  = cfg.TZ

# These two have no bot-facing use so are not in core config,
# but remain overridable via env.
import os as _os
SHEET_ZETAS = _os.getenv("CHAR_ZETAS_SHEET",    "CharactersZetas")
SHEET_OMIS  = _os.getenv("CHAR_OMICRONS_SHEET", "CharactersOmicrons")
del _os



def get_filter_ids_from_env() -> set[str]:
    return {s.strip() for s in os.getenv("FILTER_GUILD_IDS", "").split(",") if s.strip()}


DIV_MAP   = {25: "1", 20: "2", 15: "3", 10: "4", 5: "5"}
RELIC_MAP = rp.RELIC_DISPLAY  # alias — single source of truth

ROLE_MAP = {2: "Miembro", 3: "Oficial", 4: "Lider"}

GUILDS_HEADER_SYNONYMS = {"GP": ["GP", "Guild GP"]}
GUILDS_REQUIRED = [
    "Guild Id", "Guild Name", "Members", "Guild GP",
    "Last Raid Id", "Last Raid Score", "Last Update",
]
PLAYERS_REQUIRED = [
    "Player Id", "Player Name", "Ally code", "Guild Name",
    "Role", "Level", "GP", "GAC League",
]

PLAYER_UNITS_MIN_PREFIX = ["Guild Name", "Player Name"]

# ----------------- HELPERS -----------------

def now_ts() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")


def preflight_comlink() -> bool:
    base = COMLINK_BASE
    log.info("COMLINK_BASE host check starting.")
    try:
        u = urlparse(base)
        host = u.hostname or ""
        port = u.port or (443 if u.scheme == "https" else 80)
    except Exception as e:
        log.error("Invalid COMLINK_BASE URL: %s", e)
        return False

    try:
        addrs = {ai[4][0] for ai in socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)}
        log.info("DNS resolved %s -> %s", host, ", ".join(sorted(addrs)))
    except Exception as e:
        log.error("Cannot resolve host %s: %s", host, e)
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
        log.error("Preflight /metadata failed: %s", e)
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
    arr = _safe_get(guild_data, ["lastRaidPointsSummary"], None)
    if arr is None:
        arr = _safe_get(guild_data, ["guild", "lastRaidPointsSummary"], [])
    if isinstance(arr, list) and arr:
        first = arr[0] or {}
        ident = first.get("identifier", {})
        pts = _to_int(first.get("totalPoints", 0), 0)
        return _to_compact_json(ident), pts
    return "", 0


def _parse_player_rating(p: Dict[str, Any]) -> str:
    league = _safe_get(p, ["playerRating", "playerRankStatus", "leagueId"], "")
    div_raw = _safe_get(p, ["playerRating", "playerRankStatus", "divisionId"], None)
    div = DIV_MAP.get(_to_int(div_raw, 0), "")
    return f"{league} {div}".strip()


def _parse_allycode(p: Dict[str, Any]) -> str:
    v = (
        p.get("allycode")
        or p.get("allyCode")
        or _safe_get(p, ["player", "allyCode"], None)
    )
    return "".join(ch for ch in str(v or "") if ch.isdigit())


def _exclude_baseid(base_id: str) -> bool:
    if not EXCLUDE_BASEID_CONTAINS:
        return False
    b = (base_id or "").upper()
    return any(sub in b for sub in EXCLUDE_BASEID_CONTAINS)


def _exclude_skillid(skill_id: str) -> bool:
    if not EXCLUDE_BASEID_CONTAINS:
        return False
    s = (skill_id or "").upper()
    return any(sub in s for sub in EXCLUDE_BASEID_CONTAINS)


def map_member_level(val) -> str:
    try:
        c = int(val)
    except Exception:
        try:
            c = int(str(val).strip())
        except Exception:
            c = 0
    return ROLE_MAP.get(c, str(c) if c else "")


def write_table_body(ws, headers: List[str], rows: List[List[str]]):
    cols = len(headers) if headers else 1
    target_rows = max(len(rows) + 1, 1)
    ws.resize(target_rows, cols)
    if rows:
        ws.update(values=rows, range_name="A2")
    else:
        ws.resize(1, cols)


# ----------------- REBUILD ÍNDICES -----------------

def rebuild_players_index_by_pid(
    rows: List[List[str]], idx_pid_col_1b: Optional[int]
) -> Dict[str, int]:
    out: Dict[str, int] = {}
    if not idx_pid_col_1b:
        return out
    col = idx_pid_col_1b - 1
    for i, r in enumerate(rows):
        pid = (r[col] if col < len(r) else "").strip()
        if pid:
            out[pid] = i
    return out


def rebuild_pu_index_by_guild_name(
    rows: List[List[str]],
    idx_guild_1b: Optional[int],
    idx_pname_1b: Optional[int],
) -> Dict[str, int]:
    out: Dict[str, int] = {}
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


def _ensure_headers(
    ws,
    required: List[str],
    synonyms: Dict[str, List[str]] | None = None,
) -> Dict[str, int]:
    headers = _headers(ws)
    if not headers:
        ws_update(ws, "A1", [required])
        headers = required[:]
    else:
        existing_lower = [h.strip().lower() for h in headers]
        changed = False
        for req in required:
            req_l = req.lower()
            if req_l in existing_lower:
                continue
            has_syn = False
            if synonyms and req in synonyms:
                for alt in synonyms[req]:
                    if alt.strip().lower() in existing_lower:
                        has_syn = True
                        break
            if not has_syn:
                headers.append(req)
                existing_lower.append(req_l)
                changed = True
        if changed:
            ws_update(ws, "1:1", [headers])
    return {h.strip().lower(): i for i, h in enumerate(headers, start=1)}


def _resolve_col(
    colmap: Dict[str, int],
    name: str,
    synonyms: Dict[str, List[str]] | None = None,
) -> Optional[int]:
    key = name.strip().lower()
    if key in colmap:
        return colmap[key]
    if synonyms and name in synonyms:
        for alt in synonyms[name]:
            k = alt.strip().lower()
            if k in colmap:
                return colmap[k]
    return None


def upsert_guild_row(
    ws,
    colmap: Dict[str, int],
    row_idx_1b: int,
    prev_row: List[str],
    newvals: Dict[str, Any],
):
    headers_now = _headers(ws)
    row = prev_row[:] if prev_row else [""] * len(headers_now)

    def should_set(val: Any) -> bool:
        if val is None:
            return False
        if isinstance(val, (int, float)):
            return True
        return str(val).strip() != ""

    def setv(colname: str, val: Any):
        if not should_set(val):
            return
        idx = _resolve_col(colmap, colname, GUILDS_HEADER_SYNONYMS)
        if idx:
            row[idx - 1] = str(val)

    for key in ("Guild Name", "Members", "GP", "Last Raid Id", "Last Raid Score"):
        if key in newvals:
            setv(key, newvals[key])
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

    base_to_name: Dict[str, str] = {}
    is_ship: Dict[str, bool] = {}

    for sheet_name, ship_flag in ((SHEET_CHARACTERS, False), (SHEET_SHIPS, True)):
        try:
            h, rows = _read(sheet_name)
            cm = {v.lower(): i for i, v in enumerate(h)}
            idx_base = cm.get("base_id")
            idx_name = cm.get("name")
            if idx_base is not None and idx_name is not None:
                for r in rows:
                    base = (r[idx_base] if idx_base < len(r) else "").strip()
                    if not base or _exclude_baseid(base):
                        continue
                    name = (r[idx_name] if idx_name < len(r) else "").strip()
                    if name:
                        base_to_name[base] = name
                        is_ship[base] = ship_flag
        except Exception as e:
            log.warning("Could not read sheet %s: %s", sheet_name, e)

    unit_base_ids = sorted(base_to_name.keys(), key=lambda b: base_to_name[b].lower())
    return unit_base_ids, base_to_name, is_ship


def ensure_player_units_headers(
    ws,
    unit_base_ids: List[str],
    base_to_name: Dict[str, str],
) -> Tuple[Dict[str, int], Dict[str, int], List[str]]:
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
    unit_col_by_friendly = {
        base_to_name[b].strip().lower(): colmap[base_to_name[b].strip().lower()]
        for b in unit_base_ids
        if base_to_name[b].strip().lower() in colmap
    }
    return colmap, unit_col_by_friendly, headers


def roster_to_unit_values(
    roster_units: List[Dict[str, Any]],
    is_ship_by_base: Dict[str, bool],
) -> Dict[str, str]:
    raw = rp.extract_relic_tiers_by_base_id(roster_units, is_ship_by_base)
    return {
        base: ("Nave" if v is None else RELIC_MAP.get(v, RELIC_MAP[0]))
        for base, v in raw.items()
        if not _exclude_baseid(base)
    }


# ----------------- SKILL CATALOG (Zetas + Omicrons) -----------------

def read_skill_catalog(ss) -> Tuple[Dict[str, str], List[str]]:
    skill_id_to_header: Dict[str, str] = {}
    headers_set: set[str] = set()

    def _ingest(sheet_name: str):
        try:
            ws = ss.worksheet(sheet_name)
        except Exception:
            return
        headers, rows = _get_all(ws)
        if not rows:
            return
        cm = {h.lower(): i for i, h in enumerate(headers)}
        i_sid  = cm.get("skillid")
        i_pref = cm.get("charactername|skill name")
        i_sname = cm.get("skill name")
        if i_sid is None:
            return
        for r in rows:
            sid = (r[i_sid] if i_sid < len(r) else "").strip()
            if not sid or _exclude_skillid(sid):
                continue
            header = ""
            if i_pref is not None and i_pref < len(r):
                header = (r[i_pref] or "").strip()
            if not header and i_sname is not None and i_sname < len(r):
                header = (r[i_sname] or "").strip()
            if header:
                skill_id_to_header.setdefault(sid, header)
                headers_set.add(header)

    _ingest(SHEET_ZETAS)
    _ingest(SHEET_OMIS)

    headers_catalog = sorted(headers_set, key=lambda s: s.lower())
    return skill_id_to_header, headers_catalog


# ----------------- Player_Skills matrix helpers -----------------

def read_ps_matrix(ws):
    headers, rows = _get_all(ws)
    if not headers:
        return [], [], [], {}
    cmap = {h.lower(): i for i, h in enumerate(headers)}
    i_g = cmap.get("player guild")
    i_n = cmap.get("player name")
    if i_g is None or i_n is None:
        return ["Player Guild", "Player Name"], [], [], {}
    skill_headers = headers[2:]
    mat = {}
    for r in rows:
        g = (r[i_g] if i_g < len(r) else "").strip()
        n = (r[i_n] if i_n < len(r) else "").strip()
        if not g or not n:
            continue
        key = (g, n)
        d = {}
        for j, sh in enumerate(skill_headers, start=2):
            v = r[j] if j < len(r) else ""
            if v:
                d[sh] = v
        mat[key] = d
    return headers, rows, skill_headers, mat


def write_ps_matrix(ws, matrix_dict, skill_headers):
    headers = ["Player Guild", "Player Name"] + list(skill_headers)
    ws.resize(1, max(len(headers), 1))
    ws_update(ws, "1:1", [headers])
    keys_sorted = sorted(matrix_dict.keys(), key=lambda k: (k[0].lower(), k[1].lower()))
    data_rows = []
    for (g, n) in keys_sorted:
        row = [g, n] + ["" for _ in skill_headers]
        vals = matrix_dict[(g, n)]
        for idx, sh in enumerate(skill_headers, start=0):
            v = vals.get(sh)
            if v is not None:
                row[2 + idx] = v
        data_rows.append(row)
    if data_rows:
        ws.resize(len(data_rows) + 1, len(headers))
        ws_update(ws, "A2", data_rows)
        ws.resize(len(data_rows) + 1, len(headers))
    else:
        ws.resize(1, len(headers))


# ----------------- GUILD PROCESSING -----------------

def process_guild(
    ss,
    ws_guilds,
    ws_players,
    guild_id: str,
    guild_row_idx_1b: int,
    guild_row_vals: List[str],
) -> Tuple[str, int, Dict[str, Dict[str, Any]]]:
    try:
        gdata = fetch_guild({"guildId": guild_id, "includeRecentGuildActivityInfo": True})
    except Exception as e:
        log.warning("Error fetching guild %s: %s", guild_id, e)
        raise

    guild_obj = (
        gdata.get("guild")
        if isinstance(gdata.get("guild"), dict)
        else gdata
    )

    guild_name = (
        _safe_get(guild_obj, ["profile", "name"], "")
        or guild_obj.get("name", "")
    )
    if not guild_name and guild_row_vals:
        try:
            hdrs = _headers(ws_guilds)
            idx_name = hdrs.index("Guild Name")
            guild_name = guild_row_vals[idx_name] if idx_name < len(guild_row_vals) else guild_name
        except Exception:
            pass

    guild_gp = _safe_get(guild_obj, ["profile", "guildGalacticPower"], None)
    if guild_gp is None:
        guild_gp = guild_obj.get("galacticPower", 0)

    members_arr = _safe_get(gdata, ["guild", "member"], []) or []
    members_count = len(members_arr)
    last_raid_id, last_raid_points = _parse_last_raid(gdata)

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

    players_data: Dict[str, Dict[str, Any]] = {}
    for m in members_arr:
        pid = str(m.get("playerId") or "").strip()
        name_guess = str(m.get("playerName") or "").strip()
        role_text = map_member_level(m.get("memberLevel"))
        gp_member = _to_int(m.get("galacticPower"), 0)
        if not pid:
            log.warning("Member %r has no playerId; skipping /player lookup.", name_guess)
            continue

        p_resp: Dict[str, Any] = {}
        try:
            p_resp = fetch_player_by_id(pid)
        except Exception as e:
            log.warning("Error fetching /player playerId=%s (%s): %s", pid, name_guess, e)

        name = str(
            p_resp.get("name")
            or _safe_get(p_resp, ["player", "name"], "")
            or name_guess
        ).strip()
        ally = _parse_allycode(p_resp)
        level = str(
            _safe_get(p_resp, ["level"], "")
            or _safe_get(p_resp, ["player", "level"], "")
        )
        gac = _parse_player_rating(p_resp)
        roster = (
            p_resp.get("rosterUnit")
            or _safe_get(p_resp, ["player", "rosterUnit"], [])
            or []
        )

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

def run(filter_guild_ids: Optional[Set[str]] = None) -> str:
    if not preflight_comlink():
        log.error("Aborting: COMLINK_BASE is not reachable.")
        return "error: comlink preflight"

    active_filter_ids = set(filter_guild_ids) if filter_guild_ids else get_filter_ids_from_env()

    # Use the shared spreadsheet client — no duplicate credential loading
    ss = open_spreadsheet()
    ws_guilds  = ss.worksheet(SHEET_GUILDS)
    ws_players = ss.worksheet(SHEET_PLAYERS)
    ws_pu      = ss.worksheet(SHEET_PLAYER_UNITS)
    ws_ps      = ss.worksheet(SHEET_PLAYER_SKILLS)

    _ensure_headers(ws_guilds, GUILDS_REQUIRED, GUILDS_HEADER_SYNONYMS)
    _ensure_headers(ws_players, PLAYERS_REQUIRED)

    unit_base_ids, base_to_name, is_ship = read_unit_catalog(ss)
    colmap_pu, unit_col_by_friendly, pu_headers = ensure_player_units_headers(
        ws_pu, unit_base_ids, base_to_name
    )
    idx_pu_guild = colmap_pu.get("guild name")
    idx_pu_pname = colmap_pu.get("player name")
    _, pu_existing_rows = _get_all(ws_pu)
    current_by_guild_player = rebuild_pu_index_by_guild_name(
        pu_existing_rows, idx_pu_guild, idx_pu_pname
    )

    p_headers = _headers(ws_players)
    pcol = {h.lower(): i for i, h in enumerate(p_headers, start=1)}
    _, players_existing_rows = _get_all(ws_players)
    players_index_by_pid = rebuild_players_index_by_pid(
        players_existing_rows, pcol.get("player id")
    )

    final_pu_rows = pu_existing_rows[:]
    final_players_rows = players_existing_rows[:]

    ps_headers_exist, ps_rows_exist, ps_skill_headers_exist, ps_matrix_exist = read_ps_matrix(ws_ps)
    processed_guild_names: set[str] = set()

    skill_id_to_header, headers_catalog = read_skill_catalog(ss)

    g_headers, g_rows = _get_all(ws_guilds)
    if not g_rows:
        log.info("No guild rows found.")
        ws_ps.update(values=[["Player Guild", "Player Name"]], range_name="1:1")
        ws_ps.resize(1, 2)
        return "ok: 0 guilds"

    try:
        idx_gid = g_headers.index("Guild Id")
    except ValueError:
        low = [h.lower() for h in g_headers]
        if "guild id" in low:
            idx_gid = low.index("guild id")
        else:
            raise RuntimeError("Guilds sheet is missing the 'Guild Id' column.")

    processed = 0
    skills_matrix: Dict[Tuple[str, str], Dict[str, str]] = {}

    log.info("Processing %d guild row(s)…", len(g_rows))
    for i, row in enumerate(g_rows, start=2):
        gid = (row[idx_gid].strip() if idx_gid < len(row) else "")
        if not gid:
            continue
        if active_filter_ids and gid not in active_filter_ids:
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
                last_exc = e
                time.sleep(delay)
                delay *= 1.6

        if last_exc:
            log.error("Failed to process guildId=%s after %d attempts: %s", gid, attempts, last_exc)
            continue
        if not players_data:
            continue

        guild_name = next(iter(players_data.values())).get("guild_name", "")
        processed_guild_names.add(guild_name)

        idx_gn_players = pcol.get("guild name")
        if guild_name and idx_gn_players:
            final_players_rows = [
                r for r in final_players_rows
                if not (
                    idx_gn_players - 1 < len(r)
                    and (r[idx_gn_players - 1] or "").strip() == guild_name
                )
            ]

        if idx_pu_guild:
            final_pu_rows = [
                r for r in final_pu_rows
                if not (
                    idx_pu_guild - 1 < len(r)
                    and (r[idx_pu_guild - 1] or "").strip() == guild_name
                )
            ]

        players_index_by_pid = rebuild_players_index_by_pid(
            final_players_rows, pcol.get("player id")
        )
        current_by_guild_player = rebuild_pu_index_by_guild_name(
            final_pu_rows, idx_pu_guild, idx_pu_pname
        )

        for pid, pdata in players_data.items():
            pname  = pdata.get("name", "") or ""
            ally   = pdata.get("ally", "") or ""
            level  = pdata.get("level", "") or ""
            gp     = pdata.get("gp", "") or ""
            role   = pdata.get("role", "") or ""
            gac    = pdata.get("gac", "") or ""
            roster = pdata.get("roster", []) or []

            idx_row = players_index_by_pid.get(pid)
            if idx_row is not None and 0 <= idx_row < len(final_players_rows):
                prev = final_players_rows[idx_row]
                merged = prev[:] + [""] * (len(p_headers) - len(prev))
            else:
                merged = [""] * len(p_headers)

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

            if idx_row is not None and 0 <= idx_row < len(final_players_rows):
                final_players_rows[idx_row] = merged
            else:
                final_players_rows.append(merged)
                players_index_by_pid[pid] = len(final_players_rows) - 1

            key = f"{guild_name}|{pname.lower()}"
            idx_row_pu: Optional[int] = current_by_guild_player.get(key)
            if idx_row_pu is not None and 0 <= idx_row_pu < len(final_pu_rows):
                prev = final_pu_rows[idx_row_pu]
                merged_pu = prev[:] + [""] * (len(pu_headers) - len(prev))
            else:
                merged_pu = [""] * len(pu_headers)
                if idx_pu_guild:
                    merged_pu[idx_pu_guild - 1] = guild_name
                if idx_pu_pname:
                    merged_pu[idx_pu_pname - 1] = pname
                current_by_guild_player[key] = len(final_pu_rows)

            if idx_pu_guild:
                merged_pu[idx_pu_guild - 1] = guild_name
            if idx_pu_pname:
                merged_pu[idx_pu_pname - 1] = pname

            base_to_val = roster_to_unit_values(roster, is_ship)
            for base_id, val in base_to_val.items():
                fname = base_to_name.get(base_id)
                if not fname:
                    continue
                col = unit_col_by_friendly.get(fname.strip().lower())
                if not col:
                    continue
                merged_pu[col - 1] = val

            if idx_row_pu is not None and 0 <= idx_row_pu < len(final_pu_rows):
                final_pu_rows[idx_row_pu] = merged_pu
            else:
                final_pu_rows.append(merged_pu)

            if skill_id_to_header:
                rowdict = skills_matrix.setdefault((guild_name, pname), {})
                skill_tiers = rp.extract_skill_tiers_by_id(roster)
                for sid, tier_int in skill_tiers.items():
                    if sid not in skill_id_to_header or _exclude_skillid(sid):
                        continue
                    header = skill_id_to_header[sid]
                    prevv = rowdict.get(header)
                    if prevv is None or tier_int > _to_int(prevv, 0):
                        rowdict[header] = str(tier_int)
                        
        processed += 1

    if final_players_rows != players_existing_rows:
        write_table_body(ws_players, p_headers, final_players_rows)
    if final_pu_rows != pu_existing_rows:
        write_table_body(ws_pu, _headers(ws_pu), final_pu_rows)

    for g in processed_guild_names:
        for key in list(ps_matrix_exist.keys()):
            if key[0] == g:
                del ps_matrix_exist[key]

    for key, vals in skills_matrix.items():
        ps_matrix_exist[key] = vals

    skills_in_use: set[str] = set()
    for vals in ps_matrix_exist.values():
        for sh, v in vals.items():
            if sh and v is not None and str(v) != "":
                skills_in_use.add(sh)

    skill_headers_merged = sorted(skills_in_use, key=str.lower)
    write_ps_matrix(ws_ps, ps_matrix_exist, skill_headers_merged)

    if processed == 0:
        log.info("No new rows to write.")

    return (
        f"ok: guilds={processed}, "
        f"players_upserted~={len(final_players_rows)}, "
        f"player_units_rows={len(final_pu_rows)}, "
        f"skill_matrix_rows={len(ps_matrix_exist)}, "
        f"skill_cols={len(skill_headers_merged)}"
    )


if __name__ == "__main__":
    print(run())
