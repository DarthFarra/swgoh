# src/swgoh/bot/services/sheets.py
from __future__ import annotations
from typing import List, Tuple, Dict, Optional
from datetime import datetime, date
from zoneinfo import ZoneInfo

from .. import config as bot_cfg
from ... import sheets as core_sheets

USERS_SHEET            = bot_cfg.USERS_SHEET
GUILDS_SHEET           = bot_cfg.GUILDS_SHEET
PLAYERS_SHEET          = bot_cfg.PLAYERS_SHEET
DEFAULT_ROTE_SHEET     = bot_cfg.DEFAULT_ROTE_SHEET
TICKET_SNAPSHOTS_SHEET = bot_cfg.TICKET_SNAPSHOTS_SHEET
TZ                     = bot_cfg.TZ


def open_ss():
    """Abre el Spreadsheet con el cliente core."""
    return core_sheets.spreadsheet()


def _get_all(ws):
    vals = ws.get_all_values() or []
    if not vals:
        return [], []
    return [h.strip() for h in (vals[0] or [])], (vals[1:] if len(vals) > 1 else [])


# ---------------------------------------------------------------------------
# Guilds
# ---------------------------------------------------------------------------

def map_guild_name_to_label_id_rote(ss) -> Dict[str, Tuple[str, str, str]]:
    """Guild Name -> (label, guild_id, rote_sheet_name)"""
    ws = ss.worksheet(GUILDS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    try:
        i_name = hl.index("guild name")
        i_id   = hl.index("guild id")
    except ValueError:
        return {}
    i_abbr = hl.index("nombre abreviado") if "nombre abreviado" in hl else None
    i_rote = hl.index("rote") if "rote" in hl else None

    out = {}
    for r in rows:
        gname = (r[i_name] if i_name < len(r) else "").strip()
        gid   = (r[i_id]   if i_id   < len(r) else "").strip()
        if not (gname and gid):
            continue
        abbr = (r[i_abbr] if (i_abbr is not None and i_abbr < len(r)) else "").strip() if i_abbr is not None else ""
        rote = (r[i_rote] if (i_rote is not None and i_rote < len(r)) else "").strip() if i_rote is not None else ""
        out[gname] = (abbr or gname, gid, rote or DEFAULT_ROTE_SHEET)
    return out


def resolve_label_name_rote_by_id(ss, guild_id: str) -> Tuple[str, str, str]:
    gmap = map_guild_name_to_label_id_rote(ss)
    for gname, (label, gid, rote) in gmap.items():
        if gid == guild_id:
            return (label or gname or "gremio seleccionado", gname, rote or DEFAULT_ROTE_SHEET)
    return ("gremio seleccionado", "", DEFAULT_ROTE_SHEET)


def already_synced_today(ss, guild_id: str) -> bool:
    ws = ss.worksheet(GUILDS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    if "guild id" not in hl or "last update" not in hl:
        return False
    i_id   = hl.index("guild id")
    i_last = hl.index("last update")
    today  = datetime.now(TZ).date().isoformat()
    for r in rows:
        gid = (r[i_id] if i_id < len(r) else "").strip()
        if gid != guild_id:
            continue
        last = (r[i_last] if i_last < len(r) else "").strip()
        if last and last[:10] == today:
            return True
    return False


def get_guild_reset_time(ss, guild_id: str) -> Optional[str]:
    """Returns reset time (HH:MM, Madrid TZ) for guild_id, or None."""
    ws = ss.worksheet(GUILDS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.strip().lower() for h in headers]
    if "guild id" not in hl or "reset_time" not in hl:
        return None
    i_id = hl.index("guild id")
    i_rt = hl.index("reset_time")
    for r in rows:
        gid = (r[i_id] if i_id < len(r) else "").strip()
        if gid == guild_id:
            raw = (r[i_rt] if i_rt < len(r) else "").strip()
            return raw if raw else None
    return None


def list_guilds_with_reset_time(ss) -> list[tuple[str, str, str]]:
    """Returns [(guild_id, guild_name, reset_time_str), ...] for guilds with reset_time set."""
    ws = ss.worksheet(GUILDS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.strip().lower() for h in headers]
    if "guild id" not in hl or "guild name" not in hl or "reset_time" not in hl:
        return []
    i_id   = hl.index("guild id")
    i_name = hl.index("guild name")
    i_rt   = hl.index("reset_time")
    out = []
    for r in rows:
        gid  = (r[i_id]   if i_id   < len(r) else "").strip()
        name = (r[i_name] if i_name < len(r) else "").strip()
        rt   = (r[i_rt]   if i_rt   < len(r) else "").strip()
        if gid and name and rt:
            out.append((gid, name, rt))
    return out


def get_channel_id_for_guild(ss, guild_name: str) -> Optional[str]:
    """Returns announcements channel ID for guild_name, or None."""
    channel_id, _ = get_channel_config_for_guild(ss, guild_name)
    return channel_id


def get_channel_config_for_guild(
    ss, guild_name: str
) -> tuple[Optional[str], Optional[int]]:
    """
    Returns (channel_id, thread_id) for guild_name in a single sheet read.
    thread_id is None if the column is missing, empty, or not a valid integer.
    """
    ws = ss.worksheet(GUILDS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.strip().lower() for h in headers]
    if "guild name" not in hl or "announcements_channel" not in hl:
        return None, None
    i_name   = hl.index("guild name")
    i_ch     = hl.index("announcements_channel")
    i_thread = hl.index("announcements_thread_id") if "announcements_thread_id" in hl else None
    for r in rows:
        gn = (r[i_name] if i_name < len(r) else "").strip()
        if gn != guild_name:
            continue
        channel_id = (r[i_ch] if i_ch < len(r) else "").strip() or None
        thread_id: Optional[int] = None
        if i_thread is not None:
            raw_thread = (r[i_thread] if i_thread < len(r) else "").strip()
            if raw_thread:
                try:
                    thread_id = int(raw_thread)
                except ValueError:
                    pass
        return channel_id, thread_id
    return None, None


# ---------------------------------------------------------------------------
# Usuarios
# ---------------------------------------------------------------------------

def ensure_usuarios_headers(ws) -> Dict[str, int]:
    headers = ws.row_values(1) or []
    low     = [h.strip().lower() for h in headers]
    needed  = ["alias", "username", "user_id", "chat_id", "rol", "allycode", "guild_name"]
    changed = False
    for k in needed:
        if k not in low:
            headers.append(k)
            low.append(k)
            changed = True
    if changed:
        ws.update("1:1", [headers])
    return {h: i for i, h in enumerate([h.strip().lower() for h in headers])}


def usuarios_already_registered(ss, user_id: int, guild_name: str) -> bool:
    ws = ss.worksheet(USERS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    try:
        i_uid = hl.index("user_id")
        i_gn  = hl.index("guild_name")
    except ValueError:
        return False
    for r in rows:
        if i_uid < len(r) and str(r[i_uid]).strip() == str(user_id):
            if i_gn < len(r) and (r[i_gn] or "").strip() == guild_name:
                return True
    return False


def usuarios_guilds_for_user(ss, user_id: int) -> List[Tuple[str, str, str]]:
    """Returns [(label, guild_id, guild_name)] for all guilds the user belongs to."""
    ws = ss.worksheet(USERS_SHEET)
    uh, ur = _get_all(ws)
    ul    = [h.lower() for h in uh]
    i_uid = ul.index("user_id")    if "user_id"    in ul else None
    i_gn  = ul.index("guild_name") if "guild_name" in ul else None
    if i_uid is None or i_gn is None:
        return []
    gmap = map_guild_name_to_label_id_rote(ss)
    out, seen = [], set()
    for r in ur:
        if i_uid < len(r) and str(r[i_uid]).strip() == str(user_id):
            gname = (r[i_gn] if i_gn < len(r) else "").strip()
            if gname and gname in gmap:
                label, gid, _ = gmap[gname]
                if gid not in seen:
                    seen.add(gid)
                    out.append((label, gid, gname))
    return out


def upsert_usuario(ss, info: dict, tg_username: str, user_id: int, chat_id: int):
    """Inserta/actualiza fila en Usuarios por (guild_name + alias)."""
    ws      = ss.worksheet(USERS_SHEET)
    hdr_map = ensure_usuarios_headers(ws)
    vals    = ws.get_all_values() or []
    rows    = vals[1:] if len(vals) > 1 else []

    i_alias = hdr_map["alias"];     i_user = hdr_map["username"]; i_uid = hdr_map["user_id"]
    i_chat  = hdr_map["chat_id"];   i_rol  = hdr_map["rol"];      i_ac  = hdr_map["allycode"]
    i_gn    = hdr_map["guild_name"]

    alias = info.get("alias", "")
    gname = info.get("guild_name", "")
    role  = info.get("role", "")
    ally  = info.get("allycode", "")

    target_idx = None
    for idx, r in enumerate(rows):
        gn = (r[i_gn]    if i_gn    < len(r) else "").strip()
        al = (r[i_alias] if i_alias < len(r) else "").strip()
        if gn == gname and al.strip().lower() == alias.strip().lower():
            target_idx = idx
            break

    def setf(row, i, v):
        if i < len(row):
            row[i] = "" if v is None else str(v)

    if target_idx is None:
        new_row = [""] * len(ws.row_values(1) or [])
        setf(new_row, i_alias, alias);        setf(new_row, i_user, tg_username or "")
        setf(new_row, i_uid,   str(user_id)); setf(new_row, i_chat, str(chat_id))
        setf(new_row, i_rol,   role);         setf(new_row, i_ac,   ally)
        setf(new_row, i_gn,    gname)
        ws.append_row(new_row, value_input_option="USER_ENTERED")
    else:
        row_vals = rows[target_idx][:]
        need_len = len(ws.row_values(1) or [])
        if len(row_vals) < need_len:
            row_vals += [""] * (need_len - len(row_vals))
        setf(row_vals, i_alias, alias);        setf(row_vals, i_user, tg_username or "")
        setf(row_vals, i_uid,   str(user_id)); setf(row_vals, i_chat, str(chat_id))
        setf(row_vals, i_rol,   role);         setf(row_vals, i_ac,   ally)
        setf(row_vals, i_gn,    gname)
        ws.update(f"{target_idx+2}:{target_idx+2}", [row_vals])


def user_has_leadership_role(ss, user_id: int, guild_name: str) -> bool:
    ws = ss.worksheet(USERS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    try:
        i_uid = hl.index("user_id")
        i_gn  = hl.index("guild_name")
        i_rol = hl.index("rol")
    except ValueError:
        return False
    for r in rows:
        if str(r[i_uid] if i_uid < len(r) else "").strip() == str(user_id):
            gn  = (r[i_gn]  if i_gn  < len(r) else "").strip()
            if gn == guild_name:
                rol = (r[i_rol] if i_rol < len(r) else "").strip().lower()
                return rol in ["oficial", "lider", "líder"]
    return False


def user_alias_for_guild(ss, user_id: int, guild_name: str) -> Optional[str]:
    ws = ss.worksheet(USERS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    try:
        i_uid   = hl.index("user_id")
        i_gn    = hl.index("guild_name")
        i_alias = hl.index("alias")
    except ValueError:
        return None
    for r in rows:
        try:
            if str(r[i_uid]).strip() != str(user_id):
                continue
        except Exception:
            continue
        gn = (r[i_gn] if i_gn < len(r) else "").strip()
        if gn == guild_name:
            return (r[i_alias] if i_alias < len(r) else "").strip() or None
    return None


def get_chat_ids_for_members(
    ss, guild_name: str, player_names: list[str]
) -> Dict[str, int]:
    """
    Returns {player_name_lower: chat_id} for members of guild_name
    whose name (case-insensitive) is in player_names.
    Only includes rows where chat_id is a valid integer.
    """
    ws = ss.worksheet(USERS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.strip().lower() for h in headers]
    required = ["alias", "guild_name", "chat_id"]
    if any(col not in hl for col in required):
        return {}
    i_alias = hl.index("alias")
    i_gn    = hl.index("guild_name")
    i_chat  = hl.index("chat_id")
    targets = {n.strip().lower() for n in player_names if n.strip()}
    result: Dict[str, int] = {}
    for r in rows:
        gn = (r[i_gn] if i_gn < len(r) else "").strip()
        if gn != guild_name:
            continue
        alias = (r[i_alias] if i_alias < len(r) else "").strip()
        if alias.lower() not in targets:
            continue
        raw_chat = (r[i_chat] if i_chat < len(r) else "").strip()
        try:
            result[alias.lower()] = int(raw_chat)
        except (ValueError, TypeError):
            pass
    return result


def get_usernames_for_members(
    ss, guild_name: str, player_names: list[str]
) -> Dict[str, Optional[str]]:
    """
    Returns {player_name_lower: username_or_None} for members of guild_name.
    username is the bare @handle without '@', or None if not set.
    """
    ws = ss.worksheet(USERS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.strip().lower() for h in headers]
    required = ["alias", "guild_name"]
    if any(col not in hl for col in required):
        return {}
    i_alias = hl.index("alias")
    i_gn    = hl.index("guild_name")
    i_user  = hl.index("username") if "username" in hl else None
    targets = {n.strip().lower() for n in player_names if n.strip()}
    result: Dict[str, Optional[str]] = {}
    for r in rows:
        gn = (r[i_gn] if i_gn < len(r) else "").strip()
        if gn != guild_name:
            continue
        alias = (r[i_alias] if i_alias < len(r) else "").strip()
        if alias.lower() not in targets:
            continue
        username = None
        if i_user is not None:
            raw = (r[i_user] if i_user < len(r) else "").strip()
            username = raw.lstrip("@") if raw else None
        result[alias.lower()] = username
    return result


# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------

def players_find_by_alias(ss, guild_name: str, alias: str) -> Optional[dict]:
    ws = ss.worksheet(PLAYERS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    try:
        i_name = hl.index("player name")
        i_gn   = hl.index("guild name")
        i_ac   = hl.index("ally code")
    except ValueError:
        return None
    i_role = hl.index("role") if "role" in hl else (hl.index("rol") if "rol" in hl else None)
    anorm  = (alias or "").strip().lower()
    for r in rows:
        gn = (r[i_gn] if i_gn < len(r) else "").strip()
        if gn != guild_name:
            continue
        name = (r[i_name] if i_name < len(r) else "").strip()
        if name.strip().lower() == anorm:
            ac  = (r[i_ac] if i_ac < len(r) else "").strip()
            acd = "".join(ch for ch in ac if ch.isdigit())
            return {
                "alias":      name,
                "allycode":   acd,
                "role":       (r[i_role] if (i_role is not None and i_role < len(r)) else "").strip(),
                "guild_name": gn,
            }
    return None


def players_find_by_ally(ss, guild_name: str, allycode: str) -> Optional[dict]:
    ws = ss.worksheet(PLAYERS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    try:
        i_name = hl.index("player name")
        i_gn   = hl.index("guild name")
        i_ac   = hl.index("ally code")
    except ValueError:
        return None
    i_role  = hl.index("role") if "role" in hl else (hl.index("rol") if "rol" in hl else None)
    acd_in  = "".join(ch for ch in str(allycode) if ch.isdigit())
    for r in rows:
        gn = (r[i_gn] if i_gn < len(r) else "").strip()
        if gn != guild_name:
            continue
        ac  = (r[i_ac] if i_ac < len(r) else "").strip()
        acd = "".join(ch for ch in ac if ch.isdigit())
        if acd and acd == acd_in:
            name = (r[i_name] if i_name < len(r) else "").strip()
            return {
                "alias":      name,
                "allycode":   acd,
                "role":       (r[i_role] if (i_role is not None and i_role < len(r)) else "").strip(),
                "guild_name": gn,
            }
    return None


def list_players_for_guild(ss, guild_name: str) -> List[Tuple[str, str]]:
    ws = ss.worksheet(PLAYERS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    try:
        i_gn   = hl.index("guild name")
        i_name = hl.index("player name")
    except ValueError:
        return []
    players = []
    for r in rows:
        gn = (r[i_gn] if i_gn < len(r) else "").strip()
        if gn == guild_name:
            name = (r[i_name] if i_name < len(r) else "").strip()
            if name:
                players.append((name, name))
    return sorted(players, key=lambda x: x[0].lower())

def player_id_for_alias(ss, guild_name: str, alias: str) -> Optional[str]:
    """
    Returns the Player Id for (guild_name, alias), or None if
    no match. Case-insensitive on alias; exact match on guild_name.
 
    The Player Id column is populated by /syncguild. If a freshly
    registered user runs /omicrones before the next /syncguild, they'll
    get None here — the caller should explain that politely.
    """
    ws = ss.worksheet(PLAYERS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    try:
        i_pid  = hl.index("player id")
        i_name = hl.index("player name")
        i_gn   = hl.index("guild name")
    except ValueError:
        return None
 
    anorm = (alias or "").strip().lower()
    for r in rows:
        gn = (r[i_gn] if i_gn < len(r) else "").strip()
        if gn != guild_name:
            continue
        name = (r[i_name] if i_name < len(r) else "").strip()
        if name.strip().lower() != anorm:
            continue
        pid = (r[i_pid] if i_pid < len(r) else "").strip()
        return pid or None
    return None


# ---------------------------------------------------------------------------
# Asignaciones ROTE
# ---------------------------------------------------------------------------

def render_assignments_for_alias(ss, rote_sheet: str, alias: str) -> str:
    ws = ss.worksheet(rote_sheet)
    headers, rows = _get_all(ws)
    if not rows:
        return "No hay asignaciones."
    hl  = [h.lower() for h in headers]
    col = {h: i for i, h in enumerate(hl)}
    need = ["fase", "planeta", "operacion", "personaje", "reliquia", "jugador"]
    for n in need:
        if n not in col:
            return f"No se encontró la columna '{n}' en la hoja '{rote_sheet}'."
    i_fase, i_plan, i_op, i_char, i_rel, i_jug = (col[n] for n in need)
    alias_norm = (alias or "").strip().lower()
    per_fase: Dict[str, list] = {}
    for r in rows:
        jugador = (r[i_jug] if i_jug < len(r) else "").strip()
        if jugador.strip().lower() != alias_norm:
            continue
        fase      = (r[i_fase] if i_fase < len(r) else "").strip()
        planeta   = (r[i_plan] if i_plan < len(r) else "").strip()
        oper      = (r[i_op]   if i_op   < len(r) else "").strip()
        personaje = (r[i_char] if i_char < len(r) else "").strip()
        req       = (r[i_rel]  if i_rel  < len(r) else "").strip() or "R0"
        per_fase.setdefault(fase or "—", []).append((planeta, oper, personaje, req))
    if not per_fase:
        return "No tienes asignaciones."

    def fase_key(fv: str):
        try:    return (0, int(fv))
        except: return (1, fv.lower())

    parts = []
    for fase in sorted(per_fase.keys(), key=fase_key):
        items = per_fase[fase]
        items.sort(key=lambda x: (x[0].lower(), x[1].lower(), x[2].lower()))
        parts.append(f"**Fase {fase}** ({len(items)})")
        for planeta, oper, personaje, req in items:
            parts.append(f"• {planeta} / {oper} — *{personaje}* (`{req}`)")
        parts.append("")
    return "\n".join(parts).strip()


def list_phases_in_rote(ss, rote_sheet: str):
    ws = ss.worksheet(rote_sheet)
    headers, rows = _get_all(ws)
    if not rows:
        return []
    hl = [h.lower() for h in headers]
    if "fase" not in hl:
        return []
    i_fase  = hl.index("fase")
    phases  = set()
    for r in rows:
        fv = (r[i_fase] if i_fase < len(r) else "").strip()
        if fv and fv.strip().lower() != "x":
            phases.add(fv.strip())

    def _key(x):
        try:    return (0, int(x))
        except: return (1, x.lower())

    return sorted(phases, key=_key)


def render_ops_for_alias_phase_grouped(ss, rote_sheet: str, alias: str, phase: str) -> str:
    ws = ss.worksheet(rote_sheet)
    headers, rows = _get_all(ws)
    if not rows:
        return "No tienes asignaciones en esta fase."
    hl   = [h.lower() for h in headers]
    need = ["fase", "planeta", "operacion", "personaje", "jugador"]
    for n in need:
        if n not in hl:
            return "No tienes asignaciones en esta fase."
    i_fase = hl.index("fase");     i_plan = hl.index("planeta")
    i_op   = hl.index("operacion"); i_char = hl.index("personaje")
    i_jug  = hl.index("jugador")
    alias_norm = (alias or "").strip().lower()
    phase_str  = str(phase).strip()
    groups: Dict[str, list] = {}
    for r in rows:
        jugador = (r[i_jug] if i_jug < len(r) else "").strip().lower()
        if jugador != alias_norm:
            continue
        fase_val = (r[i_fase] if i_fase < len(r) else "").strip()
        if fase_val != phase_str:
            continue
        planeta   = (r[i_plan] if i_plan < len(r) else "").strip() or "—"
        personaje = (r[i_char] if i_char < len(r) else "").strip()
        oper      = (r[i_op]   if i_op   < len(r) else "").strip()
        groups.setdefault(planeta, []).append((personaje, oper))
    if not groups:
        return "No tienes asignaciones en esta fase."
    parts = []
    for planeta in sorted(groups.keys(), key=lambda s: s.lower()):
        parts.append(f"{planeta}")
        for personaje, oper in sorted(groups[planeta], key=lambda t: (t[1].lower(), t[0].lower())):
            parts.append(f"- {personaje} ({oper})")
        parts.append("")
    return "\n".join(parts).strip()


# ---------------------------------------------------------------------------
# Ticket Snapshots — read / write
# ---------------------------------------------------------------------------
#
# Schema (one row per member per guild):
#   guild_name | player_name | last_snapshot_date | lifetime_d | lifetime_d1
#
# lifetime_d  = lifetimeValue taken at today's reset
# lifetime_d1 = lifetimeValue taken at yesterday's reset (promoted from lifetime_d)
#
# "Yesterday missed": lifetime_d - lifetime_d1 < 600

SNAPSHOT_COLS = ["guild_name", "player_name", "last_snapshot_date", "lifetime_d", "lifetime_d1"]


def _ensure_snapshot_headers(ws) -> Dict[str, int]:
    """Ensures Ticket_Snapshots has required columns. Returns header->index map."""
    headers = ws.row_values(1) or []
    low     = [h.strip().lower() for h in headers]
    changed = False
    for col in SNAPSHOT_COLS:
        if col not in low:
            headers.append(col)
            low.append(col)
            changed = True
    if changed:
        ws.update("A1", [headers])
    return {h.strip().lower(): i for i, h in enumerate(headers)}


def upsert_ticket_snapshots(ss, guild_name: str, snapshots: Dict[str, int]) -> None:
    """
    Called at reset time. For every member of guild_name:
      1. Promotes lifetime_d -> lifetime_d1
      2. Writes fresh lifetimeValue -> lifetime_d
      3. Updates last_snapshot_date to today

    New members get lifetime_d1 = lifetime_d (can't be judged on first snapshot).
    Members absent from snapshots (left guild) are removed.
    Other guilds' rows are preserved untouched.
    """
    ws  = ss.worksheet(TICKET_SNAPSHOTS_SHEET)
    hdr = _ensure_snapshot_headers(ws)

    i_gn   = hdr["guild_name"]
    i_pn   = hdr["player_name"]
    i_date = hdr["last_snapshot_date"]
    i_ld   = hdr["lifetime_d"]
    i_ld1  = hdr["lifetime_d1"]
    n_cols = len(hdr)

    today_str = date.today().isoformat()

    all_vals = ws.get_all_values() or []
    existing: list[list[str]] = all_vals[1:] if len(all_vals) > 1 else []

    other_rows = [
        r for r in existing
        if (r[i_gn] if i_gn < len(r) else "").strip() != guild_name
    ]

    this_guild: Dict[str, list[str]] = {}
    for r in existing:
        if (r[i_gn] if i_gn < len(r) else "").strip() != guild_name:
            continue
        pn = (r[i_pn] if i_pn < len(r) else "").strip()
        if pn:
            this_guild[pn.lower()] = r

    new_rows: list[list[str]] = []
    for player_name, fresh_lifetime in snapshots.items():
        row        = [""] * n_cols
        row[i_gn]  = guild_name
        row[i_pn]  = player_name
        row[i_date] = today_str

        existing_row = this_guild.get(player_name.lower())
        if existing_row is not None:
            old_d      = (existing_row[i_ld] if i_ld < len(existing_row) else "").strip()
            row[i_ld1] = old_d if old_d else str(fresh_lifetime)
        else:
            row[i_ld1] = str(fresh_lifetime)

        row[i_ld] = str(fresh_lifetime)
        new_rows.append(row)

    new_rows.sort(key=lambda r: r[i_pn].lower())

    final_rows = other_rows + new_rows
    hdr_row    = [""] * n_cols
    for col, idx in hdr.items():
        hdr_row[idx] = col

    ws.clear()
    ws.update("A1", [hdr_row])
    if final_rows:
        ws.update("A2", final_rows)


def read_ticket_snapshot(
    ss, guild_name: str
) -> Optional[tuple[str, Dict[str, int], Dict[str, int]]]:
    """
    Returns (last_snapshot_date, lifetime_d, lifetime_d1) for guild_name,
    or None if no snapshot exists.
    """
    try:
        ws = ss.worksheet(TICKET_SNAPSHOTS_SHEET)
    except Exception:
        return None

    headers, rows = _get_all(ws)
    if not rows:
        return None

    hl = [h.strip().lower() for h in headers]
    if any(col not in hl for col in SNAPSHOT_COLS):
        return None

    i_gn   = hl.index("guild_name")
    i_pn   = hl.index("player_name")
    i_date = hl.index("last_snapshot_date")
    i_ld   = hl.index("lifetime_d")
    i_ld1  = hl.index("lifetime_d1")

    snapshot_date: Optional[str] = None
    d:  Dict[str, int] = {}
    d1: Dict[str, int] = {}

    for r in rows:
        gn = (r[i_gn] if i_gn < len(r) else "").strip()
        if gn != guild_name:
            continue
        pn  = (r[i_pn]   if i_pn   < len(r) else "").strip()
        dt  = (r[i_date] if i_date  < len(r) else "").strip()
        ld  = (r[i_ld]   if i_ld   < len(r) else "").strip()
        ld1 = (r[i_ld1]  if i_ld1  < len(r) else "").strip()
        if not pn:
            continue
        d[pn.lower()]  = _safe_int(ld)
        d1[pn.lower()] = _safe_int(ld1)
        if snapshot_date is None:
            snapshot_date = dt

    if not d:
        return None

    return snapshot_date, d, d1


def snapshot_taken_today(ss, guild_name: str) -> bool:
    """Returns True if last_snapshot_date for guild_name equals today (Madrid TZ)."""
    today_str = datetime.now(ZoneInfo("Europe/Madrid")).date().isoformat()
    try:
        ws = ss.worksheet(TICKET_SNAPSHOTS_SHEET)
    except Exception:
        return False

    headers, rows = _get_all(ws)
    if not rows:
        return False

    hl = [h.strip().lower() for h in headers]
    if "guild_name" not in hl or "last_snapshot_date" not in hl:
        return False

    i_gn   = hl.index("guild_name")
    i_date = hl.index("last_snapshot_date")

    for r in rows:
        gn = (r[i_gn] if i_gn < len(r) else "").strip()
        if gn != guild_name:
            continue
        dt = (r[i_date] if i_date < len(r) else "").strip()
        return dt == today_str

    return False


def _safe_int(val: str, default: int = 0) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default
