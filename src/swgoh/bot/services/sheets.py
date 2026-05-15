# src/swgoh/bot/services/sheets.py
from __future__ import annotations
from typing import List, Tuple, Dict, Optional
from datetime import datetime

# Reutiliza tu cliente y spreadsheet del core
from .. import config as bot_cfg
from ... import sheets as core_sheets  # <- tu src/swgoh/sheets.py

USERS_SHEET   = bot_cfg.USERS_SHEET
GUILDS_SHEET  = bot_cfg.GUILDS_SHEET
PLAYERS_SHEET = bot_cfg.PLAYERS_SHEET
DEFAULT_ROTE_SHEET = bot_cfg.DEFAULT_ROTE_SHEET
TICKET_SNAPSHOTS_SHEET = bot_cfg.TICKET_SNAPSHOTS_SHEET
TZ = bot_cfg.TZ

def open_ss():
    """Abre el Spreadsheet con TU cliente core (no reautorizamos)."""
    return core_sheets.spreadsheet()  # tu función core

def _get_all(ws):
    vals = ws.get_all_values() or []
    if not vals:
        return [], []
    return [h.strip() for h in (vals[0] or [])], (vals[1:] if len(vals) > 1 else [])

# ---------- Guilds ----------
def map_guild_name_to_label_id_rote(ss) -> Dict[str, Tuple[str, str, str]]:
    """
    Guild Name -> (label, guild_id, rote_sheet_name)
    label = 'nombre abreviado' si existe; si no, 'Guild Name'
    """
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
        abbr  = (r[i_abbr] if (i_abbr is not None and i_abbr < len(r)) else "").strip() if i_abbr is not None else ""
        rote  = (r[i_rote] if (i_rote is not None and i_rote < len(r)) else "").strip() if i_rote is not None else ""
        out[gname] = (abbr or gname, gid, (rote or DEFAULT_ROTE_SHEET))
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
    i_id = hl.index("guild id")
    i_last = hl.index("last update")
    today = datetime.now(TZ).date().isoformat()
    for r in rows:
        gid = (r[i_id] if i_id < len(r) else "").strip()
        if gid != guild_id:
            continue
        last = (r[i_last] if i_last < len(r) else "").strip()
        if last and last[:10] == today:
            return True
    return False

# ---------- Usuarios ----------
def ensure_usuarios_headers(ws) -> Dict[str,int]:
    headers = ws.row_values(1) or []
    low = [h.strip().lower() for h in headers]
    needed = ["alias","username","user_id","chat_id","rol","allycode","guild_name"]
    changed = False
    for k in needed:
        if k not in low:
            headers.append(k); low.append(k); changed = True
    if changed:
        ws.update("1:1", [headers])
    return {h: i for i, h in enumerate([h.strip().lower() for h in headers])}

def usuarios_already_registered(ss, user_id: int, guild_name: str) -> bool:
    ws = ss.worksheet(USERS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    try:
        i_uid = hl.index("user_id"); i_gn = hl.index("guild_name")
    except ValueError:
        return False
    for r in rows:
        if i_uid < len(r) and str(r[i_uid]).strip() == str(user_id):
            if i_gn < len(r) and (r[i_gn] or "").strip() == guild_name:
                return True
    return False

def usuarios_guilds_for_user(ss, user_id: int) -> List[Tuple[str, str, str]]:
    """Gremios (label, guild_id, guild_name) donde está el user_id (cualquier rol)."""
    ws = ss.worksheet(USERS_SHEET)
    uh, ur = _get_all(ws)
    ul = [h.lower() for h in uh]
    i_uid = ul.index("user_id") if "user_id" in ul else None
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
                    seen.add(gid); out.append((label, gid, gname))
    return out

def upsert_usuario(ss, info: dict, tg_username: str, user_id: int, chat_id: int):
    """Inserta/actualiza fila en Usuarios por (guild_name + alias)."""
    ws = ss.worksheet(USERS_SHEET)
    hdr_map = ensure_usuarios_headers(ws)
    vals = ws.get_all_values() or []
    rows = vals[1:] if len(vals) > 1 else []

    i_alias = hdr_map["alias"]; i_user = hdr_map["username"]; i_uid = hdr_map["user_id"]
    i_chat = hdr_map["chat_id"]; i_rol = hdr_map["rol"]; i_ac = hdr_map["allycode"]; i_gn = hdr_map["guild_name"]

    alias = info.get("alias","")
    gname = info.get("guild_name","")
    role  = info.get("role","")
    ally  = info.get("allycode","")

    # localizar fila por (guild_name + alias)
    target_idx = None
    for idx, r in enumerate(rows):
        gn = (r[i_gn] if i_gn < len(r) else "").strip()
        al = (r[i_alias] if i_alias < len(r) else "").strip()
        if gn == gname and al.strip().lower() == alias.strip().lower():
            target_idx = idx
            break

    if target_idx is None:
        headers_now = ws.row_values(1) or []
        new_row = [""] * len(headers_now)
        def setf(i, v):
            if i < len(new_row): new_row[i] = "" if v is None else str(v)
        setf(i_alias, alias); setf(i_user, tg_username or ""); setf(i_uid, str(user_id))
        setf(i_chat, str(chat_id)); setf(i_rol, role); setf(i_ac, ally); setf(i_gn, gname)
        ws.append_row(new_row, value_input_option="USER_ENTERED")
    else:
        row_vals = rows[target_idx][:]
        need_len = len(ws.row_values(1) or [])
        if len(row_vals) < need_len:
            row_vals += [""] * (need_len - len(row_vals))
        def setf(i, v):
            if i < len(row_vals): row_vals[i] = "" if v is None else str(v)
        setf(i_alias, alias); setf(i_user, tg_username or ""); setf(i_uid, str(user_id))
        setf(i_chat, str(chat_id)); setf(i_rol, role); setf(i_ac, ally); setf(i_gn, gname)
        ws.update(f"{target_idx+2}:{target_idx+2}", [row_vals])

# ---------- Players ----------
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

    anorm = (alias or "").strip().lower()
    for r in rows:
        gn = (r[i_gn] if i_gn < len(r) else "").strip()
        if gn != guild_name:
            continue
        name = (r[i_name] if i_name < len(r) else "").strip()
        if name.strip().lower() == anorm:
            ac = (r[i_ac] if i_ac < len(r) else "").strip()
            acd = "".join(ch for ch in ac if ch.isdigit())
            return {
                "alias": name,
                "allycode": acd,
                "role": (r[i_role] if (i_role is not None and i_role < len(r)) else "").strip(),
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
    i_role = hl.index("role") if "role" in hl else (hl.index("rol") if "rol" in hl else None)

    acd_in = "".join(ch for ch in str(allycode) if ch.isdigit())
    for r in rows:
        gn = (r[i_gn] if i_gn < len(r) else "").strip()
        if gn != guild_name:
            continue
        ac = (r[i_ac] if i_ac < len(r) else "").strip()
        acd = "".join(ch for ch in ac if ch.isdigit())
        if acd and acd == acd_in:
            name = (r[i_name] if i_name < len(r) else "").strip()
            return {
                "alias": name,
                "allycode": acd,
                "role": (r[i_role] if (i_role is not None and i_role < len(r)) else "").strip(),
                "guild_name": gn,
            }
    return None

def user_alias_for_guild(ss, user_id: int, guild_name: str) -> Optional[str]:
    ws = ss.worksheet(USERS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    try:
        i_uid = hl.index("user_id")
        i_gn  = hl.index("guild_name")
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

# ---------- Asignaciones ----------
def render_assignments_for_alias(ss, rote_sheet: str, alias: str) -> str:
    """
    Render bonito en Markdown:
    - Título por fase en negrita y conteo de asignaciones
    - Cada línea: • Planeta / Operación — Personaje (`req`)
    - Ordena por planeta y operación
    """
    ws = ss.worksheet(rote_sheet)
    headers, rows = _get_all(ws)
    if not rows:
        return "No hay asignaciones."

    hl = [h.lower() for h in headers]
    col = {h: i for i, h in enumerate(hl)}
    need = ["fase","planeta","operacion","personaje","reliquia","jugador"]
    for n in need:
        if n not in col:
            return f"No se encontró la columna '{n}' en la hoja '{rote_sheet}'."

    i_fase, i_plan, i_op, i_char, i_rel, i_jug = (col[n] for n in need)
    alias_norm = (alias or "").strip().lower()

    # Recolectar asignaciones del alias
    per_fase = {}  # fase -> list[(planeta, oper, personaje, req)]
    for r in rows:
        jugador = (r[i_jug] if i_jug < len(r) else "").strip()
        if (jugador or "").strip().lower() != alias_norm:
            continue
        fase = (r[i_fase] if i_fase < len(r) else "").strip()
        planeta = (r[i_plan] if i_plan < len(r) else "").strip()
        oper = (r[i_op] if i_op < len(r) else "").strip()
        personaje = (r[i_char] if i_char < len(r) else "").strip()
        req = (r[i_rel] if i_rel < len(r) else "").strip() or "R0"
        per_fase.setdefault(fase or "—", []).append((planeta, oper, personaje, req))

    if not per_fase:
        return "No tienes asignaciones."

    # Orden auxiliar para la fase (numérica cuando se pueda)
    def fase_key(fv: str):
        try:
            return (0, int(fv))
        except Exception:
            return (1, fv.lower())

    # Componer en Markdown
    parts = []
    for fase in sorted(per_fase.keys(), key=fase_key):
        items = per_fase[fase]
        # ordenar por planeta, luego operación, luego personaje
        items.sort(key=lambda x: (x[0].lower(), x[1].lower(), x[2].lower()))
        parts.append(f"**Fase {fase}** ({len(items)})")
        for planeta, oper, personaje, req in items:
            # `req` en monoespaciado para que destaque y no rompa Markdown
            parts.append(f"• {planeta} / {oper} — *{personaje}* (`{req}`)")
        parts.append("")  # línea en blanco
    return "\n".join(parts).strip()

def list_phases_in_rote(ss, rote_sheet: str):
    """
    Devuelve la lista de fases distintas presentes en la hoja ROTE,
    ordenadas (numérico si aplica, luego alfabético), EXCLUYENDO 'x'.
    """
    ws = ss.worksheet(rote_sheet)
    headers, rows = _get_all(ws)
    if not rows:
        return []
    hl = [h.lower() for h in headers]
    if "fase" not in hl:
        return []
    i_fase = hl.index("fase")

    phases = set()
    for r in rows:
        fv = (r[i_fase] if i_fase < len(r) else "").strip()
        if not fv:
            continue
        # excluir fase fantasma 'x' (case-insensitive)
        if fv.strip().lower() == "x":
            continue
        phases.add(fv.strip())

    def _key(x):
        try:
            return (0, int(x))
        except Exception:
            return (1, x.lower())

    return sorted(phases, key=_key)


def render_ops_for_alias_phase_grouped(ss, rote_sheet: str, alias: str, phase: str) -> str:
    """
    Render de asignaciones para un alias y una fase concreta, agrupadas por PLANETA.
    Formato:
      {Planeta}
      - {Personaje} ({Operacion})
      ...
    """
    ws = ss.worksheet(rote_sheet)
    headers, rows = _get_all(ws)
    if not rows:
        return "No tienes asignaciones en esta fase."

    hl = [h.lower() for h in headers]
    need = ["fase", "planeta", "operacion", "personaje", "jugador"]
    for n in need:
        if n not in hl:
            return "No tienes asignaciones en esta fase."

    i_fase = hl.index("fase")
    i_plan = hl.index("planeta")
    i_op   = hl.index("operacion")
    i_char = hl.index("personaje")
    i_jug  = hl.index("jugador")

    alias_norm = (alias or "").strip().lower()
    phase_str = str(phase).strip()

    groups = {}  # planeta -> list[(personaje, operacion)]
    for r in rows:
        jugador = (r[i_jug] if i_jug < len(r) else "").strip().lower()
        if jugador != alias_norm:
            continue
        fase_val = (r[i_fase] if i_fase < len(r) else "").strip()
        if fase_val != phase_str:
            continue
        planeta = (r[i_plan] if i_plan < len(r) else "").strip() or "—"
        personaje = (r[i_char] if i_char < len(r) else "").strip()
        oper = (r[i_op] if i_op < len(r) else "").strip()
        groups.setdefault(planeta, []).append((personaje, oper))

    if not groups:
        return "No tienes asignaciones en esta fase."

    parts = []
    for planeta in sorted(groups.keys(), key=lambda s: s.lower()):
        parts.append(f"{planeta}")
        # Ordena por operación y personaje para estabilidad
        for personaje, oper in sorted(groups[planeta], key=lambda t: (t[1].lower(), t[0].lower())):
            parts.append(f"- {personaje} ({oper})")
        parts.append("")  # línea en blanco entre planetas

    return "\n".join(parts).strip()

# ---------- Asignaciones jugador ----------
def user_has_leadership_role(ss, user_id: int, guild_name: str) -> bool:
    ws = ss.worksheet(USERS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    try:
        i_uid = hl.index("user_id")
        i_gn = hl.index("guild_name")
        i_rol = hl.index("rol")
    except ValueError:
        return False

    for r in rows:
        if str(r[i_uid] if i_uid < len(r) else "").strip() == str(user_id):
            gn = (r[i_gn] if i_gn < len(r) else "").strip()
            if gn == guild_name:
                rol = (r[i_rol] if i_rol < len(r) else "").strip().lower()
                return rol in ["oficial", "lider", "líder"]
    return False

def list_players_for_guild(ss, guild_name: str) -> List[Tuple[str, str]]:
    ws = ss.worksheet(PLAYERS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    try:
        i_gn = hl.index("guild name")
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

# ---------------------------------------------------------------------------
# Guilds — reset time
# ---------------------------------------------------------------------------
 
def get_guild_reset_time(ss, guild_id: str) -> Optional[str]:
    """
    Returns the ticket reset time (HH:MM, 24h, Madrid TZ) for the given
    guild_id, or None if the column/row is missing.
 
    Reads from the 'reset_time' column in GUILDS_SHEET.
    """
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
    """
    Returns [(guild_id, guild_name, reset_time_str), ...] for all guilds
    that have a non-empty reset_time column.
    Used by the snapshot scheduler to know which guilds to snapshot and when.
    """
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
    """
    Returns the announcements channel ID for the given guild_name.
    Prefer get_channel_config_for_guild() when you also need the thread_id.
    """
    channel_id, _ = get_channel_config_for_guild(ss, guild_name)
    return channel_id
 
 
def get_channel_config_for_guild(
    ss, guild_name: str
) -> tuple[Optional[str], Optional[int]]:
    """
    Returns (channel_id, thread_id) for the given guild_name in a single
    sheet read.
 
    - channel_id: value of 'announcements_channel' column, or None if absent/empty.
    - thread_id:  integer value of 'announcements_thread_id' column, or None if
                  the column is missing, empty, or not a valid integer.
                  Pass as message_thread_id to post in a specific forum topic.
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
                    pass  # malformed value — treat as no thread
 
        return channel_id, thread_id
 
    return None, None
 
 
def get_usernames_for_members(
    ss, guild_name: str, player_names: list[str]
) -> Dict[str, Optional[str]]:
    """
    Returns {player_name_lower: username_or_None} for members of guild_name.
 
    'username' is the Telegram @handle without the '@' prefix, or None if
    the member has no username registered. Caller decides the fallback.
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
            # Strip leading '@' if present; store bare handle or None
            username = raw.lstrip("@") if raw else None
        result[alias.lower()] = username
 
    return result
 
 
# ---------------------------------------------------------------------------
# Ticket Snapshots — read / write
# ---------------------------------------------------------------------------
#
# Schema (one row per member per guild):
#   guild_name | player_name | last_snapshot_date | lifetime_d | lifetime_d1
#
# lifetime_d  = lifetimeValue taken at today's reset (18:30)
# lifetime_d1 = lifetimeValue taken at yesterday's reset (promoted from lifetime_d)
#
# "Yesterday missed" query: lifetime_d - lifetime_d1 < 600
 
 
SNAPSHOT_COLS = ["guild_name", "player_name", "last_snapshot_date", "lifetime_d", "lifetime_d1"]
 
 
def _ensure_snapshot_headers(ws) -> Dict[str, int]:
    """
    Ensures Ticket_Snapshots has the required columns.
    Returns a header→0-based-index map.
    Adds missing columns non-destructively (never removes existing ones).
    """
    headers = ws.row_values(1) or []
    low = [h.strip().lower() for h in headers]
 
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
    Called at reset time (18:30). For every member of guild_name:
      1. Promotes current lifetime_d -> lifetime_d1
      2. Writes fresh lifetimeValue -> lifetime_d
      3. Updates last_snapshot_date to today
 
    Members not yet in the sheet are inserted.
    Members in the sheet but absent from snapshots (left the guild) are removed.
    Rows from other guilds are preserved untouched.
    """
    from .. import config as bot_cfg
    ws = ss.worksheet(bot_cfg.TICKET_SNAPSHOTS_SHEET)
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
 
    # Keep other guilds exactly as-is
    other_rows = [
        r for r in existing
        if (r[i_gn] if i_gn < len(r) else "").strip() != guild_name
    ]
 
    # Build existing lookup for THIS guild: player_name_lower -> row
    this_guild: Dict[str, list[str]] = {}
    for r in existing:
        if (r[i_gn] if i_gn < len(r) else "").strip() != guild_name:
            continue
        pn = (r[i_pn] if i_pn < len(r) else "").strip()
        if pn:
            this_guild[pn.lower()] = r
 
    new_rows: list[list[str]] = []
    for player_name, fresh_lifetime in snapshots.items():
        row = [""] * n_cols
        row[i_gn]   = guild_name
        row[i_pn]   = player_name
        row[i_date] = today_str
 
        existing_row = this_guild.get(player_name.lower())
        if existing_row is not None:
            # Promote today -> yesterday before overwriting
            old_d = (existing_row[i_ld] if i_ld < len(existing_row) else "").strip()
            row[i_ld1] = old_d if old_d else str(fresh_lifetime)
        else:
            # New member: both columns start at the same value
            # (they can't have "missed yesterday" on their first snapshot)
            row[i_ld1] = str(fresh_lifetime)
 
        row[i_ld] = str(fresh_lifetime)
        new_rows.append(row)
 
    new_rows.sort(key=lambda r: r[i_pn].lower())
 
    final_rows = other_rows + new_rows
    hdr_row = [""] * n_cols
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
    Reads the latest snapshot for guild_name.
 
    Returns:
        (last_snapshot_date, lifetime_d, lifetime_d1)
        where lifetime_d and lifetime_d1 are {player_name_lower: int}
        or None if no snapshot exists for this guild.
    """
    from .. import config as bot_cfg
    try:
        ws = ss.worksheet(bot_cfg.TICKET_SNAPSHOTS_SHEET)
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
    """
    Returns True if last_snapshot_date for guild_name equals today (Madrid TZ).
    Used by the catch-up logic on bot startup.
    """
    from zoneinfo import ZoneInfo
    from datetime import datetime
    from .. import config as bot_cfg
 
    today_str = datetime.now(ZoneInfo("Europe/Madrid")).date().isoformat()
 
    try:
        ws = ss.worksheet(bot_cfg.TICKET_SNAPSHOTS_SHEET)
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
