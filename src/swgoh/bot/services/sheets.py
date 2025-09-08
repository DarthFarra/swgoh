from __future__ import annotations
from typing import List, Tuple, Dict, Optional
import gspread
from google.oauth2.service_account import Credentials
from ..config import (
    SPREADSHEET_ID, load_service_account_info, USERS_SHEET, GUILDS_SHEET, PLAYERS_SHEET,
    DEFAULT_ROTE_SHEET, TZ
)
from datetime import datetime

# ---------- Conexión ----------
def open_ss():
    info = load_service_account_info()
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID")
    return gc.open_by_key(SPREADSHEET_ID)

def _get_all(ws) -> Tuple[List[str], List[List[str]]]:
    vals = ws.get_all_values() or []
    if not vals:
        return [], []
    headers = [h.strip() for h in vals[0]]
    rows = vals[1:] if len(vals) > 1 else []
    return headers, rows

# ---------- Guilds ----------
def map_guild_name_to_label_id_rote(ss) -> Dict[str, Tuple[str, str, str]]:
    """
    Guild Name -> (label, guild_id, rote_sheet_name)
    label = 'nombre abreviado' si existe, si no 'Guild Name'
    """
    ws = ss.worksheet(GUILDS_SHEET)
    headers, rows = _get_all(ws)
    hl = [h.lower() for h in headers]
    try:
        i_name = hl.index("guild name")
        i_id = hl.index("guild id")
    except ValueError:
        return {}
    i_abbr = hl.index("nombre abreviado") if "nombre abreviado" in hl else None
    i_rote = hl.index("rote") if "rote" in hl else None

    out = {}
    for r in rows:
        gname = (r[i_name] if i_name < len(r) else "").strip()
        gid   = (r[i_id] if i_id < len(r) else "").strip()
        abbr  = (r[i_abbr] if (i_abbr is not None and i_abbr < len(r)) else "").strip() if i_abbr is not None else ""
        rote  = (r[i_rote] if (i_rote is not None and i_rote < len(r)) else "").strip() if i_rote is not None else ""
        if gname and gid:
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
    try:
        i_id   = hl.index("guild id")
        i_last = hl.index("last update")
    except ValueError:
        return False

    today = datetime.now(TZ).date().isoformat()
    for r in rows:
        gid = (r[i_id] if i_id < len(r) else "").strip()
        if gid != guild_id:
            continue
        last = (r[i_last] if i_last < len(r) else "").strip()
        if not last:
            return False
        if last[:10] == today:
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
    """
    Gremios (label, guild_id, guild_name) donde está el user_id (cualquier rol).
    """
    ws_u = ss.worksheet(USERS_SHEET)
    uh, ur = _get_all(ws_u)
    ul = [h.lower() for h in uh]
    i_uid = ul.index("user_id") if "user_id" in ul else None
    i_gn  = ul.index("guild_name") if "guild_name" in ul else None
    if i_uid is None or i_gn is None:
        return []

    gmap = map_guild_name_to_label_id_rote(ss)
    out = []
    for r in ur:
        uid_ok = (i_uid < len(r) and str(r[i_uid]).strip() == str(user_id))
        if not uid_ok:
            continue
        gname = (r[i_gn] if i_gn < len(r) else "").strip()
        if gname and gname in gmap:
            label, gid, _ = gmap[gname]
            out.append((label, gid, gname))
    # quitar duplicados por guild_id
    seen, unique = set(), []
    for item in out:
        gid = item[1]
        if gid in seen: continue
        seen.add(gid); unique.append(item)
    return unique

def upsert_usuario(ss, info: dict, tg_username: str, user_id: int, chat_id: int):
    ws = ss.worksheet(USERS_SHEET)
    hdr_map = ensure_usuarios_headers(ws)  # 0-based
    vals = ws.get_all_values() or []
    rows = vals[1:] if len(vals) > 1 else []

    i_alias = hdr_map["alias"]; i_user = hdr_map["username"]; i_uid = hdr_map["user_id"]
    i_chat = hdr_map["chat_id"]; i_rol = hdr_map["rol"]; i_ac = hdr_map["allycode"]; i_gn = hdr_map["guild_name"]

    alias = info.get("alias","")
    gname = info.get("guild_name","")
    role  = info.get("role","")
    ally  = info.get("allycode","")

    # Buscar fila por (guild_name + alias)
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

# ---------- Asignaciones (render simple) ----------
def render_assignments_for_alias(ss, rote_sheet: str, alias: str) -> str:
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
    per_fase: Dict[str, List[str]] = {}
    for r in rows:
        jugador = (r[i_jug] if i_jug < len(r) else "").strip()
        if (jugador or "").strip().lower() != alias_norm:
            continue
        fase = (r[i_fase] if i_fase < len(r) else "").strip()
        planeta = (r[i_plan] if i_plan < len(r) else "").strip()
        oper = (r[i_op] if i_op < len(r) else "").strip()
        personaje = (r[i_char] if i_char < len(r) else "").strip()
        reliq = (r[i_rel] if i_rel < len(r) else "").strip()
        line = f"• {planeta} / {oper} — {personaje} (req: {reliq})"
        per_fase.setdefault(fase or "—", []).append(line)

    if not per_fase:
        return "No tienes asignaciones."

    def fase_key(fv: str):
        try: return (0, int(fv))
        except Exception: return (1, fv.lower())

    parts = []
    for fase in sorted(per_fase.keys(), key=fase_key):
        parts.append(f"*Fase {fase}*")
        parts.extend(per_fase[fase])
        parts.append("")
    return "\n".join(parts).strip()
