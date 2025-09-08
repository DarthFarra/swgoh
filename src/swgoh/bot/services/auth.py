from __future__ import annotations
from typing import List, Tuple
from .sheets import _get_all, map_guild_name_to_label_id_rote
from ..config import USERS_SHEET, GUILDS_SHEET

def user_authorized_guilds(ss, user_id: int) -> List[Tuple[str, str]]:
    """
    Retorna (label, guild_id) donde el user_id tiene Rol {Lider,Oficial}.
    """
    ws_u = ss.worksheet(USERS_SHEET)
    uh, ur = _get_all(ws_u)
    ul = [h.lower() for h in uh]
    i_uid = ul.index("user_id") if "user_id" in ul else None
    i_gn  = ul.index("guild_name") if "guild_name" in ul else None
    i_role = ul.index("rol") if "rol" in ul else (ul.index("role") if "role" in ul else None)
    if i_uid is None or i_gn is None or i_role is None:
        return []

    allowed = {"lider","oficial"}
    guild_names = set()
    for r in ur:
        try:
            if i_uid < len(r) and str(r[i_uid]).strip() == str(user_id):
                gname = (r[i_gn] if i_gn < len(r) else "").strip()
                role  = (r[i_role] if i_role < len(r) else "").strip().lower()
                if gname and role in allowed:
                    guild_names.add(gname)
        except Exception:
            continue

    if not guild_names:
        return []

    gmap = map_guild_name_to_label_id_rote(ss)
    out = []
    for gname in guild_names:
        if gname in gmap:
            label, gid, _ = gmap[gname]
            out.append((label, gid))
    return out

def user_has_role_in_guild(ss, user_id: int, guild_id: str) -> bool:
    """
    Comprueba que el user tenga Rol {Lider,Oficial} en el guild_id dado.
    """
    ws_g = ss.worksheet(GUILDS_SHEET)
    gh, gr = _get_all(ws_g)
    gl = [h.lower() for h in gh]
    try:
        ig_id = gl.index("guild id"); ig_name = gl.index("guild name")
    except ValueError:
        return False

    gid_to_name = {}
    for r in gr:
        gid = (r[ig_id] if ig_id < len(r) else "").strip()
        gname = (r[ig_name] if ig_name < len(r) else "").strip()
        if gid and gname:
            gid_to_name[gid] = gname

    gname = gid_to_name.get(guild_id)
    if not gname:
        return False

    ws_u = ss.worksheet(USERS_SHEET)
    uh, ur = _get_all(ws_u)
    ul = [h.lower() for h in uh]
    i_uid = ul.index("user_id") if "user_id" in ul else None
    i_gn  = ul.index("guild_name") if "guild_name" in ul else None
    i_role = ul.index("rol") if "rol" in ul else (ul.index("role") if "role" in ul else None)
    if i_uid is None or i_gn is None or i_role is None:
        return False

    allowed = {"lider","oficial"}
    for r in ur:
        try:
            if i_uid < len(r) and str(r[i_uid]).strip() == str(user_id):
                gn = (r[i_gn] if i_gn < len(r) else "").strip()
                role = (r[i_role] if i_role < len(r) else "").strip().lower()
                if gn == gname and role in allowed:
                    return True
        except Exception:
            continue
    return False
