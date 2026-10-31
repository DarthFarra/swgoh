from __future__ import annotations
import logging
from typing import List, Tuple
from .sheets import _get_all, map_guild_name_to_label_id_rote
from .sheets_io import read_values_cached
from ..config import USERS_SHEET, GUILDS_SHEET
from ...comlink import fetch_guild

log = logging.getLogger(__name__)

def _normalise_allycode(value: object) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _normalise_name(value: object) -> str:
    return " ".join(str(value or "").casefold().split())


def _current_member_level(
    guild_id: str, allycodes: set[str], aliases: set[str]
) -> int | None:
    """Fetch the current in-game member level for one of the user's players."""
    data = fetch_guild(guild_id)
    members = data.get("guild", {}).get("member", []) if isinstance(data, dict) else []
    for member in members or []:
        if not isinstance(member, dict):
            continue
        member_allycode = _normalise_allycode(
            member.get("allyCode") or member.get("allycode")
        )
        member_alias = _normalise_name(member.get("playerName"))
        if member_allycode in allycodes or member_alias in aliases:
            try:
                return int(member.get("memberLevel", 0))
            except (TypeError, ValueError):
                return None
    return None


def _user_allycodes(ss, *, user_id: int | None = None, discord_id: int | None = None,
                    guild_name: str | None = None) -> set[str]:
    headers, rows = read_values_cached(ss, USERS_SHEET)
    hl = [header.strip().lower() for header in headers]
    identity_column = "user_id" if user_id is not None else "discord_id"
    if identity_column not in hl or "allycode" not in hl:
        return set()
    identity_index = hl.index(identity_column)
    allycode_index = hl.index("allycode")
    guild_index = hl.index("guild_name") if "guild_name" in hl else None
    return {
        _normalise_allycode(row[allycode_index])
        for row in rows
        if identity_index < len(row)
        and str(row[identity_index]).strip() == str(user_id if user_id is not None else discord_id)
        and (guild_name is None or (
            guild_index is not None
            and guild_index < len(row)
            and str(row[guild_index]).strip() == guild_name
        ))
        and allycode_index < len(row)
        and _normalise_allycode(row[allycode_index])
    }


def _user_aliases(ss, *, user_id: int | None = None, discord_id: int | None = None,
                  guild_name: str | None = None) -> set[str]:
    headers, rows = read_values_cached(ss, USERS_SHEET)
    hl = [header.strip().lower() for header in headers]
    identity_column = "user_id" if user_id is not None else "discord_id"
    if identity_column not in hl or "alias" not in hl:
        return set()
    identity_index = hl.index(identity_column)
    alias_index = hl.index("alias")
    guild_index = hl.index("guild_name") if "guild_name" in hl else None
    return {
        _normalise_name(row[alias_index])
        for row in rows
        if identity_index < len(row)
        and str(row[identity_index]).strip() == str(user_id if user_id is not None else discord_id)
        and (guild_name is None or (
            guild_index is not None
            and guild_index < len(row)
            and str(row[guild_index]).strip() == guild_name
        ))
        and alias_index < len(row)
        and _normalise_name(row[alias_index])
    }


def _guild_name_by_id(ss, guild_id: str) -> str | None:
    for name, (_label, configured_id, _rote) in map_guild_name_to_label_id_rote(ss).items():
        if configured_id == guild_id:
            return name
    return None


def _has_current_role(ss, guild_id: str, *, user_id: int | None = None,
                      discord_id: int | None = None, minimum_level: int = 3) -> bool:
    guild_name = _guild_name_by_id(ss, guild_id)
    if not guild_name:
        return False
    allycodes = _user_allycodes(
        ss, user_id=user_id, discord_id=discord_id, guild_name=guild_name
    )
    aliases = _user_aliases(
        ss, user_id=user_id, discord_id=discord_id, guild_name=guild_name
    )
    if not allycodes and not aliases:
        return False
    try:
        level = _current_member_level(guild_id, allycodes, aliases)
    except Exception:
        log.exception("Could not verify current Comlink role for guild %s", guild_id)
        return False
    return level is not None and level >= minimum_level

def user_authorized_guilds(ss, user_id: int) -> List[Tuple[str, str]]:
    """
    Retorna (label, guild_id) donde el user_id tiene Rol {Lider,Oficial}.
    """
    uh, ur = read_values_cached(ss, USERS_SHEET)
    ul = [h.lower() for h in uh]
    i_uid = ul.index("user_id") if "user_id" in ul else None
    i_gn  = ul.index("guild_name") if "guild_name" in ul else None
    if i_uid is None or i_gn is None:
        return []

    guild_names = set()
    for r in ur:
        try:
            if i_uid < len(r) and str(r[i_uid]).strip() == str(user_id):
                gname = (r[i_gn] if i_gn < len(r) else "").strip()
                if gname:
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
            if _has_current_role(ss, gid, user_id=user_id):
                out.append((label, gid))
    return out

def user_has_role_in_guild(ss, user_id: int, guild_id: str) -> bool:
    """
    Comprueba que el user tenga Rol {Lider,Oficial} en el guild_id dado.
    """
    gh, gr = read_values_cached(ss, GUILDS_SHEET)
    
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

    return _has_current_role(ss, guild_id, user_id=user_id)
