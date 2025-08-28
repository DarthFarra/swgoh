# src/swgoh/processing/sync_guilds.py

from typing import Any, Dict, List, Tuple, Optional, Set
import json

from ..config import (
    SHEET_GUILDS,
    SHEET_PLAYERS,
    SHEET_PLAYER_UNITS,
    SHEET_CHARACTERS,
    SHEET_SHIPS,
    EXCLUDE_BASEID_CONTAINS,
)
from ..sheets import open_or_create, try_get_worksheet, write_sheet
from ..comlink import fetch_metadata, fetch_data_items, fetch_guild, fetch_player


# ---------------------------
# Helpers: lectura de nombres amigables desde Sheets
# ---------------------------

def _read_friendly_map() -> Dict[str, str]:
    """
    Lee las hojas Characters y Ships para construir baseId -> friendlyName.
    (No usamos /localization en este script.)
    """
    def read(ws) -> Dict[str, str]:
        mapping = {}
        if not ws:
            return mapping
        vals = ws.get_all_values() or []
        if not vals:
            return mapping
        headers = [h.strip().lower() for h in (vals[0] or [])]

        def find(cands):
            for i, h in enumerate(headers):
                for c in cands:
                    if c in h:
                        return i
            return None

        bi = find(["base_id", "baseid", "base id", "unit id", "unit_base_id"]) or 0
        ni = find(["name", "friendly", "display", "ui name", "uiname"]) or 1

        for row in vals[1:]:
            if not row:
                continue
            bid = (row[bi] if bi < len(row) else "").strip().upper()
            name = (row[ni] if ni < len(row) else "").strip()
            if bid and name:
                mapping[bid] = name
        return mapping

    return {
        **read(try_get_worksheet(SHEET_CHARACTERS)),
        **read(try_get_worksheet(SHEET_SHIPS)),
    }


# ---------------------------
# Helpers: GAC League unificada
# ---------------------------

def _gac_league_str(p: Dict[str, Any]) -> str:
    prs = (
        p.get("playerRating", {}).get("playerRankStatus")
        or p.get("payload", {}).get("playerRating", {}).get("playerRankStatus")
        or {}
    )
    league_id = prs.get("leagueId") or prs.get("league") or prs.get("leagueName")
    div_id = prs.get("divisionId") or prs.get("division") or prs.get("divisionNumber")
    div_map = {25: 1, 20: 2, 15: 3, 10: 4, 5: 5}
    try:
        div_num = div_map.get(int(div_id)) if div_id is not None else None
    except Exception:
        div_num = None

    if league_id and div_num:
        return f"{str(league_id).upper()} {div_num}"
    return str(league_id).upper() if league_id else ""


# ---------------------------
# Helpers: indexar roster y formato de reliquia
# ---------------------------

def _build_roster_index(p: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Devuelve un índice baseId(UPPER)->rosterUnit a partir de p['rosterUnit'].
    Usa definitionId y corta en ':' (ej: BARRISSOFFEE:SEVEN_STAR → BARRISSOFFEE).
    """
    roster = p.get("rosterUnit") or p.get("payload", {}).get("rosterUnit") or []
    by_unit: Dict[str, Dict[str, Any]] = {}
    for ru in roster:
        definition_id = ru.get("definitionId") or ""
        if definition_id:
            base = str(definition_id).split(":", 1)[0]
            key = base.upper().strip()
        else:
            key = str(ru.get("defId") or ru.get("baseId") or ru.get("id") or "").upper().strip()
        if key:
            by_unit[key] = ru
    return by_unit


def _format_relic_cell(ru: Dict[str, Any]) -> str:
    raw = (ru.get("relic") or {}).get("currentTier") or ru.get("currentRelicTier") or ru.get("relicTier") or (ru.get("relic") or {}).get("tier")
    try:
        val = int(raw)
    except Exception:
        val = None
    if val is None:
        return ""
    table = {11: "R9", 10: "R8", 9: "R7", 8: "R6", 7: "R5", 6: "R4", 5: "R3", 4: "R2", 3: "R1", 2: "R0", 1: "G12", 0: "<G12"}
    return table.get(val, str(val))


# ---------------------------
# Helpers: construir cabeceras de Player_Units con filtro por baseId
# ---------------------------

def _collect_unit_headers(units_arr: List[Dict[str, Any]], friendly_map: Dict[str, str]) -> Tuple[List[str], Set[str], List[str]]:
    """
    Aplica EXCLUDE_BASEID_CONTAINS sobre baseId (en mayúsculas) ANTES de deduplicar,
    separa naves (combatType==2) y genera headers con friendly name desde Sheets.
    Devuelve: (unit_ids_filtrados_y_ordenados, ship_ids, headers_units)
    """
    excl = {s.upper() for s in (EXCLUDE_BASEID_CONTAINS or [])}

    all_ids: List[str] = []
    ship_ids: Set[str] = set()

    for u in units_arr:
        bid = (u.get("baseId") or u.get("id") or "").strip().upper()
        if not bid:
            continue
        if excl and any(x in bid for x in excl):
            continue
        all_ids.append(bid)

        ctype = u.get("combatType") or u.get("combat_type")
        try:
            ctype = int(ctype)
        except Exception:
            ctype = None
        if ctype == 2:
            ship_ids.add(bid)

    # dedup + orden estable
    unit_ids = sorted(set(all_ids))

    # headers amigables (desambiguando duplicados)
    headers_units: List[str] = []
    seen_names: Set[str] = set()
    for b in unit_ids:
        name = str(friendly_map.get(b) or b)
        if name in seen_names:
            name = f"{name} ({b})"
        seen_names.add(name)
        headers_units.append(name)

    print(f"[INFO] Player_Units columnas: {len(unit_ids)} (ships: {len(ship_ids)})")
    return unit_ids, ship_ids, headers_units


# ---------------------------
# Proceso principal
# ---------------------------

def run(guild_ids: Optional[List[str]] = None) -> Dict[str, int]:
    # 1) Metadata + units para columnas de Player_Units
    meta = fetch_metadata()
    version = (
        meta.get("latestGamedataVersion")
        or meta.get("payload", {}).get("latestGamedataVersion")
        or meta.get("data", {}).get("latestGamedataVersion")
    )
    if not version:
        raise SystemExit("No latestGamedataVersion")

    du = fetch_data_items(version, "units")
    units_arr = du.get("units") or du.get("payload", {}).get("units") or []

    # friendly names desde Sheets
    friendly_map = _read_friendly_map()

    # Construir columnas de Player_Units con filtro por baseId
    unit_ids, ship_ids, headers_units = _collect_unit_headers(units_arr, friendly_map)

    # 2) Preparar hojas
    ws_guilds = open_or_create(SHEET_GUILDS)
    ws_players = open_or_create(SHEET_PLAYERS)
    ws_units = open_or_create(SHEET_PLAYER_UNITS)

    # guild ids de la hoja si no vienen por arg (columna A)
    gids = guild_ids or []
    if not gids:
        vals = ws_guilds.get("A2:A") or []
        gids = [r[0].strip() for r in vals if r]
    if not gids:
        raise SystemExit("No hay Guild IDs en hoja Guilds ni por parámetro")

    guilds_rows: List[List[Any]] = []
    players_rows: List[List[Any]] = []
    units_rows: List[List[Any]] = []

    # 3) Por cada guild
    for gid in gids:
        # Nota: en comlink.fetch_guild ya añadimos includeRecentGuildActivityInfo=true
        g = fetch_guild(gid)
        gg = g.get("guild") or g.get("payload", {}).get("guild") or g
        profile = gg.get("profile", {})
        members = gg.get("members") or gg.get("member") or g.get("members") or []

        gname = profile.get("name", "") or gg.get("guildName", "")
        mcount = profile.get("memberCount") or len(members)
        ggp = profile.get("guildGalacticPower") or profile.get("galacticPower") or 0
        try:
            ggp = int(ggp)
        except Exception:
            ggp = 0

        # Last Raid desde lastRaidPointsSummary
        last_raid_id, last_raid_points = "", ""
        lrs = gg.get("lastRaidPointsSummary") or profile.get("lastRaidPointsSummary")
        if isinstance(lrs, list) and lrs:
            first = lrs[0] or {}
            last_raid_id = json.dumps(first.get("identifier", {}), ensure_ascii=False)
            pts = first.get("totalPoints")
            if isinstance(pts, (int, float)):
                last_raid_points = str(int(pts))
            elif pts not in (None, ""):
                last_raid_points = str(pts)

        # Miembros
        for m in members:
            pid = str(m.get("playerId") or m.get("playerID") or m.get("id") or "").strip()
            ally = m.get("allyCode") or m.get("allycode") or m.get("ally")
            ally = str(ally) if ally not in (None, "") else ""

            # /player con UNA sola clave (playerId si existe; si no, allyCode)
            try:
                p = fetch_player(pid if pid else None, None if pid else ally)
            except Exception as e:
                print(f"[WARN] fallo /player pid={pid} ally={ally}: {e}")
                p = {}

            pname = (m.get("playerName") or m.get("name") or p.get("name") or "").strip() or (ally or pid or "")
            role_val = 2
            try:
                role_val = int(m.get("memberLevel") or 2)
            except Exception:
                role_val = 2
            role = "Leader" if role_val == 4 else ("Officer" if role_val == 3 else "Member")

            # GAC League
            gac = _gac_league_str(p)

            # GP
            gp = m.get("galacticPower") or m.get("gp") or p.get("galacticPower") or p.get("statistics", {}).get("galacticPower") or 0
            try:
                gp = int(gp)
            except Exception:
                gp = 0

            # Índice de roster por baseId (UPPER)
            by_unit = _build_roster_index(p)

            # Fila de unidades: [Guild, Player, ...units filtradas y ordenadas...]
            row_u = [gname, pname]
            for b in unit_ids:  # usar EXACTAMENTE el mismo orden de columnas
                ru = by_unit.get(b)
                if not ru:
                    row_u.append("")
                else:
                    row_u.append("Nave" if b in ship_ids else _format_relic_cell(ru))
            units_rows.append(row_u)

            # Fila de players
            players_rows.append([gname, pname, pid, ally, str(gp), role, gac])

        # Fila de guilds
        guilds_rows.append(
            [gid, gname, str(mcount), str(ggp), last_raid_id, last_raid_points]
        )

    # 4) Escribir hojas
    write_sheet(ws_guilds, ["Guild Id", "Guild Name", "Number of members", "Guild GP", "Last Raid Id", "Last Raid Score"], guilds_rows)
    write_sheet(ws_players, ["Guild Name", "Player Name", "Player Id", "Ally code", "GP", "Role", "GAC League"], players_rows)
    write_sheet(ws_units, ["Player Guild", "Player Name", *headers_units], units_rows)

    return {
        "guilds": len(guilds_rows),
        "players": len(players_rows),
        "units_rows": len(units_rows),
    }


if __name__ == "__main__":
    print(run())
