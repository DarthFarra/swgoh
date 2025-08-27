from typing import Any, Dict, List, Tuple, Optional
import json
from ..config import (
    SHEET_GUILDS, SHEET_PLAYERS, SHEET_PLAYER_UNITS, SHEET_PLAYER_SKILLS,
    SHEET_CHARACTERS, SHEET_SHIPS, EXCLUDE_BASEID_CONTAINS
)
from ..sheets import open_or_create, try_get_worksheet, write_sheet
from ..comlink import fetch_metadata, fetch_data_items, fetch_guild, fetch_player, fetch_events

# Buckets (placeholder sencillo)
OMICRON_BUCKETS = {"TW": {8}, "TB": {7}, "RAID": {4}, "CONQ": {11}, "CHAL": {12}, "GAC": {9, 14, 15}}

def _read_friendly_map() -> Dict[str, str]:
    def read(ws) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
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
        bi = find(["baseid", "base id", "base_id", "unit id", "unit_base_id"]) or 0
        ni = find(["friendly", "name", "display", "ui name", "uiname"]) or 1
        for row in vals[1:]:
            if bi < len(row) and ni < len(row):
                bid = (row[bi] or "").strip().upper()
                nm = (row[ni] or "").strip()
                if bid and nm:
                    mapping[bid] = nm
        return mapping
    return {**read(try_get_worksheet(SHEET_CHARACTERS)), **read(try_get_worksheet(SHEET_SHIPS))}

def _gac_league_str(p: Dict[str, Any]) -> str:
    prs = p.get("playerRating", {}).get("playerRankStatus") or p.get("payload", {}).get("playerRating", {}).get("playerRankStatus") or {}
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

def _build_roster_indices(p: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    roster = p.get("rosterUnit") or p.get("payload", {}).get("rosterUnit") or p.get("roster") or []
    by_unit: Dict[str, Any] = {}
    by_skill: Dict[str, Any] = {}
    for ru in roster:
        definition_id = ru.get("definitionId")
        if definition_id:
            base = str(definition_id).split(":", 1)[0]
            key = base.upper().strip()
        else:
            key = str(ru.get("defId") or ru.get("baseId") or ru.get("id") or "").upper().strip()
        if key:
            by_unit[key] = ru
        for sk in (ru.get("skill") or ru.get("skills") or []):
            sid = sk.get("id") or sk.get("skillId")
            tier = sk.get("tier") or sk.get("tierIndex") or sk.get("currentTier")
            if sid:
                by_skill[str(sid)] = tier
    return by_unit, by_skill

def _format_unit_cell(ru: Dict[str, Any]) -> str:
    raw = (ru.get("relic") or {}).get("currentTier") or ru.get("currentRelicTier") or ru.get("relicTier") or (ru.get("relic") or {}).get("tier")
    try:
        val = int(raw)
    except Exception:
        val = None
    if val is None:
        return ""
    table = {11: "R9", 10: "R8", 9: "R7", 8: "R6", 7: "R5", 6: "R4", 5: "R3", 4: "R2", 3: "R1", 2: "R0", 1: "G12", 0: "<G12"}
    return table.get(val, str(val))

def run(guild_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    # Metadata/version
    meta = fetch_metadata()
    version = meta.get("latestGamedataVersion") or meta.get("payload", {}).get("latestGamedataVersion") or meta.get("data", {}).get("latestGamedataVersion")
    if not version:
        raise SystemExit("No latestGamedataVersion")

    # /data units & skills
    du = fetch_data_items(version, "units")
    ds = fetch_data_items(version, "skill")
    units_arr = du.get("units") or du.get("payload", {}).get("units") or []
    skills_arr = ds.get("skill") or ds.get("payload", {}).get("skill") or []

    # unit ids & friendly names (usando hojas Characters/Ships)
    friendly_map = _read_friendly_map()
    unit_ids: List[str] = []
    unit_name: Dict[str, str] = {}
    ship_ids: set[str] = set()

    for u in units_arr:
        bid = (u.get("baseId") or u.get("id") or "").upper()
        if not bid:
            continue
        unit_ids.append(bid)
        unit_name[bid] = friendly_map.get(bid) or u.get("uiName") or u.get("longName") or u.get("name") or u.get("localizedName") or bid
        ctype = u.get("combatType") or u.get("combat_type")
        try:
            ctype = int(ctype)
        except Exception:
            ctype = None
        if ctype == 2:
            ship_ids.add(bid)

    unit_ids = sorted(list(set(unit_ids)))
    if EXCLUDE_BASEID_CONTAINS:
        before = len(unit_ids)
        unit_ids = [b for b in unit_ids if all(s not in b for s in EXCLUDE_BASEID_CONTAINS)]
        ship_ids = {b for b in ship_ids if b in set(unit_ids)}
        print(f"[INFO] Excluidas {before - len(unit_ids)} unidades por EXCLUDE_BASEID_CONTAINS")

    # headers amigables (desambiguando duplicados)
    headers_units = []
    seen = set()
    for b in unit_ids:
        name = str(unit_name.get(b) or b)
        if name in seen:
            name = f"{name} ({b})"
        seen.add(name)
        headers_units.append(name)

    # preparar hojas
    ws_guilds  = open_or_create(SHEET_GUILDS)
    ws_players = open_or_create(SHEET_PLAYERS)
    ws_units   = open_or_create(SHEET_PLAYER_UNITS)
    ws_skills  = open_or_create(SHEET_PLAYER_SKILLS)

    # guild ids desde hoja si no vienen por arg
    gids = guild_ids or []
    if not gids:
        vals = ws_guilds.get("A2:A") or []
        gids = [r[0].strip() for r in vals if r]
    if not gids:
        raise SystemExit("No hay Guild IDs en hoja Guilds ni por parámetro")

    # skill ids para matriz
    skill_ids: List[str] = []
    for s in skills_arr:
        sid = s.get("id") or s.get("skillId")
        if sid is not None:
            skill_ids.append(str(sid))

    guilds_rows: List[List[Any]] = []
    players_rows: List[List[Any]] = []
    units_rows: List[List[Any]]   = []
    skills_rows: List[List[Any]]  = []

    # por cada guild
    for gid in gids:
        g = fetch_guild(gid)
        gg = g.get("guild") or g.get("payload", {}).get("guild") or g
        profile = gg.get("profile", {})
        members = gg.get("members") or gg.get("member") or g.get("members") or g.get("guild", {}).get("members") or []

        gname = profile.get("name", "") or gg.get("guildName", "")
        mcount = profile.get("memberCount") or len(members)
        ggp = profile.get("guildGalacticPower") or profile.get("galacticPower") or 0
        try:
            ggp = int(ggp)
        except Exception:
            ggp = 0

        # last raid desde /guild
        last_raid_id, last_raid_points = "", ""
        lrs = gg.get("lastRaidPointsSummary") or profile.get("lastRaidPointsSummary")
        if isinstance(lrs, list) and lrs:
            first = lrs[0] or {}
            last_raid_id = json.dumps(first.get("identifier", {}), ensure_ascii=False)
            pts = first.get("totalPoints")
            last_raid_points = str(int(pts)) if isinstance(pts, (int, float)) else (str(pts) if pts not in (None, "") else "")

        # fallback: raid por jugador desde /getEvents (para Players y posible suma guild)
        events = fetch_events()
        instances = events.get("instances") or events.get("payload", {}).get("instances") or []
        latest, latest_ts = None, -1
        for inst in instances:
            ts = inst.get("startTime") or inst.get("timestamp") or 0
            try:
                ts = int(ts)
            except Exception:
                ts = 0
            if ts >= latest_ts:
                latest_ts, latest = ts, inst
        raid_score_by_player: Dict[str, int] = {}
        if latest:
            for mp in latest.get("memberProgress", []) or []:
                pid_mp = str(mp.get("playerId") or mp.get("id") or "").strip()
                try:
                    sc = int(mp.get("score") or mp.get("points") or 0)
                except Exception:
                    sc = 0
                if pid_mp:
                    raid_score_by_player[pid_mp] = sc

        guild_tw_omicrons = 0
        guild_player_ids = set()

        # miembros
        for m in members:
            pid = str(m.get("playerId") or m.get("playerID") or m.get("id") or "").strip()
            ally = m.get("allyCode") or m.get("allycode") or m.get("ally")
            if pid:
                guild_player_ids.add(pid)

            p = {}
            try:
                p = fetch_player(pid, None if pid else ally)
            except Exception as e:
                print(f"[WARN] /player {pid or ally}: {e}")

            pname = (m.get("playerName") or m.get("name") or p.get("name") or "").strip() or str(ally or pid)
            role_val = int(m.get("memberLevel") or 2)
            role = "Leader" if role_val == 4 else ("Officer" if role_val == 3 else "Member")
            ally = str(ally) if ally not in (None, "") else ""

            gac = _gac_league_str(p)
            by_unit, by_skill = _build_roster_indices(p)

            # GP
            gp = m.get("galacticPower") or m.get("gp") or p.get("galacticPower") or p.get("statistics", {}).get("galacticPower") or 0
            try:
                gp = int(gp)
           
