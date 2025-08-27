from typing import Any, Dict, List, Set, Tuple
from ..sheets import open_or_create, write_sheet
from ..comlink import fetch_metadata, fetch_data_items
from .. import config as cfg


def _as_int(value, default: int | None = None) -> int | None:
    try:
        return int(value)
    except Exception:
        return default


def _unit_name(u: Dict[str, Any], fallback: str) -> str:
    # Sin /localization: intenta varios campos del /data units
    return str(
        u.get("uiName")
        or u.get("longName")
        or u.get("name")
        or u.get("localizedName")
        or fallback
    )


def _collect_unit_skill_ids(u: Dict[str, Any]) -> List[str]:
    """
    Extrae la lista de skillIds referenciadas por la unidad desde distintos campos posibles.
    """
    out: List[str] = []

    # 1) skillReferenceList: [{ skillId, tierMax, ...}, ...]
    for ref in (u.get("skillReferenceList") or []):
        sid = ref.get("skillId") or ref.get("id")
        if sid:
            out.append(str(sid))

    # 2) skillList: ["skchr_blah", ...]
    for sid in (u.get("skillList") or []):
        out.append(str(sid))

    # 3) unitSkillIds / skills
    for sid in (u.get("unitSkillIds") or u.get("skills") or []):
        if isinstance(sid, dict):
            sid = sid.get("id") or sid.get("skillId")
        if sid:
            out.append(str(sid))

    return out


def _skill_has_zeta(s: Dict[str, Any]) -> bool:
    """
    Heurística robusta para detectar zeta en un skill.
    """
    if s.get("isZeta") is True:
        return True
    if _as_int(s.get("zetaTier")) is not None:
        return True

    tiers = s.get("tier") or s.get("tiers") or []
    for t in tiers:
        if t.get("isZeta") is True:
            return True
        # algunos schemas usan flags tipo 'zeta' o nombre de tier
        if str(t.get("name") or "").lower().find("zeta") >= 0:
            return True
    return False


def _skill_has_omicron(s: Dict[str, Any]) -> Tuple[bool, int | None]:
    """
    Detecta omicron y devuelve (tiene_omicron, omicronMode:int|None).
    """
    tiers = s.get("tier") or s.get("tiers") or []
    for t in tiers:
        if t.get("isOmicron") is True or (t.get("omicronMode") is not None):
            return True, _as_int(t.get("omicronMode"))
    # algunos schemas ponen el modo a nivel de skill
    if s.get("isOmicron") is True or (s.get("omicronMode") is not None):
        return True, _as_int(s.get("omicronMode"))
    return False, None


def run() -> Dict[str, int]:
    # 0) Config: nombres de pestañas (las dos extra con fallback a literales)
    SHEET_CHARACTERS = getattr(cfg, "SHEET_CHARACTERS", "Characters")
    SHEET_SHIPS = getattr(cfg, "SHEET_SHIPS", "Ships")
    SHEET_CHARACTERS_ZETAS = getattr(cfg, "SHEET_CHARACTERS_ZETAS", "CharactersZetas")
    SHEET_CHARACTERS_OMICRONS = getattr(cfg, "SHEET_CHARACTERS_OMICRONS", "CharactersOmicrons")
    EXCLUDE = set(getattr(cfg, "EXCLUDE_BASEID_CONTAINS", []))

    # 1) Obtener versión para /data
    meta = fetch_metadata()
    version = (
        meta.get("latestGamedataVersion")
        or meta.get("payload", {}).get("latestGamedataVersion")
        or meta.get("data", {}).get("latestGamedataVersion")
    )
    if not version:
        raise SystemExit("No latestGamedataVersion")

    # 2) Descargar units y skills
    du = fetch_data_items(version, "units")
    ds = fetch_data_items(version, "skill")
    units = du.get("units") or du.get("payload", {}).get("units") or []
    skills = ds.get("skill") or ds.get("payload", {}).get("skill") or []

    # 3) Indexar skills por id
    skill_by_id: Dict[str, Dict[str, Any]] = {}
    for s in skills:
        sid = s.get("id") or s.get("skillId")
        if sid is not None:
            skill_by_id[str(sid)] = s

    # 4) Construir Characters / Ships, y map baseId -> skillIds
    headers_units = ["baseId", "friendlyName", "combatType"]
    chars_rows: List[List[Any]] = []
    ships_rows: List[List[Any]] = []
    seen_units: Set[str] = set()
    unit_skills: Dict[str, List[str]] = {}

    for u in units:
        base_id = (u.get("baseId") or u.get("id") or "").upper()
        if not base_id:
            continue
        if base_id in seen_units:
            continue
        if EXCLUDE and any(substr in base_id for substr in EXCLUDE):
            continue
        seen_units.add(base_id)

        name = _unit_name(u, base_id)
        ctype = u.get("combatType") or u.get("combat_type") or 1
        ctype = _as_int(ctype, 1) or 1

        row = [base_id, name, ctype]
        if ctype == 2:
            ships_rows.append(row)
        else:
            chars_rows.append(row)

        unit_skills[base_id] = _collect_unit_skill_ids(u)

    # 5) Construir CharactersZetas y CharactersOmicrons cruzando unit → skills
    headers_zetas = ["baseId", "skillId", "skillName"]
    headers_omis = ["baseId", "skillId", "skillName", "omicronMode"]

    zeta_rows: List[List[Any]] = []
    omi_rows: List[List[Any]] = []
    seen_zeta: Set[Tuple[str, str]] = set()
    seen_omi: Set[Tuple[str, str]] = set()

    def _skill_name(s: Dict[str, Any], fallback: str) -> str:
        return str(
            s.get("name")
            or s.get("uiName")
            or s.get("nameKey")
            or s.get("descKey")
            or fallback
        )

    for base_id, sids in unit_skills.items():
        for sid in sids:
            s = skill_by_id.get(sid)
            if not s:
                continue

            # Zetas
            if _skill_has_zeta(s):
                key = (base_id, sid)
                if key not in seen_zeta:
                    seen_zeta.add(key)
                    zeta_rows.append([base_id, sid, _skill_name(s, sid)])

            # Omicrons
            has_omi, omi_mode = _skill_has_omicron(s)
            if has_omi:
                key = (base_id, sid)
                if key not in seen_omi:
                    seen_omi.add(key)
                    omi_rows.append([base_id, sid, _skill_name(s, sid), "" if omi_mode is None else str(omi_mode)])

    # (opcional) ordenar para estabilidad visual
    chars_rows.sort(key=lambda r: r[1])  # por friendlyName
    ships_rows.sort(key=lambda r: r[1])
    zeta_rows.sort(key=lambda r: (r[0], r[2]))  # por baseId, skillName
    omi_rows.sort(key=lambda r: (r[0], r[2], r[3]))

    # 6) Escribir en Sheets
    ws_chars = open_or_create(SHEET_CHARACTERS)
    ws_ships = open_or_create(SHEET_SHIPS)
    ws_zetas = open_or_create(SHEET_CHARACTERS_ZETAS)
    ws_omis = open_or_create(SHEET_CHARACTERS_OMICRONS)

    write_sheet(ws_chars, headers_units, chars_rows)
    write_sheet(ws_ships, headers_units, ships_rows)
    write_sheet(ws_zetas, headers_zetas, zeta_rows)
    write_sheet(ws_omis, headers_omis, omi_rows)

    return {
        "characters": len(chars_rows),
        "ships": len(ships_rows),
        "zetas": len(zeta_rows),
        "omicrons": len(omi_rows),
    }


if __name__ == "__main__":
    print(run())
