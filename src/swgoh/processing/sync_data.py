# src/swgoh/processing/sync_data.py

from typing import Any, Dict, List, Set, Tuple
import os
from ..sheets import open_or_create, write_sheet, try_get_worksheet
from ..comlink import fetch_metadata, fetch_data_items
from ..http import post_json
from .. import config as cfg


# -------------------------------
# Helpers generales
# -------------------------------

def _as_int(value, default: int | None = None) -> int | None:
    try:
        return int(value)
    except Exception:
        return default


def _unit_name_fallback(u: Dict[str, Any], fallback: str) -> str:
    # Si localization falla, usamos estos campos de /data units
    return str(
        u.get("uiName")
        or u.get("longName")
        or u.get("name")
        or u.get("localizedName")
        or fallback
    )


def _normalize_alignment(val: Any) -> str:
    """
    Normaliza alignment a: "Light Side" / "Dark Side" / "Neutral" / "".
    Acepta strings (LIGHT/DARK/NEUTRAL) o números (1,2,3) y variantes.
    """
    if val is None:
        return ""
    s = str(val).strip().upper()
    # valores numéricos comunes
    if s.isdigit():
        n = int(s)
        if n == 1:
            return "Light Side"
        if n == 2:
            return "Dark Side"
        if n == 3:
            return "Neutral"
    # variantes de texto
    if "LIGHT" in s or s in {"LS", "LIGHT_SIDE", "LIGHTSIDE"}:
        return "Light Side"
    if "DARK" in s or s in {"DS", "DARK_SIDE", "DARKSIDE"}:
        return "Dark Side"
    if "NEUTRAL" in s:
        return "Neutral"
    return ""


def _alignment_from_unit(u: Dict[str, Any]) -> str:
    """
    Intenta extraer el alignment de forma robusta:
      1) forceAlignment / alignment
      2) categorías (alignment_light / alignment_dark / neutral)
    """
    val = u.get("forceAlignment") or u.get("alignment")
    norm = _normalize_alignment(val)
    if norm:
        return norm

    # 2) categorías
    cats = (u.get("categoryIdList") or u.get("categories") or [])
    for c in cats:
        cs = str(c).lower()
        if "alignment_light" in cs or "light_side" in cs or cs.endswith("_light"):
            return "Light Side"
        if "alignment_dark" in cs or "dark_side" in cs or cs.endswith("_dark"):
            return "Dark Side"
        if "neutral" in cs:
            return "Neutral"

    return ""


def _collect_unit_skill_ids(u: Dict[str, Any]) -> List[str]:
    """
    Extrae la lista de skillIds referenciadas por la unidad desde distintos campos posibles.
    """
    out: List[str] = []

    # 1) skillReferenceList: [{ skillId, ...}, ...]
    for ref in (u.get("skillReferenceList") or []):
        sid = ref.get("skillId") or ref.get("id")
        if sid:
            out.append(str(sid))

    # 2) skillList: ["skchr_xxx", ...]
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
    if s.get("isZeta") is True:
        return True
    if _as_int(s.get("zetaTier")) is not None:
        return True
    tiers = s.get("tier") or s.get("tiers") or []
    for t in tiers:
        if t.get("isZeta") is True:
            return True
        if str(t.get("name") or "").lower().find("zeta") >= 0:
            return True
    return False


def _skill_has_omicron(s: Dict[str, Any]) -> Tuple[bool, int | None]:
    tiers = s.get("tier") or s.get("tiers") or []
    for t in tiers:
        if t.get("isOmicron") is True or (t.get("omicronMode") is not None):
            return True, _as_int(t.get("omicronMode"))
    if s.get("isOmicron") is True or (s.get("omicronMode") is not None):
        return True, _as_int(s.get("omicronMode"))
    return False, None


# -------------------------------
# Localization
# -------------------------------

def _fetch_localization(locale: str) -> Dict[str, Any]:
    """
    Llama a /localization con el payload estándar que usábamos antes.
    Si no responde, devuelve {} (el script seguirá con fallback de nombres).
    """
    try:
        return post_json("/localization", {"payload": {"language": str(locale)}, "enums": False})
    except BaseException:
        return {}


def _build_localization_map(loc: Dict[str, Any]) -> Dict[str, str]:
    """
    Construye un mapa key -> texto desde la respuesta de /localization.
    Soporta varios formatos (dict/list) sin romperse.
    """
    mapping: Dict[str, str] = {}
    if not loc:
        return mapping

    strings = (
        loc.get("strings")
        or loc.get("payload", {}).get("strings")
        or loc.get("data", {}).get("strings")
        or loc.get("localizedStrings")
        or loc.get("stringsList")
        or []
    )
    if isinstance(strings, dict):
        for k, v in strings.items():
            try:
                mapping[str(k)] = str(v)
            except Exception:
                pass
    elif isinstance(strings, list):
        for s in strings:
            key = s.get("key") or s.get("id") or s.get("name")
            val = s.get("text") or s.get("value") or s.get("localizedString") or s.get("locString")
            if key is not None and val is not None:
                try:
                    mapping[str(key)] = str(val)
                except Exception:
                    pass
    return mapping


# -------------------------------
# Proceso principal
# -------------------------------

def run() -> Dict[str, int]:
    # Config
    SHEET_CHARACTERS = getattr(cfg, "SHEET_CHARACTERS", "Characters")
    SHEET_SHIPS = getattr(cfg, "SHEET_SHIPS", "Ships")
    SHEET_CHARACTERS_ZETAS = getattr(cfg, "SHEET_CHARACTERS_ZETAS", "CharactersZetas")
    SHEET_CHARACTERS_OMICRONS = getattr(cfg, "SHEET_CHARACTERS_OMICRONS", "CharactersOmicrons")
    EXCLUDE = set(getattr(cfg, "EXCLUDE_BASEID_CONTAINS", []))
    LOCALE = os.getenv("LOCALE", "ENG_US")

    # /metadata
    meta = fetch_metadata()
    version = (
        meta.get("latestGamedataVersion")
        or meta.get("payload", {}).get("latestGamedataVersion")
        or meta.get("data", {}).get("latestGamedataVersion")
    )
    if not version:
        raise SystemExit("No latestGamedataVersion")

    # /data units + skills
    du = fetch_data_items(version, "units")
    ds = fetch_data_items(version, "skill")
    units = du.get("units") or du.get("payload", {}).get("units") or []
    skills = ds.get("skill") or ds.get("payload", {}).get("skill") or []

    # Localization map
    loc_map = _build_localization_map(_fetch_localization(LOCALE))

    # Index de skills
    skill_by_id: Dict[str, Dict[str, Any]] = {}
    for s in skills:
        sid = s.get("id") or s.get("skillId")
        if sid is not None:
            skill_by_id[str(sid)] = s

    # Construcción de Characters / Ships (base_id; Name; Alignment)
    headers_units = ["base_id", "Name", "Alignment"]
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

        # Friendly Name desde localization (como antes): nameKey -> loc_map
        name_key = u.get("nameKey")
        name = loc_map.get(name_key) if name_key else None
        if not name:
            name = _unit_name_fallback(u, base_id)

        alignment = _alignment_from_unit(u)

        ctype = u.get("combatType") or u.get("combat_type") or 1
        ctype = _as_int(ctype, 1) or 1

        row = [base_id, str(name), alignment]
        if ctype == 2:
            ships_rows.append(row)
        else:
            chars_rows.append(row)

        unit_skills[base_id] = _collect_unit_skill_ids(u)

    # Zetas / Omicrons
    headers_zetas = ["base_id", "skillId", "skillName"]
    headers_omis = ["base_id", "skillId", "skillName", "omicronMode"]

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
            if _skill_has_zeta(s):
                key = (base_id, sid)
                if key not in seen_zeta:
                    seen_zeta.add(key)
                    zeta_rows.append([base_id, sid, _skill_name(s, sid)])
            has_omi, omi_mode = _skill_has_omicron(s)
            if has_omi:
                key = (base_id, sid)
                if key not in seen_omi:
                    seen_omi.add(key)
                    omi_rows.append([base_id, sid, _skill_name(s, sid), "" if omi_mode is None else str(omi_mode)])

    # Orden estable
    chars_rows.sort(key=lambda r: r[1])  # por Name
    ships_rows.sort(key=lambda r: r[1])
    zeta_rows.sort(key=lambda r: (r[0], r[2]))
    omi_rows.sort(key=lambda r: (r[0], r[2], r[3]))

    # Escribir
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
