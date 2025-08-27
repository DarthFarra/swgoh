# src/swgoh/processing/sync_data.py

from typing import Any, Dict, List, Set, Tuple, Optional
import os, re, json
from ..sheets import open_or_create, write_sheet
from ..comlink import fetch_metadata, fetch_data_items
from ..http import post_json
from .. import config as cfg


# =========================
#   Config / Entorno
# =========================
LOCALE = os.getenv("LOCALE", "ENG_US").strip() or "ENG_US"

# Filtros (como en el original)
EXCLUDE = set(getattr(cfg, "EXCLUDE_BASEID_CONTAINS", []))
SKIP_EMPTY_BASEID = True
DEDUP_UNITS = True

# Nombres de pestañas (ajuste pedido)
SHEET_CHARACTERS = getattr(cfg, "SHEET_CHARACTERS", "Characters")
SHEET_SHIPS = getattr(cfg, "SHEET_SHIPS", "Ships")
SHEET_ZETAS = getattr(cfg, "SHEET_CHARACTERS_ZETAS", "CharactersZetas")
SHEET_OMICRONS = getattr(cfg, "SHEET_CHARACTERS_OMICRONS", "CharactersOmicrons")

# (Opcional) mapeo de omicronMode → texto, como en tu script
OMICRON_MODE_MAP_JSON = os.getenv("OMICRON_MODE_MAP_JSON", "").strip()
OMICRON_MODE_MAP = os.getenv("OMICRON_MODE_MAP", "").strip()


# =========================
#   Helpers generales
# =========================
def _as_int(value, default: int | None = None) -> int | None:
    try:
        return int(value)
    except Exception:
        return default

def ensure_array(root: Any, key: str) -> List[Dict[str, Any]]:
    if isinstance(root, dict):
        for r in (root, root.get("data", {}), root.get("payload", {})):
            if isinstance(r, dict) and isinstance(r.get(key), list):
                return r[key]
    return []

def _extract_field(d: Dict[str, Any], paths: List[str]) -> Optional[str]:
    for p in paths:
        cur: Any = d
        ok = True
        for part in p.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                ok = False
                break
        if ok and isinstance(cur, (str, int)):
            return str(cur)
    return None

def force_alignment_text(val: Any) -> str:
    """
    Igual que el original:
      1 -> Neutral
      2 -> Light Side
      3 -> Dark Side
    """
    try:
        x = int(val)
    except Exception:
        return ""
    return {1: "Neutral", 2: "Light Side", 3: "Dark Side"}.get(x, "")


# =========================
#   Localization helpers
# =========================
def fetch_localization_raw(loc_bundle: str, locale: str) -> Dict[str, Any]:
    """
    Igual que el script original:
    /localization con {"unzip": true, "payload": {"id": f"{bundle}:{locale}"}}
    """
    return post_json(
        "/localization",
        {"unzip": True, "payload": {"id": f"{loc_bundle}:{locale}"}, "enums": False},
    )

def parse_loc_txt_map(loc_data: Dict[str, Any], locale: str) -> Dict[str, str]:
    """
    Busca "Loc_<LOCALE>.txt" (contenido KEY|VALUE por línea) y devuelve mapa UPPER(KEY) -> VALUE
    """
    candidates = [f"Loc_{locale}.txt", f"Loc_{locale.upper()}.txt", f"Loc_{locale.lower()}.txt"]
    text_blob = None
    for k in candidates:
        v = loc_data.get(k)
        if isinstance(v, str):
            text_blob = v
            break
    if text_blob is None:
        for k, v in (loc_data or {}).items():
            if isinstance(k, str) and k.endswith(".txt") and isinstance(v, str):
                text_blob = v
                break
    if not text_blob:
        raise SystemExit(f"No encontré 'Loc_{locale}.txt' en /localization.")

    mapping: Dict[str, str] = {}
    for raw in text_blob.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "|" not in line:
            continue
        key, val = line.split("|", 1)
        key = key.strip()
        if key.startswith("[") and key.endswith("]"):
            key = key[1:-1].strip()
        mapping[key] = val.strip()
    return {k.upper(): v for k, v in mapping.items()}  # lookup case-insensitive

def loc_lookup_ci(loc_upper: Dict[str, str], key: Optional[str]) -> str:
    if not key:
        return ""
    return loc_upper.get(str(key).upper(), "")


# =========================
#   Abilities / Skills
# =========================
def index_abilities(abilities: List[Dict[str, Any]]):
    by_id, by_namekey, by_desckey = {}, {}, {}
    for ab in abilities:
        if not isinstance(ab, dict):
            continue
        aid = (ab.get("id") or ab.get("abilityId") or "").strip()
        if aid:
            by_id[aid.upper()] = ab
        nk = (ab.get("nameKey") or "").strip()
        if nk:
            by_namekey[nk.upper()] = ab
        dk = (ab.get("descKey") or "").strip()
        if dk:
            by_desckey[dk.upper()] = ab
    return by_id, by_namekey, by_desckey

def map_skill_to_ability(
    skill: Dict[str, Any], ab_by_id: Dict[str, Any], ab_by_namekey: Dict[str, Any], ab_by_desckey: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    ar = (skill.get("abilityReference") or "").strip()
    if ar and ar.upper() in ab_by_id:
        return ab_by_id[ar.upper()]
    nk = (skill.get("nameKey") or "").strip()
    if nk and nk.upper() in ab_by_namekey:
        return ab_by_namekey[nk.upper()]
    dk = (skill.get("descKey") or "").strip()
    if dk and dk.upper() in ab_by_desckey:
        return ab_by_desckey[dk.upper()]
    return None

def friendly_ability_name_for_skill(ab: Dict[str, Any], loc_upper: Dict[str, str]) -> Tuple[str, str, str]:
    """
    Devuelve (friendly_name, skill_name_key, abilityReference_NAME_usado)
    Intenta {ability.id}_NAME; si no, ability.nameKey.
    """
    ab_id = (ab.get("id") or ab.get("abilityId") or "").strip().upper()
    if ab_id:
        cand = f"{ab_id}_NAME"
        val = loc_lookup_ci(loc_upper, cand)
        if val:
            return val, cand, cand
    nk = (ab.get("nameKey") or "").strip()
    if nk:
        val2 = loc_lookup_ci(loc_upper, nk)
        if val2:
            return val2, nk, ""
    return "No encontrado", "", ""


def load_omicron_mode_map() -> Dict[str, str]:
    if OMICRON_MODE_MAP_JSON:
        try:
            d = json.loads(OMICRON_MODE_MAP_JSON)
            return {str(k): str(v) for k, v in d.items()}
        except Exception as e:
            print(f"[WARN] OMICRON_MODE_MAP_JSON inválido: {e}")
    mapping: Dict[str, str] = {}
    if OMICRON_MODE_MAP:
        try:
            for p in [p.strip() for p in OMICRON_MODE_MAP.split(",") if p.strip()]:
                if ":" in p:
                    k, v = p.split(":", 1)
                    mapping[str(k).strip()] = str(v).strip()
        except Exception as e:
            print(f"[WARN] OMICRON_MODE_MAP inválido: {e}")
    return mapping

def omicron_mode_text(mode_val: Any, mapping: Dict[str, str]) -> str:
    if mode_val in (None, ""):
        return ""
    s = str(mode_val).strip()
    if s in mapping:
        return mapping[s]
    try:
        si = str(int(float(s)))
        return mapping.get(si, "")
    except Exception:
        return ""


# =========================
#   Proceso principal
# =========================
def run() -> Dict[str, int]:
    # 1) /metadata: version + bundle
    meta = fetch_metadata()
    version = _extract_field(meta, [
        "latestGamedataVersion",
        "payload.latestGamedataVersion",
        "data.latestGamedataVersion",
    ])
    loc_bundle = _extract_field(meta, [
        "latestLocalizationBundleVersion",
        "payload.latestLocalizationBundleVersion",
        "data.latestLocalizationBundleVersion",
    ])
    if not version:
        raise SystemExit("No pude obtener latestGamedataVersion.")
    if not loc_bundle:
        raise SystemExit("No pude obtener latestLocalizationBundleVersion.")

    # 2) /localization unzip
    loc_raw = fetch_localization_raw(loc_bundle, LOCALE)
    loc_upper = parse_loc_txt_map(loc_raw, LOCALE)

    # 3) /data units → Characters/Ships + índice skillId→units
    du = fetch_data_items(version, "units")
    units = ensure_array(du, "units")
    print(f"[INFO] Unidades recibidas: {len(units)}")

    units_by_base_norm: Dict[str, Dict[str, Any]] = {}
    for u in units:
        if not isinstance(u, dict):
            continue
        raw_base = (u.get("baseId") or u.get("base_id") or "").strip()
        if EXCLUDE and any(substr in raw_base.upper() for substr in EXCLUDE):
            continue
        base_norm = re.sub(r"\s+", "", raw_base).upper()
        if not base_norm and SKIP_EMPTY_BASEID:
            continue
        if DEDUP_UNITS and base_norm in units_by_base_norm:
            continue
        units_by_base_norm[base_norm] = u

    dedup_units = list(units_by_base_norm.values())
    print(f"[INFO] Unidades tras filtro/dedup: {len(dedup_units)}")

    unit_friendly_by_base: Dict[str, str] = {}
    skillid_to_unit_bases: Dict[str, set] = {}

    characters_rows: List[List[Any]] = []
    ships_rows: List[List[Any]] = []

    for u in dedup_units:
        base_id = (u.get("baseId") or u.get("base_id") or "").strip()
        combat_type = _as_int(u.get("combatType"), 0) or 0
        align_val = u.get("forceAlignment")

        # Friendly unit name (nameKey → localization); fallback a campos de unit si no existe
        name_key = (u.get("nameKey") or u.get("unitNameKey") or "").strip()
        friendly_unit = ""
        if name_key:
            friendly_unit = loc_lookup_ci(loc_upper, name_key)
        if not friendly_unit and base_id:
            friendly_unit = loc_lookup_ci(loc_upper, f"UNIT_{base_id}_NAME")
        if not friendly_unit:
            friendly_unit = (
                u.get("uiName") or u.get("longName") or u.get("name") or u.get("localizedName") or ""
            )
        unit_friendly_by_base[base_id] = str(friendly_unit or "")

        # skill → units (como en original, usa skillReference; dejamos robusto por si el schema varía)
        for ref in (u.get("skillReference") or u.get("skillReferenceList") or []):
            if isinstance(ref, dict):
                sid = ref.get("id") or ref.get("skillId") or ref.get("abilityId")
            else:
                sid = str(ref)
            if sid:
                skillid_to_unit_bases.setdefault(str(sid), set()).add(base_id)

        # Characters/Ships filas
        row = [base_id, str(friendly_unit or ""), force_alignment_text(align_val)]
        if combat_type == 2:
            ships_rows.append(row)
        else:
            characters_rows.append(row)

    # 4) /data ability para friendly de skills
    da = fetch_data_items(version, "ability")
    abilities = ensure_array(da, "ability")
    ab_by_id, ab_by_namekey, ab_by_desckey = index_abilities(abilities)

    # 5) /data skill (zetas/omicrons) — SOLO tiers con isZetaTier / isOmicronTier == true
    ds = fetch_data_items(version, "skill")
    skills = ensure_array(ds, "skill")
    print(f"[INFO] Skills: {len(skills)}")
    om_map = load_omicron_mode_map()

    zetas_rows: List[List[Any]] = []
    omicrons_rows: List[List[Any]] = []

    def friendly_skill_for_skill(s: Dict[str, Any]) -> Tuple[str, str, str]:
        # Intenta resolver vía ability → localization (ID_NAME) y si no, por nameKey
        friendly_skill, skill_key_used, ability_key_used = ("No encontrado", "", "")
        ab = map_skill_to_ability(s, ab_by_id, ab_by_namekey, ab_by_desckey)
        if ab:
            friendly_skill, skill_key_used, ability_key_used = friendly_ability_name_for_skill(ab, loc_upper)
        if friendly_skill == "No encontrado":
            nk = (s.get("nameKey") or "").strip()
            if nk:
                val2 = loc_lookup_ci(loc_upper, nk)
                if val2:
                    friendly_skill, skill_key_used, ability_key_used = val2, nk, ""
        return friendly_skill, skill_key_used, ability_key_used

    for s in skills:
        if not isinstance(s, dict):
            continue

        sid = str(s.get("id") or "")
        ability_ref = (s.get("abilityReference") or "").strip()

        # Nombre amigable de la skill
        friendly_skill, skill_key_used, ability_key_used = friendly_skill_for_skill(s)

        # omicronMode a nivel de skill (se muestra como valor y texto)
        skill_omicron_mode = s.get("omicronMode")
        omicron_mode_value = "" if skill_omicron_mode in (None, "") else str(skill_omicron_mode)
        omicron_mode_text_val = omicron_mode_text(omicron_mode_value, om_map)

        # tiers (solo añadimos filas cuando el tier marca explícitamente isZetaTier / isOmicronTier)
        tiers = s.get("tier")
        if tiers is None:
            tiers = s.get("tiers", [])
        if not isinstance(tiers, list):
            continue

        # CharacterName (primer unit que referencia la skill)
        bases = sorted(list(skillid_to_unit_bases.get(sid, set())))
        character_name = (unit_friendly_by_base.get(bases[0], "") if bases else "")
        char_skill_concat = (f"{character_name}|{friendly_skill}" if character_name else friendly_skill)

        for tr in tiers:
            if not isinstance(tr, dict):
                continue
            is_zeta = bool(tr.get("isZetaTier"))
            is_omicron = bool(tr.get("isOmicronTier"))  # ← filtro exacto pedido

            if not (is_zeta or is_omicron):
                continue

            # recipeId (mismo patrón del original: varios nombres posibles)
            recipe_id = ""
            for k in ("recipeId", "tierUpRecipeId", "upgradeRecipeId", "upgradeRecipeReference"):
                v = tr.get(k)
                if v not in (None, ""):
                    recipe_id = str(v)
                    break

            row = [
                sid,
                ability_ref,
                friendly_skill,
                skill_key_used,           # skill name key que resolvió
                ability_key_used,         # {ability.id}_NAME si se usó
                omicron_mode_value,       # omicronMode (skill-level)
                omicron_mode_text_val,    # texto opcional por mapping
                recipe_id,
                character_name,
                char_skill_concat,
            ]
            if is_zeta:
                zetas_rows.append(row)
            if is_omicron:
                omicrons_rows.append(row)

    # Orden estable (igual que veníamos haciendo)
    characters_rows.sort(key=lambda r: r[1])
    ships_rows.sort(key=lambda r: r[1])
    zetas_rows.sort(key=lambda r: (r[0], r[2]))
    omicrons_rows.sort(key=lambda r: (r[0], r[2]))

    # 6) Escribir en Sheets (usa write_sheet que redimensiona antes de escribir)
    ws_chars = open_or_create(SHEET_CHARACTERS)
    ws_ships = open_or_create(SHEET_SHIPS)
    ws_zetas = open_or_create(SHEET_ZETAS)
    ws_omis = open_or_create(SHEET_OMICRONS)

    headers_units = ["base_id", "Name", "Alignment"]
    write_sheet(ws_chars, headers_units, characters_rows)
    write_sheet(ws_ships, headers_units, ships_rows)

    headers_skills = [
        "skillid", "abilityReference", "skill name", "skill name key", "abilityReference_NAME",
        "omicronMode", "omicronModeText", "recipeId", "CharacterName", "CharacterName|skill name"
    ]
    write_sheet(ws_zetas, headers_skills, zetas_rows)
    write_sheet(ws_omis, headers_skills, omicrons_rows)

    return {
        "characters": len(characters_rows),
        "ships": len(ships_rows),
        "zetas": len(zetas_rows),
        "omicrons": len(omicrons_rows),
    }


if __name__ == "__main__":
    print(run())
