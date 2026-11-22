from __future__ import annotations

import re
import json
import urllib.request
import urllib.error
from dataclasses import dataclass
from html import escape
from typing import Iterable, Optional

from ...comlink import fetch_player_by_id
from ..config import (
    PLAYERS_SHEET,
    SWGOH_STATS_BASE,
    TW_REQS_SHEET,
)
from .sheets_io import read_values_cached


STAT_COLUMNS = (
    "Health",
    "Protection",
    "Speed",
    "CritDamage",
    "Potency",
    "Tenacity",
    "PhysDamage",
    "PhysCritChance",
    "Armor",
    "PhysCritAvoidance",
    "SpecDamage",
    "SpecCritChance",
    "Resistance",
    "SpecCritAvoidance",
)

STAT_LABELS_ES = {
    "Health": "Salud",
    "Protection": "Protección",
    "Speed": "Velocidad",
    "CritDamage": "Daño crítico",
    "Potency": "Potencia",
    "Tenacity": "Tenacidad",
    "PhysDamage": "Daño físico",
    "PhysCritChance": "Probabilidad de crítico físico",
    "Armor": "Armadura",
    "PhysCritAvoidance": "Evasión de crítico físico",
    "SpecDamage": "Daño especial",
    "SpecCritChance": "Probabilidad de crítico especial",
    "Resistance": "Resistencia",
    "SpecCritAvoidance": "Evasión de crítico especial",
}

# Comlink unit-stat ids used only for final structures that expose them.
# The normal /player roster does not provide this field and must not be
# treated as a source of final character totals.
STAT_UNIT_IDS = {
    "Health": (1, 41),
    "Protection": (28, 42),
    "Speed": (5, 18),
    "CritDamage": (12, 16),
    "Potency": (17, 56),
    "Tenacity": (17, 55),
    "PhysDamage": (8, 48),
    "PhysCritChance": (9, 53),
    "Armor": (6, 49),
    "PhysCritAvoidance": (20, 54),
    "SpecDamage": (10, 48),
    "SpecCritChance": (11, 53),
    "Resistance": (7, 49),
    "SpecCritAvoidance": (21, 54),
}


@dataclass(frozen=True)
class StatRequirement:
    character: str
    character_id: str
    hard: dict[str, float]
    recommended: dict[str, float]


@dataclass(frozen=True)
class StatFailure:
    character: str
    stat: str
    actual: Optional[float]
    expected: float
    hard: bool


@dataclass(frozen=True)
class PlayerPrep:
    player_name: str
    failures: tuple[StatFailure, ...]


def _safe_float(value: object) -> Optional[float]:
    try:
        text = str(value).strip().replace(",", ".").replace("%", "")
        if not text:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def parse_requirement(value: object) -> tuple[Optional[float], Optional[float]]:
    text = str(value or "").strip()
    if not text:
        return None, None
    match = re.fullmatch(r"\s*([^()]*)?(?:\(([^()]*)\))?\s*", text)
    if not match:
        return _safe_float(text), None
    hard = _safe_float(match.group(1))
    recommended = _safe_float(match.group(2))
    if recommended is None and "(" not in text:
        recommended = None
    return hard, recommended


def read_tw_requirements(ss) -> list[StatRequirement]:
    headers, rows = read_values_cached(ss, TW_REQS_SHEET)
    if not rows:
        return []
    lower = {header.strip().lower(): i for i, header in enumerate(headers)}
    character_index = lower.get("character")
    character_id_index = lower.get("characterid")
    if character_index is None or character_id_index is None:
        return []

    column_indexes = {
        stat: lower.get(stat.lower())
        for stat in STAT_COLUMNS
    }
    requirements: list[StatRequirement] = []
    for row in rows:
        character = (
            row[character_index].strip()
            if character_index < len(row)
            else ""
        )
        character_id = (
            row[character_id_index].strip()
            if character_id_index < len(row)
            else ""
        )
        if not character or not character_id:
            continue
        hard: dict[str, float] = {}
        recommended: dict[str, float] = {}
        for stat, index in column_indexes.items():
            if index is None or index >= len(row):
                continue
            hard_value, recommended_value = parse_requirement(row[index])
            if hard_value is not None:
                hard[stat] = hard_value
            if recommended_value is not None:
                recommended[stat] = recommended_value
        requirements.append(
            StatRequirement(character, character_id, hard, recommended)
        )
    return requirements


def _unit_stats(unit: dict) -> dict[str, float]:
    raw = (
        unit.get("baseStat")
        or unit.get("stats")
        or []
    )
    out: dict[str, float] = {}
    if isinstance(raw, dict):
        raw = raw.get("stat") or raw
        if isinstance(raw, dict):
            raw = [raw]
    if not isinstance(raw, list):
        return out

    for entry in raw:
        if not isinstance(entry, dict):
            continue
        stat_id = entry.get("unitStatId")
        value = entry.get("statValueDecimal")
        if stat_id is None:
            stat_id = entry.get("id")
        if stat_id is None:
            continue
        try:
            stat_id_int = int(stat_id)
        except (TypeError, ValueError):
            continue
        numeric = _safe_float(value)
        if numeric is None:
            continue
        for stat_name, aliases in STAT_UNIT_IDS.items():
            if stat_id_int in aliases:
                out.setdefault(stat_name, numeric)
    return out


def _character_id(unit: dict) -> str:
    definition_id = str(unit.get("definitionId") or "")
    return definition_id.split(":", 1)[0].strip()


def _post_calculated_roster(player_response: dict) -> list[dict]:
    if not SWGOH_STATS_BASE:
        return []
    url = SWGOH_STATS_BASE.rstrip("/") + "/api?flags=enums"
    body = json.dumps([player_response], ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            data = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError):
        return []
    if not isinstance(data, list) or not data:
        return []
    profile = data[0]
    if not isinstance(profile, dict):
        return []
    return profile.get("rosterUnit") or profile.get("roster") or []


def _combined_unit_stats(unit: dict) -> dict[str, float]:
    stats = unit.get("stats")
    if not isinstance(stats, dict):
        return {}
    base = stats.get("base") if isinstance(stats.get("base"), dict) else {}
    mods = stats.get("mods") if isinstance(stats.get("mods"), dict) else {}

    def add(*keys: str) -> Optional[float]:
        value = 0.0
        found = False
        for key in keys:
            if key in base:
                try:
                    value += float(base[key])
                    found = True
                except (TypeError, ValueError):
                    pass
            if key in mods:
                try:
                    value += float(mods[key])
                    found = True
                except (TypeError, ValueError):
                    pass
        return value if found else None

    health = add("UnitStat_Health")
    protection = add("UnitStat_MaxShield")
    speed = add("UnitStat_Speed")
    crit_damage = add("UnitStat_CriticalDamage")
    potency = add("UnitStat_Accuracy")
    tenacity = add("UnitStat_Resistance")
    physical_damage = add("UnitStat_AttackDamage")
    armor = add("UnitStat_Armor")
    special_damage = add("UnitStat_AbilityPower")
    resistance = add("UnitStat_Suppression")

    physical_crit = None
    if "UnitStat_AttackCriticalRating_TU5V" in base:
        try:
            physical_crit = float(base["UnitStat_AttackCriticalRating_TU5V"]) / 100.0
        except (TypeError, ValueError):
            pass
        try:
            physical_crit = (physical_crit or 0.0) + float(
                mods.get("UnitStat_AttackCriticalPercentAdditive", 0)
            ) * 100.0
        except (TypeError, ValueError):
            pass

    special_crit = None
    if "UnitStat_AbilityCriticalRating_TU5V" in base:
        try:
            special_crit = float(base["UnitStat_AbilityCriticalRating_TU5V"]) / 100.0
        except (TypeError, ValueError):
            pass
        try:
            special_crit = (special_crit or 0.0) + float(
                mods.get("UnitStat_AbilityCriticalPercentAdditive", 0)
            ) * 100.0
        except (TypeError, ValueError):
            pass

    return {
        "Health": health,
        "Protection": protection,
        "Speed": speed,
        "CritDamage": crit_damage,
        "Potency": potency,
        "Tenacity": tenacity,
        "PhysDamage": physical_damage,
        "PhysCritChance": physical_crit,
        "Armor": armor,
        "PhysCritAvoidance": None,
        "SpecDamage": special_damage,
        "SpecCritChance": special_crit,
        "Resistance": resistance,
        "SpecCritAvoidance": None,
    }


def evaluate_player(
    ss,
    player_id: str,
    player_name: str,
) -> PlayerPrep:
    requirements = read_tw_requirements(ss)
    if not requirements:
        return PlayerPrep(player_name, ())

    try:
        response = fetch_player_by_id(player_id)
    except Exception:
        return PlayerPrep(
            player_name,
            tuple(
                StatFailure(req.character, stat_name, None, value, True)
                for req in requirements
                for stat_name, value in req.hard.items()
            ),
        )

    roster = (
        response.get("rosterUnit")
        or response.get("player", {}).get("rosterUnit")
        or []
    )
    calculated_roster = _post_calculated_roster(response)
    units_by_id: dict[str, dict] = {}
    for unit in calculated_roster:
        if not isinstance(unit, dict):
            continue
        character_id = _character_id(unit)
        if character_id:
            units_by_id[character_id] = unit

    failures: list[StatFailure] = []
    for req in requirements:
        unit = units_by_id.get(req.character_id)
        if unit is None:
            for stat_name, value in req.hard.items():
                failures.append(StatFailure(req.character, stat_name, None, value, True))
            continue

        actual = _combined_unit_stats(unit)
        for stat_name, expected in req.hard.items():
            actual_value = actual.get(stat_name)
            if actual_value is None or actual_value + 1e-9 < expected:
                failures.append(
                    StatFailure(req.character, stat_name, actual_value, expected, True)
                )
        for stat_name, expected in req.recommended.items():
            actual_value = actual.get(stat_name)
            if actual_value is None or actual_value + 1e-9 < expected:
                failures.append(
                    StatFailure(req.character, stat_name, actual_value, expected, False)
                )
    return PlayerPrep(player_name, tuple(failures))


def has_hard_failure(prep: PlayerPrep) -> bool:
    return any(failure.hard for failure in prep.failures)


def format_value(stat: str, value: Optional[float]) -> str:
    if value is None:
        return "dato no disponible"
    if stat in {"CritDamage", "Potency", "Tenacity", "PhysCritChance", "PhysCritAvoidance", "SpecCritChance", "SpecCritAvoidance"}:
        return f"{value:.1f}%"
    return f"{value:,.0f}".replace(",", ".")


def render_player_prep(prep: PlayerPrep, telegram: bool = True) -> str:
    lines: list[str] = []
    title = f"Preparación GT — {prep.player_name}"
    lines.append(f"<b>{escape(title)}</b>" if telegram else f"**{title}**")
    if not prep.failures:
        lines.append("Esta cuenta se puede apuntar a GT")
        return "\n".join(lines)

    grouped: dict[str, list[StatFailure]] = {}
    for failure in prep.failures:
        grouped.setdefault(failure.character, []).append(failure)

    for character, failures in grouped.items():
        lines.append(f"{escape(character)}:")
        for failure in failures:
            label = STAT_LABELS_ES.get(failure.stat, failure.stat)
            requirement = (
                f"(Mínimo {format_value(failure.stat, failure.expected)})"
                if failure.hard
                else f"(Recomendado {format_value(failure.stat, failure.expected)})"
            )
            if telegram and failure.hard:
                requirement = f"<b>{escape(requirement)}</b>"
            elif not telegram and failure.hard:
                requirement = f"**{requirement}**"

            prefix = "❌" if failure.hard else "⚠️"
            lines.append(
                f" - {prefix} {label}: "
                f"{format_value(failure.stat, failure.actual)} "
                f"{requirement}"
            )

    if has_hard_failure(prep):
        lines.append("")
        lines.append(
            "<b>Esta cuenta no se puede apuntar a GT hasta que no resuelva "
            "los stats marcados con ❌.</b>"
        )
    elif any(not failure.hard for failure in prep.failures):
        lines.append("")
        lines.append(
            "<b>Esta cuenta se puede apuntar a GT,</b>"
            " pero ojo a los stats recomendados 👀."
        )
    else:
        lines.append("")
        lines.append("<b>¡Perfecto! ¡Esta cuenta se puede apuntar a GT! ✅</b>")
    return "\n".join(lines)


def read_guild_players(ss, guild_name: str) -> list[tuple[str, str]]:
    headers, rows = read_values_cached(ss, PLAYERS_SHEET)
    lower = {header.strip().lower(): i for i, header in enumerate(headers)}
    id_index = lower.get("player id")
    name_index = lower.get("player name")
    guild_index = lower.get("guild name")
    if id_index is None or name_index is None or guild_index is None:
        return []

    out: list[tuple[str, str]] = []
    for row in rows:
        row_guild = (
            row[guild_index].strip()
            if guild_index < len(row)
            else ""
        )
        if row_guild != guild_name:
            continue
        player_id = (
            row[id_index].strip()
            if id_index < len(row)
            else ""
        )
        player_name = (
            row[name_index].strip()
            if name_index < len(row)
            else ""
        )
        if player_id and player_name:
            out.append((player_id, player_name))
    out.sort(key=lambda item: item[1].lower())
    return out


def evaluate_guild(ss, guild_name: str) -> list[PlayerPrep]:
    return [
        evaluate_player(ss, player_id, player_name)
        for player_id, player_name in read_guild_players(ss, guild_name)
    ]


def render_guild_prep(
    guild_name: str,
    preps: Iterable[PlayerPrep],
    telegram: bool = True,
) -> str:
    lines: list[str] = []
    title = f"Preparación GT — {guild_name}"
    lines.append(f"<b>{escape(title)}</b>" if telegram else f"**{title}**")

    for prep in preps:
        if not prep.failures:
            lines.append(f"✅ {escape(prep.player_name)}")
            continue

        hard_failures = [failure for failure in prep.failures if failure.hard]
        if hard_failures:
            lines.append(f"❌ {escape(prep.player_name)}")
            for failure in hard_failures:
                label = STAT_LABELS_ES.get(failure.stat, failure.stat)
                lines.append(
                    "  - "
                    f"{escape(failure.character)} — {label}: "
                    f"{format_value(failure.stat, failure.actual)} "
                    f"(Mínimo {format_value(failure.stat, failure.expected)})"
                )
        else:
            lines.append(f"✅ {escape(prep.player_name)} 👀")
            for failure in prep.failures:
                label = STAT_LABELS_ES.get(failure.stat, failure.stat)
                lines.append(
                    "  - "
                    f"{escape(failure.character)} — {label}: "
                    f"{format_value(failure.stat, failure.actual)} "
                    f"(Recomendado {format_value(failure.stat, failure.expected)})"
                )
    return "\n".join(lines)
