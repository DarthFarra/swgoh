from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from html import escape
from typing import Dict, Iterable, Optional

from .sheets_io import read_values_cached
from ..config import RAID_HISTORY_SHEET, GUILDS_SHEET, PLAYERS_SHEET


@dataclass(frozen=True)
class RaidHistoryRow:
    date: str
    member_id: str
    member_name: str
    raid_id: str
    score: int
    relic_levels: int
    guild_id: str
    guild_name: str


def _safe_int(value: object) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


def read_raid_history_rows(
    ss,
    guild_id: str,
    guild_name: str,
) -> list[RaidHistoryRow]:
    try:
        headers, rows = read_values_cached(ss, RAID_HISTORY_SHEET)
    except Exception:
        return []
    if not rows:
        return []

    lower = {h.strip().lower(): i for i, h in enumerate(headers)}
    required = ("date", "member_id", "member_name", "raid_id", "score", "relic_levels")
    if any(name not in lower for name in required):
        return []

    out: list[RaidHistoryRow] = []

    known_player_ids: set[str] = set()
    known_player_names: set[str] = set()
    try:
        player_headers, player_rows = read_values_cached(ss, PLAYERS_SHEET)
        player_lower = {
            h.strip().lower(): i
            for i, h in enumerate(player_headers)
        }
        player_id_index = player_lower.get("player id")
        player_name_index = player_lower.get("player name")
        guild_name_index = player_lower.get("guild name")
        for player_row in player_rows:
            row_guild = (
                player_row[guild_name_index].strip()
                if guild_name_index is not None and guild_name_index < len(player_row)
                else ""
            )
            if guild_name and row_guild and row_guild != guild_name:
                continue
            if player_id_index is not None and player_id_index < len(player_row):
                player_id = player_row[player_id_index].strip()
                if player_id:
                    known_player_ids.add(player_id)
            if player_name_index is not None and player_name_index < len(player_row):
                player_name = player_row[player_name_index].strip()
                if player_name:
                    known_player_names.add(player_name.lower())
    except Exception:
        pass

    for row in rows:
        def get(name: str) -> str:
            idx = lower.get(name)
            return (row[idx] if idx is not None and idx < len(row) else "").strip()

        row_guild_id = get("guild_id")
        row_guild_name = get("guild_name")
        if guild_id and row_guild_id and row_guild_id != guild_id:
            continue
        # Legacy rows may contain a Comlink name that differs from the
        # operator-curated Guilds sheet name. A matching guild_id is enough.
        if not guild_id and guild_name and row_guild_name and row_guild_name != guild_name:
            continue

        member_id = get("member_id")
        member_name = get("member_name")
        if known_player_ids or known_player_names:
            in_players = (
                member_id in known_player_ids
                if member_id
                else member_name.lower() in known_player_names
            )
            if not in_players:
                continue

        out.append(RaidHistoryRow(
            date=get("date"),
            member_id=get("member_id"),
            member_name=get("member_name"),
            raid_id=get("raid_id"),
            score=_safe_int(get("score")),
            relic_levels=_safe_int(get("relic_levels")),
            guild_id=row_guild_id,
            guild_name=row_guild_name,
        ))
    return out


def raid_name(raid_id: str) -> str:
    normalized = (raid_id or "").strip().lower()
    names = {
        "order66": "La venganza de los Sith",
        "droiddestruction": "Destrucción droide",
        "kraytdragon": "Caza del dragón krayt",
        "speederbike": "Asalto de speeders",
        "naboo": "Asalto de Naboo",
        "sith_raid": "Triunvirato",
        "aat": "AAT",
        "rancor": "Rancor",
        "rancor_challenge": "Rancor Challenge",
    }
    if normalized in names:
        return names[normalized]
    words = normalized.replace("_", " ").replace("-", " ").split()
    return " ".join(word.upper() if len(word) <= 3 else word.capitalize() for word in words) or "Asalto"


def format_raid_date(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        return value


def format_raid_threshold(value: int) -> str:
    if value % 1_000_000 == 0:
        return f"{value // 1_000_000}M"
    value_text = f"{value / 1_000_000:.2f}".rstrip("0").rstrip(".")
    return f"{value_text.replace('.', ',')}M"


def parse_raid_threshold(value: str) -> int:
    raw = (value or "").strip().lower().replace(" ", "")
    if not raw:
        raise ValueError("Raid threshold is empty")

    try:
        if raw.endswith("m"):
            number = raw[:-1].replace(",", ".")
            return int(float(number) * 1_000_000)
        return int(float(raw.replace(",", ".")))
    except (TypeError, ValueError):
        raise ValueError(f"Invalid raid threshold: {value!r}") from None


def read_raid_thresholds(
    ss,
    guild_id: str,
    guild_name: str,
) -> tuple[int, int]:
    headers, rows = read_values_cached(ss, GUILDS_SHEET)
    lower = {h.strip().lower(): i for i, h in enumerate(headers)}
    id_index = lower.get("guild id")
    name_index = lower.get("guild name")
    high_index = lower.get("raid_threshold_high")
    low_index = lower.get("raid_threshold_low")
    if id_index is None or high_index is None or low_index is None:
        raise ValueError(
            "Guilds must contain raid_threshold_high and raid_threshold_low"
        )

    for row in rows:
        row_id = (row[id_index] if id_index < len(row) else "").strip()
        row_name = (
            row[name_index] if name_index is not None and name_index < len(row) else ""
        ).strip()
        if (guild_id and row_id == guild_id) or (
            not row_id and guild_name and row_name == guild_name
        ):
            high = parse_raid_threshold(
                row[high_index] if high_index < len(row) else ""
            )
            low = parse_raid_threshold(
                row[low_index] if low_index < len(row) else ""
            )
            return high, low

    raise ValueError(f"Raid thresholds not found for guild {guild_name or guild_id}")


def _member_label(
    name: str,
    usernames: Dict[str, Optional[str]],
    mention_mode: str = "telegram",
) -> str:
    mention = usernames.get(name.lower())
    if not mention:
        return escape(name, quote=False) if mention_mode == "telegram" else name
    if mention_mode == "discord":
        return f"{name} {mention}"
    handle = mention.lstrip("@")
    return f"{escape(name, quote=False)} @{escape(handle, quote=False)}"


def _bold(text: str, mention_mode: str) -> str:
    if mention_mode == "discord":
        return f"**{text}**"
    if mention_mode == "telegram":
        return f"<b>{escape(text, quote=False)}</b>"
    return text


def render_raid_report(
    guild_name: str,
    rows: Iterable[RaidHistoryRow],
    usernames: Dict[str, Optional[str]],
    threshold_high: int,
    threshold_low: int,
    mention_mode: str = "telegram",
) -> Optional[str]:
    if threshold_high <= 0 or threshold_low <= 0:
        raise ValueError("Raid thresholds must be positive")

    grouped: Dict[str, list[RaidHistoryRow]] = defaultdict(list)
    for row in rows:
        if row.date:
            grouped[row.date].append(row)
    if not grouped:
        return None

    dates = sorted(grouped, reverse=True)
    latest_date = dates[0]
    latest = grouped[latest_date]
    previous = grouped[dates[1]] if len(dates) > 1 else []

    raid_id = next((row.raid_id for row in latest if row.raid_id), "")
    raid_label = raid_name(raid_id)
    latest_relic = {row.member_id: row.relic_levels for row in latest}
    latest_score = {row.member_id: row.score for row in latest}
    previous_relic = {row.member_id: row.relic_levels for row in previous}

    gains: Dict[str, int] = {}
    gain_names: Dict[str, str] = {}
    for row in latest:
        if not row.member_id:
            continue
        gain_names[row.member_id] = row.member_name or row.member_id
        gains[row.member_id] = max(
            0,
            latest_relic.get(row.member_id, 0)
            - previous_relic.get(row.member_id, 0),
        ) if previous else 0

    reached = sum(
        1 for row in latest
        if latest_score.get(row.member_id, 0) >= threshold_high
    )
    total = len(latest)
    percentage = round(reached * 100 / total) if total else 0

    below_primary = [
        (
            gain_names.get(member_id, member_id),
            gain,
            latest_score.get(member_id, 0),
        )
        for member_id, gain in gains.items()
        if latest_score.get(member_id, 0) < threshold_high
    ]
    positive_gains = [
        (name, gain)
        for name, gain, _score in below_primary
        if gain > 0
    ]
    positive_gains.sort(key=lambda item: (-item[1], item[0].lower()))
    total_gains = sum(gain for _, gain in positive_gains)

    no_gains = [
        name
        for name, gain, score in below_primary
        if gain <= 0 and score < threshold_low
    ]
    no_gains.sort(key=str.lower)

    high_text = format_raid_threshold(threshold_high)
    low_text = format_raid_threshold(threshold_low)

    header = f"Informe del último asalto; {raid_label} – {guild_name}"
    if mention_mode == "telegram":
        header = escape(header, quote=False)
    lines = [
        _bold(header, mention_mode),
        format_raid_date(latest_date),
        "",
        (
            f"{_bold(f'{reached} ({percentage}%)', mention_mode)} "
            f"han llegado a los {high_text} requeridos."
        ),
        "",
        (
            _bold(
                "Niveles de reliquia subidos "
                "(desde que terminó la raid anterior) "
                f"por quienes no llegan a los {high_text}: {total_gains}",
                mention_mode,
            )
        ),
        "",
        _bold(
            f"Reliquias subidas de quienes no han llegado a los {high_text} "
            "en esta última raid:",
            mention_mode,
        ),
    ]

    if positive_gains:
        for index, (name, gain) in enumerate(positive_gains, start=1):
            relic_word = "reliquia" if gain == 1 else "reliquias"
            suffix = "subida" if gain == 1 else "subidas"
            lines.append(
                f"{index}. {_bold(f'Con {gain} {relic_word} {suffix}', mention_mode)}: "
                f"{_member_label(name, usernames, mention_mode)}"
            )
    else:
        lines.append("Ninguno.")

    lines.extend([
        "",
        _bold(
            f"Jugadores que no han llegado a {low_text} en esta última raid "
            "y no han subido reliquias:",
            mention_mode,
        ),
    ])
    if no_gains:
        for name in no_gains:
            lines.append(_member_label(name, usernames, mention_mode))
    else:
        lines.append("Ninguno.")

    lines.extend([
        "",
        "Próximo informe cuando termine el próximo asalto.",
        "",
        _bold("¡Vamos por más!", mention_mode),
    ])
    return "\n".join(lines)
