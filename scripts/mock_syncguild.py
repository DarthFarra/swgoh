"""Simulate ``/syncguild`` for one guild using a saved Comlink response.

The real sync obtains guild and roster data from Comlink and writes several
Google Sheets tabs. This script keeps the same sheet-shaped outputs while
remaining completely offline, which makes it useful when Sheets or Comlink
are unavailable. The default input is ``guild-comlink.json``; the historical
ticket CSV remains supported with ``--snapshot``.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


GUILDS_HEADERS = [
    "Guild Id",
    "Guild Name",
    "Members",
    "Guild GP",
    "Last Raid Id",
    "Last Raid Score",
    "Last Update",
]
PLAYERS_HEADERS = [
    "Player Id",
    "Player Name",
    "Ally code",
    "Guild Name",
    "Role",
    "Level",
    "GP",
    "GAC League",
]
ROLE_MAP = {2: "Miembro", 3: "Oficial", 4: "Lider"}


def _read_snapshot(path: Path, guild_id: str) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))

    required = {"guild_id", "player_id", "player_name", "current_value", "lifetime_value"}
    missing = required - set(rows[0]) if rows else required
    if missing:
        raise ValueError(
            f"{path} no contiene las columnas requeridas: {', '.join(sorted(missing))}"
        )

    selected = [row for row in rows if row["guild_id"].strip() == guild_id]
    if not selected:
        raise ValueError(f"No hay jugadores para el guildId {guild_id} en {path}")
    return selected


def _read_comlink_guild(path: Path) -> tuple[str, str, list[dict[str, str]]]:
    with path.open(encoding="utf-8") as handle:
        response = json.load(handle)

    guild = response.get("guild", response)
    members = guild.get("member", []) if isinstance(guild, dict) else []
    if not isinstance(members, list) or not members:
        raise ValueError(f"{path} no contiene guild.member")

    profile = guild.get("profile", {}) if isinstance(guild, dict) else {}
    guild_name = str(profile.get("name") or guild.get("name") or "").strip()
    guild_gp = str(
        profile.get("guildGalacticPower")
        or guild.get("guildGalacticPower")
        or guild.get("galacticPower")
        or ""
    ).strip()
    rows = []
    for member in members:
        if not isinstance(member, dict):
            continue
        player_id = str(member.get("playerId") or "").strip()
        player_name = str(member.get("playerName") or "").strip()
        if player_id and player_name:
            rows.append(
                {
                    "player_id": player_id,
                    "player_name": player_name,
                    "role": ROLE_MAP.get(
                        int(member.get("memberLevel", 0) or 0), "Miembro"
                    ),
                    "gp": str(member.get("galacticPower") or "").strip(),
                }
            )
    if not rows:
        raise ValueError(f"{path} no contiene miembros con playerId y playerName")
    return guild_name, guild_gp, rows


def _write_csv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def mock_syncguild(
    guild_id: str,
    input_path: Path,
    output_dir: Path,
    input_is_snapshot: bool = False,
) -> list[Path]:
    if input_is_snapshot:
        guild_name = guild_id
        guild_gp = ""
        members = _read_snapshot(input_path, guild_id)
        for member in members:
            member.setdefault("role", "Miembro")
            member.setdefault("gp", "")
    else:
        guild_name, guild_gp, members = _read_comlink_guild(input_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    outputs = {
        output_dir / f"syncguild_mock_{guild_id}_guilds.csv": (
            GUILDS_HEADERS,
            [[guild_id, guild_name, str(len(members)), guild_gp, "", "", ""]],
        ),
        output_dir / f"syncguild_mock_{guild_id}_players.csv": (
            PLAYERS_HEADERS,
            [
                [
                    row["player_id"].strip(),
                    row["player_name"].strip(),
                    "",
                    guild_name,
                    row["role"],
                    "",
                    row["gp"],
                    "",
                ]
                for row in members
            ],
        ),
        output_dir / f"syncguild_mock_{guild_id}_player_units.csv": (
            ["Guild Name", "Player Name"],
            [[guild_name, row["player_name"].strip()] for row in members],
        ),
        output_dir / f"syncguild_mock_{guild_id}_player_skills.csv": (
            ["Player Guild", "Player Name"],
            [[guild_name, row["player_name"].strip()] for row in members],
        ),
    }

    for path, (headers, rows) in outputs.items():
        _write_csv(path, headers, rows)
    return list(outputs)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("guild_id", help="guildId que se quiere simular")
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=None,
        help="usar un CSV histórico en vez del JSON de Comlink",
    )
    parser.add_argument(
        "--comlink-json",
        type=Path,
        default=Path("guild-comlink.json"),
        help="respuesta JSON guardada de Comlink (por defecto: guild-comlink.json)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("."),
        help="directorio donde guardar los CSV de salida",
    )
    args = parser.parse_args()

    input_path = args.snapshot or args.comlink_json
    try:
        outputs = mock_syncguild(
            args.guild_id,
            input_path,
            args.output_dir,
            input_is_snapshot=args.snapshot is not None,
        )
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    for path in outputs:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
