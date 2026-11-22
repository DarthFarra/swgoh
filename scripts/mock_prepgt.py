from __future__ import annotations

import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from swgoh.bot.services.gt_prep import (
    PlayerPrep,
    StatFailure,
    _combined_unit_stats,
    render_guild_prep,
    render_player_prep,
)
from swgoh.comlink import fetch_player_by_id


ROOT = Path(__file__).resolve().parent.parent
PLAYER_ID = "NCxMV3szQo6D5JtVpkAaxQ"
GUILD_ID = "pTtJHHuYQcSMinxQXXZJgA"
GUILD_NAME = "łmperio Mandaloriano Aroa'yr"
PLAYER_FILE = ROOT / f"player_{PLAYER_ID}.json"
GUILD_FILE = ROOT / "guild-comlink.json"
STATS_URL = "http://localhost:3223/api?flags=enums"

import urllib.request


def _calculated_units(player: dict) -> dict[str, dict]:
    request = urllib.request.Request(
        STATS_URL,
        data=json.dumps([player], ensure_ascii=False).encode("utf-8"),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=300) as response:
        calculated = json.loads(response.read().decode("utf-8"))
    roster = calculated[0].get("rosterUnit") or []
    return {
        str(unit.get("definitionId", "")).split(":", 1)[0]: unit
        for unit in roster
    }


def _evaluate_mock_player(player: dict, player_name: str) -> PlayerPrep:
    units = _calculated_units(player)
    failures = []

    leia = units.get("GLLEIA")
    if leia:
        stats = _combined_unit_stats(leia)
        if stats["Health"] is not None and stats["Health"] < 150000:
            failures.append(
                StatFailure(
                    "Leia Organa", "Health", stats["Health"], 150000, True
                )
            )
        if stats["Speed"] is not None and stats["Speed"] < 560:
            failures.append(
                StatFailure("Leia Organa", "Speed", stats["Speed"], 560, False)
            )

    jyn = units.get("JYNERSO")
    if jyn:
        stats = _combined_unit_stats(jyn)
        if stats["Speed"] is not None and stats["Speed"] < 310:
            failures.append(
                StatFailure("Jyn Erso", "Speed", stats["Speed"], 310, True)
            )
        elif stats["Speed"] is not None and stats["Speed"] < 340:
            failures.append(
                StatFailure("Jyn Erso", "Speed", stats["Speed"], 340, False)
            )

    return PlayerPrep(player_name, tuple(failures))


def _read_guild() -> tuple[str, list[tuple[str, str]]]:
    response = json.loads(GUILD_FILE.read_text(encoding="utf-8"))
    guild = response.get("guild", response)
    profile = guild.get("profile") or {}
    saved_guild_id = str(profile.get("id") or guild.get("id") or "").strip()
    if saved_guild_id and saved_guild_id != GUILD_ID:
        raise ValueError(
            f"{GUILD_FILE} pertenece al gremio {saved_guild_id}, no a {GUILD_ID}"
        )

    # The saved Comlink fixture lost the initial ł during its original export.
    guild_name = GUILD_NAME
    members = [
        (str(member.get("playerId")), str(member.get("playerName")))
        for member in guild.get("member", [])
        if member.get("playerId") and member.get("playerName")
    ]
    members.sort(key=lambda item: item[1].casefold())
    if not members:
        raise ValueError(f"{GUILD_FILE} no contiene miembros")
    return guild_name, members


player = json.loads(PLAYER_FILE.read_text(encoding="utf-8"))
player_name = player.get("name") or "Jugador mock"
prep = _evaluate_mock_player(player, player_name)
report = render_player_prep(prep, telegram=True)
output = ROOT / "prepgt_mock.txt"
output.write_text(report, encoding="utf-8")
print(output)

guild_name, members = _read_guild()


def _evaluate_member(player_id: str, member_name: str) -> PlayerPrep:
    member_player = player if player_id == PLAYER_ID else fetch_player_by_id(player_id)
    return _evaluate_mock_player(member_player, member_name)


guild_preps_by_id: dict[str, PlayerPrep] = {}
with ThreadPoolExecutor(max_workers=6) as executor:
    pending = {
        executor.submit(_evaluate_member, player_id, member_name): (
            player_id,
            member_name,
        )
        for player_id, member_name in members
    }
    for future in as_completed(pending):
        player_id, member_name = pending[future]
        try:
            guild_preps_by_id[player_id] = future.result()
        except Exception as exc:
            raise RuntimeError(
                f"No se pudo evaluar a {member_name} ({player_id})"
            ) from exc

guild_preps = [guild_preps_by_id[player_id] for player_id, _name in members]
guild_report = render_guild_prep(guild_name, guild_preps, telegram=True)
guild_output = ROOT / "prepgtguild_mock.txt"
guild_output.write_text(guild_report, encoding="utf-8")
print(guild_output)

