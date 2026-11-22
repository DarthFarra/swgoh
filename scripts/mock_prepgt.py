from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from swgoh.bot.services.gt_prep import (
    PlayerPrep,
    StatFailure,
    _combined_unit_stats,
    render_guild_prep,
    render_player_prep,
)


player_file = Path(r"C:\Users\salvador.martinez\Source\Repos\swgoh\player_NCxMV3szQo6D5JtVpkAaxQ.json")
player = json.loads(player_file.read_text(encoding="utf-8"))
player_name = player.get("name") or "Jugador mock"

import urllib.request

request = urllib.request.Request(
    "http://localhost:3223/api?flags=enums",
    data=json.dumps([player], ensure_ascii=False).encode("utf-8"),
    headers={"content-type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=300) as response:
    calculated = json.loads(response.read().decode("utf-8"))

roster = calculated[0].get("rosterUnit") or []
units = {
    str(unit.get("definitionId", "")).split(":", 1)[0]: unit
    for unit in roster
}

failures = []

leia = units.get("GLLEIA")
if leia:
    stats = _combined_unit_stats(leia)
    if stats["Health"] is not None and stats["Health"] < 100000:
        failures.append(
            StatFailure("Leia Organa", "Health", stats["Health"], 100000, True)
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

prep = PlayerPrep(player_name, tuple(failures))
report = render_player_prep(prep, telegram=True)
output = Path(r"C:\Users\salvador.martinez\Source\Repos\swgoh\prepgt_mock.txt")
output.write_text(report, encoding="utf-8")
print(output)

guild_preps = [
    prep,
    PlayerPrep(
        "Alpha",
        (
            StatFailure("Leia Organa", "Health", 50000, 100000, True),
        ),
    ),
    PlayerPrep("Beta", ()),
    PlayerPrep(
        "Gamma",
        (
            StatFailure("Jyn Erso", "Speed", 313, 340, False),
        ),
    ),
]
guild_report = render_guild_prep(
    "łmperio Mandaloriano",
    guild_preps,
    telegram=True,
)
guild_output = Path(r"C:\Users\salvador.martinez\Source\Repos\swgoh\prepgtguild_mock.txt")
guild_output.write_text(guild_report, encoding="utf-8")
print(guild_output)

