# src/swgoh/comlink.py
from __future__ import annotations

import logging
from typing import Any, Dict

from .http import post_json_retry

log = logging.getLogger("comlink")

# ----- /metadata -------------------------------------------------------------

def fetch_metadata() -> Dict[str, Any]:
    """
    POST /metadata
    Body: {"payload": {}, "enums": false}
    """
    body = {"payload": {}, "enums": False}
    log.debug("POST /metadata payload=%s", body)
    return post_json_retry("/metadata", body, attempts=6, base_sleep=1.2)

# ----- /data -----------------------------------------------------------------

def fetch_data_items(version: str, items: str) -> Dict[str, Any]:
    """
    POST /data
    Body:
    {
      "payload": {
        "version": "<version>",
        "includePveUnits": false,
        "devicePlatform": "Android",
        "requestSegment": 0,
        "items": "<items>"
      },
      "enums": false
    }
    Nota: 'requestSegment' y 'items' son mutuamente excluyentes; usamos siempre requestSegment=0.
    """
    body = {
        "payload": {
            "version": str(version),
            "includePveUnits": False,
            "devicePlatform": "Android",
            "requestSegment": 0,
            "items": str(items),
        },
        "enums": False,
    }
    log.debug("POST /data payload=%s", body)
    return post_json_retry("/data", body, attempts=8, base_sleep=1.5)

# ----- /guild ----------------------------------------------------------------

def fetch_guild(identifier: Dict[str, Any] | str) -> Dict[str, Any]:
    """
    POST /guild
    Body:
    {
      "payload": {
        "guildId": "<id>",
        "includeRecentGuildActivityInfo": true
      },
      "enums": false
    }
    """
    if isinstance(identifier, dict):
        gid = str(identifier.get("guildId", "")).strip()
    else:
        gid = str(identifier).strip()
    if not gid:
        raise ValueError("fetch_guild requiere guildId")

    body = {
        "payload": {
            "guildId": gid,
            "includeRecentGuildActivityInfo": True,
        },
        "enums": False,
    }
    log.debug("POST /guild payload=%s", body)
    return post_json_retry("/guild", body, attempts=8, base_sleep=1.3)

# ----- /player ---------------------------------------------------------------

def fetch_player_by_id(player_id: str) -> Dict[str, Any]:
    """
    POST /player usando SIEMPRE playerId.
    Body:
    {
      "payload": { "playerId": "<player_id>" },
      "enums": false
    }
    """
    pid = str(player_id or "").strip()
    if not pid:
        raise ValueError("fetch_player_by_id: player_id vacío")

    body = {"payload": {"playerId": pid}, "enums": False}
    log.debug("POST /player payload=%s", body)
    return post_json_retry("/player", body, attempts=8, base_sleep=1.3)

# (Compat opcional) Si en algún sitio antiguo llaman a fetch_player({"playerId":...})
def fetch_player(identifier: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compatibilidad: acepta {"playerId": "..."} y llama a /player con el schema correcto.
    (No acepta allycode; a partir de ahora siempre trabajamos por playerId)
    """
    if not isinstance(identifier, dict) or "playerId" not in identifier:
        raise ValueError("fetch_player requiere {'playerId': '<id>'}")
    return fetch_player_by_id(str(identifier["playerId"]))
