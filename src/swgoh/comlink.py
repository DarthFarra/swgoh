# src/swgoh/comlink.py
import logging
from typing import Any, Dict
from .http import post_json_retry

log = logging.getLogger("comlink")

# --- /metadata ---
def fetch_metadata() -> Dict[str, Any]:
    """
    /metadata con 'payload' vacío; 'enums' opcional.
    """
    payloads = [
        {"payload": {}, "enums": False},
        {"payload": {}},
    ]
    return post_json_retry("/metadata", payloads, attempts=8, base_sleep=1.3)

# --- /data ---
def fetch_data_items(
    version: str,
    kind: str,                     # "units", "skill", ...
    include_pve_units: bool = False,
    request_segment: int = 0,
    device_platform: str = "Android",
) -> Dict[str, Any]:
    """
    Llama a /data usando el esquema:
    {
      "payload": {
        "version": "<version>",
        "includePveUnits": <bool>,
        "devicePlatform": "Android",
        "requestSegment": 0,
        "items": "<kind>"
      },
      "enums": false
    }

    Notas:
    - 'items' es STRING (no array).
    - 'requestSegment' y 'items' pueden coexistir en este schema, pero no usamos 'data' ni 'language'.
    """
    v = version or "latest"
    payloads = [
        {
            "payload": {
                "version": v,
                "includePveUnits": bool(include_pve_units),
                "devicePlatform": device_platform,
                "requestSegment": int(request_segment),
                "items": str(kind),
            },
            "enums": False,
        }
    ]
    return post_json_retry("/data", payloads, attempts=8, base_sleep=1.5)

# --- /guild ---
def fetch_guild(identifier: dict | str) -> dict:
    guild_id = identifier["guildId"] if isinstance(identifier, dict) else str(identifier)
    payload = {
        "payload": {
            "guildId": guild_id,
            "includeRecentGuildActivityInfo": True,  # pon False si lo quieres así
        },
        "enums": False,
    }
    return post_json_retry("/guild", [payload], attempts=8, base_sleep=1.3)

# --- /player ---
def fetch_player_by_id(player_id: str) -> dict:
    """
    Llama a /player usando SIEMPRE playerId.
    Payload: {"payload":{"playerId": "<id>"},"enums": false}
    """
    pid = str(player_id or "").strip()
    if not pid:
        raise ValueError("fetch_player_by_id: player_id vacío")

    payload = {
        "payload": { "playerId": pid },
        "enums": False
    }
    # Útil para ver el JSON exacto en logs (sube el nivel a DEBUG si quieres verlo)
    log.debug("POST /player payload=%s", payload)

    return post_json_retry("/player", payload, attempts=8, base_sleep=1.3)
