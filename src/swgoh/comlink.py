# src/swgoh/comlink.py
from typing import Any, Dict
from .http import post_json_retry

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
def fetch_guild(identifier: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fuerza includeRecentGuildActivityInfo=true.
    Acepta tanto plano como dentro de 'identifier' por compatibilidad.
    """
    payloads = [
        {**identifier, "includeRecentGuildActivityInfo": True},
        {"identifier": identifier, "includeRecentGuildActivityInfo": True},
    ]
    return post_json_retry("/guild", payloads, attempts=8, base_sleep=1.3)

# --- /player ---
def fetch_player(identifier: Dict[str, Any]) -> Dict[str, Any]:
    """
    Idealmente sólo playerId; si también viene allycode, priorizamos playerId.
    """
    if "playerId" in identifier and "allycode" in identifier:
        identifier = {"playerId": identifier["playerId"]}
    payloads = [
        identifier,                 # plano
        {"identifier": identifier}  # fallback
    ]
    return post_json_retry("/player", payloads, attempts=8, base_sleep=1.3)
