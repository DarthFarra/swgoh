# src/swgoh/comlink.py
from typing import Any, Dict, List
from .http import post_json, post_json_retry

# --- /metadata ---
def fetch_metadata() -> Dict[str, Any]:
    # reintentos con backoff por si el servicio despierta
    payloads = [
        {"payload": {}, "enums": False},
        {"payload": {}},  # fallback por si 'enums' causara 4xx en alguna versión
    ]
    return post_json_retry("/metadata", payloads, attempts=8, base_sleep=1.5)

# --- /data ---
def fetch_data_items(version: str, kind: str, request_segment: int | None = 0, include_pve_units: bool = False) -> dict:
    payload = {
        "version": version,
        "data": [{"type": kind}],
        "requestSegment": request_segment,  # requestSegment e items son excluyentes
        "includePveUnits": include_pve_units,
    }
    # reintentos por si hay cold-start/5xx
    return post_json_retry("/data", [payload], attempts=8, base_sleep=1.5)

# --- /guild ---
def fetch_guild(identifier: Dict[str, Any]) -> Dict[str, Any]:
    """
    Soporta guildId directo o dentro de "identifier".
    Fuerza includeRecentGuildActivityInfo=true.
    """
    payloads = [
        {**identifier, "includeRecentGuildActivityInfo": True},
        {"identifier": identifier, "includeRecentGuildActivityInfo": True},
    ]
    return post_json_retry("/guild", payloads)

# --- /player ---
def fetch_player(identifier: Dict[str, Any]) -> Dict[str, Any]:
    """
    Idealmente sólo playerId; si no, allycode. No mandes los dos a la vez.
    """
    if "playerId" in identifier and "allycode" in identifier:
        identifier = {"playerId": identifier["playerId"]}
    payloads = [
        identifier,
        {"identifier": identifier},  # fallback si la API lo requiere
    ]
    return post_json_retry("/player", payloads)
