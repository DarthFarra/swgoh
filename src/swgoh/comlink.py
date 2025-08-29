# src/swgoh/comlink.py
from typing import Any, Dict, List
from .http import post_json, post_json_retry

# --- /metadata ---
def fetch_metadata() -> Dict[str, Any]:
    # Ajustado a tu llamada original
    return post_json("/metadata", {"payload": {}, "enums": False})

# --- /data ---
def fetch_data_items(version: str, kind: str, request_segment: int | None = 0, include_pve_units: bool = False) -> Dict[str, Any]:
    """
    Ejemplo para /data (unidades, skills, etc.)
    """
    payload = {
        "version": version,
        "data": [{"type": kind}],
        # requestSegment e items son excluyentes; usamos requestSegment=0 por defecto
        "requestSegment": request_segment,
        "includePveUnits": include_pve_units,
    }
    return post_json("/data", payload)

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
