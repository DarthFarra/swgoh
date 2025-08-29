from typing import Any, Dict
from .http import post_json_retry

def fetch_metadata() -> Dict[str, Any]:
    payloads = [
        {"payload": {}, "enums": False},
        {"payload": {}},
    ]
    return post_json_retry("/metadata", payloads, attempts=8, base_sleep=1.3)

def fetch_data_items(version: str, kind: str, request_segment: int | None = 0, include_pve_units: bool = False) -> Dict[str, Any]:
    """
    Endpoint /data con fallbacks de payload. 'kind' suele ser: 'units', 'skill', etc.
    """
    base = {
        "version": version,
        "language": "ENG_US",
    }
    # requestSegment e items son excluyentes -> usamos requestSegment
    payloads = [
        {**base, "data": [{"type": kind}], "requestSegment": request_segment, "includePveUnits": include_pve_units},
        {**base, "data": [{"id": kind, "type": kind}], "requestSegment": request_segment, "includePveUnits": include_pve_units},
        {**base, "data": [{"type": kind}], "requestSegment": request_segment},
        {**base, "data": [{"type": kind}]},
    ]
    return post_json_retry("/data", payloads, attempts=8, base_sleep=1.5)

def fetch_guild(identifier: Dict[str, Any]) -> Dict[str, Any]:
    payloads = [
        {**identifier, "includeRecentGuildActivityInfo": True},
        {"identifier": identifier, "includeRecentGuildActivityInfo": True},
    ]
    return post_json_retry("/guild", payloads, attempts=8, base_sleep=1.3)

def fetch_player(identifier: Dict[str, Any]) -> Dict[str, Any]:
    if "playerId" in identifier and "allycode" in identifier:
        identifier = {"playerId": identifier["playerId"]}
    payloads = [
        identifier,
        {"identifier": identifier},
    ]
    return post_json_retry("/player", payloads, attempts=8, base_sleep=1.3)
