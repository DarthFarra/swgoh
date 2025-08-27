from typing import Any, Dict, Optional
from .http import post_json

def fetch_metadata() -> Dict[str, Any]:
    return post_json("/metadata", {"payload": {}, "enums": False})

def fetch_data_items(version: str, items: str) -> Dict[str, Any]:
    return post_json("/data", {"payload": {
        "version": version,
        "items": items,
        "includePveUnits": False,
        "devicePlatform": "Android",
        "requestSegment": 0,
    }, "enums": False})

def fetch_guild(guild_id: str) -> Dict[str, Any]:
    return post_json("/guild", {"payload": {"guildId": str(guild_id), "includeRecentGuildActivityInfo": True}, "enums": False})

def fetch_player(player_id: Optional[str], ally_code: Optional[str]) -> Dict[str, Any]:
    if player_id:
        return post_json("/player", {"payload": {"playerId": str(player_id)}, "enums": False})
    if ally_code:
        return post_json("/player", {"payload": {"allyCode": str(ally_code)}, "enums": False})
    raise SystemExit("/player sin playerId ni allyCode")

def fetch_events() -> Dict[str, Any]:
    return post_json("/getEvents", {"payload": {}, "enums": False})
