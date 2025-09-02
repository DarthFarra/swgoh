# src/swgoh/comlink.py
from __future__ import annotations
import json
import logging
import urllib.request
from typing import Any, Dict
from .http import COMLINK_BASE  # usamos la URL base ya validada

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
def fetch_player_by_id(player_id: str) -> Dict[str, Any]:
    """
    Llama a /player usando SIEMPRE playerId con el esquema:
    {"payload":{"playerId":"..."},"enums":false}
    Evitamos doble-serialización y forzamos Content-Type correcto.
    """
    pid = str(player_id or "").strip()
    if not pid:
        raise ValueError("fetch_player_by_id: player_id vacío")

    url = COMLINK_BASE.rstrip("/") + "/player"
    body_obj = {"payload": {"playerId": pid}, "enums": False}
    body_bytes = json.dumps(body_obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=body_bytes,
        headers={"content-type": "application/json"},
        method="POST",
    )

    # Log DEBUG para inspección del payload real
    log.debug("POST %s payload=%s", url, body_obj)

    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        try:
            return json.loads(raw)
        except Exception as e:
            log.error("Respuesta no JSON de /player: %s | body=%s", e, raw[:300])
            raise
