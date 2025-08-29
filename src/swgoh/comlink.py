# src/swgoh/comlink.py
from typing import Any, Dict, List
from .http import post_json_retry

def fetch_metadata() -> Dict[str, Any]:
    payloads = [
        {"payload": {}, "enums": False},
        {"payload": {}},
    ]
    return post_json_retry("/metadata", payloads, attempts=8, base_sleep=1.3)

def _compact(d: Dict[str, Any]) -> Dict[str, Any]:
    """Elimina claves con valor None (no toca False/0)."""
    return {k: v for k, v in d.items() if v is not None}

def fetch_data_items(version: str, kind: str, request_segment: int | None = 0, include_pve_units: bool = False) -> Dict[str, Any]:
    """
    Llama a /data con 'payload' (requerido por el schema).
    Probaremos varias formas compatibles según el despliegue de Comlink.
    """
    base_payload = {
        "version": version or "latest",
        "language": "ENG_US",
        "requestSegment": request_segment,            # requestSegment e items son excluyentes; usamos RS por defecto
        "includePveUnits": include_pve_units,
    }

    # Variantes (todas dentro de {"payload": ...})
    variants: List[Dict[str, Any]] = []

    # v1: data [{type}]
    variants.append({"payload": _compact({**base_payload, "data": [{"type": kind}]})})
    # v2: data [{id,type}]
    variants.append({"payload": _compact({**base_payload, "data": [{"id": kind, "type": kind}]})})
    # v3: igual que v1 pero sin includePveUnits (por si el schema no lo acepta)
    v3 = dict(base_payload)
    v3.pop("includePveUnits", None)
    variants.append({"payload": _compact({**v3, "data": [{"type": kind}]})})
    # v4: igual que v1 pero sin requestSegment (por si RS no es admitido)
    v4 = dict(base_payload)
    v4.pop("requestSegment", None)
    variants.append({"payload": _compact({**v4, "data": [{"type": kind}]})})
    # v5: items [{type}] (fallback absoluto, aunque RS y items son excluyentes)
    v5 = dict(base_payload)
    v5.pop("requestSegment", None)
    variants.append({"payload": _compact({**v5, "items": [{"type": kind}]})})
    # v6: items [{id}]
    variants.append({"payload": _compact({**v5, "items": [{"id": kind}]})})

    return post_json_retry("/data", variants, attempts=8, base_sleep=1.5)

def fetch_guild(identifier: Dict[str, Any]) -> Dict[str, Any]:
    payloads = [
        {**identifier, "includeRecentGuildActivityInfo": True},
        {"identifier": identifier, "includeRecentGuildActivityInfo": True},
    ]
    return post_json_retry("/guild", payloads, attempts=8, base_sleep=1.3)

def fetch_player(identifier: Dict[str, Any]) -> Dict[str, Any]:
    # Idealmente solo playerId; si viene allycode también, nos quedamos con playerId.
    if "playerId" in identifier and "allycode" in identifier:
        identifier = {"playerId": identifier["playerId"]}
    payloads = [
        identifier,
        {"identifier": identifier},
    ]
    return post_json_retry("/player", payloads, attempts=8, base_sleep=1.3)
