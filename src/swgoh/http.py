
# src/swgoh/http.py
import os
import json
import urllib.request
import urllib.error
from typing import Any, Dict
import socket
from urllib.parse import urlparse

def _preflight_tcp(url: str, timeout: float = 2.0) -> None:
    """Intenta conectar por TCP antes de hacer el POST para dar un error claro."""
    u = urlparse(url)
    host = u.hostname
    port = u.port or (443 if u.scheme == "https" else 80)
    if not host:
        return
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return
    except Exception as e:
        raise ConnectionError(f"No puedo conectar con {host}:{port} ({e}). ¿Servicio Comlink caído o puerto incorrecto?")

def post_json(path: str, payload: Dict[str, Any], timeout: float = 45.0) -> Dict[str, Any]:
    url = _join_url(COMLINK_BASE, path)
    # Preflight: opcional; si te molesta, comenta la siguiente línea
    _preflight_tcp(url)
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=HEADERS, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
        if not body:
            return {}
        return json.loads(body.decode("utf-8"))

# Base del servicio Comlink (con esquema http/https)
# Ejemplo en Railway (Private Networking): http://swgoh-comlink:3000
COMLINK_BASE = os.getenv("COMLINK_BASE", "http://swgoh-comlink:3000")

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}

def _join_url(base: str, path: str) -> str:
    base = (base or "").strip().rstrip("/")
    if not base.startswith(("http://", "https://")):
        # Evita errores tipo 'unknown url type'
        raise ValueError(f"COMLINK_BASE inválido: {base!r} (debe empezar por http:// o https://)")
    if not path.startswith("/"):
        path = "/" + path
    return base + path

def post_json(path: str, payload: Dict[str, Any], timeout: float = 45.0) -> Dict[str, Any]:
    """
    Realiza un POST JSON a COMLINK_BASE + path y devuelve dict (o {}).
    Lanza la excepción original ante HTTPError/URLError para que el caller decida qué hacer.
    """
    url = _join_url(COMLINK_BASE, path)
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=HEADERS, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
        if not body:
            return {}
        return json.loads(body.decode("utf-8"))

def post_json_retry(path: str, payloads: list[Dict[str, Any]], attempts: int = 4, base_sleep: float = 0.8) -> Dict[str, Any]:
    """
    Intenta múltiples payloads (fallbacks) y reintenta en caso de error.
    """
    import time
    last_exc = None
    for attempt in range(1, attempts + 1):
        for p in payloads:
            try:
                return post_json(path, p)
            except Exception as e:
                last_exc = e
                # probar siguiente payload
                continue
        if attempt < attempts:
            time.sleep(base_sleep * (attempt ** 1.5))
    if last_exc:
        raise last_exc
    return {}
