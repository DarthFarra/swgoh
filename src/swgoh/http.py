# src/swgoh/http.py
from __future__ import annotations

import os
import json
import time
import logging
import urllib.request
import urllib.error
from typing import Any, Union, List

log = logging.getLogger("http")

# ==========
# Base URL
# ==========
COMLINK_BASE = os.getenv("COMLINK_BASE", "").strip()
if not COMLINK_BASE:
    raise RuntimeError("Falta COMLINK_BASE")
if COMLINK_BASE.endswith("/"):
    COMLINK_BASE = COMLINK_BASE[:-1]

HEADERS = {
    "content-type": "application/json",
    "accept": "application/json",
}

# ==========
# Serialización segura (una sola vez)
# ==========
def _to_json_bytes(data: Any) -> bytes:
    """
    Serializa solo una vez:
      - dict/list -> json.dumps (compacto)
      - str       -> se asume JSON ya serializado -> enviar tal cual (NO volver a hacer dumps)
      - bytes     -> tal cual
    """
    if isinstance(data, (dict, list)):
        return json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    if isinstance(data, str):
        return data.encode("utf-8")
    raise TypeError(f"Tipo no soportado para cuerpo JSON: {type(data)}")

def _norm_path(path: str) -> str:
    return path if path.startswith("/") else f"/{path}"

def _request(path: str, body: Union[dict, list, str, bytes], timeout: float = 30.0) -> dict:
    url = f"{COMLINK_BASE}{_norm_path(path)}"
    data = _to_json_bytes(body)
    req = urllib.request.Request(url, data=data, headers=HEADERS, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                return json.loads(raw)
            except Exception as e:
                log.error("Respuesta no JSON de %s: %s | body=%s", url, e, raw[:300])
                raise
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} {e.reason} at {url} | body={err_body}") from None
    except urllib.error.URLError as e:
        raise RuntimeError(f"URL error at {url}: {e}") from None

# ==========
# API pública
# ==========
def post_json(path: str, body: Union[dict, list, str, bytes], timeout: float = 30.0) -> dict:
    """Una sola llamada POST (sin reintentos)."""
    return _request(path, body, timeout=timeout)

def post_json_retry(
    path: str,
    body_or_variants: Union[dict, list, str, bytes, List[Union[dict, list, str, bytes]]],
    attempts: int = 5,
    base_sleep: float = 1.2,
    timeout: float = 30.0,
) -> dict:
    """
    Reintentos con backoff exponencial.
    Admite:
      - dict/list/str/bytes  -> se envía tal cual (serializado una única vez)
      - lista de variantes    -> se prueban en orden en cada intento
    Nota: si necesitas enviar realmente un JSON array, pásalo como 'str' pre-serializado.
    """
    # Normalizar a lista de variantes
    if isinstance(body_or_variants, list) and body_or_variants and isinstance(body_or_variants[0], (dict, list, str, bytes)):
        variants = body_or_variants
    else:
        variants = [body_or_variants]

    last_exc: Exception | None = None
    sleep = base_sleep
    for _ in range(attempts):
        for body in variants:
            try:
                return _request(path, body, timeout=timeout)
            except Exception as e:
                last_exc = e
                log.debug("POST %s falló con %r; reintentando…", path, e)
                time.sleep(sleep)
        sleep *= 1.6
    assert last_exc is not None
    raise last_exc
