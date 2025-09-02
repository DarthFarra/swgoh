import os
import json
import time
import urllib.request
import urllib.error
from typing import Any, Dict

COMLINK_BASE = (os.getenv("COMLINK_BASE", "").strip().rstrip("/"))
if not COMLINK_BASE or not COMLINK_BASE.startswith(("http://", "https://")):
    raise RuntimeError(f"COMLINK_BASE inválido o ausente: {repr(os.getenv('COMLINK_BASE', ''))}")


HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}

def _join_url(base: str, path: str) -> str:
    if not base or not base.startswith(("http://", "https://")):
        raise ValueError(f"COMLINK_BASE inválido: {base!r}")
    if not path.startswith("/"):
        path = "/" + path
    return base + path

def post_json(path: str, payload: Dict[str, Any], timeout: float = 45.0) -> Dict[str, Any]:
    url = _join_url(COMLINK_BASE, path)
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=HEADERS, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return json.loads(body.decode("utf-8")) if body else {}
    except urllib.error.HTTPError as e:
        # Super útil: ver el cuerpo del error (razón de 400/500)
        err_body = ""
        try:
            err_body = e.read().decode("utf-8", "replace")
        except Exception:
            pass
        raise RuntimeError(f"HTTP {e.code} {e.reason} at {url} | body={err_body[:600]}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"URL error at {url}: {e}")

def post_json_retry(path: str, payloads: list[Dict[str, Any]], attempts: int = 6, base_sleep: float = 1.0) -> Dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        for p in payloads:
            try:
                return post_json(path, p)
            except Exception as e:
                last_exc = e
                # probamos siguiente payload; luego backoff
                continue
        if attempt < attempts:
            time.sleep(base_sleep * (attempt ** 1.4))
    if last_exc:
        raise last_exc
    return {}
