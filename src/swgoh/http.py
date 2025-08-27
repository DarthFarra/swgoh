import json, time, urllib.request
from urllib import error as urlerror
from .config import COMLINK_BASE, COMLINK_HEADERS_JSON, HTTP_RETRIES, HTTP_BACKOFF, HTTP_TIMEOUT

HEADERS = {"Content-Type": "application/json"}
try:
    if COMLINK_HEADERS_JSON:
        HEADERS.update(json.loads(COMLINK_HEADERS_JSON))
except Exception:
    pass

def _build_url(path: str) -> str:
    base = COMLINK_BASE.rstrip("/")
    path = path if path.startswith("/") else "/" + path
    return base + path

def post_json(path: str, payload: dict) -> dict:
    url = _build_url(path)
    data = json.dumps(payload).encode("utf-8")
    attempt = 0
    while True:
        attempt += 1
        req = urllib.request.Request(url, data=data, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                raw = resp.read().decode("utf-8")
                try:
                    return json.loads(raw)
                except Exception:
                    return {"raw": raw}
        except urlerror.HTTPError as e:
            status = e.code
            if status in (429, 500, 502, 503, 504) and attempt <= HTTP_RETRIES:
                sleep_s = HTTP_BACKOFF * (2 ** (attempt - 1))
                print(f"[WARN] {status} en {path}. Retry en {sleep_s:.1f}s ({attempt}/{HTTP_RETRIES})")
                time.sleep(sleep_s)
                continue
            raise SystemExit(f"HTTP {status} en {path}: {getattr(e, 'reason', '')}")
        except urlerror.URLError as e:
            if attempt <= HTTP_RETRIES:
                sleep_s = HTTP_BACKOFF * (2 ** (attempt - 1))
                print(f"[WARN] URLError en {path}: {e.reason}. Retry en {sleep_s:.1f}s ({attempt}/{HTTP_RETRIES})")
                time.sleep(sleep_s)
                continue
            raise SystemExit(f"URLError en {path}: {e.reason}")
