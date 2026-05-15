# src/swgoh/http.py
from __future__ import annotations

import os
import json
import time
import logging
import urllib.request
import urllib.error
from typing import Any, Union, List

log = logging.getLogger(__name__)

# ==========
# Base URL
# ==========
COMLINK_BASE = os.getenv("COMLINK_BASE", "").strip()
if not COMLINK_BASE:
    raise RuntimeError("COMLINK_BASE environment variable is not set.")
if COMLINK_BASE.endswith("/"):
    COMLINK_BASE = COMLINK_BASE[:-1]

_HEADERS = {
    "content-type": "application/json",
    "accept": "application/json",
}

# Maximum number of bytes from an error response body included in log messages.
# Keeps logs useful without risking leaking large payloads.
_MAX_ERROR_BODY_LOG = 300


def _to_json_bytes(data: Any) -> bytes:
    """
    Serialize to JSON bytes exactly once:
      - dict/list  -> json.dumps (compact)
      - str        -> assumed pre-serialized JSON; encode as-is
      - bytes      -> passed through unchanged
    Raises TypeError for any other type.
    """
    if isinstance(data, (dict, list)):
        return json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    if isinstance(data, str):
        return data.encode("utf-8")
    raise TypeError(f"Unsupported body type for JSON request: {type(data)!r}")


def _norm_path(path: str) -> str:
    return path if path.startswith("/") else f"/{path}"


def _sanitize_error_body(raw: str) -> str:
    """
    Truncate error response bodies before logging to avoid leaking
    large or potentially sensitive payloads.
    """
    if len(raw) > _MAX_ERROR_BODY_LOG:
        return raw[:_MAX_ERROR_BODY_LOG] + "…[truncated]"
    return raw


def _request(path: str, body: Union[dict, list, str, bytes], timeout: float = 30.0) -> dict:
    url = f"{COMLINK_BASE}{_norm_path(path)}"
    data = _to_json_bytes(body)
    req = urllib.request.Request(url, data=data, headers=_HEADERS, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                return json.loads(raw)
            except json.JSONDecodeError as e:
                # Log a truncated preview — never the full body
                log.error(
                    "Non-JSON response from %s: %s | preview=%s",
                    path,
                    e,
                    _sanitize_error_body(raw),
                )
                raise
    except urllib.error.HTTPError as e:
        err_body = _sanitize_error_body(e.read().decode("utf-8", errors="replace"))
        # Raise without embedding body in the exception message chain —
        # the log line is sufficient; callers don't need the raw body.
        log.error("HTTP %s %s at %s | body_preview=%s", e.code, e.reason, path, err_body)
        raise RuntimeError(f"HTTP {e.code} {e.reason} at {path}") from None
    except urllib.error.URLError as e:
        log.error("URL error at %s: %s", path, e)
        raise RuntimeError(f"URL error at {path}: {e}") from None


# ==========
# Public API
# ==========

def post_json(
    path: str,
    body: Union[dict, list, str, bytes],
    timeout: float = 30.0,
) -> dict:
    """Single POST with no retries."""
    return _request(path, body, timeout=timeout)


def post_json_retry(
    path: str,
    body_or_variants: Union[dict, list, str, bytes, List[Union[dict, list, str, bytes]]],
    attempts: int = 5,
    base_sleep: float = 1.2,
    timeout: float = 30.0,
) -> dict:
    """
    POST with exponential-backoff retries.

    body_or_variants may be:
      - A single dict/list/str/bytes  -> tried on every attempt
      - A list of variants            -> tried in order per attempt round

    Note: to send an actual JSON array, pass it as a pre-serialized str,
    otherwise it will be treated as a list of variants.
    """
    # Normalize to a list of variants
    if (
        isinstance(body_or_variants, list)
        and body_or_variants
        and isinstance(body_or_variants[0], (dict, list, str, bytes))
    ):
        variants: list = body_or_variants
    else:
        variants = [body_or_variants]

    last_exc: Exception | None = None
    sleep = base_sleep

    for attempt in range(1, attempts + 1):
        for body in variants:
            try:
                return _request(path, body, timeout=timeout)
            except Exception as e:
                last_exc = e
                log.debug(
                    "POST %s failed (attempt %d/%d): %r — retrying in %.1fs",
                    path,
                    attempt,
                    attempts,
                    e,
                    sleep,
                )
        time.sleep(sleep)
        sleep *= 1.6

    assert last_exc is not None
    log.error("POST %s failed after %d attempts. Last error: %r", path, attempts, last_exc)
    raise last_exc
