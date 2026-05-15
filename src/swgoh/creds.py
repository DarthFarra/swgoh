# src/swgoh/creds.py
from __future__ import annotations

import os
import json
import base64
import logging

from google.oauth2.service_account import Credentials

log = logging.getLogger(__name__)

# Minimal scopes: only the target spreadsheet, no broad Drive access.
# If you ever need to create new spreadsheets (not just open by ID/name),
# add "https://www.googleapis.com/auth/drive.file" — but not the full drive scope.
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


def _from_info(info: dict) -> Credentials:
    return Credentials.from_service_account_info(info, scopes=SCOPES)


def _try_parse_json(raw: str) -> dict | None:
    """Attempt to parse a string as JSON. Returns None on failure."""
    try:
        result = json.loads(raw)
        if not isinstance(result, dict):
            return None
        return result
    except (json.JSONDecodeError, ValueError):
        return None


def _try_decode_base64(raw: str) -> str | None:
    """Attempt to base64-decode a string. Returns None on failure."""
    try:
        return base64.b64decode(raw).decode("utf-8")
    except Exception:
        return None


def load_credentials() -> Credentials:
    """
    Load Google service account credentials from one of these sources (in order):

    1. SERVICE_ACCOUNT_FILE env var:
       - Path to a JSON file on disk
       - Raw JSON string
       - Base64-encoded JSON string
    2. GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_CREDENTIALS_JSON env var (raw JSON string)
    3. GOOGLE_SERVICE_ACCOUNT_BASE64 env var (base64-encoded JSON string)
    4. GOOGLE_APPLICATION_CREDENTIALS env var or default file 'service_account.json'

    Raises SystemExit if no credentials can be loaded.
    Never logs credential content.
    """
    # --- 1. SERVICE_ACCOUNT_FILE ---
    saf = os.getenv("SERVICE_ACCOUNT_FILE", "").strip()
    if saf:
        # 1a. Path to file on disk
        if os.path.isfile(saf):
            log.debug("Loading credentials from file path in SERVICE_ACCOUNT_FILE")
            return Credentials.from_service_account_file(saf, scopes=SCOPES)

        # 1b. Raw JSON string
        info = _try_parse_json(saf)
        if info:
            log.debug("Loading credentials from JSON string in SERVICE_ACCOUNT_FILE")
            return _from_info(info)

        # 1c. Base64-encoded JSON
        decoded = _try_decode_base64(saf)
        if decoded:
            info = _try_parse_json(decoded)
            if info:
                log.debug("Loading credentials from base64 JSON in SERVICE_ACCOUNT_FILE")
                return _from_info(info)

        log.warning(
            "SERVICE_ACCOUNT_FILE is set but could not be interpreted "
            "as a file path, JSON string, or base64 JSON. Trying other sources."
        )

    # --- 2. GOOGLE_SERVICE_ACCOUNT_JSON / GOOGLE_CREDENTIALS_JSON ---
    for env_var in ("GOOGLE_SERVICE_ACCOUNT_JSON", "GOOGLE_CREDENTIALS_JSON"):
        raw = os.getenv(env_var, "").strip()
        if raw:
            info = _try_parse_json(raw)
            if info:
                log.debug("Loading credentials from %s", env_var)
                return _from_info(info)
            log.warning("%s is set but is not valid JSON. Skipping.", env_var)

    # --- 3. GOOGLE_SERVICE_ACCOUNT_BASE64 ---
    b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_BASE64", "").strip()
    if b64:
        decoded = _try_decode_base64(b64)
        if decoded:
            info = _try_parse_json(decoded)
            if info:
                log.debug("Loading credentials from GOOGLE_SERVICE_ACCOUNT_BASE64")
                return _from_info(info)
        log.warning("GOOGLE_SERVICE_ACCOUNT_BASE64 is set but could not be decoded/parsed. Skipping.")

    # --- 4. GOOGLE_APPLICATION_CREDENTIALS or default file ---
    default_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
    if os.path.isfile(default_path):
        log.debug("Loading credentials from file: %s", default_path)
        return Credentials.from_service_account_file(default_path, scopes=SCOPES)

    raise SystemExit(
        "Google credentials not found. Set one of: SERVICE_ACCOUNT_FILE, "
        "GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SERVICE_ACCOUNT_BASE64, "
        "or GOOGLE_APPLICATION_CREDENTIALS."
    )
