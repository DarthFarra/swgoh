# src/swgoh/sheets.py
from __future__ import annotations

import logging
import threading
from typing import Any, List

import gspread
from google.auth.transport.requests import Request

from .creds import load_credentials
from .config import SPREADSHEET_ID, SPREADSHEET_NAME

log = logging.getLogger(__name__)

# Lock ensures only one thread initializes the client/spreadsheet at a time.
# Under python-telegram-bot v20 (asyncio, single OS thread) this is mostly
# defensive, but it also protects the token-refresh path.
_lock = threading.Lock()
_gc: gspread.Client | None = None
_sh: gspread.Spreadsheet | None = None


def _refresh_if_needed(creds) -> None:
    """Refresh the credential token if it has expired or is about to expire."""
    if creds.expired or not creds.valid:
        try:
            creds.refresh(Request())
            log.debug("Google credentials refreshed successfully.")
        except Exception as e:
            log.error("Failed to refresh Google credentials: %s", e)
            raise


def _client() -> gspread.Client:
    global _gc
    with _lock:
        if _gc is None:
            creds = load_credentials()
            _gc = gspread.authorize(creds)
            log.debug("gspread client initialized.")
        else:
            # Refresh token if needed before reusing the existing client
            try:
                _refresh_if_needed(_gc.auth)
            except Exception:
                # On refresh failure, force re-initialization on next call
                _gc = None
                raise
    return _gc


def spreadsheet() -> gspread.Spreadsheet:
    global _sh
    with _lock:
        if _sh is not None:
            # Ensure credentials are still valid even for cached spreadsheet
            try:
                _refresh_if_needed(_sh.client.auth)
            except Exception:
                # Force full re-initialization
                _sh = None
                raise
            return _sh

        gc = _client()
        if SPREADSHEET_ID:
            _sh = gc.open_by_key(SPREADSHEET_ID)
        elif SPREADSHEET_NAME:
            _sh = gc.open(SPREADSHEET_NAME)
        else:
            raise SystemExit("Set SPREADSHEET_ID or SPREADSHEET_NAME in environment.")

        log.debug("Spreadsheet opened: %s", _sh.title)
    return _sh


def open_or_create(title: str) -> gspread.Worksheet:
    sh = spreadsheet()
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=1, cols=1)


def try_get_worksheet(title: str) -> gspread.Worksheet | None:
    sh = spreadsheet()
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return None


def write_sheet(
    ws: gspread.Worksheet,
    headers: List[str],
    rows: List[List[Any]],
    chunk_size: int = 500,
) -> None:
    """
    Clear the worksheet, write headers to row 1, then write data rows in chunks.
    Resizes the sheet to fit exactly before writing to avoid stale data.
    """
    need_rows = max(1 + len(rows), 1)
    need_cols = max(len(headers), 1)
    cur_rows = getattr(ws, "row_count", 0) or 0
    cur_cols = getattr(ws, "col_count", 0) or 0

    if need_rows > cur_rows or need_cols > cur_cols:
        ws.resize(rows=need_rows, cols=need_cols)

    ws.clear()
    ws.update(values=[headers], range_name="A1")

    if not rows:
        return

    start_row = 2
    for i in range(0, len(rows), chunk_size):
        block = rows[i : i + chunk_size]
        end_row = start_row + len(block) - 1
        rng = f"A{start_row}:{gspread.utils.rowcol_to_a1(end_row, need_cols)}"
        ws.update(values=block, range_name=rng, value_input_option="RAW")
        start_row = end_row + 1
