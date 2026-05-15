# src/swgoh/bot/jobs/send_assignments_daily.py
from __future__ import annotations

import os
import json
import time
import random
import datetime
import unicodedata
import urllib.request
import logging
from typing import Any, Dict, List, Optional, Tuple, DefaultDict
from collections import defaultdict

import pytz
from gspread.exceptions import APIError

# ── shared infrastructure ────────────────────────────────────────────────────
# Use the single canonical credential + spreadsheet loader from the core.
# No credential duplication here.
from ...sheets import spreadsheet as open_spreadsheet   # ← shared client
# ─────────────────────────────────────────────────────────────────────────────

log = logging.getLogger(__name__)

# ==========
# Config env
# ==========
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ID_ZONA = os.getenv("ID_ZONA", "Europe/Madrid")
DEBUG_MODE = os.getenv("DEBUG_ASSIGNMENTS", "").strip().lower() in ("1", "true", "yes", "on")

SHEET_USUARIOS = os.getenv("USUARIOS_SHEET", "Usuarios")
SHEET_GUILDS   = os.getenv("GUILDS_SHEET", "Guilds")


# ==========
# gspread helpers with backoff
# ==========

def _with_backoff(fn, *args, **kwargs):
    """Execute a gspread call with retry on 429/5xx responses."""
    max_attempts = kwargs.pop("_attempts", 6)
    base_sleep   = kwargs.pop("_base_sleep", 0.6)
    for attempt in range(1, max_attempts + 1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            status  = getattr(getattr(e, "response", None), "status_code", None)
            msg     = str(e)
            transient = (
                status in (429, 500, 502, 503, 504)
                or "429" in msg
                or "Rate Limit" in msg
            )
            if transient and attempt < max_attempts:
                sleep = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                log.debug("gspread retry %d/%d after status=%s; sleeping %.2fs", attempt, max_attempts, status or "??", sleep)
                time.sleep(sleep)
                continue
            raise


def _read_all_values(ss, sheet_name: str) -> Tuple[List[str], List[List[str]]]:
    ws      = _with_backoff(ss.worksheet, sheet_name)
    vals    = _with_backoff(ws.get_all_values)
    headers = [h.strip() for h in (vals[0] if vals else [])]
    rows    = vals[1:] if len(vals) > 1 else []
    return headers, rows


# ==========
# Normalisation (accent-insensitive)
# ==========

def _strip_accents(s: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFD", s or "")
        if unicodedata.category(ch) != "Mn"
    )


def _slug(s: str) -> str:
    return " ".join(_strip_accents(str(s or "")).lower().split())


def _norm_cell(s: str) -> str:
    return _slug(s)


def _hmap(headers: List[str]) -> Dict[str, int]:
    return {_slug(h): i for i, h in enumerate(headers)}


def _find_col(hm: Dict[str, int], aliases: List[str]) -> int:
    for a in aliases:
        idx = hm.get(_slug(a), -1)
        if idx != -1:
            return idx
    return -1


def _gv_by_idx(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""


# Column aliases (ES/EN + accent variants)
HEADERS_ASSIGN = {
    "fase":      ["fase", "phase"],
    "planeta":   ["planeta", "planet"],
    "operacion": ["operacion", "operación", "operation"],
    "personaje": ["personaje", "character", "unit"],
    "jugador":   ["jugador", "player"],
    "user_id":   ["user_id", "userid", "user id", "telegram_user_id"],
}

HEADERS_USUARIOS = {
    "guild_name": ["guild_name", "guild name", "gremio", "nombre de gremio"],
    "chat_id":    ["chat_id", "chat id"],
    "user_id":    ["user_id", "userid", "user id", "telegram_user_id"],
    "alias":      ["alias", "player name", "jugador"],
}


# =========================
# Phase calculation
# =========================

def obtener_fase_actual() -> Optional[str]:
    """
    Returns the current ROTE phase string, or None if today is not a send day.
    - Sunday  → None
    - Even ISO week → phase = weekday + 1 (Mon=1 … Sat=6)
    - Odd ISO week  → None
    """
    tz  = pytz.timezone(ID_ZONA)
    hoy = datetime.datetime.now(tz)
    if hoy.weekday() >= 6:
        log.debug("Sunday: no assignments sent.")
        return None
    even_week = (hoy.isocalendar()[1] % 2) == 0
    phase = str(hoy.weekday() + 1) if even_week else None
    log.debug("Date=%s even_week=%s -> phase=%s", hoy.isoformat(), even_week, phase)
    return phase


# =========================
# Assignment index (one read per guild sheet)
# =========================

class AssignIndex:
    def __init__(
        self,
        sheet_name: str,
        idxs: Dict[str, int],
        rows: List[List[str]],
        fase: str,
    ):
        self.sheet_name  = sheet_name
        self.fase        = str(fase)
        self.idx_fase    = idxs["fase"]
        self.idx_planeta = idxs["planeta"]
        self.idx_oper    = idxs["operacion"]
        self.idx_pers    = idxs["personaje"]
        self.idx_userid  = idxs["user_id"]
        self.idx_jugador = idxs["jugador"]

        self.by_uid: DefaultDict[str, List[Tuple[str, str, str]]] = defaultdict(list)
        self.by_alias_norm: DefaultDict[str, List[Tuple[str, str, str]]] = defaultdict(list)

        total_fase = 0
        for r in rows:
            f = _gv_by_idx(r, self.idx_fase)
            if f != self.fase:
                continue
            total_fase += 1
            planeta = _gv_by_idx(r, self.idx_planeta) or "Sin planeta"
            oper    = _gv_by_idx(r, self.idx_oper)    or "Sin operación"
            pers    = _gv_by_idx(r, self.idx_pers)    or "Sin personaje"
            uid     = _gv_by_idx(r, self.idx_userid)
            if uid:
                self.by_uid[uid].append((planeta, oper, pers))
            else:
                jug = _gv_by_idx(r, self.idx_jugador)
                if jug:
                    self.by_alias_norm[_norm_cell(jug)].append((planeta, oper, pers))

        log.debug(
            "Index '%s' phase=%s: rows_in_phase=%d uids=%d aliases=%d",
            sheet_name, fase, total_fase, len(self.by_uid), len(self.by_alias_norm),
        )

    def build_message_for(
        self, guild_name: str, user_id: str, alias: str
    ) -> Optional[str]:
        items = self.by_uid.get(user_id)
        if not items and alias:
            items = self.by_alias_norm.get(_norm_cell(alias))
        if not items:
            return None

        por_planeta: DefaultDict[str, List[str]] = defaultdict(list)
        for planeta, oper, pers in items:
            por_planeta[planeta].append(f"- {pers} ({oper})")

        lines = [
            f"Asignaciones de *{alias or 'tu usuario'}* — *{guild_name}* (Fase {self.fase})",
            "",
        ]
        for planeta, asigns in por_planeta.items():
            lines.append(f" {planeta}:")
            lines.extend(asigns)
            lines.append("")
        return "\n".join(lines)


# =========================
# Telegram
# =========================

def _tg_send_message(
    token: str,
    chat_id: str | int,
    text: str,
    parse_mode: str = "Markdown",
) -> None:
    url  = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({
        "chat_id": int(chat_id),
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=data,
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        resp.read()


# =========================
# Main
# =========================

def main() -> int:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set.")

    # Use the shared spreadsheet client — credentials loaded once, centrally.
    ss = open_spreadsheet()

    fase = obtener_fase_actual()
    if not fase:
        log.info("[send_assignments_daily] Today is not a send day.")
        return 0
    log.info("[send_assignments_daily] Phase: %s", fase)

    # --- Read USUARIOS once ---
    u_headers, u_rows = _read_all_values(ss, SHEET_USUARIOS)
    if not u_rows:
        log.info("[send_assignments_daily] No registered users found.")
        return 0

    uhm       = _hmap(u_headers)
    idx_gname = _find_col(uhm, HEADERS_USUARIOS["guild_name"])
    idx_chat  = _find_col(uhm, HEADERS_USUARIOS["chat_id"])
    idx_uid   = _find_col(uhm, HEADERS_USUARIOS["user_id"])
    idx_alias = _find_col(uhm, HEADERS_USUARIOS["alias"])

    users: List[Tuple[str, str, str, str]] = []
    for r in u_rows:
        g  = _gv_by_idx(r, idx_gname)
        ch = _gv_by_idx(r, idx_chat)
        ui = _gv_by_idx(r, idx_uid)
        al = _gv_by_idx(r, idx_alias)
        if g and ch and ui:
            users.append((g, ch, ui, al))
    log.debug("Valid users: %d", len(users))

    # --- Read GUILDS once, map Guild Name → ROTE sheet ---
    g_headers, g_rows = _read_all_values(ss, SHEET_GUILDS)
    ghm            = _hmap(g_headers)
    idx_guild_name = _find_col(ghm, ["Guild Name", "guild_name", "gremio"])
    idx_rote       = _find_col(ghm, ["ROTE"])

    guild_to_rote: Dict[str, str] = {}
    for r in g_rows:
        gname = _gv_by_idx(r, idx_guild_name)
        rote  = _gv_by_idx(r, idx_rote)
        if gname and rote:
            guild_to_rote[gname] = rote
    log.debug("Guilds with ROTE configured: %d", len(guild_to_rote))

    # --- Group users by guild ---
    per_guild: DefaultDict[str, List[Tuple[str, str, str]]] = defaultdict(list)
    for g, ch, ui, al in users:
        per_guild[g].append((ch, ui, al))

    sent    = 0
    skipped = 0

    for guild_name, lst in per_guild.items():
        sheet_name = guild_to_rote.get(guild_name) or "Asignaciones ROTE"

        try:
            a_headers, a_rows = _read_all_values(ss, sheet_name)
        except Exception as e:
            log.warning("Cannot open sheet '%s' for guild '%s': %s", sheet_name, guild_name, e)
            skipped += len(lst)
            continue

        if not a_rows:
            log.debug("Sheet '%s' is empty for guild '%s'.", sheet_name, guild_name)
            skipped += len(lst)
            continue

        ahm  = _hmap(a_headers)
        idxs = {
            "fase":      _find_col(ahm, HEADERS_ASSIGN["fase"]),
            "planeta":   _find_col(ahm, HEADERS_ASSIGN["planeta"]),
            "operacion": _find_col(ahm, HEADERS_ASSIGN["operacion"]),
            "personaje": _find_col(ahm, HEADERS_ASSIGN["personaje"]),
            "user_id":   _find_col(ahm, HEADERS_ASSIGN["user_id"]),
            "jugador":   _find_col(ahm, HEADERS_ASSIGN["jugador"]),
        }

        if min(idxs.values()) == -1:
            log.warning("Missing required columns in '%s' for guild '%s'.", sheet_name, guild_name)
            skipped += len(lst)
            continue

        assign_index = AssignIndex(sheet_name, idxs, a_rows, fase)

        for chat_id, user_id, alias in lst:
            try:
                msg = assign_index.build_message_for(guild_name, user_id, alias)
                if not msg:
                    skipped += 1
                    log.debug("No assignments: guild=%s user_id=%s alias='%s'", guild_name, user_id, alias)
                    continue
                _tg_send_message(TELEGRAM_BOT_TOKEN, chat_id, msg, parse_mode="Markdown")
                sent += 1
                time.sleep(0.05)
            except Exception as e:
                log.warning("Failed to send to chat %s: %s", chat_id, e)
                skipped += 1

    log.info("[send_assignments_daily] Phase %s: sent=%d skipped=%d", fase, sent, skipped)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
