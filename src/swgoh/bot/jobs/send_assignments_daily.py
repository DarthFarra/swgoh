# src/swgoh/bot/jobs/send_assignments_daily.py
import os
import json
import base64
import datetime
import unicodedata
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

import pytz
import gspread
from google.oauth2.service_account import Credentials

# ==========
# Config env
# ==========
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_ENV = "SERVICE_ACCOUNT_FILE"  # JSON directo / base64 / ruta
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ID_ZONA = os.getenv("ID_ZONA", "Europe/Madrid")  # zona horaria para fase

SHEET_USUARIOS = os.getenv("USUARIOS_SHEET", "Usuarios")
SHEET_GUILDS   = os.getenv("GUILDS_SHEET", "Guilds")

# ==========
# Helpers GS
# ==========
def _load_service_account_creds():
    raw = os.getenv(SERVICE_ACCOUNT_ENV)
    if not raw:
        raise RuntimeError(f"Variable de entorno {SERVICE_ACCOUNT_ENV} no definida")

    def try_json(s: str) -> Optional[dict]:
        try:
            return json.loads(s)
        except Exception:
            return None

    info = try_json(raw)
    if info is None:
        # base64
        try:
            decoded = base64.b64decode(raw).decode("utf-8")
            info = try_json(decoded)
        except Exception:
            info = None

    if info is None:
        # ruta a archivo
        try:
            with open(raw, "r", encoding="utf-8") as f:
                info = json.load(f)
        except Exception as e:
            raise RuntimeError(
                f"No pude interpretar {SERVICE_ACCOUNT_ENV} como JSON/base64/ruta: {e}"
            )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)

def _open_sheet():
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID")
    client = gspread.authorize(_load_service_account_creds())
    return client.open_by_key(SPREADSHEET_ID)

def _read_all_values(ss, sheet_name: str) -> Tuple[List[str], List[List[str]]]:
    ws = ss.worksheet(sheet_name)
    vals = ws.get_all_values() or []
    headers = [h.strip() for h in (vals[0] if vals else [])]
    rows = vals[1:] if len(vals) > 1 else []
    return headers, rows

def _hmap(headers: List[str]) -> Dict[str, int]:
    return {h.strip().lower(): i for i, h in enumerate(headers)}

def _gv(row: List[str], hmap: Dict[str, int], name: str) -> str:
    i = hmap.get(name.lower(), -1)
    return (row[i].strip() if 0 <= i < len(row) else "")

def _sanitize_ally(s: str) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFC", str(s or ""))
    return " ".join(s.split()).strip().lower()

# =========================
# Cálculo de fase (como el original)
# =========================
def obtener_fase_actual() -> Optional[str]:
    """
    - Si es domingo -> None (no se envía)
    - Si la semana ISO es PAR -> fase = weekday+1 (L=1..S=6)
    - Si la semana es IMPAR -> None (no se envía)
    """
    tz = pytz.timezone(ID_ZONA)
    hoy = datetime.datetime.now(tz)
    if hoy.weekday() >= 6:  # domingo (0=lunes .. 6=domingo)
        return None
    semana_par = (hoy.isocalendar()[1] % 2) == 0
    return str(hoy.weekday() + 1) if semana_par else None

# =========================
# Lógica de asignaciones
# =========================
def _assignments_sheet_for_guild(ss, guild_name: str) -> Optional[str]:
    headers, rows = _read_all_values(ss, SHEET_GUILDS)
    if not headers:
        return None
    hm = _hmap(headers)
    for r in rows:
        gname = _gv(r, hm, "Guild Name")
        rote  = _gv(r, hm, "ROTE")
        if gname and rote and gname == guild_name:
            try:
                ss.worksheet(rote)
                return rote
            except Exception:
                return None
    return None

def _build_assignments_text(ss, sheet_name: str, guild_name: str, fase: str, user_id: str, alias: str) -> Optional[str]:
    try:
        headers, rows = _read_all_values(ss, sheet_name)
    except Exception:
        return None
    if not headers:
        return None

    hm = _hmap(headers)

    por_planeta: Dict[str, List[str]] = {}
    for row in rows:
        f = _gv(row, hm, "fase")
        if f != str(fase):
            continue

        uid_cell = _gv(row, hm, "user_id")
        if uid_cell:
            if uid_cell != user_id:
                continue
        else:
            jugador = _gv(row, hm, "jugador")
            if not jugador or not alias or _norm(jugador) != _norm(alias):
                continue

        planeta = _gv(row, hm, "planeta") or "Sin planeta"
        oper    = _gv(row, hm, "operacion") or "Sin operación"
        pers    = _gv(row, hm, "personaje") or "Sin personaje"
        por_planeta.setdefault(planeta, []).append(f"- {pers} ({oper})")

    if not por_planeta:
        return None

    lines = [f"Asignaciones de *{alias or 'tu usuario'}* — *{guild_name}* (Fase {fase})", ""]
    for planeta, asigns in por_planeta.items():
        lines.append(f" {planeta}:")
        lines.extend(asigns)
        lines.append("")
    return "\n".join(lines)

# =========================
# Telegram
# =========================
def _tg_send_message(token: str, chat_id: str | int, text: str, parse_mode: str = "Markdown") -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({
        "chat_id": int(chat_id),
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"content-type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=20) as resp:
        resp.read()  # ignoramos cuerpo

# =========================
# Main
# =========================
def main() -> int:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN")
    ss = _open_sheet()

    fase = obtener_fase_actual()
    if not fase:
        print("[send_assignments_daily] Hoy no se envía (fuera de ventana de fases).")
        return 0

    # Leer usuarios
    ws_u = ss.worksheet(SHEET_USUARIOS)
    u_vals = ws_u.get_all_values() or []
    if len(u_vals) < 2:
        print("[send_assignments_daily] No hay usuarios registrados.")
        return 0
    u_headers = [h.strip() for h in u_vals[0]]
    um = _hmap(u_headers)

    sent = 0
    skipped = 0

    for row in u_vals[1:]:
        try:
            guild_name = _gv(row, um, "guild_name")
            chat_id    = _gv(row, um, "chat_id")
            user_id    = _gv(row, um, "user_id")
            alias      = _gv(row, um, "alias")

            if not guild_name or not chat_id or not user_id:
                skipped += 1
                continue

            sheet_name = _assignments_sheet_for_guild(ss, guild_name) or "Asignaciones ROTE"
            msg = _build_assignments_text(ss, sheet_name, guild_name, fase, user_id, alias)
            if not msg:
                skipped += 1
                continue

            _tg_send_message(TELEGRAM_BOT_TOKEN, chat_id, msg, parse_mode="Markdown")
            sent += 1
        except Exception as e:
            # No paramos todo el job por un usuario que falle
            print(f"[WARN] Fallo enviando a chat { _gv(row, um, 'chat_id') }: {e}")
            skipped += 1
            continue

    print(f"[send_assignments_daily] Fase {fase}: enviados={sent}, omitidos={skipped}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
