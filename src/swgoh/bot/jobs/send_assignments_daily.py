import os
import json
import base64
import asyncio
import traceback
from typing import Any, Dict, List, Tuple

import gspread
from google.oauth2.service_account import Credentials
from telegram import Bot

# ====== CONFIG (según tu petición) ======
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_ENV = "SERVICE_ACCOUNT_FILE"

SHEET_USUARIOS = os.getenv("USUARIOS_SHEET", "Usuarios")
SHEET_ASIGNACIONES = os.getenv("ASIGNACIONES_SHEET", "Asignaciones ROTE")

# Envío por fase (opcional)
ASSIGNMENTS_PHASE = os.getenv("ASSIGNMENTS_PHASE", "").strip()  # si vacío, enviará todas

# ====== Google Sheets ======
def _load_service_account_creds() -> Credentials:
    raw = os.getenv(SERVICE_ACCOUNT_ENV)
    if not raw:
        raise RuntimeError(f"Variable de entorno {SERVICE_ACCOUNT_ENV} no definida")

    def try_json(s: str):
        try:
            return json.loads(s)
        except Exception:
            return None

    info = try_json(raw)
    if info is None:
        # ¿base64?
        try:
            decoded = base64.b64decode(raw).decode("utf-8")
            info = try_json(decoded)
        except Exception:
            info = None

    if info is None:
        # ¿ruta a archivo?
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


def open_spreadsheet():
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID")
    credentials = _load_service_account_creds()
    client = gspread.authorize(credentials)
    return client.open_by_key(SPREADSHEET_ID)


def read_sheet(spreadsheet, name: str) -> List[Dict[str, Any]]:
    ws = spreadsheet.worksheet(name)
    rows = ws.get_all_records()
    return [{(k or "").strip().lower(): v for k, v in r.items()} for r in rows]


# ====== Mensajes ======
def build_message(alias: str, fase: str, rows: List[Dict[str, Any]]) -> str:
    por_planeta: Dict[str, List[str]] = {}
    for r in rows:
        planeta = str(r.get("planeta", "Sin planeta")).strip()
        oper = str(r.get("operacion", "Sin operación")).strip()
        pers = str(r.get("personaje", "Sin personaje")).strip()
        por_planeta.setdefault(planeta, []).append(f"- {pers} ({oper})")

    if not por_planeta:
        return f"Asignaciones de {alias} (Fase {fase})\n\nNo tienes asignaciones registradas."

    lines = [f"Asignaciones de {alias} (Fase {fase})", ""]
    for planeta, asigns in por_planeta.items():
        lines.append(f" {planeta}:")
        lines.extend(asigns)
        lines.append("")
    return "\n".join(lines)


# ====== Main ======
async def main_async():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN")
    bot = Bot(token=TELEGRAM_TOKEN)

    ss = open_spreadsheet()

    usuarios = read_sheet(ss, SHEET_USUARIOS)   # user_id, chat_id, username, alias, rol
    asign = read_sheet(ss, SHEET_ASIGNACIONES)  # fase, planeta, operacion, personaje, jugador, user_id, chat_id

    alias_by_uid = {str(u.get("user_id", "")).strip(): str(u.get("alias", "")).strip() for u in usuarios}
    chat_by_uid = {str(u.get("user_id", "")).strip(): str(u.get("chat_id", "")).strip() for u in usuarios}

    fases_target = [ASSIGNMENTS_PHASE] if ASSIGNMENTS_PHASE else sorted({str(r.get("fase", "")).strip() for r in asign if str(r.get("fase", "")).strip()})
    enviados = 0
    errores = 0

    for fase in fases_target:
        rows_by_uid: Dict[str, List[Dict[str, Any]]] = {}
        for r in asign:
            if str(r.get("fase", "")).strip() != str(fase):
                continue
            uid = str(r.get("user_id", "")).strip()
            rows_by_uid.setdefault(uid, []).append(r)

        for uid, rows in rows_by_uid.items():
            chat_id = chat_by_uid.get(uid, "")
            if not chat_id:
                continue
            alias = alias_by_uid.get(uid, uid or "Sin alias")
            text = build_message(alias, fase, rows)
            try:
                await bot.send_message(chat_id=int(chat_id), text=text)
                enviados += 1
            except Exception as e:
                errores += 1
                print(f"❌ Error al enviar a chat_id={chat_id}: {e}")
                traceback.print_exc()

    print(f"✅ Envío finalizado. Mensajes enviados: {enviados}, errores: {errores}")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
