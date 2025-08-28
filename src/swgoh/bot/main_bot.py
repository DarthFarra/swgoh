import os
import json
import base64
import logging
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# =========================
# Config (según tu petición)
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  # <- solo esta
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")      # <- ID del spreadsheet
SERVICE_ACCOUNT_ENV = "SERVICE_ACCOUNT_FILE"      # <- JSON / base64 / path

SHEET_USUARIOS = os.getenv("USUARIOS_SHEET", "Usuarios")
SHEET_ASIGNACIONES = os.getenv("ASIGNACIONES_SHEET", "Asignaciones ROTE")

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("AroaBot")


# =========================
# Conexión a Google Sheets
# =========================
def _load_service_account_creds() -> Credentials:
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


def init_spreadsheet():
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID en variables de entorno")
    credentials = _load_service_account_creds()
    client = gspread.authorize(credentials)
    return client.open_by_key(SPREADSHEET_ID)


def _leer_hoja(spreadsheet, nombre_hoja: str) -> List[Dict[str, Any]]:
    ws = spreadsheet.worksheet(nombre_hoja)
    rows = ws.get_all_records()
    # normaliza claves a minúsculas
    norm = []
    for r in rows:
        norm.append({(k or "").strip().lower(): v for k, v in r.items()})
    return norm


def _headers(ws) -> List[str]:
    vals = ws.row_values(1) or []
    return [h.strip() for h in vals]


def _ensure_usuarios_headers(ws):
    """
    Garantiza cabecera mínima SIN borrar filas existentes.
    Si falta alguna columna requerida, la añade al final.
    """
    required = ["user_id", "chat_id", "username", "alias", "rol"]
    headers = ws.row_values(1) or []
    if not headers:
        ws.update("A1", [required])
        return
    lower = [h.strip().lower() for h in headers]
    added = False
    for col in required:
        if col not in lower:
            headers.append(col)   # añade al final
            added = True
    if added:
        ws.update("1:1", [headers])  # solo cabecera


def _header_map(ws) -> Dict[str, int]:
    """
    Devuelve mapa header_lower -> índice (1-based). Asegura cabecera primero.
    """
    _ensure_usuarios_headers(ws)
    headers = _headers(ws)
    return {h.strip().lower(): i for i, h in enumerate(headers, start=1)}


# =========================
# Registro (/register)
# =========================
PEDIR_ALIAS = 1

class RegistroHandler:
    def __init__(self, spreadsheet):
        self.spreadsheet = spreadsheet

    async def iniciar(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        ws = self.spreadsheet.worksheet(SHEET_USUARIOS)
        _ensure_usuarios_headers(ws)
        await update.message.reply_text(
            "¡Hola! 😊\n"
            "Envíame tu *alias* (como aparecerá en la hoja) para registrarte.",
            parse_mode="Markdown",
        )
        return PEDIR_ALIAS

    async def recibir_alias(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        alias_input = (update.message.text or "").strip()
        if not alias_input:
            await update.message.reply_text("El alias no puede estar vacío. Intenta de nuevo.")
            return PEDIR_ALIAS

        user = update.effective_user
        chat = update.effective_chat
        user_id = str(user.id)
        chat_id = str(chat.id)
        username = f"@{user.username}" if user.username else ""

        ws = self.spreadsheet.worksheet(SHEET_USUARIOS)
        col = _header_map(ws)  # asegura cabecera y devuelve índices

        # Leemos todas las filas para buscar coincidencias
        # Usamos get_all_values para incluir filas con celdas vacías
        all_vals = ws.get_all_values() or []
        headers = all_vals[0] if all_vals else []
        rows = all_vals[1:] if len(all_vals) > 1 else []

        # Helper para obtener celda segura por nombre de columna
        def cell_val(row: List[str], colname: str) -> str:
            idx = col.get(colname, 0)
            return (row[idx - 1].strip() if idx and idx - 1 < len(row) else "")

        # 1) Buscar por user_id exacto
        row_index_by_uid: Optional[int] = None
        for i, row in enumerate(rows, start=2):
            if cell_val(row, "user_id") == user_id:
                row_index_by_uid = i
                break

        if row_index_by_uid:
            # Ya registrado: actualizamos chat_id y username
            updates = []
            if col.get("chat_id"):
                updates.append((row_index_by_uid, col["chat_id"], chat_id))
            if col.get("username"):
                updates.append((row_index_by_uid, col["username"], username))
            # Si el alias está vacío, rellenamos con el proporcionado
            current_alias = ws.cell(row_index_by_uid, col["alias"]).value if col.get("alias") else ""
            if col.get("alias") and not (current_alias or "").strip():
                updates.append((row_index_by_uid, col["alias"], alias_input))
            if col.get("rol"):
                current_rol = ws.cell(row_index_by_uid, col["rol"]).value
                if not (current_rol or "").strip():
                    updates.append((row_index_by_uid, col["rol"], "miembro"))
            for r, c, v in updates:
                ws.update_cell(r, c, v)

            await update.message.reply_text("✅ Ya estabas registrado. He actualizado tus datos de Telegram.")
            return ConversationHandler.END

        # 2) Buscar por alias (case-insensitive) con user_id vacío → vincular
        row_index_by_alias: Optional[int] = None
        for i, row in enumerate(rows, start=2):
            a = cell_val(row, "alias")
            uid_cell = cell_val(row, "user_id")
            if a and a.lower() == alias_input.lower() and not uid_cell:
                row_index_by_alias = i
                break

        if row_index_by_alias:
            updates = []
            if col.get("user_id"):
                updates.append((row_index_by_alias, col["user_id"], user_id))
            if col.get("chat_id"):
                updates.append((row_index_by_alias, col["chat_id"], chat_id))
            if col.get("username"):
                updates.append((row_index_by_alias, col["username"], username))
            # Mantener alias tal cual está en la hoja (no lo cambiamos)
            if col.get("rol"):
                current_rol = ws.cell(row_index_by_alias, col["rol"]).value
                if not (current_rol or "").strip():
                    updates.append((row_index_by_alias, col["rol"], "miembro"))
            for r, c, v in updates:
                ws.update_cell(r, c, v)

            await update.message.reply_text("✅ Alias existente vinculado a tu Telegram.")
            return ConversationHandler.END

        # 3) No hay coincidencias → insertar nueva fila
        # Construimos una fila con el largo de la cabecera actual
        new_row = [""] * len(headers)
        if col.get("user_id"):
            new_row[col["user_id"] - 1] = user_id
        if col.get("chat_id"):
            new_row[col["chat_id"] - 1] = chat_id
        if col.get("username"):
            new_row[col["username"] - 1] = username
        if col.get("alias"):
            new_row[col["alias"] - 1] = alias_input
        if col.get("rol"):
            new_row[col["rol"] - 1] = "miembro"
        ws.append_row(new_row, value_input_option="RAW")

        await update.message.reply_text(f"✅ Registrado como: *{alias_input}*", parse_mode="Markdown")
        return ConversationHandler.END

    async def cancelar(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("❌ Registro cancelado.")
        return ConversationHandler.END

    def get_handler(self) -> ConversationHandler:
        return ConversationHandler(
            entry_points=[CommandHandler("register", self.iniciar)],
            states={PEDIR_ALIAS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.recibir_alias)]},
            fallbacks=[CommandHandler("cancel", self.cancelar)],
        )


# =========================
# Asignaciones (/misoperaciones, /operaciones, /operacionesjugador)
# =========================
class AsignacionOperacionesHandler:
    def __init__(self, spreadsheet):
        self.spreadsheet = spreadsheet
        self.fases = [str(i) for i in range(1, 7)]  # "1".."6"
        self.roles_permitidos = {"oficial", "líder", "lider", "leader", "admin"}

    def _es_oficial(self, user_id: int) -> bool:
        try:
            datos_usuarios = _leer_hoja(self.spreadsheet, SHEET_USUARIOS)
            uid = str(user_id).strip()
            for r in datos_usuarios:
                if str(r.get("user_id", "")).strip() == uid:
                    rol = str(r.get("rol", "")).strip().lower()
                    return rol in self.roles_permitidos
            return False
        except Exception as e:
            logging.getLogger("AroaBot").warning("No pude validar rol: %s", e)
            return True

    def _alias_map(self) -> Dict[str, str]:
        datos_usuarios = _leer_hoja(self.spreadsheet, SHEET_USUARIOS)
        m = {}
        for r in datos_usuarios:
            uid = str(r.get("user_id", "")).strip()
            alias = str(r.get("alias", "")).strip()
            if uid:
                m[uid] = alias
        return m

    def _leer_asignaciones(self) -> List[Dict[str, Any]]:
        return _leer_hoja(self.spreadsheet, SHEET_ASIGNACIONES)

    # ---------- /misoperaciones ----------
    async def cmd_misoperaciones(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        kb = [[InlineKeyboardButton(f"Fase {f}", callback_data=f"misop_fase_{f}")] for f in self.fases]
        await update.message.reply_text("Elige una fase:", reply_markup=InlineKeyboardMarkup(kb))

    async def cb_misoperaciones(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        fase = query.data.split("_")[-1]
        uid = str(update.effective_user.id)
        alias_map = self._alias_map()
        alias = alias_map.get(uid, "Sin alias")

        datos = self._leer_asignaciones()
        por_planeta: Dict[str, List[str]] = {}
        for r in datos:
            if str(r.get("user_id", "")).strip() != uid:
                continue
            if str(r.get("fase", "")).strip() != str(fase):
                continue
            planeta = str(r.get("planeta", "Sin planeta")).strip()
            oper = str(r.get("operacion", "Sin operación")).strip()
            pers = str(r.get("personaje", "Sin personaje")).strip()
            por_planeta.setdefault(planeta, []).append(f"- {pers} ({oper})")

        if not por_planeta:
            await query.edit_message_text(f"No tienes asignaciones para la fase {fase}.")
            return

        lines = [f"Asignaciones de {alias} (Fase {fase})", ""]
        for planeta, asigns in por_planeta.items():
            lines.append(f" {planeta}:")
            lines.extend(asigns)
            lines.append("")
        await query.edit_message_text("\n".join(lines))

    # ---------- /operaciones (oficiales por fase→planeta) ----------
    async def cmd_operaciones(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._es_oficial(update.effective_user.id):
            await update.message.reply_text("⛔ No tienes permisos para usar /operaciones.")
            return
        kb = [[InlineKeyboardButton(f"Fase {f}", callback_data=f"op_fase_{f}")] for f in self.fases]
        await update.message.reply_text("Elige una fase:", reply_markup=InlineKeyboardMarkup(kb))

    async def cb_operaciones(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        datos = self._leer_asignaciones()
        alias_map = self._alias_map()

        if data.startswith("op_fase_"):
            fase = data.split("_")[-1]
            planetas = []
            for r in datos:
                if str(r.get("fase", "")) == str(fase):
                    planetas.append(str(r.get("planeta", "Sin planeta")).strip())
            planetas = sorted(set(planetas))
            if not planetas:
                await query.edit_message_text(f"No hay asignaciones en fase {fase}.")
                return
            kb = [[InlineKeyboardButton(p, callback_data=f"op_planeta_{fase}_{p}")] for p in planetas]
            await query.edit_message_text(
                f"Fase {fase}: elige un planeta",
                reply_markup=InlineKeyboardMarkup(kb),
            )
            return

    # ---------- /operacionesjugador (oficiales por fase→jugador) ----------
    async def cmd_operaciones_jugador(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._es_oficial(update.effective_user.id):
            await update.message.reply_text("⛔ No tienes permisos para usar /operacionesjugador.")
            return
        kb = [[InlineKeyboardButton(f"Fase {f}", callback_data=f"opj_fase_{f}")] for f in self.fases]
        await update.message.reply_text("Elige una fase:", reply_markup=InlineKeyboardMarkup(kb))

    async def cb_operaciones_jugador(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        datos = self._leer_asignaciones()
        alias_map = self._alias_map()

        if data.startswith("opj_fase_"):
            fase = data.split("_")[-1]
            jugadores = []
            for r in datos:
                if str(r.get("fase", "")) == str(fase):
                    uid = str(r.get("user_id", "")).strip()
                    alias = alias_map.get(uid, uid or "Sin alias")
                    jugadores.append(alias)
            jugadores = sorted(set(jugadores))
            if not jugadores:
                await query.edit_message_text(f"No hay asignaciones en fase {fase}.")
                return
            kb = [[InlineKeyboardButton(j, callback_data=f"opj_jugador_{fase}_{j}")] for j in jugadores]
            await query.edit_message_text(
                f"Fase {fase}: elige jugador",
                reply_markup=InlineKeyboardMarkup(kb),
            )
            return

        if data.startswith("opj_jugador_"):
            _, _, fase, jugador_alias = data.split("_", 3)
            lines = [f"Asignaciones de {jugador_alias} (Fase {fase})", ""]
            por_planeta: Dict[str, List[str]] = {}
            for r in datos:
                if str(r.get("fase", "")) != str(fase):
                    continue
                uid = str(r.get("user_id", "")).strip()
                alias = alias_map.get(uid, uid or "Sin alias")
                if alias != jugador_alias:
                    continue
                planeta = str(r.get("planeta", "Sin planeta")).strip()
                oper = str(r.get("operacion", "Sin operación")).strip()
                pers = str(r.get("personaje", "Sin personaje")).strip()
                por_planeta.setdefault(planeta, []).append(f"- {pers} ({oper})")
            if not por_planeta:
                await query.edit_message_text(f"Sin asignaciones en Fase {fase} para {jugador_alias}.")
                return
            for planeta, asigns in por_planeta.items():
                lines.append(f" {planeta}:")
                lines.extend(asigns)
                lines.append("")
            await query.edit_message_text("\n".join(lines))
            return

    def get_handlers(self):
        return [
            # Abierto
            CommandHandler("misoperaciones", self.cmd_misoperaciones),
            CallbackQueryHandler(self.cb_misoperaciones, pattern=r"^misop_fase_\d+$"),

            # Oficiales
            CommandHandler("operaciones", self.cmd_operaciones),
            CallbackQueryHandler(self.cb_operaciones, pattern=r"^op_fase_\d+$"),
            # el callback de planeta ya está dentro de cb_operaciones

            CommandHandler("operacionesjugador", self.cmd_operaciones_jugador),
            CallbackQueryHandler(self.cb_operaciones_jugador, pattern=r"^opj_fase_\d+$"),
            CallbackQueryHandler(self.cb_operaciones_jugador, pattern=r"^opj_jugador_"),
        ]


# =========================
# App
# =========================
async def _post_init(app: Application) -> None:
    # Desactiva cualquier webhook antiguo para que funcione polling
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
        log.info("Webhook eliminado; polling activado.")
    except Exception as e:
        log.warning("No pude eliminar webhook (quizá no existía): %s", e)

def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN en variables de entorno")
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID en variables de entorno")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(_post_init).build()

    # 1) Conectar a Google Sheets
    spreadsheet = init_spreadsheet()

    # 2) Registro (/register)
    registro = RegistroHandler(spreadsheet)
    app.add_handler(registro.get_handler())

    # 3) Operaciones (/misoperaciones, /operaciones, /operacionesjugador)
    asign = AsignacionOperacionesHandler(spreadsheet)
    for h in asign.get_handlers():
        app.add_handler(h)

    log.info("🤖 Bot en marcha (polling)…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
