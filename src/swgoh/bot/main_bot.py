# src/swgoh/bot/main_bot.py
import os
import json
import base64
import logging
from typing import Any, Dict, List, Optional, Tuple

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
# Configuración (variables de entorno)
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")          # requerido
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")              # requerido
SERVICE_ACCOUNT_ENV = "SERVICE_ACCOUNT_FILE"              # JSON directo / base64 / ruta a archivo

# Nombres de pestañas
SHEET_USUARIOS = os.getenv("USUARIOS_SHEET", "Usuarios")
SHEET_GUILDS = os.getenv("GUILDS_SHEET", "Guilds")
SHEET_PLAYERS = os.getenv("PLAYERS_SHEET", "Players")

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("AroaBot")

# =========================
# Google Sheets helpers
# =========================
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


def _headers(ws) -> List[str]:
    vals = ws.row_values(1) or []
    return [h.strip() for h in vals]


def _ensure_headers(ws, required: List[str]) -> Dict[str, int]:
    """
    Añade columnas requeridas al final de la cabecera si faltan (NO borra datos).
    Devuelve un mapa header_lower -> índice 1-based.
    """
    headers = _headers(ws)
    if not headers:
        ws.update("A1", [required])
        headers = required[:]
    else:
        lower = [h.strip().lower() for h in headers]
        changed = False
        for col in required:
            if col not in lower:
                headers.append(col)
                changed = True
        if changed:
            ws.update("1:1", [headers])
    return {h.strip().lower(): i for i, h in enumerate(headers, start=1)}


def _ensure_usuarios_headers(ws) -> Dict[str, int]:
    # Añadimos guild_name + allycode a las columnas mínimas
    return _ensure_headers(ws, ["guild_name", "user_id", "chat_id", "username", "alias", "rol", "allycode"])


def _sheet_records_lower(spreadsheet, sheet_name: str) -> List[Dict[str, Any]]:
    ws = spreadsheet.worksheet(sheet_name)
    rows = ws.get_all_records()
    return [{(k or "").strip().lower(): v for k, v in r.items()} for r in rows]


def _sanitize_allycode(s: str) -> str:
    return "".join(ch for ch in str(s) if ch.isdigit())


# =========================
# Registro multi-gremio (flujo solicitado)
# 1) /register: elegir gremio (lista desde Guilds con Guild Name)
# 2) Comprobar si ya está en Usuarios por (user_id + guild_name)
#    - Si ya está: mensaje y fin
# 3) Si no está: elegir método (Alias / Allycode), pedir dato, validar en Players:
#    - Alias: Player Name + Guild Name
#    - Allycode: Ally code + Guild Name
# 4) Si no existe → mensaje de error (no registrar)
# 5) Si existe → insertar en Usuarios:
#    alias (Player Name), username (telegram), user_id (telegram), chat_id (telegram),
#    rol (Role), allycode (Ally code), guild_name (seleccionado)
# =========================

# Estados de conversación
CHOOSE_GUILD, CHOOSE_METHOD, ASK_ALIAS, ASK_ALLY = range(200, 204)

class RegistroMultiGuildHandler:
    def __init__(self, spreadsheet):
        self.ss = spreadsheet

    # ----- Paso 1: /register -> elegir gremio -----
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        ws_guilds = self.ss.worksheet(SHEET_GUILDS)
        guild_rows = ws_guilds.get_all_records()
        names: List[str] = []
        for r in guild_rows:
            # intentamos varias claves por si difieren
            name = str(
                r.get("Guild Name")
                or r.get("guild name")
                or r.get("Name")
                or r.get("name")
                or ""
            ).strip()
            if name:
                names.append(name)

        if not names:
            await update.message.reply_text("No hay gremios configurados en la hoja Guilds.")
            return ConversationHandler.END

        context.user_data["register_guild_list"] = names

        # Teclado inline (3 por fila)
        buttons: List[List[InlineKeyboardButton]] = []
        row: List[InlineKeyboardButton] = []
        for i, name in enumerate(names):
            row.append(InlineKeyboardButton(name, callback_data=f"reg_guild_idx_{i}"))
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)

        await update.message.reply_text("Elige tu gremio:", reply_markup=InlineKeyboardMarkup(buttons))
        return CHOOSE_GUILD

    # ----- Callback elección de gremio -----
    async def choose_guild_cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        data = query.data
        if not data.startswith("reg_guild_idx_"):
            await query.edit_message_text("Selección inválida.")
            return ConversationHandler.END

        try:
            idx = int(data.split("reg_guild_idx_", 1)[1])
        except Exception:
            await query.edit_message_text("Selección inválida.")
            return ConversationHandler.END

        names: List[str] = context.user_data.get("register_guild_list") or []
        if idx < 0 or idx >= len(names):
            await query.edit_message_text("Selección inválida.")
            return ConversationHandler.END

        guild_name = names[idx]
        context.user_data["register_guild_name"] = guild_name

        # Paso 2: comprobar si ya está registrado (user_id + guild_name)
        ws_u = self.ss.worksheet(SHEET_USUARIOS)
        col_u = _ensure_usuarios_headers(ws_u)

        all_vals = ws_u.get_all_values() or []
        rows = all_vals[1:] if len(all_vals) > 1 else []

        user_id = str(update.effective_user.id)

        def cell_val(row: List[str], colname: str) -> str:
            idx = col_u.get(colname, 0)
            return (row[idx - 1].strip() if idx and idx - 1 < len(row) else "")

        for row in rows:
            if cell_val(row, "user_id") == user_id and cell_val(row, "guild_name") == guild_name:
                await query.edit_message_text(
                    f"✅ Ya estás registrado en el gremio: *{guild_name}*.",
                    parse_mode="Markdown",
                )
                return ConversationHandler.END

        # Paso 3: elegir método de registro
        buttons = [
            [InlineKeyboardButton("Registrar por Alias", callback_data="reg_method_alias")],
            [InlineKeyboardButton("Registrar por Allycode", callback_data="reg_method_ally")],
        ]
        await query.edit_message_text(
            f"Gremio seleccionado: *{guild_name}*\n\n¿Cómo quieres registrarte?",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return CHOOSE_METHOD

    # ----- Callback elección de método -----
    async def choose_method_cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data

        if data == "reg_method_alias":
            context.user_data["register_method"] = "alias"
            await query.edit_message_text(
                "Perfecto. Envíame tu *alias de jugador* exactamente como aparece en la pestaña *Players*.",
                parse_mode="Markdown",
            )
            return ASK_ALIAS

        if data == "reg_method_ally":
            context.user_data["register_method"] = "ally"
            await query.edit_message_text(
                "Genial. Envíame tu *código de aliado* (solo números).",
                parse_mode="Markdown",
            )
            return ASK_ALLY

        await query.edit_message_text("Selección inválida.")
        return ConversationHandler.END

    # ----- Paso 3A: recibir alias -----
    async def receive_alias(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (update.message.text or "").strip()
        if not text:
            await update.message.reply_text("El alias no puede estar vacío. Intenta de nuevo.")
            return ASK_ALIAS

        guild_name = context.user_data.get("register_guild_name")
        if not guild_name:
            await update.message.reply_text("No se encontró el gremio seleccionado. Reinicia con /register.")
            return ConversationHandler.END

        ok, row = self._lookup_player(guild_name=guild_name, alias=text, allycode=None)
        if not ok or not row:
            await update.message.reply_text(
                "❌ No encontré ese *alias* en la pestaña *Players* para el gremio seleccionado.",
                parse_mode="Markdown",
            )
            return ConversationHandler.END

        await self._store_user(update, guild_name, row)
        return ConversationHandler.END

    # ----- Paso 3B: recibir allycode -----
    async def receive_ally(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = _sanitize_allycode(update.message.text or "")
        if not text:
            await update.message.reply_text("El código de aliado debe contener números. Intenta de nuevo.")
            return ASK_ALLY

        guild_name = context.user_data.get("register_guild_name")
        if not guild_name:
            await update.message.reply_text("No se encontró el gremio seleccionado. Reinicia con /register.")
            return ConversationHandler.END

        ok, row = self._lookup_player(guild_name=guild_name, alias=None, allycode=text)
        if not ok or not row:
            await update.message.reply_text(
                "❌ No encontré ese *código de aliado* en la pestaña *Players* para el gremio seleccionado.",
                parse_mode="Markdown",
            )
            return ConversationHandler.END

        await self._store_user(update, guild_name, row)
        return ConversationHandler.END

    # ----- Búsqueda en Players -----
    def _lookup_player(self, guild_name: str, alias: Optional[str], allycode: Optional[str]) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Busca en la pestaña Players una fila que cumpla:
          - Guild Name == guild_name
          - y (Player Name == alias)  (CI)   o
            (Ally code == allycode) (numérico)
        Devuelve (ok, fila_dict_normalizada).
        """
        ws = self.ss.worksheet(SHEET_PLAYERS)
        vals = ws.get_all_values() or []
        if not vals:
            return False, None

        headers = [h.strip() for h in (vals[0] or [])]
        hmap = {h.strip().lower(): i for i, h in enumerate(headers)}
        def gv(row: List[str], name: str) -> str:
            i = hmap.get(name.lower(), -1)
            return (row[i].strip() if 0 <= i < len(row) else "")

        target_guild = str(guild_name).strip()
        target_alias = (alias or "").strip()
        target_ally = _sanitize_allycode(allycode or "")

        for row in vals[1:]:
            gname = gv(row, "Guild Name")
            if gname != target_guild:
                continue

            if alias is not None:
                pname = gv(row, "Player Name")
                if pname and pname.lower() == target_alias.lower():
                    return True, {
                        "alias": pname,
                        "role": gv(row, "Role"),
                        "allycode": _sanitize_allycode(gv(row, "Ally code")),
                        "guild_name": gname,
                    }

            else:
                ac = _sanitize_allycode(gv(row, "Ally code"))
                if ac and ac == target_ally:
                    return True, {
                        "alias": gv(row, "Player Name"),
                        "role": gv(row, "Role"),
                        "allycode": ac,
                        "guild_name": gname,
                    }

        return False, None

    # ----- Inserción en Usuarios -----
    async def _store_user(self, update: Update, guild_name: str, player_row: Dict[str, Any]):
        """Inserta en Usuarios: alias, username, user_id, chat_id, rol, allycode, guild_name."""
        ws_u = self.ss.worksheet(SHEET_USUARIOS)
        col_u = _ensure_usuarios_headers(ws_u)

        headers_now = _headers(ws_u)
        new_row = [""] * len(headers_now)

        # Telegram
        user = update.effective_user
        chat = update.effective_chat
        user_id = str(user.id)
        chat_id = str(chat.id)
        username = f"@{user.username}" if user.username else ""

        # Player (validado)
        alias = str(player_row.get("alias") or "").strip()
        role = str(player_row.get("role") or "").strip()
        allycode = _sanitize_allycode(player_row.get("allycode") or "")

        def set_cell(colname: str, value: str):
            idx = col_u.get(colname)
            if idx:
                new_row[idx - 1] = value

        set_cell("guild_name", guild_name)
        set_cell("user_id", user_id)
        set_cell("chat_id", chat_id)
        set_cell("username", username)
        set_cell("alias", alias)
        set_cell("rol", role)
        set_cell("allycode", allycode)

        ws_u.append_row(new_row, value_input_option="RAW")
        await update.message.reply_text(
            f"✅ Registrado en *{guild_name}* como *{alias}*.",
            parse_mode="Markdown",
        )

    # Cancelación
    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("❌ Registro cancelado.")
        return ConversationHandler.END


# =========================
# App básica (/register + /help)
# =========================
async def _post_init(app: Application) -> None:
    # Desactiva cualquier webhook antiguo para que funcione polling
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
        log.info("Webhook eliminado; polling activado.")
    except Exception as e:
        log.warning("No pude eliminar webhook (quizá no existía): %s", e)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Comandos:\n"
        "/register – registrar usuario en un gremio\n"
        "/help – ayuda"
    )


def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN en variables de entorno")
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID en variables de entorno")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(_post_init).build()
    ss = init_spreadsheet()

    reg = RegistroMultiGuildHandler(ss)

    conv = ConversationHandler(
        entry_points=[CommandHandler("register", reg.start)],
        states={
            CHOOSE_GUILD: [CallbackQueryHandler(reg.choose_guild_cb, pattern=r"^reg_guild_idx_\d+$")],
            CHOOSE_METHOD: [CallbackQueryHandler(reg.choose_method_cb, pattern=r"^reg_method_(alias|ally)$")],
            ASK_ALIAS: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg.receive_alias)],
            ASK_ALLY: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg.receive_ally)],
        },
        fallbacks=[CommandHandler("cancel", reg.cancel)],
        allow_reentry=True,
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("help", help_cmd))

    log.info("🤖 Bot en marcha (polling)…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
