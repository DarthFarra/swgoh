# src/swgoh/bot/main_bot.py
import os
import json
import base64
import logging
import unicodedata
import asyncio
from asyncio.subprocess import PIPE
from typing import Any, Dict, List, Optional, Tuple

import socket
import urllib.request, urllib.error
from urllib.parse import urlparse

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
SHEET_GUILDS   = os.getenv("GUILDS_SHEET", "Guilds")
SHEET_PLAYERS  = os.getenv("PLAYERS_SHEET", "Players")

def _parse_csv_ints(s: str) -> set[int]:
    out = set()
    for part in (s or "").split(","):
        part = part.strip()
        if part:
            try:
                out.add(int(part))
            except ValueError:
                pass
    return out

# Permitir override por env: SYNC_CHAT_IDS="123,456"
ALLOWED_SYNC_CHAT_IDS = _parse_csv_ints(os.getenv("SYNC_CHAT_IDS", "7367477801,30373681"))

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


def _read_all_values(ss, sheet_name: str) -> Tuple[List[str], List[List[str]]]:
    ws = ss.worksheet(sheet_name)
    vals = ws.get_all_values() or []
    if not vals:
        return [], []
    headers = [h.strip() for h in vals[0]]
    rows = vals[1:] if len(vals) > 1 else []
    return headers, rows


def _header_index_map(headers: List[str]) -> Dict[str, int]:
    return {h.strip().lower(): i for i, h in enumerate(headers)}


def _sanitize_allycode(s: str) -> str:
    return "".join(ch for ch in str(s) if ch.isdigit())


# =========================
# Helpers UI (botones con nombre abreviado)
# =========================
def _clean_label(s: str) -> str:
    """Normaliza y elimina invisibles; además colapsa espacios."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFC", str(s))
    invisibles = ["\u200b", "\u200c", "\u200d", "\ufeff", "\u2066", "\u2067", "\u2068", "\u2069"]
    for ch in invisibles:
        s = s.replace(ch, "")
    s = " ".join(s.split())
    return s


def _build_label_keyboard(labels: List[str], prefix: str, per_row: int = 3) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for i, lab in enumerate(labels):
        row.append(InlineKeyboardButton(lab, callback_data=f"{prefix}{i}"))
        if len(row) == per_row:
            rows.append(row); row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def _guilds_with_abbrev(ss) -> Tuple[List[str], List[str]]:
    """
    Lee Guilds y devuelve (guild_names, display_names) donde display_names usa la columna 'nombre abreviado'
    si existe, o cae en Guild Name si no.
    """
    headers, rows = _read_all_values(ss, SHEET_GUILDS)
    if not headers:
        return [], []
    hmap = _header_index_map(headers)
    def gv(row: List[str], name: str) -> str:
        i = hmap.get(name.lower(), -1)
        return (row[i].strip() if 0 <= i < len(row) else "")
    guild_names: List[str] = []
    display_names: List[str] = []
    for r in rows:
        gname = gv(r, "Guild Name")
        if not gname:
            continue
        abre = gv(r, "nombre abreviado") or gname
        guild_names.append(gname)
        display_names.append(_clean_label(abre))
    return guild_names, display_names


def _abbrev_map(ss) -> Dict[str, str]:
    """
    Mapa {Guild Name -> nombre abreviado (o Guild Name si falta)} para mostrar en /misoperaciones.
    """
    gnames, dnames = _guilds_with_abbrev(ss)
    return {g: d for g, d in zip(gnames, dnames)}


# =========================
# Registro multi-gremio
# =========================

# Estados de conversación (registro)
CHOOSE_GUILD, CHOOSE_METHOD, ASK_ALIAS, ASK_ALLY = range(200, 204)

class RegistroMultiGuildHandler:
    def __init__(self, spreadsheet):
        self.ss = spreadsheet

    # ----- /register: elegir gremio (muestra 'nombre abreviado') -----
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        guild_names, display_names = _guilds_with_abbrev(self.ss)
        if not guild_names:
            await update.message.reply_text("No hay gremios configurados en la hoja Guilds.")
            return ConversationHandler.END

        # Guardamos ambas listas (mostramos display, usamos guild_names para la lógica)
        context.user_data["register_guild_names"] = guild_names
        context.user_data["register_guild_display"] = display_names

        await update.message.reply_text(
            "Elige tu gremio:",
            reply_markup=_build_label_keyboard(display_names, prefix="reg_guild_idx_", per_row=3),
        )
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

        gnames: List[str] = context.user_data.get("register_guild_names") or []
        gdisp:  List[str] = context.user_data.get("register_guild_display") or []
        if idx < 0 or idx >= len(gnames):
            await query.edit_message_text("Selección inválida.")
            return ConversationHandler.END

        guild_name = gnames[idx]
        guild_disp = gdisp[idx] if idx < len(gdisp) else guild_name
        context.user_data["register_guild_name"] = guild_name

        # Ya registrado en (user_id + guild_name)?
        ws_u = self.ss.worksheet(SHEET_USUARIOS)
        col_u = _ensure_usuarios_headers(ws_u)

        all_vals = ws_u.get_all_values() or []
        rows = all_vals[1:] if len(all_vals) > 1 else []

        user_id = str(update.effective_user.id)

        def cell_val(row: List[str], colname: str) -> str:
            idx_ = col_u.get(colname, 0)
            return (row[idx_ - 1].strip() if idx_ and idx_ - 1 < len(row) else "")

        for row in rows:
            if cell_val(row, "user_id") == user_id and cell_val(row, "guild_name") == guild_name:
                await query.edit_message_text(
                    f"✅ Ya estás registrado en el gremio: *{guild_disp}*.",
                    parse_mode="Markdown",
                )
                return ConversationHandler.END

        buttons = [
            [InlineKeyboardButton("Registrar por Alias", callback_data="reg_method_alias")],
            [InlineKeyboardButton("Registrar por Allycode", callback_data="reg_method_ally")],
        ]
        await query.edit_message_text(
            f"Gremio seleccionado: *{guild_disp}*\n\n¿Cómo quieres registrarte?",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return CHOOSE_METHOD

    # ----- Elección de método -----
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

    # ----- Recibir alias -----
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

    # ----- Recibir allycode -----
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
        target_ally  = _sanitize_allycode(allycode or "")

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
# /misoperaciones (multi-gremio, pestañas ROTE por guild)
# =========================

MISOP_CHOOSE_GUILD = 300  # estado de conversación

class MisOperacionesHandler:
    def __init__(self, spreadsheet):
        self.ss = spreadsheet
        self.fases = [str(i) for i in range(1, 7)]  # "1".."6"

    async def cmd_misoperaciones(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Permite elegir gremio (si hay varios) y luego fase. Muestra 'nombre abreviado' en botones."""
        user_id = str(update.effective_user.id)
        guilds = self._user_guilds(user_id)  # Guild Name completos almacenados en Usuarios
        if not guilds:
            await update.message.reply_text("No encuentro tu registro en ningún gremio. Usa /register primero.")
            return ConversationHandler.END

        # Construir mapa de abreviados para mostrar
        amap = _abbrev_map(self.ss)
        labels = [_clean_label(amap.get(g, g)) for g in guilds]

        if len(guilds) == 1:
            guild_name = guilds[0]
            context.user_data["misop_guild"] = guild_name
            await self._ask_phase(update, context, amap.get(guild_name, guild_name))
            return ConversationHandler.END

        # Varios gremios -> elegir por botones (abreviados)
        context.user_data["misop_guild_list"] = guilds  # seguimos guardando nombres completos
        await update.message.reply_text(
            "Elige el gremio:",
            reply_markup=_build_label_keyboard(labels, prefix="misop_guild_idx_", per_row=3),
        )
        return MISOP_CHOOSE_GUILD

    async def cb_choose_guild(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        if not data.startswith("misop_guild_idx_"):
            await query.edit_message_text("Selección inválida.")
            return ConversationHandler.END
        try:
            idx = int(data.split("misop_guild_idx_", 1)[1])
        except Exception:
            await query.edit_message_text("Selección inválida.")
            return ConversationHandler.END
        guilds = context.user_data.get("misop_guild_list") or []
        if idx < 0 or idx >= len(guilds):
            await query.edit_message_text("Selección inválida.")
            return ConversationHandler.END
        guild_name = guilds[idx]
        context.user_data["misop_guild"] = guild_name

        amap = _abbrev_map(self.ss)
        await query.edit_message_text(f"Gremio: *{amap.get(guild_name, guild_name)}*", parse_mode="Markdown")
        # preguntar fase
        await self._ask_phase(query, context, amap.get(guild_name, guild_name))
        return ConversationHandler.END

    async def cb_choose_phase(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Callback cuando el usuario pulsa una fase."""
        query = update.callback_query
        await query.answer()
        data = query.data
        if not data.startswith("misop_fase_"):
            return
        fase = data.split("misop_fase_")[1]
        guild_name = context.user_data.get("misop_guild")
        if not guild_name:
            await query.edit_message_text("No hay gremio activo. Usa /misoperaciones de nuevo.")
            return

        # Mostrar asignaciones
        text = self._build_assignments_text(guild_name, str(update.effective_user.id), fase)
        await query.edit_message_text(text, disable_web_page_preview=True, parse_mode="Markdown")

    async def _ask_phase(self, update_or_query: Update, context: ContextTypes.DEFAULT_TYPE, guild_label: str):
        kb = [[InlineKeyboardButton(f"Fase {f}", callback_data=f"misop_fase_{f}")] for f in self.fases]
        if isinstance(update_or_query, Update) and update_or_query.message:
            await update_or_query.message.reply_text(
                f"Gremio activo: *{guild_label}*\nElige una fase:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(kb),
            )
        else:
            await update_or_query.edit_message_text(
                f"Gremio activo: *{guild_label}*\nElige una fase:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(kb),
            )

    # ----- Helpers -----

    def _user_guilds(self, user_id: str) -> List[str]:
        ws = self.ss.worksheet(SHEET_USUARIOS)
        col = _ensure_usuarios_headers(ws)
        vals = ws.get_all_values() or []
        rows = vals[1:] if len(vals) > 1 else []

        def cv(row: List[str], name: str) -> str:
            idx = col.get(name, 0)
            return (row[idx - 1].strip() if idx and idx - 1 < len(row) else "")

        guilds: List[str] = []
        for r in rows:
            if cv(r, "user_id") == user_id:
                g = cv(r, "guild_name")
                if g:
                    guilds.append(g)
        # únicos preservando orden
        out: List[str] = []
        seen = set()
        for g in guilds:
            if g not in seen:
                out.append(g); seen.add(g)
        return out

    def _assignments_sheet_for_guild(self, guild_name: str) -> Optional[str]:
        """Busca en Guilds la pestaña ROTE asociada al guild_name."""
        headers, rows = _read_all_values(self.ss, SHEET_GUILDS)
        if not headers:
            return None
        hmap = _header_index_map(headers)
        def gv(row: List[str], name: str) -> str:
            i = hmap.get(name.lower(), -1)
            return (row[i].strip() if 0 <= i < len(row) else "")

        for row in rows:
            gname = gv(row, "Guild Name")
            rote  = gv(row, "ROTE")
            if gname and rote and gname == guild_name:
                # Verifica que exista hoja
                try:
                    self.ss.worksheet(rote)
                    return rote
                except Exception:
                    return None
        return None

    def _build_assignments_text(self, guild_name: str, user_id: str, fase: str) -> str:
        # Determinar pestaña ROTE del gremio
        sheet_name = self._assignments_sheet_for_guild(guild_name) or "Asignaciones ROTE"
        try:
            headers, rows = _read_all_values(self.ss, sheet_name)
        except Exception:
            return f"No encuentro la pestaña de asignaciones para *{guild_name}*."

        if not headers:
            return f"No hay datos en la pestaña de asignaciones de *{guild_name}*."

        hmap = _header_index_map(headers)
        def gv(row: List[str], name: str) -> str:
            i = hmap.get(name.lower(), -1)
            return (row[i].strip() if 0 <= i < len(row) else "")

        # Alias del usuario para fallback (si no hay user_id en asignaciones)
        alias = ""
        try:
            ws_u = self.ss.worksheet(SHEET_USUARIOS)
            col_u = _ensure_usuarios_headers(ws_u)
            vals_u = ws_u.get_all_values() or []
            rows_u = vals_u[1:] if len(vals_u) > 1 else []
            def cv_u(row: List[str], name: str) -> str:
                idx = col_u.get(name, 0)
                return (row[idx - 1].strip() if idx and idx - 1 < len(row) else "")
            for r in rows_u:
                if cv_u(r, "user_id") == user_id and cv_u(r, "guild_name") == guild_name:
                    alias = cv_u(r, "alias")
                    break
        except Exception:
            pass

        por_planeta: Dict[str, List[str]] = {}

        for row in rows:
            f = gv(row, "fase")
            if f != str(fase):
                continue

            uid_cell = gv(row, "user_id")
            if uid_cell:
                if uid_cell != user_id:
                    continue
            else:
                jugador = gv(row, "jugador")
                if not jugador or not alias or jugador.strip().lower() != alias.strip().lower():
                    continue

            planeta = gv(row, "planeta") or "Sin planeta"
            oper    = gv(row, "operacion") or "Sin operación"
            pers    = gv(row, "personaje") or "Sin personaje"
            por_planeta.setdefault(planeta, []).append(f"- {pers} ({oper})")

        if not por_planeta:
            return f"No tienes asignaciones en *Fase {fase}* para *{guild_name}*."

        lines = [f"Asignaciones de *{alias or 'tu usuario'}* — *{guild_name}* (Fase {fase})", ""]
        for planeta, asigns in por_planeta.items():
            lines.append(f" {planeta}:")
            lines.extend(asigns)
            lines.append("")
        return "\n".join(lines)


# =========================
# /syncdata: ejecutar swgoh.processing.sync_data (chats autorizados)
# =========================

class SyncDataRunner:
    def __init__(self):
        self._lock = asyncio.Lock()

    async def _run_subprocess(self, *args: str) -> tuple[int, str, str]:
        # Asegura PYTHONPATH=src para -m swgoh.processing.sync_data
        env = os.environ.copy()
        pp = env.get("PYTHONPATH", "")
        if "src" not in (pp.split(":") if pp else []):
            env["PYTHONPATH"] = f"src:{pp}" if pp else "src"

        proc = await asyncio.create_subprocess_exec(
            *args, stdout=PIPE, stderr=PIPE, env=env
        )
        stdout_b, stderr_b = await proc.communicate()
        return proc.returncode, stdout_b.decode("utf-8", "replace"), stderr_b.decode("utf-8", "replace")

    async def cmd_syncdata(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id if update.effective_chat else None
        if chat_id not in ALLOWED_SYNC_CHAT_IDS:
            await update.message.reply_text("⛔ No autorizado para ejecutar /syncdata en este chat.")
            return

        if self._lock.locked():
            await update.message.reply_text("⚙️ Ya hay una sincronización en curso. Inténtalo más tarde.")
            return

        await update.message.reply_text("🚀 Iniciando *sync_data*…", parse_mode="Markdown")

        async with self._lock:
            try:
                # Intenta como módulo; si falla, cae a script directo.
                rc, out, err = await self._run_subprocess("python", "-m", "swgoh.processing.sync_data")
            except FileNotFoundError:
                rc, out, err = await self._run_subprocess("python", "src/swgoh/processing/sync_data.py")
            except Exception as e:
                await update.message.reply_text(f"❌ Error al lanzar el proceso: {e}")
                return

        def tail(txt: str, lines: int = 60) -> str:
            arr = txt.strip().splitlines()
            return "\n".join(arr[-lines:])

        if rc == 0:
            summary = tail(out) or "(sin salida)"
            msg = "✅ *sync_data* finalizado correctamente.\n\n*Últimas líneas:*\n```\n" + summary + "\n```"
            await update.message.reply_text(msg, parse_mode="Markdown")
        else:
            summary_out = tail(out)
            summary_err = tail(err) or "(sin errores capturados)"
            msg = (
                f"❌ *sync_data* terminó con código {rc}.\n\n"
                f"*STDOUT (tail):*\n```\n{summary_out}\n```\n"
                f"*STDERR (tail):*\n```\n{summary_err}\n```"
            )
            await update.message.reply_text(msg, parse_mode="Markdown")


# =========================
# /diagcomlink: diagnóstico de conectividad a COMLINK (chats autorizados)
# =========================
async def cmd_diagcomlink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id not in ALLOWED_SYNC_CHAT_IDS:
        await update.message.reply_text("⛔ No autorizado.")
        return

    base = os.getenv("COMLINK_BASE_URL", "")
    if not base:
        await update.message.reply_text("COMLINK_BASE_URL no está definido en el entorno de LiveBot.")
        return

    parsed = urlparse(base)
    host = parsed.hostname or base
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    lines = []
    lines.append(f"COMLINK_BASE_URL = {base}")
    lines.append(f"Host = {host}  Port = {port}  Scheme = {parsed.scheme or 'http'}")

    # DNS
    try:
        addrs = {ai[4][0] for ai in socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)}
        lines.append("DNS -> " + (", ".join(sorted(addrs)) or "(sin direcciones)"))
    except Exception as e:
        lines.append(f"DNS -> error: {e}")

    # TCP check
    try:
        socket.create_connection((host, port), timeout=3.0).close()
        lines.append("TCP -> open")
    except Exception as e:
        lines.append(f"TCP -> closed ({e})")

    # Warm-up GET raíz
    try:
        req0 = urllib.request.Request(base, method="GET")
        with urllib.request.urlopen(req0, timeout=10) as resp0:
            lines.append(f"GET / -> {resp0.status} {resp0.reason}")
    except Exception as e:
        lines.append(f"GET / -> ERROR {e}")

    # POST /metadata (dos intentos)
    url = base.rstrip("/") + "/metadata"
    payload = b'{"payload":{},"enums":false}'
    headers = {"content-type": "application/json"}

    for i in (1, 2):
        try:
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = resp.read(200).decode("utf-8", "replace")
                lines.append(f"POST /metadata (#{i}) -> {resp.status} {resp.reason}  body(200B)='{body}'")
        except Exception as e:
            lines.append(f"POST /metadata (#{i}) -> ERROR {e}")

    msg = "\n".join(lines)
    if len(msg) > 3500:
        msg = msg[:3500] + "\n…(truncado)…"
    await update.message.reply_text("```\n" + msg + "\n```", parse_mode="Markdown")


# =========================
# App básica
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
        "/register o /registrar – registrar usuario en un gremio\n"
        "/misoperaciones – ver tus asignaciones (elige gremio/fase)\n"
        "/syncdata – ejecutar la sincronización de datos (autorizados)\n"
        "/diagcomlink – diagnóstico de conectividad a Comlink (autorizados)\n"
        "/help – ayuda"
    )


def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN en variables de entorno")
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID en variables de entorno")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(_post_init).build()
    ss = init_spreadsheet()

    # Registro
    reg = RegistroMultiGuildHandler(ss)
    conv_reg = ConversationHandler(
        entry_points=[
            CommandHandler("register", reg.start),
            CommandHandler("registrar", reg.start),
        ],
        states={
            CHOOSE_GUILD: [CallbackQueryHandler(reg.choose_guild_cb, pattern=r"^reg_guild_idx_\d+$")],
            CHOOSE_METHOD: [CallbackQueryHandler(reg.choose_method_cb, pattern=r"^reg_method_(alias|ally)$")],
            ASK_ALIAS: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg.receive_alias)],
            ASK_ALLY: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg.receive_ally)],
        },
        fallbacks=[CommandHandler("cancel", reg.cancel)],
        allow_reentry=True,
    )
    app.add_handler(conv_reg)

    # Mis operaciones
    misop = MisOperacionesHandler(ss)
    conv_misop = ConversationHandler(
        entry_points=[CommandHandler("misoperaciones", misop.cmd_misoperaciones)],
        states={
            MISOP_CHOOSE_GUILD: [CallbackQueryHandler(misop.cb_choose_guild, pattern=r"^misop_guild_idx_\d+$")],
        },
        fallbacks=[],
        allow_reentry=True,
    )
    app.add_handler(conv_misop)
    app.add_handler(CallbackQueryHandler(misop.cb_choose_phase, pattern=r"^misop_fase_\d+$"))

    # /syncdata
    sync_runner = SyncDataRunner()
    app.add_handler(CommandHandler("syncdata", sync_runner.cmd_syncdata))

    # /diagcomlink
    app.add_handler(CommandHandler("diagcomlink", cmd_diagcomlink))

    app.add_handler(CommandHandler("help", help_cmd))

    log.info("🤖 Bot en marcha (polling)…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
