# src/swgoh/bot/main_bot.py
from __future__ import annotations

import os
import sys
import json
import asyncio
import subprocess
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove,
)
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler,
    ContextTypes, ConversationHandler, filters
)

# ===================== Config & ENV =====================
SERVICE_ACCOUNT_ENV = "SERVICE_ACCOUNT_FILE"
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ID_ZONA = os.getenv("ID_ZONA", "Europe/Amsterdam")
TZ = ZoneInfo(ID_ZONA)

# Sheet names (por defecto)
SHEET_GUILDS = os.getenv("GUILDS_SHEET", "Guilds")
SHEET_USERS  = os.getenv("USERS_SHEET",  "Usuarios")
SHEET_PLAYERS = os.getenv("PLAYERS_SHEET", "Players")
SHEET_ASSIGN_DEFAULT = os.getenv("ASSIGN_SHEET_DEFAULT", "Asignaciones ROTE")

# Scopes de permisos
SYNC_DATA_ALLOWED_CHATS  = {int(x) for x in os.getenv("SYNC_DATA_ALLOWED_CHATS",  "7367477801,30373681").split(",") if x.strip().isdigit()}
SYNC_GUILD_ALLOWED_CHATS = {int(x) for x in os.getenv("SYNC_GUILD_ALLOWED_CHATS", "7367477801,30373681").split(",") if x.strip().isdigit()}

# Conversación /registrar
REG_PICK_GUILD, REG_PICK_METHOD, REG_INPUT = range(3)

@dataclass
class RegContext:
    guild_id: str = ""
    guild_name: str = ""
    method: str = ""   # "alias" | "ally"
    pending_prompt: str = ""


# ===================== Helpers Sheets =====================
def _load_service_account_creds():
    raw = os.getenv(SERVICE_ACCOUNT_ENV)
    if not raw:
        raise RuntimeError(f"Falta {SERVICE_ACCOUNT_ENV}")
    def try_json(s: str):
        try: return json.loads(s)
        except Exception: return None
    info = try_json(raw)
    if info is None:
        # ¿base64?
        try:
            import base64
            info = try_json(base64.b64decode(raw).decode("utf-8"))
        except Exception:
            info = None
    if info is None:
        # ¿ruta a fichero?
        with open(raw, "r", encoding="utf-8") as f:
            info = json.load(f)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)

def _open_spreadsheet():
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID")
    gc = gspread.authorize(_load_service_account_creds())
    return gc.open_by_key(SPREADSHEET_ID)

def _get_all(ws):
    vals = ws.get_all_values() or []
    if not vals:
        return [], []
    headers = [h.strip() for h in vals[0]]
    rows = vals[1:] if len(vals) > 1 else []
    return headers, rows

def _is_same_day(iso_str: str) -> bool:
    if not iso_str:
        return False
    try:
        dt = datetime.fromisoformat(iso_str)
    except Exception:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    dt_local = dt.astimezone(TZ)
    now_local = datetime.now(TZ)
    return (dt_local.date() == now_local.date())

def _norm(s: str) -> str:
    return (s or "").strip()

# ===================== Guild helpers =====================
def _guild_rows(ss):
    ws = ss.worksheet(SHEET_GUILDS)
    headers, rows = _get_all(ws)
    cm = {h.lower(): i for i, h in enumerate(headers)}
    # columnas necesarias
    igid   = cm.get("guild id")
    iname  = cm.get("guild name")
    ishort = cm.get("nombre abreviado")
    ilast  = cm.get("last update")
    irote  = cm.get("rote")  # nombre de pestaña de asignaciones del gremio
    return {
        "headers": headers, "rows": rows,
        "idx": {"id": igid, "name": iname, "short": ishort, "last": ilast, "rote": irote}
    }

def _guild_map_by_id(ss) -> Dict[str, Dict[str,str]]:
    """Devuelve dict por Guild Id -> {name, short, last, rote}"""
    g = _guild_rows(ss)
    rows = g["rows"]; idx = g["idx"]
    out = {}
    for r in rows:
        gid   = _norm(r[idx["id"]])   if idx["id"]   is not None and idx["id"]   < len(r) else ""
        name  = _norm(r[idx["name"]]) if idx["name"] is not None and idx["name"] < len(r) else ""
        short = _norm(r[idx["short"]])if idx["short"]is not None and idx["short"]< len(r) else ""
        last  = _norm(r[idx["last"]]) if idx["last"] is not None and idx["last"] < len(r) else ""
        rote  = _norm(r[idx["rote"]]) if idx["rote"] is not None and idx["rote"] < len(r) else ""
        if gid:
            out[gid] = {"name": name, "short": (short or name), "last": last, "rote": (rote or "")}
    return out

def _guild_map_by_name(ss) -> Dict[str, Dict[str,str]]:
    """Devuelve dict por Guild Name -> {id, short, last, rote}"""
    g = _guild_rows(ss)
    rows = g["rows"]; idx = g["idx"]
    out = {}
    for r in rows:
        gid   = _norm(r[idx["id"]])   if idx["id"]   is not None and idx["id"]   < len(r) else ""
        name  = _norm(r[idx["name"]]) if idx["name"] is not None and idx["name"] < len(r) else ""
        short = _norm(r[idx["short"]])if idx["short"]is not None and idx["short"]< len(r) else ""
        last  = _norm(r[idx["last"]]) if idx["last"] is not None and idx["last"] < len(r) else ""
        rote  = _norm(r[idx["rote"]]) if idx["rote"] is not None and idx["rote"] < len(r) else ""
        if name:
            out[name] = {"id": gid, "short": (short or name), "last": last, "rote": (rote or "")}
    return out

def _assignments_sheet_for_guild(ss, guild_name: str) -> str:
    """Busca en Guilds la columna 'ROTE' para ese guild_name; si no existe, devuelve el default."""
    ws = ss.worksheet(SHEET_GUILDS)
    headers, rows = _get_all(ws)
    cm = {h.lower(): i for i, h in enumerate(headers)}
    iname = cm.get("guild name"); irote = cm.get("rote")
    if iname is None:
        return SHEET_ASSIGN_DEFAULT
    for r in rows:
        name = _norm(r[iname]) if iname < len(r) else ""
        if name == guild_name:
            if irote is not None and irote < len(r):
                val = _norm(r[irote])
                return val if val else SHEET_ASSIGN_DEFAULT
            break
    return SHEET_ASSIGN_DEFAULT

# ===================== Users helpers =====================
def _user_guilds(ss, user_id: int) -> List[str]:
    """Devuelve lista de Guild Name (columna 'guild_name') para user_id en hoja Usuarios."""
    try:
        ws = ss.worksheet(SHEET_USERS)
    except Exception:
        return []
    headers, rows = _get_all(ws)
    cm = {h.lower(): i for i, h in enumerate(headers)}
    i_uid = cm.get("user_id")
    i_gn  = cm.get("guild_name")
    if i_uid is None or i_gn is None:
        return []
    out = []
    for r in rows:
        uid = _norm(r[i_uid]) if i_uid < len(r) else ""
        gn  = _norm(r[i_gn])  if i_gn  < len(r) else ""
        if uid and gn and str(user_id) == uid:
            out.append(gn)
    return sorted(set(out))

def _users_header_map(ws) -> Dict[str,int]:
    headers, _ = _get_all(ws)
    if not headers:
        headers = ["alias","username","user_id","chat_id","Rol","allycode","guild_name"]
        ws.update(values=[headers], range_name="A1")
    return {h.lower(): i for i, h in enumerate(headers)}

def _players_header_map(ws) -> Dict[str,int]:
    headers, _ = _get_all(ws)
    return {h.lower(): i for i, h in enumerate(headers)}

# ===================== Access helpers =====================
def _is_allowed(chat_id: int, allowed: set[int]) -> bool:
    return chat_id in allowed

# ===================== /start =====================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "¡Hola! Soy el bot de Aroa'yr SWGOH.\n"
        "Comandos:\n"
        "• /registrar — vincula tu usuario con tu jugador del gremio\n"
        "• /misoperaciones — muestra tus asignaciones ROTE\n"
        "• /syncguild — sincroniza un gremio (admins)\n"
        "• /syncdata — refresca catálogos (admins)"
    )

# ===================== /registrar (multi-gremio) =====================
def _guild_keyboard_for_register(ss) -> InlineKeyboardMarkup:
    by_name = _guild_map_by_name(ss)
    # Ordenar por short name
    items = sorted(((name, meta["short"], meta.get("id","")) for name, meta in by_name.items()),
                   key=lambda t: t[1].lower())
    buttons: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for _, short, gid in items:
        if not gid:
            continue
        row.append(InlineKeyboardButton(short, callback_data=f"reg|gid|{gid}"))
        if len(row) == 2:  # 2 por fila para móviles
            buttons.append(row); row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)

async def cmd_registrar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss = _open_spreadsheet()
    kb = _guild_keyboard_for_register(ss)
    context.user_data["reg"] = RegContext().__dict__
    await update.effective_message.reply_text(
        "Selecciona tu **gremio**:", reply_markup=kb, parse_mode=ParseMode.MARKDOWN
    )
    return REG_PICK_GUILD

async def cb_registrar_pick_guild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("reg|gid|"):
        return ConversationHandler.END
    gid = data.split("|", 2)[2]
    ss = _open_spreadsheet()
    by_id = _guild_map_by_id(ss)
    meta = by_id.get(gid)
    if not meta:
        await q.edit_message_text("❌ No se encontró el gremio.")
        return ConversationHandler.END

    guild_name = meta["name"]
    # ¿Ya registrado user_id + guild_name?
    ws = ss.worksheet(SHEET_USERS)
    cm = _users_header_map(ws)
    headers, rows = _get_all(ws)
    i_uid = cm.get("user_id"); i_gn = cm.get("guild_name")
    already = False
    if i_uid is not None and i_gn is not None:
        for r in rows:
            uid = _norm(r[i_uid]) if i_uid < len(r) else ""
            gn  = _norm(r[i_gn])  if i_gn  < len(r) else ""
            if uid == str(update.effective_user.id) and gn == guild_name:
                already = True; break
    if already:
        await q.edit_message_text(f"✅ Ya estás registrado para el gremio *{meta['short']}*.", parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

    # Guardar en contexto
    reg = RegContext(guild_id=gid, guild_name=guild_name)
    context.user_data["reg"] = reg.__dict__

    # Elegir método
    buttons = [
        [InlineKeyboardButton("Registrar por alias", callback_data="reg|method|alias"),
         InlineKeyboardButton("Registrar por ally code", callback_data="reg|method|ally")]
    ]
    await q.edit_message_text(
        f"Gremio: *{meta['short']}*\n¿Cómo quieres registrarte?",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=ParseMode.MARKDOWN
    )
    return REG_PICK_METHOD

async def cb_registrar_pick_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("reg|method|"):
        return ConversationHandler.END
    method = data.split("|", 2)[2]  # alias | ally
    regd = context.user_data.get("reg", {}) or {}
    regd["method"] = method
    if method == "ally":
        prompt = "Introduce tu *código de aliado* (solo números)."
    else:
        prompt = "Introduce tu *alias exacto* como aparece en la hoja Players (columna \"Player Name\")."
    regd["pending_prompt"] = prompt
    context.user_data["reg"] = regd
    await q.edit_message_text(prompt, parse_mode=ParseMode.MARKDOWN)
    return REG_INPUT

async def registrar_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss = _open_spreadsheet()
    text = (update.message.text or "").strip()
    regd = context.user_data.get("reg") or {}
    method = regd.get("method")
    guild_name = regd.get("guild_name") or ""
    if not method or not guild_name:
        await update.message.reply_text("❌ Sesión de registro inválida. Vuelve a usar /registrar.")
        return ConversationHandler.END

    # Validar en Players (coincidiendo Guild Name)
    ws_p = ss.worksheet(SHEET_PLAYERS)
    headers, rows = _get_all(ws_p)
    cm = {h.lower(): i for i, h in enumerate(headers)}
    i_pid = cm.get("player id")
    i_name = cm.get("player name")
    i_ally = cm.get("ally code")
    i_gn   = cm.get("guild name")
    i_role = cm.get("role")

    if i_name is None or i_gn is None or i_ally is None or i_role is None:
        await update.message.reply_text("❌ La hoja Players no tiene los encabezados esperados.")
        return ConversationHandler.END

    match_row: Optional[List[str]] = None
    if method == "ally":
        ally_digits = "".join(ch for ch in text if ch.isdigit())
        for r in rows:
            gn = r[i_gn] if i_gn < len(r) else ""
            ac = r[i_ally] if i_ally < len(r) else ""
            if gn == guild_name and "".join(ch for ch in ac if ch.isdigit()) == ally_digits:
                match_row = r; break
    else:
        alias = text
        for r in rows:
            gn = r[i_gn] if i_gn < len(r) else ""
            nm = r[i_name] if i_name < len(r) else ""
            if gn == guild_name and nm == alias:
                match_row = r; break

    if not match_row:
        await update.message.reply_text(
            "⚠️ No pude encontrar ese alias/código en *Players* para el gremio seleccionado. "
            "Revisa el dato y vuelve a intentarlo.",
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationHandler.END

    alias   = match_row[i_name] if i_name < len(match_row) else ""
    ally    = match_row[i_ally] if i_ally < len(match_row) else ""
    role    = match_row[i_role] if i_role < len(match_row) else ""
    pid     = match_row[i_pid] if i_pid is not None and i_pid < len(match_row) else ""

    # Escribir en Usuarios: si ya existe fila con user_id+guild_name, no duplicar.
    ws_u = ss.worksheet(SHEET_USERS)
    ucm = _users_header_map(ws_u)
    u_headers, u_rows = _get_all(ws_u)
    i_alias = ucm.get("alias")
    i_usern = ucm.get("username")
    i_uid   = ucm.get("user_id")
    i_chat  = ucm.get("chat_id")
    i_rol   = ucm.get("rol")
    i_ac    = ucm.get("allycode")
    i_gn    = ucm.get("guild_name")

    # 1) ¿ya tiene fila por user_id+guild?
    exists_idx = None
    if i_uid is not None and i_gn is not None:
        for idx, r in enumerate(u_rows):
            if (i_uid < len(r) and i_gn < len(r)
                and _norm(r[i_uid]) == str(update.effective_user.id)
                and _norm(r[i_gn]) == guild_name):
                exists_idx = idx
                break

    if exists_idx is not None:
        # Actualiza columnas vacías
        row = (u_rows[exists_idx] + [""] * (len(u_headers) - len(u_rows[exists_idx])))[:len(u_headers)]
        def setv(i, val):
            if i is not None and i < len(row) and _norm(val):
                row[i] = str(val)
        setv(i_alias, alias)
        setv(i_usern, update.effective_user.username or "")
        setv(i_chat, update.effective_chat.id)
        setv(i_rol, role)
        setv(i_ac, ally)
        setv(i_gn, guild_name)
        u_rows[exists_idx] = row
        ws_u.update(values=[row], range_name=f"{exists_idx+2}:{exists_idx+2}")
        await update.message.reply_text(f"✅ Registro actualizado para *{alias}* en *{guild_name}*.", parse_mode=ParseMode.MARKDOWN)
    else:
        # 2) ¿hay fila por alias sin telegram? (mismo guild)
        alias_idx = None
        if i_alias is not None and i_gn is not None:
            for idx, r in enumerate(u_rows):
                if (i_alias < len(r) and i_gn < len(r)
                    and _norm(r[i_alias]) == alias and _norm(r[i_gn]) == guild_name):
                    alias_idx = idx
                    break
        if alias_idx is not None:
            row = (u_rows[alias_idx] + [""] * (len(u_headers) - len(u_rows[alias_idx])))[:len(u_headers)]
            def setv(i, val):
                if i is not None and i < len(row) and _norm(val):
                    row[i] = str(val)
            setv(i_usern, update.effective_user.username or "")
            setv(i_uid, update.effective_user.id)
            setv(i_chat, update.effective_chat.id)
            setv(i_rol, role)
            setv(i_ac, ally)
            u_rows[alias_idx] = row
            ws_u.update(values=[row], range_name=f"{alias_idx+2}:{alias_idx+2}")
            await update.message.reply_text(f"✅ Usuario vinculado a *{alias}* en *{guild_name}*.", parse_mode=ParseMode.MARKDOWN)
        else:
            # 3) insertar nueva fila
            row = [""] * len(u_headers)
            def setv(name: str, val):
                i = {h.lower(): idx for idx,h in enumerate(u_headers)}.get(name)
                if i is not None and i < len(row):
                    row[i] = str(val) if val is not None else ""
            setv("alias", alias)
            setv("username", update.effective_user.username or "")
            setv("user_id", update.effective_user.id)
            setv("chat_id", update.effective_chat.id)
            setv("rol", role)
            setv("allycode", ally)
            setv("guild_name", guild_name)
            ws_u.append_row(row, value_input_option="RAW")
            await update.message.reply_text(f"✅ Registrado como *{alias}* en *{guild_name}*.", parse_mode=ParseMode.MARKDOWN)

    return ConversationHandler.END

async def registrar_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("Registro cancelado.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

# ===================== /misoperaciones =====================
async def cmd_misoperaciones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss = _open_spreadsheet()
    user_id = update.effective_user.id
    my_guilds = _user_guilds(ss, user_id)
    if not my_guilds:
        await update.effective_message.reply_text("No estás registrado en ningún gremio. Usa /registrar.")
        return
    if len(my_guilds) > 1:
        # elegir gremio (por nombre abreviado)
        gmap = _guild_map_by_name(ss)
        buttons = []
        for gn in my_guilds:
            short = gmap.get(gn, {}).get("short", gn)
            buttons.append([InlineKeyboardButton(short, callback_data=f"misops|gname|{gn}")])
        await update.effective_message.reply_text("Selecciona el gremio:", reply_markup=InlineKeyboardMarkup(buttons))
        return
    # uno solo
    await _send_ops_for_guild(update, context, my_guilds[0])

async def cb_misops_pick_guild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("misops|gname|"):
        return
    gn = data.split("|", 2)[2]
    await _send_ops_for_guild(update, context, gn, edit=True)

async def _send_ops_for_guild(update: Update, context: ContextTypes.DEFAULT_TYPE, guild_name: str, edit: bool=False):
    ss = _open_spreadsheet()
    # Obtener alias del usuario para ese guild
    ws_u = ss.worksheet(SHEET_USERS)
    uh, ur = _get_all(ws_u)
    ucm = {h.lower(): i for i, h in enumerate(uh)}
    i_uid = ucm.get("user_id"); i_gn = ucm.get("guild_name"); i_alias = ucm.get("alias")
    alias = None
    if i_uid is not None and i_gn is not None and i_alias is not None:
        for r in ur:
            if (i_uid < len(r) and i_gn < len(r) and i_alias < len(r)
                and _norm(r[i_uid]) == str(update.effective_user.id)
                and _norm(r[i_gn]) == guild_name):
                alias = _norm(r[i_alias]); break
    if not alias:
        msg = f"No encuentro tu alias en *{guild_name}*. Repite /registrar."
        if edit:
            await update.callback_query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN)
        else:
            await update.effective_message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        return

    # Hoja de asignaciones para ese gremio
    sheet_name = _assignments_sheet_for_guild(ss, guild_name)
    try:
        ws = ss.worksheet(sheet_name)
    except Exception:
        msg = f"No existe la hoja de asignaciones *{sheet_name}*."
        if edit: await update.callback_query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN)
        else:    await update.effective_message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        return

    # Asumimos columnas A..F: fase, planeta, operacion, personaje, reliquia, jugador
    vals = ws.get_all_values() or []
    if len(vals) < 2:
        txt = f"No hay asignaciones en *{sheet_name}*."
        if edit: await update.callback_query.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN)
        else:    await update.effective_message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)
        return

    rows = vals[1:]
    # filtrar por alias en col F
    out: Dict[str, List[Tuple[str,str,str]]] = {}
    for r in rows:
        if len(r) < 6:
            continue
        fase, planeta, operacion, personaje, reliquia, jugador = r[:6]
        if _norm(jugador) != alias:
            continue
        out.setdefault(_norm(fase), []).append((planeta, personaje, reliquia))

    if not out:
        txt = f"No tienes asignaciones hoy para *{guild_name}*."
    else:
        lines = [f"*Asignaciones de {alias}* — *{guild_name}*"]
        for fase in sorted(out.keys(), key=lambda x: (len(x), x)):
            lines.append(f"\n*Fase {fase}*")
            for planeta, personaje, reliquia in out[fase]:
                req = reliquia or "—"
                lines.append(f"• {personaje} ({req}) — {planeta}")
        txt = "\n".join(lines)

    if edit:
        await update.callback_query.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN)
    else:
        await update.effective_message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)

# ===================== /syncdata (admins) =====================
async def cmd_syncdata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_chat.id, SYNC_DATA_ALLOWED_CHATS):
        await update.effective_message.reply_text("❌ No tienes permiso para usar /syncdata.")
        return
    await update.effective_message.reply_text("🔄 Ejecutando *sync_data*…", parse_mode=ParseMode.MARKDOWN)
    def run_sync():
        cmd = [sys.executable, "-u", "-m", "swgoh.processing.sync_data"]
        return subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    try:
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(None, run_sync)
    except subprocess.TimeoutExpired:
        await update.effective_message.reply_text("❌ Timeout ejecutando sync_data.")
        return
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Error ejecutando sync_data: {e}")
        return

    if res.returncode == 0:
        tail = (res.stdout or "").strip()
        if len(tail) > 900: tail = tail[-900:]
        await update.effective_message.reply_text(f"✅ *sync_data* OK:\n```\n{tail}\n```", parse_mode=ParseMode.MARKDOWN)
    else:
        err = (res.stderr or res.stdout or "").strip()
        if len(err) > 900: err = err[-900:]
        await update.effective_message.reply_text(f"❌ *sync_data* falló:\n```\n{err}\n```", parse_mode=ParseMode.MARKDOWN)

# ===================== /syncguild (admins) =====================
def _guild_keyboard_for_sync(ss, user_id: int) -> InlineKeyboardMarkup:
    user_gnames = _user_guilds(ss, user_id)
    by_name = _guild_map_by_name(ss)
    buttons: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for gn in sorted(user_gnames, key=lambda s: by_name.get(s, {}).get("short", s).lower()):
        meta = by_name.get(gn) or {}
        gid = meta.get("id", "")
        short = meta.get("short", gn)
        if not gid:
            continue
        row.append(InlineKeyboardButton(short, callback_data=f"syncguild|gid|{gid}"))
        if len(row) == 2:
            buttons.append(row); row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)

async def cmd_syncguild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_chat.id, SYNC_GUILD_ALLOWED_CHATS):
        await update.effective_message.reply_text("❌ No tienes permiso para usar /syncguild.")
        return
    ss = _open_spreadsheet()
    kb = _guild_keyboard_for_sync(ss, update.effective_user.id)
    await update.effective_message.reply_text("Elige el gremio a sincronizar:", reply_markup=kb)

async def cb_syncguild_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("syncguild|gid|"):
        return
    gid = data.split("|", 2)[2]

    ss = _open_spreadsheet()
    by_id = _guild_map_by_id(ss)
    meta = by_id.get(gid)
    if not meta:
        await q.edit_message_text("❌ No se encontró el gremio seleccionado.")
        return

    guild_name = meta["name"]
    short = meta["short"]
    last = meta["last"]

    if _is_same_day(last or ""):
        try:
            dt = datetime.fromisoformat(last)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ)
            dt = dt.astimezone(TZ)
            hora = dt.strftime("%H:%M")
            fecha = dt.strftime("%Y-%m-%d")
            await q.edit_message_text(f"⏳ Ya se sincronizó hoy *{short}* ({fecha} {hora}).")
        except Exception:
            await q.edit_message_text(f"⏳ Ya se sincronizó hoy *{short}*.")
        return

    await q.edit_message_text(f"🔄 Sincronizando *{short}*…", parse_mode=ParseMode.MARKDOWN)

    # Ejecutar sync_guilds solo para ese guildId
    def run_sync():
        env = os.environ.copy()
        env["FILTER_GUILD_IDS"] = gid
        cmd = [sys.executable, "-u", "-m", "swgoh.processing.sync_guilds"]
        return subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=1800)

    try:
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(None, run_sync)
        if res.returncode == 0:
            tail = (res.stdout or "").strip()
            if len(tail) > 900: tail = tail[-900:]
            await q.edit_message_text(f"✅ Sync completada para *{short}*.\n```\n{tail}\n```", parse_mode=ParseMode.MARKDOWN)
        else:
            err = (res.stderr or res.stdout or "").strip()
            if len(err) > 900: err = err[-900:]
            await q.edit_message_text(f"❌ Error sincronizando *{short}*.\n```\n{err}\n```", parse_mode=ParseMode.MARKDOWN)
    except subprocess.TimeoutExpired:
        await q.edit_message_text("❌ Timeout ejecutando la sincronización.")
    except Exception as e:
        await q.edit_message_text(f"❌ Fallo ejecutando la sincronización: {e}")

# ===================== Arranque =====================
def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN")

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # /start
    app.add_handler(CommandHandler("start", cmd_start))

    # /registrar (conversación)
    conv = ConversationHandler(
        entry_points=[CommandHandler("registrar", cmd_registrar)],
        states={
            REG_PICK_GUILD: [CallbackQueryHandler(cb_registrar_pick_guild, pattern=r"^reg\|gid\|")],
            REG_PICK_METHOD:[CallbackQueryHandler(cb_registrar_pick_method, pattern=r"^reg\|method\|")],
            REG_INPUT:      [MessageHandler(filters.TEXT & ~filters.COMMAND, registrar_input)],
        },
        fallbacks=[CommandHandler("cancelar", registrar_cancel)],
        name="registrar_conversation",
        persistent=False,
    )
    app.add_handler(conv)

    # /misoperaciones
    app.add_handler(CommandHandler("misoperaciones", cmd_misoperaciones))
    app.add_handler(CallbackQueryHandler(cb_misops_pick_guild, pattern=r"^misops\|gname\|"))

    # Admin: /syncdata y /syncguild
    app.add_handler(CommandHandler("syncdata", cmd_syncdata))
    app.add_handler(CommandHandler("syncguild", cmd_syncguild))
    app.add_handler(CallbackQueryHandler(cb_syncguild_pick, pattern=r"^syncguild\|gid\|"))

    app.run_polling(drop_pending_updates=True)
    print("[main_bot] Bot iniciado (polling).")

if __name__ == "__main__":
    main()
