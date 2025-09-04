# src/swgoh/bot/main_bot.py
from __future__ import annotations

import os
import sys
import re
import json
import base64
import asyncio
import logging
import subprocess
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# -----------------------------------------------------------------------------
# Config & logging
# -----------------------------------------------------------------------------
log = logging.getLogger("main_bot")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Falta TELEGRAM_BOT_TOKEN en config o entorno.")

ID_ZONA = os.getenv("ID_ZONA", "Europe/Madrid")
TZ = ZoneInfo(ID_ZONA)

SHEET_GUILDS = os.getenv("GUILDS_SHEET", "Guilds")
SHEET_USUARIOS = os.getenv("USUARIOS_SHEET", "Usuarios")
SHEET_PLAYERS = os.getenv("PLAYERS_SHEET", "Players")

# Permisos: preferimos SYNC_GUILD_ALLOWED_CHATS; si no, caemos en SYNC_ALLOWED_CHATS
def _parse_ids(env_name: str) -> set[int]:
    raw = os.getenv(env_name, "")
    out: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except Exception:
            pass
    return out

ALLOWED_SYNC_GUILD = _parse_ids("SYNC_GUILD_ALLOWED_CHATS") or _parse_ids("SYNC_ALLOWED_CHATS")
ALLOWED_SYNC_DATA = _parse_ids("SYNC_ALLOWED_CHATS")

# -----------------------------------------------------------------------------
# Google Sheets helpers
# -----------------------------------------------------------------------------
def _load_service_account_creds():
    raw = os.getenv("SERVICE_ACCOUNT_FILE")
    if not raw:
        raise RuntimeError("Falta SERVICE_ACCOUNT_FILE")

    def _try_json(s: str):
        try:
            return json.loads(s)
        except Exception:
            return None

    info = _try_json(raw)
    if info is None:
        try:
            decoded = base64.b64decode(raw).decode("utf-8")
            info = _try_json(decoded)
        except Exception:
            info = None

    if info is None:
        with open(raw, "r", encoding="utf-8") as f:
            info = json.load(f)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)

def _open_spreadsheet():
    sid = os.getenv("SPREADSHEET_ID")
    if not sid:
        raise RuntimeError("Falta SPREADSHEET_ID")
    gc = gspread.authorize(_load_service_account_creds())
    return gc.open_by_key(sid)

def _headers(ws) -> List[str]:
    vals = ws.row_values(1) or []
    return [h.strip() for h in vals]

def _get_all(ws) -> Tuple[List[str], List[List[str]]]:
    vals = ws.get_all_values() or []
    if not vals:
        return [], []
    headers = [h.strip() for h in vals[0]]
    rows = vals[1:] if len(vals) > 1 else []
    return headers, rows

def _colmap(ws) -> Dict[str, int]:
    return {h.strip().lower(): i + 1 for i, h in enumerate(_headers(ws))}

def _ensure_headers(ws, required: List[str]):
    headers = _headers(ws)
    if not headers:
        ws.update(values=[required], range_name="A1")
        return
    lower = [h.lower() for h in headers]
    changed = False
    for r in required:
        if r.lower() not in lower:
            headers.append(r)
            lower.append(r.lower())
            changed = True
    if changed:
        ws.update(values=[headers], range_name="1:1")

# -----------------------------------------------------------------------------
# Utilidades varias
# -----------------------------------------------------------------------------
def _now_ts() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")

def _split_chunks(txt: str, maxlen: int = 3500) -> List[str]:
    if len(txt) <= maxlen:
        return [txt]
    parts: List[str] = []
    cur = []
    cur_len = 0
    for line in txt.splitlines(True):
        if cur_len + len(line) > maxlen:
            parts.append("".join(cur))
            cur = [line]
            cur_len = len(line)
        else:
            cur.append(line)
            cur_len += len(line)
    if cur:
        parts.append("".join(cur))
    return parts

# -----------------------------------------------------------------------------
# Registro de usuarios (/registrar)
# -----------------------------------------------------------------------------
@dataclass
class GuildOption:
    name: str
    short: str
    rownum: int

def _list_guild_options(ss) -> List[GuildOption]:
    ws = ss.worksheet(SHEET_GUILDS)
    headers, rows = _get_all(ws)
    if not rows:
        return []
    cmap = {h.lower(): i for i, h in enumerate(headers)}
    i_name = cmap.get("guild name")
    i_short = cmap.get("nombre abreviado")  # columna opcional
    options: List[GuildOption] = []
    for idx, r in enumerate(rows, start=2):
        name = (r[i_name] if i_name is not None and i_name < len(r) else "").strip()
        if not name:
            continue
        short = (r[i_short] if i_short is not None and i_short < len(r) else "").strip()
        options.append(GuildOption(name=name, short=short or name, rownum=idx))
    return options

def _user_guilds(ss, user_id: int) -> List[str]:
    ws = ss.worksheet(SHEET_USUARIOS)
    headers, rows = _get_all(ws)
    if not rows:
        return []
    cm = {h.lower(): i for i, h in enumerate(headers)}
    i_uid = cm.get("user_id")
    i_gname = cm.get("guild_name")
    out, seen = [], set()
    for r in rows:
        if i_uid is not None and i_uid < len(r) and r[i_uid] == str(user_id):
            g = (r[i_gname] if i_gname is not None and i_gname < len(r) else "").strip()
            if g and g.lower() not in seen:
                out.append(g)
                seen.add(g.lower())
    return out

async def cmd_registrar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss = _open_spreadsheet()
    u = update.effective_user
    uid = u.id

    # 1) Elegir gremio (se muestra "nombre abreviado" si existe)
    options = _list_guild_options(ss)
    if not options:
        await update.effective_message.reply_text("No hay gremios en la hoja Guilds.")
        return

    # Menú en varias filas si hay muchos
    buttons: List[List[InlineKeyboardButton]] = []
    for opt in options:
        data = f"reg|g|{opt.rownum}"
        label = opt.short
        buttons.append([InlineKeyboardButton(label, callback_data=data)])
    kb = InlineKeyboardMarkup(buttons)
    await update.effective_message.reply_text(
        "Selecciona tu *gremio*:", reply_markup=kb, parse_mode=ParseMode.MARKDOWN
    )

async def cb_registrar_pick_guild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("reg|g|"):
        return
    ss = _open_spreadsheet()
    ws_g = ss.worksheet(SHEET_GUILDS)
    headers, rows = _get_all(ws_g)
    rownum = int(data.split("|")[-1])
    idx = rownum - 2
    if idx < 0 or idx >= len(rows):
        await q.edit_message_text("Gremio no encontrado.")
        return

    cm = {h.lower(): i for i, h in enumerate(headers)}
    i_name = cm.get("guild name")
    guild_name = (rows[idx][i_name] if i_name is not None and i_name < len(rows[idx]) else "").strip()

    # 2) Antes de pedir alias/allycode: ¿ya registrado para ese gremio?
    ws_u = ss.worksheet(SHEET_USUARIOS)
    _ensure_headers(ws_u, ["alias", "username", "user_id", "chat_id", "Rol", "allycode", "guild_name"])
    uh, ur = _get_all(ws_u)
    ucm = {h.lower(): i for i, h in enumerate(uh)}
    i_uid = ucm.get("user_id"); i_g = ucm.get("guild_name")
    uid = str(update.effective_user.id)

    for r in ur:
        if i_uid is not None and i_g is not None and i_uid < len(r) and i_g < len(r):
            if r[i_uid] == uid and (r[i_g] or "").strip() == guild_name:
                await q.edit_message_text(f"Ya estabas registrado para *{guild_name}* ✅", parse_mode=ParseMode.MARKDOWN)
                return

    # 3) Elegir método de registro
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Por alias", callback_data=f"reg|m|alias|{rownum}")],
        [InlineKeyboardButton("Por allycode", callback_data=f"reg|m|ally|{rownum}")],
    ])
    await q.edit_message_text(
        f"Gremio: *{guild_name}*\n¿Cómo quieres registrarte?",
        reply_markup=kb, parse_mode=ParseMode.MARKDOWN
    )

async def cb_registrar_pick_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("reg|m|"):
        return

    _, _, method, rownum = data.split("|", 3)
    context.user_data["reg"] = {"method": method, "rownum": int(rownum)}
    prompt = "Escribe tu *alias exactamente como aparece en Players*:" if method == "alias" else "Escribe tu *allycode* (solo dígitos):"
    await q.edit_message_text(prompt, parse_mode=ParseMode.MARKDOWN)

async def on_text_registration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = context.user_data.get("reg")
    if not state:
        return
    ss = _open_spreadsheet()
    ws_g = ss.worksheet(SHEET_GUILDS)
    gh, gr = _get_all(ws_g)
    rownum = int(state["rownum"])
    idx = rownum - 2
    cmg = {h.lower(): i for i, h in enumerate(gh)}
    i_name = cmg.get("guild name")
    guild_name = (gr[idx][i_name] if i_name is not None and i_name < len(gr[idx]) else "").strip()

    method = state["method"]
    text = (update.message.text or "").strip()

    # Validar entrada
    if method == "ally":
        digits = re.sub(r"\D+", "", text)
        if not digits:
            await update.message.reply_text("Allycode inválido. Inténtalo de nuevo con solo dígitos.")
            return
        key = ("ally", digits)
    else:
        key = ("alias", text)

    # Buscar en Players por guild + alias/ally
    ws_p = ss.worksheet(SHEET_PLAYERS)
    ph, pr = _get_all(ws_p)
    pcm = {h.lower(): i for i, h in enumerate(ph)}
    i_pname = pcm.get("player name")
    i_ally = pcm.get("ally code")
    i_gn = pcm.get("guild name")
    i_role = pcm.get("role")

    if i_pname is None or i_ally is None or i_gn is None:
        await update.message.reply_text("Hoja Players sin columnas esperadas (Player Name, Ally code, Guild Name).")
        context.user_data.pop("reg", None)
        return

    found_row: Optional[List[str]] = None
    for r in pr:
        g = (r[i_gn] if i_gn < len(r) else "").strip()
        if g != guild_name:
            continue
        if key[0] == "alias":
            if i_pname < len(r) and (r[i_pname] or "").strip() == key[1]:
                found_row = r
                break
        else:
            a = (r[i_ally] if i_ally < len(r) else "").strip()
            a_digits = re.sub(r"\D+", "", a)
            if a_digits == key[1]:
                found_row = r
                break

    if not found_row:
        await update.message.reply_text("No encontré ese jugador en Players para el gremio elegido. No te he registrado.")
        context.user_data.pop("reg", None)
        return

    alias = (found_row[i_pname] if i_pname < len(found_row) else "").strip()
    allycode = (found_row[i_ally] if i_ally < len(found_row) else "").strip()
    role = (found_row[i_role] if i_role is not None and i_role < len(found_row) else "").strip()

    # Escribir/actualizar en Usuarios:
    ws_u = ss.worksheet(SHEET_USUARIOS)
    _ensure_headers(ws_u, ["alias", "username", "user_id", "chat_id", "Rol", "allycode", "guild_name"])
    uh, ur = _get_all(ws_u)
    ucm = {h.lower(): i for i, h in enumerate(uh)}
    i_alias = ucm["alias"]; i_username = ucm["username"]; i_uid = ucm["user_id"]
    i_chat = ucm["chat_id"]; i_rol = ucm["rol"]; i_ac = ucm["allycode"]; i_g = ucm["guild_name"]

    uid = str(update.effective_user.id)
    uname = (update.effective_user.username or "").strip()
    chat_id = str(update.effective_chat.id)

    # 1) ¿Existe fila con mismo user_id + guild_name? -> ya estaba (pero rellenamos campos vacíos)
    for idx_r, r in enumerate(ur):
        if i_uid < len(r) and i_g < len(r) and r[i_uid] == uid and (r[i_g] or "").strip() == guild_name:
            # actualizar campos vacíos
            r = r[:] + [""] * (len(uh) - len(r))
            if not r[i_alias]: r[i_alias] = alias
            if not r[i_username]: r[i_username] = uname
            if not r[i_chat]: r[i_chat] = chat_id
            if not r[i_rol]: r[i_rol] = role
            if not r[i_ac]: r[i_ac] = allycode
            if not r[i_g]: r[i_g] = guild_name
            ur[idx_r] = r
            ws_u.update(values=ur, range_name=f"2:{len(ur)+1}")
            await update.message.reply_text(f"Actualizado tu registro en *{guild_name}* ✅", parse_mode=ParseMode.MARKDOWN)
            context.user_data.pop("reg", None)
            return

    # 2) Si no, ¿existe fila con ese alias + guild? -> completar/actualizar con Telegram
    for idx_r, r in enumerate(ur):
        if i_alias < len(r) and i_g < len(r) and (r[i_alias] or "").strip() == alias and (r[i_g] or "").strip() == guild_name:
            r = r[:] + [""] * (len(uh) - len(r))
            r[i_username] = uname
            r[i_uid] = uid
            r[i_chat] = chat_id
            r[i_rol] = role
            r[i_ac] = allycode
            r[i_g] = guild_name
            ur[idx_r] = r
            ws_u.update(values=ur, range_name=f"2:{len(ur)+1}")
            await update.message.reply_text(f"Registro completado en *{guild_name}* ✅", parse_mode=ParseMode.MARKDOWN)
            context.user_data.pop("reg", None)
            return

    # 3) Si no existe, insertamos nueva fila
    new_row = [""] * len(uh)
    new_row[i_alias] = alias
    new_row[i_username] = uname
    new_row[i_uid] = uid
    new_row[i_chat] = chat_id
    new_row[i_rol] = role
    new_row[i_ac] = allycode
    new_row[i_g] = guild_name
    ur.append(new_row)
    ws_u.update(values=ur, range_name=f"2:{len(ur)+1}")
    await update.message.reply_text(f"Registrado en *{guild_name}* ✅", parse_mode=ParseMode.MARKDOWN)
    context.user_data.pop("reg", None)

# -----------------------------------------------------------------------------
# Mis operaciones (/misoperaciones)
# -----------------------------------------------------------------------------
def _guild_row_by_name(ss, guild_name: str):
    ws = ss.worksheet(SHEET_GUILDS)
    headers, rows = _get_all(ws)
    if not rows:
        return None, None, None  # gid, rote, rownum
    cm = {h.lower(): i for i, h in enumerate(headers)}
    i_name = cm.get("guild name")
    i_gid = cm.get("guild id")
    i_rote = cm.get("rote")
    for idx, r in enumerate(rows, start=2):
        n = (r[i_name] if i_name is not None and i_name < len(r) else "").strip()
        if n == guild_name:
            gid = (r[i_gid] if i_gid is not None and i_gid < len(r) else "").strip()
            rote = (r[i_rote] if i_rote is not None and i_rote < len(r) else "").strip()
            return gid, rote, idx
    return None, None, None

def _user_alias_in_guild(ss, user_id: int, guild_name: str) -> Optional[str]:
    ws = ss.worksheet(SHEET_USUARIOS)
    headers, rows = _get_all(ws)
    cm = {h.lower(): i for i, h in enumerate(headers)}
    i_uid = cm.get("user_id"); i_g = cm.get("guild_name"); i_alias = cm.get("alias")
    for r in rows:
        if i_uid is not None and i_g is not None and i_alias is not None:
            if (r[i_uid] if i_uid < len(r) else "") == str(user_id) and (r[i_g] if i_g < len(r) else "") == guild_name:
                return (r[i_alias] if i_alias < len(r) else "").strip()
    return None

async def cmd_misoperaciones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss = _open_spreadsheet()
    uid = update.effective_user.id
    guilds = _user_guilds(ss, uid)
    if not guilds:
        await update.effective_message.reply_text("No estás registrado en ningún gremio.")
        return
    if len(guilds) > 1:
        buttons = [[InlineKeyboardButton(g, callback_data=f"ops|{g}")] for g in guilds]
        kb = InlineKeyboardMarkup(buttons)
        await update.effective_message.reply_text("Elige el gremio:", reply_markup=kb)
        return
    await _send_ops_for_guild(update, context, ss, guilds[0])

async def _send_ops_for_guild(update: Update, context: ContextTypes.DEFAULT_TYPE, ss, guild_name: str):
    gid, rote_sheet, _ = _guild_row_by_name(ss, guild_name)
    if not rote_sheet:
        await update.effective_message.reply_text(f"El gremio *{guild_name}* no tiene configurada la hoja ROTE.", parse_mode=ParseMode.MARKDOWN)
        return
    alias = _user_alias_in_guild(ss, update.effective_user.id, guild_name)
    if not alias:
        await update.effective_message.reply_text("No encuentro tu alias para ese gremio en Usuarios.")
        return
    try:
        ws = ss.worksheet(rote_sheet)
    except Exception:
        await update.effective_message.reply_text(f"No existe la hoja “{rote_sheet}”.")
        return

    headers, rows = _get_all(ws)
    cm = {h.lower(): i for i, h in enumerate(headers)}
    i_fase = cm.get("fase"); i_planeta = cm.get("planeta"); i_op = cm.get("operacion")
    i_pers = cm.get("personaje"); i_rel = cm.get("reliquia"); i_j = cm.get("jugador")
    if None in (i_fase, i_planeta, i_op, i_pers, i_rel, i_j):
        await update.effective_message.reply_text("La hoja de asignaciones no tiene columnas esperadas (fase, planeta, operacion, personaje, reliquia, jugador).")
        return

    # Filtrar por alias exacto
    data = []
    for r in rows:
        j = (r[i_j] if i_j < len(r) else "").strip()
        if j != alias:
            continue
        fase = (r[i_fase] if i_fase < len(r) else "").strip()
        planeta = (r[i_planeta] if i_planeta < len(r) else "").strip()
        op = (r[i_op] if i_op < len(r) else "").strip()
        pj = (r[i_pers] if i_pers < len(r) else "").strip()
        rel = (r[i_rel] if i_rel < len(r) else "").strip()
        data.append((fase, planeta, op, pj, rel))

    if not data:
        await update.effective_message.reply_text("No tienes asignaciones aún.")
        return

    # Ordenar por fase, planeta
    def _key(x):
        f = x[0]
        try:
            fn = int(f)
        except Exception:
            fn = 999
        return (fn, x[1], x[2], x[3])
    data.sort(key=_key)

    lines = [f"*{guild_name}* — Asignaciones de *{alias}*"]
    cur_f = None
    for fase, planeta, op, pj, rel in data:
        if fase != cur_f:
            lines.append(f"\n*Fase {fase}*")
            cur_f = fase
        lines.append(f"• {planeta} — {op} — {pj} ({rel or '-'})")

    text = "\n".join(lines)
    for chunk in _split_chunks(text):
        await update.effective_message.reply_text(chunk, parse_mode=ParseMode.MARKDOWN)

async def cb_ops_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("ops|"):
        return
    guild_name = data.split("|", 1)[1]
    ss = _open_spreadsheet()
    await _send_ops_for_guild(update, context, ss, guild_name)

# -----------------------------------------------------------------------------
# /syncdata (solo admins)
# -----------------------------------------------------------------------------
def _is_allowed_syncdata(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else None
    cid = update.effective_chat.id if update.effective_chat else None
    if not ALLOWED_SYNC_DATA:
        return False
    return (uid in ALLOWED_SYNC_DATA) or (cid in ALLOWED_SYNC_DATA)

async def cmd_syncdata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed_syncdata(update):
        await update.effective_message.reply_text("❌ No tienes permiso para usar /syncdata.")
        return

    await update.effective_message.reply_text("🔄 Ejecutando *sync_data*…", parse_mode=ParseMode.MARKDOWN)

    def run_sync():
        cmd = [sys.executable, "-u", "-m", "swgoh.processing.sync_data"]
        return subprocess.run(cmd, capture_output=True, text=True, timeout=1800)

    try:
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(None, run_sync)
        if res.returncode == 0:
            tail = (res.stdout or "").strip()
            if len(tail) > 500:
                tail = tail[-500:]
            await update.effective_message.reply_text(f"✅ sync_data OK\n```\n{tail}\n```", parse_mode=ParseMode.MARKDOWN)
        else:
            err = (res.stderr or res.stdout or "").strip()
            if len(err) > 600:
                err = err[-600:]
            await update.effective_message.reply_text(f"❌ Error en sync_data\n```\n{err}\n```", parse_mode=ParseMode.MARKDOWN)
    except subprocess.TimeoutExpired:
        await update.effective_message.reply_text("❌ Timeout ejecutando sync_data.")
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Fallo ejecutando sync_data: {e}")

# -----------------------------------------------------------------------------
# /syncguild (solo admins)
# -----------------------------------------------------------------------------
def _is_allowed_syncguild(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else None
    cid = update.effective_chat.id if update.effective_chat else None
    if not ALLOWED_SYNC_GUILD:
        return False
    return (uid in ALLOWED_SYNC_GUILD) or (cid in ALLOWED_SYNC_GUILD)

def _guild_last_update(ss, guild_name: str) -> Tuple[Optional[str], Optional[str]]:
    """Devuelve (guild_id, last_update_iso)"""
    ws = ss.worksheet(SHEET_GUILDS)
    headers, rows = _get_all(ws)
    cm = {h.lower(): i for i, h in enumerate(headers)}
    i_name = cm.get("guild name"); i_id = cm.get("guild id"); i_last = cm.get("last update")
    for r in rows:
        name = (r[i_name] if i_name is not None and i_name < len(r) else "").strip()
        if name == guild_name:
            gid = (r[i_id] if i_id is not None and i_id < len(r) else "").strip()
            last = (r[i_last] if i_last is not None and i_last < len(r) else "").strip()
            return gid, last
    return None, None

def _is_same_day(ts_iso: str) -> bool:
    if not ts_iso:
        return False
    try:
        dt = datetime.fromisoformat(ts_iso)
    except Exception:
        if ts_iso.endswith("Z"):
            try:
                dt = datetime.fromisoformat(ts_iso[:-1])
            except Exception:
                return False
        else:
            return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    else:
        dt = dt.astimezone(TZ)
    return dt.date() == datetime.now(TZ).date()

async def cmd_syncguild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed_syncguild(update):
        await update.effective_message.reply_text("❌ No tienes permiso para usar /syncguild.")
        return
    ss = _open_spreadsheet()
    guilds = _user_guilds(ss, update.effective_user.id)
    if not guilds:
        await update.effective_message.reply_text("No estás registrado en ningún gremio.")
        return
    buttons = [[InlineKeyboardButton(g, callback_data=f"syncguild|{g}")] for g in guilds]
    kb = InlineKeyboardMarkup(buttons)
    await update.effective_message.reply_text("Elige el gremio a sincronizar:", reply_markup=kb)

async def cb_syncguild_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("syncguild|"):
        return
    guild_name = data.split("|", 1)[1]
    ss = _open_spreadsheet()
    gid, last = _guild_last_update(ss, guild_name)
    if not gid:
        await q.edit_message_text(f"❌ No se encontró el gremio “{guild_name}”.")
        return
    if _is_same_day(last or ""):
        try:
            dt = datetime.fromisoformat(last)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ)
            dt = dt.astimezone(TZ)
            hora = dt.strftime("%H:%M")
            fecha = dt.strftime("%Y-%m-%d")
            await q.edit_message_text(f"⏳ Ya se sincronizó hoy ({fecha} {hora}).")
        except Exception:
            await q.edit_message_text("⏳ Ya se sincronizó hoy. Inténtalo mañana.")
        return

    await q.edit_message_text(f"🔄 Sincronizando *{guild_name}*…", parse_mode=ParseMode.MARKDOWN)

    # Ejecutar sync_guilds sólo para ese guild usando FILTER_GUILD_IDS
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
            if len(tail) > 600:
                tail = tail[-600:]
            await q.edit_message_text(f"✅ Sync completada para *{guild_name}*.\n```\n{tail}\n```", parse_mode=ParseMode.MARKDOWN)
        else:
            err = (res.stderr or res.stdout or "").strip()
            if len(err) > 800:
                err = err[-800:]
            await q.edit_message_text(f"❌ Error sincronizando *{guild_name}*.\n```\n{err}\n```", parse_mode=ParseMode.MARKDOWN)
    except subprocess.TimeoutExpired:
        await q.edit_message_text("❌ Timeout ejecutando la sincronización.")
    except Exception as e:
        await q.edit_message_text(f"❌ Fallo ejecutando la sincronización: {e}")

# -----------------------------------------------------------------------------
# /start y /help
# -----------------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "Hola 👋\n\n"
        "Comandos disponibles:\n"
        "• /registrar — Regístrate en un gremio\n"
        "• /misoperaciones — Tus asignaciones ROTE\n"
        "• /syncdata — (admins) Actualiza catálogos\n"
        "• /syncguild — (admins) Sincroniza un gremio\n"
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cmd_start(update, context)

# -----------------------------------------------------------------------------
# Arranque
# -----------------------------------------------------------------------------
def build_app():
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # Registro
    app.add_handler(CommandHandler("registrar", cmd_registrar))
    app.add_handler(CallbackQueryHandler(cb_registrar_pick_guild, pattern=r"^reg\|g\|\d+$"))
    app.add_handler(CallbackQueryHandler(cb_registrar_pick_method, pattern=r"^reg\|m\|(alias|ally)\|\d+$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_registration))

    # Mis operaciones
    app.add_handler(CommandHandler("misoperaciones", cmd_misoperaciones))
    app.add_handler(CallbackQueryHandler(cb_ops_pick, pattern=r"^ops\|"))

    # Sync data / guild
    app.add_handler(CommandHandler("syncdata", cmd_syncdata))
    app.add_handler(CommandHandler("syncguild", cmd_syncguild))
    app.add_handler(CallbackQueryHandler(cb_syncguild_pick, pattern=r"^syncguild\|"))

    # Básicos
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))

    return app

def main():
    app = build_app()
    log.info("Bot iniciado (polling).")
    app.run_polling(allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    main()
