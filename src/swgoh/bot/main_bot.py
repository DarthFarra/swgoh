# src/swgoh/bot/main_bot.py
import os
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Tuple, Any

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
    ContextTypes,
)

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [main_bot] %(message)s")
log = logging.getLogger("main_bot")

# --- Entorno ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_ENV = "SERVICE_ACCOUNT_FILE"

USERS_SHEET = os.getenv("USERS_SHEET", "Usuarios")
GUILDS_SHEET = os.getenv("GUILDS_SHEET", "Guilds")
TZ = ZoneInfo(os.getenv("ID_ZONA", "Europe/Amsterdam"))

# ---------------- Google Sheets helpers ----------------
def _load_creds():
    raw = os.getenv(SERVICE_ACCOUNT_ENV)
    if not raw:
        raise RuntimeError("Falta SERVICE_ACCOUNT_FILE")
    # puede venir como JSON, base64 o ruta a fichero
    try:
        info = json.loads(raw)
    except Exception:
        try:
            import base64
            info = json.loads(base64.b64decode(raw).decode("utf-8"))
        except Exception:
            with open(raw, "r", encoding="utf-8") as f:
                info = json.load(f)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly"]
    return Credentials.from_service_account_info(info, scopes=scopes)

def _open_ss():
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID")
    gc = gspread.authorize(_load_creds())
    return gc.open_by_key(SPREADSHEET_ID)

def _headers_lower(ws) -> List[str]:
    vals = ws.row_values(1) or []
    return [h.strip().lower() for h in vals]

def _map_guild_name_to_label_and_id(ss) -> Dict[str, Tuple[str, str]]:
    """
    Devuelve: Guild Name -> (label_para_mostrar, Guild Id)
    label = 'nombre abreviado' si existe, si no 'Guild Name'
    """
    ws_g = ss.worksheet(GUILDS_SHEET)
    vals = ws_g.get_all_values() or []
    if not vals:
        return {}
    hdr = [h.strip() for h in (vals[0] or [])]
    rows = vals[1:] if len(vals) > 1 else []
    hdr_l = [h.lower() for h in hdr]
    try:
        i_name = hdr_l.index("guild name")
        i_id = hdr_l.index("guild id")
    except ValueError:
        return {}
    i_abbr = hdr_l.index("nombre abreviado") if "nombre abreviado" in hdr_l else None

    out: Dict[str, Tuple[str, str]] = {}
    for r in rows:
        gname = (r[i_name] if i_name < len(r) else "").strip()
        gid = (r[i_id] if i_id < len(r) else "").strip()
        abbr = (r[i_abbr] if (i_abbr is not None and i_abbr < len(r)) else "").strip() if i_abbr is not None else ""
        if gname and gid:
            out[gname] = (abbr or gname, gid)
    return out

def _get_user_authorized_guilds(ss, user_id: int) -> List[Tuple[str, str]]:
    """
    Devuelve lista (label, guild_id) SOLO para gremios donde el user_id
    tenga Rol 'Lider' u 'Oficial' en hoja Usuarios.
    """
    ws_u = ss.worksheet(USERS_SHEET)
    vals_u = ws_u.get_all_values() or []
    if not vals_u:
        return []
    uh = [h.strip().lower() for h in vals_u[0]]
    rows_u = vals_u[1:]

    idx_uid = uh.index("user_id") if "user_id" in uh else None
    idx_gn = uh.index("guild_name") if "guild_name" in uh else None
    idx_role = uh.index("rol") if "rol" in uh else (uh.index("role") if "role" in uh else None)
    if idx_uid is None or idx_gn is None or idx_role is None:
        return []

    allowed = {"lider", "oficial"}
    guild_names = set()
    for r in rows_u:
        uid_ok = (idx_uid < len(r) and str(r[idx_uid]).strip() == str(user_id))
        if not uid_ok:
            continue
        gname = (r[idx_gn] if idx_gn < len(r) else "").strip()
        role = (r[idx_role] if idx_role < len(r) else "").strip().lower()
        if gname and role in allowed:
            guild_names.add(gname)

    if not guild_names:
        return []

    gmap = _map_guild_name_to_label_and_id(ss)
    out: List[Tuple[str, str]] = []
    for gname in guild_names:
        if gname in gmap:
            out.append(gmap[gname])  # (label, gid)
    return out

def _user_has_role_in_guild(ss, user_id: int, guild_id: str) -> bool:
    """
    Verifica que user_id tenga Rol Lider/Oficial en ese guild_id.
    """
    # Mapear guild_id -> guild_name
    ws_g = ss.worksheet(GUILDS_SHEET)
    vals_g = ws_g.get_all_values() or []
    if not vals_g:
        return False
    gh = [h.strip().lower() for h in (vals_g[0] or [])]
    rows_g = vals_g[1:]
    try:
        ig_id = gh.index("guild id")
        ig_name = gh.index("guild name")
    except ValueError:
        return False

    gid_to_name = {}
    for r in rows_g:
        gid = (r[ig_id] if ig_id < len(r) else "").strip()
        gname = (r[ig_name] if ig_name < len(r) else "").strip()
        if gid and gname:
            gid_to_name[gid] = gname

    gname = gid_to_name.get(guild_id)
    if not gname:
        return False

    # Verificar en Usuarios
    ws_u = ss.worksheet(USERS_SHEET)
    vals_u = ws_u.get_all_values() or []
    if not vals_u:
        return False
    uh = [h.strip().lower() for h in (vals_u[0] or [])]
    rows_u = vals_u[1:]

    idx_uid = uh.index("user_id") if "user_id" in uh else None
    idx_gn = uh.index("guild_name") if "guild_name" in uh else None
    idx_role = uh.index("rol") if "rol" in uh else (uh.index("role") if "role" in uh else None)
    if idx_uid is None or idx_gn is None or idx_role is None:
        return False

    allowed = {"lider", "oficial"}
    for r in rows_u:
        uid_ok = (idx_uid < len(r) and str(r[idx_uid]).strip() == str(user_id))
        if not uid_ok:
            continue
        gn = (r[idx_gn] if idx_gn < len(r) else "").strip()
        role = (r[idx_role] if idx_role < len(r) else "").strip().lower()
        if gn == gname and role in allowed:
            return True
    return False

def _find_guild_by_id(ss, guild_id: str) -> Tuple[str, str]:
    """
    Devuelve (label, guild_name) para un guild_id. label = nombre abreviado o Guild Name.
    """
    ws_g = ss.worksheet(GUILDS_SHEET)
    vals = ws_g.get_all_values() or []
    hdr = [h.strip() for h in (vals[0] if vals else [])]
    rows = vals[1:] if len(vals) > 1 else []
    lower = [h.lower() for h in hdr]
    i_id = lower.index("guild id")
    i_name = lower.index("guild name")
    i_abbr = lower.index("nombre abreviado") if "nombre abreviado" in lower else None

    for r in rows:
        gid = (r[i_id] if i_id < len(r) else "").strip()
        if gid == guild_id:
            gname = (r[i_name] if i_name < len(r) else "").strip()
            abbr = (r[i_abbr] if (i_abbr is not None and i_abbr < len(r)) else "").strip() if i_abbr is not None else ""
            label = abbr or gname or "gremio seleccionado"
            return label, gname
    return "gremio seleccionado", ""

def _already_synced_today(ss, guild_id: str) -> bool:
    """
    Revisa Guilds → Last Update para el guild_id.
    Si la fecha (YYYY-MM-DD) coincide con hoy (Europe/Amsterdam), devuelve True.
    """
    ws_g = ss.worksheet(GUILDS_SHEET)
    vals = ws_g.get_all_values() or []
    if not vals:
        return False
    hdr = [h.strip().lower() for h in (vals[0] or [])]
    rows = vals[1:] if len(vals) > 1 else []

    try:
        i_id = hdr.index("guild id")
        i_last = hdr.index("last update")
    except ValueError:
        return False

    today = datetime.now(TZ).date().isoformat()
    for r in rows:
        gid = (r[i_id] if i_id < len(r) else "").strip()
        if gid != guild_id:
            continue
        last = (r[i_last] if i_last < len(r) else "").strip()
        if not last:
            return False
        # last suele ser ISO (ej: 2025-09-04T16:43:33+02:00). Cogemos los 10 primeros caracteres.
        last_date = last[:10]
        return last_date == today
    return False

# ---------------- Telegram Handlers ----------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hola 👋\n"
        "Comandos disponibles:\n"
        "/registrar - Registrar usuario\n"
        "/misoperaciones - Ver tus operaciones\n"
        "/syncguild - Sincronizar tu gremio (Lider/Oficial)\n"
        "/syncdata - (admins) sincronizar data"
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cmd_start(update, context)

async def cmd_syncguild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        ss = _open_ss()
    except Exception as e:
        await update.message.reply_text(f"❌ No puedo abrir el Spreadsheet: {e}")
        return

    guild_opts = _get_user_authorized_guilds(ss, user_id)
    if not guild_opts:
        await update.message.reply_text("No tienes permisos para sincronizar (se requiere rol Lider u Oficial).")
        return

    # Teclado con opciones
    buttons = [
        [InlineKeyboardButton(text=label, callback_data=f"syncguild:{gid}")]
        for (label, gid) in guild_opts
    ]
    kb = InlineKeyboardMarkup(buttons)
    await update.message.reply_text("Elige el gremio a sincronizar:", reply_markup=kb)

async def cb_syncguild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    if not data.startswith("syncguild:"):
        return
    guild_id = data.split(":", 1)[1]
    user_id = query.from_user.id

    try:
        ss = _open_ss()
    except Exception as e:
        await query.edit_message_text(f"❌ No puedo abrir el Spreadsheet: {e}")
        return

    # Seguridad: revalidar rol en ese gremio
    if not _user_has_role_in_guild(ss, user_id, guild_id):
        await query.edit_message_text("❌ No tienes permisos para sincronizar este gremio.")
        return

    # Límite: 1 vez al día
    if _already_synced_today(ss, guild_id):
        label, _ = _find_guild_by_id(ss, guild_id)
        await query.edit_message_text(f"ℹ️ {label} ya se sincronizó hoy.")
        return

    label, _ = _find_guild_by_id(ss, guild_id)
    await query.edit_message_text(f"⏳ Sincronizando {label}…")

    # Ejecutar la sync solo para ese guild
    prev = os.getenv("FILTER_GUILD_IDS", "")
    try:
        os.environ["FILTER_GUILD_IDS"] = guild_id
        # Import tardío para evitar dependencias circulares en tiempo de import
        from ..processing import sync_guilds as mod_sync_guilds
        _ = mod_sync_guilds.run()  # ignoramos métricas internas
        await query.edit_message_text(f"✅ Sincronización completada para {label}.")
    except Exception as e:
        await query.edit_message_text(f"❌ Error sincronizando {label}.\n{e}")
    finally:
        if prev:
            os.environ["FILTER_GUILD_IDS"] = prev
        else:
            os.environ.pop("FILTER_GUILD_IDS", None)

def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN")
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID")

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("syncguild", cmd_syncguild))
    app.add_handler(CallbackQueryHandler(cb_syncguild, pattern=r"^syncguild:"))

    log.info("Bot iniciado (polling).")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
