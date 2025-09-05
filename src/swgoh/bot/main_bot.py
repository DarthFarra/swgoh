# src/swgoh/bot/main_bot.py
import os
import json
import asyncio
import logging
from typing import List, Dict, Any, Optional, Tuple

from google.oauth2.service_account import Credentials
import gspread

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from ..http import COMLINK_BASE  # asegura que COMLINK_BASE esté definida al importar
from ..processing import sync_guilds as mod_sync_guilds

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [main_bot] %(message)s")
log = logging.getLogger("main_bot")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_ENV = "SERVICE_ACCOUNT_FILE"

USERS_SHEET = os.getenv("USERS_SHEET", "Usuarios")
GUILDS_SHEET = os.getenv("GUILDS_SHEET", "Guilds")
PLAYERS_SHEET = os.getenv("PLAYERS_SHEET", "Players")

SYNC_DATA_ALLOWED_CHATS = {s.strip() for s in os.getenv("SYNC_DATA_ALLOWED_CHATS", "7367477801,30373681").split(",") if s.strip()}

# ------- Google Sheets helpers -------
def _load_creds():
    raw = os.getenv(SERVICE_ACCOUNT_ENV)
    if not raw:
        raise RuntimeError("Falta SERVICE_ACCOUNT_FILE")
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
    gc = gspread.authorize(_load_creds())
    return gc.open_by_key(SPREADSHEET_ID)

def _headers_lower(ws) -> List[str]:
    vals = ws.row_values(1) or []
    return [h.strip().lower() for h in vals]

# Devuelve (label_para_mostrar, guild_id)
def _map_guild_name_to_label_and_id(ss) -> Dict[str, Tuple[str, str]]:
    """Lee Guilds y construye un mapa: Guild Name -> (nombre abreviado o Guild Name, Guild Id)"""
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
    Solo devuelve gremios donde el usuario (por user_id) tenga rol 'Lider' u 'Oficial'.
    Retorna lista de (label, guild_id) usando nombre abreviado si existe.
    """
    ws_u = ss.worksheet(USERS_SHEET)
    vals_u = ws_u.get_all_values() or []
    if not vals_u:
        return []
    uh = [h.strip().lower() for h in vals_u[0]]
    rows_u = vals_u[1:]

    idx_uid = uh.index("user_id") if "user_id" in uh else None
    # En Usuarios guardamos guild_name y Rol (o role)
    idx_gn = uh.index("guild_name") if "guild_name" in uh else None
    idx_role = None
    if "rol" in uh:
        idx_role = uh.index("rol")
    elif "role" in uh:
        idx_role = uh.index("role")

    if idx_uid is None or idx_gn is None or idx_role is None:
        return []

    # Gremios (por nombre) donde el user_id tiene rol permitido
    allowed_roles = {"lider", "oficial"}
    user_guilds_allowed = set()
    for r in rows_u:
        try:
            if str(r[idx_uid]).strip() != str(user_id):
                continue
        except Exception:
            continue
        gname = (r[idx_gn] if idx_gn < len(r) else "").strip()
        role = (r[idx_role] if idx_role < len(r) else "").strip().lower()
        if gname and role in allowed_roles:
            user_guilds_allowed.add(gname)

    if not user_guilds_allowed:
        return []

    # Mapear a (label, id) a partir de Guilds
    gmap = _map_guild_name_to_label_and_id(ss)
    out: List[Tuple[str, str]] = []
    for gname in user_guilds_allowed:
        if gname in gmap:
            out.append(gmap[gname])
    return out

def _user_has_role_in_guild(ss, user_id: int, guild_id: str) -> bool:
    """
    Seguridad adicional: comprueba que el user tenga rol Lider/Oficial en el guild_id concreto.
    """
    # 1) Mapear guild_id -> guild_name
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

    # 2) Verificar en Usuarios que (user_id, gname) tenga rol válido
    ws_u = ss.worksheet(USERS_SHEET)
    vals_u = ws_u.get_all_values() or []
    if not vals_u:
        return False
    uh = [h.strip().lower() for h in vals_u[0]]
    rows_u = vals_u[1:]

    idx_uid = uh.index("user_id") if "user_id" in uh else None
    idx_gn = uh.index("guild_name") if "guild_name" in uh else None
    idx_role = uh.index("rol") if "rol" in uh else uh.index("role") if "role" in uh else None
    if idx_uid is None or idx_gn is None or idx_role is None:
        return False

    allowed_roles = {"lider", "oficial"}
    for r in rows_u:
        try:
            if str(r[idx_uid]).strip() != str(user_id):
                continue
        except Exception:
            continue
        gn = (r[idx_gn] if idx_gn < len(r) else "").strip()
        role = (r[idx_role] if idx_role < len(r) else "").strip().lower()
        if gn == gname and role in allowed_roles:
            return True
    return False

# ------- Telegram bot -------
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(bot)

@dp.message_handler(commands=["syncguild"])
async def cmd_syncguild(message: types.Message):
    try:
        ss = _open_ss()
    except Exception as e:
        await message.reply(f"❌ No puedo abrir el Spreadsheet: {e}")
        return

    user_id = message.from_user.id
    guild_opts = _get_user_authorized_guilds(ss, user_id)

    if not guild_opts:
        await message.reply("No tienes permisos para sincronizar (se requiere rol Lider u Oficial).")
        return

    kb = InlineKeyboardMarkup(row_width=1)
    for label, gid in guild_opts:
        kb.add(InlineKeyboardButton(text=label, callback_data=f"syncguild:{gid}"))
    await message.reply("Elige el gremio a sincronizar:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("syncguild:"))
async def cb_syncguild(call: types.CallbackQuery):
    gid = call.data.split(":", 1)[1]
    user_id = call.from_user.id

    try:
        ss = _open_ss()
    except Exception as e:
        await call.answer("Error de acceso a Sheets", show_alert=True)
        await call.message.answer(f"❌ No puedo abrir el Spreadsheet: {e}")
        return

    # Seguridad: revalidar que tiene rol Lider/Oficial en ese guild_id
    if not _user_has_role_in_guild(ss, user_id, gid):
        await call.answer("Sin permisos", show_alert=True)
        await call.message.answer("❌ No tienes permisos para sincronizar este gremio.")
        return

    # Resolver etiqueta (nombre abreviado o Guild Name) para el mensaje
    label = None
    for gname, (lab, gmap_id) in _map_guild_name_to_label_and_id(ss).items():
        if gmap_id == gid:
            label = lab or gname
            break
    if not label:
        label = "gremio seleccionado"

    await call.answer()
    await call.message.answer(f"⏳ Sincronizando {label}…")

    prev_filter = os.getenv("FILTER_GUILD_IDS", "")
    try:
        os.environ["FILTER_GUILD_IDS"] = gid
        # Ejecutar sync (ignoramos el texto de retorno para no mostrar stats)
        _ = mod_sync_guilds.run()
        await call.message.answer(f"✅ Sincronización completada para {label}.")
    except Exception as e:
        await call.message.answer(f"❌ Error sincronizando {label}.\n{e}")
    finally:
        if prev_filter:
            os.environ["FILTER_GUILD_IDS"] = prev_filter
        else:
            os.environ.pop("FILTER_GUILD_IDS", None)

@dp.message_handler(commands=["start", "help"])
async def cmd_help(message: types.Message):
    await message.reply(
        "Hola 👋\n"
        "Comandos disponibles:\n"
        "/registrar - Registrar usuario\n"
        "/misoperaciones - Ver tus operaciones\n"
        "/syncguild - Sincronizar tu gremio (Lider/Oficial)\n"
        "/syncdata - (admins) sincronizar data"
    )

def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN")
    if not SPREADSHEET_ID:
        raise RuntimeError("Falta SPREADSHEET_ID")
    loop = asyncio.get_event_loop()
    loop.create_task(dp.start_polling())
    log.info("Bot iniciado (polling).")
    loop.run_forever()

if __name__ == "__main__":
    main()
