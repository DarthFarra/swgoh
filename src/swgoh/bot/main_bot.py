import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from ..sheets import open_or_create, try_get_worksheet
from .. import config as cfg

# Config
BOT_TOKEN = getattr(cfg, "TELEGRAM_BOT_TOKEN", None) or getattr(cfg, "BOT_TOKEN", None)
SHEET_USUARIOS = getattr(cfg, "SHEET_USUARIOS", "Usuarios")
SHEET_ASIGNACIONES = getattr(cfg, "SHEET_ASIGNACIONES_ROTE", "Asignaciones ROTE")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("main_bot")


# ------------------ Helpers Sheets ------------------

def _headers_index(ws, candidates: List[str]) -> Optional[int]:
    values = ws.get("1:1") or [[]]
    headers = [h.strip().lower() for h in (values[0] if values else [])]
    for i, h in enumerate(headers, start=1):
        for c in candidates:
            if c in h:
                return i
    return None


def _upsert_user(chat_id: int, username: str, first: str, last: str) -> Tuple[int, Dict[str, int]]:
    """
    Inserta o actualiza al usuario en la hoja Usuarios.
    Devuelve (row_index, col_map) para posteriores updates.
    """
    ws = open_or_create(SHEET_USUARIOS)

    # Mapa columnas (crea cabecera mínima si estuviera vacía)
    if not (ws.get_all_values() or []):
        ws.update("A1", [["chat_id", "username", "first_name", "last_name", "player_name", "ally_code"]])

    col_map = {
        "chat_id": _headers_index(ws, ["chat_id", "id de chat"]),
        "username": _headers_index(ws, ["username", "usuario"]),
        "first_name": _headers_index(ws, ["first_name", "nombre"]),
        "last_name": _headers_index(ws, ["last_name", "apellidos", "apellido"]),
        "player_name": _headers_index(ws, ["player_name", "jugador", "player"]),
        "ally_code": _headers_index(ws, ["ally_code", "ally", "allycode"]),
    }

    if not col_map["chat_id"]:
        # reconstruir cabecera estándar
        ws.clear()
        ws.update("A1", [["chat_id", "username", "first_name", "last_name", "player_name", "ally_code"]])
        col_map = {
            "chat_id": 1, "username": 2, "first_name": 3, "last_name": 4, "player_name": 5, "ally_code": 6
        }

    # Buscar fila por chat_id
    rows = ws.get_all_values() or []
    target_row = None
    for idx, row in enumerate(rows[1:], start=2):
        val = row[col_map["chat_id"] - 1] if len(row) >= col_map["chat_id"] else ""
        if str(val).strip() == str(chat_id):
            target_row = idx
            break

    if target_row is None:
        # append
        data = [""] * max(col_map.values())
        data[col_map["chat_id"] - 1] = str(chat_id)
        data[col_map["username"] - 1] = username or ""
        data[col_map["first_name"] - 1] = first or ""
        data[col_map["last_name"] - 1] = last or ""
        ws.append_row(data, value_input_option="RAW")
        target_row = ws.row_count  # tras append_row, suele ser la última, pero por seguridad:
        # Mejor recuperar la última fila con get_all_values si hiciera falta; normalmente vale.
    else:
        # update mínimos
        updates = []
        updates.append((target_row, col_map["username"], username or ""))
        updates.append((target_row, col_map["first_name"], first or ""))
        updates.append((target_row, col_map["last_name"], last or ""))
        for r, c, v in updates:
            ws.update_cell(r, c, v)

    return target_row, col_map


def _find_user_record(chat_id: int) -> Optional[Dict[str, str]]:
    ws = try_get_worksheet(SHEET_USUARIOS)
    if not ws:
        return None
    vals = ws.get_all_values() or []
    if not vals:
        return None
    headers = [h.strip() for h in (vals[0] or [])]
    idx = None
    for i, row in enumerate(vals[1:], start=2):
        if i < 2:
            continue
        if not row:
            continue
        if str(row[0]).strip() == str(chat_id):
            idx = i
            break
    if idx is None:
        return None
    row = vals[idx - 1]
    rec = {}
    for i, k in enumerate(headers):
        rec[k] = row[i] if i < len(row) else ""
    return rec


def _sanitize_ally(ally: str) -> str:
    return "".join(ch for ch in str(ally) if ch.isdigit())


def _load_assignments_for(player_name: Optional[str], ally_code: Optional[str]) -> List[Dict[str, str]]:
    ws = try_get_worksheet(SHEET_ASIGNACIONES)
    if not ws:
        return []

    vals = ws.get_all_values() or []
    if not vals:
        return []

    headers = [h.strip().lower() for h in (vals[0] or [])]

    def col(*cands: str) -> Optional[int]:
        for i, h in enumerate(headers):
            for c in cands:
                if c in h:
                    return i
        return None

    idx_player = col("player", "jugador", "player name")
    idx_phase = col("fase", "phase")
    idx_planet = col("planeta", "planet")
    idx_task = col("personaje", "character", "tarea", "assignment", "asignacion")

    results: List[Dict[str, str]] = []
    for row in vals[1:]:
        if not row:
            continue
        # match por nombre o ally_code si existiera en la hoja (flexible)
        candidate_player = (row[idx_player] if (idx_player is not None and idx_player < len(row)) else "").strip() if idx_player is not None else ""
        if player_name and candidate_player and candidate_player.lower() != player_name.lower():
            continue
        results.append({
            "phase": (row[idx_phase] if (idx_phase is not None and idx_phase < len(row)) else "").strip(),
            "planet": (row[idx_planet] if (idx_planet is not None and idx_planet < len(row)) else "").strip(),
            "task": (row[idx_task] if (idx_task is not None and idx_task < len(row)) else "").strip(),
        })
    return results


def _format_assignments(rows: List[Dict[str, str]]) -> str:
    if not rows:
        return "No tienes asignaciones registradas."
    # Agrupar por fase/planeta
    grouped: Dict[Tuple[str, str], List[str]] = {}
    for r in rows:
        key = (r.get("phase", ""), r.get("planet", ""))
        grouped.setdefault(key, []).append(r.get("task", ""))

    lines = []
    for (phase, planet), tasks in sorted(grouped.items(), key=lambda x: (x[0][0], x[0][1])):
        title = f"• Fase {phase} – {planet}" if phase else f"• {planet or 'Sin planeta'}"
        lines.append(title)
        for t in tasks:
            lines.append(f"   · {t}")
    return "\n".join(lines)


# ------------------ Handlers ------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    _upsert_user(
        chat_id=update.effective_chat.id,
        username=user.username or "",
        first=user.first_name or "",
        last=user.last_name or "",
    )
    msg = (
        "¡Hola! 👋 Quedaste registrado.\n"
        "Usa /assignments para ver tus asignaciones de ROTE.\n"
        "Si quieres actualizar tu nombre de jugador en la hoja, usa:\n"
        "<code>/setname Tu Nombre Exacto</code>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Comandos disponibles:\n"
        "/start – registrarte\n"
        "/assignments – ver asignaciones\n"
        "/setname &lt;nombre exacto en la hoja&gt; – vincular tu jugador",
        parse_mode="HTML",
    )


async def setname(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ws = open_or_create(SHEET_USUARIOS)
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Uso: /setname <nombre exacto en la hoja>")
        return
    new_name = " ".join(context.args).strip()
    if not new_name:
        await update.message.reply_text("Nombre vacío. Intenta de nuevo.")
        return

    # encontrar fila por chat_id y actualizar player_name
    row_idx, col_map = _upsert_user(
        chat_id=chat_id,
        username=update.effective_user.username or "",
        first=update.effective_user.first_name or "",
        last=update.effective_user.last_name or "",
    )
    player_col = col_map.get("player_name", 5)
    ws.update_cell(row_idx, player_col, new_name)
    await update.message.reply_text(f"Vinculado a jugador: <b>{new_name}</b>", parse_mode="HTML")


async def assignments(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rec = _find_user_record(update.effective_chat.id)
    player_name = (rec or {}).get("player_name") or (rec or {}).get("Player Name") or ""
    ally_code = _sanitize_ally((rec or {}).get("ally_code") or "")
    rows = _load_assignments_for(player_name=player_name, ally_code=ally_code)
    text = _format_assignments(rows)
    if player_name:
        text = f"<b>Asignaciones de {player_name}</b>\n\n{text}"
    await update.message.reply_text(text, parse_mode="HTML")


# ------------------ App ------------------

def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Falta TELEGRAM_BOT_TOKEN en config.py o env.")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("assignments", assignments))
    app.add_handler(CommandHandler("setname", setname))

    # fallback: echo para debug (opcional)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, help_cmd))

    log.info("Bot iniciado (polling).")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
