import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

from telegram import Bot
from ..sheets import try_get_worksheet
from .. import config as cfg

BOT_TOKEN = getattr(cfg, "TELEGRAM_BOT_TOKEN", None) or getattr(cfg, "BOT_TOKEN", None)
SHEET_USUARIOS = getattr(cfg, "SHEET_USUARIOS", "Usuarios")
SHEET_ASIGNACIONES = getattr(cfg, "SHEET_ASIGNACIONES_ROTE", "Asignaciones ROTE")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("send_assignments_daily")


def _headers(ws) -> List[str]:
    vals = ws.get("1:1") or [[]]
    return [h.strip() for h in (vals[0] if vals else [])]


def _col_index(headers: List[str], *cands: str) -> Optional[int]:
    h_low = [h.lower() for h in headers]
    for i, h in enumerate(h_low):
        for c in cands:
            if c in h:
                return i
    return None


def _get_usuarios() -> List[Dict[str, str]]:
    ws = try_get_worksheet(SHEET_USUARIOS)
    if not ws:
        return []
    vals = ws.get_all_values() or []
    if not vals:
        return []
    headers = [h.strip() for h in (vals[0] or [])]
    out = []
    for row in vals[1:]:
        if not row:
            continue
        rec = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        out.append(rec)
    return out


def _get_assignments() -> Tuple[List[str], List[List[str]]]:
    ws = try_get_worksheet(SHEET_ASIGNACIONES)
    if not ws:
        return [], []
    vals = ws.get_all_values() or []
    if not vals:
        return [], []
    return vals[0], vals[1:]


def _filter_assignments_for(rows: List[List[str]], headers: List[str], player_name: str) -> List[Dict[str, str]]:
    idx_player = _col_index(headers, "player", "jugador", "player name")
    idx_phase = _col_index(headers, "fase", "phase")
    idx_planet = _col_index(headers, "planeta", "planet")
    idx_task = _col_index(headers, "personaje", "character", "tarea", "assignment", "asignacion")

    res: List[Dict[str, str]] = []
    for r in rows:
        if not r:
            continue
        cand = (r[idx_player] if (idx_player is not None and idx_player < len(r)) else "").strip() if idx_player is not None else ""
        if player_name and cand and cand.lower() != player_name.lower():
            continue
        res.append({
            "phase": (r[idx_phase] if (idx_phase is not None and idx_phase < len(r)) else "").strip(),
            "planet": (r[idx_planet] if (idx_planet is not None and idx_planet < len(r)) else "").strip(),
            "task": (r[idx_task] if (idx_task is not None and idx_task < len(r)) else "").strip(),
        })
    return res


def _format_assignments(rows: List[Dict[str, str]], player_name: str) -> str:
    if not rows:
        return f"<b>Asignaciones de {player_name}</b>\n\nNo tienes asignaciones registradas."
    grouped: Dict[Tuple[str, str], List[str]] = {}
    for r in rows:
        key = (r.get("phase", ""), r.get("planet", ""))
        grouped.setdefault(key, []).append(r.get("task", ""))

    lines = [f"<b>Asignaciones de {player_name}</b>", ""]
    for (phase, planet), tasks in sorted(grouped.items(), key=lambda x: (x[0][0], x[0][1])):
        title = f"• Fase {phase} – {planet}" if phase else f"• {planet or 'Sin planeta'}"
        lines.append(title)
        for t in tasks:
            lines.append(f"   · {t}")
    return "\n".join(lines)


async def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Falta TELEGRAM_BOT_TOKEN en config.py o env.")
    bot = Bot(BOT_TOKEN)

    users = _get_usuarios()
    if not users:
        log.info("No hay usuarios en la hoja; nada que enviar.")
        return

    headers, rows = _get_assignments()
    if not headers:
        log.info("No hay hoja de asignaciones o está vacía.")
        return

    # Detectar columnas básicas en Usuarios
    uheaders = [h.strip().lower() for h in (users[0].keys() if users else [])]
    def uget(rec: Dict[str, str], *cands: str) -> str:
        for c in cands:
            for k in rec.keys():
                if c in k.lower():
                    return rec.get(k, "")
        return ""

    sent = 0
    for rec in users:
        chat_id_str = uget(rec, "chat_id")
        player_name = uget(rec, "player_name", "jugador", "player")
        if not chat_id_str:
            continue
        try:
            chat_id = int(chat_id_str)
        except Exception:
            continue
        if not player_name:
            # si no tiene player_name, omitir con aviso leve
            log.info("Usuario %s sin player_name; se omite", chat_id)
            continue

        filtered = _filter_assignments_for(rows, [h.strip() for h in headers], player_name)
        text = _format_assignments(filtered, player_name)

        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True)
            sent += 1
        except Exception as e:
            log.warning("No pude enviar a %s (%s): %s", chat_id, player_name, e)

        await asyncio.sleep(0.05)  # pequeño respiro para Telegram

    log.info("Mensajes enviados: %s", sent)


if __name__ == "__main__":
    asyncio.run(main())
