# src/swgoh/bot/commands/omicronsummary.py
"""
/omicronsummary — officer-only roll-up of omicron counts across the guild.

Flow:
  1. Officer runs /omicronsummary.
  2. If they're an officer in >1 guild → pick guild. Otherwise auto-use.
  3. Bot fetches every guild member's roster from Comlink in parallel,
     aggregates omicron counts per mode, and renders a table.
  4. Inline buttons per mode → drill-down showing per-skill applied
     counts (sorted descending), with no re-fetch.

Diagnostic logging:
  Every outgoing message goes through _send_with_diagnostics(), which:
    - Logs the message length at INFO before sending.
    - On Telegram's 'Message_too_long' error, logs the FULL content at
      WARNING (chunked) so it's visible in default log configs, then
      replies with a short notice that itself can't be too long.
  This means you can grep your logs for [summary_table] / [mode_detail]
  to see exactly how big the renderer's output is in production.

Why officer-only:
  Roster-wide intelligence; mirrors /sendassignments and
  /operacionesjugador conventions.

Security:
  - Officer role required at command entry AND re-validated on each
    button click against the guild_id stored in the cached summary.
  - Random short token in callback_data — not guessable across summaries.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable, List, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

from ..services.sheets import (
    open_ss,
    resolve_label_name_rote_by_id,
    list_players_with_id_for_guild,
)
from ..services.auth import user_authorized_guilds, user_has_role_in_guild
from ..services.omicrons import read_omicron_catalog
from ..services.comlink_player import load_unit_catalog
from ..services.omicron_summary import (
    GuildOmicronSummary,
    aggregate,
    cache_summary,
    fetch_all_players,
    get_cached_summary,
    mode_short,
)
from ..keyboards.guild_select import make_keyboard_guilds
from ..security import (
    rate_limit,
    validate_guild_id,
    CallbackValidationError,
)

log = logging.getLogger(__name__)

# Callback prefixes
CB_GUILD = "omisumg"
CB_MODE  = "omisum"

# Tunables
_CONCURRENCY       = 5
_COMLINK_TIMEOUT_S = 10.0
_PROGRESS_EVERY_N  = 10

# Telegram's hard limit is 4096 characters. We target 3800 to leave
# margin for markdown formatting chars and emoji weighting in UTF-16.
_MAX_MESSAGE_CHARS = 3800

# When logging an oversize message's full content, this is the chunk
# size we slice it into. Most log backends accept lines of ~2000 chars;
# 1500 leaves room for prefix/timestamp formatting.
_LOG_CHUNK_SIZE = 1500


# ---------------------------------------------------------------------------
# Diagnostic send helper
# ---------------------------------------------------------------------------

# Type alias for any Telegram coroutine that accepts (text, parse_mode, reply_markup).
SendCallable = Callable[..., Awaitable]


async def _send_with_diagnostics(
    send_callable: SendCallable,
    text: str,
    *,
    kind: str,
    parse_mode: Optional[str] = "Markdown",
    reply_markup=None,
) -> bool:
    """
    Send a Telegram message via `send_callable` (e.g. message.edit_text
    or query.edit_message_text) with diagnostic logging.

    Always logs the rendered length at INFO before the send, so you can
    grep production logs to see what the renderer is actually producing.

    On Telegram's 'Message_too_long' error:
      - Logs the full message content at WARNING in 1500-char chunks
        so it's visible without enabling DEBUG logging.
      - Sends a short notice via the same callable (with no markdown
        and no reply_markup) so the user sees that something happened.
      - Returns False.

    Other exceptions propagate.

    Returns True on success, False if the message had to be replaced
    with the short notice.
    """
    log.info("[%s] sending message: length=%d chars (limit ~4096)", kind, len(text))

    try:
        await send_callable(text, parse_mode=parse_mode, reply_markup=reply_markup)
        return True
    except BadRequest as e:
        msg = str(e).lower()
        if "too long" not in msg:
            # Different BadRequest — let the global handler deal with it.
            raise

    # 'Too long' path. Dump the full content so we can see what overflowed.
    log.warning(
        "[%s] Telegram rejected message: length=%d, limit=4096. "
        "Full content follows in %d chunk(s) of up to %d chars each.",
        kind, len(text),
        (len(text) + _LOG_CHUNK_SIZE - 1) // _LOG_CHUNK_SIZE,
        _LOG_CHUNK_SIZE,
    )
    for i in range(0, len(text), _LOG_CHUNK_SIZE):
        chunk = text[i:i + _LOG_CHUNK_SIZE]
        # Bracket each chunk with line markers so multi-line content is obvious.
        log.warning("[%s] chunk[%d:%d]\n%s\n--- end chunk ---",
                    kind, i, i + len(chunk), chunk)

    # Send a short notice as graceful fallback. No reply_markup so a
    # follow-up edit can replace it cleanly; no markdown to avoid any
    # parsing oddities. This message is short enough that it cannot
    # itself be 'too long', so we don't need a defence in depth here.
    try:
        await send_callable(
            f"⚠️ El mensaje generado es demasiado largo para Telegram "
            f"({len(text)} caracteres, límite 4096). "
            f"Revisa los logs del bot — se ha registrado el contenido completo.",
            parse_mode=None,
            reply_markup=None,
        )
    except Exception:
        # If even THIS fails, just log and move on. We've already logged
        # the full original content above, so the operator has what they need.
        log.exception(
            "[%s] fallback short notice also failed to send", kind,
        )

    return False


# ---------------------------------------------------------------------------
# /omicronsummary entry point
# ---------------------------------------------------------------------------

@rate_limit(cooldown_seconds=30)
async def cmd_omicronsummary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss      = open_ss()
    user_id = update.effective_user.id

    authorized = user_authorized_guilds(ss, user_id)
    if not authorized:
        await update.message.reply_text(
            "❌ Este comando requiere rol Líder u Oficial en algún gremio."
        )
        return

    if len(authorized) > 1:
        await update.message.reply_text(
            "Elige el gremio para ver el resumen de omicrones:",
            reply_markup=make_keyboard_guilds(authorized, CB_GUILD),
        )
        return

    label, gid = authorized[0]
    await _run_summary(update, context, ss, gid, label, via_callback=False)


async def cb_summary_guild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith(f"{CB_GUILD}:"):
        return

    raw_gid = data.split(":", 1)[1]
    user_id = q.from_user.id
    ss      = open_ss()

    authorized = user_authorized_guilds(ss, user_id)
    known_ids  = {gid for _, gid in authorized}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    label, _, _ = resolve_label_name_rote_by_id(ss, gid)
    await _run_summary(update, context, ss, gid, label, via_callback=True)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def _run_summary(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    ss,
    gid: str,
    label: str,
    via_callback: bool,
):
    user_id = update.effective_user.id

    if not user_has_role_in_guild(ss, user_id, gid):
        msg = "❌ No tienes permisos en este gremio."
        if via_callback:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    _, gname, _ = resolve_label_name_rote_by_id(ss, gid)

    players = list_players_with_id_for_guild(ss, gname)
    if not players:
        msg = (f"❌ No hay jugadores con Player Id en '{gname}'. "
               "Ejecuta /syncguild primero.")
        if via_callback:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    catalog = read_omicron_catalog(ss)
    if not catalog:
        msg = "❌ Catálogo de omicrones vacío. Ejecuta /syncdata."
        if via_callback:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    if via_callback:
        await update.callback_query.edit_message_text(
            f"⏳ Consultando {len(players)} jugadores en *{label}*…",
            parse_mode="Markdown",
        )
        progress_msg = update.callback_query.message
    else:
        progress_msg = await update.message.reply_text(
            f"⏳ Consultando {len(players)} jugadores en *{label}*…",
            parse_mode="Markdown",
        )

    base_to_name, is_ship_by_base = load_unit_catalog(
        ss, context.application.bot_data,
    )

    last_reported = [0]
    step = max(_PROGRESS_EVERY_N, len(players) // 10)

    async def _progress(done: int, total: int):
        if done == total or done - last_reported[0] >= step:
            last_reported[0] = done
            try:
                await progress_msg.edit_text(
                    f"⏳ Consultando *{label}*: {done}/{total}…",
                    parse_mode="Markdown",
                )
            except Exception:
                pass

    started = time.monotonic()
    fetched = await fetch_all_players(
        players=players,
        bot_data=context.application.bot_data,
        is_ship_by_base=is_ship_by_base,
        concurrency=_CONCURRENCY,
        timeout_seconds=_COMLINK_TIMEOUT_S,
        progress=_progress,
    )
    elapsed = time.monotonic() - started

    summary = aggregate(
        guild_id=gid,
        guild_name=gname,
        guild_label=label,
        players=players,
        fetched=fetched,
        catalog=catalog,
        elapsed_seconds=elapsed,
    )

    token = cache_summary(context.application.bot_data, summary)
    text  = render_summary_table(summary)
    kb    = build_mode_keyboard(summary, token, include_back=False)

    await _send_with_diagnostics(
        progress_msg.edit_text,
        text,
        kind="summary_table",
        parse_mode="Markdown",
        reply_markup=kb,
    )


# ---------------------------------------------------------------------------
# Mode drill-down handler
# ---------------------------------------------------------------------------

async def cb_summary_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith(f"{CB_MODE}:"):
        return

    parts = data.split(":")
    if len(parts) != 3:
        await q.edit_message_text("❌ Datos no válidos.")
        return

    _, token, target = parts
    summary = get_cached_summary(context.application.bot_data, token)
    if summary is None:
        await q.edit_message_text(
            "⌛ El resumen ha expirado. Ejecuta /omicronsummary de nuevo."
        )
        return

    user_id = q.from_user.id
    ss      = open_ss()
    if not user_has_role_in_guild(ss, user_id, summary.guild_id):
        await q.edit_message_text("❌ No tienes permisos en este gremio.")
        return

    if target == "back":
        text = render_summary_table(summary)
        kind = "summary_table"
        kb   = build_mode_keyboard(summary, token, include_back=False)
    else:
        try:
            mode_idx = int(target)
        except ValueError:
            await q.edit_message_text("❌ Datos no válidos.")
            return
        if not (0 <= mode_idx < len(summary.modes)):
            await q.edit_message_text("❌ Modo no válido.")
            return
        mode_text = summary.modes[mode_idx]
        text = render_mode_detail(summary, mode_text)
        kind = f"mode_detail[{mode_short(mode_text)}]"
        kb   = build_mode_keyboard(summary, token, include_back=True)

    await _send_with_diagnostics(
        q.edit_message_text,
        text,
        kind=kind,
        parse_mode="Markdown",
        reply_markup=kb,
    )


# ---------------------------------------------------------------------------
# Rendering — with structured truncation to fit Telegram's char limit
# ---------------------------------------------------------------------------

def render_summary_table(
    summary: GuildOmicronSummary,
    char_budget: int = _MAX_MESSAGE_CHARS,
) -> str:
    """
    Monospace table inside a Markdown code block.

    If the full table exceeds char_budget, player rows are truncated
    from the end (alphabetical order is preserved) and a note is
    appended outside the code block.
    """
    if not summary.players:
        return f"🔮 *Omicrones — {summary.guild_label}*\nSin jugadores."

    alias_w = max(
        len("Jugador"),
        max(len(p.alias) for p in summary.players),
    )
    mode_headers = {
        m: f"{mode_short(m)} ({summary.catalog_totals[m]})"
        for m in summary.modes
    }
    mode_widths = {}
    for m in summary.modes:
        max_val_w = max(
            (len(str(p.counts_by_mode.get(m, 0))) for p in summary.players),
            default=1,
        )
        mode_widths[m] = max(len(mode_headers[m]), max_val_w)

    n_ok = sum(1 for p in summary.players if p.fetch_ok)
    title = (
        f"🔮 *Omicrones — {summary.guild_label}*  "
        f"_({n_ok}/{summary.total_players} jugadores)_"
    )

    header_cells = ["Jugador".ljust(alias_w)]
    for m in summary.modes:
        header_cells.append(mode_headers[m].ljust(mode_widths[m]))
    header_row = "  ".join(header_cells)

    sep_cells = ["-" * alias_w]
    for m in summary.modes:
        sep_cells.append("-" * mode_widths[m])
    sep_row = "  ".join(sep_cells)

    player_rows: List[str] = []
    for p in summary.players:
        row = [p.alias.ljust(alias_w)]
        for m in summary.modes:
            cell = "—" if not p.fetch_ok else str(p.counts_by_mode.get(m, 0))
            row.append(cell.ljust(mode_widths[m]))
        player_rows.append("  ".join(row))

    footer_lines: List[str] = []
    if summary.failed_player_aliases:
        n = len(summary.failed_player_aliases)
        footer_lines.append(
            f"⚠️ {n} {'jugador sin datos' if n == 1 else 'jugadores sin datos'} "
            f"(Comlink falló)."
        )
    footer_lines.append(f"_Generado en {summary.elapsed_seconds:.0f}s._")

    full_parts = (
        [title, "```", header_row, sep_row]
        + player_rows
        + ["```"]
        + footer_lines
    )
    full_text = "\n".join(full_parts)
    if len(full_text) <= char_budget:
        return full_text

    NOTE_RESERVE = 120
    base_parts = [title, "```", header_row, sep_row, "```"] + footer_lines
    base_size = len("\n".join(base_parts)) + 1
    rows_budget = char_budget - base_size - NOTE_RESERVE

    included: List[str] = []
    used = 0
    for row in player_rows:
        size = len(row) + 1
        if used + size > rows_budget:
            break
        included.append(row)
        used += size

    hidden = len(player_rows) - len(included)
    note = (
        f"_… y {hidden} jugadores más "
        f"(mensaje truncado por límite de Telegram)._"
    )

    parts = (
        [title, "```", header_row, sep_row]
        + included
        + ["```", note]
        + footer_lines
    )
    return "\n".join(parts)


def render_mode_detail(
    summary: GuildOmicronSummary,
    mode_text: str,
    char_budget: int = _MAX_MESSAGE_CHARS,
) -> str:
    """
    Per-mode drill-down. Lists every omicron in this mode with the
    count of how many guild members have it applied. Sorted by count
    descending; ties broken alphabetically.

    If the rendered list exceeds char_budget, the lowest-count entries
    are dropped.
    """
    catalog_for_mode = summary.catalog_by_mode.get(mode_text, [])
    counts = summary.skill_counts_by_mode.get(mode_text, {})
    n_ok = sum(1 for p in summary.players if p.fetch_ok)

    rows = [
        (counts.get(entry.skill_key, 0), entry.skill_key)
        for entry in catalog_for_mode
    ]
    rows.sort(key=lambda r: (-r[0], r[1].lower()))

    title_lines = [
        f"🔮 *Omicrones de {mode_short(mode_text)}* — {summary.guild_label}",
        f"_{n_ok} jugadores analizados, "
        f"{summary.catalog_totals.get(mode_text, 0)} omicrones en el catálogo_",
        "",
    ]

    if not rows:
        return "\n".join(title_lines + ["_(catálogo vacío para este modo)_"])

    formatted_rows = [f"• {sk}  →  *{c}*" for c, sk in rows]

    full_text = "\n".join(title_lines + formatted_rows)
    if len(full_text) <= char_budget:
        return full_text

    NOTE_RESERVE = 140
    base_size = len("\n".join(title_lines)) + 1
    rows_budget = char_budget - base_size - NOTE_RESERVE

    included: List[str] = []
    used = 0
    for row in formatted_rows:
        size = len(row) + 1
        if used + size > rows_budget:
            break
        included.append(row)
        used += size

    hidden = len(formatted_rows) - len(included)
    note = (
        f"_… y {hidden} omicrones más con menor uso "
        f"(mensaje truncado por límite de Telegram)._"
    )

    return "\n".join(title_lines + included + [note])


def build_mode_keyboard(
    summary: GuildOmicronSummary,
    token: str,
    include_back: bool,
) -> InlineKeyboardMarkup:
    """Mode buttons packed up to 4 per row, plus an optional 'back' row."""
    mode_buttons = [
        InlineKeyboardButton(
            text=mode_short(mode),
            callback_data=f"{CB_MODE}:{token}:{idx}",
        )
        for idx, mode in enumerate(summary.modes)
    ]
    rows = []
    for i in range(0, len(mode_buttons), 4):
        rows.append(mode_buttons[i:i + 4])
    if include_back:
        rows.append([
            InlineKeyboardButton(
                "◀ Volver al resumen",
                callback_data=f"{CB_MODE}:{token}:back",
            )
        ])
    return InlineKeyboardMarkup(rows)


# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------

def get_handlers():
    return [
        CommandHandler("omicronsummary", cmd_omicronsummary),
        CallbackQueryHandler(cb_summary_guild, pattern=rf"^{CB_GUILD}:[^:]+$"),
        CallbackQueryHandler(cb_summary_mode,  pattern=rf"^{CB_MODE}:[^:]+:[^:]+$"),
    ]
