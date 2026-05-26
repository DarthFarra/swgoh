# src/swgoh/bot/commands/omicronsummary.py
"""
/omicronsummary — officer-only roll-up of omicron counts across the guild.

Flow:
  1. Officer runs /omicronsummary.
  2. If they're an officer in >1 guild → pick guild. Otherwise auto-use.
  3. Bot fetches every guild member's roster from Comlink in parallel,
     aggregates omicron counts per mode, and renders a table.
  4. Inline buttons per mode → drill-down showing per-skill applied
     counts (sorted descending), with no re-fetch (drill-down reads
     from the cached aggregation).

Why officer-only:
  This is roster-wide intelligence. Different guilds will have different
  views on whether members should see each other's progress; officer-gate
  is the conservative default and mirrors /sendassignments and
  /operacionesjugador.

Performance:
  ~50 players via semaphore(5) typically completes in 20-30s cold, much
  faster if comlink_player's 60s player cache has warm entries. Progress
  is reported every ~10 completed players.

Security:
  - Officer role required at command entry AND re-validated on each
    button click against the guild_id stored in the cached summary.
  - Random short token in callback_data: not guessable across summaries.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import List

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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

# Callback prefixes. Both are distinct from /omicrones' "omi" / "omimode"
# (the regex anchors + colon boundary prevent any overlap).
CB_GUILD = "omisumg"   # guild picker selection
CB_MODE  = "omisum"    # mode drill-down (or 'back')

# Tunables. If Comlink starts rate-limiting, drop _CONCURRENCY first.
_CONCURRENCY       = 5
_COMLINK_TIMEOUT_S = 10.0
_PROGRESS_EVERY_N  = 10


# ---------------------------------------------------------------------------
# /omicronsummary entry point
# ---------------------------------------------------------------------------

@rate_limit(cooldown_seconds=30)
async def cmd_omicronsummary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss      = open_ss()
    user_id = update.effective_user.id

    authorized = user_authorized_guilds(ss, user_id)  # [(label, gid), ...]
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
    """Handles guild picker selection."""
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

    # Belt-and-braces: re-check the officer role on the resolved guild.
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

    # Initial progress message — captured as a Message we can edit later.
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

    # Throttled progress callback. We update at most every 10 players
    # (or proportionally for smaller guilds) to avoid Telegram rate
    # limits on message edits.
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
                # Edit failures (rate limit, message-not-modified) are
                # not actionable — keep fetching.
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

    try:
        await progress_msg.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    except Exception:
        log.exception(
            "Failed to edit summary message (likely too long); "
            "falling back to a new reply."
        )
        await progress_msg.reply_text(text, parse_mode="Markdown", reply_markup=kb)


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

    # Re-validate authorisation against the guild this summary belongs to.
    # Prevents a leaked token from being used by someone who isn't an
    # officer in this specific guild.
    user_id = q.from_user.id
    ss      = open_ss()
    if not user_has_role_in_guild(ss, user_id, summary.guild_id):
        await q.edit_message_text(
            "❌ No tienes permisos en este gremio."
        )
        return

    if target == "back":
        text = render_summary_table(summary)
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
        kb   = build_mode_keyboard(summary, token, include_back=True)

    await q.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_summary_table(summary: GuildOmicronSummary) -> str:
    """
    Monospace table inside a Markdown code block. Format:

        Jugador        TW (90)  GAC (115)  TB (20)
        ─────────────  ───────  ─────────  ───────
        Alice          12       8          3
        Bob            15       10         5
        ...

    Players with failed fetches render '—' for every mode cell.
    """
    if not summary.players:
        return f"🔮 *Omicrones — {summary.guild_label}*\nSin jugadores."

    alias_w = max(
        len("Jugador"),
        max(len(p.alias) for p in summary.players),
    )

    mode_headers = {m: f"{mode_short(m)} ({summary.catalog_totals[m]})"
                    for m in summary.modes}
    mode_widths = {}
    for m in summary.modes:
        max_val_w = max(
            (len(str(p.counts_by_mode.get(m, 0))) for p in summary.players),
            default=1,
        )
        mode_widths[m] = max(len(mode_headers[m]), max_val_w)

    lines: List[str] = []
    n_ok = sum(1 for p in summary.players if p.fetch_ok)
    lines.append(
        f"🔮 *Omicrones — {summary.guild_label}*  "
        f"_({n_ok}/{summary.total_players} jugadores)_"
    )
    lines.append("```")

    # Header
    header_cells = ["Jugador".ljust(alias_w)]
    for m in summary.modes:
        header_cells.append(mode_headers[m].ljust(mode_widths[m]))
    lines.append("  ".join(header_cells))

    # Separator (use plain ASCII '-' to avoid font issues on some clients)
    sep_cells = ["-" * alias_w]
    for m in summary.modes:
        sep_cells.append("-" * mode_widths[m])
    lines.append("  ".join(sep_cells))

    # Rows
    for p in summary.players:
        row = [p.alias.ljust(alias_w)]
        for m in summary.modes:
            cell = "—" if not p.fetch_ok else str(p.counts_by_mode.get(m, 0))
            row.append(cell.ljust(mode_widths[m]))
        lines.append("  ".join(row))

    lines.append("```")

    # Footer
    if summary.failed_player_aliases:
        n = len(summary.failed_player_aliases)
        lines.append(
            f"⚠️ {n} {'jugador sin datos' if n == 1 else 'jugadores sin datos'} "
            f"(Comlink falló)."
        )
    lines.append(f"_Generado en {summary.elapsed_seconds:.0f}s._")

    return "\n".join(lines)


def render_mode_detail(summary: GuildOmicronSummary, mode_text: str) -> str:
    """
    Per-mode drill-down. Lists every omicron in this mode with the
    count of how many guild members have it applied. Sorted by count
    descending; ties broken alphabetically for stable output.
    """
    catalog_for_mode = summary.catalog_by_mode.get(mode_text, [])
    counts = summary.skill_counts_by_mode.get(mode_text, {})
    n_ok = sum(1 for p in summary.players if p.fetch_ok)

    rows: List[tuple] = [
        (counts.get(entry.skill_key, 0), entry.skill_key)
        for entry in catalog_for_mode
    ]
    rows.sort(key=lambda r: (-r[0], r[1].lower()))

    lines: List[str] = [
        f"🔮 *Omicrones de {mode_short(mode_text)}* — {summary.guild_label}",
        f"_{n_ok} jugadores analizados, "
        f"{summary.catalog_totals.get(mode_text, 0)} omicrones en el catálogo_",
        "",
    ]
    if not rows:
        lines.append("_(catálogo vacío para este modo)_")
    else:
        for count, skill_key in rows:
            lines.append(f"• {skill_key}  →  *{count}*")

    return "\n".join(lines)


def build_mode_keyboard(
    summary: GuildOmicronSummary,
    token: str,
    include_back: bool,
) -> InlineKeyboardMarkup:
    """
    Mode buttons packed up to 4 per row. Optional 'back' row for the
    drill-down view so the user can return to the main table.
    """
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
