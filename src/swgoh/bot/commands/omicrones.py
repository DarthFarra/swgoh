# src/swgoh/bot/commands/omicrones.py
"""
/omicrones — self-service omicron recommendation command.

Flow:
  1. User runs /omicrones.
  2. If registered in >1 guild → pick guild. Otherwise auto-use it.
  3. Pick mode (Territory War / Grand Arena / Territory Battles / etc.).
     The list is read live from the CharactersOmicrons catalog, so it
     adapts to whatever modes the game adds.
  4. Bot fetches the player's current roster from Comlink (NOT from
     Player_Skills/Player_Units), reads priorities (OmicronPriorities)
     for (guild, mode), and renders the top-N missing omicrons ranked
     by guild priority.

Why Comlink instead of the snapshot sheets:
  Player_Skills/Player_Units are written weekly by /syncguild. For a
  recommendation command, a user who just upgraded an omicron and runs
  /omicrones must see the result reflected — otherwise they're staring
  at a stale list and lose trust in the bot. The trade-off is one extra
  HTTP call per invocation, mitigated by a 60s in-memory TTL cache.

Security:
  - Callback data is validated against whitelists from Sheets — same
    pattern as /misoperaciones and /operacionesjugador.
  - Rate-limited per user to avoid hammering Comlink and the spreadsheet.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

from ..services.sheets import (
    open_ss,
    usuarios_guilds_for_user,
    resolve_label_name_rote_by_id,
    user_alias_for_guild,
    player_id_for_alias,
)
from ..services.omicrons import (
    _norm,
    list_omicron_modes,
    read_omicron_catalog,
    read_omicron_priorities,
    compute_recommendations,
    Recommendation,
)
from ..services.comlink_player import (
    fetch_player_state,
    load_unit_catalog,
)
from ...processing import _roster_parse as rp
from ..keyboards.guild_select import make_keyboard_guilds
from ..security import (
    rate_limit,
    session_set,
    session_get,
    validate_guild_id,
    CallbackValidationError,
)
from .. import config as bot_cfg

log = logging.getLogger(__name__)

# Session keys
_S_GID    = "omi_guild_id"
_S_GNAME  = "omi_guild_name"
_S_LABEL  = "omi_label"
_S_MODES  = "omi_modes"

# Callback prefixes
CB_GUILD = "omi"
CB_MODE  = "omimode"

_COMLINK_TIMEOUT_S = 10.0


# ---------------------------------------------------------------------------
# /omicrones entry point
# ---------------------------------------------------------------------------

@rate_limit(cooldown_seconds=15)
async def cmd_omicrones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ss     = open_ss()
    guilds = usuarios_guilds_for_user(ss, update.effective_user.id)

    if not guilds:
        await update.message.reply_text(
            "❌ No estás registrado en ningún gremio. Usa /register primero."
        )
        return

    if len(guilds) > 1:
        opts = [(label, gid) for label, gid, _ in guilds]
        await update.message.reply_text(
            "Elige el gremio para ver tus omicrones pendientes:",
            reply_markup=make_keyboard_guilds(opts, CB_GUILD),
        )
        return

    label, gid, gname = guilds[0]
    await _show_mode_picker(update, context, ss, gid, gname, label, via_callback=False)


async def cb_omi_guild(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles guild selection from the keyboard."""
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith(f"{CB_GUILD}:"):
        return

    raw_gid = data.split(":", 1)[1]
    user_id = q.from_user.id
    ss      = open_ss()

    guilds    = usuarios_guilds_for_user(ss, user_id)
    known_ids = {gid for _, gid, _ in guilds}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)
    await _show_mode_picker(update, context, ss, gid, gname, label, via_callback=True)


# ---------------------------------------------------------------------------
# Mode picker
# ---------------------------------------------------------------------------

async def _show_mode_picker(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    ss,
    gid: str,
    gname: str,
    label: str,
    via_callback: bool,
):
    user_id = update.effective_user.id

    alias = user_alias_for_guild(ss, user_id, gname)
    if not alias:
        msg = f"❌ No encuentro tu alias en '{gname}'. ¿Te has registrado?"
        if via_callback:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    modes = list_omicron_modes(ss)
    if not modes:
        msg = (
            "❌ No hay datos de omicrones todavía. "
            "Ejecuta /syncdata para poblar el catálogo."
        )
        if via_callback:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    session_set(context, user_id, _S_GID,   gid)
    session_set(context, user_id, _S_GNAME, gname)
    session_set(context, user_id, _S_LABEL, label)
    session_set(context, user_id, _S_MODES, modes)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            text=mode,
            callback_data=f"{CB_MODE}:{gid}:{idx}",
        )]
        for idx, mode in enumerate(modes)
    ])
    text = f"Elige el tipo de omicron para *{alias}* en *{label}*:"
    if via_callback:
        await update.callback_query.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")


# ---------------------------------------------------------------------------
# Mode selected — compute & render
# ---------------------------------------------------------------------------

async def cb_omi_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith(f"{CB_MODE}:"):
        return

    try:
        _, raw_gid, raw_idx = data.split(":", 2)
        mode_idx = int(raw_idx)
    except (ValueError, IndexError):
        await q.edit_message_text("❌ Datos no válidos.")
        return

    user_id = q.from_user.id
    ss      = open_ss()

    guilds    = usuarios_guilds_for_user(ss, user_id)
    known_ids = {gid for _, gid, _ in guilds}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)

    modes: List[str] = session_get(context, user_id, _S_MODES) or list_omicron_modes(ss)
    if not (0 <= mode_idx < len(modes)):
        await q.edit_message_text("❌ Modo no válido. Vuelve a empezar con /omicrones.")
        return
    mode_text = modes[mode_idx]

    alias = user_alias_for_guild(ss, user_id, gname)
    if not alias:
        await q.edit_message_text(f"❌ No encuentro tu alias en '{gname}'.")
        return

    player_id = player_id_for_alias(ss, gname, alias)
    if not player_id:
        await q.edit_message_text(
            f"❌ No encuentro tu Player Id en '{gname}'. "
            "Pide a un oficial que ejecute /syncguild."
        )
        return

    catalog = read_omicron_catalog(ss)
    if not catalog:
        await q.edit_message_text(
            "❌ Catálogo de omicrones vacío. Ejecuta /syncdata."
        )
        return

    priorities = read_omicron_priorities(ss, gname, mode_text)

    await q.edit_message_text(
        f"⏳ Consultando Comlink para *{alias}*…",
        parse_mode="Markdown",
    )

    base_to_name, is_ship_by_base = load_unit_catalog(ss, context.application.bot_data)

    try:
        state = await fetch_player_state(
            player_id=player_id,
            bot_data=context.application.bot_data,
            is_ship_by_base=is_ship_by_base,
            timeout_seconds=_COMLINK_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        log.warning("Comlink timeout for player_id=%s alias=%s", player_id, alias)
        await q.edit_message_text(
            "⌛ Comlink no respondió a tiempo. Inténtalo de nuevo en unos segundos."
        )
        return
    except Exception:
        log.exception("Comlink fetch failed for player_id=%s alias=%s", player_id, alias)
        await q.edit_message_text(
            "❌ Error consultando Comlink. Inténtalo más tarde."
        )
        return

    skill_tiers_by_key = _build_skill_tiers_by_key(state.skill_tiers_by_id, catalog)
    relics_by_char = _build_relics_by_char(state.relic_by_base_id, base_to_name)

    recs, stats = compute_recommendations(
        catalog=catalog,
        priorities=priorities,
        player_skill_tiers=skill_tiers_by_key,
        player_relics=relics_by_char,
        mode_text=mode_text,
        min_relic=bot_cfg.OMICRON_MIN_RELIC,
        top_n=bot_cfg.OMICRON_RECOMMEND_TOP_N,
    )

    text = _render_message(
        alias=alias, label=label, mode_text=mode_text,
        recs=recs, stats=stats,
        min_relic=bot_cfg.OMICRON_MIN_RELIC,
    )
    await q.edit_message_text(text, parse_mode="Markdown")


# ---------------------------------------------------------------------------
# Translation helpers
# ---------------------------------------------------------------------------

def _build_skill_tiers_by_key(
    skill_tiers_by_id: Dict[str, int],
    catalog,
) -> Dict[str, int]:
    """
    Translate {skill_id: tier} from Comlink into {skill_key_norm: tier}
    using the omicron catalog as the mapping table.
    """
    out: Dict[str, int] = {}
    for entry in catalog:
        key_norm = _norm(entry.skill_key)
        tier = skill_tiers_by_id.get(entry.skill_id, 0)
        if tier > out.get(key_norm, 0):
            out[key_norm] = tier
    return out


def _build_relics_by_char(
    relic_by_base_id: Dict[str, Optional[int]],
    base_to_name: Dict[str, str],
) -> Dict[str, Optional[int]]:
    """
    Translate {base_id: comlink_currentTier|None} into
    {character_name_norm: relic_level_int|None}.
    """
    out: Dict[str, Optional[int]] = {}
    for base_id, current_tier in relic_by_base_id.items():
        name = base_to_name.get(base_id)
        if not name:
            continue
        char_norm = _norm(name)
        if current_tier is None:
            out[char_norm] = None
        else:
            out[char_norm] = rp.relic_level(current_tier)
    return out


# ---------------------------------------------------------------------------
# Message rendering
# ---------------------------------------------------------------------------

def _render_message(
    *,
    alias: str,
    label: str,
    mode_text: str,
    recs: List[Recommendation],
    stats: Dict[str, int],
    min_relic: int,
) -> str:
    """
    Build the user-facing Markdown message. Kept separate from the
    handler so formatting can be tweaked without touching async logic.
    """
    header = f"🔮 *Omicrones pendientes* — {label} / {mode_text}\n👤 {alias}\n"

    # Case A: no priorities set at all
    if stats["priorities_total"] == 0:
        return (
            f"{header}\n"
            f"⚠️ Vuestro gremio aún no tiene prioridades configuradas para "
            f"*{mode_text}*.\n\n"
            f"Pídele a un oficial que rellene la pestaña *OmicronPriorities* "
            f"con las columnas: Guild Name, Mode, Skill, Priority, Notes."
        )

    # Case B: priorities exist but no actionable recommendations to show
    if not recs:
        # B1: nothing actionable AND nothing excluded → user has them all
        if (stats["actionable_pending"] == 0
                and stats["excluded_not_owned"] == 0
                and stats["excluded_low_relic"] == 0
                and stats["already_have"] > 0):
            return (
                f"{header}\n"
                f"🎉 ¡Tienes todos los omicrones prioritarios para "
                f"*{mode_text}*!\n\n"
                f"_({stats['already_have']} de "
                f"{stats['priorities_matched_catalog']} prioridades cumplidas)_"
            )
        # B2: nothing actionable because everything is excluded or unmatched
        lines = [header, "No hay recomendaciones aplicables ahora mismo.", ""]
        lines.append(_summary_block(stats, min_relic))
        return "\n".join(lines)

    # Case C: actionable recommendations to show
    lines = [header]
    for i, r in enumerate(recs, start=1):
        if r.player_relic is None or r.player_relic < 0:
            relic_str = "<R0"
        else:
            relic_str = f"R{r.player_relic}"
        lines.append(f"*{i}.* {r.skill_key}  _({relic_str})_")
        if r.notes:
            lines.append(f"   ↳ _{r.notes}_")

    lines.append("")
    lines.append(_summary_block(stats, min_relic))
    return "\n".join(lines)


def _summary_block(stats: Dict[str, int], min_relic: int) -> str:
    """
    Build the summary footer.

    Design goals:
      - Every number is unambiguous: anyone can verify it against the
        OmicronPriorities sheet without doing arithmetic in their head.
      - Counters with value 0 are omitted (no "0 personajes excluidos"
        noise).
      - The "displayed N of M" line only appears if top_n actually
        truncated something.
      - Three sections, each on its own line: progress, truncation,
        exclusions / catalog issues.

    Reads from these stats keys (see compute_recommendations docstring
    for invariants):
      already_have, actionable_pending, recommended,
      excluded_not_owned, excluded_low_relic, priorities_unmatched
    """
    lines: List[str] = []

    # --- Progress line: always shown.
    # Show "X cumplidas, Y pendientes" relative to actionable items;
    # the totals (matched / sheet) come after, so users can sanity-check.
    progress = (
        f"_Progreso_: {stats['already_have']} cumplidas, "
        f"{stats['actionable_pending']} pendientes."
    )
    lines.append(progress)

    # --- Truncation line: only if top_n actually capped the list.
    not_shown = stats["actionable_pending"] - stats["recommended"]
    if not_shown > 0:
        lines.append(
            f"_Mostrando_: {stats['recommended']} de "
            f"{stats['actionable_pending']} pendientes "
            f"(faltan {not_shown} por mostrar)."
        )

    # --- Exclusions: only if non-zero. Pluralised manually because
    # Spanish doesn't like English-style "1 personaje(s)".
    excluded_parts: List[str] = []
    if stats["excluded_not_owned"]:
        n = stats["excluded_not_owned"]
        excluded_parts.append(
            f"{n} {'personaje no desbloqueado' if n == 1 else 'personajes no desbloqueados'}"
        )
    if stats["excluded_low_relic"]:
        n = stats["excluded_low_relic"]
        excluded_parts.append(f"{n} por debajo de R{min_relic}")
    if excluded_parts:
        lines.append("_Excluidos_: " + ", ".join(excluded_parts) + ".")

    # --- Catalog warning: priorities in the sheet that don't match any
    # known omicron skill (typos / wrong mode / renamed by CG).
    if stats["priorities_unmatched"]:
        n = stats["priorities_unmatched"]
        lines.append(
            f"⚠️ {n} "
            f"{'prioridad' if n == 1 else 'prioridades'} sin correspondencia "
            f"en el catálogo (revisar *OmicronPriorities*)."
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------

def get_handlers():
    return [
        CommandHandler("omicrones", cmd_omicrones),
        CallbackQueryHandler(cb_omi_guild, pattern=rf"^{CB_GUILD}:[^:]+$"),
        CallbackQueryHandler(cb_omi_mode,  pattern=rf"^{CB_MODE}:[^:]+:\d+$"),
    ]
