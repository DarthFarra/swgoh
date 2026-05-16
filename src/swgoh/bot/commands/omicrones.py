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

# Hard timeout on the Comlink call. Reasoning: PTB callback queries
# should answer within ~10s for a good UX, and Comlink player calls
# normally complete in 1-3s. 10s leaves headroom without making the
# user stare at "loading…" forever.
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

    # Never trust callback_data — re-validate against the user's own guilds.
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

    # Persist context. Modes can contain spaces, so we pass them by index
    # in callback_data and keep the actual strings in the session.
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

    # Guild whitelist
    guilds    = usuarios_guilds_for_user(ss, user_id)
    known_ids = {gid for _, gid, _ in guilds}
    try:
        gid = validate_guild_id(raw_gid, known_ids)
    except CallbackValidationError:
        await q.edit_message_text("❌ Gremio no válido.")
        return

    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)

    # Mode whitelist — re-fetch live if the session is gone (process restart).
    modes: List[str] = session_get(context, user_id, _S_MODES) or list_omicron_modes(ss)
    if not (0 <= mode_idx < len(modes)):
        await q.edit_message_text("❌ Modo no válido. Vuelve a empezar con /omicrones.")
        return
    mode_text = modes[mode_idx]

    alias = user_alias_for_guild(ss, user_id, gname)
    if not alias:
        await q.edit_message_text(f"❌ No encuentro tu alias en '{gname}'.")
        return

    # We need the player's Comlink Player Id, populated by /syncguild.
    player_id = player_id_for_alias(ss, gname, alias)
    if not player_id:
        await q.edit_message_text(
            f"❌ No encuentro tu Player Id en '{gname}'. "
            "Pide a un oficial que ejecute /syncguild."
        )
        return

    # Read the omicron catalog — needed both for ID→name translation and
    # to feed the engine.
    catalog = read_omicron_catalog(ss)
    if not catalog:
        await q.edit_message_text(
            "❌ Catálogo de omicrones vacío. Ejecuta /syncdata."
        )
        return

    priorities = read_omicron_priorities(ss, gname, mode_text)

    # Progress message — user sees it before the Comlink call starts.
    await q.edit_message_text(
        f"⏳ Consultando Comlink para *{alias}*…",
        parse_mode="Markdown",
    )

    # Unit catalog (base_id → name, base_id → is_ship). Cached 1h.
    base_to_name, is_ship_by_base = load_unit_catalog(ss, context.application.bot_data)

    # Live fetch with timeout. Two failure paths to handle: timeout
    # (asyncio.TimeoutError) and everything else (network, 5xx, etc.).
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

    # Translate IDs → the keys the engine expects.
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
# Translation helpers (Comlink-ID-keyed → engine-key-keyed)
# ---------------------------------------------------------------------------

def _build_skill_tiers_by_key(
    skill_tiers_by_id: Dict[str, int],
    catalog,
) -> Dict[str, int]:
    """
    Translate {skill_id: tier} from Comlink into {skill_key_norm: tier}
    using the omicron catalog as the mapping table.

    If a skill_id appears in Comlink data but not in the catalog, it's
    ignored (we don't care about non-omicron skills here).

    Two catalog entries could in theory share a skill_id; if so, take max.
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

    None propagates (ships). Unknown base_ids (not in the unit catalog)
    are skipped — without a friendly name we can't match to the catalog's
    character_name column.
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

    # Case B: priorities exist but no actionable recommendation
    if not recs:
        if (stats["already_have"] >= stats["priorities_matched_catalog"]
                and stats["priorities_matched_catalog"] > 0):
            return (
                f"{header}\n"
                f"🎉 ¡Tienes todos los omicrones prioritarios para "
                f"*{mode_text}*!\n\n"
                f"_({stats['already_have']} de "
                f"{stats['priorities_matched_catalog']} prioridades cumplidas)_"
            )
        lines = [header, "No hay recomendaciones aplicables ahora mismo.", ""]
        lines.append(_stats_block(stats, min_relic))
        return "\n".join(lines)

    # Case C: actionable recommendations
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
    lines.append(_stats_block(stats, min_relic))
    return "\n".join(lines)


def _stats_block(stats: Dict[str, int], min_relic: int) -> str:
    """Compact footer with diagnostic counts."""
    parts = [
        f"_Resumen_: "
        f"{stats['already_have']} tienes / "
        f"{stats['priorities_matched_catalog']} prioridades del gremio."
    ]
    notes = []
    if stats["excluded_not_owned"]:
        notes.append(f"{stats['excluded_not_owned']} personajes no desbloqueados")
    if stats["excluded_low_relic"]:
        notes.append(f"{stats['excluded_low_relic']} por debajo de R{min_relic}")
    if stats["priorities_unmatched"]:
        notes.append(
            f"{stats['priorities_unmatched']} prioridades sin correspondencia "
            f"(revisar OmicronPriorities)"
        )
    if notes:
        parts.append("_Excluidos_: " + ", ".join(notes) + ".")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------

def get_handlers():
    return [
        CommandHandler("omicrones", cmd_omicrones),
        CallbackQueryHandler(cb_omi_guild, pattern=rf"^{CB_GUILD}:[^:]+$"),
        CallbackQueryHandler(cb_omi_mode,  pattern=rf"^{CB_MODE}:[^:]+:\d+$"),
    ]
