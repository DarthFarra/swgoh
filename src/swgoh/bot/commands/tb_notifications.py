# src/swgoh/bot/commands/tb_notifications.py
"""
Callback handlers for the auto-summary's inline buttons.

The buttons themselves are built in discord_listener._build_undeployed_keyboard
and attached to the auto-summary message when it's sent. This module
provides the four callbacks that handle them:

  tbudm:{guild_id}          — DM confirm step (shows Yes/Cancel)
  tbudmconfirm:{guild_id}   — DM execution (sends DMs, reports result)
  tbupub:{guild_id}         — Publish-to-channel confirm step
  tbupubconfirm:{guild_id}  — Publish execution

Pattern follows commands/tickets.py — same two-step confirm-then-act
shape, same MarkdownV2 dialect for the result messages, same "edit the
message rather than sending a new one" UX.

Authorization:
  Every callback re-checks `user_has_leadership_role(ss, user_id, gname)`.
  Authorization can't be derived from "they ran the command" — there IS
  no command; the bot sent the auto-summary unprompted, and any chat
  member could tap a button. So we authorize at the action site.

Cache contract:
  The undeployed list is read from `tb_undeployed_cache` keyed by
  `update.callback_query.message.message_id`. If the cache entry is
  gone (>48h TTL, or bot restart), we show "session expired" and stop.
  This is intentional and acceptable — the message is stale enough
  that re-running /tb export is the right action.

PTB API note:
  bot_data is accessed via `context.application.bot_data` — NOT via
  `q.bot.application.bot_data`. CallbackQuery has no public `.bot`
  attribute in PTB v20+. The `context` argument is the canonical
  access path for application state inside a callback.
"""
from __future__ import annotations

import logging
from typing import Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Forbidden, BadRequest
from telegram.ext import CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

from ..services.sheets import (
    open_ss,
    user_has_leadership_role,
    get_chat_ids_for_members,
    get_usernames_for_members,
    get_tb_channel_config_for_guild,
    resolve_label_name_by_guild_id,
)
from ..services.tb_undeployed_cache import (
    UndeployedSnapshot,
    get_snapshot as get_undeployed_snapshot,
)
from ..services.tb_notify import (
    send_deployment_reminders,
    publish_deployment_to_channel,
)

log = logging.getLogger(__name__)


# Callback prefixes — namespaced "tbu" (TB undeployed) to avoid collision.
_CB_DM_CONFIRM_PREFIX     = "tbudm:"
_CB_DM_EXECUTE_PREFIX     = "tbudmconfirm:"
_CB_PUB_CONFIRM_PREFIX    = "tbupub:"
_CB_PUB_EXECUTE_PREFIX    = "tbupubconfirm:"


# ---------------------------------------------------------------------------
# MarkdownV2 escaper — local copy to keep this module self-contained.
# Matches the implementation in services/tb_notify.py.
# ---------------------------------------------------------------------------

_MD2_SPECIAL = r"_*[]()~`>#+-=|{}.!\\"


def _md2(text) -> str:
    """
    Escape every MarkdownV2 special character. Conservative — we escape
    even characters that are usually safe in our outputs, because GP
    values like "13.7M" contain a literal '.' that must be escaped, and
    catching everything is simpler than tracking per-context rules.
    """
    if not text:
        return ""
    return "".join(
        f"\\{ch}" if ch in _MD2_SPECIAL else ch
        for ch in str(text)
    )


def _disable_buttons_kb() -> InlineKeyboardMarkup:
    """Empty keyboard — used to remove buttons after action completes."""
    return InlineKeyboardMarkup([])


# ---------------------------------------------------------------------------
# Resolver helpers
#
# Both the CONFIRM step (button on the auto-summary message) and the
# EXECUTE step (button on the confirm dialog) need to:
#   1. Validate the callback_data prefix and extract guild_id.
#   2. Look up the cached undeployed snapshot by message_id.
#   3. Resolve the guild label/name from the sheet.
#   4. Authorize the user as a guild officer.
#
# They differ in WHICH message_id keys the cache:
#   - CONFIRM step: q.message.message_id IS the original auto-summary.
#   - EXECUTE step: q.message is the confirm dialog. The original
#     auto-summary is q.message.reply_to_message (Telegram populates
#     this when the dialog was sent via reply_text()).
# ---------------------------------------------------------------------------

async def _resolve_or_reply(
    q,
    context: ContextTypes.DEFAULT_TYPE,
    callback_data_prefix: str,
) -> Optional[tuple[str, str, str, UndeployedSnapshot]]:
    """
    CONFIRM-step resolver. Cache key is q.message.message_id.

    Returns (guild_id, sheet_label, sheet_gname, cached_snapshot) on
    success; returns None and posts a user-facing error if any step fails.
    """
    data = q.data or ""
    if not data.startswith(callback_data_prefix):
        await q.answer()
        return None

    guild_id = data[len(callback_data_prefix):]
    if not guild_id:
        await q.answer("Datos inválidos.", show_alert=True)
        return None

    snapshot = get_undeployed_snapshot(
        context.application.bot_data, q.message.message_id
    )
    if snapshot is None:
        await q.answer()
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            # Best-effort: the message may no longer be editable.
            log.debug("Could not remove buttons from expired auto-summary.", exc_info=True)
        await q.message.reply_text(
            "_Sesión expirada \\(este mensaje tiene más de 48h o el bot "
            "se reinició\\)\\. Ejecuta /tb export en Discord para refrescar\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return None

    if snapshot.guild_id != guild_id:
        # Defensive: button claims one guild, cache says another.
        log.error(
            "Cache guild_id mismatch: button=%s cache=%s message_id=%d",
            guild_id, snapshot.guild_id, q.message.message_id,
        )
        await q.answer("Estado inconsistente. Reintenta /tb export.", show_alert=True)
        return None

    return await _resolve_authorization(q, guild_id, snapshot)


async def _resolve_execute_step(
    q,
    context: ContextTypes.DEFAULT_TYPE,
    prefix: str,
) -> Optional[tuple[str, str, str, UndeployedSnapshot]]:
    """
    EXECUTE-step resolver. Cache key is the ORIGINAL auto-summary's
    message_id, reached via q.message.reply_to_message (populated by
    Telegram when the confirm dialog was sent via reply_text()).
    """
    data = q.data or ""
    if not data.startswith(prefix):
        await q.answer()
        return None
    guild_id = data[len(prefix):]
    if not guild_id:
        await q.answer("Datos inválidos.", show_alert=True)
        return None

    original = q.message.reply_to_message
    if original is None:
        # Shouldn't happen given how we built the dialog, but cope.
        log.warning("Execute callback fired with no reply_to_message; q.message=%r", q.message)
        await q.answer("No encuentro el mensaje original.", show_alert=True)
        return None

    snapshot = get_undeployed_snapshot(
        context.application.bot_data, original.message_id
    )
    if snapshot is None:
        await q.answer()
        await q.edit_message_text(
            "_Sesión expirada\\. Ejecuta /tb export para refrescar\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return None

    if snapshot.guild_id != guild_id:
        log.error(
            "Cache guild_id mismatch (execute): button=%s cache=%s message_id=%d",
            guild_id, snapshot.guild_id, original.message_id,
        )
        await q.answer("Estado inconsistente.", show_alert=True)
        return None

    return await _resolve_authorization(q, guild_id, snapshot)


async def _resolve_authorization(
    q,
    guild_id: str,
    snapshot: UndeployedSnapshot,
) -> Optional[tuple[str, str, str, UndeployedSnapshot]]:
    """
    Common authorization tail: look up the guild in the sheet,
    re-verify the user has the leadership role.

    Factored out because both confirm and execute resolvers need the
    same exact behavior here, and duplication would invite drift.
    """
    try:
        ss = open_ss()
        label, gname = resolve_label_name_by_guild_id(ss, guild_id)
    except Exception:
        log.exception("Sheet open failed during TB undeployed callback.")
        await q.answer("Error leyendo configuración.", show_alert=True)
        return None

    if not gname:
        # Guild exists in cache but not in sheet — config drift.
        log.warning(
            "TB undeployed callback for guild_id=%s but no matching sheet row.",
            guild_id,
        )
        await q.answer(
            "Gremio no encontrado en la configuración.", show_alert=True
        )
        return None

    if not user_has_leadership_role(ss, q.from_user.id, gname):
        await q.answer("Solo oficiales pueden usar esta acción.", show_alert=True)
        return None

    return guild_id, label, gname, snapshot


# ---------------------------------------------------------------------------
# DM confirm
# ---------------------------------------------------------------------------

async def cb_tbu_dm_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Step 1 of DM flow — show the confirmation dialog."""
    q = update.callback_query
    resolved = await _resolve_or_reply(q, context, _CB_DM_CONFIRM_PREFIX)
    if resolved is None:
        return
    guild_id, label, _gname, snapshot = resolved
    await q.answer()

    n = len(snapshot.members)
    if n == 0:
        # Defensive — empty undeployed list shouldn't have shown buttons,
        # but cope.
        await q.message.reply_text(
            "Todos han desplegado\\. Nada que enviar\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            f"Sí, enviar a {n}",
            callback_data=f"{_CB_DM_EXECUTE_PREFIX}{guild_id}",
        ),
        InlineKeyboardButton(
            "Cancelar",
            callback_data=f"tbucancel:{guild_id}",
        ),
    ]])
    await q.message.reply_text(
        f"*{_md2(label)}* \u2014 Recordatorio TB\n\n"
        f"Se enviará un DM a *{_md2(str(n))}* miembro\\(s\\) con despliegue "
        f"pendiente\\.\n\nConfirmas?",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN_V2,
    )


# ---------------------------------------------------------------------------
# DM execute
# ---------------------------------------------------------------------------

async def cb_tbu_dm_execute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Step 2 of DM flow — send the DMs and report sent/failed counts."""
    q = update.callback_query
    resolved = await _resolve_execute_step(q, context, _CB_DM_EXECUTE_PREFIX)
    if resolved is None:
        return
    guild_id, label, gname, snapshot = resolved
    await q.answer()

    members = snapshot.members
    if not members:
        await q.edit_message_text(
            "Todos han desplegado\\. Nada que enviar\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    await q.edit_message_text(
        f"Enviando recordatorios para *{_md2(label)}*\u2026",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    try:
        ss = open_ss()
        chat_ids = get_chat_ids_for_members(
            ss, gname, [m.player_name for m in members]
        )
    except Exception as exc:
        log.exception("Error reading chat_ids for guild %s", gname)
        await q.edit_message_text(
            f"Error leyendo los chat IDs\\.\n\n`{_md2(str(exc))}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    sent, failed = await send_deployment_reminders(
        bot=context.bot, members=members, chat_ids=chat_ids
    )

    lines = [
        f"*{_md2(label)}* \u2014 Resultado del recordatorio TB\n",
        f"Enviados: *{_md2(str(sent))}* / {_md2(str(sent + failed))}",
    ]
    if failed:
        lines.append(
            f"Fallidos: *{_md2(str(failed))}* "
            f"\\(sin chat\\_id o bot bloqueado\\)"
        )

    await q.edit_message_text(
        "\n".join(lines),
        reply_markup=_disable_buttons_kb(),
        parse_mode=ParseMode.MARKDOWN_V2,
    )


# ---------------------------------------------------------------------------
# Publish-to-channel confirm
# ---------------------------------------------------------------------------

async def cb_tbu_publish_confirm(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Step 1 of publish flow — validate channel config, show confirm."""
    q = update.callback_query
    resolved = await _resolve_or_reply(q, context, _CB_PUB_CONFIRM_PREFIX)
    if resolved is None:
        return
    guild_id, label, gname, snapshot = resolved
    await q.answer()

    try:
        ss = open_ss()
        channel_id, thread_id = get_tb_channel_config_for_guild(ss, gname)
    except Exception:
        log.exception("Sheet read failed for tb_channel_config")
        await q.message.reply_text(
            "Error leyendo configuración del canal\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if not channel_id:
        await q.message.reply_text(
            f"*{_md2(label)}* \u2014 No hay canal de avisos configurado\\.\n\n"
            f"Agrega `announcements\\_channel` y `tb\\_notifications\\_thread` "
            f"en la hoja Guilds\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    n = len(snapshot.members)
    if n == 0:
        await q.message.reply_text(
            "Todos han desplegado\\. Nada que publicar\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            f"Sí, publicar ({n} miembros)",
            callback_data=f"{_CB_PUB_EXECUTE_PREFIX}{guild_id}",
        ),
        InlineKeyboardButton(
            "Cancelar",
            callback_data=f"tbucancel:{guild_id}",
        ),
    ]])

    thread_note = (
        f"thread `{_md2(str(thread_id))}`" if thread_id is not None
        else "topic general del canal"
    )
    await q.message.reply_text(
        f"*{_md2(label)}* \u2014 Publicar despliegue pendiente\n\n"
        f"Se publicará la lista de *{_md2(str(n))}* miembro\\(s\\) en "
        f"el canal `{_md2(channel_id)}` \\({thread_note}\\)\\.\n\nConfirmas?",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN_V2,
    )


# ---------------------------------------------------------------------------
# Publish-to-channel execute
# ---------------------------------------------------------------------------

async def cb_tbu_publish_execute(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Step 2 of publish flow — post to channel and report success."""
    q = update.callback_query
    resolved = await _resolve_execute_step(q, context, _CB_PUB_EXECUTE_PREFIX)
    if resolved is None:
        return
    guild_id, label, gname, snapshot = resolved
    await q.answer()

    try:
        ss = open_ss()
        channel_id, thread_id = get_tb_channel_config_for_guild(ss, gname)
        usernames = get_usernames_for_members(
            ss, gname, [m.player_name for m in snapshot.members]
        )
    except Exception:
        log.exception("Sheet read failed in publish_execute")
        await q.edit_message_text(
            "Error leyendo configuración\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if not channel_id:
        await q.edit_message_text(
            "No hay canal configurado\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    try:
        await publish_deployment_to_channel(
            bot=context.bot,
            channel_id=channel_id,
            members=snapshot.members,
            usernames=usernames,
            guild_label=label,
            thread_id=thread_id,
        )
    except Forbidden:
        log.error("Bot lacks admin on channel %s", channel_id)
        await q.edit_message_text(
            f"El bot no es admin del canal `{_md2(channel_id)}`\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return
    except BadRequest as exc:
        log.error("BadRequest publishing to %s: %s", channel_id, exc)
        await q.edit_message_text(
            f"ID de canal/thread inválido\\.\n\n`{_md2(str(exc))}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return
    except Exception as exc:
        log.exception("Unexpected error publishing TB undeployed")
        await q.edit_message_text(
            f"Error inesperado al publicar\\.\n\n`{_md2(str(exc))}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    await q.edit_message_text(
        f"*{_md2(label)}* \u2014 Publicado en el canal \u2705",
        reply_markup=_disable_buttons_kb(),
        parse_mode=ParseMode.MARKDOWN_V2,
    )


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------

async def cb_tbu_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """User pressed Cancel on a confirm dialog. Just delete the dialog."""
    q = update.callback_query
    await q.answer("Cancelado.")
    try:
        await q.message.delete()
    except Exception:
        # If we can't delete (e.g. >48h old), edit to a tombstone.
        try:
            await q.edit_message_text(
                "_Cancelado\\._",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        except Exception:
            log.debug("Could not cancel/delete confirm dialog.", exc_info=True)


# ---------------------------------------------------------------------------
# Handler registration — called by main_bot.py
# ---------------------------------------------------------------------------

def get_handlers():
    return [
        CallbackQueryHandler(cb_tbu_dm_confirm,       pattern=r"^tbudm:"),
        CallbackQueryHandler(cb_tbu_dm_execute,       pattern=r"^tbudmconfirm:"),
        CallbackQueryHandler(cb_tbu_publish_confirm,  pattern=r"^tbupub:"),
        CallbackQueryHandler(cb_tbu_publish_execute,  pattern=r"^tbupubconfirm:"),
        CallbackQueryHandler(cb_tbu_cancel,           pattern=r"^tbucancel:"),
    ]
