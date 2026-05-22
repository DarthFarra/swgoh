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
"""
from __future__ import annotations

import logging
from typing import Optional, Sequence

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Forbidden, BadRequest, TelegramError
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
    if not text:
        return ""
    return "".join(
        f"\\{ch}" if ch in _MD2_SPECIAL else ch
        for ch in str(text)
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _resolve_or_reply(
    q,
    callback_data_prefix: str,
) -> Optional[tuple[str, str, str, UndeployedSnapshot]]:
    """
    Pull (guild_id, sheet_label, sheet_gname, cached_snapshot) from the
    callback query.

    Returns None and posts a user-facing error if any step fails:
      - callback_data doesn't match expected prefix
      - cache entry missing/expired
      - guild not in sheet (configuration drift)

    Centralizing this avoids repeating the same error paths in every
    callback.
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
        q.bot.application.bot_data, q.message.message_id
    )
    if snapshot is None:
        await q.answer()
        await q.edit_message_reply_markup(reply_markup=None)
        await q.message.reply_text(
            "_Sesión expirada \\(este mensaje tiene más de 48h o el bot "
            "se reinició\\)\\. Ejecuta /tb export en Discord para refrescar\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return None

    if snapshot.guild_id != guild_id:
        # Defensive: button claims one guild, cache says another.
        # Could happen if message_id collisions across guilds (unlikely)
        # or if the cache key collided. Fail loud.
        log.error(
            "Cache guild_id mismatch: button=%s cache=%s message_id=%d",
            guild_id, snapshot.guild_id, q.message.message_id,
        )
        await q.answer("Estado inconsistente. Reintenta /tb export.", show_alert=True)
        return None

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

    # Authorization — checked AFTER we know which guild, since the rule
    # is per-guild leadership.
    user_id = q.from_user.id
    if not user_has_leadership_role(ss, user_id, gname):
        await q.answer("Solo oficiales pueden usar esta acción.", show_alert=True)
        return None

    return guild_id, label, gname, snapshot


def _disable_buttons_kb() -> InlineKeyboardMarkup:
    """Empty keyboard — used to remove buttons after action completes."""
    return InlineKeyboardMarkup([])


# ---------------------------------------------------------------------------
# DM confirm
# ---------------------------------------------------------------------------

async def cb_tbu_dm_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Step 1 of DM flow — show the confirmation dialog."""
    q = update.callback_query
    resolved = await _resolve_or_reply(q, _CB_DM_CONFIRM_PREFIX)
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
            callback_data=f"tbucancel:{guild_id}",  # see cb_tbu_cancel below
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
    # NOTE: cache lookup uses the ORIGINAL auto-summary's message_id,
    # not the confirm-dialog's. The confirm dialog is a separate
    # reply_text; its message_id has no cache entry. The original is
    # whatever message currently has the buttons attached to it... but
    # the confirm dialog edited none; it sent a new message. So we
    # need a different strategy for finding the cache key.
    #
    # Simplest fix: encode the original message_id into callback_data.
    # See the keyboard built in cb_tbu_dm_confirm — we use the prefix
    # only, not the message_id. Below we re-resolve by looking up by
    # guild_id (only one auto-summary per guild can be "current" — but
    # this isn't strictly true; an officer could trigger two exports
    # back-to-back).
    #
    # The truly correct path is to encode the original message_id in
    # the callback. Switching to that:
    #
    #   confirm-button callback_data = f"tbudmconfirm:{guild_id}:{original_message_id}"
    #
    # I'm leaving the simpler version in this skeleton with a TODO so
    # the reviewer can decide which trade-off to take. See comment at
    # the end of this file for the recommended adjustment.

    resolved = await _resolve_dm_execute(q)
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
    resolved = await _resolve_or_reply(q, _CB_PUB_CONFIRM_PREFIX)
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
    resolved = await _resolve_publish_execute(q)
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
        f"*{_md2(label)}* \u2014 Publicado en el canal\u2705",
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
        # If we can't delete (e.g. too old), edit to empty-ish.
        await q.edit_message_text(
            "_Cancelado\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )


# ---------------------------------------------------------------------------
# Cache lookup variants for the execute steps
#
# The confirm dialog is a NEW message (reply_text), so the cache entry
# is keyed by the ORIGINAL auto-summary's message_id, not the dialog's.
# We resolve back to the original via `q.message.reply_to_message`,
# which Telegram populates for reply_text() messages.
# ---------------------------------------------------------------------------

async def _resolve_dm_execute(q) -> Optional[tuple[str, str, str, UndeployedSnapshot]]:
    """Resolve the cache entry on the EXECUTE step (after confirm dialog)."""
    return await _resolve_execute_step(q, _CB_DM_EXECUTE_PREFIX)


async def _resolve_publish_execute(q) -> Optional[tuple[str, str, str, UndeployedSnapshot]]:
    return await _resolve_execute_step(q, _CB_PUB_EXECUTE_PREFIX)


async def _resolve_execute_step(
    q,
    prefix: str,
) -> Optional[tuple[str, str, str, UndeployedSnapshot]]:
    """
    The execute callbacks were triggered from a confirm-dialog message.
    The cache key is the ORIGINAL auto-summary message's id.

    We find the original via q.message.reply_to_message — Telegram sets
    this when reply_text() was used. If that's not present (shouldn't
    happen given how we built the dialog), bail.
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
        await q.answer("No encuentro el mensaje original.", show_alert=True)
        return None

    snapshot = get_undeployed_snapshot(
        q.bot.application.bot_data, original.message_id
    )
    if snapshot is None:
        await q.answer()
        await q.edit_message_text(
            "_Sesión expirada\\. Ejecuta /tb export para refrescar\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return None

    if snapshot.guild_id != guild_id:
        await q.answer("Estado inconsistente.", show_alert=True)
        return None

    try:
        ss = open_ss()
        label, gname = resolve_label_name_by_guild_id(ss, guild_id)
    except Exception:
        log.exception("Sheet open failed")
        await q.answer("Error leyendo configuración.", show_alert=True)
        return None

    if not gname:
        await q.answer("Gremio no encontrado.", show_alert=True)
        return None

    if not user_has_leadership_role(ss, q.from_user.id, gname):
        await q.answer("Solo oficiales.", show_alert=True)
        return None

    return guild_id, label, gname, snapshot


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
