# src/swgoh/bot/commands/tb_notifications.py
"""
Callback handlers for the auto-summary's inline buttons.

The buttons are built in discord_listener._build_undeployed_keyboard and
attached to the auto-summary message when it's sent. This module provides
the four callbacks that handle them:

  tbudm:{guild_id}                          — DM confirm step
  tbudmconfirm:{guild_id}:{original_msg_id} — DM execution
  tbupub:{guild_id}                         — Publish-to-channel confirm
  tbupubconfirm:{guild_id}:{original_msg_id}— Publish execution

Pattern follows commands/tickets.py — same two-step confirm-then-act
shape, same MarkdownV2 dialect for the result messages, same "edit the
message rather than sending a new one" UX.

Authorization:
  Every callback re-checks `user_has_leadership_role(ss, user_id, gname)`.
  Authorization can't be derived from "they ran the command" — there IS
  no command; the bot sent the auto-summary unprompted, and any chat
  member could tap a button. So we authorize at the action site.

Cache contract:
  The undeployed list is read from `tb_undeployed_cache`, keyed by the
  ORIGINAL auto-summary's message_id. The CONFIRM step reads that id
  directly from q.message.message_id (the buttons live on the original).
  The EXECUTE step receives the original's message_id THROUGH the
  callback_data, encoded by the confirm handler when it built the
  Yes-button.

  Why callback_data and not Telegram's reply_to_message:
    PTB v20's Message.reply_text() does NOT set reply_to_message_id by
    default — that's the v13 behavior. We could pass do_quote=True to
    restore it, but encoding the message_id explicitly in callback_data
    is robust against future PTB API changes and removes the implicit
    dependency on Telegram's "did the reply chain attach?" semantics.

If the cache entry is gone (>48h TTL, or bot restart), we show
"session expired" and stop. This is intentional and acceptable —
the message is stale enough that re-running /tb export is the right
action.

PTB API note:
  bot_data is accessed via `context.application.bot_data` — NOT via
  `q.bot.application.bot_data`. CallbackQuery has no public `.bot`
  attribute in PTB v20+. The `context` argument is the canonical
  access path for application state inside a callback.

Callback_data size budget:
  Telegram caps callback_data at 64 bytes. With the encoded form:
    "tbudmconfirm:<22-char-guild-id>:<up-to-19-digit-message-id>"
    = 12 + 1 + 22 + 1 + 19 = 55 bytes max.
  Comfortable headroom. We hard-check in _build_confirm_callback() so
  any future prefix change that pushes us over gets caught at build
  time rather than failing silently when Telegram rejects the keyboard.
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
_CB_CANCEL_PREFIX         = "tbucancel:"

# Telegram's hard limit on callback_data; we enforce in _build_confirm_callback.
_CALLBACK_DATA_MAX_BYTES = 64


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


def _build_confirm_callback(
    execute_prefix: str,
    guild_id: str,
    original_message_id: int,
) -> str:
    """
    Build a confirm-step Yes-button callback_data, embedding the original
    auto-summary's message_id so the execute handler can find the cache
    entry without depending on Telegram's reply-linkage semantics.

    Raises ValueError if the result exceeds Telegram's 64-byte limit.
    This is a build-time guard: if our prefix or id formats ever grow
    such that we'd silently fail at button-creation time, we want a loud
    error instead.
    """
    data = f"{execute_prefix}{guild_id}:{original_message_id}"
    if len(data.encode("utf-8")) > _CALLBACK_DATA_MAX_BYTES:
        raise ValueError(
            f"callback_data exceeds {_CALLBACK_DATA_MAX_BYTES}-byte limit: "
            f"{len(data)} bytes for {data!r}"
        )
    return data


def _parse_execute_callback(
    data: str,
    expected_prefix: str,
) -> Optional[tuple[str, int]]:
    """
    Parse an execute-step callback_data: "prefix:guild_id:message_id".

    Returns (guild_id, message_id) on success, None on any malformed input.
    Logs a warning when malformed because that means we built a button
    incorrectly somewhere, not a user issue.
    """
    if not data.startswith(expected_prefix):
        return None
    tail = data[len(expected_prefix):]
    parts = tail.rsplit(":", 1)  # rsplit so guild_id with hypothetical ':' survives
    if len(parts) != 2:
        log.warning("Malformed execute callback_data: %r", data)
        return None
    guild_id, msg_id_str = parts
    if not guild_id:
        log.warning("Empty guild_id in callback_data: %r", data)
        return None
    try:
        msg_id = int(msg_id_str)
    except ValueError:
        log.warning("Non-integer message_id in callback_data: %r", data)
        return None
    return guild_id, msg_id


# ---------------------------------------------------------------------------
# Resolver helpers
#
# Both the CONFIRM step (button on the auto-summary message) and the
# EXECUTE step (button on the confirm dialog) need to:
#   1. Validate the callback_data prefix and extract guild_id.
#   2. Look up the cached undeployed snapshot by the ORIGINAL auto-
#      summary's message_id.
#   3. Resolve the guild label/name from the sheet.
#   4. Authorize the user as a guild officer.
#
# They differ in WHERE the original message_id comes from:
#   - CONFIRM step: q.message.message_id IS the original.
#   - EXECUTE step: the original's message_id is encoded in callback_data
#     (see _build_confirm_callback above).
# ---------------------------------------------------------------------------

async def _resolve_confirm_step(
    q,
    context: ContextTypes.DEFAULT_TYPE,
    callback_data_prefix: str,
) -> Optional[tuple[str, str, str, UndeployedSnapshot]]:
    """
    CONFIRM-step resolver. Cache key is q.message.message_id (the
    auto-summary message itself).

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
            log.debug(
                "Could not remove buttons from expired auto-summary.",
                exc_info=True,
            )
        await q.message.reply_text(
            "_Sesión expirada \\(este mensaje tiene más de 48h o el bot "
            "se reinició\\)\\. Ejecuta /tb export en Discord para refrescar\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return None

    if snapshot.guild_id != guild_id:
        log.error(
            "Cache guild_id mismatch (confirm): button=%s cache=%s message_id=%d",
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
    EXECUTE-step resolver. Cache key is the original auto-summary's
    message_id, extracted from callback_data (NOT from reply_to_message,
    which is unreliable in PTB v20 group/topic chats).
    """
    parsed = _parse_execute_callback(q.data or "", prefix)
    if parsed is None:
        await q.answer("Datos inválidos.", show_alert=True)
        return None
    guild_id, original_message_id = parsed

    snapshot = get_undeployed_snapshot(
        context.application.bot_data, original_message_id
    )
    if snapshot is None:
        await q.answer()
        try:
            await q.edit_message_text(
                "_Sesión expirada\\. Ejecuta /tb export para refrescar\\._",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        except Exception:
            log.debug("Could not edit expired confirm dialog.", exc_info=True)
        return None

    if snapshot.guild_id != guild_id:
        log.error(
            "Cache guild_id mismatch (execute): button=%s cache=%s message_id=%d",
            guild_id, snapshot.guild_id, original_message_id,
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

    Factored out because both resolvers need identical behavior here.
    """
    try:
        ss = open_ss()
        label, gname = resolve_label_name_by_guild_id(ss, guild_id)
    except Exception:
        log.exception("Sheet open failed during TB undeployed callback.")
        await q.answer("Error leyendo configuración.", show_alert=True)
        return None

    if not gname:
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
    """
    Step 1 of DM flow — show the confirmation dialog.

    The Yes-button's callback_data embeds the auto-summary's message_id
    so the execute step can find the cache entry without depending on
    Telegram's reply-linkage semantics.
    """
    q = update.callback_query
    resolved = await _resolve_confirm_step(q, context, _CB_DM_CONFIRM_PREFIX)
    if resolved is None:
        return
    guild_id, label, _gname, snapshot = resolved
    await q.answer()

    n = len(snapshot.members)
    if n == 0:
        await q.message.reply_text(
            "Todos han desplegado\\. Nada que enviar\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    # q.message.message_id IS the original auto-summary at confirm time.
    yes_callback = _build_confirm_callback(
        _CB_DM_EXECUTE_PREFIX, guild_id, q.message.message_id
    )

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            f"Sí, enviar a {n}",
            callback_data=yes_callback,
        ),
        InlineKeyboardButton(
            "Cancelar",
            callback_data=f"{_CB_CANCEL_PREFIX}{guild_id}",
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
    resolved = await _resolve_confirm_step(q, context, _CB_PUB_CONFIRM_PREFIX)
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

    yes_callback = _build_confirm_callback(
        _CB_PUB_EXECUTE_PREFIX, guild_id, q.message.message_id
    )

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            f"Sí, publicar ({n} miembros)",
            callback_data=yes_callback,
        ),
        InlineKeyboardButton(
            "Cancelar",
            callback_data=f"{_CB_CANCEL_PREFIX}{guild_id}",
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
