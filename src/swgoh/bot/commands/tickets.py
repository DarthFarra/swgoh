# src/swgoh/bot/commands/tickets.py
"""
/tickets command — lets officers check daily ticket contributions.

Flow:
  1. /tickets -> guild selector (if multiple guilds)
  2. Guild chosen -> two buttons: "Hoy (en vivo)" | "Ayer (faltas)"
  3a. Today -> fetches live data, shows delinquents +
               "Actualizar" | "Ayer" | "Enviar Recordatorio" | "Publicar en Avisos"
      -> "Enviar Recordatorio" -> confirm -> sends DMs, reports sent/failed
      -> "Publicar en Avisos"  -> confirm -> posts to channel, reports result
  3b. Yesterday -> reads snapshot + live lifetimeValue, shows who missed
"""
from __future__ import annotations

import logging
from zoneinfo import ZoneInfo

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import Forbidden, BadRequest
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

from ..services.sheets import (
    open_ss,
    usuarios_guilds_for_user,
    resolve_label_name_rote_by_id,
    user_has_leadership_role,
    read_ticket_snapshot,
    get_chat_ids_for_members,
    get_channel_id_for_guild,
    get_channel_config_for_guild,
    get_usernames_for_members,
)
from ..services.tickets import (
    fetch_guild_tickets,
    render_tickets_today,
    render_tickets_yesterday,
    render_tickets_today_channel,
    send_ticket_reminders,
    publish_tickets_to_channel,
    DAILY_TICKET_GOAL,
)
from ..keyboards.guild_select import make_keyboard_guilds

log = logging.getLogger(__name__)

MADRID_TZ = ZoneInfo("Europe/Madrid")


# ---------------------------------------------------------------------------
# Command entry point
# ---------------------------------------------------------------------------

async def cmd_tickets(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ss = open_ss()
    user_id = update.effective_user.id
    guilds = usuarios_guilds_for_user(ss, user_id)

    if not guilds:
        await update.message.reply_text("No estas registrado en ningun gremio.")
        return

    leadership_guilds = [
        (label, gid, gname)
        for label, gid, gname in guilds
        if user_has_leadership_role(ss, user_id, gname)
    ]

    if not leadership_guilds:
        await update.message.reply_text(
            "Necesitas rol de Oficial o Lider para usar /tickets."
        )
        return

    if len(leadership_guilds) > 1:
        opts = [(label, gid) for label, gid, _ in leadership_guilds]
        kb = make_keyboard_guilds(opts, "tickets")
        await update.message.reply_text(
            "Elige el gremio para revisar tickets:", reply_markup=kb
        )
        return

    label, gid, gname = leadership_guilds[0]
    await _show_mode_selector(update, context, gid, label, via_callback=False)


# ---------------------------------------------------------------------------
# Guild selected (only reached when multiple guilds exist)
# ---------------------------------------------------------------------------

async def cb_tickets_guild(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("tickets:"):
        return

    gid = data.split(":", 1)[1]
    ss = open_ss()

    if not user_has_leadership_role(ss, q.from_user.id, _guild_name_for_id(ss, gid)):
        await q.edit_message_text("No tienes permisos de Oficial/Lider en este gremio.")
        return

    label, _, _ = resolve_label_name_rote_by_id(ss, gid)
    await _show_mode_selector(update, context, gid, label, via_callback=True)


# ---------------------------------------------------------------------------
# Mode selector
# ---------------------------------------------------------------------------

async def _show_mode_selector(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    gid: str,
    label: str,
    via_callback: bool,
) -> None:
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Hoy (en vivo)", callback_data=f"ticketstoday:{gid}"),
            InlineKeyboardButton("Ayer (faltas)", callback_data=f"ticketsyest:{gid}"),
        ]
    ])
    text = f"Tickets *{_escape_md(label)}* \u2014 Elige un informe:"

    if via_callback:
        await update.callback_query.edit_message_text(
            text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2
        )
    else:
        await update.message.reply_text(
            text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2
        )


# ---------------------------------------------------------------------------
# Today (live)
# ---------------------------------------------------------------------------

async def cb_tickets_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("ticketstoday:"):
        return
    gid = data.split(":", 1)[1]
    await _fetch_and_show_today(q, gid, context)


async def _fetch_and_show_today(q, gid: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    ss = open_ss()
    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)

    await q.edit_message_text(
        f"Obteniendo datos de tickets para *{_escape_md(label)}*\u2026",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    try:
        members = fetch_guild_tickets(gid)
    except Exception as exc:
        log.error("Error fetching tickets for guild %s: %s", gid, exc)
        await q.edit_message_text(
            f"Error obteniendo tickets para *{_escape_md(label)}*\\.\n\n"
            f"`{_escape_md(str(exc))}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    delinquents = [m for m in members if not m.completed_today]

    # Cache for reminder and publish flows (avoids second API call)
    context.user_data[f"tickets_members_{gid}"] = members
    context.user_data[f"tickets_gname_{gid}"]   = gname

    text = render_tickets_today(members, label)

    rows = [
        [
            InlineKeyboardButton("Actualizar", callback_data=f"ticketstoday:{gid}"),
            InlineKeyboardButton("Ayer", callback_data=f"ticketsyest:{gid}"),
        ],
    ]
    if delinquents:
        rows.append([
            InlineKeyboardButton(
                f"Enviar Recordatorio ({len(delinquents)})",
                callback_data=f"ticketsremind:{gid}",
            ),
        ])
        rows.append([
            InlineKeyboardButton(
                f"Publicar en Avisos ({len(delinquents)})",
                callback_data=f"ticketspublish:{gid}",
            ),
        ])
    rows.append([InlineKeyboardButton("Volver", callback_data=f"ticketsback:{gid}")])

    await q.edit_message_text(
        text, reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.MARKDOWN_V2
    )


# ---------------------------------------------------------------------------
# Reminder — confirmation
# ---------------------------------------------------------------------------

async def cb_tickets_remind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("ticketsremind:"):
        return

    gid = data.split(":", 1)[1]
    ss  = open_ss()
    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)
    members = await _get_cached_or_fetch(q, context, gid, gname)
    if members is None:
        return

    delinquents = [m for m in members if not m.completed_today]
    if not delinquents:
        await q.edit_message_text(
            "Todos han completado sus tickets\\. No hay recordatorios que enviar\\!",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    count     = len(delinquents)
    esc_label = _escape_md(label)
    esc_count = _escape_md(str(count))
    esc_goal  = _escape_md(str(DAILY_TICKET_GOAL))

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"Si, enviar a {count} miembros",
                callback_data=f"ticketsremindconfirm:{gid}",
            ),
            InlineKeyboardButton("Cancelar", callback_data=f"ticketstoday:{gid}"),
        ]
    ])
    await q.edit_message_text(
        f"*{esc_label}* \u2014 Enviar recordatorio de tickets\n\n"
        f"Se enviara un mensaje a *{esc_count}* miembro\\(s\\) que aun no han "
        f"alcanzado los {esc_goal} tickets de hoy\\.\n\nConfirmas?",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN_V2,
    )


# ---------------------------------------------------------------------------
# Reminder — confirmed, send DMs
# ---------------------------------------------------------------------------

async def cb_tickets_remind_confirm(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("ticketsremindconfirm:"):
        return

    gid = data.split(":", 1)[1]
    ss  = open_ss()
    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)
    members = context.user_data.get(f"tickets_members_{gid}")
    if members is None:
        await q.edit_message_text(
            "Sesion expirada\\. Ejecuta /tickets de nuevo\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    delinquents = [m for m in members if not m.completed_today]
    if not delinquents:
        await q.edit_message_text(
            "Todos han completado sus tickets\\. Nada que enviar\\!",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    await q.edit_message_text(
        f"Enviando recordatorios para *{_escape_md(label)}*\u2026",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    try:
        chat_ids = get_chat_ids_for_members(ss, gname, [m.player_name for m in delinquents])
    except Exception as exc:
        log.error("Error reading chat_ids for guild %s: %s", gname, exc)
        await q.edit_message_text(
            f"Error leyendo los chat IDs\\.\n\n`{_escape_md(str(exc))}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    sent, failed = await send_ticket_reminders(
        bot=context.bot, members=delinquents, chat_ids=chat_ids
    )
    _clear_cache(context, gid)

    lines = [
        f"*{_escape_md(label)}* \u2014 Resultado del recordatorio\n",
        f"Enviados: *{_escape_md(str(sent))}* / {_escape_md(str(sent + failed))}",
    ]
    if failed:
        lines.append(
            f"Fallidos: *{_escape_md(str(failed))}* "
            f"\\(sin chat\\_id registrado o bot bloqueado\\)"
        )

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Actualizar tickets", callback_data=f"ticketstoday:{gid}"),
        InlineKeyboardButton("Volver", callback_data=f"ticketsback:{gid}"),
    ]])
    await q.edit_message_text(
        "\n".join(lines), reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2
    )


# ---------------------------------------------------------------------------
# Publish to channel — confirmation
# ---------------------------------------------------------------------------

async def cb_tickets_publish(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("ticketspublish:"):
        return

    gid = data.split(":", 1)[1]
    ss  = open_ss()
    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)

    # Validate channel is configured before showing confirm
    channel_id, thread_id = get_channel_config_for_guild(ss, gname)
    if not channel_id:
        await q.edit_message_text(
            f"*{_escape_md(label)}* \u2014 No hay canal de avisos configurado\\.\n\n"
            f"Agrega la columna `announcements\\_channel` en la hoja Guilds con el "
            f"ID del canal \\(ej\\: `\\-1002461429674`\\)\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    members = await _get_cached_or_fetch(q, context, gid, gname)
    if members is None:
        return

    delinquents = [m for m in members if not m.completed_today]
    if not delinquents:
        await q.edit_message_text(
            "Todos han completado sus tickets\\. No hay nada que publicar\\!",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    count     = len(delinquents)
    esc_label = _escape_md(label)
    esc_count = _escape_md(str(count))
    esc_chan   = _escape_md(channel_id)

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"Si, publicar ({count} miembros)",
                callback_data=f"ticketspublishconfirm:{gid}",
            ),
            InlineKeyboardButton("Cancelar", callback_data=f"ticketstoday:{gid}"),
        ]
    ])
    await q.edit_message_text(
        f"*{esc_label}* \u2014 Publicar en avisos\n\n"
        f"Se publicara el informe de *{esc_count}* miembro\\(s\\) con tickets "
        f"pendientes en el canal `{esc_chan}`\\.\n\nConfirmas?",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN_V2,
    )


# ---------------------------------------------------------------------------
# Publish to channel — confirmed
# ---------------------------------------------------------------------------

async def cb_tickets_publish_confirm(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("ticketspublishconfirm:"):
        return

    gid = data.split(":", 1)[1]
    ss  = open_ss()
    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)

    channel_id, thread_id = get_channel_config_for_guild(ss, gname)
    if not channel_id:
        await q.edit_message_text(
            "No hay canal configurado\\. Agrega `announcements\\_channel` en Guilds\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    members = context.user_data.get(f"tickets_members_{gid}")
    if members is None:
        await q.edit_message_text(
            "Sesion expirada\\. Ejecuta /tickets de nuevo\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    delinquents = [m for m in members if not m.completed_today]
    if not delinquents:
        await q.edit_message_text(
            "Todos han completado sus tickets\\. Nada que publicar\\!",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    await q.edit_message_text(
        f"Publicando en el canal para *{_escape_md(label)}*\u2026",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    # Fetch usernames only for delinquent members
    try:
        usernames = get_usernames_for_members(
            ss, gname, [m.player_name for m in delinquents]
        )
    except Exception as exc:
        log.error("Error reading usernames for guild %s: %s", gname, exc)
        usernames = {}  # degrade gracefully: fall back to plain names for all

    try:
        await publish_tickets_to_channel(
            bot=context.bot,
            channel_id=channel_id,
            members=delinquents,
            usernames=usernames,
            guild_label=label,
            thread_id=thread_id,
        )
    except Forbidden:
        log.error("Bot is not admin of channel %s", channel_id)
        await q.edit_message_text(
            f"El bot no tiene permisos de admin en el canal `{_escape_md(channel_id)}`\\.\n\n"
            f"Asegurate de que el bot es administrador del canal e intentalo de nuevo\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return
    except BadRequest as exc:
        log.error("Bad channel ID %s: %s", channel_id, exc)
        await q.edit_message_text(
            f"ID de canal invalido: `{_escape_md(channel_id)}`\\.\n\n"
            f"`{_escape_md(str(exc))}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return
    except Exception as exc:
        log.error("Unexpected error publishing to channel %s: %s", channel_id, exc)
        await q.edit_message_text(
            f"Error inesperado al publicar\\.\n\n`{_escape_md(str(exc))}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    _clear_cache(context, gid)

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Actualizar tickets", callback_data=f"ticketstoday:{gid}"),
        InlineKeyboardButton("Volver", callback_data=f"ticketsback:{gid}"),
    ]])
    await q.edit_message_text(
        f"*{_escape_md(label)}* \u2014 Publicado correctamente en el canal\\!",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN_V2,
    )


# ---------------------------------------------------------------------------
# Yesterday (missed)
# ---------------------------------------------------------------------------

async def cb_tickets_yesterday(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("ticketsyest:"):
        return

    gid = data.split(":", 1)[1]
    ss  = open_ss()
    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)
    snapshot_result  = read_ticket_snapshot(ss, gname)

    if snapshot_result is None:
        await q.edit_message_text(
            f"*{_escape_md(label)}* \u2014 Aun no hay snapshot disponible\\.\n\n"
            f"El primer snapshot se tomara automaticamente a la hora de reinicio\\. Vuelve despues\\!",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    snapshot_date, snapshot_d, snapshot_d1 = snapshot_result

    text  = render_tickets_yesterday(snapshot_d, snapshot_d1, label)
    text += f"\n\n_Snapshot: {_escape_md(snapshot_date)}_"

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Hoy", callback_data=f"ticketstoday:{gid}"),
        InlineKeyboardButton("Volver", callback_data=f"ticketsback:{gid}"),
    ]])
    await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)


# ---------------------------------------------------------------------------
# Back to mode selector
# ---------------------------------------------------------------------------

async def cb_tickets_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("ticketsback:"):
        return

    gid = data.split(":", 1)[1]
    ss  = open_ss()
    label, _, _ = resolve_label_name_rote_by_id(ss, gid)
    await _show_mode_selector(update, context, gid, label, via_callback=True)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

async def _get_cached_or_fetch(q, context, gid: str, gname: str):
    """
    Returns cached members list or re-fetches from API.
    Edits the message with an error and returns None on failure.
    """
    members = context.user_data.get(f"tickets_members_{gid}")
    if members is not None:
        return members
    try:
        members = fetch_guild_tickets(gid)
        context.user_data[f"tickets_members_{gid}"] = members
        context.user_data[f"tickets_gname_{gid}"]   = gname
        return members
    except Exception as exc:
        log.error("Error re-fetching tickets for guild %s: %s", gid, exc)
        await q.edit_message_text(
            "No se pudieron obtener los datos\\. Ejecuta /tickets de nuevo\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return None


def _clear_cache(context, gid: str) -> None:
    context.user_data.pop(f"tickets_members_{gid}", None)
    context.user_data.pop(f"tickets_gname_{gid}", None)


def _guild_name_for_id(ss, guild_id: str) -> str:
    _, gname, _ = resolve_label_name_rote_by_id(ss, guild_id)
    return gname


def _escape_md(text: str) -> str:
    """Escapes special characters for Telegram MarkdownV2."""
    special = r"\_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{ch}" if ch in special else ch for ch in str(text))


# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------

def get_handlers():
    return [
        CommandHandler("tickets", cmd_tickets),
        CallbackQueryHandler(cb_tickets_guild,           pattern=r"^tickets:"),
        CallbackQueryHandler(cb_tickets_today,           pattern=r"^ticketstoday:"),
        CallbackQueryHandler(cb_tickets_yesterday,       pattern=r"^ticketsyest:"),
        CallbackQueryHandler(cb_tickets_remind,          pattern=r"^ticketsremind:"),
        CallbackQueryHandler(cb_tickets_remind_confirm,  pattern=r"^ticketsremindconfirm:"),
        CallbackQueryHandler(cb_tickets_publish,         pattern=r"^ticketspublish:"),
        CallbackQueryHandler(cb_tickets_publish_confirm, pattern=r"^ticketspublishconfirm:"),
        CallbackQueryHandler(cb_tickets_back,            pattern=r"^ticketsback:"),
    ]
