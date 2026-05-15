# src/swgoh/bot/commands/tickets.py
"""
/tickets command — lets officers check daily ticket contributions.

Flow:
  1. /tickets -> guild selector (if multiple guilds)
  2. Guild chosen -> two buttons: "Today (live)" | "Yesterday (missed)"
  3a. Today -> fetches live data, shows delinquents + "Refresh" + "Send Reminder" buttons
      -> "Send Reminder" -> confirmation step (count) -> confirm/cancel
      -> confirmed -> sends messages, reports sent/failed
  3b. Yesterday -> reads snapshot + live lifetimeValue, shows who missed
"""
from __future__ import annotations

import logging
from datetime import date
from zoneinfo import ZoneInfo

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

from ..services.sheets import (
    open_ss,
    usuarios_guilds_for_user,
    resolve_label_name_rote_by_id,
    user_has_leadership_role,
    read_ticket_snapshot,
    get_chat_ids_for_members,
)
from ..services.tickets import (
    fetch_guild_tickets,
    render_tickets_today,
    render_tickets_yesterday,
    send_ticket_reminders,
    DAILY_TICKET_GOAL,
)
from ..keyboards.guild_select import make_keyboard_guilds

log = logging.getLogger(__name__)

MADRID_TZ = ZoneInfo("Europe/Madrid")

# ---------------------------------------------------------------------------
# Command entry point
# ---------------------------------------------------------------------------

async def cmd_tickets(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Entry point: /tickets"""
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
# Mode selector (Today / Yesterday)
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
    """Shared logic for initial load and refresh of Today view."""
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

    # Cache members so the reminder flow reuses them without a second API call
    context.user_data[f"tickets_members_{gid}"] = members
    context.user_data[f"tickets_gname_{gid}"] = gname

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
            )
        ])
    rows.append([InlineKeyboardButton("Volver", callback_data=f"ticketsback:{gid}")])

    kb = InlineKeyboardMarkup(rows)
    await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)


# ---------------------------------------------------------------------------
# Reminder -- confirmation step
# ---------------------------------------------------------------------------

async def cb_tickets_remind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Shows confirmation before sending reminders."""
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("ticketsremind:"):
        return

    gid = data.split(":", 1)[1]
    ss = open_ss()
    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)

    # Retrieve cached members; re-fetch if missing (e.g. bot restarted)
    members = context.user_data.get(f"tickets_members_{gid}")
    if members is None:
        try:
            members = fetch_guild_tickets(gid)
            context.user_data[f"tickets_members_{gid}"] = members
            context.user_data[f"tickets_gname_{gid}"] = gname
        except Exception as exc:
            log.error("Error re-fetching tickets for reminder confirmation: %s", exc)
            await q.edit_message_text(
                "No se pudieron obtener los datos\\. Ejecuta /tickets de nuevo\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return

    delinquents = [m for m in members if not m.completed_today]

    if not delinquents:
        await q.edit_message_text(
            "Todos han completado sus tickets\\. No hay recordatorios que enviar\\!",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    count = len(delinquents)
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
        f"alcanzado los {esc_goal} tickets de hoy\\.\n\n"
        f"Confirmas?",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN_V2,
    )


# ---------------------------------------------------------------------------
# Reminder -- confirmed, send messages
# ---------------------------------------------------------------------------

async def cb_tickets_remind_confirm(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Sends reminders to all delinquent members and reports results."""
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if not data.startswith("ticketsremindconfirm:"):
        return

    gid = data.split(":", 1)[1]
    ss = open_ss()
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

    delinquent_names = [m.player_name for m in delinquents]
    try:
        chat_ids = get_chat_ids_for_members(ss, gname, delinquent_names)
    except Exception as exc:
        log.error("Error reading chat_ids for guild %s: %s", gname, exc)
        await q.edit_message_text(
            f"Error leyendo los chat IDs del spreadsheet\\.\n\n"
            f"`{_escape_md(str(exc))}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    sent, failed = await send_ticket_reminders(
        bot=context.bot,
        members=delinquents,
        chat_ids=chat_ids,
    )

    # Clear cache -- data is stale after reminders are sent
    context.user_data.pop(f"tickets_members_{gid}", None)
    context.user_data.pop(f"tickets_gname_{gid}", None)

    esc_label  = _escape_md(label)
    esc_sent   = _escape_md(str(sent))
    esc_failed = _escape_md(str(failed))
    total      = _escape_md(str(sent + failed))

    lines = [
        f"*{esc_label}* \u2014 Resultado del recordatorio\n",
        f"Enviados: *{esc_sent}* / {total}",
    ]
    if failed:
        lines.append(
            f"Fallidos: *{esc_failed}* \\(sin chat\\_id registrado o bot bloqueado\\)"
        )

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Actualizar tickets", callback_data=f"ticketstoday:{gid}"),
            InlineKeyboardButton("Volver", callback_data=f"ticketsback:{gid}"),
        ]
    ])
    await q.edit_message_text(
        "\n".join(lines),
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
    ss = open_ss()
    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)

    snapshot_result = read_ticket_snapshot(ss, gname)

    if snapshot_result is None:
        await q.edit_message_text(
            f"*{_escape_md(label)}* \u2014 Aun no hay snapshot disponible\\.\n\n"
            f"El primer snapshot se tomara automaticamente 5 minutos antes de "
            f"la hora de reinicio configurada\\. Vuelve despues\\!",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    snapshot_date, snapshot_data = snapshot_result

    await q.edit_message_text(
        f"Obteniendo datos para *{_escape_md(label)}*\u2026",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    try:
        members_live = fetch_guild_tickets(gid)
    except Exception as exc:
        log.error("Error fetching tickets for guild %s: %s", gid, exc)
        await q.edit_message_text(
            f"Error obteniendo datos para *{_escape_md(label)}*\\.\n\n"
            f"`{_escape_md(str(exc))}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    text = render_tickets_yesterday(members_live, snapshot_data, label)
    text += f"\n\n_Snapshot: {_escape_md(snapshot_date)}_"

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Hoy", callback_data=f"ticketstoday:{gid}"),
            InlineKeyboardButton("Volver", callback_data=f"ticketsback:{gid}"),
        ]
    ])
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
    ss = open_ss()
    label, _, _ = resolve_label_name_rote_by_id(ss, gid)
    await _show_mode_selector(update, context, gid, label, via_callback=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
        CallbackQueryHandler(cb_tickets_guild,          pattern=r"^tickets:"),
        CallbackQueryHandler(cb_tickets_today,          pattern=r"^ticketstoday:"),
        CallbackQueryHandler(cb_tickets_yesterday,      pattern=r"^ticketsyest:"),
        CallbackQueryHandler(cb_tickets_remind,         pattern=r"^ticketsremind:"),
        CallbackQueryHandler(cb_tickets_remind_confirm, pattern=r"^ticketsremindconfirm:"),
        CallbackQueryHandler(cb_tickets_back,           pattern=r"^ticketsback:"),
    ]
