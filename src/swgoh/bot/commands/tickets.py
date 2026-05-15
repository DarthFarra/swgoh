# src/swgoh/bot/commands/tickets.py
"""
/tickets command — lets officers check daily ticket contributions.

Flow:
  1. /tickets → guild selector (if multiple guilds)
  2. Guild chosen → two buttons: "Today (live)" | "Yesterday (missed)"
  3a. Today → fetches live data, shows delinquents + "Refresh" button
  3b. Yesterday → reads snapshot + live lifetimeValue, shows who missed
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
)
from ..services.tickets import (
    fetch_guild_tickets,
    render_tickets_today,
    render_tickets_yesterday,
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
        await update.message.reply_text("❌ You are not registered in any guild.")
        return

    # Filter to guilds where the user has leadership role
    leadership_guilds = [
        (label, gid, gname)
        for label, gid, gname in guilds
        if user_has_leadership_role(ss, user_id, gname)
    ]

    if not leadership_guilds:
        await update.message.reply_text(
            "❌ You need Officer or Leader role to use /tickets."
        )
        return

    if len(leadership_guilds) > 1:
        opts = [(label, gid) for label, gid, _ in leadership_guilds]
        kb = make_keyboard_guilds(opts, "tickets")
        await update.message.reply_text(
            "Choose a guild to check tickets:", reply_markup=kb
        )
        return

    # Single guild — skip guild selector
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
        await q.edit_message_text("❌ You don't have Officer/Leader permissions for this guild.")
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
            InlineKeyboardButton("📊 Today (live)", callback_data=f"ticketstoday:{gid}"),
            InlineKeyboardButton("📅 Yesterday (missed)", callback_data=f"ticketsyest:{gid}"),
        ]
    ])
    text = f"🎫 *{_escape_md(label)}* — Ticket checker\n\nChoose a report:"

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
    await _fetch_and_show_today(q, gid)


async def _fetch_and_show_today(q, gid: str) -> None:
    """Shared logic for initial load and refresh of Today view."""
    ss = open_ss()
    label, gname, _ = resolve_label_name_rote_by_id(ss, gid)

    await q.edit_message_text(
        f"⏳ Fetching live ticket data for *{_escape_md(label)}*…",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    try:
        members = fetch_guild_tickets(gid)
    except Exception as exc:
        log.error("Error fetching tickets for guild %s: %s", gid, exc)
        await q.edit_message_text(
            f"❌ Failed to fetch ticket data for *{_escape_md(label)}*\\.\n\n"
            f"`{_escape_md(str(exc))}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    text = render_tickets_today(members, label)

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔄 Refresh", callback_data=f"ticketstoday:{gid}"),
            InlineKeyboardButton("📅 Yesterday", callback_data=f"ticketsyest:{gid}"),
        ],
        [InlineKeyboardButton("« Back", callback_data=f"ticketsback:{gid}")],
    ])
    await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)


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

    # Check snapshot existence first — cheap, no API call
    snapshot_result = read_ticket_snapshot(ss, gname)

    if snapshot_result is None:
        today_str = date.today().isoformat()
        await q.edit_message_text(
            f"ℹ️ *{_escape_md(label)}* — No snapshot available yet\\.\n\n"
            f"The first snapshot will be taken automatically 5 minutes before "
            f"the configured reset time\\. Come back after that\\!",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    snapshot_date, snapshot_data = snapshot_result

    await q.edit_message_text(
        f"⏳ Fetching live data for *{_escape_md(label)}*…",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    try:
        members_live = fetch_guild_tickets(gid)
    except Exception as exc:
        log.error("Error fetching tickets for guild %s: %s", gid, exc)
        await q.edit_message_text(
            f"❌ Failed to fetch live data for *{_escape_md(label)}*\\.\n\n"
            f"`{_escape_md(str(exc))}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    text = render_tickets_yesterday(members_live, snapshot_data, label)

    # Append snapshot date as footer
    text += f"\n\n_Snapshot taken: {_escape_md(snapshot_date)}_"

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Today", callback_data=f"ticketstoday:{gid}"),
            InlineKeyboardButton("« Back", callback_data=f"ticketsback:{gid}"),
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
    """Returns guild_name for a guild_id, or empty string if not found."""
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
        CallbackQueryHandler(cb_tickets_guild,     pattern=r"^tickets:"),
        CallbackQueryHandler(cb_tickets_today,     pattern=r"^ticketstoday:"),
        CallbackQueryHandler(cb_tickets_yesterday, pattern=r"^ticketsyest:"),
        CallbackQueryHandler(cb_tickets_back,      pattern=r"^ticketsback:"),
    ]
