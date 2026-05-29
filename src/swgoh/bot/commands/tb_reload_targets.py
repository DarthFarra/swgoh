# src/swgoh/bot/commands/tb_reload_targets.py
"""
Admin command to re-read the TBTargets sheet without restarting the bot.

Authorization:
  Only officers (user_has_leadership_role) for any guild can trigger
  a reload. This is a guild-internal admin tool — non-officers don't
  need access, and restricting it avoids surprise sheet reads from
  random chat members.

  The leadership check is "any guild" rather than "for a specific guild"
  because targets are loaded for all guilds at once — there's no
  per-guild reload. So requiring leadership in at least one guild is
  the right gate.

Behavior:
  Reads the TBTargets sheet, replaces the in-memory cache, and reports
  the count of loaded entries. If the sheet is unreachable or empty,
  the load fails soft and the message reports an empty load — the
  formatter will then skip estimation lines silently until the sheet
  is fixed.

Implementation notes:
  - The sheet read happens inline in the handler (blocking). It's
    ~100ms typical, fine for a manually-triggered command.
  - On error, we still report success-by-empty rather than crashing.
    The previous cache value is left untouched.
"""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes
from telegram.constants import ParseMode

from ..services import tb_targets_cache
from ..services.auth import user_authorized_guilds
from ..services.sheets import open_ss

log = logging.getLogger(__name__)


_MD2_SPECIAL = r"_*[]()~`>#+-=|{}.!\\"


def _md2(text) -> str:
    """MarkdownV2 escaper — used for the success/failure reply only."""
    if not text:
        return ""
    return "".join(
        f"\\{ch}" if ch in _MD2_SPECIAL else ch
        for ch in str(text)
    )


async def cmd_tb_reload_targets(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    /tb_reload_targets — refresh the TBTargets cache from the sheet.

    Officer-only. Reports counts loaded; nothing else.
    """
    user = update.effective_user
    if user is None:
        return

    # Authorize: must be a leadership role in at least one guild.
    # user_authorized_guilds returns [(label, guild_id), ...] for every
    # guild where the user has Lider or Oficial role. An empty list
    # means "no leadership anywhere" — we deny.
    try:
        ss = open_ss()
        authorized = user_authorized_guilds(ss, user.id)
        if not authorized:
            await update.message.reply_text(
                "Solo oficiales pueden usar este comando\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return
    except Exception:
        log.exception("Auth check failed during /tb_reload_targets")
        await update.message.reply_text(
            "Error verificando permisos\\. Intenta de nuevo\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    # Re-load. tb_targets_cache.load_into_bot_data swallows errors and
    # returns an empty TBTargets — so we don't need our own try/except
    # here. We DO want to report the result honestly: success with N
    # entries, or success-by-empty if the sheet was unreachable.
    try:
        targets = tb_targets_cache.load_into_bot_data(context.application.bot_data)
    except Exception as exc:
        # Defensive: should not happen because load is fail-soft, but
        # cope if a future refactor changes that contract.
        log.exception("Unexpected error during TBTargets reload")
        await update.message.reply_text(
            f"Error inesperado al recargar\\.\n\n`{_md2(str(exc))}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    count = len(targets.targets)
    if count == 0:
        await update.message.reply_text(
            "TBTargets recargado\\. *0 entradas* \\(hoja vacía o no encontrada\\)\\.\n\n"
            "_Revisa los logs si esto no era lo esperado\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    else:
        await update.message.reply_text(
            f"TBTargets recargado\\. *{_md2(str(count))}* entradas cargadas\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )

    log.info(
        "TBTargets reloaded by user_id=%d: %d entries.",
        user.id, count,
    )


def get_handlers():
    return [
        CommandHandler("tb_reload_targets", cmd_tb_reload_targets),
    ]
