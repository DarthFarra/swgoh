from typing import List, Tuple
from telegram import InlineKeyboardMarkup, InlineKeyboardButton

def make_keyboard_guilds(options: List[Tuple[str, str]], prefix: str) -> InlineKeyboardMarkup:
    """
    options: [(label, guild_id)]
    prefix:  "syncguild" | "myops" | "reg:gid"
    """
    buttons = [[InlineKeyboardButton(text=label, callback_data=f"{prefix}:{gid}")]
               for (label, gid) in options]
    return InlineKeyboardMarkup(buttons)
