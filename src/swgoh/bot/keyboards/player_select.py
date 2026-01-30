from telegram import InlineKeyboardMarkup, InlineKeyboardButton
from typing import List, Tuple

def make_keyboard_players(players: List[Tuple[str, str]], prefix: str) -> InlineKeyboardMarkup:
"""
Crea un teclado inline con botones para cada jugador.

Args:
    players: Lista de tuplas (player_name, player_name)
    prefix: Prefijo para el callback_data (ej: "playeropsplayer:123")

Returns:
    InlineKeyboardMarkup con los botones de jugadores
"""
buttons = [
    [InlineKeyboardButton(text=name, callback_data=f"{prefix}:{identifier}")]
    for name, identifier in players
]
return InlineKeyboardMarkup(buttons)
