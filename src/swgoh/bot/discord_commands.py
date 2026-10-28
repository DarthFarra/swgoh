from __future__ import annotations

import logging
from typing import Optional

import discord
from discord import app_commands

from .jobs.send_assignments_daily import (
    AssignIndex,
    HEADERS_ASSIGN,
    _find_col,
    _gv_by_idx,
    _hmap,
    obtener_fase_actual,
)
from .services.sheets_io import read_values_cached
from .services.sheets import (
    discord_usuario_already_registered,
    list_phases_in_rote,
    list_players_for_guild,
    map_guild_name_to_label_id_rote,
    open_ss,
    players_find_by_alias,
    players_find_by_ally,
    render_ops_for_alias_phase_grouped,
    upsert_discord_usuario,
    usuarios_guilds_for_discord_user,
    usuarios_registrations_for_discord_user,
    discord_user_has_leadership_role,
)
from .. import config as cfg

log = logging.getLogger(__name__)


def _phase_key(value: str):
    try:
        return 0, int(value)
    except (TypeError, ValueError):
        return 1, str(value).lower()


def _registrations_phases(ss, registrations) -> list[str]:
    phases = set()
    for _label, _gid, _gname, rote_sheet, _alias in registrations:
        phases.update(list_phases_in_rote(ss, rote_sheet))
    return sorted(phases, key=_phase_key)


def _operations_for_players(ss, registrations, phase: str) -> list[str]:
    messages = []
    for label, _gid, gname, rote_sheet, alias in registrations:
        body = render_ops_for_alias_phase_grouped(ss, rote_sheet, alias, phase)
        if "No tienes asignaciones" in body:
            continue
        messages.append(
            f"Asignaciones de **{alias}** — **{label or gname}** "
            f"(Fase {phase})\n\n{body}"
        )
    return messages


async def _send_parts(
    interaction: discord.Interaction,
    messages: list[str],
    *,
    view: Optional[discord.ui.View] = None,
) -> None:
    if not messages:
        await interaction.edit_original_response(
            content="No tienes asignaciones en esta fase.", view=view
        )
        return

    parts = _split_message(messages[0])
    await interaction.edit_original_response(content=parts[0], view=view)
    for message in messages[1:]:
        parts.extend(_split_message(message))
    for part in parts[1:]:
        await interaction.followup.send(part, ephemeral=True)


def _split_message(message: str, limit: int = 2000) -> list[str]:
    if len(message) <= limit:
        return [message]
    parts = []
    remaining = message
    while remaining:
        cut = remaining.rfind("\n", 0, limit + 1)
        if cut <= 0:
            cut = limit
        parts.append(remaining[:cut])
        remaining = remaining[cut:].lstrip("\n")
    return parts


class RegisterMethodView(discord.ui.View):
    def __init__(self, guild_name: str, label: str):
        super().__init__(timeout=300)
        self.guild_name = guild_name
        self.label = label

    @discord.ui.button(label="Registrar por alias", style=discord.ButtonStyle.primary)
    async def alias(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            RegistrationModal(self.guild_name, self.label, "alias")
        )

    @discord.ui.button(
        label="Registrar por código de aliado", style=discord.ButtonStyle.secondary
    )
    async def ally(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            RegistrationModal(self.guild_name, self.label, "ally")
        )


class GuildSelect(discord.ui.Select):
    def __init__(self, guilds: list[tuple[str, str, str]]):
        options = [
            discord.SelectOption(label=(label or gname)[:100], value=gname)
            for gname, (label, _gid, _rote) in guilds[:25]
        ]
        super().__init__(placeholder="Elige tu gremio", options=options)
        self.guild_map = {gname: (label or gname) for gname, (label, _, _) in guilds}

    async def callback(self, interaction: discord.Interaction) -> None:
        gname = self.values[0]
        await interaction.response.edit_message(
            content=f"Gremio: **{self.guild_map[gname]}**\n\n¿Cómo quieres registrarte?",
            view=RegisterMethodView(gname, self.guild_map[gname]),
        )


class GuildSelectView(discord.ui.View):
    def __init__(self, guilds: list[tuple[str, str, str]]):
        super().__init__(timeout=300)
        self.add_item(GuildSelect(guilds))


class RegistrationModal(discord.ui.Modal):
    def __init__(self, guild_name: str, label: str, method: str):
        super().__init__(title=f"Registrar en {label}"[:45])
        self.guild_name = guild_name
        self.label = label
        self.method = method
        prompt = (
            "Alias de jugador"
            if method == "alias"
            else "Código de aliado"
        )
        self.value = discord.ui.TextInput(
            label=prompt,
            placeholder="Escríbelo exactamente como aparece en el juego",
            required=True,
            max_length=100,
        )
        self.add_item(self.value)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        ss = open_ss()
        if discord_usuario_already_registered(
            ss, interaction.user.id, self.guild_name
        ):
            await interaction.followup.send(
                f"Ya estás registrado para **{self.label}**.", ephemeral=True
            )
            return

        value = str(self.value).strip()
        found = (
            players_find_by_ally(ss, self.guild_name, value)
            if self.method == "ally"
            else players_find_by_alias(ss, self.guild_name, value)
        )
        if not found:
            kind = "alias" if self.method == "alias" else "código de aliado"
            await interaction.followup.send(
                f"❌ El {kind} no se pudo encontrar en **{self.label}**.",
                ephemeral=True,
            )
            return

        upsert_discord_usuario(
            ss,
            found,
            interaction.user.id,
        )
        await interaction.followup.send(
            f"✅ Registrado en **{self.label}** como **{found.get('alias', '')}** "
            f"(allycode: {found.get('allycode', '')}).",
            ephemeral=True,
        )


class PhaseSelect(discord.ui.Select):
    def __init__(self, ss, registrations):
        phases = _registrations_phases(ss, registrations)
        options = [
            discord.SelectOption(label=f"Fase {phase}", value=phase)
            for phase in phases[:25]
        ]
        super().__init__(placeholder="Elige la fase", options=options)
        self.ss = ss
        self.registrations = registrations

    async def callback(self, interaction: discord.Interaction) -> None:
        phase = self.values[0]
        messages = _operations_for_players(
            self.ss, self.registrations, phase
        )
        await interaction.response.defer(ephemeral=True)
        await _send_parts(interaction, messages)


class PhaseSelectView(discord.ui.View):
    def __init__(self, ss, registrations):
        super().__init__(timeout=300)
        self.add_item(PhaseSelect(ss, registrations))


class OfficerGuildSelect(discord.ui.Select):
    def __init__(self, guilds):
        super().__init__(
            placeholder="Elige el gremio",
            options=[
                discord.SelectOption(label=(label or gname)[:100], value=gid)
                for label, gid, gname in guilds[:25]
            ],
        )
        self.guilds = {gid: (label, gname) for label, gid, gname in guilds}

    async def callback(self, interaction: discord.Interaction) -> None:
        label, gname = self.guilds[self.values[0]]
        ss = open_ss()
        if not discord_user_has_leadership_role(
            ss, interaction.user.id, gname
        ):
            await interaction.response.edit_message(
                content="❌ No tienes permisos de Oficial o Líder en este gremio.",
                view=None,
            )
            return
        await interaction.response.edit_message(
            content=f"Elige el jugador de **{label}**:",
            view=OfficerPlayerView(ss, interaction.user.id, label, gname),
        )


class OfficerGuildView(discord.ui.View):
    def __init__(self, guilds):
        super().__init__(timeout=300)
        self.add_item(OfficerGuildSelect(guilds))


class OfficerPlayerSelect(discord.ui.Select):
    def __init__(self, ss, user_id: int, label: str, gname: str):
        players = list_players_for_guild(ss, gname)
        super().__init__(
            placeholder="Elige el jugador",
            options=[
                discord.SelectOption(label=name[:100], value=name)
                for name, _ in players[:25]
            ],
        )
        self.ss = ss
        self.user_id = user_id
        self.label = label
        self.gname = gname
        self.rote = map_guild_name_to_label_id_rote(ss)[gname][2]

    async def callback(self, interaction: discord.Interaction) -> None:
        if not discord_user_has_leadership_role(
            self.ss, interaction.user.id, self.gname
        ):
            await interaction.response.edit_message(
                content="❌ No tienes permisos de Oficial o Líder en este gremio.",
                view=None,
            )
            return
        await interaction.response.edit_message(
            content=f"Elige la fase para **{self.values[0]}** en **{self.label}**:",
            view=OfficerPhaseView(
                self.ss, interaction.user.id, self.label, self.gname,
                self.rote, self.values[0],
            ),
        )


class OfficerPlayerView(discord.ui.View):
    def __init__(self, ss, user_id: int, label: str, gname: str):
        super().__init__(timeout=300)
        self.add_item(OfficerPlayerSelect(ss, user_id, label, gname))


class OfficerPhaseSelect(discord.ui.Select):
    def __init__(self, ss, user_id, label, gname, rote, player):
        phases = list_phases_in_rote(ss, rote)
        super().__init__(
            placeholder="Elige la fase",
            options=[
                discord.SelectOption(label=f"Fase {phase}", value=phase)
                for phase in phases[:25]
            ],
        )
        self.ss = ss
        self.user_id = user_id
        self.label = label
        self.gname = gname
        self.rote = rote
        self.player = player

    async def callback(self, interaction: discord.Interaction) -> None:
        if not discord_user_has_leadership_role(
            self.ss, interaction.user.id, self.gname
        ):
            await interaction.response.edit_message(
                content="❌ No tienes permisos de Oficial o Líder en este gremio.",
                view=None,
            )
            return
        phase = self.values[0]
        body = render_ops_for_alias_phase_grouped(
            self.ss, self.rote, self.player, phase
        )
        if not body or "No tienes asignaciones" in body:
            body = f"El jugador no tiene asignaciones para la Fase {phase}."
        await interaction.response.edit_message(
            content=(
                f"Asignaciones de **{self.player}** — **{self.label}** "
                f"(Fase {phase})\n\n{body}"
            ),
            view=None,
        )


class OfficerPhaseView(discord.ui.View):
    def __init__(self, ss, user_id, label, gname, rote, player):
        super().__init__(timeout=300)
        self.add_item(
            OfficerPhaseSelect(ss, user_id, label, gname, rote, player)
        )


class DiscordCommands:
    def __init__(self, client: discord.Client):
        self.client = client

    async def register(self, interaction: discord.Interaction) -> None:
        ss = open_ss()
        guilds = list(map_guild_name_to_label_id_rote(ss).items())
        if not guilds:
            await interaction.response.send_message(
                "No hay gremios configurados.", ephemeral=True
            )
            return
        await interaction.response.send_message(
            "Elige tu gremio:", view=GuildSelectView(guilds), ephemeral=True
        )

    async def misoperaciones(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        ss = open_ss()
        registrations = usuarios_registrations_for_discord_user(
            ss, interaction.user.id
        )
        if not registrations:
            await interaction.edit_original_response(
                content="No estás registrado en ningún gremio."
            )
            return
        phases = _registrations_phases(ss, registrations)
        if not phases:
            await interaction.edit_original_response(
                content="❌ No hay fases en tus hojas ROTE."
            )
            return
        await interaction.edit_original_response(
            content="Elige la fase:",
            view=PhaseSelectView(ss, registrations),
        )

    async def operacionesjugador(self, interaction: discord.Interaction) -> None:
        ss = open_ss()
        guilds = [
            item for item in usuarios_guilds_for_discord_user(
                ss, interaction.user.id
            )
            if discord_user_has_leadership_role(ss, interaction.user.id, item[2])
        ]
        if not guilds:
            await interaction.response.send_message(
                "No tienes permisos de Oficial o Líder en ningún gremio.",
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            "Elige el gremio para ver operaciones de jugadores:",
            view=OfficerGuildView(guilds),
            ephemeral=True,
        )


async def send_discord_assignments(client: discord.Client) -> None:
    """Send the daily assignment message to every registered Discord user."""
    fase = obtener_fase_actual()
    if not fase:
        return
    ss = open_ss()
    headers, rows = read_values_cached(ss, cfg.SHEET_USERS)
    hmap = _hmap(headers)
    indices = {
        "guild_name": _find_col(hmap, ["guild_name", "Guild Name", "gremio"]),
        "discord_id": _find_col(hmap, ["discord_id"]),
        "alias": _find_col(hmap, ["alias"]),
    }
    if min(indices.values()) < 0:
        return

    guild_headers, guild_rows = read_values_cached(ss, cfg.SHEET_GUILDS)
    guild_map = _hmap(guild_headers)
    name_index = _find_col(guild_map, ["Guild Name", "guild_name", "gremio"])
    rote_index = _find_col(guild_map, ["ROTE"])
    rote_by_guild = {
        _gv_by_idx(row, name_index): _gv_by_idx(row, rote_index)
        for row in guild_rows
        if _gv_by_idx(row, name_index) and _gv_by_idx(row, rote_index)
    }
    by_guild = {}
    for row in rows:
        gname = _gv_by_idx(row, indices["guild_name"])
        did = _gv_by_idx(row, indices["discord_id"])
        alias = _gv_by_idx(row, indices["alias"])
        if gname and did and alias:
            by_guild.setdefault(gname, []).append((did, alias))

    for gname, users in by_guild.items():
        sheet = rote_by_guild.get(gname) or cfg.SHEET_ASSIGNMENTS
        assignment_headers, assignment_rows = read_values_cached(ss, sheet)
        assignment_map = _hmap(assignment_headers)
        idxs = {
            key: _find_col(assignment_map, values)
            for key, values in HEADERS_ASSIGN.items()
        }
        if min(idxs.values()) < 0:
            continue
        index = AssignIndex(sheet, idxs, assignment_rows, fase)
        for discord_id, alias in users:
            message = index.build_message_for(gname, discord_id, alias)
            if not message:
                continue
            try:
                user = await client.fetch_user(int(discord_id))
                await user.send(message.replace("*", "**"))
            except Exception:
                log.exception(
                    "Could not send scheduled assignments to Discord user %s",
                    discord_id,
                )


async def job_send_discord_assignments(context) -> None:
    entry = context.application.bot_data.get("__discord_listener__")
    if not entry:
        return
    client, _task = entry
    await send_discord_assignments(client)


def install_commands(client: discord.Client) -> None:
    commands = DiscordCommands(client)
    client.tree.add_command(
        app_commands.Command(
            name="register",
            description="Registra tu jugador",
            callback=commands.register,
        )
    )
    client.tree.add_command(
        app_commands.Command(
            name="misoperaciones",
            description="Consulta tus asignaciones ROTE",
            callback=commands.misoperaciones,
        )
    )
    client.tree.add_command(
        app_commands.Command(
            name="operacionesjugador",
            description="Consulta las asignaciones de un jugador",
            callback=commands.operacionesjugador,
        )
    )
