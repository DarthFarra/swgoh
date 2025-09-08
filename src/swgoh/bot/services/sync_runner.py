import asyncio

async def run_sync_guilds_once(guild_id: str):
    from ...processing import sync_guilds as mod_sync_guilds
    # Ejecuta en thread para no bloquear el loop
    return await asyncio.to_thread(mod_sync_guilds.run, filter_guild_ids={guild_id})

async def run_sync_data():
    from ...processing import sync_data as mod_sync_data
    return await asyncio.to_thread(mod_sync_data.run)
