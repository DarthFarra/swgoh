# swgoh/bot/commands/syncguild.py
import os
import asyncio
from telegram import Update
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes
from ..services.sheets import open_ss, already_synced_today, resolve_label_name_rote_by_id
from ..services.auth import user_authorized_guilds, user_has_role_in_guild
from ..keyboards.guild_select import make_keyboard_guilds
import requests
import logging

log = logging.getLogger(__name__)

# URL del Apps Script Web App (configurar en .env)
APPS_SCRIPT_URL = os.getenv('APPS_SCRIPT_WEBHOOK_URL')
APPS_SCRIPT_TIMEOUT = int(os.getenv('APPS_SCRIPT_TIMEOUT', '350'))  # 350 segundos (5m 50s)

async def cmd_syncguild(update: Update, context: ContextTypes.DEFAULT_TYPE):
 """Comando /syncguild - Muestra selector de gremios"""
 ss = open_ss()
 opts = user_authorized_guilds(ss, update.effective_user.id)  # [(label,gid)]
 
 if not opts:
     await update.message.reply_text("No tienes permisos para sincronizar (se requiere rol Lider u Oficial).")
     return
 
 kb = make_keyboard_guilds(opts, "syncguild")
 await update.message.reply_text("Elige el gremio a sincronizar:", reply_markup=kb)

async def cb_syncguild(update: Update, context: ContextTypes.DEFAULT_TYPE):
 """Callback cuando se selecciona un gremio del keyboard"""
 q = update.callback_query
 await q.answer()
 
 data = q.data or ""
 if not data.startswith("syncguild:"):
     return
 
 gid = data.split(":", 1)[1]
 ss = open_ss()

 # Verificar permisos
 if not user_has_role_in_guild(ss, q.from_user.id, gid):
     await q.edit_message_text("❌ No tienes permisos para sincronizar este gremio.")
     return

 # Verificar si ya se sincronizó hoy
 if already_synced_today(ss, gid):
     label, _, _ = resolve_label_name_rote_by_id(ss, gid)
     await q.edit_message_text(f"ℹ️ {label} ya se sincronizó hoy.")
     return

 label, _, _ = resolve_label_name_rote_by_id(ss, gid)
 
 # Verificar que Apps Script esté configurado
 if not APPS_SCRIPT_URL:
     await q.edit_message_text("❌ Error: APPS_SCRIPT_WEBHOOK_URL no configurado.")
     log.error("APPS_SCRIPT_WEBHOOK_URL no está definido en variables de entorno")
     return
 
 # Iniciar sincronización
 await q.edit_message_text(f"⏳ Sincronizando {label}…\n\n_Esto puede tardar varios minutos._")
 
 try:
     # Llamar a Apps Script de forma asíncrona
     result = await call_apps_script_sync(gid)
     
     # Parsear resultado
     if result.get('status') == 'success':
         summary = result.get('result', 'Completado')
         await q.edit_message_text(f"✅ Sincronización completada para {label}.\n\n`{summary}`", parse_mode='Markdown')
     else:
         error_msg = result.get('message', 'Error desconocido')
         await q.edit_message_text(f"❌ Error sincronizando {label}.\n\n{error_msg}")
         
 except asyncio.TimeoutError:
     # Timeout - pero el proceso puede seguir en Apps Script
     await q.edit_message_text(
         f"⏱️ {label}: La sincronización está tomando más tiempo del esperado.\n\n"
         f"El proceso continúa en segundo plano. Verifica los datos en unos minutos."
     )
     log.warning(f"Timeout esperando respuesta de Apps Script para guild {gid}")
     
 except requests.RequestException as e:
     await q.edit_message_text(f"❌ Error de conexión sincronizando {label}.\n\n{str(e)}")
     log.error(f"Error llamando a Apps Script: {e}")
     
 except Exception as e:
     await q.edit_message_text(f"❌ Error sincronizando {label}.\n\n{str(e)}")
     log.exception(f"Error inesperado en syncguild para {gid}")

async def call_apps_script_sync(guild_id: str) -> dict:
 """
 Llama al Apps Script para sincronizar un gremio específico.
 
 Args:
     guild_id: ID del gremio a sincronizar
     
 Returns:
     dict con la respuesta del Apps Script
     
 Raises:
     requests.RequestException: Si hay error de conexión
     asyncio.TimeoutError: Si excede el timeout
 """
 payload = {
     "action": "sync_guilds",
     "filterGuildIds": [guild_id]
 }
 
 log.info(f"Llamando a Apps Script para guild {guild_id}")
 
 # Ejecutar en thread pool para no bloquear el event loop
 loop = asyncio.get_event_loop()
 
 def _sync_request():
     response = requests.post(
         APPS_SCRIPT_URL,
         json=payload,
         timeout=APPS_SCRIPT_TIMEOUT
     )
     response.raise_for_status()
     return response.json()
 
 try:
     result = await asyncio.wait_for(
         loop.run_in_executor(None, _sync_request),
         timeout=APPS_SCRIPT_TIMEOUT
     )
     log.info(f"Apps Script completado para guild {guild_id}: {result.get('status')}")
     return result
     
 except asyncio.TimeoutError:
     log.warning(f"Timeout esperando Apps Script para guild {guild_id}")
     raise
 except requests.Timeout:
     log.warning(f"Request timeout para guild {guild_id}")
     raise asyncio.TimeoutError()
 except Exception as e:
     log.error(f"Error en call_apps_script_sync: {e}")
     raise

def get_handlers():
 """Retorna los handlers para registrar en el bot"""
 return [
     CommandHandler("syncguild", cmd_syncguild),
     CallbackQueryHandler(cb_syncguild, pattern=r"^syncguild:")
 ]
