import logging
from telegram.ext import ApplicationBuilder
from .config import BOT_TOKEN
from .commands import syncguild, misoperaciones, register, syncdata, operacionesjugador

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [main_bot] %(message)s")

def main():
    if not BOT_TOKEN:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN")
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Registrar handlers de cada comando
    for h in (
        syncguild.get_handlers() +
        misoperaciones.get_handlers() +
        register.get_handlers() +
        syncdata.get_handlers() +
        operacionesjugador.get_handlers():
    ):

        
        app.add_handler(h)

    logging.info("Bot iniciado (polling).")
    app.run_polling()

if __name__ == "__main__":
    main()
