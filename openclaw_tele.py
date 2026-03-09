from telegram.ext import ApplicationBuilder, MessageHandler, filters
from telegram import Update
import MetaTrader5 as mt5

TOKEN = "8604080004:AAEd4aMKDwDYoHjzgX-NJdGwLXBL8Ez_7f8"

# connect MT5
mt5.initialize()

async def reply(update: Update, context):
    text = update.message.text.lower()

    if text == "ping":
        await update.message.reply_text("AI aktif")

    elif text == "gold":
        symbol = "XAUUSD"
        tick = mt5.symbol_info_tick(symbol)

        if tick:
            price = tick.ask
            await update.message.reply_text(f"Harga Gold sekarang: {price}")
        else:
            await update.message.reply_text("Tidak bisa ambil harga")

    else:
        await update.message.reply_text(f"AI menerima: {text}")

app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(MessageHandler(filters.TEXT, reply))

print("Bot running...")

app.run_polling()