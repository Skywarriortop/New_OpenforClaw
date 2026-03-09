import MetaTrader5 as mt5
import pandas as pd
import csv
from datetime import datetime

from telegram.ext import ApplicationBuilder, MessageHandler, filters
from telegram import Update

TOKEN = "TOKEN_KAMU"

# ======================
# CONNECT MT5
# ======================

mt5.initialize()

# ======================
# TRADE LOGGER
# ======================

def log_trade(symbol, trade_type, entry, reason):

    with open("trade_log.csv", "a", newline="") as f:

        writer = csv.writer(f)

        writer.writerow([
            datetime.now(),
            symbol,
            trade_type,
            entry,
            reason
        ])

# ======================
# GET PRICE
# ======================

def get_price(symbol):

    tick = mt5.symbol_info_tick(symbol)

    if tick:
        return tick.ask

    return None


# ======================
# SIMPLE AI ANALYZER
# ======================

def analyze_market(symbol):

    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M5, 0, 50)

    df = pd.DataFrame(rates)

    trend = "bullish" if df["close"].iloc[-1] > df["close"].mean() else "bearish"

    rsi = 50

    return {
        "trend": trend,
        "rsi": rsi
    }


# ======================
# TRADE VALIDATOR
# ======================

def validate_trade(symbol, direction):

    analysis = analyze_market(symbol)

    trend = analysis["trend"]
    rsi = analysis["rsi"]

    if direction == "buy":

        if trend != "bullish":
            return False, "Trend tidak bullish"

        if rsi > 70:
            return False, "RSI overbought"

        return True, f"Trend bullish | RSI {rsi}"

    if direction == "sell":

        if trend != "bearish":
            return False, "Trend tidak bearish"

        if rsi < 30:
            return False, "RSI oversold"

        return True, f"Trend bearish | RSI {rsi}"


# ======================
# EXECUTE TRADE
# ======================

def open_trade(symbol, direction):

    price = get_price(symbol)

    lot = 0.01

    if direction == "buy":
        order_type = mt5.ORDER_TYPE_BUY
    else:
        order_type = mt5.ORDER_TYPE_SELL

    request = {

        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": lot,
        "type": order_type,
        "price": price,
        "deviation": 20,
        "magic": 123456,
        "comment": "OpenClaw AI",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,

    }

    result = mt5.order_send(request)

    return result


# ======================
# TRADE REPORT
# ======================

def analyze_trades():

    try:

        df = pd.read_csv("trade_log.csv")

        total = len(df)

        return f"Total trade: {total}"

    except:

        return "Belum ada trade"


# ======================
# TELEGRAM HANDLER
# ======================

async def reply(update: Update, context):

    text = update.message.text.lower()


    if text == "ping":

        await update.message.reply_text("AI aktif")


    elif text == "status":

        await update.message.reply_text("OpenClaw running | MT5 connected")


    elif text == "gold":

        price = get_price("XAUUSD")

        await update.message.reply_text(f"Harga Gold: {price}")


    elif text == "buy gold":

        valid, reason = validate_trade("XAUUSD", "buy")

        if valid:

            open_trade("XAUUSD", "buy")

            log_trade("XAUUSD", "BUY", get_price("XAUUSD"), reason)

            await update.message.reply_text(
                f"BUY GOLD executed\nReason: {reason}"
            )

        else:

            await update.message.reply_text(
                f"Trade rejected\nReason: {reason}"
            )


    elif text == "sell gold":

        valid, reason = validate_trade("XAUUSD", "sell")

        if valid:

            open_trade("XAUUSD", "sell")

            log_trade("XAUUSD", "SELL", get_price("XAUUSD"), reason)

            await update.message.reply_text(
                f"SELL GOLD executed\nReason: {reason}"
            )

        else:

            await update.message.reply_text(
                f"Trade rejected\nReason: {reason}"
            )


    elif text == "report":

        report = analyze_trades()

        await update.message.reply_text(report)


    else:

        await update.message.reply_text("Command tidak dikenal")


# ======================
# RUN BOT
# ======================

app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(MessageHandler(filters.TEXT, reply))

print("OpenClaw Telegram Bot Running...")

app.run_polling()