from ai_analyzer import analyze_market

def validate_trade(symbol, direction):

    analysis = analyze_market(symbol)

    trend = analysis["trend"]
    rsi = analysis["rsi"]

    reason = []

    if direction == "buy":

        if trend != "bullish":
            return False, "Trend tidak bullish"

        if rsi > 70:
            return False, "RSI overbought"

        reason.append("Trend bullish")
        reason.append("RSI normal")

    if direction == "sell":

        if trend != "bearish":
            return False, "Trend tidak bearish"

        if rsi < 30:
            return False, "RSI oversold"

        reason.append("Trend bearish")
        reason.append("RSI normal")

    return True, ", ".join(reason)