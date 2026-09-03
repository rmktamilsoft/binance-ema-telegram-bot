import os
import requests
import pandas as pd
from datetime import datetime, timezone

BINANCE_API = "https://api.binance.com"

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]


def get_usdt_pairs():
    url = f"{BINANCE_API}/api/v3/exchangeInfo"
    data = requests.get(url, timeout=20).json()

    symbols = []

    for s in data["symbols"]:
        if (
            s["quoteAsset"] == "USDT"
            and s["status"] == "TRADING"
            and s["isSpotTradingAllowed"]
        ):
            symbols.append(s["symbol"])

    return symbols


def get_klines(symbol, interval, limit=250):
    url = f"{BINANCE_API}/api/v3/klines"

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()

    return response.json()


def calculate_ema_signals(symbol, interval):
    candles = get_klines(symbol, interval)

    if len(candles) < 205:
        return []

    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trades",
        "taker_base",
        "taker_quote",
        "ignore",
    ]

    df = pd.DataFrame(candles, columns=columns)

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col])

    # Last candle may still be open.
    # We only use CLOSED candles.
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    if df.iloc[-1]["close_time"] >= now_ms:
        df = df.iloc[:-1].copy()

    if len(df) < 205:
        return []

    df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
    df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()

    previous = df.iloc[-2]
    current = df.iloc[-1]

    signals = []

    # --------------------------------------------------
    # EMA 50 / EMA 200 CROSS
    # --------------------------------------------------

    if (
        previous["ema50"] <= previous["ema200"]
        and current["ema50"] > current["ema200"]
    ):
        signals.append(
            {
                "type": "EMA CROSS",
                "direction": "BULLISH",
                "message": (
                    f"EMA 50 crossed ABOVE EMA 200"
                ),
            }
        )

    elif (
        previous["ema50"] >= previous["ema200"]
        and current["ema50"] < current["ema200"]
    ):
        signals.append(
            {
                "type": "EMA CROSS",
                "direction": "BEARISH",
                "message": (
                    f"EMA 50 crossed BELOW EMA 200"
                ),
            }
        )

    # --------------------------------------------------
    # PRICE CROSS EMA 50
    # --------------------------------------------------

    if (
        previous["close"] <= previous["ema50"]
        and current["close"] > current["ema50"]
    ):
        signals.append(
            {
                "type": "PRICE BREAK",
                "direction": "BULLISH",
                "message": "Price crossed ABOVE EMA 50",
            }
        )

    elif (
        previous["close"] >= previous["ema50"]
        and current["close"] < current["ema50"]
    ):
        signals.append(
            {
                "type": "PRICE BREAK",
                "direction": "BEARISH",
                "message": "Price crossed BELOW EMA 50",
            }
        )

    # --------------------------------------------------
    # PRICE CROSS EMA 200
    # --------------------------------------------------

    if (
        previous["close"] <= previous["ema200"]
        and current["close"] > current["ema200"]
    ):
        signals.append(
            {
                "type": "PRICE BREAK",
                "direction": "BULLISH",
                "message": "Price crossed ABOVE EMA 200",
            }
        )

    elif (
        previous["close"] >= previous["ema200"]
        and current["close"] < current["ema200"]
    ):
        signals.append(
            {
                "type": "PRICE BREAK",
                "direction": "BEARISH",
                "message": "Price crossed BELOW EMA 200",
            }
        )

    return signals, current


def send_telegram(message):
    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
    }

    response = requests.post(
        url,
        data=payload,
        timeout=20,
    )

    response.raise_for_status()


def format_message(
    symbol,
    interval,
    signal,
    candle,
):
    emoji = "🟢" if signal["direction"] == "BULLISH" else "🔴"

    close = candle["close"]
    ema50 = candle["ema50"]
    ema200 = candle["ema200"]

    return f"""
{emoji} Binance EMA Alert

Symbol: {symbol}
Timeframe: {interval}

Signal: {signal["type"]}
Direction: {signal["direction"]}

{signal["message"]}

Price: {close:.8g}
EMA 50: {ema50:.8g}
EMA 200: {ema200:.8g}

Candle: CLOSED
""".strip()


def run_scan(interval):
    print(f"\nScanning {interval}...")

    symbols = get_usdt_pairs()

    print(f"USDT pairs: {len(symbols)}")

    for i, symbol in enumerate(symbols, 1):

        try:
            result = calculate_ema_signals(
                symbol,
                interval,
            )

            if not result:
                continue

            signals, candle = result

            for signal in signals:

                message = format_message(
                    symbol,
                    interval,
                    signal,
                    candle,
                )

                send_telegram(message)

                print(
                    f"ALERT: {symbol} "
                    f"{interval} "
                    f"{signal['message']}"
                )

        except Exception as e:
            print(
                f"ERROR {symbol} {interval}: {e}"
            )

        if i % 50 == 0:
            print(
                f"Processed {i}/{len(symbols)}"
            )


if __name__ == "__main__":

    # 4 Hour scan
    run_scan("4h")

    # Daily scan
    run_scan("1d")
