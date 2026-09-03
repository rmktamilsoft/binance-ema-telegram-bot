import os
import sys
import json
import requests
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone


BINANCE_API = "https://data-api.binance.vision"

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

STATE_FILE = "state.json"

MAX_WORKERS = 10
KLINE_LIMIT = 210


def load_state():
    if not os.path.exists(STATE_FILE):
        return set()

    try:
        with open(STATE_FILE, "r") as file:
            data = json.load(file)

        return set(data)

    except Exception:
        return set()


def save_state(state):
    with open(STATE_FILE, "w") as file:
        json.dump(sorted(state), file, indent=2)


def get_usdt_pairs():
    url = f"{BINANCE_API}/api/v3/exchangeInfo"

    response = requests.get(
        url,
        timeout=30,
        headers={
            "User-Agent": "Binance-EMA-Scanner/1.0"
        },
    )

    response.raise_for_status()

    data = response.json()

    if "symbols" not in data:
        raise RuntimeError(
            f"Unexpected Binance response: {data}"
        )

    symbols = []

    for symbol in data["symbols"]:
        if (
            symbol.get("quoteAsset") == "USDT"
            and symbol.get("status") == "TRADING"
            and symbol.get("isSpotTradingAllowed", False)
        ):
            symbols.append(symbol["symbol"])

    return symbols


def get_klines(symbol, interval):
    url = f"{BINANCE_API}/api/v3/klines"

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": KLINE_LIMIT,
    }

    response = requests.get(
        url,
        params=params,
        timeout=20,
    )

    response.raise_for_status()

    return response.json()


def calculate_signals(symbol, interval):
    candles = get_klines(symbol, interval)

    if len(candles) < 205:
        return None

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

    df = pd.DataFrame(
        candles,
        columns=columns
    )

    for column in [
        "open",
        "high",
        "low",
        "close"
    ]:
        df[column] = pd.to_numeric(
            df[column]
        )

    # Remove currently forming candle
    now_ms = int(
        datetime.now(timezone.utc).timestamp()
        * 1000
    )

    if df.iloc[-1]["close_time"] >= now_ms:
        df = df.iloc[:-1].copy()

    if len(df) < 205:
        return None

    # EMA 50
    df["ema50"] = df["close"].ewm(
        span=50,
        adjust=False
    ).mean()

    # EMA 200
    df["ema200"] = df["close"].ewm(
        span=200,
        adjust=False
    ).mean()

    previous = df.iloc[-2]
    current = df.iloc[-1]

    signals = []

    # 1. EMA 50 crosses ABOVE EMA 200
    if (
        previous["ema50"] <= previous["ema200"]
        and current["ema50"] > current["ema200"]
    ):
        signals.append(
            "🟢 EMA 50 crossed ABOVE EMA 200"
        )

    # 2. EMA 50 crosses BELOW EMA 200
    # 1D ONLY
    elif (
        interval == "1d"
        and previous["ema50"] >= previous["ema200"]
        and current["ema50"] < current["ema200"]
    ):
        signals.append(
            "🔴 EMA 50 crossed BELOW EMA 200"
        )

    # 3. Price crosses ABOVE EMA 50
    if (
        previous["close"] <= previous["ema50"]
        and current["close"] > current["ema50"]
    ):
        signals.append(
            "🟢 Price crossed ABOVE EMA 50"
        )

    # 4. Price crosses BELOW EMA 50
    # 1D ONLY
    elif (
        interval == "1d"
        and previous["close"] >= previous["ema50"]
        and current["close"] < current["ema50"]
    ):
        signals.append(
            "🔴 Price crossed BELOW EMA 50"
        )

    # 5. Price crosses ABOVE EMA 200
    if (
        previous["close"] <= previous["ema200"]
        and current["close"] > current["ema200"]
    ):
        signals.append(
            "🟢 Price crossed ABOVE EMA 200"
        )

    # 6. Price crosses BELOW EMA 200
    # 1D ONLY
    elif (
        interval == "1d"
        and previous["close"] >= previous["ema200"]
        and current["close"] < current["ema200"]
    ):
        signals.append(
            "🔴 Price crossed BELOW EMA 200"
        )

    if not signals:
        return None

    return {
        "symbol": symbol,
        "interval": interval,
        "price": current["close"],
        "ema50": current["ema50"],
        "ema200": current["ema200"],
        "signals": signals,
        "candle_time": int(current["close_time"]),
    }


def send_telegram(message):
    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/sendMessage"
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

    if not response.ok:
        raise RuntimeError(
            f"Telegram error "
            f"{response.status_code}: "
            f"{response.text}"
        )


def format_message(result):
    symbol = result["symbol"]
    interval = result["interval"]
    price = result["price"]
    ema50 = result["ema50"]
    ema200 = result["ema200"]

    signal_text = "\n".join(
        f"• {signal}"
        for signal in result["signals"]
    )

    return (
        "📊 Binance EMA Alert\n\n"
        f"Symbol: {symbol}\n"
        f"Timeframe: {interval}\n\n"
        f"{signal_text}\n\n"
        f"Price: {price:.8g}\n"
        f"EMA 50: {ema50:.8g}\n"
        f"EMA 200: {ema200:.8g}\n\n"
        "Candle: CLOSED"
    )


def make_signal_id(result):
    """
    Unique ID for this exact symbol + timeframe
    + candle + signal.
    """

    return "|".join([
        result["symbol"],
        result["interval"],
        str(result["candle_time"]),
        "|".join(result["signals"]),
    ])


def scan(interval):
    print(
        f"\n🔎 Scanning {interval}...",
        flush=True
    )

    state = load_state()

    print(
        f"🛡️ Stored alerts: {len(state)}",
        flush=True
    )

    symbols = get_usdt_pairs()

    print(
        f"📈 USDT pairs: {len(symbols)}",
        flush=True
    )

    results = []
    completed = 0

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                calculate_signals,
                symbol,
                interval,
            ): symbol
            for symbol in symbols
        }

        for future in as_completed(futures):

            symbol = futures[future]
            completed += 1

            try:
                result = future.result()

                if result:
                    results.append(result)

            except Exception as error:
                print(
                    f"⚠️ {symbol}: {error}",
                    flush=True
                )

            if completed % 50 == 0:
                print(
                    f"Processed "
                    f"{completed}/{len(symbols)}",
                    flush=True
                )

    print(
        f"✅ Scan finished: {interval}",
        flush=True
    )

    print(
        f"🚨 Signals found: {len(results)}",
        flush=True
    )

    new_alerts = 0

    for result in results:

        signal_id = make_signal_id(result)

        # Duplicate protection
        if signal_id in state:
            print(
                f"⏭️ Duplicate skipped: "
                f"{result['symbol']} "
                f"{interval}",
                flush=True
            )
            continue

        try:
            message = format_message(result)

            send_telegram(message)

            state.add(signal_id)
            new_alerts += 1

            print(
                f"📨 Alert sent: "
                f"{result['symbol']} "
                f"{interval}",
                flush=True
            )

        except Exception as error:

            print(
                f"❌ Telegram error "
                f"{result['symbol']}: "
                f"{error}",
                flush=True
            )

    save_state(state)

    print(
        f"🛡️ New alerts sent: {new_alerts}",
        flush=True
    )

    print(
        f"💾 State saved: {len(state)} records",
        flush=True
    )


if __name__ == "__main__":

    if len(sys.argv) != 2:

        print(
            "Usage: python scanner.py 4h"
        )

        print(
            "   or: python scanner.py 1d"
        )

        sys.exit(1)

    timeframe = sys.argv[1].lower()

    if timeframe not in ["4h", "1d"]:

        print(
            "Timeframe must be 4h or 1d"
        )

        sys.exit(1)

    scan(timeframe)
