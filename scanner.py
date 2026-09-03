import os
import sys
import json
import time
import requests
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


BINANCE_API = "https://data-api.binance.vision"

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

STATE_FILE = "state.json"

MAX_WORKERS = 10
KLINE_LIMIT = 210

IST = ZoneInfo("Asia/Kolkata")


# ==================================================
# STATE
# ==================================================

def load_state():

    if not os.path.exists(STATE_FILE):
        return {
            "alerts": [],
            "alerted_symbols": []
        }

    try:

        with open(STATE_FILE, "r") as file:
            data = json.load(file)

        # Backward compatibility with old state.json
        if isinstance(data, list):
            symbols = set()

            for alert_id in data:
                parts = alert_id.split("|")

                if parts:
                    symbols.add(parts[0])

            return {
                "alerts": data,
                "alerted_symbols": sorted(symbols)
            }

        return {
            "alerts": data.get("alerts", []),
            "alerted_symbols": data.get(
                "alerted_symbols",
                []
            )
        }

    except Exception:
        return {
            "alerts": [],
            "alerted_symbols": []
        }


def save_state(state):

    with open(STATE_FILE, "w") as file:
        json.dump(
            state,
            file,
            indent=2
        )


# ==================================================
# BINANCE REQUEST WITH RETRY
# ==================================================

def binance_get(url, params=None, retries=3):

    last_error = None

    for attempt in range(1, retries + 1):

        try:

            response = requests.get(
                url,
                params=params,
                timeout=20,
                headers={
                    "User-Agent":
                    "Binance-EMA-Scanner/2.0"
                }
            )

            response.raise_for_status()

            return response.json()

        except Exception as error:

            last_error = error

            if attempt < retries:

                wait_time = attempt * 2

                print(
                    f"🔄 Retry {attempt}/{retries - 1} "
                    f"in {wait_time}s...",
                    flush=True
                )

                time.sleep(wait_time)

    raise last_error


# ==================================================
# GET USDT PAIRS
# ==================================================

def get_usdt_pairs():

    url = f"{BINANCE_API}/api/v3/exchangeInfo"

    data = binance_get(url)

    if "symbols" not in data:

        raise RuntimeError(
            f"Unexpected Binance response: {data}"
        )

    symbols = []

    for symbol in data["symbols"]:

        if (
            symbol.get("quoteAsset") == "USDT"
            and symbol.get("status") == "TRADING"
            and symbol.get(
                "isSpotTradingAllowed",
                False
            )
        ):
            symbols.append(
                symbol["symbol"]
            )

    return symbols


# ==================================================
# GET KLINES
# ==================================================

def get_klines(symbol, interval):

    url = f"{BINANCE_API}/api/v3/klines"

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": KLINE_LIMIT
    }

    return binance_get(
        url,
        params=params
    )


# ==================================================
# CALCULATE SIGNALS
# ==================================================

def calculate_signals(symbol, interval):

    candles = get_klines(
        symbol,
        interval
    )

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
        "ignore"
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

    # --------------------------------------------------
    # Remove currently forming candle
    # --------------------------------------------------

    now_ms = int(
        datetime.now(
            timezone.utc
        ).timestamp() * 1000
    )

    if df.iloc[-1]["close_time"] >= now_ms:

        df = df.iloc[:-1].copy()

    if len(df) < 205:
        return None

    # --------------------------------------------------
    # EMA
    # --------------------------------------------------

    df["ema50"] = df["close"].ewm(
        span=50,
        adjust=False
    ).mean()

    df["ema200"] = df["close"].ewm(
        span=200,
        adjust=False
    ).mean()

    previous = df.iloc[-2]
    current = df.iloc[-1]

    signals = []

    # ==================================================
    # 1. EMA 50 ABOVE EMA 200
    # ==================================================

    if (
        previous["ema50"] <= previous["ema200"]
        and current["ema50"] > current["ema200"]
    ):

        signals.append(
            "🟢 EMA 50 crossed ABOVE EMA 200"
        )

    # ==================================================
    # 2. EMA 50 BELOW EMA 200
    # 1D ONLY
    # ==================================================

    elif (
        interval == "1d"
        and previous["ema50"] >= previous["ema200"]
        and current["ema50"] < current["ema200"]
    ):

        signals.append(
            "🔴 EMA 50 crossed BELOW EMA 200"
        )

    # ==================================================
    # 3. PRICE ABOVE EMA 50
    # ==================================================

    if (
        previous["close"] <= previous["ema50"]
        and current["close"] > current["ema50"]
    ):

        signals.append(
            "🟢 Price crossed ABOVE EMA 50"
        )

    # ==================================================
    # 4. PRICE BELOW EMA 50
    # 1D ONLY
    # ==================================================

    elif (
        interval == "1d"
        and previous["close"] >= previous["ema50"]
        and current["close"] < current["ema50"]
    ):

        signals.append(
            "🔴 Price crossed BELOW EMA 50"
        )

    # ==================================================
    # 5. PRICE ABOVE EMA 200
    # ==================================================

    if (
        previous["close"] <= previous["ema200"]
        and current["close"] > current["ema200"]
    ):

        signals.append(
            "🟢 Price crossed ABOVE EMA 200"
        )

    # ==================================================
    # 6. PRICE BELOW EMA 200
    # 1D ONLY
    # ==================================================

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
        "price": float(current["close"]),
        "ema50": float(current["ema50"]),
        "ema200": float(current["ema200"]),
        "signals": signals,
        "candle_time": int(
            current["close_time"]
        )
    }


# ==================================================
# TELEGRAM
# ==================================================

def send_telegram(message):

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True
    }

    response = requests.post(
        url,
        data=payload,
        timeout=20
    )

    if not response.ok:

        raise RuntimeError(
            f"Telegram error "
            f"{response.status_code}: "
            f"{response.text}"
        )


# ==================================================
# TIME FORMAT
# ==================================================

def format_times(timestamp_ms):

    dt_utc = datetime.fromtimestamp(
        timestamp_ms / 1000,
        tz=timezone.utc
    )

    dt_ist = dt_utc.astimezone(IST)

    utc_text = dt_utc.strftime(
        "%Y-%m-%d %H:%M UTC"
    )

    ist_text = dt_ist.strftime(
        "%Y-%m-%d %H:%M IST"
    )

    return utc_text, ist_text


# ==================================================
# SIGNAL ID
# ==================================================

def make_signal_id(result):

    return "|".join([
        result["symbol"],
        result["interval"],
        str(result["candle_time"]),
        "|".join(result["signals"])
    ])


# ==================================================
# TELEGRAM MESSAGE
# ==================================================

def format_message(
    result,
    previously_alerted
):

    symbol = result["symbol"]
    interval = result["interval"]

    price = result["price"]
    ema50 = result["ema50"]
    ema200 = result["ema200"]

    candle_time = result["candle_time"]

    utc_text, ist_text = format_times(
        candle_time
    )

    # --------------------------------------------------
    # Price distance from EMA
    # --------------------------------------------------

    ema50_distance = (
        (price - ema50)
        / ema50
    ) * 100

    ema200_distance = (
        (price - ema200)
        / ema200
    ) * 100

    # --------------------------------------------------
    # Timeframe display
    # --------------------------------------------------

    if interval == "4h":
        timeframe = "🔥 4H"
    else:
        timeframe = "🔥 1D"

    # --------------------------------------------------
    # Previous alert highlight
    # --------------------------------------------------

    if previously_alerted:

        history_text = (
            "🔁 PREVIOUSLY ALERTED COIN\n"
            "⚠️ This coin has triggered an "
            "EMA alert before.\n\n"
        )

    else:

        history_text = (
            "🆕 FIRST EMA ALERT FOR THIS COIN\n\n"
        )

    # --------------------------------------------------
    # Signals
    # --------------------------------------------------

    signal_text = "\n".join(
        f"• {signal}"
        for signal in result["signals"]
    )

    # --------------------------------------------------
    # Binance chart
    # --------------------------------------------------

    chart_url = (
        f"https://www.binance.com/en/trade/"
        f"{symbol}?type=spot"
    )

    return (
        "📊 BINANCE EMA ALERT\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        f"🪙 Coin: {symbol}\n"
        f"⏱ Timeframe: {timeframe}\n\n"

        f"{history_text}"

        "📌 SIGNAL\n"
        f"{signal_text}\n\n"

        "💰 PRICE DATA\n"
        f"Price: {price:.8g}\n"
        f"EMA 50: {ema50:.8g}\n"
        f"EMA 200: {ema200:.8g}\n\n"

        "📏 EMA DISTANCE\n"
        f"Price vs EMA 50: "
        f"{ema50_distance:+.2f}%\n"
        f"Price vs EMA 200: "
        f"{ema200_distance:+.2f}%\n\n"

        "🕐 CANDLE CLOSE\n"
        f"UTC: {utc_text}\n"
        f"IST: {ist_text}\n\n"

        "🔒 Candle Status: CLOSED\n\n"

        f"🔗 Binance Chart:\n"
        f"{chart_url}"
    )


# ==================================================
# SCAN
# ==================================================

def scan(interval):

    print(
        f"\n🔎 Scanning {interval}...",
        flush=True
    )

    state = load_state()

    alerts = set(
        state["alerts"]
    )

    alerted_symbols = set(
        state["alerted_symbols"]
    )

    print(
        f"🛡️ Stored alerts: "
        f"{len(alerts)}",
        flush=True
    )

    print(
        f"🔁 Previously alerted coins: "
        f"{len(alerted_symbols)}",
        flush=True
    )

    symbols = get_usdt_pairs()

    print(
        f"📈 USDT pairs: "
        f"{len(symbols)}",
        flush=True
    )

    results = []
    completed = 0

    # --------------------------------------------------
    # Parallel Binance requests
    # --------------------------------------------------

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                calculate_signals,
                symbol,
                interval
            ): symbol

            for symbol in symbols
        }

        for future in as_completed(
            futures
        ):

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
                    f"{completed}/"
                    f"{len(symbols)}",
                    flush=True
                )

    print(
        f"✅ Scan finished: {interval}",
        flush=True
    )

    print(
        f"🚨 Signals found: "
        f"{len(results)}",
        flush=True
    )

    new_alerts = 0

    # ==================================================
    # SEND ALERTS
    # ==================================================

    for result in results:

        signal_id = make_signal_id(
            result
        )

        symbol = result["symbol"]

        # --------------------------------------------------
        # Duplicate protection
        # --------------------------------------------------

        if signal_id in alerts:

            print(
                f"⏭️ Duplicate skipped: "
                f"{symbol} {interval}",
                flush=True
            )

            continue

        # --------------------------------------------------
        # Check previous coin history
        # --------------------------------------------------

        previously_alerted = (
            symbol in alerted_symbols
        )

        if previously_alerted:

            print(
                f"🔁 Previously alerted coin: "
                f"{symbol}",
                flush=True
            )

        try:

            message = format_message(
                result,
                previously_alerted
            )

            send_telegram(
                message
            )

            # Save exact alert
            alerts.add(
                signal_id
            )

            # Save coin history
            alerted_symbols.add(
                symbol
            )

            new_alerts += 1

            print(
                f"📨 Alert sent: "
                f"{symbol} "
                f"{interval}",
                flush=True
            )

        except Exception as error:

            print(
                f"❌ Telegram error "
                f"{symbol}: {error}",
                flush=True
            )

    # --------------------------------------------------
    # Save state
    # --------------------------------------------------

    state = {
        "alerts": sorted(alerts),
        "alerted_symbols": sorted(
            alerted_symbols
        )
    }

    save_state(state)

    print(
        f"🛡️ New alerts sent: "
        f"{new_alerts}",
        flush=True
    )

    print(
        f"🔁 Total alerted coins: "
        f"{len(alerted_symbols)}",
        flush=True
    )

    print(
        f"💾 State saved",
        flush=True
    )


# ==================================================
# MAIN
# ==================================================

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

    if timeframe not in [
        "4h",
        "1d"
    ]:

        print(
            "Timeframe must be 4h or 1d"
        )

        sys.exit(1)

    scan(timeframe)
