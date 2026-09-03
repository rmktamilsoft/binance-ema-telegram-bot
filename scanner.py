import os
import sys
import json
import time
import requests
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


# =========================================================
# CONFIG
# =========================================================

STATE_FILE = "state.json"
MAX_WORKERS = 10
KLINE_LIMIT = 210

IST = ZoneInfo("Asia/Kolkata")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_IDS = os.environ.get("TELEGRAM_CHAT_IDS", "").split(",")

# Manual test mode:
# true  = resend already-alerted signals
# false = normal duplicate protection
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"


# =========================================================
# STATE
# =========================================================

def load_state():
    if not os.path.exists(STATE_FILE):
        return {
            "alerts": [],
            "alerted_symbols": []
        }

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        # Backward compatibility if old state was just a list
        if isinstance(state, list):
            return {
                "alerts": state,
                "alerted_symbols": []
            }

        return {
            "alerts": state.get("alerts", []),
            "alerted_symbols": state.get("alerted_symbols", [])
        }

    except Exception:
        return {
            "alerts": [],
            "alerted_symbols": []
        }


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


# =========================================================
# BINANCE API
# =========================================================

def binance_get(url, params=None):
    for attempt in range(3):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=20
            )

            response.raise_for_status()
            return response.json()

        except Exception as e:
            print(
                f"Binance request failed "
                f"(attempt {attempt + 1}/3): {e}"
            )

            if attempt < 2:
                time.sleep(2 ** (attempt + 1))

    return None


def get_usdt_pairs():
    url = "https://data-api.binance.vision/api/v3/exchangeInfo"

    data = binance_get(url)

    if not data:
        return []

    symbols = []

    for item in data.get("symbols", []):
        if (
            item.get("quoteAsset") == "USDT"
            and item.get("status") == "TRADING"
            and item.get("isSpotTradingAllowed") is True
        ):
            symbols.append(item["symbol"])

    return sorted(symbols)


def get_klines(symbol, interval):
    url = "https://data-api.binance.vision/api/v3/klines"

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": KLINE_LIMIT
    }

    return binance_get(url, params)


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_IDS:
        print("❌ Telegram credentials missing.")
        return False

    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_TOKEN}/sendMessage"
    )

    all_sent = True

    for chat_id in TELEGRAM_CHAT_IDS:
        chat_id = chat_id.strip()

        if not chat_id:
            continue

        payload = {
            "chat_id": chat_id,
            "text": message,
            "disable_web_page_preview": True
        }

        try:
            response = requests.post(
                url,
                json=payload,
                timeout=20
            )

            if response.ok:
                print(f"✅ Telegram sent to {chat_id}")
            else:
                print(
                    f"❌ Telegram error for {chat_id}:",
                    response.status_code,
                    response.text
                )
                all_sent = False

        except Exception as e:
            print(
                f"❌ Telegram request failed for {chat_id}:",
                e
            )
            all_sent = False

    return all_sent


# =========================================================
# DATA ANALYSIS
# =========================================================

def prepare_dataframe(klines):
    if not klines:
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
        "taker_buy_base",
        "taker_buy_quote",
        "ignore"
    ]

    df = pd.DataFrame(klines, columns=columns)

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    for col in numeric_columns:
        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )

    df["open_time"] = pd.to_datetime(
        df["open_time"],
        unit="ms",
        utc=True
    )

    df["close_time"] = pd.to_datetime(
        df["close_time"],
        unit="ms",
        utc=True
    )

    # -----------------------------------------------------
    # Remove current forming candle
    # -----------------------------------------------------

    now = pd.Timestamp.now(tz="UTC")

    df = df[df["close_time"] < now].copy()

    if len(df) < 205:
        return None

    df["ema50"] = df["close"].ewm(
        span=50,
        adjust=False
    ).mean()

    df["ema200"] = df["close"].ewm(
        span=200,
        adjust=False
    ).mean()

    return df


# =========================================================
# TREND
# =========================================================

def get_trend(close, ema50, ema200):

    if close > ema50 > ema200:
        return "STRONG BULLISH"

    if close > ema50 and ema50 <= ema200:
        return "BULLISH / TRANSITION"

    if close < ema50 < ema200:
        return "STRONG BEARISH"

    if close < ema50 and ema50 >= ema200:
        return "BEARISH / TRANSITION"

    return "NEUTRAL"


# =========================================================
# SIGNAL DETECTION
# =========================================================

def analyze_symbol(symbol, interval):

    klines = get_klines(symbol, interval)

    if not klines:
        return None

    df = prepare_dataframe(klines)

    if df is None or len(df) < 205:
        return None

    previous = df.iloc[-2]
    current = df.iloc[-1]

    prev_close = float(previous["close"])
    close = float(current["close"])

    prev_ema50 = float(previous["ema50"])
    ema50 = float(current["ema50"])

    prev_ema200 = float(previous["ema200"])
    ema200 = float(current["ema200"])

    signals = []

    # -----------------------------------------------------
    # EMA 50 crosses ABOVE EMA 200
    # -----------------------------------------------------

    if (
        prev_ema50 <= prev_ema200
        and ema50 > ema200
    ):
        signals.append(
            "EMA 50 crossed ABOVE EMA 200 🟢"
        )

    # -----------------------------------------------------
    # EMA 50 crosses BELOW EMA 200
    # Only allowed on 1D
    # -----------------------------------------------------

    if interval == "1d":
        if (
            prev_ema50 >= prev_ema200
            and ema50 < ema200
        ):
            signals.append(
                "EMA 50 crossed BELOW EMA 200 🔴"
            )

    # -----------------------------------------------------
    # Price crosses ABOVE EMA 50
    # -----------------------------------------------------

    if (
        prev_close <= prev_ema50
        and close > ema50
    ):
        signals.append(
            "Price crossed ABOVE EMA 50 🟢"
        )

    # -----------------------------------------------------
    # Price crosses BELOW EMA 50
    # Only allowed on 1D
    # -----------------------------------------------------

    if interval == "1d":
        if (
            prev_close >= prev_ema50
            and close < ema50
        ):
            signals.append(
                "Price crossed BELOW EMA 50 🔴"
            )

    # -----------------------------------------------------
    # Price crosses ABOVE EMA 200
    # -----------------------------------------------------

    if (
        prev_close <= prev_ema200
        and close > ema200
    ):
        signals.append(
            "Price crossed ABOVE EMA 200 🟢"
        )

    # -----------------------------------------------------
    # Price crosses BELOW EMA 200
    # Only allowed on 1D
    # -----------------------------------------------------

    if interval == "1d":
        if (
            prev_close >= prev_ema200
            and close < ema200
        ):
            signals.append(
                "Price crossed BELOW EMA 200 🔴"
            )

    if not signals:
        return None

    trend = get_trend(
        close,
        ema50,
        ema200
    )

    return {
        "symbol": symbol,
        "interval": interval,
        "signals": signals,
        "trend": trend,
        "close": close,
        "ema50": ema50,
        "ema200": ema200,
        "candle_time": current["close_time"]
    }


# =========================================================
# SIGNAL STRENGTH
# =========================================================

def get_signal_strength(result):

    signals = result["signals"]
    trend = result["trend"]

    if len(signals) >= 2:
        return "🔥 STRONG SIGNAL"

    if (
        any("EMA 50 crossed" in s for s in signals)
        and any(
            "Price crossed ABOVE EMA 50" in s
            or "Price crossed BELOW EMA 50" in s
            or "Price crossed ABOVE EMA 200" in s
            or "Price crossed BELOW EMA 200" in s
            for s in signals
        )
    ):
        return "🔥 STRONG SIGNAL"

    if "BULLISH" in trend:
        return "🟢 BULLISH SIGNAL"

    if "BEARISH" in trend:
        return "🔴 BEARISH SIGNAL"

    return "⚠️ SIGNAL"


# =========================================================
# TIME FORMAT
# =========================================================

def format_times(timestamp):

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(
            tzinfo=timezone.utc
        )

    utc_time = timestamp.astimezone(timezone.utc)
    ist_time = timestamp.astimezone(IST)

    return (
        utc_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
        ist_time.strftime("%Y-%m-%d %H:%M:%S IST")
    )


# =========================================================
# SIGNAL ID
# =========================================================

def make_signal_id(result):

    candle_time = result["candle_time"].isoformat()

    signal_text = "|".join(
        result["signals"]
    )

    return (
        f"{result['symbol']}|"
        f"{result['interval']}|"
        f"{candle_time}|"
        f"{signal_text}"
    )


# =========================================================
# MULTI TIMEFRAME
# =========================================================

def get_mtf_confirmation(symbol, current_interval):

    other_interval = (
        "1d"
        if current_interval == "4h"
        else "4h"
    )

    klines = get_klines(
        symbol,
        other_interval
    )

    if not klines:
        return None

    df = prepare_dataframe(klines)

    if df is None:
        return None

    current = df.iloc[-1]

    close = float(current["close"])
    ema50 = float(current["ema50"])
    ema200 = float(current["ema200"])

    trend = get_trend(
        close,
        ema50,
        ema200
    )

    return {
        "interval": other_interval,
        "trend": trend,
        "close": close,
        "ema50": ema50,
        "ema200": ema200
    }


# =========================================================
# MESSAGE
# =========================================================

def format_message(
    result,
    previously_alerted=False,
    mtf=None
):

    symbol = result["symbol"]
    interval = result["interval"]

    close = result["close"]
    ema50 = result["ema50"]
    ema200 = result["ema200"]

    signals = result["signals"]
    trend = result["trend"]

    strength = get_signal_strength(result)

    utc_time, ist_time = format_times(
        result["candle_time"]
    )

    distance_50 = (
        (close - ema50) / ema50
    ) * 100

    distance_200 = (
        (close - ema200) / ema200
    ) * 100

    if interval == "4h":
        timeframe_text = "4H"
    else:
        timeframe_text = "1D"

    lines = []

    lines.append("🚨 BINANCE EMA ALERT")
    lines.append("")
    lines.append(f"🪙 Coin: {symbol}")
    lines.append(f"⏱ Timeframe: {timeframe_text}")

    if previously_alerted:
        lines.append("")
        lines.append(
            "🔁 PREVIOUSLY ALERTED COIN"
        )

    lines.append("")
    lines.append(f"📊 Signal Strength: {strength}")
    lines.append("")

    lines.append("📢 SIGNALS:")

    for signal in signals:
        lines.append(f"• {signal}")

    lines.append("")
    lines.append(f"📈 Trend: {trend}")

    lines.append("")
    lines.append("💰 PRICE / EMA")
    lines.append(
        f"Price: {close:.8f}"
    )
    lines.append(
        f"EMA 50: {ema50:.8f}"
    )
    lines.append(
        f"EMA 200: {ema200:.8f}"
    )

    lines.append("")
    lines.append(
        f"📏 Price vs EMA 50: "
        f"{distance_50:+.2f}%"
    )

    lines.append(
        f"📏 Price vs EMA 200: "
        f"{distance_200:+.2f}%"
    )

    # -----------------------------------------------------
    # MTF
    # -----------------------------------------------------

    if mtf:

        if (
            "BULLISH" in trend
            and "BULLISH" in mtf["trend"]
        ):
            lines.append("")
            lines.append(
                "🔥🔥 MULTI-TIMEFRAME "
                "BULLISH CONFIRMATION"
            )

        elif (
            "BEARISH" in trend
            and "BEARISH" in mtf["trend"]
        ):
            lines.append("")
            lines.append(
                "🔴🔴 MULTI-TIMEFRAME "
                "BEARISH CONFIRMATION"
            )

        else:
            lines.append("")
            lines.append(
                "⚠️ MULTI-TIMEFRAME MIXED"
            )

        lines.append(
            f"{mtf['interval'].upper()} Trend: "
            f"{mtf['trend']}"
        )

    lines.append("")
    lines.append("🕯 CANDLE CLOSE")
    lines.append(f"UTC: {utc_time}")
    lines.append(f"IST: {ist_time}")

    lines.append("")
    lines.append(
        "✅ CLOSED CANDLE — CONFIRMED"
    )

    lines.append("")
    lines.append(
        f"🔗 Binance Chart: "
        f"https://www.binance.com/en/trade/"
        f"{symbol}?type=spot"
    )

    return "\n".join(lines)


# =========================================================
# SUMMARY
# =========================================================

def send_summary(
    results,
    interval,
    new_alert_count
):

    if not results:
        return

    timeframe = (
        "4H"
        if interval == "4h"
        else "1D"
    )

    bullish_count = 0
    bearish_count = 0
    strong_coins = []

    for result in results:

        if any(
            "🟢" in signal
            for signal in result["signals"]
        ):
            bullish_count += 1

        if any(
            "🔴" in signal
            for signal in result["signals"]
        ):
            bearish_count += 1

        if (
            "STRONG" in
            get_signal_strength(result)
        ):
            strong_coins.append(
                result["symbol"]
            )

    lines = []

    lines.append(
        f"📋 BINANCE {timeframe} SCAN SUMMARY"
    )

    lines.append("")
    lines.append(
        f"🔎 Signals found: {len(results)}"
    )

    lines.append(
        f"🆕 New alerts sent: {new_alert_count}"
    )

    lines.append(
        f"🟢 Bullish signals: {bullish_count}"
    )

    lines.append(
        f"🔴 Bearish signals: {bearish_count}"
    )

    if strong_coins:
        lines.append("")
        lines.append("🔥 STRONG SIGNALS:")

        for coin in strong_coins[:20]:
            lines.append(f"• {coin}")

    send_telegram(
        "\n".join(lines)
    )


# =========================================================
# SCAN
# =========================================================

def scan(interval):

    print("")
    print("=" * 60)
    print(
        f"🚀 Starting Binance EMA Scanner: "
        f"{interval.upper()}"
    )
    print("=" * 60)

    if TEST_MODE:
        print(
            "🧪 TEST MODE ENABLED"
        )
        print(
            "Duplicate protection is bypassed "
            "for this manual run."
        )

    state = load_state()

    alerts = state.get(
        "alerts",
        []
    )

    alerted_symbols = set(
        state.get(
            "alerted_symbols",
            []
        )
    )

    symbols = get_usdt_pairs()

    print(
        f"📊 USDT Spot pairs found: "
        f"{len(symbols)}"
    )

    if not symbols:
        print(
            "❌ No Binance USDT pairs found."
        )
        return

    results = []

    # -----------------------------------------------------
    # Parallel scan
    # -----------------------------------------------------

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                analyze_symbol,
                symbol,
                interval
            ): symbol
            for symbol in symbols
        }

        for future in as_completed(futures):

            symbol = futures[future]

            try:
                result = future.result()

                if result:
                    results.append(result)

            except Exception as e:
                print(
                    f"❌ Error scanning "
                    f"{symbol}: {e}"
                )

    results.sort(
        key=lambda x: x["symbol"]
    )

    print(
        f"📢 Signals found: "
        f"{len(results)}"
    )

    new_alert_count = 0

    # -----------------------------------------------------
    # Process alerts
    # -----------------------------------------------------

    for result in results:

        signal_id = make_signal_id(
            result
        )

        # Normal mode:
        # exact same signal will not repeat
        #
        # Test mode:
        # same signal can be sent again

        if (
            signal_id in alerts
            and not TEST_MODE
        ):
            continue

        previously_alerted = (
            result["symbol"]
            in alerted_symbols
        )

        mtf = get_mtf_confirmation(
            result["symbol"],
            interval
        )

        message = format_message(
            result,
            previously_alerted,
            mtf
        )

        print("")
        print(
            f"📤 Sending alert: "
            f"{result['symbol']}"
        )

        sent = send_telegram(
            message
        )

        if sent:

            new_alert_count += 1

            # Keep state updated
            if signal_id not in alerts:
                alerts.append(
                    signal_id
                )

            alerted_symbols.add(
                result["symbol"]
            )

            print(
                f"✅ Alert sent: "
                f"{result['symbol']}"
            )

        else:
            print(
                f"❌ Alert failed: "
                f"{result['symbol']}"
            )

    # -----------------------------------------------------
    # Summary
    # -----------------------------------------------------

    if results:
        send_summary(
            results,
            interval,
            new_alert_count
        )

    # -----------------------------------------------------
    # Save state
    # -----------------------------------------------------

    state["alerts"] = alerts

    state["alerted_symbols"] = sorted(
        alerted_symbols
    )

    save_state(state)

    print("")
    print("=" * 60)
    print(
        f"✅ Scan completed: "
        f"{interval.upper()}"
    )
    print(
        f"📨 New alerts sent: "
        f"{new_alert_count}"
    )
    print("=" * 60)


# =========================================================
# MAIN
# =========================================================

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
            "❌ Invalid timeframe."
        )
        print(
            "Use 4h or 1d."
        )
        sys.exit(1)

    scan(timeframe)
