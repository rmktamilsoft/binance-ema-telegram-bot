import os
import sys
import json
import time
import requests
import pandas as pd

from html import escape
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timezone
from zoneinfo import ZoneInfo


# =========================================================
# CONFIG
# =========================================================

STATE_FILE = "state.json"

MAX_WORKERS = 10

# Binance maximum candles per API request
BINANCE_KLINE_PAGE_SIZE = 1000

# Small pause between historical pages
KLINE_PAGE_SLEEP = 0.05

IST = ZoneInfo("Asia/Kolkata")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")

TELEGRAM_CHAT_IDS = os.environ.get(
    "TELEGRAM_CHAT_IDS",
    ""
).split(",")

# Manual test mode:
# true  = resend already-alerted signals
# false = normal duplicate protection

TEST_MODE = (
    os.environ.get(
        "TEST_MODE",
        "false"
    ).lower() == "true"
)


# =========================================================
# STATE
# =========================================================

def load_state():

    if not os.path.exists(STATE_FILE):

        return {
            "alerts": [],
            "alerted_symbols": [],
            "alert_message_links": {}
        }

    try:

        with open(
            STATE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            state = json.load(f)

        # Backward compatibility:
        # Old state.json may have been only a list.

        if isinstance(state, list):

            return {
                "alerts": state,
                "alerted_symbols": [],
                "alert_message_links": {}
            }

        return {
            "alerts": state.get(
                "alerts",
                []
            ),
            "alerted_symbols": state.get(
                "alerted_symbols",
                []
            ),
            "alert_message_links": state.get(
                "alert_message_links",
                {}
            )
        }

    except Exception as e:

        print(
            f"⚠️ Could not load state.json: {e}"
        )

        return {
            "alerts": [],
            "alerted_symbols": [],
            "alert_message_links": {}
        }


def save_state(state):

    with open(
        STATE_FILE,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            state,
            f,
            indent=2
        )


# =========================================================
# BINANCE API
# =========================================================

def binance_get(
    url,
    params=None
):

    for attempt in range(3):

        try:

            response = requests.get(
                url,
                params=params,
                timeout=30
            )

            response.raise_for_status()

            return response.json()

        except Exception as e:

            print(
                f"Binance request failed "
                f"(attempt {attempt + 1}/3): {e}"
            )

            if attempt < 2:

                time.sleep(
                    2 ** (attempt + 1)
                )

    return None


# =========================================================
# BINANCE SYMBOLS
# =========================================================

def get_usdt_pairs():

    url = (
        "https://data-api.binance.vision/"
        "api/v3/exchangeInfo"
    )

    data = binance_get(url)

    if not data:
        return []

    symbols = []

    for item in data.get(
        "symbols",
        []
    ):

        if (
            item.get("quoteAsset") == "USDT"
            and item.get("status") == "TRADING"
            and item.get("isSpotTradingAllowed") is True
        ):

            symbols.append(
                item["symbol"]
            )

    return sorted(symbols)


# =========================================================
# FULL HISTORICAL KLINES
# =========================================================

def get_klines(
    symbol,
    interval
):

    """
    Download ALL available Binance historical candles.

    Binance returns a maximum of 1000 candles per request,
    so we paginate from oldest to newest.

    Result:
        Oldest candle -> newest candle
    """

    url = (
        "https://data-api.binance.vision/"
        "api/v3/klines"
    )

    all_klines = []

    # Start from beginning of Binance history.
    start_time = 0

    while True:

        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": BINANCE_KLINE_PAGE_SIZE,
            "startTime": start_time
        }

        klines = binance_get(
            url,
            params
        )

        if not klines:
            break

        all_klines.extend(
            klines
        )

        # Fewer than 1000 means we reached the end.
        if len(klines) < BINANCE_KLINE_PAGE_SIZE:
            break

        # Continue after last returned candle.
        last_open_time = klines[-1][0]

        next_start_time = (
            last_open_time + 1
        )

        # Safety check
        if next_start_time <= start_time:
            break

        start_time = next_start_time

        if KLINE_PAGE_SLEEP > 0:

            time.sleep(
                KLINE_PAGE_SLEEP
            )

    if not all_klines:
        return None

    # Remove duplicate candles.
    unique = {}

    for candle in all_klines:

        open_time = candle[0]

        unique[open_time] = candle

    all_klines = [
        unique[key]
        for key in sorted(unique)
    ]

    print(
        f"📚 {symbol} {interval}: "
        f"{len(all_klines)} historical candles loaded"
    )

    return all_klines


# =========================================================
# TELEGRAM HELPERS
# =========================================================

def get_telegram_chat_type(
    chat_id
):

    """
    Ask Telegram what type of chat this is.

    Returns:
        private
        group
        supergroup
        channel
        None
    """

    if not TELEGRAM_TOKEN:
        return None

    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_TOKEN}/getChat"
    )

    try:

        response = requests.get(
            url,
            params={
                "chat_id": chat_id
            },
            timeout=20
        )

        if not response.ok:

            print(
                f"⚠️ Could not get Telegram chat type "
                f"for {chat_id}: "
                f"{response.text}"
            )

            return None

        data = response.json()

        if not data.get("ok"):
            return None

        return (
            data.get("result", {})
            .get("type")
        )

    except Exception as e:

        print(
            f"⚠️ Telegram getChat failed "
            f"for {chat_id}: {e}"
        )

        return None


def build_telegram_message_link(
    chat_id,
    message_id,
    chat_type=None
):

    """
    Build a direct Telegram message link.

    Supported direct-link case:
        supergroup

    For private chats there is no universal
    direct message URL that can be generated
    from chat_id + message_id.
    """

    if not message_id:
        return None

    if chat_type == "supergroup":

        try:

            numeric_chat_id = int(
                str(chat_id)
            )

            # Telegram supergroup IDs normally:
            # -100xxxxxxxxxx

            if str(numeric_chat_id).startswith(
                "-100"
            ):

                internal_id = str(
                    numeric_chat_id
                )[4:]

                return (
                    f"https://t.me/c/"
                    f"{internal_id}/"
                    f"{message_id}"
                )

        except Exception:
            pass

    return None


def send_telegram_to_chat(
    message,
    chat_id
):

    """
    Send one message to one Telegram chat.

    Returns:
        {
            "success": bool,
            "chat_id": str,
            "message_id": int or None,
            "chat_type": str or None,
            "message_link": str or None
        }
    """

    result = {
        "success": False,
        "chat_id": str(chat_id),
        "message_id": None,
        "chat_type": None,
        "message_link": None
    }

    if not TELEGRAM_TOKEN:

        print(
            "❌ Telegram token missing."
        )

        return result

    chat_id = str(
        chat_id
    ).strip()

    if not chat_id:
        return result

    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    try:

        response = requests.post(
            url,
            json=payload,
            timeout=20
        )

        if not response.ok:

            print(
                f"❌ Telegram error for {chat_id}:",
                response.status_code,
                response.text
            )

            return result

        data = response.json()

        if not data.get("ok"):

            print(
                f"❌ Telegram API error for {chat_id}:",
                data
            )

            return result

        message_data = data.get(
            "result",
            {}
        )

        message_id = message_data.get(
            "message_id"
        )

        # Telegram returned chat type.
        chat_data = message_data.get(
            "chat",
            {}
        )

        chat_type = chat_data.get(
            "type"
        )

        # Fallback: ask Telegram if needed.
        if not chat_type:

            chat_type = get_telegram_chat_type(
                chat_id
            )

        message_link = build_telegram_message_link(
            chat_id,
            message_id,
            chat_type
        )

        result["success"] = True
        result["message_id"] = message_id
        result["chat_type"] = chat_type
        result["message_link"] = message_link

        print(
            f"✅ Telegram sent to {chat_id}"
        )

        if message_link:

            print(
                f"🔗 Message link created: "
                f"{message_link}"
            )

        elif chat_type == "private":

            print(
                f"ℹ️ Private chat {chat_id}: "
                f"direct message link is not available"
            )

        return result

    except Exception as e:

        print(
            f"❌ Telegram request failed "
            f"for {chat_id}: {e}"
        )

        return result


def send_telegram(
    message
):

    """
    Send same message to all configured Telegram chats.

    Returns:
        True only if every configured destination
        successfully receives the message.
    """

    chat_ids = [
        chat_id.strip()
        for chat_id in TELEGRAM_CHAT_IDS
        if chat_id.strip()
    ]

    if (
        not TELEGRAM_TOKEN
        or not chat_ids
    ):

        print(
            "❌ Telegram credentials missing."
        )

        return False

    all_sent = True

    for chat_id in chat_ids:

        result = send_telegram_to_chat(
            message,
            chat_id
        )

        if not result["success"]:
            all_sent = False

    return all_sent


# =========================================================
# DATA ANALYSIS
# =========================================================

def prepare_dataframe(
    klines
):

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

    df = pd.DataFrame(
        klines,
        columns=columns
    )

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

    # Remove invalid rows.
    df = df.dropna(
        subset=[
            "open_time",
            "close_time",
            "close"
        ]
    )

    # Sort oldest -> newest.
    df = df.sort_values(
        "open_time"
    )

    # Remove duplicate candles.
    df = df.drop_duplicates(
        subset=[
            "open_time"
        ],
        keep="last"
    )

    # Remove current forming candle.
    now = pd.Timestamp.now(
        tz="UTC"
    )

    df = df[
        df["close_time"] < now
    ].copy()

    if len(df) < 205:
        return None

    # =====================================================
    # FULL-HISTORY EMA
    # =====================================================

    df["ema50"] = df["close"].ewm(
        span=50,
        adjust=False,
        min_periods=50
    ).mean()

    df["ema200"] = df["close"].ewm(
        span=200,
        adjust=False,
        min_periods=200
    ).mean()

    # Remove rows where EMA is not ready.
    df = df.dropna(
        subset=[
            "ema50",
            "ema200"
        ]
    ).copy()

    if len(df) < 2:
        return None

    return df


# =========================================================
# TREND
# =========================================================

def get_trend(
    close,
    ema50,
    ema200
):

    if close > ema50 > ema200:

        return "STRONG BULLISH"

    if (
        close > ema50
        and ema50 <= ema200
    ):

        return "BULLISH / TRANSITION"

    if close < ema50 < ema200:

        return "STRONG BEARISH"

    if (
        close < ema50
        and ema50 >= ema200
    ):

        return "BEARISH / TRANSITION"

    return "NEUTRAL"


# =========================================================
# SIGNAL DETECTION
# =========================================================

def analyze_symbol(
    symbol,
    interval
):

    klines = get_klines(
        symbol,
        interval
    )

    if not klines:
        return None

    df = prepare_dataframe(
        klines
    )

    if df is None or len(df) < 2:
        return None

    # Closed candles only.
    previous = df.iloc[-2]
    current = df.iloc[-1]

    prev_close = float(
        previous["close"]
    )

    close = float(
        current["close"]
    )

    prev_ema50 = float(
        previous["ema50"]
    )

    ema50 = float(
        current["ema50"]
    )

    prev_ema200 = float(
        previous["ema200"]
    )

    ema200 = float(
        current["ema200"]
    )

    signals = []

    # =====================================================
    # EMA 50 CROSSES ABOVE EMA 200
    # =====================================================

    if (
        prev_ema50 <= prev_ema200
        and ema50 > ema200
    ):

        signals.append(
            "EMA 50 crossed ABOVE EMA 200 🟢"
        )

    # =====================================================
    # EMA 50 CROSSES BELOW EMA 200
    # ONLY 1D
    # =====================================================

    if interval == "1d":

        if (
            prev_ema50 >= prev_ema200
            and ema50 < ema200
        ):

            signals.append(
                "EMA 50 crossed BELOW EMA 200 🔴"
            )

    # =====================================================
    # PRICE CROSSES ABOVE EMA 50
    # =====================================================

    if (
        prev_close <= prev_ema50
        and close > ema50
    ):

        signals.append(
            "Price crossed ABOVE EMA 50 🟢"
        )

    # =====================================================
    # PRICE CROSSES BELOW EMA 50
    # ONLY 1D
    # =====================================================

    if interval == "1d":

        if (
            prev_close >= prev_ema50
            and close < ema50
        ):

            signals.append(
                "Price crossed BELOW EMA 50 🔴"
            )

    # =====================================================
    # PRICE CROSSES ABOVE EMA 200
    # =====================================================

    if (
        prev_close <= prev_ema200
        and close > ema200
    ):

        signals.append(
            "Price crossed ABOVE EMA 200 🟢"
        )

    # =====================================================
    # PRICE CROSSES BELOW EMA 200
    # ONLY 1D
    # =====================================================

    if interval == "1d":

        if (
            prev_close >= prev_ema200
            and close < ema200
        ):

            signals.append(
                "Price crossed BELOW EMA 200 🔴"
            )

    # =====================================================
    # No signal
    # =====================================================

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

def get_signal_strength(
    result
):

    signals = result["signals"]
    trend = result["trend"]

    if len(signals) >= 2:

        return "🔥 STRONG SIGNAL"

    if (
        any(
            "EMA 50 crossed" in s
            for s in signals
        )
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

def format_times(
    timestamp
):

    if timestamp.tzinfo is None:

        timestamp = timestamp.replace(
            tzinfo=timezone.utc
        )

    utc_time = timestamp.astimezone(
        timezone.utc
    )

    ist_time = timestamp.astimezone(
        IST
    )

    return (
        utc_time.strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        ),
        ist_time.strftime(
            "%Y-%m-%d %H:%M:%S IST"
        )
    )


# =========================================================
# SIGNAL ID
# =========================================================

def make_signal_id(
    result
):

    candle_time = (
        result["candle_time"].isoformat()
    )

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

def get_mtf_confirmation(
    symbol,
    current_interval
):

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

    df = prepare_dataframe(
        klines
    )

    if df is None:
        return None

    current = df.iloc[-1]

    close = float(
        current["close"]
    )

    ema50 = float(
        current["ema50"]
    )

    ema200 = float(
        current["ema200"]
    )

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

    strength = get_signal_strength(
        result
    )

    utc_time, ist_time = format_times(
        result["candle_time"]
    )

    distance_50 = (
        (close - ema50)
        / ema50
    ) * 100

    distance_200 = (
        (close - ema200)
        / ema200
    ) * 100

    if interval == "4h":
        timeframe_text = "4H"
    else:
        timeframe_text = "1D"

    lines = []

    lines.append(
        "🚨 BINANCE EMA ALERT"
    )

    lines.append("")

    lines.append(
        f"🪙 Coin: {escape(symbol)}"
    )

    lines.append(
        f"⏱ Timeframe: {timeframe_text}"
    )

    if previously_alerted:

        lines.append("")

        lines.append(
            "🔁 PREVIOUSLY ALERTED COIN"
        )

    lines.append("")

    lines.append(
        f"📊 Signal Strength: {escape(strength)}"
    )

    lines.append("")

    lines.append(
        "📢 SIGNALS:"
    )

    for signal in signals:

        lines.append(
            f"• {escape(signal)}"
        )

    lines.append("")

    lines.append(
        f"📈 Trend: {escape(trend)}"
    )

    lines.append("")

    lines.append(
        "💰 PRICE / EMA"
    )

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

    # =====================================================
    # MTF
    # =====================================================

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
            f"{escape(mtf['trend'])}"
        )

    lines.append("")

    lines.append(
        "🕯 CANDLE CLOSE"
    )

    lines.append(
        f"UTC: {utc_time}"
    )

    lines.append(
        f"IST: {ist_time}"
    )

    lines.append("")

    lines.append(
        "✅ CLOSED CANDLE — CONFIRMED"
    )

    lines.append("")

    # =====================================================
    # Binance Chart
    # =====================================================

    chart_url = (
        "https://www.binance.com/en/trade/"
        f"{symbol}?type=spot"
    )

    lines.append(
        f'🔗 Binance Chart: '
        f'<a href="{chart_url}">Open Chart</a>'
    )

    return "\n".join(lines)


# =========================================================
# SUMMARY HELPERS
# =========================================================

def make_summary_pair_text(
    symbols,
    chat_id,
    message_links
):

    """
    Create summary pair list.

    For supergroup:
        Pair name becomes clickable and points to
        the original alert message.

    For private chat:
        Pair name remains plain text.
    """

    if not symbols:
        return "• None"

    output = []

    for symbol in symbols:

        signal_ids = []

        # Find signal IDs belonging to this symbol.
        for signal_id in message_links:

            if signal_id.startswith(
                f"{symbol}|"
            ):

                signal_ids.append(
                    signal_id
                )

        link = None

        # Find first available message link
        # for this symbol in current destination.

        for signal_id in signal_ids:

            chat_links = message_links.get(
                signal_id,
                {}
            )

            link = chat_links.get(
                str(chat_id)
            )

            if link:
                break

        if link:

            output.append(
                f'<a href="{escape(link)}">'
                f'{escape(symbol)}'
                f'</a>'
            )

        else:

            output.append(
                escape(symbol)
            )

    return "• " + " • ".join(
        output
    )


# =========================================================
# SUMMARY
# =========================================================

def build_summary_message(
    results,
    interval,
    new_alert_symbols,
    chat_id,
    message_links
):

    timeframe = (
        "4H"
        if interval == "4h"
        else "1D"
    )

    bullish_symbols = []
    bearish_symbols = []
    strong_coins = []

    for result in results:

        symbol = result["symbol"]

        # Bullish
        if any(
            "🟢" in signal
            for signal in result["signals"]
        ):

            bullish_symbols.append(
                symbol
            )

        # Bearish
        if any(
            "🔴" in signal
            for signal in result["signals"]
        ):

            bearish_symbols.append(
                symbol
            )

        # Strong
        if (
            "STRONG"
            in get_signal_strength(
                result
            )
        ):

            strong_coins.append(
                symbol
            )

    # Remove duplicates.

    new_alert_symbols = list(
        dict.fromkeys(
            new_alert_symbols
        )
    )

    bullish_symbols = list(
        dict.fromkeys(
            bullish_symbols
        )
    )

    bearish_symbols = list(
        dict.fromkeys(
            bearish_symbols
        )
    )

    strong_coins = list(
        dict.fromkeys(
            strong_coins
        )
    )

    lines = []

    lines.append(
        f"📋 BINANCE {timeframe} SCAN SUMMARY"
    )

    # =====================================================
    # Total
    # =====================================================

    lines.append("")

    lines.append(
        f"🔎 Signals found: "
        f"{len(results)}"
    )

    # =====================================================
    # New alerts
    # =====================================================

    lines.append("")

    lines.append(
        f"🆕 New alerts sent: "
        f"{len(new_alert_symbols)}"
    )

    lines.append(
        make_summary_pair_text(
            new_alert_symbols,
            chat_id,
            message_links
        )
    )

    # =====================================================
    # Bullish
    # =====================================================

    lines.append("")

    lines.append(
        f"🟢 Bullish signals: "
        f"{len(bullish_symbols)}"
    )

    lines.append(
        make_summary_pair_text(
            bullish_symbols,
            chat_id,
            message_links
        )
    )

    # =====================================================
    # Bearish
    # =====================================================

    lines.append("")

    lines.append(
        f"🔴 Bearish signals: "
        f"{len(bearish_symbols)}"
    )

    lines.append(
        make_summary_pair_text(
            bearish_symbols,
            chat_id,
            message_links
        )
    )

    # =====================================================
    # Strong signals
    # =====================================================

    if strong_coins:

        lines.append("")

        lines.append(
            "🔥 STRONG SIGNALS:"
        )

        for coin in strong_coins[:20]:

            lines.append(
                make_summary_pair_text(
                    [coin],
                    chat_id,
                    message_links
                )
            )

        if len(strong_coins) > 20:

            lines.append(
                f"• +"
                f"{len(strong_coins) - 20}"
                f" more"
            )

    return "\n".join(
        lines
    )


def send_summary(
    results,
    interval,
    new_alert_symbols,
    message_links
):

    if not results:
        return

    chat_ids = [
        chat_id.strip()
        for chat_id in TELEGRAM_CHAT_IDS
        if chat_id.strip()
    ]

    if not chat_ids:
        return

    # Send separate summary to each destination.
    #
    # Group message links and private chat
    # destinations are different.

    for chat_id in chat_ids:

        summary = build_summary_message(
            results,
            interval,
            new_alert_symbols,
            chat_id,
            message_links
        )

        print("")

        print(
            f"📋 Sending summary to {chat_id}"
        )

        result = send_telegram_to_chat(
            summary,
            chat_id
        )

        if result["success"]:

            print(
                f"✅ Summary sent to {chat_id}"
            )

        else:

            print(
                f"❌ Summary failed for {chat_id}"
            )


# =========================================================
# SCAN
# =========================================================

def scan(
    interval
):

    print("")

    print("=" * 60)

    print(
        f"🚀 Starting Binance EMA Scanner: "
        f"{interval.upper()}"
    )

    print("=" * 60)

    print(
        "📚 EMA calculation mode: "
        "FULL HISTORICAL CANDLES"
    )

    print(
        "🔒 Signal mode: "
        "CLOSED CANDLES ONLY"
    )

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

    # Stores Telegram message links.
    #
    # {
    #   signal_id: {
    #       chat_id: message_link
    #   }
    # }

    message_links = state.get(
        "alert_message_links",
        {}
    )

    if not isinstance(
        message_links,
        dict
    ):

        message_links = {}

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

    # =====================================================
    # PARALLEL SCAN
    # =====================================================

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

        for future in as_completed(
            futures
        ):

            symbol = futures[future]

            try:

                result = future.result()

                if result:

                    results.append(
                        result
                    )

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

    # =====================================================
    # NEW ALERT SYMBOLS
    # =====================================================

    new_alert_symbols = []

    # =====================================================
    # PROCESS ALERTS
    # =====================================================

    for result in results:

        signal_id = make_signal_id(
            result
        )

        # Duplicate protection

        if (
            signal_id in alerts
            and not TEST_MODE
        ):

            continue

        previously_alerted = (
            result["symbol"]
            in alerted_symbols
        )

        # =================================================
        # MTF
        # =================================================

        mtf = get_mtf_confirmation(
            result["symbol"],
            interval
        )

        # =================================================
        # Message
        # =================================================

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

        # Send individually to each configured chat.

        chat_ids = [
            chat_id.strip()
            for chat_id in TELEGRAM_CHAT_IDS
            if chat_id.strip()
        ]

        all_sent = True

        for chat_id in chat_ids:

            telegram_result = (
                send_telegram_to_chat(
                    message,
                    chat_id
                )
            )

            if telegram_result["success"]:

                # Save direct message link
                # if Telegram supports it.

                if telegram_result["message_link"]:

                    if signal_id not in message_links:

                        message_links[
                            signal_id
                        ] = {}

                    message_links[
                        signal_id
                    ][chat_id] = (
                        telegram_result[
                            "message_link"
                        ]
                    )

            else:

                all_sent = False

        # =================================================
        # Mark successful only when EVERY destination
        # received the alert.
        # =================================================

        if (
            all_sent
            and chat_ids
        ):

            new_alert_symbols.append(
                result["symbol"]
            )

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

    # =====================================================
    # SUMMARY
    # =====================================================

    if results:

        send_summary(
            results,
            interval,
            new_alert_symbols,
            message_links
        )

    # =====================================================
    # SAVE STATE
    # =====================================================

    state["alerts"] = alerts

    state["alerted_symbols"] = sorted(
        alerted_symbols
    )

    state["alert_message_links"] = (
        message_links
    )

    save_state(
        state
    )

    print("")

    print("=" * 60)

    print(
        f"✅ Scan completed: "
        f"{interval.upper()}"
    )

    print(
        f"📨 New alerts sent: "
        f"{len(new_alert_symbols)}"
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

    timeframe = (
        sys.argv[1].lower()
    )

    if timeframe not in [
        "4h",
        "1d"
    ]:

        print(
            "❌ Invalid timeframe."
        )

        print(
            "Use 4h or 1d."
        )

        sys.exit(1)

    scan(
        timeframe
    )
