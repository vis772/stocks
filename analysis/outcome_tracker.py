# analysis/outcome_tracker.py
# Background thread — runs hourly during market hours (9:30-16:00 ET).
# Fetches post-signal prices and fills in 1hr/1day/5day outcomes.
#
# Price source priority:
#   1. Finnhub REST  (/quote for live, /stock/candle for history)
#   2. Tiingo WebSocket stream (lazy import from scanner_loop)
#   3. yfinance (last resort — no new dependency, already in requirements)

import os
import time
import threading
import requests
from datetime import datetime, timedelta
from typing import Optional

FINNHUB_BASE = "https://finnhub.io/api/v1"


def _now_et():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except ImportError:
        import pytz
        return datetime.now(pytz.timezone("America/New_York"))


def _is_market_hours() -> bool:
    et = _now_et()
    if et.weekday() >= 5:
        return False
    after_open   = (et.hour > 9)  or (et.hour == 9  and et.minute >= 30)
    before_close = (et.hour < 16) or (et.hour == 16 and et.minute == 0)
    return after_open and before_close


def _fh_key() -> str:
    return os.environ.get("FINNHUB_API_KEY", "")


def _fetch_price_now(ticker: str) -> Optional[float]:
    """Live price: Finnhub quote → Tiingo stream → yfinance."""
    # 1. Finnhub /quote
    key = _fh_key()
    if key:
        try:
            resp = requests.get(
                f"{FINNHUB_BASE}/quote",
                params={"symbol": ticker, "token": key},
                timeout=8,
            )
            if resp.status_code == 200:
                data  = resp.json()
                price = data.get("c", 0)
                if price and price > 0:
                    return float(price)
        except Exception as e:
            print(f"  [outcome_tracker] Finnhub quote failed for {ticker}: {e}")

    # 2. Tiingo stream — lazy import to avoid circular dependency at module load
    try:
        import scanner_loop as _sl
        stream = getattr(_sl, "_stream", None)
        if stream is not None and stream.get_age_ms(ticker) < 30_000:
            tick = stream.get_last(ticker)
            if tick and tick.get("price"):
                return float(tick["price"])
    except Exception:
        pass

    # 3. yfinance fallback
    try:
        import yfinance as yf
        hist = yf.Ticker(ticker).history(period="1d", interval="5m")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception as e:
        print(f"  [outcome_tracker] yfinance live failed for {ticker}: {e}")

    return None


def _fetch_close_on_day(ticker: str, target_dt: datetime) -> Optional[float]:
    """
    Closing price on (or just after) target_dt.
    Uses Finnhub daily candles; falls back to yfinance history.
    """
    # Convert target_dt to unix timestamps for a ±2-day window
    start_ts = int((target_dt - timedelta(days=1)).timestamp())
    end_ts   = int((target_dt + timedelta(days=2)).timestamp())
    target_str = target_dt.strftime("%Y-%m-%d")

    # 1. Finnhub /stock/candle (resolution=D)
    key = _fh_key()
    if key:
        try:
            resp = requests.get(
                f"{FINNHUB_BASE}/stock/candle",
                params={"symbol": ticker, "resolution": "D",
                        "from": start_ts, "to": end_ts, "token": key},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("s") == "ok" and data.get("c"):
                    closes = data["c"]
                    timestamps = data["t"]
                    # Find the candle closest to (but not before) target_dt
                    best_price = None
                    best_diff  = float("inf")
                    for ts, close in zip(timestamps, closes):
                        candle_date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                        diff = abs((datetime.strptime(candle_date, "%Y-%m-%d")
                                    - datetime.strptime(target_str, "%Y-%m-%d")).days)
                        if diff < best_diff:
                            best_diff  = diff
                            best_price = close
                    if best_price and best_price > 0:
                        return float(best_price)
        except Exception as e:
            print(f"  [outcome_tracker] Finnhub candle failed for {ticker}: {e}")

    # 2. yfinance fallback for historical closes
    try:
        import yfinance as yf
        start = (target_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        end   = (target_dt + timedelta(days=2)).strftime("%Y-%m-%d")
        hist  = yf.Ticker(ticker).history(start=start, end=end)
        if not hist.empty:
            dates = [str(d)[:10] for d in hist.index]
            future = [d for d in dates if d >= target_str]
            idx    = dates.index(future[0]) if future else -1
            return float(hist["Close"].iloc[idx])
    except Exception as e:
        print(f"  [outcome_tracker] yfinance history failed for {ticker}: {e}")

    return None


def _parse_created_at(val) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.replace(tzinfo=None)
    s = str(val)
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt)
        except ValueError:
            continue
    return None


def run_one_pass():
    """Check all pending signals and fill whichever time buckets are ready."""
    try:
        from db.database import get_pending_signals, update_signal_outcome
    except Exception as e:
        print(f"  [outcome_tracker] DB import failed: {e}")
        return

    pending = get_pending_signals(max_age_days=8)
    if not pending:
        return

    now = datetime.utcnow()
    print(f"  [outcome_tracker] Checking {len(pending)} pending signal(s)")

    for sig in pending:
        signal_id   = sig["id"]
        ticker      = sig["ticker"]
        entry_price = sig.get("price_at_signal") or 0
        created_at  = _parse_created_at(sig.get("created_at"))

        if not entry_price or not created_at:
            continue

        age_hrs = (now - created_at).total_seconds() / 3600

        price_1hr  = sig.get("price_1hr")
        price_1day = sig.get("price_1day")
        price_5day = sig.get("price_5day")

        new_1hr  = None
        new_1day = None
        new_5day = None

        # 1hr: fill once ≥1 hour old; use live price if still same session
        if price_1hr is None and age_hrs >= 1.0:
            if age_hrs < 8:
                new_1hr = _fetch_price_now(ticker)
            else:
                new_1hr = _fetch_close_on_day(ticker, created_at + timedelta(hours=1))

        # 1day: fill once ≥24 hours old
        if price_1day is None and age_hrs >= 24:
            new_1day = _fetch_close_on_day(ticker, created_at + timedelta(days=1))

        # 5day: fill once ≥5 calendar days old
        if price_5day is None and age_hrs >= 5 * 24:
            new_5day = _fetch_close_on_day(ticker, created_at + timedelta(days=5))

        if new_1hr or new_1day or new_5day:
            update_signal_outcome(
                signal_id       = signal_id,
                price_1hr       = new_1hr   or price_1hr,
                price_1day      = new_1day  or price_1day,
                price_5day      = new_5day  or price_5day,
                price_at_signal = entry_price,
            )
            print(f"  [outcome_tracker] {ticker} updated: "
                  f"1hr={new_1hr} 1day={new_1day} 5day={new_5day}")

        time.sleep(0.5)  # avoid hammering Finnhub rate limit


def _tracker_loop():
    """Run hourly during market hours; sleep 10 min otherwise."""
    while True:
        try:
            if _is_market_hours():
                run_one_pass()
                time.sleep(3600)
            else:
                time.sleep(600)
        except Exception as e:
            print(f"  [outcome_tracker] loop error: {e}")
            time.sleep(300)


def start_outcome_tracker():
    """Start the outcome-tracking background thread. Call once at scanner startup."""
    thread = threading.Thread(target=_tracker_loop, daemon=True, name="outcome_tracker")
    thread.start()
    print("  ✓ Outcome tracker started — fills 1hr/1day/5day prices hourly during market hours")
    return thread
