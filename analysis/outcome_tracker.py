# analysis/outcome_tracker.py
# Background thread — runs hourly during market hours (9:30-16:00 ET).
# Fetches post-signal prices via yfinance and fills in 1hr/1day/5day outcomes.

import threading
import time
from datetime import datetime, timedelta
from typing import Optional


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


def _fetch_price_now(ticker: str) -> Optional[float]:
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        info = t.fast_info
        price = getattr(info, "last_price", None)
        if price and price > 0:
            return float(price)
        hist = t.history(period="1d", interval="5m")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception as e:
        print(f"  [outcome_tracker] price fetch failed for {ticker}: {e}")
    return None


def _fetch_close_on_day(ticker: str, target_date: datetime) -> Optional[float]:
    """Return the closing price on the closest trading day to target_date."""
    try:
        import yfinance as yf
        start = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
        end   = (target_date + timedelta(days=2)).strftime("%Y-%m-%d")
        hist  = yf.Ticker(ticker).history(start=start, end=end)
        if hist.empty:
            return None
        # Find the row whose date is closest to target_date
        target_str = target_date.strftime("%Y-%m-%d")
        dates = [str(d)[:10] for d in hist.index]
        if target_str in dates:
            idx = dates.index(target_str)
        else:
            # Closest date that is >= target (next trading day)
            future = [d for d in dates if d >= target_str]
            if not future:
                return None
            idx = dates.index(future[0])
        return float(hist["Close"].iloc[idx])
    except Exception as e:
        print(f"  [outcome_tracker] history fetch failed for {ticker}: {e}")
    return None


def _parse_created_at(val) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.replace(tzinfo=None)
    try:
        s = str(val)
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(s[:19], fmt[:len(fmt)])
            except ValueError:
                continue
    except Exception:
        pass
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
        signal_id      = sig["id"]
        ticker         = sig["ticker"]
        entry_price    = sig.get("price_at_signal") or 0
        created_at     = _parse_created_at(sig.get("created_at"))

        if not entry_price or not created_at:
            continue

        age_hrs  = (now - created_at).total_seconds() / 3600

        price_1hr  = sig.get("price_1hr")
        price_1day = sig.get("price_1day")
        price_5day = sig.get("price_5day")

        new_1hr  = None
        new_1day = None
        new_5day = None

        # 1hr bucket: fill once the signal is at least 1 hour old
        if price_1hr is None and age_hrs >= 1.0:
            if age_hrs < 8:
                new_1hr = _fetch_price_now(ticker)
            else:
                target = created_at + timedelta(hours=1)
                new_1hr = _fetch_close_on_day(ticker, target)

        # 1day bucket: fill once the signal is at least 1 calendar day old
        if price_1day is None and age_hrs >= 24:
            target = created_at + timedelta(days=1)
            new_1day = _fetch_close_on_day(ticker, target)

        # 5day bucket: fill once the signal is at least 5 calendar days old
        if price_5day is None and age_hrs >= 5 * 24:
            target = created_at + timedelta(days=5)
            new_5day = _fetch_close_on_day(ticker, target)

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

        # Rate-limit to avoid hammering yfinance
        time.sleep(0.5)


def _tracker_loop():
    """Run hourly during market hours; sleep 10 min otherwise to re-check."""
    while True:
        try:
            if _is_market_hours():
                run_one_pass()
                time.sleep(3600)   # next run in 1 hour
            else:
                time.sleep(600)    # check again in 10 min
        except Exception as e:
            print(f"  [outcome_tracker] loop error: {e}")
            time.sleep(300)


def start_outcome_tracker():
    """Start the outcome-tracking background thread. Call once at scanner startup."""
    thread = threading.Thread(target=_tracker_loop, daemon=True, name="outcome_tracker")
    thread.start()
    print("  ✓ Outcome tracker started — fills 1hr/1day/5day prices hourly during market hours")
    return thread
