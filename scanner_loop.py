# scanner_loop.py
# The real-time background scanner.
# Runs continuously during market hours, scanning the dynamic watchlist
# every 60 seconds and sending phone alerts for significant events.
#
# This runs as a SEPARATE PROCESS alongside the Streamlit dashboard.
# Railway runs both via the Procfile.

import os
import time
import json
import requests
import feedparser
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set
import sqlite3

from alerts import (
    alert_volume_spike, alert_price_move,
    alert_sec_filing, alert_news,
    alert_morning_brief, alert_digest,
    PRIORITY_HIGH, PRIORITY_NORMAL,
    send_alert
)
from morning_screen import build_todays_watchlist, load_todays_watchlist, WATCHLIST_FILE

FINNHUB_BASE = "https://finnhub.io/api/v1"
STATE_FILE   = "scanner_state.json"

# ─── Alert thresholds ─────────────────────────────────────────────────────────
VOLUME_SPIKE_THRESHOLD  = 2.5   # Alert if volume > 2.5x average
PRICE_MOVE_THRESHOLD    = 5.0   # Alert if price moves > 5% in one scan cycle
PRICE_MOVE_10MIN        = 8.0   # Alert if price moves > 8% in 10 minutes
GAP_UP_THRESHOLD        = 4.0   # Alert on gap-up > 4%
SEC_LOOKBACK_HOURS      = 2     # Check for SEC filings in last 2 hours

# ─── Market hours (ET) ────────────────────────────────────────────────────────
MARKET_OPEN_HOUR    = 9
MARKET_OPEN_MIN     = 25   # Start scanning 5 min before open
MARKET_CLOSE_HOUR   = 16
MARKET_CLOSE_MIN    = 30   # Scan 30 min after close for after-hours

PREMARKET_HOUR      = 6    # Morning screen time
SCAN_INTERVAL_SEC   = 60   # Scan every 60 seconds during market hours
AFTERHOURS_INTERVAL = 300  # Every 5 minutes after hours


def _fh_key() -> str:
    return os.environ.get("FINNHUB_API_KEY", "")


def _fh_get(endpoint: str, params: dict) -> Optional[dict]:
    key = _fh_key()
    if not key:
        return None
    try:
        params["token"] = key
        resp = requests.get(f"{FINNHUB_BASE}/{endpoint}", params=params, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            return data if data else None
        return None
    except Exception:
        return None


# ─── State management ─────────────────────────────────────────────────────────
# Tracks what we've already alerted on to avoid duplicate notifications

class ScannerState:
    def __init__(self):
        self.alerted_today: Set[str] = set()  # "TICKER_event_type" keys
        self.last_prices:   Dict[str, float] = {}
        self.last_volumes:  Dict[str, float] = {}
        self.price_at_10min: Dict[str, float] = {}
        self.alert_log:     List[str] = []
        self.scan_count:    int = 0
        self.last_sec_check: datetime = datetime.now() - timedelta(hours=3)
        self.known_filings: Set[str] = set()  # accession numbers seen
        self._load()

    def _load(self):
        try:
            with open(STATE_FILE) as f:
                data = json.load(f)
            today = datetime.now().strftime("%Y-%m-%d")
            if data.get("date") == today:
                self.alerted_today = set(data.get("alerted_today", []))
                self.known_filings = set(data.get("known_filings", []))
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    def save(self):
        try:
            with open(STATE_FILE, "w") as f:
                json.dump({
                    "date":          datetime.now().strftime("%Y-%m-%d"),
                    "alerted_today": list(self.alerted_today),
                    "known_filings": list(self.known_filings),
                    "scan_count":    self.scan_count,
                    "last_updated":  datetime.now().isoformat(),
                }, f, indent=2)
        except Exception:
            pass

    def already_alerted(self, key: str) -> bool:
        return key in self.alerted_today

    def mark_alerted(self, key: str):
        self.alerted_today.add(key)
        self.save()

    def log_alert(self, msg: str):
        self.alert_log.append(f"{datetime.now().strftime('%H:%M')} {msg}")
        # Keep last 50 alerts
        if len(self.alert_log) > 50:
            self.alert_log = self.alert_log[-50:]
        # Write to shared state file so dashboard can show it
        try:
            with open("alert_log.json", "w") as f:
                json.dump(self.alert_log, f)
        except Exception:
            pass


# ─── Single ticker scan ────────────────────────────────────────────────────────

def scan_one_ticker(ticker: str, state: ScannerState) -> List[str]:
    """
    Scan a single ticker and fire alerts if thresholds are met.
    Returns list of alert messages fired (for digest).
    """
    fired = []

    try:
        # Get real-time quote from Finnhub
        quote = _fh_get("quote", {"symbol": ticker})
        if not quote or not quote.get("c") or quote["c"] <= 0:
            return fired

        price      = quote["c"]
        prev_close = quote["pc"]
        volume     = quote.get("v", 0)

        if prev_close <= 0:
            return fired

        change_pct = (price - prev_close) / prev_close * 100

        # ── Volume spike ─────────────────────────────────────────────────────
        last_vol = state.last_volumes.get(ticker, 0)
        if last_vol > 0 and volume > 0:
            # Compare current volume rate vs recent rate
            vol_ratio = volume / max(last_vol, 1)
            # Approximate relative volume vs typical
            # We estimate based on time of day
            hour = datetime.now().hour
            minutes_into_day = max(1, (hour - 9) * 60 + datetime.now().minute - 30)
            expected_vol_pct = min(minutes_into_day / 390, 1.0)  # 390 min trading day
            if expected_vol_pct > 0.05:
                annualized_rvol = (volume / expected_vol_pct) / max(last_vol / 0.5, 1)
                if annualized_rvol > VOLUME_SPIKE_THRESHOLD:
                    key = f"{ticker}_volume_{datetime.now().strftime('%Y-%m-%d-%H')}"
                    if not state.already_alerted(key):
                        alert_volume_spike(ticker, annualized_rvol, price, change_pct)
                        state.mark_alerted(key)
                        msg = f"⚡ {ticker} volume spike {annualized_rvol:.1f}x"
                        state.log_alert(msg)
                        fired.append(msg)

        state.last_volumes[ticker] = volume

        # ── Price move since last scan ────────────────────────────────────────
        last_price = state.last_prices.get(ticker)
        if last_price and last_price > 0:
            move_pct = (price - last_price) / last_price * 100
            if abs(move_pct) >= PRICE_MOVE_THRESHOLD:
                key = f"{ticker}_price_{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
                if not state.already_alerted(key):
                    alert_price_move(ticker, price, move_pct, "1min")
                    state.mark_alerted(key)
                    msg = f"{'🟢' if move_pct > 0 else '🔴'} {ticker} {move_pct:+.1f}% in 1min"
                    state.log_alert(msg)
                    fired.append(msg)

        # ── 10-minute price move ───────────────────────────────────────────────
        price_10min_ago = state.price_at_10min.get(ticker)
        if price_10min_ago and price_10min_ago > 0:
            move_10min = (price - price_10min_ago) / price_10min_ago * 100
            if abs(move_10min) >= PRICE_MOVE_10MIN:
                key = f"{ticker}_10min_{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
                if not state.already_alerted(key):
                    alert_price_move(ticker, price, move_10min, "10min")
                    state.mark_alerted(key)
                    msg = f"{'🔥' if move_10min > 0 else '🔴'} {ticker} {move_10min:+.1f}% in 10min"
                    state.log_alert(msg)
                    fired.append(msg)

        # Update price tracking
        state.last_prices[ticker] = price
        # Rotate 10-min price every ~10 scans
        if state.scan_count % 10 == 0:
            state.price_at_10min[ticker] = price

        # ── Daily gap check (only do once per day) ────────────────────────────
        gap_key = f"{ticker}_gap_{datetime.now().strftime('%Y-%m-%d')}"
        if not state.already_alerted(gap_key):
            if change_pct >= GAP_UP_THRESHOLD:
                send_alert(
                    title=f"🚀 {ticker} Gap-Up {change_pct:+.1f}%",
                    message=(
                        f"Price: ${price:.4f}\n"
                        f"Gap: {change_pct:+.1f}% from yesterday's close\n"
                        f"Time: {datetime.now().strftime('%H:%M ET')}"
                    ),
                    priority=PRIORITY_HIGH,
                    url=f"https://finance.yahoo.com/quote/{ticker}",
                )
                state.mark_alerted(gap_key)
                msg = f"🚀 {ticker} gap-up {change_pct:+.1f}%"
                state.log_alert(msg)
                fired.append(msg)
            elif change_pct <= -GAP_UP_THRESHOLD:
                send_alert(
                    title=f"📉 {ticker} Gap-Down {change_pct:.1f}%",
                    message=(
                        f"Price: ${price:.4f}\n"
                        f"Gap: {change_pct:.1f}% from yesterday's close\n"
                        f"Time: {datetime.now().strftime('%H:%M ET')}"
                    ),
                    priority=PRIORITY_NORMAL,
                    url=f"https://finance.yahoo.com/quote/{ticker}",
                )
                state.mark_alerted(gap_key)

        # ── News check ────────────────────────────────────────────────────────
        today     = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        news = _fh_get("company-news", {"symbol": ticker, "from": yesterday, "to": today})

        if news:
            # Check for articles in the last 30 minutes
            cutoff = datetime.now() - timedelta(minutes=30)
            for article in news[:5]:
                pub_time = article.get("datetime", 0)
                if pub_time:
                    pub_dt = datetime.fromtimestamp(pub_time)
                    if pub_dt > cutoff:
                        headline = article.get("headline", "")
                        art_key  = f"{ticker}_news_{pub_time}"
                        if headline and not state.already_alerted(art_key):
                            # Basic sentiment
                            text = headline.lower()
                            positive_words = ["surge","soar","beat","win","launch","partner","contract","buy","upgrade"]
                            negative_words = ["fall","drop","miss","loss","warning","sell","downgrade","lawsuit","concern"]
                            pos = sum(1 for w in positive_words if w in text)
                            neg = sum(1 for w in negative_words if w in text)
                            sentiment = "positive" if pos > neg else "negative" if neg > pos else "neutral"

                            alert_news(ticker, headline, sentiment, price)
                            state.mark_alerted(art_key)
                            msg = f"📰 {ticker} news: {headline[:60]}..."
                            state.log_alert(msg)
                            fired.append(msg)
                            break  # One news alert per ticker per scan

    except Exception as e:
        pass  # Never crash the scanner on a single ticker failure

    return fired


# ─── SEC Filing Monitor ────────────────────────────────────────────────────────

def check_sec_filings(watchlist: List[str], state: ScannerState):
    """
    Check EDGAR for new filings from any company in our watchlist.
    Uses the EDGAR recent filings RSS feed.
    """
    try:
        # High-value filing types to alert on
        priority_forms = {"8-K", "S-3", "424B3", "424B4", "SC 13D"}

        feed = feedparser.parse(
            "https://www.sec.gov/cgi-bin/browse-edgar"
            "?action=getcurrent&type=8-K&dateb=&owner=include&count=40&output=atom"
        )

        ticker_set = set(t.upper() for t in watchlist)

        for entry in feed.entries[:20]:
            title     = entry.get("title", "")
            link      = entry.get("link", "")
            summary   = entry.get("summary", "")
            accession = re.search(r"(\d{10}-\d{2}-\d{6})", link)
            acc_id    = accession.group(1) if accession else link

            # Skip if we've already seen this filing
            if acc_id in state.known_filings:
                continue

            state.known_filings.add(acc_id)

            # Check if this filing is for one of our watchlist stocks
            title_upper = title.upper()
            for ticker in ticker_set:
                if ticker in title_upper or ticker in summary.upper():
                    filing_key = f"{ticker}_filing_{acc_id}"
                    if not state.already_alerted(filing_key):
                        # Determine form type
                        form_type = "8-K"
                        for ft in priority_forms:
                            if ft in title_upper:
                                form_type = ft
                                break

                        alert_sec_filing(
                            ticker=ticker,
                            form_type=form_type,
                            days_ago=0,
                            filing_url=link,
                            summary=f"New {form_type} detected for {ticker}. Review immediately.",
                        )
                        state.mark_alerted(filing_key)
                        msg = f"📋 {ticker} new {form_type} filing"
                        state.log_alert(msg)
                        break

        state.last_sec_check = datetime.now()

    except Exception as e:
        print(f"  [scanner] SEC check failed: {e}")


# ─── Market hours check ────────────────────────────────────────────────────────

def is_market_hours() -> bool:
    now = datetime.now()
    # Skip weekends
    if now.weekday() >= 5:
        return False
    h, m = now.hour, now.minute
    after_open  = (h > MARKET_OPEN_HOUR) or (h == MARKET_OPEN_HOUR and m >= MARKET_OPEN_MIN)
    before_close = (h < MARKET_CLOSE_HOUR) or (h == MARKET_CLOSE_HOUR and m <= MARKET_CLOSE_MIN)
    return after_open and before_close


def is_premarket() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    return PREMARKET_HOUR <= now.hour < MARKET_OPEN_HOUR


def is_morning_screen_time() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    return now.hour == PREMARKET_HOUR and now.minute < 10


# ─── Main scanner loop ────────────────────────────────────────────────────────

def run_scanner():
    """
    Main infinite loop. This runs forever on Railway.
    """
    print("\n" + "="*60)
    print("APEX Real-Time Scanner Starting...")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S ET')}")
    print("="*60 + "\n")

    state = ScannerState()
    watchlist: List[str] = []
    morning_brief_sent = False
    last_digest_time   = datetime.now()

    # Send startup notification
    send_alert(
        title="⚡ APEX Scanner Started",
        message=(
            f"Real-time scanner is now running on Railway.\n"
            f"Started at: {datetime.now().strftime('%H:%M ET')}\n"
            f"Scanning every 60 seconds during market hours."
        ),
        priority=PRIORITY_NORMAL,
    )

    while True:
        try:
            now = datetime.now()

            # ── Morning screen at 6 AM ────────────────────────────────────────
            if is_morning_screen_time() and not watchlist:
                print("\n[MORNING SCREEN] Building today's watchlist...")
                watchlist = build_todays_watchlist(max_stocks=50)
                morning_brief_sent = False

            # ── Load watchlist if empty ────────────────────────────────────────
            if not watchlist:
                watchlist = load_todays_watchlist()
                if not watchlist:
                    from config import DEFAULT_UNIVERSE
                    watchlist = DEFAULT_UNIVERSE
                    print(f"  Using default universe: {len(watchlist)} stocks")

            # ── Morning brief at 9:25 AM ───────────────────────────────────────
            if (now.hour == 8 and now.minute >= 30 and not morning_brief_sent and watchlist):
                try:
                    with open(WATCHLIST_FILE) as f:
                        wl_data = json.load(f)
                    gap_ups   = wl_data.get("stats", {}).get("gap_ups", [])
                    n_filings = len(wl_data.get("stats", {}).get("new_filings", []))
                except Exception:
                    gap_ups   = []
                    n_filings = 0

                alert_morning_brief(watchlist, n_filings, gap_ups)
                morning_brief_sent = True
                state.log_alert(f"☀️ Morning brief sent — {len(watchlist)} stocks on watchlist")

            # ── Market hours scan ─────────────────────────────────────────────
            if is_market_hours():
                state.scan_count += 1
                print(f"\n[SCAN #{state.scan_count}] {now.strftime('%H:%M:%S')} — {len(watchlist)} stocks")

                all_alerts = []

                # Check SEC filings every 5 minutes
                sec_elapsed = (now - state.last_sec_check).total_seconds()
                if sec_elapsed >= 300:
                    print("  Checking SEC EDGAR for new filings...")
                    check_sec_filings(watchlist, state)

                # Scan all tickers in parallel
                with ThreadPoolExecutor(max_workers=15) as executor:
                    futures = {
                        executor.submit(scan_one_ticker, ticker, state): ticker
                        for ticker in watchlist
                    }
                    for future in as_completed(futures, timeout=45):
                        try:
                            alerts = future.result()
                            all_alerts.extend(alerts)
                        except Exception:
                            pass

                if all_alerts:
                    print(f"  🔔 {len(all_alerts)} alert(s) fired this scan")
                else:
                    print(f"  ✓ No alerts — market quiet")

                # ── 30-min digest ─────────────────────────────────────────────
                digest_elapsed = (now - last_digest_time).total_seconds()
                if digest_elapsed >= 1800 and state.alert_log:
                    recent_alerts = [a for a in state.alert_log[-10:]]
                    if recent_alerts:
                        alert_digest(recent_alerts)
                    last_digest_time = now

                # Wait until next scan
                time.sleep(SCAN_INTERVAL_SEC)

            # ── After hours / pre-market ──────────────────────────────────────
            elif is_premarket():
                # Light scan during pre-market
                print(f"  [PRE-MARKET] {now.strftime('%H:%M')} — light scan")

                # Just check SEC filings during pre-market
                if watchlist:
                    sec_elapsed = (now - state.last_sec_check).total_seconds()
                    if sec_elapsed >= 600:  # Every 10 min pre-market
                        check_sec_filings(watchlist, state)

                time.sleep(AFTERHOURS_INTERVAL)

            else:
                # Market closed — sleep longer
                next_check = 600  # Check every 10 min when closed
                print(f"  [CLOSED] {now.strftime('%H:%M')} — market closed, sleeping {next_check//60}min")
                time.sleep(next_check)

        except KeyboardInterrupt:
            print("\nScanner stopped by user.")
            break
        except Exception as e:
            print(f"  [scanner] Unexpected error: {e}")
            time.sleep(30)  # Brief pause then continue


if __name__ == "__main__":
    run_scanner()
