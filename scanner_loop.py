# scanner_loop.py
# Fixed: timezone (ET via pytz), duplicate alerts, correct EOD timing

import os
import time
import json
import requests
import feedparser
import re
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set

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
VOLUME_SPIKE_THRESHOLD = 2.5
PRICE_MOVE_THRESHOLD   = 5.0
PRICE_MOVE_10MIN       = 8.0
GAP_UP_THRESHOLD       = 4.0

# ─── Market hours in ET ───────────────────────────────────────────────────────
# Railway runs UTC. We convert to ET for all time checks.
MARKET_OPEN_HOUR    = 9
MARKET_OPEN_MIN     = 25
MARKET_CLOSE_HOUR   = 16
MARKET_CLOSE_MIN    = 30
PREMARKET_HOUR      = 6
SCAN_INTERVAL_SEC   = 60
AFTERHOURS_INTERVAL = 300


def now_et():
    """Get current time in US/Eastern timezone."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except ImportError:
        # Fallback: UTC-5 (EST) or UTC-4 (EDT)
        # Simple DST approximation: EDT March-Nov, EST otherwise
        utc_now = datetime.utcnow()
        month = utc_now.month
        offset = -4 if 3 <= month <= 11 else -5
        return utc_now + timedelta(hours=offset)


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


# ─── State ────────────────────────────────────────────────────────────────────

class ScannerState:
    def __init__(self):
        self.alerted_today:   Set[str] = set()
        self.last_prices:     Dict[str, float] = {}
        self.last_volumes:    Dict[str, float] = {}
        self.price_at_10min:  Dict[str, float] = {}
        self.alert_log:       List[str] = []
        self.scan_count:      int = 0
        self.last_sec_check:  datetime = now_et() - timedelta(hours=3)
        self.known_filings:   Set[str] = set()
        self._load()

    def _load(self):
        try:
            from db.database import load_scanner_state
            data = load_scanner_state()
            if data.get("date") == now_et().strftime("%Y-%m-%d"):
                self.alerted_today = set(data.get("alerted_today", []))
                self.known_filings = set(data.get("known_filings", []))
                self.scan_count    = data.get("scan_count", 0)
        except Exception as e:
            print(f"  [state] Load failed: {e}")

    def save(self):
        try:
            from db.database import save_scanner_state
            save_scanner_state({
                "date":          now_et().strftime("%Y-%m-%d"),
                "alerted_today": list(self.alerted_today),
                "known_filings": list(self.known_filings),
                "scan_count":    self.scan_count,
                "last_updated":  now_et().isoformat(),
            })
        except Exception as e:
            print(f"  [state] Save failed: {e}")

    def already_alerted(self, key: str) -> bool:
        return key in self.alerted_today

    def mark_alerted(self, key: str):
        self.alerted_today.add(key)
        self.save()

    def log_alert(self, msg: str):
        et = now_et()
        full_msg = f"{et.strftime('%H:%M ET')} {msg}"
        self.alert_log.append(full_msg)
        if len(self.alert_log) > 100:
            self.alert_log = self.alert_log[-100:]
        # Save to shared DB so dashboard can read it
        try:
            from db.database import save_alert
            # Extract ticker from message if present
            ticker = msg.split(" ")[1] if len(msg.split(" ")) > 1 else ""
            save_alert(msg, ticker=ticker)
        except Exception:
            pass
        # Also write local JSON as fallback
        try:
            with open("alert_log.json", "w") as f:
                import json as _json
                _json.dump(self.alert_log, f)
        except Exception:
            pass


# ─── Market hours (all using ET) ──────────────────────────────────────────────

def is_market_hours() -> bool:
    et = now_et()
    if et.weekday() >= 5:
        return False
    h, m = et.hour, et.minute
    after_open   = (h > MARKET_OPEN_HOUR)  or (h == MARKET_OPEN_HOUR  and m >= MARKET_OPEN_MIN)
    before_close = (h < MARKET_CLOSE_HOUR) or (h == MARKET_CLOSE_HOUR and m <= MARKET_CLOSE_MIN)
    return after_open and before_close


def is_premarket() -> bool:
    et = now_et()
    if et.weekday() >= 5:
        return False
    return PREMARKET_HOUR <= et.hour < MARKET_OPEN_HOUR


def is_morning_screen_time() -> bool:
    et = now_et()
    if et.weekday() >= 5:
        return False
    return et.hour == PREMARKET_HOUR and et.minute < 10


# ─── Claude News Analysis ─────────────────────────────────────────────────────

def analyze_news_with_claude(ticker: str, headline: str, summary: str = "") -> dict:
    """Use Claude Haiku to analyze news for sentiment, context, and alert worthiness."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return _keyword_sentiment(headline)
    try:
        import anthropic
        client  = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{
                "role": "user",
                "content": (
                    f"Analyze this news for stock {ticker}.\n\n"
                    f"Headline: {headline}\n"
                    f"Summary: {summary[:300] if summary else 'None'}\n\n"
                    f"Reply in JSON only, no markdown:\n"
                    f'{{"sentiment": "positive|negative|neutral", '
                    f'"significance": "high|medium|low", '
                    f'"context": "one sentence: what happened and why it matters for the stock price", '
                    f'"alert_worthy": true|false, '
                    f'"reason": "brief reason"}}'
                )
            }]
        )
        text   = message.content[0].text.strip()
        text   = text.replace("```json", "").replace("```", "").strip()
        result = json.loads(text)
        if "sentiment" not in result or "alert_worthy" not in result:
            return _keyword_sentiment(headline)
        return result
    except Exception as e:
        print(f"  [claude] News analysis failed: {e}")
        return _keyword_sentiment(headline)


def _keyword_sentiment(headline: str) -> dict:
    text = headline.lower()
    pos  = sum(1 for w in ["surge","soar","beat","win","launch","partner","contract","awarded","upgrade","record","buy","gains","jumps","rises","approval","milestone"] if w in text)
    neg  = sum(1 for w in ["fall","drop","miss","loss","warning","sell","downgrade","lawsuit","concern","bankruptcy","dilut","reverse split","investigation","fraud","decline"] if w in text)
    sentiment = "positive" if pos > neg else "negative" if neg > pos else "neutral"
    return {"sentiment": sentiment, "significance": "medium", "context": headline, "alert_worthy": True, "reason": "keyword fallback"}


def format_news_alert(ticker: str, headline: str, analysis: dict, price: float) -> str:
    sentiment    = analysis.get("sentiment", "neutral")
    significance = analysis.get("significance", "medium")
    context      = analysis.get("context", headline)
    sig_tag      = " 🔥 HIGH" if significance == "high" else ""
    return (
        f"Headline: {headline[:150]}\n\n"
        f"Analysis: {context}\n\n"
        f"Sentiment: {sentiment.upper()}{sig_tag}\n"
        f"Price: ${price:.4f} | {now_et().strftime('%H:%M ET')}"
    )


# ─── Single ticker scan ───────────────────────────────────────────────────────

def scan_one_ticker(ticker: str, state: ScannerState) -> List[str]:
    fired = []
    try:
        quote = _fh_get("quote", {"symbol": ticker})
        if not quote or not quote.get("c") or quote["c"] <= 0:
            return fired

        price      = quote["c"]
        prev_close = quote["pc"]
        volume     = quote.get("v", 0)

        if prev_close <= 0:
            return fired

        change_pct = (price - prev_close) / prev_close * 100
        et         = now_et()

        # ── Volume spike ──────────────────────────────────────────────────────
        last_vol = state.last_volumes.get(ticker, 0)
        if last_vol > 0 and volume > 0:
            minutes_into_day = max(1, (et.hour - 9) * 60 + et.minute - 30)
            expected_pct = min(minutes_into_day / 390, 1.0)
            if expected_pct > 0.05:
                rvol = (volume / expected_pct) / max(last_vol / 0.5, 1)
                if rvol > VOLUME_SPIKE_THRESHOLD:
                    # Key uses hour only — one alert per hour max
                    key = f"{ticker}_volume_{et.strftime('%Y-%m-%d-%H')}"
                    if not state.already_alerted(key):
                        alert_volume_spike(ticker, rvol, price, change_pct)
                        state.mark_alerted(key)
                        msg = f"⚡ {ticker} volume spike {rvol:.1f}x"
                        state.log_alert(msg)
                        fired.append(msg)
        state.last_volumes[ticker] = volume

        # ── Price move since last scan ─────────────────────────────────────────
        last_price = state.last_prices.get(ticker)
        if last_price and last_price > 0:
            move_pct = (price - last_price) / last_price * 100
            if abs(move_pct) >= PRICE_MOVE_THRESHOLD:
                # Key uses 5-minute window — prevents spam within same 5 min
                key = f"{ticker}_price_{et.strftime('%Y-%m-%d-%H')}_{et.minute // 5}"
                if not state.already_alerted(key):
                    alert_price_move(ticker, price, move_pct, "1min")
                    state.mark_alerted(key)
                    msg = f"{'🟢' if move_pct > 0 else '🔴'} {ticker} {move_pct:+.1f}% in 1min"
                    state.log_alert(msg)
                    fired.append(msg)

        # ── 10-minute price move ───────────────────────────────────────────────
        price_10min = state.price_at_10min.get(ticker)
        if price_10min and price_10min > 0:
            move_10min = (price - price_10min) / price_10min * 100
            if abs(move_10min) >= PRICE_MOVE_10MIN:
                # Key uses 10-minute window — prevents spam
                key = f"{ticker}_10min_{et.strftime('%Y-%m-%d-%H')}_{et.minute // 10}"
                if not state.already_alerted(key):
                    alert_price_move(ticker, price, move_10min, "10min")
                    state.mark_alerted(key)
                    msg = f"{'🔥' if move_10min > 0 else '🔴'} {ticker} {move_10min:+.1f}% in 10min"
                    state.log_alert(msg)
                    fired.append(msg)

        state.last_prices[ticker] = price
        if state.scan_count % 10 == 0:
            state.price_at_10min[ticker] = price

        # ── Gap check (once per day) ───────────────────────────────────────────
        gap_key = f"{ticker}_gap_{et.strftime('%Y-%m-%d')}"
        if not state.already_alerted(gap_key):
            if change_pct >= GAP_UP_THRESHOLD:
                send_alert(
                    title=f"🚀 {ticker} Gap-Up {change_pct:+.1f}%",
                    message=f"Price: ${price:.4f}\nGap: {change_pct:+.1f}% from yesterday\nTime: {et.strftime('%H:%M ET')}",
                    priority=PRIORITY_HIGH,
                    url=f"https://finance.yahoo.com/quote/{ticker}",
                )
                state.mark_alerted(gap_key)
                state.log_alert(f"🚀 {ticker} gap-up {change_pct:+.1f}%")
                fired.append(f"🚀 {ticker} gap-up {change_pct:+.1f}%")
            elif change_pct <= -GAP_UP_THRESHOLD:
                send_alert(
                    title=f"📉 {ticker} Gap-Down {change_pct:.1f}%",
                    message=f"Price: ${price:.4f}\nGap: {change_pct:.1f}% from yesterday\nTime: {et.strftime('%H:%M ET')}",
                    priority=PRIORITY_NORMAL,
                    url=f"https://finance.yahoo.com/quote/{ticker}",
                )
                state.mark_alerted(gap_key)

        # ── News check ────────────────────────────────────────────────────────
        today     = et.strftime("%Y-%m-%d")
        yesterday = (et - timedelta(days=1)).strftime("%Y-%m-%d")
        news      = _fh_get("company-news", {"symbol": ticker, "from": yesterday, "to": today})
        if news:
            cutoff = et - timedelta(minutes=30)
            for article in news[:5]:
                pub_time = article.get("datetime", 0)
                if not pub_time:
                    continue
                pub_dt = datetime.utcfromtimestamp(pub_time) + timedelta(hours=-4 if 3 <= et.month <= 11 else -5)
                if pub_dt.replace(tzinfo=None) < cutoff.replace(tzinfo=None):
                    continue
                headline = article.get("headline", "")
                art_key  = f"{ticker}_news_{pub_time}"
                if headline and not state.already_alerted(art_key):
                    analysis = analyze_news_with_claude(ticker, headline, article.get("summary", ""))
                    if not analysis.get("alert_worthy", True):
                        state.mark_alerted(art_key)
                        continue
                    sentiment = analysis.get("sentiment", "neutral")
                    msg_body  = format_news_alert(ticker, headline, analysis, price)
                    priority  = PRIORITY_HIGH if analysis.get("significance") == "high" else PRIORITY_NORMAL
                    send_alert(
                        title=f"{'🟢' if sentiment=='positive' else '🔴' if sentiment=='negative' else '📰'} {ticker} — News",
                        message=msg_body,
                        priority=priority,
                        url=f"https://finance.yahoo.com/quote/{ticker}/news",
                        url_title=f"{ticker} News",
                    )
                    state.mark_alerted(art_key)
                    state.log_alert(f"📰 {ticker} news ({sentiment}): {headline[:50]}...")
                    fired.append(f"📰 {ticker} news ({sentiment})")
                    break

    except Exception:
        pass
    return fired


# ─── SEC Filing Monitor ───────────────────────────────────────────────────────

def check_sec_filings(watchlist: List[str], state: ScannerState):
    try:
        priority_forms = {"8-K", "S-3", "424B3", "424B4", "SC 13D"}
        feed = feedparser.parse(
            "https://www.sec.gov/cgi-bin/browse-edgar"
            "?action=getcurrent&type=8-K&dateb=&owner=include&count=40&output=atom"
        )
        ticker_set = set(t.upper() for t in watchlist)
        for entry in feed.entries[:20]:
            title     = entry.get("title", "")
            link      = entry.get("link", "")
            accession = re.search(r"(\d{10}-\d{2}-\d{6})", link)
            acc_id    = accession.group(1) if accession else link
            if acc_id in state.known_filings:
                continue
            state.known_filings.add(acc_id)
            for ticker in ticker_set:
                if ticker in title.upper():
                    filing_key = f"{ticker}_filing_{acc_id}"
                    if not state.already_alerted(filing_key):
                        form_type = "8-K"
                        for ft in priority_forms:
                            if ft in title.upper():
                                form_type = ft
                                break
                        alert_sec_filing(ticker, form_type, 0, link, f"New {form_type} for {ticker}. Review immediately.")
                        state.mark_alerted(filing_key)
                        state.log_alert(f"📋 {ticker} new {form_type} filing")
                    break
        state.last_sec_check = now_et()
    except Exception as e:
        print(f"  [scanner] SEC check failed: {e}")


# ─── Fast News Monitor — Top 10 stocks, 4x per minute ────────────────────────

def run_news_monitor(watchlist: List[str], state: ScannerState):
    def news_loop():
        while True:
            try:
                if not is_market_hours():
                    time.sleep(60)
                    continue

                priority_stocks = watchlist[:10]
                et        = now_et()
                today     = et.strftime("%Y-%m-%d")
                yesterday = (et - timedelta(days=1)).strftime("%Y-%m-%d")
                cutoff_dt = et - timedelta(minutes=15)

                for ticker in priority_stocks:
                    try:
                        news = _fh_get("company-news", {"symbol": ticker, "from": yesterday, "to": today})
                        if not news:
                            continue
                        for article in news[:3]:
                            pub_time = article.get("datetime", 0)
                            if not pub_time:
                                continue
                            pub_dt = datetime.utcfromtimestamp(pub_time) + timedelta(hours=-4 if 3 <= et.month <= 11 else -5)
                            if pub_dt.replace(tzinfo=None) < cutoff_dt.replace(tzinfo=None):
                                continue
                            headline = article.get("headline", "")
                            art_key  = f"{ticker}_fastnews_{pub_time}"
                            if headline and not state.already_alerted(art_key):
                                analysis = analyze_news_with_claude(ticker, headline, article.get("summary", ""))
                                if not analysis.get("alert_worthy", True):
                                    state.mark_alerted(art_key)
                                    continue
                                sentiment    = analysis.get("sentiment", "neutral")
                                significance = analysis.get("significance", "medium")
                                quote = _fh_get("quote", {"symbol": ticker})
                                price = quote.get("c", 0) if quote else 0
                                msg_body = format_news_alert(ticker, headline, analysis, price)
                                priority = PRIORITY_HIGH if significance == "high" else PRIORITY_NORMAL
                                send_alert(
                                    title=f"{'🟢' if sentiment=='positive' else '🔴' if sentiment=='negative' else '📰'} {ticker} — Fast News",
                                    message=msg_body,
                                    priority=priority,
                                    url=f"https://finance.yahoo.com/quote/{ticker}/news",
                                    url_title=f"{ticker} News",
                                )
                                state.mark_alerted(art_key)
                                state.log_alert(f"📰 {ticker} fast news ({sentiment}/{significance}): {headline[:45]}...")
                            break
                        time.sleep(0.3)
                    except Exception:
                        continue
            except Exception as e:
                print(f"  [news_monitor] Error: {e}")
            time.sleep(15)

    thread = threading.Thread(target=news_loop, daemon=True)
    thread.start()
    print("  ✓ Fast news monitor started — top 10 stocks 4x per minute")
    return thread


# ─── Main loop ────────────────────────────────────────────────────────────────

def run_scanner():
    print("\n" + "="*60)
    print("APEX Real-Time Scanner Starting...")
    print(f"Time: {now_et().strftime('%Y-%m-%d %H:%M:%S ET')}")
    print("="*60 + "\n")

    state              = ScannerState()
    watchlist:         List[str] = []
    morning_brief_sent = False
    last_digest_time   = now_et()
    eod_report_sent    = False
    news_thread        = None

    while True:
        try:
            et = now_et()

            # ── Morning screen at 6 AM ET ─────────────────────────────────────
            if is_morning_screen_time() and not watchlist:
                print("\n[MORNING SCREEN] Building today's watchlist...")
                watchlist          = build_todays_watchlist(max_stocks=50)
                morning_brief_sent = False
                eod_report_sent    = False
                news_thread        = None

            # ── Load watchlist if empty ────────────────────────────────────────
            if not watchlist:
                watchlist = load_todays_watchlist()
                if not watchlist:
                    from config import DEFAULT_UNIVERSE
                    watchlist = DEFAULT_UNIVERSE
                    print(f"  Using default universe: {len(watchlist)} stocks")

            # ── Morning brief at 8:30 AM ET (7:30 CST) ────────────────────────
            if et.hour == 8 and et.minute >= 30 and not morning_brief_sent and watchlist:
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
                state.log_alert(f"☀️ Morning brief sent — {len(watchlist)} stocks")

            # ── Market hours scan ─────────────────────────────────────────────
            if is_market_hours():
                state.scan_count += 1
                print(f"\n[SCAN #{state.scan_count}] {et.strftime('%H:%M:%S ET')} — {len(watchlist)} stocks")

                if news_thread is None or not news_thread.is_alive():
                    news_thread = run_news_monitor(watchlist, state)

                all_alerts = []

                if (et - state.last_sec_check).total_seconds() >= 300:
                    print("  Checking SEC EDGAR...")
                    check_sec_filings(watchlist, state)

                with ThreadPoolExecutor(max_workers=15) as executor:
                    futures = {executor.submit(scan_one_ticker, t, state): t for t in watchlist}
                    for future in as_completed(futures, timeout=45):
                        try:
                            all_alerts.extend(future.result())
                        except Exception:
                            pass

                if all_alerts:
                    print(f"  🔔 {len(all_alerts)} alert(s) fired")
                else:
                    print(f"  ✓ No alerts this scan")

                # 30-min digest
                if (et - last_digest_time).total_seconds() >= 1800 and state.alert_log:
                    alert_digest(state.alert_log[-10:])
                    last_digest_time = et

                # EOD report at exactly 4:00 PM ET
                if et.hour == 16 and et.minute < 5 and not eod_report_sent:
                    print("\n[EOD REPORT] Generating end-of-day report...")
                    try:
                        from eod_report import run_eod_report
                        run_eod_report()
                        eod_report_sent = True
                        state.log_alert("📊 EOD report generated and sent")
                    except Exception as e:
                        print(f"  [EOD] Failed: {e}")

                if et.hour == 6 and et.minute < 5:
                    eod_report_sent = False

                time.sleep(SCAN_INTERVAL_SEC)

            elif is_premarket():
                print(f"  [PRE-MARKET] {et.strftime('%H:%M ET')} — light scan")
                if watchlist and (et - state.last_sec_check).total_seconds() >= 600:
                    check_sec_filings(watchlist, state)
                time.sleep(AFTERHOURS_INTERVAL)

            else:
                print(f"  [CLOSED] {et.strftime('%H:%M ET')} — sleeping 10 min")
                time.sleep(600)

        except KeyboardInterrupt:
            print("\nScanner stopped.")
            break
        except Exception as e:
            print(f"  [scanner] Error: {e}")
            time.sleep(30)


if __name__ == "__main__":
    run_scanner()
