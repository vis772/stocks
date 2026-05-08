# scanner_loop.py
# Real-time background scanner with Claude-powered news analysis.
# - All 50 watchlist stocks scanned every 60 seconds simultaneously
# - Top 10 priority stocks get news checked every 15 seconds (4x per minute)
# - Claude analyzes every news article for context, tone, and significance
# - SEC filings checked every 5 minutes
# - EOD report generated at 4:00 PM ET

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

# ─── Market hours (ET) ────────────────────────────────────────────────────────
MARKET_OPEN_HOUR    = 9
MARKET_OPEN_MIN     = 25
MARKET_CLOSE_HOUR   = 16
MARKET_CLOSE_MIN    = 30
PREMARKET_HOUR      = 6
SCAN_INTERVAL_SEC   = 60
AFTERHOURS_INTERVAL = 300


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
        self.last_sec_check:  datetime = datetime.now() - timedelta(hours=3)
        self.known_filings:   Set[str] = set()
        self._load()

    def _load(self):
        try:
            with open(STATE_FILE) as f:
                data = json.load(f)
            if data.get("date") == datetime.now().strftime("%Y-%m-%d"):
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
        if len(self.alert_log) > 50:
            self.alert_log = self.alert_log[-50:]
        try:
            with open("alert_log.json", "w") as f:
                json.dump(self.alert_log, f)
        except Exception:
            pass


# ─── Market hours ─────────────────────────────────────────────────────────────

def is_market_hours() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    h, m = now.hour, now.minute
    after_open   = (h > MARKET_OPEN_HOUR)  or (h == MARKET_OPEN_HOUR  and m >= MARKET_OPEN_MIN)
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


# ─── Claude News Analysis ─────────────────────────────────────────────────────

def analyze_news_with_claude(ticker: str, headline: str, summary: str = "") -> dict:
    """
    Use Claude Haiku to analyze a news headline for:
    - Sentiment (positive/negative/neutral)
    - Significance (high/medium/low)
    - Context (what actually happened in plain English)
    - Alert worthiness (is this actually relevant to a trader?)

    Cost: ~$0.0008 per article = ~$1/month at typical volume.
    Falls back to keyword matching if no API key.
    """
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
                    f"Reply in JSON only, no markdown, no explanation outside JSON:\n"
                    f'{{"sentiment": "positive|negative|neutral", '
                    f'"significance": "high|medium|low", '
                    f'"context": "one sentence: what happened and why it matters for the stock price", '
                    f'"alert_worthy": true|false, '
                    f'"reason": "brief reason for alert_worthy decision"}}'
                )
            }]
        )

        text   = message.content[0].text.strip()
        text   = text.replace("```json", "").replace("```", "").strip()
        result = json.loads(text)

        # Validate expected fields exist
        if "sentiment" not in result or "alert_worthy" not in result:
            return _keyword_sentiment(headline)

        return result

    except Exception as e:
        print(f"  [claude] News analysis failed for {ticker}: {e}")
        return _keyword_sentiment(headline)


def _keyword_sentiment(headline: str) -> dict:
    """Fallback keyword-based sentiment when Claude is unavailable."""
    text = headline.lower()
    pos  = sum(1 for w in [
        "surge","soar","beat","win","launch","partner","contract",
        "awarded","upgrade","record","buy","gains","jumps","rises",
        "approval","milestone","expansion","agreement"
    ] if w in text)
    neg  = sum(1 for w in [
        "fall","drop","miss","loss","warning","sell","downgrade",
        "lawsuit","concern","bankruptcy","dilut","reverse split",
        "investigation","fraud","debt","default","decline"
    ] if w in text)
    sentiment = "positive" if pos > neg else "negative" if neg > pos else "neutral"
    return {
        "sentiment":    sentiment,
        "significance": "medium",
        "context":      headline,
        "alert_worthy": True,
        "reason":       "keyword analysis fallback",
    }


def format_news_alert(ticker: str, headline: str, analysis: dict, price: float) -> str:
    """Build a rich alert message using Claude's analysis."""
    sentiment    = analysis.get("sentiment", "neutral")
    significance = analysis.get("significance", "medium")
    context      = analysis.get("context", headline)
    reason       = analysis.get("reason", "")

    emoji = "🟢" if sentiment == "positive" else "🔴" if sentiment == "negative" else "📰"
    sig   = "🔥 HIGH SIGNIFICANCE" if significance == "high" else ""

    return (
        f"{emoji} {ticker} News Alert {sig}\n\n"
        f"Headline: {headline[:150]}\n\n"
        f"Analysis: {context}\n\n"
        f"Sentiment: {sentiment.upper()} | Significance: {significance.upper()}\n"
        f"Price: ${price:.4f} | Time: {datetime.now().strftime('%H:%M ET')}"
    )


# ─── Single ticker scan ───────────────────────────────────────────────────────

def scan_one_ticker(ticker: str, state: ScannerState) -> List[str]:
    """
    Scan one ticker for price, volume, and news alerts.
    Returns list of alert messages fired this scan.
    """
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

        # ── Volume spike ──────────────────────────────────────────────────────
        last_vol = state.last_volumes.get(ticker, 0)
        if last_vol > 0 and volume > 0:
            hour = datetime.now().hour
            minutes_into_day = max(1, (hour - 9) * 60 + datetime.now().minute - 30)
            expected_pct = min(minutes_into_day / 390, 1.0)
            if expected_pct > 0.05:
                rvol = (volume / expected_pct) / max(last_vol / 0.5, 1)
                if rvol > VOLUME_SPIKE_THRESHOLD:
                    key = f"{ticker}_volume_{datetime.now().strftime('%Y-%m-%d-%H')}"
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
                key = f"{ticker}_price_{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
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
                key = f"{ticker}_10min_{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
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

        # ── News check (main scan) ────────────────────────────────────────────
        today     = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        news      = _fh_get("company-news", {"symbol": ticker, "from": yesterday, "to": today})
        if news:
            cutoff = datetime.now() - timedelta(minutes=30)
            for article in news[:5]:
                pub_time = article.get("datetime", 0)
                if not pub_time:
                    continue
                pub_dt   = datetime.fromtimestamp(pub_time)
                if pub_dt < cutoff:
                    continue

                headline = article.get("headline", "")
                art_key  = f"{ticker}_news_{pub_time}"

                if headline and not state.already_alerted(art_key):
                    # Claude analysis
                    analysis = analyze_news_with_claude(
                        ticker, headline, article.get("summary", "")
                    )

                    # Skip if Claude says not alert worthy
                    if not analysis.get("alert_worthy", True):
                        state.mark_alerted(art_key)
                        print(f"  [claude] {ticker} news skipped — not alert worthy: {analysis.get('reason','')}")
                        continue

                    sentiment = analysis.get("sentiment", "neutral")
                    msg_body  = format_news_alert(ticker, headline, analysis, price)

                    # High significance = high priority alert
                    priority = PRIORITY_HIGH if analysis.get("significance") == "high" else PRIORITY_NORMAL

                    send_alert(
                        title=f"{'🟢' if sentiment=='positive' else '🔴' if sentiment=='negative' else '📰'} {ticker} — News",
                        message=msg_body,
                        priority=priority,
                        url=f"https://finance.yahoo.com/quote/{ticker}/news",
                        url_title=f"{ticker} News",
                    )
                    state.mark_alerted(art_key)
                    msg = f"📰 {ticker} news ({sentiment}): {headline[:50]}..."
                    state.log_alert(msg)
                    fired.append(msg)
                    break

    except Exception:
        pass

    return fired


# ─── SEC Filing Monitor ───────────────────────────────────────────────────────

def check_sec_filings(watchlist: List[str], state: ScannerState):
    """Check EDGAR RSS for new filings from watchlist stocks."""
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
                        alert_sec_filing(
                            ticker=ticker,
                            form_type=form_type,
                            days_ago=0,
                            filing_url=link,
                            summary=f"New {form_type} detected for {ticker}. Review immediately.",
                        )
                        state.mark_alerted(filing_key)
                        state.log_alert(f"📋 {ticker} new {form_type} filing")
                    break

        state.last_sec_check = datetime.now()

    except Exception as e:
        print(f"  [scanner] SEC check failed: {e}")


# ─── Fast News Monitor — Top 10 stocks, 4x per minute ────────────────────────

def run_news_monitor(watchlist: List[str], state: ScannerState):
    """
    Dedicated news monitor thread.
    Checks top 10 priority stocks every 15 seconds = 4 times per minute.
    Uses Claude to analyze each article for context, tone, and significance.
    Runs alongside the main 60-second scanner loop.
    """
    def news_loop():
        while True:
            try:
                if not is_market_hours():
                    time.sleep(60)
                    continue

                priority_stocks = watchlist[:10]
                today     = datetime.now().strftime("%Y-%m-%d")
                yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                cutoff    = datetime.now() - timedelta(minutes=15)

                for ticker in priority_stocks:
                    try:
                        news = _fh_get("company-news", {
                            "symbol": ticker,
                            "from":   yesterday,
                            "to":     today,
                        })

                        if not news:
                            continue

                        for article in news[:3]:
                            pub_time = article.get("datetime", 0)
                            if not pub_time:
                                continue
                            pub_dt = datetime.fromtimestamp(pub_time)
                            if pub_dt < cutoff:
                                continue

                            headline = article.get("headline", "")
                            art_key  = f"{ticker}_fastnews_{pub_time}"

                            if headline and not state.already_alerted(art_key):
                                # Claude analysis for accurate context and tone
                                analysis = analyze_news_with_claude(
                                    ticker, headline, article.get("summary", "")
                                )

                                # Skip if Claude says not worth alerting
                                if not analysis.get("alert_worthy", True):
                                    state.mark_alerted(art_key)
                                    print(f"  [claude] {ticker} fast news skipped: {analysis.get('reason','')}")
                                    continue

                                sentiment    = analysis.get("sentiment", "neutral")
                                significance = analysis.get("significance", "medium")

                                # Get current price
                                quote = _fh_get("quote", {"symbol": ticker})
                                price = quote.get("c", 0) if quote else 0

                                msg_body = format_news_alert(ticker, headline, analysis, price)

                                # High significance gets high priority alert
                                priority = PRIORITY_HIGH if significance == "high" else PRIORITY_NORMAL

                                send_alert(
                                    title=f"{'🟢' if sentiment=='positive' else '🔴' if sentiment=='negative' else '📰'} {ticker} — Fast News",
                                    message=msg_body,
                                    priority=priority,
                                    url=f"https://finance.yahoo.com/quote/{ticker}/news",
                                    url_title=f"{ticker} News",
                                )
                                state.mark_alerted(art_key)
                                state.log_alert(
                                    f"📰 {ticker} fast news ({sentiment}/{significance}): {headline[:45]}..."
                                )
                            break

                        time.sleep(0.3)

                    except Exception:
                        continue

            except Exception as e:
                print(f"  [news_monitor] Error: {e}")

            time.sleep(15)  # 15 seconds = 4 times per minute

    thread = threading.Thread(target=news_loop, daemon=True)
    thread.start()
    print("  ✓ Fast news monitor started — top 10 stocks checked 4x per minute with Claude analysis")
    return thread


# ─── Main scanner loop ────────────────────────────────────────────────────────

def run_scanner():
    """Main infinite loop. Runs forever on Railway."""
    print("\n" + "="*60)
    print("APEX Real-Time Scanner Starting...")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S ET')}")
    print("="*60 + "\n")

    state              = ScannerState()
    watchlist:         List[str] = []
    morning_brief_sent = False
    last_digest_time   = datetime.now()
    eod_report_sent    = False
    news_thread        = None

    while True:
        try:
            now = datetime.now()

            # ── Morning screen at 6 AM ────────────────────────────────────────
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
                state.log_alert(f"☀️ Morning brief sent — {len(watchlist)} stocks")

            # ── Market hours scan ─────────────────────────────────────────────
            if is_market_hours():
                state.scan_count += 1
                print(f"\n[SCAN #{state.scan_count}] {now.strftime('%H:%M:%S')} — {len(watchlist)} stocks")

                # Start fast news monitor thread if not running
                if news_thread is None or not news_thread.is_alive():
                    news_thread = run_news_monitor(watchlist, state)

                all_alerts = []

                # SEC filings every 5 minutes
                if (now - state.last_sec_check).total_seconds() >= 300:
                    print("  Checking SEC EDGAR...")
                    check_sec_filings(watchlist, state)

                # Scan ALL stocks simultaneously
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
                    print(f"  🔔 {len(all_alerts)} alert(s) fired")
                else:
                    print(f"  ✓ No alerts this scan")

                # 30-min digest
                if (now - last_digest_time).total_seconds() >= 1800 and state.alert_log:
                    alert_digest(state.alert_log[-10:])
                    last_digest_time = now

                # EOD report at 4:00 PM ET
                if now.hour == 16 and now.minute < 5 and not eod_report_sent:
                    print("\n[EOD REPORT] Generating end-of-day report...")
                    try:
                        from eod_report import run_eod_report
                        run_eod_report()
                        eod_report_sent = True
                        state.log_alert("📊 EOD report generated and sent")
                    except Exception as e:
                        print(f"  [EOD] Failed: {e}")

                # Reset EOD flag for next day
                if now.hour == 6 and now.minute < 5:
                    eod_report_sent = False

                time.sleep(SCAN_INTERVAL_SEC)

            # ── Pre-market ────────────────────────────────────────────────────
            elif is_premarket():
                print(f"  [PRE-MARKET] {now.strftime('%H:%M')} — light scan")
                if watchlist and (now - state.last_sec_check).total_seconds() >= 600:
                    check_sec_filings(watchlist, state)
                time.sleep(AFTERHOURS_INTERVAL)

            # ── Market closed ─────────────────────────────────────────────────
            else:
                print(f"  [CLOSED] {now.strftime('%H:%M')} — sleeping 10 min")
                time.sleep(600)

        except KeyboardInterrupt:
            print("\nScanner stopped.")
            break
        except Exception as e:
            print(f"  [scanner] Error: {e}")
            time.sleep(30)


if __name__ == "__main__":
    run_scanner()
