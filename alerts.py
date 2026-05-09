# alerts.py
# Sends push notifications to your phone via Pushover.
# Pushover is $5 one-time, works on iPhone and Android.
# Get keys at pushover.net

import os
import requests
from datetime import datetime
from typing import Optional

try:
    from zoneinfo import ZoneInfo
    _CST = ZoneInfo("America/Chicago")
except ImportError:
    import pytz
    _CST = pytz.timezone("America/Chicago")

def _now_cst():
    from datetime import timezone
    return datetime.now(_CST)

PUSHOVER_API = "https://api.pushover.net/1/messages.json"

# Alert priority levels
PRIORITY_LOW    = -1   # No sound
PRIORITY_NORMAL =  0   # Sound + notification
PRIORITY_HIGH   =  1   # Bypasses quiet hours
PRIORITY_URGENT =  2   # Repeats until acknowledged


def send_alert(
    title: str,
    message: str,
    priority: int = PRIORITY_NORMAL,
    url: Optional[str] = None,
    url_title: Optional[str] = None,
) -> bool:
    """
    Send a push notification to your phone.
    Returns True if sent successfully.
    """
    user_key  = os.environ.get("PUSHOVER_USER_KEY", "")
    api_token = os.environ.get("PUSHOVER_API_TOKEN", "")

    if not user_key or not api_token:
        print(f"  [alerts] No Pushover keys set — skipping notification: {title}")
        return False

    payload = {
        "token":    api_token,
        "user":     user_key,
        "title":    title[:250],
        "message":  message[:1024],
        "priority": priority,
        "sound":    "cashregister" if priority >= PRIORITY_HIGH else "pushover",
    }

    if url:
        payload["url"]       = url
        payload["url_title"] = url_title or "View Chart"

    # Urgent messages require retry/expire
    if priority == PRIORITY_URGENT:
        payload["retry"]  = 60   # Retry every 60 seconds
        payload["expire"] = 300  # Give up after 5 minutes

    try:
        resp = requests.post(PUSHOVER_API, data=payload, timeout=10)
        if resp.status_code == 200:
            print(f"  [alerts] ✓ Sent: {title}")
            return True
        else:
            print(f"  [alerts] Failed ({resp.status_code}): {resp.text}")
            return False
    except Exception as e:
        print(f"  [alerts] Error: {e}")
        return False


def alert_volume_spike(ticker: str, rvol: float, price: float, change_pct: float):
    """Alert for unusual volume spike."""
    arrow = "▲" if change_pct >= 0 else "▼"
    send_alert(
        title=f"⚡ {ticker} — Volume Spike {rvol:.1f}x",
        message=(
            f"${price:.4f} {arrow}{abs(change_pct):.1f}%\n"
            f"Volume: {rvol:.1f}x 20-day average\n"
            f"Time: {_now_cst().strftime('%H:%M:%S CST')}\n"
            f"Action: Check for catalyst — news or SEC filing"
        ),
        priority=PRIORITY_HIGH,
        url=f"https://finance.yahoo.com/quote/{ticker}",
        url_title=f"View {ticker}",
    )


def alert_price_move(ticker: str, price: float, change_pct: float, timeframe: str = "10min"):
    """Alert for significant price move."""
    arrow = "▲" if change_pct >= 0 else "▼"
    priority = PRIORITY_HIGH if abs(change_pct) > 10 else PRIORITY_NORMAL
    send_alert(
        title=f"{'🟢' if change_pct >= 0 else '🔴'} {ticker} {arrow}{abs(change_pct):.1f}% in {timeframe}",
        message=(
            f"Price: ${price:.4f}\n"
            f"Move: {arrow}{abs(change_pct):.1f}% in last {timeframe}\n"
            f"Time: {_now_cst().strftime('%H:%M:%S CST')}"
        ),
        priority=priority,
        url=f"https://finance.yahoo.com/quote/{ticker}",
        url_title=f"View {ticker}",
    )


def alert_sec_filing(ticker: str, form_type: str, days_ago: int, filing_url: str, summary: str = ""):
    """Alert for new SEC filing detected."""
    send_alert(
        title=f"📋 {ticker} — New {form_type} Filing",
        message=(
            f"Filed: {days_ago} minutes ago\n"
            f"{summary[:300] if summary else 'Review filing for details'}\n"
            f"Time: {_now_cst().strftime('%H:%M:%S CST')}"
        ),
        priority=PRIORITY_HIGH,
        url=filing_url,
        url_title="View SEC Filing",
    )


def alert_news(ticker: str, headline: str, sentiment: str, price: float):
    """Alert for significant news article."""
    emoji = "🟢" if sentiment == "positive" else "🔴" if sentiment == "negative" else "📰"
    send_alert(
        title=f"{emoji} {ticker} — News Alert",
        message=(
            f"Price: ${price:.4f}\n"
            f"Headline: {headline[:200]}\n"
            f"Sentiment: {sentiment.upper()}\n"
            f"Time: {_now_cst().strftime('%H:%M:%S CST')}"
        ),
        priority=PRIORITY_NORMAL,
        url=f"https://finance.yahoo.com/quote/{ticker}/news",
        url_title=f"{ticker} News",
    )


def alert_morning_brief(watchlist: list, new_filings: int, gap_ups: list):
    """Send the daily morning briefing."""
    gap_str = ", ".join(gap_ups[:5]) if gap_ups else "None"
    send_alert(
        title=f"Axiom Morning Brief — {_now_cst().strftime('%b %d')}",
        message=(
            f"Today's watchlist: {len(watchlist)} stocks\n"
            f"Top names: {', '.join(watchlist[:8])}\n"
            f"Overnight filings: {new_filings}\n"
            f"Gap-ups: {gap_str}\n"
            f"Market opens in ~5 minutes"
        ),
        priority=PRIORITY_NORMAL,
    )


def alert_digest(alerts_summary: list, top_movers: list = None):
    """Send a 30-minute digest with recent alerts and top momentum movers."""
    if not alerts_summary and not top_movers:
        return
    parts = []
    if top_movers:
        mover_strs = [
            f"{m['ticker']} {m['change']:+.1f}% rvol={m['rvol']:.1f}x {'▲' if m.get('above_vwap') else '▼'}VWAP"
            for m in top_movers[:5]
        ]
        parts.append("Top movers: " + "  |  ".join(mover_strs))
    if alerts_summary:
        parts.extend(alerts_summary[-6:])
    send_alert(
        title=f"Axiom Digest — {_now_cst().strftime('%H:%M CST')}",
        message="\n".join(parts),
        priority=PRIORITY_LOW,
    )


def alert_level_break(ticker: str, price: float, level: float, level_name: str, change_pct: float):
    """Alert for break of a key intraday level (session high/low, pre-market high/low)."""
    is_up = price >= level
    emoji  = "🚀" if is_up else "🔻"
    arrow  = "above" if is_up else "below"
    color_word = "Bullish" if is_up else "Bearish"
    send_alert(
        title=f"{emoji} {ticker} — Broke {arrow} {level_name}",
        message=(
            f"Price: ${price:.4f}  Level: ${level:.4f}\n"
            f"{color_word}: break of {level_name}\n"
            f"Day chg: {change_pct:+.1f}%\n"
            f"Time: {_now_cst().strftime('%H:%M CST')}"
        ),
        priority=PRIORITY_HIGH,
        url=f"https://finance.yahoo.com/quote/{ticker}",
        url_title=f"View {ticker}",
    )


def alert_vwap_cross(ticker: str, price: float, vwap: float, direction: str, change_pct: float):
    """Alert for VWAP cross or extended move above VWAP."""
    if direction == "above":
        title    = f"🟢 {ticker} — Reclaimed VWAP"
        body     = (
            f"Price: ${price:.4f}  VWAP: ${vwap:.4f}\n"
            f"Day chg: {change_pct:+.1f}%\n"
            f"Bullish: price crossed back above VWAP\n"
            f"Time: {_now_cst().strftime('%H:%M CST')}"
        )
        priority = PRIORITY_HIGH
    elif direction == "below":
        title    = f"🔴 {ticker} — Lost VWAP"
        body     = (
            f"Price: ${price:.4f}  VWAP: ${vwap:.4f}\n"
            f"Day chg: {change_pct:+.1f}%\n"
            f"Bearish: price dropped below VWAP\n"
            f"Time: {_now_cst().strftime('%H:%M CST')}"
        )
        priority = PRIORITY_HIGH
    else:  # extended
        ext_pct = (price - vwap) / vwap * 100 if vwap > 0 else 0
        title    = f"⚡ {ticker} — Extended {ext_pct:.1f}% above VWAP"
        body     = (
            f"Price: ${price:.4f}  VWAP: ${vwap:.4f}\n"
            f"Extension: +{ext_pct:.1f}% above VWAP\n"
            f"Day chg: {change_pct:+.1f}%  Overextended — watch for pullback\n"
            f"Time: {_now_cst().strftime('%H:%M CST')}"
        )
        priority = PRIORITY_NORMAL
    send_alert(
        title=title,
        message=body,
        priority=priority,
        url=f"https://finance.yahoo.com/quote/{ticker}",
        url_title=f"View {ticker}",
    )
