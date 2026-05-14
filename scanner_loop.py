# scanner_loop.py
# Fixed: timezone (ET via pytz), duplicate alerts, correct EOD timing

import os
import time
import json
import requests
import feedparser
import re
import threading
import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set

print(f"AXIOM_TEST_SCAN = {os.environ.get('AXIOM_TEST_SCAN', 'NOT SET')}")

from alerts import (
    alert_volume_spike, alert_price_move,
    alert_sec_filing, alert_news,
    alert_morning_brief, alert_digest,
    alert_vwap_cross, alert_level_break,
    PRIORITY_HIGH, PRIORITY_NORMAL,
    send_alert
)
from morning_screen import build_todays_watchlist, load_todays_watchlist, WATCHLIST_FILE

try:
    from quant_engine import run_quant_for_ticker, QuantEngine
    _QUANT_AVAILABLE = True
except Exception as _qe:
    _QUANT_AVAILABLE = False
    print(f"  [quant] import failed: {_qe}")

try:
    from analysis.regime import update_daily_regime, get_current_regime
    _REGIME_AVAILABLE = True
except Exception as _re:
    _REGIME_AVAILABLE = False
    print(f"  [regime] import failed: {_re}")

try:
    from quant.factor_engine import compute_and_store_ic
    _IC_AVAILABLE = True
except Exception as _ice:
    _IC_AVAILABLE = False
    print(f"  [ic] import failed: {_ice}")

FINNHUB_BASE = "https://finnhub.io/api/v1"
STATE_FILE   = "scanner_state.json"


def _log(level: str, message: str) -> None:
    """Fire-and-forget structured log to scanner_logs table."""
    try:
        from db.database import log_scanner_event
        log_scanner_event(level, message)
    except Exception:
        pass

try:
    from data.tiingo_stream import TiingoStream
except Exception as _tiingo_import_err:
    TiingoStream = None
    print(f"  [tiingo] import failed: {_tiingo_import_err}")
_stream: Optional["TiingoStream"] = None
TIINGO_FRESH_MS = 90_000

# ─── Alert thresholds ─────────────────────────────────────────────────────────
VOLUME_SPIKE_THRESHOLD     = 2.5
PRICE_MOVE_THRESHOLD       = 5.0
PRICE_MOVE_10MIN           = 8.0
GAP_UP_THRESHOLD           = 4.0
VWAP_EXTENDED_PCT          = 5.0
PREDICTION_SCAN_INTERVAL   = 1800
PREDICTION_BUY_THRESHOLD   = 65
PREDICTION_SELL_THRESHOLD  = 30
PREDICTION_TOP_N           = 10

# ─── Market hours in ET ───────────────────────────────────────────────────────
MARKET_OPEN_HOUR    = 9
MARKET_OPEN_MIN     = 25
MARKET_CLOSE_HOUR   = 16
MARKET_CLOSE_MIN    = 30
PREMARKET_HOUR          = 4
MORNING_SCREEN_HOUR     = 6
SCAN_INTERVAL_SEC       = 60
PREMARKET_SCAN_INTERVAL = 90
PREMARKET_SEC_INTERVAL  = 300
AFTERHOURS_INTERVAL     = 300


def now_et():
    """Get current time in US/Eastern timezone."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except ImportError:
        utc_now = datetime.utcnow()
        month = utc_now.month
        offset = -4 if 3 <= month <= 11 else -5
        return utc_now + timedelta(hours=offset)


# ─── Telegram bot restart flag ────────────────────────────────────────────────

def check_restart_flag() -> bool:
    """
    Check if a restart was requested via the Telegram bot.
    The bot sets restart_requested = TRUE in scanner_control (id=1).
    If found, we clear it and return True — caller does sys.exit(0)
    so Railway auto-restarts Service 2.
    """
    try:
        from db.database import _get_pg_conn
        conn = _get_pg_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT restart_requested FROM scanner_control WHERE id = 1")
            row = cur.fetchone()
            if row and row[0]:
                cur.execute(
                    "UPDATE scanner_control SET restart_requested = FALSE, updated_at = NOW() WHERE id = 1"
                )
                conn.commit()
                print("[restart] Restart flag detected — exiting for Railway restart")
                _log("warning", "Restart requested via Telegram bot — restarting service")
                conn.close()
                return True
        conn.close()
    except Exception as e:
        print(f"[restart] Flag check error: {e}")
    return False


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
        self.vwap_cum_tp_vol:   Dict[str, float] = {}
        self.vwap_cum_vol:      Dict[str, float] = {}
        self.above_vwap:        Dict[str, Optional[bool]] = {}
        self.vwap_values:       Dict[str, float] = {}
        self.vwap_session_date: str = ""
        self.last_change_pct:   Dict[str, float] = {}
        self.last_rvol:         Dict[str, float] = {}
        self.session_high:      Dict[str, float] = {}
        self.session_low:       Dict[str, float] = {}
        self.premarket_high:    Dict[str, float] = {}
        self.premarket_low:     Dict[str, float] = {}
        self.level_session_date: str = ""
        self.last_prediction_run:    datetime = now_et() - timedelta(hours=2)
        self.last_conviction_live_run: datetime = now_et() - timedelta(hours=2)
        self.signals_suppressed:     int = 0
        self.universe_size:          int = 0
        self.open_positions:         int = 0
        self.last_conviction_run_ts: Optional[str] = None
        self.last_accuracy_run_ts:   Optional[str] = None
        self.current_regime:         str = "NEUTRAL"
        self.regime_updated_date:    str = ""
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
            vwap_snap = {}
            for ticker, vwap in self.vwap_values.items():
                price    = self.last_prices.get(ticker, 0)
                above    = self.above_vwap.get(ticker)
                dist_pct = round((price - vwap) / vwap * 100, 2) if vwap > 0 and price > 0 else 0
                vwap_snap[ticker] = {
                    "vwap":     vwap,
                    "price":    round(price, 4),
                    "above":    above,
                    "dist_pct": dist_pct,
                }
            save_scanner_state({
                "date":                     now_et().strftime("%Y-%m-%d"),
                "alerted_today":            list(self.alerted_today),
                "known_filings":            list(self.known_filings),
                "scan_count":               self.scan_count,
                "last_updated":             now_et().isoformat(),
                "vwap_snapshot":            vwap_snap,
                "momentum_ranking":         self._momentum_ranking_cache,
                "signals_suppressed_today": self.signals_suppressed,
                "universe_size":            self.universe_size,
                "open_positions":           self.open_positions,
                "last_conviction_run":      self.last_conviction_run_ts,
                "last_accuracy_run":        self.last_accuracy_run_ts,
                "current_regime":           self.current_regime,
            })
        except Exception as e:
            print(f"  [state] Save failed: {e}")

    _momentum_ranking_cache: list = []

    def already_alerted(self, key: str) -> bool:
        return key in self.alerted_today

    def mark_alerted(self, key: str):
        self.alerted_today.add(key)
        self.save()

    def log_alert(self, msg: str):
        try:
            from zoneinfo import ZoneInfo
            cst = ZoneInfo("America/Chicago")
        except ImportError:
            import pytz
            cst = pytz.timezone("America/Chicago")
        from datetime import datetime as _dt
        now_cst = _dt.now(cst)
        full_msg = f"{now_cst.strftime('%H:%M CST')} {msg}"
        self.alert_log.append(full_msg)
        if len(self.alert_log) > 100:
            self.alert_log = self.alert_log[-100:]
        try:
            from db.database import save_alert
            ticker = msg.split(" ")[1] if len(msg.split(" ")) > 1 else ""
            save_alert(msg, ticker=ticker)
        except Exception:
            pass
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
    h, m = et.hour, et.minute
    if h < PREMARKET_HOUR:
        return False
    if h < MARKET_OPEN_HOUR:
        return True
    return h == MARKET_OPEN_HOUR and m < MARKET_OPEN_MIN


def is_morning_screen_time() -> bool:
    et = now_et()
    if et.weekday() >= 5:
        return False
    return et.hour == MORNING_SCREEN_HOUR and et.minute < 10


def get_scan_mode() -> str:
    et = now_et()
    if et.weekday() >= 5:
        return "WEEKEND"
    h, m = et.hour, et.minute
    after_open   = (h > 9) or (h == 9 and m >= 25)
    before_close = (h < 16) or (h == 16 and m <= 30)
    if after_open and before_close:
        return "MARKET"
    if h >= 4 and not after_open:
        return "PREMARKET"
    if (h == 16 and m > 30) or (17 <= h < 20):
        return "AFTERHOURS"
    return "OVERNIGHT"


# ─── Claude News Analysis ─────────────────────────────────────────────────────

def analyze_news_with_claude(ticker: str, headline: str, summary: str = "") -> dict:
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


# ─── Tiingo stream lifecycle ──────────────────────────────────────────────────

def _stream_subscription_set(watchlist: List[str]) -> List[str]:
    from config import DEFAULT_UNIVERSE
    tickers = set(t.upper() for t in watchlist if t)
    tickers.update(DEFAULT_UNIVERSE)
    try:
        from db.database import get_portfolio
        pf = get_portfolio()
        if not pf.empty:
            tickers.update(t.upper() for t in pf["ticker"].tolist())
    except Exception:
        pass
    return sorted(tickers)


def _ensure_stream(watchlist: List[str]) -> None:
    global _stream
    if TiingoStream is None:
        return
    token = os.environ.get("TIINGO_API_KEY", "")
    if not token:
        return
    tickers = _stream_subscription_set(watchlist)
    if _stream is None:
        try:
            _stream = TiingoStream(token)
            _stream.start(tickers)
        except Exception as e:
            print(f"  [tiingo] start failed: {e}")
            _stream = None
    else:
        try:
            _stream.update_tickers(tickers)
        except Exception as e:
            print(f"  [tiingo] update_tickers failed: {e}")


# ─── Intraday levels ──────────────────────────────────────────────────────────

def _check_level_breaks(ticker, price, change_pct, state, et) -> List[str]:
    fired = []
    today = et.strftime("%Y-%m-%d")

    if state.level_session_date != today:
        state.session_high      = {}
        state.session_low       = {}
        state.premarket_high    = {}
        state.premarket_low     = {}
        state.level_session_date = today

    if is_premarket():
        pm_high = state.premarket_high.get(ticker, 0.0)
        pm_low  = state.premarket_low.get(ticker, float("inf"))
        state.premarket_high[ticker] = max(pm_high, price)
        state.premarket_low[ticker]  = min(pm_low, price)
        return fired

    if not is_market_hours():
        return fired

    sess_high = state.session_high.get(ticker, 0.0)
    sess_low  = state.session_low.get(ticker, float("inf"))

    if sess_high > 0 and price > sess_high:
        key = f"{ticker}_sess_high_{et.strftime('%Y-%m-%d-%H')}"
        if not state.already_alerted(key):
            alert_level_break(ticker, price, sess_high, "Session High", change_pct)
            state.mark_alerted(key)
            msg = f"{ticker} new session high ${price:.4f}"
            state.log_alert(msg)
            fired.append(msg)

    if sess_low < float("inf") and price < sess_low:
        key = f"{ticker}_sess_low_{et.strftime('%Y-%m-%d-%H')}"
        if not state.already_alerted(key):
            alert_level_break(ticker, price, sess_low, "Session Low", change_pct)
            state.mark_alerted(key)
            msg = f"{ticker} new session low ${price:.4f}"
            state.log_alert(msg)
            fired.append(msg)

    pm_high = state.premarket_high.get(ticker, 0.0)
    if pm_high > 0 and price > pm_high:
        key = f"{ticker}_pm_high_{today}"
        if not state.already_alerted(key):
            alert_level_break(ticker, price, pm_high, "Pre-Market High", change_pct)
            state.mark_alerted(key)
            msg = f"{ticker} broke pre-market high ${pm_high:.4f}"
            state.log_alert(msg)
            fired.append(msg)

    state.session_high[ticker] = max(sess_high, price)
    state.session_low[ticker]  = min(sess_low, price) if sess_low < float("inf") else price

    return fired


# ─── Momentum ranking ─────────────────────────────────────────────────────────

def _build_momentum_ranking(watchlist: List[str], state: ScannerState) -> List[dict]:
    rows = []
    for ticker in watchlist:
        change = state.last_change_pct.get(ticker, 0)
        rvol   = state.last_rvol.get(ticker, 1.0)
        above  = state.above_vwap.get(ticker)
        price  = state.last_prices.get(ticker, 0)
        vwap   = state.vwap_values.get(ticker, 0)
        dist   = round((price - vwap) / vwap * 100, 2) if vwap > 0 and price > 0 else 0

        rvol_pts   = min(max(rvol - 1.0, 0) / 4.0 * 35, 35)
        change_pts = max(min(change / 10.0 * 40, 40), change / 10.0 * 20)
        vwap_pts   = 25 if above else 0

        score = round(max(0, min(100, rvol_pts + change_pts + vwap_pts)), 1)
        rows.append({
            "ticker":     ticker,
            "score":      score,
            "change":     round(change, 2),
            "rvol":       round(rvol, 2),
            "above_vwap": above,
            "dist_pct":   dist,
            "price":      round(price, 4),
        })

    rows.sort(key=lambda x: x["score"], reverse=True)
    state._momentum_ranking_cache = rows
    return rows


# ─── VWAP ─────────────────────────────────────────────────────────────────────

def _update_vwap(ticker, price, volume, high, low, last_vol, state) -> Optional[float]:
    today = now_et().strftime("%Y-%m-%d")
    if state.vwap_session_date != today:
        state.vwap_cum_tp_vol   = {}
        state.vwap_cum_vol      = {}
        state.above_vwap        = {}
        state.vwap_values       = {}
        state.vwap_session_date = today

    delta_vol = max(0.0, volume - last_vol)
    if delta_vol > 0:
        tp = (high + low + price) / 3.0
        state.vwap_cum_tp_vol[ticker] = state.vwap_cum_tp_vol.get(ticker, 0.0) + tp * delta_vol
        state.vwap_cum_vol[ticker]    = state.vwap_cum_vol.get(ticker, 0.0) + delta_vol

    cum_vol = state.vwap_cum_vol.get(ticker, 0.0)
    if cum_vol <= 0:
        return None

    vwap = state.vwap_cum_tp_vol[ticker] / cum_vol
    state.vwap_values[ticker] = round(vwap, 4)
    return vwap


def _check_vwap_alerts(ticker, price, vwap, change_pct, state, et) -> List[str]:
    fired     = []
    was_above = state.above_vwap.get(ticker)
    is_above  = price >= vwap
    state.above_vwap[ticker] = is_above

    if was_above is not None and was_above != is_above:
        key = f"{ticker}_vwap_cross_{et.strftime('%Y-%m-%d-%H')}_{et.minute // 5}"
        if not state.already_alerted(key):
            direction = "above" if is_above else "below"
            alert_vwap_cross(ticker, price, vwap, direction, change_pct)
            state.mark_alerted(key)
            msg   = f"{ticker} crossed {direction} VWAP ${vwap:.4f}"
            state.log_alert(msg)
            fired.append(msg)
            if is_above:
                try:
                    from db.database import log_paper_trade
                    log_paper_trade(ticker, "vwap_reclaim", price,
                                    round(price * 0.95, 4),
                                    round(price * 1.08, 4),
                                    round(price * 1.15, 4))
                except Exception:
                    pass

    if is_above and vwap > 0:
        ext_pct = (price - vwap) / vwap * 100
        if ext_pct >= VWAP_EXTENDED_PCT:
            key = f"{ticker}_vwap_ext_{et.strftime('%Y-%m-%d-%H')}"
            if not state.already_alerted(key):
                alert_vwap_cross(ticker, price, vwap, "extended", change_pct)
                state.mark_alerted(key)
                msg = f"{ticker} extended {ext_pct:.1f}% above VWAP"
                state.log_alert(msg)
                fired.append(msg)

    return fired


# ─── Single ticker scan ───────────────────────────────────────────────────────

def scan_one_ticker(ticker: str, state: ScannerState) -> List[str]:
    fired = []
    try:
        quote = _fh_get("quote", {"symbol": ticker})
        if not quote or not quote.get("c") or quote["c"] <= 0:
            return fired

        prev_close = quote["pc"]
        volume     = quote.get("v", 0)
        high       = quote.get("h") or quote["c"]
        low        = quote.get("l") or quote["c"]

        price = quote["c"]
        if _stream is not None:
            try:
                if _stream.get_age_ms(ticker) < TIINGO_FRESH_MS:
                    fresh = _stream.get_last(ticker)
                    if fresh and fresh.get("price"):
                        price = fresh["price"]
            except Exception:
                pass

        if prev_close <= 0:
            return fired

        change_pct = (price - prev_close) / prev_close * 100
        et         = now_et()
        last_vol   = state.last_volumes.get(ticker, 0)

        if is_market_hours():
            vwap = _update_vwap(ticker, price, volume, high, low, last_vol, state)
            if vwap is not None:
                fired.extend(_check_vwap_alerts(ticker, price, vwap, change_pct, state, et))

        fired.extend(_check_level_breaks(ticker, price, change_pct, state, et))

        state.last_change_pct[ticker] = change_pct

        if last_vol > 0 and volume > 0:
            minutes_into_day = max(1, (et.hour - 9) * 60 + et.minute - 30)
            expected_pct = min(minutes_into_day / 390, 1.0)
            if expected_pct > 0.05:
                rvol = (volume / expected_pct) / max(last_vol / 0.5, 1)
                state.last_rvol[ticker] = rvol
                if rvol > VOLUME_SPIKE_THRESHOLD:
                    key = f"{ticker}_volume_{et.strftime('%Y-%m-%d-%H')}"
                    if not state.already_alerted(key):
                        alert_volume_spike(ticker, rvol, price, change_pct)
                        state.mark_alerted(key)
                        msg = f"{ticker} volume spike {rvol:.1f}x"
                        state.log_alert(msg)
                        fired.append(msg)
        state.last_volumes[ticker] = volume

        last_price = state.last_prices.get(ticker)
        if last_price and last_price > 0:
            move_pct = (price - last_price) / last_price * 100
            if abs(move_pct) >= PRICE_MOVE_THRESHOLD:
                key = f"{ticker}_price_{et.strftime('%Y-%m-%d-%H')}_{et.minute // 5}"
                if not state.already_alerted(key):
                    alert_price_move(ticker, price, move_pct, "1min")
                    state.mark_alerted(key)
                    msg = f"{ticker} {move_pct:+.1f}% in 1min"
                    state.log_alert(msg)
                    fired.append(msg)

        price_10min = state.price_at_10min.get(ticker)
        if price_10min and price_10min > 0:
            move_10min = (price - price_10min) / price_10min * 100
            if abs(move_10min) >= PRICE_MOVE_10MIN:
                key = f"{ticker}_10min_{et.strftime('%Y-%m-%d-%H')}_{et.minute // 10}"
                if not state.already_alerted(key):
                    alert_price_move(ticker, price, move_10min, "10min")
                    state.mark_alerted(key)
                    msg = f"{ticker} {move_10min:+.1f}% in 10min"
                    state.log_alert(msg)
                    fired.append(msg)

        state.last_prices[ticker] = price
        if state.scan_count % 10 == 0:
            state.price_at_10min[ticker] = price

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
                state.log_alert(f"{ticker} gap-up {change_pct:+.1f}%")
                fired.append(f"{ticker} gap-up {change_pct:+.1f}%")
                try:
                    from db.database import log_paper_trade, log_signal
                    log_paper_trade(ticker, "gap_up", price,
                                    round(price * 0.93, 4),
                                    round(price * 1.10, 4),
                                    round(price * 1.20, 4))
                    sig_id = log_signal(
                        ticker           = ticker,
                        signal_label     = "Gap-Up",
                        score            = round(change_pct, 1),
                        score_breakdown  = {},
                        price_at_signal  = price,
                        volume_at_signal = volume,
                        alert_type       = "gap_up",
                    )
                    if sig_id:
                        print(f"  [signal_log] ✓ {ticker} Gap-Up {change_pct:+.1f}% | id={sig_id}")
                    else:
                        print(f"  [signal_log] ✗ {ticker} gap_up — log_signal returned None")
                except Exception as _gap_e:
                    print(f"  [signal_log] gap_up FAILED for {ticker}: {_gap_e}")
            elif change_pct <= -GAP_UP_THRESHOLD:
                send_alert(
                    title=f"📉 {ticker} Gap-Down {change_pct:.1f}%",
                    message=f"Price: ${price:.4f}\nGap: {change_pct:.1f}% from yesterday\nTime: {et.strftime('%H:%M ET')}",
                    priority=PRIORITY_NORMAL,
                    url=f"https://finance.yahoo.com/quote/{ticker}",
                )
                state.mark_alerted(gap_key)

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
                    state.log_alert(f"{ticker} news ({sentiment}): {headline[:50]}...")
                    fired.append(f"{ticker} news ({sentiment})")
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
                        state.log_alert(f"{ticker} new {form_type} filing")
                    break
        state.last_sec_check = now_et()
    except Exception as e:
        print(f"  [scanner] SEC check failed: {e}")


# ─── Fast News Monitor ────────────────────────────────────────────────────────

def run_news_monitor(watchlist: List[str], state: ScannerState):
    def news_loop():
        while True:
            try:
                if not is_market_hours() and not is_premarket():
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
                                state.log_alert(f"{ticker} fast news ({sentiment}/{significance}): {headline[:45]}...")
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


# ─── Prediction scan ──────────────────────────────────────────────────────────

def run_prediction_scan(watchlist: List[str], state: ScannerState, session_mode: str = "MARKET") -> None:
    from core.scanner import scan_ticker
    from db.database import log_paper_trade
    from concurrent.futures import ThreadPoolExecutor, as_completed

    top_tickers = [r["ticker"] for r in state._momentum_ranking_cache[:PREDICTION_TOP_N]]
    if not top_tickers:
        top_tickers = watchlist[:PREDICTION_TOP_N]

    print(f"  [prediction] Scoring: {', '.join(top_tickers)}")

    # Multi-factor scoring for top tickers when AXIOM_QUANT_MODE=1
    if os.environ.get("AXIOM_QUANT_MODE", "1") == "1" and _IC_AVAILABLE:
        try:
            import yfinance as yf
            from quant.factor_engine import MultiFactorScorer
            _mfs = MultiFactorScorer()
            _iwm_hist = yf.Ticker("IWM").history(period="3mo", auto_adjust=True)
            _iwm_prices = _iwm_hist["Close"].dropna() if not _iwm_hist.empty else None
            for _qt in top_tickers[:5]:
                try:
                    _snap = {"ticker": _qt, "price": state.last_prices.get(_qt, 0)}
                    _hist = yf.Ticker(_qt).history(period="1y", auto_adjust=True)
                    if not _hist.empty and _iwm_prices is not None:
                        _mfs.score_ticker(
                            _qt, _snap, _hist,
                            regime=state.current_regime,
                            log_factors=True,
                        )
                except Exception:
                    pass
        except Exception as _mfse:
            print(f"  [quant_mode] MultiFactorScorer in prediction scan failed: {_mfse}")

    def _score(ticker):
        try:
            return ticker, scan_ticker(ticker, save=False)
        except Exception as e:
            print(f"  [prediction] {ticker} error: {e}")
            return ticker, None

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(_score, t): t for t in top_tickers}
        for fut in as_completed(futures, timeout=120):
            try:
                ticker, result = fut.result()
            except Exception:
                continue
            if not result or result.get("error") or result.get("filtered_out"):
                continue

            score = result.get("final_score", 0)
            price = result.get("price", 0)
            if not price:
                continue

            signal_label = result.get("signal", "Hold")

            try:
                from db.database import should_suppress
                if should_suppress(ticker, signal_label, window_minutes=5):
                    print(f"  [dedup] Suppressed {ticker} {signal_label} — duplicate within 5min")
                    state.signals_suppressed += 1
                    continue
            except Exception:
                pass

            quant_adj    = 0.0
            atr_14       = 0.0
            rsi_quant    = None
            sigma_hist   = None
            source_qual  = "live"
            if _QUANT_AVAILABLE:
                try:
                    quant_adj, atr_14, rsi_quant, sigma_hist = run_quant_for_ticker(ticker, result)
                    adj_score = round(float(max(0, min(100, score + quant_adj))), 1)
                    print(f"  [quant] {ticker} base={score:.0f} adj={quant_adj:+.1f} final={adj_score:.0f}")
                    score = adj_score
                    signal_label = result.get("signal", "Hold")
                except Exception as _qerr:
                    print(f"  [quant] {ticker} error: {_qerr}")

            _rvol = state.last_rvol.get(ticker, 1.0)
            _qt_adj = float(quant_adj) if quant_adj else 0.0
            if score >= 75 and _qt_adj >= 5 and _rvol >= 2.0:
                quality_tag = "HIGH"
            elif score >= 65 and _rvol >= 1.3:
                quality_tag = "MEDIUM"
            else:
                quality_tag = "LOW"

            try:
                from db.database import log_signal
                breakdown = {
                    "technical":   float(result.get("technical_score") or 0),
                    "catalyst":    float(result.get("catalyst_score") or 0),
                    "fundamental": float(result.get("fundamental_score") or 0),
                    "risk":        float(result.get("risk_score") or 0),
                    "sentiment":   float(result.get("sentiment_score") or 0),
                }
                if rsi_quant is not None:
                    breakdown["rsi"] = round(float(rsi_quant), 1)
                if sigma_hist is not None:
                    breakdown["sigma_hist"] = round(float(sigma_hist), 4)
                if atr_14:
                    breakdown["atr_14"] = round(float(atr_14), 4)
                sig_id = log_signal(
                    ticker           = ticker,
                    signal_label     = signal_label,
                    score            = round(score, 1),
                    score_breakdown  = breakdown,
                    price_at_signal  = price,
                    volume_at_signal = result.get("volume") or state.last_volumes.get(ticker) or 0,
                    alert_type       = "scan",
                    quant_adj        = quant_adj,
                    source_quality   = source_qual,
                    session_mode     = session_mode,
                    quality_tag      = quality_tag,
                )
                if sig_id:
                    print(f"  [signal_log] ✓ {ticker} | {signal_label} | score={score:.0f} | quant={quant_adj:+.1f} | id={sig_id}")
                else:
                    print(f"  [signal_log] ✗ {ticker} — log_signal returned None (check DB connection)")
            except Exception as _e:
                print(f"  [signal_log] FAILED for {ticker}: {_e}")

            if score >= PREDICTION_BUY_THRESHOLD:
                stop_ = result.get("stop_loss")  or round(price * 0.93, 4)
                t1    = result.get("target_1")   or round(price * 1.10, 4)
                t2    = result.get("target_2")   or round(price * 1.20, 4)
                log_paper_trade(ticker, "prediction_buy", price, stop_, t1, t2,
                                source_type="prediction_buy", score_at_entry=round(score, 1))
                try:
                    from paper_broker import get_broker
                    get_broker().submit_order(
                        ticker       = ticker,
                        side         = "buy",
                        qty          = 10,
                        order_type   = "market",
                        stop_loss    = stop_,
                        target_1     = t1,
                        target_2     = t2,
                        entry_reason = "prediction_buy",
                        conviction   = round(score, 1),
                        session_mode = session_mode,
                    )
                except Exception as _pbe:
                    print(f"  [broker] prediction_buy submit failed: {_pbe}")
                state.log_alert(f"PRED BUY {ticker} ({score:.0f}pt)")
                print(f"  [prediction] BUY {ticker} score={score:.0f}")

            elif score <= PREDICTION_SELL_THRESHOLD:
                s_stop = round(price * 1.07, 4)
                s_t1   = round(price * 0.90, 4)
                s_t2   = round(price * 0.80, 4)
                log_paper_trade(ticker, "prediction_sell", price, s_stop, s_t1, s_t2,
                                source_type="prediction_sell", score_at_entry=round(score, 1))
                state.log_alert(f"PRED SELL {ticker} ({score:.0f}pt)")
                print(f"  [prediction] SELL {ticker} score={score:.0f}")

    try:
        from reports.checkpoint_reports import check_and_run_checkpoints
        check_and_run_checkpoints()
    except Exception as _cp_e:
        print(f"  [checkpoint] inline check failed: {_cp_e}")


# ─── Main loop ────────────────────────────────────────────────────────────────

def run_scanner():
  try:
    print("\n" + "="*60)
    print("Axiom Terminal Scanner Starting...")
    print(f"Time: {now_et().strftime('%Y-%m-%d %H:%M:%S ET')}")
    print("="*60 + "\n")

    try:
        from db.database import initialize_db
        initialize_db()
        print("  ✓ DB initialized")
    except Exception as _dbi_e:
        print(f"  [startup] DB init failed: {_dbi_e}")

    _QUANT_MODE_ACTIVE = os.environ.get("AXIOM_QUANT_MODE", "1") == "1"
    print(f"  [quant_mode] AXIOM_QUANT_MODE={'ON (MultiFactorScorer active)' if _QUANT_MODE_ACTIVE else 'OFF (legacy scorer only)'}")

    try:
        print("[AccuracyValidator] Running startup grade...")
        from accuracy_validator import AccuracyValidator
        AccuracyValidator().grade_signals()
        print("[AccuracyValidator] Startup grade complete")
    except Exception as _av_e:
        print(f"  [startup] AccuracyValidator startup grade failed: {_av_e}")

    _et_start = now_et()
    _test_scan_mode = os.environ.get("AXIOM_TEST_SCAN", "").strip() == "1"
    print(f"  [startup] test_scan_mode={_test_scan_mode}  weekday={_et_start.weekday()}")
    if _et_start.weekday() >= 5:
        print("Market closed - weekend. Next scan Monday 9:30 AM ET")
        send_alert(
            title="Axiom Terminal — Online",
            message="Market closed today. Next scan Monday 9:30 AM ET",
            priority=PRIORITY_NORMAL,
        )

    state              = ScannerState()
    watchlist:         List[str] = []
    morning_brief_sent = False
    last_digest_time   = now_et()
    eod_report_sent    = False
    news_thread        = None
    last_screen_date: Optional[str] = None

    try:
        from analysis.outcome_tracker import start_outcome_tracker
        start_outcome_tracker()
    except Exception as _e:
        print(f"  [scanner] outcome tracker failed to start: {_e}")

    try:
        from db.database import set_scanner_control
        set_scanner_control(force_scan=False, scanner_started_at=now_et().isoformat())
    except Exception as _ctrl_start_e:
        print(f"  [control] startup init failed: {_ctrl_start_e}")

    # ── Test scan mode ────────────────────────────────────────────────────────
    if _test_scan_mode:
        print("\n[TEST MODE] Bypassing market-hours guardrail — running one scan cycle")
        try:
            _test_wl = load_todays_watchlist()
        except Exception as _wl_e:
            print(f"  [TEST MODE] load_todays_watchlist failed ({_wl_e}), falling back to DEFAULT_UNIVERSE")
            _test_wl = []
        if not _test_wl:
            from config import DEFAULT_UNIVERSE
            _test_wl = DEFAULT_UNIVERSE
        print(f"  Watchlist: {len(_test_wl)} stocks")

        print("  Running scan_one_ticker on all watchlist stocks...")
        _test_alerts: List[str] = []
        with ThreadPoolExecutor(max_workers=15) as _ex:
            _futs = {_ex.submit(scan_one_ticker, t, state): t for t in _test_wl}
            for _fut in as_completed(_futs, timeout=60):
                try:
                    _test_alerts.extend(_fut.result())
                except Exception:
                    pass
        print(f"  scan_one_ticker complete — {len(_test_alerts)} alert(s)")

        print("  Running prediction scan (logs to signal_log)...")
        watchlist = _test_wl
        try:
            run_prediction_scan(watchlist, state)
        except Exception as _pred_e:
            print(f"  [TEST MODE] prediction scan error:")
            traceback.print_exc()

        print("[TEST MODE] Complete — resuming normal schedule\n")

    _SLEEP_MAP = {"MARKET": 60, "PREMARKET": 120, "AFTERHOURS": 120, "OVERNIGHT": 300, "WEEKEND": 600}

    _MODE_LABELS = {
        "MARKET":     "Market Hours",
        "PREMARKET":  "Pre-Market",
        "AFTERHOURS": "After-Hours",
        "OVERNIGHT":  "Overnight",
        "WEEKEND":    "Weekend",
    }
    _MODE_DESC = {
        "MARKET":     "Full scan every 60s — tickers, predictions, paper broker active.",
        "PREMARKET":  "Pre-market scan every 120s — gaps, news, SEC EDGAR. Conviction at 8:55 AM ET.",
        "AFTERHOURS": "After-hours every 120s — AH price alerts, conviction at 8:30 PM ET.",
        "OVERNIGHT":  "Light scan every 5min — SEC EDGAR + portfolio maintenance only.",
        "WEEKEND":    "Weekend mode — grading signals & refreshing universe.",
    }
    _last_mode: Optional[str] = None

    while True:
        try:
            # ── Telegram bot restart flag ─────────────────────────────────────
            if check_restart_flag():
                import sys
                sys.exit(0)

            et        = now_et()
            today_str = et.strftime("%Y-%m-%d")
            mode      = get_scan_mode()

            if mode != _last_mode:
                print(f"\n[MODE] {_last_mode or 'STARTUP'} → {mode} at {et.strftime('%H:%M ET')}")
                _log("info", f"Mode transition: {_last_mode or 'STARTUP'} → {mode}")
                try:
                    from db.database import set_scanner_control
                    set_scanner_control(current_mode=mode)
                except Exception:
                    pass
                if _last_mode is not None:
                    send_alert(
                        title=f"Axiom — {_MODE_LABELS.get(mode, mode)}",
                        message=_MODE_DESC.get(mode, mode),
                        priority=PRIORITY_NORMAL,
                    )
                _last_mode = mode

            try:
                from db.database import get_scanner_control, set_scanner_control
                _ctrl = get_scanner_control()
                if _ctrl.get("paused"):
                    print(f"  [PAUSED] Manual hold — sleeping 30s")
                    _log("info", "Scanner paused — manual hold active")
                    time.sleep(30)
                    continue
                if _ctrl.get("force_scan"):
                    print(f"\n[FORCE SCAN] Triggered from dashboard")
                    _log("info", "Force scan triggered from dashboard")
                    if not watchlist:
                        watchlist = load_todays_watchlist() or []
                        if not watchlist:
                            from config import DEFAULT_UNIVERSE
                            watchlist = DEFAULT_UNIVERSE
                    _fs_alerts: List[str] = []
                    with ThreadPoolExecutor(max_workers=15) as _fex:
                        _ffs = {_fex.submit(scan_one_ticker, t, state): t for t in watchlist}
                        for _ff in as_completed(_ffs, timeout=60):
                            try:
                                _fs_alerts.extend(_ff.result())
                            except Exception:
                                pass
                    run_prediction_scan(watchlist, state, session_mode=mode)
                    set_scanner_control(force_scan=False)
                    state.scan_count += 1
                    state.save()
                    print(f"  [FORCE SCAN] Complete — {len(_fs_alerts)} alert(s)")
                    _log("info", f"Force scan complete — {len(_fs_alerts)} alert(s) fired")
                    continue
            except Exception as _ctrl_e:
                print(f"  [control] check failed: {_ctrl_e}")

            # ── WEEKEND ───────────────────────────────────────────────────────
            if mode == "WEEKEND":
                print(f"  [WEEKEND {et.strftime('%H:%M ET')}] Grading signals / checking universe...")
                try:
                    from accuracy_validator import AccuracyValidator
                    AccuracyValidator().grade_signals()
                except Exception as _wknd_av:
                    print(f"  [weekend] grade_signals failed: {_wknd_av}")
                if et.weekday() == 5 and et.hour == 8 and et.minute < 10:
                    _sat_key = f"universe_refresh_sat_{today_str}"
                    if not state.already_alerted(_sat_key):
                        try:
                            from universe_manager import refresh_universe, needs_refresh
                            if needs_refresh(max_age_days=6):
                                print("  [universe] Stale — starting weekly refresh...")
                                refresh_universe()
                            else:
                                print("  [universe] Universe is fresh — skipping refresh")
                            state.mark_alerted(_sat_key)
                        except Exception as _ure:
                            print(f"  [weekend] universe refresh failed: {_ure}")
                if et.weekday() == 6 and et.hour == 18 and et.minute < 10:
                    _sun_key = f"sunday_prep_{today_str}"
                    if not state.already_alerted(_sun_key):
                        send_alert(
                            title="Axiom — Weekend Summary",
                            message="Universe refresh complete. Scanner resumes Monday 4:00 AM ET.",
                            priority=PRIORITY_NORMAL,
                        )
                        state.mark_alerted(_sun_key)
                time.sleep(_SLEEP_MAP["WEEKEND"])
                continue

            # ── Morning screen at 6 AM ET ─────────────────────────────────────
            if is_morning_screen_time() and last_screen_date != today_str:
                print("\n[MORNING SCREEN] Building today's watchlist...")
                watchlist          = build_todays_watchlist(max_stocks=50)
                last_screen_date   = today_str
                morning_brief_sent = False
                eod_report_sent    = False
                news_thread        = None
                _ensure_stream(watchlist)

            if not watchlist:
                watchlist = load_todays_watchlist()
                if not watchlist:
                    from config import DEFAULT_UNIVERSE
                    watchlist = DEFAULT_UNIVERSE
                    print(f"  Using default universe: {len(watchlist)} stocks")
                _ensure_stream(watchlist)
            state.universe_size = len(watchlist)

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
                state.log_alert(f"Morning brief sent — {len(watchlist)} stocks")

            if et.hour == 9 and 0 <= et.minute < 15 and state.regime_updated_date != today_str:
                if _REGIME_AVAILABLE:
                    try:
                        _regime_data = update_daily_regime()
                        state.current_regime      = _regime_data.get("regime", "NEUTRAL")
                        state.regime_updated_date = today_str
                        print(
                            f"  [regime] {state.current_regime} | IWM ${_regime_data.get('iwm_price', 0):.2f} | "
                            f"MA20/50 {_regime_data.get('iwm_ma20', 0):.2f}/{_regime_data.get('iwm_ma50', 0):.2f} | "
                            f"ADX {_regime_data.get('adx_14', 0):.1f} | Vol {_regime_data.get('volatility_20d', 0):.1f}%"
                        )
                        _log("info", f"Regime detected: {state.current_regime} | IWM ${_regime_data.get('iwm_price', 0):.2f}")
                    except Exception as _rde:
                        print(f"  [regime] update failed: {_rde}")

            if et.hour == 9 and 29 <= et.minute < 35:
                _open_key = f"broker_open_{today_str}"
                if not state.already_alerted(_open_key):
                    try:
                        from paper_broker import get_broker
                        get_broker()._update_account_metrics()
                        state.mark_alerted(_open_key)
                    except Exception as _pbo:
                        print(f"  [broker] open-bell update failed: {_pbo}")

            if et.hour == 0 and et.minute < 5:
                eod_report_sent = False
                _midnight_key = f"midnight_reset_{today_str}"
                if not state.already_alerted(_midnight_key):
                    state.signals_suppressed = 0
                    state.mark_alerted(_midnight_key)
                    _log("info", "Daily reset complete — signal counters reset")

            # ── PREMARKET ─────────────────────────────────────────────────────
            if mode == "PREMARKET":
                state.scan_count += 1
                print(f"\n[PRE-MARKET #{state.scan_count}] {et.strftime('%H:%M:%S ET')} — {len(watchlist)} stocks")
                _log("info", f"Pre-market scan #{state.scan_count} started | {et.strftime('%H:%M ET')}")
                if news_thread is None or not news_thread.is_alive():
                    news_thread = run_news_monitor(watchlist, state)
                if (et - state.last_sec_check).total_seconds() >= 300:
                    print("  Checking SEC EDGAR...")
                    check_sec_filings(watchlist, state)
                _pre_alerts: List[str] = []
                with ThreadPoolExecutor(max_workers=15) as executor:
                    futures = {executor.submit(scan_one_ticker, t, state): t for t in watchlist}
                    for future in as_completed(futures, timeout=45):
                        try:
                            _pre_alerts.extend(future.result())
                        except Exception:
                            pass
                if et.hour == 8 and 55 <= et.minute <= 59:
                    _preopen_key = f"conviction_preopen_{today_str}"
                    if not state.already_alerted(_preopen_key):
                        print("\n[CONVICTION] Running pre-open conviction scan (8:55 AM ET)...")
                        try:
                            from conviction_engine import run_conviction_engine
                            run_conviction_engine(session="preopen", regime=state.current_regime)
                            state.last_conviction_run_ts = now_et().isoformat()
                            state.mark_alerted(_preopen_key)
                        except Exception as _cve_pre:
                            print(f"  [conviction] pre-open scan failed: {_cve_pre}")
                try:
                    from paper_broker import get_broker
                    _pb = get_broker()
                    _pb.process_pending_orders()
                    _pb.update_all_positions()
                except Exception as _pbp:
                    print(f"  [broker] pre-market update failed: {_pbp}")
                if _pre_alerts:
                    print(f"  🔔 {len(_pre_alerts)} alert(s) fired")
                    _log("info", f"Pre-market scan #{state.scan_count} complete — {len(_pre_alerts)} alert(s)")
                else:
                    print(f"  ✓ No alerts this scan")
                    _log("info", f"Pre-market scan #{state.scan_count} complete — no alerts")

            # ── MARKET ────────────────────────────────────────────────────────
            elif mode == "MARKET":
                state.scan_count += 1
                print(f"\n[SCAN #{state.scan_count}] {et.strftime('%H:%M:%S ET')} — {len(watchlist)} stocks")
                _log("info", f"Scan #{state.scan_count} started — {len(watchlist)} stocks | {et.strftime('%H:%M ET')}")
                if news_thread is None or not news_thread.is_alive():
                    news_thread = run_news_monitor(watchlist, state)
                all_alerts: List[str] = []
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
                    _log("info", f"Scan #{state.scan_count} complete — {len(all_alerts)} alert(s) fired")
                else:
                    print(f"  ✓ No alerts this scan")
                    _log("info", f"Scan #{state.scan_count} complete — no alerts")
                top_movers = _build_momentum_ranking(watchlist, state)
                try:
                    from db.database import get_open_position_count
                    state.open_positions = get_open_position_count()
                except Exception:
                    pass
                state.save()
                if (et - state.last_prediction_run).total_seconds() >= PREDICTION_SCAN_INTERVAL:
                    print("\n[PREDICTION SCAN] Running 30-min full-score scan...")
                    _log("info", f"Prediction scan started — top {PREDICTION_TOP_N} stocks")
                    run_prediction_scan(watchlist, state, session_mode="MARKET")
                    state.last_prediction_run = et
                    _log("info", "Prediction scan complete")
                if is_market_hours() and (et - state.last_conviction_live_run).total_seconds() >= PREDICTION_SCAN_INTERVAL:
                    try:
                        from conviction_engine import generate_live_conviction_list
                        generate_live_conviction_list(session="market")
                        state.last_conviction_run_ts = et.isoformat()
                        print("  [conviction] Live list refreshed")
                    except Exception as _cve_live:
                        print(f"  [conviction] live refresh failed: {_cve_live}")
                    state.last_conviction_live_run = et
                if (et - last_digest_time).total_seconds() >= 1800 and state.alert_log:
                    alert_digest(state.alert_log[-10:], top_movers=top_movers[:5])
                    last_digest_time = et
                try:
                    from paper_broker import get_broker
                    _pb = get_broker()
                    _pb.process_pending_orders()
                    _pb.update_all_positions()
                    _pb._snapshot_equity_curve()
                except Exception as _pbm:
                    print(f"  [broker] market update failed: {_pbm}")
                if et.hour == 16 and 0 <= et.minute < 5:
                    _close_key = f"conviction_close_{today_str}"
                    if not state.already_alerted(_close_key):
                        print("\n[CONVICTION] Running 4 PM close conviction scan...")
                        try:
                            from conviction_engine import run_conviction_engine
                            run_conviction_engine(session="close", regime=state.current_regime)
                            state.last_conviction_run_ts = now_et().isoformat()
                            state.mark_alerted(_close_key)
                            try:
                                from db.database import _get_pg_conn, _is_postgres, _get_sqlite_conn
                                from paper_broker import get_broker
                                _pb2 = get_broker()
                                if _is_postgres():
                                    _conn2 = _get_pg_conn()
                                    _c2    = _conn2.cursor()
                                    _c2.execute(
                                        "SELECT ticker, COALESCE(shares, 1), COALESCE(limit_entry, entry), stop_loss, COALESCE(conviction_score, conviction) FROM conviction_buys WHERE session=%s ORDER BY rank LIMIT 5",
                                        ("close",)
                                    )
                                    _buys = _c2.fetchall()
                                    _conn2.close()
                                else:
                                    _conn2 = _get_sqlite_conn()
                                    _c2    = _conn2.cursor()
                                    _c2.execute(
                                        "SELECT ticker, COALESCE(shares, 1), COALESCE(limit_entry, entry), stop_loss, COALESCE(conviction_score, conviction) FROM conviction_buys WHERE session=? ORDER BY rank LIMIT 5",
                                        ("close",)
                                    )
                                    _buys  = _c2.fetchall()
                                    _conn2.close()
                                for _row in _buys:
                                    _pb2.submit_order(
                                        ticker      = _row[0],
                                        side        = "buy",
                                        qty         = int(_row[1]) if _row[1] else 1,
                                        order_type  = "limit",
                                        limit_price = float(_row[2]) if _row[2] else None,
                                        stop_price  = float(_row[3]) if _row[3] else None,
                                        notes       = f"conviction_close score={_row[4] or 0:.0f}",
                                    )
                            except Exception as _pb_buy:
                                print(f"  [broker] conviction buy submit failed: {_pb_buy}")
                            try:
                                get_broker().snapshot_daily_stats()
                            except Exception as _pbs:
                                print(f"  [broker] snapshot_daily_stats failed: {_pbs}")
                        except Exception as _cve:
                            print(f"  [conviction] close scan failed: {_cve}")
                if et.hour == 16 and 15 <= et.minute < 20 and not eod_report_sent:
                    print("\n[EOD REPORT] Generating end-of-day report...")
                    try:
                        from eod_report import run_eod_report
                        run_eod_report()
                        eod_report_sent = True
                        state.log_alert("EOD report generated and sent")
                    except Exception as e:
                        print(f"  [EOD] Failed: {e}")
                    if _IC_AVAILABLE:
                        try:
                            for _hz in (1, 3, 5, 10):
                                _ic_result = compute_and_store_ic(horizon_days=_hz)
                                _n = len(_ic_result.get("ics", {}))
                                if _n:
                                    print(f"  [IC] horizon={_hz}d — {_n} factor ICs computed")
                            _log("info", "Factor IC computation complete (1d/3d/5d/10d)")
                        except Exception as _ice2:
                            print(f"  [IC] compute failed: {_ice2}")
                    try:
                        from reports.checkpoint_reports import check_and_run_checkpoints
                        check_and_run_checkpoints()
                    except Exception as _cp_e:
                        print(f"  [checkpoint] Failed: {_cp_e}")
                    try:
                        from db.database import close_paper_trades_eod
                        close_paper_trades_eod(
                            {t: p for t, p in state.last_prices.items() if p > 0}
                        )
                        print("  ✓ Paper trades closed for EOD")
                    except Exception as e:
                        print(f"  [paper] EOD close failed: {e}")

            # ── AFTERHOURS ────────────────────────────────────────────────────
            elif mode == "AFTERHOURS":
                state.scan_count += 1
                print(f"\n[AH SCAN #{state.scan_count}] {et.strftime('%H:%M:%S ET')} — {len(watchlist)} stocks")
                _log("info", f"After-hours scan #{state.scan_count} | {et.strftime('%H:%M ET')}")
                try:
                    import yfinance as yf
                    for _ah_ticker in watchlist[:20]:
                        try:
                            _ah_hist = yf.Ticker(_ah_ticker).history(period="1d", prepost=True)
                            if _ah_hist.empty:
                                continue
                            _ah_price = float(_ah_hist["Close"].iloc[-1])
                            _ah_prev  = state.last_prices.get(_ah_ticker, 0)
                            if _ah_prev > 0:
                                _ah_move = (_ah_price - _ah_prev) / _ah_prev * 100
                                if abs(_ah_move) >= 5.0:
                                    _ah_mv_key = f"{_ah_ticker}_ah_move_{today_str}_{et.hour}"
                                    if not state.already_alerted(_ah_mv_key):
                                        send_alert(
                                            title=f"{'🌙🟢' if _ah_move > 0 else '🌙🔴'} {_ah_ticker} AH {_ah_move:+.1f}%",
                                            message=(
                                                f"After-hours move: {_ah_move:+.1f}%\n"
                                                f"AH Price: ${_ah_price:.4f} | Prev Close: ${_ah_prev:.4f}\n"
                                                f"Time: {et.strftime('%H:%M ET')}"
                                            ),
                                            priority=PRIORITY_HIGH if abs(_ah_move) >= 8 else PRIORITY_NORMAL,
                                        )
                                        state.mark_alerted(_ah_mv_key)
                                        state.log_alert(f"{_ah_ticker} AH {_ah_move:+.1f}%")
                            state.last_prices[_ah_ticker] = _ah_price
                        except Exception:
                            pass
                except Exception as _yf_ah:
                    print(f"  [AH] yfinance fetch failed: {_yf_ah}")
                try:
                    from paper_broker import get_broker
                    _pb3 = get_broker()
                    _pb3.process_pending_orders()
                    _pb3.update_all_positions()
                except Exception as _pbah:
                    print(f"  [broker] AH update failed: {_pbah}")
                if et.hour == 20 and 30 <= et.minute < 35:
                    _ah_key = f"conviction_ah_{today_str}"
                    if not state.already_alerted(_ah_key):
                        print("\n[CONVICTION] Running 8:30 PM after-hours conviction scan...")
                        try:
                            from conviction_engine import run_conviction_engine
                            run_conviction_engine(session="afterhours", regime=state.current_regime)
                            state.last_conviction_run_ts = now_et().isoformat()
                            state.mark_alerted(_ah_key)
                        except Exception as _cve2:
                            print(f"  [conviction] afterhours scan failed: {_cve2}")
                if et.hour == 22 and 0 <= et.minute < 5:
                    _acc_key = f"accuracy_nightly_{today_str}"
                    if not state.already_alerted(_acc_key):
                        print("\n[ACCURACY] Running nightly validation...")
                        try:
                            from accuracy_validator import run_nightly_validation
                            run_nightly_validation()
                            state.last_accuracy_run_ts = now_et().isoformat()
                            state.mark_alerted(_acc_key)
                        except Exception as _ave:
                            print(f"  [accuracy] nightly validation failed: {_ave}")
                try:
                    from reports.checkpoint_reports import check_and_run_checkpoints
                    check_and_run_checkpoints()
                except Exception as _cpe:
                    print(f"  [checkpoint] afterhours checkpoint check failed: {_cpe}")

            # ── OVERNIGHT ─────────────────────────────────────────────────────
            else:
                print(f"  [OVERNIGHT {et.strftime('%H:%M ET')}] Light scan — conviction buys + portfolio")
                _log("info", f"Overnight scan | {et.strftime('%H:%M ET')}")
                if (et - state.last_sec_check).total_seconds() >= 300:
                    try:
                        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
                        if _is_postgres():
                            _oconn = _get_pg_conn()
                            _oc    = _oconn.cursor()
                            _oc.execute("SELECT ticker FROM conviction_buys ORDER BY created_at DESC LIMIT 20")
                            _overnight_wl = [r[0] for r in _oc.fetchall()]
                            _oconn.close()
                        else:
                            _oconn = _get_sqlite_conn()
                            _oc    = _oconn.cursor()
                            _oc.execute("SELECT ticker FROM conviction_buys ORDER BY created_at DESC LIMIT 20")
                            _overnight_wl = [r[0] for r in _oc.fetchall()]
                            _oconn.close()
                    except Exception:
                        _overnight_wl = []
                    if _overnight_wl:
                        check_sec_filings(_overnight_wl, state)
                try:
                    from paper_broker import get_broker
                    _pb4 = get_broker()
                    _pb4.process_pending_orders()
                    _pb4.update_all_positions()
                except Exception as _pbon:
                    print(f"  [broker] overnight update failed: {_pbon}")
                if et.hour == 22 and 0 <= et.minute < 5:
                    _acc_key2 = f"accuracy_nightly_{today_str}"
                    if not state.already_alerted(_acc_key2):
                        print("\n[ACCURACY] Running nightly validation...")
                        try:
                            from accuracy_validator import run_nightly_validation
                            run_nightly_validation()
                            state.last_accuracy_run_ts = now_et().isoformat()
                            state.mark_alerted(_acc_key2)
                        except Exception as _ave2:
                            print(f"  [accuracy] nightly validation failed: {_ave2}")

            time.sleep(_SLEEP_MAP.get(mode, 60))

        except KeyboardInterrupt:
            print("\nScanner stopped.")
            _log("warning", "Scanner stopped via KeyboardInterrupt")
            break
        except Exception as e:
            print(f"  [scanner] Error: {e}")
            _log("error", f"Scanner loop error: {e}")
            time.sleep(30)

  except Exception as _fatal:
    print("\n[FATAL] run_scanner() crashed before entering main loop:")
    traceback.print_exc()
    raise


if __name__ == "__main__":
    run_scanner()
