# prediction_engine/stage8_sanity.py
# Stage 8 (9:45 AM ET): Lightweight CPU-only post-open sanity check.
# No SLMs involved. Validates each pick is still acting within thesis.
# Cancels picks that have broken their stop or reversed direction.

import logging
import requests
import yfinance as yf
from typing import Optional

from .config import FINNHUB_API_KEY
from .stage7_notify import send_cancellation
from .db import _et_date

logger = logging.getLogger("pe.stage8")

FINNHUB_BASE = "https://finnhub.io/api/v1"


def run(picks: list) -> list:
    """
    Validate each pick against live market data at 9:45 AM.
    Returns the subset of picks still valid after sanity checks.
    Sends Pushover cancellation for any removed pick.
    """
    if not picks:
        logger.info("[stage8] No picks to validate")
        return []

    logger.info("[stage8] Running post-open sanity check on %d picks", len(picks))
    valid   = []
    removed = []

    for pick in picks:
        ticker = pick["ticker"]
        entry  = pick["entry"]
        stop   = pick["stop"]

        live_price = _get_live_price(ticker)
        if live_price is None or live_price <= 0:
            logger.warning("[stage8] %s: could not fetch live price — keeping pick", ticker)
            valid.append(pick)
            continue

        # Check 1: Price has breached stop
        if live_price <= stop:
            reason = (f"Live price ${live_price:.2f} has breached stop ${stop:.2f} "
                      f"({((live_price - stop) / stop * 100):+.1f}%%).")
            logger.warning("[stage8] %s CANCELLED — %s", ticker, reason)
            removed.append((pick, reason))
            send_cancellation(ticker, reason)
            continue

        # Check 2: Price has gapped DOWN more than 5% below entry (thesis broken preopen)
        if live_price < entry * 0.95:
            reason = (f"Live price ${live_price:.2f} is more than 5%% below "
                      f"entry ${entry:.2f} — thesis reversal.")
            logger.warning("[stage8] %s CANCELLED — %s", ticker, reason)
            removed.append((pick, reason))
            send_cancellation(ticker, reason)
            continue

        # Check 3: Volume has dried up — first 15-min volume well below expected
        intraday_vol = _get_intraday_volume(ticker)
        avg_daily    = pick.get("avg_volume", 0)
        if intraday_vol is not None and avg_daily > 0:
            # By 9:45, expect at least 5% of average daily volume traded
            expected_min = avg_daily * 0.05
            if intraday_vol < expected_min * 0.3:
                reason = (f"Intraday volume {intraday_vol:,} far below expected "
                          f"minimum {int(expected_min):,} — volume thesis not confirmed.")
                logger.warning("[stage8] %s CANCELLED — %s", ticker, reason)
                removed.append((pick, reason))
                send_cancellation(ticker, reason)
                continue

        pick["live_price_at_sanity"] = live_price
        valid.append(pick)
        logger.info(
            "[stage8] %s VALID — live=$%.2f  entry=$%.2f  stop=$%.2f  "
            "drift=%+.1f%%",
            ticker, live_price, entry, stop,
            (live_price - entry) / entry * 100,
        )

    logger.info(
        "[stage8] Sanity check complete: %d valid, %d cancelled",
        len(valid), len(removed),
    )
    return valid


def _get_live_price(ticker: str) -> Optional[float]:
    """Try Finnhub first, then yfinance."""
    if FINNHUB_API_KEY:
        try:
            resp = requests.get(
                f"{FINNHUB_BASE}/quote",
                params={"symbol": ticker, "token": FINNHUB_API_KEY},
                timeout=8,
            )
            if resp.status_code == 200:
                price = resp.json().get("c", 0)
                if price and price > 0:
                    return float(price)
        except Exception as e:
            logger.debug("[stage8] Finnhub quote for %s failed: %s", ticker, e)

    try:
        tk   = yf.Ticker(ticker)
        hist = tk.history(period="1d", interval="1m", prepost=False)
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception as e:
        logger.debug("[stage8] yfinance quote for %s failed: %s", ticker, e)

    return None


def _get_intraday_volume(ticker: str) -> Optional[int]:
    """Return total traded volume since open (best effort)."""
    try:
        tk   = yf.Ticker(ticker)
        hist = tk.history(period="1d", interval="1m", prepost=False)
        if not hist.empty:
            return int(hist["Volume"].sum())
    except Exception:
        pass
    return None
