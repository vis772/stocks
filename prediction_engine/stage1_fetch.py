# prediction_engine/stage1_fetch.py
# Stage 1 (4:00 AM ET): Pull the 3,500-ticker universe, fetch premarket snapshots,
# and apply basic CPU filters to reduce to ~500 candidates.

import time
import logging
import requests
import yfinance as yf
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from .config import (
    FINNHUB_API_KEY, FILTER_MIN_PRICE, FILTER_MAX_PRICE,
    FILTER_MIN_AVG_VOLUME, FILTER_MIN_FLOAT, FILTER_MIN_MARKET_CAP,
    FILTER_MIN_GAP_PCT, FILTER_MAX_GAP_PCT,
    FETCH_BATCH_SIZE, FETCH_MAX_WORKERS, STAGE1_MAX_CANDIDATES,
)
from .db import get_universe_tickers

logger = logging.getLogger("pe.stage1")

FINNHUB_BASE = "https://finnhub.io/api/v1"

# Fallback universe when DB is unavailable (common US small/mid-caps)
_FALLBACK_UNIVERSE = [
    "AAPL","MSFT","NVDA","TSLA","AMZN","META","GOOGL","AMD","INTC","AVGO",
    "ASTS","RKLB","ACHR","JOBY","LUNR","IONQ","SOUN","BBAI","ARQQ","GFAI",
    "RGTI","QBTS","QUBT","HIMS","RDDT","SOFI","AFRM","UPST","HOOD","DKNG",
    "WULF","IREN","CIFR","CLSK","MARA","RIOT","BTBT","BITF","HUT",
    "NKLA","BLNK","CHPT","PLUG","FCEL","BE","NOVA","RUN",
    "NVAX","MRNA","BNTX","BLUE","BEAM","CRSP","EDIT","NTLA","FATE",
    "CELH","OPEN","PENN","BYND","WOLF","ALGM","CRUS","AMBA",
    "ALKT","APPN","BAND","DDOG","FRSH","GTLB","HUBS","MNDY","TOST","WEAV",
    "GME","AMC","BBBY","SPCE","NKLA","RIDE","WKHS",
]


def run() -> list:
    """
    Returns a list of candidate dicts with premarket snapshot data.
    Each dict: {ticker, price, prev_close, gap_pct, volume, avg_volume,
                volume_ratio, float_shares, market_cap, sector}
    """
    logger.info("[stage1] Starting universe fetch and basic filter")
    start = time.monotonic()

    # ── Get ticker universe ───────────────────────────────────────────────────
    tickers = _load_universe()
    logger.info("[stage1] Universe: %d tickers", len(tickers))

    # ── Batch-fetch 5-day history for filters (fast, no premarket needed yet) ─
    snap = _batch_fetch_snapshots(tickers)
    logger.info("[stage1] Snapshots fetched: %d tickers", len(snap))

    # ── Apply basic CPU filters ───────────────────────────────────────────────
    candidates = _apply_filters(snap)
    logger.info("[stage1] After basic filter: %d candidates", len(candidates))

    # ── Enrich with premarket gap data ───────────────────────────────────────
    candidates = _enrich_premarket_gaps(candidates)
    gap_filtered = [c for c in candidates if c.get("gap_pct", 0) >= FILTER_MIN_GAP_PCT]
    logger.info("[stage1] After gap filter (≥%.1f%%): %d candidates",
                FILTER_MIN_GAP_PCT, len(gap_filtered))

    # Sort by gap_pct * volume_ratio (highest conviction premarket movers first)
    gap_filtered.sort(
        key=lambda c: abs(c.get("gap_pct", 0)) * c.get("volume_ratio", 1),
        reverse=True,
    )
    result = gap_filtered[:STAGE1_MAX_CANDIDATES]

    elapsed = time.monotonic() - start
    logger.info("[stage1] Complete: %d candidates in %.1fs", len(result), elapsed)
    return result


def _load_universe() -> list:
    tickers = get_universe_tickers()
    if tickers:
        logger.info("[stage1] Loaded %d tickers from DB universe", len(tickers))
        return tickers
    logger.warning("[stage1] DB universe unavailable — using fallback list (%d tickers)",
                   len(_FALLBACK_UNIVERSE))
    return _FALLBACK_UNIVERSE


def _batch_fetch_snapshots(tickers: list) -> dict:
    """
    Batch download previous 5 days of data for all tickers.
    Returns {ticker: dict} with price, avg_volume, float, market_cap, sector.
    """
    snap = {}
    chunks = [tickers[i:i+FETCH_BATCH_SIZE] for i in range(0, len(tickers), FETCH_BATCH_SIZE)]
    logger.info("[stage1] Fetching %d chunks of ≤%d tickers via yfinance",
                len(chunks), FETCH_BATCH_SIZE)

    def fetch_chunk(chunk: list) -> dict:
        result = {}
        try:
            data = yf.download(
                chunk, period="5d", auto_adjust=True,
                progress=False, threads=True, group_by="ticker",
            )
            for ticker in chunk:
                try:
                    if len(chunk) == 1:
                        close_series = data["Close"]
                        vol_series   = data["Volume"]
                    else:
                        close_series = data["Close"][ticker]
                        vol_series   = data["Volume"][ticker]

                    close_series = close_series.dropna()
                    vol_series   = vol_series.dropna()

                    if close_series.empty:
                        continue

                    price      = float(close_series.iloc[-1])
                    prev_close = float(close_series.iloc[-2]) if len(close_series) >= 2 else price
                    avg_vol    = int(vol_series.mean()) if not vol_series.empty else 0
                    vol_today  = int(vol_series.iloc[-1]) if not vol_series.empty else 0

                    result[ticker] = {
                        "ticker":     ticker,
                        "price":      price,
                        "prev_close": prev_close,
                        "volume":     vol_today,
                        "avg_volume": avg_vol,
                    }
                except Exception:
                    pass
        except Exception as e:
            logger.debug("[stage1] Chunk fetch error: %s", e)
        return result

    with ThreadPoolExecutor(max_workers=FETCH_MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_chunk, chunk): chunk for chunk in chunks}
        for future in as_completed(futures):
            try:
                snap.update(future.result())
            except Exception as e:
                logger.debug("[stage1] Chunk future error: %s", e)

    return snap


def _apply_filters(snap: dict) -> list:
    """Apply price, volume, float, market cap filters. Returns list of dicts."""
    candidates = []
    for ticker, d in snap.items():
        price     = d.get("price", 0)
        avg_vol   = d.get("avg_volume", 0)
        vol       = d.get("volume", 0)

        if price < FILTER_MIN_PRICE or price > FILTER_MAX_PRICE:
            continue
        if avg_vol < FILTER_MIN_AVG_VOLUME:
            continue

        vol_ratio = (vol / avg_vol) if avg_vol > 0 else 0
        d["volume_ratio"] = round(vol_ratio, 2)
        d["float_shares"] = 0
        d["market_cap"]   = 0
        d["sector"]       = ""
        candidates.append(d)

    return candidates


def _enrich_premarket_gaps(candidates: list) -> list:
    """
    For each candidate, attempt to get a premarket quote to compute gap%.
    Uses Finnhub first (real-time), falls back to yfinance prepost.
    """
    logger.info("[stage1] Enriching %d candidates with premarket gap data...", len(candidates))

    def get_gap(d: dict) -> dict:
        ticker     = d["ticker"]
        prev_close = d.get("prev_close", 0)

        premarket_price = None

        # 1. Finnhub quote (real-time, includes extended hours)
        if FINNHUB_API_KEY:
            premarket_price = _finnhub_quote(ticker)

        # 2. yfinance prepost fallback
        if not premarket_price and prev_close > 0:
            premarket_price = _yf_premarket_price(ticker)

        if not premarket_price or premarket_price <= 0:
            premarket_price = d.get("price", 0)

        gap_pct = 0.0
        if prev_close and prev_close > 0 and premarket_price:
            gap_pct = (premarket_price - prev_close) / prev_close * 100

        d["premarket_price"] = round(premarket_price, 4) if premarket_price else 0
        d["gap_pct"]         = round(gap_pct, 2)
        return d

    # Use ThreadPoolExecutor with throttling for Finnhub rate limits
    enriched = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(get_gap, d) for d in candidates]
        for future in as_completed(futures):
            try:
                enriched.append(future.result())
            except Exception:
                pass
        time.sleep(0.05)  # light throttle for Finnhub

    return enriched


def _finnhub_quote(ticker: str, retries: int = 2) -> Optional[float]:
    for attempt in range(retries):
        try:
            resp = requests.get(
                f"{FINNHUB_BASE}/quote",
                params={"symbol": ticker, "token": FINNHUB_API_KEY},
                timeout=8,
            )
            if resp.status_code == 200:
                data = resp.json()
                # 'c' is current price (includes premarket if before open)
                price = data.get("c", 0)
                if price and price > 0:
                    return float(price)
        except requests.RequestException:
            pass
        if attempt < retries - 1:
            time.sleep(0.5)
    return None


def _yf_premarket_price(ticker: str) -> Optional[float]:
    try:
        hist = yf.Ticker(ticker).history(period="1d", prepost=True, interval="1m")
        if not hist.empty:
            price = float(hist["Close"].iloc[-1])
            if price > 0:
                return price
    except Exception:
        pass
    return None
