# data/tiingo_stream.py
# Tiingo IEX REST batch poller.
# Replaces the former WebSocket client which suffered repeated subscription errors.
#
# Polls GET https://api.tiingo.com/iex?tickers=...&token=...
# up to 100 tickers per call in a background daemon thread.
# Interval: 60 s (MARKET), 120 s (PREMARKET/AFTERHOURS), 300 s otherwise.
# Reads TIINGO_API_KEY from env — never hardcoded.
#
# Public API is identical to the old TiingoStream so scanner_loop.py
# needs zero import changes:
#   start(tickers), update_tickers(tickers),
#   get_last(ticker) → dict | None,
#   get_age_ms(ticker) → int (ms),
#   stop()

import os
import time
import threading
import requests
from typing import Dict, List, Optional, Any


class TiingoStream:
    """
    Tiingo IEX REST batch poller with a drop-in API matching the old WebSocket client.

    Background thread fires GET /iex for all watched tickers, batched in
    groups of BATCH_SIZE (100). Results land in _last[ticker]; callers
    use get_last() / get_age_ms() exactly as before.
    """

    BATCH_SIZE = 100

    def __init__(self, token: str):
        if not token:
            raise ValueError("TiingoStream requires a non-empty token")
        self._token    = token
        self._lock     = threading.Lock()
        self._last:    Dict[str, Dict[str, Any]] = {}
        self._tickers: List[str] = []
        self._thread:  Optional[threading.Thread] = None
        self._stop_evt = threading.Event()

    # ── Public API ───────────────────────────────────────────────────────────

    def start(self, tickers: List[str]) -> None:
        """Start background polling in a daemon thread. Safe to call repeatedly."""
        with self._lock:
            self._tickers = sorted({t.upper() for t in tickers if t})
        if self._thread and self._thread.is_alive():
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(
            target=self._poll_loop, daemon=True, name="tiingo-rest-poller"
        )
        self._thread.start()
        print(f"  [tiingo_rest] started — {len(self._tickers)} tickers")

    def update_tickers(self, tickers: List[str]) -> None:
        """Replace the tracked ticker list; takes effect on next poll cycle."""
        new = sorted({t.upper() for t in tickers if t})
        with self._lock:
            old_n = len(self._tickers)
            self._tickers = new
        if len(new) != old_n:
            print(f"  [tiingo_rest] ticker list updated: {len(new)} tickers")

    def get_last(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Return latest cached tick for ticker, or None if never seen."""
        with self._lock:
            row = self._last.get(ticker.upper())
            return dict(row) if row else None

    def get_age_ms(self, ticker: str) -> int:
        """Milliseconds since the last successful poll for this ticker."""
        with self._lock:
            row = self._last.get(ticker.upper())
        if not row:
            return 10 ** 9
        return int((time.time() - row["received_at"]) * 1000)

    def stop(self) -> None:
        self._stop_evt.set()

    # ── Background polling ────────────────────────────────────────────────────

    def _poll_loop(self) -> None:
        while not self._stop_evt.is_set():
            try:
                self._poll_once()
            except Exception as e:
                print(f"  [tiingo_rest] poll error: {e}")
            interval = self._poll_interval()
            deadline = time.time() + interval
            while time.time() < deadline and not self._stop_evt.is_set():
                time.sleep(2)

    def _poll_interval(self) -> int:
        """Return seconds between polls based on current ET time-of-day."""
        try:
            import pytz
            from datetime import datetime as _dt
            et  = _dt.now(pytz.timezone("America/New_York"))
            wd, h, m = et.weekday(), et.hour, et.minute
            if wd >= 5:
                return 300                                       # WEEKEND
            after_open   = (h > 9) or (h == 9 and m >= 25)
            before_close = (h < 16) or (h == 16 and m <= 30)
            if after_open and before_close:
                return 60                                        # MARKET
            if h >= 4 and not after_open:
                return 120                                       # PREMARKET
            if (h == 16 and m > 30) or (17 <= h < 20):
                return 120                                       # AFTERHOURS
            return 300                                           # OVERNIGHT
        except Exception:
            return 60

    def _poll_once(self) -> None:
        with self._lock:
            tickers = list(self._tickers)
        if not tickers:
            return
        updated = 0
        for i in range(0, len(tickers), self.BATCH_SIZE):
            batch   = tickers[i : i + self.BATCH_SIZE]
            results = self._fetch_batch(batch)
            now = time.time()
            with self._lock:
                for r in results:
                    r["received_at"] = now
                    self._last[r["ticker"]] = r
            updated += len(results)
        if updated:
            print(f"  [tiingo_rest] polled {updated}/{len(tickers)} tickers")

    def _fetch_batch(self, tickers: List[str]) -> List[Dict[str, Any]]:
        """
        Call GET /iex?tickers=AAPL,MSFT,...&token=...
        Returns list of normalised tick dicts (no received_at yet).
        """
        try:
            resp = requests.get(
                "https://api.tiingo.com/iex",
                params={"tickers": ",".join(tickers), "token": self._token},
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
        except Exception as e:
            print(f"  [tiingo_rest] request failed: {e}")
            return []

        if resp.status_code != 200:
            print(f"  [tiingo_rest] HTTP {resp.status_code}: {resp.text[:120]}")
            return []
        try:
            data = resp.json()
        except Exception:
            return []
        if not isinstance(data, list):
            return []

        out = []
        for d in data:
            ticker = (d.get("ticker") or "").upper()
            price  = float(d.get("last") or d.get("tngoLast") or 0)
            if not ticker or price <= 0:
                continue
            out.append({
                "ticker":    ticker,
                "price":     price,
                "prevClose": float(d.get("prevClose") or price),
                "volume":    float(d.get("lastVolume") or 0),
                "high":      float(d.get("high") or price),
                "low":       float(d.get("low") or price),
                "source":    "tiingo_rest",
            })
        return out
