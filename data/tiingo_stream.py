# data/tiingo_stream.py
# Tiingo IEX real-time WebSocket client.
#
# Connects to wss://api.tiingo.com/iex, subscribes to a list of tickers,
# parses incoming Q (top-of-book + last sale) and T (trade) messages,
# and exposes the latest price + a rolling tick history per ticker
# from in-memory state. Designed to run as a daemon thread inside
# scanner_loop.py.
#
# When TIINGO_TOKEN is unset, scanner_loop never instantiates this class —
# the scanner falls back to Finnhub REST polling exactly as before.
#
# Future steps (VWAP, momentum ranking, paper trading) consume the
# rolling tick deque via get_recent_ticks(). Per-tick DB writes are
# intentionally avoided — Postgres is touched only by existing flows.

import os
import json
import time
import threading
from collections import deque
from datetime import datetime
from typing import Dict, List, Optional, Set, Any

try:
    import websocket  # provided by websocket-client
except ImportError:
    websocket = None

TIINGO_WS_URL = "wss://api.tiingo.com/iex"

# Tiingo IEX A-message data layout (positional). The official spec is sparsely
# documented; indices below are the consensus from the Tiingo Python client and
# live observation. Code parses defensively — out-of-range or non-numeric
# fields are silently skipped instead of crashing the stream.
#
# Q (top-of-book + last sale):
#   [0]  type ("Q")
#   [1]  date (ISO-8601)
#   [2]  nanoseconds since epoch
#   [3]  ticker (lowercase)
#   [4]  bidSize     [5]  bidPrice
#   [6]  midPrice
#   [7]  askSize     [8]  askPrice
#   [9]  lastSaleTimestamp (ISO)
#   [10] lastSaleSize    [11] lastSalePrice
#   [12] halted   [13] afterHours   [14] intermarketSweep   [15] oddLot
#
# T (trade-only):
#   [0]  type ("T")
#   [1]  date    [2]  nanoseconds   [3]  ticker
#   [4]  lastSize    [5]  lastPrice
#   [6]  halted   [7]  afterHours   ...


def _safe_float(arr: list, idx: int) -> Optional[float]:
    try:
        v = arr[idx]
    except (IndexError, TypeError):
        return None
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None


def _safe_int(arr: list, idx: int) -> Optional[int]:
    try:
        v = arr[idx]
    except (IndexError, TypeError):
        return None
    if v is None:
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _safe_str(arr: list, idx: int) -> Optional[str]:
    try:
        v = arr[idx]
    except (IndexError, TypeError):
        return None
    if v is None:
        return None
    try:
        return str(v)
    except Exception:
        return None


class TiingoStream:
    """Tiingo IEX WebSocket client with auto-reconnect and in-memory tick store.

    Public API:
      start(tickers)           — connects in a daemon thread and subscribes
      update_tickers(tickers)  — diff-based add/remove of subscriptions
      get_last(ticker)         — newest tick dict, or None
      get_age_ms(ticker)       — staleness of newest tick in milliseconds
      get_recent_ticks(ticker) — snapshot list of bounded deque (for VWAP etc.)
      stop()                   — graceful shutdown

    Threading: all mutations of _last/_recent/_subs go through self._lock
    (an RLock). Reads return defensive copies so callers never see torn state.
    """

    def __init__(self, token: str, threshold_level: int = 5, max_ticks: int = 600):
        if not token:
            raise ValueError("TiingoStream requires a non-empty token")
        if websocket is None:
            raise ImportError(
                "websocket-client not installed — add `websocket-client>=1.6.0` "
                "to requirements.txt"
            )

        self._token            = token
        self._threshold_level  = threshold_level
        self._max_ticks        = max_ticks

        self._lock             = threading.RLock()
        self._last:    Dict[str, Dict[str, Any]] = {}
        self._recent:  Dict[str, deque]          = {}
        self._subs:    Set[str]                  = set()

        self._ws:      Optional[websocket.WebSocketApp] = None
        self._thread:  Optional[threading.Thread]       = None
        self._stop_evt = threading.Event()
        self._connected_evt = threading.Event()
        self._backoff  = 1   # seconds; reset on subscribe ack

    # ── Public API ───────────────────────────────────────────────────────────

    def start(self, tickers: List[str]) -> None:
        """Open the WebSocket in a background thread and subscribe."""
        with self._lock:
            self._subs = {t.upper() for t in tickers if t}
        if self._thread and self._thread.is_alive():
            # Already running; just send a subscribe diff.
            self._send_subscribe(self._subs)
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()
        print(f"  [tiingo] started, {len(self._subs)} tickers")

    def update_tickers(self, tickers: List[str]) -> None:
        """Diff against current subs; subscribe to additions, unsubscribe from removals."""
        new_set = {t.upper() for t in tickers if t}
        with self._lock:
            current = set(self._subs)
            additions = new_set - current
            removals  = current - new_set
            self._subs = new_set
        if additions:
            self._send_subscribe(additions)
        if removals:
            self._send_unsubscribe(removals)
        if additions or removals:
            print(f"  [tiingo] sub diff: +{len(additions)} / -{len(removals)} (total {len(new_set)})")

    def get_last(self, ticker: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._last.get(ticker.upper())
            return dict(row) if row else None

    def get_age_ms(self, ticker: str) -> int:
        with self._lock:
            row = self._last.get(ticker.upper())
        if not row:
            return 10 ** 9
        return int((time.time() - row["received_at"]) * 1000)

    def get_recent_ticks(self, ticker: str) -> List[Dict[str, Any]]:
        with self._lock:
            d = self._recent.get(ticker.upper())
            return list(d) if d else []

    def stop(self) -> None:
        self._stop_evt.set()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

    # ── WebSocket lifecycle ──────────────────────────────────────────────────

    def _run_forever(self) -> None:
        while not self._stop_evt.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    TIINGO_WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                # ping_interval drives liveness; the lib also closes the
                # socket if no pong returns within ping_timeout.
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                print(f"  [tiingo] run_forever error: {e}")

            if self._stop_evt.is_set():
                break

            # Exponential backoff on disconnect.
            wait = min(self._backoff, 60)
            print(f"  [tiingo] reconnecting in {wait}s")
            slept = 0
            while slept < wait and not self._stop_evt.is_set():
                time.sleep(0.5)
                slept += 0.5
            self._backoff = min(self._backoff * 2, 60)

    def _on_open(self, ws):
        with self._lock:
            tickers = sorted(self._subs)
        print(f"  [tiingo] connected — subscribing to {len(tickers)} tickers")
        if tickers:
            self._send_subscribe(tickers, ws=ws)

    def _on_close(self, ws, status_code, msg):
        self._connected_evt.clear()
        if status_code or msg:
            print(f"  [tiingo] closed (code={status_code}, msg={msg})")

    def _on_error(self, ws, err):
        print(f"  [tiingo] error: {err}")

    def _on_message(self, ws, message: str):
        try:
            msg = json.loads(message)
        except Exception:
            return
        mtype = msg.get("messageType")
        if mtype == "A":
            data = msg.get("data") or []
            if data:
                self._parse_a(data)
        elif mtype == "I":
            # subscribe / info ack
            self._connected_evt.set()
            self._backoff = 1
            resp = msg.get("response") or {}
            code = resp.get("code")
            if code == 200:
                with self._lock:
                    n = len(self._subs)
                print(f"  [tiingo] subscribed to {n} tickers")
        elif mtype == "H":
            return  # heartbeat
        elif mtype == "E":
            print(f"  [tiingo] server error: {msg.get('response')}")

    # ── Subscribe / unsubscribe ──────────────────────────────────────────────

    def _send_subscribe(self, tickers, ws=None) -> None:
        ws = ws or self._ws
        if ws is None:
            return
        payload = {
            "eventName":     "subscribe",
            "authorization": self._token,
            "eventData": {
                "thresholdLevel": self._threshold_level,
                "tickers":        [t.lower() for t in tickers],
            },
        }
        try:
            ws.send(json.dumps(payload))
        except Exception as e:
            print(f"  [tiingo] subscribe send failed: {e}")

    def _send_unsubscribe(self, tickers, ws=None) -> None:
        ws = ws or self._ws
        if ws is None:
            return
        payload = {
            "eventName":     "unsubscribe",
            "authorization": self._token,
            "eventData": {
                "thresholdLevel": self._threshold_level,
                "tickers":        [t.lower() for t in tickers],
            },
        }
        try:
            ws.send(json.dumps(payload))
        except Exception as e:
            print(f"  [tiingo] unsubscribe send failed: {e}")

    # ── Message parsing ──────────────────────────────────────────────────────

    def _parse_a(self, data: list) -> None:
        try:
            kind = data[0]
        except (IndexError, TypeError):
            return

        if kind == "Q":
            ticker = _safe_str(data, 3)
            bid    = _safe_float(data, 5)
            ask    = _safe_float(data, 8)
            last   = _safe_float(data, 11)
            size   = _safe_int(data, 10)
            ts_iso = _safe_str(data, 1)
            price  = last
            if price is None and bid and ask:
                price = (bid + ask) / 2
            elif price is None:
                price = bid or ask
        elif kind == "T":
            ticker = _safe_str(data, 3)
            size   = _safe_int(data, 4)
            price  = _safe_float(data, 5)
            ts_iso = _safe_str(data, 1)
            bid = ask = None
        else:
            return

        if not ticker or not price:
            return

        ticker = ticker.upper()
        self._record(ticker, price, ts_iso, bid, ask, size, source=f"tiingo:{kind}")

    def _record(self, ticker, price, ts_iso, bid, ask, size, source):
        tick = {
            "ticker":       ticker,
            "price":        float(price),
            "bid":          bid,
            "ask":          ask,
            "last_size":    size,
            "ts":           ts_iso,
            "received_at":  time.time(),
            "source":       source,
        }
        with self._lock:
            self._last[ticker] = tick
            d = self._recent.get(ticker)
            if d is None:
                d = deque(maxlen=self._max_ticks)
                self._recent[ticker] = d
            d.append(tick)


# ── Smoke-test entrypoint ────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    token = os.environ.get("TIINGO_TOKEN", "")
    if not token:
        print("Set TIINGO_TOKEN in your environment first.")
        sys.exit(1)

    arg = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    tickers = [t.strip().upper() for t in arg.split(",") if t.strip()]
    print(f"Subscribing to: {tickers}")

    s = TiingoStream(token)
    s.start(tickers)

    t0 = time.time()
    try:
        while time.time() - t0 < 30:
            time.sleep(2)
            for t in tickers:
                last = s.get_last(t)
                age  = s.get_age_ms(t)
                if last:
                    print(f"  {t}  ${last['price']:.4f}  age={age}ms  src={last['source']}")
                else:
                    print(f"  {t}  (no tick yet)  age={age}ms")
    finally:
        s.stop()
        print("Done.")
