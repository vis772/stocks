# universe_manager.py
# Manages the expanded US small/mid-cap universe (target: 1500–2500 tickers).
# Refresh strategy:
#   1. Bulk pull from Finnhub (all US symbols, free tier)
#   2. Pre-filter on name/symbol heuristics (remove ETFs, warrants, preferred)
#   3. Concurrent yfinance fast_info fetch (25 workers) for market_cap + avg_volume
#   4. Apply CRITERIA filters
#   5. Bulk-upsert to stock_universe + legacy universe table
#
# Refresh cadence: Saturday 8 AM ET (async, non-blocking)
# Startup bootstrap: triggered immediately if stock_universe < 500 rows

import os
import time
import threading
import requests
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

FINNHUB_BASE = "https://finnhub.io/api/v1"

CRITERIA = {
    "min_market_cap": 100_000_000,     # $100M floor — filters out micro-cap noise
    "max_market_cap": 20_000_000_000,  # $20B ceiling (small + mid-cap universe)
    "min_adv":        1_000_000,       # 1M shares/day — ensures real institutional interest
    "min_price":      2.00,            # $2+ minimum — eliminates most penny/OTC noise
    "max_price":      300.0,           # $300 cap — focus on actionable price range
}

# Symbols containing these patterns are almost certainly not common stock
_SKIP_PATTERNS = {
    ".", "-W", "-U", "-R", "W1", "UN", "UT", "P1",
    "WARR", "UNIT", "PREF", "ETF",
}

# Prevents concurrent refreshes from stacking (e.g. startup + Saturday trigger)
_REFRESH_LOCK = threading.Lock()


def _fh_key() -> str:
    return os.environ.get("FINNHUB_API_KEY", "")


def _log(msg: str) -> None:
    try:
        from db.database import log_scanner_event
        log_scanner_event("info", f"[universe] {msg}")
    except Exception:
        pass
    print(f"  [universe] {msg}")


def _is_common_stock_symbol(symbol: str) -> bool:
    """Quick heuristic to skip warrants, ETFs, preferred shares, units."""
    if len(symbol) > 6:
        return False
    for pat in _SKIP_PATTERNS:
        if pat in symbol:
            return False
    return True


def _fetch_finnhub_symbols() -> List[Dict]:
    """Pull all US stock symbols from Finnhub. Returns list of {ticker, name}."""
    key = _fh_key()
    if not key:
        _log("FINNHUB_API_KEY not set — cannot fetch symbol list")
        return []
    try:
        resp = requests.get(
            f"{FINNHUB_BASE}/stock/symbol",
            params={"exchange": "US", "token": key},
            timeout=30,
        )
        if resp.status_code != 200:
            _log(f"Finnhub symbol list HTTP {resp.status_code}")
            return []
        data = resp.json()
        result = []
        for item in data:
            sym = (item.get("symbol") or "").upper().strip()
            if not sym or not _is_common_stock_symbol(sym):
                continue
            result.append({
                "ticker": sym,
                "name":   item.get("description", ""),
                "mic":    item.get("mic", ""),
            })
        _log(f"Finnhub returned {len(data)} symbols → {len(result)} common-stock candidates")
        return result
    except Exception as e:
        _log(f"Finnhub symbol fetch error: {e}")
        return []


def _fetch_yfinance_concurrent(tickers: List[str], max_workers: int = 25) -> Dict[str, dict]:
    """
    Fetch yfinance fast_info for all tickers using a thread pool.
    25 workers × ~2s per ticker ≈ 8,000 tickers in ~10–15 min
    (vs 30–60 min sequential). Returns {ticker: {market_cap, avg_volume, price, exchange}}.
    """
    import yfinance as yf

    def _fetch_one(t: str):
        try:
            fi = yf.Ticker(t).fast_info
            mc   = int(getattr(fi, "market_cap",                  None) or 0)
            avol = int(getattr(fi, "three_month_average_volume",  None) or 0)
            pr   = float(getattr(fi, "last_price",                None) or 0)
            exch = str(getattr(fi, "exchange",                    None) or "")
            if mc > 0 or avol > 0:
                return t, {"market_cap": mc, "avg_volume": avol,
                           "price": pr, "exchange": exch, "sector": ""}
        except Exception:
            pass
        return None

    results: Dict[str, dict] = {}
    total     = len(tickers)
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_one, t): t for t in tickers}
        for future in as_completed(futures):
            completed += 1
            try:
                res = future.result(timeout=20)
                if res:
                    results[res[0]] = res[1]
            except Exception:
                pass
            if completed % 500 == 0:
                _log(f"yf progress {completed}/{total} — {len(results)} valid so far")

    return results


def _persist_universe(candidates: List[Dict]) -> Dict[str, int]:
    """
    Bulk-upsert candidates to stock_universe (single executemany call) and
    sync the legacy universe table. Returns {"added": N, "total": N}.
    """
    from db.database import (
        _is_postgres, _get_pg_conn, _get_sqlite_conn, _put_pg_conn,
        bulk_upsert_universe_stocks,
    )
    now_iso = datetime.now().isoformat()
    new_set = {c["ticker"] for c in candidates}

    # Bulk upsert stock_universe — one DB round-trip for all rows
    bulk_upsert_universe_stocks(candidates)

    # Sync legacy universe table (mark stale rows inactive)
    added = 0
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("SELECT ticker FROM universe WHERE active = TRUE")
            existing = {r[0] for r in cur.fetchall()}

            rows = [
                (c["ticker"], c.get("exchange", ""), c.get("market_cap", 0),
                 c.get("avg_volume", 0), c.get("sector", ""), now_iso)
                for c in candidates
            ]
            cur.executemany("""
                INSERT INTO universe
                    (ticker, exchange, market_cap, adv_20, sector, active, last_refreshed)
                VALUES (%s,%s,%s,%s,%s,TRUE,%s)
                ON CONFLICT (ticker) DO UPDATE SET
                    exchange=EXCLUDED.exchange, market_cap=EXCLUDED.market_cap,
                    adv_20=EXCLUDED.adv_20, sector=EXCLUDED.sector,
                    active=TRUE, last_refreshed=EXCLUDED.last_refreshed
            """, rows)

            stale = existing - new_set
            if stale:
                cur.executemany(
                    "UPDATE universe SET active=FALSE WHERE ticker=%s",
                    [(t,) for t in stale],
                )
            conn.commit(); cur.close(); _put_pg_conn(conn)
            added = len(new_set - existing)
        else:
            conn = _get_sqlite_conn()
            existing_rows = conn.execute(
                "SELECT ticker FROM universe WHERE active=1"
            ).fetchall()
            existing = {r[0] for r in existing_rows}

            rows = [
                (c["ticker"], c.get("exchange", ""), c.get("market_cap", 0),
                 c.get("avg_volume", 0), c.get("sector", ""), now_iso)
                for c in candidates
            ]
            conn.executemany("""
                INSERT OR REPLACE INTO universe
                    (ticker, exchange, market_cap, adv_20, sector, active, last_refreshed)
                VALUES (?,?,?,?,?,1,?)
            """, rows)

            stale = existing - new_set
            if stale:
                conn.executemany(
                    "UPDATE universe SET active=0 WHERE ticker=?",
                    [(t,) for t in stale],
                )
            conn.commit(); conn.close()
            added = len(new_set - existing)
    except Exception as e:
        _log(f"Legacy universe sync failed: {e}")

    return {"added": added, "total": len(candidates)}


def refresh_universe() -> Dict[str, int]:
    """
    Full universe refresh. Typical runtime: 10–20 min (concurrent yfinance fetch).
    Thread-safe: skips silently if another refresh is already running.

    Steps:
      1. Acquire lock (skip if already refreshing)
      2. Pull ~8K common-stock symbols from Finnhub
      3. Concurrent yfinance fast_info fetch (25 workers)
      4. Apply CRITERIA filters ($20M–$20B mcap, 500K ADV, $0.50–$500 price)
      5. Bulk-upsert to DB
    """
    if not _REFRESH_LOCK.acquire(blocking=False):
        _log("Refresh already in progress — skipping duplicate request")
        return {"added": 0, "removed": 0, "total": 0}

    try:
        _log("Starting full universe refresh...")
        start = time.time()

        # Step 1: Symbol list from Finnhub
        raw_symbols = _fetch_finnhub_symbols()
        if not raw_symbols:
            _log("Finnhub returned no symbols — aborting refresh")
            return {"added": 0, "removed": 0, "total": 0}

        tickers_only = [s["ticker"] for s in raw_symbols]
        sym_map      = {s["ticker"]: s for s in raw_symbols}

        # Step 2: Concurrent yfinance enrichment
        _log(f"Enriching {len(tickers_only)} symbols via yfinance (25 workers)...")
        yf_data = _fetch_yfinance_concurrent(tickers_only, max_workers=25)

        # Step 3: Apply CRITERIA
        candidates = []
        for ticker, info in yf_data.items():
            mc  = info.get("market_cap", 0)
            adv = info.get("avg_volume", 0)
            pr  = info.get("price", 0)
            if not (CRITERIA["min_market_cap"] <= mc <= CRITERIA["max_market_cap"]):
                continue
            if adv < CRITERIA["min_adv"]:
                continue
            if not (CRITERIA["min_price"] <= pr <= CRITERIA["max_price"]):
                continue
            sym_info = sym_map.get(ticker, {})
            candidates.append({
                "ticker":     ticker,
                "name":       sym_info.get("name", ""),
                "exchange":   info.get("exchange", ""),
                "market_cap": mc,
                "avg_volume": adv,
                "price":      pr,
                "sector":     info.get("sector", ""),
            })

        _log(f"Candidates after filter: {len(candidates)} (from {len(yf_data)} enriched)")

        if not candidates:
            _log("No candidates after filtering — persisting nothing")
            return {"added": 0, "removed": 0, "total": 0}

        # Step 4: Bulk persist
        result  = _persist_universe(candidates)
        elapsed = time.time() - start

        summary = (
            f"Universe refresh complete — {result['total']} stocks | "
            f"+{result['added']} new | {elapsed:.0f}s elapsed"
        )
        _log(summary)

        try:
            from alerts import send_alert, PRIORITY_NORMAL
            send_alert(
                title="Axiom — Universe Refreshed",
                message=f"{result['total']} small-cap stocks\n+{result['added']} new | {elapsed/60:.0f}min",
                priority=PRIORITY_NORMAL,
            )
        except Exception:
            pass

        return {**result, "removed": 0}

    finally:
        _REFRESH_LOCK.release()


def refresh_universe_async() -> threading.Thread:
    """
    Start refresh_universe() in a background daemon thread.
    Returns the thread so callers can optionally join().
    The lock inside refresh_universe() prevents duplicate runs.
    """
    t = threading.Thread(target=refresh_universe, daemon=True, name="universe-refresh")
    t.start()
    _log("Background universe refresh started")
    return t


def get_universe_tickers(min_market_cap: int = 100_000_000,
                          max_market_cap: int = 20_000_000_000,
                          min_adv: int = 1_000_000,
                          limit: int = 500) -> List[str]:
    """
    Fast path: return tickers from stock_universe (cached in DB).
    Falls back to legacy universe table → Finnhub symbol list → DEFAULT_UNIVERSE.
    """
    try:
        from db.database import get_active_universe
        tickers = get_active_universe(min_market_cap, max_market_cap, min_adv)
        if len(tickers) >= 50:
            return tickers[:limit]
        if tickers:
            _log(f"DB returned only {len(tickers)} tickers — too thin, checking fallbacks")
    except Exception as e:
        _log(f"get_universe_tickers DB query failed: {e}")

    # Try legacy universe table
    try:
        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute(
                "SELECT ticker FROM universe WHERE active=TRUE ORDER BY market_cap DESC LIMIT %s",
                (limit,)
            )
            rows = cur.fetchall(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute(
                "SELECT ticker FROM universe WHERE active=1 ORDER BY market_cap DESC LIMIT ?",
                (limit,)
            )
            rows = cur.fetchall(); conn.close()
        if rows:
            return [r[0] for r in rows]
    except Exception as e:
        _log(f"Legacy universe table fallback failed: {e}")

    # Dynamic fallback: Finnhub symbols (unfiltered — market-cap gate applied at scan time)
    _log("DB universe empty — fetching candidate list from Finnhub as temporary universe")
    raw = _fetch_finnhub_symbols()
    if raw:
        tickers = [s["ticker"] for s in raw]
        _log(f"Dynamic Finnhub fallback: {len(tickers)} symbol candidates")
        return tickers[:limit]

    from config import DEFAULT_UNIVERSE
    _log(f"All universe sources failed — using DEFAULT_UNIVERSE ({len(DEFAULT_UNIVERSE)} tickers)")
    return DEFAULT_UNIVERSE


def get_universe_size() -> int:
    """Return count of active tickers in stock_universe."""
    try:
        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM stock_universe WHERE active=TRUE")
            n = cur.fetchone()[0]; cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM stock_universe WHERE active=1")
            n = cur.fetchone()[0]; conn.close()
        return int(n or 0)
    except Exception:
        return 0


def needs_refresh(max_age_days: int = 7) -> bool:
    """Return True if stock_universe hasn't been refreshed within max_age_days."""
    try:
        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute(
                "SELECT MAX(last_updated) FROM stock_universe WHERE active=TRUE"
            )
            row = cur.fetchone(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute(
                "SELECT MAX(last_updated) FROM stock_universe WHERE active=1"
            )
            row = cur.fetchone(); conn.close()
        if not row or not row[0]:
            return True
        last = row[0]
        if isinstance(last, str):
            last = datetime.fromisoformat(last.replace("Z", ""))
        return (datetime.now() - last).days >= max_age_days
    except Exception:
        return True


# ─── Legacy compatibility shim ────────────────────────────────────────────────

class UniverseManager:
    """Thin wrapper kept for backward compatibility with old imports."""

    def refresh_universe(self) -> dict:
        return refresh_universe()

    def get_scan_batch(self, batch_size: int = 50) -> List[str]:
        tickers = get_universe_tickers(limit=batch_size * 10)
        return tickers[:batch_size]
