# universe_manager.py
# Manages the expanded US small/mid-cap universe (target: 1500–2500 tickers).
# Refresh strategy:
#   1. Bulk pull from Finnhub (all US symbols, free tier)
#   2. Pre-filter on name/symbol heuristics (remove ETFs, warrants, preferred)
#   3. Batch yfinance info on the remainder to get market_cap + avg_volume
#   4. Persist to stock_universe table + legacy universe table
#   5. Fall back to cached DB tickers if API is unavailable
#
# Refresh cadence: Saturday 8 AM ET (triggered from scanner_loop WEEKEND block)

import os
import time
import json
import requests
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

FINNHUB_BASE = "https://finnhub.io/api/v1"

CRITERIA = {
    "min_market_cap": 20_000_000,     # $20M floor
    "max_market_cap": 2_000_000_000,  # $2B ceiling
    "min_adv":        50_000,         # 50K shares/day
    "min_price":      0.50,
    "max_price":      500.0,
}

# Symbols containing these patterns are almost certainly not common stock
_SKIP_PATTERNS = {
    ".", "-W", "-U", "-R", "W1", "UN", "UT", "P1",
    "WARR", "UNIT", "PREF", "ETF",
}


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


def _fetch_yfinance_batch(tickers: List[str],
                          batch_size: int = 100,
                          delay: float = 0.3) -> Dict[str, dict]:
    """
    Fetch yfinance fast_info for a list of tickers in batches.
    Returns {ticker: {"market_cap": int, "avg_volume": int, "price": float,
                       "exchange": str, "sector": str}}.
    """
    import yfinance as yf

    results: Dict[str, dict] = {}
    total = len(tickers)
    for i in range(0, total, batch_size):
        chunk = tickers[i: i + batch_size]
        chunk_str = " ".join(chunk)
        try:
            raw = yf.Tickers(chunk_str)
            for t in chunk:
                try:
                    fi = raw.tickers[t].fast_info
                    mc   = getattr(fi, "market_cap", None) or 0
                    avol = getattr(fi, "three_month_average_volume", None) or 0
                    pr   = getattr(fi, "last_price", None) or 0
                    exch = getattr(fi, "exchange", None) or ""
                    results[t] = {
                        "market_cap":  int(mc),
                        "avg_volume":  int(avol),
                        "price":       float(pr),
                        "exchange":    exch,
                        "sector":      "",
                    }
                except Exception:
                    pass
        except Exception as e:
            _log(f"yfinance batch {i}–{i+batch_size} error: {e}")
        if i + batch_size < total:
            time.sleep(delay)
        if (i // batch_size + 1) % 10 == 0:
            _log(f"  yf batch {i+batch_size}/{total} — {len(results)} enriched so far")

    return results


def _persist_universe(candidates: List[Dict]) -> Dict[str, int]:
    """
    Upsert candidates to both stock_universe and legacy universe tables.
    Returns {"added": N, "total": N}.
    """
    from db.database import (
        _is_postgres, _get_pg_conn, _get_sqlite_conn, upsert_universe_stock
    )
    now_iso = datetime.now().isoformat()
    new_set = {c["ticker"] for c in candidates}

    # Bulk upsert stock_universe (new table)
    for c in candidates:
        upsert_universe_stock(
            ticker     = c["ticker"],
            name       = c.get("name", ""),
            exchange   = c.get("exchange", ""),
            market_cap = c.get("market_cap", 0),
            avg_volume = c.get("avg_volume", 0),
            sector     = c.get("sector", ""),
            min_price  = c.get("price", 0.0),
        )

    # Also keep legacy universe table in sync
    added = 0
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("SELECT ticker FROM universe WHERE active = TRUE")
            existing = {r[0] for r in cur.fetchall()}
            for c in candidates:
                cur.execute("""
                    INSERT INTO universe
                        (ticker, exchange, market_cap, adv_20, sector, active, last_refreshed)
                    VALUES (%s,%s,%s,%s,%s,TRUE,%s)
                    ON CONFLICT (ticker) DO UPDATE SET
                        exchange=EXCLUDED.exchange, market_cap=EXCLUDED.market_cap,
                        adv_20=EXCLUDED.adv_20, sector=EXCLUDED.sector,
                        active=TRUE, last_refreshed=EXCLUDED.last_refreshed
                """, (c["ticker"], c.get("exchange",""), c.get("market_cap",0),
                      c.get("avg_volume",0), c.get("sector",""), now_iso))
            stale = existing - new_set
            for t in stale:
                cur.execute("UPDATE universe SET active=FALSE WHERE ticker=%s", (t,))
            conn.commit(); cur.close(); conn.close()
            added = len(new_set - existing)
        else:
            conn = _get_sqlite_conn()
            existing_rows = conn.execute("SELECT ticker FROM universe WHERE active=1").fetchall()
            existing = {r[0] for r in existing_rows}
            for c in candidates:
                conn.execute("""
                    INSERT OR REPLACE INTO universe
                        (ticker, exchange, market_cap, adv_20, sector, active, last_refreshed)
                    VALUES (?,?,?,?,?,1,?)
                """, (c["ticker"], c.get("exchange",""), c.get("market_cap",0),
                      c.get("avg_volume",0), c.get("sector",""), now_iso))
            stale = existing - new_set
            for t in stale:
                conn.execute("UPDATE universe SET active=0 WHERE ticker=?", (t,))
            conn.commit(); conn.close()
            added = len(new_set - existing)
    except Exception as e:
        _log(f"Legacy universe upsert failed: {e}")

    return {"added": added, "total": len(candidates)}


def refresh_universe() -> Dict[str, int]:
    """
    Full universe refresh. Runtime: ~15–30 min on a fresh build.
    Safe to call from scanner_loop weekend block.

    Steps:
      1. Pull ~8K common-stock symbols from Finnhub
      2. Pre-filter on heuristics (skip warrants, ETFs)
      3. Batch yfinance for market_cap + avg_volume (takes the most time)
      4. Apply CRITERIA filters
      5. Persist to DB
    """
    _log("Starting full universe refresh...")
    start = time.time()

    # Step 1: Symbol list from Finnhub
    raw_symbols = _fetch_finnhub_symbols()
    if not raw_symbols:
        _log("Finnhub returned no symbols — aborting refresh")
        return {"added": 0, "removed": 0, "total": 0}

    tickers_only = [s["ticker"] for s in raw_symbols]
    sym_map = {s["ticker"]: s for s in raw_symbols}

    # Step 2: Batch yfinance enrichment (1–4 hours without parallelism; use Tickers batch)
    _log(f"Enriching {len(tickers_only)} symbols via yfinance batch API...")
    yf_data = _fetch_yfinance_batch(tickers_only, batch_size=200, delay=0.5)

    # Step 3: Apply CRITERIA
    candidates = []
    for ticker, info in yf_data.items():
        mc   = info.get("market_cap", 0)
        adv  = info.get("avg_volume", 0)
        pr   = info.get("price", 0)
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

    # Step 4: Persist
    result = _persist_universe(candidates)
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
            message=f"{result['total']} small-cap stocks\n+{result['added']} new",
            priority=PRIORITY_NORMAL,
        )
    except Exception:
        pass

    return {**result, "removed": 0}


def get_universe_tickers(min_market_cap: int = 20_000_000,
                          max_market_cap: int = 2_000_000_000,
                          min_adv: int = 50_000,
                          limit: int = 3000) -> List[str]:
    """
    Fast path: return tickers from stock_universe (cached in DB).
    Falls back to DEFAULT_UNIVERSE if the table is empty.
    """
    try:
        from db.database import get_active_universe
        tickers = get_active_universe(min_market_cap, max_market_cap, min_adv)
        if tickers:
            return tickers[:limit]
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

    from config import DEFAULT_UNIVERSE
    _log(f"Falling back to DEFAULT_UNIVERSE ({len(DEFAULT_UNIVERSE)} stocks)")
    return DEFAULT_UNIVERSE


def get_universe_size() -> int:
    """Return count of active tickers in universe."""
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
    """Return True if the universe hasn't been refreshed in max_age_days days."""
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
        from datetime import datetime as _dt
        last = row[0]
        if isinstance(last, str):
            last = _dt.fromisoformat(last.replace("Z", ""))
        age = (datetime.now() - last).days
        return age >= max_age_days
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
