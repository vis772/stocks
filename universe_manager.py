# universe_manager.py
# Manages the expanded US small-cap universe (800–2000 tickers).
# Refreshes every Sunday 6 PM ET. Provides batched scan queue for the scanner loop.

import os
import time
import requests
from datetime import datetime
from typing import List, Optional

TIINGO_BASE = "https://api.tiingo.com"

CRITERIA = {
    "min_market_cap": 50_000_000,
    "max_market_cap": 2_000_000_000,
    "min_adv":        75_000,
    "exchanges":      {"NYSE", "NASDAQ", "AMEX"},
    "exclude_sic":    {6770, 6199},
    "min_price":      1.00,
    "max_price":      500.00,
}


def _tiingo_headers() -> dict:
    token = os.environ.get("TIINGO_API_KEY", "")
    return {"Authorization": f"Token {token}", "Content-Type": "application/json"}


def _log(msg: str) -> None:
    try:
        from db.database import log_scanner_event
        log_scanner_event("info", f"[universe] {msg}")
    except Exception:
        pass
    print(f"  [universe] {msg}")


class UniverseManager:
    """
    Pulls and maintains the full US small-cap universe.
    Weekly cron: every Sunday 6 PM ET via APScheduler.
    """

    def refresh_universe(self) -> dict:
        """
        Fetch Tiingo ticker list, filter by CRITERIA, upsert to universe table.
        Returns {"added": N, "removed": N, "total": N}.
        """
        _log("Starting universe refresh...")
        token = os.environ.get("TIINGO_API_KEY", "")
        if not token:
            _log("TIINGO_API_KEY not set — universe refresh skipped")
            return {"added": 0, "removed": 0, "total": 0}

        # 1. Fetch full ticker list from Tiingo
        all_tickers = []
        for exchange in ["NYSE", "NASDAQ", "AMEX"]:
            try:
                r = requests.get(
                    f"{TIINGO_BASE}/tiingo/daily",
                    params={"token": token, "exchange": exchange},
                    headers=_tiingo_headers(),
                    timeout=30,
                )
                if r.status_code == 200:
                    data = r.json()
                    for row in data:
                        if isinstance(row, dict) and row.get("ticker"):
                            all_tickers.append({
                                "ticker":   row["ticker"].upper(),
                                "exchange": exchange,
                            })
                    _log(f"  {exchange}: {len(data)} tickers")
                else:
                    _log(f"  {exchange}: HTTP {r.status_code}")
            except Exception as e:
                _log(f"  {exchange}: error — {e}")
            time.sleep(0.5)

        if not all_tickers:
            _log("No tickers fetched — aborting")
            return {"added": 0, "removed": 0, "total": 0}

        _log(f"Raw ticker count: {len(all_tickers)}")

        # 2. Filter by fundamental criteria
        import yfinance as yf
        candidates = []
        checked = 0
        for row in all_tickers:
            t = row["ticker"]
            try:
                info = yf.Ticker(t).info
                mktcap = info.get("marketCap") or 0
                price  = info.get("currentPrice") or info.get("regularMarketPrice") or 0
                adv    = info.get("averageVolume") or info.get("averageDailyVolume10Day") or 0
                sic    = info.get("sic")
                sic_int = int(sic) if sic else 0

                if not (CRITERIA["min_market_cap"] <= mktcap <= CRITERIA["max_market_cap"]):
                    continue
                if not (CRITERIA["min_price"] <= price <= CRITERIA["max_price"]):
                    continue
                if adv < CRITERIA["min_adv"]:
                    continue
                if sic_int in CRITERIA["exclude_sic"]:
                    continue

                candidates.append({
                    "ticker":       t,
                    "exchange":     row["exchange"],
                    "market_cap":   int(mktcap),
                    "adv_20":       int(adv),
                    "sector":       info.get("sector", ""),
                    "industry":     info.get("industry", ""),
                    "sic_code":     sic_int,
                })
                checked += 1
                if checked % 50 == 0:
                    _log(f"  Filtered {checked}/{len(all_tickers)}, passing: {len(candidates)}")
            except Exception:
                pass
            time.sleep(0.05)

        _log(f"Candidates after filter: {len(candidates)}")

        # 3. Upsert to universe table
        added = removed = 0
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
            now_iso = datetime.now().isoformat()
            new_tickers = {c["ticker"] for c in candidates}

            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute("SELECT ticker FROM universe WHERE active = TRUE")
                existing = {r[0] for r in cur.fetchall()}
                for c in candidates:
                    cur.execute("""
                        INSERT INTO universe
                            (ticker, exchange, market_cap, adv_20, sector, industry, sic_code, active, last_refreshed)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,TRUE,%s)
                        ON CONFLICT (ticker) DO UPDATE SET
                            exchange=EXCLUDED.exchange, market_cap=EXCLUDED.market_cap,
                            adv_20=EXCLUDED.adv_20, sector=EXCLUDED.sector,
                            industry=EXCLUDED.industry, sic_code=EXCLUDED.sic_code,
                            active=TRUE, last_refreshed=EXCLUDED.last_refreshed
                    """, (c["ticker"], c["exchange"], c["market_cap"], c["adv_20"],
                          c["sector"], c["industry"], c["sic_code"], now_iso))
                # Mark removed tickers as inactive
                stale = existing - new_tickers
                for t in stale:
                    cur.execute("UPDATE universe SET active=FALSE WHERE ticker=%s", (t,))
                removed = len(stale)
                added   = len(new_tickers - existing)
                conn.commit(); cur.close(); conn.close()
            else:
                conn = _get_sqlite_conn()
                existing_rows = conn.execute("SELECT ticker FROM universe WHERE active=1").fetchall()
                existing = {r[0] for r in existing_rows}
                for c in candidates:
                    conn.execute("""
                        INSERT OR REPLACE INTO universe
                            (ticker, exchange, market_cap, adv_20, sector, industry, sic_code, active, last_refreshed)
                        VALUES (?,?,?,?,?,?,?,1,?)
                    """, (c["ticker"], c["exchange"], c["market_cap"], c["adv_20"],
                          c["sector"], c["industry"], c["sic_code"], now_iso))
                stale = existing - new_tickers
                for t in stale:
                    conn.execute("UPDATE universe SET active=0 WHERE ticker=?", (t,))
                conn.commit(); conn.close()
                removed = len(stale)
                added   = len(new_tickers - existing)
        except Exception as e:
            _log(f"DB upsert failed: {e}")

        total = len(candidates)
        summary = f"Universe refresh complete — {total} active | +{added} added | -{removed} removed"
        _log(summary)

        # 4. Pushover notification
        try:
            from alerts import send_alert, PRIORITY_NORMAL
            send_alert(
                title="Axiom — Universe Refreshed",
                message=f"{total} small-cap stocks\n+{added} added  |  -{removed} removed",
                priority=PRIORITY_NORMAL,
            )
        except Exception:
            pass

        return {"added": added, "removed": removed, "total": total}

    def get_scan_batch(self, batch_size: int = 50) -> List[str]:
        """
        Round-robin through the active universe. Prioritizes high-RVOL tickers.
        Advances last_scanned cursor so each ticker gets scanned eventually.
        Falls back to DEFAULT_UNIVERSE if no universe data exists.
        """
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT ticker FROM universe
                    WHERE active = TRUE
                    ORDER BY last_scanned ASC NULLS FIRST, adv_20 DESC
                    LIMIT %s
                """, (batch_size,))
                rows = cur.fetchall()
                tickers = [r[0] for r in rows]
                if tickers:
                    cur.execute("""
                        UPDATE universe SET last_scanned = NOW()
                        WHERE ticker = ANY(%s)
                    """, (tickers,))
                conn.commit(); cur.close(); conn.close()
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT ticker FROM universe
                    WHERE active = 1
                    ORDER BY last_scanned ASC, adv_20 DESC
                    LIMIT ?
                """, (batch_size,))
                tickers = [r[0] for r in cur.fetchall()]
                if tickers:
                    placeholders = ",".join("?" * len(tickers))
                    conn.execute(
                        f"UPDATE universe SET last_scanned = datetime('now') WHERE ticker IN ({placeholders})",
                        tickers,
                    )
                conn.commit(); conn.close()

            if tickers:
                return tickers
        except Exception as e:
            _log(f"get_scan_batch failed: {e}")

        from config import DEFAULT_UNIVERSE
        return DEFAULT_UNIVERSE[:batch_size]
