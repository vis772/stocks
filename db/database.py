# db/database.py
# Shared PostgreSQL database for both Service 1 (dashboard) and Service 2 (scanner).
# Falls back to SQLite if no DATABASE_URL is set (local development).
#
# Tables:
#   portfolio      - user holdings
#   scan_results   - scanner output per ticker per day
#   watchlist      - today's dynamic watchlist (written by scanner, read by dashboard)
#   alert_log      - real-time alerts (written by scanner, read by dashboard)
#   scanner_state  - scanner internals (alerted_today, known_filings etc)

import os
import json
import sqlite3
import pandas as pd
from datetime import datetime
from typing import Optional, List

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def _is_postgres() -> bool:
    return bool(DATABASE_URL and DATABASE_URL.startswith("postgres"))


def _get_pg_conn():
    """Get a PostgreSQL connection."""
    import psycopg2
    # Railway sometimes uses postgres:// instead of postgresql://
    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, sslmode="require")


def _get_sqlite_conn() -> sqlite3.Connection:
    """Get SQLite connection for local development."""
    conn = sqlite3.connect("scanner.db")
    conn.row_factory = sqlite3.Row
    return conn


def initialize_db():
    """Create all tables if they don't exist. Safe to call on every startup."""
    if _is_postgres():
        _init_postgres()
    else:
        _init_sqlite()


def _init_postgres():
    conn = _get_pg_conn()
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS portfolio (
            ticker      TEXT PRIMARY KEY,
            shares      REAL NOT NULL,
            avg_cost    REAL NOT NULL,
            notes       TEXT DEFAULT '',
            added_at    TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_results (
            id                  SERIAL PRIMARY KEY,
            scan_date           TEXT NOT NULL,
            ticker              TEXT NOT NULL,
            company_name        TEXT,
            price               REAL,
            market_cap          REAL,
            volume              REAL,
            avg_volume          REAL,
            technical_score     REAL,
            catalyst_score      REAL,
            fundamental_score   REAL,
            risk_score          REAL,
            sentiment_score     REAL,
            final_score         REAL,
            signal              TEXT,
            risk_flags          TEXT,
            summary             TEXT,
            data_sources        TEXT,
            created_at          TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            id          SERIAL PRIMARY KEY,
            date        TEXT NOT NULL,
            tickers     TEXT NOT NULL,
            stats       TEXT,
            updated_at  TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS alert_log (
            id          SERIAL PRIMARY KEY,
            alert_time  TEXT NOT NULL,
            message     TEXT NOT NULL,
            ticker      TEXT,
            alert_type  TEXT,
            created_at  TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scanner_state (
            id              SERIAL PRIMARY KEY,
            date            TEXT NOT NULL UNIQUE,
            alerted_today   TEXT DEFAULT '[]',
            known_filings   TEXT DEFAULT '[]',
            scan_count      INTEGER DEFAULT 0,
            last_updated    TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades (
            id           SERIAL PRIMARY KEY,
            trade_date   TEXT NOT NULL,
            ticker       TEXT NOT NULL,
            signal_type  TEXT NOT NULL,
            entry_price  REAL NOT NULL,
            stop_price   REAL NOT NULL,
            target1      REAL NOT NULL,
            target2      REAL NOT NULL,
            entry_time   TEXT NOT NULL,
            exit_price   REAL,
            exit_time    TEXT,
            outcome      TEXT DEFAULT 'open',
            pnl_pct      REAL,
            created_at   TIMESTAMP DEFAULT NOW()
        )
    """)

    # Migration-safe: add new columns to existing deployments
    cur.execute("ALTER TABLE scanner_state ADD COLUMN IF NOT EXISTS vwap_snapshot     TEXT DEFAULT '{}'")
    cur.execute("ALTER TABLE scanner_state ADD COLUMN IF NOT EXISTS momentum_ranking  TEXT DEFAULT '[]'")

    conn.commit()
    cur.close()
    conn.close()
    print("  ✓ PostgreSQL tables initialized")


def _init_sqlite():
    conn = _get_sqlite_conn()
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS portfolio (
            ticker    TEXT PRIMARY KEY,
            shares    REAL NOT NULL,
            avg_cost  REAL NOT NULL,
            notes     TEXT DEFAULT '',
            added_at  TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_results (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date         TEXT NOT NULL,
            ticker            TEXT NOT NULL,
            company_name      TEXT,
            price             REAL,
            market_cap        REAL,
            volume            REAL,
            avg_volume        REAL,
            technical_score   REAL,
            catalyst_score    REAL,
            fundamental_score REAL,
            risk_score        REAL,
            sentiment_score   REAL,
            final_score       REAL,
            signal            TEXT,
            risk_flags        TEXT,
            summary           TEXT,
            data_sources      TEXT,
            created_at        TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            date       TEXT NOT NULL,
            tickers    TEXT NOT NULL,
            stats      TEXT,
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS alert_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_time TEXT NOT NULL,
            message    TEXT NOT NULL,
            ticker     TEXT,
            alert_type TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scanner_state (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            date          TEXT NOT NULL UNIQUE,
            alerted_today TEXT DEFAULT '[]',
            known_filings TEXT DEFAULT '[]',
            scan_count    INTEGER DEFAULT 0,
            last_updated  TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date   TEXT NOT NULL,
            ticker       TEXT NOT NULL,
            signal_type  TEXT NOT NULL,
            entry_price  REAL NOT NULL,
            stop_price   REAL NOT NULL,
            target1      REAL NOT NULL,
            target2      REAL NOT NULL,
            entry_time   TEXT NOT NULL,
            exit_price   REAL,
            exit_time    TEXT,
            outcome      TEXT DEFAULT 'open',
            pnl_pct      REAL,
            created_at   TEXT DEFAULT (datetime('now'))
        )
    """)

    for col_sql in [
        "ALTER TABLE scanner_state ADD COLUMN vwap_snapshot    TEXT DEFAULT '{}'",
        "ALTER TABLE scanner_state ADD COLUMN momentum_ranking TEXT DEFAULT '[]'",
    ]:
        try:
            cur.execute(col_sql)
        except Exception:
            pass  # Column already exists

    conn.commit()
    conn.close()


# ─── Portfolio ─────────────────────────────────────────────────────────────────

def upsert_holding(ticker: str, shares: float, avg_cost: float, notes: str = ""):
    ticker = ticker.upper()
    if _is_postgres():
        conn = _get_pg_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO portfolio (ticker, shares, avg_cost, notes)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
                shares   = EXCLUDED.shares,
                avg_cost = EXCLUDED.avg_cost,
                notes    = EXCLUDED.notes
        """, (ticker, shares, avg_cost, notes))
        conn.commit()
        cur.close()
        conn.close()
    else:
        conn = _get_sqlite_conn()
        conn.execute("""
            INSERT INTO portfolio (ticker, shares, avg_cost, notes)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                shares   = excluded.shares,
                avg_cost = excluded.avg_cost,
                notes    = excluded.notes
        """, (ticker, shares, avg_cost, notes))
        conn.commit()
        conn.close()


def delete_holding(ticker: str):
    ticker = ticker.upper()
    if _is_postgres():
        conn = _get_pg_conn()
        cur  = conn.cursor()
        cur.execute("DELETE FROM portfolio WHERE ticker = %s", (ticker,))
        conn.commit()
        cur.close()
        conn.close()
    else:
        conn = _get_sqlite_conn()
        conn.execute("DELETE FROM portfolio WHERE ticker = ?", (ticker,))
        conn.commit()
        conn.close()


def get_portfolio() -> pd.DataFrame:
    if _is_postgres():
        conn = _get_pg_conn()
        cur  = conn.cursor()
        cur.execute("SELECT ticker, shares, avg_cost, notes, added_at FROM portfolio ORDER BY ticker")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        if not rows:
            return pd.DataFrame(columns=["ticker","shares","avg_cost","notes","added_at"])
        return pd.DataFrame(rows, columns=["ticker","shares","avg_cost","notes","added_at"])
    else:
        conn = _get_sqlite_conn()
        df   = pd.read_sql("SELECT * FROM portfolio ORDER BY ticker", conn)
        conn.close()
        return df


def get_connection():
    """For direct SQL access (used by CLEAR ALL button)."""
    if _is_postgres():
        return _get_pg_conn()
    return _get_sqlite_conn()


# ─── Scan Results ──────────────────────────────────────────────────────────────

def save_scan_result(result: dict):
    scan_date = result.get("scan_date", datetime.now().strftime("%Y-%m-%d"))
    ticker    = result.get("ticker", "")

    if _is_postgres():
        conn = _get_pg_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO scan_results (
                scan_date, ticker, company_name, price, market_cap,
                volume, avg_volume, technical_score, catalyst_score,
                fundamental_score, risk_score, sentiment_score,
                final_score, signal, risk_flags, summary, data_sources
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            scan_date, ticker,
            result.get("company_name"), result.get("price"), result.get("market_cap"),
            result.get("volume"), result.get("avg_volume"),
            result.get("technical_score"), result.get("catalyst_score"),
            result.get("fundamental_score"), result.get("risk_score"),
            result.get("sentiment_score"), result.get("final_score"),
            result.get("signal"),
            json.dumps(result.get("risk_flags", [])),
            result.get("summary"),
            json.dumps(result.get("data_sources", [])),
        ))
        conn.commit()
        cur.close()
        conn.close()
    else:
        conn = _get_sqlite_conn()
        conn.execute("""
            INSERT INTO scan_results (
                scan_date, ticker, company_name, price, market_cap,
                volume, avg_volume, technical_score, catalyst_score,
                fundamental_score, risk_score, sentiment_score,
                final_score, signal, risk_flags, summary, data_sources
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            scan_date, ticker,
            result.get("company_name"), result.get("price"), result.get("market_cap"),
            result.get("volume"), result.get("avg_volume"),
            result.get("technical_score"), result.get("catalyst_score"),
            result.get("fundamental_score"), result.get("risk_score"),
            result.get("sentiment_score"), result.get("final_score"),
            result.get("signal"),
            json.dumps(result.get("risk_flags", [])),
            result.get("summary"),
            json.dumps(result.get("data_sources", [])),
        ))
        conn.commit()
        conn.close()


def get_latest_scan() -> pd.DataFrame:
    if _is_postgres():
        conn = _get_pg_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT * FROM scan_results
            WHERE scan_date = (SELECT MAX(scan_date) FROM scan_results)
            ORDER BY final_score DESC
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        cur.close()
        conn.close()
        df = pd.DataFrame(rows, columns=cols)
    else:
        conn = _get_sqlite_conn()
        df   = pd.read_sql("""
            SELECT * FROM scan_results
            WHERE scan_date = (SELECT MAX(scan_date) FROM scan_results)
            ORDER BY final_score DESC
        """, conn)
        conn.close()

    if not df.empty:
        df["risk_flags"]   = df["risk_flags"].apply(lambda x: json.loads(x) if x else [])
        df["data_sources"] = df["data_sources"].apply(lambda x: json.loads(x) if x else [])
    return df


# ─── Watchlist (shared between scanner and dashboard) ─────────────────────────

def save_watchlist(tickers: List[str], stats: dict = {}):
    """Called by scanner to save today's watchlist to shared DB."""
    date_str = datetime.now().strftime("%Y-%m-%d")
    tickers_json = json.dumps(tickers)
    stats_json   = json.dumps(stats)

    if _is_postgres():
        conn = _get_pg_conn()
        cur  = conn.cursor()
        # Delete today's entry and reinsert
        cur.execute("DELETE FROM watchlist WHERE date = %s", (date_str,))
        cur.execute(
            "INSERT INTO watchlist (date, tickers, stats) VALUES (%s, %s, %s)",
            (date_str, tickers_json, stats_json)
        )
        conn.commit()
        cur.close()
        conn.close()
    else:
        conn = _get_sqlite_conn()
        conn.execute("DELETE FROM watchlist WHERE date = ?", (date_str,))
        conn.execute(
            "INSERT INTO watchlist (date, tickers, stats) VALUES (?, ?, ?)",
            (date_str, tickers_json, stats_json)
        )
        conn.commit()
        conn.close()

    # Also write local JSON as fallback
    try:
        with open("watchlist_today.json", "w") as f:
            json.dump({"date": date_str, "tickers": tickers, "stats": stats}, f)
    except Exception:
        pass


def load_watchlist() -> dict:
    """Called by dashboard to load today's watchlist from shared DB."""
    date_str = datetime.now().strftime("%Y-%m-%d")

    if _is_postgres():
        try:
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute(
                "SELECT tickers, stats FROM watchlist WHERE date = %s ORDER BY id DESC LIMIT 1",
                (date_str,)
            )
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                return {
                    "date":    date_str,
                    "tickers": json.loads(row[0]),
                    "stats":   json.loads(row[1]) if row[1] else {},
                }
        except Exception as e:
            print(f"  [db] Watchlist load failed: {e}")

    # Fallback to local JSON
    try:
        with open("watchlist_today.json") as f:
            return json.load(f)
    except Exception:
        return {}


# ─── Alert Log (shared between scanner and dashboard) ─────────────────────────

def save_alert(message: str, ticker: str = "", alert_type: str = ""):
    """Called by scanner to save an alert to shared DB."""
    try:
        from datetime import datetime as dt
        alert_time = dt.now().strftime("%H:%M ET")

        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute(
                "INSERT INTO alert_log (alert_time, message, ticker, alert_type) VALUES (%s, %s, %s, %s)",
                (alert_time, message, ticker, alert_type)
            )
            conn.commit()
            cur.close()
            conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute(
                "INSERT INTO alert_log (alert_time, message, ticker, alert_type) VALUES (?, ?, ?, ?)",
                (alert_time, message, ticker, alert_type)
            )
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"  [db] Alert save failed: {e}")


def load_alerts(limit: int = 100) -> List[str]:
    """Called by dashboard to load today's alerts from shared DB."""
    date_str = datetime.now().strftime("%Y-%m-%d")

    if _is_postgres():
        try:
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT alert_time, message FROM alert_log
                WHERE DATE(created_at) = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (date_str, limit))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return [f"{r[0]} {r[1]}" for r in reversed(rows)]
        except Exception as e:
            print(f"  [db] Alert load failed: {e}")

    # Fallback to local JSON
    try:
        with open("alert_log.json") as f:
            return json.load(f)
    except Exception:
        return []


# ─── Scanner State (used by scanner service only) ─────────────────────────────

def load_scanner_state() -> dict:
    date_str = datetime.now().strftime("%Y-%m-%d")
    if _is_postgres():
        try:
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT alerted_today, known_filings, scan_count, last_updated,
                       vwap_snapshot, momentum_ranking
                FROM scanner_state WHERE date = %s
            """, (date_str,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                return {
                    "date":             date_str,
                    "alerted_today":    json.loads(row[0] or "[]"),
                    "known_filings":    json.loads(row[1] or "[]"),
                    "scan_count":       row[2] or 0,
                    "last_updated":     row[3] or "",
                    "vwap_snapshot":    json.loads(row[4] or "{}"),
                    "momentum_ranking": json.loads(row[5] or "[]"),
                }
        except Exception as e:
            print(f"  [db] State load failed: {e}")
    # Fallback to local JSON
    try:
        with open("scanner_state.json") as f:
            return json.load(f)
    except Exception:
        return {}


def save_scanner_state(state_dict: dict):
    date_str = datetime.now().strftime("%Y-%m-%d")
    if _is_postgres():
        try:
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                INSERT INTO scanner_state
                    (date, alerted_today, known_filings, scan_count, last_updated,
                     vwap_snapshot, momentum_ranking)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date) DO UPDATE SET
                    alerted_today    = EXCLUDED.alerted_today,
                    known_filings    = EXCLUDED.known_filings,
                    scan_count       = EXCLUDED.scan_count,
                    last_updated     = EXCLUDED.last_updated,
                    vwap_snapshot    = EXCLUDED.vwap_snapshot,
                    momentum_ranking = EXCLUDED.momentum_ranking
            """, (
                date_str,
                json.dumps(list(state_dict.get("alerted_today", []))),
                json.dumps(list(state_dict.get("known_filings", []))),
                state_dict.get("scan_count", 0),
                state_dict.get("last_updated", ""),
                json.dumps(state_dict.get("vwap_snapshot", {})),
                json.dumps(state_dict.get("momentum_ranking", [])),
            ))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"  [db] State save failed: {e}")
    # Also save local JSON as fallback
    try:
        with open("scanner_state.json", "w") as f:
            json.dump(state_dict, f)
    except Exception:
        pass


# ─── Paper Trades ──────────────────────────────────────────────────────────────

def log_paper_trade(ticker: str, signal_type: str, entry_price: float,
                    stop_price: float, target1: float, target2: float) -> Optional[int]:
    """Log a new paper trade entry. Returns the row id or None on failure."""
    trade_date = datetime.now().strftime("%Y-%m-%d")
    entry_time = datetime.now().strftime("%H:%M ET")
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                INSERT INTO paper_trades
                    (trade_date, ticker, signal_type, entry_price, stop_price,
                     target1, target2, entry_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (trade_date, ticker, signal_type, entry_price, stop_price,
                  target1, target2, entry_time))
            row_id = cur.fetchone()[0]
            conn.commit(); cur.close(); conn.close()
            return row_id
        else:
            conn = _get_sqlite_conn()
            cur  = conn.cursor()
            cur.execute("""
                INSERT INTO paper_trades
                    (trade_date, ticker, signal_type, entry_price, stop_price,
                     target1, target2, entry_time)
                VALUES (?,?,?,?,?,?,?,?)
            """, (trade_date, ticker, signal_type, entry_price, stop_price,
                  target1, target2, entry_time))
            row_id = cur.lastrowid
            conn.commit(); conn.close()
            return row_id
    except Exception as e:
        print(f"  [db] Paper trade log failed: {e}")
        return None


def close_paper_trades_eod(exit_prices: dict):
    """
    Called at EOD with {ticker: exit_price}. Closes all open trades from today.
    Marks win/loss based on whether exit >= target1 or exit <= stop.
    """
    trade_date = datetime.now().strftime("%Y-%m-%d")
    exit_time  = datetime.now().strftime("%H:%M ET")
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT id, ticker, entry_price, stop_price, target1
                FROM paper_trades
                WHERE trade_date = %s AND outcome = 'open'
            """, (trade_date,))
            rows = cur.fetchall()
            for row_id, ticker, entry, stop, t1 in rows:
                exit_p = exit_prices.get(ticker, entry)
                pnl    = (exit_p - entry) / entry * 100
                outcome = "win" if exit_p >= t1 else "loss" if exit_p <= stop else "open"
                cur.execute("""
                    UPDATE paper_trades
                    SET exit_price=%s, exit_time=%s, outcome=%s, pnl_pct=%s
                    WHERE id=%s
                """, (exit_p, exit_time, outcome, round(pnl, 2), row_id))
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT id, ticker, entry_price, stop_price, target1
                FROM paper_trades WHERE trade_date=? AND outcome='open'
            """, (trade_date,))
            rows = cur.fetchall()
            for row_id, ticker, entry, stop, t1 in rows:
                exit_p = exit_prices.get(ticker, entry)
                pnl    = (exit_p - entry) / entry * 100
                outcome = "win" if exit_p >= t1 else "loss" if exit_p <= stop else "open"
                cur.execute("""
                    UPDATE paper_trades
                    SET exit_price=?, exit_time=?, outcome=?, pnl_pct=?
                    WHERE id=?
                """, (exit_p, exit_time, outcome, round(pnl, 2), row_id))
            conn.commit(); conn.close()
    except Exception as e:
        print(f"  [db] EOD paper trade close failed: {e}")


def get_paper_trades(days: int = 30) -> pd.DataFrame:
    """Return paper trades from the last N days, most recent first."""
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT id, trade_date, ticker, signal_type, entry_price, stop_price,
                       target1, target2, entry_time, exit_price, exit_time, outcome, pnl_pct
                FROM paper_trades
                WHERE trade_date >= NOW() - INTERVAL '%s days'
                ORDER BY created_at DESC
            """, (days,))
            rows = cur.fetchall()
            cols = ["id","trade_date","ticker","signal_type","entry_price","stop_price",
                    "target1","target2","entry_time","exit_price","exit_time","outcome","pnl_pct"]
            cur.close(); conn.close()
            return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
        else:
            conn = _get_sqlite_conn()
            df = pd.read_sql(f"""
                SELECT id, trade_date, ticker, signal_type, entry_price, stop_price,
                       target1, target2, entry_time, exit_price, exit_time, outcome, pnl_pct
                FROM paper_trades
                WHERE date(trade_date) >= date('now', '-{days} days')
                ORDER BY created_at DESC
            """, conn)
            conn.close()
            return df
    except Exception as e:
        print(f"  [db] Paper trades load failed: {e}")
        return pd.DataFrame()
