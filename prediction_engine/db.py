# prediction_engine/db.py
# PostgreSQL connection and schema for the prediction engine.
# Shares the same DATABASE_URL as Axiom Terminal but owns its own tables.
# Falls back to SQLite for local dev when DATABASE_URL is unset.

import os
import json
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional, List

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# ── Connection pool ───────────────────────────────────────────────────────────
_pg_pool      = None
_pg_pool_lock = threading.Lock()
_PG_MIN       = 2
_PG_MAX       = 10
_PG_TIMEOUT   = 30

_pooled_ids      = set()
_pooled_ids_lock = threading.Lock()

SQLITE_PATH = "prediction_engine.db"


def _is_postgres() -> bool:
    return bool(DATABASE_URL and DATABASE_URL.startswith("postgres"))


def _get_pg_pool():
    global _pg_pool
    if _pg_pool is not None:
        return _pg_pool
    with _pg_pool_lock:
        if _pg_pool is None:
            try:
                import psycopg2.pool
                url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
                _pg_pool = psycopg2.pool.ThreadedConnectionPool(
                    _PG_MIN, _PG_MAX, url,
                    sslmode="require",
                    connect_timeout=_PG_TIMEOUT,
                )
                print(f"  [pe/db] Pool created (min={_PG_MIN} max={_PG_MAX})")
            except Exception as e:
                print(f"  [pe/db] Pool creation failed: {e}")
    return _pg_pool


def _get_conn():
    import psycopg2
    import time as _t
    pool = _get_pg_pool()
    if pool is not None:
        deadline = _t.monotonic() + _PG_TIMEOUT
        delay = 0.05
        while True:
            try:
                conn = pool.getconn()
                with _pooled_ids_lock:
                    _pooled_ids.add(id(conn))
                return conn
            except psycopg2.pool.PoolError:
                remaining = deadline - _t.monotonic()
                if remaining <= 0:
                    break
                _t.sleep(min(delay, remaining))
                delay = min(delay * 2, 2.0)
            except Exception as e:
                print(f"  [pe/db] Pool.getconn error: {e}")
                break
    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, sslmode="require", connect_timeout=_PG_TIMEOUT)


def _put_conn(conn):
    try:
        with _pooled_ids_lock:
            from_pool = id(conn) in _pooled_ids
            if from_pool:
                _pooled_ids.discard(id(conn))
        pool = _get_pg_pool()
        if from_pool and pool:
            pool.putconn(conn)
            return
        conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


def _get_sqlite():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _et_now() -> datetime:
    utc = datetime.now(timezone.utc)
    offset = -4 if 3 <= utc.month <= 11 else -5
    return utc + timedelta(hours=offset)


def _et_date() -> str:
    return _et_now().strftime("%Y-%m-%d")


# ── Schema ────────────────────────────────────────────────────────────────────

def initialize_schema():
    """Create prediction engine tables. Safe to call on every startup."""
    if _is_postgres():
        _init_pg()
    else:
        _init_sqlite()


def _init_pg():
    conn = _get_conn()
    cur  = conn.cursor()
    # Advisory lock to serialize concurrent schema migrations
    cur.execute("SELECT pg_advisory_xact_lock(20260527)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS prediction_engine_signals (
            id                   SERIAL PRIMARY KEY,
            date                 DATE NOT NULL,
            ticker               TEXT NOT NULL,
            conviction_score     REAL NOT NULL,
            conviction_tier      TEXT NOT NULL,
            model_agreement_count INTEGER NOT NULL,
            catalyst             TEXT,
            technical_setup      TEXT,
            pattern_summary      TEXT,
            sentiment_score      REAL,
            short_interest       REAL,
            entry                REAL,
            target               REAL,
            stop                 REAL,
            thesis               TEXT,
            outcome_1d           TEXT,
            outcome_3d           TEXT,
            outcome_5d           TEXT,
            created_at           TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_pe_signals_date
        ON prediction_engine_signals (date DESC)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS prediction_engine_model_votes (
            id         SERIAL PRIMARY KEY,
            date       DATE NOT NULL,
            ticker     TEXT NOT NULL,
            model_name TEXT NOT NULL,
            score      REAL,
            verdict    TEXT,
            raw_output TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (date, ticker, model_name)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_pe_votes_date_ticker
        ON prediction_engine_model_votes (date, ticker)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS prediction_engine_runs (
            id            SERIAL PRIMARY KEY,
            run_date      DATE NOT NULL UNIQUE,
            stage         TEXT NOT NULL,
            status        TEXT NOT NULL DEFAULT 'running',
            tickers_input INTEGER DEFAULT 0,
            picks_count   INTEGER DEFAULT 0,
            error_msg     TEXT,
            started_at    TIMESTAMP DEFAULT NOW(),
            completed_at  TIMESTAMP
        )
    """)

    conn.commit()
    cur.close()
    _put_conn(conn)
    print("  [pe/db] PostgreSQL schema initialized")


def _init_sqlite():
    conn = _get_sqlite()
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS prediction_engine_signals (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            date                  TEXT NOT NULL,
            ticker                TEXT NOT NULL,
            conviction_score      REAL NOT NULL,
            conviction_tier       TEXT NOT NULL,
            model_agreement_count INTEGER NOT NULL,
            catalyst              TEXT,
            technical_setup       TEXT,
            pattern_summary       TEXT,
            sentiment_score       REAL,
            short_interest        REAL,
            entry                 REAL,
            target                REAL,
            stop                  REAL,
            thesis                TEXT,
            outcome_1d            TEXT,
            outcome_3d            TEXT,
            outcome_5d            TEXT,
            created_at            TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS prediction_engine_model_votes (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            date       TEXT NOT NULL,
            ticker     TEXT NOT NULL,
            model_name TEXT NOT NULL,
            score      REAL,
            verdict    TEXT,
            raw_output TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE (date, ticker, model_name)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS prediction_engine_runs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date      TEXT NOT NULL UNIQUE,
            stage         TEXT NOT NULL,
            status        TEXT NOT NULL DEFAULT 'running',
            tickers_input INTEGER DEFAULT 0,
            picks_count   INTEGER DEFAULT 0,
            error_msg     TEXT,
            started_at    TEXT DEFAULT (datetime('now')),
            completed_at  TEXT
        )
    """)

    conn.commit()
    conn.close()
    print("  [pe/db] SQLite schema initialized")


# ── Write functions ───────────────────────────────────────────────────────────

def upsert_run_status(date: str, stage: str, status: str,
                      tickers_input: int = 0, picks_count: int = 0,
                      error_msg: str = None):
    try:
        if _is_postgres():
            conn = _get_conn(); cur = conn.cursor()
            cur.execute("""
                INSERT INTO prediction_engine_runs (run_date, stage, status, tickers_input, picks_count, error_msg)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_date) DO UPDATE SET
                    stage         = EXCLUDED.stage,
                    status        = EXCLUDED.status,
                    tickers_input = EXCLUDED.tickers_input,
                    picks_count   = EXCLUDED.picks_count,
                    error_msg     = EXCLUDED.error_msg,
                    completed_at  = CASE WHEN EXCLUDED.status IN ('completed','failed')
                                         THEN NOW() ELSE NULL END
            """, (date, stage, status, tickers_input, picks_count, error_msg))
            conn.commit(); cur.close(); _put_conn(conn)
        else:
            conn = _get_sqlite()
            conn.execute("""
                INSERT OR REPLACE INTO prediction_engine_runs
                    (run_date, stage, status, tickers_input, picks_count, error_msg)
                VALUES (?,?,?,?,?,?)
            """, (date, stage, status, tickers_input, picks_count, error_msg))
            conn.commit(); conn.close()
    except Exception as e:
        print(f"  [pe/db] upsert_run_status failed: {e}")


def save_model_vote(date: str, ticker: str, model_name: str,
                    score: float, verdict: str, raw_output: str):
    try:
        if _is_postgres():
            conn = _get_conn(); cur = conn.cursor()
            cur.execute("""
                INSERT INTO prediction_engine_model_votes
                    (date, ticker, model_name, score, verdict, raw_output)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, ticker, model_name) DO UPDATE SET
                    score      = EXCLUDED.score,
                    verdict    = EXCLUDED.verdict,
                    raw_output = EXCLUDED.raw_output
            """, (date, ticker, model_name, score, verdict, raw_output))
            conn.commit(); cur.close(); _put_conn(conn)
        else:
            conn = _get_sqlite()
            conn.execute("""
                INSERT OR REPLACE INTO prediction_engine_model_votes
                    (date, ticker, model_name, score, verdict, raw_output)
                VALUES (?,?,?,?,?,?)
            """, (date, ticker, model_name, score, verdict, raw_output))
            conn.commit(); conn.close()
    except Exception as e:
        print(f"  [pe/db] save_model_vote failed for {ticker}/{model_name}: {e}")


def save_picks(date: str, picks: list):
    """Write the final top-5 picks. One row per pick."""
    if not picks:
        return
    for pick in picks:
        try:
            ticker = pick["ticker"]
            if _is_postgres():
                conn = _get_conn(); cur = conn.cursor()
                cur.execute("""
                    INSERT INTO prediction_engine_signals
                        (date, ticker, conviction_score, conviction_tier,
                         model_agreement_count, catalyst, technical_setup,
                         pattern_summary, sentiment_score, short_interest,
                         entry, target, stop, thesis)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    date, ticker,
                    pick.get("conviction_score"),
                    pick.get("conviction_tier"),
                    pick.get("model_agreement_count"),
                    pick.get("catalyst"),
                    pick.get("technical_setup"),
                    pick.get("pattern_summary"),
                    pick.get("sentiment_score"),
                    pick.get("short_interest"),
                    pick.get("entry"),
                    pick.get("target"),
                    pick.get("stop"),
                    pick.get("thesis"),
                ))
                conn.commit(); cur.close(); _put_conn(conn)
            else:
                conn = _get_sqlite()
                conn.execute("""
                    INSERT OR IGNORE INTO prediction_engine_signals
                        (date, ticker, conviction_score, conviction_tier,
                         model_agreement_count, catalyst, technical_setup,
                         pattern_summary, sentiment_score, short_interest,
                         entry, target, stop, thesis)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    date, ticker,
                    pick.get("conviction_score"),
                    pick.get("conviction_tier"),
                    pick.get("model_agreement_count"),
                    pick.get("catalyst"),
                    pick.get("technical_setup"),
                    pick.get("pattern_summary"),
                    pick.get("sentiment_score"),
                    pick.get("short_interest"),
                    pick.get("entry"),
                    pick.get("target"),
                    pick.get("stop"),
                    pick.get("thesis"),
                ))
                conn.commit(); conn.close()
            print(f"  [pe/db] Saved pick: {ticker}")
        except Exception as e:
            print(f"  [pe/db] save_picks failed for {pick.get('ticker')}: {e}")


def load_todays_picks(date: str) -> list:
    """Load today's picks for the sanity check stage."""
    try:
        if _is_postgres():
            conn = _get_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT ticker, conviction_score, entry, stop, target, thesis,
                       catalyst, technical_setup
                FROM prediction_engine_signals
                WHERE date = %s
                ORDER BY conviction_score DESC
            """, (date,))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            cur.close(); _put_conn(conn)
            return [dict(zip(cols, r)) for r in rows]
        else:
            conn = _get_sqlite(); cur = conn.cursor()
            cur.execute("""
                SELECT ticker, conviction_score, entry, stop, target, thesis,
                       catalyst, technical_setup
                FROM prediction_engine_signals
                WHERE date = ?
                ORDER BY conviction_score DESC
            """, (date,))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
            conn.close()
            return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        print(f"  [pe/db] load_todays_picks failed: {e}")
        return []


def get_universe_tickers() -> list:
    """Pull the active ticker universe from the shared stock_universe table."""
    try:
        if _is_postgres():
            conn = _get_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT ticker FROM stock_universe
                WHERE active = TRUE
                ORDER BY avg_volume DESC NULLS LAST
                LIMIT 4000
            """)
            rows = cur.fetchall()
            cur.close(); _put_conn(conn)
            return [r[0] for r in rows]
        else:
            conn = _get_sqlite(); cur = conn.cursor()
            cur.execute("""
                SELECT ticker FROM stock_universe
                WHERE active = 1
                ORDER BY avg_volume DESC
                LIMIT 4000
            """)
            rows = cur.fetchall()
            conn.close()
            return [r[0] for r in rows]
    except Exception as e:
        print(f"  [pe/db] get_universe_tickers failed: {e}")
        return []
