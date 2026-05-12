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
    try:
        from paper_broker import init_paper_trading_tables
        init_paper_trading_tables()
    except Exception as _pt_e:
        print(f"  [db] paper trading tables init failed: {_pt_e}")


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
        CREATE TABLE IF NOT EXISTS scanner_control (
            id                 INTEGER PRIMARY KEY DEFAULT 1,
            paused             BOOLEAN DEFAULT FALSE,
            force_scan         BOOLEAN DEFAULT FALSE,
            scanner_started_at TIMESTAMP,
            current_mode       TEXT DEFAULT 'UNKNOWN',
            updated_at         TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("INSERT INTO scanner_control (id) VALUES (1) ON CONFLICT DO NOTHING")

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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            SERIAL PRIMARY KEY,
            username      TEXT NOT NULL UNIQUE,
            email         TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role          TEXT NOT NULL DEFAULT 'user',
            created_at    TIMESTAMP DEFAULT NOW(),
            last_login    TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            session_token TEXT PRIMARY KEY,
            user_id       INTEGER NOT NULL REFERENCES users(id),
            created_at    TIMESTAMP DEFAULT NOW(),
            expires_at    TIMESTAMP NOT NULL,
            is_active     BOOLEAN DEFAULT TRUE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_log (
            id               SERIAL PRIMARY KEY,
            ticker           TEXT NOT NULL,
            signal_label     TEXT NOT NULL,
            score            REAL,
            score_breakdown  TEXT,
            price_at_signal  REAL,
            volume_at_signal REAL,
            alert_type       TEXT,
            created_at       TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_outcomes (
            id                 SERIAL PRIMARY KEY,
            signal_id          INTEGER REFERENCES signal_log(id),
            price_1hr          REAL,
            price_1day         REAL,
            price_5day         REAL,
            pct_change_1hr     REAL,
            pct_change_1day    REAL,
            pct_change_5day    REAL,
            outcome_label      TEXT DEFAULT 'pending',
            outcome_updated_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS accuracy_reports (
            id           SERIAL PRIMARY KEY,
            report_type  TEXT NOT NULL,
            checkpoint   INTEGER,
            filename     TEXT,
            download_url TEXT,
            status_label TEXT,
            generated_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scanner_logs (
            id         SERIAL PRIMARY KEY,
            level      TEXT NOT NULL DEFAULT 'info',
            message    TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS data_quality (
            id         SERIAL PRIMARY KEY,
            ticker     TEXT NOT NULL,
            source     TEXT NOT NULL,
            status     TEXT NOT NULL,
            latency_ms REAL,
            error      TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS quote_cache (
            ticker      TEXT PRIMARY KEY,
            price       REAL,
            prev_close  REAL,
            volume      REAL,
            high        REAL,
            low         REAL,
            source      TEXT,
            created_at  TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS universe (
            ticker         TEXT PRIMARY KEY,
            exchange       TEXT,
            market_cap     BIGINT,
            adv_20         INTEGER,
            sector         TEXT,
            industry       TEXT,
            sic_code       INTEGER,
            active         BOOLEAN DEFAULT TRUE,
            last_refreshed TIMESTAMP,
            last_scanned   TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS conviction_buys (
            id             SERIAL PRIMARY KEY,
            date           DATE,
            session        TEXT,
            rank           INTEGER,
            ticker         TEXT,
            conviction     FLOAT,
            hold_type      TEXT,
            entry          FLOAT,
            stop_loss      FLOAT,
            target_1       FLOAT,
            target_2       FLOAT,
            target_3       FLOAT,
            position_pct   FLOAT,
            expected_value FLOAT,
            reasoning      TEXT,
            composite      FLOAT,
            quant_adj      FLOAT,
            created_at     TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS validator_health (
            id          SERIAL PRIMARY KEY,
            check_time  TIMESTAMP DEFAULT NOW(),
            signals_graded   INTEGER DEFAULT 0,
            signals_pending  INTEGER DEFAULT 0,
            oldest_pending   TEXT,
            error_msg        TEXT DEFAULT ''
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS accuracy_metrics (
            bucket       TEXT PRIMARY KEY,
            sample_n     INTEGER,
            win_rate     REAL,
            profit_factor REAL,
            ev_per_trade REAL,
            sharpe       REAL,
            max_drawdown REAL,
            t_stat       REAL,
            p_value      REAL,
            disabled     BOOLEAN DEFAULT FALSE,
            updated_at   TIMESTAMP DEFAULT NOW()
        )
    """)

    # Migration-safe: add new columns to existing deployments
    for ddl in [
        "ALTER TABLE scanner_state   ADD COLUMN IF NOT EXISTS vwap_snapshot      TEXT DEFAULT '{}'",
        "ALTER TABLE scanner_state   ADD COLUMN IF NOT EXISTS momentum_ranking   TEXT DEFAULT '[]'",
        "ALTER TABLE paper_trades    ADD COLUMN IF NOT EXISTS source_type        TEXT DEFAULT 'signal'",
        "ALTER TABLE paper_trades    ADD COLUMN IF NOT EXISTS score_at_entry     REAL",
        "ALTER TABLE portfolio       ADD COLUMN IF NOT EXISTS user_id            INTEGER DEFAULT 1",
        "ALTER TABLE signal_outcomes ADD COLUMN IF NOT EXISTS price_15day        REAL",
        "ALTER TABLE signal_outcomes ADD COLUMN IF NOT EXISTS pct_change_15day   REAL",
        "ALTER TABLE signal_log      ADD COLUMN IF NOT EXISTS quant_adj          FLOAT",
        "ALTER TABLE signal_log      ADD COLUMN IF NOT EXISTS source_quality     TEXT",
        "ALTER TABLE signal_log      ADD COLUMN IF NOT EXISTS session_mode       TEXT DEFAULT 'MARKET'",
        "ALTER TABLE signal_outcomes ADD COLUMN IF NOT EXISTS outcome_1d         TEXT",
        "ALTER TABLE signal_outcomes ADD COLUMN IF NOT EXISTS outcome_3d         TEXT",
        "ALTER TABLE signal_outcomes ADD COLUMN IF NOT EXISTS outcome_5d         TEXT",
        "ALTER TABLE signal_outcomes ADD COLUMN IF NOT EXISTS ret_1d             REAL",
        "ALTER TABLE signal_outcomes ADD COLUMN IF NOT EXISTS ret_3d             REAL",
        "ALTER TABLE signal_outcomes ADD COLUMN IF NOT EXISTS ret_5d             REAL",
        "ALTER TABLE conviction_buys  ADD COLUMN IF NOT EXISTS shares             REAL",
        "ALTER TABLE conviction_buys  ADD COLUMN IF NOT EXISTS limit_entry        REAL",
        "ALTER TABLE conviction_buys  ADD COLUMN IF NOT EXISTS conviction_score   REAL",
        "ALTER TABLE scanner_control  ADD COLUMN IF NOT EXISTS current_mode              TEXT DEFAULT 'UNKNOWN'",
        "ALTER TABLE scanner_state    ADD COLUMN IF NOT EXISTS signals_suppressed_today INTEGER DEFAULT 0",
        "ALTER TABLE scanner_state    ADD COLUMN IF NOT EXISTS universe_size             INTEGER DEFAULT 0",
        "ALTER TABLE scanner_state    ADD COLUMN IF NOT EXISTS data_quality_tiingo       FLOAT DEFAULT 0",
        "ALTER TABLE scanner_state    ADD COLUMN IF NOT EXISTS data_quality_yfinance     FLOAT DEFAULT 0",
        "ALTER TABLE scanner_state    ADD COLUMN IF NOT EXISTS data_quality_stale        FLOAT DEFAULT 0",
        "ALTER TABLE scanner_state    ADD COLUMN IF NOT EXISTS last_conviction_run       TIMESTAMP",
        "ALTER TABLE scanner_state    ADD COLUMN IF NOT EXISTS last_accuracy_run         TIMESTAMP",
        "ALTER TABLE scanner_state    ADD COLUMN IF NOT EXISTS open_positions            INTEGER DEFAULT 0",
        "ALTER TABLE signal_log       ADD COLUMN IF NOT EXISTS quality_tag               TEXT",
        "ALTER TABLE users            ADD COLUMN IF NOT EXISTS display_name              TEXT",
        "ALTER TABLE signal_log       ADD COLUMN IF NOT EXISTS outcome_1d               FLOAT",
        "ALTER TABLE signal_log       ADD COLUMN IF NOT EXISTS outcome_3d               FLOAT",
        "ALTER TABLE signal_log       ADD COLUMN IF NOT EXISTS outcome_5d               FLOAT",
        "ALTER TABLE signal_log       ADD COLUMN IF NOT EXISTS graded_at                TIMESTAMP",
    ]:
        cur.execute(ddl)

    # Migrate existing portfolio rows to admin (user_id=1)
    cur.execute("UPDATE portfolio SET user_id = 1 WHERE user_id IS NULL")

    # Add composite unique constraint for per-user portfolio (idempotent via DO NOTHING)
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE portfolio ADD CONSTRAINT portfolio_ticker_user_uq UNIQUE (ticker, user_id);
        EXCEPTION WHEN duplicate_table OR duplicate_object THEN NULL;
        END $$
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("  ✓ PostgreSQL tables initialized")
    _seed_admin_user_pg()


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
        CREATE TABLE IF NOT EXISTS scanner_control (
            id                 INTEGER PRIMARY KEY DEFAULT 1,
            paused             INTEGER DEFAULT 0,
            force_scan         INTEGER DEFAULT 0,
            scanner_started_at TEXT,
            current_mode       TEXT DEFAULT 'UNKNOWN',
            updated_at         TEXT DEFAULT (datetime('now'))
        )
    """)
    cur.execute("INSERT OR IGNORE INTO scanner_control (id) VALUES (1)")

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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT NOT NULL UNIQUE,
            email         TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role          TEXT NOT NULL DEFAULT 'user',
            created_at    TEXT DEFAULT (datetime('now')),
            last_login    TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            session_token TEXT PRIMARY KEY,
            user_id       INTEGER NOT NULL REFERENCES users(id),
            created_at    TEXT DEFAULT (datetime('now')),
            expires_at    TEXT NOT NULL,
            is_active     INTEGER DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_log (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker           TEXT NOT NULL,
            signal_label     TEXT NOT NULL,
            score            REAL,
            score_breakdown  TEXT,
            price_at_signal  REAL,
            volume_at_signal REAL,
            alert_type       TEXT,
            created_at       TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_outcomes (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id          INTEGER REFERENCES signal_log(id),
            price_1hr          REAL,
            price_1day         REAL,
            price_5day         REAL,
            pct_change_1hr     REAL,
            pct_change_1day    REAL,
            pct_change_5day    REAL,
            outcome_label      TEXT DEFAULT 'pending',
            outcome_updated_at TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS accuracy_reports (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            report_type  TEXT NOT NULL,
            checkpoint   INTEGER,
            filename     TEXT,
            download_url TEXT,
            status_label TEXT,
            generated_at TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scanner_logs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            level      TEXT NOT NULL DEFAULT 'info',
            message    TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS data_quality (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker     TEXT NOT NULL,
            source     TEXT NOT NULL,
            status     TEXT NOT NULL,
            latency_ms REAL,
            error      TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS quote_cache (
            ticker      TEXT PRIMARY KEY,
            price       REAL,
            prev_close  REAL,
            volume      REAL,
            high        REAL,
            low         REAL,
            source      TEXT,
            created_at  TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS universe (
            ticker         TEXT PRIMARY KEY,
            exchange       TEXT,
            market_cap     INTEGER,
            adv_20         INTEGER,
            sector         TEXT,
            industry       TEXT,
            sic_code       INTEGER,
            active         INTEGER DEFAULT 1,
            last_refreshed TEXT,
            last_scanned   TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS conviction_buys (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            date           TEXT,
            session        TEXT,
            rank           INTEGER,
            ticker         TEXT,
            conviction     REAL,
            hold_type      TEXT,
            entry          REAL,
            stop_loss      REAL,
            target_1       REAL,
            target_2       REAL,
            target_3       REAL,
            position_pct   REAL,
            expected_value REAL,
            reasoning      TEXT,
            composite      REAL,
            quant_adj      REAL,
            created_at     TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS validator_health (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            check_time     TEXT DEFAULT (datetime('now')),
            signals_graded INTEGER DEFAULT 0,
            signals_pending INTEGER DEFAULT 0,
            oldest_pending TEXT,
            error_msg      TEXT DEFAULT ''
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS accuracy_metrics (
            bucket        TEXT PRIMARY KEY,
            sample_n      INTEGER,
            win_rate      REAL,
            profit_factor REAL,
            ev_per_trade  REAL,
            sharpe        REAL,
            max_drawdown  REAL,
            t_stat        REAL,
            p_value       REAL,
            disabled      INTEGER DEFAULT 0,
            updated_at    TEXT DEFAULT (datetime('now'))
        )
    """)

    for col_sql in [
        "ALTER TABLE scanner_state   ADD COLUMN vwap_snapshot     TEXT DEFAULT '{}'",
        "ALTER TABLE scanner_state   ADD COLUMN momentum_ranking  TEXT DEFAULT '[]'",
        "ALTER TABLE paper_trades    ADD COLUMN source_type       TEXT DEFAULT 'signal'",
        "ALTER TABLE paper_trades    ADD COLUMN score_at_entry    REAL",
        "ALTER TABLE portfolio       ADD COLUMN user_id           INTEGER DEFAULT 1",
        "ALTER TABLE signal_outcomes ADD COLUMN price_15day       REAL",
        "ALTER TABLE signal_outcomes ADD COLUMN pct_change_15day  REAL",
        "ALTER TABLE signal_log      ADD COLUMN quant_adj         REAL",
        "ALTER TABLE signal_log      ADD COLUMN source_quality    TEXT",
        "ALTER TABLE signal_log      ADD COLUMN session_mode      TEXT DEFAULT 'MARKET'",
        "ALTER TABLE signal_outcomes ADD COLUMN outcome_1d        TEXT",
        "ALTER TABLE signal_outcomes ADD COLUMN outcome_3d        TEXT",
        "ALTER TABLE signal_outcomes ADD COLUMN outcome_5d        TEXT",
        "ALTER TABLE signal_outcomes ADD COLUMN ret_1d            REAL",
        "ALTER TABLE signal_outcomes ADD COLUMN ret_3d            REAL",
        "ALTER TABLE signal_outcomes ADD COLUMN ret_5d            REAL",
        "ALTER TABLE conviction_buys  ADD COLUMN shares            REAL",
        "ALTER TABLE conviction_buys  ADD COLUMN limit_entry       REAL",
        "ALTER TABLE conviction_buys  ADD COLUMN conviction_score  REAL",
        "ALTER TABLE scanner_control  ADD COLUMN current_mode              TEXT DEFAULT 'UNKNOWN'",
        "ALTER TABLE scanner_state    ADD COLUMN signals_suppressed_today INTEGER DEFAULT 0",
        "ALTER TABLE scanner_state    ADD COLUMN universe_size             INTEGER DEFAULT 0",
        "ALTER TABLE scanner_state    ADD COLUMN data_quality_tiingo       REAL DEFAULT 0",
        "ALTER TABLE scanner_state    ADD COLUMN data_quality_yfinance     REAL DEFAULT 0",
        "ALTER TABLE scanner_state    ADD COLUMN data_quality_stale        REAL DEFAULT 0",
        "ALTER TABLE scanner_state    ADD COLUMN last_conviction_run       TEXT",
        "ALTER TABLE scanner_state    ADD COLUMN last_accuracy_run         TEXT",
        "ALTER TABLE scanner_state    ADD COLUMN open_positions            INTEGER DEFAULT 0",
        "ALTER TABLE signal_log       ADD COLUMN quality_tag               TEXT",
        "ALTER TABLE users            ADD COLUMN display_name              TEXT",
        "ALTER TABLE signal_log       ADD COLUMN outcome_1d               REAL",
        "ALTER TABLE signal_log       ADD COLUMN outcome_3d               REAL",
        "ALTER TABLE signal_log       ADD COLUMN outcome_5d               REAL",
        "ALTER TABLE signal_log       ADD COLUMN graded_at                TEXT",
    ]:
        try:
            cur.execute(col_sql)
        except Exception:
            pass  # Column already exists

    cur.execute("UPDATE portfolio SET user_id = 1 WHERE user_id IS NULL")

    conn.commit()
    conn.close()
    _seed_admin_user_sqlite()


# ─── Portfolio ─────────────────────────────────────────────────────────────────

def upsert_holding(ticker: str, shares: float, avg_cost: float, notes: str = "", user_id: int = 1):
    ticker = ticker.upper()
    if _is_postgres():
        conn = _get_pg_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO portfolio (ticker, shares, avg_cost, notes, user_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ticker, user_id) DO UPDATE SET
                shares   = EXCLUDED.shares,
                avg_cost = EXCLUDED.avg_cost,
                notes    = EXCLUDED.notes
        """, (ticker, shares, avg_cost, notes, user_id))
        conn.commit()
        cur.close()
        conn.close()
    else:
        conn = _get_sqlite_conn()
        exists = conn.execute(
            "SELECT 1 FROM portfolio WHERE ticker = ? AND user_id = ?", (ticker, user_id)
        ).fetchone()
        if exists:
            conn.execute(
                "UPDATE portfolio SET shares=?, avg_cost=?, notes=? WHERE ticker=? AND user_id=?",
                (shares, avg_cost, notes, ticker, user_id)
            )
        else:
            conn.execute(
                "INSERT INTO portfolio (ticker, shares, avg_cost, notes, user_id) VALUES (?,?,?,?,?)",
                (ticker, shares, avg_cost, notes, user_id)
            )
        conn.commit()
        conn.close()


def delete_holding(ticker: str, user_id: int = 1):
    ticker = ticker.upper()
    if _is_postgres():
        conn = _get_pg_conn()
        cur  = conn.cursor()
        cur.execute("DELETE FROM portfolio WHERE ticker = %s AND user_id = %s", (ticker, user_id))
        conn.commit()
        cur.close()
        conn.close()
    else:
        conn = _get_sqlite_conn()
        conn.execute("DELETE FROM portfolio WHERE ticker = ? AND user_id = ?", (ticker, user_id))
        conn.commit()
        conn.close()


def get_portfolio(user_id: int = 1) -> pd.DataFrame:
    if _is_postgres():
        conn = _get_pg_conn()
        cur  = conn.cursor()
        cur.execute(
            "SELECT ticker, shares, avg_cost, notes, added_at FROM portfolio WHERE user_id = %s ORDER BY ticker",
            (user_id,)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        if not rows:
            return pd.DataFrame(columns=["ticker","shares","avg_cost","notes","added_at"])
        return pd.DataFrame(rows, columns=["ticker","shares","avg_cost","notes","added_at"])
    else:
        conn = _get_sqlite_conn()
        df   = pd.read_sql("SELECT * FROM portfolio WHERE user_id = ? ORDER BY ticker", conn, params=(user_id,))
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
                       vwap_snapshot, momentum_ranking,
                       signals_suppressed_today, universe_size, open_positions,
                       last_conviction_run, last_accuracy_run
                FROM scanner_state WHERE date = %s
            """, (date_str,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                return {
                    "date":                     date_str,
                    "alerted_today":            json.loads(row[0] or "[]"),
                    "known_filings":            json.loads(row[1] or "[]"),
                    "scan_count":               row[2] or 0,
                    "last_updated":             row[3] or "",
                    "vwap_snapshot":            json.loads(row[4] or "{}"),
                    "momentum_ranking":         json.loads(row[5] or "[]"),
                    "signals_suppressed_today": row[6] or 0,
                    "universe_size":            row[7] or 0,
                    "open_positions":           row[8] or 0,
                    "last_conviction_run":      row[9],
                    "last_accuracy_run":        row[10],
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
                     vwap_snapshot, momentum_ranking,
                     signals_suppressed_today, universe_size, open_positions,
                     last_conviction_run, last_accuracy_run)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date) DO UPDATE SET
                    alerted_today             = EXCLUDED.alerted_today,
                    known_filings             = EXCLUDED.known_filings,
                    scan_count                = EXCLUDED.scan_count,
                    last_updated              = EXCLUDED.last_updated,
                    vwap_snapshot             = EXCLUDED.vwap_snapshot,
                    momentum_ranking          = EXCLUDED.momentum_ranking,
                    signals_suppressed_today  = EXCLUDED.signals_suppressed_today,
                    universe_size             = EXCLUDED.universe_size,
                    open_positions            = EXCLUDED.open_positions,
                    last_conviction_run       = EXCLUDED.last_conviction_run,
                    last_accuracy_run         = EXCLUDED.last_accuracy_run
            """, (
                date_str,
                json.dumps(list(state_dict.get("alerted_today", []))),
                json.dumps(list(state_dict.get("known_filings", []))),
                state_dict.get("scan_count", 0),
                state_dict.get("last_updated", ""),
                json.dumps(state_dict.get("vwap_snapshot", {})),
                json.dumps(state_dict.get("momentum_ranking", [])),
                state_dict.get("signals_suppressed_today", 0),
                state_dict.get("universe_size", 0),
                state_dict.get("open_positions", 0),
                state_dict.get("last_conviction_run"),
                state_dict.get("last_accuracy_run"),
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


# ─── Scanner Control (pause / force-scan flags) ───────────────────────────────

def get_scanner_control() -> dict:
    """Read pause, force_scan, and current_mode from scanner_control table."""
    default = {"paused": False, "force_scan": False, "scanner_started_at": None,
               "updated_at": None, "current_mode": "UNKNOWN"}
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT paused, force_scan, scanner_started_at, updated_at, current_mode
                FROM scanner_control WHERE id = 1
            """)
            row = cur.fetchone()
            cur.close(); conn.close()
            if row:
                return {"paused": bool(row[0]), "force_scan": bool(row[1]),
                        "scanner_started_at": row[2], "updated_at": row[3],
                        "current_mode": row[4] or "UNKNOWN"}
        else:
            conn = _get_sqlite_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT paused, force_scan, scanner_started_at, updated_at, current_mode
                FROM scanner_control WHERE id = 1
            """)
            row = cur.fetchone()
            conn.close()
            if row:
                return {"paused": bool(row[0]), "force_scan": bool(row[1]),
                        "scanner_started_at": row[2], "updated_at": row[3],
                        "current_mode": row[4] or "UNKNOWN"}
    except Exception as e:
        print(f"  [db] get_scanner_control failed: {e}")
    return default


def set_scanner_control(paused: bool = None, force_scan: bool = None,
                        scanner_started_at=None, current_mode: str = None) -> None:
    """Update scanner control flags."""
    if _is_postgres():
        ph = "%s"
    else:
        ph = "?"
    sets, vals = [], []
    if paused is not None:
        sets.append(f"paused = {ph}"); vals.append(paused)
    if force_scan is not None:
        sets.append(f"force_scan = {ph}"); vals.append(force_scan)
    if scanner_started_at is not None:
        sets.append(f"scanner_started_at = {ph}"); vals.append(str(scanner_started_at))
    if current_mode is not None:
        sets.append(f"current_mode = {ph}"); vals.append(current_mode)
    if not sets:
        return
    sets.append("updated_at = NOW()" if _is_postgres() else "updated_at = datetime('now')")
    sql = f"UPDATE scanner_control SET {', '.join(sets)} WHERE id = 1"
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute(sql, vals)
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute(sql, vals)
            conn.commit(); conn.close()
    except Exception as e:
        print(f"  [db] set_scanner_control failed: {e}")


def get_control_stats() -> dict:
    """Return summary stats for the Control tab: signals today, alerts today, top signal, scan count."""
    stats = {"signals_today": 0, "alerts_today": 0, "top_signal": None,
             "scan_count": 0, "last_updated": None}
    date_str = datetime.now().strftime("%Y-%m-%d")
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM signal_log WHERE DATE(created_at) = %s", (date_str,))
            stats["signals_today"] = cur.fetchone()[0] or 0
            cur.execute("SELECT COUNT(*) FROM alert_log WHERE DATE(created_at) = %s", (date_str,))
            stats["alerts_today"] = cur.fetchone()[0] or 0
            cur.execute("""
                SELECT ticker, signal_label, score FROM signal_log
                WHERE DATE(created_at) = %s ORDER BY score DESC LIMIT 1
            """, (date_str,))
            row = cur.fetchone()
            if row:
                stats["top_signal"] = {"ticker": row[0], "label": row[1], "score": row[2]}
            cur.execute("SELECT scan_count, last_updated FROM scanner_state WHERE date = %s", (date_str,))
            row = cur.fetchone()
            if row:
                stats["scan_count"] = row[0] or 0
                stats["last_updated"] = row[1]
            cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            cur  = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM signal_log WHERE DATE(created_at) = ?", (date_str,))
            stats["signals_today"] = cur.fetchone()[0] or 0
            cur.execute("SELECT COUNT(*) FROM alert_log WHERE DATE(created_at) = ?", (date_str,))
            stats["alerts_today"] = cur.fetchone()[0] or 0
            cur.execute("SELECT ticker, signal_label, score FROM signal_log WHERE DATE(created_at) = ? ORDER BY score DESC LIMIT 1", (date_str,))
            row = cur.fetchone()
            if row:
                stats["top_signal"] = {"ticker": row[0], "label": row[1], "score": row[2]}
            cur.execute("SELECT scan_count, last_updated FROM scanner_state WHERE date = ?", (date_str,))
            row = cur.fetchone()
            if row:
                stats["scan_count"] = row[0] or 0
                stats["last_updated"] = row[1]
            conn.close()
    except Exception as e:
        print(f"  [db] get_control_stats failed: {e}")
    return stats


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _f(v) -> Optional[float]:
    """Convert numpy/pandas scalars to a plain Python float (or None)."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ─── Paper Trades ──────────────────────────────────────────────────────────────

def log_paper_trade(ticker: str, signal_type: str, entry_price: float,
                    stop_price: float, target1: float, target2: float,
                    source_type: str = "signal", score_at_entry: float = None) -> Optional[int]:
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
                     target1, target2, entry_time, source_type, score_at_entry)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (trade_date, ticker, signal_type, _f(entry_price), _f(stop_price),
                  _f(target1), _f(target2), entry_time, source_type, _f(score_at_entry)))
            row_id = cur.fetchone()[0]
            conn.commit(); cur.close(); conn.close()
            return row_id
        else:
            conn = _get_sqlite_conn()
            cur  = conn.cursor()
            cur.execute("""
                INSERT INTO paper_trades
                    (trade_date, ticker, signal_type, entry_price, stop_price,
                     target1, target2, entry_time, source_type, score_at_entry)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (trade_date, ticker, signal_type, _f(entry_price), _f(stop_price),
                  _f(target1), _f(target2), entry_time, source_type, _f(score_at_entry)))
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
                SELECT id, ticker, entry_price, stop_price, target1, source_type
                FROM paper_trades WHERE trade_date = %s AND outcome = 'open'
            """, (trade_date,))
            rows = cur.fetchall()
            for row_id, ticker, entry, stop, t1, src in rows:
                exit_p  = exit_prices.get(ticker, entry)
                is_short = src == "prediction_sell"
                pnl     = (exit_p - entry) / entry * 100 * (-1 if is_short else 1)
                if is_short:
                    outcome = "win" if exit_p <= t1 else "loss" if exit_p >= stop else "open"
                else:
                    outcome = "win" if exit_p >= t1 else "loss" if exit_p <= stop else "open"
                cur.execute("""
                    UPDATE paper_trades
                    SET exit_price=%s, exit_time=%s, outcome=%s, pnl_pct=%s
                    WHERE id=%s
                """, (_f(exit_p), exit_time, outcome, _f(round(pnl, 2)), row_id))
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT id, ticker, entry_price, stop_price, target1, source_type
                FROM paper_trades WHERE trade_date=? AND outcome='open'
            """, (trade_date,))
            rows = cur.fetchall()
            for row_id, ticker, entry, stop, t1, src in rows:
                exit_p   = exit_prices.get(ticker, entry)
                is_short = (src or "") == "prediction_sell"
                pnl      = (exit_p - entry) / entry * 100 * (-1 if is_short else 1)
                if is_short:
                    outcome = "win" if exit_p <= t1 else "loss" if exit_p >= stop else "open"
                else:
                    outcome = "win" if exit_p >= t1 else "loss" if exit_p <= stop else "open"
                cur.execute("""
                    UPDATE paper_trades
                    SET exit_price=?, exit_time=?, outcome=?, pnl_pct=?
                    WHERE id=?
                """, (exit_p, exit_time, outcome, round(pnl, 2), row_id))
            conn.commit(); conn.close()
    except Exception as e:
        print(f"  [db] EOD paper trade close failed: {e}")


_PAPER_TRADE_COLS = [
    "id", "trade_date", "ticker", "signal_type", "entry_price", "stop_price",
    "target1", "target2", "entry_time", "exit_price", "exit_time",
    "outcome", "pnl_pct", "source_type", "score_at_entry",
]


def get_paper_trades(days: int = 30) -> pd.DataFrame:
    """Return paper trades from the last N days, most recent first."""
    try:
        col_list = ", ".join(_PAPER_TRADE_COLS)
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute(f"""
                SELECT {col_list}
                FROM paper_trades
                WHERE trade_date >= NOW() - INTERVAL '{days} days'
                ORDER BY created_at DESC
            """)
            rows = cur.fetchall()
            cur.close(); conn.close()
            df = pd.DataFrame(rows, columns=_PAPER_TRADE_COLS) if rows else pd.DataFrame(columns=_PAPER_TRADE_COLS)
        else:
            conn = _get_sqlite_conn()
            df = pd.read_sql(f"""
                SELECT {col_list}
                FROM paper_trades
                WHERE date(trade_date) >= date('now', '-{days} days')
                ORDER BY created_at DESC
            """, conn)
            conn.close()
        # Fallback: ensure source_type column exists even on old DBs
        if "source_type" not in df.columns:
            df["source_type"] = "signal"
        if "score_at_entry" not in df.columns:
            df["score_at_entry"] = None
        return df
    except Exception as e:
        print(f"  [db] Paper trades load failed: {e}")
        return pd.DataFrame(columns=_PAPER_TRADE_COLS)


# ─── Signal Log ────────────────────────────────────────────────────────────────

def should_suppress(ticker: str, signal_label: str, window_minutes: int = 5) -> bool:
    """Return True if the same ticker+signal_label fired within the last window_minutes."""
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT MAX(created_at) FROM signal_log
                WHERE ticker = %s AND signal_label = %s
                  AND created_at > NOW() - INTERVAL '%s minutes'
            """, (ticker, signal_label, window_minutes))
            row = cur.fetchone(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT MAX(created_at) FROM signal_log
                WHERE ticker = ? AND signal_label = ?
                  AND created_at > datetime('now', ? || ' minutes')
            """, (ticker, signal_label, f"-{window_minutes}"))
            row = cur.fetchone(); conn.close()
        return row is not None and row[0] is not None
    except Exception:
        return False


def log_signal(ticker: str, signal_label: str, score: float,
               score_breakdown: dict, price_at_signal: float,
               volume_at_signal: float, alert_type: str,
               quant_adj: float = None, source_quality: str = None,
               session_mode: str = None, quality_tag: str = None) -> Optional[int]:
    """Log a scanner signal. Returns the signal_log row id or None on failure."""
    breakdown_json = json.dumps(score_breakdown)
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                INSERT INTO signal_log
                    (ticker, signal_label, score, score_breakdown,
                     price_at_signal, volume_at_signal, alert_type,
                     quant_adj, source_quality, session_mode, quality_tag)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (ticker, signal_label, _f(score), breakdown_json,
                  _f(price_at_signal), _f(volume_at_signal), alert_type,
                  _f(quant_adj) if quant_adj is not None else None,
                  source_quality, session_mode or "MARKET", quality_tag))
            sig_id = cur.fetchone()[0]
            cur.execute("INSERT INTO signal_outcomes (signal_id) VALUES (%s)", (sig_id,))
            conn.commit(); cur.close(); conn.close()
            return sig_id
        else:
            conn = _get_sqlite_conn()
            cur  = conn.cursor()
            cur.execute("""
                INSERT INTO signal_log
                    (ticker, signal_label, score, score_breakdown,
                     price_at_signal, volume_at_signal, alert_type,
                     quant_adj, source_quality, session_mode, quality_tag)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (ticker, signal_label, _f(score), breakdown_json,
                  _f(price_at_signal), _f(volume_at_signal), alert_type,
                  _f(quant_adj) if quant_adj is not None else None,
                  source_quality, session_mode or "MARKET", quality_tag))
            sig_id = cur.lastrowid
            cur.execute("INSERT INTO signal_outcomes (signal_id) VALUES (?)", (sig_id,))
            conn.commit(); conn.close()
            return sig_id
    except Exception as e:
        print(f"  [db] Signal log failed: {e}")
        return None


def get_pending_signals(max_age_days: int = 20) -> List[dict]:
    """Return signal_log rows whose outcomes are still incomplete (missing 15-day price)."""
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT sl.id, sl.ticker, sl.price_at_signal, sl.created_at,
                       so.price_1hr, so.price_1day, so.price_5day, so.price_15day
                FROM signal_log sl
                JOIN signal_outcomes so ON so.signal_id = sl.id
                WHERE so.price_15day IS NULL
                  AND sl.created_at >= NOW() - INTERVAL '%s days'
                ORDER BY sl.created_at ASC
            """, (max_age_days,))
            rows = cur.fetchall()
            cur.close(); conn.close()
            return [{"id": r[0], "ticker": r[1], "price_at_signal": r[2],
                     "created_at": r[3], "price_1hr": r[4],
                     "price_1day": r[5], "price_5day": r[6], "price_15day": r[7]} for r in rows]
        else:
            conn = _get_sqlite_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT sl.id, sl.ticker, sl.price_at_signal, sl.created_at,
                       so.price_1hr, so.price_1day, so.price_5day, so.price_15day
                FROM signal_log sl
                JOIN signal_outcomes so ON so.signal_id = sl.id
                WHERE so.price_15day IS NULL
                  AND sl.created_at >= datetime('now', ?)
                ORDER BY sl.created_at ASC
            """, (f"-{max_age_days} days",))
            rows = cur.fetchall()
            conn.close()
            return [{"id": r[0], "ticker": r[1], "price_at_signal": r[2],
                     "created_at": r[3], "price_1hr": r[4],
                     "price_1day": r[5], "price_5day": r[6], "price_15day": r[7]} for r in rows]
    except Exception as e:
        print(f"  [db] get_pending_signals failed: {e}")
        return []


def update_signal_outcome(signal_id: int, price_1hr: Optional[float] = None,
                          price_1day: Optional[float] = None,
                          price_5day: Optional[float] = None,
                          price_15day: Optional[float] = None,
                          price_at_signal: Optional[float] = None):
    """Fill in available price outcomes and compute outcome_label when 5-day is known."""
    try:
        pct_1hr   = (price_1hr   / price_at_signal - 1) * 100 if price_1hr   and price_at_signal else None
        pct_1day  = (price_1day  / price_at_signal - 1) * 100 if price_1day  and price_at_signal else None
        pct_5day  = (price_5day  / price_at_signal - 1) * 100 if price_5day  and price_at_signal else None
        pct_15day = (price_15day / price_at_signal - 1) * 100 if price_15day and price_at_signal else None

        if pct_5day is not None:
            if pct_5day >= 5.0:
                label = "win"
            elif pct_5day <= -5.0:
                label = "loss"
            else:
                label = "neutral"
        else:
            label = "pending"

        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                UPDATE signal_outcomes SET
                    price_1hr          = COALESCE(%s, price_1hr),
                    price_1day         = COALESCE(%s, price_1day),
                    price_5day         = COALESCE(%s, price_5day),
                    price_15day        = COALESCE(%s, price_15day),
                    pct_change_1hr     = COALESCE(%s, pct_change_1hr),
                    pct_change_1day    = COALESCE(%s, pct_change_1day),
                    pct_change_5day    = COALESCE(%s, pct_change_5day),
                    pct_change_15day   = COALESCE(%s, pct_change_15day),
                    outcome_label      = %s,
                    outcome_updated_at = NOW()
                WHERE signal_id = %s
            """, (price_1hr, price_1day, price_5day, price_15day,
                  pct_1hr, pct_1day, pct_5day, pct_15day, label, signal_id))
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute("""
                UPDATE signal_outcomes SET
                    price_1hr          = COALESCE(?, price_1hr),
                    price_1day         = COALESCE(?, price_1day),
                    price_5day         = COALESCE(?, price_5day),
                    price_15day        = COALESCE(?, price_15day),
                    pct_change_1hr     = COALESCE(?, pct_change_1hr),
                    pct_change_1day    = COALESCE(?, pct_change_1day),
                    pct_change_5day    = COALESCE(?, pct_change_5day),
                    pct_change_15day   = COALESCE(?, pct_change_15day),
                    outcome_label      = ?,
                    outcome_updated_at = datetime('now')
                WHERE signal_id = ?
            """, (price_1hr, price_1day, price_5day, price_15day,
                  pct_1hr, pct_1day, pct_5day, pct_15day, label, signal_id))
            conn.commit(); conn.close()
    except Exception as e:
        print(f"  [db] update_signal_outcome failed: {e}")


def get_signal_log(days: int = 30) -> pd.DataFrame:
    """Return recent signal_log rows joined with outcomes for the dashboard."""
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT sl.id, sl.ticker, sl.signal_label, sl.score,
                       sl.score_breakdown, sl.price_at_signal, sl.alert_type,
                       sl.created_at,
                       so.pct_change_1hr, so.pct_change_1day, so.pct_change_5day,
                       so.pct_change_15day, so.outcome_label
                FROM signal_log sl
                LEFT JOIN signal_outcomes so ON so.signal_id = sl.id
                WHERE sl.created_at >= NOW() - INTERVAL '%s days'
                ORDER BY sl.created_at DESC
            """, (days,))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            cur.close(); conn.close()
            df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()
        else:
            conn = _get_sqlite_conn()
            df = pd.read_sql(f"""
                SELECT sl.id, sl.ticker, sl.signal_label, sl.score,
                       sl.score_breakdown, sl.price_at_signal, sl.alert_type,
                       sl.created_at,
                       so.pct_change_1hr, so.pct_change_1day, so.pct_change_5day,
                       so.pct_change_15day, so.outcome_label
                FROM signal_log sl
                LEFT JOIN signal_outcomes so ON so.signal_id = sl.id
                WHERE sl.created_at >= datetime('now', '-{days} days')
                ORDER BY sl.created_at DESC
            """, conn)
            conn.close()
        if not df.empty and "score_breakdown" in df.columns:
            df["score_breakdown"] = df["score_breakdown"].apply(
                lambda x: json.loads(x) if x else {}
            )
        return df
    except Exception as e:
        print(f"  [db] get_signal_log failed: {e}")
        return pd.DataFrame()


def get_signal_stats() -> dict:
    """Aggregate win-rate and component correlation stats for the Accuracy tab."""
    try:
        df = get_signal_log(days=90)
        if df.empty or "outcome_label" not in df.columns:
            return {}

        resolved = df[df["outcome_label"].isin(["win", "loss", "neutral"])].copy()
        if resolved.empty:
            return {"total": len(df), "resolved": 0}

        resolved["is_win"] = resolved["outcome_label"] == "win"

        by_signal = {}
        for label, grp in resolved.groupby("signal_label"):
            wins = grp["is_win"].sum()
            total = len(grp)
            avg_gain = grp.loc[grp["is_win"], "pct_change_5day"].mean()
            avg_loss = grp.loc[~grp["is_win"], "pct_change_5day"].mean()
            by_signal[label] = {
                "count":    total,
                "wins":     int(wins),
                "win_rate": round(wins / total * 100, 1),
                "avg_gain": round(float(avg_gain), 2) if pd.notna(avg_gain) else None,
                "avg_loss": round(float(avg_loss), 2) if pd.notna(avg_loss) else None,
            }

        # Component correlation: mean component score for wins vs losses
        component_corr = {}
        components = ["technical", "catalyst", "fundamental", "risk", "sentiment"]
        for comp in components:
            win_scores  = []
            loss_scores = []
            for _, row in resolved.iterrows():
                bd = row.get("score_breakdown") or {}
                val = bd.get(comp)
                if val is None:
                    continue
                (win_scores if row["is_win"] else loss_scores).append(val)
            if win_scores or loss_scores:
                component_corr[comp] = {
                    "win_mean":  round(sum(win_scores)  / len(win_scores),  1) if win_scores  else None,
                    "loss_mean": round(sum(loss_scores) / len(loss_scores), 1) if loss_scores else None,
                }

        return {
            "total":          len(df),
            "resolved":       len(resolved),
            "overall_win_rate": round(resolved["is_win"].mean() * 100, 1),
            "avg_5day_gain":  round(float(resolved.loc[resolved["is_win"], "pct_change_5day"].mean()), 2)
                              if resolved["is_win"].any() else None,
            "by_signal":      by_signal,
            "component_corr": component_corr,
        }
    except Exception as e:
        print(f"  [db] get_signal_stats failed: {e}")
        return {}


# ─── Accuracy Reports ──────────────────────────────────────────────────────────

def save_accuracy_report(report_type: str, checkpoint: int, filename: str,
                          download_url: str, status_label: str) -> None:
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                INSERT INTO accuracy_reports (report_type, checkpoint, filename, download_url, status_label)
                VALUES (%s, %s, %s, %s, %s)
            """, (report_type, checkpoint, filename, download_url, status_label))
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute("""
                INSERT INTO accuracy_reports (report_type, checkpoint, filename, download_url, status_label)
                VALUES (?, ?, ?, ?, ?)
            """, (report_type, checkpoint, filename, download_url, status_label))
            conn.commit(); conn.close()
        print(f"  [db] accuracy_report saved: {report_type} verdict={status_label}")
    except Exception as e:
        print(f"  [db] save_accuracy_report failed: {e}")


def get_accuracy_reports() -> List[dict]:
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT report_type, checkpoint, filename, download_url, status_label, generated_at
                FROM accuracy_reports ORDER BY generated_at DESC
            """)
            rows = cur.fetchall(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT report_type, checkpoint, filename, download_url, status_label, generated_at
                FROM accuracy_reports ORDER BY generated_at DESC
            """)
            rows = cur.fetchall(); conn.close()
        return [{"report_type": r[0], "checkpoint": r[1], "filename": r[2],
                 "download_url": r[3], "status_label": r[4], "generated_at": r[5]}
                for r in rows]
    except Exception as e:
        print(f"  [db] get_accuracy_reports failed: {e}")
        return []


# ─── Users & Sessions ──────────────────────────────────────────────────────────

def _seed_admin_user_pg():
    """Create or promote the admin account from env vars on every startup (Postgres)."""
    import os
    username = os.environ.get("ADMIN_USERNAME", "").strip()
    password = os.environ.get("ADMIN_PASSWORD", "").strip()
    if not username:
        return
    try:
        from auth import hash_password
        conn = _get_pg_conn()
        cur  = conn.cursor()
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        if row is None:
            if not password:
                cur.close(); conn.close(); return
            cur.execute("""
                INSERT INTO users (username, email, password_hash, role)
                VALUES (%s, %s, %s, 'admin')
            """, (username, f"{username}@admin.local", hash_password(password)))
            print(f"  ✓ Admin user '{username}' created")
        else:
            cur.execute("UPDATE users SET role = 'admin' WHERE username = %s", (username,))
            print(f"  ✓ Admin role granted to '{username}'")
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print(f"  [db] Admin seed failed: {e}")


def _seed_admin_user_sqlite():
    """Create or promote the admin account from env vars on every startup (SQLite)."""
    import os
    username = os.environ.get("ADMIN_USERNAME", "").strip()
    password = os.environ.get("ADMIN_PASSWORD", "").strip()
    if not username:
        return
    try:
        from auth import hash_password
        conn = _get_sqlite_conn()
        cur  = conn.cursor()
        cur.execute("SELECT id FROM users WHERE username = ?", (username,))
        row = cur.fetchone()
        if row is None:
            if not password:
                conn.close(); return
            cur.execute("""
                INSERT INTO users (username, email, password_hash, role)
                VALUES (?, ?, ?, 'admin')
            """, (username, f"{username}@admin.local", hash_password(password)))
            print(f"  ✓ Admin user '{username}' created")
        else:
            cur.execute("UPDATE users SET role = 'admin' WHERE username = ?", (username,))
            print(f"  ✓ Admin role granted to '{username}'")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  [db] Admin seed failed: {e}")


def create_user(username: str, email: str, password_hash: str, role: str = "user") -> int:
    """Insert a new user. Returns the new user id. Raises on duplicate username/email."""
    if _is_postgres():
        conn = _get_pg_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO users (username, email, password_hash, role)
            VALUES (%s, %s, %s, %s) RETURNING id
        """, (username.strip(), email.strip().lower(), password_hash, role))
        uid = cur.fetchone()[0]
        conn.commit(); cur.close(); conn.close()
        return uid
    else:
        conn = _get_sqlite_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO users (username, email, password_hash, role)
            VALUES (?, ?, ?, ?)
        """, (username.strip(), email.strip().lower(), password_hash, role))
        uid = cur.lastrowid
        conn.commit(); conn.close()
        return uid


def get_user_by_username(username: str) -> Optional[dict]:
    if _is_postgres():
        conn = _get_pg_conn()
        cur  = conn.cursor()
        cur.execute(
            "SELECT id, username, email, password_hash, role, last_login, display_name FROM users WHERE username = %s",
            (username.strip(),)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return None
        return {"id": row[0], "username": row[1], "email": row[2],
                "password_hash": row[3], "role": row[4], "last_login": row[5],
                "display_name": row[6]}
    else:
        conn = _get_sqlite_conn()
        cur  = conn.cursor()
        cur.execute(
            "SELECT id, username, email, password_hash, role, last_login, display_name FROM users WHERE username = ?",
            (username.strip(),)
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        return {"id": row[0], "username": row[1], "email": row[2],
                "password_hash": row[3], "role": row[4], "last_login": row[5],
                "display_name": row[6]}


def update_last_login(user_id: int):
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (user_id,))
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute("UPDATE users SET last_login = datetime('now') WHERE id = ?", (user_id,))
            conn.commit(); conn.close()
    except Exception as e:
        print(f"  [db] update_last_login failed: {e}")


def create_session(user_id: int) -> str:
    """Create a new session token valid for SESSION_EXPIRY_HOURS hours."""
    from auth import generate_session_token, SESSION_EXPIRY_HOURS
    from datetime import timezone
    token      = generate_session_token()
    expires_at = datetime.now(timezone.utc) + __import__("datetime").timedelta(hours=SESSION_EXPIRY_HOURS)
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                INSERT INTO user_sessions (session_token, user_id, expires_at)
                VALUES (%s, %s, %s)
            """, (token, user_id, expires_at))
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute("""
                INSERT INTO user_sessions (session_token, user_id, expires_at)
                VALUES (?, ?, ?)
            """, (token, user_id, expires_at.strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit(); conn.close()
    except Exception as e:
        print(f"  [db] create_session failed: {e}")
    return token


def validate_session(token: str) -> Optional[dict]:
    """Return user dict if session is valid and not expired, else None."""
    if not token:
        return None
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT u.id, u.username, u.email, u.role
                FROM user_sessions s
                JOIN users u ON u.id = s.user_id
                WHERE s.session_token = %s
                  AND s.is_active = TRUE
                  AND s.expires_at > NOW()
            """, (token,))
            row = cur.fetchone()
            cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT u.id, u.username, u.email, u.role
                FROM user_sessions s
                JOIN users u ON u.id = s.user_id
                WHERE s.session_token = ?
                  AND s.is_active = 1
                  AND s.expires_at > datetime('now')
            """, (token,))
            row = cur.fetchone()
            conn.close()
        if not row:
            return None
        return {"id": row[0], "username": row[1], "email": row[2], "role": row[3]}
    except Exception as e:
        print(f"  [db] validate_session failed: {e}")
        return None


def invalidate_session(token: str):
    """Mark a session as inactive (logout)."""
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("UPDATE user_sessions SET is_active = FALSE WHERE session_token = %s", (token,))
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute("UPDATE user_sessions SET is_active = 0 WHERE session_token = ?", (token,))
            conn.commit(); conn.close()
    except Exception as e:
        print(f"  [db] invalidate_session failed: {e}")


def get_all_users() -> list:
    """Admin-only: return all users (no password hashes)."""
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("SELECT id, username, email, role, created_at, last_login FROM users ORDER BY created_at")
            rows = cur.fetchall()
            cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            cur  = conn.cursor()
            cur.execute("SELECT id, username, email, role, created_at, last_login FROM users ORDER BY created_at")
            rows = cur.fetchall()
            conn.close()
        return [{"id": r[0], "username": r[1], "email": r[2],
                 "role": r[3], "created_at": r[4], "last_login": r[5]} for r in rows]
    except Exception as e:
        print(f"  [db] get_all_users failed: {e}")


# ─── Scanner Logs ──────────────────────────────────────────────────────────────

def log_scanner_event(level: str, message: str) -> None:
    """Write a structured log line to scanner_logs. Trims table to 500 rows."""
    level = level.lower().strip()
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute(
                "INSERT INTO scanner_logs (level, message) VALUES (%s, %s)",
                (level, message[:500])
            )
            # Keep only the newest 500 rows
            cur.execute("""
                DELETE FROM scanner_logs WHERE id NOT IN (
                    SELECT id FROM scanner_logs ORDER BY created_at DESC LIMIT 500
                )
            """)
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute(
                "INSERT INTO scanner_logs (level, message) VALUES (?, ?)",
                (level, message[:500])
            )
            conn.execute("""
                DELETE FROM scanner_logs WHERE id NOT IN (
                    SELECT id FROM scanner_logs ORDER BY created_at DESC LIMIT 500
                )
            """)
            conn.commit(); conn.close()
    except Exception as e:
        print(f"  [db] log_scanner_event failed: {e}")


def get_scanner_logs(limit: int = 50) -> List[dict]:
    """Return the most recent scanner log entries, newest first."""
    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT id, level, message, created_at
                FROM scanner_logs ORDER BY created_at DESC LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT id, level, message, created_at
                FROM scanner_logs ORDER BY created_at DESC LIMIT ?
            """, (limit,))
            rows = cur.fetchall()
            conn.close()
        return [{"id": r[0], "level": r[1], "message": r[2], "created_at": r[3]}
                for r in rows]
    except Exception as e:
        print(f"  [db] get_scanner_logs failed: {e}")
        return []


# ─── Shared query functions (used by both app.py and mobile.py) ───────────────

def get_scanner_state() -> dict:
    """Return combined scanner_state + scanner_control dict for status display."""
    date_str = datetime.now().strftime("%Y-%m-%d")
    result = {
        "current_mode": "UNKNOWN", "paused": False,
        "last_heartbeat": None, "scan_count": 0,
        "signals_today": 0, "signals_suppressed_today": 0,
        "universe_size": 0, "open_positions": 0,
        "data_quality_tiingo": 0.0, "data_quality_yfinance": 0.0, "data_quality_stale": 0.0,
        "last_conviction_run": None, "last_accuracy_run": None,
        "scanner_started_at": None,
    }
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT paused, current_mode, scanner_started_at, updated_at
                FROM scanner_control WHERE id = 1
            """)
            row = cur.fetchone()
            if row:
                result["paused"]            = bool(row[0])
                result["current_mode"]      = row[1] or "UNKNOWN"
                result["scanner_started_at"] = row[2]
                result["last_heartbeat"]    = row[3]
            cur.execute("""
                SELECT scan_count, last_updated,
                       signals_suppressed_today, universe_size, open_positions,
                       data_quality_tiingo, data_quality_yfinance, data_quality_stale,
                       last_conviction_run, last_accuracy_run
                FROM scanner_state WHERE date = %s
            """, (date_str,))
            row = cur.fetchone()
            if row:
                result["scan_count"]               = row[0] or 0
                if row[1]: result["last_heartbeat"] = row[1]
                result["signals_suppressed_today"] = row[2] or 0
                result["universe_size"]            = row[3] or 0
                result["open_positions"]           = row[4] or 0
                result["data_quality_tiingo"]      = float(row[5] or 0)
                result["data_quality_yfinance"]    = float(row[6] or 0)
                result["data_quality_stale"]       = float(row[7] or 0)
                result["last_conviction_run"]      = row[8]
                result["last_accuracy_run"]        = row[9]
            cur.execute(
                "SELECT COUNT(*) FROM signal_log WHERE DATE(created_at) = %s", (date_str,)
            )
            result["signals_today"] = cur.fetchone()[0] or 0
            cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute(
                "SELECT paused, current_mode, scanner_started_at, updated_at "
                "FROM scanner_control WHERE id = 1"
            )
            row = cur.fetchone()
            if row:
                result["paused"]             = bool(row[0])
                result["current_mode"]       = row[1] or "UNKNOWN"
                result["scanner_started_at"] = row[2]
                result["last_heartbeat"]     = row[3]
            cur.execute(
                "SELECT scan_count, last_updated FROM scanner_state WHERE date = ?", (date_str,)
            )
            row = cur.fetchone()
            if row:
                result["scan_count"] = row[0] or 0
                if row[1]: result["last_heartbeat"] = row[1]
            cur.execute(
                "SELECT COUNT(*) FROM signal_log WHERE DATE(created_at) = ?", (date_str,)
            )
            result["signals_today"] = cur.fetchone()[0] or 0
            conn.close()
    except Exception as e:
        print(f"  [db] get_scanner_state failed: {e}")
    return result


def get_accuracy_metrics() -> list:
    """Return per-bucket rows from accuracy_metrics table."""
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT bucket, sample_n, win_rate, profit_factor, ev_per_trade,
                       sharpe, t_stat, p_value, disabled, updated_at
                FROM accuracy_metrics ORDER BY bucket
            """)
            rows = cur.fetchall(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT bucket, sample_n, win_rate, profit_factor, ev_per_trade,
                       sharpe, t_stat, p_value, disabled, updated_at
                FROM accuracy_metrics ORDER BY bucket
            """)
            rows = cur.fetchall(); conn.close()
        return [
            {"bucket": r[0], "n": r[1] or 0, "win_rate": r[2],
             "profit_factor": r[3], "ev_per_trade": r[4], "sharpe": r[5],
             "t_stat": r[6], "p_value": r[7], "disabled": bool(r[8]),
             "updated_at": r[9]}
            for r in rows
        ]
    except Exception as e:
        print(f"  [db] get_accuracy_metrics failed: {e}")
        return []


def get_validator_health() -> dict:
    """Return the latest validator_health row."""
    result = {
        "signals_graded": 0, "signals_pending": 0,
        "oldest_pending": None, "error_msg": "", "check_time": None,
    }
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT signals_graded, signals_pending, oldest_pending, error_msg, check_time
                FROM validator_health ORDER BY id DESC LIMIT 1
            """)
            row = cur.fetchone(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT signals_graded, signals_pending, oldest_pending, error_msg, check_time
                FROM validator_health ORDER BY id DESC LIMIT 1
            """)
            row = cur.fetchone(); conn.close()
        if row:
            result = {
                "signals_graded":  row[0] or 0,
                "signals_pending": row[1] or 0,
                "oldest_pending":  row[2],
                "error_msg":       row[3] or "",
                "check_time":      row[4],
            }
    except Exception as e:
        print(f"  [db] get_validator_health failed: {e}")
    return result


def get_account_summary() -> dict:
    """Return pt_account row as a dict."""
    cols = ["balance", "equity", "day_start_equity", "total_pnl", "realized_pnl",
            "unrealized_pnl", "total_trades", "winning_trades", "losing_trades",
            "win_rate", "profit_factor", "max_drawdown", "peak_equity", "sharpe"]
    defaults = dict(zip(cols, [100000.0, 100000.0, 100000.0, 0.0, 0.0, 0.0,
                                0, 0, 0, 0.0, 0.0, 0.0, 100000.0, 0.0]))
    try:
        q = ", ".join(cols)
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute(f"SELECT {q} FROM pt_account WHERE id = 1")
            row = cur.fetchone(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute(f"SELECT {q} FROM pt_account WHERE id = 1")
            row = cur.fetchone(); conn.close()
        if row:
            return {c: (float(v) if v is not None else 0.0) for c, v in zip(cols, row)}
    except Exception as e:
        print(f"  [db] get_account_summary failed: {e}")
    return defaults


def get_open_positions() -> list:
    """Return all open pt_positions rows as list of dicts."""
    cols = ["ticker", "side", "shares", "avg_cost", "current_price", "market_value",
            "unrealized_pnl", "unrealized_pct", "stop_loss", "target_1", "target_2",
            "target_3", "hold_type", "entry_reason", "conviction", "mae", "mfe", "opened_at"]
    try:
        q = ", ".join(cols)
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute(f"SELECT {q} FROM pt_positions ORDER BY market_value DESC NULLS LAST")
            rows = cur.fetchall(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute(f"SELECT {q} FROM pt_positions ORDER BY market_value DESC")
            rows = cur.fetchall(); conn.close()
        return [dict(zip(cols, row)) for row in rows]
    except Exception as e:
        print(f"  [db] get_open_positions failed: {e}")
        return []


def get_open_position_count() -> int:
    """Return count of open pt_positions rows."""
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM pt_positions")
            n = cur.fetchone()[0]; cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM pt_positions")
            n = cur.fetchone()[0]; conn.close()
        return n or 0
    except Exception:
        return 0


def get_recent_trades(n: int = 10) -> list:
    """Return last n closed pt_trades rows."""
    cols = ["ticker", "side", "shares", "entry_price", "exit_price", "entry_at", "exit_at",
            "hold_days", "gross_pnl", "net_pnl", "pnl_pct", "exit_reason", "hold_type", "conviction"]
    try:
        q = ", ".join(cols)
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute(f"SELECT {q} FROM pt_trades ORDER BY exit_at DESC LIMIT %s", (n,))
            rows = cur.fetchall(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute(f"SELECT {q} FROM pt_trades ORDER BY exit_at DESC LIMIT ?", (n,))
            rows = cur.fetchall(); conn.close()
        return [dict(zip(cols, row)) for row in rows]
    except Exception as e:
        print(f"  [db] get_recent_trades failed: {e}")
        return []


def get_equity_curve(n: int = 200) -> list:
    """Return last n pt_equity_curve rows, oldest-first for charting."""
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT recorded_at, equity FROM (
                    SELECT recorded_at, equity FROM pt_equity_curve
                    ORDER BY recorded_at DESC LIMIT %s
                ) sub ORDER BY recorded_at ASC
            """, (n,))
            rows = cur.fetchall(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT recorded_at, equity FROM (
                    SELECT recorded_at, equity FROM pt_equity_curve
                    ORDER BY recorded_at DESC LIMIT ?
                ) sub ORDER BY recorded_at ASC
            """, (n,))
            rows = cur.fetchall(); conn.close()
        return [{"recorded_at": r[0], "equity": float(r[1]) if r[1] else 0.0} for r in rows]
    except Exception as e:
        print(f"  [db] get_equity_curve failed: {e}")
        return []


def get_graded_signals(n: int = 20) -> list:
    """Return last n signals with graded 5d outcomes (reads signal_log directly)."""
    cols = ["ticker", "score", "signal_label", "price_at_signal", "created_at",
            "ret_1d", "ret_3d", "ret_5d"]
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT ticker, score, signal_label, price_at_signal, created_at,
                       outcome_1d, outcome_3d, outcome_5d
                FROM signal_log
                WHERE outcome_5d IS NOT NULL
                ORDER BY created_at DESC LIMIT %s
            """, (n,))
            rows = cur.fetchall(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT ticker, score, signal_label, price_at_signal, created_at,
                       outcome_1d, outcome_3d, outcome_5d
                FROM signal_log
                WHERE outcome_5d IS NOT NULL
                ORDER BY created_at DESC LIMIT ?
            """, (n,))
            rows = cur.fetchall(); conn.close()
        return [dict(zip(cols, row)) for row in rows]
    except Exception as e:
        print(f"  [db] get_graded_signals failed: {e}")
        return []


def get_rolling_win_rate() -> list:
    """Return daily win rate over last 37 days from signal_log.outcome_5d."""
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT DATE(created_at) AS signal_date,
                       SUM(CASE WHEN outcome_5d > 2.0 THEN 1 ELSE 0 END)::float
                           / NULLIF(COUNT(*), 0) AS win_rate,
                       COUNT(*) AS n
                FROM signal_log
                WHERE outcome_5d IS NOT NULL
                  AND created_at >= NOW() - INTERVAL '37 days'
                GROUP BY DATE(created_at)
                ORDER BY signal_date
            """)
            rows = cur.fetchall(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT DATE(created_at) AS signal_date,
                       CAST(SUM(CASE WHEN outcome_5d > 2.0 THEN 1 ELSE 0 END) AS REAL)
                           / NULLIF(COUNT(*), 0) AS win_rate,
                       COUNT(*) AS n
                FROM signal_log
                WHERE outcome_5d IS NOT NULL
                  AND created_at >= datetime('now', '-37 days')
                GROUP BY DATE(created_at)
                ORDER BY signal_date
            """)
            rows = cur.fetchall(); conn.close()
        return [{"date": str(r[0]), "win_rate": float(r[1]) if r[1] else 0.0, "n": r[2]}
                for r in rows]
    except Exception as e:
        print(f"  [db] get_rolling_win_rate failed: {e}")
        return []


def get_overall_accuracy() -> dict:
    """Return overall accuracy stats from signal_log.outcome_* for the last 90 days."""
    result = {
        "total": 0, "win_rate_1d": None, "win_rate_3d": None, "win_rate_5d": None,
        "profit_factor": None, "ev": None, "sharpe": None,
    }
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT
                    COUNT(*) AS n,
                    SUM(CASE WHEN outcome_1d > 2.0 THEN 1 ELSE 0 END)::float
                        / NULLIF(SUM(CASE WHEN outcome_1d IS NOT NULL THEN 1 ELSE 0 END), 0) AS wr1,
                    SUM(CASE WHEN outcome_3d > 2.0 THEN 1 ELSE 0 END)::float
                        / NULLIF(SUM(CASE WHEN outcome_3d IS NOT NULL THEN 1 ELSE 0 END), 0) AS wr3,
                    SUM(CASE WHEN outcome_5d > 2.0 THEN 1 ELSE 0 END)::float
                        / NULLIF(SUM(CASE WHEN outcome_5d IS NOT NULL THEN 1 ELSE 0 END), 0) AS wr5,
                    AVG(outcome_5d) AS avg_ret,
                    STDDEV(outcome_5d) AS std_ret
                FROM signal_log
                WHERE created_at >= NOW() - INTERVAL '90 days'
            """)
            row = cur.fetchone(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT
                    COUNT(*) AS n,
                    CAST(SUM(CASE WHEN outcome_1d > 2.0 THEN 1 ELSE 0 END) AS REAL)
                        / NULLIF(SUM(CASE WHEN outcome_1d IS NOT NULL THEN 1 ELSE 0 END), 0),
                    CAST(SUM(CASE WHEN outcome_3d > 2.0 THEN 1 ELSE 0 END) AS REAL)
                        / NULLIF(SUM(CASE WHEN outcome_3d IS NOT NULL THEN 1 ELSE 0 END), 0),
                    CAST(SUM(CASE WHEN outcome_5d > 2.0 THEN 1 ELSE 0 END) AS REAL)
                        / NULLIF(SUM(CASE WHEN outcome_5d IS NOT NULL THEN 1 ELSE 0 END), 0),
                    AVG(outcome_5d),
                    NULL
                FROM signal_log
                WHERE created_at >= datetime('now', '-90 days')
            """)
            row = cur.fetchone(); conn.close()
        if row and row[0]:
            result["total"]       = row[0]
            result["win_rate_1d"] = round(float(row[1]) * 100, 1) if row[1] else None
            result["win_rate_3d"] = round(float(row[2]) * 100, 1) if row[2] else None
            result["win_rate_5d"] = round(float(row[3]) * 100, 1) if row[3] else None
            if row[4] and row[5] and float(row[5]) > 0:
                result["sharpe"] = round(float(row[4]) / float(row[5]) * (252 ** 0.5), 2)
    except Exception as e:
        print(f"  [db] get_overall_accuracy failed: {e}")
    return result


def seed_mobile_admin() -> None:
    """Ensure 'admin'/'axiom2026' exists in users table. No-op if admin already exists."""
    try:
        user = get_user_by_username("admin")
        if user is not None:
            return
        from auth import hash_password
        ph_hash = hash_password("axiom2026")
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                INSERT INTO users (username, email, password_hash, display_name, role)
                VALUES (%s, %s, %s, %s, 'admin')
                ON CONFLICT DO NOTHING
            """, ("admin", "admin@admin.local", ph_hash, "Vishwa"))
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute("""
                INSERT OR IGNORE INTO users (username, email, password_hash, display_name, role)
                VALUES (?, ?, ?, ?, 'admin')
            """, ("admin", "admin@admin.local", ph_hash, "Vishwa"))
            conn.commit(); conn.close()
        print("  [db] Mobile admin 'admin' seeded")
    except Exception as e:
        print(f"  [db] seed_mobile_admin failed: {e}")
