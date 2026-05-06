# db/database.py
# Handles all SQLite persistence: portfolio, scan results, flag history.
# Using SQLAlchemy Core for simplicity — no ORM needed here.

import sqlite3
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from config import DB_PATH


def get_connection() -> sqlite3.Connection:
    """Return a SQLite connection with row_factory for dict-like access."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_db():
    """
    Create all tables if they don't exist.
    Safe to call on every startup — uses IF NOT EXISTS.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Portfolio holdings: what you own and at what cost
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS portfolio (
            ticker          TEXT PRIMARY KEY,
            shares          REAL NOT NULL,
            avg_cost        REAL NOT NULL,
            notes           TEXT DEFAULT '',
            added_at        TEXT DEFAULT (datetime('now'))
        )
    """)

    # Scan results: one row per ticker per scan run
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scan_results (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date       TEXT NOT NULL,
            ticker          TEXT NOT NULL,
            company_name    TEXT,
            price           REAL,
            market_cap      REAL,
            volume          REAL,
            avg_volume      REAL,
            technical_score REAL,
            catalyst_score  REAL,
            fundamental_score REAL,
            risk_score      REAL,
            sentiment_score REAL,
            final_score     REAL,
            signal          TEXT,
            risk_flags      TEXT,   -- JSON array of active flags
            summary         TEXT,   -- Plain-English explanation
            data_sources    TEXT,   -- JSON array of sources used
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)

    # Risk flags log: track when flags appear/disappear over time
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS flag_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT NOT NULL,
            flag_type   TEXT NOT NULL,
            flag_date   TEXT NOT NULL,
            detail      TEXT
        )
    """)

    conn.commit()
    conn.close()


# ─── Portfolio CRUD ────────────────────────────────────────────────────────────

def upsert_holding(ticker: str, shares: float, avg_cost: float, notes: str = ""):
    """Add or update a portfolio holding."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO portfolio (ticker, shares, avg_cost, notes)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
            shares   = excluded.shares,
            avg_cost = excluded.avg_cost,
            notes    = excluded.notes
    """, (ticker.upper(), shares, avg_cost, notes))
    conn.commit()
    conn.close()


def delete_holding(ticker: str):
    """Remove a holding from portfolio."""
    conn = get_connection()
    conn.execute("DELETE FROM portfolio WHERE ticker = ?", (ticker.upper(),))
    conn.commit()
    conn.close()


def get_portfolio() -> pd.DataFrame:
    """Return all portfolio holdings as a DataFrame."""
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM portfolio ORDER BY ticker", conn)
    conn.close()
    return df


# ─── Scan Results ──────────────────────────────────────────────────────────────

def save_scan_result(result: dict):
    """Persist a single ticker's scan result to the database."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO scan_results (
            scan_date, ticker, company_name, price, market_cap,
            volume, avg_volume, technical_score, catalyst_score,
            fundamental_score, risk_score, sentiment_score,
            final_score, signal, risk_flags, summary, data_sources
        ) VALUES (
            :scan_date, :ticker, :company_name, :price, :market_cap,
            :volume, :avg_volume, :technical_score, :catalyst_score,
            :fundamental_score, :risk_score, :sentiment_score,
            :final_score, :signal, :risk_flags, :summary, :data_sources
        )
    """, {
        **result,
        "risk_flags":   json.dumps(result.get("risk_flags", [])),
        "data_sources": json.dumps(result.get("data_sources", [])),
        "scan_date":    result.get("scan_date", datetime.now().strftime("%Y-%m-%d")),
    })
    conn.commit()
    conn.close()


def get_latest_scan() -> pd.DataFrame:
    """Return the most recent scan results for all tickers."""
    conn = get_connection()
    df = pd.read_sql("""
        SELECT * FROM scan_results
        WHERE scan_date = (SELECT MAX(scan_date) FROM scan_results)
        ORDER BY final_score DESC
    """, conn)
    conn.close()
    if not df.empty:
        df["risk_flags"]   = df["risk_flags"].apply(lambda x: json.loads(x) if x else [])
        df["data_sources"] = df["data_sources"].apply(lambda x: json.loads(x) if x else [])
    return df


def get_scan_history(ticker: str, days: int = 30) -> pd.DataFrame:
    """Return scan history for a specific ticker over N days."""
    conn = get_connection()
    df = pd.read_sql("""
        SELECT scan_date, final_score, signal, price
        FROM scan_results
        WHERE ticker = ?
        ORDER BY scan_date DESC
        LIMIT ?
    """, conn, params=(ticker.upper(), days))
    conn.close()
    return df
