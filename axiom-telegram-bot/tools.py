"""
Axiom Terminal — Tool Implementations
All database and system tools available to the Claude agent.
"""

import os
import json
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]

READONLY_KEYWORDS = ["drop", "delete", "truncate", "insert", "update", "alter", "create", "grant", "revoke"]


def _get_conn():
    return psycopg2.connect(DATABASE_URL)


def _safe_sql(sql: str) -> bool:
    """Reject any SQL that isn't a SELECT."""
    cleaned = sql.strip().lower()
    if not cleaned.startswith("select"):
        return False
    for kw in READONLY_KEYWORDS:
        if f" {kw} " in f" {cleaned} ":
            return False
    return True


# ── READ TOOLS ───────────────────────────────────────────────────────────────

def query_database(sql: str, description: str = "") -> dict:
    """Run a read-only SQL query."""
    if not _safe_sql(sql):
        return {"error": "Only SELECT queries are allowed via this tool."}
    try:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchmany(100)
                return {
                    "rows": [dict(r) for r in rows],
                    "count": len(rows),
                    "description": description
                }
    except Exception as e:
        logger.error(f"query_database error: {e}")
        return {"error": str(e)}


def get_scanner_status() -> dict:
    """Check scanner health — last run time, today's signal count, watchlist size, current mode."""
    try:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT created_at, ticker, signal_label, score
                    FROM signal_log
                    ORDER BY created_at DESC
                    LIMIT 1
                """)
                last_signal = cur.fetchone()

                cur.execute("SELECT COUNT(*) as count FROM signal_log WHERE created_at >= CURRENT_DATE")
                today_count = cur.fetchone()["count"]

                cur.execute("SELECT COUNT(*) as count FROM signal_log")
                total_count = cur.fetchone()["count"]

                cur.execute("SELECT COUNT(*) as count FROM watchlist")
                watchlist_size = cur.fetchone()["count"]

                cur.execute("""
                    SELECT paused, force_scan, current_mode, scanner_started_at, updated_at
                    FROM scanner_control WHERE id = 1
                """)
                ctrl = cur.fetchone()

                now = datetime.now(timezone.utc)
                if last_signal and last_signal["created_at"]:
                    last_run = last_signal["created_at"]
                    if last_run.tzinfo is None:
                        last_run = last_run.replace(tzinfo=timezone.utc)
                    minutes_since = (now - last_run).total_seconds() / 60
                    status = "ACTIVE" if minutes_since < 45 else "IDLE"
                else:
                    minutes_since = None
                    status = "NO DATA"

                return {
                    "status": status,
                    "current_mode": ctrl["current_mode"] if ctrl else "UNKNOWN",
                    "paused": ctrl["paused"] if ctrl else False,
                    "last_signal": {
                        "ticker": last_signal["ticker"] if last_signal else None,
                        "signal_label": last_signal["signal_label"] if last_signal else None,
                        "score": last_signal["score"] if last_signal else None,
                        "created_at": str(last_signal["created_at"]) if last_signal else None,
                    },
                    "minutes_since_last_signal": round(minutes_since, 1) if minutes_since else None,
                    "signals_today": today_count,
                    "total_signals": total_count,
                    "watchlist_size": watchlist_size,
                    "scanner_started_at": str(ctrl["scanner_started_at"]) if ctrl and ctrl["scanner_started_at"] else None,
                }
    except Exception as e:
        logger.error(f"get_scanner_status error: {e}")
        return {"error": str(e)}


def get_accuracy_summary(window: str = "5day") -> dict:
    """Summarize accuracy test progress and win rates."""
    price_col_map = {
        "1hr": "price_1hr",
        "1day": "price_1day",
        "5day": "price_5day",
        "15day": "price_15day"
    }
    price_col = price_col_map.get(window, "price_5day")

    try:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT COUNT(*) as total FROM signal_log")
                total = cur.fetchone()["total"]

                cur.execute(f"""
                    SELECT
                        sl.signal_label,
                        COUNT(*) as total,
                        COUNT(so.{price_col}) as resolved,
                        SUM(CASE
                            WHEN so.{price_col} IS NOT NULL
                            AND so.{price_col} > sl.price_at_signal * 1.05
                            AND sl.signal_label ILIKE '%buy%'
                            THEN 1
                            WHEN so.{price_col} IS NOT NULL
                            AND so.{price_col} < sl.price_at_signal * 0.95
                            AND sl.signal_label ILIKE '%short%'
                            THEN 1
                            ELSE 0
                        END) as wins
                    FROM signal_log sl
                    LEFT JOIN signal_outcomes so ON so.signal_id = sl.id
                    GROUP BY sl.signal_label
                    ORDER BY total DESC
                """)
                by_label = cur.fetchall()

                breakdown = []
                for row in by_label:
                    resolved = row["resolved"] or 0
                    wins = row["wins"] or 0
                    win_rate = round(wins / resolved * 100, 1) if resolved > 0 else None
                    breakdown.append({
                        "signal_label": row["signal_label"],
                        "total": row["total"],
                        "resolved": resolved,
                        "wins": wins,
                        "win_rate_pct": win_rate
                    })

                # Checkpoints: 150 / 350 / 600
                checkpoints = {
                    "checkpoint_1": {"target": 150, "label": "Sanity Check", "reached": total >= 150},
                    "checkpoint_2": {"target": 350, "label": "Preliminary Assessment", "reached": total >= 350},
                    "checkpoint_3": {"target": 600, "label": "Final Verdict", "reached": total >= 600},
                }
                next_checkpoint = None
                for cp, info in checkpoints.items():
                    if not info["reached"]:
                        next_checkpoint = info["target"]
                        break

                return {
                    "total_signals": total,
                    "outcome_window": window,
                    "next_checkpoint": next_checkpoint,
                    "signals_to_next_checkpoint": (next_checkpoint - total) if next_checkpoint else 0,
                    "checkpoints": checkpoints,
                    "by_signal_label": breakdown
                }
    except Exception as e:
        logger.error(f"get_accuracy_summary error: {e}")
        return {"error": str(e)}


def get_signal_log(limit: int = 10, signal_label: str = None, ticker: str = None) -> dict:
    """Get recent signals with optional filters."""
    limit = min(limit, 50)
    try:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                conditions = []
                params = []

                if signal_label:
                    conditions.append("signal_label ILIKE %s")
                    params.append(f"%{signal_label}%")
                if ticker:
                    conditions.append("ticker ILIKE %s")
                    params.append(ticker.upper())

                where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
                params.append(limit)

                cur.execute(f"""
                    SELECT ticker, signal_label, score, price_at_signal, created_at
                    FROM signal_log
                    {where}
                    ORDER BY created_at DESC
                    LIMIT %s
                """, params)

                rows = cur.fetchall()
                return {
                    "signals": [dict(r) for r in rows],
                    "count": len(rows)
                }
    except Exception as e:
        logger.error(f"get_signal_log error: {e}")
        return {"error": str(e)}


# ── WRITE TOOLS (all require confirmation, called only after user confirms) ──

def update_weights(weights: dict) -> dict:
    """Update scoring component weights."""
    required_keys = {"technical", "catalyst", "fundamental", "risk", "sentiment"}
    provided_keys = set(weights.keys())

    if not provided_keys.issubset(required_keys):
        return {"error": f"Unknown weight keys: {provided_keys - required_keys}"}

    total = sum(weights.values())
    if abs(total - 100) > 0.1:
        return {"error": f"Weights must sum to 100, got {total}"}

    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE scanner_control SET updated_at = NOW() WHERE id = 1")
            conn.commit()
        return {
            "success": True,
            "weights_updated": weights,
            "note": "Update the WEIGHT constants in your scanner config and redeploy Service 2 to apply."
        }
    except Exception as e:
        logger.error(f"update_weights error: {e}")
        return {"error": str(e)}


def adjust_thresholds(thresholds: dict) -> dict:
    """Adjust score thresholds."""
    valid_keys = {"strong_buy", "buy", "short", "strong_short"}
    if not set(thresholds.keys()).issubset(valid_keys):
        return {"error": f"Unknown threshold keys. Valid: {valid_keys}"}

    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE scanner_control SET updated_at = NOW() WHERE id = 1")
            conn.commit()
        return {
            "success": True,
            "thresholds_updated": thresholds,
            "note": "Update PREDICTION_BUY_THRESHOLD / PREDICTION_SELL_THRESHOLD in scanner_loop.py and redeploy Service 2."
        }
    except Exception as e:
        logger.error(f"adjust_thresholds error: {e}")
        return {"error": str(e)}


def modify_watchlist(action: str, tickers: list) -> dict:
    """Add or remove tickers from the watchlist."""
    tickers = [t.upper().strip() for t in tickers]

    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                if action == "add":
                    for ticker in tickers:
                        cur.execute("""
                            INSERT INTO watchlist (ticker)
                            VALUES (%s)
                            ON CONFLICT DO NOTHING
                        """, (ticker,))
                elif action == "remove":
                    cur.execute("DELETE FROM watchlist WHERE ticker = ANY(%s)", (tickers,))
                else:
                    return {"error": f"Unknown action: {action}"}
            conn.commit()
        return {"success": True, "action": action, "tickers": tickers}
    except Exception as e:
        logger.error(f"modify_watchlist error: {e}")
        return {"error": str(e)}


def restart_scanner(reason: str) -> dict:
    """
    Signal the scanner to restart by setting restart_requested = TRUE
    in scanner_control. The scanner checks this at the top of every loop cycle
    and calls sys.exit(0) which triggers Railway to restart Service 2.
    """
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE scanner_control
                    SET restart_requested = TRUE, updated_at = NOW()
                    WHERE id = 1
                """)
            conn.commit()
        return {
            "success": True,
            "message": "Restart flag set. Scanner will restart at the start of its next cycle.",
            "reason": reason
        }
    except Exception as e:
        logger.error(f"restart_scanner error: {e}")
        return {"error": str(e)}
