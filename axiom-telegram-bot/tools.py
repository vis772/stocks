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
                rows = cur.fetchmany(100)  # Cap at 100 rows
                return {
                    "rows": [dict(r) for r in rows],
                    "count": len(rows),
                    "description": description
                }
    except Exception as e:
        logger.error(f"query_database error: {e}")
        return {"error": str(e)}


def get_scanner_status() -> dict:
    """Check scanner health — last run time, today's signal count, watchlist size."""
    try:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Last signal logged
                cur.execute("""
                    SELECT created_at, ticker, signal_label, score
                    FROM signal_log
                    ORDER BY created_at DESC
                    LIMIT 1
                """)
                last_signal = cur.fetchone()

                # Today's signal count
                cur.execute("""
                    SELECT COUNT(*) as count
                    FROM signal_log
                    WHERE created_at >= CURRENT_DATE
                """)
                today_count = cur.fetchone()["count"]

                # Total signal count
                cur.execute("SELECT COUNT(*) as count FROM signal_log")
                total_count = cur.fetchone()["count"]

                # Watchlist size
                cur.execute("SELECT COUNT(*) as count FROM watchlist")
                watchlist_size = cur.fetchone()["count"]

                # Check if scanner ran in last 45 min (market hours indicator)
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
                    "last_signal": {
                        "ticker": last_signal["ticker"] if last_signal else None,
                        "signal_label": last_signal["signal_label"] if last_signal else None,
                        "score": last_signal["score"] if last_signal else None,
                        "created_at": str(last_signal["created_at"]) if last_signal else None,
                    },
                    "minutes_since_last_signal": round(minutes_since, 1) if minutes_since else None,
                    "signals_today": today_count,
                    "total_signals": total_count,
                    "watchlist_size": watchlist_size
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
                # Total signals
                cur.execute("SELECT COUNT(*) as total FROM signal_log")
                total = cur.fetchone()["total"]

                # Win rates by signal label
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

                # Checkpoint progress
                checkpoints = {
                    "checkpoint_1": {"target": 150, "reached": total >= 150},
                    "checkpoint_2": {"target": 350, "reached": total >= 350},
                    "checkpoint_3": {"target": 600, "reached": total >= 600},
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
    """Update scoring component weights in scanner_control."""
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
                for component, value in weights.items():
                    cur.execute("""
                        UPDATE scanner_control
                        SET value = %s, updated_at = NOW()
                        WHERE key = %s
                    """, (str(value), f"weight_{component}"))
                    if cur.rowcount == 0:
                        cur.execute("""
                            INSERT INTO scanner_control (key, value, updated_at)
                            VALUES (%s, %s, NOW())
                        """, (f"weight_{component}", str(value)))
            conn.commit()
        return {"success": True, "weights_updated": weights}
    except Exception as e:
        logger.error(f"update_weights error: {e}")
        return {"error": str(e)}


def adjust_thresholds(thresholds: dict) -> dict:
    """Adjust score thresholds in scanner_control."""
    valid_keys = {"strong_buy", "buy", "short", "strong_short"}
    if not set(thresholds.keys()).issubset(valid_keys):
        return {"error": f"Unknown threshold keys. Valid: {valid_keys}"}

    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                for key, value in thresholds.items():
                    cur.execute("""
                        UPDATE scanner_control
                        SET value = %s, updated_at = NOW()
                        WHERE key = %s
                    """, (str(value), f"threshold_{key}"))
                    if cur.rowcount == 0:
                        cur.execute("""
                            INSERT INTO scanner_control (key, value, updated_at)
                            VALUES (%s, %s, NOW())
                        """, (f"threshold_{key}", str(value)))
            conn.commit()
        return {"success": True, "thresholds_updated": thresholds}
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
                            INSERT INTO watchlist (ticker, added_at)
                            VALUES (%s, NOW())
                            ON CONFLICT (ticker) DO NOTHING
                        """, (ticker,))
                elif action == "remove":
                    cur.execute("""
                        DELETE FROM watchlist
                        WHERE ticker = ANY(%s)
                    """, (tickers,))
                else:
                    return {"error": f"Unknown action: {action}"}
            conn.commit()
        return {"success": True, "action": action, "tickers": tickers}
    except Exception as e:
        logger.error(f"modify_watchlist error: {e}")
        return {"error": str(e)}


def restart_scanner(reason: str) -> dict:
    """
    Signal the scanner to restart by writing a restart flag to scanner_control.
    The scanner loop should check for this flag at the start of each cycle.
    """
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO scanner_control (key, value, updated_at)
                    VALUES ('restart_requested', %s, NOW())
                    ON CONFLICT (key) DO UPDATE
                    SET value = EXCLUDED.value, updated_at = NOW()
                """, (json.dumps({"reason": reason, "requested_at": datetime.utcnow().isoformat()}),))
            conn.commit()
        return {
            "success": True,
            "message": "Restart flag set. Scanner will restart at the start of its next cycle.",
            "reason": reason
        }
    except Exception as e:
        logger.error(f"restart_scanner error: {e}")
        return {"error": str(e)}
