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


# ── NEW TOOLS ────────────────────────────────────────────────────────────────

def get_conviction_list(session: str = "", limit: int = 5) -> dict:
    """
    Fetch today's conviction buy list from conviction_buys table.
    Returns ranked picks with entry, stop, targets, hold type, and reasoning.
    Falls back to yesterday if today has no entries.
    """
    try:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                today = datetime.now(timezone.utc).date().isoformat()
                yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()

                where_session = "AND session = %s" if session else ""
                params_today = [today, session] if session else [today]
                params_yest  = [yesterday, session] if session else [yesterday]

                cur.execute(f"""
                    SELECT rank, ticker, conviction, hold_type, entry, stop_loss,
                           target_1, target_2, session, date, reasoning,
                           ai_key_reason, ai_conviction, ai_catalyst_quality,
                           ai_risk, ai_time_sensitivity, created_at
                    FROM conviction_buys
                    WHERE date = %s {where_session}
                    ORDER BY rank ASC
                    LIMIT %s
                """, params_today + [limit])
                rows = cur.fetchall()
                is_yesterday = False

                if not rows:
                    cur.execute(f"""
                        SELECT rank, ticker, conviction, hold_type, entry, stop_loss,
                               target_1, target_2, session, date, reasoning,
                               ai_key_reason, ai_conviction, ai_catalyst_quality,
                               ai_risk, ai_time_sensitivity, created_at
                        FROM conviction_buys
                        WHERE date = %s {where_session}
                        ORDER BY rank ASC
                        LIMIT %s
                    """, params_yest + [limit])
                    rows = cur.fetchall()
                    is_yesterday = True

                picks = [dict(r) for r in rows]
                return {
                    "picks": picks,
                    "count": len(picks),
                    "date": today if not is_yesterday else yesterday,
                    "is_yesterday": is_yesterday,
                    "session_filter": session or "all",
                }
    except Exception as e:
        logger.error(f"get_conviction_list error: {e}")
        return {"error": str(e)}


def get_portfolio() -> dict:
    """Get current portfolio holdings with P&L."""
    try:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT ticker, shares, avg_cost, current_price,
                           ROUND((current_price - avg_cost) / avg_cost * 100, 2) AS pnl_pct,
                           ROUND((current_price - avg_cost) * shares, 2) AS pnl_dollars,
                           notes, updated_at
                    FROM portfolio
                    ORDER BY pnl_pct DESC NULLS LAST
                """)
                rows = cur.fetchall()
                holdings = [dict(r) for r in rows]

                total_pnl = sum(float(h.get("pnl_dollars") or 0) for h in holdings)
                return {
                    "holdings": holdings,
                    "count": len(holdings),
                    "total_unrealized_pnl": round(total_pnl, 2),
                }
    except Exception as e:
        logger.error(f"get_portfolio error: {e}")
        return {"error": str(e)}


def get_paper_trades(limit: int = 20, status: str = "all") -> dict:
    """Get paper trading history with P&L. Status: open | closed | all."""
    limit = min(limit, 50)
    try:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                where = ""
                if status == "open":
                    where = "WHERE exit_price IS NULL"
                elif status == "closed":
                    where = "WHERE exit_price IS NOT NULL"

                cur.execute(f"""
                    SELECT ticker, signal_type, entry_price, exit_price,
                           ROUND((exit_price - entry_price) / entry_price * 100, 2) AS return_pct,
                           stop_loss, target_1, target_2,
                           score_at_entry, created_at, closed_at
                    FROM paper_trades
                    {where}
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
                trades = [dict(r) for r in rows]

                # Summary stats for closed trades
                closed = [t for t in trades if t.get("exit_price")]
                wins   = [t for t in closed if (t.get("return_pct") or 0) > 0]
                win_rate = round(len(wins) / len(closed) * 100, 1) if closed else None
                avg_return = round(sum(float(t.get("return_pct") or 0) for t in closed) / len(closed), 2) if closed else None

                return {
                    "trades": trades,
                    "count": len(trades),
                    "closed_count": len(closed),
                    "win_rate_pct": win_rate,
                    "avg_return_pct": avg_return,
                }
    except Exception as e:
        logger.error(f"get_paper_trades error: {e}")
        return {"error": str(e)}


def get_todays_graded_signals() -> dict:
    """
    Show today's signals that have been graded by AccuracyValidator.
    Joins signal_log with signal_outcomes to show which picks were right/wrong.
    """
    try:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Today's signals with any resolved outcome
                cur.execute("""
                    SELECT
                        sl.ticker,
                        sl.signal_label,
                        sl.score,
                        sl.price_at_signal,
                        so.ret_1d,
                        so.ret_3d,
                        so.ret_5d,
                        so.ret_10d,
                        so.outcome_1d,
                        so.outcome_5d,
                        sl.created_at
                    FROM signal_log sl
                    LEFT JOIN signal_outcomes so ON so.signal_id = sl.id
                    WHERE DATE(sl.created_at AT TIME ZONE 'America/New_York') = CURRENT_DATE AT TIME ZONE 'America/New_York'
                    ORDER BY sl.score DESC
                    LIMIT 50
                """)
                today_rows = cur.fetchall()

                # All-time summary for context
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE so.ret_5d IS NOT NULL) AS graded_5d,
                        COUNT(*) FILTER (WHERE so.ret_5d > 0)          AS wins_5d,
                        COUNT(*) FILTER (WHERE so.ret_1d IS NOT NULL)  AS graded_1d,
                        COUNT(*) FILTER (WHERE so.ret_1d > 0)          AS wins_1d,
                        ROUND(AVG(so.ret_5d) * 100, 2)                 AS avg_ret_5d_pct,
                        COUNT(sl.id)                                   AS total_signals
                    FROM signal_log sl
                    LEFT JOIN signal_outcomes so ON so.signal_id = sl.id
                """)
                summary = dict(cur.fetchone())

                graded_today = [dict(r) for r in today_rows if r.get("ret_1d") or r.get("ret_5d")]
                ungraded_today = [dict(r) for r in today_rows if not r.get("ret_1d") and not r.get("ret_5d")]

                return {
                    "today_total": len(today_rows),
                    "today_graded": graded_today,
                    "today_ungraded_count": len(ungraded_today),
                    "all_time_summary": summary,
                }
    except Exception as e:
        logger.error(f"get_todays_graded_signals error: {e}")
        return {"error": str(e)}


def get_regime() -> dict:
    """Get current market regime (TRENDING_UP, TRENDING_DOWN, MEAN_REVERSION, HIGH_VOL, NEUTRAL)."""
    try:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT regime, iwm_price, iwm_ma20, iwm_ma50, adx_14,
                           volatility_20d, computed_at
                    FROM regime_log
                    ORDER BY computed_at DESC
                    LIMIT 1
                """)
                row = cur.fetchone()
                if not row:
                    # Fall back to scanner_control state
                    cur.execute("SELECT current_regime FROM scanner_control WHERE id = 1")
                    ctrl = cur.fetchone()
                    return {"regime": ctrl["current_regime"] if ctrl else "UNKNOWN",
                            "source": "scanner_control"}
                return dict(row)
    except Exception as e:
        logger.error(f"get_regime error: {e}")
        return {"error": str(e)}


def pause_scanner() -> dict:
    """Pause the scanner loop (it will sleep without scanning until resumed)."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE scanner_control SET paused = TRUE, updated_at = NOW() WHERE id = 1")
            conn.commit()
        return {"success": True, "message": "Scanner paused. Send resume_scanner to restart scanning."}
    except Exception as e:
        logger.error(f"pause_scanner error: {e}")
        return {"error": str(e)}


def resume_scanner() -> dict:
    """Resume a paused scanner loop."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE scanner_control SET paused = FALSE, updated_at = NOW() WHERE id = 1")
            conn.commit()
        return {"success": True, "message": "Scanner resumed. It will begin scanning on its next cycle."}
    except Exception as e:
        logger.error(f"resume_scanner error: {e}")
        return {"error": str(e)}


def force_scan() -> dict:
    """Trigger an immediate scan cycle (scanner picks this up within 60 seconds)."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE scanner_control SET force_scan = TRUE, updated_at = NOW() WHERE id = 1")
            conn.commit()
        return {"success": True, "message": "Force scan flag set. Scanner will run an immediate cycle within ~60s."}
    except Exception as e:
        logger.error(f"force_scan error: {e}")
        return {"error": str(e)}


def trigger_conviction_scan(session: str = "market") -> dict:
    """
    Run the conviction engine right now and return the top picks.
    Valid sessions: preopen | market | close | afterhours.
    REQUIRES USER CONFIRMATION.
    """
    valid = {"preopen", "market", "close", "afterhours", "market_open"}
    if session not in valid:
        return {"error": f"Invalid session. Choose from: {valid}"}
    try:
        # Import from the main service codebase (same Railway env or local)
        import sys, os as _os
        _base = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
        if _base not in sys.path:
            sys.path.insert(0, _base)
        from conviction_engine import generate_live_conviction_list
        results = generate_live_conviction_list(session=session)
        picks = [
            {
                "rank": r.get("rank"),
                "ticker": r.get("ticker"),
                "score": r.get("score") or r.get("conviction"),
                "hold": r.get("hold_type"),
                "entry": r.get("entry"),
                "stop": r.get("stop_loss"),
                "reason": (r.get("ai_key_reason") or r.get("reasoning", ""))[:120],
            }
            for r in results
        ]
        return {
            "success": True,
            "session": session,
            "picks": picks,
            "count": len(picks),
        }
    except Exception as e:
        logger.error(f"trigger_conviction_scan error: {e}")
        return {"error": str(e)}


def add_to_portfolio(ticker: str, shares: float, avg_cost: float, notes: str = "") -> dict:
    """
    Add or update a holding in the portfolio table.
    REQUIRES USER CONFIRMATION.
    """
    ticker = ticker.upper().strip()
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO portfolio (ticker, shares, avg_cost, notes, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (ticker) DO UPDATE
                      SET shares = EXCLUDED.shares,
                          avg_cost = EXCLUDED.avg_cost,
                          notes = EXCLUDED.notes,
                          updated_at = NOW()
                """, (ticker, shares, avg_cost, notes))
            conn.commit()
        return {
            "success": True,
            "ticker": ticker,
            "shares": shares,
            "avg_cost": avg_cost,
        }
    except Exception as e:
        logger.error(f"add_to_portfolio error: {e}")
        return {"error": str(e)}
