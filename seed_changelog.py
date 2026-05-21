"""
seed_changelog.py
Run once to populate the changelog table with all historical changes.
Safe to re-run — uses INSERT OR IGNORE / ON CONFLICT DO NOTHING.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.database import seed_changelog_entry

ENTRIES = [
    # ── 2026-05-21 ────────────────────────────────────────────────────────────
    dict(
        change_date  = "2026-05-21 20:00:00",
        category     = "scoring",
        title        = "Unified scoring path — catalyst removed, signal floor raised to 70",
        description  = (
            "Merged two-path scoring (dynamic/static) into one formula: "
            "tech*0.43 + fund*0.30 + risk_inv*0.20 + sent*0.07. "
            "Catalyst factor dropped (zero predictive value per checkpoint data) — "
            "its 3% weight folded into technical. MIN_SIGNAL_SCORE raised from 45→70, "
            "eliminating Watchlist/Hold noise from signals and alerts."
        ),
        files        = "config.py, core/scanner.py",
        commit_hash  = "529c605",
        impact       = "high",
    ),
    dict(
        change_date  = "2026-05-21 20:00:00",
        category     = "scanner",
        title        = "Three hard signal gates: time, volume, regime",
        description  = (
            "Gate 1 (time): first/last 15 min of session suppressed — eliminates open/close noise. "
            "Gate 2 (volume): current volume must be ≥1.5× 3-month avg — confirms real institutional interest. "
            "Gate 3 (regime): score≥75 signals suppressed in MEAN_REVERSION regime — "
            "momentum breakouts don't work when market is reverting."
        ),
        files        = "scanner_loop.py, config.py",
        commit_hash  = "a6cf098",
        impact       = "high",
    ),
    dict(
        change_date  = "2026-05-21 20:00:00",
        category     = "universe",
        title        = "Universe quality filters tightened — target ~150 liquid names",
        description  = (
            "min_market_cap: $20M → $100M. "
            "min_adv: 500K → 1M shares/day. "
            "min_price: $0.50 → $2.00 (eliminates penny/OTC noise). "
            "max_price: $500 → $300. "
            "Universe shrinks from ~440 to ~150 high-quality names for cleaner signals."
        ),
        files        = "universe_manager.py",
        commit_hash  = "a6cf098",
        impact       = "high",
    ),
    dict(
        change_date  = "2026-05-21 20:00:00",
        category     = "validator",
        title        = "Accuracy validator fixed — 501 pending grades, 3 bugs patched",
        description  = (
            "Bug 1: GAP_CONTINUATION signals excluded from _LABELS filter — never got graded. Fixed. "
            "Bug 2: yfinance fetch had no retry — one timeout = permanent skip. "
            "Now retries 3× with backoff. "
            "Bug 3: Grader only ran at 10 PM ET — if service restarted, missed the window. "
            "Now also runs every 4 hours throughout the day. "
            "Added force_grade_all_pending() for manual catch-up."
        ),
        files        = "accuracy_validator.py, scanner_loop.py",
        commit_hash  = "a6cf098",
        impact       = "high",
    ),
    dict(
        change_date  = "2026-05-21 18:00:00",
        category     = "dashboard",
        title        = "Dashboard tab redesigned as live terminal UI",
        description  = (
            "New AXIOM TERMINAL layout: scrolling ticker tape, LIVE WATCHLIST panel "
            "(today's signals with SYMBOL/SCORE/PRICE/CHANGE/SIGNAL columns), "
            "REGIME DETECTOR panel (regime badge + VIX/ADV/Breadth + factor weight bars), "
            "FACTOR IC TRACKER (20-day rolling IC bar chart), "
            "CONVICTION PANEL (this-week picks). "
            "Auto-refreshes every 15s. Control strip: PAUSE/RESUME + SCAN NOW."
        ),
        files        = "app.py",
        commit_hash  = "5bd1395",
        impact       = "medium",
    ),
    dict(
        change_date  = "2026-05-21 16:00:00",
        category     = "database",
        title        = "Changelog table added — all system changes now tracked",
        description  = (
            "New `changelog` table in Supabase PostgreSQL. "
            "Columns: id, change_date, category, title, description, files, commit_hash, impact. "
            "Functions: log_change(), get_changelog(), seed_changelog_entry(). "
            "Enables querying full change history via bot or dashboard."
        ),
        files        = "db/database.py",
        commit_hash  = "current",
        impact       = "medium",
    ),
    # ── 2026-05-21 earlier (merge + prior features) ───────────────────────────
    dict(
        change_date  = "2026-05-21 14:00:00",
        category     = "infrastructure",
        title        = "Merged FCF branch into clean-combined-version",
        description  = (
            "Merged claude/fix-scanner-universe-fcf-R9nxW into clean-combined-version. "
            "Includes: deadlock fix, universe stability, volume fix, health monitor, Paper Trading tab, Health tab."
        ),
        files        = "app.py, db/database.py, scanner_loop.py, universe_manager.py, "
                       "resilient_fetcher.py, health/monitor.py, health/__init__.py",
        commit_hash  = "983fd6b",
        impact       = "high",
    ),
    dict(
        change_date  = "2026-05-21 12:00:00",
        category     = "health",
        title        = "Self-healing health monitor added — 9 subsystem checks",
        description  = (
            "health/monitor.py: HealthMonitor class with checks for database, Finnhub, Tiingo, "
            "yfinance, scanner loop, universe size, conviction engine, accuracy validator, Telegram bot. "
            "Auto-recovery: degraded mode for Finnhub failures, universe thin fallback, scanner stall detection. "
            "Pushover alerting with 1h cooldowns. Anomaly detection (score=100 bad data, dedup rate). "
            "Health tab added to Streamlit dashboard with status grid, data quality table, events log."
        ),
        files        = "health/monitor.py, health/__init__.py, app.py, scanner_loop.py",
        commit_hash  = "c00fefb",
        impact       = "high",
    ),
    dict(
        change_date  = "2026-05-21 10:00:00",
        category     = "universe",
        title        = "Universe stability fix — was dropping from 120 to 5 tickers between days",
        description  = (
            "Root cause: upsert was overwriting valid market_cap/avg_volume with 0 during partial refresh. "
            "Fix 1: CASE WHEN EXCLUDED.field > 0 guard in bulk_upsert_universe_stocks(). "
            "Fix 2: Two-tier query in get_active_universe() — falls back to market-cap-only if ADV filter returns <100. "
            "Fix 3: refresh cadence 1 day → 5 days (was refreshing daily, overwriting good data). "
            "Fix 4: Threshold 500 → 200 for bootstrap trigger."
        ),
        files        = "db/database.py, universe_manager.py",
        commit_hash  = "2f4c046",
        impact       = "high",
    ),
    dict(
        change_date  = "2026-05-21 10:00:00",
        category     = "scanner",
        title        = "Volume showing 0 in morning reports — fixed cascade",
        description  = (
            "yfinance hist['Volume'].iloc[-1] is previous day's close in daily OHLCV, not today. "
            "Fix: cascade through regularMarketVolume → volume → hist as fallback. "
            "resilient_fetcher.py: use last_volume (today) before three_month_average_volume (90-day avg)."
        ),
        files        = "data/market_data.py, resilient_fetcher.py",
        commit_hash  = "2f4c046",
        impact       = "medium",
    ),
    dict(
        change_date  = "2026-05-20 20:00:00",
        category     = "infrastructure",
        title        = "Deadlock fix — concurrent Railway service startup",
        description  = (
            "Root cause: Streamlit + scanner_loop both calling initialize_db() simultaneously. "
            "DDL with FK relationships caused circular lock waits. "
            "Fix: switched pg_try_advisory_xact_lock (non-blocking, returned false) → "
            "pg_advisory_xact_lock (blocking, serializes both). "
            "Added SAVEPOINT guards to all ALTER TABLE loops so individual failures don't abort migration. "
            "Fixed conn.close() → _put_pg_conn(conn) to return connections to pool."
        ),
        files        = "db/database.py",
        commit_hash  = "9624741",
        impact       = "high",
    ),
]

if __name__ == "__main__":
    print(f"Seeding {len(ENTRIES)} changelog entries...")
    for e in ENTRIES:
        seed_changelog_entry(**e)
        print(f"  ✓ {e['change_date'][:10]} [{e['category']}] {e['title'][:60]}")
    print("Done.")
