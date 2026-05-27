-- Migration: drop portfolio tracking + dashboard auth tables
-- Run ONCE after deploying the refactored stack.
-- Safe to run multiple times (IF EXISTS guards).
--
-- Tables being removed:
--   portfolio      — per-user position tracking (Streamlit portfolio tab)
--   users          — dashboard login accounts
--   user_sessions  — dashboard JWT sessions
--
-- Tables NOT touched (scanner accuracy pipeline):
--   signal_log, signal_outcomes, accuracy_reports, accuracy_metrics,
--   conviction_buys, stock_universe, watchlist, scanner_control,
--   scanner_state, scanner_logs, alert_log, regime_log, factor_scores

BEGIN;

DROP TABLE IF EXISTS portfolio      CASCADE;
DROP TABLE IF EXISTS user_sessions  CASCADE;
DROP TABLE IF EXISTS users          CASCADE;

COMMIT;
