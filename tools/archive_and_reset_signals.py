#!/usr/bin/env python3
"""
Archive signal_log and signal_outcomes into *_v1 tables, then truncate
signal_log, signal_outcomes, and conviction_buys to start fresh.

Run from repo root with Railway env vars injected:
    railway run python3 tools/archive_and_reset_signals.py

Safety checks:
  - Aborts if *_v1 archive tables already exist (prevents double-archiving).
  - Prints row counts before and after so you can verify.
  - Truncates signal_outcomes before signal_log (FK order).
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import _is_postgres, _get_pg_conn, _put_pg_conn, _get_sqlite_conn


def run() -> None:
    if not _is_postgres():
        print("ERROR: This script targets Postgres only. SQLite path not implemented.")
        sys.exit(1)

    conn = _get_pg_conn()
    cur  = conn.cursor()

    # ── 1. Safety check: abort if archives already exist ────────────────────
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN ('signal_log_v1', 'signal_outcomes_v1')
    """)
    existing = cur.fetchone()[0]
    if existing > 0:
        print("ABORT: signal_log_v1 or signal_outcomes_v1 already exists.")
        print("       Drop them manually first if you really want to re-archive.")
        cur.close(); _put_pg_conn(conn)
        sys.exit(1)

    # ── 2. Print pre-truncate row counts ────────────────────────────────────
    for tbl in ("signal_log", "signal_outcomes", "conviction_buys"):
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        print(f"  {tbl}: {cur.fetchone()[0]:,} rows before archive")

    # ── 3. Create archive copies ─────────────────────────────────────────────
    print("\nCreating signal_log_v1 ...")
    cur.execute("CREATE TABLE signal_log_v1 AS SELECT * FROM signal_log")
    cur.execute("SELECT COUNT(*) FROM signal_log_v1")
    print(f"  signal_log_v1: {cur.fetchone()[0]:,} rows archived")

    print("Creating signal_outcomes_v1 ...")
    cur.execute("CREATE TABLE signal_outcomes_v1 AS SELECT * FROM signal_outcomes")
    cur.execute("SELECT COUNT(*) FROM signal_outcomes_v1")
    print(f"  signal_outcomes_v1: {cur.fetchone()[0]:,} rows archived")

    conn.commit()
    print("Archives committed.\n")

    # ── 4. Truncate all three in one statement (Postgres requires this when
    #        FKs exist between tables, even if child is listed first) ─────────
    print("Truncating signal_outcomes, conviction_buys, signal_log ...")
    cur.execute(
        "TRUNCATE TABLE signal_outcomes, conviction_buys, signal_log RESTART IDENTITY"
    )

    conn.commit()

    # ── 5. Verify empty ──────────────────────────────────────────────────────
    for tbl in ("signal_log", "signal_outcomes", "conviction_buys"):
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        print(f"  {tbl}: {cur.fetchone()[0]} rows after truncate")

    cur.close(); _put_pg_conn(conn)
    print("\nDone. Old data is in signal_log_v1 and signal_outcomes_v1.")


if __name__ == "__main__":
    run()
