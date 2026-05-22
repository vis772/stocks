#!/usr/bin/env python3
"""
scripts/reset_accuracy.py
─────────────────────────
Hard-reset the accuracy testing pipeline.

Clears:
  signal_log        — all historical signals (CASCADE wipes signal_outcomes via FK)
  signal_outcomes   — explicit clear as safety net
  accuracy_reports  — lets 150 / 350 / 600 checkpoints re-fire from scratch
  accuracy_metrics  — per-bucket win-rate cache
  validator_health  — log noise

Safe to run any time. Does NOT touch:
  scan_results, portfolio, watchlist, alert_log, conviction_buys, stock_universe,
  data_quality, quote_cache, scanner_logs, changelog

Usage (on EC2):
  cd /home/ubuntu/axiom
  python3 scripts/reset_accuracy.py
"""

import os
import sys

# ─── Load .env ────────────────────────────────────────────────────────────────

_env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(_env_path):
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

# ─── Connect ──────────────────────────────────────────────────────────────────

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed — run: pip install psycopg2-binary")
    sys.exit(1)

db_url = (
    os.environ.get("DATABASE_URL") or
    os.environ.get("SUPABASE_URL") or
    ""
)
if not db_url:
    print("ERROR: DATABASE_URL or SUPABASE_URL not set in environment / .env")
    sys.exit(1)

try:
    conn = psycopg2.connect(db_url, connect_timeout=15)
    conn.autocommit = False
    cur  = conn.cursor()
except Exception as e:
    print(f"ERROR: could not connect to database: {e}")
    sys.exit(1)

# ─── Pre-reset counts ─────────────────────────────────────────────────────────

def count(table: str) -> int:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0] or 0
    except Exception:
        return -1

print("\nAxiom Terminal — Accuracy Reset")
print("=" * 40)
print("Before:")
for t in ("signal_log", "signal_outcomes", "accuracy_reports", "accuracy_metrics", "validator_health"):
    print(f"  {t:<22} {count(t):>6} rows")

# ─── Confirm ──────────────────────────────────────────────────────────────────

print()
answer = input("Proceed with reset? This is irreversible. (yes/no): ").strip().lower()
if answer != "yes":
    print("Aborted.")
    cur.close(); conn.close()
    sys.exit(0)

# ─── Truncate ─────────────────────────────────────────────────────────────────

TABLES = [
    ("signal_outcomes",  "TRUNCATE signal_outcomes"),
    ("signal_log",       "TRUNCATE signal_log CASCADE"),    # CASCADE handles signal_outcomes FK
    ("accuracy_reports", "TRUNCATE accuracy_reports"),
    ("accuracy_metrics", "TRUNCATE accuracy_metrics"),
    ("validator_health", "TRUNCATE validator_health"),
]

errors = []
for name, sql in TABLES:
    try:
        cur.execute(sql)
        print(f"  TRUNCATE {name} ... OK")
    except Exception as e:
        print(f"  TRUNCATE {name} ... FAILED: {e}")
        errors.append((name, str(e)))

if errors:
    print(f"\n{len(errors)} error(s) — rolling back.")
    conn.rollback()
    cur.close(); conn.close()
    sys.exit(1)

conn.commit()
cur.close()

# ─── Post-reset counts ────────────────────────────────────────────────────────

# Re-open for verification
conn2 = psycopg2.connect(db_url, connect_timeout=15)
cur2  = conn2.cursor()

def count2(table: str) -> int:
    try:
        cur2.execute(f"SELECT COUNT(*) FROM {table}")
        return cur2.fetchone()[0] or 0
    except Exception:
        return -1

print("\nAfter:")
for t in ("signal_log", "signal_outcomes", "accuracy_reports", "accuracy_metrics", "validator_health"):
    print(f"  {t:<22} {count2(t):>6} rows")

cur2.close(); conn2.close()

print()
print("Reset complete.")
print("Next checkpoints will fire at 150 / 350 / 600 total signals.")
print("New signals will be generated under v3 scoring weights (post-audit).")
