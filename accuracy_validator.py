# accuracy_validator.py
# Nightly signal accuracy grader. Evaluates 1d, 3d, 5d outcomes per signal.
# Runs at 10 PM ET. Updates the existing signal_outcomes pipeline.

import numpy as np
from datetime import datetime
from typing import Optional


WIN_THRESHOLD  =  0.02   # >+2% = win
LOSS_THRESHOLD = -0.02   # <-2% = loss

SCORE_BUCKETS = [
    (65, 70, "65-70"),
    (70, 75, "70-75"),
    (75, 80, "75-80"),
    (80, 85, "80-85"),
    (85, 101, "85+"),
]


def get_close_n_trading_days_after(ticker: str, entry_date, n: int):
    """Return close price n trading days after entry_date, or None if unavailable."""
    import yfinance as yf
    from datetime import timedelta

    if isinstance(entry_date, str):
        entry_date = datetime.fromisoformat(entry_date.replace("Z", "")).date()
    elif hasattr(entry_date, "date"):
        entry_date = entry_date.date()

    start = entry_date
    end   = entry_date + timedelta(days=n * 3 + 7)  # buffer for weekends/holidays

    try:
        df = yf.download(ticker, start=str(start), end=str(end),
                         progress=False, auto_adjust=True)
    except Exception:
        return None

    if df is None or df.empty:
        return None

    close_col = df["Close"]
    if hasattr(close_col, "squeeze"):
        close_col = close_col.squeeze()

    closes = close_col.dropna()
    idx_dates = [d.date() if hasattr(d, "date") else d for d in closes.index]
    after = [(d, float(c)) for d, c in zip(idx_dates, closes) if d > entry_date]

    if len(after) < n:
        return None
    return after[n - 1][1]


def _fetch_closes_after(ticker: str, entry_date) -> dict:
    """
    Single yf.download() per signal. Returns {1: close, 3: close, 5: close}
    for whichever horizons have data. entry_date is excluded (strictly after).
    """
    import yfinance as yf
    from datetime import timedelta

    if isinstance(entry_date, str):
        entry_date = datetime.fromisoformat(entry_date.replace("Z", "")).date()
    elif hasattr(entry_date, "date"):
        entry_date = entry_date.date()

    end = entry_date + timedelta(days=30)  # enough for 10 trading days + holidays

    try:
        df = yf.download(ticker, start=str(entry_date), end=str(end),
                         progress=False, auto_adjust=True)
    except Exception:
        return {}

    if df is None or df.empty:
        return {}

    close_col = df["Close"]
    if isinstance(close_col, type(df)):   # MultiIndex → DataFrame, take first col
        close_col = close_col.iloc[:, 0]
    closes = close_col.dropna()

    idx_dates = [d.date() if hasattr(d, "date") else d for d in closes.index]
    after = [float(c) for d, c in zip(idx_dates, closes) if d > entry_date]

    return {n: after[n - 1] for n in [1, 3, 5, 10] if len(after) >= n}


def _safe(v, fallback=0.0):
    try:
        f = float(v)
        return fallback if (f != f) else f
    except Exception:
        return fallback


def _bucket(score: float) -> str:
    for lo, hi, label in SCORE_BUCKETS:
        if lo <= score < hi:
            return label
    return "other"


class AccuracyValidator:
    """
    Evaluates signal accuracy by checking 1d, 3d, 5d outcomes after each signal.
    Works with existing signal_log + signal_outcomes tables.
    """

    def grade_signals(self) -> int:
        """
        Grade signals horizon-by-horizon as soon as data is available.

        Pass 1 — outcome_1d IS NULL, signal >= 1 day old:
            Write outcome_1d now; also write 3d/5d if already available.
        Pass 2 — outcome_1d set but outcome_5d still NULL, signal >= 3 days old:
            Fill in remaining 3d/5d outcomes as data becomes available.

        _write_signal_log_outcomes() uses COALESCE so passing None never
        overwrites a previously graded outcome.
        """
        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn

        _LABELS = (
            "'Strong Buy Candidate','Speculative Buy','Watchlist',"
            "'Hold','Trim','Sell','Avoid','Gap-Up'"
        )
        _BASE = (
            "price_at_signal IS NOT NULL AND price_at_signal > 0 "
            f"AND signal_label IN ({_LABELS})"
        )

        graded  = 0
        failed  = 0
        skipped = 0

        def _query(pg_sql, sq_sql):
            try:
                if _is_postgres():
                    conn = _get_pg_conn(); cur = conn.cursor()
                    cur.execute(pg_sql); rows = cur.fetchall()
                    cur.close(); conn.close()
                else:
                    conn = _get_sqlite_conn(); cur = conn.cursor()
                    cur.execute(sq_sql); rows = cur.fetchall()
                    conn.close()
                return rows
            except Exception as e:
                print(f"  [accuracy] query failed: {e}")
                return []

        def _process(rows, label, has_10d_col=False):
            nonlocal graded, failed, skipped
            total = len(rows)
            print(f"  [accuracy] {label}: {total} signals")
            for i, row in enumerate(rows, 1):
                if has_10d_col:
                    sig_id, ticker, entry_price, created_at, o1, o3, o5, o10 = row
                else:
                    sig_id, ticker, entry_price, created_at, o1, o3, o5 = row
                    o10 = None
                try:
                    closes = _fetch_closes_after(ticker, created_at)
                    if not closes:
                        skipped += 1
                        continue

                    def _ret(n, existing):
                        if existing is not None or n not in closes:
                            return None
                        return round((closes[n] - entry_price) / entry_price * 100, 3)

                    ret_1d  = _ret(1,  o1)
                    ret_3d  = _ret(3,  o3)
                    ret_5d  = _ret(5,  o5)
                    ret_10d = _ret(10, o10)

                    if ret_1d is None and ret_3d is None and ret_5d is None and ret_10d is None:
                        skipped += 1
                        continue

                    _write_signal_log_outcomes(sig_id, ret_1d, ret_3d, ret_5d)

                    # Keep signal_outcomes populated (includes 10d)
                    so_upd = {}
                    for col, ret in [("outcome_1d", ret_1d), ("outcome_3d", ret_3d),
                                     ("outcome_5d", ret_5d), ("outcome_10d", ret_10d)]:
                        if ret is not None:
                            so_upd[col] = ("win"  if ret >  WIN_THRESHOLD  * 100 else
                                           "loss" if ret <  LOSS_THRESHOLD * 100 else "neutral")
                            so_upd[f"ret_{col[8:]}"] = ret
                    if so_upd:
                        _write_graded_outcome(sig_id, so_upd)

                    graded += 1
                except Exception as _e:
                    failed += 1
                    print(f"  [accuracy] {ticker} id={sig_id} error: {_e}")

                if i % 10 == 0:
                    print(f"  [AccuracyValidator] Graded {i}/{total}...")

        # Pass 1: signals where outcome_1d hasn't been touched yet (>= 1 day old)
        _process(
            _query(
                f"SELECT id,ticker,price_at_signal,created_at,outcome_1d,outcome_3d,outcome_5d "
                f"FROM signal_log WHERE outcome_1d IS NULL AND {_BASE} "
                f"AND created_at <= NOW() - INTERVAL '1 day' ORDER BY created_at ASC LIMIT 300",
                f"SELECT id,ticker,price_at_signal,created_at,outcome_1d,outcome_3d,outcome_5d "
                f"FROM signal_log WHERE outcome_1d IS NULL AND {_BASE} "
                f"AND created_at <= datetime('now','-1 day') ORDER BY created_at ASC LIMIT 300",
            ),
            "Pass 1 (outcome_1d IS NULL, >=1d old)"
        )

        # Pass 2: partially-graded signals — outcome_1d set but outcome_5d still NULL (>= 3 days old)
        _process(
            _query(
                f"SELECT id,ticker,price_at_signal,created_at,outcome_1d,outcome_3d,outcome_5d "
                f"FROM signal_log WHERE outcome_1d IS NOT NULL AND outcome_5d IS NULL AND {_BASE} "
                f"AND created_at <= NOW() - INTERVAL '3 days' ORDER BY created_at ASC LIMIT 300",
                f"SELECT id,ticker,price_at_signal,created_at,outcome_1d,outcome_3d,outcome_5d "
                f"FROM signal_log WHERE outcome_1d IS NOT NULL AND outcome_5d IS NULL AND {_BASE} "
                f"AND created_at <= datetime('now','-3 days') ORDER BY created_at ASC LIMIT 300",
            ),
            "Pass 2 (outcome_5d IS NULL, >=3d old)"
        )

        # Pass 3: grade 10-day outcomes via signal_outcomes (outcome_5d set, >=8d old)
        def _pass3_rows():
            try:
                if _is_postgres():
                    conn = _get_pg_conn(); cur = conn.cursor()
                    cur.execute(f"""
                        SELECT sl.id, sl.ticker, sl.price_at_signal, sl.created_at,
                               sl.outcome_1d, sl.outcome_3d, sl.outcome_5d, so.outcome_10d
                        FROM signal_log sl
                        LEFT JOIN signal_outcomes so ON so.signal_id = sl.id
                        WHERE sl.outcome_5d IS NOT NULL AND {_BASE}
                          AND (so.outcome_10d IS NULL OR so.ret_10d IS NULL)
                          AND sl.created_at <= NOW() - INTERVAL '8 days'
                        ORDER BY sl.created_at ASC LIMIT 200
                    """)
                    rows = cur.fetchall(); cur.close(); conn.close()
                else:
                    conn = _get_sqlite_conn(); cur = conn.cursor()
                    cur.execute(f"""
                        SELECT sl.id, sl.ticker, sl.price_at_signal, sl.created_at,
                               sl.outcome_1d, sl.outcome_3d, sl.outcome_5d, so.outcome_10d
                        FROM signal_log sl
                        LEFT JOIN signal_outcomes so ON so.signal_id = sl.id
                        WHERE sl.outcome_5d IS NOT NULL AND {_BASE}
                          AND (so.outcome_10d IS NULL OR so.ret_10d IS NULL)
                          AND sl.created_at <= datetime('now', '-8 days')
                        ORDER BY sl.created_at ASC LIMIT 200
                    """)
                    rows = cur.fetchall(); conn.close()
                return rows
            except Exception as _p3e:
                print(f"  [accuracy] Pass 3 query failed: {_p3e}")
                return []

        _process(_pass3_rows(), "Pass 3 (ret_10d, >=8d old)", has_10d_col=True)

        summary = f"[AccuracyValidator] Done. Graded: {graded}, Failed: {failed}, Skipped: {skipped}"
        print(summary)

        if graded > 0:
            try:
                from alerts import send_alert, PRIORITY_NORMAL
                send_alert(title="Accuracy Grader", message=summary, priority=PRIORITY_NORMAL)
            except Exception:
                pass

        return graded

    def compute_metrics(self) -> dict:
        """
        Returns win-rate, profit factor, EV, Sharpe, max-drawdown, Calmar, t-stat, p-value
        aggregated overall and by score bucket.
        """
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT sl.score, so.outcome_5d, so.ret_5d, so.ret_10d, so.outcome_10d
                    FROM signal_log sl
                    JOIN signal_outcomes so ON so.signal_id = sl.id
                    WHERE so.outcome_5d IS NOT NULL AND so.ret_5d IS NOT NULL
                      AND sl.created_at >= NOW() - INTERVAL '90 days'
                """)
                rows = cur.fetchall(); cur.close(); conn.close()
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT sl.score, so.outcome_5d, so.ret_5d, so.ret_10d, so.outcome_10d
                    FROM signal_log sl
                    JOIN signal_outcomes so ON so.signal_id = sl.id
                    WHERE so.outcome_5d IS NOT NULL AND so.ret_5d IS NOT NULL
                      AND sl.created_at >= datetime('now', '-90 days')
                """)
                rows = cur.fetchall(); conn.close()

            if not rows:
                return {"n": 0, "insufficient": True}

            all_rets    = []
            all_rets_10 = []
            bucket_data: dict = {label: {"rets": [], "rets_10": [], "outcomes": []} for _, _, label in SCORE_BUCKETS}
            bucket_data["other"] = {"rets": [], "rets_10": [], "outcomes": []}

            for score, outcome, ret, ret10, outcome10 in rows:
                ret_f   = _safe(ret)
                score_f = _safe(score)
                all_rets.append(ret_f)
                b = _bucket(score_f)
                bucket_data[b]["rets"].append(ret_f)
                bucket_data[b]["outcomes"].append(outcome)
                if ret10 is not None:
                    r10 = _safe(ret10)
                    all_rets_10.append(r10)
                    bucket_data[b]["rets_10"].append(r10)

            overall    = _metrics_from_rets(all_rets)
            overall_10 = _metrics_from_rets(all_rets_10) if all_rets_10 else {}

            by_bucket = {}
            for label, bd in bucket_data.items():
                if bd["rets"]:
                    m = _metrics_from_rets(bd["rets"])
                    if bd["rets_10"]:
                        m["metrics_10d"] = _metrics_from_rets(bd["rets_10"])
                    by_bucket[label] = m

            result = {
                "n":           len(all_rets),
                "overall":     overall,
                "overall_10d": overall_10,
                "by_bucket":   by_bucket,
                "insufficient": len(all_rets) < 30,
            }

            # Upsert per-bucket stats into accuracy_metrics
            self._upsert_accuracy_metrics(by_bucket)

            return result
        except Exception as e:
            print(f"  [accuracy] compute_metrics failed: {e}")
            return {"n": 0, "insufficient": True}

    def _upsert_accuracy_metrics(self, by_bucket: dict) -> None:
        """Write per-bucket metrics to accuracy_metrics table."""
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                for bucket, m in by_bucket.items():
                    cur.execute("""
                        INSERT INTO accuracy_metrics
                            (bucket, sample_n, win_rate, profit_factor, ev_per_trade,
                             sharpe, max_drawdown, t_stat, p_value, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
                        ON CONFLICT (bucket) DO UPDATE SET
                            sample_n      = EXCLUDED.sample_n,
                            win_rate      = EXCLUDED.win_rate,
                            profit_factor = EXCLUDED.profit_factor,
                            ev_per_trade  = EXCLUDED.ev_per_trade,
                            sharpe        = EXCLUDED.sharpe,
                            max_drawdown  = EXCLUDED.max_drawdown,
                            t_stat        = EXCLUDED.t_stat,
                            p_value       = EXCLUDED.p_value,
                            updated_at    = NOW()
                    """, (bucket, m.get("n"), m.get("win_rate"), m.get("profit_factor"),
                          m.get("expected_value"), m.get("sharpe"), m.get("max_drawdown"),
                          m.get("t_stat"), m.get("p_value")))
                conn.commit(); cur.close(); conn.close()
            else:
                conn = _get_sqlite_conn()
                for bucket, m in by_bucket.items():
                    conn.execute("""
                        INSERT INTO accuracy_metrics
                            (bucket, sample_n, win_rate, profit_factor, ev_per_trade,
                             sharpe, max_drawdown, t_stat, p_value)
                        VALUES (?,?,?,?,?,?,?,?,?)
                        ON CONFLICT (bucket) DO UPDATE SET
                            sample_n      = excluded.sample_n,
                            win_rate      = excluded.win_rate,
                            profit_factor = excluded.profit_factor,
                            ev_per_trade  = excluded.ev_per_trade,
                            sharpe        = excluded.sharpe,
                            max_drawdown  = excluded.max_drawdown,
                            t_stat        = excluded.t_stat,
                            p_value       = excluded.p_value,
                            updated_at    = datetime('now')
                    """, (bucket, m.get("n"), m.get("win_rate"), m.get("profit_factor"),
                          m.get("expected_value"), m.get("sharpe"), m.get("max_drawdown"),
                          m.get("t_stat"), m.get("p_value")))
                conn.commit(); conn.close()
        except Exception as e:
            print(f"  [accuracy] _upsert_accuracy_metrics failed: {e}")

    def get_bucket_win_rate(self, score: float) -> Optional[float]:
        """Return win rate for the score's bucket, or None if insufficient data."""
        try:
            metrics = self.compute_metrics()
            if metrics.get("insufficient"):
                return None
            b = _bucket(score)
            bm = metrics.get("by_bucket", {}).get(b, {})
            n = bm.get("n", 0)
            if n < 30:
                return None
            return bm.get("win_rate")
        except Exception:
            return None

    def is_bucket_disabled(self, score: float) -> bool:
        """Return True if the accuracy_metrics row has disabled=True OR win_rate < 0.45."""
        b = _bucket(score)
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute("SELECT disabled, win_rate, sample_n FROM accuracy_metrics WHERE bucket = %s", (b,))
                row = cur.fetchone(); cur.close(); conn.close()
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute("SELECT disabled, win_rate, sample_n FROM accuracy_metrics WHERE bucket = ?", (b,))
                row = cur.fetchone(); conn.close()
            if row:
                if bool(row[0]):
                    return True
                n = row[2] or 0
                if n >= 30 and row[1] is not None and row[1] < 0.45:
                    return True
                return False
        except Exception:
            pass
        wr = self.get_bucket_win_rate(score)
        if wr is None:
            return False
        return wr < 0.45

    def self_check(self) -> dict:
        """
        Run a health check on the validator pipeline. Logs result to validator_health table.
        Returns a dict with signals_graded, signals_pending, oldest_pending, error_msg.
        """
        result = {"signals_graded": 0, "signals_pending": 0, "oldest_pending": None, "error_msg": ""}
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM signal_outcomes WHERE outcome_5d IS NOT NULL")
                result["signals_graded"] = cur.fetchone()[0] or 0
                cur.execute("SELECT COUNT(*) FROM signal_outcomes WHERE outcome_5d IS NULL")
                result["signals_pending"] = cur.fetchone()[0] or 0
                cur.execute("""
                    SELECT sl.created_at FROM signal_log sl
                    JOIN signal_outcomes so ON so.signal_id = sl.id
                    WHERE so.outcome_5d IS NULL
                    ORDER BY sl.created_at ASC LIMIT 1
                """)
                row = cur.fetchone()
                result["oldest_pending"] = str(row[0]) if row else None
                cur.close(); conn.close()
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM signal_outcomes WHERE outcome_5d IS NOT NULL")
                result["signals_graded"] = cur.fetchone()[0] or 0
                cur.execute("SELECT COUNT(*) FROM signal_outcomes WHERE outcome_5d IS NULL")
                result["signals_pending"] = cur.fetchone()[0] or 0
                cur.execute("""
                    SELECT sl.created_at FROM signal_log sl
                    JOIN signal_outcomes so ON so.signal_id = sl.id
                    WHERE so.outcome_5d IS NULL
                    ORDER BY sl.created_at ASC LIMIT 1
                """)
                row = cur.fetchone()
                result["oldest_pending"] = str(row[0]) if row else None
                conn.close()
        except Exception as e:
            result["error_msg"] = str(e)[:300]

        # Log to validator_health
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute("""
                    INSERT INTO validator_health (signals_graded, signals_pending, oldest_pending, error_msg)
                    VALUES (%s, %s, %s, %s)
                """, (result["signals_graded"], result["signals_pending"],
                      result["oldest_pending"], result["error_msg"]))
                conn.commit(); cur.close(); conn.close()
            else:
                conn = _get_sqlite_conn()
                conn.execute("""
                    INSERT INTO validator_health (signals_graded, signals_pending, oldest_pending, error_msg)
                    VALUES (?, ?, ?, ?)
                """, (result["signals_graded"], result["signals_pending"],
                      result["oldest_pending"], result["error_msg"]))
                conn.commit(); conn.close()
        except Exception as _hle:
            print(f"  [accuracy] self_check health log failed: {_hle}")

        print(f"  [accuracy] self_check: graded={result['signals_graded']} pending={result['signals_pending']}")
        return result


def _metrics_from_rets(rets: list) -> dict:
    if not rets:
        return {"n": 0}
    arr = np.array(rets)
    n   = len(arr)
    wins   = [r for r in rets if r > WIN_THRESHOLD * 100]
    losses = [r for r in rets if r < LOSS_THRESHOLD * 100]

    win_rate = len(wins) / n if n else 0
    avg_win  = _safe(np.mean(wins)) if wins else 0
    avg_loss = _safe(abs(np.mean(losses))) if losses else 0

    pf = _safe(sum(wins) / abs(sum(losses))) if losses and sum(losses) != 0 else None
    ev = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

    mean_r = _safe(np.mean(arr))
    std_r  = _safe(np.std(arr))
    sharpe = _safe(mean_r / std_r * np.sqrt(252)) if std_r > 0 else 0

    # Max drawdown on cumulative returns
    cumulative = np.cumsum(arr)
    peak = np.maximum.accumulate(cumulative)
    dd   = cumulative - peak
    max_dd = float(np.min(dd)) if len(dd) else 0

    ann_ret = mean_r * 252
    calmar  = _safe(ann_ret / abs(max_dd)) if max_dd < 0 else 0

    t_stat = 0.0
    p_val  = 1.0
    significant = False
    if n >= 5 and std_r > 0:
        try:
            from scipy import stats
            t_stat, p_val = stats.ttest_1samp(arr, 0)
            t_stat  = _safe(float(t_stat))
            p_val   = _safe(float(p_val), 1.0)
            significant = p_val < 0.05 and n >= 30
        except Exception:
            pass

    return {
        "n":           n,
        "win_rate":    round(win_rate, 4),
        "avg_win":     round(avg_win, 4),
        "avg_loss":    round(avg_loss, 4),
        "profit_factor": round(pf, 3) if pf is not None else None,
        "expected_value": round(ev, 4),
        "sharpe":      round(sharpe, 3),
        "max_drawdown": round(max_dd, 4),
        "calmar":      round(calmar, 3),
        "t_stat":      round(t_stat, 3),
        "p_value":     round(p_val, 4),
        "significant": significant,
        "insufficient": n < 30,
    }


def _write_graded_outcome(sig_id: int, updates: dict) -> None:
    """Write outcome_1d, outcome_3d, outcome_5d (and ret_ columns) to signal_outcomes."""
    try:
        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            sets, vals = [], []
            for col, val in updates.items():
                sets.append(f"{col} = %s")
                vals.append(val)
            vals.append(sig_id)
            cur.execute(
                f"UPDATE signal_outcomes SET {', '.join(sets)} WHERE signal_id = %s",
                vals,
            )
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            sets, vals = [], []
            for col, val in updates.items():
                sets.append(f"{col} = ?")
                vals.append(val)
            vals.append(sig_id)
            conn.execute(
                f"UPDATE signal_outcomes SET {', '.join(sets)} WHERE signal_id = ?",
                vals,
            )
            conn.commit(); conn.close()
    except Exception as e:
        print(f"  [accuracy] _write_graded_outcome failed for sig_id={sig_id}: {e}")


def _write_signal_log_outcomes(sig_id: int, ret_1d, ret_3d, ret_5d) -> None:
    """
    Write outcome_1d/3d/5d (% returns) and graded_at to signal_log.
    Uses COALESCE so passing None never overwrites an already-graded outcome.
    """
    try:
        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                UPDATE signal_log
                SET outcome_1d = COALESCE(%s, outcome_1d),
                    outcome_3d = COALESCE(%s, outcome_3d),
                    outcome_5d = COALESCE(%s, outcome_5d),
                    graded_at  = NOW()
                WHERE id = %s
            """, (ret_1d, ret_3d, ret_5d, sig_id))
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute("""
                UPDATE signal_log
                SET outcome_1d = COALESCE(?, outcome_1d),
                    outcome_3d = COALESCE(?, outcome_3d),
                    outcome_5d = COALESCE(?, outcome_5d),
                    graded_at  = datetime('now')
                WHERE id = ?
            """, (ret_1d, ret_3d, ret_5d, sig_id))
            conn.commit(); conn.close()
    except Exception as e:
        print(f"  [accuracy] _write_signal_log_outcomes failed for sig_id={sig_id}: {e}")


def run_nightly_validation():
    """Entry point called by scanner_loop at 10 PM ET."""
    print("\n[ACCURACY] Running nightly signal validation...")
    try:
        v = AccuracyValidator()
        health = v.self_check()
        n = v.grade_signals()
        m = v.compute_metrics()
        overall = m.get("overall", {})
        print(f"  [accuracy] n={m.get('n',0)} | win_rate={overall.get('win_rate','?')} | "
              f"EV={overall.get('expected_value','?')} | significant={overall.get('significant','?')} | "
              f"pending={health.get('signals_pending', '?')}")
        return m
    except Exception as e:
        print(f"  [accuracy] nightly validation failed: {e}")
        return {}
