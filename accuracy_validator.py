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
        Grade all ungraded signals where outcome_1d is available.
        Returns number of signals graded.
        """
        graded = 0
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
            import yfinance as yf

            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT sl.id, sl.ticker, sl.score, sl.price_at_signal, sl.created_at,
                           so.outcome_1d, so.outcome_3d, so.outcome_5d
                    FROM signal_log sl
                    JOIN signal_outcomes so ON so.signal_id = sl.id
                    WHERE so.outcome_5d IS NULL
                      AND sl.created_at <= NOW() - INTERVAL '1 day'
                    ORDER BY sl.created_at ASC
                    LIMIT 200
                """)
                rows = cur.fetchall()
                cur.close(); conn.close()
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT sl.id, sl.ticker, sl.score, sl.price_at_signal, sl.created_at,
                           so.outcome_1d, so.outcome_3d, so.outcome_5d
                    FROM signal_log sl
                    JOIN signal_outcomes so ON so.signal_id = sl.id
                    WHERE so.outcome_5d IS NULL
                      AND sl.created_at <= datetime('now', '-1 day')
                    ORDER BY sl.created_at ASC
                    LIMIT 200
                """)
                rows = cur.fetchall()
                conn.close()

            for row in rows:
                sig_id, ticker, score, entry_price, created_at = row[0], row[1], row[2], row[3], row[4]
                o1d, o3d, o5d = row[5], row[6], row[7]

                if not entry_price or entry_price <= 0:
                    continue

                try:
                    hist = yf.Ticker(ticker).history(period="15d", auto_adjust=True)
                    if hist is None or hist.empty:
                        continue
                    closes = hist["Close"].dropna().tolist()
                    if len(closes) < 2:
                        continue

                    # Find prices 1, 3, 5 days after signal
                    if isinstance(created_at, str):
                        sig_dt = datetime.fromisoformat(created_at.replace("Z", ""))
                    else:
                        sig_dt = created_at.replace(tzinfo=None) if hasattr(created_at, 'replace') else created_at

                    idx_dates = hist.index.normalize().tolist()
                    sig_date  = sig_dt.date() if hasattr(sig_dt, 'date') else sig_dt

                    price_on = {}
                    for offset, key in [(1, "1d"), (3, "3d"), (5, "5d")]:
                        try:
                            target = sig_date
                            trading_days_after = 0
                            for i, d in enumerate(idx_dates):
                                dt = d.date() if hasattr(d, 'date') else d
                                if dt > target:
                                    trading_days_after += 1
                                    if trading_days_after == offset:
                                        price_on[key] = float(closes[i])
                                        break
                        except Exception:
                            pass

                    updates = {}
                    for key, col in [("1d", "outcome_1d"), ("3d", "outcome_3d"), ("5d", "outcome_5d")]:
                        if key in price_on:
                            ret = (price_on[key] - entry_price) / entry_price
                            if ret > WIN_THRESHOLD:
                                updates[col] = "win"
                            elif ret < LOSS_THRESHOLD:
                                updates[col] = "loss"
                            else:
                                updates[col] = "neutral"
                            updates[f"ret_{key}"] = round(ret * 100, 3)

                    if updates:
                        _write_graded_outcome(sig_id, updates)
                        graded += 1
                except Exception:
                    pass

        except Exception as e:
            print(f"  [accuracy] grade_signals failed: {e}")

        print(f"  [accuracy] Graded {graded} signals")
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
                    SELECT sl.score, so.outcome_5d, so.ret_5d
                    FROM signal_log sl
                    JOIN signal_outcomes so ON so.signal_id = sl.id
                    WHERE so.outcome_5d IS NOT NULL AND so.ret_5d IS NOT NULL
                      AND sl.created_at >= NOW() - INTERVAL '90 days'
                """)
                rows = cur.fetchall(); cur.close(); conn.close()
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT sl.score, so.outcome_5d, so.ret_5d
                    FROM signal_log sl
                    JOIN signal_outcomes so ON so.signal_id = sl.id
                    WHERE so.outcome_5d IS NOT NULL AND so.ret_5d IS NOT NULL
                      AND sl.created_at >= datetime('now', '-90 days')
                """)
                rows = cur.fetchall(); conn.close()

            if not rows:
                return {"n": 0, "insufficient": True}

            all_rets = []
            bucket_data: dict = {label: {"rets": [], "outcomes": []} for _, _, label in SCORE_BUCKETS}
            bucket_data["other"] = {"rets": [], "outcomes": []}

            for score, outcome, ret in rows:
                ret_f  = _safe(ret)
                score_f = _safe(score)
                all_rets.append(ret_f)
                b = _bucket(score_f)
                bucket_data[b]["rets"].append(ret_f)
                bucket_data[b]["outcomes"].append(outcome)

            overall = _metrics_from_rets(all_rets)

            by_bucket = {}
            for label, bd in bucket_data.items():
                if bd["rets"]:
                    by_bucket[label] = _metrics_from_rets(bd["rets"])

            return {
                "n":         len(all_rets),
                "overall":   overall,
                "by_bucket": by_bucket,
                "insufficient": len(all_rets) < 30,
            }
        except Exception as e:
            print(f"  [accuracy] compute_metrics failed: {e}")
            return {"n": 0, "insufficient": True}

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
        """Return True if win_rate < 0.45 for this score bucket (disable buys)."""
        wr = self.get_bucket_win_rate(score)
        if wr is None:
            return False
        return wr < 0.45


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


def run_nightly_validation():
    """Entry point called by scanner_loop at 10 PM ET."""
    print("\n[ACCURACY] Running nightly signal validation...")
    try:
        v = AccuracyValidator()
        n = v.grade_signals()
        m = v.compute_metrics()
        overall = m.get("overall", {})
        print(f"  [accuracy] n={m.get('n',0)} | win_rate={overall.get('win_rate','?')} | "
              f"EV={overall.get('expected_value','?')} | significant={overall.get('significant','?')}")
        return m
    except Exception as e:
        print(f"  [accuracy] nightly validation failed: {e}")
        return {}
