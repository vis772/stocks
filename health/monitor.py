# health/monitor.py
# Self-healing health monitor for Axiom Terminal.
# Called every 5 minutes from scanner_loop.py and read from Streamlit dashboard.
# All state is stored in Supabase — this class is stateless and process-safe.

import os
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# ── Status constants ───────────────────────────────────────────────────────────
OK       = "ok"
WARN     = "warn"
CRITICAL = "critical"

# Thresholds
FINNHUB_FAIL_THRESHOLD   = 0.50   # >50% failure rate triggers degraded mode
UNIVERSE_MIN             = 50     # below this triggers emergency refresh
SCANNER_STALL_HOURS      = 2      # no signal in 2h during market hours = stalled
SCORE_100_CHANGE_MIN_PCT = 1.0    # score=100 but price change <1% = bad data
MAX_PERFECT_SCORES_DAY   = 3      # >3 score=100 signals in one day = suspicious
DEDUP_RATE_ALERT         = 0.20   # >20% suppressed signals = alert
QUOTE_DROP_ALERT         = 0.20   # >20% drop from 7-day avg success rate = alert

# Cooldown: don't send the same alert more than once per hour
_ALERT_COOLDOWNS: Dict[str, float] = {}
_COOLDOWN_SEC = 3600
_LOCK = threading.Lock()


def _now_et() -> datetime:
    try:
        from utils import now_et
        return now_et()
    except Exception:
        from datetime import timezone
        utc = datetime.now(timezone.utc)
        et_offset = -4 if 3 <= utc.month <= 11 else -5
        return utc.replace(tzinfo=None) + timedelta(hours=et_offset)


def _is_market_hours(dt: Optional[datetime] = None) -> bool:
    """True during regular trading hours 9:30–16:00 ET."""
    dt = dt or _now_et()
    if dt.weekday() >= 5:
        return False
    return dt.hour == 9 and dt.minute >= 30 or 10 <= dt.hour <= 15 or (dt.hour == 16 and dt.minute == 0)


def _checked_now() -> str:
    return _now_et().strftime("%H:%M:%S ET")


class HealthMonitor:
    """
    Runs subsystem checks, self-heals where possible, and logs anomalies.
    Stateless — all state lives in the database.
    """

    # ── Individual subsystem checks ───────────────────────────────────────────

    def check_database(self) -> Dict[str, Any]:
        """Verify Supabase connectivity with a lightweight SELECT 1."""
        t0 = time.time()
        try:
            from db.database import _is_postgres, _get_pg_conn, _put_pg_conn
            if not _is_postgres():
                return {"status": OK, "detail": "SQLite (local dev)", "latency_ms": 0, "checked_at": _checked_now()}
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            _put_pg_conn(conn)
            ms = int((time.time() - t0) * 1000)
            return {"status": OK, "detail": f"Connected ({ms} ms)", "latency_ms": ms, "checked_at": _checked_now()}
        except Exception as e:
            return {"status": CRITICAL, "detail": str(e), "latency_ms": -1, "checked_at": _checked_now()}

    def check_api_source(self, source: str, hours_back: int = 1) -> Dict[str, Any]:
        """Compute quote success rate for a given data source over the last N hours."""
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn, _put_pg_conn
            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT COUNT(*) AS total,
                           SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END) AS ok_cnt,
                           AVG(latency_ms) AS avg_ms
                    FROM data_quality
                    WHERE source = %s
                      AND created_at > NOW() - INTERVAL '%s hours'
                """, (source, hours_back))
                row = cur.fetchone(); cur.close(); _put_pg_conn(conn)
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT COUNT(*) AS total,
                           SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END) AS ok_cnt,
                           AVG(latency_ms) AS avg_ms
                    FROM data_quality
                    WHERE source = ?
                      AND created_at > datetime('now', ? || ' hours')
                """, (source, f"-{hours_back}"))
                row = cur.fetchone(); conn.close()

            total    = row[0] or 0
            ok_cnt   = row[1] or 0
            avg_ms   = round(row[2] or 0, 1)
            if total == 0:
                return {"status": WARN, "detail": "No data in last hour", "success_rate": None,
                        "total": 0, "avg_latency_ms": 0, "checked_at": _checked_now()}
            rate = ok_cnt / total
            if rate >= 0.80:
                status = OK
            elif rate >= 0.50:
                status = WARN
            else:
                status = CRITICAL
            return {
                "status":        status,
                "detail":        f"{ok_cnt}/{total} ok ({rate*100:.0f}%) | avg {avg_ms:.0f} ms",
                "success_rate":  round(rate, 4),
                "total":         total,
                "avg_latency_ms": avg_ms,
                "checked_at":    _checked_now(),
            }
        except Exception as e:
            return {"status": WARN, "detail": f"Check failed: {e}", "success_rate": None,
                    "total": 0, "avg_latency_ms": 0, "checked_at": _checked_now()}

    def check_finnhub(self)  -> Dict[str, Any]: return self.check_api_source("finnhub")
    def check_tiingo(self)   -> Dict[str, Any]: return self.check_api_source("tiingo")
    def check_yfinance(self) -> Dict[str, Any]: return self.check_api_source("yfinance")

    def check_scanner_loop(self) -> Dict[str, Any]:
        """During market hours, warn if no signals logged in the last SCANNER_STALL_HOURS hours."""
        now = _now_et()
        if not _is_market_hours(now):
            return {"status": OK, "detail": "Market closed — no signal expected", "checked_at": _checked_now()}
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn, _put_pg_conn
            cutoff_str = (now - timedelta(hours=SCANNER_STALL_HOURS)).isoformat()
            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM signal_log WHERE created_at > %s",
                    (cutoff_str,),
                )
                n = cur.fetchone()[0]; cur.close(); _put_pg_conn(conn)
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM signal_log WHERE created_at > ?", (cutoff_str,)
                )
                n = cur.fetchone()[0]; conn.close()

            if n > 0:
                return {"status": OK, "detail": f"{n} signal(s) in last {SCANNER_STALL_HOURS}h",
                        "signal_count": n, "checked_at": _checked_now()}
            return {"status": WARN, "detail": f"No signals in last {SCANNER_STALL_HOURS}h — scanner may be stalled",
                    "signal_count": 0, "checked_at": _checked_now()}
        except Exception as e:
            return {"status": WARN, "detail": f"Check failed: {e}", "checked_at": _checked_now()}

    def check_universe_size(self) -> Dict[str, Any]:
        try:
            from universe_manager import get_universe_size
            n = get_universe_size()
            if n >= 100:
                return {"status": OK,   "detail": f"{n} tickers active", "size": n, "checked_at": _checked_now()}
            if n >= UNIVERSE_MIN:
                return {"status": WARN, "detail": f"Only {n} tickers — consider refresh", "size": n, "checked_at": _checked_now()}
            return {"status": CRITICAL, "detail": f"Universe collapsed to {n} tickers!", "size": n, "checked_at": _checked_now()}
        except Exception as e:
            return {"status": WARN, "detail": f"Check failed: {e}", "size": 0, "checked_at": _checked_now()}

    def check_conviction_engine(self) -> Dict[str, Any]:
        """Warn if conviction engine hasn't run today."""
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn, _put_pg_conn
            today = _now_et().strftime("%Y-%m-%d")
            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM conviction_buys WHERE DATE(created_at) = %s", (today,)
                )
                n = cur.fetchone()[0]; cur.close(); _put_pg_conn(conn)
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM conviction_buys WHERE DATE(created_at) = ?", (today,)
                )
                n = cur.fetchone()[0]; conn.close()
            if n > 0:
                return {"status": OK, "detail": f"{n} conviction picks today", "checked_at": _checked_now()}
            # Only warn during market hours or after preopen
            et = _now_et()
            if et.hour >= 9:
                return {"status": WARN, "detail": "No conviction picks today yet", "checked_at": _checked_now()}
            return {"status": OK, "detail": "Pre-market — conviction run pending", "checked_at": _checked_now()}
        except Exception as e:
            return {"status": WARN, "detail": f"Check failed: {e}", "checked_at": _checked_now()}

    def check_accuracy_validator(self) -> Dict[str, Any]:
        """Warn if validator_health shows >20 pending signals."""
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn, _put_pg_conn
            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT signals_pending, oldest_pending
                    FROM validator_health
                    ORDER BY check_time DESC LIMIT 1
                """)
                row = cur.fetchone(); cur.close(); _put_pg_conn(conn)
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT signals_pending, oldest_pending
                    FROM validator_health
                    ORDER BY check_time DESC LIMIT 1
                """)
                row = cur.fetchone(); conn.close()
            if not row:
                return {"status": WARN, "detail": "No validator_health records", "checked_at": _checked_now()}
            pending, oldest = row
            pending = pending or 0
            if pending > 50:
                return {"status": WARN, "detail": f"{pending} pending grades | oldest: {oldest}",
                        "pending": pending, "checked_at": _checked_now()}
            return {"status": OK, "detail": f"{pending} pending grades", "pending": pending, "checked_at": _checked_now()}
        except Exception as e:
            return {"status": WARN, "detail": f"Check failed: {e}", "checked_at": _checked_now()}

    def check_telegram_bot(self) -> Dict[str, Any]:
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        uid   = os.environ.get("TELEGRAM_ALLOWED_USER_ID", "")
        if token and uid:
            return {"status": OK, "detail": "Token + user ID configured", "checked_at": _checked_now()}
        missing = []
        if not token: missing.append("TELEGRAM_BOT_TOKEN")
        if not uid:   missing.append("TELEGRAM_ALLOWED_USER_ID")
        return {"status": WARN, "detail": f"Missing env vars: {', '.join(missing)}", "checked_at": _checked_now()}

    # ── All checks ────────────────────────────────────────────────────────────

    def run_all_checks(self) -> Dict[str, Dict]:
        return {
            "database":           self.check_database(),
            "finnhub":            self.check_finnhub(),
            "tiingo":             self.check_tiingo(),
            "yfinance":           self.check_yfinance(),
            "scanner_loop":       self.check_scanner_loop(),
            "universe":           self.check_universe_size(),
            "conviction_engine":  self.check_conviction_engine(),
            "accuracy_validator": self.check_accuracy_validator(),
            "telegram_bot":       self.check_telegram_bot(),
        }

    # ── Self-healing ──────────────────────────────────────────────────────────

    def _cooldown_ok(self, key: str) -> bool:
        with _LOCK:
            last = _ALERT_COOLDOWNS.get(key, 0)
            if time.time() - last < _COOLDOWN_SEC:
                return False
            _ALERT_COOLDOWNS[key] = time.time()
            return True

    def _alert(self, title: str, msg: str, priority: int = 1) -> None:
        try:
            from alerts import send_alert
            send_alert(title=title, message=msg, priority=priority)
        except Exception as e:
            print(f"  [health] alert failed: {e}")

    def _log(self, subsystem: str, status: str, detail: str = "", action: str = "") -> None:
        try:
            from db.database import log_health_event
            log_health_event(subsystem, status, detail, action)
        except Exception as e:
            print(f"  [health] log failed: {e}")

    def _heal_finnhub_degraded(self, rate: float) -> None:
        if not self._cooldown_ok("finnhub_degraded"):
            return
        try:
            from db.database import set_scanner_control
            set_scanner_control(finnhub_degraded=True)
        except Exception:
            pass
        # Also set the in-process flag if scanner_loop is imported
        try:
            import scanner_loop
            scanner_loop._FINNHUB_DEGRADED_MODE = True
        except Exception:
            pass
        msg = f"Finnhub failure rate {rate*100:.0f}% — switched to yfinance fallback mode"
        self._log("finnhub", CRITICAL, f"failure_rate={rate:.2f}", "switched to degraded mode")
        self._alert("Axiom — Finnhub Degraded", msg, priority=1)
        print(f"  [health] HEAL: {msg}")

    def _heal_universe_thin(self, size: int) -> None:
        if not self._cooldown_ok("universe_thin"):
            return
        try:
            from universe_manager import refresh_universe_async
            refresh_universe_async()
            action = "emergency refresh triggered"
        except Exception as e:
            action = f"refresh failed: {e}"
        self._log("universe", CRITICAL, f"size={size}", action)
        self._alert("Axiom — Universe Collapsed",
                    f"Universe dropped to {size} tickers. {action}.", priority=1)
        print(f"  [health] HEAL: Universe {size} tickers — {action}")

    def _heal_scanner_stalled(self) -> None:
        if not self._cooldown_ok("scanner_stalled"):
            return
        try:
            from db.database import set_scanner_control
            set_scanner_control(force_scan=True)
            action = "force_scan flag set"
        except Exception as e:
            action = f"force_scan failed: {e}"
        self._log("scanner_loop", WARN, "no signals in 2h during market hours", action)
        self._alert("Axiom — Scanner Stalled",
                    f"No signals logged in {SCANNER_STALL_HOURS}h during market hours. {action}.",
                    priority=1)
        print(f"  [health] HEAL: Scanner stalled — {action}")

    def _heal_database_with_backoff(self) -> bool:
        """Retry DB connection up to 4 times with exponential backoff. Returns True on success."""
        for attempt in range(4):
            delay = 2 ** attempt  # 1s, 2s, 4s, 8s
            time.sleep(delay)
            result = self.check_database()
            if result["status"] == OK:
                print(f"  [health] DB reconnected on attempt {attempt + 1}")
                return True
            print(f"  [health] DB retry {attempt + 1}/4 failed: {result['detail']}")
        if self._cooldown_ok("db_critical"):
            self._alert("Axiom — Database Down",
                        "Supabase connection failed after 4 retries with exponential backoff.",
                        priority=1)
        return False

    def heal(self, results: Dict[str, Dict], state=None) -> None:
        """Dispatch self-healing actions based on check results."""
        # Database
        if results.get("database", {}).get("status") == CRITICAL:
            self._heal_database_with_backoff()

        # Finnhub
        fh = results.get("finnhub", {})
        if fh.get("status") == CRITICAL and fh.get("success_rate") is not None:
            self._heal_finnhub_degraded(1 - fh["success_rate"])
        elif fh.get("status") == OK:
            # Restore normal mode if Finnhub recovered
            try:
                from db.database import set_scanner_control
                set_scanner_control(finnhub_degraded=False)
                import scanner_loop
                if getattr(scanner_loop, "_FINNHUB_DEGRADED_MODE", False):
                    scanner_loop._FINNHUB_DEGRADED_MODE = False
                    print("  [health] Finnhub recovered — normal mode restored")
            except Exception:
                pass

        # Universe
        uv = results.get("universe", {})
        if uv.get("status") == CRITICAL:
            self._heal_universe_thin(uv.get("size", 0))

        # Scanner stall
        sc = results.get("scanner_loop", {})
        if sc.get("status") == WARN:
            self._heal_scanner_stalled()

    # ── Anomaly detection ─────────────────────────────────────────────────────

    def _detect_score_100_bad_data(self) -> List[Dict]:
        """Return signals today with score=100 but abs(price_change) < 1%."""
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn, _put_pg_conn
            today = _now_et().strftime("%Y-%m-%d")
            query = """
                SELECT id, ticker, score, price_at_signal, created_at
                FROM signal_log
                WHERE score >= 99.5
                  AND DATE(created_at) = {ph}
            """.replace("{ph}", "%s" if _is_postgres() else "?")
            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute(query, (today,))
                rows = cur.fetchall(); cur.close(); _put_pg_conn(conn)
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute(query, (today,))
                rows = cur.fetchall(); conn.close()
            # We can't easily check price change here without the pre-signal price,
            # so flag all score=100 signals for review — scanner_loop suppresses
            # bad-data ones at signal time; these are the ones that slipped through.
            return [{"id": r[0], "ticker": r[1], "score": r[2],
                     "price": r[3], "created_at": str(r[4])} for r in rows]
        except Exception:
            return []

    def _detect_suspicious_scores(self) -> Dict:
        """Alert if >MAX_PERFECT_SCORES_DAY score=100 signals in one trading day."""
        hits = self._detect_score_100_bad_data()
        return {"count": len(hits), "signals": hits,
                "alert": len(hits) > MAX_PERFECT_SCORES_DAY}

    def _detect_quote_rate_drop(self) -> List[Dict]:
        """Compare last-hour quote success rate vs 7-day average per source."""
        anomalies = []
        for source in ("finnhub", "tiingo", "yfinance"):
            try:
                current = self.check_api_source(source, hours_back=1)
                week    = self.check_api_source(source, hours_back=168)  # 7 days
                if current["success_rate"] is None or week["success_rate"] is None:
                    continue
                drop = week["success_rate"] - current["success_rate"]
                if drop > QUOTE_DROP_ALERT:
                    anomalies.append({
                        "source":      source,
                        "current_rate": current["success_rate"],
                        "week_avg":    week["success_rate"],
                        "drop":        round(drop, 4),
                    })
            except Exception:
                pass
        return anomalies

    def _detect_high_dedup_rate(self, state=None) -> Dict:
        """Check if suppressed signal rate exceeds threshold."""
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn, _put_pg_conn
            today = _now_et().strftime("%Y-%m-%d")
            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM signal_log WHERE DATE(created_at) = %s", (today,)
                )
                n = cur.fetchone()[0]; cur.close(); _put_pg_conn(conn)
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM signal_log WHERE DATE(created_at) = ?", (today,)
                )
                n = cur.fetchone()[0]; conn.close()
            suppressed = getattr(state, "signals_suppressed", 0) if state else 0
            total = (n or 0) + suppressed
            if total == 0:
                return {"rate": 0.0, "alert": False}
            rate = suppressed / total
            return {"rate": round(rate, 4), "logged": n, "suppressed": suppressed,
                    "alert": rate > DEDUP_RATE_ALERT}
        except Exception:
            return {"rate": 0.0, "alert": False}

    def detect_anomalies(self, state=None) -> List[Dict]:
        anomalies = []

        # Score=100 suspicious
        sc = self._detect_suspicious_scores()
        if sc["alert"]:
            anomalies.append({
                "type":    "suspicious_scores",
                "detail":  f"{sc['count']} score=100 signals today (>{MAX_PERFECT_SCORES_DAY} threshold)",
                "signals": sc["signals"],
            })

        # Quote rate drop
        for drop in self._detect_quote_rate_drop():
            anomalies.append({
                "type":   "quote_rate_drop",
                "detail": f"{drop['source']}: {drop['current_rate']*100:.0f}% success "
                          f"(7-day avg: {drop['week_avg']*100:.0f}%, drop: {drop['drop']*100:.0f}%)",
                **drop,
            })

        # Dedup rate
        dedup = self._detect_high_dedup_rate(state)
        if dedup.get("alert"):
            anomalies.append({
                "type":   "high_dedup_rate",
                "detail": f"Dedup rate {dedup['rate']*100:.0f}% > {DEDUP_RATE_ALERT*100:.0f}% threshold "
                          f"({dedup.get('suppressed',0)} suppressed / {dedup.get('logged',0)} logged)",
                **dedup,
            })

        return anomalies

    # ── Daily summary ─────────────────────────────────────────────────────────

    def daily_summary(self) -> None:
        """Send 9 PM ET daily health summary via Pushover."""
        if not self._cooldown_ok("daily_summary"):
            return
        try:
            today = _now_et().strftime("%Y-%m-%d")
            results = self.run_all_checks()

            # Count statuses
            counts = {OK: 0, WARN: 0, CRITICAL: 0}
            for v in results.values():
                counts[v.get("status", WARN)] += 1

            # Signals today
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn, _put_pg_conn
            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM signal_log WHERE DATE(created_at) = %s", (today,))
                signals_today = cur.fetchone()[0]; cur.close(); _put_pg_conn(conn)
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM signal_log WHERE DATE(created_at) = ?", (today,))
                signals_today = cur.fetchone()[0]; conn.close()

            # Quote success rates
            fh_rate = results["finnhub"].get("success_rate")
            ti_rate = results["tiingo"].get("success_rate")
            yf_rate = results["yfinance"].get("success_rate")
            def _pct(v): return f"{v*100:.0f}%" if v is not None else "n/a"

            msg = (
                f"✓ {counts[OK]} | ⚠ {counts[WARN]} | ✗ {counts[CRITICAL]}\n"
                f"Signals today: {signals_today}\n"
                f"Finnhub: {_pct(fh_rate)} | Tiingo: {_pct(ti_rate)} | yfinance: {_pct(yf_rate)}\n"
                f"Universe: {results['universe'].get('detail','—')}"
            )
            self._alert("Axiom — Daily Health Summary", msg, priority=0)
            self._log("system", OK, "daily summary sent", "pushover")
        except Exception as e:
            print(f"  [health] daily_summary failed: {e}")

    # ── Main cycle (called every 5 min from scanner_loop) ─────────────────────

    def run_health_check_cycle(self, state=None) -> Dict[str, Dict]:
        """
        Full health check + self-healing + anomaly detection.
        Returns results dict for optional inspection by the caller.
        """
        print(f"  [health] Running checks @ {_checked_now()}")
        results = self.run_all_checks()

        # Log criticals and warns
        for name, info in results.items():
            if info.get("status") == CRITICAL:
                self._log(name, CRITICAL, info.get("detail", ""))
                print(f"  [health] CRITICAL — {name}: {info.get('detail')}")
            elif info.get("status") == WARN:
                self._log(name, WARN, info.get("detail", ""))

        # Self-heal
        self.heal(results, state)

        # Anomaly detection
        anomalies = self.detect_anomalies(state)
        for a in anomalies:
            self._log("anomaly", WARN, a["detail"])
            key = f"anomaly_{a['type']}"
            if self._cooldown_ok(key):
                self._alert(
                    f"Axiom — Anomaly: {a['type'].replace('_',' ').title()}",
                    a["detail"],
                    priority=0,
                )
                print(f"  [health] ANOMALY — {a['detail']}")

        # Daily summary at 9 PM ET
        et = _now_et()
        if et.hour == 21 and et.minute < 5:
            self.daily_summary()

        return results
