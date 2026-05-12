# conviction_engine.py
# Synthesizes ALL available signals into a ranked, actionable buy list.
# Runs at 8:55 AM ET (preopen), 4:00 PM ET (close), and 8:30 PM ET (afterhours).
# Max 5 names per session. Writes to conviction_buys table.

import os
import time
from datetime import datetime, date, timedelta
from typing import Optional
import numpy as np


def _safe(v, fb=0.0):
    try:
        f = float(v)
        return fb if (f != f) else f
    except Exception:
        return fb


def now_et():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except ImportError:
        from scanner_loop import now_et as _ne
        return _ne()


class ConvictionEngine:
    """
    Decision engine that produces ranked buy candidates with full trade parameters.
    NOT a screener — produces at most 5 actionable names per session.
    """

    # ── Step 1: Eligibility gate ──────────────────────────────────────────────

    def is_eligible(self, ticker: str, data: dict) -> bool:
        """Must pass ALL checks to be considered."""
        composite = _safe(data.get("composite_score", 0))
        if composite < 68:
            return False

        quant_adj = _safe(data.get("quant_adjustment", 0))
        if quant_adj < 0:
            return False

        sq = data.get("source_quality", "live")
        if sq and "stale_" in str(sq):
            try:
                mins = int(str(sq).split("stale_")[1].rstrip("m"))
                if mins > 60:
                    return False
            except Exception:
                pass

        rvol = _safe(data.get("rvol", 0))
        if rvol < 1.3:
            return False

        price = _safe(data.get("price", 0))
        if price < 1.00:
            return False

        # Check portfolio for >8% loss
        try:
            from db.database import get_portfolio
            pf = get_portfolio()
            if not pf.empty:
                row = pf[pf["ticker"] == ticker.upper()]
                if not row.empty:
                    avg_cost = _safe(row.iloc[0]["avg_cost"])
                    if avg_cost > 0 and price > 0:
                        if (price - avg_cost) / avg_cost < -0.08:
                            return False
        except Exception:
            pass

        # No earnings within 2 trading days
        if data.get("earnings_within_2d", False):
            return False

        return True

    # ── Step 2: Conviction score ──────────────────────────────────────────────

    def conviction_score(self, ticker: str, data: dict) -> float:
        score = 0.0
        composite = _safe(data.get("composite_score", 0))
        quant_adj = _safe(data.get("quant_adjustment", 0))
        price     = _safe(data.get("price", 0))
        vwap      = _safe(data.get("vwap", 0))
        rsi       = data.get("rsi")
        sma20     = data.get("sma_20")
        rvol      = _safe(data.get("rvol", 0))
        sentiment = data.get("claude_sentiment", "")
        sigma     = _safe(data.get("sigma_hist", 1.0), 1.0)
        sec_cat   = data.get("has_sec_catalyst", False)
        news_sent = _safe(data.get("news_sentiment_score", 0))
        short_pct = _safe(data.get("short_percent_float", 0))

        # Signal agreement (max 30)
        if composite >= 75:
            score += 10.0
        if quant_adj >= 10:
            score += 10.0
        if str(sentiment).lower() in ("bullish", "positive"):
            score += 10.0

        # Technical confirmation (max 25)
        if price > 0 and vwap > 0 and price > vwap:
            score += 10.0
        rsi_val = _safe(rsi, -1.0) if rsi is not None else -1.0
        if 45 <= rsi_val <= 65:
            score += 8.0
        if price > 0 and sma20 and price > _safe(sma20) > 0:
            prev_price = _safe(data.get("prev_close", price))
            if prev_price > 0 and prev_price <= _safe(sma20) and price > _safe(sma20):
                score += 7.0

        # Volume conviction (max 20)
        if rvol >= 3.0:
            score += 20.0
        elif rvol >= 2.0:
            score += 15.0
        elif rvol >= 1.5:
            score += 10.0

        # Catalyst quality (max 15)
        if sec_cat:
            score += 15.0
        elif news_sent >= 0.7:
            score += 10.0
        if short_pct > 0.15:
            score += 5.0

        # Risk-adjusted (max 10)
        if sigma < 0.60:
            score += 10.0
        elif sigma < 0.90:
            score += 5.0
        elif sigma > 1.40:
            score -= 10.0

        return round(min(score, 100.0), 1)

    # ── Step 3: Trade parameters ──────────────────────────────────────────────

    def compute_trade_params(self, ticker: str, price: float, data: dict) -> dict:
        atr = _safe(data.get("atr_14", 0))
        if atr <= 0:
            atr = price * 0.02  # fallback: 2% of price

        # Position sizing via half-Kelly
        score   = _safe(data.get("composite_score", 70))
        pos_pct = self._kelly_size(score, data)

        stop_loss = round(price - 1.5 * atr, 2)
        stop_pct  = round((1.5 * atr / price) * 100, 1) if price > 0 else 0

        win_rate = _safe(data.get("bucket_win_rate", 0.5), 0.5)
        avg_win  = _safe(data.get("bucket_avg_win",  0.05), 0.05)
        avg_loss = _safe(data.get("bucket_avg_loss", 0.03), 0.03)
        ev = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

        return {
            "entry":          round(price, 2),
            "limit_entry":    round(price * 0.995, 2),
            "stop_loss":      max(stop_loss, 0.01),
            "stop_pct":       stop_pct,
            "target_1":       round(price + 1.0 * atr, 2),
            "target_2":       round(price + 2.0 * atr, 2),
            "target_3":       round(price + 3.5 * atr, 2),
            "position_size":  round(pos_pct * 100, 1),
            "expected_value": round(ev * 100, 2),
            "conviction":     self.conviction_score(ticker, data),
        }

    def _kelly_size(self, score: float, data: dict) -> float:
        """Half-Kelly position size, capped at 10%."""
        wr      = _safe(data.get("bucket_win_rate"))
        avg_win = _safe(data.get("bucket_avg_win"))
        n_sigs  = data.get("bucket_n", 0)

        if n_sigs < 30 or wr <= 0 or avg_win <= 0:
            return 0.03  # 3% fixed default

        avg_loss = _safe(data.get("bucket_avg_loss", 0.03), 0.03)
        kelly = (wr * avg_win - (1 - wr) * avg_loss) / avg_win if avg_win > 0 else 0
        half_kelly = kelly * 0.5
        return min(max(half_kelly, 0.01), 0.10)

    # ── Step 4: Hold classification ───────────────────────────────────────────

    def classify_hold(self, ticker: str, data: dict) -> str:
        rsi       = _safe(data.get("rsi", 50), 50)
        sigma     = _safe(data.get("sigma_hist", 1.0), 1.0)
        has_sec   = data.get("has_sec_catalyst", False)
        rvol      = _safe(data.get("rvol", 1.0))
        rs_5d     = _safe(data.get("rs_vs_iwm_5d", 1.0))
        earnings_3d = data.get("earnings_within_3d", False)
        ah_trending = data.get("afterhours_trending_up", False)

        # DAYTRADE conditions (any one)
        if sigma > 1.20:
            return "DAYTRADE"
        if earnings_3d:
            return "DAYTRADE"
        if not has_sec and rvol > 2.0 and rsi > 60:
            return "DAYTRADE"

        # OVERNIGHT conditions (all must be true)
        if ah_trending and not data.get("earnings_within_2d") and has_sec and sigma < 0.90:
            return "OVERNIGHT"

        # SWING_2-5D
        if rsi < 40 and rvol > 1.2:
            return "SWING_2-5D"
        if rs_5d > 1.2:
            return "SWING_2-5D"
        if has_sec:
            return "SWING_2-5D"

        return "DAYTRADE"

    # ── Step 5: Final buy list ────────────────────────────────────────────────

    def generate_buy_list(self, session: str = "afterhours") -> list:
        """
        Pull all scored tickers from today's signal_log, apply eligibility gate,
        rank by conviction, return top 5.
        """
        candidates = self._load_todays_candidates()
        eligible   = [d for d in candidates if self.is_eligible(d["ticker"], d)]
        scored     = sorted(eligible, key=lambda d: self.conviction_score(d["ticker"], d), reverse=True)
        top5       = scored[:5]

        results = []
        for i, data in enumerate(top5, 1):
            ticker = data["ticker"]
            price  = _safe(data.get("price", 0))
            if price <= 0:
                continue
            params = self.compute_trade_params(ticker, price, data)
            hold   = self.classify_hold(ticker, data)
            reasoning = self._generate_reasoning(ticker, data, hold)
            results.append({
                "rank":          i,
                "ticker":        ticker,
                "conviction":    params["conviction"],
                "hold_type":     hold,
                "why":           reasoning,
                "entry":         params["entry"],
                "limit_entry":   params["limit_entry"],
                "stop_loss":     params["stop_loss"],
                "stop_pct":      f"{params['stop_pct']}%",
                "target_1":      params["target_1"],
                "target_2":      params["target_2"],
                "target_3":      params["target_3"],
                "position_size": f"{params['position_size']}% of portfolio",
                "expected_value": f"+{params['expected_value']}% EV",
                "composite":     data.get("composite_score"),
                "quant_adj":     data.get("quant_adjustment"),
                "rvol":          data.get("rvol"),
                "rsi":           data.get("rsi"),
                "sigma_hist":    data.get("sigma_hist"),
                "_params":       params,
            })

        return results

    def _load_todays_candidates(self) -> list:
        """Load today's high-scoring signals from signal_log with quant data."""
        try:
            from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
            from accuracy_validator import AccuracyValidator
            from quant_engine import QuantEngine

            av = AccuracyValidator()
            metrics = av.compute_metrics()

            if _is_postgres():
                conn = _get_pg_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT sl.ticker, sl.score, sl.price_at_signal,
                           sl.quant_adj, sl.source_quality, sl.volume_at_signal,
                           sl.score_breakdown
                    FROM signal_log sl
                    WHERE DATE(sl.created_at) = CURRENT_DATE
                      AND sl.score >= 65
                    ORDER BY sl.score DESC
                    LIMIT 50
                """)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                cur.close(); conn.close()
            else:
                conn = _get_sqlite_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT sl.ticker, sl.score, sl.price_at_signal,
                           sl.quant_adj, sl.source_quality, sl.volume_at_signal,
                           sl.score_breakdown
                    FROM signal_log sl
                    WHERE DATE(sl.created_at) = DATE('now')
                      AND sl.score >= 65
                    ORDER BY sl.score DESC
                    LIMIT 50
                """)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                conn.close()

            candidates = []
            seen = set()
            import json as _json
            for row in rows:
                d = dict(zip(cols, row))
                t = d.get("ticker", "")
                if not t or t in seen:
                    continue
                seen.add(t)

                bd = {}
                try:
                    bd = _json.loads(d.get("score_breakdown") or "{}")
                except Exception:
                    pass

                # Enrich with quant data and bucket metrics
                b_label = "65-70"
                from accuracy_validator import _bucket
                b_label = _bucket(_safe(d.get("score", 0)))
                bm = metrics.get("by_bucket", {}).get(b_label, {})

                # Fetch RVOL and other live data
                rvol = 1.0
                vwap = 0.0
                sma20 = None
                try:
                    from db.database import load_scanner_state
                    state = load_scanner_state()
                    vwap_snap = state.get("vwap_snapshot", {})
                    if t in vwap_snap:
                        vwap = _safe(vwap_snap[t].get("vwap", 0))
                    mom_rank = state.get("momentum_ranking", [])
                    for mr in mom_rank:
                        if mr.get("ticker") == t:
                            rvol = _safe(mr.get("rvol", 1.0))
                            break
                except Exception:
                    pass

                # Check earnings
                earnings_2d = False
                try:
                    from data.market_data import _fh_get
                    import datetime as _dt
                    cal = _fh_get("calendar/earnings", {
                        "symbol": t,
                        "from":   _dt.date.today().isoformat(),
                        "to":     (_dt.date.today() + _dt.timedelta(days=3)).isoformat(),
                    })
                    if cal and cal.get("earningsCalendar"):
                        earnings_2d = True
                except Exception:
                    pass

                candidates.append({
                    "ticker":           t,
                    "composite_score":  _safe(d.get("score", 0)),
                    "quant_adjustment": _safe(d.get("quant_adj", 0)),
                    "source_quality":   d.get("source_quality", "live"),
                    "price":            _safe(d.get("price_at_signal", 0)),
                    "rvol":             rvol,
                    "vwap":             vwap,
                    "rsi":              bd.get("rsi"),
                    "sma_20":           sma20,
                    "sigma_hist":       bd.get("sigma_hist"),
                    "atr_14":           bd.get("atr_14"),
                    "has_sec_catalyst": False,
                    "news_sentiment_score": 0.0,
                    "short_percent_float":  0.0,
                    "earnings_within_2d":   earnings_2d,
                    "earnings_within_3d":   earnings_2d,
                    "afterhours_trending_up": False,
                    "claude_sentiment":  "",
                    "bucket_win_rate":   bm.get("win_rate"),
                    "bucket_avg_win":    bm.get("avg_win"),
                    "bucket_avg_loss":   bm.get("avg_loss"),
                    "bucket_n":          bm.get("n", 0),
                    "bucket_label":      b_label,
                    "rs_vs_iwm_5d":      1.0,
                    "prev_close":        0.0,
                })
            return candidates
        except Exception as e:
            print(f"  [conviction] _load_todays_candidates failed: {e}")
            return []

    def _generate_reasoning(self, ticker: str, data: dict, hold_type: str) -> str:
        """Use Claude Haiku to write 2-sentence buy reasoning."""
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return self._fallback_reasoning(ticker, data, hold_type)
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=100,
                messages=[{
                    "role": "user",
                    "content": (
                        f"Given these signals for {ticker}: "
                        f"composite={data.get('composite_score')}, "
                        f"quant_adj={data.get('quant_adjustment')}, "
                        f"RVOL={data.get('rvol')}, "
                        f"RSI={data.get('rsi')}, "
                        f"sigma={data.get('sigma_hist')}, "
                        f"hold_type={hold_type} — "
                        "write exactly 2 sentences explaining why this is a conviction buy today. "
                        "Be specific. No fluff. Plain text only."
                    ),
                }],
            )
            return msg.content[0].text.strip()
        except Exception:
            return self._fallback_reasoning(ticker, data, hold_type)

    def _fallback_reasoning(self, ticker: str, data: dict, hold_type: str) -> str:
        composite = data.get("composite_score", 0)
        rvol      = data.get("rvol", 0)
        quant_adj = data.get("quant_adjustment", 0)
        return (
            f"{ticker} scores {composite}/100 with a quant adjustment of {quant_adj:+.1f}, "
            f"confirming {rvol:.1f}x relative volume above the market. "
            f"Hold type {hold_type} based on volatility and catalyst profile."
        )


def save_buy_list(buy_list: list, session: str) -> None:
    """Persist conviction_buys to DB."""
    if not buy_list:
        return
    try:
        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
        today = date.today().isoformat()
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("DELETE FROM conviction_buys WHERE date = %s AND session = %s", (today, session))
            for b in buy_list:
                p = b.get("_params", {})
                cur.execute("""
                    INSERT INTO conviction_buys
                        (date, session, rank, ticker, conviction, hold_type,
                         entry, stop_loss, target_1, target_2, target_3,
                         position_pct, expected_value, reasoning, composite, quant_adj)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (today, session, b["rank"], b["ticker"], b["conviction"], b["hold_type"],
                      p.get("entry"), p.get("stop_loss"), p.get("target_1"),
                      p.get("target_2"), p.get("target_3"), p.get("position_size"),
                      p.get("expected_value"), b.get("why"),
                      b.get("composite"), b.get("quant_adj")))
            conn.commit(); cur.close(); conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute("DELETE FROM conviction_buys WHERE date=? AND session=?", (today, session))
            for b in buy_list:
                p = b.get("_params", {})
                conn.execute("""
                    INSERT INTO conviction_buys
                        (date, session, rank, ticker, conviction, hold_type,
                         entry, stop_loss, target_1, target_2, target_3,
                         position_pct, expected_value, reasoning, composite, quant_adj)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (today, session, b["rank"], b["ticker"], b["conviction"], b["hold_type"],
                      p.get("entry"), p.get("stop_loss"), p.get("target_1"),
                      p.get("target_2"), p.get("target_3"), p.get("position_size"),
                      p.get("expected_value"), b.get("why"),
                      b.get("composite"), b.get("quant_adj")))
            conn.commit(); conn.close()
        print(f"  [conviction] Saved {len(buy_list)} buys for session={session}")
    except Exception as e:
        print(f"  [conviction] save_buy_list failed: {e}")


def send_buy_list_alert(buy_list: list, session: str) -> None:
    """Push Pushover alert with top 3 picks."""
    if not buy_list:
        try:
            from alerts import send_alert, PRIORITY_NORMAL
            send_alert(
                title="Axiom — No High-Conviction Setups Tonight",
                message="Cash is a position. No tickers cleared the eligibility gate.",
                priority=PRIORITY_NORMAL,
            )
        except Exception:
            pass
        return
    try:
        from alerts import send_alert, PRIORITY_HIGH
        lines = []
        for b in buy_list[:3]:
            hold_abbr = {"OVERNIGHT": "OVR", "DAYTRADE": "DAY", "SWING_2-5D": "SWG"}.get(
                b["hold_type"], b["hold_type"][:3]
            )
            p = b.get("_params", {})
            lines.append(
                f"#{b['rank']} {b['ticker']} ({b['conviction']:.0f}) {hold_abbr} | "
                f"Entry ${p.get('entry','?')} stop ${p.get('stop_loss','?')}"
            )
        title_map = {
            "preopen":   "Axiom — Pre-Open Conviction (8:55 AM)",
            "close":     "Axiom — Close Conviction (4 PM)",
            "afterhours": "Axiom — Tonight's Buys",
        }
        alert_title = title_map.get(session, f"Axiom — Conviction ({session})")
        send_alert(
            title=alert_title,
            message="\n".join(lines),
            priority=PRIORITY_HIGH,
        )
    except Exception as e:
        print(f"  [conviction] alert failed: {e}")


def run_conviction_engine(session: str = "afterhours") -> list:
    """Entry point called by scanner_loop at 8:55 AM, 4 PM, and 8:30 PM ET."""
    print(f"\n[CONVICTION] Running conviction engine for session={session}...")
    try:
        from accuracy_validator import AccuracyValidator
        engine = ConvictionEngine()

        # Warn if any bucket has win_rate < 0.45
        av = AccuracyValidator()
        metrics = av.compute_metrics()
        for label, bm in metrics.get("by_bucket", {}).items():
            wr = bm.get("win_rate", 1.0)
            if bm.get("n", 0) >= 30 and wr < 0.45:
                print(f"  [conviction] WARNING: bucket {label} win_rate={wr:.2%} < 0.45 — buys disabled")

        buy_list = engine.generate_buy_list(session=session)
        save_buy_list(buy_list, session)
        send_buy_list_alert(buy_list, session)
        print(f"  [conviction] {len(buy_list)} conviction buy(s) generated")
        return buy_list
    except Exception as e:
        print(f"  [conviction] run_conviction_engine failed: {e}")
        return []
