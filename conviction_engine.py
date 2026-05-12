# conviction_engine.py
# Synthesizes ALL available signals into a ranked, actionable buy list.
# Runs at 8:55 AM ET (preopen), 4:00 PM ET (close), and 8:30 PM ET (afterhours).
# Max 5 names per session. Writes to conviction_buys table.

import os
import time
from datetime import datetime, date, timedelta
from typing import Optional
import numpy as np

_AI_CACHE: dict = {}
_CACHE_TTL_SECS = 1800


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

    def is_eligible(self, ticker: str, data: dict, session: str = "afterhours") -> bool:
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
        rvol_min = 0.5 if session == "preopen" else 1.3
        if rvol < rvol_min:
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
        eligible   = [d for d in candidates if self.is_eligible(d["ticker"], d, session=session)]
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


def _now_et():
    """Returns current ET datetime."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except ImportError:
        try:
            import pytz
            return datetime.now(pytz.timezone("America/New_York"))
        except ImportError:
            return datetime.now()


def _is_market_hours() -> bool:
    """True from 9:30 AM to 4 PM ET on weekdays."""
    et = _now_et()
    if et.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    market_open  = et.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = et.replace(hour=16, minute=0,  second=0, microsecond=0)
    return market_open <= et < market_close


def _fetch_live_ticker(ticker: str) -> dict:
    """Calls Finnhub /quote. Returns price and pct_change_today."""
    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        return {"price": 0.0, "pct_change_today": 0.0}
    try:
        import urllib.request, json as _json
        url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={api_key}"
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = _json.loads(resp.read())
        c  = float(data.get("c") or 0)
        pc = float(data.get("pc") or 0)
        pct = (c - pc) / pc * 100 if pc and pc != 0 else 0.0
        return {"price": c, "pct_change_today": round(pct, 2)}
    except Exception:
        return {"price": 0.0, "pct_change_today": 0.0}


def _run_ai_analysis(ticker: str, data: dict) -> dict:
    """Calls Claude Haiku for structured AI analysis. Caches result for TTL."""
    # Check cache
    cached = _AI_CACHE.get(ticker)
    if cached and (time.time() - cached["ts"]) < _CACHE_TTL_SECS:
        return cached["analysis"]

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return _fallback_ai_analysis(data)

    try:
        import anthropic, json as _json
        client = anthropic.Anthropic(api_key=api_key)
        prompt = (
            f"You are a quantitative trading assistant. Analyze this small-cap stock setup for {ticker}.\n"
            f"Data: composite_score={data.get('composite_score')}, "
            f"rvol={data.get('rvol')}, rsi={data.get('rsi')}, "
            f"sigma={data.get('sigma_hist')}, pct_change_today={data.get('pct_change_today')}, "
            f"catalyst={data.get('catalyst_text','none')}, short_interest={data.get('short_interest',0)}%\n\n"
            "Respond with ONLY a JSON object (no markdown, no code fences) with these exact fields:\n"
            "{\n"
            '  "conviction_rating": "Low|Medium|High|Very High",\n'
            '  "catalyst_quality": "Weak|Moderate|Strong|Very Strong",\n'
            '  "key_reason": "one sentence plain English reason to trade this",\n'
            '  "entry_suggestion": "price range or condition as a string",\n'
            '  "ai_stop_pct": <float percent below entry>,\n'
            '  "ai_target_pct": <float percent upside target>,\n'
            '  "ai_risk": "one phrase describing the main risk",\n'
            '  "time_sensitivity": "Act Now|Today|This Week"\n'
            "}"
        )
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        # Strip markdown fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        result = _json.loads(text)
        # Normalise keys
        analysis = {
            "conviction_rating":    str(result.get("conviction_rating", "Medium")),
            "catalyst_quality":     str(result.get("catalyst_quality", "Moderate")),
            "key_reason":           str(result.get("key_reason", "")),
            "entry_suggestion":     str(result.get("entry_suggestion", "")),
            "ai_stop_pct":          float(result.get("ai_stop_pct") or 5.0),
            "ai_target_pct":        float(result.get("ai_target_pct") or 15.0),
            "ai_risk":              str(result.get("ai_risk", "")),
            "time_sensitivity":     str(result.get("time_sensitivity", "Today")),
        }
        _AI_CACHE[ticker] = {"ts": time.time(), "analysis": analysis}
        return analysis
    except Exception:
        return _fallback_ai_analysis(data)


def _fallback_ai_analysis(data: dict) -> dict:
    """Returns a fallback AI analysis dict derived from raw data."""
    score = _safe(data.get("composite_score", 0))
    rvol  = _safe(data.get("rvol", 1.0))
    si    = _safe(data.get("short_interest", 0))
    catalyst = data.get("catalyst_text", "") or ""

    if score >= 85:
        conviction = "Very High"
    elif score >= 75:
        conviction = "High"
    elif score >= 65:
        conviction = "Medium"
    else:
        conviction = "Low"

    if "sec" in catalyst.lower() or "8-k" in catalyst.lower():
        cat_quality = "Very Strong"
    elif "earnings" in catalyst.lower() or "guidance" in catalyst.lower():
        cat_quality = "Strong"
    elif catalyst:
        cat_quality = "Moderate"
    else:
        cat_quality = "Weak"

    stop_pct   = round(_safe(data.get("sigma_hist", 0.05), 0.05) * 100 * 1.5, 1)
    target_pct = round(stop_pct * 2.5, 1)

    price = _safe(data.get("price", 0))
    entry_str = f"${price:.2f}" if price > 0 else "market"

    if si > 0.20:
        ai_risk = "High short interest — squeeze or squeeze unwind risk"
    elif rvol > 3.0:
        ai_risk = "Elevated volume — momentum may stall"
    else:
        ai_risk = "Low liquidity thin small-cap risk"

    if score >= 85 and rvol >= 2.0:
        time_sens = "Act Now"
    elif score >= 75:
        time_sens = "Today"
    else:
        time_sens = "This Week"

    return {
        "conviction_rating":  conviction,
        "catalyst_quality":   cat_quality,
        "key_reason":         f"{data.get('ticker','')} scores {score:.0f}/100 with {rvol:.1f}x RVOL.",
        "entry_suggestion":   entry_str,
        "ai_stop_pct":        stop_pct,
        "ai_target_pct":      target_pct,
        "ai_risk":            ai_risk,
        "time_sensitivity":   time_sens,
    }


def generate_live_conviction_list(session: str = "market") -> list:
    """
    Orchestrates a live conviction list refresh.
    Returns list of dicts with all fields including AI analysis.
    """
    engine = ConvictionEngine()
    candidates = engine._load_todays_candidates()

    # Filter to high-composite candidates
    candidates = [c for c in candidates if _safe(c.get("composite_score", 0)) >= 75]

    # Fetch live prices and filter out runaway movers
    enriched = []
    for c in candidates:
        ticker = c.get("ticker", "")
        live = _fetch_live_ticker(ticker)
        pct = live.get("pct_change_today", 0.0)
        if abs(pct) >= 20.0:  # already moved too much
            continue
        c["price"]            = live.get("price") or _safe(c.get("price", 0))
        c["pct_change_today"] = pct
        enriched.append(c)

    # Eligibility gate
    eligible = [c for c in enriched
                if engine.is_eligible(c["ticker"], c, session=session)]

    # Score and sort
    scored = sorted(eligible,
                    key=lambda d: engine.conviction_score(d["ticker"], d),
                    reverse=True)
    top5 = scored[:5]

    results = []
    for i, data in enumerate(top5, 1):
        ticker = data["ticker"]
        price  = _safe(data.get("price", 0))
        if price <= 0:
            continue
        params = engine.compute_trade_params(ticker, price, data)
        hold   = engine.classify_hold(ticker, data)
        ai     = _run_ai_analysis(ticker, data)

        results.append({
            "rank":               i,
            "ticker":             ticker,
            "score":              engine.conviction_score(ticker, data),
            "signal_label":       data.get("signal_label", "Strong Buy Candidate"),
            "price":              price,
            "pct_change_today":   data.get("pct_change_today", 0.0),
            "volume_ratio":       _safe(data.get("rvol", 1.0)),
            "short_interest":     _safe(data.get("short_percent_float", 0)) * 100,
            "catalyst_text":      data.get("catalyst_text", ""),
            "conviction":         params["conviction"],
            "hold_type":          hold,
            "entry":              params["entry"],
            "stop_loss":          params["stop_loss"],
            "stop_pct":           params["stop_pct"],
            "target_1":           params["target_1"],
            "target_2":           params["target_2"],
            "composite":          _safe(data.get("composite_score", 0)),
            # AI fields
            "ai_conviction":         ai.get("conviction_rating", ""),
            "ai_catalyst_quality":   ai.get("catalyst_quality", ""),
            "ai_key_reason":         ai.get("key_reason", ""),
            "ai_entry_suggestion":   ai.get("entry_suggestion", ""),
            "ai_stop_pct":           ai.get("ai_stop_pct", 0.0),
            "ai_target_pct":         ai.get("ai_target_pct", 0.0),
            "ai_risk":               ai.get("ai_risk", ""),
            "ai_time_sensitivity":   ai.get("time_sensitivity", ""),
        })

    save_live_conviction_list(results, session)
    return results


def save_live_conviction_list(buy_list: list, session: str) -> None:
    """Delete today's rows for session, then INSERT each entry with AI fields."""
    if not buy_list:
        return
    try:
        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
        today = date.today().isoformat()

        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute(
                "DELETE FROM conviction_buys WHERE date = %s AND session = %s",
                (today, session)
            )
            for b in buy_list:
                cur.execute("""
                    INSERT INTO conviction_buys
                        (date, session, rank, ticker, conviction, hold_type,
                         entry, stop_loss, target_1, target_2,
                         reasoning, composite, signal_label, pct_change_today,
                         volume_ratio, short_interest, catalyst_text,
                         ai_conviction, ai_catalyst_quality, ai_key_reason,
                         ai_entry_suggestion, ai_stop_pct, ai_target_pct,
                         ai_risk, ai_time_sensitivity)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    today, session, b["rank"], b["ticker"],
                    b.get("conviction"), b.get("hold_type"),
                    b.get("entry"), b.get("stop_loss"),
                    b.get("target_1"), b.get("target_2"),
                    b.get("ai_key_reason"), b.get("composite"),
                    b.get("signal_label"), b.get("pct_change_today"),
                    b.get("volume_ratio"), b.get("short_interest"),
                    b.get("catalyst_text"),
                    b.get("ai_conviction"), b.get("ai_catalyst_quality"),
                    b.get("ai_key_reason"), b.get("ai_entry_suggestion"),
                    b.get("ai_stop_pct"), b.get("ai_target_pct"),
                    b.get("ai_risk"), b.get("ai_time_sensitivity"),
                ))
            conn.commit()
            cur.close()
            conn.close()
        else:
            conn = _get_sqlite_conn()
            conn.execute(
                "DELETE FROM conviction_buys WHERE date=? AND session=?",
                (today, session)
            )
            for b in buy_list:
                conn.execute("""
                    INSERT INTO conviction_buys
                        (date, session, rank, ticker, conviction, hold_type,
                         entry, stop_loss, target_1, target_2,
                         reasoning, composite, signal_label, pct_change_today,
                         volume_ratio, short_interest, catalyst_text,
                         ai_conviction, ai_catalyst_quality, ai_key_reason,
                         ai_entry_suggestion, ai_stop_pct, ai_target_pct,
                         ai_risk, ai_time_sensitivity)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    today, session, b["rank"], b["ticker"],
                    b.get("conviction"), b.get("hold_type"),
                    b.get("entry"), b.get("stop_loss"),
                    b.get("target_1"), b.get("target_2"),
                    b.get("ai_key_reason"), b.get("composite"),
                    b.get("signal_label"), b.get("pct_change_today"),
                    b.get("volume_ratio"), b.get("short_interest"),
                    b.get("catalyst_text"),
                    b.get("ai_conviction"), b.get("ai_catalyst_quality"),
                    b.get("ai_key_reason"), b.get("ai_entry_suggestion"),
                    b.get("ai_stop_pct"), b.get("ai_target_pct"),
                    b.get("ai_risk"), b.get("ai_time_sensitivity"),
                ))
            conn.commit()
            conn.close()
        print(f"  [conviction] Saved {len(buy_list)} live conviction entries for session={session}")
    except Exception as e:
        print(f"  [conviction] save_live_conviction_list failed: {e}")


def get_latest_conviction_list(max_age_minutes: int = 60) -> dict:
    """
    Query conviction_buys for today's most recent session.
    Falls back to yesterday if no rows today.
    Returns dict with entries, generated_at, session, is_stale, is_yesterday.
    """
    try:
        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
        today = date.today().isoformat()
        yesterday = (date.today() - timedelta(days=1)).isoformat()

        def _query(conn_fn, ph, today_val, yest_val):
            conn = conn_fn()
            try:
                if hasattr(conn, "cursor"):
                    cur = conn.cursor()
                    cur.execute(
                        f"SELECT * FROM conviction_buys WHERE date = {ph} ORDER BY created_at DESC LIMIT 50",
                        (today_val,)
                    )
                    rows = cur.fetchall()
                    cols = [d[0] for d in cur.description]
                    cur.close()
                    is_yest = False
                    if not rows:
                        cur = conn.cursor()
                        cur.execute(
                            f"SELECT * FROM conviction_buys WHERE date = {ph} ORDER BY created_at DESC LIMIT 50",
                            (yest_val,)
                        )
                        rows = cur.fetchall()
                        cols = [d[0] for d in cur.description]
                        cur.close()
                        is_yest = True
                else:
                    rows = conn.execute(
                        f"SELECT * FROM conviction_buys WHERE date = {ph} ORDER BY created_at DESC LIMIT 50",
                        (today_val,)
                    ).fetchall()
                    cols = [d[0] for d in conn.execute(
                        f"SELECT * FROM conviction_buys WHERE date = {ph} ORDER BY created_at DESC LIMIT 1",
                        (today_val,)
                    ).description] if rows else []
                    is_yest = False
                    if not rows:
                        rows = conn.execute(
                            f"SELECT * FROM conviction_buys WHERE date = {ph} ORDER BY created_at DESC LIMIT 50",
                            (yest_val,)
                        ).fetchall()
                        is_yest = True
            finally:
                conn.close()
            return rows, cols if rows else [], is_yest

        if _is_postgres():
            rows, cols, is_yest = _query(_get_pg_conn, "%s", today, yesterday)
        else:
            # For SQLite use a simpler approach
            conn = _get_sqlite_conn()
            rows = conn.execute(
                "SELECT * FROM conviction_buys WHERE date = ? ORDER BY created_at DESC LIMIT 50",
                (today,)
            ).fetchall()
            cols_raw = conn.execute("PRAGMA table_info(conviction_buys)").fetchall()
            cols = [r[1] for r in cols_raw]
            is_yest = False
            if not rows:
                rows = conn.execute(
                    "SELECT * FROM conviction_buys WHERE date = ? ORDER BY created_at DESC LIMIT 50",
                    (yesterday,)
                ).fetchall()
                is_yest = True
            conn.close()

        if not rows:
            return {"entries": [], "generated_at": None, "session": "", "is_stale": True, "is_yesterday": False}

        entries_raw = [dict(zip(cols, r)) for r in rows]

        # Deduplicate by ticker (keep first/best rank)
        seen_tickers = set()
        entries = []
        for e in entries_raw:
            t = e.get("ticker", "")
            if t not in seen_tickers:
                seen_tickers.add(t)
                entries.append(e)

        # Determine session and generated_at
        session = entries[0].get("session", "") if entries else ""
        gen_at_raw = entries[0].get("created_at") if entries else None
        try:
            if gen_at_raw:
                gen_at = datetime.fromisoformat(str(gen_at_raw).replace("Z", "+00:00"))
            else:
                gen_at = None
        except Exception:
            gen_at = gen_at_raw

        # Stale check
        is_stale = True
        if gen_at and not is_yest:
            try:
                now = datetime.now(gen_at.tzinfo) if gen_at.tzinfo else datetime.now()
                age_mins = (now - gen_at).total_seconds() / 60
                is_stale = age_mins > max_age_minutes
            except Exception:
                is_stale = True

        return {
            "entries":      entries,
            "generated_at": gen_at,
            "session":      session,
            "is_stale":     is_stale,
            "is_yesterday": is_yest,
        }
    except Exception as e:
        print(f"  [conviction] get_latest_conviction_list failed: {e}")
        return {"entries": [], "generated_at": None, "session": "", "is_stale": True, "is_yesterday": False}


def get_conviction_win_rate() -> dict:
    """
    Join conviction_buys with signal_log and signal_outcomes to get resolved 5-day outcomes.
    Returns dict with win_rate, n, avg_gain.
    """
    try:
        from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn

        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT
                    COUNT(*)                                       AS n,
                    SUM(CASE WHEN so.ret_5d > 0 THEN 1 ELSE 0 END) AS wins,
                    AVG(CASE WHEN so.ret_5d > 0 THEN so.ret_5d ELSE NULL END) AS avg_gain
                FROM conviction_buys cb
                JOIN signal_log sl
                  ON sl.ticker = cb.ticker AND DATE(sl.created_at) = cb.date
                JOIN signal_outcomes so
                  ON so.signal_id = sl.id
                WHERE so.ret_5d IS NOT NULL
            """)
            row = cur.fetchone()
            cur.close()
            conn.close()
        else:
            conn = _get_sqlite_conn()
            row = conn.execute("""
                SELECT
                    COUNT(*),
                    SUM(CASE WHEN so.ret_5d > 0 THEN 1 ELSE 0 END),
                    AVG(CASE WHEN so.ret_5d > 0 THEN so.ret_5d ELSE NULL END)
                FROM conviction_buys cb
                JOIN signal_log sl
                  ON sl.ticker = cb.ticker AND DATE(sl.created_at) = cb.date
                JOIN signal_outcomes so
                  ON so.signal_id = sl.id
                WHERE so.ret_5d IS NOT NULL
            """).fetchone()
            conn.close()

        if not row or row[0] == 0:
            return {"win_rate": None, "n": 0, "avg_gain": None}

        n, wins, avg_gain = row
        n    = int(n or 0)
        wins = int(wins or 0)
        win_rate = (wins / n * 100) if n > 0 else None
        avg_gain = float(avg_gain) if avg_gain is not None else None
        return {"win_rate": win_rate, "n": n, "avg_gain": avg_gain}
    except Exception as e:
        print(f"  [conviction] get_conviction_win_rate failed: {e}")
        return {"win_rate": None, "n": 0, "avg_gain": None}


def build_conviction_pdf(entries: list, generated_at=None, session: str = "market") -> bytes:
    """Build a clean professional PDF conviction list. Returns bytes."""
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib import colors
        from reportlab.lib.units import inch
        from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                        HRFlowable, Table, TableStyle)
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_CENTER, TA_LEFT
        import io

        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=letter,
            rightMargin=0.75 * inch,
            leftMargin=0.75 * inch,
            topMargin=0.75 * inch,
            bottomMargin=0.75 * inch,
        )

        styles = getSampleStyleSheet()
        style_normal = styles["Normal"]

        title_style = ParagraphStyle(
            "AxiomTitle",
            fontName="Helvetica-Bold",
            fontSize=18,
            spaceAfter=4,
            textColor=colors.HexColor("#0f172a"),
        )
        sub_style = ParagraphStyle(
            "AxiomSub",
            fontName="Helvetica",
            fontSize=10,
            spaceAfter=2,
            textColor=colors.HexColor("#64748b"),
        )
        section_style = ParagraphStyle(
            "AxiomSection",
            fontName="Helvetica-Bold",
            fontSize=12,
            spaceBefore=8,
            spaceAfter=4,
            textColor=colors.HexColor("#1d3461"),
        )
        body_style = ParagraphStyle(
            "AxiomBody",
            fontName="Helvetica",
            fontSize=9,
            spaceAfter=2,
            textColor=colors.HexColor("#1e293b"),
            leading=13,
        )
        small_style = ParagraphStyle(
            "AxiomSmall",
            fontName="Helvetica",
            fontSize=8,
            textColor=colors.HexColor("#64748b"),
            leading=11,
        )
        footer_style = ParagraphStyle(
            "AxiomFooter",
            fontName="Helvetica",
            fontSize=7,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#94a3b8"),
        )

        # Format generated_at
        if generated_at:
            try:
                gdt = generated_at if isinstance(generated_at, datetime) else \
                    datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
                gen_str = gdt.strftime("%A, %B %-d %Y  %-I:%M %p ET")
            except Exception:
                gen_str = str(generated_at)[:16]
        else:
            gen_str = datetime.now().strftime("%A, %B %-d %Y  %-I:%M %p ET")

        story = []

        # Header
        story.append(Paragraph("Axiom Terminal — Conviction List", title_style))
        story.append(Paragraph(f"Generated: {gen_str}  |  Session: {session.upper()}", sub_style))
        story.append(HRFlowable(width="100%", thickness=1.5, color=colors.HexColor("#1d3461"),
                                spaceAfter=10))

        _CONV_MAP = {"Very High": "#16a34a", "High": "#1d6fa5", "Medium": "#c2610f", "Low": "#94a3b8"}
        _CAT_MAP  = {"Very Strong": "#16a34a", "Strong": "#1d6fa5", "Moderate": "#c2610f", "Weak": "#94a3b8"}

        for e in entries:
            ticker = e.get("ticker", "")
            rank   = e.get("rank", "?")
            score  = float(e.get("composite") or e.get("score") or e.get("conviction") or 0)
            pct    = float(e.get("pct_change_today") or 0)
            vol    = float(e.get("volume_ratio") or e.get("rvol") or 1.0)
            si     = float(e.get("short_interest") or 0)
            ai_c   = e.get("ai_conviction", "")
            ai_cat = e.get("ai_catalyst_quality", "")
            reason = e.get("ai_key_reason") or e.get("reasoning", "—")
            entry  = float(e.get("entry") or 0)
            stop_p = float(e.get("ai_stop_pct") or e.get("stop_pct") or 0)
            tgt_p  = float(e.get("ai_target_pct") or 0)
            ai_entry = e.get("ai_entry_suggestion") or (f"${entry:.2f}" if entry else "market")
            ai_risk  = e.get("ai_risk", "")
            time_s   = e.get("ai_time_sensitivity", "—")
            hold_t   = e.get("hold_type", "")
            sig_lbl  = e.get("signal_label", "")

            conv_c = _CONV_MAP.get(ai_c, "#94a3b8")
            cat_c  = _CAT_MAP.get(ai_cat, "#94a3b8")

            pct_sign = "+" if pct >= 0 else ""
            story.append(Paragraph(
                f'<b>Rank {rank} — {ticker}</b>  <font size="10" color="#64748b">Score: {score:.0f}/100</font>',
                section_style
            ))
            story.append(Paragraph(
                f'{sig_lbl}  |  Today: {pct_sign}{pct:.1f}%  |  Vol: {vol:.1f}x'
                f'{f"  |  SI: {si:.1f}%" if si else ""}',
                small_style
            ))
            story.append(Paragraph(
                f'<font color="{conv_c}"><b>Conviction: {ai_c}</b></font>'
                f'&nbsp;&nbsp;&nbsp;<font color="{cat_c}">Catalyst: {ai_cat}</font>',
                body_style
            ))
            story.append(Paragraph(reason, body_style))
            story.append(Paragraph(
                f'Entry: {ai_entry}  |  Stop: {stop_p:.1f}% below  |  Target: +{tgt_p:.0f}% upside',
                small_style
            ))
            if ai_risk:
                story.append(Paragraph(f'Risk: {ai_risk}', small_style))
            story.append(Paragraph(
                f'Time Sensitivity: {time_s}  |  Hold: {hold_t}',
                small_style
            ))
            story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#e2e8f0"),
                                    spaceBefore=6, spaceAfter=4))

        story.append(Spacer(1, 0.2 * inch))
        story.append(Paragraph(
            "Research purposes only. Not financial advice. Past performance does not guarantee future results.",
            footer_style
        ))

        doc.build(story)
        return buf.getvalue()
    except Exception as e:
        print(f"  [conviction] build_conviction_pdf failed: {e}")
        return b""


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
