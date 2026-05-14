# morning_screen.py
# Runs at 6:00 AM ET to build today's dynamic watchlist.
# Scans the entire small/mid-cap universe and picks the
# most active/interesting stocks to monitor during the day.
#
# Returns the top 30-50 stocks that have something going on today.

import os
import requests
import yfinance as yf
import pandas as pd
import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

from config import MIN_MARKET_CAP, MAX_MARKET_CAP, MIN_AVG_VOLUME

FINNHUB_BASE = "https://finnhub.io/api/v1"
WATCHLIST_FILE = "watchlist_today.json"


def _fh_key() -> str:
    return os.environ.get("FINNHUB_API_KEY", "")


def _fh_get(endpoint: str, params: dict) -> Optional[dict]:
    key = _fh_key()
    if not key:
        return None
    try:
        params["token"] = key
        resp = requests.get(f"{FINNHUB_BASE}/{endpoint}", params=params, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            return data if data else None
        return None
    except Exception:
        return None


def get_all_us_tickers() -> List[str]:
    """
    Pull every US-listed stock symbol from Finnhub.
    Returns list of ticker strings.
    """
    print("  Fetching full US stock universe from Finnhub...")
    data = _fh_get("stock/symbol", {"exchange": "US", "mic": "XNYS,XNAS"})
    if not data:
        # Fallback to a curated small-cap list
        print("  Finnhub symbol fetch failed — using curated universe")
        return _get_curated_universe()

    tickers = []
    for item in data:
        ticker = item.get("symbol", "")
        # Skip warrants, preferred shares, ETFs, funds
        if any(x in ticker for x in [".", "-", "W", "U", "R"]):
            continue
        if item.get("type") not in ("Common Stock", "EQS", ""):
            continue
        tickers.append(ticker)

    print(f"  Found {len(tickers)} US stock symbols")
    return tickers


def _get_curated_universe() -> List[str]:
    """Fallback curated small-cap universe if Finnhub symbol fetch fails."""
    return [
        "LAES","WULF","IREN","CIFR","RKLB","SOUN","BBAI","NKLA","IONQ",
        "ARQQ","GFAI","CLSK","MARA","RIOT","BTBT","HIMS","ACHR","JOBY",
        "LUNR","RDDT","RGTI","QBTS","QUBT","CELH","BLNK","CHPT","MSTR",
        "COIN","HOOD","SOFI","AFRM","UPST","OPEN","OPRA","DKNG","PENN",
        "BYND","OUST","VLDR","GOEV","XPEV","NIO","LI","RIVN","LCID",
        "PLUG","FCEL","BLOOM","BE","NOVA","RUN","SPWR","ENPH","SEDG",
        "NVAX","MRNA","BNTX","SGEN","ALNY","BMRN","BLUE","FATE","NTLA",
        "BEAM","CRSP","EDIT","SGMO","GRPH","VERV","PRCT","AXNX","NVCR",
    ]


def quick_screen_ticker(ticker: str) -> Optional[Dict]:
    """
    Quick screen a single ticker for today's activity score.
    Uses Finnhub quote only (1 API call) for speed.
    Returns a dict with activity score or None if not interesting.
    """
    try:
        # Finnhub quote
        quote = _fh_get("quote", {"symbol": ticker})
        if not quote or not quote.get("c") or quote["c"] <= 0:
            return None

        price      = quote["c"]
        prev_close = quote["pc"]
        high       = quote["h"]
        low        = quote["l"]
        volume     = quote.get("v", 0)

        if prev_close <= 0:
            return None

        # Price change
        change_pct = (price - prev_close) / prev_close * 100

        # Activity score — higher = more interesting
        activity = 0

        # Big price move
        if abs(change_pct) > 10: activity += 40
        elif abs(change_pct) > 5: activity += 20
        elif abs(change_pct) > 3: activity += 10

        # Gap up/down (open vs prev close)
        # We use high-low range as a proxy for volatility today
        if prev_close > 0:
            day_range_pct = (high - low) / prev_close * 100
            if day_range_pct > 15: activity += 20
            elif day_range_pct > 8: activity += 10

        return {
            "ticker":      ticker,
            "price":       price,
            "change_pct":  round(change_pct, 2),
            "volume":      volume,
            "activity":    activity,
        }

    except Exception:
        return None


def screen_for_news(ticker: str) -> int:
    """Check if ticker has recent news. Returns news count."""
    try:
        today     = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        news = _fh_get("company-news", {"symbol": ticker, "from": yesterday, "to": today})
        return len(news) if news else 0
    except Exception:
        return 0


def screen_for_sec_filings(tickers: List[str]) -> Dict[str, int]:
    """
    Check EDGAR RSS for recent filings across all tickers at once.
    Returns dict of {ticker: filing_count} for tickers with recent filings.
    """
    import feedparser
    results = {}
    try:
        # EDGAR recent filings RSS — covers all companies
        feed = feedparser.parse(
            "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent"
            "&type=8-K&dateb=&owner=include&count=40&search_text=&output=atom"
        )
        ticker_set = set(t.upper() for t in tickers)
        for entry in feed.entries:
            title = entry.get("title", "").upper()
            for ticker in ticker_set:
                if ticker in title:
                    results[ticker] = results.get(ticker, 0) + 1
    except Exception as e:
        print(f"  [screen] EDGAR RSS check failed: {e}")
    return results


def build_todays_watchlist(max_stocks: int = 50) -> List[str]:
    """
    Build today's dynamic watchlist from the full 2000+ small-cap universe.

    Process:
      1. Load universe from DB (stock_universe table, 2000+ tickers)
      2. Batch pre-screen with Finnhub quotes (activity score: gap + rvol proxy)
      3. SEC filing bonus for top candidates
      4. Full multi-factor quant scoring on the top ~150 candidates
      5. Flag mean-reversion setups (z_sma20 < -2 AND composite > 65)
      6. Return top max_stocks by composite score
    """
    print(f"\n{'='*60}")
    print(f"Axiom Terminal Morning Screen — {datetime.now().strftime('%Y-%m-%d %H:%M ET')}")
    print(f"{'='*60}")

    # ── Step 1: Universe ──────────────────────────────────────────────────────
    try:
        from universe_manager import get_universe_tickers, get_universe_size
        all_tickers = get_universe_tickers(limit=3000)
        universe_size = get_universe_size()
        print(f"  Universe: {universe_size} cached | using {len(all_tickers)} for screen")
    except Exception as _ue:
        print(f"  [screen] universe_manager failed ({_ue}) — falling back to Finnhub symbol list")
        all_tickers = get_all_us_tickers()
        universe_size = len(all_tickers)

    if not all_tickers:
        from config import DEFAULT_UNIVERSE
        return DEFAULT_UNIVERSE[:max_stocks]

    # ── Step 2: Batch pre-screen (Finnhub quotes, ~2 min for 2000 stocks) ────
    print(f"  Pre-screening {len(all_tickers)} tickers via Finnhub quotes...")
    try:
        from quant.factor_engine import batch_pre_screen
        pre_results = batch_pre_screen(all_tickers, max_workers=25, timeout_per_ticker=3.0)
    except Exception as _bse:
        print(f"  [screen] batch_pre_screen failed ({_bse}) — using sequential fallback")
        pre_results = []
        for batch_start in range(0, min(len(all_tickers), 2000), 50):
            batch = all_tickers[batch_start:batch_start + 50]
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(quick_screen_ticker, t): t for t in batch}
                for future in as_completed(futures):
                    try:
                        r = future.result()
                        if r:
                            pre_results.append((r["ticker"], r["activity"],
                                                r["price"], r["change_pct"], 1.0))
                    except Exception:
                        pass
            time.sleep(0.8)
            if len(pre_results) >= max_stocks * 8:
                break

    # Convert to dict for fast lookup
    pre_map = {r[0]: {"price": r[2], "change_pct": r[3], "activity": r[1]}
               for r in pre_results if r[1] > 0}

    interesting = sorted(pre_map.items(), key=lambda x: x[1]["activity"], reverse=True)
    top_activity = [t for t, _ in interesting[:max_stocks * 5]]  # top 250
    print(f"  {len(pre_map)} active stocks | top {len(top_activity)} for deep screen")

    if not top_activity:
        from config import DEFAULT_UNIVERSE
        return DEFAULT_UNIVERSE[:max_stocks]

    # ── Step 3: SEC filing bonus ──────────────────────────────────────────────
    print(f"  Checking SEC filings...")
    filing_counts = screen_for_sec_filings(top_activity[:200])
    for t in top_activity:
        if t in filing_counts:
            pre_map[t]["activity"] = pre_map[t].get("activity", 0) + filing_counts[t] * 35
    # Re-sort
    top_activity = sorted(top_activity, key=lambda t: pre_map.get(t, {}).get("activity", 0), reverse=True)

    # ── Step 4: Full factor scoring on top ~150 candidates ────────────────────
    factor_top_n = min(150, len(top_activity))
    factor_candidates = top_activity[:factor_top_n]
    print(f"  Running multi-factor scoring on top {factor_top_n} candidates...")

    scored_stocks: List[Dict] = []
    mean_rev_setups: List[Dict] = []

    # Get current regime for factor tilt
    regime = "NEUTRAL"
    try:
        from analysis.regime import get_current_regime
        regime = get_current_regime().get("regime", "NEUTRAL")
    except Exception:
        pass

    quant_mode = os.environ.get("AXIOM_QUANT_MODE", "1") == "1"

    def _score_one(ticker: str) -> Optional[Dict]:
        try:
            act_info = pre_map.get(ticker, {})
            price = act_info.get("price", 0)
            if price <= 0:
                return None

            snapshot = {
                "price":    price,
                "news_sentiment_score": 50,
                "sec_catalyst_score":  float(bool(ticker in filing_counts)),
            }

            if quant_mode:
                import yfinance as yf
                try:
                    hist = yf.Ticker(ticker).history(period="1y", auto_adjust=True)
                except Exception:
                    hist = pd.DataFrame()

                # Enrich snapshot with fundamentals
                try:
                    fi = yf.Ticker(ticker).fast_info
                    snapshot["gross_margins"]    = None
                    snapshot["revenue_growth"]   = None
                    snapshot["current_ratio"]    = None
                    snapshot["debtToEquity"]     = None
                    info = yf.Ticker(ticker).info
                    snapshot["gross_margins"]   = info.get("grossMargins")
                    snapshot["revenue_growth"]  = info.get("revenueGrowth")
                    snapshot["current_ratio"]   = info.get("currentRatio")
                    snapshot["debtToEquity"]    = info.get("debtToEquity")
                    snapshot["market_cap"]      = info.get("marketCap", 0)
                    snapshot["avg_volume"]      = info.get("averageVolume", 0)
                    snapshot["short_percent_float"] = info.get("shortPercentOfFloat")
                except Exception:
                    hist = pd.DataFrame() if hist.empty else hist

                from quant.factor_engine import MultiFactorScorer
                scorer = MultiFactorScorer()
                result = scorer.score_ticker(
                    ticker, snapshot, hist if not hist.empty else None,
                    regime=regime, log_factors=True
                )
                composite = result.get("composite_score", 50.0)
                factor_zs = result.get("factor_z_scores", {})
            else:
                # Simple activity-based score fallback
                act = act_info.get("activity", 0)
                composite = min(85, 40 + act * 0.8)
                factor_zs = {}

            chg = act_info.get("change_pct", 0)
            row = {
                "ticker":          ticker,
                "composite_score": composite,
                "price":           price,
                "change_pct":      round(chg, 2),
                "activity":        act_info.get("activity", 0),
                "has_filing":      ticker in filing_counts,
                "factor_z_scores": factor_zs,
                "regime":          regime,
            }

            # Mean reversion setup: near lower Bollinger Band AND decent composite
            z_sma20 = factor_zs.get("z_sma20", 0)
            if z_sma20 < -1.5 and composite > 60:
                row["mean_rev_setup"] = True
            return row
        except Exception as _e:
            print(f"  [screen] {ticker} scoring error: {_e}")
            return None

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_score_one, t): t for t in factor_candidates}
        for fut in as_completed(futs, timeout=300):
            try:
                r = fut.result()
                if r:
                    scored_stocks.append(r)
                    if r.get("mean_rev_setup"):
                        mean_rev_setups.append(r)
            except Exception:
                pass

    scored_stocks.sort(key=lambda x: x["composite_score"], reverse=True)
    mean_rev_setups.sort(key=lambda x: x["composite_score"], reverse=True)
    print(f"  Scored {len(scored_stocks)} stocks | {len(mean_rev_setups)} mean-reversion setups")

    # ── Step 5: Final watchlist ───────────────────────────────────────────────
    # Top by composite + mean reversion setups + portfolio holdings
    top_by_score = [s["ticker"] for s in scored_stocks[:max_stocks]]
    # Add mean reversion setups (up to 5)
    for s in mean_rev_setups[:5]:
        if s["ticker"] not in top_by_score:
            top_by_score.append(s["ticker"])
    # Add portfolio holdings
    try:
        from db.database import get_portfolio
        pf = get_portfolio()
        if not pf.empty:
            for t in pf["ticker"].tolist():
                if t not in top_by_score:
                    top_by_score.append(t)
    except Exception:
        pass

    final_watchlist = top_by_score[:max_stocks + 10]
    print(f"\n  Today's watchlist: {len(final_watchlist)} stocks | regime: {regime}")
    print(f"  Top: {', '.join(final_watchlist[:12])}...")
    if mean_rev_setups:
        print(f"  Mean-Rev setups: {', '.join(s['ticker'] for s in mean_rev_setups[:5])}")

    # ── Step 6: Persist ───────────────────────────────────────────────────────
    gap_ups   = [s["ticker"] for s in scored_stocks if s["change_pct"] > 3][:10]
    gap_downs = [s["ticker"] for s in scored_stocks if s["change_pct"] < -3][:10]

    stats = {
        "screened":        len(all_tickers),
        "universe_size":   universe_size,
        "pre_screened":    len(pre_map),
        "factor_scored":   len(scored_stocks),
        "watchlist":       len(final_watchlist),
        "gap_ups":         gap_ups,
        "gap_downs":       gap_downs,
        "new_filings":     list(filing_counts.keys())[:10],
        "mean_rev_setups": [s["ticker"] for s in mean_rev_setups[:5]],
        "regime":          regime,
        "top_scores":      [{"ticker": s["ticker"], "score": s["composite_score"]}
                            for s in scored_stocks[:10]],
    }

    try:
        with open(WATCHLIST_FILE, "w") as f:
            json.dump({
                "date":      datetime.now().strftime("%Y-%m-%d"),
                "generated": datetime.now().isoformat(),
                "tickers":   final_watchlist,
                "stats":     stats,
            }, f, indent=2)
    except Exception as _fje:
        print(f"  [screen] JSON save failed: {_fje}")

    try:
        from db.database import save_watchlist
        save_watchlist(final_watchlist, stats)
        print("  ✓ Watchlist saved to DB")
    except Exception as _dbe:
        print(f"  [screen] DB save failed: {_dbe}")

    return final_watchlist


def load_todays_watchlist() -> List[str]:
    """Load today's watchlist from file, or trigger a new screen if stale."""
    try:
        with open(WATCHLIST_FILE, "r") as f:
            data = json.load(f)

        # Check if watchlist is from today
        if data.get("date") == datetime.now().strftime("%Y-%m-%d"):
            tickers = data.get("tickers", [])
            if tickers:
                print(f"  Loaded today's watchlist: {len(tickers)} stocks")
                return tickers

    except (FileNotFoundError, json.JSONDecodeError):
        pass

    # No valid watchlist — build one now
    print("  No watchlist found — running morning screen...")
    return build_todays_watchlist()


if __name__ == "__main__":
    # Run standalone for testing
    watchlist = build_todays_watchlist()
    print(f"\nFinal watchlist: {watchlist}")
