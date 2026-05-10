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
    Main function — scans the universe and returns today's active watchlist.

    Process:
    1. Get all US tickers
    2. Filter to small/mid-cap range
    3. Quick screen each for activity
    4. Add news and SEC filing bonus scores
    5. Return top N by activity score
    """
    print(f"\n{'='*50}")
    print(f"Axiom Terminal Morning Screen — {datetime.now().strftime('%Y-%m-%d %H:%M ET')}")
    print(f"{'='*50}")

    # Step 1: Get universe
    all_tickers = get_all_us_tickers()

    # Step 2: Filter to reasonable candidates
    # Use a pre-filter based on price range to avoid API waste
    # We'll focus on stocks $0.50-$50 which covers our target universe
    candidate_tickers = all_tickers[:3000]  # Cap at 3000 for rate limit safety
    print(f"  Screening {len(candidate_tickers)} candidates...")

    # Step 3: Parallel quick screen
    # Use 10 threads, each making 1 API call per ticker
    # Rate limit: 60 calls/min = we pace to 50 calls/min to be safe
    interesting = []
    batch_size  = 50   # Process 50 at a time, then pause

    for batch_start in range(0, min(len(candidate_tickers), 1500), batch_size):
        batch = candidate_tickers[batch_start:batch_start + batch_size]

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(quick_screen_ticker, t): t for t in batch}
            for future in as_completed(futures):
                result = future.result()
                if result and result["activity"] > 0:
                    interesting.append(result)

        # Pace to stay under rate limit
        time.sleep(1.2)

        if len(interesting) >= max_stocks * 3:
            break  # Have enough candidates, stop early

    print(f"  Found {len(interesting)} active stocks in initial screen")

    if not interesting:
        print("  No interesting stocks found — using default universe")
        from config import DEFAULT_UNIVERSE
        return DEFAULT_UNIVERSE[:max_stocks]

    # Step 4: Sort by activity score
    interesting.sort(key=lambda x: x["activity"], reverse=True)
    top_candidates = [s["ticker"] for s in interesting[:max_stocks * 2]]

    # Step 5: Check SEC filings for top candidates (adds bonus score)
    print(f"  Checking SEC filings for top {len(top_candidates)} candidates...")
    filing_counts = screen_for_sec_filings(top_candidates)

    # Add filing bonus to activity scores
    for s in interesting:
        if s["ticker"] in filing_counts:
            s["activity"] += filing_counts[s["ticker"]] * 30

    # Re-sort with filing bonus
    interesting.sort(key=lambda x: x["activity"], reverse=True)

    # Step 6: Take the top stocks
    final_watchlist = [s["ticker"] for s in interesting[:max_stocks]]

    # Always include portfolio holdings in watchlist
    try:
        from db.database import get_portfolio
        portfolio_df = get_portfolio()
        if not portfolio_df.empty:
            for ticker in portfolio_df["ticker"].tolist():
                if ticker not in final_watchlist:
                    final_watchlist.append(ticker)
    except Exception:
        pass

    print(f"\n  Today's watchlist ({len(final_watchlist)} stocks):")
    print(f"  {', '.join(final_watchlist[:20])}{'...' if len(final_watchlist) > 20 else ''}")

    # Save watchlist to file for scanner_loop to read
    with open(WATCHLIST_FILE, "w") as f:
        json.dump({
            "date":      datetime.now().strftime("%Y-%m-%d"),
            "generated": datetime.now().isoformat(),
            "tickers":   final_watchlist,
            "stats": {
                "screened":   len(candidate_tickers),
                "interesting": len(interesting),
                "watchlist":  len(final_watchlist),
                "gap_ups":    [s["ticker"] for s in interesting if s["change_pct"] > 3][:10],
                "gap_downs":  [s["ticker"] for s in interesting if s["change_pct"] < -3][:10],
                "new_filings": list(filing_counts.keys())[:10],
            }
        }, f, indent=2)

    # Also save to shared PostgreSQL database so dashboard can read it
    try:
        from db.database import save_watchlist
        stats = {
            "screened":    len(candidate_tickers),
            "interesting": len(interesting),
            "watchlist":   len(final_watchlist),
            "gap_ups":     [s["ticker"] for s in interesting if s["change_pct"] > 3][:10],
            "gap_downs":   [s["ticker"] for s in interesting if s["change_pct"] < -3][:10],
            "new_filings": list(filing_counts.keys())[:10],
        }
        save_watchlist(final_watchlist, stats)
        print("  ✓ Watchlist saved to shared database")
    except Exception as e:
        print(f"  [db] Watchlist DB save failed: {e}")

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
