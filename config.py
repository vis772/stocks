# config.py  (v3 — post 600-signal audit 2026-05-22)
# Central configuration for the scanner.
# Edit these values to tune the system to your preferences.

import os
from dataclasses import dataclass, field
from typing import List

# ─── Market Cap Filters ────────────────────────────────────────────────────────
# Small-cap: $20M–$2B | Mid-cap: $2B–$20B | Large-cap: $20B–$200B
MIN_MARKET_CAP = 20_000_000         # $20M floor
MAX_MARKET_CAP = 200_000_000_000    # $200B ceiling — includes large-cap names

# ─── Volume Filters ────────────────────────────────────────────────────────────
MIN_AVG_VOLUME = 500_000           # Minimum average daily volume (liquidity floor)
UNUSUAL_VOLUME_MULTIPLIER = 2.0    # Flag if today's volume > 2x the 20-day average

# ─── Technical Thresholds ──────────────────────────────────────────────────────
RSI_OVERSOLD = 35                  # Below this = potentially oversold (not a buy signal alone)
RSI_OVERBOUGHT = 72                # Above this = extended, caution on new entries
PRICE_MIN = 0.50                   # Minimum price — below this is near-OTC territory
PRICE_MAX = 1000.00                # Maximum price — raised to include large-cap names

# ─── Scoring Weights ───────────────────────────────────────────────────────────
# Derived from 600-signal component-correlation audit (2026-05-22):
#   Technical +20.6 differential | Fundamental +11.9 | Risk +8.5 | Sentiment +4.6
#   Catalyst dropped — 0.0 differential across 600 signals.
#
# Weight rationale: scale proportionally to predictive difference, then
# round to sensible values that sum to 1.0.
#   Raw diff: tech=20.6, fund=11.9, risk=8.5, sent=4.6  total=45.6
#   tech share: 20.6/45.6 = 45% → 0.50 (momentum scanner — round up)
#   fund share: 11.9/45.6 = 26% → 0.22 (quality gate, not predictor)
#   risk share:  8.5/45.6 = 19% → 0.20 (unchanged — dilution protection)
#   sent share:  4.6/45.6 = 10% → 0.08 (slight positive signal)
SCORING_WEIGHTS = {
    "technical":    0.50,   # +20.6 diff — momentum, gap, RSI, SMA crossovers
    "fundamental":  0.22,   # +11.9 diff — revenue growth, balance sheet quality
    "risk":         0.20,   # +8.5  diff — dilution, short interest, volatility
    "sentiment":    0.08,   # +4.6  diff — news tone, catalyst keywords
}

# Validate weights sum to 1.0
assert abs(sum(SCORING_WEIGHTS.values()) - 1.0) < 0.001, "Scoring weights must sum to 1.0"

# ─── Signal Thresholds ─────────────────────────────────────────────────────────
# Only two actionable labels — everything below 62 showed ≤4% win rate in 600-signal audit.
# Watchlist / Hold / Trim / Sell / Avoid are retained for the UI display layer only;
# they are never written to signal_log (MIN_SIGNAL_SCORE gate below prevents it).
SIGNAL_THRESHOLDS = {
    "Strong Buy Candidate": (75, 100),
    "Speculative Buy":      (62, 75),
    "Watchlist":            (50, 62),   # display only — never logged (below MIN_SIGNAL_SCORE)
    "Hold":                 (38, 50),   # display only
    "Trim":                 (25, 38),   # display only
    "Avoid":                (0,  25),   # display only
}

# Minimum score to log to signal_log.
# Raised from 45 → 62 after 600-signal audit showed:
#   Watchlist (45-60): 4% win rate (149 signals, pure noise)
#   Hold (35-45):      0% win rate (48 signals)
#   Trim (25-35):      0% win rate (18 signals)
# Only Speculative Buy (60+) and Gap-Up have positive expectancy.
MIN_SIGNAL_SCORE = 62
ALERT_SCORE_MIN  = 68   # push alert bar — raised from 60, matches conviction gate

# ─── Strong Buy Extension Guard ───────────────────────────────────────────────
# 600-signal audit: Strong Buy (75-100) had 16.1% win rate vs Speculative Buy 20.5%.
# Root cause: high-composite-score setups are often "climax" setups — stock already
# extended, RSI elevated, 5d return baked in. Cap to Speculative Buy when:
STRONG_BUY_MAX_RSI    = 68.0   # above this = overbought, demote to Spec Buy
STRONG_BUY_MAX_5D_RET = 20.0  # 5-day return already >20% = chasing, demote
STRONG_BUY_MIN_RVOL   = 1.5   # must have fresh volume — static quality ≠ momentum

VOLUME_RATIO_GATE    = 1.5    # alert gate: skip alert if vol < 1.5x 3-month avg (logging still happens)
PRICE_MOVE_GATE      = 0.01   # alert gate: skip alert if abs(pct_change) < 1% — flat day, no real action
SESSION_OPEN_GATE    = 15     # alert gate: skip first N minutes after open (alerts only)
SESSION_CLOSE_GATE   = 15     # alert gate: skip last N minutes before close (alerts only)

# ─── Risk Flags ────────────────────────────────────────────────────────────────
# These override scoring — a flagged stock gets a warning label regardless of score
RISK_FLAGS = {
    "going_concern":       "⚠️ Going Concern Warning in recent filing",
    "atm_offering":        "⚠️ Active ATM offering — dilution risk",
    "shelf_registration":  "⚠️ Shelf registration filed — offering likely pending",
    "reverse_split":       "⚠️ Recent or announced reverse stock split",
    "high_short_interest": "⚠️ Short interest > 20% of float",
    "extreme_volatility":  "⚠️ 30-day volatility > 100% annualized",
    "low_liquidity":       "⚠️ Average volume < 500K — liquidity risk",
    "pump_signal":         "⚠️ Volume spike without confirmed catalyst — pump risk",
    "earnings_imminent":   "⚠️ Earnings within 7 days — binary event risk",
}

# ─── Data Sources ──────────────────────────────────────────────────────────────
# All free sources. Add API keys to .env file when you upgrade.
DATA_SOURCES = {
    "price_data":      "yfinance (Yahoo Finance)",
    "sec_filings":     "SEC EDGAR full-text search API (free)",
    "news":            "RSS feeds — Reuters, Seeking Alpha, Yahoo Finance",
    "fundamentals":    "yfinance financial statements",
    "insider":         "SEC Form 4 via EDGAR RSS (free)",
    "short_interest":  "Estimated from yfinance (limited — upgrade to Finviz for accuracy)",
}

# ─── SEC EDGAR Settings ────────────────────────────────────────────────────────
EDGAR_BASE_URL = "https://efts.sec.gov/LATEST/search-index?q="
EDGAR_FILING_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
EDGAR_RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=4&dateb=&owner=include&count=40&search_text=&output=atom"
SEC_USER_AGENT = "StockScanner/1.0 (educational use)"  # Required by SEC

# ─── News RSS Feeds ────────────────────────────────────────────────────────────
NEWS_RSS_FEEDS = [
    "https://finance.yahoo.com/rss/headline?s={ticker}",   # Per-ticker Yahoo Finance
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US",
]

# ─── Database ──────────────────────────────────────────────────────────────────
DB_PATH = "scanner.db"   # SQLite database, stored locally

# ─── Scan Universe ─────────────────────────────────────────────────────────────
# Emergency fallback only — used when both the DB universe and Finnhub are unavailable.
# Under normal operation the scanner pulls 500–2000 tickers from the stock_universe
# table (populated by universe_manager.refresh_universe()).
DEFAULT_UNIVERSE = [
    # Crypto/mining/energy
    "WULF", "IREN", "CIFR", "CLSK", "MARA", "RIOT", "BTBT", "BITF", "HUT",
    # Space / aviation / mobility
    "RKLB", "ACHR", "JOBY", "LUNR", "ASTS",
    # AI / quantum / tech
    "SOUN", "BBAI", "IONQ", "ARQQ", "GFAI", "RGTI", "QBTS", "QUBT", "PRCT",
    # Fintech / consumer
    "HIMS", "RDDT", "SOFI", "AFRM", "UPST", "HOOD", "DKNG",
    # EV / clean energy
    "NKLA", "BLNK", "CHPT", "PLUG", "FCEL", "BE", "NOVA", "RUN",
    # Biotech / healthcare
    "NVAX", "MRNA", "BNTX", "BLUE", "BEAM", "CRSP", "EDIT", "NTLA", "FATE",
    "ALNY", "BMRN", "NVCR", "AXNX", "VERV",
    # Small/mid-cap growth
    "CELH", "TPVG", "OPEN", "PENN", "BYND",
    # Semiconductors / hardware
    "WOLF", "ALGM", "CRUS", "DIOD", "FORM", "AMBA", "PLAB",
    # Software / SaaS
    "ALKT", "APPN", "BAND", "DDOG", "FRSH", "GTLB", "HUBS", "MNDY",
    "PCVX", "TOST", "TASK", "WEAV",
    # Industrials / specialty
    "ACMR", "AZTA", "CEVA", "COHU", "KLIC", "MKSI", "ONTO", "UCTT",
    # Consumer / retail
    "LOVE", "PRPL", "PUBM", "SMAR", "SPSC", "STAA", "TRMK", "VCEL",
]

# ─── Anthropic Claude API ──────────────────────────────────────────────────────
# Used for SEC filing summarization and news synthesis.
# Set ANTHROPIC_API_KEY in your .env file.
ANTHROPIC_MODEL = "claude-sonnet-4-20250514"
ANTHROPIC_MAX_TOKENS = 800   # Keep summaries concise
