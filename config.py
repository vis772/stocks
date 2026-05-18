# config.py
# Central configuration for the scanner.
# Edit these values to tune the system to your preferences.

import os
from dataclasses import dataclass, field
from typing import List

# ─── Market Cap Filters ────────────────────────────────────────────────────────
# Small-cap: $20M–$2B | Mid-cap: $2B–$20B
MIN_MARKET_CAP = 20_000_000       # $20M floor
MAX_MARKET_CAP = 20_000_000_000   # $20B ceiling — matches universe_manager.CRITERIA

# ─── Volume Filters ────────────────────────────────────────────────────────────
MIN_AVG_VOLUME = 500_000           # Minimum average daily volume (liquidity floor)
UNUSUAL_VOLUME_MULTIPLIER = 2.0    # Flag if today's volume > 2x the 20-day average

# ─── Technical Thresholds ──────────────────────────────────────────────────────
RSI_OVERSOLD = 35                  # Below this = potentially oversold (not a buy signal alone)
RSI_OVERBOUGHT = 72                # Above this = extended, caution on new entries
PRICE_MIN = 0.50                   # Minimum price — below this is near-OTC territory
PRICE_MAX = 50.00                  # Maximum price — keep focus on speculative names

# ─── Scoring Weights ───────────────────────────────────────────────────────────
# These are YOUR assumptions, not objective truth.
# Adjust them based on what you find actually correlates with your results.
# They must sum to 1.0
SCORING_WEIGHTS = {
    "technical":    0.30,   # Price action, volume, momentum signals
    "catalyst":     0.25,   # News, SEC filings, events
    "fundamental":  0.20,   # Balance sheet, growth, burn rate
    "risk":         0.15,   # Dilution, liquidity, short interest (inverted)
    "sentiment":    0.10,   # News tone, analyst coverage
}

# Validate weights sum to 1.0
assert abs(sum(SCORING_WEIGHTS.values()) - 1.0) < 0.001, "Scoring weights must sum to 1.0"

# ─── Signal Thresholds ─────────────────────────────────────────────────────────
# What score ranges map to which recommendation label
SIGNAL_THRESHOLDS = {
    "Strong Buy Candidate": (75, 100),
    "Speculative Buy":      (60, 75),
    "Watchlist":            (45, 60),
    "Hold":                 (35, 45),
    "Trim":                 (25, 35),
    "Sell":                 (15, 25),
    "Avoid":                (0,  15),
}

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
