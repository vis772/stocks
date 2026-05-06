# data/news_data.py
# Fetches news from free RSS feeds.
# Important limitation: these headlines are already priced in by the time
# you read them. Their value here is for CONTEXT and RISK FLAGGING,
# not for predicting price moves.

import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import re
import time


# ─── Free RSS Sources (no API key needed) ─────────────────────────────────────

YAHOO_RSS = "https://finance.yahoo.com/rss/headline?s={ticker}"
SEEKING_ALPHA_RSS = "https://seekingalpha.com/api/sa/combined/{ticker}.xml"

# Keywords that indicate positive catalysts
POSITIVE_KEYWORDS = [
    "contract", "partnership", "awarded", "revenue", "growth", "launch",
    "approval", "upgrade", "buy rating", "price target raised", "beats",
    "exceeds", "record", "wins", "secures", "expands", "milestone",
    "government", "department of defense", "dod", "nasa", "fda approval",
]

# Keywords that indicate negative events or risk
NEGATIVE_KEYWORDS = [
    "dilution", "offering", "shares", "shelf", "lawsuit", "sec investigation",
    "going concern", "bankruptcy", "delisting", "reverse split", "downgrade",
    "misses", "below expectations", "sell rating", "fraud", "investigation",
    "layoffs", "ceo resign", "cfo resign", "default", "breach",
]

# Pump/hype signals — high volume of these without fundamentals = caution
HYPE_KEYWORDS = [
    "moon", "100x", "short squeeze", "squeeze", "gamma squeeze",
    "wallstreetbets", "wsb", "reddit", "viral", "trending",
    "to the moon", "diamond hands", "next big thing",
]


def fetch_ticker_news(ticker: str, max_articles: int = 10) -> List[Dict]:
    """
    Pull recent news articles for a ticker from Yahoo Finance RSS.
    Returns a list of article dicts with title, date, url, and sentiment signals.
    """
    articles = []
    try:
        url = YAHOO_RSS.format(ticker=ticker)
        feed = feedparser.parse(url)

        cutoff = datetime.now() - timedelta(days=14)  # Only last 2 weeks

        for entry in feed.entries[:max_articles]:
            # Parse date
            pub_date = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                pub_date = datetime(*entry.published_parsed[:6])
            elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                pub_date = datetime(*entry.updated_parsed[:6])

            if pub_date and pub_date < cutoff:
                continue

            title   = entry.get("title", "")
            summary = entry.get("summary", "")
            text    = (title + " " + summary).lower()

            # Score sentiment signals
            pos_hits  = [kw for kw in POSITIVE_KEYWORDS if kw in text]
            neg_hits  = [kw for kw in NEGATIVE_KEYWORDS if kw in text]
            hype_hits = [kw for kw in HYPE_KEYWORDS if kw in text]

            sentiment = "neutral"
            if len(pos_hits) > len(neg_hits):
                sentiment = "positive"
            elif len(neg_hits) > len(pos_hits):
                sentiment = "negative"

            articles.append({
                "ticker":       ticker,
                "title":        title,
                "summary":      summary[:300],
                "url":          entry.get("link", ""),
                "published":    pub_date.strftime("%Y-%m-%d") if pub_date else "unknown",
                "days_ago":     (datetime.now() - pub_date).days if pub_date else 99,
                "sentiment":    sentiment,
                "pos_signals":  pos_hits,
                "neg_signals":  neg_hits,
                "hype_signals": hype_hits,
                "is_hype":      len(hype_hits) > 0,
                "source":       "Yahoo Finance RSS",
            })

    except Exception as e:
        print(f"  [news_data] Failed to fetch news for {ticker}: {e}")

    return articles


def analyze_news_sentiment(articles: List[Dict]) -> Dict:
    """
    Summarize the sentiment picture from a list of news articles.

    Returns:
      - sentiment_score: 0-100 (50 = neutral)
      - headline_count: total articles found
      - positive_count / negative_count
      - hype_alert: bool
      - catalyst_keywords: list of notable positive keywords found
      - risk_keywords: list of notable negative keywords found
      - recent_headlines: list of (title, date, sentiment) tuples for display
    """
    if not articles:
        return {
            "sentiment_score":   50,
            "headline_count":    0,
            "positive_count":    0,
            "negative_count":    0,
            "hype_alert":        False,
            "catalyst_keywords": [],
            "risk_keywords":     [],
            "recent_headlines":  [],
        }

    # Weight recent articles more heavily (last 3 days = 2x weight)
    weighted_pos = 0
    weighted_neg = 0
    hype_count   = 0

    for art in articles:
        weight = 2.0 if art.get("days_ago", 99) <= 3 else 1.0
        if art["sentiment"] == "positive":
            weighted_pos += weight
        elif art["sentiment"] == "negative":
            weighted_neg += weight
        if art["is_hype"]:
            hype_count += 1

    total_weight = weighted_pos + weighted_neg
    if total_weight == 0:
        sentiment_score = 50
    else:
        # Map to 0-100: 50 = neutral, 100 = all positive
        sentiment_score = round(50 + 50 * (weighted_pos - weighted_neg) / total_weight)
        sentiment_score = max(0, min(100, sentiment_score))

    # Collect all keyword hits
    all_pos_signals  = [kw for art in articles for kw in art.get("pos_signals", [])]
    all_neg_signals  = [kw for art in articles for kw in art.get("neg_signals", [])]

    return {
        "sentiment_score":   sentiment_score,
        "headline_count":    len(articles),
        "positive_count":    sum(1 for a in articles if a["sentiment"] == "positive"),
        "negative_count":    sum(1 for a in articles if a["sentiment"] == "negative"),
        "hype_alert":        hype_count >= 2,  # 2+ hype articles = caution
        "catalyst_keywords": list(set(all_pos_signals))[:5],
        "risk_keywords":     list(set(all_neg_signals))[:5],
        "recent_headlines":  [
            {
                "title":     a["title"],
                "date":      a["published"],
                "sentiment": a["sentiment"],
                "url":       a["url"],
            }
            for a in articles[:5]
        ],
    }
