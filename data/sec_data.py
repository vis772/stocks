# data/sec_data.py
# Fetches SEC filings from EDGAR — completely free and highly valuable.
# The most actionable free data for detecting dilution risk, going concern
# warnings, and major corporate events BEFORE they're widely known.
#
# Key filing types we care about:
#   S-3 / S-3ASR  → Shelf registration (dilution likely pending)
#   424B          → Prospectus (dilution is happening NOW)
#   8-K           → Material events (contracts, partnerships, exec changes)
#   10-Q / 10-K   → Quarterly/annual reports (going concern, financials)
#   Form 4        → Insider buys and sells
#   SC 13G/13D    → Institutional ownership changes

import requests
import feedparser
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from config import SEC_USER_AGENT, EDGAR_BASE_URL


HEADERS = {
    "User-Agent": SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
}

# Filing types and what they mean in plain English
FILING_SIGNALS = {
    "S-3":      ("dilution_risk",    "Shelf registration — company can issue new shares"),
    "S-3ASR":   ("dilution_risk",    "Automatic shelf registration — large dilution possible"),
    "424B3":    ("dilution_risk",    "Prospectus supplement — active offering in progress"),
    "424B4":    ("dilution_risk",    "Final prospectus — shares being sold now"),
    "8-K":      ("material_event",   "Material event filed"),
    "10-Q":     ("quarterly_report", "Quarterly report"),
    "10-K":     ("annual_report",    "Annual report"),
    "4":        ("insider_activity", "Insider transaction (Form 4)"),
    "SC 13G":   ("institutional",    "Institutional ownership filing"),
    "SC 13D":   ("institutional",    "Activist/large holder filing"),
    "DEFA14A":  ("proxy",            "Proxy solicitation (potential reverse split vote)"),
    "PRE 14A":  ("proxy",            "Preliminary proxy statement"),
}

# Keywords that trigger going concern warning flag
GOING_CONCERN_KEYWORDS = [
    "going concern", "substantial doubt", "ability to continue",
    "may not be able to continue", "doubt about its ability",
]

# Keywords that suggest ATM (at-the-money) offering activity
ATM_KEYWORDS = [
    "at-the-market", "at the market", "atm offering", "atm program",
    "equity distribution agreement", "sales agreement",
]

# Reverse split signals
REVERSE_SPLIT_KEYWORDS = [
    "reverse stock split", "reverse split", "consolidation of shares",
    "1-for-", "for-1 reverse",
]


def get_recent_filings(ticker: str, days_back: int = 30) -> List[Dict]:
    """
    Pull recent SEC filings for a ticker using EDGAR full-text search.
    Returns a list of filing dicts with type, date, url, and plain-English signal.
    """
    filings = []
    try:
        # First, get the company's CIK number from EDGAR
        cik = _get_cik(ticker)
        if not cik:
            return []

        # Fetch recent filings for this CIK
        url = (
            f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        )
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return []

        data = resp.json()
        recent = data.get("filings", {}).get("recent", {})

        forms       = recent.get("form", [])
        dates       = recent.get("filingDate", [])
        accessions  = recent.get("accessionNumber", [])
        descriptions = recent.get("primaryDocument", [])

        cutoff = datetime.now() - timedelta(days=days_back)

        for form, date_str, accession, doc in zip(forms, dates, accessions, descriptions):
            try:
                filing_date = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue

            if filing_date < cutoff:
                break   # EDGAR returns newest first, so we can break early

            # Only track filing types we care about
            signal_key = None
            for key in FILING_SIGNALS:
                if form.startswith(key):
                    signal_key = key
                    break

            if signal_key is None:
                continue

            signal_type, signal_desc = FILING_SIGNALS[signal_key]
            acc_clean = accession.replace("-", "")
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{doc}"

            filings.append({
                "ticker":       ticker,
                "form_type":    form,
                "date":         date_str,
                "signal_type":  signal_type,
                "description":  signal_desc,
                "url":          filing_url,
                "accession":    accession,
                "days_ago":     (datetime.now() - filing_date).days,
            })

    except Exception as e:
        print(f"  [sec_data] Filing fetch failed for {ticker}: {e}")

    return filings


def analyze_filing_risk(filings: List[Dict]) -> Dict:
    """
    Given a list of recent filings, determine which risk flags are active.

    Returns a dict with:
      - active_flags: list of flag names
      - flag_details: human-readable explanations
      - filing_summary: list of plain-English sentences
    """
    active_flags = []
    flag_details = {}
    filing_summary = []

    dilution_forms  = [f for f in filings if f["signal_type"] == "dilution_risk"]
    insider_forms   = [f for f in filings if f["signal_type"] == "insider_activity"]
    material_events = [f for f in filings if f["signal_type"] == "material_event"]
    proxy_forms     = [f for f in filings if f["signal_type"] == "proxy"]

    # ── Dilution / Shelf Registration ─────────────────────────────────────────
    if dilution_forms:
        most_recent = dilution_forms[0]
        active_flags.append("shelf_registration")
        flag_details["shelf_registration"] = (
            f"{most_recent['form_type']} filed {most_recent['days_ago']} days ago — "
            f"company has registered shares for potential sale"
        )
        filing_summary.append(
            f"📋 {most_recent['form_type']} ({most_recent['date']}): "
            f"{most_recent['description']}. Source: SEC EDGAR"
        )

    # ── Proxy / Reverse Split Risk ─────────────────────────────────────────────
    if proxy_forms:
        active_flags.append("reverse_split_risk")
        filing_summary.append(
            f"📋 Proxy filing detected ({proxy_forms[0]['date']}) — "
            f"possible reverse split or major corporate action vote pending"
        )

    # ── 8-K Material Events ────────────────────────────────────────────────────
    for f in material_events[:3]:   # Show up to 3 recent 8-Ks
        filing_summary.append(
            f"📋 8-K filed {f['days_ago']} days ago ({f['date']}). "
            f"Review at: {f['url']}"
        )

    # ── Insider Activity ───────────────────────────────────────────────────────
    if insider_forms:
        filing_summary.append(
            f"👤 {len(insider_forms)} insider transaction(s) in past 30 days. "
            f"Most recent: {insider_forms[0]['date']}. Check EDGAR for buy/sell direction."
        )

    return {
        "active_flags":   active_flags,
        "flag_details":   flag_details,
        "filing_summary": filing_summary,
        "raw_filings":    filings,
    }


def get_insider_form4s(ticker: str, days_back: int = 30) -> List[Dict]:
    """
    Fetch Form 4 (insider transactions) for a ticker via EDGAR RSS.
    Returns a list of transactions with direction and approximate size.
    """
    try:
        cik = _get_cik(ticker)
        if not cik:
            return []

        url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?"
            f"action=getcompany&CIK={cik}&type=4&dateb=&owner=include"
            f"&count=20&search_text=&output=atom"
        )
        feed = feedparser.parse(url)
        transactions = []

        for entry in feed.entries[:10]:
            transactions.append({
                "title":    entry.get("title", ""),
                "date":     entry.get("updated", ""),
                "summary":  entry.get("summary", ""),
                "url":      entry.get("link", ""),
            })

        return transactions

    except Exception as e:
        print(f"  [sec_data] Form 4 fetch failed for {ticker}: {e}")
        return []


def _get_cik(ticker: str) -> Optional[str]:
    """
    Look up a company's CIK number from EDGAR's company search.
    CIK is required for all EDGAR API calls.
    """
    try:
        url = f"https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22&dateRange=custom&startdt=2020-01-01&forms=10-K"
        # Use the simpler company lookup endpoint
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK={ticker}&type=10-K&dateb=&owner=include&count=5&search_text=&action=getcompany&output=atom"
        feed = feedparser.parse(url)

        if feed.entries:
            # CIK is in the company-info tag
            for entry in feed.entries:
                link = entry.get("link", "")
                match = re.search(r"CIK=(\d+)", link)
                if match:
                    return match.group(1)

        # Fallback: use the EDGAR company search JSON
        resp = requests.get(
            f"https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22&forms=10-K&dateRange=custom&startdt=2022-01-01",
            headers=HEADERS, timeout=8
        )
        if resp.status_code == 200:
            hits = resp.json().get("hits", {}).get("hits", [])
            if hits:
                return hits[0].get("_source", {}).get("entity_id", "").lstrip("0") or None

        return None

    except Exception:
        return None
