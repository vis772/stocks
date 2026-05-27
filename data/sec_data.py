# data/sec_data.py
# SEC EDGAR filings + insider transaction direction parsing (Option B)

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

FILING_SIGNALS = {
    "S-3":     ("dilution_risk",    "Shelf registration — company can issue new shares"),
    "S-3ASR":  ("dilution_risk",    "Automatic shelf registration — large dilution possible"),
    "424B3":   ("dilution_risk",    "Prospectus supplement — active offering in progress"),
    "424B4":   ("dilution_risk",    "Final prospectus — shares being sold now"),
    "8-K":     ("material_event",   "Material event filed"),
    "10-Q":    ("quarterly_report", "Quarterly report"),
    "10-K":    ("annual_report",    "Annual report"),
    "4":       ("insider_activity", "Insider transaction (Form 4)"),
    "SC 13G":  ("institutional",    "Institutional ownership filing"),
    "SC 13D":  ("institutional",    "Activist/large holder filing"),
    "DEFA14A": ("proxy",            "Proxy solicitation (potential reverse split vote)"),
    "PRE 14A": ("proxy",            "Preliminary proxy statement"),
}


def get_recent_filings(ticker: str, days_back: int = 30) -> List[Dict]:
    """Pull recent SEC filings for a ticker using EDGAR."""
    filings = []
    try:
        cik = _get_cik(ticker)
        if not cik:
            return []

        url  = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return []

        data   = resp.json()
        recent = data.get("filings", {}).get("recent", {})

        forms        = recent.get("form", [])
        dates        = recent.get("filingDate", [])
        accessions   = recent.get("accessionNumber", [])
        descriptions = recent.get("primaryDocument", [])

        cutoff = datetime.now() - timedelta(days=days_back)

        for form, date_str, accession, doc in zip(forms, dates, accessions, descriptions):
            try:
                filing_date = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue
            if filing_date < cutoff:
                break

            signal_key = None
            for key in FILING_SIGNALS:
                if form.startswith(key):
                    signal_key = key
                    break
            if signal_key is None:
                continue

            signal_type, signal_desc = FILING_SIGNALS[signal_key]
            acc_clean  = accession.replace("-", "")
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{doc}"

            filings.append({
                "ticker":      ticker,
                "form_type":   form,
                "date":        date_str,
                "signal_type": signal_type,
                "description": signal_desc,
                "url":         filing_url,
                "accession":   accession,
                "days_ago":    (datetime.now() - filing_date).days,
            })

    except Exception as e:
        print(f"  [sec_data] Filing fetch failed for {ticker}: {e}")

    return filings


def analyze_filing_risk(filings: List[Dict]) -> Dict:
    """Determine which risk flags are active from recent filings."""
    active_flags   = []
    flag_details   = {}
    filing_summary = []

    dilution_forms  = [f for f in filings if f["signal_type"] == "dilution_risk"]
    insider_forms   = [f for f in filings if f["signal_type"] == "insider_activity"]
    material_events = [f for f in filings if f["signal_type"] == "material_event"]
    proxy_forms     = [f for f in filings if f["signal_type"] == "proxy"]

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

    if proxy_forms:
        active_flags.append("reverse_split_risk")
        filing_summary.append(
            f"📋 Proxy filing detected ({proxy_forms[0]['date']}) — "
            f"possible reverse split or major corporate action vote pending"
        )

    for f in material_events[:3]:
        filing_summary.append(
            f"📋 8-K filed {f['days_ago']} days ago ({f['date']}). "
            f"Review at: {f['url']}"
        )

    # ── OPTION B: Insider direction ───────────────────────────────────────────
    if insider_forms:
        insider_summary = get_insider_direction(insider_forms[0]["url"])
        if insider_summary:
            filing_summary.append(f"👤 {insider_summary}")
        else:
            filing_summary.append(
                f"👤 {len(insider_forms)} insider transaction(s) in past 30 days. "
                f"Most recent: {insider_forms[0]['date']}."
            )

    return {
        "active_flags":   active_flags,
        "flag_details":   flag_details,
        "filing_summary": filing_summary,
        "raw_filings":    filings,
    }


def get_insider_direction(filing_url: str) -> Optional[str]:
    """
    OPTION B: Parse a Form 4 filing to determine if the insider
    was BUYING or SELLING, and approximately how much.

    Form 4 XML contains:
      transactionCode: P = Purchase (buy), S = Sale (sell), A = Award
      transactionShares: number of shares
      transactionPricePerShare: price paid
    """
    try:
        # Try the XML version of the filing
        xml_url = filing_url.replace(".htm", ".xml").replace(".html", ".xml")
        resp    = requests.get(xml_url, headers=HEADERS, timeout=10)

        if resp.status_code != 200:
            # Fall back to HTML parsing
            resp = requests.get(filing_url, headers=HEADERS, timeout=10)
            if resp.status_code != 200:
                return None
            text = resp.text.lower()
        else:
            text = resp.text.lower()

        # Look for transaction codes
        is_buy  = bool(re.search(r'transactioncode.*?>p<', text) or "acquisition" in text or ">p<" in text)
        is_sell = bool(re.search(r'transactioncode.*?>s<', text) or "disposition" in text or ">s<" in text)
        is_award = bool(re.search(r'transactioncode.*?>a<', text) or ">a<" in text)

        # Extract share count
        shares_match = re.search(r'transactionshares.*?>([\d,\.]+)<', text)
        shares = None
        if shares_match:
            try:
                shares = int(float(shares_match.group(1).replace(",", "")))
            except Exception:
                pass

        # Extract price
        price_match = re.search(r'transactionpricepershare.*?>([\d,\.]+)<', text)
        price = None
        if price_match:
            try:
                price = float(price_match.group(1).replace(",", ""))
            except Exception:
                pass

        # Build summary
        value_str = ""
        if shares and price:
            value = shares * price
            value_str = f" (~${value/1e6:.2f}M)" if value >= 1_000_000 else f" (~${value:,.0f})"
        elif shares:
            value_str = f" ({shares:,} shares)"

        if is_award:
            return f"Insider received stock award{value_str} — compensation, not a market signal"
        elif is_buy:
            return f"🟢 INSIDER BUY detected{value_str} — bullish signal, insiders buying with own money"
        elif is_sell:
            return f"🔴 INSIDER SELL detected{value_str} — bearish signal, insider reducing position"

        return None

    except Exception as e:
        return None


def analyze_filing_verdict(ticker: str, form_type: str, filing_url: str) -> dict:
    """
    Deep AI analysis of an SEC filing. Returns a structured buy/not-buy verdict
    suitable for use directly in alerts and conviction pipeline.

    Returns dict:
        verdict       : "BUY" | "WATCH" | "AVOID" | "NEUTRAL"
        confidence    : int 0-100 (0 = no data)
        headline      : str — one-sentence punchy summary
        catalysts     : list[str] — specific bullish catalysts (empty if none)
        risks         : list[str] — specific risks flagged (empty if none)
        trade_setup   : str — actionable guidance (entry, timing, sizing)
        alert_worthy  : bool — should this fire a Pushover alert?
        raw_text_chars: int — chars of filing text analysed (0 if fetch failed)
        error         : str | None
    """
    import os
    import json as _json

    _default = {
        "verdict": "NEUTRAL", "confidence": 0, "headline": "Filing analysis unavailable",
        "catalysts": [], "risks": [], "trade_setup": "",
        "alert_worthy": False, "raw_text_chars": 0, "error": None,
    }

    try:
        import anthropic
    except ImportError:
        return {**_default, "error": "anthropic package not installed"}

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {**_default, "error": "ANTHROPIC_API_KEY not set"}

    # ── 1. Fetch filing text ──────────────────────────────────────────────────
    filing_text = ""
    try:
        resp = requests.get(filing_url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "lxml")
            filing_text = soup.get_text(separator=" ", strip=True)[:8000]
    except Exception as fetch_err:
        print(f"  [sec_data] Filing fetch failed ({ticker}): {fetch_err}")

    if not filing_text:
        return {**_default, "error": "could not fetch filing text", "raw_text_chars": 0}

    # ── 2. Claude analysis ────────────────────────────────────────────────────
    prompt = f"""You are a professional equity analyst specialising in small-cap stocks.
Analyse this SEC {form_type} filing for {ticker} and return ONLY a JSON object — no prose, no markdown fences.

JSON schema (all fields required):
{{
  "verdict":      "BUY" | "WATCH" | "AVOID" | "NEUTRAL",
  "confidence":   <integer 0-100>,
  "headline":     "<one punchy sentence — what happened and why it matters>",
  "catalysts":    ["<specific bullish catalyst>", ...],
  "risks":        ["<specific risk or bearish flag>", ...],
  "trade_setup":  "<actionable: timing, entry range, key level to watch, or 'No trade — X'>",
  "alert_worthy": <true | false>
}}

Verdict guide:
  BUY   — material positive catalyst (deal, partnership, FDA, revenue beat, insider buy ≥$100K)
  WATCH — mixed/ambiguous; worth monitoring at open
  AVOID — dilution offering, equity raise, reverse split vote, going concern, NASDAQ notice
  NEUTRAL — routine filing with no actionable information

Be specific: include dollar amounts, percentages, dates, and named parties when present.
alert_worthy = true only for BUY or high-impact AVOID (dilution, reverse split risk).

Filing text ({len(filing_text)} chars):
{filing_text}"""

    try:
        client  = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_response = message.content[0].text.strip()

        # Strip any accidental markdown fences
        if raw_response.startswith("```"):
            raw_response = raw_response.split("```")[1]
            if raw_response.startswith("json"):
                raw_response = raw_response[4:]

        result = _json.loads(raw_response)
        # Validate and fill missing fields
        result.setdefault("verdict",      "NEUTRAL")
        result.setdefault("confidence",   50)
        result.setdefault("headline",     f"{form_type} filing for {ticker}")
        result.setdefault("catalysts",    [])
        result.setdefault("risks",        [])
        result.setdefault("trade_setup",  "")
        result.setdefault("alert_worthy", False)
        result["raw_text_chars"] = len(filing_text)
        result["error"] = None
        print(f"  [sec_data] {ticker} {form_type} verdict={result['verdict']} confidence={result['confidence']}")
        return result

    except Exception as e:
        print(f"  [sec_data] Claude analysis failed for {ticker}: {e}")
        return {**_default, "error": str(e), "raw_text_chars": len(filing_text)}


def summarize_filing_with_claude(filing_url: str, form_type: str, ticker: str) -> str:
    """
    Backward-compatible wrapper — returns a human-readable string summary.
    Internally calls analyze_filing_verdict() for full analysis.
    """
    result = analyze_filing_verdict(ticker, form_type, filing_url)
    if result.get("error") and not result.get("headline"):
        return f"Filing summary unavailable: {result['error']}"

    lines = [f"[{result['verdict']} | {result['confidence']}% confidence]",
             result["headline"]]
    if result["catalysts"]:
        lines.append("Catalysts: " + "; ".join(result["catalysts"]))
    if result["risks"]:
        lines.append("Risks: " + "; ".join(result["risks"]))
    if result["trade_setup"]:
        lines.append(f"Setup: {result['trade_setup']}")
    return "\n".join(lines)


def get_insider_form4s(ticker: str, days_back: int = 30) -> List[Dict]:
    """Fetch Form 4 insider transactions via EDGAR RSS."""
    try:
        cik = _get_cik(ticker)
        if not cik:
            return []

        url  = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?"
            f"action=getcompany&CIK={cik}&type=4&dateb=&owner=include"
            f"&count=20&search_text=&output=atom"
        )
        feed = feedparser.parse(url)
        transactions = []

        for entry in feed.entries[:10]:
            transactions.append({
                "title":   entry.get("title", ""),
                "date":    entry.get("updated", ""),
                "summary": entry.get("summary", ""),
                "url":     entry.get("link", ""),
            })

        return transactions

    except Exception as e:
        print(f"  [sec_data] Form 4 fetch failed for {ticker}: {e}")
        return []


def _get_cik(ticker: str) -> Optional[str]:
    """Look up a company's CIK number from EDGAR."""
    try:
        url  = f"https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK={ticker}&type=10-K&dateb=&owner=include&count=5&search_text=&action=getcompany&output=atom"
        feed = feedparser.parse(url)

        if feed.entries:
            for entry in feed.entries:
                link  = entry.get("link", "")
                match = re.search(r"CIK=(\d+)", link)
                if match:
                    return match.group(1)

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
