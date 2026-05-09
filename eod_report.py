# eod_report.py
# End-of-day report generator.
# Runs at 4:00 PM ET daily, generates a PDF, uploads to Filebin,
# and sends a download link to your phone via Pushover.

import os
import json
import uuid
import requests
import yfinance as yf
import feedparser
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak
)

FINNHUB_BASE = "https://finnhub.io/api/v1"
REPORTS_DIR  = "reports"

# ─── Colors ───────────────────────────────────────────────────────────────────
C_BG      = colors.HexColor("#04080f")
C_DARK    = colors.HexColor("#080f1a")
C_CARD    = colors.HexColor("#0a1220")
C_BORDER  = colors.HexColor("#0e2040")
C_GREEN   = colors.HexColor("#00e87a")
C_GREEN2  = colors.HexColor("#00c864")
C_BLUE    = colors.HexColor("#0096ff")
C_BLUE2   = colors.HexColor("#33b3ff")
C_AMBER   = colors.HexColor("#ffb700")
C_RED     = colors.HexColor("#ff2d55")
C_PURPLE  = colors.HexColor("#9b59ff")
C_WHITE   = colors.HexColor("#e8f4ff")
C_GREY    = colors.HexColor("#3a5878")
C_DIMGREY = colors.HexColor("#1e3a52")


def _fh_key() -> str:
    return os.environ.get("FINNHUB_API_KEY", "")


def _fh_get(endpoint: str, params: dict) -> Optional[dict]:
    key = _fh_key()
    if not key:
        return None
    try:
        params["token"] = key
        resp = requests.get(f"{FINNHUB_BASE}/{endpoint}", params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return data if data else None
        return None
    except Exception:
        return None


# ─── Data collection ──────────────────────────────────────────────────────────

def get_stock_day_summary(ticker: str) -> Dict:
    """Get full day summary for one stock."""
    try:
        quote = _fh_get("quote", {"symbol": ticker})
        if not quote or not quote.get("c"):
            return {}

        price      = quote.get("c", 0)
        prev_close = quote.get("pc", 0)
        day_high   = quote.get("h", 0)
        day_low    = quote.get("l", 0)
        volume     = quote.get("v", 0)
        change_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0

        today     = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        news      = _fh_get("company-news", {"symbol": ticker, "from": yesterday, "to": today}) or []
        profile   = _fh_get("stock/profile2", {"symbol": ticker}) or {}

        try:
            yf_info   = yf.Ticker(ticker).info
            short_pct = yf_info.get("shortPercentOfFloat")
            avg_vol   = yf_info.get("averageVolume", 0)
            mkt_cap   = yf_info.get("marketCap", 0) or (profile.get("marketCapitalization", 0) * 1e6)
        except Exception:
            short_pct = None
            avg_vol   = 0
            mkt_cap   = profile.get("marketCapitalization", 0) * 1e6

        rel_volume = round(volume / avg_vol, 2) if avg_vol > 0 else 0

        alerts_for_ticker = []
        try:
            with open("alert_log.json") as f:
                all_alerts = json.load(f)
            alerts_for_ticker = [a for a in all_alerts if ticker in a]
        except Exception:
            pass

        next_day_notes = []
        if abs(change_pct) > 8:
            next_day_notes.append(f"Large {'gain' if change_pct > 0 else 'loss'} today — watch for continuation or reversal at open")
        if rel_volume > 2.5:
            next_day_notes.append(f"Elevated volume ({rel_volume:.1f}x avg) — watch pre-market activity")
        if news:
            recent = [n for n in news if datetime.fromtimestamp(n.get("datetime", 0)) > datetime.now() - timedelta(hours=6)]
            if recent:
                next_day_notes.append(f"{len(recent)} article(s) in last 6 hours — catalyst may carry into tomorrow")
        if short_pct and short_pct > 0.20:
            next_day_notes.append(f"High short interest ({short_pct*100:.1f}%) — squeeze potential if momentum continues")

        return {
            "ticker":         ticker,
            "company_name":   profile.get("name", ticker),
            "price":          round(price, 4),
            "prev_close":     round(prev_close, 4),
            "change_pct":     round(change_pct, 2),
            "day_high":       round(day_high, 4),
            "day_low":        round(day_low, 4),
            "volume":         volume,
            "avg_volume":     avg_vol,
            "rel_volume":     rel_volume,
            "market_cap":     mkt_cap,
            "short_pct":      short_pct,
            "news_count":     len(news),
            "news_today":     news[:3],
            "alerts_fired":   alerts_for_ticker,
            "next_day_notes": next_day_notes,
            "sector":         profile.get("finnhubIndustry", "Unknown"),
        }

    except Exception as e:
        print(f"  [eod] Failed {ticker}: {e}")
        return {}


def get_sec_filings_today(tickers: List[str]) -> List[Dict]:
    """Get SEC filings from today for watchlist stocks."""
    filings = []
    try:
        feed = feedparser.parse(
            "https://www.sec.gov/cgi-bin/browse-edgar"
            "?action=getcurrent&type=8-K&dateb=&owner=include&count=40&output=atom"
        )
        ticker_set = set(t.upper() for t in tickers)
        for entry in feed.entries[:40]:
            title = entry.get("title", "")
            link  = entry.get("link", "")
            for ticker in ticker_set:
                if ticker in title.upper():
                    filings.append({
                        "ticker": ticker,
                        "title":  title,
                        "url":    link,
                        "time":   entry.get("updated", ""),
                    })
                    break
    except Exception as e:
        print(f"  [eod] SEC check failed: {e}")
    return filings


# ─── Styles ───────────────────────────────────────────────────────────────────

def S(name, **kwargs) -> ParagraphStyle:
    return ParagraphStyle(name, **kwargs)

STYLES = {
    "title":      S("title",      fontName="Helvetica-Bold",    fontSize=26, textColor=C_WHITE,  alignment=TA_CENTER, spaceAfter=4),
    "subtitle":   S("subtitle",   fontName="Helvetica",         fontSize=10, textColor=C_GREY,   alignment=TA_CENTER, spaceAfter=16),
    "section":    S("section",    fontName="Helvetica-Bold",    fontSize=13, textColor=C_BLUE2,  spaceBefore=14, spaceAfter=8),
    "ticker":     S("ticker",     fontName="Helvetica-Bold",    fontSize=12, textColor=C_WHITE,  spaceAfter=2),
    "company":    S("company",    fontName="Helvetica",         fontSize=8,  textColor=C_GREY,   spaceAfter=5),
    "body":       S("body",       fontName="Helvetica",         fontSize=8.5,textColor=C_GREY,   spaceAfter=4, leading=13),
    "bullet":     S("bullet",     fontName="Helvetica",         fontSize=8,  textColor=C_GREY,   spaceAfter=3, leftIndent=14, leading=12),
    "alert":      S("alert",      fontName="Helvetica",         fontSize=8,  textColor=C_AMBER,  spaceAfter=3, leftIndent=14, leading=12),
    "nextday":    S("nextday",    fontName="Helvetica-Oblique", fontSize=8,  textColor=C_BLUE2,  spaceAfter=3, leftIndent=14, leading=12),
    "disclaimer": S("disclaimer", fontName="Helvetica-Oblique", fontSize=7,  textColor=C_DIMGREY,alignment=TA_CENTER),
    "label":      S("label",      fontName="Helvetica-Bold",    fontSize=8,  textColor=C_BLUE2,  spaceAfter=2, leftIndent=10),
    "greenlabel": S("greenlabel", fontName="Helvetica-Bold",    fontSize=8,  textColor=C_GREEN2, spaceAfter=2, leftIndent=10),
    "amberlabel": S("amberlabel", fontName="Helvetica-Bold",    fontSize=8,  textColor=C_AMBER,  spaceAfter=2, leftIndent=10),
}


# ─── PDF building blocks ──────────────────────────────────────────────────────

def overview_table(summaries, sec_filings, all_alerts) -> Table:
    gainers   = sum(1 for s in summaries if s.get("change_pct", 0) > 0)
    losers    = sum(1 for s in summaries if s.get("change_pct", 0) < 0)
    big_move  = sum(1 for s in summaries if abs(s.get("change_pct", 0)) > 5)
    high_vol  = sum(1 for s in summaries if s.get("rel_volume", 0) > 2)

    data = [
        ["MONITORED", "GAINERS", "LOSERS", "BIG MOVERS", "HIGH VOLUME", "FILINGS", "ALERTS"],
        [str(len(summaries)), str(gainers), str(losers), str(big_move), str(high_vol), str(len(sec_filings)), str(len(all_alerts))],
    ]
    t = Table(data, colWidths=[1.1*inch]*7)
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,0),  C_CARD),
        ("BACKGROUND",    (0,1), (-1,1),  C_DARK),
        ("TEXTCOLOR",     (0,0), (-1,0),  C_GREY),
        ("FONTNAME",      (0,0), (-1,0),  "Helvetica"),
        ("FONTSIZE",      (0,0), (-1,0),  6.5),
        ("FONTNAME",      (0,1), (-1,1),  "Helvetica-Bold"),
        ("FONTSIZE",      (0,1), (-1,1),  14),
        ("TEXTCOLOR",     (0,1), (0,1),   C_WHITE),
        ("TEXTCOLOR",     (1,1), (1,1),   C_GREEN),
        ("TEXTCOLOR",     (2,1), (2,1),   C_RED),
        ("TEXTCOLOR",     (3,1), (3,1),   C_AMBER),
        ("TEXTCOLOR",     (4,1), (4,1),   C_BLUE2),
        ("TEXTCOLOR",     (5,1), (5,1),   C_PURPLE),
        ("TEXTCOLOR",     (6,1), (6,1),   C_AMBER),
        ("ALIGN",         (0,0), (-1,-1), "CENTER"),
        ("TOPPADDING",    (0,0), (-1,-1), 8),
        ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ("GRID",          (0,0), (-1,-1), 0.3, C_BORDER),
    ]))
    return t


def movers_table(summaries) -> Table:
    header = ["TICKER", "PRICE", "CHANGE", "HIGH", "LOW", "VOLUME", "REL VOL", "NEWS", "ALERTS"]
    rows   = [header]
    for s in summaries[:20]:
        chg = s.get("change_pct", 0)
        p   = s.get("price", 0)
        vol = s.get("volume", 0)
        rows.append([
            s.get("ticker", ""),
            f"${p:.4f}" if p < 10 else f"${p:.2f}",
            f"+{chg:.1f}%" if chg >= 0 else f"{chg:.1f}%",
            f"${s.get('day_high',0):.4f}" if p < 10 else f"${s.get('day_high',0):.2f}",
            f"${s.get('day_low',0):.4f}"  if p < 10 else f"${s.get('day_low',0):.2f}",
            f"{vol/1e6:.1f}M" if vol >= 1e6 else f"{vol/1e3:.0f}K",
            f"{s.get('rel_volume',0):.1f}x",
            str(s.get("news_count", 0)),
            str(len(s.get("alerts_fired", []))),
        ])

    cw = [0.65*inch, 0.75*inch, 0.72*inch, 0.75*inch, 0.75*inch, 0.72*inch, 0.65*inch, 0.5*inch, 0.55*inch]
    t  = Table(rows, colWidths=cw, repeatRows=1)
    ts = TableStyle([
        ("BACKGROUND",    (0,0), (-1,0),  C_CARD),
        ("TEXTCOLOR",     (0,0), (-1,0),  C_BLUE2),
        ("FONTNAME",      (0,0), (-1,0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0,0), (-1,0),  7),
        ("TOPPADDING",    (0,0), (-1,-1), 5),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ("LEFTPADDING",   (0,0), (-1,-1), 5),
        ("FONTNAME",      (0,1), (-1,-1), "Helvetica"),
        ("FONTSIZE",      (0,1), (-1,-1), 7.5),
        ("TEXTCOLOR",     (0,1), (-1,-1), C_GREY),
        ("TEXTCOLOR",     (0,1), (0,-1),  C_WHITE),
        ("ALIGN",         (1,0), (-1,-1), "RIGHT"),
        ("ALIGN",         (0,0), (0,-1),  "LEFT"),
        ("LINEBELOW",     (0,0), (-1,0),  0.5, C_BORDER),
        ("LINEBELOW",     (0,1), (-1,-1), 0.3, C_BORDER),
        ("ROWBACKGROUNDS",(0,1), (-1,-1), [C_DARK, C_CARD]),
    ])
    for i, row in enumerate(rows[1:], 1):
        chg_val = summaries[i-1].get("change_pct", 0) if i-1 < len(summaries) else 0
        color   = C_GREEN if chg_val >= 0 else C_RED
        ts.add("TEXTCOLOR", (2,i), (2,i), color)
        ts.add("FONTNAME",  (2,i), (2,i), "Helvetica-Bold")
    t.setStyle(ts)
    return t


def stock_card(s: Dict) -> list:
    if not s:
        return []
    els = []

    ticker = s.get("ticker", "")
    name   = s.get("company_name", ticker)
    price  = s.get("price", 0)
    chg    = s.get("change_pct", 0)
    rvol   = s.get("rel_volume", 0)
    mc     = s.get("market_cap", 0)
    vol    = s.get("volume", 0)
    news   = s.get("news_today", [])
    alerts = s.get("alerts_fired", [])
    notes  = s.get("next_day_notes", [])

    chg_str  = f"+{chg:.1f}%" if chg >= 0 else f"{chg:.1f}%"
    chg_col  = C_GREEN if chg >= 0 else C_RED
    p_str    = f"${price:.4f}" if price < 10 else f"${price:.2f}"
    mc_str   = f"${mc/1e9:.2f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"
    vol_str  = f"{vol/1e6:.1f}M" if vol >= 1e6 else f"{vol/1e3:.0f}K"

    # Header row
    hd = [[
        Paragraph(f"<b>{ticker}</b>", S("h1", fontName="Helvetica-Bold", fontSize=12, textColor=C_WHITE)),
        Paragraph(name[:40], S("h2", fontName="Helvetica", fontSize=8, textColor=C_GREY)),
        Paragraph(p_str, S("h3", fontName="Helvetica-Bold", fontSize=11, textColor=C_WHITE, alignment=TA_RIGHT)),
        Paragraph(f"<b>{chg_str}</b>", S("h4", fontName="Helvetica-Bold", fontSize=11, textColor=chg_col, alignment=TA_RIGHT)),
    ]]
    ht = Table(hd, colWidths=[0.75*inch, 2.9*inch, 1.0*inch, 0.95*inch])
    ht.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), C_CARD),
        ("TOPPADDING",    (0,0), (-1,-1), 7),
        ("BOTTOMPADDING", (0,0), (-1,-1), 7),
        ("LEFTPADDING",   (0,0), (0,0),   10),
        ("RIGHTPADDING",  (-1,0),(-1,-1), 10),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
        ("LINEBELOW",     (0,0), (-1,0),  0.8, C_BORDER),
    ]))
    els.append(ht)

    # Stats row
    sd = [[
        f"H: ${s.get('day_high',0):.4f}" if price < 10 else f"H: ${s.get('day_high',0):.2f}",
        f"L: ${s.get('day_low',0):.4f}"  if price < 10 else f"L: ${s.get('day_low',0):.2f}",
        f"Vol: {vol_str}",
        f"RVol: {rvol:.1f}x",
        f"Cap: {mc_str}",
        f"{s.get('sector','')[:16]}",
    ]]
    st_ = Table(sd, colWidths=[0.92*inch]*6)
    st_.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), C_DARK),
        ("TEXTCOLOR",     (0,0), (-1,-1), C_GREY),
        ("FONTNAME",      (0,0), (-1,-1), "Helvetica"),
        ("FONTSIZE",      (0,0), (-1,-1), 7.5),
        ("TOPPADDING",    (0,0), (-1,-1), 5),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ("LEFTPADDING",   (0,0), (-1,-1), 8),
        ("LINEBELOW",     (0,0), (-1,-1), 0.3, C_BORDER),
        ("TEXTCOLOR",     (3,0), (3,0),   C_AMBER if rvol > 2 else C_GREY),
    ]))
    els.append(st_)

    # Alerts
    if alerts:
        els.append(Spacer(1, 4))
        els.append(Paragraph("Alerts Fired:", STYLES["amberlabel"]))
        for a in alerts[:5]:
            els.append(Paragraph(f"  • {a}", STYLES["alert"]))

    # News
    if news:
        els.append(Spacer(1, 4))
        els.append(Paragraph("News Today:", STYLES["label"]))
        for article in news[:3]:
            headline = article.get("headline", "")[:110]
            source   = article.get("source", "")
            ts_      = article.get("datetime", 0)
            t_str    = datetime.fromtimestamp(ts_).strftime("%H:%M ET") if ts_ else ""
            els.append(Paragraph(f"  • [{t_str}] {headline} ({source})", STYLES["bullet"]))

    # Tomorrow notes
    if notes:
        els.append(Spacer(1, 4))
        els.append(Paragraph("Tomorrow's Watchpoints:", STYLES["greenlabel"]))
        for note in notes:
            els.append(Paragraph(f"  → {note}", STYLES["nextday"]))

    els.append(Spacer(1, 8))
    els.append(HRFlowable(width="100%", thickness=0.4, color=C_BORDER))
    els.append(Spacer(1, 8))
    return els


# ─── PDF generation ───────────────────────────────────────────────────────────

def generate_eod_report(watchlist: List[str]) -> Optional[str]:
    print(f"\n{'='*55}")
    print(f"APEX EOD Report — {datetime.now().strftime('%Y-%m-%d %H:%M ET')}")
    print(f"{'='*55}")

    os.makedirs(REPORTS_DIR, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{REPORTS_DIR}/APEX_EOD_{date_str}.pdf"

    print(f"  Collecting data for {len(watchlist)} stocks...")
    summaries = []
    for ticker in watchlist:
        s = get_stock_day_summary(ticker)
        if s:
            summaries.append(s)

    if not summaries:
        print("  No data — report aborted")
        return None

    summaries.sort(key=lambda x: abs(x.get("change_pct", 0)), reverse=True)

    print("  Checking SEC filings...")
    sec_filings = get_sec_filings_today(watchlist)

    # Tag SEC filings onto stocks
    for filing in sec_filings:
        for s in summaries:
            if s["ticker"] == filing["ticker"]:
                s.setdefault("next_day_notes", []).append(
                    f"SEC {filing['title'][:60]} — review before open"
                )

    try:
        with open("alert_log.json") as f:
            all_alerts = json.load(f)
    except Exception:
        all_alerts = []

    # ── Build PDF ──────────────────────────────────────────────────────────────
    print(f"  Building PDF...")
    doc   = SimpleDocTemplate(
        filename, pagesize=letter,
        leftMargin=0.55*inch, rightMargin=0.55*inch,
        topMargin=0.55*inch, bottomMargin=0.55*inch,
    )
    story = []

    # Cover
    story.append(Spacer(1, 8))
    story.append(Paragraph("⚡ APEX SCANNER", STYLES["title"]))
    story.append(Paragraph(
        f"End-of-Day Report  ·  {datetime.now().strftime('%A, %B %d, %Y')}  ·  Market Close",
        STYLES["subtitle"]
    ))
    story.append(HRFlowable(width="100%", thickness=1, color=C_BLUE))
    story.append(Spacer(1, 14))

    # Overview
    story.append(overview_table(summaries, sec_filings, all_alerts))
    story.append(Spacer(1, 16))

    # Top movers
    story.append(Paragraph("Top Movers Today", STYLES["section"]))
    story.append(movers_table(summaries))
    story.append(Spacer(1, 16))

    # SEC filings
    if sec_filings:
        story.append(Paragraph("SEC Filings Today", STYLES["section"]))
        for f in sec_filings:
            story.append(Paragraph(
                f"<b>{f['ticker']}</b> — {f['title'][:90]}",
                S("sf", fontName="Helvetica", fontSize=8, textColor=C_PURPLE, spaceAfter=3, leftIndent=8)
            ))
            story.append(Paragraph(
                f"  Filed: {f.get('time','')}",
                S("sfu", fontName="Helvetica", fontSize=7, textColor=C_DIMGREY, spaceAfter=6, leftIndent=16)
            ))
        story.append(Spacer(1, 12))

    # Per-stock cards
    story.append(Paragraph("Stock-by-Stock Analysis", STYLES["section"]))
    story.append(Spacer(1, 6))
    for s in summaries:
        story.extend(stock_card(s))

    # Tomorrow watchpoints
    story.append(PageBreak())
    story.append(Paragraph("Tomorrow's Key Watchpoints", STYLES["section"]))
    story.append(Paragraph(
        "Stocks and events worth monitoring at tomorrow's open:",
        STYLES["body"]
    ))
    story.append(Spacer(1, 8))

    has_wp = False
    for s in summaries:
        notes = s.get("next_day_notes", [])
        if notes:
            has_wp = True
            story.append(Paragraph(
                f"<b>{s['ticker']}</b> — {s.get('company_name','')[:35]}",
                S("wpt", fontName="Helvetica-Bold", fontSize=9, textColor=C_WHITE, spaceAfter=2)
            ))
            for note in notes:
                story.append(Paragraph(f"  → {note}", STYLES["nextday"]))
            story.append(Spacer(1, 5))

    if not has_wp:
        story.append(Paragraph("No significant watchpoints identified for tomorrow.", STYLES["body"]))

    # Alert log
    if all_alerts:
        story.append(Spacer(1, 12))
        story.append(Paragraph("Today's Alert Log", STYLES["section"]))
        for alert in all_alerts[-30:]:
            story.append(Paragraph(f"  {alert}", STYLES["alert"]))

    # Disclaimer
    story.append(Spacer(1, 20))
    story.append(HRFlowable(width="100%", thickness=0.4, color=C_BORDER))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        f"APEX Scanner EOD Report — Research purposes only. Not financial advice. "
        f"Data: Finnhub, Yahoo Finance, SEC EDGAR. "
        f"Generated {datetime.now().strftime('%H:%M ET')} on {date_str}.",
        STYLES["disclaimer"]
    ))

    doc.build(story)
    print(f"  ✓ PDF saved: {filename}")
    return filename


# ─── Upload & Notify ──────────────────────────────────────────────────────────

def upload_to_filebin(filename: str) -> Optional[str]:
    """Upload PDF to Filebin.net and return direct download URL."""
    try:
        bin_id    = f"apex-eod-{uuid.uuid4().hex[:8]}"
        file_name = os.path.basename(filename)

        with open(filename, "rb") as f:
            resp = requests.post(
                f"https://filebin.net/{bin_id}/{file_name}",
                data=f,
                headers={"Content-Type": "application/pdf"},
                timeout=30,
            )

        if resp.status_code in (200, 201):
            url = f"https://filebin.net/{bin_id}/{file_name}"
            print(f"  ✓ Uploaded: {url}")
            return url
        else:
            print(f"  [eod] Upload failed ({resp.status_code})")
            return None

    except Exception as e:
        print(f"  [eod] Upload error: {e}")
        return None


def send_report_notification(filename: str, summaries: list, sec_filings: list, all_alerts: list):
    """Upload PDF and send Pushover notification with download link."""
    user_key  = os.environ.get("PUSHOVER_USER_KEY", "")
    api_token = os.environ.get("PUSHOVER_API_TOKEN", "")

    if not user_key or not api_token:
        print("  [eod] No Pushover keys — skipping notification")
        return

    print("  Uploading PDF to Filebin...")
    download_url = upload_to_filebin(filename)

    gainers  = sum(1 for s in summaries if s.get("change_pct", 0) > 0)
    losers   = sum(1 for s in summaries if s.get("change_pct", 0) < 0)
    big_move = sum(1 for s in summaries if abs(s.get("change_pct", 0)) > 5)

    # Top 3 movers for the notification
    top3 = summaries[:3]
    top3_str = "  ".join(
        f"{s['ticker']} {'+' if s['change_pct']>=0 else ''}{s['change_pct']:.1f}%"
        for s in top3
    )

    try:
        requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token":     api_token,
                "user":      user_key,
                "title":     f"📊 APEX EOD Report — {datetime.now().strftime('%b %d')}",
                "message":   (
                    f"Today's report is ready.\n\n"
                    f"Watched: {len(summaries)} stocks\n"
                    f"Gainers: {gainers}  Losers: {losers}  Big moves: {big_move}\n"
                    f"SEC filings: {len(sec_filings)}  Alerts: {len(all_alerts)}\n\n"
                    f"Top movers:\n{top3_str}\n\n"
                    f"Tap below to download the full PDF report."
                ),
                "url":       download_url or "",
                "url_title": "⬇️ Download EOD Report PDF",
                "priority":  0,
                "sound":     "cashregister",
            },
            timeout=10
        )
        print("  ✓ Notification sent with download link")
    except Exception as e:
        print(f"  [eod] Notification failed: {e}")


# ─── Main entry point ─────────────────────────────────────────────────────────

def run_eod_report():
    """Called by scanner_loop at 4:00 PM ET."""
    try:
        with open("watchlist_today.json") as f:
            data = json.load(f)
        watchlist = data.get("tickers", [])
    except Exception:
        from config import DEFAULT_UNIVERSE
        watchlist = DEFAULT_UNIVERSE

    if not watchlist:
        print("  [eod] No watchlist")
        return

    filename = generate_eod_report(watchlist)

    if filename:
        try:
            with open("alert_log.json") as f:
                all_alerts = json.load(f)
        except Exception:
            all_alerts = []

        # Re-collect summaries for notification stats
        summaries = []
        for ticker in watchlist:  # All stocks for accurate stats
            s = get_stock_day_summary(ticker)
            if s:
                summaries.append(s)
        summaries.sort(key=lambda x: abs(x.get("change_pct", 0)), reverse=True)

        sec_filings = get_sec_filings_today(watchlist)
        send_report_notification(filename, summaries, sec_filings, all_alerts)

        # Save path for dashboard
        try:
            with open("latest_report.json", "w") as f:
                json.dump({
                    "path":      filename,
                    "date":      datetime.now().strftime("%Y-%m-%d"),
                    "generated": datetime.now().isoformat(),
                }, f)
        except Exception:
            pass


if __name__ == "__main__":
    from config import DEFAULT_UNIVERSE
    run_eod_report()
