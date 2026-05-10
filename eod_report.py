# eod_report.py
# End-of-day report generator.
# Generates at 4:15 PM ET, uploads to Filebin, sends Pushover notification.
# Clean financial terminal aesthetic — Helvetica, light background, no emojis.

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
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable,
)

FINNHUB_BASE = "https://finnhub.io/api/v1"
REPORTS_DIR  = "reports"

# ─── Palette ──────────────────────────────────────────────────────────────────
C_BLACK = colors.HexColor("#1a1a1a")
C_DARK  = colors.HexColor("#2d2d2d")
C_GRAY  = colors.HexColor("#666666")
C_LGRAY = colors.HexColor("#aaaaaa")
C_XGRAY = colors.HexColor("#dddddd")
C_RULE  = colors.HexColor("#f0f0f0")
C_WHITE = colors.white
C_RED   = colors.HexColor("#cc2222")
C_GREEN = colors.HexColor("#1a7a3c")

PAGE_W, PAGE_H = letter
L_MARGIN = 0.75 * inch
R_MARGIN = 0.75 * inch
T_MARGIN = 0.75 * inch
B_MARGIN = 0.65 * inch
USABLE_W = PAGE_W - L_MARGIN - R_MARGIN


def _S(name, **kw) -> ParagraphStyle:
    return ParagraphStyle(name, **kw)


ST = {
    "title":    _S("t",   fontName="Helvetica-Bold",  fontSize=14, textColor=C_BLACK, spaceAfter=0, leading=18),
    "date":     _S("dt",  fontName="Helvetica",        fontSize=9,  textColor=C_GRAY,  spaceAfter=0, alignment=TA_RIGHT),
    "section":  _S("sh",  fontName="Helvetica-Bold",   fontSize=9,  textColor=C_GRAY,  spaceBefore=14, spaceAfter=4, leading=12),
    "body":     _S("b",   fontName="Helvetica",         fontSize=9,  textColor=C_DARK,  spaceAfter=3, leading=13),
    "mono":     _S("m",   fontName="Helvetica",         fontSize=8.5,textColor=C_DARK,  spaceAfter=2, leading=12),
    "small":    _S("sm",  fontName="Helvetica",         fontSize=8,  textColor=C_GRAY,  spaceAfter=2, leading=12),
    "note":     _S("nt",  fontName="Helvetica-Oblique", fontSize=8,  textColor=C_GRAY,  spaceAfter=2, leading=12),
}


def _footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(C_LGRAY)
    canvas.drawString(L_MARGIN, 0.38 * inch,
                      "APEX SmallCap Scanner — Confidential Research Tool")
    canvas.drawRightString(PAGE_W - R_MARGIN, 0.38 * inch, f"Page {doc.page}")
    canvas.restoreState()


def _hr():
    return HRFlowable(width="100%", thickness=0.5, color=C_XGRAY,
                      spaceAfter=6, spaceBefore=2)


def _section_rule():
    return HRFlowable(width="100%", thickness=0.3, color=C_RULE,
                      spaceAfter=4, spaceBefore=4)


def _table(headers: list, rows: list, col_widths: list,
           right_cols: list = None, total_row: bool = False) -> Table:
    right_cols = right_cols or []
    data = [headers] + rows
    t    = Table(data, colWidths=col_widths, repeatRows=1)
    ts   = TableStyle([
        ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, -1), 8.5),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  C_GRAY),
        ("TEXTCOLOR",     (0, 1), (-1, -1), C_DARK),
        ("LINEBELOW",     (0, 0), (-1, 0),  0.5, C_XGRAY),
        ("LINEBELOW",     (0, 1), (-1, -1), 0.3, C_RULE),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING",   (0, 0), (-1, -1), 4),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
        ("ALIGN",         (0, 0), (0, -1),  "LEFT"),
    ])
    for c in right_cols:
        ts.add("ALIGN", (c, 0), (c, -1), "RIGHT")
    if total_row and len(data) > 1:
        last = len(data) - 1
        ts.add("FONTNAME",  (0, last), (-1, last), "Helvetica-Bold")
        ts.add("LINEABOVE", (0, last), (-1, last), 0.5, C_XGRAY)
    t.setStyle(ts)
    return t


# ─── Data collection ──────────────────────────────────────────────────────────

def _fh_key() -> str:
    return os.environ.get("FINNHUB_API_KEY", "")


def _fh_get(endpoint: str, params: dict) -> Optional[dict]:
    key = _fh_key()
    if not key:
        return None
    try:
        params["token"] = key
        resp = requests.get(f"{FINNHUB_BASE}/{endpoint}",
                            params=params, timeout=10)
        return resp.json() if resp.status_code == 200 else None
    except Exception:
        return None


def _get_stock_summary(ticker: str) -> Dict:
    quote   = _fh_get("quote", {"symbol": ticker})
    profile = _fh_get("stock/profile2", {"symbol": ticker}) or {}

    if not quote or not quote.get("c"):
        return {"ticker": ticker, "_failed": True}

    price      = quote.get("c", 0)
    prev_close = quote.get("pc", 0)
    change_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0
    volume     = quote.get("v", 0)

    avg_vol = 0
    try:
        info    = yf.Ticker(ticker).info
        avg_vol = info.get("averageVolume", 0) or 0
    except Exception:
        pass

    rel_vol = round(volume / avg_vol, 2) if avg_vol > 0 else 0
    return {
        "ticker":       ticker,
        "company_name": profile.get("name", ticker),
        "price":        round(price, 4),
        "prev_close":   round(prev_close, 4),
        "change_pct":   round(change_pct, 2),
        "volume":       int(volume),
        "avg_volume":   int(avg_vol),
        "rel_volume":   rel_vol,
        "sector":       profile.get("finnhubIndustry", ""),
        "_failed":      False,
    }


def _get_sec_filings(tickers: List[str]) -> List[Dict]:
    filings = []
    try:
        feed = feedparser.parse(
            "https://www.sec.gov/cgi-bin/browse-edgar"
            "?action=getcurrent&type=8-K&dateb=&owner=include&count=40&output=atom"
        )
        ticker_set = {t.upper() for t in tickers}
        for entry in feed.entries[:40]:
            title = entry.get("title", "")
            for t in ticker_set:
                if t in title.upper():
                    raw_time = entry.get("updated", "")
                    time_str = raw_time[:16].replace("T", " ") if raw_time else ""
                    filings.append({
                        "ticker": t,
                        "type":   "8-K",
                        "time":   time_str,
                        "title":  title[:80],
                    })
                    break
    except Exception as e:
        print(f"  [eod] SEC check failed: {e}")
    return filings


def _get_todays_signals() -> List[Dict]:
    try:
        from db.database import get_signal_log
        df = get_signal_log(days=1)
        if df.empty:
            return []
        df = df.sort_values("score", ascending=False)
        return df.head(15).to_dict("records")
    except Exception:
        return []


def _get_todays_alerts() -> List[str]:
    try:
        from db.database import load_alerts
        return load_alerts(limit=50)
    except Exception:
        pass
    try:
        with open("alert_log.json") as f:
            return json.load(f)[-30:]
    except Exception:
        return []


def _get_portfolio_snapshot() -> List[Dict]:
    try:
        from db.database import get_portfolio
        df = get_portfolio(user_id=1)
        if df.empty:
            return []
        rows = []
        for _, row in df.iterrows():
            ticker  = row["ticker"]
            shares  = row.get("shares", 0)
            avg_c   = row.get("avg_cost", 0)
            quote   = _fh_get("quote", {"symbol": ticker})
            price   = quote.get("c", 0) if quote else 0
            pnl_pct = ((price - avg_c) / avg_c * 100) if avg_c > 0 and price > 0 else 0
            pnl_usd = (price - avg_c) * shares if price > 0 else 0
            rows.append({
                "ticker":  ticker,
                "shares":  shares,
                "avg_cost": avg_c,
                "price":   price,
                "pnl_pct": pnl_pct,
                "pnl_usd": pnl_usd,
                "value":   price * shares,
            })
        return rows
    except Exception as e:
        print(f"  [eod] Portfolio snapshot failed: {e}")
        return []


def _make_watchpoints(summaries: List[Dict], sec_filings: List[Dict]) -> List[str]:
    """Generate factual watchpoints for tomorrow. No hype language."""
    points = []
    filing_tickers = {f["ticker"] for f in sec_filings}
    for s in summaries:
        if not s or s.get("_failed"):
            continue
        ticker  = s["ticker"]
        chg     = s.get("change_pct", 0)
        rvol    = s.get("rel_volume", 0)
        lines   = []
        if ticker in filing_tickers:
            lines.append("8-K filing today, review before open")
        if abs(chg) > 8:
            mv = "gain" if chg > 0 else "decline"
            lines.append(f"{chg:+.1f}% intraday {mv}, watch for continuation or reversal")
        if rvol > 3:
            lines.append(f"volume {rvol:.1f}x average, above-normal activity")
        if lines:
            points.append(f"{ticker} — {'; '.join(lines)}")
        if len(points) >= 5:
            break
    return points


# ─── PDF generation ───────────────────────────────────────────────────────────

def generate_eod_report(watchlist: List[str]) -> Optional[str]:
    print(f"\n{'='*55}")
    print(f"APEX EOD Report — {datetime.now().strftime('%Y-%m-%d %H:%M ET')}")
    print(f"{'='*55}")

    os.makedirs(REPORTS_DIR, exist_ok=True)
    date_str  = datetime.now().strftime("%Y-%m-%d")
    date_long = datetime.now().strftime("%A, %B %d %Y")
    filename  = f"{REPORTS_DIR}/APEX_EOD_{date_str}.pdf"

    # ── Collect data ──────────────────────────────────────────────────────────
    print(f"  Collecting data for {len(watchlist)} stocks...")
    summaries   = []
    clean_count = 0
    fail_count  = 0
    for ticker in watchlist:
        s = _get_stock_summary(ticker)
        summaries.append(s)
        if s.get("_failed"):
            fail_count += 1
        else:
            clean_count += 1

    summaries_ok = [s for s in summaries if not s.get("_failed")]
    summaries_ok.sort(key=lambda x: abs(x.get("change_pct", 0)), reverse=True)

    print("  Checking SEC filings...")
    sec_filings   = _get_sec_filings(watchlist)
    todays_sigs   = _get_todays_signals()
    todays_alerts = _get_todays_alerts()
    portfolio     = _get_portfolio_snapshot()
    watchpoints   = _make_watchpoints(summaries_ok, sec_filings)

    gainers = sum(1 for s in summaries_ok if s.get("change_pct", 0) > 0)
    losers  = sum(1 for s in summaries_ok if s.get("change_pct", 0) < 0)
    if gainers > losers * 1.5:
        mkt_cond = "Broad-based buying across the watchlist. Gainers led losers."
    elif losers > gainers * 1.5:
        mkt_cond = "Selling pressure across most of the watchlist. Losers led gainers."
    else:
        mkt_cond = f"Mixed session. {gainers} gainers, {losers} decliners across the watchlist."

    # ── Build PDF ─────────────────────────────────────────────────────────────
    print("  Building PDF...")
    doc = SimpleDocTemplate(
        filename, pagesize=letter,
        leftMargin=L_MARGIN, rightMargin=R_MARGIN,
        topMargin=T_MARGIN, bottomMargin=B_MARGIN,
    )
    story = []

    # Header
    hdr = Table([[
        Paragraph("APEX Capital — Daily Scanner Report", ST["title"]),
        Paragraph(date_long, ST["date"]),
    ]], colWidths=[4.5 * inch, 2.5 * inch])
    hdr.setStyle(TableStyle([
        ("VALIGN",       (0, 0), (-1, -1), "BOTTOM"),
        ("LEFTPADDING",  (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 0),
    ]))
    story.append(hdr)
    story.append(_hr())

    # ── 1. MARKET SUMMARY ────────────────────────────────────────────────────
    story.append(Paragraph("MARKET SUMMARY", ST["section"]))
    story.append(Paragraph(
        f"{date_long}.  {mkt_cond}  "
        f"{len(summaries_ok)} stocks scanned.  {len(todays_alerts)} alerts fired today.",
        ST["body"]
    ))
    story.append(_section_rule())

    # ── 2. TOP SIGNALS TODAY ─────────────────────────────────────────────────
    story.append(Paragraph("TOP SIGNALS TODAY", ST["section"]))
    if todays_sigs:
        sig_rows = []
        for sig in todays_sigs[:12]:
            ticker  = sig.get("ticker", "")
            label   = sig.get("signal_label", "")
            score   = sig.get("score", "")
            price   = sig.get("price_at_signal", 0)
            # Try to get change_pct from summaries_ok
            chg     = next((s.get("change_pct", 0) for s in summaries_ok if s["ticker"] == ticker), None)
            vol     = sig.get("volume_at_signal", 0)
            p_str   = f"${price:.4f}" if price and price < 10 else (f"${price:.2f}" if price else "—")
            chg_str = f"{chg:+.1f}%" if chg is not None else "—"
            vol_str = f"{vol/1e6:.1f}M" if vol >= 1e6 else (f"{vol/1e3:.0f}K" if vol >= 1e3 else str(int(vol)))
            sig_rows.append([ticker, label, str(int(score)) if score else "—", p_str, chg_str, vol_str])
        story.append(_table(
            ["Ticker", "Signal", "Score", "Price", "Change%", "Volume"],
            sig_rows,
            [0.7 * inch, 2.2 * inch, 0.6 * inch, 0.9 * inch, 0.8 * inch, 0.8 * inch],
            right_cols=[2, 3, 4, 5],
        ))
    else:
        story.append(Paragraph("No signals logged today.", ST["body"]))
    story.append(_section_rule())

    # ── 3. ALERTS FIRED ──────────────────────────────────────────────────────
    story.append(Paragraph("ALERTS FIRED", ST["section"]))
    if todays_alerts:
        for alert in todays_alerts[-20:]:
            # Alerts are stored as plain strings; display as-is
            story.append(Paragraph(str(alert), ST["mono"]))
    else:
        story.append(Paragraph("No alerts fired today.", ST["body"]))
    story.append(_section_rule())

    # ── 4. SEC FILINGS DETECTED ──────────────────────────────────────────────
    story.append(Paragraph("SEC FILINGS DETECTED", ST["section"]))
    if sec_filings:
        filing_rows = [[f["ticker"], f["type"], f.get("time", "—"), f["title"]] for f in sec_filings]
        story.append(_table(
            ["Ticker", "Type", "Time", "Filing"],
            filing_rows,
            [0.7 * inch, 0.5 * inch, 1.1 * inch, 4.7 * inch],
        ))
    else:
        story.append(Paragraph("No material filings detected today.", ST["body"]))
    story.append(_section_rule())

    # ── 5. PORTFOLIO SNAPSHOT ────────────────────────────────────────────────
    story.append(Paragraph("PORTFOLIO SNAPSHOT", ST["section"]))
    if portfolio:
        port_rows = []
        total_value = 0
        total_pnl   = 0
        for h in portfolio:
            price   = h.get("price", 0)
            avg_c   = h.get("avg_cost", 0)
            pnl_pct = h.get("pnl_pct", 0)
            pnl_usd = h.get("pnl_usd", 0)
            value   = h.get("value", 0)
            total_value += value
            total_pnl   += pnl_usd
            p_str   = f"${price:.4f}" if price < 10 else f"${price:.2f}"
            ac_str  = f"${avg_c:.4f}" if avg_c < 10 else f"${avg_c:.2f}"
            port_rows.append([
                h["ticker"],
                f"{h.get('shares', 0):.0f}",
                ac_str,
                p_str,
                f"${pnl_usd:+,.2f}",
                f"{pnl_pct:+.2f}%",
            ])
        port_rows.append([
            "TOTAL", "", "", "",
            f"${total_pnl:+,.2f}",
            "",
        ])
        story.append(_table(
            ["Ticker", "Shares", "Avg Cost", "Current", "P&L $", "P&L %"],
            port_rows,
            [0.8 * inch, 0.7 * inch, 1.0 * inch, 1.0 * inch, 1.5 * inch, 1.0 * inch],
            right_cols=[1, 2, 3, 4, 5],
            total_row=True,
        ))
    else:
        story.append(Paragraph("No portfolio holdings recorded.", ST["body"]))
    story.append(_section_rule())

    # ── 6. WATCHPOINTS FOR TOMORROW ──────────────────────────────────────────
    story.append(Paragraph("WATCHPOINTS FOR TOMORROW", ST["section"]))
    if watchpoints:
        for wp in watchpoints:
            story.append(Paragraph(wp, ST["body"]))
    else:
        story.append(Paragraph("No specific watchpoints identified for tomorrow.", ST["body"]))
    story.append(_section_rule())

    # ── 7. DATA QUALITY NOTE ─────────────────────────────────────────────────
    story.append(Paragraph("DATA QUALITY NOTE", ST["section"]))
    story.append(Paragraph(
        f"{clean_count} of {len(watchlist)} tickers returned clean data. "
        f"{fail_count} tickers had no quote available.",
        ST["note"]
    ))

    # Footer spacer
    story.append(Spacer(1, 20))
    story.append(_hr())
    story.append(Paragraph(
        f"Research purposes only. Not financial advice. "
        f"Generated {datetime.now().strftime('%H:%M ET')} on {date_str}.",
        _S("disc", fontName="Helvetica", fontSize=7, textColor=C_LGRAY)
    ))

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    print(f"  PDF saved: {filename}")
    return filename


# ─── Upload & Notify ──────────────────────────────────────────────────────────

def upload_to_filebin(filename: str) -> Optional[str]:
    try:
        bin_id    = f"apex-eod-{uuid.uuid4().hex[:8]}"
        file_name = os.path.basename(filename)
        with open(filename, "rb") as f:
            resp = requests.post(
                f"https://filebin.net/{bin_id}/{file_name}",
                data=f, headers={"Content-Type": "application/pdf"}, timeout=30,
            )
        if resp.status_code in (200, 201):
            url = f"https://filebin.net/{bin_id}/{file_name}"
            print(f"  Uploaded: {url}")
            return url
        print(f"  [eod] Upload failed ({resp.status_code})")
        return None
    except Exception as e:
        print(f"  [eod] Upload error: {e}")
        return None


def send_report_notification(filename: str, n_signals: int,
                              n_alerts: int, n_stocks: int) -> None:
    user_key  = os.environ.get("PUSHOVER_USER_KEY", "")
    api_token = os.environ.get("PUSHOVER_API_TOKEN", "")
    if not user_key or not api_token:
        print("  [eod] No Pushover keys — skipping notification")
        return

    print("  Uploading PDF to Filebin...")
    download_url = upload_to_filebin(filename)

    try:
        requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token":     api_token,
                "user":      user_key,
                "title":     f"Daily report ready — {n_alerts} alerts, {n_signals} signals today",
                "message":   (
                    f"Stocks scanned: {n_stocks}\n"
                    f"Signals logged: {n_signals}\n"
                    f"Alerts fired:   {n_alerts}\n\n"
                    f"Tap to download the full PDF."
                ),
                "url":       download_url or "",
                "url_title": "Download Daily Report PDF",
                "priority":  0,
                "sound":     "cashregister",
            },
            timeout=10,
        )
        print("  Notification sent")
    except Exception as e:
        print(f"  [eod] Notification failed: {e}")


# ─── Main entry point ─────────────────────────────────────────────────────────

def run_eod_report():
    """Called by scanner_loop at 4:15 PM ET."""
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
    if not filename:
        return

    todays_sigs   = _get_todays_signals()
    todays_alerts = _get_todays_alerts()
    send_report_notification(filename, len(todays_sigs), len(todays_alerts), len(watchlist))

    # Save path for dashboard download button
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
