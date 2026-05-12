# generate_system_description.py
# Generates an official system description PDF for Axiom Terminal and
# delivers it via Pushover using the Filebin upload pattern from eod_report.py.
#
# Usage: python3 generate_system_description.py

import os
import uuid
import requests
from datetime import datetime
from typing import Optional

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak,
)

REPORTS_DIR = "reports"
os.makedirs(REPORTS_DIR, exist_ok=True)

# ─── Palette ──────────────────────────────────────────────────────────────────
C_BLACK  = colors.HexColor("#0d1117")
C_DARK   = colors.HexColor("#1c1c1c")
C_MID    = colors.HexColor("#3a3a3a")
C_GRAY   = colors.HexColor("#555555")
C_LGRAY  = colors.HexColor("#888888")
C_XGRAY  = colors.HexColor("#cccccc")
C_RULE   = colors.HexColor("#e8e8e8")
C_ACCENT = colors.HexColor("#0a3d62")   # deep navy
C_BLUE   = colors.HexColor("#1e5f99")
C_TEAL   = colors.HexColor("#0e7490")
C_WHITE  = colors.white

PAGE_W, PAGE_H = letter
L_MARGIN = 1.0 * inch
R_MARGIN = 1.0 * inch
T_MARGIN = 1.0 * inch
B_MARGIN = 0.85 * inch
USABLE_W = PAGE_W - L_MARGIN - R_MARGIN


# ─── Style helpers ────────────────────────────────────────────────────────────
def S(name, **kw):
    return ParagraphStyle(name, **kw)


ST = {
    "cover_title": S("ct",
        fontName="Helvetica-Bold", fontSize=22,
        textColor=C_WHITE, leading=28, alignment=TA_LEFT),
    "cover_sub": S("cs",
        fontName="Helvetica", fontSize=11,
        textColor=colors.HexColor("#b0c4d8"), leading=16, alignment=TA_LEFT),
    "cover_meta": S("cm",
        fontName="Helvetica", fontSize=8.5,
        textColor=colors.HexColor("#7a9bb5"), leading=13, alignment=TA_LEFT),
    "toc_head": S("th",
        fontName="Helvetica-Bold", fontSize=10,
        textColor=C_ACCENT, spaceAfter=8),
    "toc_item": S("ti",
        fontName="Helvetica", fontSize=9,
        textColor=C_DARK, leading=16, leftIndent=12),
    "section": S("sh",
        fontName="Helvetica-Bold", fontSize=10,
        textColor=C_ACCENT, spaceBefore=18, spaceAfter=6,
        borderPad=0, leading=14),
    "subsection": S("ssh",
        fontName="Helvetica-Bold", fontSize=9,
        textColor=C_DARK, spaceBefore=10, spaceAfter=4, leading=13),
    "body": S("b",
        fontName="Helvetica", fontSize=9,
        textColor=C_DARK, spaceAfter=5, leading=14, alignment=TA_JUSTIFY),
    "bullet": S("bl",
        fontName="Helvetica", fontSize=9,
        textColor=C_DARK, spaceAfter=3, leading=13,
        leftIndent=16, firstLineIndent=-8),
    "mono": S("mo",
        fontName="Helvetica", fontSize=8.5,
        textColor=C_MID, spaceAfter=2, leading=12,
        leftIndent=20),
    "caption": S("cap",
        fontName="Helvetica-Oblique", fontSize=8,
        textColor=C_LGRAY, spaceAfter=4, leading=12),
    "disclaimer": S("disc",
        fontName="Helvetica", fontSize=7.5,
        textColor=C_GRAY, leading=11, alignment=TA_JUSTIFY),
    "small_bold": S("sb",
        fontName="Helvetica-Bold", fontSize=8,
        textColor=C_GRAY, spaceAfter=2, leading=12),
}


def _hr(thick=0.5, color=C_XGRAY, before=2, after=8):
    return HRFlowable(width="100%", thickness=thick, color=color,
                      spaceBefore=before, spaceAfter=after)


def _heavy_hr():
    return HRFlowable(width="100%", thickness=1.5, color=C_ACCENT,
                      spaceBefore=4, spaceAfter=10)


def _sp(h=6):
    return Spacer(1, h)


def P(text, style="body"):
    return Paragraph(text, ST[style])


def _bullet(text):
    return Paragraph(f"&bull;&nbsp;&nbsp;{text}", ST["bullet"])


def _kv_table(rows, col1_w=2.0):
    """Two-column key-value table."""
    c1 = col1_w * inch
    c2 = USABLE_W - c1
    data = [[Paragraph(k, ST["small_bold"]),
             Paragraph(v, ST["body"])] for k, v in rows]
    t = Table(data, colWidths=[c1, c2])
    t.setStyle(TableStyle([
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING",   (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 8),
        ("LINEBELOW",     (0, 0), (-1, -1), 0.3, C_RULE),
    ]))
    return t


def _data_table(headers, rows, col_widths, right_cols=None):
    right_cols = right_cols or []
    data = [headers] + rows
    t = Table(data, colWidths=[w * inch for w in col_widths], repeatRows=1)
    ts = TableStyle([
        ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, -1), 8.5),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  C_ACCENT),
        ("TEXTCOLOR",     (0, 1), (-1, -1), C_DARK),
        ("BACKGROUND",    (0, 0), (-1, 0),  colors.HexColor("#f0f4f8")),
        ("LINEBELOW",     (0, 0), (-1, 0),  0.75, C_ACCENT),
        ("LINEBELOW",     (0, 1), (-1, -1), 0.3,  C_RULE),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 6),
        ("ALIGN",         (0, 0), (0, -1),  "LEFT"),
    ])
    for c in right_cols:
        ts.add("ALIGN", (c, 0), (c, -1), "RIGHT")
    t.setStyle(ts)
    return t


# ─── Cover page ───────────────────────────────────────────────────────────────
def _cover_page(canvas, doc):
    canvas.saveState()
    w, h = letter

    # Navy header band
    canvas.setFillColor(C_ACCENT)
    canvas.rect(0, h - 2.6 * inch, w, 2.6 * inch, fill=1, stroke=0)

    # Accent rule below band
    canvas.setFillColor(C_TEAL)
    canvas.rect(0, h - 2.65 * inch, w, 0.05 * inch, fill=1, stroke=0)

    # Wordmark
    canvas.setFont("Helvetica-Bold", 28)
    canvas.setFillColor(C_WHITE)
    canvas.drawString(L_MARGIN, h - 1.3 * inch, "AXIOM TERMINAL")

    canvas.setFont("Helvetica", 11)
    canvas.setFillColor(colors.HexColor("#b0c4d8"))
    canvas.drawString(L_MARGIN, h - 1.72 * inch,
                      "Proprietary Small-Cap Research & Signal Generation System")

    # Meta block
    canvas.setFont("Helvetica", 8.5)
    canvas.setFillColor(colors.HexColor("#7a9bb5"))
    meta_lines = [
        f"Document Type:    System Description",
        f"Classification:   Confidential — For Authorized Use Only",
        f"Prepared by:      Vishwa Esakimuthu  |  Proprietary System Owner",
        f"Date of Issue:    {datetime.now().strftime('%B %d, %Y')}",
        f"Version:          1.0",
    ]
    y = h - 2.15 * inch
    for line in meta_lines:
        canvas.drawString(L_MARGIN, y, line)
        y -= 0.155 * inch

    # Footer rule
    canvas.setStrokeColor(C_XGRAY)
    canvas.setLineWidth(0.5)
    canvas.line(L_MARGIN, 0.9 * inch, w - R_MARGIN, 0.9 * inch)

    canvas.setFont("Helvetica", 7.5)
    canvas.setFillColor(C_LGRAY)
    canvas.drawString(L_MARGIN, 0.65 * inch,
                      "Axiom Terminal  —  Confidential  —  Research Tool Only  —  Not Financial Advice")
    canvas.drawRightString(w - R_MARGIN, 0.65 * inch, "Page 1")
    canvas.restoreState()


def _later_pages(canvas, doc):
    canvas.saveState()
    w, _ = letter

    # Top accent bar
    canvas.setFillColor(C_ACCENT)
    canvas.rect(0, PAGE_H - 0.38 * inch, w, 0.38 * inch, fill=1, stroke=0)
    canvas.setFont("Helvetica-Bold", 7.5)
    canvas.setFillColor(C_WHITE)
    canvas.drawString(L_MARGIN, PAGE_H - 0.24 * inch, "AXIOM TERMINAL")
    canvas.setFont("Helvetica", 7.5)
    canvas.drawString(L_MARGIN + 1.05 * inch, PAGE_H - 0.24 * inch,
                      "Proprietary Research System Description")
    canvas.drawRightString(w - R_MARGIN, PAGE_H - 0.24 * inch,
                           f"Confidential  |  {datetime.now().strftime('%B %Y')}")

    # Bottom rule
    canvas.setStrokeColor(C_XGRAY)
    canvas.setLineWidth(0.5)
    canvas.line(L_MARGIN, 0.6 * inch, w - R_MARGIN, 0.6 * inch)
    canvas.setFont("Helvetica", 7.5)
    canvas.setFillColor(C_LGRAY)
    canvas.drawString(L_MARGIN, 0.38 * inch,
                      "For authorized use only. Not financial advice.")
    canvas.drawRightString(w - R_MARGIN, 0.38 * inch, f"Page {doc.page}")
    canvas.restoreState()


# ─── Document body ────────────────────────────────────────────────────────────
def _build_story() -> list:
    s = []

    # ── Section 1: Executive Summary ─────────────────────────────────────────
    s += [P("1.  EXECUTIVE SUMMARY", "section"), _heavy_hr()]
    s += [P(
        "Axiom Terminal is a fully automated proprietary research platform designed for the "
        "systematic identification, analysis, and monitoring of small-capitalization U.S. equities "
        "exhibiting elevated probability of near-term price appreciation. The system operates "
        "continuously across all market sessions — pre-market, regular market hours, after-hours, "
        "and overnight — and delivers real-time signal intelligence through a multi-layer scoring "
        "architecture informed by technical, fundamental, catalyst, sentiment, and quantitative inputs.",
    )]
    s += [P(
        "The platform is a personal research tool designed for a single authorized user. It is not "
        "a registered investment adviser, broker-dealer, or trading system. All output is for "
        "informational and research purposes only. No trade execution is performed by the system; "
        "paper trading simulations are conducted solely for accuracy benchmarking and methodology "
        "validation."
    )]
    s += [_sp(4)]

    # ── Section 2: System Architecture ───────────────────────────────────────
    s += [P("2.  SYSTEM ARCHITECTURE & DEPLOYMENT INFRASTRUCTURE", "section"), _heavy_hr()]
    s += [P(
        "Axiom Terminal is deployed on Railway cloud infrastructure as two discrete, independently "
        "managed services sharing a common PostgreSQL database:"
    )]
    s += [_bullet("<b>Service 1 — Web Dashboard (app.py):</b> A Streamlit-based interactive "
                  "research interface providing portfolio management, signal review, alert history, "
                  "paper trading analytics, accuracy reporting, and scanner control. Accessible via "
                  "authenticated HTTPS.")]
    s += [_bullet("<b>Service 2 — Scanner Loop (scanner_loop.py):</b> A headless background process "
                  "that runs continuously, executing market data acquisition, scoring, signal "
                  "generation, conviction analysis, and accuracy validation on a timed schedule. "
                  "Communicates with the web dashboard exclusively through shared database state.")]
    s += [_sp(4)]
    s += [_data_table(
        ["Component", "Technology", "Description"],
        [
            ["Database",       "PostgreSQL (Railway)",   "Shared persistent state — all tables"],
            ["Web Framework",  "Streamlit ≥1.37",        "Dashboard UI, authentication, controls"],
            ["Cloud Host",     "Railway.app",            "Container-based deployment, auto-restart"],
            ["Local Fallback", "SQLite",                 "Development environment fallback"],
            ["Task Schedule",  "APScheduler / polling",  "Timed scan cycles, nightly routines"],
        ],
        [2.0, 1.8, 2.8],
    )]
    s += [_sp(8)]

    # ── Section 3: Authentication & Access Control ────────────────────────────
    s += [P("3.  AUTHENTICATION & ACCESS CONTROL", "section"), _heavy_hr()]
    s += [P(
        "All access to the Axiom Terminal web dashboard and mobile interface is protected by "
        "username and password authentication. Credentials are stored using bcrypt key derivation "
        "(cost factor 12) and are never stored in plaintext. Session tokens are cryptographically "
        "random (256-bit) and expire automatically. Self-registration is disabled; new accounts "
        "may only be created by an authenticated administrator."
    )]
    s += [_kv_table([
        ("Password Storage",   "bcrypt hash, cost factor 12"),
        ("Session Tokens",     "256-bit cryptographically random, database-validated"),
        ("Registration",       "Disabled — admin-only account creation"),
        ("Access Levels",      "role='admin' (full control) | role='user' (read + control)"),
        ("Mobile Interface",   "Separate Streamlit app (mobile.py), bcrypt-authenticated"),
    ])]
    s += [_sp(8)]

    # ── Section 4: Data Sources ───────────────────────────────────────────────
    s += [P("4.  DATA SOURCES & MARKET DATA ACQUISITION", "section"), _heavy_hr()]
    s += [P(
        "The system employs a waterfall data acquisition strategy that prioritizes real-time "
        "sources and degrades gracefully to delayed or cached data when primary sources are "
        "unavailable. All external API calls are subject to rate limiting, timeout management, "
        "and error isolation."
    )]
    s += [_data_table(
        ["Source", "Data Type", "Latency", "Primary Use"],
        [
            ["Tiingo IEX",    "Real-time trades & quotes",     "< 90 sec",   "Intraday price, RVOL, VWAP"],
            ["Finnhub",       "Quotes, fundamentals, calendar", "15–60 sec",  "Scores, earnings dates"],
            ["Yahoo Finance", "Historical OHLCV, financials",   "15 min+",    "Backtesting, grading"],
            ["SEC EDGAR",     "S-3, 424B, 8-K, Form 4 filings","Near real-time","Catalyst detection"],
            ["RSS / News",    "Headlines, press releases",      "Minutes",    "Sentiment scoring"],
            ["Claude API",    "AI filing text analysis",        "On-demand",  "Filing interpretation"],
        ],
        [1.5, 2.0, 1.2, 2.0],
    )]
    s += [_sp(6)]
    s += [P(
        "A quote cache table (quote_cache) stores the most recent price data per ticker with "
        "source attribution and staleness tracking. The resilient fetcher module (resilient_fetcher.py) "
        "manages source priority, retry logic, and automatic fallback sequencing."
    )]
    s += [_sp(4)]

    # ── Section 5: Universe & Watchlist ──────────────────────────────────────
    s += [P("5.  UNIVERSE CONSTRUCTION & WATCHLIST MANAGEMENT", "section"), _heavy_hr()]
    s += [P(
        "The scannable universe consists of U.S.-listed equities meeting the following baseline "
        "criteria, maintained in the universe table and refreshed periodically:"
    )]
    s += [_bullet("Market capitalization: $10M – $2B (small-cap focus)")]
    s += [_bullet("Average daily volume (20-day): ≥ 500,000 shares")]
    s += [_bullet("Exchange: NYSE, NYSE American, NASDAQ (primary listings only)")]
    s += [_bullet("Active listing status — no pink sheets, OTC bulletin board, or suspended securities")]
    s += [_sp(4)]
    s += [P(
        "Each pre-market session (starting 6:00 AM ET), the morning screen module "
        "(morning_screen.py) constructs a dynamic daily watchlist by filtering the universe for "
        "elevated gap activity, volume pre-signals, and recent catalyst events. This intraday "
        "watchlist supplements the static default universe and is stored in the watchlist table."
    )]
    s += [_sp(4)]

    # ── Section 6: Scoring Engine ─────────────────────────────────────────────
    s += [P("6.  SIGNAL GENERATION & COMPOSITE SCORING METHODOLOGY", "section"), _heavy_hr()]
    s += [P(
        "Each ticker in the active watchlist is evaluated by a multi-component composite scoring "
        "engine (core/scanner.py) producing a normalized score from 0 to 100. Five independent "
        "analytical dimensions contribute to the composite, each computed separately and then "
        "weighted according to configurable parameters:"
    )]
    s += [_data_table(
        ["Component", "Default Weight", "Primary Inputs"],
        [
            ["Technical Score",     "35%", "RSI-14, MACD, SMA-20/50 cross, VWAP position, return_1d/5d, volume ratio"],
            ["Catalyst Score",      "25%", "SEC filings (S-3, 8-K, 424B, Form 4), news density, press release recency"],
            ["Fundamental Score",   "20%", "Revenue growth, gross margin, debt/equity, cash runway, float size"],
            ["Risk Score",          "10%", "Going-concern warnings, ATM offerings, shelf registrations, short interest, volatility"],
            ["Sentiment Score",     "10%", "News headline polarity, SEC filing language analysis via Claude API"],
        ],
        [2.0, 1.4, 3.2],
    )]
    s += [_sp(6)]
    s += [P("The composite score maps to a signal classification as follows:")]
    s += [_data_table(
        ["Score Range", "Signal Label", "Action Classification"],
        [
            ["75 – 100", "Strong Buy Candidate", "High-conviction research candidate — full analysis"],
            ["60 – 75",  "Speculative Buy",       "Moderate-conviction — further due diligence warranted"],
            ["45 – 60",  "Watchlist",             "Monitor — insufficient catalyst or technical confirmation"],
            ["35 – 45",  "Hold",                  "Neutral — no current edge identified"],
            ["25 – 35",  "Trim",                  "Mild negative bias — consider reducing exposure"],
            ["15 – 25",  "Sell",                  "Negative signals — avoid new positions"],
            ["0 – 15",   "Avoid",                 "Strong negative bias — active risk flags present"],
        ],
        [1.4, 2.0, 3.2],
        right_cols=[],
    )]
    s += [_sp(4)]
    s += [P(
        "Risk overrides are applied post-scoring: securities with active going-concern warnings, "
        "announced ATM offerings, shelf registrations, reverse splits, extreme volatility "
        "(annualized >100%), or short interest exceeding 20% of float receive forced classification "
        "to 'Avoid' regardless of composite score."
    )]
    s += [_sp(4)]

    # ── Section 7: Quantitative Enhancement ──────────────────────────────────
    s += [P("7.  QUANTITATIVE ENHANCEMENT FRAMEWORK", "section"), _heavy_hr()]
    s += [P(
        "Following the base composite score, an optional quantitative adjustment layer "
        "(quant_engine.py) computes additional statistical features and applies an additive "
        "score modification capped within the 0–100 range:"
    )]
    s += [_bullet("<b>RSI Momentum Signal:</b> RSI-14 percentile relative to 90-day historical distribution. "
                  "Scores in the 40–65 zone (momentum-neutral with room to run) contribute positive adjustment.")]
    s += [_bullet("<b>Historical Sigma:</b> Current price return normalized by 30-day realized volatility. "
                  "Moves within ±1.5σ treated as sustainable; >2σ treated as overextended.")]
    s += [_bullet("<b>ATR-14:</b> Average True Range computed for position sizing reference (stop-loss "
                  "and target calculation). Not directly used in scoring.")]
    s += [_sp(4)]
    s += [P(
        "The quant adjustment is bounded at ±15 points and is skipped if the computation exceeds "
        "a 500ms timeout to prevent latency injection into the scan cycle."
    )]
    s += [_sp(4)]

    # ── Section 8: Signal Quality Tagging ────────────────────────────────────
    s += [P("8.  SIGNAL QUALITY CLASSIFICATION", "section"), _heavy_hr()]
    s += [P(
        "Each logged signal is assigned a quality tag based on the combination of composite score, "
        "quantitative adjustment, and relative volume at time of signal:"
    )]
    s += [_data_table(
        ["Quality Tag", "Criteria", "Interpretation"],
        [
            ["HIGH",   "Score ≥ 75  AND  Quant adj ≥ +5  AND  RVOL ≥ 2.0×",  "All three signals aligned — highest confidence"],
            ["MEDIUM", "Score ≥ 65  AND  RVOL ≥ 1.3×",                        "Primary signals present — normal conviction"],
            ["LOW",    "All other signals meeting the base logging threshold", "Weak confirmation — reference only"],
        ],
        [1.1, 3.3, 2.2],
    )]
    s += [_sp(4)]

    # ── Section 9: Scan Modes & Schedule ─────────────────────────────────────
    s += [P("9.  SCAN MODES & OPERATING SCHEDULE", "section"), _heavy_hr()]
    s += [P(
        "The scanner operates in four distinct modes determined by current Eastern Time. Each mode "
        "adjusts scan interval, data source priority, and alert thresholds:"
    )]
    s += [_data_table(
        ["Mode", "Hours (ET)", "Scan Interval", "Behavior"],
        [
            ["PREMARKET",  "04:00 – 09:25",  "90 seconds",  "Focus on gap-ups, SEC filings, news. Slower cadence."],
            ["MARKET",     "09:25 – 16:00",  "60 seconds",  "Full scoring, volume spikes, VWAP, prediction scans."],
            ["AFTERHOURS", "16:00 – 22:00",  "120 seconds", "Earnings reactions, after-hours moves. Conviction run at 4PM and 8:30PM."],
            ["OVERNIGHT",  "22:00 – 04:00",  "300 seconds", "Minimal activity. Nightly validation and accuracy grading."],
            ["WEEKEND",    "Sat–Sun",         "600 seconds", "Grade pending signals, no new market data."],
        ],
        [1.3, 1.6, 1.3, 2.5],
    )]
    s += [_sp(6)]
    s += [P("Scheduled automated routines:")]
    s += [_bullet("<b>06:00 AM ET daily:</b> Morning screen — dynamic watchlist construction")]
    s += [_bullet("<b>Variable intraday:</b> Prediction scans every 30 minutes — top 10 momentum tickers scored")]
    s += [_bullet("<b>04:00 PM ET daily:</b> EOD report generation — PDF with day summary, signals, and alerts")]
    s += [_bullet("<b>04:00 PM + 08:30 PM ET:</b> Conviction analysis engine — ranked buy list with position sizing")]
    s += [_bullet("<b>10:00 PM ET daily:</b> Accuracy validation — grades all matured signals, updates metrics")]
    s += [_bullet("<b>Midnight ET daily:</b> Counter reset — suppression state and daily signal counts cleared")]
    s += [_sp(4)]

    # ── Section 10: Alert & Deduplication Logic ───────────────────────────────
    s += [P("10.  ALERT GENERATION & DEDUPLICATION LOGIC", "section"), _heavy_hr()]
    s += [P("The following event types generate push notifications via the Pushover API:")]
    s += [_data_table(
        ["Alert Type", "Trigger Condition", "Priority"],
        [
            ["Volume Spike",     "Relative volume ≥ 2.5× with price move > 0%",          "Normal"],
            ["Price Move",       "Intraday price move ≥ 5% with volume confirmation",     "Normal"],
            ["Gap Up",           "Open/prev-close gap ≥ 4% with RVOL ≥ 1.5×",            "Normal"],
            ["VWAP Cross",       "Price crosses VWAP with extended move > 5% above",      "Normal"],
            ["Level Break",      "Defined support/resistance level broken on volume",     "Normal"],
            ["SEC Filing",       "S-3, 424B, 8-K, or Form 4 detected for watched ticker","High"],
            ["Morning Brief",    "06:30 AM daily watchlist and market context summary",   "Normal"],
            ["Prediction Buy",   "Composite score ≥ 65 with quality tag = HIGH/MEDIUM",  "High"],
            ["Accuracy Report",  "Nightly grading complete — summary of graded signals",  "Normal"],
            ["Mode Transition",  "Scanner mode changes (MARKET→AFTERHOURS, etc.)",        "Normal"],
        ],
        [1.8, 3.1, 0.9],
    )]
    s += [_sp(6)]
    s += [P(
        "<b>Deduplication:</b> A suppression mechanism prevents duplicate alerts for the same "
        "ticker/signal combination within a 5-minute rolling window. All alerts are persisted to "
        "the alert_log table and are visible in the dashboard and mobile interface."
    )]
    s += [_sp(4)]

    # ── Section 11: Conviction Engine ────────────────────────────────────────
    s += [P("11.  CONVICTION ANALYSIS ENGINE", "section"), _heavy_hr()]
    s += [P(
        "The conviction engine (conviction_engine.py) runs twice daily — once at 4:00 PM ET (end "
        "of regular session) and once at 8:30 PM ET (after-hours reassessment). It re-evaluates "
        "the current watchlist and produces a ranked list of the highest-conviction research "
        "candidates with associated position parameters:"
    )]
    s += [_bullet("Conviction score: composite of base score, quant adjustment, catalyst recency, and volume profile")]
    s += [_bullet("Hold type classification: swing (2–10 day), momentum (intraday–2 day), or value (longer-term)")]
    s += [_bullet("Entry, stop-loss (default 7% below entry), and three-tier target levels (T1: +10%, T2: +20%, T3: variable)")]
    s += [_bullet("Position sizing expressed as percentage of notional portfolio")]
    s += [_bullet("Expected value calculation: (win_probability × avg_win) − (loss_probability × avg_loss)")]
    s += [_sp(4)]
    s += [P(
        "Conviction buy list entries are persisted to the conviction_buys table and displayed "
        "on the Signals tab of the dashboard."
    )]
    s += [_sp(4)]

    # ── Section 12: Paper Trading ─────────────────────────────────────────────
    s += [P("12.  PAPER TRADING SIMULATION", "section"), _heavy_hr()]
    s += [P(
        "Axiom Terminal includes a paper trading module (paper_broker.py) that simulates trade "
        "execution against system-generated signals using realistic entry, stop-loss, and target "
        "parameters. No real capital is deployed through this system. The paper trading module "
        "serves exclusively as a performance benchmarking and methodology validation tool."
    )]
    s += [_kv_table([
        ("Entry Price",       "Signal price at time of signal generation"),
        ("Default Stop-Loss", "7% below entry price"),
        ("Target 1",          "+10% above entry"),
        ("Target 2",          "+20% above entry"),
        ("Starting Capital",  "$25,000 simulated (configurable)"),
        ("Position Sizing",   "Conviction-weighted, subject to max portfolio concentration"),
        ("Tracked Metrics",   "P&L %, win rate, profit factor, equity curve, Sharpe ratio"),
    ])]
    s += [_sp(8)]

    # ── Section 13: Accuracy Validation ──────────────────────────────────────
    s += [P("13.  ACCURACY VALIDATION & SIGNAL QUALITY ASSESSMENT", "section"), _heavy_hr()]
    s += [P(
        "The accuracy validator (accuracy_validator.py) grades all historical signals against "
        "actual market outcomes using verified closing prices retrieved from Yahoo Finance. "
        "Grading occurs automatically on startup and nightly, using a horizon-specific approach:"
    )]
    s += [_data_table(
        ["Horizon", "Eligibility", "Win Threshold", "Metric"],
        [
            ["1-Day  (outcome_1d)", "Signal ≥ 1 calendar day old", "> +2.0%",  "% return at 1 trading day close"],
            ["3-Day  (outcome_3d)", "Signal ≥ 3 calendar days old","> +2.0%",  "% return at 3 trading day close"],
            ["5-Day  (outcome_5d)", "Signal ≥ 5 calendar days old","> +2.0%",  "% return at 5 trading day close"],
        ],
        [2.0, 2.0, 1.2, 1.5],
    )]
    s += [_sp(6)]
    s += [P("Accuracy metrics are computed in five score buckets:")]
    s += [_bullet("65–70  |  70–75  |  75–80  |  80–85  |  85+")]
    s += [_sp(4)]
    s += [P(
        "Per-bucket statistics include: sample count (N), win rate, profit factor, expected value "
        "per trade, Sharpe ratio, maximum drawdown, t-statistic, and p-value. A bucket is "
        "automatically disabled (suppressing future signals from that range) when win rate falls "
        "below 45% with N ≥ 30 observations."
    )]
    s += [_sp(4)]

    # ── Section 14: Risk Controls ─────────────────────────────────────────────
    s += [P("14.  RISK CONTROLS & SYSTEM SAFEGUARDS", "section"), _heavy_hr()]
    s += [P("The following risk management controls are implemented at the system level:")]
    s += [_bullet("<b>Risk flag overrides:</b> Six categories of structural risk flags force 'Avoid' "
                  "classification regardless of technical or fundamental score.")]
    s += [_bullet("<b>Signal suppression:</b> Identical ticker/signal combinations are suppressed for 5 minutes "
                  "to prevent alert flooding.")]
    s += [_bullet("<b>Accuracy-based bucket disabling:</b> Score buckets with demonstrated poor accuracy "
                  "(<45% win rate, N ≥ 30) are automatically disabled.")]
    s += [_bullet("<b>Market hours enforcement:</b> Alert thresholds are higher in pre-market and after-hours "
                  "sessions to account for reduced liquidity.")]
    s += [_bullet("<b>Data staleness detection:</b> Quotes older than 90 seconds are flagged; scoring is "
                  "degraded for stale data inputs.")]
    s += [_bullet("<b>Scanner pause control:</b> Authorized users can pause the scanner loop instantly "
                  "via the dashboard control interface.")]
    s += [_bullet("<b>Timeout management:</b> All external API calls are subject to hard timeouts (10–30 sec). "
                  "The quantitative enhancement module is skipped if computation exceeds 500ms.")]
    s += [_sp(4)]

    # ── Section 15: Compliance Considerations ────────────────────────────────
    s += [P("15.  COMPLIANCE CONSIDERATIONS & REGULATORY CONTEXT", "section"), _heavy_hr()]
    s += [P(
        "Axiom Terminal is a personal research and data aggregation tool. The system does not "
        "provide investment advice within the meaning of the Investment Advisers Act of 1940, "
        "does not manage assets on behalf of any third party, and does not execute any real "
        "securities transactions. The following representations apply:"
    )]
    s += [_bullet("<b>No trade execution:</b> The system generates research signals only. No brokerage "
                  "connectivity, order routing, or automated execution capability exists.")]
    s += [_bullet("<b>Single-user research tool:</b> Output is consumed exclusively by the authorized "
                  "system owner for personal research purposes.")]
    s += [_bullet("<b>Data sourced from public APIs:</b> All market data is obtained from commercially "
                  "available APIs (Finnhub, Tiingo, Yahoo Finance, SEC EDGAR EDGAR RSS) under the "
                  "respective providers' terms of service.")]
    s += [_bullet("<b>SEC EDGAR access:</b> SEC filing data is accessed through the public EDGAR "
                  "full-text search system (efts.sec.gov) and RSS feeds in compliance with SEC "
                  "fair access policies.")]
    s += [_bullet("<b>AI-assisted analysis:</b> Claude API (Anthropic) is used for natural language "
                  "analysis of SEC filing text. AI output is treated as one signal input among many "
                  "and is not the sole basis for any research output.")]
    s += [_bullet("<b>No distribution:</b> System output, signals, and reports are not distributed, "
                  "published, or made available to any third party.")]
    s += [_sp(4)]

    # ── Section 16: Technical Specifications ─────────────────────────────────
    s += [P("16.  TECHNICAL SPECIFICATIONS", "section"), _heavy_hr()]
    s += [_data_table(
        ["Specification", "Detail"],
        [
            ["Runtime",            "Python 3.10+"],
            ["Web Framework",      "Streamlit ≥ 1.37"],
            ["Database",           "PostgreSQL (production) / SQLite (development)"],
            ["ORM / DB Driver",    "psycopg2-binary (PG), sqlite3 (SQLite)"],
            ["Data Libraries",     "pandas ≥ 2.0, numpy ≥ 1.24, yfinance ≥ 0.2.36"],
            ["Technical Analysis", "ta ≥ 0.11, pandas-ta ≥ 0.3.14b"],
            ["Statistics",         "scipy ≥ 1.11 (t-tests, significance testing)"],
            ["AI Integration",     "anthropic ≥ 0.25 (Claude API)"],
            ["PDF Generation",     "reportlab ≥ 4.0"],
            ["Push Notifications", "Pushover API via requests ≥ 2.31"],
            ["Auth / Security",    "bcrypt ≥ 4.0 (password hashing), cryptographic session tokens"],
            ["Scheduling",         "APScheduler ≥ 3.10 / polling loop with ET timezone awareness"],
            ["Cloud Deployment",   "Railway.app — containerized, auto-restart, shared PostgreSQL"],
            ["Source Control",     "Git / GitHub (private repository)"],
        ],
        [2.0, 4.65],
    )]
    s += [_sp(8)]

    # ── Disclaimer ────────────────────────────────────────────────────────────
    s += [_hr(thick=1.0, color=C_ACCENT, before=16, after=10)]
    s += [P("IMPORTANT DISCLOSURES", "small_bold")]
    s += [P(
        "Axiom Terminal is a personal research tool and is not registered with, approved by, or "
        "affiliated with the U.S. Securities and Exchange Commission (SEC), FINRA, or any other "
        "regulatory body. This system does not constitute investment advice, a recommendation to "
        "buy or sell any security, or a solicitation of any investment. Past signal accuracy does "
        "not guarantee future results. Small-capitalization securities involve substantial risk "
        "of loss, including total loss of capital, due to low liquidity, high volatility, limited "
        "public information, and susceptibility to manipulation. The operator of this system is "
        "not a registered investment adviser or broker-dealer. All trading decisions are made "
        "solely by the authorized user based on their own independent research and judgment.",
        "disclaimer",
    )]
    s += [_sp(6)]
    s += [P(
        f"Document generated: {datetime.now().strftime('%B %d, %Y at %H:%M ET')}  |  "
        f"Axiom Terminal v1.0  |  Confidential — For Authorized Use Only",
        "caption",
    )]

    return s


# ─── PDF build ────────────────────────────────────────────────────────────────
def generate_pdf() -> str:
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{REPORTS_DIR}/AxiomTerminal_SystemDescription_{date_str}.pdf"

    doc = SimpleDocTemplate(
        filename,
        pagesize=letter,
        leftMargin=L_MARGIN,
        rightMargin=R_MARGIN,
        topMargin=T_MARGIN + 0.3 * inch,   # extra for header band on later pages
        bottomMargin=B_MARGIN,
        title="Axiom Terminal — System Description",
        author="Vishwa Esakimuthu",
        subject="Proprietary Research System Description",
    )

    # Cover page: blank first page (drawn entirely in _cover_page canvas callback)
    story = [Spacer(1, 4.5 * inch)]

    # Table of contents page
    story += [PageBreak()]
    story += [P("TABLE OF CONTENTS", "toc_head"), _heavy_hr()]
    toc = [
        ("1",  "Executive Summary"),
        ("2",  "System Architecture & Deployment Infrastructure"),
        ("3",  "Authentication & Access Control"),
        ("4",  "Data Sources & Market Data Acquisition"),
        ("5",  "Universe Construction & Watchlist Management"),
        ("6",  "Signal Generation & Composite Scoring Methodology"),
        ("7",  "Quantitative Enhancement Framework"),
        ("8",  "Signal Quality Classification"),
        ("9",  "Scan Modes & Operating Schedule"),
        ("10", "Alert Generation & Deduplication Logic"),
        ("11", "Conviction Analysis Engine"),
        ("12", "Paper Trading Simulation"),
        ("13", "Accuracy Validation & Signal Quality Assessment"),
        ("14", "Risk Controls & System Safeguards"),
        ("15", "Compliance Considerations & Regulatory Context"),
        ("16", "Technical Specifications"),
    ]
    for num, title in toc:
        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;{num}.&nbsp;&nbsp;{title}", ST["toc_item"]))
    story += [_sp(10)]

    # Body
    story += [PageBreak()]
    story += _build_story()

    def _first_page(canvas, doc):
        _cover_page(canvas, doc)

    def _later(canvas, doc):
        if doc.page == 2:
            _later_pages(canvas, doc)   # TOC page uses later_pages header
        else:
            _later_pages(canvas, doc)

    doc.build(story, onFirstPage=_first_page, onLaterPages=_later)
    print(f"  PDF saved: {filename}")
    return filename


# ─── Upload & notify ──────────────────────────────────────────────────────────
def upload_to_filebin(filename: str) -> Optional[str]:
    try:
        bin_id    = f"axiom-sysdesc-{uuid.uuid4().hex[:8]}"
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
            print(f"  Uploaded: {url}")
            return url
        print(f"  Upload failed ({resp.status_code}): {resp.text[:200]}")
        return None
    except Exception as e:
        print(f"  Upload error: {e}")
        return None


def send_pushover(download_url: Optional[str]) -> None:
    user_key  = os.environ.get("PUSHOVER_USER_KEY", "")
    api_token = os.environ.get("PUSHOVER_API_TOKEN", "")
    if not user_key or not api_token:
        print("  No Pushover keys set — skipping notification")
        return

    msg = (
        "Axiom Terminal — System Description PDF generated.\n\n"
        "16 sections covering architecture, data sources, scoring methodology, "
        "conviction engine, paper trading, accuracy validation, and compliance."
    )
    if not download_url:
        msg += "\n\n(Upload failed — check reports/ folder on server)"

    payload = {
        "token":     api_token,
        "user":      user_key,
        "title":     "System Description Ready",
        "message":   msg,
        "priority":  0,
        "sound":     "cashregister",
    }
    if download_url:
        payload["url"]       = download_url
        payload["url_title"] = "Download PDF"

    try:
        resp = requests.post("https://api.pushover.net/1/messages.json",
                             data=payload, timeout=10)
        if resp.status_code == 200:
            print("  Pushover notification sent")
        else:
            print(f"  Pushover failed ({resp.status_code}): {resp.text}")
    except Exception as e:
        print(f"  Pushover error: {e}")


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Axiom Terminal — Generating System Description PDF...")
    print(f"  Date: {datetime.now().strftime('%B %d, %Y')}")
    print()

    print("  Building PDF...")
    filename = generate_pdf()

    print("  Uploading to Filebin...")
    url = upload_to_filebin(filename)

    print("  Sending Pushover notification...")
    send_pushover(url)

    print()
    print(f"Done. File: {filename}")
    if url:
        print(f"URL:  {url}")
