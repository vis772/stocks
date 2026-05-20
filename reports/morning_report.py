"""
reports/morning_report.py
Daily morning PDF — top signals from overnight/premarket scans with
score, action (BUY / WATCH), and a plain-English reason per ticker.

Called by scanner_loop at 8:30 AM ET after the morning screen runs.
"""
import os
import uuid
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable

REPORTS_DIR = "reports"

C_BLACK = colors.HexColor("#1a1a1a")
C_DARK  = colors.HexColor("#2d2d2d")
C_GRAY  = colors.HexColor("#666666")
C_LGRAY = colors.HexColor("#aaaaaa")
C_XGRAY = colors.HexColor("#dddddd")
C_RULE  = colors.HexColor("#f0f0f0")
C_WHITE = colors.white
C_GREEN = colors.HexColor("#1a7a3c")
C_AMBER = colors.HexColor("#b86400")
C_RED   = colors.HexColor("#cc2222")
C_BLUE  = colors.HexColor("#1a56b0")

PAGE_W, PAGE_H = letter
L_MARGIN = 0.75 * inch
R_MARGIN = 0.75 * inch
USABLE_W = PAGE_W - L_MARGIN - R_MARGIN

ET = timezone(timedelta(hours=-4))   # EDT (UTC-4); adjust to -5 for EST in winter


def _S(name, **kw):
    return ParagraphStyle(name, **kw)


ST = {
    "title":   _S("t",  fontName="Helvetica-Bold",   fontSize=14, textColor=C_BLACK, spaceAfter=0, leading=18),
    "date":    _S("dt", fontName="Helvetica",         fontSize=9,  textColor=C_GRAY,  spaceAfter=0, alignment=TA_RIGHT),
    "section": _S("sh", fontName="Helvetica-Bold",    fontSize=9,  textColor=C_GRAY,  spaceBefore=14, spaceAfter=4, leading=12),
    "body":    _S("b",  fontName="Helvetica",         fontSize=9,  textColor=C_DARK,  spaceAfter=4, leading=14),
    "small":   _S("sm", fontName="Helvetica",         fontSize=8,  textColor=C_GRAY,  spaceAfter=3, leading=12),
    "concl":   _S("cl", fontName="Helvetica-Oblique", fontSize=9,  textColor=C_DARK,  spaceAfter=4, leading=14, leftIndent=8),
}


def _footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(C_LGRAY)
    canvas.drawString(L_MARGIN, 0.38 * inch, "Axiom Terminal — Confidential Research Tool")
    canvas.drawRightString(PAGE_W - R_MARGIN, 0.38 * inch, f"Page {doc.page}")
    canvas.restoreState()


def _hr():
    return HRFlowable(width="100%", thickness=0.5, color=C_XGRAY, spaceAfter=6, spaceBefore=2)


def _action(signal_label: str, score: float) -> tuple[str, object]:
    """Return (action_text, color) based on signal label and score."""
    if signal_label in ("Strong Buy Candidate",) or score >= 75:
        return "BUY", C_GREEN
    if signal_label in ("Speculative Buy", "Gap-Up") or score >= 60:
        return "BUY", C_GREEN
    if signal_label == "Watchlist" or score >= 45:
        return "WATCH", C_AMBER
    return "SKIP", C_RED


def _reason(score_breakdown: dict, signal_label: str, score: float) -> str:
    """Generate a plain-English reason from score_breakdown."""
    if not isinstance(score_breakdown, dict):
        return f"{signal_label} — score {score:.0f}"

    tech  = score_breakdown.get("technical",   0)
    fund  = score_breakdown.get("fundamental", 0)
    risk  = score_breakdown.get("risk",        0)
    sent  = score_breakdown.get("sentiment",   0)
    cat   = score_breakdown.get("catalyst",    0)

    parts = []

    # Strengths
    if fund >= 80:
        parts.append("strong fundamentals")
    elif fund >= 65:
        parts.append("solid fundamentals")

    if tech >= 75:
        parts.append("high technical momentum")
    elif tech >= 60:
        parts.append("positive technical setup")

    if risk >= 70:
        parts.append("low dilution/liquidity risk")

    if sent >= 75:
        parts.append("positive news sentiment")

    if cat >= 65:
        parts.append("recent catalyst")

    # Warnings
    if risk < 30:
        parts.append("elevated risk")
    if fund < 35:
        parts.append("weak fundamentals")
    if tech < 30:
        parts.append("poor technical setup")

    if not parts:
        # Fallback: name top component
        top = max(score_breakdown, key=lambda k: score_breakdown.get(k, 0))
        parts.append(f"led by {top} score ({score_breakdown[top]:.0f})")

    return "; ".join(parts).capitalize() + "."


def _fetch_todays_signals() -> pd.DataFrame:
    """Pull today's signals from signal_log — most recent per ticker, score >= 45."""
    from db.database import _is_postgres, _get_pg_conn, _put_pg_conn, _get_sqlite_conn
    import json

    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT ON (ticker)
                       id, ticker, signal_label, score, score_breakdown,
                       price_at_signal, created_at
                FROM signal_log
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                  AND score >= 45
                ORDER BY ticker, score DESC, created_at DESC
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            cur.close(); _put_pg_conn(conn)
        else:
            conn = _get_sqlite_conn(); cur = conn.cursor()
            cur.execute("""
                SELECT id, ticker, signal_label, score, score_breakdown,
                       price_at_signal, created_at
                FROM signal_log
                WHERE created_at >= datetime('now', '-24 hours')
                  AND score >= 45
                ORDER BY ticker, score DESC, created_at DESC
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            conn.close()
            # Dedupe by ticker keeping highest score
            seen = {}
            deduped = []
            for r in rows:
                d = dict(zip(cols, r))
                if d["ticker"] not in seen:
                    seen[d["ticker"]] = True
                    deduped.append(r)
            rows = deduped

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=cols)
        df["score_breakdown"] = df["score_breakdown"].apply(
            lambda x: json.loads(x) if isinstance(x, str) and x else {}
        )
        df = df.sort_values("score", ascending=False).reset_index(drop=True)
        return df

    except Exception as e:
        print(f"  [morning_report] DB fetch failed: {e}")
        return pd.DataFrame()


def generate_morning_report() -> Optional[str]:
    """Generate PDF, upload to filebin, notify via Pushover. Returns filebin URL or None."""
    print("\n[MORNING REPORT] Generating daily signal PDF...")
    os.makedirs(REPORTS_DIR, exist_ok=True)

    df = _fetch_todays_signals()
    today_str = datetime.now(ET).strftime("%Y-%m-%d")
    filename  = f"{REPORTS_DIR}/Axiom_Morning_{today_str}.pdf"

    doc   = SimpleDocTemplate(filename, pagesize=letter,
                               leftMargin=L_MARGIN, rightMargin=R_MARGIN,
                               topMargin=0.65 * inch, bottomMargin=0.65 * inch)
    story = []

    # ── Header ──────────────────────────────────────────────────────────────────
    hdr = Table([[
        Paragraph("Axiom Terminal — Morning Report", ST["title"]),
        Paragraph(datetime.now(ET).strftime("%A, %B %d %Y"), ST["date"]),
    ]], colWidths=[4.5 * inch, 2.5 * inch])
    hdr.setStyle(TableStyle([
        ("VALIGN",      (0, 0), (-1, -1), "BOTTOM"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",(0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",(0, 0),(-1, -1), 0),
    ]))
    story.append(hdr)
    story.append(_hr())
    story.append(Spacer(1, 6))

    if df.empty:
        story.append(Paragraph("No signals above score 45 in the last 24 hours.", ST["body"]))
        story.append(Spacer(1, 20))
        story.append(_hr())
        story.append(Paragraph("Research purposes only. Not financial advice.", ST["small"]))
        doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
        print(f"  [morning_report] No signals — empty report saved: {filename}")
        return _upload_and_notify(filename, len(df))

    n_buy   = len(df[df["signal_label"].isin(["Strong Buy Candidate", "Speculative Buy", "Gap-Up"])])
    n_watch = len(df[df["signal_label"] == "Watchlist"])
    story.append(Paragraph("SUMMARY", ST["section"]))

    ts = [
        ["Signals scanned (last 24h)", str(len(df))],
        ["BUY signals",               str(n_buy)],
        ["WATCH signals",              str(n_watch)],
        ["Generated at (ET)",         datetime.now(ET).strftime("%I:%M %p")],
    ]
    summary_tbl = Table(ts, colWidths=[4.5 * inch, 2.5 * inch])
    summary_tbl.setStyle(TableStyle([
        ("FONTNAME",     (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE",     (0, 0), (-1, -1), 8.5),
        ("TEXTCOLOR",    (0, 0), (-1, -1), C_DARK),
        ("LINEBELOW",    (0, 0), (-1, -1), 0.3, C_RULE),
        ("TOPPADDING",   (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 4),
        ("LEFTPADDING",  (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("ALIGN",        (1, 0), (1, -1),  "RIGHT"),
    ]))
    story.append(summary_tbl)
    story.append(Spacer(1, 12))

    # ── Signal table ─────────────────────────────────────────────────────────────
    story.append(Paragraph("SIGNALS — RANKED BY SCORE", ST["section"]))

    headers = ["#", "Ticker", "Score", "Signal", "Action", "Entry $", "Reason"]
    col_w   = [0.3*inch, 0.7*inch, 0.55*inch, 1.4*inch, 0.65*inch, 0.65*inch, 2.65*inch]

    rows_data = [headers]
    action_colors = []   # (row_index, color) for ACTION column coloring

    for i, row in df.iterrows():
        rank        = i + 1
        ticker      = row["ticker"]
        score       = float(row["score"] or 0)
        label       = row["signal_label"] or ""
        bd          = row["score_breakdown"] if isinstance(row["score_breakdown"], dict) else {}
        price       = row["price_at_signal"]
        action, col = _action(label, score)
        reason      = _reason(bd, label, score)
        price_str   = f"${price:.2f}" if price else "—"

        rows_data.append([
            str(rank), ticker, f"{score:.0f}", label, action, price_str,
            Paragraph(reason, ParagraphStyle("r", fontName="Helvetica", fontSize=7.5,
                                              textColor=C_DARK, leading=10)),
        ])
        action_colors.append((len(rows_data) - 1, col))

    tbl = Table(rows_data, colWidths=col_w, repeatRows=1)
    ts2 = TableStyle([
        ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, -1), 8.5),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  C_GRAY),
        ("TEXTCOLOR",     (0, 1), (-1, -1), C_DARK),
        ("LINEBELOW",     (0, 0), (-1, 0),  0.5, C_XGRAY),
        ("LINEBELOW",     (0, 1), (-1, -1), 0.3, C_RULE),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 4),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
        ("ALIGN",         (0, 0), (0, -1),  "CENTER"),
        ("ALIGN",         (2, 0), (2, -1),  "RIGHT"),
        ("ALIGN",         (4, 0), (4, -1),  "CENTER"),
        ("ALIGN",         (5, 0), (5, -1),  "RIGHT"),
        ("FONTNAME",      (4, 1), (4, -1),  "Helvetica-Bold"),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
    ])
    # Color the action column per row
    for row_idx, col in action_colors:
        ts2.add("TEXTCOLOR", (4, row_idx), (4, row_idx), col)
    tbl.setStyle(ts2)
    story.append(tbl)

    # ── Component breakdown for top BUY signals ───────────────────────────────
    top_buys = df[df["signal_label"].isin(["Strong Buy Candidate", "Speculative Buy"])].head(5)
    if not top_buys.empty:
        story.append(Spacer(1, 12))
        story.append(Paragraph("COMPONENT BREAKDOWN — TOP BUY SIGNALS", ST["section"]))
        comp_headers = ["Ticker", "Score", "Technical", "Fundamental", "Risk", "Sentiment", "Catalyst"]
        comp_widths  = [0.8*inch, 0.6*inch, 0.85*inch, 1.0*inch, 0.7*inch, 0.85*inch, 0.8*inch]
        comp_rows    = [comp_headers]
        for _, row in top_buys.iterrows():
            bd = row["score_breakdown"] if isinstance(row["score_breakdown"], dict) else {}
            comp_rows.append([
                row["ticker"],
                f"{float(row['score']):.0f}",
                f"{bd.get('technical',   0):.0f}",
                f"{bd.get('fundamental', 0):.0f}",
                f"{bd.get('risk',        0):.0f}",
                f"{bd.get('sentiment',   0):.0f}",
                f"{bd.get('catalyst',    0):.0f}",
            ])
        comp_tbl = Table(comp_rows, colWidths=comp_widths, repeatRows=1)
        comp_tbl.setStyle(TableStyle([
            ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 8.5),
            ("TEXTCOLOR",     (0, 0), (-1, 0),  C_GRAY),
            ("TEXTCOLOR",     (0, 1), (-1, -1), C_DARK),
            ("LINEBELOW",     (0, 0), (-1, 0),  0.5, C_XGRAY),
            ("LINEBELOW",     (0, 1), (-1, -1), 0.3, C_RULE),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING",   (0, 0), (-1, -1), 4),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
            ("ALIGN",         (1, 0), (-1, -1), "RIGHT"),
        ]))
        story.append(comp_tbl)

    story.append(Spacer(1, 20))
    story.append(_hr())
    story.append(Paragraph("Research purposes only. Not financial advice.", ST["small"]))

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    print(f"  [morning_report] Saved: {filename} ({len(df)} signals, {n_buy} BUY)")
    return _upload_and_notify(filename, len(df))


def _upload_and_notify(filename: str, n_signals: int) -> Optional[str]:
    user_key  = os.environ.get("PUSHOVER_USER_KEY", "")
    api_token = os.environ.get("PUSHOVER_API_TOKEN", "")

    # Upload to filebin
    url = None
    try:
        bin_id    = f"axiom-am-{uuid.uuid4().hex[:8]}"
        file_name = os.path.basename(filename)
        with open(filename, "rb") as fh:
            resp = requests.post(
                f"https://filebin.net/{bin_id}/{file_name}",
                data=fh, headers={"Content-Type": "application/pdf"}, timeout=30,
            )
        if resp.status_code in (200, 201):
            url = f"https://filebin.net/{bin_id}/{file_name}"
            print(f"  [morning_report] Uploaded: {url}")
        else:
            print(f"  [morning_report] Upload failed ({resp.status_code})")
    except Exception as e:
        print(f"  [morning_report] Upload error: {e}")

    # Pushover notification
    if user_key and api_token:
        today = datetime.now(ET).strftime("%b %d")
        msg   = f"{n_signals} signals today. Tap to open full report."
        try:
            resp = requests.post(
                "https://api.pushover.net/1/messages.json",
                data={"token": api_token, "user": user_key,
                      "title": f"Axiom Morning Report — {today}",
                      "message": msg, "url": url or "",
                      "url_title": "Download PDF",
                      "priority": 0, "sound": "cashregister"},
                timeout=10,
            )
            if resp.status_code == 200:
                print("  [morning_report] Pushover sent")
            else:
                print(f"  [morning_report] Pushover failed ({resp.status_code}): {resp.text[:100]}")
        except Exception as e:
            print(f"  [morning_report] Pushover error: {e}")
    else:
        print("  [morning_report] Pushover env vars not set — skipping notification")

    return url
