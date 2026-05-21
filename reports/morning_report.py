"""
reports/morning_report.py
Daily morning PDF — top signals from overnight/premarket scans.
Shows score, action (BUY / WATCH), factor breakdown, and trade levels.

Auto-called by scanner_loop at 8:30 AM ET.
Manual trigger:
    python3 -c "from reports.morning_report import generate_morning_report; generate_morning_report()"
"""
import os
import json
from datetime import datetime, timezone, timedelta

# Load .env before any DB/alerts import so DATABASE_URL and Pushover keys are present
def _load_env() -> None:
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if not _line or _line.startswith("#") or "=" not in _line:
                continue
            _k, _, _v = _line.partition("=")
            _k = _k.strip()
            _v = _v.strip().strip('"').strip("'")
            if _k and _k not in os.environ:
                os.environ[_k] = _v

_load_env()
from typing import Optional

import pandas as pd

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable,
)

REPORTS_DIR = "reports"

ET = timezone(timedelta(hours=-4))   # EDT; change to -5 in winter

# ─── Palette ──────────────────────────────────────────────────────────────────
C_BLACK = colors.HexColor("#1a1a1a")
C_DARK  = colors.HexColor("#2d2d2d")
C_GRAY  = colors.HexColor("#666666")
C_LGRAY = colors.HexColor("#aaaaaa")
C_XGRAY = colors.HexColor("#dddddd")
C_WHITE = colors.white
C_GREEN = colors.HexColor("#1a7a3c")
C_AMBER = colors.HexColor("#b86400")
C_RED   = colors.HexColor("#cc2222")
C_GRBG  = colors.HexColor("#f4f9f6")
C_AMBG  = colors.HexColor("#fdf6ec")

PAGE_W, PAGE_H = letter
L_MARGIN = 0.75 * inch
R_MARGIN = 0.75 * inch
USABLE_W = PAGE_W - L_MARGIN - R_MARGIN


def _S(name, **kw) -> ParagraphStyle:
    return ParagraphStyle(name, **kw)


ST = {
    "title":   _S("t",  fontName="Helvetica-Bold",   fontSize=14, textColor=C_BLACK, spaceAfter=0, leading=18),
    "date":    _S("dt", fontName="Helvetica",         fontSize=9,  textColor=C_GRAY,  spaceAfter=0, alignment=TA_RIGHT),
    "section": _S("sh", fontName="Helvetica-Bold",    fontSize=9,  textColor=C_GRAY,  spaceBefore=14, spaceAfter=4, leading=12),
    "body":    _S("b",  fontName="Helvetica",         fontSize=9,  textColor=C_DARK,  spaceAfter=4, leading=14),
    "small":   _S("sm", fontName="Helvetica",         fontSize=8,  textColor=C_GRAY,  spaceAfter=3, leading=12),
    "ticker":  _S("tk", fontName="Helvetica-Bold",    fontSize=11, textColor=C_BLACK, spaceAfter=0, leading=14),
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


def _action(signal_label: str, score: float) -> tuple:
    if score >= 75 or signal_label == "Strong Buy Candidate":
        return "BUY", C_GREEN
    if score >= 60 or signal_label in ("Speculative Buy", "Gap-Up"):
        return "BUY", C_GREEN
    if score >= 45 or signal_label == "Watchlist":
        return "WATCH", C_AMBER
    return "SKIP", C_RED


def _reason(score_breakdown: dict, signal_label: str, score: float) -> str:
    if not isinstance(score_breakdown, dict):
        return f"{signal_label} — score {score:.0f}."
    tech = score_breakdown.get("technical",   0)
    fund = score_breakdown.get("fundamental", 0)
    risk = score_breakdown.get("risk",        0)
    sent = score_breakdown.get("sentiment",   0)
    parts = []
    if fund >= 80:    parts.append("strong fundamentals")
    elif fund >= 65:  parts.append("solid fundamentals")
    if tech >= 75:    parts.append("high technical momentum")
    elif tech >= 60:  parts.append("positive technical setup")
    if risk >= 70:    parts.append("low risk profile")
    if sent >= 75:    parts.append("positive news sentiment")
    if risk < 30:     parts.append("elevated risk")
    if fund < 35:     parts.append("weak fundamentals")
    if tech < 30:     parts.append("poor technical setup")
    if not parts:
        top = max(score_breakdown, key=lambda k: score_breakdown.get(k, 0))
        parts.append(f"led by {top} ({score_breakdown[top]:.0f})")
    return "; ".join(parts).capitalize() + "."


def _score_bar(score: float, width: int = 90, height: int = 5) -> Table:
    pct    = min(max(score / 100.0, 0), 1)
    filled = max(int(width * pct), 1)
    empty  = max(width - filled, 0)
    bar_color = C_GREEN if score >= 75 else (C_AMBER if score >= 60 else C_LGRAY)
    cols = [filled / 72 * inch]
    row  = [""]
    if empty > 0:
        cols.append(empty / 72 * inch)
        row.append("")
    t = Table([row], colWidths=cols, rowHeights=[height / 72 * inch])
    style = [
        ("BACKGROUND",    (0, 0), (0, 0),   bar_color),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ("LEFTPADDING",   (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
    ]
    if empty > 0:
        style.append(("BACKGROUND", (1, 0), (1, 0), C_XGRAY))
    t.setStyle(TableStyle(style))
    return t


def _fetch_todays_signals() -> pd.DataFrame:
    """Pull last 24h signals from signal_log, deduplicated by ticker (highest score)."""
    from db.database import _is_postgres, _get_pg_conn, _put_pg_conn, _get_sqlite_conn

    try:
        if _is_postgres():
            conn = _get_pg_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT DISTINCT ON (ticker)
                       id, ticker, signal_label, score, score_breakdown,
                       price_at_signal, entry_price, stop_loss, target_1, target_2,
                       risk_reward, created_at
                FROM signal_log
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                  AND score >= 45
                ORDER BY ticker, score DESC, created_at DESC
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            cur.close()
            _put_pg_conn(conn)
        else:
            conn = _get_sqlite_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT id, ticker, signal_label, score, score_breakdown,
                       price_at_signal, entry_price, stop_loss, target_1, target_2,
                       risk_reward, created_at
                FROM signal_log
                WHERE created_at >= datetime('now', '-24 hours')
                  AND score >= 45
                ORDER BY score DESC, created_at DESC
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            conn.close()
            seen, deduped = set(), []
            for r in rows:
                d = dict(zip(cols, r))
                if d["ticker"] not in seen:
                    seen.add(d["ticker"])
                    deduped.append(r)
            rows = deduped

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=cols)
        df["score_breakdown"] = df["score_breakdown"].apply(
            lambda x: json.loads(x) if isinstance(x, str) and x else {}
        )
        return df.sort_values("score", ascending=False).reset_index(drop=True)

    except Exception as e:
        print(f"  [morning_report] DB fetch failed: {e}")
        return pd.DataFrame()


def _build_pdf(signals: list[dict], now: datetime) -> str:
    """Render PDF from list of signal dicts. Returns file path."""
    os.makedirs(REPORTS_DIR, exist_ok=True)
    fname = os.path.join(REPORTS_DIR, f"morning_report_{now.strftime('%Y-%m-%d')}.pdf")

    doc = SimpleDocTemplate(
        fname, pagesize=letter,
        leftMargin=L_MARGIN, rightMargin=R_MARGIN,
        topMargin=0.75 * inch, bottomMargin=0.65 * inch,
    )
    story = []

    # ── Header ────────────────────────────────────────────────────────────────
    hdr = Table(
        [[Paragraph("AXIOM TERMINAL", ST["title"]),
          Paragraph(f"Morning Signal Report<br/>{now.strftime('%A, %B %d %Y · %I:%M %p ET')}", ST["date"])]],
        colWidths=[USABLE_W * 0.55, USABLE_W * 0.45],
    )
    hdr.setStyle(TableStyle([
        ("VALIGN",        (0, 0), (-1, -1), "BOTTOM"),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING",   (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
    ]))
    story.append(hdr)
    story.append(HRFlowable(width="100%", thickness=1.5, color=C_BLACK, spaceAfter=10, spaceBefore=4))

    # ── Summary bar ───────────────────────────────────────────────────────────
    buys  = sum(1 for r in signals if r["score"] >= 60)
    watch = sum(1 for r in signals if 45 <= r["score"] < 60)
    avg   = (sum(r["score"] for r in signals) / len(signals)) if signals else 0

    sum_tbl = Table(
        [[Paragraph(f"<b>{len(signals)}</b> signals", ST["body"]),
          Paragraph(f"<b>{buys}</b> BUY  ·  <b>{watch}</b> WATCH", ST["body"]),
          Paragraph(f"Avg score <b>{avg:.1f}</b>", ST["body"]),
          Paragraph(f"Generated {now.strftime('%H:%M ET')}", ST["small"])]],
        colWidths=[USABLE_W * 0.25] * 4,
    )
    sum_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), colors.HexColor("#f7f7f7")),
        ("BOX",           (0, 0), (-1, -1), 0.5, C_XGRAY),
        ("TOPPADDING",    (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING",   (0, 0), (-1, -1), 8),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(sum_tbl)
    story.append(Spacer(1, 10))

    if not signals:
        story.append(Paragraph("No signals qualified in the last 24 hours.", ST["body"]))
        doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
        return fname

    # ── Signal cards ──────────────────────────────────────────────────────────
    story.append(Paragraph("TODAY'S SIGNALS", ST["section"]))
    story.append(_hr())

    for row in signals:
        ticker = row["ticker"]
        label  = row.get("signal_label", "")
        score  = float(row.get("score", 0))
        sb     = row.get("score_breakdown") or {}
        price  = float(row.get("price_at_signal") or 0)
        entry  = float(row.get("entry_price") or price)
        stop   = float(row.get("stop_loss") or 0)
        t1     = float(row.get("target_1") or 0)
        t2     = row.get("target_2")
        rr     = float(row.get("risk_reward") or 0)
        reason = _reason(sb, label, score)
        action, action_color = _action(label, score)
        bg = C_GRBG if action == "BUY" else C_AMBG

        # Action badge
        badge_para = Paragraph(
            f"<b>{action}</b>",
            ParagraphStyle("ab", fontName="Helvetica-Bold", fontSize=10,
                           textColor=C_WHITE, alignment=TA_CENTER),
        )
        badge = Table([[badge_para]], colWidths=[0.55 * inch], rowHeights=[0.22 * inch])
        badge.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), action_color),
            ("TOPPADDING",    (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("LEFTPADDING",   (0, 0), (-1, -1), 4),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
        ]))

        # Ticker + label header
        left = Table(
            [[Paragraph(f"<b>{ticker}</b>", ST["ticker"]),
              Paragraph(f"${price:.2f}  ·  {label}  ·  Score <b>{score:.1f}</b>", ST["small"])]],
            colWidths=[USABLE_W * 0.45],
        )
        left.setStyle(TableStyle([
            ("TOPPADDING",    (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("LEFTPADDING",   (0, 0), (-1, -1), 0),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
        ]))
        header_row = Table(
            [[left, badge]],
            colWidths=[USABLE_W - 0.65 * inch, 0.65 * inch],
        )
        header_row.setStyle(TableStyle([
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING",    (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ("LEFTPADDING",   (0, 0), (-1, -1), 0),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
        ]))

        # Factor breakdown
        factor_tbl = Table(
            [["TECH", "FUND", "RISK", "SENT"],
             [f"{sb.get('technical',0):.0f}", f"{sb.get('fundamental',0):.0f}",
              f"{sb.get('risk',0):.0f}", f"{sb.get('sentiment',0):.0f}"]],
            colWidths=[0.55 * inch] * 4,
        )
        factor_tbl.setStyle(TableStyle([
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTNAME",      (0, 1), (-1, 1), "Helvetica"),
            ("FONTSIZE",      (0, 0), (-1, -1), 7),
            ("TEXTCOLOR",     (0, 0), (-1, 0), C_GRAY),
            ("TEXTCOLOR",     (0, 1), (-1, 1), C_DARK),
            ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
            ("TOPPADDING",    (0, 0), (-1, -1), 1),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
        ]))

        t2_str = f"  T2 ${float(t2):.2f}" if t2 else ""
        levels = f"Entry ${entry:.2f}  ·  Stop ${stop:.2f}  ·  T1 ${t1:.2f}{t2_str}  ·  R/R {rr:.1f}x"

        card = Table(
            [[header_row], [_score_bar(score)], [factor_tbl],
             [Paragraph(reason, ST["body"])], [Paragraph(levels, ST["small"])]],
            colWidths=[USABLE_W],
        )
        card.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), bg),
            ("BOX",           (0, 0), (-1, -1), 0.5,
             colors.HexColor("#c8e6d4") if action == "BUY" else C_XGRAY),
            ("TOPPADDING",    (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING",   (0, 0), (-1, -1), 10),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
            ("TOPPADDING",    (0, 1), (-1, 1), 3),
            ("BOTTOMPADDING", (0, 1), (-1, 1), 3),
        ]))
        story.append(card)
        story.append(Spacer(1, 6))

    # ── Footer note ───────────────────────────────────────────────────────────
    story.append(Spacer(1, 6))
    story.append(Paragraph("SCORING MODEL", ST["section"]))
    story.append(_hr())
    story.append(Paragraph(
        "Formula: TECH×0.43 + FUND×0.30 + RISK_INV×0.20 + SENT×0.07  "
        "(risk inverted: 100 − raw_risk).  "
        "BUY ≥60 · WATCH 45–59 · SKIP <45.  "
        "Gates (time / volume / regime) suppress push alerts only.",
        ST["small"],
    ))

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return fname


def generate_morning_report() -> Optional[str]:
    """Generate PDF from today's signals and send via Pushover with PDF attached.

    Returns the local file path or None.
    """
    from alerts import send_alert_with_pdf, PRIORITY_HIGH

    now = datetime.now(ET)
    print(f"\n[MORNING REPORT] Generating — {now.strftime('%Y-%m-%d %H:%M ET')}")

    df = _fetch_todays_signals()

    if df.empty:
        print("  [morning_report] No signals in last 24h — PDF will show empty state.")
        signals = []
    else:
        signals = df.to_dict("records")
        print(f"  [morning_report] {len(signals)} signals fetched")

    fname = _build_pdf(signals, now)
    print(f"  [morning_report] PDF saved: {fname}")

    buys  = sum(1 for r in signals if r.get("score", 0) >= 60)
    watch = len(signals) - buys
    msg   = f"{len(signals)} signals · {buys} BUY · {watch} WATCH · {now.strftime('%b %d')}"

    with open(fname, "rb") as f:
        pdf_bytes = f.read()

    sent = send_alert_with_pdf(
        title    = "📊 Axiom Morning Report",
        message  = msg,
        pdf_bytes = pdf_bytes,
        filename = os.path.basename(fname),
        priority = PRIORITY_HIGH,
    )

    if not sent:
        print("  [morning_report] Pushover keys not set — PDF saved locally only")

    return fname
