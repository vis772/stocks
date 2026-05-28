# prediction_engine/stage6_report.py
# Stage 6 (8:30-9:20 AM ET): Generate the pre-market conviction PDF report.
# Axiom Terminal financial terminal aesthetic — dark background, Helvetica, declarative.

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable,
)

from .config import REPORT_DIR

logger = logging.getLogger("pe.stage6")

# ─── Page geometry ─────────────────────────────────────────────────────────────
PAGE_W, PAGE_H = letter
L_MAR = 0.70 * inch
R_MAR = 0.70 * inch
T_MAR = 0.75 * inch
B_MAR = 0.65 * inch
USABLE_W = PAGE_W - L_MAR - R_MAR

# ─── Dark terminal palette ─────────────────────────────────────────────────────
C_BG        = colors.HexColor("#0d1117")   # Page background
C_CARD      = colors.HexColor("#161b22")   # Card / row background
C_HEADER    = colors.HexColor("#21262d")   # Section header background
C_TEXT      = colors.HexColor("#e6edf3")   # Primary text
C_DIM       = colors.HexColor("#7d8590")   # Dimmed / secondary text
C_ACCENT    = colors.HexColor("#58a6ff")   # Blue accent (ticker, score)
C_GREEN     = colors.HexColor("#3fb950")   # Positive / target
C_RED       = colors.HexColor("#f85149")   # Negative / stop
C_ORANGE    = colors.HexColor("#d29922")   # Warning / moderate
C_DIVIDER   = colors.HexColor("#21262d")   # Rule line
C_EXTREME   = colors.HexColor("#bc8cff")   # EXTREME conviction
C_VERYHIGH  = colors.HexColor("#58a6ff")   # VERY HIGH conviction
C_HIGH      = colors.HexColor("#3fb950")   # HIGH conviction
C_WHITE     = colors.HexColor("#f0f6fc")   # Near-white headlines

# ─── Paragraph styles ──────────────────────────────────────────────────────────
def _S(name, **kw) -> ParagraphStyle:
    return ParagraphStyle(name, **kw)

ST = {
    "doc_title":   _S("dt",  fontName="Helvetica-Bold",    fontSize=16, textColor=C_WHITE,  spaceAfter=2, leading=20),
    "doc_sub":     _S("ds",  fontName="Helvetica",          fontSize=9,  textColor=C_DIM,    spaceAfter=0, leading=12),
    "section":     _S("sh",  fontName="Helvetica-Bold",     fontSize=8,  textColor=C_DIM,    spaceBefore=12, spaceAfter=4, leading=11),
    "ticker":      _S("tk",  fontName="Helvetica-Bold",     fontSize=18, textColor=C_ACCENT, spaceAfter=2, leading=22),
    "tier":        _S("tr",  fontName="Helvetica-Bold",     fontSize=9,  textColor=C_GREEN,  spaceAfter=4, leading=12),
    "label":       _S("lb",  fontName="Helvetica-Bold",     fontSize=7.5,textColor=C_DIM,    spaceAfter=0, leading=10),
    "value":       _S("vl",  fontName="Helvetica",          fontSize=9,  textColor=C_TEXT,   spaceAfter=2, leading=12),
    "value_g":     _S("vg",  fontName="Helvetica-Bold",     fontSize=9,  textColor=C_GREEN,  spaceAfter=2, leading=12),
    "value_r":     _S("vr",  fontName="Helvetica-Bold",     fontSize=9,  textColor=C_RED,    spaceAfter=2, leading=12),
    "body":        _S("bd",  fontName="Helvetica",          fontSize=8.5,textColor=C_TEXT,   spaceAfter=4, leading=13),
    "thesis":      _S("th",  fontName="Helvetica-Oblique",  fontSize=8.5,textColor=C_TEXT,   spaceAfter=6, leading=13),
    "catalyst":    _S("ct",  fontName="Helvetica",          fontSize=8,  textColor=C_DIM,    spaceAfter=2, leading=12),
    "small":       _S("sm",  fontName="Helvetica",          fontSize=7.5,textColor=C_DIM,    spaceAfter=1, leading=11),
    "footer_txt":  _S("ft",  fontName="Helvetica",          fontSize=7,  textColor=C_DIM,    spaceAfter=0, leading=10),
    "score_big":   _S("sb",  fontName="Helvetica-Bold",     fontSize=28, textColor=C_ACCENT, spaceAfter=0, leading=34, alignment=TA_CENTER),
    "score_label": _S("sl",  fontName="Helvetica",          fontSize=7,  textColor=C_DIM,    spaceAfter=0, leading=10, alignment=TA_CENTER),
}


def _tier_color(tier: str) -> colors.Color:
    t = tier.upper()
    if "EXTREME"   in t: return C_EXTREME
    if "VERY HIGH" in t: return C_VERYHIGH
    if "HIGH"      in t: return C_HIGH
    return C_DIM


# ─── Canvas callbacks ──────────────────────────────────────────────────────────

def _draw_bg(canvas, doc):
    """Paint dark background and top header bar on every page."""
    canvas.saveState()
    # Full page background
    canvas.setFillColor(C_BG)
    canvas.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)
    # Header accent bar
    canvas.setFillColor(C_HEADER)
    canvas.rect(0, PAGE_H - 0.35 * inch, PAGE_W, 0.35 * inch, fill=1, stroke=0)
    # Footer bar
    canvas.setFillColor(C_HEADER)
    canvas.rect(0, 0, PAGE_W, 0.42 * inch, fill=1, stroke=0)
    # Footer text
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(C_DIM)
    canvas.drawString(L_MAR, 0.16 * inch,
                      "Axiom Terminal — Pre-Market Conviction Report — Confidential Research Tool")
    canvas.drawRightString(PAGE_W - R_MAR, 0.16 * inch, f"Page {doc.page}")
    canvas.restoreState()


def _hr_dark():
    return HRFlowable(width="100%", thickness=0.4, color=C_DIVIDER,
                      spaceAfter=6, spaceBefore=4)


# ─── Report entry point ────────────────────────────────────────────────────────

def run(picks: list) -> str:
    """
    Build the PDF report and return the output file path.
    Raises on failure (caller handles retry logic).
    """
    os.makedirs(REPORT_DIR, exist_ok=True)
    et_now    = _et_now()
    date_str  = et_now.strftime("%Y-%m-%d")
    ts_str    = et_now.strftime("%Y-%m-%d %I:%M %p ET")
    filename  = f"axiom_pe_report_{date_str}.pdf"
    filepath  = os.path.join(REPORT_DIR, filename)

    doc = SimpleDocTemplate(
        filepath,
        pagesize=letter,
        leftMargin=L_MAR, rightMargin=R_MAR,
        topMargin=T_MAR + 0.1 * inch,
        bottomMargin=B_MAR + 0.1 * inch,
        title=f"Axiom Terminal Pre-Market Report {date_str}",
        author="Axiom Terminal",
    )

    story = _build_story(picks, date_str, ts_str)
    doc.build(story, onFirstPage=_draw_bg, onLaterPages=_draw_bg)

    logger.info("[stage6] Report written: %s (%d picks)", filepath, len(picks))
    return filepath


def _build_story(picks: list, date_str: str, ts_str: str) -> list:
    story = []

    # ── Header ────────────────────────────────────────────────────────────────
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("AXIOM TERMINAL", ST["doc_title"]))
    story.append(Paragraph("Pre-Market Conviction Report", _S("dsub", fontName="Helvetica", fontSize=10,
                                                              textColor=C_DIM, spaceAfter=2, leading=13)))
    story.append(Paragraph(
        f"{date_str} &nbsp;&nbsp;|&nbsp;&nbsp; Generated {ts_str} &nbsp;&nbsp;|&nbsp;&nbsp; "
        f"{len(picks)} High-Conviction Picks",
        _S("dmeta", fontName="Helvetica", fontSize=8, textColor=C_DIM, spaceAfter=8, leading=11),
    ))
    story.append(_hr_dark())

    # ── Model consensus legend ─────────────────────────────────────────────────
    story.append(Paragraph("CONSENSUS ARCHITECTURE", ST["section"]))
    story.append(Paragraph(
        "Four independent SLMs score each stock 0-100. A stock advances only when "
        "3 or 4 models agree (score ≥ 55). Final conviction score is the mean of "
        "agreeing models only. This eliminates single-model false positives.",
        ST["body"],
    ))
    story.append(_hr_dark())
    story.append(Spacer(1, 0.06 * inch))

    # ── Individual picks ───────────────────────────────────────────────────────
    if not picks:
        story.append(Paragraph("No picks met the consensus threshold today.", ST["body"]))
    else:
        for pick in picks:
            story.extend(_build_pick_section(pick))

    # ── Summary table ─────────────────────────────────────────────────────────
    story.append(_hr_dark())
    story.append(Paragraph("PICK SUMMARY", ST["section"]))
    story.extend(_build_summary_table(picks))

    # ── Disclaimer ────────────────────────────────────────────────────────────
    story.append(Spacer(1, 0.12 * inch))
    story.append(_hr_dark())
    story.append(Paragraph(
        "EXPECTED VALUE DISCLAIMER: This report is generated by four small language models "
        "running locally on private infrastructure. It does not constitute financial advice. "
        "All analysis is speculative and based on pre-market data which may be incomplete, "
        "delayed, or erroneous. Past model performance does not guarantee future results. "
        "Position sizing, risk management, and trading decisions remain the sole "
        "responsibility of the user. No warranty of accuracy is expressed or implied.",
        _S("disc", fontName="Helvetica", fontSize=7, textColor=C_DIM, spaceAfter=4,
           leading=10),
    ))

    return story


def _build_pick_section(pick: dict) -> list:
    parts = []

    ticker  = pick["ticker"]
    score   = pick["conviction_score"]
    tier    = pick["conviction_tier"]
    rank    = pick["rank"]
    models_agreed = pick.get("model_agreement_count", 0)
    model_scores  = pick.get("model_scores", {})

    tier_color = _tier_color(tier)

    # ── Rank + Ticker header row ───────────────────────────────────────────────
    rank_label = f"PICK #{rank}"
    header_data = [[
        Paragraph(rank_label, _S(f"rl{rank}", fontName="Helvetica-Bold", fontSize=8,
                                 textColor=C_DIM, leading=10)),
        Paragraph(ticker, ST["ticker"]),
        Paragraph(tier, _S(f"tr{rank}", fontName="Helvetica-Bold", fontSize=9,
                           textColor=tier_color, leading=12)),
        Paragraph(f"{models_agreed}/4 MODELS", _S(f"ma{rank}", fontName="Helvetica-Bold",
                                                   fontSize=9, textColor=C_TEXT, leading=12)),
    ]]
    t = Table(header_data, colWidths=[0.6*inch, 1.2*inch, 1.4*inch, 1.2*inch])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0), C_CARD),
        ("TOPPADDING",    (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING",   (0, 0), (-1, -1), 8),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 8),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ]))
    parts.append(t)
    parts.append(Spacer(1, 0.04 * inch))

    # ── Score + pricing row ────────────────────────────────────────────────────
    entry  = pick["entry"]
    target = pick["target"]
    stop   = pick["stop"]
    gap    = pick.get("gap_pct", 0)
    vol_r  = pick.get("volume_ratio", 0)
    rr     = pick.get("risk_reward", 0)
    target_pct = pick.get("target_pct", 0)
    stop_pct   = pick.get("stop_pct", 0)

    metrics_data = [
        [
            _metric("CONVICTION SCORE", f"{score:.0f}/100", color=C_ACCENT),
            _metric("ENTRY",            f"${entry:.2f}"),
            _metric("TARGET",           f"${target:.2f} (+{target_pct:.1f}%)", color=C_GREEN),
            _metric("STOP",             f"${stop:.2f} (-{stop_pct:.1f}%)",     color=C_RED),
            _metric("RISK/REWARD",      f"{rr:.1f}R"),
        ],
        [
            _metric("GAP",              f"{gap:+.1f}%", color=C_GREEN if gap > 0 else C_RED),
            _metric("VOLUME RATIO",     f"{vol_r:.1f}x"),
            _metric("RSI(14)",          f"{pick.get('rsi', 0):.0f}"),
            _metric("SHORT FLOAT",      f"{pick.get('short_interest', 0) or 0:.1f}%"),
            _metric("SECTOR",           pick.get("sector", "—")[:18]),
        ],
    ]
    mw = USABLE_W / 5
    for row_data in metrics_data:
        mt = Table([row_data], colWidths=[mw] * 5)
        mt.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), C_CARD),
            ("TOPPADDING",    (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING",   (0, 0), (-1, -1), 8),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 8),
            ("GRID",          (0, 0), (-1, -1), 0.3, C_DIVIDER),
        ]))
        parts.append(mt)

    parts.append(Spacer(1, 0.05 * inch))

    # ── Model consensus breakdown ──────────────────────────────────────────────
    parts.append(Paragraph("MODEL CONSENSUS BREAKDOWN", ST["section"]))

    model_label_map = {
        "qwen2.5:1.5b":  "Qwen 1.5B (Momentum)",
        "phi4-mini":     "Phi-4 Mini (Technical)",
        "gemma3:1b":     "Gemma3 1B (Pattern)",
        "smollm2:1.7b":  "SmolLM2 1.7B (Sentiment)",
    }
    agreeing = set(pick.get("agreeing_models", []))
    all_model_data = pick.get("all_model_data", {})

    consensus_rows = []
    for model, label in model_label_map.items():
        mv    = all_model_data.get(model, {})
        mscore = model_scores.get(model)
        agreed = model in agreeing
        status = "AGREE" if agreed else "DISSENT"
        s_color = C_GREEN if agreed else C_RED

        score_str = f"{mscore:.0f}" if mscore is not None else "—"
        consensus_rows.append([
            Paragraph(label, _S(f"ml{model}", fontName="Helvetica", fontSize=8,
                                textColor=C_TEXT, leading=10)),
            Paragraph(score_str, _S(f"ms{model}", fontName="Helvetica-Bold", fontSize=9,
                                    textColor=C_ACCENT if agreed else C_DIM, leading=12)),
            Paragraph(status, _S(f"mv{model}", fontName="Helvetica-Bold", fontSize=8,
                                  textColor=s_color, leading=10)),
        ])

    cw = [USABLE_W * 0.55, USABLE_W * 0.15, USABLE_W * 0.30]
    ct = Table(
        [["MODEL", "SCORE", "VERDICT"]] + consensus_rows,
        colWidths=cw,
    )
    ct.setStyle(TableStyle([
        ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, 0),  7),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  C_DIM),
        ("FONTSIZE",      (0, 1), (-1, -1), 8),
        ("BACKGROUND",    (0, 0), (-1, 0),  C_HEADER),
        ("BACKGROUND",    (0, 1), (-1, -1), C_CARD),
        ("GRID",          (0, 0), (-1, -1), 0.3, C_DIVIDER),
        ("TOPPADDING",    (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
    ]))
    parts.append(ct)
    parts.append(Spacer(1, 0.05 * inch))

    # ── Analysis lines ─────────────────────────────────────────────────────────
    catalyst = pick.get("catalyst", "").strip()
    technical = pick.get("technical_setup", "").strip()
    pattern  = pick.get("pattern_summary", "").strip()

    if catalyst:
        parts.append(Paragraph("CATALYST", ST["section"]))
        parts.append(Paragraph(catalyst, ST["body"]))
    if technical:
        parts.append(Paragraph("TECHNICAL SETUP", ST["section"]))
        parts.append(Paragraph(technical, ST["body"]))
    if pattern:
        parts.append(Paragraph("PATTERN RECOGNITION", ST["section"]))
        parts.append(Paragraph(pattern, ST["body"]))

    # ── Conviction thesis ──────────────────────────────────────────────────────
    thesis = pick.get("thesis", "").strip()
    if thesis:
        parts.append(Paragraph("CONVICTION THESIS", ST["section"]))
        parts.append(Paragraph(thesis, ST["thesis"]))

    parts.append(Spacer(1, 0.08 * inch))
    parts.append(_hr_dark())
    parts.append(Spacer(1, 0.08 * inch))

    return parts


def _metric(label: str, value: str, color=None) -> list:
    """Return a two-paragraph [label, value] cell for the metrics table."""
    vc = color or C_TEXT
    return [
        Paragraph(label, _S(f"lbl_{label}", fontName="Helvetica-Bold", fontSize=6.5,
                            textColor=C_DIM, leading=9, spaceAfter=2)),
        Paragraph(value, _S(f"val_{label}", fontName="Helvetica-Bold", fontSize=9,
                            textColor=vc, leading=12)),
    ]


def _build_summary_table(picks: list) -> list:
    if not picks:
        return [Paragraph("No picks generated.", ST["body"])]

    headers = ["RANK", "TICKER", "SCORE", "TIER", "MODELS", "ENTRY", "TARGET", "STOP", "R/R", "SECTOR"]
    rows = []
    for p in picks:
        rows.append([
            str(p["rank"]),
            p["ticker"],
            f"{p['conviction_score']:.0f}",
            p["conviction_tier"],
            f"{p['model_agreement_count']}/4",
            f"${p['entry']:.2f}",
            f"${p['target']:.2f}",
            f"${p['stop']:.2f}",
            f"{p['risk_reward']:.1f}R",
            (p.get("sector") or "—")[:12],
        ])

    col_w = [
        0.4*inch, 0.65*inch, 0.55*inch, 0.95*inch, 0.55*inch,
        0.65*inch, 0.65*inch, 0.65*inch, 0.50*inch, 0.95*inch,
    ]
    t = Table([headers] + rows, colWidths=col_w)
    ts = TableStyle([
        ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, 0),  7),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  C_DIM),
        ("BACKGROUND",    (0, 0), (-1, 0),  C_HEADER),
        ("FONTNAME",      (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE",      (0, 1), (-1, -1), 8),
        ("TEXTCOLOR",     (0, 1), (-1, -1), C_TEXT),
        ("BACKGROUND",    (0, 1), (-1, -1), C_CARD),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [C_CARD, C_BG]),
        ("GRID",          (0, 0), (-1, -1), 0.3, C_DIVIDER),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING",   (0, 0), (-1, -1), 5),
        ("ALIGN",         (2, 0), (-1, -1), "RIGHT"),
    ])
    # Color ticker column
    for i, p in enumerate(picks, 1):
        ts.add("TEXTCOLOR", (1, i), (1, i), C_ACCENT)
        ts.add("FONTNAME",  (1, i), (1, i), "Helvetica-Bold")
    t.setStyle(ts)
    return [t]


# ─── ET time helper ───────────────────────────────────────────────────────────

def _et_now() -> datetime:
    utc    = datetime.now(timezone.utc)
    offset = -4 if 3 <= utc.month <= 11 else -5
    return utc + timedelta(hours=offset)
