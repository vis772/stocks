# reports/checkpoint_reports.py
# Three-checkpoint accuracy test report generator.
#
# Checkpoint 1  Day 15  — Sanity Check:        is the scanner broken or working?
# Checkpoint 2  Day 30  — Preliminary:         is there a directional signal?
# Checkpoint 3  Day 60  — Final Verdict:       trust it or not?
#
# Each PDF is uploaded to Filebin and sent via Pushover.
# Metadata is stored in the accuracy_reports DB table.

import os
import uuid
import requests
import traceback
from datetime import datetime
from typing import Optional, Tuple

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

# ─── Palette (light, professional) ───────────────────────────────────────────
C_BLACK = colors.HexColor("#1a1a1a")
C_DARK  = colors.HexColor("#2d2d2d")
C_GRAY  = colors.HexColor("#666666")
C_LGRAY = colors.HexColor("#aaaaaa")
C_XGRAY = colors.HexColor("#dddddd")
C_RULE  = colors.HexColor("#f0f0f0")
C_WHITE = colors.white
C_BLUE  = colors.HexColor("#1a56b0")   # OPERATIONAL
C_GREEN = colors.HexColor("#1a7a3c")   # APPROVED / PROMISING
C_RED   = colors.HexColor("#cc2222")   # REJECTED / NEEDS FIXING
C_AMBER = colors.HexColor("#b86400")   # INCONCLUSIVE / EXTEND TEST

PAGE_W, PAGE_H = letter
L_MARGIN = 0.75 * inch
R_MARGIN = 0.75 * inch
USABLE_W = PAGE_W - L_MARGIN - R_MARGIN  # ~7.0 inches


def _S(name, **kw) -> ParagraphStyle:
    return ParagraphStyle(name, **kw)


ST = {
    "title":   _S("t",  fontName="Helvetica-Bold",    fontSize=15, textColor=C_BLACK, spaceAfter=0, leading=19),
    "date":    _S("dt", fontName="Helvetica",          fontSize=9,  textColor=C_GRAY,  spaceAfter=0, alignment=TA_RIGHT),
    "section": _S("sh", fontName="Helvetica-Bold",     fontSize=9,  textColor=C_GRAY,  spaceBefore=16, spaceAfter=4, leading=12),
    "body":    _S("b",  fontName="Helvetica",          fontSize=9,  textColor=C_DARK,  spaceAfter=4, leading=14),
    "bullet":  _S("bu", fontName="Helvetica",          fontSize=9,  textColor=C_DARK,  spaceAfter=3, leading=13, leftIndent=12),
    "small":   _S("sm", fontName="Helvetica",          fontSize=8,  textColor=C_GRAY,  spaceAfter=3, leading=12),
    "warn":    _S("w",  fontName="Helvetica",          fontSize=9,  textColor=C_RED,   spaceAfter=6, leading=13),
    "concl":   _S("cl", fontName="Helvetica-Oblique",  fontSize=9,  textColor=C_DARK,  spaceAfter=4, leading=14, leftIndent=8),
}


def _footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(C_LGRAY)
    canvas.drawString(L_MARGIN, 0.38 * inch, "APEX SmallCap Scanner — Confidential Research Tool")
    canvas.drawRightString(PAGE_W - R_MARGIN, 0.38 * inch, f"Page {doc.page}")
    canvas.restoreState()


def _hr():
    return HRFlowable(width="100%", thickness=0.5, color=C_XGRAY, spaceAfter=6, spaceBefore=2)


def _header_table(title: str) -> Table:
    t = Table([[
        Paragraph(title, ST["title"]),
        Paragraph(datetime.now().strftime("%A, %B %d %Y"), ST["date"]),
    ]], colWidths=[4.5 * inch, 2.5 * inch])
    t.setStyle(TableStyle([
        ("VALIGN",       (0, 0), (-1, -1), "BOTTOM"),
        ("LEFTPADDING",  (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 0),
    ]))
    return t


def _verdict_box(label: str, color) -> Table:
    p = Paragraph(label, _S("vl", fontName="Helvetica-Bold", fontSize=20,
                              textColor=color, alignment=TA_CENTER))
    t = Table([[p]], colWidths=[USABLE_W])
    t.setStyle(TableStyle([
        ("BOX",           (0, 0), (-1, -1), 1.5, color),
        ("TOPPADDING",    (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("BACKGROUND",    (0, 0), (-1, -1), C_WHITE),
    ]))
    return t


def _clean_table(headers: list, rows: list, col_widths: list,
                 right_cols: list = None) -> Table:
    right_cols = right_cols or []
    data = [headers] + rows
    t = Table(data, colWidths=col_widths, repeatRows=1)
    ts = TableStyle([
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
        ("ALIGN",         (0, 0), (0, -1),  "LEFT"),
    ])
    for c in right_cols:
        ts.add("ALIGN", (c, 0), (c, -1), "RIGHT")
    t.setStyle(ts)
    return t


# ─── Stats computation ────────────────────────────────────────────────────────

def _compute_stats(df: pd.DataFrame) -> dict:
    """Derive all accuracy metrics from a signal_log DataFrame."""
    s: dict = {"total": 0}
    if df.empty:
        return s

    s["total"] = len(df)

    df = df.copy()
    df["_date"] = pd.to_datetime(df["created_at"]).dt.date
    daily = df.groupby("_date").size()
    s["trading_days"] = len(daily)
    s["avg_per_day"]  = round(s["total"] / max(len(daily), 1), 1)
    s["date_min"]     = daily.index.min()
    s["date_max"]     = daily.index.max()

    s["pct_1hr_filled"]  = round(df["pct_change_1hr"].notna().mean()  * 100, 1)
    s["pct_1day_filled"] = round(df["pct_change_1day"].notna().mean() * 100, 1)
    s["pct_5day_filled"] = round(df["pct_change_5day"].notna().mean() * 100, 1)
    p15 = df.get("pct_change_15day")
    s["pct_15day_filled"] = round(p15.notna().mean() * 100, 1) if p15 is not None else 0.0

    resolved = df[df["outcome_label"].isin(["win", "loss", "neutral"])].copy()
    s["resolved"] = len(resolved)

    if resolved.empty:
        s.update({"overall_win_rate": None, "by_signal": {}, "comp_corr": {},
                   "avg_win": None, "avg_loss": None,
                   "degenerate_labels": [], "missing_days": []})
        return s

    n_wins = (resolved["outcome_label"] == "win").sum()
    s["overall_win_rate"] = round(n_wins / len(resolved) * 100, 1)

    wins_5   = resolved.loc[resolved["outcome_label"] == "win",  "pct_change_5day"].dropna()
    losses_5 = resolved.loc[resolved["outcome_label"] == "loss", "pct_change_5day"].dropna()
    s["avg_win"]  = round(float(wins_5.mean()),   2) if not wins_5.empty  else None
    s["avg_loss"] = round(float(losses_5.mean()), 2) if not losses_5.empty else None

    by_signal: dict = {}
    for label in df["signal_label"].unique():
        sub = resolved[resolved["signal_label"] == label]
        if len(sub) < 3:
            continue
        wr  = round((sub["outcome_label"] == "win").sum() / len(sub) * 100, 1)
        wg  = sub.loc[sub["outcome_label"] == "win",  "pct_change_5day"].dropna()
        lg  = sub.loc[sub["outcome_label"] == "loss", "pct_change_5day"].dropna()

        sub_1d = df[df["signal_label"] == label].dropna(subset=["pct_change_1day"])
        wr_1d  = round((sub_1d["pct_change_1day"] > 0).sum() / len(sub_1d) * 100, 1) if not sub_1d.empty else None

        wr_15 = None
        if "pct_change_15day" in df.columns:
            sub_15 = df[df["signal_label"] == label].dropna(subset=["pct_change_15day"])
            wr_15  = round((sub_15["pct_change_15day"] > 0).sum() / len(sub_15) * 100, 1) if not sub_15.empty else None

        by_signal[label] = {
            "count":    len(sub),
            "win_rate": wr,
            "wr_1day":  wr_1d,
            "wr_15day": wr_15,
            "avg_gain": round(float(wg.mean()), 2) if not wg.empty else None,
            "avg_loss": round(float(lg.mean()), 2) if not lg.empty else None,
        }
    s["by_signal"] = by_signal

    comp_names = ["technical", "catalyst", "fundamental", "risk", "sentiment"]
    comp_corr: dict = {}
    if "score_breakdown" in df.columns:
        w_rows = resolved[resolved["outcome_label"] == "win"]["score_breakdown"]
        l_rows = resolved[resolved["outcome_label"] == "loss"]["score_breakdown"]
        for c in comp_names:
            w_vals = w_rows.apply(lambda bd: bd.get(c) if isinstance(bd, dict) else None).dropna()
            l_vals = l_rows.apply(lambda bd: bd.get(c) if isinstance(bd, dict) else None).dropna()
            wm   = round(float(w_vals.mean()), 1) if not w_vals.empty  else None
            lm   = round(float(l_vals.mean()), 1) if not l_vals.empty else None
            diff = round(wm - lm, 1) if wm is not None and lm is not None else None
            comp_corr[c] = {"win_mean": wm, "loss_mean": lm, "diff": diff}
    s["comp_corr"] = comp_corr

    s["degenerate_labels"] = [
        l for l, d in by_signal.items()
        if d["count"] >= 5 and (d["win_rate"] == 0.0 or d["win_rate"] == 100.0)
    ]

    try:
        all_bdays = pd.bdate_range(s["date_min"], s["date_max"])
        date_set  = set(daily.index)
        s["missing_days"] = [d.date() for d in all_bdays if d.date() not in date_set]
    except Exception:
        s["missing_days"] = []

    with_ret = resolved.dropna(subset=["pct_change_5day"])
    if not with_ret.empty:
        bi = with_ret["pct_change_5day"].idxmax()
        wi = with_ret["pct_change_5day"].idxmin()
        s["best_ticker"]  = (with_ret.loc[bi, "ticker"], with_ret.loc[bi, "pct_change_5day"])
        s["worst_ticker"] = (with_ret.loc[wi, "ticker"], with_ret.loc[wi, "pct_change_5day"])

    return s


# ─── Upload + Notify ─────────────────────────────────────────────────────────

def _upload_to_filebin(filename: str) -> Optional[str]:
    try:
        bin_id    = f"apex-rpt-{uuid.uuid4().hex[:8]}"
        file_name = os.path.basename(filename)
        with open(filename, "rb") as fh:
            resp = requests.post(
                f"https://filebin.net/{bin_id}/{file_name}",
                data=fh, headers={"Content-Type": "application/pdf"}, timeout=30,
            )
        if resp.status_code in (200, 201):
            url = f"https://filebin.net/{bin_id}/{file_name}"
            print(f"  [report] Uploaded: {url}")
            return url
        print(f"  [report] Filebin upload failed ({resp.status_code})")
        return None
    except Exception as e:
        print(f"  [report] Upload error: {e}")
        return None


def _send_pushover(title: str, message: str, url: str = "") -> None:
    user_key  = os.environ.get("PUSHOVER_USER_KEY", "")
    api_token = os.environ.get("PUSHOVER_API_TOKEN", "")
    if not user_key or not api_token:
        return
    try:
        requests.post(
            "https://api.pushover.net/1/messages.json",
            data={"token": api_token, "user": user_key, "title": title,
                  "message": message, "url": url,
                  "url_title": "Download PDF Report",
                  "priority": 0, "sound": "cashregister"},
            timeout=10,
        )
        print(f"  [report] Pushover sent: {title}")
    except Exception as e:
        print(f"  [report] Pushover failed: {e}")


def _upload_and_notify(filename: str, title: str, message: str) -> Optional[str]:
    url = _upload_to_filebin(filename)
    _send_pushover(title, message, url=url or "")
    return url


# ─── Checkpoint 1: Day 15 — Sanity Check ─────────────────────────────────────

def generate_checkpoint_15(df: pd.DataFrame) -> Optional[Tuple[str, str]]:
    print("\n[CHECKPOINT 15] Generating Day-15 Sanity Check...")
    os.makedirs(REPORTS_DIR, exist_ok=True)
    filename = f"{REPORTS_DIR}/APEX_Checkpoint15_{datetime.now().strftime('%Y-%m-%d')}.pdf"

    s = _compute_stats(df)
    degen   = s.get("degenerate_labels", [])
    missing = s.get("missing_days", [])

    pass_all = (
        s.get("total", 0)           >= 100 and
        s.get("pct_1hr_filled", 0)  >= 70  and
        s.get("pct_1day_filled", 0) >= 50  and
        not degen                          and
        len(missing) <= 2
    )
    verdict, v_color = ("OPERATIONAL", C_BLUE) if pass_all else ("NEEDS FIXING", C_RED)

    doc   = SimpleDocTemplate(filename, pagesize=letter,
                               leftMargin=L_MARGIN, rightMargin=R_MARGIN,
                               topMargin=0.65 * inch, bottomMargin=0.65 * inch)
    story = []

    story.append(_header_table("APEX Capital — 15 Day Sanity Check"))
    story.append(_hr())
    story.append(Spacer(1, 8))
    story.append(_verdict_box(verdict, v_color))
    story.append(Spacer(1, 14))

    story.append(Paragraph("OPERATIONAL METRICS", ST["section"]))
    story.append(_clean_table(
        ["Metric", "Value"],
        [
            ["Total signals logged",       str(s.get("total", 0))],
            ["Distinct trading days",      str(s.get("trading_days", 0))],
            ["Average signals per day",    str(s.get("avg_per_day", 0))],
            ["Date range",                 f"{s.get('date_min', '—')}  to  {s.get('date_max', '—')}"],
            ["1hr price fill rate",        f"{s.get('pct_1hr_filled', 0):.1f}%"],
            ["1day price fill rate",       f"{s.get('pct_1day_filled', 0):.1f}%"],
            ["5day price fill rate",       f"{s.get('pct_5day_filled', 0):.1f}%"],
            ["Missing trading day gaps",   str(len(missing))],
        ],
        [4.5 * inch, 2.5 * inch], right_cols=[1],
    ))

    story.append(Paragraph("WIN RATE INTEGRITY CHECK", ST["section"]))
    if degen:
        story.append(Paragraph(
            f"Warning: the following labels have 0% or 100% win rates with 5+ resolved signals, "
            f"which indicates a bug rather than real accuracy: {', '.join(degen)}.",
            ST["warn"],
        ))
    else:
        story.append(Paragraph(
            "No degenerate win rates detected. All labels with 5+ resolved signals fall between 1% and 99%.",
            ST["body"],
        ))

    story.append(Paragraph("COVERAGE GAPS", ST["section"]))
    if missing:
        miss_str = ", ".join(str(d) for d in missing[:10])
        if len(missing) > 10:
            miss_str += f" ... and {len(missing) - 10} more"
        story.append(Paragraph(
            f"{len(missing)} trading day(s) with no signals logged: {miss_str}.", ST["body"]
        ))
    else:
        story.append(Paragraph(
            "Scanner ran on every trading day in the date range. No gaps detected.", ST["body"]
        ))

    story.append(Paragraph("CONCLUSION", ST["section"]))
    if pass_all:
        concl = (
            f"The scanner is operational. {s.get('total', 0)} signals logged across "
            f"{s.get('trading_days', 0)} trading days at {s.get('avg_per_day', 0)} per day. "
            f"Outcome prices are filling in correctly. Proceed to Checkpoint 2 in 15 trading days."
        )
    else:
        issues = []
        if s.get("total", 0) < 100:
            issues.append(f"only {s.get('total', 0)} signals logged (target: 100+)")
        if s.get("pct_1hr_filled", 0) < 70:
            issues.append(f"1hr fill rate {s.get('pct_1hr_filled', 0):.1f}% (target: 70%+)")
        if s.get("pct_1day_filled", 0) < 50:
            issues.append(f"1day fill rate {s.get('pct_1day_filled', 0):.1f}% (target: 50%+)")
        if degen:
            issues.append(f"degenerate win rates: {', '.join(degen)}")
        if len(missing) > 2:
            issues.append(f"{len(missing)} days with no signals")
        concl = f"Issues detected: {'; '.join(issues)}. Investigate before Checkpoint 2."
    story.append(Paragraph(concl, ST["concl"]))

    story.append(Spacer(1, 20))
    story.append(_hr())
    story.append(Paragraph("Research purposes only. Not financial advice.", ST["small"]))

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    print(f"  [report] Saved: {filename}  verdict={verdict}")
    return filename, verdict


# ─── Checkpoint 2: Day 30 — Preliminary Assessment ───────────────────────────

def generate_checkpoint_30(df: pd.DataFrame) -> Optional[Tuple[str, str]]:
    print("\n[CHECKPOINT 30] Generating Day-30 Preliminary Assessment...")
    os.makedirs(REPORTS_DIR, exist_ok=True)
    filename = f"{REPORTS_DIR}/APEX_Checkpoint30_{datetime.now().strftime('%Y-%m-%d')}.pdf"

    s      = _compute_stats(df)
    wr     = s.get("overall_win_rate")
    by_sig = s.get("by_signal", {})
    sb_wr  = by_sig.get("Strong Buy Candidate", {}).get("win_rate")
    comp_c = s.get("comp_corr", {})

    has_correlation = any(
        abs(d.get("diff") or 0) >= 5
        for d in comp_c.values() if d.get("diff") is not None
    )

    if s.get("total", 0) >= 300 and wr is not None and wr >= 52 and (sb_wr is None or sb_wr >= 55) and has_correlation:
        verdict, v_color = "PROMISING", C_GREEN
    elif wr is not None and 50 <= wr < 52:
        verdict, v_color = "INCONCLUSIVE", C_AMBER
    else:
        verdict, v_color = "UNDERPERFORMING", C_RED

    doc   = SimpleDocTemplate(filename, pagesize=letter,
                               leftMargin=L_MARGIN, rightMargin=R_MARGIN,
                               topMargin=0.65 * inch, bottomMargin=0.65 * inch)
    story = []

    story.append(_header_table("APEX Capital — 30 Day Preliminary Assessment"))
    story.append(_hr())
    story.append(Spacer(1, 8))
    story.append(_verdict_box(verdict, v_color))
    story.append(Spacer(1, 14))

    story.append(Paragraph("SUMMARY METRICS", ST["section"]))
    story.append(_clean_table(
        ["Metric", "Value"],
        [
            ["Total signals logged",        str(s.get("total", 0))],
            ["Resolved signals (5-day)",    str(s.get("resolved", 0))],
            ["Overall win rate (5-day)",    f"{wr:.1f}%" if wr is not None else "—"],
            ["Average signals per day",     str(s.get("avg_per_day", 0))],
            ["1day price completeness",     f"{s.get('pct_1day_filled', 0):.1f}%"],
            ["5day price completeness",     f"{s.get('pct_5day_filled', 0):.1f}%"],
        ],
        [4.5 * inch, 2.5 * inch], right_cols=[1],
    ))
    story.append(Spacer(1, 10))

    story.append(Paragraph("WIN RATE BY SIGNAL TYPE (5-DAY)", ST["section"]))
    order = ["Strong Buy Candidate", "Speculative Buy", "Gap-Up", "Watchlist", "Hold", "Trim", "Sell", "Avoid"]
    sig_rows = []
    for lbl in order + [l for l in by_sig if l not in order]:
        d = by_sig.get(lbl)
        if not d:
            continue
        sig_rows.append([
            lbl,
            str(d["count"]),
            f"{d['win_rate']:.1f}%",
            f"+{d['avg_gain']:.1f}%" if d.get("avg_gain") is not None else "—",
            f"{d['avg_loss']:.1f}%"  if d.get("avg_loss") is not None else "—",
        ])
    if sig_rows:
        story.append(_clean_table(
            ["Signal Type", "N", "Win Rate", "Avg Gain", "Avg Loss"],
            sig_rows,
            [2.8 * inch, 0.6 * inch, 1.0 * inch, 1.0 * inch, 1.0 * inch],
            right_cols=[1, 2, 3, 4],
        ))
    else:
        story.append(Paragraph("Insufficient resolved signals to compute win rates.", ST["body"]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("COMPONENT SCORE — WINS VS LOSSES (5-DAY AVERAGES)", ST["section"]))
    if comp_c:
        corr_rows = []
        for c in ["technical", "catalyst", "fundamental", "risk", "sentiment"]:
            d    = comp_c.get(c, {})
            diff = d.get("diff")
            diff_str = (f"+{diff:.1f}" if diff is not None and diff > 0
                        else (f"{diff:.1f}" if diff is not None else "—"))
            corr_rows.append([
                c.title(),
                f"{d['win_mean']:.1f}"  if d.get("win_mean")  is not None else "—",
                f"{d['loss_mean']:.1f}" if d.get("loss_mean") is not None else "—",
                diff_str,
            ])
        story.append(_clean_table(
            ["Component", "Wins (avg)", "Losses (avg)", "Difference"],
            corr_rows,
            [2.2 * inch, 1.6 * inch, 1.6 * inch, 1.6 * inch],
            right_cols=[1, 2, 3],
        ))
    else:
        story.append(Paragraph("Insufficient data for component correlation.", ST["body"]))

    if by_sig:
        best_lbl  = max(by_sig, key=lambda l: by_sig[l]["win_rate"])
        worst_lbl = min(by_sig, key=lambda l: by_sig[l]["win_rate"])
        story.append(Paragraph("SIGNAL PERFORMANCE SUMMARY", ST["section"]))
        story.append(Paragraph(
            f"Best performing signal: {best_lbl} ({by_sig[best_lbl]['win_rate']:.1f}% win rate).", ST["body"]
        ))
        story.append(Paragraph(
            f"Worst performing signal: {worst_lbl} ({by_sig[worst_lbl]['win_rate']:.1f}% win rate).", ST["body"]
        ))

    story.append(Paragraph("WEIGHT WATCH — COMPONENTS TO MONITOR", ST["section"]))
    if comp_c:
        ranked = sorted(
            [(c, d) for c, d in comp_c.items() if d.get("diff") is not None],
            key=lambda x: abs(x[1]["diff"]), reverse=True,
        )
        for c, d in ranked[:3]:
            diff = d["diff"]
            direction = "higher on wins" if diff > 0 else "lower on wins"
            story.append(Paragraph(
                f"{c.title()}: {abs(diff):.1f} points {direction} than losses. "
                f"{'Monitor for potential weight increase.' if diff > 0 else 'Monitor for potential weight reduction.'}",
                ST["bullet"],
            ))
    else:
        story.append(Paragraph("Insufficient data for weight recommendations.", ST["body"]))

    story.append(Paragraph("CONCLUSION", ST["section"]))
    if verdict == "PROMISING":
        concl = (
            f"Early results are positive. {s.get('total', 0)} signals logged with a 5-day win rate of "
            f"{wr:.1f}%. At least one component shows early predictive correlation. "
            f"Run through Day 60 without changing weights."
        )
    elif verdict == "INCONCLUSIVE":
        concl = (
            f"Win rate of {wr:.1f}% is near the coin-flip baseline. More data needed. "
            f"Continue to Day 60 without changing weights or thresholds."
        )
    else:
        concl = (
            f"Win rate of {(wr or 0):.1f}% is below the coin-flip baseline, or insufficient signals logged. "
            f"Review scoring logic and component weights before continuing."
        )
    story.append(Paragraph(concl, ST["concl"]))

    story.append(Spacer(1, 20))
    story.append(_hr())
    story.append(Paragraph("Research purposes only. Not financial advice.", ST["small"]))

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    print(f"  [report] Saved: {filename}  verdict={verdict}")
    return filename, verdict


# ─── Checkpoint 3: Day 60 — Final Verdict ────────────────────────────────────

def generate_checkpoint_60(df: pd.DataFrame) -> Optional[Tuple[str, str]]:
    print("\n[CHECKPOINT 60] Generating Day-60 Final Verdict...")
    os.makedirs(REPORTS_DIR, exist_ok=True)
    filename = f"{REPORTS_DIR}/APEX_Checkpoint60_{datetime.now().strftime('%Y-%m-%d')}.pdf"

    s      = _compute_stats(df)
    wr     = s.get("overall_win_rate")
    by_sig = s.get("by_signal", {})
    sb_wr  = by_sig.get("Strong Buy Candidate", {}).get("win_rate")
    avg_w  = s.get("avg_win")
    avg_l  = s.get("avg_loss")
    comp_c = s.get("comp_corr", {})

    max_diff      = max((abs(d.get("diff") or 0) for d in comp_c.values()), default=0)
    gain_over_loss = (avg_w is not None and avg_l is not None and avg_w > abs(avg_l))

    approved = (
        s.get("total", 0) >= 500 and
        wr is not None and wr >= 55 and
        (sb_wr is None or sb_wr >= 60) and
        gain_over_loss and
        max_diff >= 10
    )
    rejected = (
        (wr is not None and wr < 50) or
        not gain_over_loss or
        max_diff < 3 or
        (sb_wr is not None and sb_wr < 50)
    )

    if approved:
        verdict, v_color = "APPROVED", C_GREEN
    elif rejected:
        verdict, v_color = "REJECTED", C_RED
    else:
        verdict, v_color = "EXTEND TEST — RUN 30 MORE DAYS", C_AMBER

    doc   = SimpleDocTemplate(filename, pagesize=letter,
                               leftMargin=L_MARGIN, rightMargin=R_MARGIN,
                               topMargin=0.65 * inch, bottomMargin=0.65 * inch)
    story = []

    story.append(_header_table("APEX Capital — 60 Day Accuracy Assessment"))
    story.append(_hr())
    story.append(Spacer(1, 8))
    story.append(_verdict_box(verdict, v_color))
    story.append(Spacer(1, 14))

    # Full summary table
    story.append(Paragraph("FULL ACCURACY SUMMARY", ST["section"]))
    metrics_rows = [
        ["Total signals logged",        str(s.get("total", 0))],
        ["Resolved signals",            str(s.get("resolved", 0))],
        ["Overall win rate (5-day)",    f"{wr:.1f}%"        if wr     is not None else "—"],
        ["Average win (5-day)",         f"+{avg_w:.2f}%"    if avg_w  is not None else "—"],
        ["Average loss (5-day)",        f"{avg_l:.2f}%"     if avg_l  is not None else "—"],
        ["Gain exceeds loss",           "Yes" if gain_over_loss else "No"],
    ]
    if by_sig:
        best_lbl  = max(by_sig, key=lambda l: by_sig[l]["win_rate"])
        worst_lbl = min(by_sig, key=lambda l: by_sig[l]["win_rate"])
        metrics_rows += [
            ["Best signal type",       f"{best_lbl} ({by_sig[best_lbl]['win_rate']:.1f}%)"],
            ["Worst signal type",      f"{worst_lbl} ({by_sig[worst_lbl]['win_rate']:.1f}%)"],
        ]
    if s.get("best_ticker"):
        bt = s["best_ticker"]
        metrics_rows.append(["Best ticker (5-day)", f"{bt[0]}  +{bt[1]:.1f}%"])
    if s.get("worst_ticker"):
        wt = s["worst_ticker"]
        metrics_rows.append(["Worst ticker (5-day)", f"{wt[0]}  {wt[1]:.1f}%"])
    story.append(_clean_table(["Metric", "Value"], metrics_rows,
                               [4.5 * inch, 2.5 * inch], right_cols=[1]))
    story.append(Spacer(1, 10))

    # Win rate table with all three windows
    story.append(Paragraph("WIN RATE BY SIGNAL TYPE — 1-DAY / 5-DAY / 15-DAY", ST["section"]))
    order = ["Strong Buy Candidate", "Speculative Buy", "Gap-Up", "Watchlist", "Hold", "Trim", "Sell", "Avoid"]
    sig_rows = []
    for lbl in order + [l for l in by_sig if l not in order]:
        d = by_sig.get(lbl)
        if not d:
            continue
        sig_rows.append([
            lbl,
            str(d["count"]),
            f"{d.get('wr_1day'):.1f}%"  if d.get("wr_1day")  is not None else "—",
            f"{d['win_rate']:.1f}%",
            f"{d.get('wr_15day'):.1f}%" if d.get("wr_15day") is not None else "—",
            f"+{d['avg_gain']:.1f}%"    if d.get("avg_gain") is not None else "—",
            f"{d['avg_loss']:.1f}%"     if d.get("avg_loss") is not None else "—",
        ])
    if sig_rows:
        story.append(_clean_table(
            ["Signal Type", "N", "1-Day WR", "5-Day WR", "15-Day WR", "Avg Gain", "Avg Loss"],
            sig_rows,
            [2.1 * inch, 0.45 * inch, 0.8 * inch, 0.8 * inch, 0.9 * inch, 0.8 * inch, 0.8 * inch],
            right_cols=[1, 2, 3, 4, 5, 6],
        ))
    story.append(Spacer(1, 10))

    # Component correlation ranked
    story.append(Paragraph("COMPONENT CORRELATION — RANKED BY PREDICTIVE POWER", ST["section"]))
    if comp_c:
        ranked = sorted(
            [(c, d) for c, d in comp_c.items() if d.get("diff") is not None],
            key=lambda x: abs(x[1]["diff"]), reverse=True,
        )
        unranked = [(c, d) for c, d in comp_c.items() if d.get("diff") is None
                    and c not in [x[0] for x in ranked]]
        all_comp = ranked + unranked
        corr_rows = []
        for rank, (c, d) in enumerate(all_comp, 1):
            diff     = d.get("diff")
            diff_str = (f"+{diff:.1f}" if diff is not None and diff > 0
                        else (f"{diff:.1f}" if diff is not None else "—"))
            corr_rows.append([
                str(rank), c.title(),
                f"{d['win_mean']:.1f}"  if d.get("win_mean")  is not None else "—",
                f"{d['loss_mean']:.1f}" if d.get("loss_mean") is not None else "—",
                diff_str,
            ])
        story.append(_clean_table(
            ["Rank", "Component", "Wins avg", "Losses avg", "Difference"],
            corr_rows,
            [0.5 * inch, 1.8 * inch, 1.6 * inch, 1.6 * inch, 1.5 * inch],
            right_cols=[2, 3, 4],
        ))

    # Weight suggestions
    story.append(Paragraph("SUGGESTED WEIGHT ADJUSTMENTS", ST["section"]))
    defaults = {"technical": 30, "catalyst": 25, "fundamental": 20, "risk": 15, "sentiment": 10}
    if comp_c:
        for c, d in (ranked if comp_c else [])[:5]:
            diff    = d.get("diff") or 0
            cur_pct = defaults.get(c, 0)
            if diff >= 10:
                rec = f"Consider increasing weight above {cur_pct}% — scores {diff:.1f} pts higher on wins."
            elif diff <= -5:
                rec = f"Consider reducing weight below {cur_pct}% — scores {abs(diff):.1f} pts lower on wins."
            elif abs(diff) < 3:
                rec = f"Weight of {cur_pct}% appears appropriate — minimal win/loss divergence."
            else:
                rec = f"No change recommended at {cur_pct}%."
            story.append(Paragraph(f"{c.title()}: {rec}", ST["bullet"]))
    else:
        story.append(Paragraph("Insufficient data for weight recommendations.", ST["body"]))

    # Conclusion
    story.append(Paragraph("FINAL CONCLUSION", ST["section"]))
    if verdict == "APPROVED":
        concl = (
            f"The scanner demonstrates statistically meaningful predictive accuracy over {s.get('resolved', 0)} "
            f"resolved signals. 5-day win rate of {wr:.1f}% with average wins ({avg_w:+.2f}%) exceeding average "
            f"losses ({avg_l:.2f}%). At least one component shows strong correlation with positive outcomes. "
            f"Adjust weights based on the component correlation table above. "
            f"Recalibrate after each additional 30-day block."
        )
    elif verdict == "REJECTED":
        issues = []
        if wr is not None and wr < 50:
            issues.append(f"win rate {wr:.1f}% is below the coin-flip baseline")
        if not gain_over_loss:
            issues.append("average losses exceed average gains")
        if max_diff < 3:
            issues.append("no component shows meaningful predictive correlation")
        concl = (
            f"The scanner is not demonstrating predictive accuracy. {'; '.join(issues).capitalize()}. "
            f"Review the scoring algorithm, data sources, and signal construction before continuing."
        )
    else:
        concl = (
            f"Win rate of {(wr or 0):.1f}% is above the coin-flip baseline but below the 55% approval threshold. "
            f"Extend the test by 30 trading days with current weights and thresholds unchanged. "
            f"Changing weights mid-test invalidates the accumulated data."
        )
    story.append(Paragraph(concl, ST["concl"]))

    story.append(Spacer(1, 20))
    story.append(_hr())
    story.append(Paragraph("Research purposes only. Not financial advice.", ST["small"]))

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    print(f"  [report] Saved: {filename}  verdict={verdict}")
    return filename, verdict


# ─── Orchestrator ─────────────────────────────────────────────────────────────

def check_and_run_checkpoints() -> None:
    """
    Called at EOD. Generates whichever checkpoint reports are newly due
    based on trading days elapsed since the first logged signal.
    """
    try:
        from db.database import get_signal_log, get_accuracy_reports, save_accuracy_report
    except Exception as e:
        print(f"  [checkpoint] DB import failed: {e}")
        return

    df = get_signal_log(days=90)
    if df.empty:
        print("  [checkpoint] No signals yet — skipping checkpoint check")
        return

    df["_date"]   = pd.to_datetime(df["created_at"]).dt.date
    first_date    = df["_date"].min()
    today         = datetime.now().date()
    bdays_elapsed = len(pd.bdate_range(first_date, today))
    print(f"  [checkpoint] {bdays_elapsed} trading days since first signal ({first_date})")

    existing = {r["report_type"] for r in get_accuracy_reports()}

    CHECKPOINTS = [
        (15, "checkpoint_15", generate_checkpoint_15,
         "APEX Scanner — 15 Day Sanity Check"),
        (30, "checkpoint_30", generate_checkpoint_30,
         "APEX Scanner — 30 Day Preliminary Assessment"),
        (60, "checkpoint_60", generate_checkpoint_60,
         "APEX Scanner — 60 Day Accuracy Assessment"),
    ]

    for days, rtype, gen_fn, title in CHECKPOINTS:
        if bdays_elapsed >= days and rtype not in existing:
            print(f"  [checkpoint] Threshold Day {days} reached — generating {rtype}...")
            try:
                result = gen_fn(df)
                if result is None:
                    print(f"  [checkpoint] {rtype} generation returned None")
                    continue
                filename, verdict = result
                url = _upload_and_notify(
                    filename, title,
                    f"Verdict: {verdict}\n{len(df)} signals in dataset. Tap to download PDF.",
                )
                save_accuracy_report(rtype, days, filename, url or "", verdict)
                print(f"  [checkpoint] {rtype} complete — verdict={verdict}")
            except Exception as exc:
                print(f"  [checkpoint] {rtype} FAILED: {exc}")
                traceback.print_exc()
