# app.py — Axiom Terminal
# ALL helper functions defined FIRST before any UI code.

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime
import sys, os, time as _time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DEFAULT_UNIVERSE, SCORING_WEIGHTS, RISK_FLAGS
from db.database import initialize_db, upsert_holding, delete_holding, get_portfolio, get_connection
from core.scanner import scan_ticker, scan_universe
from analysis.portfolio import analyze_holding, compute_portfolio_summary
from data.market_data import fetch_ticker_snapshot, get_price_history, get_chart_data
from analysis.technicals import compute_technicals

st.set_page_config(page_title="Axiom Terminal", page_icon="A", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@300;400;500;600&display=swap');
@keyframes pulse-amber {
    0%,100% { box-shadow: 0 0 0 1px rgba(194,97,15,0.2); }
    50%      { box-shadow: 0 0 0 3px rgba(194,97,15,0.35); }
}
@keyframes fadeIn { from { opacity:0; transform:translateY(3px); } to { opacity:1; transform:none; } }
@keyframes live-dot { 0%,100% { opacity:1; } 50% { opacity:0.3; } }

:root {
    --bg:       #f7f8fb;
    --bg2:      #eff1f7;
    --bg3:      #e6e9f2;
    --bgcard:   #ffffff;
    --bghover:  #f0f2f9;
    --border:   #dde1ec;
    --borderhi: #b0bace;
    --green:    #16a34a;
    --green2:   #15803d;
    --blue:     #1d6fa5;
    --blue2:    #2563eb;
    --amber:    #c2610f;
    --amber2:   #ea580c;
    --red:      #dc2626;
    --red2:     #ef4444;
    --purple:   #6d28d9;
    --t1:       #0f172a;
    --t2:       #334155;
    --t3:       #94a3b8;
    --tdim:     #cbd5e1;
}

/* ── Base ── */
.stApp { background: var(--bg) !important; }
.stApp > header { background: transparent !important; }
.main .block-container { padding: 0 1.5rem 2rem; max-width: 100%; }
* { font-family: 'Inter', system-ui, sans-serif; }
#MainMenu, footer, .stDeployButton { visibility: hidden; }

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: var(--bg2) !important;
    border-right: 1px solid var(--border) !important;
}
section[data-testid="stSidebar"] > div { padding-top: 0 !important; }

/* ── Inputs ── */
.stTextInput input, .stTextArea textarea {
    background: var(--bgcard) !important;
    border: 1px solid var(--border) !important;
    color: var(--t1) !important;
    border-radius: 6px !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.82em !important;
    transition: border-color 0.2s !important;
}
.stTextInput input:focus, .stTextArea textarea:focus {
    border-color: var(--blue) !important;
    box-shadow: 0 0 0 3px rgba(29,111,165,0.12) !important;
}

/* ── Buttons ── */
.stButton > button {
    background: var(--bg3) !important;
    border: 1px solid var(--border) !important;
    color: var(--t2) !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 500 !important;
    font-size: 0.78em !important;
    letter-spacing: 0.04em !important;
    border-radius: 6px !important;
    transition: all 0.15s !important;
    padding: 0.4rem 1rem !important;
}
.stButton > button:hover {
    border-color: var(--borderhi) !important;
    color: var(--t1) !important;
    background: var(--bghover) !important;
}
.stButton > button[kind="primary"] {
    background: var(--amber) !important;
    border-color: var(--amber) !important;
    color: #fff !important;
    font-weight: 600 !important;
}
.stButton > button[kind="primary"]:hover {
    background: var(--amber2) !important;
    border-color: var(--amber2) !important;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    border-bottom: 1px solid var(--border) !important;
    gap: 0 !important;
    padding: 0 !important;
}
.stTabs [data-baseweb="tab"] {
    font-family: 'Inter', sans-serif !important;
    font-weight: 500 !important;
    font-size: 0.8em !important;
    letter-spacing: 0.01em !important;
    color: var(--t3) !important;
    padding: 11px 20px !important;
    border-bottom: 2px solid transparent !important;
    background: transparent !important;
    transition: color 0.15s !important;
}
.stTabs [data-baseweb="tab"]:hover { color: var(--t2) !important; }
.stTabs [aria-selected="true"] {
    color: var(--amber) !important;
    border-bottom-color: var(--amber) !important;
    font-weight: 600 !important;
}

/* ── Metrics ── */
[data-testid="metric-container"] {
    background: var(--bgcard) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    padding: 12px 16px !important;
    transition: border-color 0.15s !important;
}
[data-testid="metric-container"]:hover { border-color: var(--borderhi) !important; }
[data-testid="metric-container"] label {
    color: var(--t3) !important;
    font-size: 0.66em !important;
    letter-spacing: 0.06em !important;
    text-transform: uppercase !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 500 !important;
}
[data-testid="metric-container"] [data-testid="metric-value"] {
    color: var(--t1) !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 1.15em !important;
    font-weight: 500 !important;
}
[data-testid="stMetricDelta"] { font-family: 'JetBrains Mono', monospace !important; font-size: 0.75em !important; }

/* ── Expanders ── */
[data-testid="stExpander"] {
    background: var(--bgcard) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    margin-bottom: 6px !important;
    transition: border-color 0.15s !important;
    overflow: hidden !important;
}
[data-testid="stExpander"]:hover { border-color: var(--borderhi) !important; }
[data-testid="stExpander"] summary {
    color: var(--t2) !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 0.82em !important;
    font-weight: 500 !important;
    padding: 12px 16px !important;
}
[data-testid="stExpander"] summary:hover { background: var(--bghover) !important; color: var(--t1) !important; }

/* ── Misc ── */
hr { border-color: var(--border) !important; margin: 10px 0 !important; }
.stCheckbox label { color: var(--t2) !important; font-size: 0.82em !important; }
.stSlider { padding: 0 !important; }
p, li { color: var(--t2); line-height: 1.7; }
h1,h2,h3 { color: var(--t1) !important; font-family: 'Inter', sans-serif !important; font-weight: 700 !important; letter-spacing: -0.02em !important; }

/* ── Radio (used as toggle) ── */
[data-testid="stRadio"] > div { gap: 4px !important; }
[data-testid="stRadio"] label {
    background: var(--bg3) !important;
    border: 1px solid var(--border) !important;
    border-radius: 5px !important;
    padding: 4px 12px !important;
    font-size: 0.75em !important;
    font-weight: 500 !important;
    color: var(--t2) !important;
    cursor: pointer !important;
    transition: all 0.15s !important;
}
[data-testid="stRadio"] label:hover { border-color: var(--borderhi) !important; color: var(--t1) !important; }
[data-testid="stRadio"] label[data-checked="true"],
[data-testid="stRadio"] label:has(input:checked) {
    background: var(--bghover) !important;
    border-color: var(--amber) !important;
    color: var(--amber) !important;
}

/* ── Custom components ── */

/* Score pill */
.pill { display:inline-flex; align-items:center; padding:2px 9px; border-radius:4px; font-family:'Inter',sans-serif; font-weight:600; font-size:0.68em; letter-spacing:0.03em; }
.p-sb  { background:rgba(22,163,74,0.1);  color:#16a34a; border:1px solid rgba(22,163,74,0.25); }
.p-sp  { background:rgba(21,128,61,0.1);  color:#15803d; border:1px solid rgba(21,128,61,0.25); }
.p-wl  { background:rgba(194,97,15,0.1);  color:#c2610f; border:1px solid rgba(194,97,15,0.25); }
.p-ho  { background:rgba(194,97,15,0.1);  color:#c2610f; border:1px solid rgba(194,97,15,0.2); }
.p-tr  { background:rgba(234,88,12,0.1);  color:#ea580c; border:1px solid rgba(234,88,12,0.2); }
.p-se  { background:rgba(220,38,38,0.1);  color:#dc2626; border:1px solid rgba(220,38,38,0.2); }
.p-av  { background:rgba(220,38,38,0.1);  color:#dc2626; border:1px solid rgba(220,38,38,0.3); }

/* Risk flag chips */
.flag { display:inline-flex; align-items:center; background:rgba(194,97,15,0.06); border:1px solid rgba(194,97,15,0.18); color:#c2610f; padding:1px 7px; border-radius:3px; font-size:0.64em; font-family:'JetBrains Mono',monospace; margin:2px 2px 2px 0; }
.flag.crit { background:rgba(220,38,38,0.07); border-color:rgba(220,38,38,0.2); color:#dc2626; }
.flag.earn { background:rgba(109,40,217,0.07); border-color:rgba(109,40,217,0.2); color:#6d28d9; }

/* Score bar */
.sbar-track { background:rgba(0,0,0,0.08); border-radius:2px; height:3px; width:100%; }
.sbar-fill { border-radius:2px; height:3px; }

/* Stat rows */
.stat-row { display:flex; justify-content:space-between; align-items:center; padding:5px 0; border-bottom:1px solid rgba(0,0,0,0.05); }
.slbl { color:var(--t3); font-size:0.68em; letter-spacing:0.04em; font-family:'Inter',sans-serif; font-weight:500; }
.sval { color:var(--t1); font-size:0.8em; font-family:'JetBrains Mono',monospace; font-weight:500; }
.sval.g { color:var(--green); } .sval.r { color:var(--red); } .sval.a { color:var(--amber); } .sval.b { color:var(--blue2); } .sval.p { color:var(--purple); }

/* Section headers */
.sh { font-family:'Inter',sans-serif; font-size:0.68em; font-weight:600; letter-spacing:0.08em; text-transform:uppercase; color:var(--t3); margin:16px 0 10px; padding-bottom:6px; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:6px; }

/* Info boxes */
.box { background:rgba(0,0,0,0.02); border:1px solid var(--border); border-radius:8px; padding:12px 16px; color:var(--t2); font-size:0.85em; line-height:1.75; }
.box-blue   { border-color:rgba(29,111,165,0.25);  background:rgba(29,111,165,0.04); }
.box-green  { border-color:rgba(22,163,74,0.25);   background:rgba(22,163,74,0.03); }
.box-red    { border-color:rgba(220,38,38,0.25);   background:rgba(220,38,38,0.04); }
.box-amber  { border-color:rgba(194,97,15,0.25);   background:rgba(194,97,15,0.04); }
.box-purple { border-color:rgba(109,40,217,0.25);  background:rgba(109,40,217,0.04); }

/* Disclaimer */
.disc { background:transparent; border:1px solid rgba(194,97,15,0.12); border-radius:4px; padding:6px 12px; color:rgba(194,97,15,0.5); font-size:0.62em; font-family:'JetBrains Mono',monospace; letter-spacing:0.05em; text-align:center; margin:8px 0; }

/* Empty states */
.empty { text-align:center; padding:80px 20px; color:var(--t3); }
.empty .ico { font-size:2.5em; margin-bottom:16px; opacity:0.4; }
.empty h3 { color:var(--t2) !important; letter-spacing:0.04em; font-size:1.3em; font-weight:600; }
.empty p { color:var(--t3); font-size:0.82em; line-height:1.7; }

/* Ticker header */
.ticker-hero { padding:16px 0 14px; border-bottom:1px solid var(--border); margin-bottom:20px; }

/* News item */
.news-item { display:flex; align-items:flex-start; gap:10px; padding:8px 0; border-bottom:1px solid rgba(0,0,0,0.05); }
.news-dot { width:5px; height:5px; border-radius:50%; margin-top:7px; flex-shrink:0; }

/* Summary text */
.summary-block { background:rgba(0,0,0,0.02); border-left:2px solid var(--borderhi); border-radius:0 8px 8px 0; padding:14px 16px; color:var(--t2); font-size:0.83em; line-height:1.85; white-space:pre-line; }

/* Score ring */
.score-ring-wrap { position:relative; display:inline-flex; align-items:center; justify-content:center; }

/* Sidebar logo */
.axiom-logo { padding:20px 16px 12px; border-bottom:1px solid var(--border); margin-bottom:4px; }
.axiom-logo .name { font-family:'Inter',sans-serif; font-size:1.4em; font-weight:700; color:var(--t1); letter-spacing:-0.02em; line-height:1; }
.axiom-logo .sub { font-family:'JetBrains Mono',monospace; font-size:0.58em; color:var(--t3); letter-spacing:0.2em; margin-top:4px; }

/* Signal count card */
.sig-count { text-align:center; padding:10px 6px; background:var(--bgcard); border:1px solid var(--border); border-radius:8px; transition:border-color 0.15s; }
.sig-count:hover { border-color:var(--borderhi); }
.sig-count .num { font-family:'JetBrains Mono',monospace; font-size:1.6em; font-weight:600; line-height:1; }
.sig-count .lbl { font-family:'Inter',sans-serif; font-size:0.55em; color:var(--t3); letter-spacing:0.08em; text-transform:uppercase; margin-top:3px; }

/* Portfolio card */
.port-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:14px; flex-wrap:wrap; gap:10px; }

/* RS badge */
.rs-badge { display:inline-flex; align-items:center; gap:6px; padding:5px 12px; border-radius:4px; font-family:'JetBrains Mono',monospace; font-size:0.72em; font-weight:600; }

/* Earnings warning */
.earn-warn { background:rgba(109,40,217,0.05); border:1px solid rgba(109,40,217,0.18); border-radius:6px; padding:10px 14px; color:#6d28d9; font-size:0.78em; font-family:'JetBrains Mono',monospace; margin-top:8px; line-height:1.6; }

/* Holdings sidebar row */
.holding-row { display:grid; grid-template-columns:58px 60px 65px; align-items:center; padding:5px 10px; border-bottom:1px solid rgba(0,0,0,0.05); font-family:'JetBrains Mono',monospace; font-size:0.72em; gap:4px; animation:fadeIn 0.2s ease; }
.holding-row:hover { background:var(--bghover); }
.holding-ticker { color:#1d6fa5; font-weight:600; }
.holding-val { color:var(--t3); text-align:right; }

/* Portfolio table row */
.ptrow { display:grid; grid-template-columns:60px 70px 70px 80px 90px 80px 70px 50px; align-items:center; padding:6px 12px; border-bottom:1px solid rgba(0,0,0,0.05); font-family:'JetBrains Mono',monospace; font-size:0.72em; transition:background 0.15s; }
.ptrow:hover { background:var(--bghover); }
.pthdr { color:var(--t3); font-size:0.6em; letter-spacing:0.08em; text-transform:uppercase; }

/* Trade blotter row */
.trade-row { display:flex; align-items:center; gap:8px; padding:6px 10px; border-radius:5px; margin-bottom:3px; font-family:'JetBrains Mono',monospace; font-size:0.72em; transition:background 0.15s; flex-wrap:wrap; }
.trade-open  { border-left:2px solid rgba(194,97,15,0.6); background:rgba(194,97,15,0.05); animation:pulse-amber 3s infinite; }
.trade-win   { border-left:2px solid rgba(22,163,74,0.4);  background:rgba(22,163,74,0.04); }
.trade-loss  { border-left:2px solid rgba(220,38,38,0.4);  background:rgba(220,38,38,0.04); }

/* Scanner result column header */
.result-hdr { display:grid; grid-template-columns:70px 140px 80px 65px 65px 1fr; padding:5px 18px; background:rgba(0,0,0,0.02); border-bottom:1px solid var(--border); font-family:'Inter',sans-serif; font-size:0.6em; letter-spacing:0.08em; color:var(--t3); text-transform:uppercase; font-weight:600; }

/* Win-rate badge */
.wr-badge { display:inline-flex; align-items:center; gap:5px; padding:3px 10px; border-radius:4px; font-family:'JetBrains Mono',monospace; font-size:0.78em; font-weight:600; }
.wr-good  { background:rgba(22,163,74,0.08);  border:1px solid rgba(22,163,74,0.2);  color:#16a34a; }
.wr-bad   { background:rgba(220,38,38,0.08);  border:1px solid rgba(220,38,38,0.2);  color:#dc2626; }
.wr-neu   { background:rgba(194,97,15,0.08);  border:1px solid rgba(194,97,15,0.2);  color:#c2610f; }

/* Prediction direction chip */
.dir-long  { background:rgba(22,163,74,0.07);  border:1px solid rgba(22,163,74,0.2);  color:#16a34a; padding:1px 7px; border-radius:3px; font-size:0.65em; font-family:'JetBrains Mono',monospace; }
.dir-short { background:rgba(220,38,38,0.07);  border:1px solid rgba(220,38,38,0.2);  color:#dc2626; padding:1px 7px; border-radius:3px; font-size:0.65em; font-family:'JetBrains Mono',monospace; }

/* Live chart controls */
.chart-bar { display:flex; align-items:center; justify-content:space-between; padding:8px 0 12px; gap:12px; flex-wrap:wrap; }
.live-badge { display:inline-flex; align-items:center; gap:5px; font-family:'JetBrains Mono',monospace; font-size:0.68em; color:#16a34a; }
.live-dot { width:6px; height:6px; border-radius:50%; background:#16a34a; animation:live-dot 1.4s ease-in-out infinite; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS — ALL DEFINED BEFORE UI
# ══════════════════════════════════════════════════════════════════════════════

def pill_class(signal):
    return {"Strong Buy Candidate":"p-sb","Speculative Buy":"p-sp","Watchlist":"p-wl",
            "Hold":"p-ho","Trim":"p-tr","Sell":"p-se","Avoid":"p-av"}.get(signal,"p-ho")

def sig_color(signal):
    return {"Strong Buy Candidate":"#16a34a","Speculative Buy":"#15803d","Watchlist":"#c2610f",
            "Hold":"#c2610f","Trim":"#ea580c","Sell":"#dc2626","Avoid":"#7f1d1d"}.get(signal,"#475569")

def score_col(s):
    if s>=72: return "#16a34a"
    if s>=58: return "#15803d"
    if s>=45: return "#c2610f"
    if s>=33: return "#ea580c"
    if s>=22: return "#dc2626"
    return "#991b1b"

def fp(p):
    if p is None: return "—"
    return f"${p:.4f}" if p < 10 else f"${p:.2f}"

def fm(mc):
    if not mc: return "—"
    return f"${mc/1e9:.2f}B" if mc>=1e9 else f"${mc/1e6:.0f}M"

def fpct(v):
    return f"{v*100:.1f}%" if v is not None else "—"

def has_crit(flags):
    return bool({"going_concern","reverse_split_risk"}.intersection(set(flags)))

def flag_chips(flags):
    labels = {"going_concern":"GOING CONCERN","shelf_registration":"SHELF REG",
               "atm_offering":"ATM OFFERING","reverse_split_risk":"REV SPLIT",
               "high_short_interest":"HIGH SHORT","extreme_volatility":"HIGH VOL",
               "low_liquidity":"LOW LIQ","pump_signal":"PUMP RISK",
               "earnings_imminent":"EARNINGS SOON"}
    crits  = {"going_concern","reverse_split_risk"}
    earns  = {"earnings_imminent"}
    out = []
    for f in flags[:5]:
        cls = "crit" if f in crits else "earn" if f in earns else ""
        out.append(f'<span class="flag {cls}">{labels.get(f, f.upper().replace("_"," "))}</span>')
    return "".join(out)

def sbar(score, color):
    return (f'<div class="sbar-track"><div class="sbar-fill" '
            f'style="width:{min(score,100)}%;background:{color};box-shadow:0 0 8px {color}40;"></div></div>')

def stat_row(label, value, cls=""):
    return (f'<div class="stat-row"><span class="slbl">{label}</span>'
            f'<span class="sval {cls}">{value}</span></div>')

def score_ring_svg(score, color, size=80):
    r = 32; cx = 40; cy = 40
    circ = 2 * 3.14159 * r
    dash = circ * score / 100
    return f"""
    <svg width="{size}" height="{size}" viewBox="0 0 80 80">
      <circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="rgba(255,255,255,0.05)" stroke-width="6"/>
      <circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="{color}" stroke-width="6"
              stroke-dasharray="{dash:.1f} {circ:.1f}" stroke-linecap="round"
              transform="rotate(-90 {cx} {cy})"
              style="filter:drop-shadow(0 0 4px {color})"/>
      <text x="{cx}" y="{cy+1}" text-anchor="middle" dominant-baseline="middle"
            font-family="'Bebas Neue',sans-serif" font-size="18" fill="{color}">{score:.0f}</text>
      <text x="{cx}" y="{cy+14}" text-anchor="middle" dominant-baseline="middle"
            font-family="'JetBrains Mono',monospace" font-size="7" fill="rgba(255,255,255,0.25)">/100</text>
    </svg>"""


def render_result_card(r):
    ticker  = r.get("ticker","?")
    name    = r.get("company_name", ticker)
    price   = r.get("price", 0)
    score   = r.get("final_score", 0)
    signal  = r.get("signal","—")
    rvol    = r.get("relative_volume", 0) or 0
    ret1d   = r.get("return_1d", 0) or 0
    ret5d   = r.get("return_5d", 0) or 0
    flags   = r.get("risk_flags", [])
    rsi     = r.get("rsi")
    mc      = r.get("market_cap", 0)
    pc      = pill_class(signal)
    sc      = score_col(score)
    sclr    = sig_color(signal)
    rarrow  = "▲" if ret1d >= 0 else "▼"
    rcolor  = "#16a34a" if ret1d >= 0 else "#dc2626"

    cat_notes = r.get("catalyst_notes", [])
    cat_str   = cat_notes[0] if cat_notes else "No confirmed catalyst"

    main_risk = "No major flags detected"
    if flags:
        main_risk = {
            "going_concern":       "Bankruptcy risk — going concern in filing",
            "shelf_registration":  "Dilution risk — shelf registration active",
            "atm_offering":        "Dilution risk — ATM offering in progress",
            "reverse_split_risk":  "Reverse split risk — proxy filing detected",
            "high_short_interest": f"High short interest — {fpct(r.get('short_percent_float'))} of float",
            "pump_signal":         "Volume spike without confirmed catalyst",
            "extreme_volatility":  f"Extreme volatility — {r.get('volatility',0):.0f}% annualized",
            "low_liquidity":       "Low average volume — liquidity risk",
            "earnings_imminent":   f"Earnings in {r.get('days_to_earnings','?')} days — binary event",
        }.get(flags[0], flags[0].replace("_"," ").title())

    exp_label = f"{ticker}  ·  {signal}  ·  {fp(price)}  ·  {rarrow}{abs(ret1d):.1f}%  ·  {score:.0f}/100"

    with st.expander(exp_label, expanded=False):
        # ── Header ──────────────────────────────────────────────────────────
        col_id, col_score = st.columns([5,1])
        with col_id:
            st.markdown(f"""
            <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;padding:4px 0 12px;">
                <span style="font-family:'Bebas Neue',sans-serif;font-size:2em;color:#0f172a;letter-spacing:0.08em;">{ticker}</span>
                <span style="color:#475569;font-size:0.88em;font-weight:400;">{name[:42]}</span>
                <span class="pill {pc}">{signal}</span>
                {flag_chips(flags)}
            </div>
            """, unsafe_allow_html=True)
        with col_score:
            st.markdown(score_ring_svg(score, sc, 72), unsafe_allow_html=True)

        # Score bar
        st.markdown(sbar(score, sc), unsafe_allow_html=True)
        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

        # ── Metrics row ──────────────────────────────────────────────────────
        c1,c2,c3,c4,c5,c6 = st.columns(6)
        c1.metric("Price",   fp(price))
        c2.metric("Mkt Cap", fm(mc))
        c3.metric("RVOL",    f"{rvol:.2f}×")
        c4.metric("1D",      f"{ret1d:+.2f}%")
        c5.metric("5D",      f"{ret5d:+.2f}%")
        c6.metric("RSI",     f"{rsi:.1f}" if rsi else "—")

        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

        # ── Breakdown + Zones ────────────────────────────────────────────────
        bl, br = st.columns(2)
        with bl:
            st.markdown('<div class="sh">Score Breakdown</div>', unsafe_allow_html=True)
            comps = [
                ("Technical",   r.get("technical_score",0),   SCORING_WEIGHTS["technical"]),
                ("Catalyst",    r.get("catalyst_score",0),    SCORING_WEIGHTS["catalyst"]),
                ("Fundamental", r.get("fundamental_score",0), SCORING_WEIGHTS["fundamental"]),
                ("Risk inv.",   100-r.get("risk_score",50),   SCORING_WEIGHTS["risk"]),
                ("Sentiment",   r.get("sentiment_score",50),  SCORING_WEIGHTS["sentiment"]),
            ]
            for lbl, val, wt in comps:
                c = score_col(val)
                st.markdown(f"""
                <div style="margin-bottom:8px;">
                  <div style="display:flex;justify-content:space-between;margin-bottom:3px;">
                    <span style="font-size:0.7em;color:#64748b;font-family:'JetBrains Mono',monospace;">{lbl}</span>
                    <span style="font-size:0.7em;color:{c};font-family:'JetBrains Mono',monospace;font-weight:600;">{val:.0f}<span style="color:#94a3b8;"> ×{wt:.0%}</span></span>
                  </div>
                  {sbar(val, c)}
                </div>""", unsafe_allow_html=True)

        with br:
            st.markdown('<div class="sh">Trade Zones</div>', unsafe_allow_html=True)
            entry = r.get("entry_zone"); stop_ = r.get("stop_loss")
            t1 = r.get("target_1"); t2 = r.get("target_2")
            sp = r.get("short_percent_float"); av = r.get("avg_volume",0)
            st.markdown(
                stat_row("Entry Zone",   fp(entry),             "b") +
                stat_row("Stop Loss",    fp(stop_),             "r") +
                stat_row("Target 1",     fp(t1),                "g") +
                stat_row("Target 2",     fp(t2),                "g") +
                stat_row("Short %Float", fpct(sp) if sp else "—","a") +
                stat_row("Avg Volume",   f"{av:,}" if av else "—",""),
                unsafe_allow_html=True)

        # ── Catalyst / Risk ──────────────────────────────────────────────────
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        ca, ri = st.columns(2)
        with ca:
            st.markdown('<div class="sh">Main Catalyst</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="box box-green" style="min-height:52px;">{cat_str}</div>', unsafe_allow_html=True)
        with ri:
            st.markdown('<div class="sh">Main Risk</div>', unsafe_allow_html=True)
            box_cls = "box-red" if flags and flags[0] in {"going_concern","reverse_split_risk"} else "box-amber"
            st.markdown(f'<div class="box {box_cls}" style="min-height:52px;">{main_risk}</div>', unsafe_allow_html=True)

        # ── Analysis ─────────────────────────────────────────────────────────
        summary = r.get("summary","")
        if summary:
            st.markdown('<div class="sh">Analysis</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="summary-block">{summary}</div>', unsafe_allow_html=True)

        # ── News ─────────────────────────────────────────────────────────────
        headlines = r.get("recent_headlines",[])
        if headlines:
            st.markdown('<div class="sh">Recent News</div>', unsafe_allow_html=True)
            for h in headlines[:4]:
                sent = h.get("sentiment","neutral")
                dc   = {"positive":"#16a34a","negative":"#dc2626"}.get(sent,"#475569")
                st.markdown(f"""
                <div class="news-item">
                  <div class="news-dot" style="background:{dc};"></div>
                  <div>
                    <a href="{h.get('url','#')}" target="_blank" style="color:#1d6fa5;font-size:0.82em;text-decoration:none;line-height:1.4;">{h.get('title','')}</a>
                    <div style="color:#94a3b8;font-size:0.68em;margin-top:2px;font-family:'JetBrains Mono',monospace;">{h.get('date','')} · {sent.upper()}</div>
                  </div>
                </div>""", unsafe_allow_html=True)

        # ── SEC Filings ───────────────────────────────────────────────────────
        filings = r.get("filing_summary",[])
        if filings:
            st.markdown('<div class="sh">SEC Filings</div>', unsafe_allow_html=True)
            for f in filings[:3]:
                st.markdown(f'<div style="color:#64748b;font-size:0.75em;padding:4px 0;font-family:\'JetBrains Mono\',monospace;border-bottom:1px solid rgba(0,0,0,0.05);">{f}</div>', unsafe_allow_html=True)

        # Sources
        sources = r.get("data_sources",[])
        st.markdown(f'<div style="margin-top:10px;color:#cbd5e1;font-size:0.65em;font-family:\'JetBrains Mono\',monospace;">SOURCES: {" · ".join(sources)}</div>', unsafe_allow_html=True)
        st.markdown('<div class="disc">RESEARCH TOOL ONLY — NOT FINANCIAL ADVICE</div>', unsafe_allow_html=True)


def render_holding_card(h):
    ticker  = h["ticker"]
    pnl_pct = h["unrealized_pnl_pct"]
    pnl     = h["unrealized_pnl"]
    rec     = h["recommendation"]
    score   = h.get("final_score", 50)
    flags   = h.get("active_flags", [])
    parrow  = "▲" if pnl >= 0 else "▼"
    pcolor  = "#16a34a" if pnl >= 0 else "#dc2626"
    rcol    = {"Sell":"#dc2626","Trim":"#ea580c","Hold":"#c2610f","Add (if confirmed)":"#16a34a"}.get(rec,"#475569")
    sc      = score_col(score)
    exp_label = f"{ticker}  ·  {rec}  ·  {parrow}{abs(pnl_pct):.1f}%  ·  ${h['position_value']:,.0f}"

    with st.expander(exp_label, expanded=False):
        st.markdown(f"""
        <div class="port-header">
          <div style="display:flex;align-items:center;gap:14px;">
            <span style="font-family:'Bebas Neue',sans-serif;font-size:1.8em;color:#0f172a;letter-spacing:0.08em;">{ticker}</span>
            <span style="font-family:'Space Grotesk',sans-serif;font-size:0.95em;font-weight:700;color:{rcol};letter-spacing:0.05em;text-transform:uppercase;">{rec}</span>
          </div>
          <div style="text-align:right;">
            <div style="font-family:'Bebas Neue',sans-serif;font-size:1.6em;color:{pcolor};letter-spacing:0.05em;">{parrow} {abs(pnl_pct):.2f}%</div>
            <div style="color:#64748b;font-size:0.72em;font-family:'JetBrains Mono',monospace;">${pnl:+,.2f} unrealized</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        c1,c2,c3,c4,c5,c6 = st.columns(6)
        c1.metric("Price Now",  fp(h["current_price"]))
        c2.metric("Avg Cost",   fp(h["avg_cost"]))
        c3.metric("Shares",     f"{h['shares']:,.0f}")
        c4.metric("Position",   f"${h['position_value']:,.0f}")
        c5.metric("Score",      f"{score:.0f}/100")
        c6.metric("Stop",       fp(h["suggested_stop"]))

        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        st.markdown('<div class="sh">Recommendation Reasoning</div>', unsafe_allow_html=True)
        for part in (h.get("reasoning","") or "").split(" | "):
            if part.strip():
                st.markdown(f'<div style="color:#475569;font-size:0.82em;padding:3px 0;line-height:1.6;">{part}</div>', unsafe_allow_html=True)

        if flags:
            st.markdown("<div style='margin-top:8px;'>" + flag_chips(flags) + "</div>", unsafe_allow_html=True)


def _build_live_chart(hist, chart_type, tf, r):
    """Build Plotly figure with price + volume subplots."""
    if hist.empty:
        return None

    entry = r.get("entry_zone")
    stop_ = r.get("stop_loss")
    t1    = r.get("target_1")
    t2    = r.get("target_2")

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.78, 0.22], vertical_spacing=0.02,
    )

    # ── Price trace ──────────────────────────────────────────────────────────
    if chart_type == "Candle":
        fig.add_trace(go.Candlestick(
            x=hist.index, open=hist["Open"], high=hist["High"],
            low=hist["Low"], close=hist["Close"], name="Price",
            increasing_line_color="#16a34a", increasing_fillcolor="rgba(22,163,74,0.08)",
            decreasing_line_color="#dc2626", decreasing_fillcolor="rgba(220,38,38,0.08)",
            line=dict(width=1),
        ), row=1, col=1)
    else:
        close_vals = hist["Close"]
        area_color = "#c2610f"
        fig.add_trace(go.Scatter(
            x=hist.index, y=close_vals, name="Price",
            line=dict(color=area_color, width=2),
            fill="tozeroy",
            fillcolor="rgba(194,97,15,0.06)",
        ), row=1, col=1)

    # ── Overlays ─────────────────────────────────────────────────────────────
    if tf == "1D" and "Volume" in hist.columns:
        # VWAP
        tp = (hist["High"] + hist["Low"] + hist["Close"]) / 3
        cum_tpv = (tp * hist["Volume"]).cumsum()
        cum_vol = hist["Volume"].cumsum()
        vwap = cum_tpv / cum_vol.replace(0, float("nan"))
        fig.add_trace(go.Scatter(
            x=hist.index, y=vwap, name="VWAP",
            line=dict(color="#c2610f", width=1.5, dash="dot"),
            opacity=0.85,
        ), row=1, col=1)
    elif len(hist) >= 20:
        # SMA20
        sma20 = hist["Close"].rolling(20).mean()
        fig.add_trace(go.Scatter(
            x=hist.index, y=sma20, name="SMA20",
            line=dict(color="#2563eb", width=1, dash="dot"),
            opacity=0.6,
        ), row=1, col=1)
    if len(hist) >= 9:
        ema9 = hist["Close"].ewm(span=9).mean()
        fig.add_trace(go.Scatter(
            x=hist.index, y=ema9, name="EMA9",
            line=dict(color="#6d28d9", width=1),
            opacity=0.5,
        ), row=1, col=1)

    # ── Zones ────────────────────────────────────────────────────────────────
    if entry:
        fig.add_hline(y=entry, line_color="rgba(29,111,165,0.5)", line_dash="dash",
                      annotation_text="Entry", annotation_font_color="#1d6fa5",
                      annotation_font_size=10, row=1, col=1)
    if stop_:
        fig.add_hline(y=stop_, line_color="rgba(220,38,38,0.45)", line_dash="dash",
                      annotation_text="Stop", annotation_font_color="#dc2626",
                      annotation_font_size=10, row=1, col=1)
    if t1:
        fig.add_hline(y=t1, line_color="rgba(22,163,74,0.35)", line_dash="dot",
                      annotation_text="T1", annotation_font_color="#16a34a",
                      annotation_font_size=10, row=1, col=1)
    if t2:
        fig.add_hline(y=t2, line_color="rgba(22,163,74,0.35)", line_dash="dot",
                      annotation_text="T2", annotation_font_color="#16a34a",
                      annotation_font_size=10, row=1, col=1)

    # ── Volume bars ──────────────────────────────────────────────────────────
    if "Volume" in hist.columns:
        vol_colors = [
            "rgba(22,163,74,0.3)" if c >= o else "rgba(220,38,38,0.3)"
            for c, o in zip(hist["Close"], hist["Open"])
        ]
        fig.add_trace(go.Bar(
            x=hist.index, y=hist["Volume"],
            marker_color=vol_colors, name="Vol", showlegend=False,
        ), row=2, col=1)

    # ── Layout ───────────────────────────────────────────────────────────────
    axis_style = dict(
        gridcolor="rgba(0,0,0,0.05)", showgrid=True, zeroline=False,
        showline=False, tickfont=dict(size=9, family="JetBrains Mono", color="#94a3b8"),
    )
    fig.update_layout(
        height=420,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(248,249,252,0.98)",
        font=dict(family="Inter", color="#94a3b8"),
        xaxis=dict(**axis_style, rangeslider_visible=False),
        xaxis2=dict(**axis_style, rangeslider_visible=False),
        yaxis=dict(**axis_style, side="right"),
        yaxis2=dict(**axis_style, side="right"),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(color="#94a3b8", size=9, family="JetBrains Mono"),
            orientation="h", y=1.02, x=0,
        ),
        margin=dict(l=0, r=0, t=4, b=0),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="#ffffff", font_color="#0f172a",
                        font_family="JetBrains Mono", font_size=11),
    )
    if tf == "1D":
        fig.update_xaxes(
            rangebreaks=[dict(bounds=["sat","mon"]), dict(bounds=[20,4], pattern="hour")],
        )
    return fig


def render_deep_dive(r):
    if r.get("error"):
        st.error(f"Could not analyze {r.get('ticker')}: {r['error']}")
        return
    if r.get("filtered_out"):
        st.warning(f"{r['ticker']} filtered out: {r.get('filter_reason')}")
        return

    import html as _html
    ticker  = r["ticker"]
    name    = _html.escape(r.get("company_name", ticker))
    price   = r.get("price", 0)
    signal  = r.get("signal", "—")
    score   = r.get("final_score", 0)
    flags   = r.get("risk_flags", [])
    sc      = score_col(score)
    pc      = pill_class(signal)
    ret1d   = r.get("return_1d", 0) or 0
    ret_c   = "#16a34a" if ret1d >= 0 else "#dc2626"
    ret_arr = "▲" if ret1d >= 0 else "▼"
    fetched = r.get("data_fetched_at", "")
    fetched_t = fetched[11:16] if fetched else "—"

    # ── Hero bar ─────────────────────────────────────────────────────────────
    h1, h2 = st.columns([6, 1])
    with h1:
        st.markdown(f"""
        <div style="padding:18px 0 14px;border-bottom:1px solid var(--border);margin-bottom:18px;animation:fadeIn 0.3s ease;">
          <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:10px;">
            <span style="font-size:2.4em;font-weight:700;color:var(--t1);letter-spacing:-0.03em;line-height:1;">{ticker}</span>
            <div style="display:flex;flex-direction:column;gap:4px;">
              <span style="color:var(--t3);font-size:0.82em;font-weight:400;">{name[:52]}</span>
              <div style="display:flex;align-items:center;gap:6px;">
                <span class="pill {pc}">{signal}</span>
                {flag_chips(flags)}
              </div>
            </div>
            <div class="live-badge" style="margin-left:auto;">
              <span class="live-dot"></span>
              <span>Updated {fetched_t}</span>
            </div>
          </div>
          <div style="display:flex;align-items:baseline;gap:16px;flex-wrap:wrap;">
            <span style="font-size:2.2em;font-weight:700;color:var(--t1);letter-spacing:-0.02em;font-family:'JetBrains Mono',monospace;">{fp(price)}</span>
            <span style="font-size:1em;font-weight:600;color:{ret_c};font-family:'JetBrains Mono',monospace;">{ret_arr} {abs(ret1d):.2f}%</span>
            <span style="font-size:0.85em;color:{sc};font-family:'JetBrains Mono',monospace;font-weight:500;">{score:.0f}/100</span>
          </div>
        </div>
        """, unsafe_allow_html=True)
    with h2:
        st.markdown(score_ring_svg(score, sc, 88), unsafe_allow_html=True)

    # ── Chart controls ────────────────────────────────────────────────────────
    cc1, cc2, cc3 = st.columns([3, 2, 1])
    with cc1:
        tf = st.radio("Timeframe", ["1D", "5D", "1M", "3M"],
                      horizontal=True, label_visibility="collapsed",
                      key=f"dd_tf_{ticker}")
    with cc2:
        ctype = st.radio("Chart type", ["Candle", "Line"],
                         horizontal=True, label_visibility="collapsed",
                         key=f"dd_ct_{ticker}")
    with cc3:
        do_refresh = st.button("Refresh", key=f"dd_ref_{ticker}", use_container_width=True)

    # ── Fetch & render chart ──────────────────────────────────────────────────
    chart_key = f"chart_{ticker}_{tf}"
    if chart_key not in st.session_state or do_refresh:
        with st.spinner("Loading chart data…"):
            st.session_state[chart_key] = get_chart_data(ticker, tf)
    hist = st.session_state[chart_key]

    fig = _build_live_chart(hist, ctype, tf, r)
    if fig:
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        rows_shown = len(hist)
        lbl = "candles" if ctype == "Candle" else "points"
        st.markdown(
            f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.62em;'
            f'color:var(--t3);text-align:right;margin-top:-8px;">'
            f'{tf} · {rows_shown} {lbl}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info(f"No {tf} chart data available — market may be closed or API limit reached.")

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # ── Three-column stats ────────────────────────────────────────────────────
    ca, cb, cc = st.columns(3)

    with ca:
        st.markdown('<div class="sh">Price & Volume</div>', unsafe_allow_html=True)
        rvol = r.get("relative_volume", 0) or 0
        rsi  = r.get("rsi")
        vl   = r.get("volatility")
        mc   = r.get("market_cap", 0)
        st.markdown(
            stat_row("Market Cap",    fm(mc)) +
            stat_row("RVOL",          f"{rvol:.2f}×", "b" if rvol > 2 else "") +
            stat_row("RSI (14)",      f"{rsi:.1f}" if rsi else "—") +
            stat_row("1D / 5D / 20D", f"{r.get('return_1d',0):+.1f}% / {r.get('return_5d',0):+.1f}% / {r.get('return_20d',0):+.1f}%") +
            stat_row("Volatility",    f"{vl:.0f}% ann." if vl else "—", "a" if vl and vl > 100 else "") +
            stat_row("Short % Float", fpct(r.get("short_percent_float")), "a" if (r.get("short_percent_float") or 0) > 0.2 else "") +
            stat_row("Avg Volume",    f"{r.get('avg_volume',0):,}" if r.get("avg_volume") else "—"),
            unsafe_allow_html=True,
        )

    with cb:
        st.markdown('<div class="sh">Trade Zones</div>', unsafe_allow_html=True)
        rw = r.get("runway_months"); rv = r.get("revenue_growth")
        st.markdown(
            stat_row("Entry Zone",   fp(r.get("entry_zone")), "b") +
            stat_row("Stop Loss",    fp(r.get("stop_loss")), "r") +
            stat_row("Target 1",     fp(r.get("target_1")), "g") +
            stat_row("Target 2",     fp(r.get("target_2")), "g") +
            stat_row("Rev Growth",   fpct(rv) if rv else "—", "g" if rv and rv > 0.2 else "r" if rv and rv < 0 else "") +
            stat_row("Cash Runway",  f"~{rw:.0f} mo" if rw else "—", "r" if rw and rw < 12 else "g" if rw and rw > 24 else "") +
            stat_row("Analyst",      (r.get("analyst_recommendation") or "—").upper(), "g" if r.get("analyst_recommendation") in ("buy","strong_buy") else ""),
            unsafe_allow_html=True,
        )
        ed = r.get("earnings_date"); dte = r.get("days_to_earnings")
        if ed:
            earn_str = f"{ed} ({dte}d)" if dte is not None else ed
            st.markdown(stat_row("Earnings", earn_str, "r" if r.get("earnings_warning") else "p"), unsafe_allow_html=True)
            if r.get("earnings_warning"):
                st.markdown(f'<div class="earn-warn">Earnings in {dte} days — binary event risk</div>', unsafe_allow_html=True)

    with cc:
        st.markdown('<div class="sh">Score Breakdown</div>', unsafe_allow_html=True)
        comps = [
            ("Technical",   r.get("technical_score",0),   SCORING_WEIGHTS["technical"]),
            ("Catalyst",    r.get("catalyst_score",0),    SCORING_WEIGHTS["catalyst"]),
            ("Fundamental", r.get("fundamental_score",0), SCORING_WEIGHTS["fundamental"]),
            ("Risk adj.",   100-r.get("risk_score",50),   SCORING_WEIGHTS["risk"]),
            ("Sentiment",   r.get("sentiment_score",50),  SCORING_WEIGHTS["sentiment"]),
        ]
        for lbl, val, wt in comps:
            c = score_col(val)
            st.markdown(f"""
            <div style="margin-bottom:10px;">
              <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
                <span style="font-size:0.7em;color:var(--t3);font-family:'Inter',sans-serif;">{lbl}</span>
                <span style="font-size:0.7em;color:{c};font-family:'JetBrains Mono',monospace;font-weight:600;">{val:.0f}<span style="color:var(--t3);font-weight:400;"> ×{wt:.0%}</span></span>
              </div>
              {sbar(val, c)}
            </div>""", unsafe_allow_html=True)

        rs = r.get("sector_rs_label")
        if rs:
            is_out   = "outperform" in rs.lower()
            is_under = "underperform" in rs.lower()
            rs_color = "#16a34a" if is_out else "#dc2626" if is_under else "#c2610f"
            rs_bg    = "0.06" if is_out or is_under else "0.05"
            st.markdown(f"""
            <div style="margin-top:10px;padding:7px 10px;
                        background:rgba({
                            '22,163,74' if is_out else '220,38,38' if is_under else '194,97,15'
                        },{rs_bg});
                        border:1px solid {rs_color}30;border-radius:5px;
                        color:{rs_color};font-size:0.68em;font-family:'JetBrains Mono',monospace;">
              RS: {rs}
            </div>""", unsafe_allow_html=True)

    # ── Analysis block ────────────────────────────────────────────────────────
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    summary = r.get("summary", "")
    if summary:
        st.markdown('<div class="sh">Analysis</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="summary-block">{summary}</div>', unsafe_allow_html=True)

    # ── Catalysts + SEC ───────────────────────────────────────────────────────
    cat_notes = r.get("catalyst_notes", []); fsumm = r.get("filing_summary", [])
    if cat_notes or fsumm:
        st.markdown('<div class="sh">Catalysts & Filings</div>', unsafe_allow_html=True)
        for n in cat_notes:
            st.markdown(f'<div style="color:#16a34a;font-size:0.82em;padding:4px 0;display:flex;align-items:center;gap:8px;"><span>+</span>{n}</div>', unsafe_allow_html=True)
        for f in fsumm:
            st.markdown(f'<div style="color:var(--t3);font-size:0.76em;padding:4px 0;font-family:\'JetBrains Mono\',monospace;border-bottom:1px solid rgba(0,0,0,0.05);">{f}</div>', unsafe_allow_html=True)

    # ── AI Filing Summary ─────────────────────────────────────────────────────
    ai_sum = r.get("filing_ai_summary", "")
    if ai_sum:
        st.markdown('<div class="sh">AI Filing Analysis</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="box box-blue">{ai_sum}</div>', unsafe_allow_html=True)

    # ── Risk flags ────────────────────────────────────────────────────────────
    if flags:
        st.markdown('<div class="sh">Risk Flags</div>', unsafe_allow_html=True)
        for flag in flags:
            crit  = flag in {"going_concern","reverse_split_risk"}
            earn  = flag == "earnings_imminent"
            color = "#dc2626" if crit else "#6d28d9" if earn else "#c2610f"
            st.markdown(f'<div style="color:{color};font-size:0.82em;padding:4px 0;display:flex;align-items:center;gap:6px;">{RISK_FLAGS.get(flag,flag)}</div>', unsafe_allow_html=True)

    # ── News ──────────────────────────────────────────────────────────────────
    headlines = r.get("recent_headlines", [])
    if headlines:
        st.markdown('<div class="sh">News</div>', unsafe_allow_html=True)
        for h in headlines:
            sent = h.get("sentiment", "neutral")
            dc   = {"positive":"#16a34a","negative":"#dc2626"}.get(sent,"#94a3b8")
            st.markdown(f"""
            <div class="news-item">
              <div class="news-dot" style="background:{dc};"></div>
              <div>
                <a href="{h.get('url','#')}" target="_blank"
                   style="color:#1d6fa5;font-size:0.82em;text-decoration:none;line-height:1.5;">{h.get('title','')}</a>
                <div style="color:var(--t3);font-size:0.67em;margin-top:2px;font-family:'JetBrains Mono',monospace;">
                  {h.get('date','')} · {sent.upper()}
                </div>
              </div>
            </div>""", unsafe_allow_html=True)

    st.markdown('<div class="disc" style="margin-top:20px;">Research tool only · Not financial advice · Verify all data independently</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# INIT
# ══════════════════════════════════════════════════════════════════════════════
initialize_db()


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div class="axiom-logo">
      <div class="name">Axiom</div>
      <div class="sub">TERMINAL</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown('<div class="disc">Research tool · Not financial advice</div>', unsafe_allow_html=True)

    # ── Live Holdings Manager ─────────────────────────────────────────────────
    st.markdown('<div style="padding:0 4px;"><div class="sh" style="margin-top:14px;">Holdings</div></div>', unsafe_allow_html=True)

    _pf = get_portfolio()
    if _pf.empty:
        st.markdown('<div style="color:#94a3b8;font-size:0.68em;font-family:\'JetBrains Mono\',monospace;padding:4px 4px 8px;">No holdings saved yet.</div>', unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="display:grid;grid-template-columns:58px 60px 65px 28px;
                    padding:3px 10px;gap:4px;font-family:'JetBrains Mono',monospace;
                    font-size:0.58em;letter-spacing:0.1em;color:var(--t3);
                    text-transform:uppercase;border-bottom:1px solid var(--border);">
          <span>Ticker</span><span style="text-align:right">Shares</span>
          <span style="text-align:right">Cost</span><span></span>
        </div>""", unsafe_allow_html=True)
        for _, row in _pf.iterrows():
            hc1, hc2, hc3, hc4 = st.columns([2.2, 2, 2.2, 0.9])
            hc1.markdown(f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.8em;color:#1d6fa5;font-weight:600;">{row["ticker"]}</span>', unsafe_allow_html=True)
            hc2.markdown(f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.75em;color:#475569;">{row["shares"]:,.0f}</span>', unsafe_allow_html=True)
            hc3.markdown(f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.75em;color:#64748b;">${row["avg_cost"]:.2f}</span>', unsafe_allow_html=True)
            if hc4.button("x", key=f"del_{row['ticker']}", help=f"Remove {row['ticker']}"):
                delete_holding(row["ticker"])
                st.rerun()

    # Add new holding form
    st.markdown('<div style="margin-top:8px;padding:0 2px;"><div style="font-size:0.6em;color:var(--t3);font-family:\'JetBrains Mono\',monospace;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:4px;">Add Holding</div></div>', unsafe_allow_html=True)
    with st.form("add_holding", clear_on_submit=True):
        fa, fb, fc = st.columns([2, 1.8, 1.8])
        new_tkr  = fa.text_input("Ticker", placeholder="SOUN",  label_visibility="collapsed")
        new_sh   = fb.text_input("Shares", placeholder="100",   label_visibility="collapsed")
        new_cost = fc.text_input("Cost",   placeholder="4.50",  label_visibility="collapsed")
        fa.markdown('<div style="font-size:0.58em;color:var(--t3);font-family:\'JetBrains Mono\',monospace;">TICKER</div>', unsafe_allow_html=True)
        fb.markdown('<div style="font-size:0.58em;color:var(--t3);font-family:\'JetBrains Mono\',monospace;">SHARES</div>', unsafe_allow_html=True)
        fc.markdown('<div style="font-size:0.58em;color:var(--t3);font-family:\'JetBrains Mono\',monospace;">AVG COST</div>', unsafe_allow_html=True)
        if st.form_submit_button("Add Holding", use_container_width=True):
            try:
                if new_tkr and new_sh and new_cost:
                    upsert_holding(new_tkr.strip().upper(), float(new_sh), float(new_cost))
                    st.rerun()
                else:
                    st.warning("Fill all three fields.")
            except ValueError:
                st.error("Shares and cost must be numbers.")

    st.markdown('<div style="padding:0 4px;"><div class="sh" style="margin-top:12px;">Scanner</div></div>', unsafe_allow_html=True)
    custom_tickers = st.text_input("t", placeholder="SOUN, BBAI, RGTI, IONQ",
        label_visibility="collapsed")
    use_default   = st.checkbox("Default universe", value=True)
    use_portfolio = st.checkbox("My portfolio", value=True)

    if st.button("Run Scan", type="primary", use_container_width=True):
        tickers = []
        if custom_tickers:
            tickers += [t.strip().upper() for t in custom_tickers.split(",") if t.strip()]
        if use_default:
            tickers += DEFAULT_UNIVERSE
        if use_portfolio:
            pf = get_portfolio()
            if not pf.empty:
                tickers += pf["ticker"].tolist()
        tickers = list(dict.fromkeys(tickers))
        if not tickers:
            st.warning("Add tickers first.")
        else:
            prog = st.progress(0, text="Starting...")
            results = []
            for i, t in enumerate(tickers):
                prog.progress((i+1)/len(tickers), text=f"Scanning {t}... {i+1}/{len(tickers)}")
                try:
                    res = scan_ticker(t)
                    if res: results.append(res)
                except Exception:
                    pass
            results.sort(key=lambda r: (not r.get("filtered_out",False), r.get("final_score",0)), reverse=True)
            st.session_state["scan_results"] = results
            st.session_state["scan_time"]    = datetime.now().strftime("%H:%M")
            prog.empty()
            valid_n = len([r for r in results if not r.get("filtered_out")])
            st.success(f"{valid_n} stocks analyzed")

    # Mini stats
    all_r = st.session_state.get("scan_results",[])
    if all_r:
        valid = [r for r in all_r if not r.get("filtered_out") and not r.get("error")]
        buys  = sum(1 for r in valid if r.get("signal") in ("Strong Buy Candidate","Speculative Buy"))
        sells = sum(1 for r in valid if r.get("signal") in ("Sell","Avoid"))
        watches = sum(1 for r in valid if r.get("signal") == "Watchlist")
        st.markdown(f"""
        <div style="margin-top:12px;display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;padding:0 2px;">
          <div class="sig-count">
            <div class="num" style="color:#16a34a;">{buys}</div>
            <div class="lbl">BUY</div>
          </div>
          <div class="sig-count">
            <div class="num" style="color:#c2610f;">{watches}</div>
            <div class="lbl">WATCH</div>
          </div>
          <div class="sig-count">
            <div class="num" style="color:#dc2626;">{sells}</div>
            <div class="lbl">SELL</div>
          </div>
        </div>
        <div style="color:#cbd5e1;font-size:0.6em;font-family:'JetBrains Mono',monospace;margin-top:6px;text-align:center;">
          LAST SCAN: {st.session_state.get('scan_time','—')} · {len(valid)} STOCKS
        </div>
        """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN TABS
# ══════════════════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Scanner", "Portfolio", "Deep Dive", "Predictions", "Live Alerts", "Info"])


# ── TAB 1: SCANNER ────────────────────────────────────────────────────────────
with tab1:
    results = st.session_state.get("scan_results",[])
    valid   = [r for r in results if not r.get("filtered_out") and not r.get("error")]

    if not valid:
        st.markdown("""
        <div class="empty">
          <div class="ico">--</div>
          <h3>RUN A SCAN TO SEE RESULTS</h3>
          <p>Enter tickers in the sidebar or use the default universe.<br>
          The scanner surfaces stocks worth researching — not stocks to blindly buy.</p>
        </div>""", unsafe_allow_html=True)
    else:
        # Signal summary
        sc_map = {}
        for r in valid:
            s = r.get("signal","—"); sc_map[s] = sc_map.get(s,0)+1

        labels = [("STRONG BUY","Strong Buy Candidate","#16a34a"),
                  ("SPEC BUY","Speculative Buy","#15803d"),
                  ("WATCHLIST","Watchlist","#c2610f"),
                  ("HOLD","Hold","#c2610f"),
                  ("TRIM","Trim","#ea580c"),
                  ("SELL","Sell","#dc2626"),
                  ("AVOID","Avoid","#991b1b")]

        cols = st.columns(7)
        for col,(short,full,color) in zip(cols,labels):
            cnt = sc_map.get(full,0)
            col.markdown(f"""
            <div class="sig-count">
              <div class="num" style="color:{color};">{cnt}</div>
              <div class="lbl">{short}</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

        # Filters
        fc1,fc2,fc3 = st.columns([1,2,1])
        with fc1: min_score = st.slider("Min Score",0,100,0)
        with fc2: sig_filter = st.multiselect("Signal",
            ["Strong Buy Candidate","Speculative Buy","Watchlist","Hold","Trim","Sell","Avoid"],
            default=[],placeholder="All signals")
        with fc3: hide_crit = st.checkbox("Hide critical flags",False)

        filtered = [r for r in valid
            if r.get("final_score",0) >= min_score
            and (not sig_filter or r.get("signal") in sig_filter)
            and (not hide_crit or not has_crit(r.get("risk_flags",[])))]

        st.markdown(f'<div style="color:#cbd5e1;font-size:0.68em;font-family:\'JetBrains Mono\',monospace;margin-bottom:6px;letter-spacing:0.08em;">SHOWING {len(filtered)} OF {len(valid)} · SORTED BY SCORE</div>', unsafe_allow_html=True)
        st.markdown('<div class="result-hdr"><span>TICKER</span><span>COMPANY</span><span>SIGNAL</span><span>PRICE</span><span>1D</span><span>SCORE</span></div>', unsafe_allow_html=True)

        for r in filtered:
            render_result_card(r)

        excluded = [r for r in results if r.get("filtered_out")]
        if excluded:
            with st.expander(f"{len(excluded)} tickers excluded"):
                for r in excluded:
                    st.markdown(f'<span style="font-family:\'JetBrains Mono\',monospace;color:#cbd5e1;font-size:0.75em;">{r["ticker"]} — {r.get("filter_reason","")}</span>', unsafe_allow_html=True)


# ── TAB 2: PORTFOLIO ──────────────────────────────────────────────────────────
with tab2:
    portfolio_df = get_portfolio()

    if portfolio_df.empty:
        st.markdown("""
        <div class="empty">
          <div class="ico">--</div>
          <h3>NO HOLDINGS SAVED</h3>
          <p>Enter positions in the sidebar: TICKER, SHARES, AVG_COST</p>
        </div>""", unsafe_allow_html=True)
    else:
        scan_map = {r["ticker"]:r for r in st.session_state.get("scan_results",[])
                    if not r.get("filtered_out") and not r.get("error")}

        with st.spinner("Fetching prices..."):
            holdings_analysis = []
            for _, row in portfolio_df.iterrows():
                ticker = row["ticker"]; shares = row["shares"]; avg_cost = row["avg_cost"]
                if ticker in scan_map:
                    sr = scan_map[ticker]
                    current_price = sr.get("price", avg_cost)
                    final_score   = sr.get("final_score", 50)
                    active_flags  = sr.get("risk_flags", [])
                    technicals    = {"rsi_14": sr.get("rsi"), "macd_bullish": sr.get("macd_bullish")}
                    fundamentals  = {"runway_months": sr.get("runway_months")}
                else:
                    snap          = fetch_ticker_snapshot(ticker)
                    current_price = snap.get("price", avg_cost) if snap else avg_cost
                    final_score   = 50; active_flags = []; technicals = {}; fundamentals = {}

                analysis = analyze_holding(ticker=ticker, shares=shares, avg_cost=avg_cost,
                    current_price=current_price, final_score=final_score,
                    active_flags=active_flags, technicals=technicals, fundamentals=fundamentals)
                analysis["final_score"] = final_score
                holdings_analysis.append(analysis)

        summary   = compute_portfolio_summary(holdings_analysis)
        total_pnl = summary.get("total_pnl",0)
        pnl_color = "#16a34a" if total_pnl >= 0 else "#dc2626"

        # Summary metrics
        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Total Value",    f"${summary.get('total_value',0):,.2f}")
        c2.metric("Cost Basis",     f"${summary.get('total_cost',0):,.2f}")
        c3.metric("Unrealized P&L", f"${total_pnl:+,.2f}", f"{summary.get('total_pnl_pct',0):+.1f}%")
        c4.metric("Holdings",       summary.get("num_holdings",0))
        c5.metric("Action Needed",  f"{summary.get('sell_count',0)+summary.get('trim_count',0)} Sell/Trim")

        if summary.get("concentrated_risk"):
            st.warning(f"Concentration risk: **{', '.join(summary['concentrated_risk'])}** exceed 25% of portfolio.")

        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

        # ── Summary table ─────────────────────────────────────────────────────
        tbl_rows = []
        for h in holdings_analysis:
            tbl_rows.append({
                "Ticker":   h["ticker"],
                "Shares":   h["shares"],
                "Avg Cost": h["avg_cost"],
                "Price":    h["current_price"],
                "P&L %":    round(h["unrealized_pnl_pct"], 2),
                "P&L $":    round(h["unrealized_pnl"], 2),
                "Value":    round(h["position_value"], 2),
                "Score":    h.get("final_score", 50),
                "Signal":   h.get("recommendation", "—"),
            })
        tbl_df = pd.DataFrame(tbl_rows).sort_values("P&L %", ascending=False)
        st.dataframe(
            tbl_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Ticker":   st.column_config.TextColumn("Ticker", width=70),
                "Shares":   st.column_config.NumberColumn("Shares", format="%.0f", width=70),
                "Avg Cost": st.column_config.NumberColumn("Avg Cost", format="$%.2f", width=85),
                "Price":    st.column_config.NumberColumn("Price", format="$%.4f", width=85),
                "P&L %":    st.column_config.NumberColumn("P&L %", format="%.2f%%", width=80),
                "P&L $":    st.column_config.NumberColumn("P&L $", format="$%.2f", width=90),
                "Value":    st.column_config.NumberColumn("Value", format="$%.2f", width=90),
                "Score":    st.column_config.ProgressColumn("Score", min_value=0, max_value=100, width=90),
                "Signal":   st.column_config.TextColumn("Signal", width=140),
            },
        )

        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        with st.expander("Holding Details", expanded=False):
          for h in sorted(holdings_analysis, key=lambda x: x["unrealized_pnl_pct"]):
            render_holding_card(h)

        if len(holdings_analysis) > 1:
            st.markdown('<div class="sh" style="margin-top:20px;">Allocation</div>', unsafe_allow_html=True)
            fig_pie = go.Figure(go.Pie(
                values=[h["position_value"] for h in holdings_analysis],
                labels=[h["ticker"] for h in holdings_analysis],
                hole=0.55, textinfo="label+percent",
                textfont=dict(family="JetBrains Mono",size=10,color="#0f172a"),
                marker=dict(
                    colors=["#16a34a","#1d6fa5","#c2610f","#dc2626","#6d28d9","#0891b2","#ea580c"],
                    line=dict(color="#f7f8fb",width=3)
                ),
            ))
            fig_pie.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", font_color="#334155",
                legend=dict(font=dict(family="JetBrains Mono",color="#334155",size=10)),
                height=260, margin=dict(l=0,r=0,t=0,b=0))
            st.plotly_chart(fig_pie, use_container_width=True)


# ── TAB 3: DEEP DIVE ──────────────────────────────────────────────────────────
with tab3:
    di, db = st.columns([4,1])
    with di:
        dive_ticker = st.text_input("dd", placeholder="Enter any ticker — e.g. SOUN, BBAI, CIFR, IONQ",
            label_visibility="collapsed").upper().strip()
    with db:
        run_dive = st.button("Analyze", type="primary", use_container_width=True)

    if run_dive and dive_ticker:
        with st.spinner(f"Running full analysis on {dive_ticker}..."):
            result = scan_ticker(dive_ticker, save=False)
            if result:
                st.session_state["dive_result"] = result
                st.session_state["dive_ticker"] = dive_ticker
            else:
                st.error(f"Could not fetch data for {dive_ticker}. Check the ticker and try again.")

    dive_res = st.session_state.get("dive_result")

    if dive_res:
        render_deep_dive(dive_res)
    elif not dive_ticker:
        st.markdown("""
        <div class="empty">
          <div class="ico">--</div>
          <h3>DEEP DIVE ANALYSIS</h3>
          <p>Type any ticker above for full technical, fundamental,<br>
          SEC filing, earnings calendar, sector RS, and AI analysis.</p>
        </div>""", unsafe_allow_html=True)


# ── TAB 4: PREDICTIONS ───────────────────────────────────────────────────────
with tab4:
    st.markdown("## Prediction Engine")
    st.markdown('<p style="color:#334155;font-size:0.82em;font-family:\'JetBrains Mono\',monospace;">The scanner scores each watchlist stock every 30 min and auto-logs paper trades. Score ≥65 → LONG, Score ≤30 → SHORT. Track if the predictions are actually good.</p>', unsafe_allow_html=True)

    try:
        from db.database import get_paper_trades
        pt_df = get_paper_trades(days=60)

        if pt_df.empty:
            st.markdown("""
            <div class="empty">
              <div class="ico">--</div>
              <h3>NO PREDICTIONS YET</h3>
              <p>The scanner auto-logs predictions every 30 minutes during market hours.<br>
              Make sure the background scanner is running on Railway.</p>
            </div>""", unsafe_allow_html=True)
        else:
            # Split prediction trades vs signal trades
            pred_mask   = pt_df["source_type"].isin(["prediction_buy", "prediction_sell"])
            pred_df     = pt_df[pred_mask].copy()
            signal_df   = pt_df[~pred_mask].copy()

            # ── Overall metrics ───────────────────────────────────────────────
            closed_pred = pred_df[pred_df["outcome"] != "open"]
            p_wins   = len(closed_pred[closed_pred["outcome"] == "win"])
            p_losses = len(closed_pred[closed_pred["outcome"] == "loss"])
            p_total  = p_wins + p_losses
            p_wr     = p_wins / p_total * 100 if p_total > 0 else 0
            p_avg    = closed_pred["pnl_pct"].mean() if not closed_pred.empty else 0

            closed_sig = signal_df[signal_df["outcome"] != "open"]
            s_wins   = len(closed_sig[closed_sig["outcome"] == "win"])
            s_losses = len(closed_sig[closed_sig["outcome"] == "loss"])
            s_total  = s_wins + s_losses
            s_wr     = s_wins / s_total * 100 if s_total > 0 else 0
            s_avg    = closed_sig["pnl_pct"].mean() if not closed_sig.empty else 0

            pm1, pm2, pm3, pm4, pm5, pm6 = st.columns(6)
            pm1.metric("Open Predictions", len(pred_df[pred_df["outcome"] == "open"]))
            pm2.metric("Prediction WR",    f"{p_wr:.0f}%" if p_total > 0 else "—",
                       delta=f"{p_wins}W {p_losses}L")
            pm3.metric("Pred Avg P&L",     f"{p_avg:+.1f}%" if p_total > 0 else "—")
            pm4.metric("Open Signals",     len(signal_df[signal_df["outcome"] == "open"]))
            pm5.metric("Signal WR",        f"{s_wr:.0f}%" if s_total > 0 else "—",
                       delta=f"{s_wins}W {s_losses}L")
            pm6.metric("Signal Avg P&L",   f"{s_avg:+.1f}%" if s_total > 0 else "—")

            st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

            # ── Win-rate by score band chart ──────────────────────────────────
            if not closed_pred.empty and "score_at_entry" in closed_pred.columns:
                scored = closed_pred.dropna(subset=["score_at_entry"])
                if not scored.empty:
                    scored = scored.copy()
                    scored["band"] = pd.cut(
                        scored["score_at_entry"],
                        bins=[0, 30, 45, 55, 65, 80, 100],
                        labels=["0-30\n(Short)", "30-45", "45-55", "55-65", "65-80\n(Long)", "80-100\n(Strong)"]
                    )
                    band_stats = scored.groupby("band", observed=True).apply(
                        lambda g: pd.Series({
                            "wr": (g["outcome"] == "win").mean() * 100,
                            "n":  len(g),
                            "avg_pnl": g["pnl_pct"].mean()
                        })
                    ).reset_index()

                    bar_colors = [
                        "#16a34a" if wr >= 60 else "#c2610f" if wr >= 45 else "#dc2626"
                        for wr in band_stats["wr"]
                    ]

                    fig_wr = go.Figure()
                    fig_wr.add_trace(go.Bar(
                        x=band_stats["band"].astype(str),
                        y=band_stats["wr"],
                        marker_color=bar_colors,
                        text=[f"{wr:.0f}%<br><span style='font-size:10px'>n={n}</span>"
                              for wr, n in zip(band_stats["wr"], band_stats["n"])],
                        textposition="inside",
                        textfont=dict(family="JetBrains Mono", size=11, color="#0f172a"),
                        name="Win Rate",
                    ))
                    fig_wr.add_hline(y=50, line_color="rgba(194,97,15,0.3)", line_dash="dot",
                                     annotation_text="50% breakeven",
                                     annotation_font_color="#c2610f", annotation_font_size=9)
                    fig_wr.update_layout(
                        title=dict(text="Prediction Win Rate by Score Band",
                                   font=dict(family="Bebas Neue", size=16, color="#475569"),
                                   x=0),
                        height=240,
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(248,249,252,0.98)",
                        font=dict(family="JetBrains Mono", color="#334155"),
                        xaxis=dict(gridcolor="rgba(0,0,0,0.05)", tickfont=dict(size=9)),
                        yaxis=dict(gridcolor="rgba(0,0,0,0.05)", range=[0, 105],
                                   ticksuffix="%", tickfont=dict(size=9)),
                        margin=dict(l=0, r=0, t=40, b=0),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_wr, use_container_width=True)

            # ── Trade blotters ────────────────────────────────────────────────
            col_p, col_s = st.columns([3, 2])

            def _trade_row_html(tr):
                outcome   = tr["outcome"]
                o_color   = {"win": "#16a34a", "loss": "#dc2626", "open": "#c2610f"}.get(outcome, "#475569")
                o_emoji   = {"win": "W", "loss": "L", "open": "open"}.get(outcome, "—")
                src       = tr.get("source_type", "signal")
                is_short  = src == "prediction_sell"
                dir_chip  = '<span class="dir-short">SHORT</span>' if is_short else '<span class="dir-long">LONG</span>'
                sig_map   = {"gap_up": "GAP UP", "vwap_reclaim": "VWAP RECLAIM",
                             "prediction_buy": "PRED BUY", "prediction_sell": "PRED SELL"}
                sig_label = sig_map.get(src, src.upper().replace("_", " "))
                score_str = f'{tr["score_at_entry"]:.0f}pt' if pd.notna(tr.get("score_at_entry")) else ""
                pnl_str   = f"{tr['pnl_pct']:+.1f}%" if pd.notna(tr.get("pnl_pct")) else "open"
                exit_str  = f"→ ${tr['exit_price']:.4f}" if pd.notna(tr.get("exit_price")) else "→ open"
                oc_class  = {"win": "trade-win", "loss": "trade-loss", "open": "trade-open"}.get(outcome, "")
                return f"""
                <div class="trade-row {oc_class}">
                  <span style="color:#1d6fa5;font-weight:600;min-width:48px;font-size:0.78em;">{tr['ticker']}</span>
                  {dir_chip}
                  <span style="color:#334155;background:rgba(0,0,0,0.04);padding:1px 6px;border-radius:3px;font-size:0.65em;">{sig_label}</span>
                  {f'<span style="color:#475569;font-size:0.65em;">{score_str}</span>' if score_str else ''}
                  <span style="color:#64748b;font-size:0.68em;">${tr['entry_price']:.4f} {exit_str}</span>
                  <span style="color:{o_color};font-weight:600;margin-left:auto;font-size:0.75em;">{o_emoji} {pnl_str}</span>
                  <span style="color:#cbd5e1;font-size:0.6em;">{tr['trade_date']}</span>
                </div>"""

            with col_p:
                st.markdown('<div class="sh">Prediction Trades</div>', unsafe_allow_html=True)
                if pred_df.empty:
                    st.markdown('<div style="color:#94a3b8;font-size:0.75em;font-family:\'JetBrains Mono\',monospace;padding:8px 0;">No prediction trades yet — the scanner logs these automatically every 30 min.</div>', unsafe_allow_html=True)
                else:
                    wr_class = "wr-good" if p_wr >= 55 else "wr-bad" if p_wr < 40 else "wr-neu"
                    st.markdown(
                        f'<span class="wr-badge {wr_class}">Win Rate {p_wr:.0f}% ({p_total} closed)</span>',
                        unsafe_allow_html=True)
                    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                    for _, tr in pred_df.head(30).iterrows():
                        st.markdown(_trade_row_html(tr), unsafe_allow_html=True)

            with col_s:
                st.markdown('<div class="sh">Signal Trades (Gap-up / VWAP)</div>', unsafe_allow_html=True)
                if signal_df.empty:
                    st.markdown('<div style="color:#94a3b8;font-size:0.75em;font-family:\'JetBrains Mono\',monospace;padding:8px 0;">No signal trades yet.</div>', unsafe_allow_html=True)
                else:
                    wr_class = "wr-good" if s_wr >= 55 else "wr-bad" if s_wr < 40 else "wr-neu"
                    st.markdown(
                        f'<span class="wr-badge {wr_class}">Win Rate {s_wr:.0f}% ({s_total} closed)</span>',
                        unsafe_allow_html=True)
                    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                    for _, tr in signal_df.head(20).iterrows():
                        st.markdown(_trade_row_html(tr), unsafe_allow_html=True)

    except Exception as e:
        st.error(f"Could not load predictions: {e}")

    st.markdown('<div class="disc" style="margin-top:20px;">PAPER TRADES ONLY · NOT REAL MONEY · PREDICTIONS ARE EXPERIMENTAL</div>', unsafe_allow_html=True)


# ── TAB 5: LIVE ALERTS ────────────────────────────────────────────────────────
with tab5:
    st.markdown("## Live Alert Feed")
    st.markdown('<p style="color:#334155;font-size:0.82em;font-family:\'JetBrains Mono\',monospace;">Real-time alerts from the background scanner. Refreshes every 30 seconds.</p>', unsafe_allow_html=True)

    # Auto-refresh
    refresh = st.button("Refresh Now", use_container_width=False)

    # ── EOD Report Download ───────────────────────────────────────────────────
    try:
        import json as _json
        with open("latest_report.json") as f:
            report_meta = _json.load(f)
        report_path = report_meta.get("path", "")
        report_date = report_meta.get("date", "")

        if report_path and os.path.exists(report_path):
            st.markdown('<div class="sh">Latest EOD Report</div>', unsafe_allow_html=True)
            with open(report_path, "rb") as pdf_file:
                st.download_button(
                    label=f"Download EOD Report — {report_date}",
                    data=pdf_file,
                    file_name=f"Axiom_EOD_{report_date}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )
    except Exception:
        pass

    # Load alert log from shared DB
    alert_log = []
    try:
        from db.database import load_alerts
        alert_log = list(reversed(load_alerts(100)))
    except Exception:
        try:
            import json as _json
            with open("alert_log.json") as f:
                alert_log = list(reversed(_json.load(f)))
        except Exception:
            pass

    # Load scanner state from shared DB
    scanner_state = {}
    try:
        from db.database import load_scanner_state
        scanner_state = load_scanner_state()
    except Exception:
        try:
            with open("scanner_state.json") as f:
                scanner_state = _json.load(f)
        except Exception:
            pass

    # Status bar
    scan_count   = scanner_state.get("scan_count", 0)
    last_updated = scanner_state.get("last_updated", "—")
    is_running   = scan_count > 0

    col_s1, col_s2, col_s3 = st.columns(3)
    col_s1.metric("Scanner Status", "RUNNING" if is_running else "WAITING")
    col_s2.metric("Scans Today",    scan_count)
    col_s3.metric("Last Scan",      last_updated[11:16] if len(last_updated) > 11 else "—")

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    # ── Momentum Ranking ──────────────────────────────────────────────────────
    momentum = scanner_state.get("momentum_ranking", [])
    if momentum:
        st.markdown('<div class="sh">Momentum Ranking</div>', unsafe_allow_html=True)
        top = momentum[:15]
        cols_per_row = 5
        for row_start in range(0, len(top), cols_per_row):
            chunk = top[row_start:row_start + cols_per_row]
            mcols = st.columns(cols_per_row)
            for mc, m in zip(mcols, chunk):
                chg   = m.get("change", 0)
                score = m.get("score", 0)
                above = m.get("above_vwap")
                rvol  = m.get("rvol", 1)
                chg_c = "#16a34a" if chg >= 0 else "#dc2626"
                vwap_badge = '<span style="color:#16a34a;font-size:0.7em;">+V</span>' if above else '<span style="color:#dc2626;font-size:0.7em;">-V</span>'
                mc.markdown(f"""
                <div style="background:var(--bgcard);border:1px solid var(--border);border-radius:6px;
                            padding:8px 10px;text-align:center;transition:border-color 0.2s;"
                     onmouseover="this.style.borderColor='var(--borderhi)'"
                     onmouseout="this.style.borderColor='var(--border)'">
                  <div style="font-family:'JetBrains Mono',monospace;font-size:0.85em;color:#1d6fa5;
                               font-weight:600;letter-spacing:0.05em;">{m['ticker']}</div>
                  <div style="font-family:'JetBrains Mono',monospace;font-size:0.8em;color:{chg_c};
                               font-weight:600;margin:2px 0;">{chg:+.1f}%</div>
                  <div style="display:flex;justify-content:center;gap:6px;margin-top:2px;">
                    {vwap_badge}
                    <span style="font-family:'JetBrains Mono',monospace;font-size:0.65em;color:#64748b;">{rvol:.1f}×</span>
                    <span style="font-family:'JetBrains Mono',monospace;font-size:0.65em;color:#94a3b8;">{score:.0f}pt</span>
                  </div>
                </div>""", unsafe_allow_html=True)

    # ── Live VWAP Status ──────────────────────────────────────────────────────
    vwap_snap = scanner_state.get("vwap_snapshot", {})
    if vwap_snap:
        st.markdown('<div class="sh" style="margin-top:14px;">Live VWAP Status</div>', unsafe_allow_html=True)
        above_list = sorted(
            [(t, d) for t, d in vwap_snap.items() if d.get("above")],
            key=lambda x: x[1]["dist_pct"], reverse=True
        )
        below_list = sorted(
            [(t, d) for t, d in vwap_snap.items() if d.get("above") is False],
            key=lambda x: x[1]["dist_pct"]
        )
        va, vb = st.columns(2)
        with va:
            st.markdown(
                f'<div style="color:#16a34a;font-size:0.68em;font-family:\'JetBrains Mono\','
                f'monospace;letter-spacing:0.1em;margin-bottom:5px;">ABOVE VWAP ({len(above_list)})</div>',
                unsafe_allow_html=True)
            for ticker, d in above_list[:12]:
                st.markdown(f"""
                <div style="display:flex;justify-content:space-between;align-items:center;
                            padding:3px 8px;border-left:2px solid rgba(22,163,74,0.25);
                            background:rgba(22,163,74,0.03);border-radius:0 3px 3px 0;margin-bottom:2px;">
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.72em;color:#1d6fa5;">{ticker}</span>
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.68em;color:#64748b;">${d['price']:.3f}</span>
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.72em;color:#16a34a;font-weight:600;">+{d['dist_pct']:.1f}%</span>
                </div>""", unsafe_allow_html=True)
        with vb:
            st.markdown(
                f'<div style="color:#dc2626;font-size:0.68em;font-family:\'JetBrains Mono\','
                f'monospace;letter-spacing:0.1em;margin-bottom:5px;">BELOW VWAP ({len(below_list)})</div>',
                unsafe_allow_html=True)
            for ticker, d in below_list[:12]:
                st.markdown(f"""
                <div style="display:flex;justify-content:space-between;align-items:center;
                            padding:3px 8px;border-left:2px solid rgba(220,38,38,0.25);
                            background:rgba(220,38,38,0.03);border-radius:0 3px 3px 0;margin-bottom:2px;">
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.72em;color:#1d6fa5;">{ticker}</span>
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.68em;color:#64748b;">${d['price']:.3f}</span>
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.72em;color:#dc2626;font-weight:600;">{d['dist_pct']:.1f}%</span>
                </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    # Load today's watchlist from shared DB
    try:
        from db.database import load_watchlist
        wl_data    = load_watchlist()
        wl_tickers = wl_data.get("tickers", [])
        wl_stats   = wl_data.get("stats", {})

        st.markdown('<div class="sh">Today\'s Dynamic Watchlist</div>', unsafe_allow_html=True)
        st.markdown(
            f'<div style="background:rgba(29,111,165,0.04);border:1px solid rgba(29,111,165,0.15);'
            f'border-radius:8px;padding:12px 16px;font-family:\'JetBrains Mono\',monospace;font-size:0.78em;color:#334155;">'
            f'<span style="color:#64748b;">Screened:</span> <span style="color:#1d6fa5;">{wl_stats.get("screened",0):,} stocks</span> &nbsp;·&nbsp; '
            f'<span style="color:#64748b;">Active:</span> <span style="color:#1d6fa5;">{wl_stats.get("interesting",0)}</span> &nbsp;·&nbsp; '
            f'<span style="color:#64748b;">Watching:</span> <span style="color:#16a34a;">{len(wl_tickers)}</span><br><br>'
            f'<span style="color:#94a3b8;">{" · ".join(wl_tickers[:30])}{"..." if len(wl_tickers) > 30 else ""}</span>'
            f'</div>',
            unsafe_allow_html=True
        )

        gap_ups = wl_stats.get("gap_ups", [])
        if gap_ups:
            st.markdown(f'<div style="margin-top:8px;color:#16a34a;font-size:0.78em;font-family:\'JetBrains Mono\',monospace;">Gap-ups today: {", ".join(gap_ups)}</div>', unsafe_allow_html=True)

    except FileNotFoundError:
        st.info("No watchlist generated yet. The scanner builds today's watchlist at 6 AM ET, or you can trigger it manually.")
        if st.button("Build Watchlist Now", type="primary"):
            with st.spinner("Scanning universe for active stocks... (this takes 2-3 minutes)"):
                try:
                    from morning_screen import build_todays_watchlist
                    wl = build_todays_watchlist(max_stocks=50)
                    st.success(f"Built watchlist with {len(wl)} stocks: {', '.join(wl[:10])}...")
                except Exception as e:
                    st.error(f"Screen failed: {e}")

    # Alert feed
    st.markdown('<div class="sh" style="margin-top:16px;">Alert History</div>', unsafe_allow_html=True)
    if not alert_log:
        st.markdown("""
        <div class="empty" style="padding:30px 20px;">
          <div class="ico" style="font-size:2em;">--</div>
          <h3 style="font-size:1.2em;">NO ALERTS YET</h3>
          <p>Alerts appear here when the scanner detects significant activity.<br>
          Make sure the background scanner is running on Railway.</p>
        </div>""", unsafe_allow_html=True)
    else:
        for alert in alert_log[:30]:
            # Color code by alert type (keyword-based since emojis removed)
            al = alert.lower()
            if "gap-up" in al or "session high" in al or "pre-market high" in al or "pred buy" in al:
                color = "#16a34a"; bg = "rgba(22,163,74,0.05)"
            elif "session low" in al or "news" in al or "loss" in al:
                color = "#dc2626"; bg = "rgba(220,38,38,0.05)"
            elif "filing" in al:
                color = "#6d28d9"; bg = "rgba(109,40,217,0.05)"
            elif "volume spike" in al or "extended" in al or "vwap" in al:
                color = "#c2610f"; bg = "rgba(194,97,15,0.05)"
            elif "pred sell" in al:
                color = "#dc2626"; bg = "rgba(220,38,38,0.05)"
            else:
                color = "#475569"; bg = "transparent"

            st.markdown(f"""
            <div style="display:flex;align-items:center;gap:12px;padding:8px 12px;
                        background:{bg};border-left:2px solid {color}40;
                        border-radius:0 6px 6px 0;margin-bottom:4px;">
                <span style="color:{color};font-family:'JetBrains Mono',monospace;font-size:0.8em;">{alert}</span>
            </div>""", unsafe_allow_html=True)

    # Setup instructions
    with st.expander("Setup Instructions — How to activate real-time alerts"):
        st.markdown("""
        **Step 1: Get Pushover (one-time $5)**
        1. Go to **pushover.net** → sign up
        2. Copy your **User Key** from the dashboard
        3. Click "Create an Application" → copy the **API Token**

        **Step 2: Add keys to Railway**
        1. Go to your Railway project
        2. Click your service → **Variables** tab
        3. Add these variables:
        ```
        PUSHOVER_USER_KEY = your_user_key_here
        PUSHOVER_API_TOKEN = your_api_token_here
        ANTHROPIC_API_KEY = your_anthropic_key
        FINNHUB_API_KEY = your_finnhub_key
        ```

        **Step 3: Deploy the scanner process**
        1. Make sure your GitHub repo has the new `Procfile` committed
        2. Railway will automatically run both:
           - `web` → the Streamlit dashboard
           - `scanner` → the background real-time scanner

        **What you'll get:**
        - Push notification within 60 seconds of a volume spike
        - Alert when a new 8-K is filed for any watchlist stock
        - Alert when a stock moves >5% in a single minute
        - Morning brief at 9:25 AM with today's active watchlist
        """)

# ── TAB 6: INFO ───────────────────────────────────────────────────────────────
with tab6:
    st.markdown("""
## How Axiom Terminal Works

### Scoring Model
Each stock scores 0–100 across five components. Edit `config.py` to change weights.

| Component | Weight | What it measures |
|-----------|--------|-----------------|
| Technical | 30% | RSI, MACD, moving averages, volume, momentum |
| Catalyst | 25% | SEC 8-K events, news, keyword signals |
| Fundamental | 20% | Revenue growth, cash, burn rate, margins |
| Risk (inverted) | 15% | Dilution, short interest, volatility, liquidity |
| Sentiment | 10% | News tone, analyst coverage, hype detection |

### New Features
- **Earnings Calendar** — flags stocks with earnings within 7 days (binary event warning)
- **Insider Direction** — detects if Form 4 was a BUY or SELL with approximate value
- **Sector Relative Strength** — compares stock vs its sector ETF over 20 days
- **AI Filing Analysis** — Claude reads and summarizes actual SEC filing text

### Signal Labels

| Signal | Score | Meaning |
|--------|-------|---------|
| Strong Buy Candidate | 75–100 | Strong conditions — still research before acting |
| Speculative Buy | 60–75 | Good setup, acceptable risk |
| Watchlist | 45–60 | Interesting, not compelling yet |
| Hold | 35–45 | No edge for new entries |
| Trim | 25–35 | Weakening — consider reducing |
| Sell | 15–25 | Weak across the board |
| Avoid | 0–15 | Poor setup or critical flags active |

### Data Sources
- **Finnhub** — real-time quotes, earnings calendar, company profile
- **Yahoo Finance** — price history, fundamentals, short interest
- **SEC EDGAR** — S-3, 424B, 8-K filings, Form 4 insider trades
- **Yahoo Finance RSS** — news headlines and sentiment
- **Claude API** — AI filing text analysis

---
**Research tool only. Not financial advice. Small-cap stocks can lose 100% of value. Always do your own research.**
    """)
