# app.py — Axiom Terminal
# ALL helper functions defined FIRST before any UI code.

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime
import sys, os, time as _time, json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DEFAULT_UNIVERSE, SCORING_WEIGHTS, RISK_FLAGS
from db.database import (initialize_db, upsert_holding, delete_holding, get_portfolio,
                          get_connection, validate_session, invalidate_session,
                          create_session, get_user_by_username, create_user, update_last_login,
                          get_all_users)
from auth import check_password, hash_password, validate_password_strength
from core.scanner import scan_ticker, scan_universe
from analysis.portfolio import analyze_holding, compute_portfolio_summary
from data.market_data import fetch_ticker_snapshot, get_price_history, get_chart_data
from analysis.technicals import compute_technicals

_CONFIG_OVERRIDES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config_overrides.json")

def _load_config_overrides():
    if os.path.exists(_CONFIG_OVERRIDES_FILE):
        try:
            return json.load(open(_CONFIG_OVERRIDES_FILE))
        except Exception:
            pass
    return {}

def _save_config_overrides(data):
    try:
        with open(_CONFIG_OVERRIDES_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass

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

/* ── Control panel (mobile-first) ── */
[data-testid="stTabPanel"]:first-of-type .stButton > button {
    padding: 1.1rem 1rem !important;
    font-size: 1em !important;
    font-weight: 700 !important;
    border-radius: 10px !important;
    min-height: 52px !important;
    letter-spacing: 0.02em !important;
}
.ctrl-status {
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 18px;
    display: flex;
    flex-direction: column;
    gap: 6px;
}
.ctrl-status .cs-row { display: flex; align-items: center; gap: 10px; }
.ctrl-status .cs-dot {
    width: 16px; height: 16px; border-radius: 50%;
    flex-shrink: 0; margin-top: 1px;
}
.ctrl-status .cs-label {
    font-family: 'Inter', sans-serif;
    font-size: 1.05em; font-weight: 700; color: var(--t1);
}
.ctrl-status .cs-sub {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.72em; color: var(--t3); padding-left: 26px;
}
.ctrl-summary {
    background: var(--bgcard); border: 1px solid var(--border);
    border-radius: 10px; padding: 4px 0; margin-top: 8px;
}
.ctrl-row {
    display: flex; justify-content: space-between; align-items: center;
    padding: 10px 16px; border-bottom: 1px solid rgba(0,0,0,0.05);
}
.ctrl-row:last-child { border-bottom: none; }
.ctrl-lbl { font-family: 'Inter', sans-serif; font-size: 0.78em; color: var(--t3); font-weight: 500; }
.ctrl-val { font-family: 'JetBrains Mono', monospace; font-size: 0.85em; color: var(--t1); font-weight: 600; }
.ctrl-val.g { color: var(--green); }
.ctrl-val.a { color: var(--amber); }
.ctrl-val.r { color: var(--red); }
.ctrl-section { font-family: 'Inter', sans-serif; font-size: 0.65em; font-weight: 600; letter-spacing: 0.1em; text-transform: uppercase; color: var(--t3); margin: 20px 0 8px; }
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
# AUTH
# ══════════════════════════════════════════════════════════════════════════════

def _require_auth() -> dict:
    """
    Validates the current session. Returns the user dict if authenticated.
    Shows the login/register page and calls st.stop() if not.
    """
    token = st.session_state.get("_session_token")
    if token:
        user = validate_session(token)
        if user:
            return user
        # Token expired or invalid — clear it
        st.session_state.pop("_session_token", None)

    view = st.session_state.get("_auth_view", "login")

    _, col, _ = st.columns([1, 1.6, 1])
    with col:
        st.markdown("""
        <div style="text-align:center;padding:40px 0 28px;">
          <div style="font-family:'Inter',sans-serif;font-size:2em;font-weight:700;
                      color:var(--t1);letter-spacing:-0.02em;line-height:1;">Axiom</div>
          <div style="font-family:'JetBrains Mono',monospace;font-size:0.58em;
                      color:var(--t3);letter-spacing:0.2em;margin-top:4px;">TERMINAL</div>
        </div>""", unsafe_allow_html=True)

        if view == "login":
            with st.form("_login_form", clear_on_submit=False):
                username = st.text_input("Username", placeholder="username")
                password = st.text_input("Password", placeholder="••••••••", type="password")
                submitted = st.form_submit_button("Sign in", use_container_width=True, type="primary")
            if submitted:
                u = get_user_by_username(username)
                if u and check_password(password, u["password_hash"]):
                    tok = create_session(u["id"])
                    update_last_login(u["id"])
                    st.session_state["_session_token"] = tok
                    st.session_state.pop("_auth_view", None)
                    st.rerun()
                else:
                    st.error("Invalid username or password.")
            st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
            if st.button("Create an account", use_container_width=True):
                st.session_state["_auth_view"] = "register"
                st.rerun()

        else:  # register
            with st.form("_register_form", clear_on_submit=False):
                new_user  = st.text_input("Username", placeholder="username")
                new_email = st.text_input("Email",    placeholder="you@example.com")
                new_pass  = st.text_input("Password", placeholder="min 8 characters", type="password")
                new_pass2 = st.text_input("Confirm password", placeholder="repeat password", type="password")
                submitted = st.form_submit_button("Create account", use_container_width=True, type="primary")
            if submitted:
                err = validate_password_strength(new_pass)
                if err:
                    st.error(err)
                elif new_pass != new_pass2:
                    st.error("Passwords don't match.")
                elif not new_user.strip() or not new_email.strip():
                    st.error("All fields are required.")
                else:
                    try:
                        uid = create_user(new_user.strip(), new_email.strip(), hash_password(new_pass))
                        tok = create_session(uid)
                        st.session_state["_session_token"] = tok
                        st.session_state.pop("_auth_view", None)
                        st.rerun()
                    except Exception as _reg_err:
                        st.error(f"Registration failed — username or email already taken.")
            st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
            if st.button("Back to sign in", use_container_width=True):
                st.session_state["_auth_view"] = "login"
                st.rerun()

        st.markdown('<div class="disc" style="margin-top:20px;">Research tool · Not financial advice</div>',
                    unsafe_allow_html=True)
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# INIT
# ══════════════════════════════════════════════════════════════════════════════
initialize_db()
_current_user = _require_auth()


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    _sa, _sb = st.columns([3, 1])
    _sa.markdown("""
    <div class="axiom-logo">
      <div class="name">Axiom</div>
      <div class="sub">TERMINAL</div>
    </div>
    """, unsafe_allow_html=True)
    with _sb:
        st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
        if st.button("out", help="Sign out", use_container_width=True):
            invalidate_session(st.session_state.pop("_session_token", ""))
            st.rerun()
    st.markdown(
        f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.62em;color:var(--t3);'
        f'padding:0 4px 6px;letter-spacing:0.06em;">'
        f'{_current_user["username"].upper()} · {_current_user["role"].upper()}</div>',
        unsafe_allow_html=True
    )
    st.markdown('<div class="disc">Research tool · Not financial advice</div>', unsafe_allow_html=True)

    # ── Live Holdings Manager ─────────────────────────────────────────────────
    st.markdown('<div style="padding:0 4px;"><div class="sh" style="margin-top:14px;">Holdings</div></div>', unsafe_allow_html=True)

    _pf = get_portfolio(_current_user["id"])
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
                delete_holding(row["ticker"], _current_user["id"])
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
                    upsert_holding(new_tkr.strip().upper(), float(new_sh), float(new_cost), user_id=_current_user["id"])
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
            pf = get_portfolio(_current_user["id"])
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
                    res = scan_ticker(t, weights=st.session_state.get("scoring_weights"))
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


# ── Session-state: scoring weights (loaded from saved config or defaults) ─────
if "scoring_weights" not in st.session_state:
    _overrides = _load_config_overrides()
    st.session_state["scoring_weights"] = _overrides.get("scoring_weights", dict(SCORING_WEIGHTS))

# ══════════════════════════════════════════════════════════════════════════════
# LIVE FRAGMENT FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=15)
def _live_control():
    try:
        from db.database import get_scanner_control, set_scanner_control, get_control_stats
        _ctl = get_scanner_control()
        _sts = get_control_stats()
    except Exception:
        _ctl = {"paused": False, "force_scan": False, "scanner_started_at": None}
        _sts = {"signals_today": 0, "alerts_today": 0, "top_signal": None,
                "scan_count": 0, "last_updated": None}

    _paused      = _ctl.get("paused", False)
    _force_scan  = _ctl.get("force_scan", False)
    _started_at  = _ctl.get("scanner_started_at")
    _last_upd    = _sts.get("last_updated")
    _cur_mode    = _ctl.get("current_mode", "UNKNOWN")

    _mins_ago = None
    if _last_upd:
        try:
            _lu = datetime.fromisoformat(str(_last_upd).replace("Z", "+00:00"))
            _lu_naive = _lu.replace(tzinfo=None) if _lu.tzinfo else _lu
            _mins_ago = int((datetime.utcnow() - _lu_naive).total_seconds() / 60)
        except Exception:
            pass

    _uptime_str = "Unknown"
    if _started_at:
        try:
            _sa = datetime.fromisoformat(str(_started_at).replace("Z", "+00:00"))
            _sa_naive = _sa.replace(tzinfo=None) if _sa.tzinfo else _sa
            _up_secs = int((datetime.utcnow() - _sa_naive).total_seconds())
            _uptime_str = f"{_up_secs // 3600}h {(_up_secs % 3600) // 60}m"
        except Exception:
            pass

    _MODE_UI = {
        "MARKET":     ("#16a34a", "rgba(22,163,74,0.07)",   "rgba(22,163,74,0.2)",
                       "MARKET — scanning every 60s",
                       "Full ticker scan, predictions, paper broker active."),
        "PREMARKET":  ("#2563eb", "rgba(37,99,235,0.07)",   "rgba(37,99,235,0.2)",
                       "PRE-MARKET — scanning every 120s",
                       "Gap alerts, news, SEC EDGAR checks running."),
        "AFTERHOURS": ("#7c3aed", "rgba(124,58,237,0.07)",  "rgba(124,58,237,0.2)",
                       "AFTER-HOURS — monitoring every 120s",
                       "AH price alerts active. Conviction scan at 8:30 PM ET."),
        "OVERNIGHT":  ("#475569", "rgba(71,85,105,0.07)",   "rgba(71,85,105,0.2)",
                       "OVERNIGHT — light scan every 5min",
                       "SEC EDGAR + portfolio maintenance only. No new signals."),
        "WEEKEND":    ("#475569", "rgba(71,85,105,0.07)",   "rgba(71,85,105,0.2)",
                       "WEEKEND — grading signals",
                       "Accuracy grading active. Universe refresh Sat 8 AM ET."),
        "UNKNOWN":    ("#475569", "rgba(71,85,105,0.07)",   "rgba(71,85,105,0.2)",
                       "CONNECTING…",
                       "Waiting for scanner heartbeat."),
    }

    st.markdown('<div class="ctrl-section">Scanner Status</div>', unsafe_allow_html=True)

    if _paused:
        _dot_color  = "#c2610f"
        _dot_bg     = "rgba(194,97,15,0.08)"
        _dot_border = "rgba(194,97,15,0.2)"
        _status_lbl = "PAUSED — manual hold"
        _status_sub = "Scanner is holding. Tap Resume to restart."
    else:
        _offline = _mins_ago is not None and _mins_ago > 10 and _cur_mode in ("MARKET", "PREMARKET")
        if _offline:
            _dot_color  = "#dc2626"
            _dot_bg     = "rgba(220,38,38,0.07)"
            _dot_border = "rgba(220,38,38,0.2)"
            _ago_str    = f"{_mins_ago}m ago" if _mins_ago is not None else "unknown"
            _status_lbl = "OFFLINE — no scan in 10+ min"
            _status_sub = f"Last scan: {_ago_str}. Check Railway logs."
        else:
            _ui = _MODE_UI.get(_cur_mode, _MODE_UI["UNKNOWN"])
            _dot_color, _dot_bg, _dot_border = _ui[0], _ui[1], _ui[2]
            _status_lbl = _ui[3]
            _base_sub   = _ui[4]
            if _mins_ago is not None and _cur_mode in ("MARKET", "PREMARKET"):
                _status_sub = f"{_base_sub} Last scan {_mins_ago}m ago · {_sts['scan_count']} scans today"
            else:
                _status_sub = _base_sub

    st.markdown(f"""
    <div class="ctrl-status" style="background:{_dot_bg};border:1px solid {_dot_border};">
      <div class="cs-row">
        <div class="cs-dot" style="background:{_dot_color};
             box-shadow:0 0 0 4px {_dot_color}22;"></div>
        <div class="cs-label" style="color:{_dot_color};">{_status_lbl}</div>
      </div>
      <div class="cs-sub">{_status_sub}</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="ctrl-section">Scanner Control</div>', unsafe_allow_html=True)

    _c1, _c2 = st.columns(2)
    with _c1:
        if _paused:
            if st.button("RESUME SCANNER", use_container_width=True, key="ctrl_resume"):
                try:
                    from db.database import set_scanner_control
                    set_scanner_control(paused=False)
                    st.rerun()
                except Exception as _e:
                    st.error(f"Failed: {_e}")
        else:
            if st.button("PAUSE SCANNER", use_container_width=True, key="ctrl_pause"):
                try:
                    from db.database import set_scanner_control
                    set_scanner_control(paused=True)
                    st.rerun()
                except Exception as _e:
                    st.error(f"Failed: {_e}")

    with _c2:
        _fscan_label    = "SCANNING..." if _force_scan else "SCAN NOW"
        _fscan_disabled = _force_scan
        if st.button(_fscan_label, use_container_width=True, key="ctrl_forcescan",
                     disabled=_fscan_disabled):
            try:
                from db.database import set_scanner_control
                set_scanner_control(force_scan=True)
                st.rerun()
            except Exception as _e:
                st.error(f"Failed: {_e}")

    if _force_scan:
        st.markdown(
            '<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.75em;'
            'color:#1d6fa5;margin-top:6px;">Scan triggered — scanner will pick it up within 30–90s.</div>',
            unsafe_allow_html=True
        )

    st.markdown('<div class="ctrl-section">Quick Summary</div>', unsafe_allow_html=True)

    _top          = _sts.get("top_signal")
    _top_str      = (f"{_top['ticker']} — {_top['label']} — Score {_top['score']:.0f}"
                     if _top else "No signals yet")
    _mode_color   = {"MARKET": "g", "PREMARKET": "a", "AFTERHOURS": "a"}.get(_cur_mode, "")
    _last_scan_str = f"{_mins_ago}m ago" if _mins_ago is not None else "—"

    st.markdown(f"""
    <div class="ctrl-summary">
      <div class="ctrl-row">
        <span class="ctrl-lbl">Last scan</span>
        <span class="ctrl-val">{_last_scan_str}</span>
      </div>
      <div class="ctrl-row">
        <span class="ctrl-lbl">Signals today</span>
        <span class="ctrl-val g">{_sts['signals_today']}</span>
      </div>
      <div class="ctrl-row">
        <span class="ctrl-lbl">Alerts fired today</span>
        <span class="ctrl-val">{_sts['alerts_today']}</span>
      </div>
      <div class="ctrl-row">
        <span class="ctrl-lbl">Top signal</span>
        <span class="ctrl-val" style="font-size:0.78em;max-width:58%;text-align:right;">{_top_str}</span>
      </div>
      <div class="ctrl-row">
        <span class="ctrl-lbl">Scanner mode</span>
        <span class="ctrl-val {_mode_color}">{_cur_mode}</span>
      </div>
      <div class="ctrl-row">
        <span class="ctrl-lbl">Scanner uptime</span>
        <span class="ctrl-val">{_uptime_str}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(
        '<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.65em;'
        'color:var(--t3);margin-top:12px;text-align:center;">Live — updates every 15 seconds</div>',
        unsafe_allow_html=True
    )


@st.fragment(run_every=10)
def _live_alerts_feed():
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

    scanner_state = {}
    try:
        from db.database import load_scanner_state
        scanner_state = load_scanner_state()
    except Exception:
        try:
            import json as _json
            with open("scanner_state.json") as f:
                scanner_state = _json.load(f)
        except Exception:
            pass

    scan_count   = scanner_state.get("scan_count", 0)
    last_updated = scanner_state.get("last_updated", "—")
    is_running   = scan_count > 0

    col_s1, col_s2, col_s3 = st.columns(3)
    col_s1.metric("Scanner Status", "RUNNING" if is_running else "WAITING")
    col_s2.metric("Scans Today",    scan_count)
    col_s3.metric("Last Scan",      last_updated[11:16] if len(last_updated) > 11 else "—")

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

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
                vwap_badge = ('<span style="color:#16a34a;font-size:0.7em;">+V</span>'
                              if above else '<span style="color:#dc2626;font-size:0.7em;">-V</span>')
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
            st.markdown(
                f'<div style="margin-top:8px;color:#16a34a;font-size:0.78em;'
                f'font-family:\'JetBrains Mono\',monospace;">Gap-ups today: {", ".join(gap_ups)}</div>',
                unsafe_allow_html=True
            )

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


# ══════════════════════════════════════════════════════════════════════════════
# MAIN TABS
# ══════════════════════════════════════════════════════════════════════════════
tab0, tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs(["Control", "Scanner", "Portfolio", "Deep Dive", "Predictions", "Live Alerts", "Accuracy", "Info", "Config", "Paper Trading"])


# ── TAB 0: CONTROL ───────────────────────────────────────────────────────────
with tab0:
    _live_control()


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
    portfolio_df = get_portfolio(_current_user["id"])

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
                "Score":    st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%d", width=90),
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
            result = scan_ticker(dive_ticker, save=False, weights=st.session_state.get("scoring_weights"))
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
    st.markdown(
        '<p style="color:#334155;font-size:0.82em;font-family:\'JetBrains Mono\',monospace;">'
        'Real-time alerts from the background scanner. Updates every 10 seconds.</p>',
        unsafe_allow_html=True
    )

    # ── EOD Report Download (static, no need to fragment) ─────────────────────
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

    _live_alerts_feed()

# ── TAB 6: INFO ───────────────────────────────────────────────────────────────
with tab6:
    # ── Checkpoint Reports ────────────────────────────────────────────────────
    st.markdown('<div class="sh" style="margin-top:4px;">Accuracy Test Reports</div>', unsafe_allow_html=True)
    try:
        from db.database import get_accuracy_reports, get_signal_stats
        _chk_reports  = get_accuracy_reports()
        _chk_stats    = get_signal_stats()
        _sig_total    = _chk_stats.get("total", 0)
    except Exception:
        _chk_reports = []
        _sig_total   = 0

    _FINAL_TARGET = 600
    _prog_frac    = min(_sig_total / _FINAL_TARGET, 1.0)
    _prog_pct     = int(_prog_frac * 100)
    _prog_color   = "#16a34a" if _sig_total >= _FINAL_TARGET else "#1d6fa5"
    st.markdown(
        f'<div style="margin:10px 0 16px;">'
        f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.78em;color:var(--t2);margin-bottom:6px;">'
        f'{_sig_total} / {_FINAL_TARGET} signals logged toward final verdict</div>'
        f'<div style="background:var(--border);border-radius:4px;height:6px;overflow:hidden;">'
        f'<div style="width:{_prog_pct}%;height:100%;background:{_prog_color};border-radius:4px;transition:width 0.3s;"></div>'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    _CHECKPOINT_META = {
        "checkpoint_150": ("150 Signals — Sanity Check",    150),
        "checkpoint_350": ("350 Signals — Preliminary",     350),
        "checkpoint_600": ("600 Signals — Final Verdict",   600),
    }
    _VERDICT_COLORS = {
        "OPERATIONAL":     "#1a56b0",
        "NEEDS FIXING":    "#dc2626",
        "PROMISING":       "#16a34a",
        "INCONCLUSIVE":    "#c2610f",
        "UNDERPERFORMING": "#dc2626",
        "APPROVED":        "#16a34a",
        "REJECTED":        "#dc2626",
    }

    _rpt_map = {r["report_type"]: r for r in _chk_reports}
    _chk_cols = st.columns(3)
    for _ci, _rtype in enumerate(["checkpoint_150", "checkpoint_350", "checkpoint_600"]):
        _meta  = _CHECKPOINT_META[_rtype]
        _rpt   = _rpt_map.get(_rtype)
        with _chk_cols[_ci]:
            if _rpt:
                _vc   = _VERDICT_COLORS.get(_rpt.get("status_label", ""), "#334155")
                _ts   = str(_rpt.get("generated_at", ""))[:10]
                _url  = _rpt.get("download_url", "")
                st.markdown(
                    f'<div style="background:var(--bgcard);border:1px solid var(--border);'
                    f'border-radius:8px;padding:12px 14px;">'
                    f'<div style="font-size:0.72em;color:var(--t3);font-family:\'JetBrains Mono\',monospace;">{_meta[0].upper()}</div>'
                    f'<div style="font-size:1.1em;font-weight:700;color:{_vc};margin:4px 0;">{_rpt.get("status_label","—")}</div>'
                    f'<div style="font-size:0.72em;color:var(--t3);">Generated {_ts}</div>'
                    + (f'<a href="{_url}" target="_blank" style="display:inline-block;margin-top:8px;'
                       f'font-size:0.75em;color:var(--blue);font-family:\'JetBrains Mono\',monospace;">'
                       f'Download PDF</a>' if _url else '')
                    + '</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div style="background:var(--bgcard);border:1px solid var(--border);'
                    f'border-radius:8px;padding:12px 14px;">'
                    f'<div style="font-size:0.72em;color:var(--t3);font-family:\'JetBrains Mono\',monospace;">{_meta[0].upper()}</div>'
                    f'<div style="font-size:0.9em;color:var(--t3);margin:6px 0;">Pending — {_meta[1]} signals not reached</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)

    # ── Accuracy Tab ──────────────────────────────────────────────────────────
    st.markdown('<div class="sh">Signal Accuracy Tracker</div>', unsafe_allow_html=True)

    try:
        from db.database import get_signal_log, get_signal_stats
        stats  = get_signal_stats()
        sig_df = get_signal_log(days=30)
    except Exception as _acc_e:
        stats  = {}
        sig_df = pd.DataFrame()
        st.error(f"Could not load accuracy data: {_acc_e}")

    total    = stats.get("total", 0)
    resolved = stats.get("resolved", 0)

    if total == 0:
        st.markdown("""
        <div class="empty">
          <div class="ico">--</div>
          <h3>NO SIGNALS LOGGED YET</h3>
          <p>The scanner logs prediction_buy / prediction_sell signals automatically.<br>
          Outcomes fill in over 1hr / 1day / 5day windows via yfinance.</p>
        </div>""", unsafe_allow_html=True)
    else:
        need_more = resolved < 20
        if need_more:
            st.markdown(
                f'<div class="box box-amber" style="margin-bottom:14px;">'
                f'Only {resolved} resolved signal(s) so far — statistics become reliable after 20+. '
                f'Pending signals fill in automatically each hour during market hours.</div>',
                unsafe_allow_html=True
            )

        # ── Top metrics ───────────────────────────────────────────────────────
        mc1, mc2, mc3, mc4 = st.columns(4)
        wr = stats.get("overall_win_rate")
        ag = stats.get("avg_5day_gain")
        mc1.metric("Total Signals", total)
        mc2.metric("Resolved (5-day)", resolved)
        mc3.metric("Overall Win Rate", f"{wr:.1f}%" if wr is not None else "—")
        mc4.metric("Avg 5-day Gain (wins)", f"{ag:+.2f}%" if ag is not None else "—")

        st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)

        # ── Win rate by signal type ────────────────────────────────────────────
        by_signal = stats.get("by_signal", {})
        if by_signal:
            st.markdown('<div class="sh">Win Rate by Signal Type</div>', unsafe_allow_html=True)
            _order = ["Strong Buy Candidate", "Speculative Buy", "Gap-Up", "Watchlist",
                      "Hold", "Trim", "Sell", "Avoid"]
            rows_html = ""
            for lbl in _order + [l for l in by_signal if l not in _order]:
                d = by_signal.get(lbl)
                if not d:
                    continue
                wr_v = d["win_rate"]
                if wr_v >= 55:
                    badge_cls = "wr-good"
                elif wr_v <= 40:
                    badge_cls = "wr-bad"
                else:
                    badge_cls = "wr-neu"
                ag_v  = d["avg_gain"]
                al_v  = d["avg_loss"]
                ag_s  = f'+{ag_v:.1f}%' if ag_v is not None else "—"
                al_s  = f'{al_v:.1f}%'  if al_v is not None else "—"
                rows_html += (
                    f'<div class="stat-row">'
                    f'<span class="slbl" style="width:180px;flex-shrink:0;">{lbl}</span>'
                    f'<span class="sval" style="width:60px;text-align:center;">{d["count"]}</span>'
                    f'<span style="width:110px;">'
                    f'<span class="wr-badge {badge_cls}">{wr_v:.1f}%</span>'
                    f'</span>'
                    f'<span class="sval g" style="width:80px;">{ag_s}</span>'
                    f'<span class="sval r" style="width:80px;">{al_s}</span>'
                    f'</div>'
                )
            st.markdown(
                '<div style="background:var(--bgcard);border:1px solid var(--border);border-radius:8px;padding:10px 16px;">'
                + '<div class="stat-row" style="margin-bottom:4px;">'
                + '<span class="slbl" style="width:180px;font-weight:600;">Signal Type</span>'
                + '<span class="slbl" style="width:60px;text-align:center;"># Signals</span>'
                + '<span class="slbl" style="width:110px;">Win Rate</span>'
                + '<span class="slbl" style="width:80px;">Avg Gain</span>'
                + '<span class="slbl" style="width:80px;">Avg Loss</span>'
                + '</div>'
                + rows_html
                + '</div>',
                unsafe_allow_html=True
            )

        st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)

        # ── Component correlation bar chart ────────────────────────────────────
        comp_corr = stats.get("component_corr", {})
        if comp_corr and resolved >= 5:
            st.markdown('<div class="sh">Component Score: Wins vs Losses (5-day avg)</div>', unsafe_allow_html=True)
            comp_names = ["technical", "catalyst", "fundamental", "risk", "sentiment"]
            win_means  = [comp_corr.get(c, {}).get("win_mean")  for c in comp_names]
            loss_means = [comp_corr.get(c, {}).get("loss_mean") for c in comp_names]
            fig_corr = go.Figure()
            fig_corr.add_trace(go.Bar(
                name="Wins",
                x=[c.title() for c in comp_names],
                y=win_means,
                marker_color="rgba(22,163,74,0.7)",
                text=[f"{v:.1f}" if v is not None else "" for v in win_means],
                textposition="outside",
            ))
            fig_corr.add_trace(go.Bar(
                name="Losses",
                x=[c.title() for c in comp_names],
                y=loss_means,
                marker_color="rgba(220,38,38,0.7)",
                text=[f"{v:.1f}" if v is not None else "" for v in loss_means],
                textposition="outside",
            ))
            fig_corr.update_layout(
                barmode="group",
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Inter", size=11, color="#334155"),
                legend=dict(orientation="h", y=1.12, x=0),
                yaxis=dict(range=[0, 100], gridcolor="rgba(0,0,0,0.06)"),
                xaxis=dict(showgrid=False),
            )
            st.plotly_chart(fig_corr, use_container_width=True)

        # ── Recent signal log with outcomes ───────────────────────────────────
        st.markdown('<div class="sh">Recent Signals (30 days)</div>', unsafe_allow_html=True)
        if sig_df.empty:
            st.markdown('<div class="box">No signals logged yet.</div>', unsafe_allow_html=True)
        else:
            _outcome_color = {"win": "#16a34a", "loss": "#dc2626", "neutral": "#94a3b8", "pending": "#c2610f"}
            rows_html = ""
            for _, row in sig_df.head(40).iterrows():
                ol  = row.get("outcome_label") or "pending"
                col = _outcome_color.get(ol, "#94a3b8")
                p5  = row.get("pct_change_5day")
                p5s = f'{p5:+.1f}%' if p5 is not None and not (isinstance(p5, float) and p5 != p5) else "—"
                p1  = row.get("pct_change_1day")
                p1s = f'{p1:+.1f}%' if p1 is not None and not (isinstance(p1, float) and p1 != p1) else "—"
                ts  = str(row.get("created_at", ""))[:16]
                rows_html += (
                    f'<div class="stat-row">'
                    f'<span class="sval b" style="width:60px;">{row["ticker"]}</span>'
                    f'<span class="slbl" style="width:170px;">{row.get("signal_label","")}</span>'
                    f'<span class="sval" style="width:50px;">{row.get("score","")}</span>'
                    f'<span style="width:80px;font-family:\'JetBrains Mono\',monospace;font-size:0.72em;color:{col};font-weight:600;">{ol.upper()}</span>'
                    f'<span class="sval" style="width:70px;color:{"#16a34a" if p5 and p5 > 0 else "#dc2626" if p5 and p5 < 0 else "#94a3b8"};">{p5s}</span>'
                    f'<span class="sval" style="width:70px;color:{"#16a34a" if p1 and p1 > 0 else "#dc2626" if p1 and p1 < 0 else "#94a3b8"};">{p1s}</span>'
                    f'<span class="slbl" style="width:110px;text-align:right;">{ts}</span>'
                    f'</div>'
                )
            st.markdown(
                '<div style="background:var(--bgcard);border:1px solid var(--border);border-radius:8px;padding:10px 16px;">'
                + '<div class="stat-row" style="margin-bottom:4px;">'
                + '<span class="slbl" style="width:60px;">Ticker</span>'
                + '<span class="slbl" style="width:170px;">Signal</span>'
                + '<span class="slbl" style="width:50px;">Score</span>'
                + '<span class="slbl" style="width:80px;">Outcome</span>'
                + '<span class="slbl" style="width:70px;">5-day %</span>'
                + '<span class="slbl" style="width:70px;">1-day %</span>'
                + '<span class="slbl" style="width:110px;text-align:right;">Time</span>'
                + '</div>'
                + rows_html
                + '</div>',
                unsafe_allow_html=True
            )


with tab7:
    _w = st.session_state.get("scoring_weights", SCORING_WEIGHTS)
    _wt = int(round(_w["technical"]   * 100))
    _wc = int(round(_w["catalyst"]    * 100))
    _wf = int(round(_w["fundamental"] * 100))
    _wr = int(round(_w["risk"]        * 100))
    _ws = int(round(_w["sentiment"]   * 100))
    st.markdown(f"""
## How Axiom Terminal Works

### Scoring Model
Each stock scores 0–100 across five components. Adjust weights in the **Config** tab.

| Component | Weight | What it measures |
|-----------|--------|-----------------|
| Technical | {_wt}% | RSI, MACD, moving averages, volume, momentum |
| Catalyst | {_wc}% | SEC 8-K events, news, keyword signals |
| Fundamental | {_wf}% | Revenue growth, cash, burn rate, margins |
| Risk (inverted) | {_wr}% | Dilution, short interest, volatility, liquidity |
| Sentiment | {_ws}% | News tone, analyst coverage, hype detection |

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

    if _current_user["role"] == "admin":
        st.markdown("---")
        st.markdown('<div class="sh">Admin Panel</div>', unsafe_allow_html=True)
        try:
            users = get_all_users()
            st.markdown(f'**{len(users)} registered user(s)**')
            rows_html = ""
            for u in users:
                last = str(u.get("last_login") or "—")[:16]
                since = str(u.get("created_at") or "—")[:10]
                role_color = "#c2610f" if u["role"] == "admin" else "#334155"
                rows_html += (
                    f'<div class="stat-row">'
                    f'<span class="sval b" style="width:130px;">{u["username"]}</span>'
                    f'<span class="slbl" style="width:200px;">{u["email"]}</span>'
                    f'<span style="width:70px;font-family:\'JetBrains Mono\',monospace;font-size:0.72em;'
                    f'color:{role_color};font-weight:600;">{u["role"].upper()}</span>'
                    f'<span class="slbl" style="width:100px;">{since}</span>'
                    f'<span class="slbl" style="width:130px;text-align:right;">{last}</span>'
                    f'</div>'
                )
            st.markdown(
                '<div style="background:var(--bgcard);border:1px solid var(--border);border-radius:8px;padding:10px 16px;">'
                + '<div class="stat-row" style="margin-bottom:4px;">'
                + '<span class="slbl" style="width:130px;">Username</span>'
                + '<span class="slbl" style="width:200px;">Email</span>'
                + '<span class="slbl" style="width:70px;">Role</span>'
                + '<span class="slbl" style="width:100px;">Joined</span>'
                + '<span class="slbl" style="width:130px;text-align:right;">Last login</span>'
                + '</div>'
                + rows_html
                + '</div>',
                unsafe_allow_html=True
            )
        except Exception as _adm_e:
            st.error(f"Admin panel error: {_adm_e}")


# ── TAB 8: CONFIG ─────────────────────────────────────────────────────────────
with tab8:
    st.markdown('<div class="sh">Scoring Weights</div>', unsafe_allow_html=True)
    st.markdown(
        '<p style="color:var(--t3);font-size:0.82em;margin-bottom:16px;">'
        'Drag each slider to set how much weight each component carries in the final 0–100 score. '
        'Values are auto-normalized to 100% when you apply.</p>',
        unsafe_allow_html=True,
    )

    _cur = st.session_state.get("scoring_weights", SCORING_WEIGHTS)

    _sections = [
        ("technical",   "Technical",       "RSI, MACD, moving averages, volume, momentum"),
        ("catalyst",    "Catalyst",        "SEC 8-K events, news, insider activity"),
        ("fundamental", "Fundamental",     "Revenue growth, cash runway, burn rate, margins"),
        ("risk",        "Risk (inverted)", "Dilution, short interest, volatility, liquidity"),
        ("sentiment",   "Sentiment",       "News tone, analyst coverage, hype detection"),
    ]

    _raw = {}
    for _key, _label, _desc in _sections:
        _raw[_key] = st.slider(
            _label,
            min_value=0, max_value=100,
            value=int(round(_cur.get(_key, SCORING_WEIGHTS[_key]) * 100)),
            help=_desc,
            key=f"cfg_{_key}",
        )

    _total = sum(_raw.values())
    _pct_color = "#16a34a" if _total == 100 else "#c2610f"
    st.markdown(
        f'<p style="font-size:0.83em;font-family:\'JetBrains Mono\',monospace;'
        f'color:{_pct_color};font-weight:600;margin-top:4px;">'
        f'Total: {_total}%'
        f'{"" if _total == 100 else " — will be normalized to 100% on apply"}'
        f'</p>',
        unsafe_allow_html=True,
    )

    _c1, _c2 = st.columns(2)
    with _c1:
        if st.button("Apply Weights", type="primary", use_container_width=True):
            if _total == 0:
                st.error("All weights are zero — set at least one above 0.")
            else:
                _normalized = {k: v / _total for k, v in _raw.items()}
                st.session_state["scoring_weights"] = _normalized
                _ov = _load_config_overrides()
                _ov["scoring_weights"] = _normalized
                _save_config_overrides(_ov)
                st.success("Weights saved. Next scan will use these values.")
                st.rerun()
    with _c2:
        if st.button("Reset to Defaults", use_container_width=True):
            st.session_state["scoring_weights"] = dict(SCORING_WEIGHTS)
            _ov = _load_config_overrides()
            _ov.pop("scoring_weights", None)
            _save_config_overrides(_ov)
            st.success("Reset to config.py defaults.")
            st.rerun()


# ── TAB 9: PAPER TRADING ─────────────────────────────────────────────────────
with tab9:
    st.markdown("## Paper Trading")
    st.markdown(
        '<p style="color:#334155;font-size:0.82em;font-family:\'JetBrains Mono\',monospace;">'
        '$100K virtual account. Conviction engine submits limit orders; positions managed with '
        'T1/T2/T3 partial exits and ATR-based stops. No real money involved.</p>',
        unsafe_allow_html=True,
    )

    try:
        from paper_broker import get_broker
        _broker = get_broker()
        _dash   = _broker.get_dashboard_data()
        _acct   = _dash.get("account", {})
        _pos_df = _dash.get("positions",   pd.DataFrame())
        _ord_df = _dash.get("orders",      pd.DataFrame())
        _trd_df = _dash.get("trades",      pd.DataFrame())
        _eq_df  = _dash.get("equity_curve", pd.DataFrame())
        _day_df = _dash.get("daily_stats", pd.DataFrame())

        # ── Section 1: Account summary bar ───────────────────────────────────
        st.markdown('<div class="sh">Account Summary</div>', unsafe_allow_html=True)
        _total_eq  = _acct.get("total_equity", 100000)
        _cash      = _acct.get("cash_balance", 100000)
        _open_pnl  = _acct.get("open_pnl", 0.0)
        _total_pnl = _acct.get("total_pnl", 0.0)
        _win_rate  = _acct.get("win_rate", 0.0)
        _pf        = _acct.get("profit_factor")
        _n_pos     = len(_pos_df) if not _pos_df.empty else 0
        _n_trades  = _acct.get("total_trades", 0)
        _max_dd    = _acct.get("max_drawdown", 0.0)

        _a1, _a2, _a3, _a4, _a5, _a6, _a7 = st.columns(7)
        _a1.metric("Total Equity",   f"${_total_eq:,.0f}",
                   delta=f"{(_total_eq/100000-1)*100:+.1f}%")
        _a2.metric("Cash",           f"${_cash:,.0f}")
        _a3.metric("Open P&L",       f"${_open_pnl:+,.2f}")
        _a4.metric("Total P&L",      f"${_total_pnl:+,.2f}")
        _a5.metric("Win Rate",       f"{_win_rate:.0%}" if _win_rate else "—",
                   delta=f"{_n_trades} trades")
        _a6.metric("Profit Factor",  f"{_pf:.2f}" if _pf else "—")
        _a7.metric("Max Drawdown",   f"{_max_dd:.1f}%")

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

        # ── Section 2: Equity curve chart ─────────────────────────────────────
        st.markdown('<div class="sh">Equity Curve</div>', unsafe_allow_html=True)
        if not _eq_df.empty and "equity" in _eq_df.columns:
            _eq_df = _eq_df.sort_values("snapshot_time") if "snapshot_time" in _eq_df.columns else _eq_df
            _fig_eq = go.Figure()
            _fig_eq.add_trace(go.Scatter(
                x=_eq_df.get("snapshot_time", _eq_df.index),
                y=_eq_df["equity"],
                mode="lines",
                line=dict(color="#2563eb", width=2),
                fill="tozeroy",
                fillcolor="rgba(37,99,235,0.08)",
                name="Equity",
            ))
            _fig_eq.add_hline(y=100000, line_dash="dot", line_color="#64748b",
                              annotation_text="Starting $100K")
            _fig_eq.update_layout(
                height=220, margin=dict(l=0, r=0, t=10, b=0),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="JetBrains Mono", size=11, color="#e2e8f0"),
                xaxis=dict(showgrid=False, color="#64748b"),
                yaxis=dict(showgrid=True, gridcolor="#1e293b",
                           tickprefix="$", tickformat=",.0f", color="#64748b"),
                showlegend=False,
            )
            st.plotly_chart(_fig_eq, use_container_width=True, config={"displayModeBar": False})
        else:
            st.markdown(
                '<div class="empty"><div class="ico">--</div>'
                '<p>Equity curve builds after the first scan cycle.</p></div>',
                unsafe_allow_html=True,
            )

        # ── Section 3: Open positions ─────────────────────────────────────────
        st.markdown('<div class="sh">Open Positions</div>', unsafe_allow_html=True)
        if not _pos_df.empty:
            _show_cols = [c for c in ["ticker","side","qty","avg_cost","current_price",
                                      "unrealized_pnl","unrealized_pnl_pct","stop_price",
                                      "t1_price","t2_price","t3_price","t1_hit","t2_hit",
                                      "hold_type","mae","mfe"] if c in _pos_df.columns]
            _pos_show = _pos_df[_show_cols].copy()
            for _pc in ["avg_cost","current_price","stop_price","t1_price","t2_price","t3_price","mae","mfe"]:
                if _pc in _pos_show.columns:
                    _pos_show[_pc] = _pos_show[_pc].apply(
                        lambda v: f"${v:.2f}" if pd.notna(v) and v != 0 else "—"
                    )
            if "unrealized_pnl" in _pos_show.columns:
                _pos_show["unrealized_pnl"] = _pos_show["unrealized_pnl"].apply(
                    lambda v: f"${v:+,.2f}" if pd.notna(v) else "—"
                )
            if "unrealized_pnl_pct" in _pos_show.columns:
                _pos_show["unrealized_pnl_pct"] = _pos_show["unrealized_pnl_pct"].apply(
                    lambda v: f"{v:+.1f}%" if pd.notna(v) else "—"
                )
            st.dataframe(_pos_show, use_container_width=True, hide_index=True)
        else:
            st.markdown('<p style="color:#64748b;font-size:0.85em;">No open positions.</p>',
                        unsafe_allow_html=True)

        # ── Section 4: Pending orders ─────────────────────────────────────────
        st.markdown('<div class="sh">Pending Orders</div>', unsafe_allow_html=True)
        if not _ord_df.empty:
            _pend = _ord_df[_ord_df["status"] == "pending"] if "status" in _ord_df.columns else _ord_df
            if not _pend.empty:
                _ord_cols = [c for c in ["order_id","ticker","side","order_type","qty",
                                         "limit_price","stop_price","status","created_at"]
                             if c in _pend.columns]
                st.dataframe(_pend[_ord_cols], use_container_width=True, hide_index=True)
            else:
                st.markdown('<p style="color:#64748b;font-size:0.85em;">No pending orders.</p>',
                            unsafe_allow_html=True)
        else:
            st.markdown('<p style="color:#64748b;font-size:0.85em;">No pending orders.</p>',
                        unsafe_allow_html=True)

        # ── Section 5: Trade history ──────────────────────────────────────────
        st.markdown('<div class="sh">Trade History</div>', unsafe_allow_html=True)
        if not _trd_df.empty:
            _trd_cols = [c for c in ["ticker","side","qty","entry_price","exit_price",
                                     "realized_pnl","realized_pnl_pct","hold_minutes",
                                     "exit_reason","entry_time","exit_time"]
                         if c in _trd_df.columns]
            _trd_show = _trd_df[_trd_cols].head(50).copy()
            for _tc in ["entry_price","exit_price"]:
                if _tc in _trd_show.columns:
                    _trd_show[_tc] = _trd_show[_tc].apply(
                        lambda v: f"${v:.2f}" if pd.notna(v) else "—"
                    )
            if "realized_pnl" in _trd_show.columns:
                _trd_show["realized_pnl"] = _trd_show["realized_pnl"].apply(
                    lambda v: f"${v:+,.2f}" if pd.notna(v) else "—"
                )
            if "realized_pnl_pct" in _trd_show.columns:
                _trd_show["realized_pnl_pct"] = _trd_show["realized_pnl_pct"].apply(
                    lambda v: f"{v:+.1f}%" if pd.notna(v) else "—"
                )
            st.dataframe(_trd_show, use_container_width=True, hide_index=True)
        else:
            st.markdown('<p style="color:#64748b;font-size:0.85em;">No closed trades yet.</p>',
                        unsafe_allow_html=True)

        # ── Section 6: Performance metrics ────────────────────────────────────
        st.markdown('<div class="sh">Performance Metrics</div>', unsafe_allow_html=True)
        if not _trd_df.empty and len(_trd_df) >= 3:
            _pm1, _pm2, _pm3, _pm4, _pm5, _pm6 = st.columns(6)
            _closed_pnl = _trd_df["realized_pnl"] if "realized_pnl" in _trd_df.columns else pd.Series([])
            _wins_pnl   = _closed_pnl[_closed_pnl > 0]
            _loss_pnl   = _closed_pnl[_closed_pnl < 0]
            _avg_w   = _wins_pnl.mean() if len(_wins_pnl) else 0
            _avg_l   = _loss_pnl.mean() if len(_loss_pnl) else 0
            _pf_val  = abs(_wins_pnl.sum() / _loss_pnl.sum()) if len(_loss_pnl) and _loss_pnl.sum() != 0 else None
            _avg_hld = _trd_df["hold_minutes"].mean() if "hold_minutes" in _trd_df.columns else 0
            _pm1.metric("Avg Win",      f"${_avg_w:+,.2f}")
            _pm2.metric("Avg Loss",     f"${_avg_l:+,.2f}")
            _pm3.metric("Profit Factor", f"{_pf_val:.2f}" if _pf_val else "—")
            _pm4.metric("Best Trade",   f"${_closed_pnl.max():+,.2f}" if len(_closed_pnl) else "—")
            _pm5.metric("Worst Trade",  f"${_closed_pnl.min():+,.2f}" if len(_closed_pnl) else "—")
            _pm6.metric("Avg Hold",     f"{_avg_hld:.0f}m" if _avg_hld else "—")
        else:
            st.markdown('<p style="color:#64748b;font-size:0.85em;">Need at least 3 closed trades for metrics.</p>',
                        unsafe_allow_html=True)

        # ── Section 7: Daily P&L calendar ─────────────────────────────────────
        st.markdown('<div class="sh">Daily P&L</div>', unsafe_allow_html=True)
        if not _day_df.empty and "trade_date" in _day_df.columns and "day_pnl" in _day_df.columns:
            _day_sorted = _day_df.sort_values("trade_date").tail(30)
            _day_colors = ["#16a34a" if v >= 0 else "#dc2626"
                           for v in _day_sorted["day_pnl"]]
            _fig_day = go.Figure(go.Bar(
                x=_day_sorted["trade_date"].astype(str),
                y=_day_sorted["day_pnl"],
                marker_color=_day_colors,
                text=[f"${v:+,.0f}" for v in _day_sorted["day_pnl"]],
                textposition="outside",
            ))
            _fig_day.update_layout(
                height=200, margin=dict(l=0, r=0, t=10, b=0),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="JetBrains Mono", size=11, color="#e2e8f0"),
                xaxis=dict(showgrid=False, color="#64748b", tickangle=-45),
                yaxis=dict(showgrid=True, gridcolor="#1e293b",
                           tickprefix="$", tickformat=",.0f", color="#64748b"),
                showlegend=False,
            )
            st.plotly_chart(_fig_day, use_container_width=True, config={"displayModeBar": False})
        else:
            st.markdown('<p style="color:#64748b;font-size:0.85em;">Daily P&L builds after 4 PM snapshots.</p>',
                        unsafe_allow_html=True)

    except ImportError:
        st.warning("paper_broker module not found. Deploy the latest scanner service.")
    except Exception as _pt_err:
        st.error(f"Paper trading dashboard error: {_pt_err}")

    st.markdown(
        '<div class="disc" style="margin-top:20px;">PAPER TRADING ONLY · NOT REAL MONEY · '
        'FOR STRATEGY RESEARCH PURPOSES</div>',
        unsafe_allow_html=True,
    )
