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
                          get_all_users, delete_user, change_user_password)
from auth import check_password, hash_password, validate_password_strength
# Heavy modules loaded lazily inside their tabs — keeps startup fast

# ── Cached DB helpers (ttl=60s keeps the dashboard snappy without hammering PG) ─

@st.cache_data(ttl=60, show_spinner=False)
def _cached_load_scanner_state() -> dict:
    try:
        from db.database import load_scanner_state
        return load_scanner_state()
    except Exception:
        return {}

@st.cache_data(ttl=60, show_spinner=False)
def _cached_load_alerts(n: int = 100) -> list:
    try:
        from db.database import load_alerts
        return load_alerts(n)
    except Exception:
        return []

@st.cache_data(ttl=30, show_spinner=False)
def _cached_conviction_list(max_age_minutes: int = 90) -> dict:
    try:
        from conviction_engine import get_latest_conviction_list
        return get_latest_conviction_list(max_age_minutes=max_age_minutes)
    except Exception:
        return {"entries": [], "is_stale": True, "is_yesterday": False, "generated_at": None, "session": ""}

# ──────────────────────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def _init_db_once():
    initialize_db()

@st.cache_data(ttl=60, show_spinner=False)
def _cached_scanner_control() -> dict:
    try:
        from db.database import get_scanner_control
        return get_scanner_control()
    except Exception:
        return {"paused": False, "force_scan": False, "current_mode": "UNKNOWN"}

@st.cache_data(ttl=60, show_spinner=False)
def _cached_control_stats() -> dict:
    try:
        from db.database import get_control_stats
        return get_control_stats()
    except Exception:
        return {"signals_today": 0, "alerts_today": 0, "scan_count": 0,
                "last_updated": None, "top_signal": None}

@st.cache_data(ttl=30, show_spinner=False)
def _cached_signal_log_today() -> list:
    try:
        from db.database import get_signal_log
        df = get_signal_log(days=1)
        if df.empty:
            return []
        return df.sort_values("score", ascending=False).drop_duplicates("ticker").head(20).to_dict("records")
    except Exception:
        return []

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
@keyframes pulse-blue {
    0%,100% { box-shadow: 0 0 0 1px rgba(59,130,246,0.15); }
    50%      { box-shadow: 0 0 0 3px rgba(59,130,246,0.3); }
}
@keyframes fadeIn { from { opacity:0; transform:translateY(4px); } to { opacity:1; transform:none; } }
@keyframes live-dot { 0%,100% { opacity:1; } 50% { opacity:0.25; } }
@keyframes glow-green { 0%,100% { box-shadow: 0 0 6px rgba(16,185,129,0.3); } 50% { box-shadow: 0 0 14px rgba(16,185,129,0.55); } }
@keyframes scroll-ticker { 0% { transform: translateX(0); } 100% { transform: translateX(-50%); } }

:root {
    --bg:       #080d14;
    --bg2:      #090e16;
    --bg3:      #0f1824;
    --bgcard:   #101928;
    --bghover:  #162033;
    --border:   #1a2740;
    --borderhi: #253a55;
    --green:    #10b981;
    --green2:   #059669;
    --blue:     #38bdf8;
    --blue2:    #60a5fa;
    --amber:    #3b82f6;
    --amber2:   #60a5fa;
    --red:      #f43f5e;
    --red2:     #fb7185;
    --purple:   #a78bfa;
    --t1:       #e2eaf4;
    --t2:       #8293a8;
    --t3:       #3a5068;
    --tdim:     #1e3048;
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
    background: var(--bg3) !important;
    border: 1px solid var(--border) !important;
    color: var(--t1) !important;
    border-radius: 6px !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.82em !important;
    transition: border-color 0.2s !important;
}
.stTextInput input:focus, .stTextArea textarea:focus {
    border-color: var(--amber) !important;
    box-shadow: 0 0 0 3px rgba(59,130,246,0.12) !important;
}
.stTextInput input::placeholder { color: var(--t3) !important; }

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
    color: #000 !important;
    font-weight: 700 !important;
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
    font-size: 0.79em !important;
    letter-spacing: 0.01em !important;
    color: var(--t3) !important;
    padding: 12px 22px !important;
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
    letter-spacing: 0.07em !important;
    text-transform: uppercase !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 600 !important;
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

/* ── Select boxes ── */
.stSelectbox > div > div {
    background: var(--bg3) !important;
    border-color: var(--border) !important;
    color: var(--t1) !important;
}

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
    background: rgba(245,158,11,0.1) !important;
    border-color: var(--amber) !important;
    color: var(--amber) !important;
}

/* ── Alerts / info boxes ── */
[data-testid="stAlert"] {
    background: var(--bgcard) !important;
    border-color: var(--border) !important;
    color: var(--t2) !important;
}

/* ── Custom components ── */

/* Score pill */
.pill { display:inline-flex; align-items:center; padding:2px 9px; border-radius:4px; font-family:'Inter',sans-serif; font-weight:600; font-size:0.68em; letter-spacing:0.04em; }
.p-sb  { background:rgba(16,185,129,0.12);  color:#10b981; border:1px solid rgba(16,185,129,0.3); }
.p-sp  { background:rgba(5,150,105,0.12);   color:#059669; border:1px solid rgba(5,150,105,0.3); }
.p-wl  { background:rgba(245,158,11,0.12);  color:#f59e0b; border:1px solid rgba(245,158,11,0.3); }
.p-ho  { background:rgba(245,158,11,0.08);  color:#d97706; border:1px solid rgba(245,158,11,0.2); }
.p-tr  { background:rgba(251,146,60,0.1);   color:#f97316; border:1px solid rgba(251,146,60,0.25); }
.p-se  { background:rgba(244,63,94,0.1);    color:#f43f5e; border:1px solid rgba(244,63,94,0.25); }
.p-av  { background:rgba(244,63,94,0.15);   color:#fb7185; border:1px solid rgba(244,63,94,0.35); }

/* Risk flag chips */
.flag { display:inline-flex; align-items:center; background:rgba(245,158,11,0.08); border:1px solid rgba(245,158,11,0.2); color:#f59e0b; padding:1px 7px; border-radius:3px; font-size:0.64em; font-family:'JetBrains Mono',monospace; margin:2px 2px 2px 0; }
.flag.crit { background:rgba(244,63,94,0.1); border-color:rgba(244,63,94,0.25); color:#f43f5e; }
.flag.earn { background:rgba(167,139,250,0.1); border-color:rgba(167,139,250,0.25); color:#a78bfa; }

/* Score bar */
.sbar-track { background:rgba(255,255,255,0.06); border-radius:2px; height:3px; width:100%; }
.sbar-fill { border-radius:2px; height:3px; }

/* Stat rows */
.stat-row { display:flex; justify-content:space-between; align-items:center; padding:5px 0; border-bottom:1px solid rgba(255,255,255,0.04); }
.slbl { color:var(--t3); font-size:0.68em; letter-spacing:0.04em; font-family:'Inter',sans-serif; font-weight:500; }
.sval { color:var(--t1); font-size:0.8em; font-family:'JetBrains Mono',monospace; font-weight:500; }
.sval.g { color:var(--green); } .sval.r { color:var(--red); } .sval.a { color:var(--amber); } .sval.b { color:var(--blue); } .sval.p { color:var(--purple); }

/* Section headers */
.sh { font-family:'Inter',sans-serif; font-size:0.67em; font-weight:600; letter-spacing:0.09em; text-transform:uppercase; color:var(--t3); margin:16px 0 10px; padding-bottom:6px; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:6px; }

/* Info boxes */
.box { background:rgba(255,255,255,0.02); border:1px solid var(--border); border-radius:8px; padding:12px 16px; color:var(--t2); font-size:0.85em; line-height:1.75; }
.box-blue   { border-color:rgba(56,189,248,0.2);  background:rgba(56,189,248,0.05); color:#7dd3fc; }
.box-green  { border-color:rgba(16,185,129,0.2);  background:rgba(16,185,129,0.04); color:#6ee7b7; }
.box-red    { border-color:rgba(244,63,94,0.2);   background:rgba(244,63,94,0.05);  color:#fda4af; }
.box-amber  { border-color:rgba(245,158,11,0.2);  background:rgba(245,158,11,0.05); color:#fcd34d; }
.box-purple { border-color:rgba(167,139,250,0.2); background:rgba(167,139,250,0.05);color:#c4b5fd; }

/* Disclaimer */
.disc { background:transparent; border:1px solid rgba(30,40,64,0.8); border-radius:4px; padding:6px 12px; color:#2d4460; font-size:0.62em; font-family:'JetBrains Mono',monospace; letter-spacing:0.05em; text-align:center; margin:8px 0; }

/* Empty states */
.empty { text-align:center; padding:80px 20px; color:var(--t3); }
.empty .ico { font-size:2.5em; margin-bottom:16px; opacity:0.25; }
.empty h3 { color:var(--t2) !important; letter-spacing:0.04em; font-size:1.3em; font-weight:600; }
.empty p { color:var(--t3); font-size:0.82em; line-height:1.7; }

/* News item */
.news-item { display:flex; align-items:flex-start; gap:10px; padding:8px 0; border-bottom:1px solid rgba(255,255,255,0.04); }
.news-dot { width:5px; height:5px; border-radius:50%; margin-top:7px; flex-shrink:0; }

/* Summary text */
.summary-block { background:rgba(255,255,255,0.02); border-left:2px solid var(--borderhi); border-radius:0 8px 8px 0; padding:14px 16px; color:var(--t2); font-size:0.83em; line-height:1.85; white-space:pre-line; }

/* Sidebar logo */
.axiom-logo { padding:20px 16px 12px; border-bottom:1px solid var(--border); margin-bottom:4px; }
.axiom-logo .name { font-family:'Inter',sans-serif; font-size:1.4em; font-weight:700; color:#e2eaf4; letter-spacing:-0.02em; line-height:1; }
.axiom-logo .sub { font-family:'JetBrains Mono',monospace; font-size:0.58em; color:var(--t3); letter-spacing:0.25em; margin-top:4px; }

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
.earn-warn { background:rgba(167,139,250,0.08); border:1px solid rgba(167,139,250,0.2); border-radius:6px; padding:10px 14px; color:#c4b5fd; font-size:0.78em; font-family:'JetBrains Mono',monospace; margin-top:8px; line-height:1.6; }

/* Scanner result column header */
.result-hdr { display:grid; grid-template-columns:70px 140px 80px 65px 65px 1fr; padding:5px 18px; background:rgba(255,255,255,0.02); border-bottom:1px solid var(--border); font-family:'Inter',sans-serif; font-size:0.6em; letter-spacing:0.08em; color:var(--t3); text-transform:uppercase; font-weight:600; }

/* Win-rate badge */
.wr-badge { display:inline-flex; align-items:center; gap:5px; padding:3px 10px; border-radius:4px; font-family:'JetBrains Mono',monospace; font-size:0.78em; font-weight:600; }
.wr-good  { background:rgba(16,185,129,0.1);  border:1px solid rgba(16,185,129,0.25); color:#10b981; }
.wr-bad   { background:rgba(244,63,94,0.1);   border:1px solid rgba(244,63,94,0.25);  color:#f43f5e; }
.wr-neu   { background:rgba(245,158,11,0.1);  border:1px solid rgba(245,158,11,0.25); color:#f59e0b; }

/* Prediction direction chip */
.dir-long  { background:rgba(16,185,129,0.08);  border:1px solid rgba(16,185,129,0.2);  color:#10b981; padding:1px 7px; border-radius:3px; font-size:0.65em; font-family:'JetBrains Mono',monospace; }
.dir-short { background:rgba(244,63,94,0.08);   border:1px solid rgba(244,63,94,0.2);   color:#f43f5e; padding:1px 7px; border-radius:3px; font-size:0.65em; font-family:'JetBrains Mono',monospace; }

/* Live chart controls */
.chart-bar { display:flex; align-items:center; justify-content:space-between; padding:8px 0 12px; gap:12px; flex-wrap:wrap; }
.live-badge { display:inline-flex; align-items:center; gap:5px; font-family:'JetBrains Mono',monospace; font-size:0.68em; color:var(--green); }
.live-dot { width:6px; height:6px; border-radius:50%; background:var(--green); animation:live-dot 1.4s ease-in-out infinite; }

/* Trade blotter */
.trade-open  { border-left:2px solid rgba(59,130,246,0.7);  background:rgba(59,130,246,0.06);  animation:pulse-blue 3s infinite; }
.trade-win   { border-left:2px solid rgba(34,197,94,0.5);   background:rgba(34,197,94,0.05); }
.trade-loss  { border-left:2px solid rgba(239,68,68,0.5);   background:rgba(239,68,68,0.05); }

/* Control panel */
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
.ctrl-status .cs-dot { width: 16px; height: 16px; border-radius: 50%; flex-shrink: 0; margin-top: 1px; }
.ctrl-status .cs-label { font-family: 'Inter', sans-serif; font-size: 1.05em; font-weight: 700; color: var(--t1); }
.ctrl-status .cs-sub { font-family: 'JetBrains Mono', monospace; font-size: 0.72em; color: var(--t3); padding-left: 26px; }
.ctrl-summary { background: var(--bgcard); border: 1px solid var(--border); border-radius: 10px; padding: 4px 0; margin-top: 8px; }
.ctrl-row { display: flex; justify-content: space-between; align-items: center; padding: 10px 16px; border-bottom: 1px solid rgba(255,255,255,0.04); }
.ctrl-row:last-child { border-bottom: none; }
.ctrl-lbl { font-family: 'Inter', sans-serif; font-size: 0.78em; color: var(--t3); font-weight: 500; }
.ctrl-val { font-family: 'JetBrains Mono', monospace; font-size: 0.85em; color: var(--t1); font-weight: 600; }
.ctrl-val.g { color: var(--green); }
.ctrl-val.a { color: var(--amber); }
.ctrl-val.r { color: var(--red); }
.ctrl-section { font-family: 'Inter', sans-serif; font-size: 0.65em; font-weight: 600; letter-spacing: 0.1em; text-transform: uppercase; color: var(--t3); margin: 20px 0 8px; }

/* Scrollbars */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--borderhi); }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS — ALL DEFINED BEFORE UI
# ══════════════════════════════════════════════════════════════════════════════

def pill_class(signal):
    return {"Strong Buy Candidate":"p-sb","Speculative Buy":"p-sp","Watchlist":"p-wl",
            "Hold":"p-ho","Trim":"p-tr","Sell":"p-se","Avoid":"p-av"}.get(signal,"p-ho")

def sig_color(signal):
    return {"Strong Buy Candidate":"#10b981","Speculative Buy":"#34d399","Watchlist":"#f59e0b",
            "Hold":"#d97706","Trim":"#f97316","Sell":"#f43f5e","Avoid":"#fb7185"}.get(signal,"#475569")

def score_col(s):
    if s>=72: return "#10b981"
    if s>=58: return "#34d399"
    if s>=45: return "#f59e0b"
    if s>=33: return "#f97316"
    if s>=22: return "#f43f5e"
    return "#be123c"

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
      <circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="rgba(255,255,255,0.06)" stroke-width="6"/>
      <circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="{color}" stroke-width="6"
              stroke-dasharray="{dash:.1f} {circ:.1f}" stroke-linecap="round"
              transform="rotate(-90 {cx} {cy})"
              style="filter:drop-shadow(0 0 6px {color}88)"/>
      <text x="{cx}" y="{cy+1}" text-anchor="middle" dominant-baseline="middle"
            font-family="'JetBrains Mono',monospace" font-size="16" font-weight="600" fill="{color}">{score:.0f}</text>
      <text x="{cx}" y="{cy+14}" text-anchor="middle" dominant-baseline="middle"
            font-family="'JetBrains Mono',monospace" font-size="7" fill="rgba(255,255,255,0.2)">/100</text>
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
                <span style="font-family:'Inter',sans-serif;font-size:2em;font-weight:800;color:var(--t1);letter-spacing:0.04em;">{ticker}</span>
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
            <span style="font-family:'Inter',sans-serif;font-size:1.8em;font-weight:800;color:var(--t1);letter-spacing:0.02em;">{ticker}</span>
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
        area_color = "#f59e0b"
        fig.add_trace(go.Scatter(
            x=hist.index, y=close_vals, name="Price",
            line=dict(color=area_color, width=2),
            fill="tozeroy",
            fillcolor="rgba(245,158,11,0.07)",
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
        gridcolor="rgba(255,255,255,0.04)", showgrid=True, zeroline=False,
        showline=False, tickfont=dict(size=9, family="JetBrains Mono", color="#3a5068"),
    )
    fig.update_layout(
        height=420,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(12,18,28,0.98)",
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
            from data.market_data import get_chart_data
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
    Shows the login page and calls st.stop() if not.
    """
    token = st.session_state.get("_session_token")
    if token:
        user = validate_session(token)
        if user:
            return user
        st.session_state.pop("_session_token", None)

    _, col, _ = st.columns([1, 1.6, 1])
    with col:
        st.markdown("""
        <div style="text-align:center;padding:40px 0 28px;">
          <div style="font-family:'Inter',sans-serif;font-size:2em;font-weight:700;
                      color:#f59e0b;letter-spacing:-0.02em;line-height:1;">Axiom</div>
          <div style="font-family:'JetBrains Mono',monospace;font-size:0.58em;
                      color:var(--t3);letter-spacing:0.25em;margin-top:4px;">TERMINAL</div>
        </div>""", unsafe_allow_html=True)

        with st.form("_login_form", clear_on_submit=False):
            username  = st.text_input("Username", placeholder="username")
            password  = st.text_input("Password", placeholder="••••••••", type="password")
            submitted = st.form_submit_button("Sign in", use_container_width=True, type="primary")
        if submitted:
            u = get_user_by_username(username)
            if u and check_password(password, u["password_hash"]):
                tok = create_session(u["id"])
                update_last_login(u["id"])
                st.session_state["_session_token"] = tok
                st.rerun()
            else:
                st.error("Invalid username or password.")

        st.markdown('<div class="disc" style="margin-top:20px;">Research tool · Not financial advice</div>',
                    unsafe_allow_html=True)
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# INIT
# ══════════════════════════════════════════════════════════════════════════════
_init_db_once()
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

    # ── Market Regime Badge ───────────────────────────────────────────────────
    try:
        from analysis.regime import get_current_regime, regime_label
        _regime_data = get_current_regime()
        _regime      = _regime_data.get("regime", "UNKNOWN")
        _rlabel, _rcolor, _rsymbol = regime_label(_regime)
        _r_iwm   = _regime_data.get("iwm_price", 0)
        _r_vol   = _regime_data.get("volatility_20d", 0)
        _r_adx   = _regime_data.get("adx_14", 0)
        st.markdown(
            f'<div style="margin:8px 4px 4px;padding:8px 12px;border-radius:6px;'
            f'background:{_rcolor}18;border:1px solid {_rcolor}40;">'
            f'<div style="display:flex;align-items:center;gap:6px;">'
            f'<span style="font-size:1.0em;">{_rsymbol}</span>'
            f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.72em;'
            f'color:{_rcolor};font-weight:600;letter-spacing:0.05em;">{_rlabel.upper()}</span>'
            f'</div>'
            f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.6em;color:#64748b;margin-top:3px;">'
            f'IWM ${_r_iwm:.2f} · ADX {_r_adx:.0f} · Vol {_r_vol:.1f}%'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    except Exception:
        pass

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
            from core.scanner import scan_ticker
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

    st.markdown('<div style="padding:0 4px;"><div class="sh" style="margin-top:12px;">Conviction List</div></div>', unsafe_allow_html=True)

    _cv_sidebar_data = _cached_conviction_list(max_age_minutes=90)
    _cv_ts = _cv_sidebar_data.get("generated_at")
    _cv_sidebar_stale = _cv_sidebar_data.get("is_stale", True)
    _cv_sidebar_n = len(_cv_sidebar_data.get("entries", []))

    _cv_ts_str = "—"
    if _cv_ts:
        try:
            _cv_dt = _cv_ts if isinstance(_cv_ts, datetime) else datetime.fromisoformat(str(_cv_ts).replace("Z",""))
            _cv_ts_str = _cv_dt.strftime("%-I:%M %p")
        except Exception:
            _cv_ts_str = str(_cv_ts)[:16]

    _cv_btn_label = (
        f"CONVICTION ({_cv_sidebar_n} picks)" if _cv_sidebar_n
        else ("GENERATE CONVICTION LIST" if _cv_sidebar_stale else "CONVICTION LIST — Live")
    )
    st.markdown(
        f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.62em;'
        f'color:var(--t3);padding:2px 4px 6px;">Last updated: {_cv_ts_str}</div>',
        unsafe_allow_html=True
    )
    if st.button(_cv_btn_label, key="conv_sidebar_btn",
                 use_container_width=True, type="primary"):
        # If stale / empty, generate fresh; otherwise toggle visibility
        if _cv_sidebar_stale or _cv_sidebar_n == 0:
            with st.spinner("Generating conviction list..."):
                try:
                    from conviction_engine import generate_live_conviction_list
                    generate_live_conviction_list(session="market")
                    _cached_conviction_list.clear()  # bust cache so panel loads fresh data
                except Exception as _sbcv_e:
                    st.error(f"Generation failed: {_sbcv_e}")
        st.session_state.show_conviction = True
        st.rerun()


# ── Session-state: scoring weights (loaded from saved config or defaults) ─────
if "scoring_weights" not in st.session_state:
    _overrides = _load_config_overrides()
    st.session_state["scoring_weights"] = _overrides.get("scoring_weights", dict(SCORING_WEIGHTS))

# ══════════════════════════════════════════════════════════════════════════════
# LIVE FRAGMENT FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _render_conviction_panel(cv_data: dict):
    """Render the conviction list cards + PDF download."""
    entries = cv_data.get("entries", [])
    gen_at  = cv_data.get("generated_at")
    is_yesterday = cv_data.get("is_yesterday", False)
    is_stale = cv_data.get("is_stale", False)

    # Format header timestamps
    try:
        if gen_at:
            _gdt = gen_at if isinstance(gen_at, datetime) else datetime.fromisoformat(str(gen_at).replace("Z",""))
            gen_str = _gdt.strftime("%-I:%M %p ET")
            # Next update = gen_at + 30 min
            next_dt = _gdt + timedelta(minutes=30)
            next_str = next_dt.strftime("%-I:%M %p ET")
        else:
            gen_str = "—"
            next_str = "—"
    except Exception:
        gen_str = str(gen_at)[:16] if gen_at else "—"
        next_str = "—"

    date_str = datetime.now().strftime("%A %B %-d %Y")

    if is_yesterday:
        st.markdown(
            '<div style="background:rgba(245,158,11,0.12);border:1px solid rgba(245,158,11,0.3);'
            'border-radius:8px;padding:8px 14px;margin-bottom:10px;font-family:\'JetBrains Mono\','
            'monospace;font-size:0.72em;color:#d97706;">'
            'Market closed — showing last session conviction list. '
            'Next update: Tomorrow 9:30 AM ET</div>',
            unsafe_allow_html=True
        )
    elif is_stale:
        st.markdown(
            '<div style="background:rgba(245,158,11,0.12);border:1px solid rgba(245,158,11,0.3);'
            'border-radius:8px;padding:8px 14px;margin-bottom:10px;font-family:\'JetBrains Mono\','
            'monospace;font-size:0.72em;color:#d97706;">Data may be stale. Refresh to update.</div>',
            unsafe_allow_html=True
        )

    st.markdown(
        f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.68em;color:var(--t2);'
        f'margin-bottom:8px;">'
        f'<span style="font-weight:700;font-size:1.1em;">CONVICTION LIST — {date_str}</span><br>'
        f'Generated: {gen_str} &nbsp;|&nbsp; Next update: {next_str}</div>',
        unsafe_allow_html=True
    )

    if not entries:
        st.markdown(
            '<div style="background:var(--bgcard);border:1px solid var(--border);border-radius:8px;'
            'padding:20px;text-align:center;color:#64748b;font-family:\'JetBrains Mono\',monospace;'
            'font-size:0.8em;">No high-conviction setups right now. '
            'Scanner needs score >= 75 Strong/Spec Buy signals with catalysts.</div>',
            unsafe_allow_html=True
        )
        return

    _CONV_COLOR = {"Very High": "#16a34a", "High": "#1d6fa5", "Medium": "#c2610f", "Low": "#94a3b8"}
    _CAT_COLOR  = {"Very Strong": "#16a34a", "Strong": "#1d6fa5", "Moderate": "#c2610f", "Weak": "#94a3b8"}

    # Two-column grid
    _cols = st.columns(2)
    for i, e in enumerate(entries):
        with _cols[i % 2]:
            conv_c = _CONV_COLOR.get(e.get("ai_conviction",""), "#94a3b8")
            cat_c  = _CAT_COLOR.get(e.get("ai_catalyst_quality",""), "#94a3b8")
            pct = e.get("pct_change_today") or 0
            vol = e.get("volume_ratio") or e.get("rvol") or 1.0
            si  = e.get("short_interest") or 0
            fl  = e.get("float_shares") or 0
            score = e.get("composite") or e.get("score") or e.get("conviction") or 0
            pct_c = "#16a34a" if float(pct) >= 0 else "#f85149"
            entry = e.get("entry") or 0
            stop  = e.get("stop_loss") or 0
            t1    = e.get("target_1") or 0
            st.markdown(f"""
            <div style="background:var(--bgcard);border:1px solid var(--border);border-radius:10px;
                        padding:14px 16px;margin-bottom:12px;">
              <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px;">
                <span style="font-family:'JetBrains Mono',monospace;font-size:1.0em;font-weight:700;
                             color:#58a6ff;">RANK {e.get('rank','?')} — {e.get('ticker','')}</span>
                <span style="font-family:'JetBrains Mono',monospace;font-size:0.78em;color:var(--t2);">Score: {float(score):.0f}</span>
              </div>
              <div style="font-size:0.72em;color:var(--t3);margin-bottom:8px;">
                {e.get('signal_label','')}
                {f' &nbsp;|&nbsp; SI: {float(si):.1f}%' if si else ''}
                {f' &nbsp;|&nbsp; Float: {float(fl):.0f}M' if fl else ''}
                &nbsp;|&nbsp; Today: <span style="color:{pct_c};">{float(pct):+.1f}%</span>
                &nbsp;|&nbsp; Vol: {float(vol):.1f}x
              </div>
              <div style="margin-bottom:8px;">
                <span style="font-size:0.72em;font-weight:600;color:{conv_c};font-family:'JetBrains Mono',monospace;">
                  Conviction: {e.get('ai_conviction','')}
                </span>
                &nbsp;&nbsp;
                <span style="font-size:0.72em;color:{cat_c};font-family:'JetBrains Mono',monospace;">
                  Catalyst: {e.get('ai_catalyst_quality','')}
                </span>
              </div>
              <div style="font-size:0.78em;color:var(--t2);margin-bottom:8px;line-height:1.5;">
                {e.get('ai_key_reason') or e.get('reasoning','—')}
              </div>
              <div style="font-size:0.72em;color:var(--t3);margin-bottom:4px;">
                <b>Entry:</b> {e.get('ai_entry_suggestion') or f'${float(entry):.2f}'}
                &nbsp;&nbsp;
                <b>Stop:</b> {float(e.get('ai_stop_pct') or e.get('stop_pct') or 0):.1f}% below
                &nbsp;&nbsp;
                <b>Target:</b> +{float(e.get('ai_target_pct') or 0):.0f}% upside
              </div>
              {f'<div style="font-size:0.70em;color:#64748b;margin-bottom:4px;"><b>Risk:</b> {e.get("ai_risk","")}</div>' if e.get('ai_risk') else ''}
              <div style="font-size:0.68em;font-family:'JetBrains Mono',monospace;color:#1d6fa5;margin-top:6px;">
                Time: {e.get('ai_time_sensitivity','—')} &nbsp;|&nbsp; Hold: {e.get('hold_type','')}
              </div>
            </div>
            """, unsafe_allow_html=True)

    # PDF download
    try:
        from conviction_engine import build_conviction_pdf
        _pdf_bytes = build_conviction_pdf(entries, generated_at=gen_at, session=cv_data.get("session","market"))
        if _pdf_bytes:
            _pdf_date = datetime.now().strftime("%Y-%m-%d")
            st.download_button(
                label="DOWNLOAD PDF",
                data=_pdf_bytes,
                file_name=f"AxiomConvictionList_{_pdf_date}.pdf",
                mime="application/pdf",
                use_container_width=True,
                key="conviction_pdf_download"
            )
    except Exception as _pdfe:
        pass


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
                       "Full ticker scan, predictions active."),
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

    st.markdown('<div class="ctrl-section">Conviction List</div>', unsafe_allow_html=True)

    _cv_col1, _cv_col2 = st.columns([3, 1])
    _cv_data = _cached_conviction_list(max_age_minutes=90)
    _cv_is_stale = _cv_data.get("is_stale", True)
    _cv_n        = len(_cv_data.get("entries", []))
    _cv_gen      = _cv_data.get("generated_at")
    _cv_stale_info = ""
    if _cv_gen:
        try:
            _cv_gdt = _cv_gen if isinstance(_cv_gen, datetime) else datetime.fromisoformat(str(_cv_gen).replace("Z",""))
            _cv_stale_info = _cv_gdt.strftime("%-I:%M %p")
        except Exception:
            _cv_stale_info = str(_cv_gen)[:16]

    with _cv_col1:
        _cv_label = (
            f"CONVICTION ({_cv_n} picks) — Live" if _cv_n and not _cv_is_stale
            else ("GENERATE CONVICTION LIST" if not _cv_n else "CONVICTION — Refresh Now")
        )
        if st.button(_cv_label, use_container_width=True, key="conv_ctrl_btn", type="primary"):
            # Always generate fresh when empty or stale
            if _cv_is_stale or _cv_n == 0:
                with st.spinner("Running conviction engine..."):
                    try:
                        from conviction_engine import generate_live_conviction_list
                        _new_entries = generate_live_conviction_list(session="market")
                        _cv_data = {"entries": _new_entries, "is_stale": False,
                                    "is_yesterday": False, "generated_at": datetime.utcnow()}
                        _cached_conviction_list.clear()  # invalidate so next load sees fresh DB rows
                        st.session_state.show_conviction = True
                    except Exception as _cve:
                        st.error(f"Generation failed: {_cve}")
            else:
                st.session_state.show_conviction = not st.session_state.get("show_conviction", False)
            st.rerun()
    with _cv_col2:
        if _cv_stale_info:
            st.markdown(f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.65em;color:var(--t3);padding-top:10px;">{_cv_stale_info}</div>', unsafe_allow_html=True)

    if st.session_state.get("show_conviction", False):
        _render_conviction_panel(_cv_data)


@st.fragment(run_every=10)
def _live_alerts_feed():
    # Use cached wrappers to avoid a DB round-trip on every 10-second fragment refresh
    alert_log = list(reversed(_cached_load_alerts(100)))
    if not alert_log:
        try:
            import json as _json
            with open("alert_log.json") as f:
                alert_log = list(reversed(_json.load(f)))
        except Exception:
            pass

    scanner_state = _cached_load_scanner_state()
    if not scanner_state:
        try:
            import json as _json
            with open("scanner_state.json") as f:
                scanner_state = _json.load(f)
        except Exception:
            pass

    scan_count   = scanner_state.get("scan_count", 0)
    last_updated = scanner_state.get("last_updated", "—")
    is_running   = scan_count > 0
    _state_regime = scanner_state.get("current_regime", "")

    _univ_count = 0
    try:
        from universe_manager import get_universe_size
        _univ_count = get_universe_size()
    except Exception:
        _univ_count = scanner_state.get("universe_size", 0)

    col_s1, col_s2, col_s3, col_s4 = st.columns(4)
    col_s1.metric("Scanner Status", "RUNNING" if is_running else "WAITING")
    col_s2.metric("Scans Today",    scan_count)
    col_s3.metric("Last Scan",      last_updated[11:16] if len(last_updated) > 11 else "—")
    col_s4.metric("Universe",       f"{_univ_count:,}" if _univ_count else "—")

    if _state_regime:
        try:
            from analysis.regime import regime_label as _rl
            _sl, _sc, _ss = _rl(_state_regime)
            st.markdown(
                f'<div style="display:inline-block;margin:4px 0 0;padding:3px 10px;border-radius:4px;'
                f'background:{_sc}15;border:1px solid {_sc}30;font-family:\'JetBrains Mono\',monospace;'
                f'font-size:0.7em;color:{_sc};font-weight:600;">{_ss} REGIME: {_sl.upper()}</div>',
                unsafe_allow_html=True,
            )
        except Exception:
            pass

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

        # Regime + universe size header badges
        _wl_regime   = wl_stats.get("regime", "")
        _wl_univ     = wl_stats.get("universe_size", 0)
        _wl_scored   = wl_stats.get("factor_scored", 0)
        _wl_mr_count = len(wl_stats.get("mean_rev_setups", []))
        if _wl_regime or _wl_univ:
            try:
                from analysis.regime import regime_label as _rlb
                _wlrl, _wlrc, _wlrs = _rlb(_wl_regime) if _wl_regime else ("—", "#94a3b8", "—")
            except Exception:
                _wlrl, _wlrc, _wlrs = "—", "#94a3b8", "—"
            st.markdown(
                f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px;">'
                f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.68em;padding:2px 8px;'
                f'border-radius:4px;background:{_wlrc}15;color:{_wlrc};border:1px solid {_wlrc}30;">'
                f'{_wlrs} {_wlrl}</span>'
                + (f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.68em;padding:2px 8px;'
                   f'border-radius:4px;background:#1d6fa515;color:#1d6fa5;border:1px solid #1d6fa530;">'
                   f'{_wl_univ:,} universe · {_wl_scored} factor-scored</span>' if _wl_univ else '')
                + (f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.68em;padding:2px 8px;'
                   f'border-radius:4px;background:#c2610f15;color:#c2610f;border:1px solid #c2610f30;">'
                   f'↔ {_wl_mr_count} mean-rev setup{"s" if _wl_mr_count != 1 else ""}</span>' if _wl_mr_count else '')
                + f'</div>',
                unsafe_allow_html=True
            )

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

        # Mean-reversion setups panel
        _mr_setups = wl_stats.get("mean_rev_setups", [])
        if _mr_setups:
            st.markdown(
                '<div style="margin-top:10px;padding:10px 14px;background:rgba(194,97,15,0.04);'
                'border:1px solid rgba(194,97,15,0.2);border-radius:8px;">'
                '<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.68em;color:#c2610f;'
                'font-weight:600;letter-spacing:0.08em;margin-bottom:6px;">↔ MEAN-REVERSION SETUPS</div>',
                unsafe_allow_html=True
            )
            _mr_cols = st.columns(min(len(_mr_setups), 5))
            for _mrc, _mrt in zip(_mr_cols, _mr_setups[:5]):
                _mrt_ticker = _mrt if isinstance(_mrt, str) else _mrt.get("ticker", str(_mrt))
                _mrt_score  = _mrt.get("composite", 0) if isinstance(_mrt, dict) else 0
                _mrt_z      = _mrt.get("z_sma20", 0) if isinstance(_mrt, dict) else 0
                _mrc.markdown(
                    f'<div style="text-align:center;font-family:\'JetBrains Mono\',monospace;">'
                    f'<div style="font-size:0.85em;color:#1d6fa5;font-weight:600;">{_mrt_ticker}</div>'
                    + (f'<div style="font-size:0.65em;color:#c2610f;">{_mrt_score:.0f}pt · z{_mrt_z:+.1f}</div>' if _mrt_score else '')
                    + '</div>',
                    unsafe_allow_html=True
                )
            st.markdown('</div>', unsafe_allow_html=True)

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


@st.fragment(run_every=15)
def _terminal_dashboard():
    """3-panel trading terminal: left watchlist · center live chart · right stats."""

    # ── Data (all cached) ─────────────────────────────────────────────────────
    _ctl = _cached_scanner_control()
    _sts = _cached_control_stats()

    _cur_mode   = _ctl.get("current_mode", "UNKNOWN")
    _paused     = _ctl.get("paused", False)
    _force_scan = _ctl.get("force_scan", False)
    _scan_count = _sts.get("scan_count", 0)
    _last_upd   = _sts.get("last_updated")

    _scanner_state = _cached_load_scanner_state()
    _univ_size     = _scanner_state.get("universe_size", 0)

    _last_scan_str = "—"
    _mins_ago = None
    if _last_upd:
        try:
            _lu = datetime.fromisoformat(str(_last_upd).replace("Z", "+00:00"))
            _lu_naive = _lu.replace(tzinfo=None) if _lu.tzinfo else _lu
            _mins_ago = int((datetime.utcnow() - _lu_naive).total_seconds() / 60)
            _last_scan_str = _lu_naive.strftime("%H:%M:%S ET")
        except Exception:
            pass

    # Today's signals
    _today_signals = _cached_signal_log_today()

    # Regime
    _regime_str   = "SCANNING"
    _regime_color = "#f59e0b"
    _vix_str = "—"; _adv_str = "—"; _breadth_str = "—"
    try:
        from analysis.regime import get_current_regime as _gcr, regime_label as _rl
        _rd = _gcr()
        _rlbl, _rcolor, _ = _rl(_rd.get("regime", "UNKNOWN"))
        _regime_str   = _rlbl.upper().replace("_", " ")
        _regime_color = _rcolor
        _vix_str  = f"{_rd.get('volatility_20d', 0):.1f}"
        _adr      = _rd.get("adv_dec_ratio") or _rd.get("adv_dec")
        _adv_str  = f"{float(_adr):.1f}" if _adr else "—"
        _b = _rd.get("breadth_pct", 0.5)
        _breadth_str = "NARROW" if _b < 0.35 else ("WIDE" if _b > 0.65 else "NORMAL")
    except Exception:
        pass

    # Conviction
    _cv_data    = _cached_conviction_list(max_age_minutes=90)
    _cv_entries = _cv_data.get("entries", [])

    # ── Signal label helper ──────────────────────────────────────────────────
    def _sig_display(label: str):
        if label in ("Strong Buy Candidate", "Speculative Buy"):
            return "CONVICTION", "#0d9488"
        if "Buy" in label:
            return "BUY SIGNAL", "#5eead4"
        if label == "Watchlist":
            return "WATCHLIST", "#475569"
        return label.upper()[:12], "#475569"

    # ── Ticker tape ──────────────────────────────────────────────────────────
    if _today_signals:
        _tape_parts = []
        for _s in _today_signals[:16]:
            _t  = _s.get("ticker", "")
            _sc = int(_s.get("score", 0))
            _lbl_short, _lbl_color = _sig_display(_s.get("signal_label", ""))
            _pct1 = _s.get("pct_change_1hr") or _s.get("pct_change_1day")
            _pct_html = (
                f'<span style="color:{"#10b981" if _pct1 >= 0 else "#f43f5e"};margin-left:3px;">'
                f'{_pct1:+.1f}%</span>'
                if _pct1 is not None else ""
            )
            _tape_parts.append(
                f'<span style="color:#e2eaf4;font-weight:600;">{_t}</span>{_pct_html}'
                f'<span style="color:{_lbl_color};margin-left:5px;font-size:0.85em;"> Score: {_sc}</span>'
            )
        _tape_html = ' &nbsp;<span style="color:#1e3048;margin:0 4px">·</span>&nbsp; '.join(_tape_parts * 2)
        st.markdown(
            f'<div style="background:#060b11;border-bottom:1px solid #1a2740;border-top:1px solid #1a2740;'
            f'padding:6px 0;overflow:hidden;margin:-0.5rem -1.5rem 1.2rem;'
            f'font-family:\'JetBrains Mono\',monospace;font-size:0.71em;">'
            f'<div style="white-space:nowrap;animation:scroll-ticker 50s linear infinite;'
            f'display:inline-block;padding-left:100%;">'
            f'{_tape_html}</div></div>',
            unsafe_allow_html=True,
        )

    # ── Header bar ────────────────────────────────────────────────────────────
    _live_dot = "#22c55e" if not _paused else "#3b82f6"
    _live_lbl = "LIVE" if not _paused else "PAUSED"
    _stocks_n = _univ_size or len(_today_signals)
    _today_n  = _sts.get("signals_today", 0)
    _alerts_n = _sts.get("alerts_today", 0)

    st.markdown(
        f'<div style="display:flex;align-items:center;justify-content:space-between;'
        f'padding:2px 0 10px;flex-wrap:wrap;gap:8px;">'
        f'<div style="display:flex;align-items:center;gap:10px;">'
        f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.9em;'
        f'font-weight:700;color:#e2eaf4;letter-spacing:0.06em;">AXIOM TERMINAL</span>'
        f'<span style="display:inline-flex;align-items:center;gap:4px;'
        f'background:rgba(34,197,94,0.1);border:1px solid rgba(34,197,94,0.3);'
        f'padding:2px 8px;border-radius:3px;font-family:\'JetBrains Mono\',monospace;'
        f'font-size:0.58em;color:{_live_dot};font-weight:700;letter-spacing:0.08em;">'
        f'<span style="width:5px;height:5px;border-radius:50%;background:{_live_dot};'
        f'display:inline-block;animation:live-dot 1.5s ease-in-out infinite;"></span>'
        f'{_live_lbl}</span>'
        f'<span style="display:inline-flex;align-items:center;gap:4px;'
        f'background:{_regime_color}18;border:1px solid {_regime_color}44;'
        f'padding:2px 8px;border-radius:3px;font-family:\'JetBrains Mono\',monospace;'
        f'font-size:0.58em;color:{_regime_color};font-weight:700;">● {_regime_str}</span>'
        f'</div>'
        f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.6em;color:#3a5068;'
        f'display:flex;gap:14px;">'
        f'<span><span style="color:#22c55e;">{_today_n}</span> signals</span>'
        f'<span><span style="color:#3b82f6;">{_alerts_n}</span> alerts</span>'
        f'<span><span style="color:#8293a8;">{_stocks_n:,}</span> stocks</span>'
        f'<span>Last: <span style="color:#8293a8;">{_last_scan_str}</span></span>'
        f'</div></div>',
        unsafe_allow_html=True,
    )

    # ── 3-column layout: watchlist | live chart | stats ───────────────────────
    _cl, _cc, _cr = st.columns([22, 50, 28], gap="small")

    # ═══════════════════════════ LEFT: WATCHLIST ══════════════════════════════
    with _cl:
        _wl_rows = ""
        if _today_signals:
            for _sig in _today_signals[:14]:
                _t    = _sig.get("ticker", "")
                _sc   = int(_sig.get("score", 0))
                _pr   = _sig.get("price_at_signal") or 0
                _lbl  = _sig.get("signal_label", "")
                _pct1 = _sig.get("pct_change_1hr") or _sig.get("pct_change_1day")
                _pct_html = (
                    f'<span style="color:{"#22c55e" if _pct1>=0 else "#ef4444"};">{_pct1:+.1f}%</span>'
                    if _pct1 is not None else '<span style="color:#2d4460;">—</span>'
                )
                _pr_str   = f"${_pr:.2f}" if _pr else "—"
                _sc_color = "#22c55e" if _sc >= 75 else "#3b82f6" if _sc >= 60 else "#8293a8"
                _wl_rows += (
                    f'<div style="display:grid;grid-template-columns:50px 1fr 46px 34px;'
                    f'padding:7px 12px;align-items:center;border-bottom:1px solid #0a1220;'
                    f'font-family:\'JetBrains Mono\',monospace;font-size:0.71em;">'
                    f'<span style="color:#e2eaf4;font-weight:700;">{_t}</span>'
                    f'<span style="color:#5c7a99;">{_pr_str}</span>'
                    f'<span>{_pct_html}</span>'
                    f'<span style="color:{_sc_color};font-weight:700;text-align:right;">{_sc}</span>'
                    f'</div>'
                )
        else:
            _wl_rows = (
                '<div style="padding:40px 12px;text-align:center;color:#2d4460;'
                'font-family:\'JetBrains Mono\',monospace;font-size:0.68em;line-height:1.8;">'
                'No signals yet<br>Market opens 9:30 AM ET</div>'
            )

        st.markdown(
            f'<div style="border:1px solid #1a2740;border-radius:8px;overflow:hidden;background:#101928;">'
            f'<div style="padding:8px 12px;border-bottom:1px solid #1a2740;'
            f'display:flex;align-items:center;justify-content:space-between;">'
            f'<div style="display:flex;align-items:center;gap:6px;">'
            f'<span style="width:6px;height:6px;border-radius:50%;background:#22c55e;'
            f'display:inline-block;animation:live-dot 2s ease-in-out infinite;"></span>'
            f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.56em;'
            f'font-weight:600;letter-spacing:0.12em;color:#3a5068;">SIGNALS · TODAY</span>'
            f'</div>'
            f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.56em;color:#2d4460;">'
            f'{len(_today_signals)}</span></div>'
            f'<div style="display:grid;grid-template-columns:50px 1fr 46px 34px;'
            f'padding:4px 12px;font-family:\'JetBrains Mono\',monospace;font-size:0.52em;'
            f'color:#2d4460;letter-spacing:0.1em;text-transform:uppercase;">'
            f'<span>SYM</span><span>PRICE</span><span>CHG</span>'
            f'<span style="text-align:right;">SCR</span></div>'
            f'{_wl_rows}'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ════════════════════════ CENTER: LIVE CHART ═════════════════════════════
    with _cc:
        _top = _cv_entries[0] if _cv_entries else (_today_signals[0] if _today_signals else None)
        _pick_ticker = (_top.get("ticker") if _top else None) or ""

        if _pick_ticker:
            _pick_score  = int(_top.get("composite") or _top.get("score") or _top.get("conviction") or 0)
            _pick_pr     = _top.get("price_at_signal") or 0
            _pick_pct    = _top.get("pct_change_1hr") or _top.get("pct_change_1day")
            _pick_reason = (_top.get("ai_summary") or "")[:90]
            _pr_color    = "#22c55e" if (_pick_pct or 0) >= 0 else "#ef4444"
            _pr_arrow    = "▲" if (_pick_pct or 0) >= 0 else "▼"

            _pr_html = (
                f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:1.05em;'
                f'font-weight:600;color:{_pr_color};">${_pick_pr:.2f}</span>'
                f'<span style="margin-left:8px;font-family:\'JetBrains Mono\',monospace;'
                f'font-size:0.8em;color:{_pr_color};">{_pr_arrow}{abs(_pick_pct):.2f}%</span>'
            ) if _pick_pr else ""

            st.markdown(
                f'<div style="background:#101928;border:1px solid #1a2740;border-radius:8px;'
                f'overflow:hidden;margin-bottom:6px;">'
                f'<div style="padding:10px 14px;border-bottom:1px solid #1a2740;'
                f'display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;">'
                f'<div style="display:flex;align-items:center;gap:12px;">'
                f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:1.35em;'
                f'font-weight:700;color:#e2eaf4;letter-spacing:0.04em;">{_pick_ticker}</span>'
                f'{_pr_html}</div>'
                f'<div style="display:flex;align-items:center;gap:8px;">'
                f'<span style="background:rgba(34,197,94,0.1);border:1px solid rgba(34,197,94,0.25);'
                f'padding:3px 10px;border-radius:4px;font-family:\'JetBrains Mono\',monospace;'
                f'font-size:0.65em;color:#22c55e;font-weight:700;">SCORE {_pick_score}</span>'
                f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.58em;'
                f'color:#3a5068;letter-spacing:0.06em;">TOP CONVICTION BUY</span>'
                f'</div></div>'
                + (f'<div style="padding:5px 14px 8px;font-family:\'Inter\',sans-serif;'
                   f'font-size:0.7em;color:#5c7a99;line-height:1.5;">{_pick_reason}</div>'
                   if _pick_reason else "")
                + f'</div>',
                unsafe_allow_html=True,
            )

            try:
                from data.market_data import get_chart_data as _gcd
                _cdf = _gcd(_pick_ticker, period="1d", interval="5m")
                if _cdf is not None and not _cdf.empty and "close" in _cdf.columns:
                    _opens  = _cdf.get("open",  _cdf["close"])
                    _highs  = _cdf.get("high",  _cdf["close"])
                    _lows   = _cdf.get("low",   _cdf["close"])
                    _closes = _cdf["close"]
                    _times  = _cdf.index

                    _fig_chart = go.Figure()
                    _fig_chart.add_trace(go.Candlestick(
                        x=_times,
                        open=_opens, high=_highs, low=_lows, close=_closes,
                        increasing_line_color="#22c55e",
                        decreasing_line_color="#ef4444",
                        increasing_fillcolor="rgba(34,197,94,0.65)",
                        decreasing_fillcolor="rgba(239,68,68,0.65)",
                        line=dict(width=1),
                        showlegend=False,
                        name=_pick_ticker,
                    ))
                    if "vwap" in _cdf.columns:
                        _fig_chart.add_trace(go.Scatter(
                            x=_times, y=_cdf["vwap"],
                            mode="lines",
                            line=dict(color="#3b82f6", width=1.2, dash="dot"),
                            name="VWAP", showlegend=False,
                        ))
                    _fig_chart.update_layout(
                        height=360,
                        margin=dict(l=0, r=0, t=4, b=0),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="#0b0f19",
                        font=dict(family="JetBrains Mono", size=10, color="#3a5068"),
                        xaxis=dict(
                            showgrid=False, color="#3a5068",
                            rangeslider=dict(visible=False),
                            tickfont=dict(size=9, color="#3a5068"),
                            linecolor="#1a2740",
                        ),
                        yaxis=dict(
                            showgrid=True, gridcolor="#0e1520",
                            tickfont=dict(size=9, color="#3a5068"),
                            tickprefix="$", side="right",
                            linecolor="#1a2740",
                        ),
                        hovermode="x unified",
                        hoverlabel=dict(
                            bgcolor="#101928", bordercolor="#1a2740",
                            font=dict(family="JetBrains Mono", size=11, color="#e2eaf4"),
                        ),
                    )
                    st.plotly_chart(_fig_chart, use_container_width=True,
                                    config={"displayModeBar": False})
                else:
                    st.markdown(
                        '<div style="height:340px;display:flex;align-items:center;'
                        'justify-content:center;color:#2d4460;font-family:\'JetBrains Mono\','
                        'monospace;font-size:0.72em;">Chart data unavailable — market may be closed</div>',
                        unsafe_allow_html=True,
                    )
            except Exception as _ce:
                st.markdown(
                    f'<div style="height:200px;display:flex;align-items:center;'
                    f'justify-content:center;color:#2d4460;font-family:\'JetBrains Mono\','
                    f'monospace;font-size:0.68em;">Chart: {str(_ce)[:60]}</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.markdown(
                '<div style="background:#101928;border:1px solid #1a2740;border-radius:8px;'
                'height:440px;display:flex;align-items:center;justify-content:center;">'
                '<div style="text-align:center;color:#2d4460;font-family:\'JetBrains Mono\',monospace;">'
                '<div style="font-size:2.5em;opacity:0.12;margin-bottom:16px;">◈</div>'
                '<div style="font-size:0.72em;letter-spacing:0.1em;">RUN SCANNER TO SEE TOP PICK</div>'
                '</div></div>',
                unsafe_allow_html=True,
            )

    # ════════════════════════ RIGHT: STATS + CONVICTION ══════════════════════
    with _cr:
        # Regime panel
        st.markdown(
            f'<div style="border:1px solid #1a2740;border-radius:8px;background:#101928;'
            f'overflow:hidden;margin-bottom:8px;">'
            f'<div style="padding:7px 12px;border-bottom:1px solid #1a2740;">'
            f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.56em;'
            f'font-weight:600;letter-spacing:0.12em;color:#3a5068;">MARKET REGIME</span>'
            f'</div>'
            f'<div style="padding:10px 12px;">'
            f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;">'
            f'<span style="width:7px;height:7px;border-radius:50%;background:{_regime_color};'
            f'display:inline-block;"></span>'
            f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.76em;'
            f'color:{_regime_color};font-weight:700;">{_regime_str}</span>'
            f'</div>'
            f'<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:5px;">'
            + "".join(
                f'<div style="background:#0b0f19;border:1px solid #1a2740;border-radius:4px;padding:6px 8px;">'
                f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.5em;color:#2d4460;'
                f'text-transform:uppercase;letter-spacing:0.08em;margin-bottom:3px;">{lbl}</div>'
                f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.78em;'
                f'color:#e2eaf4;font-weight:600;">{val}</div></div>'
                for lbl, val in [("VIX", _vix_str), ("A/D", _adv_str), ("BREADTH", _breadth_str)]
            )
            + f'</div></div></div>',
            unsafe_allow_html=True,
        )

        # Conviction picks
        _cv_count = len(_cv_entries)
        _cv_rows  = ""
        if _cv_entries:
            for _i, _e in enumerate(_cv_entries[:7]):
                _et   = _e.get("ticker", "")
                _es   = int(_e.get("composite") or _e.get("score") or _e.get("conviction") or 0)
                _pts  = []
                if _e.get("ai_conviction") in ("Very High", "High"):   _pts.append("High conviction")
                if _e.get("ai_catalyst_quality") in ("Very Strong", "Strong"): _pts.append("Catalyst")
                if _e.get("ai_setup_quality") in ("A+", "A"):          _pts.append("A-grade")
                _reason   = " · ".join(_pts) if _pts else ((_e.get("ai_summary") or "Strong setup")[:32])
                _row_bg   = "background:#0d1520;border-left:2px solid #3b82f6;" if _i == 0 else ""
                _cv_rows += (
                    f'<div style="display:flex;align-items:center;gap:8px;'
                    f'padding:8px 12px;border-bottom:1px solid #0a1220;{_row_bg}">'
                    f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.78em;'
                    f'color:#e2eaf4;font-weight:700;min-width:42px;">{_et}</span>'
                    f'<span style="background:rgba(34,197,94,0.1);border:1px solid rgba(34,197,94,0.2);'
                    f'color:#22c55e;font-family:\'JetBrains Mono\',monospace;font-size:0.6em;'
                    f'font-weight:700;padding:1px 6px;border-radius:3px;">{_es}</span>'
                    f'<span style="color:#5c7a99;font-family:\'JetBrains Mono\',monospace;'
                    f'font-size:0.58em;flex:1;line-height:1.4;overflow:hidden;">{_reason}</span>'
                    f'</div>'
                )
        else:
            _cv_rows = (
                '<div style="padding:20px 12px;text-align:center;color:#2d4460;'
                'font-family:\'JetBrains Mono\',monospace;font-size:0.68em;line-height:1.8;">'
                'No conviction picks yet</div>'
            )

        st.markdown(
            f'<div style="border:1px solid #1a2740;border-radius:8px;overflow:hidden;'
            f'background:#101928;margin-bottom:8px;">'
            f'<div style="padding:7px 12px;border-bottom:1px solid #1a2740;'
            f'display:flex;align-items:center;justify-content:space-between;">'
            f'<div style="display:flex;align-items:center;gap:6px;">'
            f'<span style="width:6px;height:6px;border-radius:50%;background:#22c55e;'
            f'display:inline-block;"></span>'
            f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.56em;'
            f'font-weight:600;letter-spacing:0.12em;color:#3a5068;">CONVICTION BUYS</span>'
            f'</div>'
            f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.56em;'
            f'color:#2d4460;">{_cv_count}</span></div>'
            f'{_cv_rows}</div>',
            unsafe_allow_html=True,
        )

        # Controls
        _b1, _b2 = st.columns(2)
        with _b1:
            if _paused:
                if st.button("▶ RESUME", use_container_width=True, key="dash_resume"):
                    try:
                        from db.database import set_scanner_control as _ssc
                        _ssc(paused=False); st.rerun()
                    except Exception as _e:
                        st.error(str(_e))
            else:
                if st.button("⏸ PAUSE", use_container_width=True, key="dash_pause"):
                    try:
                        from db.database import set_scanner_control as _ssc
                        _ssc(paused=True); st.rerun()
                    except Exception as _e:
                        st.error(str(_e))
        with _b2:
            if st.button("⚡ SCAN", use_container_width=True, key="dash_scannow",
                         disabled=_force_scan):
                try:
                    from db.database import set_scanner_control as _ssc
                    _ssc(force_scan=True); st.rerun()
                except Exception as _e:
                    st.error(str(_e))
        st.markdown(
            f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.57em;'
            f'color:#2d4460;margin-top:6px;line-height:1.8;">'
            f'Mode: <span style="color:#5c7a99;">{_cur_mode}</span> · '
            f'<span style="color:#5c7a99;">{_scan_count:,}</span> scans</div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB FUNCTIONS — only the active tab's function is called per rerun
# ══════════════════════════════════════════════════════════════════════════════


def _tab_scanner():
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


def _tab_portfolio():
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
                    from data.market_data import fetch_ticker_snapshot
                    snap          = fetch_ticker_snapshot(ticker)
                    current_price = snap.get("price", avg_cost) if snap else avg_cost
                    final_score   = 50; active_flags = []; technicals = {}; fundamentals = {}

                from analysis.portfolio import analyze_holding
                analysis = analyze_holding(ticker=ticker, shares=shares, avg_cost=avg_cost,
                    current_price=current_price, final_score=final_score,
                    active_flags=active_flags, technicals=technicals, fundamentals=fundamentals)
                analysis["final_score"] = final_score
                holdings_analysis.append(analysis)

        from analysis.portfolio import compute_portfolio_summary
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


def _tab_research():
    di, db = st.columns([4,1])
    with di:
        dive_ticker = st.text_input("dd", placeholder="Enter any ticker — e.g. SOUN, BBAI, CIFR, IONQ",
            label_visibility="collapsed").upper().strip()
    with db:
        run_dive = st.button("Analyze", type="primary", use_container_width=True)

    if run_dive and dive_ticker:
        with st.spinner(f"Running full analysis on {dive_ticker}..."):
            from core.scanner import scan_ticker
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

def _tab_predictions():
    st.markdown("## Prediction Engine")
    st.markdown('<p style="color:#334155;font-size:0.82em;font-family:\'JetBrains Mono\',monospace;">The scanner scores each watchlist stock every 30 min. Score ≥65 → LONG, Score ≤30 → SHORT. Track signal log outcomes in the Accuracy tab.</p>', unsafe_allow_html=True)

    st.markdown("""
    <div class="empty">
      <div class="ico">--</div>
      <h3>PREDICTIONS</h3>
      <p>Prediction signals are logged to the signal_log table.<br>
      Review outcomes in the Accuracy tab.</p>
    </div>""", unsafe_allow_html=True)


def _tab_alerts():
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


def _tab_performance():
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

        # Conviction list win rate
        try:
            from conviction_engine import get_conviction_win_rate
            _cv_wr = get_conviction_win_rate()
            if _cv_wr.get("n", 0) > 0:
                _cv_wr_val  = _cv_wr.get("win_rate") or 0
                _cv_wr_clr  = "#16a34a" if _cv_wr_val >= 55 else "#f85149"
                _cv_wr_n    = _cv_wr.get("n", 0)
                _cv_wr_gain = _cv_wr.get("avg_gain")
                _cv_gain_html = (
                    f'<span style="font-size:0.72em;color:var(--t3);">Avg gain: {_cv_wr_gain:+.2f}%</span>'
                    if _cv_wr_gain is not None else ""
                )
                st.markdown(
                    f'<div style="background:rgba(29,111,165,0.08);border:1px solid rgba(29,111,165,0.3);'
                    f'border-radius:8px;padding:8px 16px;margin-top:8px;display:flex;align-items:center;gap:20px;">'
                    f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.72em;font-weight:700;'
                    f'color:#58a6ff;">CONVICTION LIST</span>'
                    f'<span style="font-size:0.75em;color:var(--t2);">Win Rate: '
                    f'<b style="color:{_cv_wr_clr};">{_cv_wr_val:.1f}%</b></span>'
                    f'<span style="font-size:0.72em;color:var(--t3);">{_cv_wr_n} resolved</span>'
                    f'{_cv_gain_html}'
                    f'</div>',
                    unsafe_allow_html=True
                )
        except Exception:
            pass

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

        # ── Factor IC Table ───────────────────────────────────────────────────
        try:
            from db.database import get_ic_table
            _ic_rows = get_ic_table()
            if _ic_rows:
                st.markdown('<div class="sh" style="margin-top:18px;">Factor Information Coefficients (IC)</div>', unsafe_allow_html=True)
                st.markdown(
                    '<div style="font-size:0.72em;color:#64748b;font-family:\'JetBrains Mono\',monospace;margin-bottom:8px;">'
                    'IC = Pearson correlation of factor z-score vs actual 5-day return. Higher |IC| = more predictive. '
                    'Positive = factor predicts direction correctly.</div>',
                    unsafe_allow_html=True
                )
                _ic_html = (
                    '<div style="background:var(--bgcard);border:1px solid var(--border);border-radius:8px;padding:10px 16px;">'
                    '<div class="stat-row" style="margin-bottom:4px;">'
                    '<span class="slbl" style="width:130px;">Factor</span>'
                    '<span class="slbl" style="width:80px;text-align:right;">Avg IC</span>'
                    '<span class="slbl" style="width:80px;text-align:right;">Last IC</span>'
                    '<span class="slbl" style="width:60px;text-align:right;">n</span>'
                    '<span class="slbl" style="width:130px;text-align:right;">Updated</span>'
                    '</div>'
                )
                for _ir in _ic_rows[:20]:
                    _ic_v  = _ir.get("ic_value") or 0
                    _ic_c  = "#16a34a" if _ic_v > 0.05 else "#dc2626" if _ic_v < -0.05 else "#94a3b8"
                    _ic_ts = str(_ir.get("calc_date", ""))[:10]
                    _ic_html += (
                        f'<div class="stat-row">'
                        f'<span class="sval b" style="width:130px;">{_ir.get("factor_name","")}</span>'
                        f'<span class="sval" style="width:80px;text-align:right;color:{_ic_c};font-weight:600;">{_ic_v:+.3f}</span>'
                        f'<span class="sval" style="width:80px;text-align:right;">{_ir.get("sample_size","")}</span>'
                        f'<span class="sval" style="width:60px;text-align:right;color:#94a3b8;">{_ir.get("sample_size","")}</span>'
                        f'<span class="slbl" style="width:130px;text-align:right;">{_ic_ts}</span>'
                        f'</div>'
                    )
                _ic_html += '</div>'
                st.markdown(_ic_html, unsafe_allow_html=True)

                # Factor decay chart: IC by horizon
                try:
                    from db.database import _is_postgres, _get_pg_conn, _get_sqlite_conn
                    if _is_postgres():
                        _dc = _get_pg_conn().cursor()
                        _dc.execute("""
                            SELECT factor_name, horizon_days, AVG(ic_value) as avg_ic
                            FROM factor_ic_history
                            WHERE calc_date >= CURRENT_DATE - 30
                            GROUP BY factor_name, horizon_days
                            ORDER BY factor_name, horizon_days
                        """)
                        _decay_rows = _dc.fetchall()
                        _dc.connection.close()
                    else:
                        _dc = _get_sqlite_conn()
                        _decay_rows = _dc.execute("""
                            SELECT factor_name, horizon_days, AVG(ic_value)
                            FROM factor_ic_history
                            WHERE calc_date >= date('now', '-30 days')
                            GROUP BY factor_name, horizon_days
                            ORDER BY factor_name, horizon_days
                        """).fetchall()
                        _dc.close()

                    if _decay_rows:
                        _decay_df = pd.DataFrame(_decay_rows, columns=["factor", "horizon", "avg_ic"])
                        _top_factors = (
                            _decay_df.groupby("factor")["avg_ic"]
                            .apply(lambda x: abs(x).mean())
                            .nlargest(8).index.tolist()
                        )
                        _decay_filt = _decay_df[_decay_df["factor"].isin(_top_factors)]

                        st.markdown('<div class="sh" style="margin-top:18px;">Factor Decay (IC by Holding Period)</div>', unsafe_allow_html=True)
                        _fig_decay = go.Figure()
                        for _fn in _top_factors:
                            _fd = _decay_filt[_decay_filt["factor"] == _fn].sort_values("horizon")
                            _fig_decay.add_trace(go.Scatter(
                                x=_fd["horizon"], y=_fd["avg_ic"],
                                mode="lines+markers", name=_fn,
                                line=dict(width=2),
                            ))
                        _fig_decay.update_layout(
                            height=320, margin=dict(l=0, r=0, t=20, b=0),
                            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                            font=dict(family="JetBrains Mono", size=11, color="#334155"),
                            xaxis=dict(title="Holding Period (days)", tickvals=[1, 3, 5, 10], showgrid=False),
                            yaxis=dict(title="Avg IC", zeroline=True, zerolinecolor="#dde1ec"),
                            legend=dict(font=dict(size=10)),
                        )
                        st.plotly_chart(_fig_decay, use_container_width=True)
                except Exception:
                    pass
        except Exception:
            pass

        # ── Recent signal log with outcomes ───────────────────────────────────
        st.markdown('<div class="sh">Recent Signals (30 days)</div>', unsafe_allow_html=True)
        if sig_df.empty:
            st.markdown('<div class="box">No signals logged yet.</div>', unsafe_allow_html=True)
        else:
            # One row per ticker per calendar day — most recent signal wins
            _dedup_df = sig_df.copy()
            _dedup_df["_date"] = _dedup_df["created_at"].astype(str).str[:10]
            _dedup_df = _dedup_df.sort_values("created_at", ascending=False)
            _dedup_df = _dedup_df.drop_duplicates(subset=["ticker", "_date"], keep="first")
            _outcome_color = {"win": "#16a34a", "loss": "#dc2626", "neutral": "#94a3b8",
                              "pending": "#c2610f", "pending_5day": "#d97706"}
            rows_html = ""
            for _, row in _dedup_df.head(40).iterrows():
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


def _tab_system():
    _w = st.session_state.get("scoring_weights", SCORING_WEIGHTS)
    _wt = int(round(_w.get("technical",   0) * 100))
    _wf = int(round(_w.get("fundamental", 0) * 100))
    _wr = int(round(_w.get("risk",        0) * 100))
    _ws = int(round(_w.get("sentiment",   0) * 100))
    st.markdown(f"""
## How Axiom Terminal Works

### Scoring Model
Each stock scores 0–100 across four factors (unified formula, single path).

| Component | Weight | What it measures |
|-----------|--------|-----------------|
| Technical | {_wt}% | RSI, MACD, moving averages, volume, momentum |
| Fundamental | {_wf}% | Revenue growth, cash, burn rate, margins |
| Risk (inverted) | {_wr}% | Dilution, short interest, volatility, liquidity |
| Sentiment | {_ws}% | News tone, analyst coverage, hype detection |

### Signal Gates (all must pass before a signal fires)
- **Score ≥ 70** — minimum floor; Watchlist/Hold signals suppressed
- **Volume ≥ 1.5× avg** — confirms institutional interest
- **Time gate** — first/last 15 min of session blocked (open/close noise)
- **Regime gate** — Strong Buy signals suppressed in MEAN_REVERSION regime

### Signal Labels

| Signal | Score | Meaning |
|--------|-------|---------|
| Strong Buy Candidate | 75–100 | Strong conditions — still research before acting |
| Speculative Buy | 60–75 | Good setup, acceptable risk |
| Watchlist | 45–60 | Interesting — suppressed from alerts (below floor) |
| Hold / Trim / Sell / Avoid | 0–45 | Not logged |

### Data Sources
- **Finnhub** — real-time quotes, earnings calendar, company profile
- **Yahoo Finance** — price history, fundamentals, short interest
- **SEC EDGAR** — S-3, 424B, 8-K filings, Form 4 insider trades
- **Yahoo Finance RSS** — news headlines and sentiment
- **Claude API** — AI filing text analysis

---
**Research tool only. Not financial advice. Small-cap stocks can lose 100% of value. Always do your own research.**
    """)

    # ── Changelog ─────────────────────────────────────────────────────────────
    st.markdown('<div class="sh" style="margin-top:20px;">Change Log</div>',
                unsafe_allow_html=True)
    try:
        from db.database import get_changelog as _gcl
        _cl_entries = _gcl(limit=50, days_back=90)
        if _cl_entries:
            _IMPACT_COLOR = {"high": "#f43f5e", "medium": "#f59e0b", "low": "#10b981"}
            _CAT_COLOR    = {"scoring": "#a78bfa", "scanner": "#38bdf8", "universe": "#10b981",
                             "validator": "#f59e0b", "dashboard": "#60a5fa", "database": "#94a3b8",
                             "infrastructure": "#f97316", "health": "#10b981"}
            for _cl in _cl_entries:
                _dt  = str(_cl.get("change_date") or "")[:16]
                _cat = _cl.get("category", "update")
                _ttl = _cl.get("title", "")
                _dsc = _cl.get("description", "")
                _fls = _cl.get("files", "")
                _sha = (_cl.get("commit_hash") or "")[:7]
                _imp = _cl.get("impact", "medium")
                _cc  = _CAT_COLOR.get(_cat, "#64748b")
                _ic  = _IMPACT_COLOR.get(_imp, "#64748b")
                _sha_html = (
                    '<span style="font-family:JetBrains Mono,monospace;font-size:0.58em;'
                    f'color:#3a5068;">{_sha}</span>'
                ) if _sha and _sha != "current" else ""
                _dsc_html = (
                    '<div style="color:#5c7a99;font-size:0.72em;line-height:1.5;margin-bottom:4px;">'
                    f'{_dsc}</div>'
                ) if _dsc else ""
                _fls_html = (
                    '<div style="font-family:JetBrains Mono,monospace;font-size:0.6em;'
                    f'color:#3a5068;">{_fls}</div>'
                ) if _fls else ""
                _html = (
                    '<div style="border:1px solid #1a2740;border-radius:6px;'
                    'padding:10px 14px;margin-bottom:8px;background:#101928;">'
                    '<div style="display:flex;align-items:center;gap:8px;'
                    'margin-bottom:5px;flex-wrap:wrap;">'
                    f'<span style="font-family:JetBrains Mono,monospace;font-size:0.62em;color:#3a5068;">{_dt}</span>'
                    f'<span style="background:{_cc}22;border:1px solid {_cc}55;border-radius:3px;'
                    f'padding:1px 7px;font-family:JetBrains Mono,monospace;font-size:0.6em;'
                    f'color:{_cc};font-weight:600;letter-spacing:0.06em;">{_cat.upper()}</span>'
                    f'<span style="background:{_ic}18;border:1px solid {_ic}44;border-radius:3px;'
                    f'padding:1px 6px;font-family:JetBrains Mono,monospace;font-size:0.58em;'
                    f'color:{_ic};">{_imp.upper()} IMPACT</span>'
                    f'{_sha_html}</div>'
                    f'<div style="font-family:JetBrains Mono,monospace;font-size:0.8em;'
                    f'color:#e2eaf4;font-weight:600;margin-bottom:4px;">{_ttl}</div>'
                    f'{_dsc_html}{_fls_html}</div>'
                )
                st.markdown(_html,
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No changelog entries yet — seed_changelog.py populates this on first deploy.")
    except Exception as _cle:
        st.caption(f"Changelog unavailable: {_cle}")

    if _current_user["role"] == "admin":
        st.markdown("---")
        st.markdown('<div class="sh">Manage Users</div>', unsafe_allow_html=True)
        try:
            _adm_users = get_all_users() or []
        except Exception as _adm_e:
            st.error(f"User management unavailable: {_adm_e}")
            _adm_users = []

        # ── User list ─────────────────────────────────────────────────────────
        if _adm_users:
            _hdr_cols = st.columns([2, 2, 1.2, 1.5, 1.5, 1])
            for _h, _lbl in zip(_hdr_cols, ["Username", "Display Name", "Role", "Joined", "Last Login", ""]):
                _h.markdown(f'<span class="slbl">{_lbl}</span>', unsafe_allow_html=True)

            for _u in _adm_users:
                _uid    = _u["id"]
                _uname  = _u["username"]
                _udname = _u.get("display_name") or "—"
                _urole  = _u.get("role", "user")
                _ujoin  = str(_u.get("created_at") or "—")[:10]
                _ulast  = str(_u.get("last_login") or "—")[:16]
                _rc     = "#c2610f" if _urole == "admin" else "var(--t3)"
                _self   = (_uid == _current_user.get("id"))

                _c1, _c2, _c3, _c4, _c5, _c6 = st.columns([2, 2, 1.2, 1.5, 1.5, 1])
                _c1.markdown(f'<span class="sval b">{_uname}</span>', unsafe_allow_html=True)
                _c2.markdown(f'<span class="slbl">{_udname}</span>', unsafe_allow_html=True)
                _c3.markdown(
                    f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.72em;'
                    f'font-weight:600;color:{_rc};">{_urole.upper()}</span>',
                    unsafe_allow_html=True,
                )
                _c4.markdown(f'<span class="slbl">{_ujoin}</span>', unsafe_allow_html=True)
                _c5.markdown(f'<span class="slbl">{_ulast}</span>', unsafe_allow_html=True)
                with _c6:
                    if not _self:
                        if st.button("Delete", key=f"adm_del_{_uid}", type="secondary"):
                            if delete_user(_uid):
                                st.success(f"Deleted {_uname}")
                                st.rerun()
                            else:
                                st.error("Delete failed")
                    else:
                        st.markdown('<span class="slbl">you</span>', unsafe_allow_html=True)

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # ── Add user ──────────────────────────────────────────────────────────
        with st.expander("Add user", expanded=False):
            _ac1, _ac2 = st.columns(2)
            with _ac1:
                _nu_user  = st.text_input("Username",     key="adm_nu_user",  placeholder="username")
                _nu_pass  = st.text_input("Password",     key="adm_nu_pass",  type="password",
                                          placeholder="min 6 characters")
            with _ac2:
                _nu_dname = st.text_input("Display Name", key="adm_nu_dname", placeholder="display name")
                _nu_role  = st.selectbox("Role",          ["user", "admin"],  key="adm_nu_role")
            if st.button("Create user", key="adm_nu_submit", type="primary"):
                _nu_user = (_nu_user or "").strip()
                _nu_pass = (_nu_pass or "").strip()
                _err = validate_password_strength(_nu_pass) if len(_nu_pass) >= 8 else (
                    "Password must be at least 8 characters." if _nu_pass else "Password required."
                )
                if not _nu_user:
                    st.error("Username required.")
                elif _err:
                    st.error(_err)
                else:
                    try:
                        _email = f"{_nu_user}@axiom.local"
                        create_user(_nu_user, _email, hash_password(_nu_pass), _nu_role,
                                    display_name=(_nu_dname or _nu_user).strip())
                        st.success(f"Created user: {_nu_user}")
                        st.rerun()
                    except Exception as _cue:
                        st.error(f"Failed: {_cue}")

        # ── Change password ───────────────────────────────────────────────────
        with st.expander("Change password", expanded=False):
            if _adm_users:
                _cp_names = [_u["username"] for _u in _adm_users]
                _cp_c1, _cp_c2 = st.columns(2)
                with _cp_c1:
                    _cp_sel  = st.selectbox("User", _cp_names, key="adm_cp_sel")
                with _cp_c2:
                    _cp_pass = st.text_input("New password", key="adm_cp_pass", type="password",
                                             placeholder="min 8 characters")
                if st.button("Update password", key="adm_cp_submit", type="primary"):
                    _cp_pass = (_cp_pass or "").strip()
                    _cp_err  = validate_password_strength(_cp_pass) if len(_cp_pass) >= 8 else (
                        "Password must be at least 8 characters." if _cp_pass else "Password required."
                    )
                    if _cp_err:
                        st.error(_cp_err)
                    else:
                        try:
                            _cp_uid = next(u["id"] for u in _adm_users if u["username"] == _cp_sel)
                            change_user_password(_cp_uid, hash_password(_cp_pass))
                            st.success(f"Password updated for {_cp_sel}")
                        except Exception as _cpe:
                            st.error(f"Failed: {_cpe}")

def _tab_config():
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


def _tab_paper_trading():
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


def _tab_health():
    import time as _time

    _STATUS_COLORS = {"ok": "#10b981", "warn": "#f59e0b", "critical": "#f43f5e"}
    _STATUS_ICONS  = {"ok": "●", "warn": "◐", "critical": "✗"}

    def _status_badge(status: str, detail: str = "") -> str:
        color = _STATUS_COLORS.get(status, "#8293a8")
        icon  = _STATUS_ICONS.get(status, "?")
        label = status.upper()
        text  = f" <span style='color:#8293a8;font-size:0.78em'>{detail}</span>" if detail else ""
        return (
            f"<span style='color:{color};font-weight:700;font-size:0.88em'>"
            f"{icon} {label}</span>{text}"
        )

    # Header
    _ts = _time.strftime("%H:%M:%S ET", _time.localtime())
    st.markdown(
        f"<h2 style='color:#f59e0b;font-family:JetBrains Mono,monospace;margin-bottom:4px'>"
        f"System Health</h2>"
        f"<p style='color:#3a5068;font-size:0.8em;margin-top:0'>Last checked: {_ts} · auto-refreshes every 60 s</p>",
        unsafe_allow_html=True,
    )

    try:
        from health.monitor import HealthMonitor as _HM
        from db.database import get_health_events, get_data_quality_summary

        _results = _HM().run_all_checks()

        # ── Status grid ──────────────────────────────────────────────────────
        _LABELS = {
            "database":           "Supabase",
            "finnhub":            "Finnhub API",
            "tiingo":             "Tiingo API",
            "yfinance":           "yfinance",
            "scanner_loop":       "Scanner Loop",
            "universe":           "Universe",
            "conviction_engine":  "Conviction Engine",
            "accuracy_validator": "Accuracy Validator",
            "telegram_bot":       "Telegram Bot",
        }
        _keys = list(_LABELS.keys())
        _cols_per_row = 3
        for _row_start in range(0, len(_keys), _cols_per_row):
            _row_keys = _keys[_row_start:_row_start + _cols_per_row]
            _grid_cols = st.columns(len(_row_keys))
            for _ci, _key in enumerate(_row_keys):
                _info   = _results.get(_key, {})
                _status = _info.get("status", "warn")
                _detail = _info.get("detail", "")
                _chk    = _info.get("checked_at", "")
                _color  = _STATUS_COLORS.get(_status, "#8293a8")
                _border = f"2px solid {_color}44"
                with _grid_cols[_ci]:
                    st.markdown(
                        f"""<div style='background:#101928;border:{_border};border-radius:8px;
                                        padding:14px 16px;margin-bottom:8px'>
                            <div style='color:#8293a8;font-size:0.75em;text-transform:uppercase;
                                        letter-spacing:.08em;margin-bottom:6px'>{_LABELS[_key]}</div>
                            <div style='font-size:1.1em;font-weight:700;color:{_color}'>
                                {_STATUS_ICONS.get(_status,'?')} {_status.upper()}</div>
                            <div style='color:#5c7a99;font-size:0.76em;margin-top:6px;
                                        line-height:1.4'>{_detail}</div>
                            <div style='color:#3a5068;font-size:0.68em;margin-top:6px'>{_chk}</div>
                        </div>""",
                        unsafe_allow_html=True,
                    )

        st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)

        # ── Key metrics row ──────────────────────────────────────────────────
        _uv_size = _results.get("universe", {}).get("size", 0) or 0
        _fh_rate = _results.get("finnhub",  {}).get("success_rate")
        _ti_rate = _results.get("tiingo",   {}).get("success_rate")
        _yf_rate = _results.get("yfinance", {}).get("success_rate")
        _fh_ms   = _results.get("finnhub",  {}).get("avg_latency_ms", 0)
        _ti_ms   = _results.get("tiingo",   {}).get("avg_latency_ms", 0)

        def _pct_str(v):
            return f"{v*100:.0f}%" if v is not None else "n/a"

        _m1, _m2, _m3, _m4 = st.columns(4)
        with _m1:
            st.metric("Universe Size",   f"{_uv_size:,} tickers")
        with _m2:
            st.metric("Finnhub Success", _pct_str(_fh_rate), delta=f"{_fh_ms:.0f} ms avg")
        with _m3:
            st.metric("Tiingo Success",  _pct_str(_ti_rate), delta=f"{_ti_ms:.0f} ms avg")
        with _m4:
            st.metric("yfinance Success", _pct_str(_yf_rate))

        # ── Data quality table ───────────────────────────────────────────────
        st.markdown("#### Quote Source Performance (last 24 h)")
        _dq = get_data_quality_summary(hours_back=24)
        if _dq:
            import pandas as _pd_h
            _dq_df = _pd_h.DataFrame(_dq)
            _dq_df["success_rate"] = (_dq_df["success_rate"] * 100).round(1).astype(str) + "%"
            _dq_df["avg_latency_ms"] = _dq_df["avg_latency_ms"].round(1).astype(str) + " ms"
            _dq_df.columns = ["Source", "Total Calls", "OK", "Success Rate", "Avg Latency"]
            st.dataframe(_dq_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No data_quality records in the last 24 hours.")

        # ── Recent health events ─────────────────────────────────────────────
        st.markdown("#### Recent Health Events")
        _events = get_health_events(limit=40, hours_back=24)
        if _events:
            import pandas as _pd_h2
            _ev_df = _pd_h2.DataFrame(_events)
            # Color-code status column
            def _color_row(row):
                color = {"ok": "#10b98120", "warn": "#f59e0b20",
                         "critical": "#f43f5e20"}.get(row["status"], "")
                return [f"background-color:{color}"] * len(row)
            _ev_df.columns = ["Subsystem", "Status", "Detail", "Action", "Time"]
            st.dataframe(_ev_df.style.apply(_color_row, axis=1),
                         use_container_width=True, hide_index=True)
        else:
            st.caption("No health events in the last 24 hours — all clean.")

        # ── Manual refresh button ────────────────────────────────────────────
        if st.button("↻ Refresh Now", key="health_manual_refresh"):
            st.rerun()

        if st.button("⚡ Force Grade Pending Signals", key="force_grade_btn"):
            try:
                from accuracy_validator import force_grade_all_pending
                _result = force_grade_all_pending()
                st.success(f"Graded {_result.get('graded', 0)} signals (was {_result.get('was_pending', 0)} pending)")
            except Exception as _fge:
                st.error(f"Force grade failed: {_fge}")

    except ImportError as _hi:
        st.error(f"Health monitor not available: {_hi}")
    except Exception as _he:
        st.error(f"Health tab error: {_he}")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN NAVIGATION — custom tab switcher; only the selected tab's code runs
# ══════════════════════════════════════════════════════════════════════════════

_TABS = [
    "Dashboard", "Scanner", "Portfolio", "Research", "Signals",
    "Alerts", "Performance", "System", "Config", "Paper Trading", "Health",
]

st.session_state.setdefault("active_tab", "Dashboard")

# ── Nav bar ───────────────────────────────────────────────────────────────────
_nav_cols = st.columns(len(_TABS))
for _ni, (_nc, _tn) in enumerate(zip(_nav_cols, _TABS)):
    _is_active = st.session_state["active_tab"] == _tn
    if _nc.button(_tn, key=f"_nav_{_ni}", use_container_width=True,
                  type="primary" if _is_active else "secondary"):
        st.session_state["active_tab"] = _tn
        st.rerun()

st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

_at = st.session_state["active_tab"]
if   _at == "Dashboard":      _terminal_dashboard()
elif _at == "Scanner":        _tab_scanner()
elif _at == "Portfolio":      _tab_portfolio()
elif _at == "Research":       _tab_research()
elif _at == "Signals":        _tab_predictions()
elif _at == "Alerts":         _tab_alerts()
elif _at == "Performance":    _tab_performance()
elif _at == "System":         _tab_system()
elif _at == "Config":         _tab_config()
elif _at == "Paper Trading":  _tab_paper_trading()
elif _at == "Health":         _tab_health()
