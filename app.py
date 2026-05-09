# app.py — APEX SmallCap Scanner
# ALL helper functions defined FIRST before any UI code.

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import sys, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DEFAULT_UNIVERSE, SCORING_WEIGHTS, RISK_FLAGS
from db.database import initialize_db, upsert_holding, get_portfolio, get_connection
from core.scanner import scan_ticker, scan_universe
from analysis.portfolio import analyze_holding, compute_portfolio_summary
from data.market_data import fetch_ticker_snapshot, get_price_history
from analysis.technicals import compute_technicals

st.set_page_config(page_title="APEX Scanner", page_icon="⚡", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=Bebas+Neue&family=JetBrains+Mono:wght@300;400;500;600&display=swap');

:root {
    --bg:       #04080f;
    --bg2:      #080f1a;
    --bg3:      #0c1520;
    --bgcard:   #0a1220;
    --bghover:  #0f1c30;
    --border:   #0e2040;
    --borderhi: #1a3a6a;
    --borderglow: #1e4d8c;
    --green:    #00e87a;
    --green2:   #00ff88;
    --blue:     #0096ff;
    --blue2:    #33b3ff;
    --amber:    #ffb700;
    --red:      #ff2d55;
    --red2:     #ff4d6d;
    --purple:   #9b59ff;
    --t1:       #e8f4ff;
    --t2:       #6b8caa;
    --t3:       #2a4060;
    --tdim:     #112030;
}

/* ── Base ── */
.stApp { background: var(--bg) !important; }
.stApp > header { background: transparent !important; }
.main .block-container { padding: 0 1.5rem 2rem; max-width: 100%; }
* { font-family: 'Space Grotesk', sans-serif; }
#MainMenu, footer, .stDeployButton { visibility: hidden; }

/* ── Animated grid background ── */
.stApp::before {
    content: '';
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background-image:
        linear-gradient(rgba(0,150,255,0.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(0,150,255,0.03) 1px, transparent 1px);
    background-size: 40px 40px;
    pointer-events: none;
    z-index: 0;
}

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
    border-color: var(--blue) !important;
    box-shadow: 0 0 0 2px rgba(0,150,255,0.1) !important;
}

/* ── Buttons ── */
.stButton > button {
    background: var(--bg3) !important;
    border: 1px solid var(--borderhi) !important;
    color: var(--t2) !important;
    font-family: 'Space Grotesk', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.78em !important;
    letter-spacing: 0.12em !important;
    border-radius: 4px !important;
    text-transform: uppercase !important;
    transition: all 0.15s !important;
    padding: 0.4rem 0.8rem !important;
}
.stButton > button:hover {
    border-color: var(--blue) !important;
    color: var(--blue) !important;
    background: rgba(0,150,255,0.06) !important;
    box-shadow: 0 0 12px rgba(0,150,255,0.15) !important;
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, rgba(0,100,200,0.4), rgba(0,150,255,0.2)) !important;
    border-color: var(--blue) !important;
    color: var(--blue2) !important;
    box-shadow: 0 0 20px rgba(0,150,255,0.2) !important;
}
.stButton > button[kind="primary"]:hover {
    box-shadow: 0 0 30px rgba(0,150,255,0.35) !important;
    color: #fff !important;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    border-bottom: 1px solid var(--border) !important;
    gap: 0 !important;
    padding: 0 !important;
}
.stTabs [data-baseweb="tab"] {
    font-family: 'Space Grotesk', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.72em !important;
    letter-spacing: 0.15em !important;
    color: var(--t3) !important;
    text-transform: uppercase !important;
    padding: 12px 20px !important;
    border-bottom: 2px solid transparent !important;
    background: transparent !important;
    transition: all 0.2s !important;
}
.stTabs [data-baseweb="tab"]:hover { color: var(--t2) !important; }
.stTabs [aria-selected="true"] {
    color: var(--blue2) !important;
    border-bottom-color: var(--blue) !important;
    text-shadow: 0 0 20px rgba(0,150,255,0.5) !important;
}

/* ── Metrics ── */
[data-testid="metric-container"] {
    background: var(--bgcard) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    padding: 10px 14px !important;
    position: relative !important;
    overflow: hidden !important;
    transition: border-color 0.2s !important;
}
[data-testid="metric-container"]:hover { border-color: var(--borderhi) !important; }
[data-testid="metric-container"] label {
    color: var(--t3) !important;
    font-size: 0.65em !important;
    letter-spacing: 0.12em !important;
    text-transform: uppercase !important;
    font-family: 'JetBrains Mono', monospace !important;
}
[data-testid="metric-container"] [data-testid="metric-value"] {
    color: var(--t1) !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 1.1em !important;
    font-weight: 500 !important;
}
[data-testid="stMetricDelta"] { font-family: 'JetBrains Mono', monospace !important; font-size: 0.75em !important; }

/* ── Expanders ── */
[data-testid="stExpander"] {
    background: var(--bgcard) !important;
    border: 1px solid var(--border) !important;
    border-radius: 10px !important;
    margin-bottom: 6px !important;
    transition: all 0.2s !important;
    overflow: hidden !important;
}
[data-testid="stExpander"]:hover { border-color: var(--borderhi) !important; }
[data-testid="stExpander"] summary {
    color: var(--t1) !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.8em !important;
    padding: 12px 16px !important;
}
[data-testid="stExpander"] summary:hover { background: var(--bghover) !important; }

/* ── Misc ── */
hr { border-color: var(--border) !important; margin: 10px 0 !important; }
.stCheckbox label { color: var(--t2) !important; font-size: 0.82em !important; }
.stSlider { padding: 0 !important; }
p, li { color: var(--t2); }
h1,h2,h3 { color: var(--t1) !important; font-family: 'Bebas Neue', sans-serif !important; letter-spacing: 0.08em !important; }

/* ── Custom components ── */

/* Score pill */
.pill { display:inline-flex; align-items:center; padding:3px 10px; border-radius:20px; font-family:'Space Grotesk',sans-serif; font-weight:700; font-size:0.68em; letter-spacing:0.1em; text-transform:uppercase; gap:4px; }
.p-sb  { background:rgba(0,232,122,0.1); color:#00e87a; border:1px solid rgba(0,232,122,0.25); }
.p-sp  { background:rgba(0,200,100,0.1); color:#00c864; border:1px solid rgba(0,200,100,0.25); }
.p-wl  { background:rgba(255,183,0,0.1); color:#ffb700; border:1px solid rgba(255,183,0,0.25); }
.p-ho  { background:rgba(255,160,0,0.08); color:#ffa000; border:1px solid rgba(255,160,0,0.2); }
.p-tr  { background:rgba(255,100,0,0.08); color:#ff6400; border:1px solid rgba(255,100,0,0.2); }
.p-se  { background:rgba(255,45,85,0.08); color:#ff2d55; border:1px solid rgba(255,45,85,0.2); }
.p-av  { background:rgba(200,20,50,0.1); color:#c81432; border:1px solid rgba(200,20,50,0.3); }

/* Risk flag chips */
.flag { display:inline-flex; align-items:center; background:rgba(255,183,0,0.06); border:1px solid rgba(255,183,0,0.2); color:#ffb700; padding:2px 8px; border-radius:3px; font-size:0.65em; font-family:'JetBrains Mono',monospace; margin:2px 2px 2px 0; letter-spacing:0.05em; }
.flag.crit { background:rgba(255,45,85,0.07); border-color:rgba(255,45,85,0.25); color:#ff2d55; }
.flag.earn { background:rgba(155,89,255,0.07); border-color:rgba(155,89,255,0.25); color:#9b59ff; }

/* Score bar */
.sbar-track { background:rgba(255,255,255,0.04); border-radius:3px; height:4px; width:100%; position:relative; overflow:hidden; }
.sbar-fill { border-radius:3px; height:4px; position:relative; }
.sbar-fill::after { content:''; position:absolute; top:0; right:0; width:4px; height:4px; border-radius:50%; background:inherit; box-shadow:0 0 6px currentColor; }

/* Stat rows */
.stat-row { display:flex; justify-content:space-between; align-items:center; padding:5px 0; border-bottom:1px solid rgba(255,255,255,0.03); }
.slbl { color:var(--t3); font-size:0.68em; letter-spacing:0.08em; text-transform:uppercase; font-family:'JetBrains Mono',monospace; }
.sval { color:var(--t1); font-size:0.8em; font-family:'JetBrains Mono',monospace; font-weight:500; }
.sval.g { color:var(--green); } .sval.r { color:var(--red); } .sval.a { color:var(--amber); } .sval.b { color:var(--blue2); } .sval.p { color:var(--purple); }

/* Section headers */
.sh { font-family:'Space Grotesk',sans-serif; font-size:0.62em; font-weight:700; letter-spacing:0.2em; text-transform:uppercase; color:var(--t3); margin:14px 0 8px; padding-bottom:5px; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:6px; }

/* Info boxes */
.box { background:rgba(255,255,255,0.02); border:1px solid var(--border); border-radius:8px; padding:12px 16px; color:var(--t2); font-size:0.85em; line-height:1.75; }
.box-blue { border-color:rgba(0,150,255,0.3); background:rgba(0,150,255,0.04); }
.box-green { border-color:rgba(0,232,122,0.3); background:rgba(0,232,122,0.03); }
.box-red { border-color:rgba(255,45,85,0.3); background:rgba(255,45,85,0.04); }
.box-amber { border-color:rgba(255,183,0,0.3); background:rgba(255,183,0,0.04); }
.box-purple { border-color:rgba(155,89,255,0.3); background:rgba(155,89,255,0.04); }

/* Disclaimer */
.disc { background:rgba(255,183,0,0.03); border:1px solid rgba(255,183,0,0.1); border-radius:4px; padding:6px 12px; color:rgba(255,183,0,0.4); font-size:0.62em; font-family:'JetBrains Mono',monospace; letter-spacing:0.06em; text-align:center; margin:8px 0; }

/* Empty states */
.empty { text-align:center; padding:80px 20px; color:var(--t3); }
.empty .ico { font-size:3em; margin-bottom:16px; opacity:0.5; }
.empty h3 { font-family:'Bebas Neue',sans-serif; color:var(--t2) !important; letter-spacing:0.12em; font-size:1.6em; }
.empty p { color:var(--t3); font-size:0.8em; font-family:'JetBrains Mono',monospace; }

/* Glow number */
.glow-green { color:var(--green2); text-shadow:0 0 20px rgba(0,255,136,0.4); }
.glow-red   { color:var(--red2); text-shadow:0 0 20px rgba(255,77,109,0.4); }
.glow-blue  { color:var(--blue2); text-shadow:0 0 20px rgba(51,179,255,0.4); }
.glow-amber { color:var(--amber); text-shadow:0 0 20px rgba(255,183,0,0.4); }

/* Ticker header */
.ticker-hero { padding:20px 0 16px; border-bottom:1px solid var(--border); margin-bottom:20px; }

/* News item */
.news-item { display:flex; align-items:flex-start; gap:10px; padding:8px 0; border-bottom:1px solid rgba(255,255,255,0.03); }
.news-dot { width:6px; height:6px; border-radius:50%; margin-top:6px; flex-shrink:0; }

/* Summary text */
.summary-block { background:rgba(255,255,255,0.02); border-left:2px solid var(--borderhi); border-radius:0 8px 8px 0; padding:14px 16px; color:var(--t2); font-size:0.83em; line-height:1.8; white-space:pre-line; }

/* Score ring */
.score-ring-wrap { position:relative; display:inline-flex; align-items:center; justify-content:center; }

/* Sidebar logo */
.apex-logo { padding:20px 16px 12px; border-bottom:1px solid var(--border); margin-bottom:4px; }
.apex-logo .name { font-family:'Bebas Neue',sans-serif; font-size:2em; color:var(--t1); letter-spacing:0.15em; line-height:1; }
.apex-logo .sub { font-family:'JetBrains Mono',monospace; font-size:0.58em; color:var(--t3); letter-spacing:0.25em; margin-top:2px; }

/* Signal count card */
.sig-count { text-align:center; padding:10px 6px; background:var(--bgcard); border:1px solid var(--border); border-radius:8px; transition:all 0.2s; }
.sig-count:hover { border-color:var(--borderhi); transform:translateY(-1px); }
.sig-count .num { font-family:'Bebas Neue',sans-serif; font-size:1.8em; line-height:1; }
.sig-count .lbl { font-family:'JetBrains Mono',monospace; font-size:0.55em; color:var(--t3); letter-spacing:0.1em; margin-top:2px; }

/* Portfolio card */
.port-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:14px; flex-wrap:wrap; gap:10px; }

/* RS badge */
.rs-badge { display:inline-flex; align-items:center; gap:6px; padding:5px 12px; border-radius:4px; font-family:'JetBrains Mono',monospace; font-size:0.72em; font-weight:600; }

/* Earnings warning */
.earn-warn { background:rgba(155,89,255,0.08); border:1px solid rgba(155,89,255,0.3); border-radius:6px; padding:10px 14px; color:#b980ff; font-size:0.78em; font-family:'JetBrains Mono',monospace; margin-top:8px; line-height:1.6; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS — ALL DEFINED BEFORE UI
# ══════════════════════════════════════════════════════════════════════════════

def pill_class(signal):
    return {"Strong Buy Candidate":"p-sb","Speculative Buy":"p-sp","Watchlist":"p-wl",
            "Hold":"p-ho","Trim":"p-tr","Sell":"p-se","Avoid":"p-av"}.get(signal,"p-ho")

def sig_color(signal):
    return {"Strong Buy Candidate":"#00e87a","Speculative Buy":"#00c864","Watchlist":"#ffb700",
            "Hold":"#ffa000","Trim":"#ff6400","Sell":"#ff2d55","Avoid":"#c81432"}.get(signal,"#6b8caa")

def score_col(s):
    if s>=72: return "#00e87a"
    if s>=58: return "#00c864"
    if s>=45: return "#ffb700"
    if s>=33: return "#ffa000"
    if s>=22: return "#ff6400"
    return "#ff2d55"

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
    rcolor  = "#00e87a" if ret1d >= 0 else "#ff2d55"

    cat_notes = r.get("catalyst_notes", [])
    cat_str   = cat_notes[0] if cat_notes else "No confirmed catalyst"

    main_risk = "No major flags detected"
    if flags:
        main_risk = {
            "going_concern":       "⚠ Bankruptcy risk — going concern in filing",
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
                <span style="font-family:'Bebas Neue',sans-serif;font-size:2em;color:#e8f4ff;letter-spacing:0.08em;">{ticker}</span>
                <span style="color:#3a5878;font-size:0.88em;font-weight:400;">{name[:42]}</span>
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
                    <span style="font-size:0.7em;color:#2a4060;font-family:'JetBrains Mono',monospace;">{lbl}</span>
                    <span style="font-size:0.7em;color:{c};font-family:'JetBrains Mono',monospace;font-weight:600;">{val:.0f}<span style="color:#112030;"> ×{wt:.0%}</span></span>
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
                dc   = {"positive":"#00e87a","negative":"#ff2d55"}.get(sent,"#3a5878")
                st.markdown(f"""
                <div class="news-item">
                  <div class="news-dot" style="background:{dc};box-shadow:0 0 6px {dc};"></div>
                  <div>
                    <a href="{h.get('url','#')}" target="_blank" style="color:#5ba3d9;font-size:0.82em;text-decoration:none;line-height:1.4;">{h.get('title','')}</a>
                    <div style="color:#1e3a52;font-size:0.68em;margin-top:2px;font-family:'JetBrains Mono',monospace;">{h.get('date','')} · {sent.upper()}</div>
                  </div>
                </div>""", unsafe_allow_html=True)

        # ── SEC Filings ───────────────────────────────────────────────────────
        filings = r.get("filing_summary",[])
        if filings:
            st.markdown('<div class="sh">SEC Filings</div>', unsafe_allow_html=True)
            for f in filings[:3]:
                st.markdown(f'<div style="color:#2a4060;font-size:0.75em;padding:4px 0;font-family:\'JetBrains Mono\',monospace;border-bottom:1px solid rgba(255,255,255,0.03);">{f}</div>', unsafe_allow_html=True)

        # Sources
        sources = r.get("data_sources",[])
        st.markdown(f'<div style="margin-top:10px;color:#0e2040;font-size:0.65em;font-family:\'JetBrains Mono\',monospace;">SOURCES: {" · ".join(sources)}</div>', unsafe_allow_html=True)
        st.markdown('<div class="disc">⚠ RESEARCH TOOL ONLY — NOT FINANCIAL ADVICE</div>', unsafe_allow_html=True)


def render_holding_card(h):
    ticker  = h["ticker"]
    pnl_pct = h["unrealized_pnl_pct"]
    pnl     = h["unrealized_pnl"]
    rec     = h["recommendation"]
    score   = h.get("final_score", 50)
    flags   = h.get("active_flags", [])
    parrow  = "▲" if pnl >= 0 else "▼"
    pcolor  = "#00e87a" if pnl >= 0 else "#ff2d55"
    rcol    = {"Sell":"#ff2d55","Trim":"#ff6400","Hold":"#ffb700","Add (if confirmed)":"#00e87a"}.get(rec,"#6b8caa")
    sc      = score_col(score)
    exp_label = f"{ticker}  ·  {rec}  ·  {parrow}{abs(pnl_pct):.1f}%  ·  ${h['position_value']:,.0f}"

    with st.expander(exp_label, expanded=False):
        st.markdown(f"""
        <div class="port-header">
          <div style="display:flex;align-items:center;gap:14px;">
            <span style="font-family:'Bebas Neue',sans-serif;font-size:1.8em;color:#e8f4ff;letter-spacing:0.08em;">{ticker}</span>
            <span style="font-family:'Space Grotesk',sans-serif;font-size:0.95em;font-weight:700;color:{rcol};letter-spacing:0.05em;text-transform:uppercase;">{rec}</span>
          </div>
          <div style="text-align:right;">
            <div style="font-family:'Bebas Neue',sans-serif;font-size:1.6em;color:{pcolor};letter-spacing:0.05em;text-shadow:0 0 15px {pcolor}60;">{parrow} {abs(pnl_pct):.2f}%</div>
            <div style="color:#2a4060;font-size:0.72em;font-family:'JetBrains Mono',monospace;">${pnl:+,.2f} unrealized</div>
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
                st.markdown(f'<div style="color:#3a5878;font-size:0.82em;padding:3px 0;line-height:1.6;">{part}</div>', unsafe_allow_html=True)

        if flags:
            st.markdown("<div style='margin-top:8px;'>" + flag_chips(flags) + "</div>", unsafe_allow_html=True)


def render_deep_dive(r):
    if r.get("error"):
        st.error(f"Could not analyze {r.get('ticker')}: {r['error']}")
        return
    if r.get("filtered_out"):
        st.warning(f"{r['ticker']} filtered out: {r.get('filter_reason')}")
        return

    ticker = r["ticker"]
    name   = r.get("company_name", ticker)
    price  = r.get("price", 0)
    signal = r.get("signal", "—")
    score  = r.get("final_score", 0)
    flags  = r.get("risk_flags", [])
    sc     = score_col(score)
    pc     = pill_class(signal)
    sclr   = sig_color(signal)

    # ── Hero header ──────────────────────────────────────────────────────────
    col_hero, col_ring = st.columns([5, 1])
    with col_hero:
        st.markdown(f"""
        <div class="ticker-hero">
          <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap;">
            <span style="font-family:'Bebas Neue',sans-serif;font-size:2.8em;color:#e8f4ff;letter-spacing:0.1em;line-height:1;">{ticker}</span>
            <div>
              <div style="color:#3a5878;font-size:0.88em;margin-bottom:4px;">{name}</div>
              <span class="pill {pc}">{signal}</span>
              <span style="margin-left:8px;">{flag_chips(flags)}</span>
            </div>
          </div>
          <div style="display:flex;align-items:baseline;gap:20px;margin-top:12px;flex-wrap:wrap;">
            <span style="font-family:'Bebas Neue',sans-serif;font-size:2.2em;color:#e8f4ff;letter-spacing:0.05em;">{fp(price)}</span>
            <span style="font-family:'JetBrains Mono',monospace;font-size:0.85em;color:{sc};font-weight:600;">{score:.0f}/100</span>
          </div>
        </div>
        """, unsafe_allow_html=True)
    with col_ring:
        st.markdown(score_ring_svg(score, sc, 90), unsafe_allow_html=True)

    # ── Price Chart ───────────────────────────────────────────────────────────
    hist = get_price_history(ticker, 60)
    if not hist.empty:
        fig = go.Figure()
        fig.add_trace(go.Candlestick(
            x=hist.index, open=hist["Open"], high=hist["High"],
            low=hist["Low"], close=hist["Close"], name=ticker,
            increasing_line_color="#00e87a", increasing_fillcolor="rgba(0,232,122,0.1)",
            decreasing_line_color="#ff2d55", decreasing_fillcolor="rgba(255,45,85,0.1)",
        ))
        if len(hist) >= 20:
            sma20 = hist["Close"].rolling(20).mean()
            fig.add_trace(go.Scatter(x=hist.index, y=sma20, name="SMA20",
                line=dict(color="#ffb700", width=1, dash="dot"), opacity=0.7))
        entry = r.get("entry_zone"); stop_ = r.get("stop_loss")
        t1 = r.get("target_1")
        if entry: fig.add_hline(y=entry, line_color="rgba(0,150,255,0.4)", line_dash="dash",
                                annotation_text="Entry", annotation_font_color="#33b3ff", annotation_font_size=10)
        if stop_: fig.add_hline(y=stop_, line_color="rgba(255,45,85,0.4)", line_dash="dash",
                                annotation_text="Stop", annotation_font_color="#ff2d55", annotation_font_size=10)
        if t1:    fig.add_hline(y=t1, line_color="rgba(0,232,122,0.3)", line_dash="dot",
                                annotation_text="T1", annotation_font_color="#00e87a", annotation_font_size=10)

        fig.update_layout(
            height=310,
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(8,15,26,0.8)",
            font_color="#2a4060", font_family="JetBrains Mono",
            xaxis=dict(gridcolor="rgba(255,255,255,0.03)", showgrid=True, zeroline=False,
                      showline=False, tickfont=dict(size=9)),
            yaxis=dict(gridcolor="rgba(255,255,255,0.03)", showgrid=True, zeroline=False,
                      showline=False, tickfont=dict(size=9)),
            xaxis_rangeslider_visible=False,
            legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#2a4060", size=9)),
            margin=dict(l=0,r=0,t=8,b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Radar + Stats ─────────────────────────────────────────────────────────
    cr, cs = st.columns([1,1])
    with cr:
        st.markdown('<div class="sh">Score Radar</div>', unsafe_allow_html=True)
        cats = ["Technical","Catalyst","Fundamental","Risk inv.","Sentiment"]
        vals = [r.get("technical_score",50), r.get("catalyst_score",50),
                r.get("fundamental_score",50), 100-r.get("risk_score",50), r.get("sentiment_score",50)]
        fig_r = go.Figure(go.Scatterpolar(
            r=vals+[vals[0]], theta=cats+[cats[0]], fill="toself",
            fillcolor="rgba(0,150,255,0.06)",
            line=dict(color="#0096ff", width=2),
            marker=dict(color="#0096ff", size=5),
        ))
        fig_r.update_layout(
            polar=dict(
                bgcolor="rgba(0,0,0,0)",
                radialaxis=dict(visible=True, range=[0,100], gridcolor="rgba(255,255,255,0.04)",
                                tickfont=dict(color="#112030",size=8,family="JetBrains Mono"),
                                linecolor="rgba(255,255,255,0.05)"),
                angularaxis=dict(gridcolor="rgba(255,255,255,0.04)",
                                 tickfont=dict(color="#2a4060",size=9,family="JetBrains Mono"),
                                 linecolor="rgba(255,255,255,0.05)"),
            ),
            paper_bgcolor="rgba(0,0,0,0)", font_color="#2a4060",
            showlegend=False, height=250, margin=dict(l=20,r=20,t=15,b=15),
        )
        st.plotly_chart(fig_r, use_container_width=True)

    with cs:
        st.markdown('<div class="sh">Key Data</div>', unsafe_allow_html=True)
        rw = r.get("runway_months"); rv = r.get("revenue_growth"); vl = r.get("volatility")
        rows = [
            ("Market Cap",    fm(r.get("market_cap")),                              ""),
            ("Rel. Volume",   f"{r.get('relative_volume',0):.2f}×",                 "b" if (r.get("relative_volume") or 0)>2 else ""),
            ("RSI (14)",      f"{r.get('rsi',0):.1f}" if r.get("rsi") else "—",    ""),
            ("1D/5D/20D",     f"{r.get('return_1d',0):+.1f}% / {r.get('return_5d',0):+.1f}% / {r.get('return_20d',0):+.1f}%",""),
            ("Volatility",    f"{vl:.0f}% ann." if vl else "—",                    "a" if vl and vl>100 else ""),
            ("Short % Float", fpct(r.get("short_percent_float")),                   "a" if (r.get("short_percent_float") or 0)>0.2 else ""),
            ("Rev Growth",    fpct(rv) if rv else "—",                              "g" if rv and rv>0.2 else "r" if rv and rv<0 else ""),
            ("Cash Runway",   f"~{rw:.0f} mo" if rw else "—",                      "r" if rw and rw<12 else "g" if rw and rw>24 else ""),
            ("Entry Zone",    fp(r.get("entry_zone")),                              "b"),
            ("Stop Loss",     fp(r.get("stop_loss")),                               "r"),
            ("Target 1",      fp(r.get("target_1")),                                "g"),
            ("Target 2",      fp(r.get("target_2")),                                "g"),
            ("Analyst",       (r.get("analyst_recommendation") or "—").upper(),     "g" if r.get("analyst_recommendation") in ("buy","strong_buy") else ""),
        ]
        for lbl, val, cls in rows:
            st.markdown(stat_row(lbl, val, cls), unsafe_allow_html=True)

        # Option A: Earnings
        ed = r.get("earnings_date"); dte = r.get("days_to_earnings")
        if ed:
            earn_cls = "r" if r.get("earnings_warning") else "p"
            earn_str = f"{ed} ({dte}d)" if dte is not None else ed
            st.markdown(stat_row("Earnings Date", earn_str, earn_cls), unsafe_allow_html=True)
            if r.get("earnings_warning"):
                st.markdown(f'<div class="earn-warn">⚠ EARNINGS IN {dte} DAYS — Binary event. Can move ±20-50%. Do not enter blindly.</div>', unsafe_allow_html=True)

        # Option C: Sector RS
        rs = r.get("sector_rs_label")
        if rs:
            is_out = "outperform" in rs.lower()
            is_under = "underperform" in rs.lower()
            rs_color = "#00e87a" if is_out else "#ff2d55" if is_under else "#ffb700"
            rs_bg    = "rgba(0,232,122,0.07)" if is_out else "rgba(255,45,85,0.07)" if is_under else "rgba(255,183,0,0.07)"
            st.markdown(f"""
            <div style="margin-top:8px;padding:7px 10px;background:{rs_bg};border:1px solid {rs_color}30;
                        border-radius:5px;color:{rs_color};font-size:0.7em;font-family:'JetBrains Mono',monospace;">
                RS: {rs}
            </div>""", unsafe_allow_html=True)

    # ── Full Analysis ─────────────────────────────────────────────────────────
    st.markdown('<div class="sh">Full Analysis</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="summary-block">{r.get("summary","")}</div>', unsafe_allow_html=True)

    # ── Catalysts & SEC ───────────────────────────────────────────────────────
    cat_notes = r.get("catalyst_notes",[]); fsumm = r.get("filing_summary",[])
    if cat_notes or fsumm:
        st.markdown('<div class="sh">Catalysts & SEC Filings</div>', unsafe_allow_html=True)
        for n in cat_notes:
            st.markdown(f'<div style="color:#00c864;font-size:0.82em;padding:4px 0;display:flex;align-items:center;gap:8px;"><span style="color:#00e87a;">✓</span> {n}</div>', unsafe_allow_html=True)
        for f in fsumm:
            st.markdown(f'<div style="color:#2a4060;font-size:0.78em;padding:4px 0;font-family:\'JetBrains Mono\',monospace;border-bottom:1px solid rgba(255,255,255,0.02);">{f}</div>', unsafe_allow_html=True)

    # ── AI Filing Summary ─────────────────────────────────────────────────────
    ai_sum = r.get("filing_ai_summary","")
    if ai_sum:
        st.markdown('<div class="sh">⚡ AI Filing Analysis</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="box box-blue">{ai_sum}</div>', unsafe_allow_html=True)

    # ── Risk Flags ────────────────────────────────────────────────────────────
    if flags:
        st.markdown('<div class="sh">Risk Flags</div>', unsafe_allow_html=True)
        for flag in flags:
            crit  = flag in {"going_concern","reverse_split_risk"}
            earn  = flag == "earnings_imminent"
            color = "#ff2d55" if crit else "#9b59ff" if earn else "#ffb700"
            st.markdown(f'<div style="color:{color};font-size:0.82em;padding:4px 0;display:flex;align-items:center;gap:6px;"><span>⚠</span> {RISK_FLAGS.get(flag,flag)}</div>', unsafe_allow_html=True)

    # ── Headlines ─────────────────────────────────────────────────────────────
    headlines = r.get("recent_headlines",[])
    if headlines:
        st.markdown('<div class="sh">News Headlines</div>', unsafe_allow_html=True)
        for h in headlines:
            sent = h.get("sentiment","neutral")
            dc   = {"positive":"#00e87a","negative":"#ff2d55"}.get(sent,"#2a4060")
            st.markdown(f"""
            <div class="news-item">
              <div class="news-dot" style="background:{dc};box-shadow:0 0 6px {dc}80;"></div>
              <div>
                <a href="{h.get('url','#')}" target="_blank" style="color:#4a90c4;font-size:0.82em;text-decoration:none;line-height:1.5;">{h.get('title','')}</a>
                <div style="color:#1a3050;font-size:0.68em;margin-top:2px;font-family:'JetBrains Mono',monospace;">{h.get('date','')} · {sent.upper()}</div>
              </div>
            </div>""", unsafe_allow_html=True)

    st.markdown('<div class="disc" style="margin-top:16px;">⚠ RESEARCH TOOL ONLY · NOT FINANCIAL ADVICE · VERIFY ALL DATA INDEPENDENTLY BEFORE ACTING</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# INIT
# ══════════════════════════════════════════════════════════════════════════════
initialize_db()


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div class="apex-logo">
      <div class="name">⚡ APEX</div>
      <div class="sub">SMALLCAP SCANNER</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown('<div class="disc">Research tool · Not financial advice</div>', unsafe_allow_html=True)

    st.markdown('<div style="padding:0 4px;"><div class="sh" style="margin-top:14px;">💼 Portfolio</div></div>', unsafe_allow_html=True)
    st.markdown('<div style="color:#112030;font-size:0.65em;font-family:\'JetBrains Mono\',monospace;margin-bottom:5px;padding:0 4px;">TICKER, SHARES, AVG_COST</div>', unsafe_allow_html=True)

    portfolio_text = st.text_area("h", height=120,
        placeholder="LAES, 100, 1.45\nWULF, 50, 4.20\nIREN, 25, 8.10",
        label_visibility="collapsed")

    col_save, col_clear = st.columns(2)
    with col_save:
        if st.button("SAVE", use_container_width=True):
            lines = [l.strip() for l in portfolio_text.strip().split("\n") if l.strip()]
            saved, errors = 0, []
            for line in lines:
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 3:
                    try:
                        upsert_holding(parts[0].upper(), float(parts[1]), float(parts[2]))
                        saved += 1
                    except ValueError as e:
                        errors.append(str(e))
                else:
                    errors.append(f"Bad format: {line}")
            if saved: st.success(f"✓ {saved} saved")
            for e in errors: st.error(e)
    with col_clear:
        if st.button("CLEAR", use_container_width=True):
            conn = get_connection()
            conn.execute("DELETE FROM portfolio")
            conn.commit()
            conn.close()
            st.rerun()

    st.markdown('<div style="padding:0 4px;"><div class="sh" style="margin-top:12px;">🔍 Scanner</div></div>', unsafe_allow_html=True)
    custom_tickers = st.text_input("t", placeholder="SOUN, BBAI, RGTI, IONQ",
        label_visibility="collapsed")
    use_default   = st.checkbox("Default universe", value=True)
    use_portfolio = st.checkbox("My portfolio", value=True)

    if st.button("⚡ RUN SCAN", type="primary", use_container_width=True):
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
            st.success(f"✓ {valid_n} stocks analyzed")

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
            <div class="num" style="color:#00e87a;text-shadow:0 0 15px rgba(0,232,122,0.5);">{buys}</div>
            <div class="lbl">BUY</div>
          </div>
          <div class="sig-count">
            <div class="num" style="color:#ffb700;text-shadow:0 0 15px rgba(255,183,0,0.5);">{watches}</div>
            <div class="lbl">WATCH</div>
          </div>
          <div class="sig-count">
            <div class="num" style="color:#ff2d55;text-shadow:0 0 15px rgba(255,45,85,0.5);">{sells}</div>
            <div class="lbl">SELL</div>
          </div>
        </div>
        <div style="color:#0e2040;font-size:0.6em;font-family:'JetBrains Mono',monospace;margin-top:6px;text-align:center;">
          LAST SCAN: {st.session_state.get('scan_time','—')} · {len(valid)} STOCKS
        </div>
        """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN TABS
# ══════════════════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4, tab5 = st.tabs(["⚡  Scanner","💼  Portfolio","🔬  Deep Dive","🔔  Live Alerts","ℹ️  Info"])


# ── TAB 1: SCANNER ────────────────────────────────────────────────────────────
with tab1:
    results = st.session_state.get("scan_results",[])
    valid   = [r for r in results if not r.get("filtered_out") and not r.get("error")]

    if not valid:
        st.markdown("""
        <div class="empty">
          <div class="ico">⚡</div>
          <h3>RUN A SCAN TO SEE RESULTS</h3>
          <p>Enter tickers in the sidebar or use the default universe.<br>
          The scanner surfaces stocks worth researching — not stocks to blindly buy.</p>
        </div>""", unsafe_allow_html=True)
    else:
        # Signal summary
        sc_map = {}
        for r in valid:
            s = r.get("signal","—"); sc_map[s] = sc_map.get(s,0)+1

        labels = [("STRONG BUY","Strong Buy Candidate","#00e87a"),
                  ("SPEC BUY","Speculative Buy","#00c864"),
                  ("WATCHLIST","Watchlist","#ffb700"),
                  ("HOLD","Hold","#ffa000"),
                  ("TRIM","Trim","#ff6400"),
                  ("SELL","Sell","#ff2d55"),
                  ("AVOID","Avoid","#c81432")]

        cols = st.columns(7)
        for col,(short,full,color) in zip(cols,labels):
            cnt = sc_map.get(full,0)
            col.markdown(f"""
            <div class="sig-count">
              <div class="num" style="color:{color};text-shadow:0 0 12px {color}60;">{cnt}</div>
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

        st.markdown(f'<div style="color:#0e2040;font-size:0.68em;font-family:\'JetBrains Mono\',monospace;margin-bottom:10px;letter-spacing:0.08em;">SHOWING {len(filtered)} OF {len(valid)} · SORTED BY SCORE</div>', unsafe_allow_html=True)

        for r in filtered:
            render_result_card(r)

        excluded = [r for r in results if r.get("filtered_out")]
        if excluded:
            with st.expander(f"🚫 {len(excluded)} tickers excluded"):
                for r in excluded:
                    st.markdown(f'<span style="font-family:\'JetBrains Mono\',monospace;color:#112030;font-size:0.75em;">{r["ticker"]} — {r.get("filter_reason","")}</span>', unsafe_allow_html=True)


# ── TAB 2: PORTFOLIO ──────────────────────────────────────────────────────────
with tab2:
    portfolio_df = get_portfolio()

    if portfolio_df.empty:
        st.markdown("""
        <div class="empty">
          <div class="ico">💼</div>
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
        pnl_color = "#00e87a" if total_pnl >= 0 else "#ff2d55"

        # Summary metrics
        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Total Value",    f"${summary.get('total_value',0):,.2f}")
        c2.metric("Cost Basis",     f"${summary.get('total_cost',0):,.2f}")
        c3.metric("Unrealized P&L", f"${total_pnl:+,.2f}", f"{summary.get('total_pnl_pct',0):+.1f}%")
        c4.metric("Holdings",       summary.get("num_holdings",0))
        c5.metric("Action Needed",  f"{summary.get('sell_count',0)+summary.get('trim_count',0)} Sell/Trim")

        if summary.get("concentrated_risk"):
            st.warning(f"⚠️ Concentration risk: **{', '.join(summary['concentrated_risk'])}** exceed 25% of portfolio.")

        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

        for h in sorted(holdings_analysis, key=lambda x: x["unrealized_pnl_pct"]):
            render_holding_card(h)

        if len(holdings_analysis) > 1:
            st.markdown('<div class="sh" style="margin-top:20px;">Allocation</div>', unsafe_allow_html=True)
            fig_pie = go.Figure(go.Pie(
                values=[h["position_value"] for h in holdings_analysis],
                labels=[h["ticker"] for h in holdings_analysis],
                hole=0.55, textinfo="label+percent",
                textfont=dict(family="JetBrains Mono",size=10,color="#e8f4ff"),
                marker=dict(
                    colors=["#00e87a","#0096ff","#ffb700","#ff2d55","#9b59ff","#00c8d4","#ff6400"],
                    line=dict(color="#04080f",width=3)
                ),
            ))
            fig_pie.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", font_color="#2a4060",
                legend=dict(font=dict(family="JetBrains Mono",color="#2a4060",size=10)),
                height=260, margin=dict(l=0,r=0,t=0,b=0))
            st.plotly_chart(fig_pie, use_container_width=True)


# ── TAB 3: DEEP DIVE ──────────────────────────────────────────────────────────
with tab3:
    di, db = st.columns([4,1])
    with di:
        dive_ticker = st.text_input("dd", placeholder="Enter any ticker — e.g. SOUN, BBAI, CIFR, IONQ",
            label_visibility="collapsed").upper().strip()
    with db:
        run_dive = st.button("🔬 ANALYZE", type="primary", use_container_width=True)

    if run_dive and dive_ticker:
        with st.spinner(f"Running full analysis on {dive_ticker}..."):
            result = scan_ticker(dive_ticker, save=False)
            st.session_state["dive_result"] = result
            st.session_state["dive_ticker"] = dive_ticker

    dive_res = st.session_state.get("dive_result")
    dive_key = st.session_state.get("dive_ticker","")

    if dive_res and dive_key == dive_ticker and dive_ticker:
        render_deep_dive(dive_res)
    elif not dive_ticker:
        st.markdown("""
        <div class="empty">
          <div class="ico">🔬</div>
          <h3>DEEP DIVE ANALYSIS</h3>
          <p>Type any ticker above for full technical, fundamental,<br>
          SEC filing, earnings calendar, sector RS, and AI analysis.</p>
        </div>""", unsafe_allow_html=True)


# ── TAB 4: LIVE ALERTS ────────────────────────────────────────────────────────
with tab4:
    st.markdown("## 🔔 Live Alert Feed")
    st.markdown('<p style="color:#2a4060;font-size:0.82em;font-family:\'JetBrains Mono\',monospace;">Real-time alerts from the background scanner. Refreshes every 30 seconds.</p>', unsafe_allow_html=True)

    # Auto-refresh
    refresh = st.button("🔄 Refresh Now", use_container_width=False)

    # ── EOD Report Download ───────────────────────────────────────────────────
    try:
        import json as _json
        with open("latest_report.json") as f:
            report_meta = _json.load(f)
        report_path = report_meta.get("path", "")
        report_date = report_meta.get("date", "")

        if report_path and os.path.exists(report_path):
            st.markdown('<div class="sh">📊 Latest EOD Report</div>', unsafe_allow_html=True)
            with open(report_path, "rb") as pdf_file:
                st.download_button(
                    label=f"⬇️ Download EOD Report — {report_date}",
                    data=pdf_file,
                    file_name=f"APEX_EOD_{report_date}.pdf",
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
    col_s1.metric("Scanner Status", "🟢 RUNNING" if is_running else "⚪ WAITING")
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
                chg_c = "#00e87a" if chg >= 0 else "#ff2d55"
                vwap_badge = '<span style="color:#00e87a;font-size:0.7em;">▲V</span>' if above else '<span style="color:#ff2d55;font-size:0.7em;">▼V</span>'
                mc.markdown(f"""
                <div style="background:var(--bgcard);border:1px solid var(--border);border-radius:6px;
                            padding:8px 10px;text-align:center;transition:border-color 0.2s;"
                     onmouseover="this.style.borderColor='var(--borderhi)'"
                     onmouseout="this.style.borderColor='var(--border)'">
                  <div style="font-family:'JetBrains Mono',monospace;font-size:0.85em;color:#5ba3d9;
                               font-weight:600;letter-spacing:0.05em;">{m['ticker']}</div>
                  <div style="font-family:'JetBrains Mono',monospace;font-size:0.8em;color:{chg_c};
                               font-weight:600;margin:2px 0;">{chg:+.1f}%</div>
                  <div style="display:flex;justify-content:center;gap:6px;margin-top:2px;">
                    {vwap_badge}
                    <span style="font-family:'JetBrains Mono',monospace;font-size:0.65em;color:#2a4060;">{rvol:.1f}×</span>
                    <span style="font-family:'JetBrains Mono',monospace;font-size:0.65em;color:#1e3a52;">{score:.0f}pt</span>
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
                f'<div style="color:#00e87a;font-size:0.68em;font-family:\'JetBrains Mono\','
                f'monospace;letter-spacing:0.1em;margin-bottom:5px;">▲ ABOVE VWAP ({len(above_list)})</div>',
                unsafe_allow_html=True)
            for ticker, d in above_list[:12]:
                st.markdown(f"""
                <div style="display:flex;justify-content:space-between;align-items:center;
                            padding:3px 8px;border-left:2px solid rgba(0,232,122,0.25);
                            background:rgba(0,232,122,0.02);border-radius:0 3px 3px 0;margin-bottom:2px;">
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.72em;color:#5ba3d9;">{ticker}</span>
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.68em;color:#2a4060;">${d['price']:.3f}</span>
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.72em;color:#00e87a;font-weight:600;">+{d['dist_pct']:.1f}%</span>
                </div>""", unsafe_allow_html=True)
        with vb:
            st.markdown(
                f'<div style="color:#ff2d55;font-size:0.68em;font-family:\'JetBrains Mono\','
                f'monospace;letter-spacing:0.1em;margin-bottom:5px;">▼ BELOW VWAP ({len(below_list)})</div>',
                unsafe_allow_html=True)
            for ticker, d in below_list[:12]:
                st.markdown(f"""
                <div style="display:flex;justify-content:space-between;align-items:center;
                            padding:3px 8px;border-left:2px solid rgba(255,45,85,0.25);
                            background:rgba(255,45,85,0.02);border-radius:0 3px 3px 0;margin-bottom:2px;">
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.72em;color:#5ba3d9;">{ticker}</span>
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.68em;color:#2a4060;">${d['price']:.3f}</span>
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.72em;color:#ff2d55;font-weight:600;">{d['dist_pct']:.1f}%</span>
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
            f'<div style="background:rgba(0,150,255,0.04);border:1px solid rgba(0,150,255,0.15);'
            f'border-radius:8px;padding:12px 16px;font-family:\'JetBrains Mono\',monospace;font-size:0.78em;color:#3a5878;">'
            f'<span style="color:#2a4060;">Screened:</span> <span style="color:#5ba3d9;">{wl_stats.get("screened",0):,} stocks</span> &nbsp;·&nbsp; '
            f'<span style="color:#2a4060;">Active:</span> <span style="color:#5ba3d9;">{wl_stats.get("interesting",0)}</span> &nbsp;·&nbsp; '
            f'<span style="color:#2a4060;">Watching:</span> <span style="color:#00e87a;">{len(wl_tickers)}</span><br><br>'
            f'<span style="color:#1e3a52;">{" · ".join(wl_tickers[:30])}{"..." if len(wl_tickers) > 30 else ""}</span>'
            f'</div>',
            unsafe_allow_html=True
        )

        gap_ups = wl_stats.get("gap_ups", [])
        if gap_ups:
            st.markdown(f'<div style="margin-top:8px;color:#00e87a;font-size:0.78em;font-family:\'JetBrains Mono\',monospace;">🚀 Gap-ups today: {", ".join(gap_ups)}</div>', unsafe_allow_html=True)

    except FileNotFoundError:
        st.info("No watchlist generated yet. The scanner builds today's watchlist at 6 AM ET, or you can trigger it manually.")
        if st.button("🔍 Build Watchlist Now", type="primary"):
            with st.spinner("Scanning universe for active stocks... (this takes 2-3 minutes)"):
                try:
                    from morning_screen import build_todays_watchlist
                    wl = build_todays_watchlist(max_stocks=50)
                    st.success(f"✓ Built watchlist with {len(wl)} stocks: {', '.join(wl[:10])}...")
                except Exception as e:
                    st.error(f"Screen failed: {e}")

    # Alert feed
    st.markdown('<div class="sh" style="margin-top:16px;">Alert History</div>', unsafe_allow_html=True)
    if not alert_log:
        st.markdown("""
        <div class="empty" style="padding:30px 20px;">
          <div class="ico" style="font-size:2em;">🔔</div>
          <h3 style="font-size:1.2em;">NO ALERTS YET</h3>
          <p>Alerts appear here when the scanner detects significant activity.<br>
          Make sure the background scanner is running on Railway.</p>
        </div>""", unsafe_allow_html=True)
    else:
        for alert in alert_log[:30]:
            # Color code by alert type
            if "🟢" in alert or "🚀" in alert or "✓" in alert:
                color = "#00e87a"; bg = "rgba(0,232,122,0.05)"
            elif "🔴" in alert or "⚠" in alert:
                color = "#ff2d55"; bg = "rgba(255,45,85,0.05)"
            elif "📋" in alert:
                color = "#9b59ff"; bg = "rgba(155,89,255,0.05)"
            elif "⚡" in alert:
                color = "#ffb700"; bg = "rgba(255,183,0,0.05)"
            elif "📰" in alert:
                color = "#0096ff"; bg = "rgba(0,150,255,0.05)"
            else:
                color = "#3a5878"; bg = "transparent"

            st.markdown(f"""
            <div style="display:flex;align-items:center;gap:12px;padding:8px 12px;
                        background:{bg};border-left:2px solid {color}40;
                        border-radius:0 6px 6px 0;margin-bottom:4px;">
                <span style="color:{color};font-family:'JetBrains Mono',monospace;font-size:0.8em;">{alert}</span>
            </div>""", unsafe_allow_html=True)

    # ── Paper Trades ──────────────────────────────────────────────────────────
    st.markdown('<div class="sh" style="margin-top:16px;">📝 Paper Trades</div>', unsafe_allow_html=True)
    try:
        from db.database import get_paper_trades
        pt_df = get_paper_trades(days=30)
        if pt_df.empty:
            st.markdown('<div style="color:#1e3a52;font-size:0.78em;font-family:\'JetBrains Mono\',monospace;padding:8px 0;">No paper trades logged yet — trades are logged automatically on gap-up and VWAP reclaim alerts.</div>', unsafe_allow_html=True)
        else:
            closed = pt_df[pt_df["outcome"] != "open"]
            wins   = len(closed[closed["outcome"] == "win"])
            losses = len(closed[closed["outcome"] == "loss"])
            total  = wins + losses
            wr     = wins / total * 100 if total > 0 else 0
            avg_pnl = closed["pnl_pct"].mean() if not closed.empty else 0

            pm1, pm2, pm3, pm4 = st.columns(4)
            pm1.metric("Open Trades",  len(pt_df[pt_df["outcome"] == "open"]))
            pm2.metric("Win Rate",     f"{wr:.0f}%"  if total > 0 else "—")
            pm3.metric("Closed W/L",   f"{wins}W / {losses}L")
            pm4.metric("Avg P&L",      f"{avg_pnl:+.1f}%" if total > 0 else "—")

            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
            for _, tr in pt_df.head(20).iterrows():
                outcome  = tr["outcome"]
                o_color  = {"win": "#00e87a", "loss": "#ff2d55", "open": "#ffb700"}.get(outcome, "#3a5878")
                o_emoji  = {"win": "✅", "loss": "❌", "open": "⏳"}.get(outcome, "—")
                sig_map  = {"gap_up": "GAP UP", "vwap_reclaim": "VWAP RECLAIM"}
                sig_label = sig_map.get(tr["signal_type"], tr["signal_type"].upper())
                pnl_str  = f"{tr['pnl_pct']:+.1f}%" if pd.notna(tr.get("pnl_pct")) else "—"
                exit_str = f"→ ${tr['exit_price']:.4f}" if pd.notna(tr.get("exit_price")) else ""
                st.markdown(f"""
                <div style="display:flex;align-items:center;gap:10px;padding:6px 10px;
                            border-left:2px solid {o_color}40;background:{o_color}08;
                            border-radius:0 5px 5px 0;margin-bottom:3px;flex-wrap:wrap;">
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.75em;color:#5ba3d9;font-weight:600;min-width:50px;">{tr['ticker']}</span>
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.65em;color:#2a4060;background:rgba(255,255,255,0.04);padding:1px 6px;border-radius:3px;">{sig_label}</span>
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.72em;color:#3a5878;">${tr['entry_price']:.4f} {exit_str}</span>
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.65em;color:#1e3a52;">stop ${tr['stop_price']:.4f} · t1 ${tr['target1']:.4f}</span>
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.72em;color:{o_color};font-weight:600;margin-left:auto;">{o_emoji} {pnl_str}</span>
                  <span style="font-family:'JetBrains Mono',monospace;font-size:0.62em;color:#0e2040;">{tr['trade_date']} {tr['entry_time']}</span>
                </div>""", unsafe_allow_html=True)
    except Exception as e:
        st.markdown(f'<div style="color:#1e3a52;font-size:0.75em;font-family:\'JetBrains Mono\',monospace;">Paper trades unavailable: {e}</div>', unsafe_allow_html=True)

    # Setup instructions
    with st.expander("⚙️ Setup Instructions — How to activate real-time alerts"):
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
        - 📱 Push notification within 60 seconds of a volume spike
        - 📱 Alert when a new 8-K is filed for any watchlist stock
        - 📱 Alert when a stock moves >5% in a single minute
        - ☀️ Morning brief at 9:25 AM with today's active watchlist
        """)

# ── TAB 5: INFO ───────────────────────────────────────────────────────────────
with tab5:
    st.markdown("""
## How APEX Scanner Works

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
- **⚡ Earnings Calendar** — flags stocks with earnings within 7 days (binary event warning)
- **👤 Insider Direction** — detects if Form 4 was a BUY or SELL with approximate value
- **📊 Sector Relative Strength** — compares stock vs its sector ETF over 20 days
- **🤖 AI Filing Analysis** — Claude reads and summarizes actual SEC filing text

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
⚠️ **Research tool only. Not financial advice. Small-cap stocks can lose 100% of value. Always do your own research.**
    """)
