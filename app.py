# app.py — APEX SmallCap Scanner
# ALL helper functions are defined FIRST (top of file).
# This fixes the NameError: functions called before definition.

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import sys, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DEFAULT_UNIVERSE, SCORING_WEIGHTS, RISK_FLAGS
from db.database import initialize_db, upsert_holding, get_portfolio
from core.scanner import scan_ticker, scan_universe
from analysis.portfolio import analyze_holding, compute_portfolio_summary
from data.market_data import fetch_ticker_snapshot, get_price_history
from analysis.technicals import compute_technicals
from db.database import initialize_db, upsert_holding, get_portfolio, get_connection

# ─── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="APEX Scanner",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;500;600;700&family=JetBrains+Mono:wght@300;400;500;600&family=Outfit:wght@300;400;500;600&display=swap');

:root {
    --bg:        #080c10;
    --bg2:       #0e1419;
    --bgcard:    #111820;
    --border:    #1e2d3d;
    --borderhi:  #2a4060;
    --green:     #00ff88;
    --blue:      #00aaff;
    --amber:     #ffaa00;
    --red:       #ff3355;
    --purple:    #aa55ff;
    --t1:        #e8f0f8;
    --t2:        #7a9ab8;
    --t3:        #3a5068;
    --tdim:      #1e2d3d;
}

.stApp { background: var(--bg) !important; }
.stApp > header { background: transparent !important; }
section[data-testid="stSidebar"] { background: var(--bg2) !important; border-right: 1px solid var(--border) !important; }
.main .block-container { padding: 1rem 2rem; max-width: 100%; }
* { font-family: 'Outfit', sans-serif; }
h1,h2,h3 { font-family: 'Rajdhani', sans-serif !important; letter-spacing: 0.05em; color: var(--t1) !important; }
#MainMenu, footer, .stDeployButton { visibility: hidden; }

.stTextInput input, .stTextArea textarea {
    background: var(--bgcard) !important; border: 1px solid var(--border) !important;
    color: var(--t1) !important; border-radius: 6px !important;
    font-family: 'JetBrains Mono', monospace !important;
}
.stButton > button {
    background: transparent !important; border: 1px solid var(--borderhi) !important;
    color: var(--t1) !important; font-family: 'Rajdhani', sans-serif !important;
    font-weight: 700 !important; font-size: 0.9em !important; letter-spacing: 0.1em !important;
    border-radius: 4px !important; text-transform: uppercase !important; transition: all 0.2s !important;
}
.stButton > button:hover { border-color: var(--blue) !important; color: var(--blue) !important; background: rgba(0,170,255,0.05) !important; }
.stButton > button[kind="primary"] { background: rgba(0,85,136,0.3) !important; border-color: var(--blue) !important; color: var(--blue) !important; }

.stTabs [data-baseweb="tab-list"] { background: transparent !important; border-bottom: 1px solid var(--border) !important; }
.stTabs [data-baseweb="tab"] { font-family: 'Rajdhani', sans-serif !important; font-weight: 700 !important; font-size: 0.9em !important; letter-spacing: 0.12em !important; color: var(--t3) !important; text-transform: uppercase !important; padding: 10px 22px !important; background: transparent !important; }
.stTabs [aria-selected="true"] { color: var(--blue) !important; border-bottom: 2px solid var(--blue) !important; }

[data-testid="metric-container"] { background: var(--bgcard) !important; border: 1px solid var(--border) !important; border-radius: 8px !important; padding: 12px 16px !important; }
[data-testid="metric-container"] label { color: var(--t2) !important; font-size: 0.72em !important; letter-spacing: 0.1em !important; text-transform: uppercase !important; font-family: 'JetBrains Mono', monospace !important; }
[data-testid="metric-container"] [data-testid="metric-value"] { color: var(--t1) !important; font-family: 'JetBrains Mono', monospace !important; }
[data-testid="stMetricDelta"] { font-family: 'JetBrains Mono', monospace !important; font-size: 0.8em !important; }

[data-testid="stExpander"] { background: var(--bgcard) !important; border: 1px solid var(--border) !important; border-radius: 8px !important; margin-bottom: 8px !important; }
[data-testid="stExpander"]:hover { border-color: var(--borderhi) !important; }
[data-testid="stExpander"] summary { color: var(--t1) !important; font-family: 'JetBrains Mono', monospace !important; font-size: 0.85em !important; }
hr { border-color: var(--border) !important; }
.stCheckbox label { color: var(--t2) !important; }

.pill { display:inline-block; padding:3px 12px; border-radius:20px; font-family:'Rajdhani',sans-serif; font-weight:700; font-size:0.72em; letter-spacing:0.12em; text-transform:uppercase; }
.p-sb  { background:rgba(0,255,136,0.1); color:#00ff88; border:1px solid rgba(0,255,136,0.3); }
.p-sp  { background:rgba(0,221,102,0.1); color:#00dd66; border:1px solid rgba(0,221,102,0.3); }
.p-wl  { background:rgba(255,221,0,0.1); color:#ffdd00; border:1px solid rgba(255,221,0,0.3); }
.p-ho  { background:rgba(255,170,0,0.1); color:#ffaa00; border:1px solid rgba(255,170,0,0.3); }
.p-tr  { background:rgba(255,119,0,0.1); color:#ff7700; border:1px solid rgba(255,119,0,0.3); }
.p-se  { background:rgba(255,51,85,0.1); color:#ff3355; border:1px solid rgba(255,51,85,0.3); }
.p-av  { background:rgba(204,17,51,0.1); color:#cc1133; border:1px solid rgba(204,17,51,0.4); }

.flag { display:inline-block; background:rgba(255,170,0,0.07); border:1px solid rgba(255,170,0,0.25); color:#ffaa00; padding:2px 8px; border-radius:3px; font-size:0.68em; font-family:'JetBrains Mono',monospace; margin:2px 2px 2px 0; }
.flag.crit { background:rgba(255,51,85,0.08); border-color:rgba(255,51,85,0.3); color:#ff3355; }

.sbar-bg { background:var(--border); border-radius:3px; height:5px; width:100%; }
.sbar-fill { border-radius:3px; height:5px; }

.stat-row { display:flex; justify-content:space-between; align-items:center; padding:5px 0; border-bottom:1px solid var(--border); }
.slbl { color:var(--t2); font-size:0.72em; letter-spacing:0.06em; text-transform:uppercase; font-family:'JetBrains Mono',monospace; }
.sval { color:var(--t1); font-size:0.82em; font-family:'JetBrains Mono',monospace; font-weight:500; }
.sval.g { color:#00ff88; } .sval.r { color:#ff3355; } .sval.a { color:#ffaa00; } .sval.b { color:#00aaff; }

.sh { font-family:'Rajdhani',sans-serif; font-size:0.68em; font-weight:700; letter-spacing:0.2em; text-transform:uppercase; color:var(--t3); margin:14px 0 6px; padding-bottom:5px; border-bottom:1px solid var(--border); }
.box { background:var(--bg2); border:1px solid var(--border); border-radius:7px; padding:12px 16px; color:var(--t2); font-size:0.875em; line-height:1.7; }
.disc { background:rgba(255,170,0,0.04); border:1px solid rgba(255,170,0,0.15); border-radius:5px; padding:8px 14px; color:rgba(255,170,0,0.5); font-size:0.68em; font-family:'JetBrains Mono',monospace; letter-spacing:0.04em; text-align:center; margin:6px 0; }
.empty { text-align:center; padding:60px 20px; color:var(--t3); }
.empty .ico { font-size:2.8em; margin-bottom:14px; }
.empty h3 { font-family:'Rajdhani',sans-serif; color:var(--t2) !important; letter-spacing:0.1em; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS — ALL DEFINED HERE BEFORE ANY UI CODE
# ══════════════════════════════════════════════════════════════════════════════

def pill_class(signal: str) -> str:
    return {"Strong Buy Candidate":"p-sb","Speculative Buy":"p-sp","Watchlist":"p-wl",
            "Hold":"p-ho","Trim":"p-tr","Sell":"p-se","Avoid":"p-av"}.get(signal,"p-ho")

def sig_color(signal: str) -> str:
    return {"Strong Buy Candidate":"#00ff88","Speculative Buy":"#00dd66","Watchlist":"#ffdd00",
            "Hold":"#ffaa00","Trim":"#ff7700","Sell":"#ff3355","Avoid":"#cc1133"}.get(signal,"#7a9ab8")

def score_col(s: float) -> str:
    if s>=70: return "#00ff88"
    if s>=55: return "#00dd66"
    if s>=45: return "#ffdd00"
    if s>=35: return "#ffaa00"
    if s>=25: return "#ff7700"
    return "#ff3355"

def fp(p) -> str:
    if p is None: return "—"
    return f"${p:.4f}" if p < 10 else f"${p:.2f}"

def fm(mc) -> str:
    if not mc: return "—"
    return f"${mc/1e9:.2f}B" if mc>=1e9 else f"${mc/1e6:.0f}M"

def fpct(v) -> str:
    return f"{v*100:.1f}%" if v is not None else "—"

def has_crit(flags: list) -> bool:
    return bool({"going_concern","reverse_split_risk"}.intersection(set(flags)))

def flag_chips(flags: list) -> str:
    labels = {"going_concern":"GOING CONCERN","shelf_registration":"SHELF REG",
               "atm_offering":"ATM OFFERING","reverse_split_risk":"REV SPLIT RISK",
               "high_short_interest":"HIGH SHORT","extreme_volatility":"HIGH VOL",
               "low_liquidity":"LOW LIQ","pump_signal":"PUMP RISK"}
    crits  = {"going_concern","reverse_split_risk"}
    return "".join(
        f'<span class="flag {"crit" if f in crits else ""}">{labels.get(f,f.upper().replace("_"," "))}</span>'
        for f in flags[:4]
    )

def sbar(score: float, color: str) -> str:
    return f'<div class="sbar-bg"><div class="sbar-fill" style="width:{score}%;background:{color};"></div></div>'

def stat_row(label: str, value: str, cls: str = "") -> str:
    return f'<div class="stat-row"><span class="slbl">{label}</span><span class="sval {cls}">{value}</span></div>'


def render_result_card(r: dict):
    """Render one scan result card. All functions used here are defined above."""
    ticker = r.get("ticker","?")
    name   = r.get("company_name", ticker)
    price  = r.get("price", 0)
    score  = r.get("final_score", 0)
    signal = r.get("signal","—")
    rvol   = r.get("relative_volume", 0) or 0
    ret1d  = r.get("return_1d", 0) or 0
    ret5d  = r.get("return_5d", 0) or 0
    flags  = r.get("risk_flags", [])
    rsi    = r.get("rsi")
    mc     = r.get("market_cap", 0)

    pc       = pill_class(signal)
    sc       = score_col(score)
    scolor   = sig_color(signal)
    rc       = "g" if ret1d >= 0 else "r"
    rarrow   = "▲" if ret1d >= 0 else "▼"

    cat_notes = r.get("catalyst_notes", [])
    cat_str   = cat_notes[0] if cat_notes else "No confirmed catalyst"
    main_risk = ""
    if flags:
        main_risk = {
            "going_concern":"Bankruptcy risk — going concern warning",
            "shelf_registration":"Dilution — shelf registration active",
            "atm_offering":"Dilution — ATM offering in progress",
            "reverse_split_risk":"Reverse split risk — proxy filed",
            "high_short_interest":f"High short interest — {fpct(r.get('short_percent_float'))} of float",
            "pump_signal":"Volume spike without confirmed catalyst",
            "extreme_volatility":f"High volatility — {r.get('volatility',0):.0f}% annualized",
            "low_liquidity":"Low avg volume — liquidity risk",
        }.get(flags[0], flags[0].replace("_"," ").title())

    exp_label = f"{ticker}   {signal}   Score: {score:.0f}   {fp(price)}   {rarrow}{abs(ret1d):.1f}%"

    with st.expander(exp_label, expanded=False):
        # Header row
        st.markdown(f"""
        <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;margin-bottom:12px;">
            <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
                <span style="font-family:'Rajdhani',sans-serif;font-size:1.55em;font-weight:700;color:#e8f0f8;letter-spacing:0.06em;">{ticker}</span>
                <span style="color:#7a9ab8;font-size:0.88em;">{name[:38]}</span>
                <span class="pill {pc}">{signal}</span>
                {flag_chips(flags)}
            </div>
            <div style="text-align:right;">
                <div style="font-family:'JetBrains Mono',monospace;font-size:1.7em;font-weight:600;color:{sc};">{score:.0f}</div>
                <div style="font-size:0.65em;color:#1e2d3d;letter-spacing:0.1em;">/100</div>
            </div>
        </div>
        {sbar(score, sc)}
        """, unsafe_allow_html=True)

        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

        # Metrics
        c1,c2,c3,c4,c5,c6 = st.columns(6)
        c1.metric("Price",    fp(price))
        c2.metric("Mkt Cap",  fm(mc))
        c3.metric("RVOL",     f"{rvol:.2f}×")
        c4.metric("1D Ret",   f"{ret1d:+.2f}%")
        c5.metric("5D Ret",   f"{ret5d:+.2f}%")
        c6.metric("RSI",      f"{rsi:.1f}" if rsi else "—")

        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

        # Score breakdown + trade zones
        left, right = st.columns(2)

        with left:
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
                <div style="margin-bottom:7px;">
                  <div style="display:flex;justify-content:space-between;margin-bottom:2px;">
                    <span style="font-size:0.72em;color:#7a9ab8;font-family:'JetBrains Mono',monospace;">{lbl}</span>
                    <span style="font-size:0.72em;color:{c};font-family:'JetBrains Mono',monospace;font-weight:600;">{val:.0f} <span style="color:#1e2d3d;">×{wt:.0%}</span></span>
                  </div>
                  {sbar(val, c)}
                </div>
                """, unsafe_allow_html=True)

        with right:
            st.markdown('<div class="sh">Trade Zones</div>', unsafe_allow_html=True)
            entry = r.get("entry_zone"); stop_ = r.get("stop_loss")
            t1    = r.get("target_1");   t2    = r.get("target_2")
            sp    = r.get("short_percent_float"); av  = r.get("avg_volume",0)
            st.markdown(
                stat_row("Entry Zone",   fp(entry),              "b") +
                stat_row("Stop Loss",    fp(stop_),              "r") +
                stat_row("Target 1",     fp(t1),                 "g") +
                stat_row("Target 2",     fp(t2),                 "g") +
                stat_row("Short %Float", fpct(sp) if sp else "—","a") +
                stat_row("Avg Volume",   f"{av:,}" if av else "—",""),
                unsafe_allow_html=True
            )

        # Catalyst / Risk
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        ca, ri = st.columns(2)
        with ca:
            st.markdown('<div class="sh">Main Catalyst</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="box" style="min-height:56px;">{cat_str}</div>', unsafe_allow_html=True)
        with ri:
            st.markdown('<div class="sh">Main Risk</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="box" style="min-height:56px;">{main_risk or "No major flags"}</div>', unsafe_allow_html=True)

        # Summary
        summary = r.get("summary","")
        if summary:
            st.markdown('<div class="sh">Analysis</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="box">{summary}</div>', unsafe_allow_html=True)

        # Headlines
        headlines = r.get("recent_headlines",[])
        if headlines:
            st.markdown('<div class="sh">Recent News</div>', unsafe_allow_html=True)
            for h in headlines[:4]:
                sent = h.get("sentiment","neutral")
                dc   = {"positive":"#00ff88","negative":"#ff3355"}.get(sent,"#7a9ab8")
                st.markdown(f"""
                <div style="display:flex;align-items:flex-start;gap:8px;padding:7px 0;border-bottom:1px solid #1e2d3d;">
                  <div style="width:7px;height:7px;border-radius:50%;background:{dc};margin-top:5px;flex-shrink:0;"></div>
                  <div>
                    <a href="{h.get('url','#')}" target="_blank" style="color:#7ab8ff;font-size:0.83em;text-decoration:none;">{h.get('title','')}</a>
                    <div style="color:#3a5068;font-size:0.7em;margin-top:1px;font-family:'JetBrains Mono',monospace;">{h.get('date','')} · {sent.upper()}</div>
                  </div>
                </div>""", unsafe_allow_html=True)

        # SEC filings
        filings = r.get("filing_summary",[])
        if filings:
            st.markdown('<div class="sh">SEC Filings</div>', unsafe_allow_html=True)
            for f in filings[:3]:
                st.markdown(f'<div style="color:#7a9ab8;font-size:0.78em;padding:3px 0;font-family:\'JetBrains Mono\',monospace;border-bottom:1px solid #1e2d3d;">{f}</div>', unsafe_allow_html=True)

        # Sources + disclaimer
        sources = r.get("data_sources",[])
        st.markdown(f'<div style="margin-top:10px;color:#1e2d3d;font-size:0.68em;font-family:\'JetBrains Mono\',monospace;">SOURCES: {" · ".join(sources)}</div>', unsafe_allow_html=True)
        st.markdown('<div class="disc">⚠ RESEARCH TOOL ONLY — NOT FINANCIAL ADVICE</div>', unsafe_allow_html=True)


def render_holding_card(h: dict):
    """Render a portfolio holding card."""
    ticker  = h["ticker"]
    pnl_pct = h["unrealized_pnl_pct"]
    pnl     = h["unrealized_pnl"]
    rec     = h["recommendation"]
    score   = h.get("final_score", 50)
    flags   = h.get("active_flags", [])

    pcls    = "g" if pnl >= 0 else "r"
    parrow  = "▲" if pnl >= 0 else "▼"
    rcol    = {"Sell":"#ff3355","Trim":"#ff7700","Hold":"#ffaa00","Add (if confirmed)":"#00ff88"}.get(rec,"#7a9ab8")

    exp_label = f"{ticker}   {rec}   P&L: {parrow}{abs(pnl_pct):.1f}%   Value: ${h['position_value']:,.0f}"

    with st.expander(exp_label, expanded=False):
        st.markdown(f"""
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;flex-wrap:wrap;gap:10px;">
          <div>
            <span style="font-family:'Rajdhani',sans-serif;font-size:1.45em;font-weight:700;color:#e8f0f8;">{ticker}</span>
            <span style="margin-left:14px;font-family:'Rajdhani',sans-serif;font-size:1.05em;font-weight:700;color:{rcol};letter-spacing:0.05em;">{rec.upper()}</span>
          </div>
          <div style="text-align:right;">
            <div style="font-family:'JetBrains Mono',monospace;font-size:1.5em;font-weight:600;color:{'#00ff88' if pnl>=0 else '#ff3355'};">{parrow} {abs(pnl_pct):.2f}%</div>
            <div style="color:#3a5068;font-size:0.78em;font-family:'JetBrains Mono',monospace;">${pnl:+,.2f} unrealized</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        c1,c2,c3,c4,c5,c6 = st.columns(6)
        c1.metric("Price Now",   fp(h["current_price"]))
        c2.metric("Avg Cost",    fp(h["avg_cost"]))
        c3.metric("Shares",      f"{h['shares']:,.0f}")
        c4.metric("Position",    f"${h['position_value']:,.0f}")
        c5.metric("Scan Score",  f"{score:.0f}/100")
        c6.metric("Stop Loss",   fp(h["suggested_stop"]))

        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        st.markdown('<div class="sh">Recommendation Reasoning</div>', unsafe_allow_html=True)
        for part in (h.get("reasoning","") or "").split(" | "):
            if part.strip():
                st.markdown(f'<div style="color:#7a9ab8;font-size:0.83em;padding:3px 0;line-height:1.6;">{part}</div>', unsafe_allow_html=True)

        if flags:
            st.markdown("<div style='margin-top:8px;'>" + flag_chips(flags) + "</div>", unsafe_allow_html=True)


def render_deep_dive(r: dict):
    """Full deep-dive view for one stock."""
    if r.get("error"):
        st.error(f"Could not analyze {r.get('ticker')}: {r['error']}")
        return
    if r.get("filtered_out"):
        st.warning(f"{r['ticker']} filtered out: {r.get('filter_reason')}")
        return

    ticker = r["ticker"]; name = r.get("company_name",ticker)
    price  = r.get("price",0); signal = r.get("signal","—")
    score  = r.get("final_score",0); flags = r.get("risk_flags",[])
    sc     = score_col(score); pc = pill_class(signal)

    st.markdown(f"""
    <div style="padding:18px 0 14px;border-bottom:1px solid #1e2d3d;margin-bottom:18px;">
      <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;">
        <span style="font-family:'Rajdhani',sans-serif;font-size:2.1em;font-weight:700;color:#e8f0f8;letter-spacing:0.07em;">{ticker}</span>
        <span style="color:#7a9ab8;font-size:0.95em;">{name}</span>
        <span class="pill {pc}">{signal}</span>
      </div>
      <div style="display:flex;align-items:center;gap:22px;margin-top:10px;flex-wrap:wrap;">
        <span style="font-family:'JetBrains Mono',monospace;font-size:1.9em;font-weight:600;color:#e8f0f8;">{fp(price)}</span>
        <span style="font-family:'JetBrains Mono',monospace;font-size:1.3em;font-weight:700;color:{sc};">{score:.0f}<span style="font-size:0.5em;color:#1e2d3d;">/100</span></span>
        {flag_chips(flags)}
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Chart
    hist = get_price_history(ticker, 60)
    if not hist.empty:
        fig = go.Figure()
        fig.add_trace(go.Candlestick(
            x=hist.index, open=hist["Open"], high=hist["High"],
            low=hist["Low"], close=hist["Close"], name=ticker,
            increasing_line_color="#00ff88", increasing_fillcolor="rgba(0,255,136,0.12)",
            decreasing_line_color="#ff3355", decreasing_fillcolor="rgba(255,51,85,0.12)",
        ))
        if len(hist) >= 20:
            fig.add_trace(go.Scatter(x=hist.index, y=hist["Close"].rolling(20).mean(),
                name="SMA20", line=dict(color="#ffaa00", width=1, dash="dot")))
        entry = r.get("entry_zone"); stop_ = r.get("stop_loss")
        if entry: fig.add_hline(y=entry, line_color="rgba(0,170,255,0.35)", line_dash="dash", annotation_text="Entry", annotation_font_color="#00aaff")
        if stop_: fig.add_hline(y=stop_, line_color="rgba(255,51,85,0.35)",  line_dash="dash", annotation_text="Stop",  annotation_font_color="#ff3355")
        fig.update_layout(
            height=320, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#0e1419",
            font_color="#7a9ab8", font_family="JetBrains Mono",
            xaxis=dict(gridcolor="#1e2d3d", showgrid=True, zeroline=False),
            yaxis=dict(gridcolor="#1e2d3d", showgrid=True, zeroline=False),
            xaxis_rangeslider_visible=False,
            legend=dict(bgcolor="rgba(0,0,0,0)"),
            margin=dict(l=0,r=0,t=10,b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Radar + stats
    cr, cs = st.columns(2)
    with cr:
        st.markdown('<div class="sh">Score Radar</div>', unsafe_allow_html=True)
        cats = ["Technical","Catalyst","Fundamental","Risk inv.","Sentiment"]
        vals = [r.get("technical_score",50), r.get("catalyst_score",50),
                r.get("fundamental_score",50), 100-r.get("risk_score",50), r.get("sentiment_score",50)]
        fig_r = go.Figure(go.Scatterpolar(
            r=vals+[vals[0]], theta=cats+[cats[0]], fill="toself",
            fillcolor="rgba(0,170,255,0.07)", line=dict(color="#00aaff",width=2),
            marker=dict(color="#00aaff",size=5),
        ))
        fig_r.update_layout(
            polar=dict(bgcolor="rgba(0,0,0,0)",
                radialaxis=dict(visible=True, range=[0,100], gridcolor="#1e2d3d",
                                tickfont=dict(color="#3a5068",size=8,family="JetBrains Mono")),
                angularaxis=dict(gridcolor="#1e2d3d",
                                 tickfont=dict(color="#7a9ab8",size=9,family="JetBrains Mono"))),
            paper_bgcolor="rgba(0,0,0,0)", font_color="#7a9ab8",
            showlegend=False, height=260, margin=dict(l=25,r=25,t=15,b=15),
        )
        st.plotly_chart(fig_r, use_container_width=True)

    with cs:
        st.markdown('<div class="sh">Key Data</div>', unsafe_allow_html=True)
        rw = r.get("runway_months"); rv = r.get("revenue_growth"); vl = r.get("volatility")
        rows = [
            ("Market Cap",    fm(r.get("market_cap")),                           ""),
            ("Rel. Volume",   f"{r.get('relative_volume',0):.2f}×",              "b" if (r.get("relative_volume") or 0)>2 else ""),
            ("RSI (14)",      f"{r.get('rsi',0):.1f}" if r.get("rsi") else "—", ""),
            ("1D/5D/20D Ret", f"{r.get('return_1d',0):+.1f}% / {r.get('return_5d',0):+.1f}% / {r.get('return_20d',0):+.1f}%",""),
            ("Volatility",    f"{vl:.0f}% ann." if vl else "—",                 "a" if vl and vl>100 else ""),
            ("Short % Float", fpct(r.get("short_percent_float")),                ""),
            ("Rev Growth",    fpct(rv) if rv else "—",                           "g" if rv and rv>0.2 else ""),
            ("Cash Runway",   f"~{rw:.0f} mo" if rw else "—",                   "r" if rw and rw<12 else ""),
            ("Entry Zone",    fp(r.get("entry_zone")),                           "b"),
            ("Stop Loss",     fp(r.get("stop_loss")),                            "r"),
            ("Target 1",      fp(r.get("target_1")),                             "g"),
            ("Target 2",      fp(r.get("target_2")),                             "g"),
            ("Analyst",       (r.get("analyst_recommendation") or "—").upper(), ""),
        ]
        for lbl, val, cls in rows:
            st.markdown(stat_row(lbl, val, cls), unsafe_allow_html=True)

    # Summary
    st.markdown('<div class="sh">Full Analysis</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="box">{r.get("summary","")}</div>', unsafe_allow_html=True)

    # Catalysts
    cat_notes = r.get("catalyst_notes",[]); fsumm = r.get("filing_summary",[])
    if cat_notes or fsumm:
        st.markdown('<div class="sh">Catalysts & SEC Filings</div>', unsafe_allow_html=True)
        for n in cat_notes:
            st.markdown(f'<div style="color:#00dd66;font-size:0.83em;padding:3px 0;">✓ {n}</div>', unsafe_allow_html=True)
        for f in fsumm:
            st.markdown(f'<div style="color:#7a9ab8;font-size:0.8em;padding:3px 0;font-family:\'JetBrains Mono\',monospace;">{f}</div>', unsafe_allow_html=True)

    # Risk flags
    if flags:
        st.markdown('<div class="sh">Risk Flags</div>', unsafe_allow_html=True)
        for flag in flags:
            crit  = flag in {"going_concern","reverse_split_risk"}
            color = "#ff3355" if crit else "#ffaa00"
            st.markdown(f'<div style="color:{color};font-size:0.83em;padding:3px 0;">{RISK_FLAGS.get(flag,flag)}</div>', unsafe_allow_html=True)

    # Headlines
    headlines = r.get("recent_headlines",[])
    if headlines:
        st.markdown('<div class="sh">News Headlines</div>', unsafe_allow_html=True)
        for h in headlines:
            sent = h.get("sentiment","neutral")
            dc   = {"positive":"#00ff88","negative":"#ff3355"}.get(sent,"#7a9ab8")
            st.markdown(f"""
            <div style="display:flex;align-items:flex-start;gap:8px;padding:7px 0;border-bottom:1px solid #1e2d3d;">
              <div style="width:7px;height:7px;border-radius:50%;background:{dc};margin-top:5px;flex-shrink:0;"></div>
              <div>
                <a href="{h.get('url','#')}" target="_blank" style="color:#7ab8ff;font-size:0.83em;text-decoration:none;">{h.get('title','')}</a>
                <div style="color:#3a5068;font-size:0.7em;margin-top:1px;font-family:'JetBrains Mono',monospace;">{h.get('date','')} · {sent.upper()}</div>
              </div>
            </div>""", unsafe_allow_html=True)

    st.markdown('<div class="disc">⚠ RESEARCH TOOL ONLY · NOT FINANCIAL ADVICE · VERIFY ALL DATA INDEPENDENTLY BEFORE ACTING</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# INIT DATABASE
# ══════════════════════════════════════════════════════════════════════════════
initialize_db()


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style="padding:14px 0 6px;">
      <div style="font-family:'Rajdhani',sans-serif;font-size:1.55em;font-weight:700;color:#e8f0f8;letter-spacing:0.1em;">⚡ APEX</div>
      <div style="font-family:'JetBrains Mono',monospace;font-size:0.62em;color:#1e2d3d;letter-spacing:0.2em;margin-top:1px;">SMALLCAP SCANNER</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown('<div class="disc">Research tool · Not financial advice</div>', unsafe_allow_html=True)

    st.markdown('<div style="font-family:\'Rajdhani\',sans-serif;font-size:0.68em;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:#1e2d3d;margin:14px 0 5px;">💼 Portfolio</div>', unsafe_allow_html=True)
    st.markdown('<div style="color:#1e2d3d;font-size:0.68em;font-family:\'JetBrains Mono\',monospace;margin-bottom:5px;">TICKER, SHARES, AVG_COST</div>', unsafe_allow_html=True)

    portfolio_text = st.text_area("holdings", height=130,
        placeholder="LAES, 100, 1.45\nWULF, 50, 4.20\nIREN, 25, 8.10",
        label_visibility="collapsed")

    if st.button("SAVE PORTFOLIO", use_container_width=True):
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
        if saved: st.success(f"✓ {saved} holding(s) saved")
        for e in errors: st.error(e)

    if st.button("🗑️ CLEAR ALL", use_container_width=True):
        conn = get_connection()
        conn.execute("DELETE FROM portfolio")
        conn.commit()
        conn.close()
        st.rerun()
    st.markdown('<div style="font-family:\'Rajdhani\',sans-serif;font-size:0.68em;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;color:#1e2d3d;margin:14px 0 5px;">🔍 Scanner</div>', unsafe_allow_html=True)

    custom_tickers = st.text_input("tickers", placeholder="SOUN, BBAI, RGTI, IONQ",
        label_visibility="collapsed")
    use_default   = st.checkbox("Default universe", value=True)
    use_portfolio = st.checkbox("My portfolio tickers", value=True)

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
            prog = st.progress(0, text="Starting scan...")
            results = []
            for i, t in enumerate(tickers):
                prog.progress((i+1)/len(tickers), text=f"Scanning {t}... ({i+1}/{len(tickers)})")
                try:
                    res = scan_ticker(t)
                    if res: results.append(res)
                except Exception:
                    pass
            results.sort(key=lambda r: (not r.get("filtered_out",False), r.get("final_score",0)), reverse=True)
            st.session_state["scan_results"] = results
            st.session_state["scan_time"] = datetime.now().strftime("%H:%M:%S")
            prog.empty()
            valid_count = len([r for r in results if not r.get("filtered_out")])
            st.success(f"✓ {valid_count} stocks analyzed")

    # Mini stats
    all_r = st.session_state.get("scan_results",[])
    if all_r:
        valid = [r for r in all_r if not r.get("filtered_out") and not r.get("error")]
        buys  = sum(1 for r in valid if r.get("signal") in ("Strong Buy Candidate","Speculative Buy"))
        sells = sum(1 for r in valid if r.get("signal") in ("Sell","Avoid"))
        st.markdown(f"""
        <div style="margin-top:10px;padding:10px;background:#0e1419;border:1px solid #1e2d3d;border-radius:6px;">
          <div style="display:flex;justify-content:space-between;margin-bottom:3px;">
            <span style="color:#3a5068;font-size:0.68em;font-family:'JetBrains Mono',monospace;">SCANNED</span>
            <span style="color:#7a9ab8;font-size:0.68em;font-family:'JetBrains Mono',monospace;">{len(valid)}</span>
          </div>
          <div style="display:flex;justify-content:space-between;margin-bottom:3px;">
            <span style="color:#3a5068;font-size:0.68em;font-family:'JetBrains Mono',monospace;">BUY CANDIDATES</span>
            <span style="color:#00ff88;font-size:0.68em;font-family:'JetBrains Mono',monospace;">{buys}</span>
          </div>
          <div style="display:flex;justify-content:space-between;">
            <span style="color:#3a5068;font-size:0.68em;font-family:'JetBrains Mono',monospace;">SELL/AVOID</span>
            <span style="color:#ff3355;font-size:0.68em;font-family:'JetBrains Mono',monospace;">{sells}</span>
          </div>
        </div>
        <div style="color:#1e2d3d;font-size:0.62em;font-family:'JetBrains Mono',monospace;margin-top:6px;text-align:center;">LAST SCAN: {st.session_state.get('scan_time','—')}</div>
        """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN TABS
# ══════════════════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4 = st.tabs(["⚡  SCANNER","💼  PORTFOLIO","🔬  DEEP DIVE","ℹ️  INFO"])


# ── TAB 1: SCANNER ────────────────────────────────────────────────────────────
with tab1:
    results = st.session_state.get("scan_results",[])
    valid   = [r for r in results if not r.get("filtered_out") and not r.get("error")]

    if not valid:
        st.markdown('<div class="empty"><div class="ico">⚡</div><h3>RUN A SCAN TO SEE RESULTS</h3><p style="color:#3a5068;font-family:\'JetBrains Mono\',monospace;font-size:0.83em;max-width:380px;margin:0 auto;">Enter tickers in the sidebar or use the default universe. The scanner surfaces stocks worth researching — not stocks to blindly buy.</p></div>', unsafe_allow_html=True)
    else:
        # Signal summary bar
        sc_map = {}
        for r in valid:
            s = r.get("signal","—"); sc_map[s] = sc_map.get(s,0)+1
        labels = [("Strong Buy","Strong Buy Candidate","#00ff88"),("Spec Buy","Speculative Buy","#00dd66"),
                  ("Watchlist","Watchlist","#ffdd00"),("Hold","Hold","#ffaa00"),
                  ("Trim","Trim","#ff7700"),("Sell","Sell","#ff3355"),("Avoid","Avoid","#cc1133")]
        cols = st.columns(7)
        for col,(short,full,color) in zip(cols,labels):
            cnt = sc_map.get(full,0)
            col.markdown(f'<div style="text-align:center;padding:8px;background:#0e1419;border:1px solid #1e2d3d;border-radius:6px;"><div style="font-family:\'JetBrains Mono\',monospace;font-size:1.35em;font-weight:600;color:{color};">{cnt}</div><div style="font-family:\'JetBrains Mono\',monospace;font-size:0.58em;color:#3a5068;letter-spacing:0.1em;">{short.upper()}</div></div>', unsafe_allow_html=True)

        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

        # Filters
        fc1,fc2,fc3 = st.columns([1,2,1])
        with fc1: min_score = st.slider("Min score",0,100,0)
        with fc2: sig_filter = st.multiselect("Signal",["Strong Buy Candidate","Speculative Buy","Watchlist","Hold","Trim","Sell","Avoid"],default=[],placeholder="All signals")
        with fc3: hide_crit = st.checkbox("Hide critical flags",False)

        filtered = [r for r in valid
            if r.get("final_score",0) >= min_score
            and (not sig_filter or r.get("signal") in sig_filter)
            and (not hide_crit or not has_crit(r.get("risk_flags",[])))]

        st.markdown(f'<div style="color:#1e2d3d;font-size:0.7em;font-family:\'JetBrains Mono\',monospace;margin-bottom:10px;">SHOWING {len(filtered)} OF {len(valid)} · SORTED BY SCORE</div>', unsafe_allow_html=True)

        for r in filtered:
            render_result_card(r)

        excluded = [r for r in results if r.get("filtered_out")]
        if excluded:
            with st.expander(f"🚫 {len(excluded)} tickers excluded by universe filter"):
                for r in excluded:
                    st.markdown(f'<span style="font-family:\'JetBrains Mono\',monospace;color:#3a5068;font-size:0.78em;">{r["ticker"]} — {r.get("filter_reason","")}</span>', unsafe_allow_html=True)


# ── TAB 2: PORTFOLIO ──────────────────────────────────────────────────────────
with tab2:
    portfolio_df = get_portfolio()

    if portfolio_df.empty:
        st.markdown('<div class="empty"><div class="ico">💼</div><h3>NO HOLDINGS SAVED</h3><p style="color:#3a5068;font-family:\'JetBrains Mono\',monospace;font-size:0.83em;">Enter your positions in the sidebar: TICKER, SHARES, AVG_COST</p></div>', unsafe_allow_html=True)
    else:
        scan_map = {r["ticker"]:r for r in st.session_state.get("scan_results",[]) if not r.get("filtered_out") and not r.get("error")}

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

        summary = compute_portfolio_summary(holdings_analysis)
        total_pnl = summary.get("total_pnl",0)

        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Total Value",  f"${summary.get('total_value',0):,.2f}")
        c2.metric("Cost Basis",   f"${summary.get('total_cost',0):,.2f}")
        c3.metric("Unrealized P&L", f"${total_pnl:+,.2f}", f"{summary.get('total_pnl_pct',0):+.1f}%")
        c4.metric("Holdings",     summary.get("num_holdings",0))
        c5.metric("Action Needed",f"{summary.get('sell_count',0)+summary.get('trim_count',0)} Sell/Trim")

        if summary.get("concentrated_risk"):
            st.warning(f"⚠️ Concentration risk: **{', '.join(summary['concentrated_risk'])}** exceed 25% of portfolio.")

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        for h in sorted(holdings_analysis, key=lambda x: x["unrealized_pnl_pct"]):
            render_holding_card(h)

        if len(holdings_analysis) > 1:
            st.markdown('<div class="sh">Allocation</div>', unsafe_allow_html=True)
            fig_pie = go.Figure(go.Pie(
                values=[h["position_value"] for h in holdings_analysis],
                labels=[h["ticker"] for h in holdings_analysis],
                hole=0.5, textinfo="label+percent",
                textfont=dict(family="JetBrains Mono",size=11,color="#e8f0f8"),
                marker=dict(colors=["#00ff88","#00aaff","#ffaa00","#ff3355","#aa55ff","#00ddff","#ff7700"],
                            line=dict(color="#080c10",width=2)),
            ))
            fig_pie.update_layout(paper_bgcolor="rgba(0,0,0,0)",font_color="#7a9ab8",
                legend=dict(font=dict(family="JetBrains Mono",color="#7a9ab8")),
                height=280, margin=dict(l=0,r=0,t=0,b=0))
            st.plotly_chart(fig_pie, use_container_width=True)


# ── TAB 3: DEEP DIVE ──────────────────────────────────────────────────────────
with tab3:
    di, db = st.columns([3,1])
    with di:
        dive_ticker = st.text_input("t", placeholder="Enter any ticker — e.g. SOUN, BBAI, IONQ",
            label_visibility="collapsed").upper().strip()
    with db:
        run_dive = st.button("🔬 ANALYZE", type="primary", use_container_width=True)

    if run_dive and dive_ticker:
        with st.spinner(f"Full analysis on {dive_ticker}..."):
            result = scan_ticker(dive_ticker, save=False)
            st.session_state["dive_result"] = result
            st.session_state["dive_ticker"] = dive_ticker

    dive_res = st.session_state.get("dive_result")
    dive_key = st.session_state.get("dive_ticker","")

    if dive_res and (not dive_ticker or dive_key == dive_ticker):
        render_deep_dive(dive_res)
    elif not dive_ticker:
        st.markdown('<div class="empty"><div class="ico">🔬</div><h3>DEEP DIVE ANALYSIS</h3><p style="color:#3a5068;font-family:\'JetBrains Mono\',monospace;font-size:0.83em;">Type any ticker above for full technical, fundamental,<br>SEC filing, and news analysis with interactive charts.</p></div>', unsafe_allow_html=True)


# ── TAB 4: INFO ───────────────────────────────────────────────────────────────
with tab4:
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

### Signal Labels

| Signal | Score | Meaning |
|--------|-------|---------|
| Strong Buy Candidate | 75–100 | Strong conditions — **still research before acting** |
| Speculative Buy | 60–75 | Good setup, acceptable risk |
| Watchlist | 45–60 | Interesting, not compelling yet |
| Hold | 35–45 | No edge for new entries |
| Trim | 25–35 | Weakening — consider reducing |
| Sell | 15–25 | Weak across the board |
| Avoid | 0–15 | Poor setup or critical flags active |

### Data Sources (All Free)
- **Yahoo Finance (yfinance)** — price, volume, fundamentals
- **SEC EDGAR** — S-3, 424B, 8-K filings, Form 4 insider trades
- **Yahoo Finance RSS** — news headlines and sentiment

---
⚠️ **Research tool only. Not financial advice. Small-cap stocks can lose 100% of value. Always do your own research.**
    """)
