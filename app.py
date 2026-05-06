# app.py
# Main Streamlit dashboard for the Small-Cap Stock Scanner.
# Run with: streamlit run app.py
#
# Layout:
#   Sidebar      → Portfolio input, scan controls
#   Tab 1        → Market Scanner (ranked scan results)
#   Tab 2        → Portfolio Dashboard (your holdings)
#   Tab 3        → Stock Deep Dive (individual analysis)
#   Tab 4        → Settings & Risk Disclaimer

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import json
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DEFAULT_UNIVERSE, SCORING_WEIGHTS, RISK_FLAGS
from db.database import initialize_db, upsert_holding, delete_holding, get_portfolio, get_latest_scan
from core.scanner import scan_ticker, scan_universe
from analysis.portfolio import analyze_holding, compute_portfolio_summary
from data.market_data import fetch_ticker_snapshot, get_price_history
from analysis.technicals import compute_technicals

# ─── Page Config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SmallCap Scanner",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Dark terminal aesthetic — fits the quant tool vibe */
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

    .stApp {
        background-color: #0d1117;
        font-family: 'IBM Plex Sans', sans-serif;
    }

    /* Score badge colors */
    .score-strong   { background: #0d3d0d; color: #4ade80; border: 1px solid #4ade80; padding: 2px 10px; border-radius: 4px; font-family: monospace; font-weight: 600; }
    .score-spec     { background: #1a3a0d; color: #86efac; border: 1px solid #86efac; padding: 2px 10px; border-radius: 4px; font-family: monospace; }
    .score-watch    { background: #1a2a0d; color: #d4d44a; border: 1px solid #d4d44a; padding: 2px 10px; border-radius: 4px; font-family: monospace; }
    .score-hold     { background: #1a1a0d; color: #fbbf24; border: 1px solid #fbbf24; padding: 2px 10px; border-radius: 4px; font-family: monospace; }
    .score-trim     { background: #2a1a0d; color: #fb923c; border: 1px solid #fb923c; padding: 2px 10px; border-radius: 4px; font-family: monospace; }
    .score-sell     { background: #2a0d0d; color: #f87171; border: 1px solid #f87171; padding: 2px 10px; border-radius: 4px; font-family: monospace; }
    .score-avoid    { background: #1a0d0d; color: #dc2626; border: 1px solid #dc2626; padding: 2px 10px; border-radius: 4px; font-family: monospace; }

    /* Warning flags */
    .risk-flag { background: #1a0d00; border: 1px solid #f59e0b; color: #fcd34d; padding: 4px 10px; border-radius: 4px; font-size: 0.8em; margin: 2px 0; display: inline-block; }

    /* Metric cards */
    .metric-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; margin: 4px 0; }
    .metric-label { color: #8b949e; font-size: 0.75em; text-transform: uppercase; letter-spacing: 0.1em; font-family: 'IBM Plex Mono', monospace; }
    .metric-value { color: #e6edf3; font-size: 1.4em; font-weight: 600; font-family: 'IBM Plex Mono', monospace; }

    /* Disclaimer box */
    .disclaimer { background: #160d00; border: 1px solid #f59e0b; border-radius: 6px; padding: 16px; color: #fcd34d; font-size: 0.85em; }

    h1, h2, h3 { color: #e6edf3 !important; font-family: 'IBM Plex Mono', monospace !important; }
    p, li { color: #8b949e; }

    .stTabs [data-baseweb="tab"] { color: #8b949e; }
    .stTabs [data-baseweb="tab"][aria-selected="true"] { color: #4ade80; border-bottom-color: #4ade80; }
</style>
""", unsafe_allow_html=True)

# ─── Initialize Database ───────────────────────────────────────────────────────
initialize_db()


# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📡 SmallCap Scanner")
    st.markdown("*Speculative stock research tool*")
    st.divider()

    # ── Portfolio Input ────────────────────────────────────────────────────────
    st.markdown("### 💼 My Portfolio")
    st.markdown(
        '<p style="font-size:0.8em; color:#8b949e;">Enter one holding per line:<br>'
        '<code>TICKER, SHARES, AVG_COST</code></p>',
        unsafe_allow_html=True
    )

    portfolio_text = st.text_area(
        "Holdings",
        height=160,
        placeholder="LAES, 100, 1.45\nWULF, 50, 4.20\nIREN, 25, 8.10",
        label_visibility="collapsed",
    )

    if st.button("💾 Save Portfolio", use_container_width=True):
        lines = [l.strip() for l in portfolio_text.strip().split("\n") if l.strip()]
        saved = 0
        errors = []
        for line in lines:
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 3:
                try:
                    ticker   = parts[0].upper()
                    shares   = float(parts[1])
                    avg_cost = float(parts[2])
                    upsert_holding(ticker, shares, avg_cost)
                    saved += 1
                except ValueError as e:
                    errors.append(f"Bad line: {line} ({e})")
            else:
                errors.append(f"Need 3 fields: {line}")
        if saved:
            st.success(f"✓ Saved {saved} holding(s)")
        for e in errors:
            st.error(e)

    st.divider()

    # ── Scan Controls ──────────────────────────────────────────────────────────
    st.markdown("### 🔍 Market Scanner")

    custom_tickers = st.text_input(
        "Scan these tickers (comma-separated):",
        placeholder="SOUN, BBAI, RGTI, QBTS",
        help="Leave empty to use the default universe"
    )

    use_default = st.checkbox("Include default universe", value=True)
    use_portfolio = st.checkbox("Include my portfolio tickers", value=True)

    if st.button("🚀 Run Scan", type="primary", use_container_width=True):
        tickers = []
        if custom_tickers:
            tickers += [t.strip().upper() for t in custom_tickers.split(",") if t.strip()]
        if use_default:
            tickers += DEFAULT_UNIVERSE
        if use_portfolio:
            portfolio_df = get_portfolio()
            if not portfolio_df.empty:
                tickers += portfolio_df["ticker"].tolist()

        tickers = list(dict.fromkeys(tickers))  # deduplicate, preserve order

        if not tickers:
            st.warning("Add some tickers first.")
        else:
            with st.spinner(f"Scanning {len(tickers)} tickers..."):
                results = scan_universe(tickers, delay=0.8)
                st.session_state["scan_results"] = results
                st.session_state["scan_time"] = datetime.now().strftime("%H:%M:%S")
            st.success(f"✓ Scan complete — {len(results)} stocks analyzed")

    st.divider()
    st.markdown(
        '<p style="font-size:0.75em; color:#555; text-align:center;">'
        '⚠️ Not financial advice.<br>Research tool only.</p>',
        unsafe_allow_html=True
    )


# ─── Main Content ─────────────────────────────────────────────────────────────
st.markdown("# 📡 SmallCap Stock Scanner")

scan_time = st.session_state.get("scan_time", "Not run yet")
st.markdown(
    f'<p style="color:#555; font-family:monospace; font-size:0.85em;">'
    f'Last scan: {scan_time} &nbsp;|&nbsp; '
    f'Weights: Tech {SCORING_WEIGHTS["technical"]:.0%} · '
    f'Catalyst {SCORING_WEIGHTS["catalyst"]:.0%} · '
    f'Fundamental {SCORING_WEIGHTS["fundamental"]:.0%} · '
    f'Risk {SCORING_WEIGHTS["risk"]:.0%} · '
    f'Sentiment {SCORING_WEIGHTS["sentiment"]:.0%}'
    f'</p>',
    unsafe_allow_html=True
)

tab1, tab2, tab3, tab4 = st.tabs(["🔍 Market Scanner", "💼 Portfolio", "🔬 Deep Dive", "⚙️ Info"])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1: MARKET SCANNER
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    results = st.session_state.get("scan_results", [])

    if not results:
        st.info("👈 Run a scan from the sidebar to see results.")
        st.markdown("""
        **How to use this scanner:**
        1. Enter any tickers in the sidebar, or use the default universe
        2. Click **Run Scan**
        3. Review the shortlist — these are stocks *worth researching*, not stocks to blindly buy
        4. Click into a stock in **Deep Dive** for full details

        **Remember:** A high score means "interesting conditions today." It does not mean "will go up."
        Always look at the actual catalysts and risk flags before acting.
        """)
    else:
        # ── Filter Controls ────────────────────────────────────────────────────
        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            min_score = st.slider("Min score", 0, 100, 0, key="min_score_filter")
        with col_f2:
            signal_filter = st.multiselect(
                "Signal filter",
                ["Strong Buy Candidate", "Speculative Buy", "Watchlist", "Hold", "Trim", "Sell", "Avoid"],
                default=[],
            )
        with col_f3:
            hide_flagged = st.checkbox("Hide stocks with critical flags", value=False)

        # Filter results
        display_results = [
            r for r in results
            if not r.get("filtered_out")
            and r.get("final_score", 0) >= min_score
            and (not signal_filter or r.get("signal") in signal_filter)
            and (not hide_flagged or not _has_critical_flag(r.get("risk_flags", [])))
        ]

        st.markdown(f"**Showing {len(display_results)} stocks** (of {len(results)} scanned)")
        st.divider()

        # ── Result Cards ───────────────────────────────────────────────────────
        for r in display_results:
            _render_result_card(r)

        # ── Filtered Out ───────────────────────────────────────────────────────
        filtered_out = [r for r in results if r.get("filtered_out")]
        if filtered_out:
            with st.expander(f"🚫 {len(filtered_out)} tickers filtered out (size/volume/price)"):
                for r in filtered_out:
                    st.markdown(
                        f"**{r['ticker']}** — {r.get('filter_reason', 'Does not meet filters')}"
                    )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2: PORTFOLIO DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    portfolio_df = get_portfolio()

    if portfolio_df.empty:
        st.info("No portfolio holdings saved. Enter your positions in the sidebar.")
    else:
        # Fetch current prices for portfolio
        with st.spinner("Fetching current prices for portfolio..."):
            holdings_analysis = []
            scan_results_map = {
                r["ticker"]: r
                for r in st.session_state.get("scan_results", [])
                if not r.get("filtered_out") and not r.get("error")
            }

            for _, row in portfolio_df.iterrows():
                ticker   = row["ticker"]
                shares   = row["shares"]
                avg_cost = row["avg_cost"]

                # Use scan results if available, otherwise fetch fresh
                if ticker in scan_results_map:
                    sr = scan_results_map[ticker]
                    current_price = sr.get("price", avg_cost)
                    final_score   = sr.get("final_score", 50)
                    active_flags  = sr.get("risk_flags", [])
                    technicals    = {"rsi_14": sr.get("rsi"), "macd_bullish": sr.get("macd_bullish")}
                    fundamentals  = {"runway_months": sr.get("runway_months")}
                else:
                    snap = fetch_ticker_snapshot(ticker)
                    current_price = snap.get("price", avg_cost) if snap else avg_cost
                    final_score   = 50
                    active_flags  = []
                    technicals    = {}
                    fundamentals  = {}

                analysis = analyze_holding(
                    ticker=ticker,
                    shares=shares,
                    avg_cost=avg_cost,
                    current_price=current_price,
                    final_score=final_score,
                    active_flags=active_flags,
                    technicals=technicals,
                    fundamentals=fundamentals,
                )
                holdings_analysis.append(analysis)

        summary = compute_portfolio_summary(holdings_analysis)

        # ── Portfolio Summary Bar ──────────────────────────────────────────────
        c1, c2, c3, c4, c5 = st.columns(5)
        pnl_color = "🟢" if summary.get("total_pnl", 0) >= 0 else "🔴"
        c1.metric("Total Value",    f"${summary.get('total_value', 0):,.2f}")
        c2.metric("Total Cost",     f"${summary.get('total_cost', 0):,.2f}")
        c3.metric("Unrealized P&L", f"${summary.get('total_pnl', 0):+,.2f}",
                  f"{summary.get('total_pnl_pct', 0):+.1f}%")
        c4.metric("Holdings",       summary.get("num_holdings", 0))
        c5.metric("Sell/Trim Flags", f"{summary.get('sell_count',0) + summary.get('trim_count',0)}")

        # Concentration warning
        if summary.get("concentrated_risk"):
            st.warning(
                f"⚠️ Concentration risk: {', '.join(summary['concentrated_risk'])} "
                f"each represent >25% of your portfolio. Over-concentration in speculative names is dangerous."
            )

        st.divider()

        # ── Holdings Table ────────────────────────────────────────────────────
        for h in sorted(holdings_analysis, key=lambda x: x["unrealized_pnl_pct"]):
            _render_holding_card(h)

        # ── Portfolio Pie Chart ────────────────────────────────────────────────
        if len(holdings_analysis) > 1:
            st.subheader("Portfolio Allocation")
            fig = px.pie(
                values=[h["position_value"] for h in holdings_analysis],
                names=[h["ticker"] for h in holdings_analysis],
                hole=0.4,
                color_discrete_sequence=px.colors.sequential.Viridis,
            )
            fig.update_layout(
                paper_bgcolor="#0d1117",
                plot_bgcolor="#0d1117",
                font_color="#8b949e",
                showlegend=True,
            )
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3: DEEP DIVE
# ═══════════════════════════════════════════════════════════════════════════════
with tab3:
    dive_ticker = st.text_input(
        "Enter ticker for deep dive analysis:",
        placeholder="SOUN",
        key="dive_ticker"
    ).upper().strip()

    if dive_ticker:
        if st.button(f"🔬 Analyze {dive_ticker}", type="primary"):
            with st.spinner(f"Running full analysis on {dive_ticker}..."):
                result = scan_ticker(dive_ticker, save=False)
                st.session_state["dive_result"] = result

        result = st.session_state.get("dive_result")
        if result and result.get("ticker") == dive_ticker:
            _render_deep_dive(result)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4: INFO & DISCLAIMER
# ═══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown("""
    ## ⚙️ How This Scanner Works

    ### Scoring Model

    Each stock receives a score from 0-100 based on five components.
    **These weights are your assumptions** — edit `config.py` to change them.

    | Component | Weight | What it measures |
    |-----------|--------|-----------------|
    | Technical | 30% | Price action, volume, momentum, moving averages |
    | Catalyst  | 25% | SEC filings, news, partnerships, sector tailwinds |
    | Fundamental | 20% | Revenue growth, cash, debt, burn rate |
    | Risk (inverted) | 15% | Short interest, dilution, volatility, liquidity |
    | Sentiment | 10% | News tone, analyst coverage, hype detection |

    ### Data Sources (All Free)

    | Source | What we use it for |
    |--------|-------------------|
    | **Yahoo Finance (yfinance)** | Price, volume, fundamentals, history |
    | **SEC EDGAR** | S-3/424B filings (dilution), 8-K events, Form 4 (insider trades) |
    | **Yahoo Finance RSS** | News headlines, sentiment analysis |

    ### Signal Labels

    | Signal | Score Range | Meaning |
    |--------|-------------|---------|
    | Strong Buy Candidate | 75-100 | Strong signals across multiple factors — **still research before acting** |
    | Speculative Buy | 60-75 | Good signals with acceptable risk — speculative by nature |
    | Watchlist | 45-60 | Interesting but not compelling yet |
    | Hold | 35-45 | No clear edge for new positions |
    | Trim | 25-35 | Weakening — consider reducing exposure |
    | Sell | 15-25 | Weak across factors |
    | Avoid | 0-15 | Poor setup or critical risk flags active |

    ### Upgrade Path (When You're Ready to Pay)

    | Data you need | Paid option | Monthly cost |
    |--------------|-------------|-------------|
    | Real short interest | Finviz Elite or Quandl | $25-50/mo |
    | Options flow | Unusual Whales or Market Chameleon | $50-150/mo |
    | Better news API | Benzinga Pro or Polygon.io | $50-200/mo |
    | SEC real-time alerts | SEC EDGAR Pro or Sentieo | $100-500/mo |
    | Institutional ownership | 13F data from WhaleWisdom | Free-$50/mo |
    """)

    st.markdown("""
    ---
    ## ⚠️ Risk Disclaimer

    **This tool is for educational and research purposes only.**

    - This is NOT financial advice
    - Scanner signals are NOT buy or sell recommendations
    - Past patterns do NOT predict future price movements
    - Small-cap and speculative stocks can lose 50-100% of their value rapidly
    - Dilution, reverse splits, and bankruptcy are real and common risks in this universe
    - The scoring model contains **your assumptions**, not objective market truth
    - No backtested track record exists for this system
    - Always do your own due diligence before investing any money
    - Never invest money you cannot afford to lose entirely

    **If you are unsure about any investment, consult a licensed financial advisor.**
    """)


# ─── Helper Functions ──────────────────────────────────────────────────────────

def _has_critical_flag(flags: list) -> bool:
    critical = {"going_concern", "reverse_split_risk"}
    return bool(critical.intersection(set(flags)))


def _signal_color(signal: str) -> str:
    colors = {
        "Strong Buy Candidate": "#4ade80",
        "Speculative Buy":      "#86efac",
        "Watchlist":            "#d4d44a",
        "Hold":                 "#fbbf24",
        "Trim":                 "#fb923c",
        "Sell":                 "#f87171",
        "Avoid":                "#dc2626",
    }
    return colors.get(signal, "#8b949e")


def _signal_css_class(signal: str) -> str:
    mapping = {
        "Strong Buy Candidate": "score-strong",
        "Speculative Buy":      "score-spec",
        "Watchlist":            "score-watch",
        "Hold":                 "score-hold",
        "Trim":                 "score-trim",
        "Sell":                 "score-sell",
        "Avoid":                "score-avoid",
    }
    return mapping.get(signal, "score-hold")


def _render_result_card(r: dict):
    """Render a single scan result as a styled expandable card."""
    ticker    = r.get("ticker", "?")
    name      = r.get("company_name", ticker)
    price     = r.get("price", 0)
    mc        = r.get("market_cap", 0)
    score     = r.get("final_score", 0)
    signal    = r.get("signal", "—")
    rvol      = r.get("relative_volume", 0)
    ret1d     = r.get("return_1d", 0)
    flags     = r.get("risk_flags", [])
    sig_color = _signal_color(signal)
    sig_class = _signal_css_class(signal)

    mc_str    = f"${mc/1e9:.2f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"
    ret_str   = f"{'▲' if ret1d >= 0 else '▼'} {abs(ret1d):.1f}%"
    ret_color = "#4ade80" if ret1d >= 0 else "#f87171"

    flag_html = " ".join(
        f'<span class="risk-flag">{RISK_FLAGS.get(f, f).split(" ", 1)[-1][:30]}</span>'
        for f in flags[:3]
    )

    header = (
        f'<div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap;">'
        f'<span style="font-family:monospace; font-size:1.1em; font-weight:700; color:#e6edf3;">{ticker}</span>'
        f'<span style="color:#8b949e; font-size:0.85em;">{name[:35]}</span>'
        f'<span class="{sig_class}">{signal}</span>'
        f'<span style="font-family:monospace; color:#e6edf3; font-size:0.95em;">${price:.4f}</span>'
        f'<span style="color:{ret_color}; font-family:monospace; font-size:0.85em;">{ret_str}</span>'
        f'<span style="color:#8b949e; font-family:monospace; font-size:0.8em;">RVOL: {rvol:.1f}x</span>'
        f'<span style="color:#8b949e; font-family:monospace; font-size:0.8em;">{mc_str}</span>'
        f'<span style="font-family:monospace; font-size:0.9em; color:{sig_color}; font-weight:600;">{score}/100</span>'
        f'{flag_html}'
        f'</div>'
    )

    with st.expander(f"{ticker} — {signal} — {score}/100", expanded=False):
        st.markdown(header, unsafe_allow_html=True)
        st.divider()

        # Score breakdown
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Score Breakdown**")
            breakdown = r.get("score_breakdown", {})
            for k, v in breakdown.items():
                st.markdown(f"<span style='color:#8b949e; font-family:monospace; font-size:0.85em;'>  {k}: {v}</span>", unsafe_allow_html=True)

        with col_b:
            st.markdown("**Key Metrics**")
            rsi = r.get("rsi")
            entry = r.get("entry_zone")
            stop  = r.get("stop_loss")
            t1    = r.get("target_1")

            metrics = {
                "RSI (14)":      f"{rsi:.1f}" if rsi else "—",
                "RVOL":          f"{rvol:.2f}x",
                "Entry Zone":    f"${entry:.4f}" if entry else "—",
                "Stop Loss":     f"${stop:.4f}" if stop else "—",
                "Target 1":      f"${t1:.4f}" if t1 else "—",
                "Short %Float":  f"{r.get('short_percent_float',0)*100:.1f}%" if r.get('short_percent_float') else "—",
            }
            for k, v in metrics.items():
                st.markdown(f"<span style='color:#8b949e; font-family:monospace; font-size:0.85em;'>  {k}: <span style='color:#e6edf3;'>{v}</span></span>", unsafe_allow_html=True)

        # Summary
        st.divider()
        st.markdown(
            f'<p style="color:#8b949e; font-size:0.9em; line-height:1.6;">{r.get("summary", "")}</p>',
            unsafe_allow_html=True
        )

        # Headlines
        headlines = r.get("recent_headlines", [])
        if headlines:
            st.markdown("**Recent Headlines**")
            for h in headlines[:3]:
                sent_color = {"positive": "#4ade80", "negative": "#f87171"}.get(h.get("sentiment"), "#8b949e")
                st.markdown(
                    f'<p style="font-size:0.8em; margin:2px 0;">'
                    f'<span style="color:{sent_color}; font-family:monospace;">[{h.get("sentiment","?").upper()}]</span> '
                    f'<a href="{h.get("url","")}" style="color:#58a6ff;">{h.get("title","")}</a> '
                    f'<span style="color:#555;">({h.get("date","")})</span>'
                    f'</p>',
                    unsafe_allow_html=True
                )

        # SEC Filings
        filing_summary = r.get("filing_summary", [])
        if filing_summary:
            st.markdown("**Recent SEC Filings**")
            for f in filing_summary[:3]:
                st.markdown(f'<p style="color:#8b949e; font-size:0.8em; margin:2px 0;">{f}</p>', unsafe_allow_html=True)

        # Risk flags
        if flags:
            st.markdown("**⚠️ Active Risk Flags**")
            for flag in flags:
                flag_text = RISK_FLAGS.get(flag, flag)
                st.markdown(f'<div class="risk-flag">{flag_text}</div>', unsafe_allow_html=True)

        # Sources
        sources = r.get("data_sources", [])
        if sources:
            st.markdown(
                f'<p style="color:#444; font-size:0.75em; margin-top:8px;">Sources: {" | ".join(sources)}</p>',
                unsafe_allow_html=True
            )


def _render_holding_card(h: dict):
    """Render a portfolio holding card."""
    ticker  = h["ticker"]
    pnl_pct = h["unrealized_pnl_pct"]
    pnl     = h["unrealized_pnl"]
    rec     = h["recommendation"]
    pnl_color = "#4ade80" if pnl >= 0 else "#f87171"
    rec_colors = {
        "Sell":             "#dc2626",
        "Trim":             "#fb923c",
        "Hold":             "#fbbf24",
        "Add (if confirmed)": "#4ade80",
    }
    rec_color = rec_colors.get(rec, "#8b949e")

    with st.expander(
        f"{ticker} — {rec} — P&L: {'▲' if pnl >= 0 else '▼'}{abs(pnl_pct):.1f}%",
        expanded=False
    ):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Current Price", f"${h['current_price']:.4f}")
        c2.metric("Avg Cost",      f"${h['avg_cost']:.4f}")
        c3.metric("Shares",        f"{h['shares']:,.0f}")
        c4.metric("Position Value", f"${h['position_value']:,.2f}")

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Unrealized P&L",  f"${pnl:+,.2f}", f"{pnl_pct:+.1f}%")
        c6.metric("Scanner Score",   f"{h['final_score']:.0f}/100")
        c7.metric("Suggested Stop",  f"${h['suggested_stop']:.4f}")
        c8.metric("Hard Stop",       f"${h['hard_stop']:.4f}")

        st.markdown(
            f'<div style="background:#161b22; border:1px solid #30363d; border-radius:6px; padding:12px; margin-top:8px;">'
            f'<span style="color:{rec_color}; font-weight:600; font-family:monospace;">Recommendation: {rec}</span><br>'
            f'<span style="color:#8b949e; font-size:0.85em;">{h.get("reasoning", "")}</span>'
            f'</div>',
            unsafe_allow_html=True
        )

        if h.get("active_flags"):
            for flag in h["active_flags"]:
                flag_text = RISK_FLAGS.get(flag, flag)
                st.markdown(f'<div class="risk-flag" style="margin-top:4px;">{flag_text}</div>', unsafe_allow_html=True)


def _render_deep_dive(r: dict):
    """Render the full deep-dive analysis for a single stock."""
    if r.get("error"):
        st.error(f"Could not analyze {r.get('ticker')}: {r['error']}")
        return

    if r.get("filtered_out"):
        st.warning(f"**{r['ticker']} filtered out:** {r.get('filter_reason')}")
        return

    ticker = r["ticker"]
    name   = r.get("company_name", ticker)
    price  = r.get("price", 0)
    signal = r.get("signal", "—")

    st.markdown(f"## {ticker} — {name}")
    st.markdown(f"**Signal:** <span style='color:{_signal_color(signal)}; font-family:monospace; font-size:1.2em;'>{signal}</span> &nbsp; **Score:** {r.get('final_score',0)}/100", unsafe_allow_html=True)

    # ── Price Chart ────────────────────────────────────────────────────────────
    hist = get_price_history(ticker, 60)
    if not hist.empty:
        fig = go.Figure()
        fig.add_trace(go.Candlestick(
            x=hist.index,
            open=hist["Open"], high=hist["High"],
            low=hist["Low"],   close=hist["Close"],
            name=ticker,
            increasing_line_color="#4ade80",
            decreasing_line_color="#f87171",
        ))
        # SMA 20
        if len(hist) >= 20:
            sma20 = hist["Close"].rolling(20).mean()
            fig.add_trace(go.Scatter(x=hist.index, y=sma20, name="SMA 20",
                                      line=dict(color="#fbbf24", width=1)))

        fig.update_layout(
            height=350,
            paper_bgcolor="#0d1117",
            plot_bgcolor="#161b22",
            font_color="#8b949e",
            xaxis=dict(gridcolor="#21262d", showgrid=True),
            yaxis=dict(gridcolor="#21262d", showgrid=True),
            xaxis_rangeslider_visible=False,
            margin=dict(l=0, r=0, t=20, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Score Radar ────────────────────────────────────────────────────────────
    col_radar, col_metrics = st.columns([1, 1])
    with col_radar:
        categories = ["Technical", "Catalyst", "Fundamental", "Risk (inv.)", "Sentiment"]
        values = [
            r.get("technical_score", 50),
            r.get("catalyst_score", 50),
            r.get("fundamental_score", 50),
            100 - r.get("risk_score", 50),
            r.get("sentiment_score", 50),
        ]
        fig_radar = go.Figure(data=go.Scatterpolar(
            r=values + [values[0]],
            theta=categories + [categories[0]],
            fill="toself",
            line_color="#4ade80",
            fillcolor="rgba(74, 222, 128, 0.15)",
        ))
        fig_radar.update_layout(
            polar=dict(
                bgcolor="#161b22",
                radialaxis=dict(visible=True, range=[0,100], gridcolor="#21262d", color="#555"),
                angularaxis=dict(gridcolor="#21262d", color="#8b949e"),
            ),
            paper_bgcolor="#0d1117",
            font_color="#8b949e",
            showlegend=False,
            height=280,
            margin=dict(l=30, r=30, t=20, b=20),
        )
        st.plotly_chart(fig_radar, use_container_width=True)

    with col_metrics:
        st.markdown("**Key Data Points**")
        mc = r.get("market_cap", 0)
        mc_str = f"${mc/1e9:.2f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"
        data_rows = {
            "Price":          f"${price:.4f}",
            "Market Cap":     mc_str,
            "Volume Today":   f"{r.get('volume',0):,}",
            "Avg Volume":     f"{r.get('avg_volume',0):,}",
            "Rel. Volume":    f"{r.get('relative_volume',0):.2f}x",
            "RSI (14)":       f"{r.get('rsi',0):.1f}" if r.get('rsi') else "—",
            "1-Day Return":   f"{r.get('return_1d',0):+.2f}%",
            "5-Day Return":   f"{r.get('return_5d',0):+.2f}%",
            "20-Day Return":  f"{r.get('return_20d',0):+.2f}%",
            "Volatility":     f"{r.get('volatility',0):.0f}% ann." if r.get('volatility') else "—",
            "Short % Float":  f"{r.get('short_percent_float',0)*100:.1f}%" if r.get('short_percent_float') else "—",
            "Revenue Growth": f"{r.get('revenue_growth',0)*100:.0f}% YoY" if r.get('revenue_growth') else "—",
        }
        for k, v in data_rows.items():
            st.markdown(
                f'<div style="display:flex; justify-content:space-between; border-bottom:1px solid #21262d; padding:3px 0;">'
                f'<span style="color:#8b949e; font-size:0.85em;">{k}</span>'
                f'<span style="color:#e6edf3; font-family:monospace; font-size:0.85em;">{v}</span>'
                f'</div>',
                unsafe_allow_html=True
            )

    # ── Full Summary ──────────────────────────────────────────────────────────
    st.divider()
    st.markdown("**Analysis Summary**")
    st.markdown(
        f'<div style="background:#161b22; border:1px solid #30363d; border-radius:6px; padding:16px;">'
        f'<p style="color:#8b949e; line-height:1.7; margin:0;">{r.get("summary", "")}</p>'
        f'</div>',
        unsafe_allow_html=True
    )

    # ── Entry / Exit Zones ────────────────────────────────────────────────────
    st.divider()
    st.markdown("**Entry / Exit Zones** *(Starting points only — not trade instructions)*")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Entry Zone",  f"${r.get('entry_zone',0):.4f}" if r.get('entry_zone') else "—")
    c2.metric("Stop Loss",   f"${r.get('stop_loss',0):.4f}" if r.get('stop_loss') else "—")
    c3.metric("Target 1",    f"${r.get('target_1',0):.4f}" if r.get('target_1') else "—")
    c4.metric("Target 2",    f"${r.get('target_2',0):.4f}" if r.get('target_2') else "—")

    # ── Catalysts ─────────────────────────────────────────────────────────────
    cat_notes = r.get("catalyst_notes", [])
    filing_summary = r.get("filing_summary", [])
    if cat_notes or filing_summary:
        st.divider()
        st.markdown("**Catalysts & SEC Filings**")
        for n in cat_notes:
            st.markdown(f"- {n}")
        for f in filing_summary:
            st.markdown(f"- {f}")

    # ── Risk Flags ────────────────────────────────────────────────────────────
    flags = r.get("risk_flags", [])
    if flags:
        st.divider()
        st.markdown("**⚠️ Active Risk Flags**")
        for flag in flags:
            st.markdown(
                f'<div class="risk-flag" style="display:block; margin:4px 0;">{RISK_FLAGS.get(flag, flag)}</div>',
                unsafe_allow_html=True
            )

    # ── Headlines ─────────────────────────────────────────────────────────────
    headlines = r.get("recent_headlines", [])
    if headlines:
        st.divider()
        st.markdown("**Recent Headlines**")
        for h in headlines:
            sent_color = {"positive": "#4ade80", "negative": "#f87171"}.get(h.get("sentiment"), "#8b949e")
            st.markdown(
                f'[{h.get("title","")}]({h.get("url","")}) '
                f'— <span style="color:{sent_color}; font-family:monospace;">{h.get("sentiment","").upper()}</span> '
                f'({h.get("date","")})',
                unsafe_allow_html=True
            )

    # ── Data Sources ──────────────────────────────────────────────────────────
    st.divider()
    st.markdown(
        f'<p style="color:#444; font-size:0.8em;">📊 Data sources: {" | ".join(r.get("data_sources", []))}</p>',
        unsafe_allow_html=True
    )
    st.markdown(
        '<p style="color:#444; font-size:0.75em;">⚠️ This analysis is for research purposes only. Not financial advice. '
        'Verify all data independently before making investment decisions.</p>',
        unsafe_allow_html=True
    )
