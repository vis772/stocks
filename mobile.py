# mobile.py — Axiom Terminal Mobile
# Separate Streamlit app optimized for 390px (iPhone 15).
# Connects to the same PostgreSQL database as the main dashboard.
#
# Railway deployment: create a new Railway service in the same project,
# set the start command to:
#   streamlit run mobile.py --server.port $PORT --server.address 0.0.0.0 --server.headless true
# Add the same DATABASE_URL and other env vars as the main service.

import os
import sys
import json
import time as _time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import streamlit as st

st.set_page_config(
    page_title="Axiom Mobile",
    page_icon="A",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# ── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

:root {
  --bg:      #0d1117;
  --bg2:     #161b22;
  --bg3:     #21262d;
  --border:  #30363d;
  --t1:      #e6edf3;
  --t2:      #8b949e;
  --t3:      #484f58;
  --green:   #3fb950;
  --amber:   #d29922;
  --red:     #f85149;
  --blue:    #58a6ff;
  --purple:  #bc8cff;
}

html, body, [class*="css"] { background: var(--bg) !important; color: var(--t1) !important; }
.stApp { background: var(--bg) !important; }
.stApp > header { display: none !important; }
#MainMenu, footer, .stDeployButton { visibility: hidden !important; }
.main .block-container { padding: 0 0 100px !important; max-width: 100% !important; }
* { font-family: 'Inter', system-ui, sans-serif; box-sizing: border-box; }
section[data-testid="stSidebar"] { display: none !important; }

/* ── Top bar ── */
.mob-topbar {
  position: sticky; top: 0; z-index: 100;
  background: var(--bg); border-bottom: 1px solid var(--border);
  padding: 12px 16px 10px;
  display: flex; align-items: center; justify-content: space-between;
}
.mob-title {
  font-family: 'Inter', sans-serif; font-size: 1.05em;
  font-weight: 700; color: var(--t1); letter-spacing: -0.01em;
}
.mob-subtitle {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.6em; color: var(--t2); letter-spacing: 0.15em;
}
.mob-time {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.72em; color: var(--t2);
}

/* ── Nav bar ── */
.stButton > button {
  background: var(--bg2) !important;
  border: 1px solid var(--border) !important;
  color: var(--t2) !important;
  font-family: 'Inter', sans-serif !important;
  font-weight: 500 !important;
  font-size: 0.78em !important;
  border-radius: 8px !important;
  padding: 10px 4px !important;
  min-height: 44px !important;
  transition: all 0.15s !important;
  width: 100% !important;
}
.stButton > button:hover {
  border-color: #58a6ff !important;
  color: var(--t1) !important;
}
.nav-active .stButton > button {
  background: rgba(88,166,255,0.12) !important;
  border-color: var(--blue) !important;
  color: var(--blue) !important;
  font-weight: 700 !important;
}

/* ── Status card ── */
.status-card {
  background: var(--bg2); border: 1px solid var(--border);
  border-radius: 12px; padding: 18px 16px; margin: 12px 12px 8px;
}
.status-dot {
  display: inline-block; width: 12px; height: 12px;
  border-radius: 50%; margin-right: 8px; vertical-align: middle;
  flex-shrink: 0;
}
.status-label {
  font-size: 1em; font-weight: 700; color: var(--t1); vertical-align: middle;
}
.status-sub {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.72em; color: var(--t2); margin-top: 8px;
  display: flex; gap: 16px; flex-wrap: wrap;
}

/* ── Stats row ── */
.stats-row {
  display: grid; grid-template-columns: 1fr 1fr 1fr;
  gap: 8px; padding: 0 12px; margin-bottom: 12px;
}
.stat-card {
  background: var(--bg2); border: 1px solid var(--border);
  border-radius: 10px; padding: 12px 8px; text-align: center;
}
.stat-num {
  font-family: 'JetBrains Mono', monospace;
  font-size: 1.5em; font-weight: 600; line-height: 1;
}
.stat-lbl {
  font-size: 0.6em; color: var(--t2); letter-spacing: 0.08em;
  text-transform: uppercase; margin-top: 4px;
}

/* ── Log feed ── */
.log-header {
  font-size: 0.65em; font-weight: 600; letter-spacing: 0.1em;
  text-transform: uppercase; color: var(--t2);
  padding: 0 16px; margin: 16px 0 8px;
}
.log-item {
  display: flex; align-items: flex-start; gap: 8px;
  padding: 7px 16px; border-bottom: 1px solid rgba(48,54,61,0.5);
  font-family: 'JetBrains Mono', monospace; font-size: 0.72em;
}
.log-time { color: var(--t3); flex-shrink: 0; width: 42px; }
.log-msg  { color: var(--t2); line-height: 1.4; }
.log-msg.info    { color: #8b949e; }
.log-msg.warning { color: var(--amber); }
.log-msg.error   { color: var(--red); }

/* ── Signal card ── */
.sig-card {
  background: var(--bg2); border: 1px solid var(--border);
  border-radius: 10px; padding: 14px 16px; margin: 6px 12px;
  cursor: pointer;
}
.sig-ticker {
  font-family: 'JetBrains Mono', monospace;
  font-size: 1.15em; font-weight: 700; color: var(--blue);
}
.sig-label { font-size: 0.78em; color: var(--t2); margin-top: 2px; }
.sig-row {
  display: flex; justify-content: space-between; align-items: center;
  margin-top: 10px;
}
.sig-score {
  font-family: 'JetBrains Mono', monospace;
  font-size: 1.1em; font-weight: 700;
}
.sig-price { font-family: 'JetBrains Mono', monospace; font-size: 0.82em; color: var(--t2); }
.sig-ago   { font-size: 0.7em; color: var(--t3); margin-top: 6px; }
.breakdown-row {
  display: flex; justify-content: space-between;
  padding: 5px 0; border-bottom: 1px solid rgba(48,54,61,0.5);
  font-size: 0.72em;
}
.breakdown-row:last-child { border-bottom: none; }
.bk-lbl { color: var(--t2); }
.bk-val { font-family: 'JetBrains Mono', monospace; color: var(--t1); font-weight: 600; }

/* ── Alert item ── */
.alert-item {
  display: flex; align-items: flex-start; gap: 10px;
  padding: 10px 16px; border-bottom: 1px solid rgba(48,54,61,0.5);
}
.alert-dot {
  width: 6px; height: 6px; border-radius: 50%;
  flex-shrink: 0; margin-top: 5px;
}
.alert-time { font-family: 'JetBrains Mono', monospace; font-size: 0.68em; color: var(--t3); }
.alert-msg  { font-size: 0.8em; color: var(--t1); line-height: 1.4; }

/* ── Control buttons ── */
.ctrl-btn-red .stButton > button {
  background: rgba(248,81,73,0.12) !important;
  border-color: rgba(248,81,73,0.4) !important;
  color: #f85149 !important;
  font-size: 0.9em !important;
  font-weight: 700 !important;
  min-height: 54px !important;
  padding: 16px 8px !important;
}
.ctrl-btn-green .stButton > button {
  background: rgba(63,185,80,0.12) !important;
  border-color: rgba(63,185,80,0.4) !important;
  color: #3fb950 !important;
  font-size: 0.9em !important;
  font-weight: 700 !important;
  min-height: 54px !important;
  padding: 16px 8px !important;
}
.ctrl-btn-blue .stButton > button {
  background: rgba(88,166,255,0.12) !important;
  border-color: rgba(88,166,255,0.4) !important;
  color: #58a6ff !important;
  font-size: 0.9em !important;
  font-weight: 700 !important;
  min-height: 54px !important;
  padding: 16px 8px !important;
}

/* ── Section header ── */
.sec-hdr {
  font-size: 0.65em; font-weight: 600; letter-spacing: 0.1em;
  text-transform: uppercase; color: var(--t2);
  padding: 16px 16px 8px; border-top: 1px solid var(--border); margin-top: 8px;
}

/* ── Summary rows ── */
.sum-row {
  display: flex; justify-content: space-between; align-items: center;
  padding: 10px 16px; border-bottom: 1px solid rgba(48,54,61,0.5);
}
.sum-lbl { font-size: 0.8em; color: var(--t2); }
.sum-val {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.85em; color: var(--t1); font-weight: 600;
}

/* ── Filter pills ── */
.stRadio > div { display: flex !important; flex-wrap: wrap !important; gap: 6px !important; padding: 0 12px 12px !important; }
.stRadio label {
  background: var(--bg2) !important; border: 1px solid var(--border) !important;
  border-radius: 20px !important; padding: 5px 14px !important;
  font-size: 0.78em !important; font-weight: 500 !important;
  color: var(--t2) !important; cursor: pointer !important;
}
.stRadio label:has(input:checked) {
  background: rgba(88,166,255,0.12) !important;
  border-color: var(--blue) !important; color: var(--blue) !important;
}

/* ── Refresh hint ── */
.refresh-hint {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.6em; color: var(--t3); text-align: center; padding: 12px;
}
</style>
""", unsafe_allow_html=True)

# ── Auto-refresh JS ───────────────────────────────────────────────────────────
st.markdown(
    "<script>setTimeout(function(){window.location.reload();},60000);</script>",
    unsafe_allow_html=True,
)


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _now_et():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except ImportError:
        import pytz
        return datetime.now(pytz.timezone("America/New_York"))


def _mkt_status() -> str:
    et = _now_et()
    wd, h, m = et.weekday(), et.hour, et.minute
    if wd >= 5:                           return "Weekend"
    if h < 4:                             return "Closed"
    if h < 9 or (h == 9 and m < 25):     return "Pre-Market"
    if h < 16 or (h == 16 and m <= 30):  return "Open"
    return "After-Hours"


def _mins_ago(ts) -> str:
    if not ts:
        return "—"
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        dt_naive = dt.replace(tzinfo=None) if dt.tzinfo else dt
        mins = int((datetime.utcnow() - dt_naive).total_seconds() / 60)
        if mins < 1:   return "just now"
        if mins < 60:  return f"{mins}m ago"
        return f"{mins // 60}h {mins % 60}m ago"
    except Exception:
        return "—"


def _uptime(ts) -> str:
    if not ts:
        return "—"
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        dt_naive = dt.replace(tzinfo=None) if dt.tzinfo else dt
        secs = int((datetime.utcnow() - dt_naive).total_seconds())
        h, m = secs // 3600, (secs % 3600) // 60
        return f"{h}h {m}m"
    except Exception:
        return "—"


def _score_color(score) -> str:
    if score is None:  return "#8b949e"
    s = float(score)
    if s >= 75:  return "#3fb950"
    if s >= 60:  return "#58a6ff"
    if s >= 45:  return "#d29922"
    return "#f85149"


def _signal_pill_color(label: str) -> str:
    return {
        "Strong Buy Candidate": "#3fb950",
        "Speculative Buy":      "#58a6ff",
        "Watchlist":            "#d29922",
        "Hold":                 "#d29922",
        "Trim":                 "#f0883e",
        "Sell":                 "#f85149",
        "Avoid":                "#f85149",
    }.get(label, "#8b949e")


def _log_color(level: str) -> str:
    return {"warning": "warning", "error": "error"}.get(level.lower(), "info")


def _alert_dot_color(msg: str) -> str:
    m = msg.lower()
    if any(x in m for x in ["gap-up", "session high", "pre-market high", "pred buy", "volume spike"]):
        return "#3fb950"
    if any(x in m for x in ["gap-down", "session low", "pred sell", "loss", "error"]):
        return "#f85149"
    if any(x in m for x in ["news", "filing", "vwap", "extended"]):
        return "#d29922"
    return "#484f58"


def _load_control():
    try:
        from db.database import get_scanner_control, get_control_stats
        return get_scanner_control(), get_control_stats()
    except Exception:
        return (
            {"paused": False, "force_scan": False, "scanner_started_at": None},
            {"signals_today": 0, "alerts_today": 0, "top_signal": None,
             "scan_count": 0, "last_updated": None},
        )


# ══════════════════════════════════════════════════════════════════════════════
# SESSION STATE
# ══════════════════════════════════════════════════════════════════════════════

if "screen" not in st.session_state:
    st.session_state.screen = "status"


# ══════════════════════════════════════════════════════════════════════════════
# TOP BAR
# ══════════════════════════════════════════════════════════════════════════════

_et_now = _now_et()
st.markdown(f"""
<div class="mob-topbar">
  <div>
    <div class="mob-title">Axiom</div>
    <div class="mob-subtitle">TERMINAL MOBILE</div>
  </div>
  <div class="mob-time">{_et_now.strftime('%H:%M ET')}</div>
</div>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# NAVIGATION BAR
# ══════════════════════════════════════════════════════════════════════════════

_screens = [
    ("status",  "Status"),
    ("signals", "Signals"),
    ("alerts",  "Alerts"),
    ("control", "Control"),
]

_nc = st.columns(4)
for _col, (_key, _label) in zip(_nc, _screens):
    _active = st.session_state.screen == _key
    _wrap = "nav-active" if _active else ""
    with _col:
        st.markdown(f'<div class="{_wrap}">', unsafe_allow_html=True)
        if st.button(_label, key=f"nav_{_key}", use_container_width=True):
            st.session_state.screen = _key
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

_screen = st.session_state.screen


# ══════════════════════════════════════════════════════════════════════════════
# SCREEN 1 — STATUS
# ══════════════════════════════════════════════════════════════════════════════

if _screen == "status":
    _ctl, _sts = _load_control()
    _mkt = _mkt_status()
    _last_upd = _sts.get("last_updated")
    _mins = _mins_ago(_last_upd)

    # Determine scanner state
    _paused = _ctl.get("paused", False)
    if _paused:
        _dot = "#d29922"; _dot_bg = "rgba(210,153,34,0.1)"; _dot_border = "rgba(210,153,34,0.25)"
        _status_lbl = "PAUSED"
        _status_detail = "Manual hold active"
    elif _mkt in ("Open", "Pre-Market"):
        try:
            _lu = datetime.fromisoformat(str(_last_upd).replace("Z", "+00:00"))
            _lu_naive = _lu.replace(tzinfo=None) if _lu.tzinfo else _lu
            _m = int((datetime.utcnow() - _lu_naive).total_seconds() / 60)
        except Exception:
            _m = 999
        if _m <= 45:
            _dot = "#3fb950"; _dot_bg = "rgba(63,185,80,0.08)"; _dot_border = "rgba(63,185,80,0.2)"
            _status_lbl = "ACTIVE"
            _status_detail = f"Scanning every {'60s' if _mkt == 'Open' else '90s'}"
        else:
            _dot = "#f85149"; _dot_bg = "rgba(248,81,73,0.08)"; _dot_border = "rgba(248,81,73,0.2)"
            _status_lbl = "OFFLINE"
            _status_detail = "No scan in 45+ min"
    else:
        _dot = "#d29922"; _dot_bg = "rgba(210,153,34,0.08)"; _dot_border = "rgba(210,153,34,0.2)"
        _status_lbl = "SLEEPING"
        _status_detail = "Market closed"

    st.markdown(f"""
    <div class="status-card" style="border-color:{_dot_border};background:{_dot_bg};">
      <div style="display:flex;align-items:center;gap:10px;">
        <div class="status-dot" style="background:{_dot};box-shadow:0 0 0 4px {_dot}33;"></div>
        <span class="status-label" style="color:{_dot};">{_status_lbl}</span>
        <span style="font-size:0.78em;color:#8b949e;margin-left:4px;">{_status_detail}</span>
      </div>
      <div class="status-sub">
        <span>Last scan: {_mins}</span>
        <span>Market: {_mkt}</span>
        <span>Scans today: {_sts.get('scan_count', 0)}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Quick stats
    _top = _sts.get("top_signal")
    _top_score = f"{_top['score']:.0f}" if _top else "—"
    st.markdown(f"""
    <div class="stats-row">
      <div class="stat-card">
        <div class="stat-num" style="color:#58a6ff;">{_sts.get('signals_today', 0)}</div>
        <div class="stat-lbl">Signals</div>
      </div>
      <div class="stat-card">
        <div class="stat-num" style="color:#d29922;">{_sts.get('alerts_today', 0)}</div>
        <div class="stat-lbl">Alerts</div>
      </div>
      <div class="stat-card">
        <div class="stat-num" style="color:#3fb950;">{_top_score}</div>
        <div class="stat-lbl">Top Score</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    if _top:
        st.markdown(f"""
        <div style="padding:0 12px;margin-bottom:8px;">
          <div style="background:#161b22;border:1px solid #30363d;border-radius:8px;
                      padding:10px 14px;font-size:0.78em;color:#8b949e;">
            Top signal: <span style="color:#58a6ff;font-family:'JetBrains Mono',monospace;
            font-weight:700;">{_top['ticker']}</span>
            &nbsp;·&nbsp;{_top['label']}
            &nbsp;·&nbsp;Score <span style="color:#3fb950;">{_top['score']:.0f}</span>
          </div>
        </div>
        """, unsafe_allow_html=True)

    # Live log feed
    st.markdown('<div class="log-header">Live Log Feed</div>', unsafe_allow_html=True)
    try:
        from db.database import get_scanner_logs
        _logs = get_scanner_logs(limit=20)
    except Exception:
        _logs = []

    if not _logs:
        st.markdown(
            '<div style="padding:20px 16px;color:#484f58;font-size:0.78em;'
            'font-family:\'JetBrains Mono\',monospace;">No log entries yet.</div>',
            unsafe_allow_html=True,
        )
    else:
        _log_html = ""
        for _entry in _logs:
            _ts = _entry.get("created_at", "")
            try:
                _dt = datetime.fromisoformat(str(_ts).replace("Z", "+00:00"))
                _dt_naive = _dt.replace(tzinfo=None) if _dt.tzinfo else _dt
                _t_str = _dt_naive.strftime("%H:%M")
            except Exception:
                _t_str = "——"
            _lvl = _log_color(_entry.get("level", "info"))
            _msg = _entry.get("message", "")
            _log_html += f"""
            <div class="log-item">
              <span class="log-time">{_t_str}</span>
              <span class="log-msg {_lvl}">{_msg}</span>
            </div>"""
        st.markdown(_log_html, unsafe_allow_html=True)

    st.markdown('<div class="refresh-hint">Auto-refreshes every 60s</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# SCREEN 2 — SIGNALS
# ══════════════════════════════════════════════════════════════════════════════

elif _screen == "signals":
    # Filter bar
    _filter = st.radio(
        "Filter",
        ["All", "Strong Buy", "Spec Buy", "Watchlist"],
        horizontal=True,
        label_visibility="collapsed",
        key="sig_filter",
    )

    try:
        from db.database import get_signal_log
        _sig_df = get_signal_log(days=1)
    except Exception:
        _sig_df = None

    if _sig_df is None or (hasattr(_sig_df, "empty") and _sig_df.empty):
        st.markdown("""
        <div style="padding:60px 20px;text-align:center;color:#484f58;">
          <div style="font-size:2em;margin-bottom:12px;opacity:0.4;">—</div>
          <div style="font-size:0.9em;font-weight:600;color:#8b949e;">No signals today yet</div>
          <div style="font-size:0.75em;margin-top:6px;">Signals appear when the scanner runs</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        import pandas as pd
        _rows = _sig_df.to_dict("records") if not _sig_df.empty else []

        # Filter by selection
        _filter_map = {
            "Strong Buy": "Strong Buy Candidate",
            "Spec Buy":   "Speculative Buy",
            "Watchlist":  "Watchlist",
        }
        if _filter != "All":
            _rows = [r for r in _rows if r.get("signal_label") == _filter_map.get(_filter, _filter)]

        if not _rows:
            st.markdown(
                f'<div style="padding:40px 16px;text-align:center;color:#484f58;font-size:0.82em;">'
                f'No {_filter} signals today.</div>',
                unsafe_allow_html=True,
            )
        else:
            for _i, _r in enumerate(_rows):
                _ticker  = _r.get("ticker", "—")
                _label   = _r.get("signal_label", "—")
                _score   = _r.get("score")
                _price   = _r.get("price_at_signal")
                _created = _r.get("created_at")
                _ago_str = _mins_ago(_created)
                _sc      = _score_color(_score)
                _lc      = _signal_pill_color(_label)
                _score_s = f"{float(_score):.0f}" if _score is not None else "—"
                _price_s = f"${float(_price):.4f}" if _price is not None else "—"

                # Parse breakdown
                _breakdown_raw = _r.get("score_breakdown", "{}")
                try:
                    _bd = json.loads(_breakdown_raw) if isinstance(_breakdown_raw, str) else (_breakdown_raw or {})
                except Exception:
                    _bd = {}

                with st.expander(f"{_ticker}  ·  {_label}  ·  {_score_s}pts", expanded=False):
                    st.markdown(f"""
                    <div style="padding:4px 0 8px;">
                      <span style="font-family:'JetBrains Mono',monospace;font-size:1.1em;
                                   font-weight:700;color:{_lc};">{_ticker}</span>
                      <span style="font-size:0.78em;color:#8b949e;margin-left:8px;">{_label}</span>
                    </div>
                    <div style="display:flex;gap:16px;margin-bottom:12px;">
                      <div>
                        <div style="font-family:'JetBrains Mono',monospace;font-size:1.4em;
                                    font-weight:700;color:{_sc};">{_score_s}</div>
                        <div style="font-size:0.62em;color:#484f58;text-transform:uppercase;
                                    letter-spacing:0.08em;">Score</div>
                      </div>
                      <div>
                        <div style="font-family:'JetBrains Mono',monospace;font-size:1.1em;
                                    font-weight:600;color:#e6edf3;">{_price_s}</div>
                        <div style="font-size:0.62em;color:#484f58;text-transform:uppercase;
                                    letter-spacing:0.08em;">Price</div>
                      </div>
                      <div>
                        <div style="font-size:0.82em;color:#8b949e;padding-top:4px;">{_ago_str}</div>
                      </div>
                    </div>
                    """, unsafe_allow_html=True)

                    if _bd:
                        _bd_html = ""
                        for _cat, _val in _bd.items():
                            try:
                                _v = float(_val)
                            except Exception:
                                _v = 0
                            _bar_w = min(int(_v), 100)
                            _bar_color = "#3fb950" if _v >= 60 else "#d29922" if _v >= 40 else "#f85149"
                            _bd_html += f"""
                            <div class="breakdown-row">
                              <span class="bk-lbl">{_cat.title()}</span>
                              <div style="display:flex;align-items:center;gap:8px;">
                                <div style="width:60px;background:#21262d;border-radius:2px;height:3px;">
                                  <div style="width:{_bar_w}%;background:{_bar_color};height:3px;border-radius:2px;"></div>
                                </div>
                                <span class="bk-val">{_v:.0f}</span>
                              </div>
                            </div>"""
                        st.markdown(_bd_html, unsafe_allow_html=True)

    st.markdown('<div class="refresh-hint">Auto-refreshes every 60s</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# SCREEN 3 — ALERTS
# ══════════════════════════════════════════════════════════════════════════════

elif _screen == "alerts":
    try:
        from db.database import load_alerts
        _alerts = load_alerts(limit=50)
    except Exception:
        _alerts = []

    if not _alerts:
        st.markdown("""
        <div style="padding:60px 20px;text-align:center;color:#484f58;">
          <div style="font-size:2em;margin-bottom:12px;opacity:0.4;">—</div>
          <div style="font-size:0.9em;font-weight:600;color:#8b949e;">No alerts today</div>
          <div style="font-size:0.75em;margin-top:6px;">Alerts fire when the scanner detects activity</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        _al_html = ""
        for _a in reversed(_alerts):  # newest first
            _dot_c = _alert_dot_color(_a)
            _al_html += f"""
            <div class="alert-item">
              <div class="alert-dot" style="background:{_dot_c};margin-top:6px;"></div>
              <span class="alert-msg">{_a}</span>
            </div>"""
        st.markdown(_al_html, unsafe_allow_html=True)

    st.markdown('<div class="refresh-hint">Auto-refreshes every 60s</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# SCREEN 4 — CONTROL
# ══════════════════════════════════════════════════════════════════════════════

elif _screen == "control":
    _ctl, _sts = _load_control()
    _paused     = _ctl.get("paused", False)
    _force_scan = _ctl.get("force_scan", False)
    _mkt        = _mkt_status()
    _last_upd   = _sts.get("last_updated")
    _top        = _sts.get("top_signal")

    # Scanner status banner
    if _paused:
        _bcolor = "#d29922"; _bbg = "rgba(210,153,34,0.08)"; _bborder = "rgba(210,153,34,0.2)"
        _blabel = "PAUSED — manual hold"
    elif _mkt in ("Open", "Pre-Market"):
        try:
            _lu = datetime.fromisoformat(str(_last_upd).replace("Z", "+00:00"))
            _lu_naive = _lu.replace(tzinfo=None) if _lu.tzinfo else _lu
            _m = int((datetime.utcnow() - _lu_naive).total_seconds() / 60)
        except Exception:
            _m = 999
        if _m <= 45:
            _bcolor = "#3fb950"; _bbg = "rgba(63,185,80,0.08)"; _bborder = "rgba(63,185,80,0.2)"
            _blabel = "ACTIVE — scanner running"
        else:
            _bcolor = "#f85149"; _bbg = "rgba(248,81,73,0.08)"; _bborder = "rgba(248,81,73,0.2)"
            _blabel = "OFFLINE — no scan in 45+ min"
    else:
        _bcolor = "#d29922"; _bbg = "rgba(210,153,34,0.08)"; _bborder = "rgba(210,153,34,0.2)"
        _blabel = "SLEEPING — market closed"

    st.markdown(f"""
    <div style="margin:12px 12px 16px;background:{_bbg};border:1px solid {_bborder};
                border-radius:10px;padding:12px 16px;display:flex;align-items:center;gap:10px;">
      <div style="width:10px;height:10px;border-radius:50%;background:{_bcolor};
                  box-shadow:0 0 0 3px {_bcolor}33;flex-shrink:0;"></div>
      <span style="font-size:0.85em;font-weight:700;color:{_bcolor};">{_blabel}</span>
    </div>
    """, unsafe_allow_html=True)

    # Buttons
    st.markdown('<div style="padding:0 12px;">', unsafe_allow_html=True)
    _bc1, _bc2 = st.columns(2)

    with _bc1:
        if _paused:
            st.markdown('<div class="ctrl-btn-green">', unsafe_allow_html=True)
            if st.button("RESUME\nSCANNER", use_container_width=True, key="mob_resume"):
                try:
                    from db.database import set_scanner_control
                    set_scanner_control(paused=False)
                    st.rerun()
                except Exception as _e:
                    st.error(str(_e))
            st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.markdown('<div class="ctrl-btn-red">', unsafe_allow_html=True)
            if st.button("PAUSE\nSCANNER", use_container_width=True, key="mob_pause"):
                try:
                    from db.database import set_scanner_control
                    set_scanner_control(paused=True)
                    st.rerun()
                except Exception as _e:
                    st.error(str(_e))
            st.markdown("</div>", unsafe_allow_html=True)

    with _bc2:
        _scan_lbl = "SCANNING..." if _force_scan else "SCAN NOW"
        st.markdown('<div class="ctrl-btn-blue">', unsafe_allow_html=True)
        if st.button(_scan_lbl, use_container_width=True,
                     key="mob_scan", disabled=_force_scan):
            try:
                from db.database import set_scanner_control
                set_scanner_control(force_scan=True)
                st.rerun()
            except Exception as _e:
                st.error(str(_e))
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

    if _force_scan:
        st.markdown(
            '<div style="padding:0 12px 4px;font-size:0.72em;color:#58a6ff;'
            'font-family:\'JetBrains Mono\',monospace;">Scan queued — scanner picks up within 30–90s</div>',
            unsafe_allow_html=True,
        )

    # Quick summary
    st.markdown('<div class="sec-hdr">Quick Summary</div>', unsafe_allow_html=True)

    _top_str = (f"{_top['ticker']} · {_top['label']} · {_top['score']:.0f}pts"
                if _top else "No signals yet")
    _uptime_s = _uptime(_ctl.get("scanner_started_at"))
    _last_s = _mins_ago(_last_upd)
    _mkt_color = {"Open": "#3fb950", "Pre-Market": "#d29922"}.get(_mkt, "#8b949e")

    st.markdown(f"""
    <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;
                margin:0 12px;overflow:hidden;">
      <div class="sum-row">
        <span class="sum-lbl">Last scan</span>
        <span class="sum-val">{_last_s}</span>
      </div>
      <div class="sum-row">
        <span class="sum-lbl">Signals today</span>
        <span class="sum-val" style="color:#58a6ff;">{_sts.get('signals_today',0)}</span>
      </div>
      <div class="sum-row">
        <span class="sum-lbl">Alerts today</span>
        <span class="sum-val">{_sts.get('alerts_today',0)}</span>
      </div>
      <div class="sum-row">
        <span class="sum-lbl">Top signal</span>
        <span class="sum-val" style="font-size:0.72em;max-width:55%;text-align:right;">{_top_str}</span>
      </div>
      <div class="sum-row">
        <span class="sum-lbl">Market</span>
        <span class="sum-val" style="color:{_mkt_color};">{_mkt}</span>
      </div>
      <div class="sum-row" style="border-bottom:none;">
        <span class="sum-lbl">Scanner uptime</span>
        <span class="sum-val">{_uptime_s}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Log feed
    st.markdown('<div class="sec-hdr">Live Logs</div>', unsafe_allow_html=True)
    try:
        from db.database import get_scanner_logs
        _logs = get_scanner_logs(limit=20)
    except Exception:
        _logs = []

    if not _logs:
        st.markdown(
            '<div style="padding:16px;color:#484f58;font-size:0.78em;'
            'font-family:\'JetBrains Mono\',monospace;">No log entries yet.</div>',
            unsafe_allow_html=True,
        )
    else:
        _lh = ""
        for _entry in _logs:
            _ts = _entry.get("created_at", "")
            try:
                _dt = datetime.fromisoformat(str(_ts).replace("Z", "+00:00"))
                _dt_naive = _dt.replace(tzinfo=None) if _dt.tzinfo else _dt
                _t_str = _dt_naive.strftime("%H:%M")
            except Exception:
                _t_str = "——"
            _lvl = _log_color(_entry.get("level", "info"))
            _msg = _entry.get("message", "")
            _lh += f"""
            <div class="log-item">
              <span class="log-time">{_t_str}</span>
              <span class="log-msg {_lvl}">{_msg}</span>
            </div>"""
        st.markdown(_lh, unsafe_allow_html=True)

    st.markdown('<div class="refresh-hint">Auto-refreshes every 60s</div>', unsafe_allow_html=True)
