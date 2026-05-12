# mobile.py — Axiom Terminal Mobile
# Separate Streamlit app for 390px (iPhone 15).
# Connects to same PostgreSQL DB as app.py. One source of truth.
#
# Railway start command:
#   streamlit run mobile.py --server.port $PORT --server.address 0.0.0.0 --server.headless true

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

# ── CSS ───────────────────────────────────────────────────────────────────────
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
.stButton > button:hover { border-color: #58a6ff !important; color: var(--t1) !important; }
.nav-active .stButton > button {
  background: rgba(88,166,255,0.12) !important;
  border-color: var(--blue) !important;
  color: var(--blue) !important; font-weight: 700 !important;
}

/* ── Status / card ── */
.status-card {
  background: var(--bg2); border: 1px solid var(--border);
  border-radius: 12px; padding: 18px 16px; margin: 12px 12px 8px;
}
.status-dot {
  display: inline-block; width: 12px; height: 12px;
  border-radius: 50%; margin-right: 8px; vertical-align: middle; flex-shrink: 0;
}
.status-label { font-size: 1em; font-weight: 700; color: var(--t1); vertical-align: middle; }
.status-sub {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.72em; color: var(--t2); margin-top: 8px;
  display: flex; gap: 16px; flex-wrap: wrap;
}

/* ── Status bar (compact top row) ── */
.status-bar {
  background: var(--bg2); border-bottom: 1px solid var(--border);
  padding: 6px 12px; display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
  font-family: 'JetBrains Mono', monospace; font-size: 0.68em;
}
.sbar-chip {
  padding: 2px 8px; border-radius: 4px; font-weight: 600;
  border: 1px solid; letter-spacing: 0.04em;
}
.sbar-label { color: var(--t2); }
.sbar-val   { color: var(--t1); font-weight: 600; }
.sbar-offline { color: #f85149; font-weight: 700; letter-spacing: 0.06em; }

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

/* ── Health card ── */
.health-card {
  background: var(--bg2); border: 1px solid var(--border);
  border-radius: 10px; margin: 0 12px 12px; overflow: hidden;
}
.health-row {
  display: flex; justify-content: space-between; align-items: center;
  padding: 8px 14px; border-bottom: 1px solid rgba(48,54,61,0.5);
}
.health-row:last-child { border-bottom: none; }
.health-lbl { font-size: 0.75em; color: var(--t2); }
.health-val {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.78em; color: var(--t1); font-weight: 600;
}

/* ── Log feed ── */
.log-header {
  font-size: 0.65em; font-weight: 600; letter-spacing: 0.1em;
  text-transform: uppercase; color: var(--t2); padding: 0 16px; margin: 16px 0 8px;
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
}
.sig-ticker {
  font-family: 'JetBrains Mono', monospace;
  font-size: 1.15em; font-weight: 700; color: var(--blue);
}
.sig-label { font-size: 0.78em; color: var(--t2); margin-top: 2px; }
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
  background: rgba(248,81,73,0.12) !important; border-color: rgba(248,81,73,0.4) !important;
  color: #f85149 !important; font-size: 0.9em !important; font-weight: 700 !important;
  min-height: 54px !important; padding: 16px 8px !important;
}
.ctrl-btn-green .stButton > button {
  background: rgba(63,185,80,0.12) !important; border-color: rgba(63,185,80,0.4) !important;
  color: #3fb950 !important; font-size: 0.9em !important; font-weight: 700 !important;
  min-height: 54px !important; padding: 16px 8px !important;
}
.ctrl-btn-blue .stButton > button {
  background: rgba(88,166,255,0.12) !important; border-color: rgba(88,166,255,0.4) !important;
  color: #58a6ff !important; font-size: 0.9em !important; font-weight: 700 !important;
  min-height: 54px !important; padding: 16px 8px !important;
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

/* ── Accuracy / bucket cards ── */
.bucket-card {
  background: var(--bg2); border: 1px solid var(--border);
  border-radius: 10px; padding: 12px 14px; margin: 6px 12px;
}
.bucket-title {
  font-family: 'JetBrains Mono', monospace; font-size: 0.88em;
  font-weight: 700; color: var(--t2); letter-spacing: 0.06em;
  margin-bottom: 8px;
}
.bucket-row {
  display: flex; justify-content: space-between;
  padding: 3px 0; font-size: 0.72em;
}
.bucket-row:last-child { padding-bottom: 0; }
.b-lbl { color: var(--t3); }
.b-val { font-family: 'JetBrains Mono', monospace; color: var(--t1); font-weight: 600; }
.disabled-banner {
  background: rgba(248,81,73,0.12); border: 1px solid rgba(248,81,73,0.3);
  border-radius: 4px; padding: 3px 8px; margin-top: 6px;
  font-size: 0.68em; color: #f85149; font-weight: 700; letter-spacing: 0.06em;
}
.insufficient { opacity: 0.5; }

/* ── Account / position cards ── */
.pos-card {
  background: var(--bg2); border: 1px solid var(--border);
  border-radius: 10px; padding: 14px; margin: 6px 12px;
}
.pos-ticker {
  font-family: 'JetBrains Mono', monospace;
  font-size: 1.1em; font-weight: 700; color: var(--blue);
}
.pos-side {
  font-size: 0.65em; color: var(--t2); text-transform: uppercase;
  letter-spacing: 0.08em; margin-left: 8px;
}
.pos-pnl-pos { font-family: 'JetBrains Mono', monospace; color: #3fb950; font-weight: 700; }
.pos-pnl-neg { font-family: 'JetBrains Mono', monospace; color: #f85149; font-weight: 700; }
.pos-meta { font-size: 0.72em; color: var(--t2); margin-top: 4px; }

/* ── Graded signal row ── */
.graded-row {
  display: flex; align-items: center; gap: 6px; flex-wrap: wrap;
  padding: 8px 16px; border-bottom: 1px solid rgba(48,54,61,0.5);
  font-size: 0.72em;
}
.g-ticker { font-family: 'JetBrains Mono', monospace; font-weight: 700; color: var(--blue); min-width: 48px; }
.g-score  { font-family: 'JetBrains Mono', monospace; color: var(--t2); min-width: 32px; }
.g-price  { font-family: 'JetBrains Mono', monospace; color: var(--t2); min-width: 60px; }
.g-ret-pos { font-family: 'JetBrains Mono', monospace; color: #3fb950; }
.g-ret-neg { font-family: 'JetBrains Mono', monospace; color: #f85149; }
.g-ret-neu { font-family: 'JetBrains Mono', monospace; color: var(--t3); }

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

/* ── Login screen ── */
.login-wrap {
  max-width: 340px; margin: 60px auto 0; padding: 0 20px;
}
.login-wordmark {
  font-family: 'JetBrains Mono', monospace; font-size: 1.4em;
  font-weight: 600; color: var(--t1); text-align: center;
  letter-spacing: 0.12em; margin-bottom: 4px;
}
.login-sub {
  font-family: 'JetBrains Mono', monospace; font-size: 0.68em;
  color: var(--t3); text-align: center; letter-spacing: 0.2em;
  margin-bottom: 32px;
}
.login-divider {
  border: none; border-top: 1px solid var(--border); margin: 20px 0;
}

/* ── Refresh hint ── */
.refresh-hint {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.6em; color: var(--t3); text-align: center; padding: 12px;
}

/* ── Logout button ── */
.logout-btn .stButton > button {
  background: transparent !important; border: 1px solid var(--border) !important;
  color: var(--t3) !important; font-size: 0.7em !important;
  padding: 4px 10px !important; min-height: 28px !important;
  border-radius: 6px !important;
}
.logout-btn .stButton > button:hover { color: #f85149 !important; border-color: rgba(248,81,73,0.4) !important; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# DB INIT + MOBILE ADMIN SEED
# ══════════════════════════════════════════════════════════════════════════════

try:
    from db.database import initialize_db, seed_mobile_admin
    initialize_db()
    seed_mobile_admin()
except Exception as _dbi_e:
    pass  # non-fatal — DB may already be initialized


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


def _secs_ago(ts) -> int:
    if not ts:
        return 99999
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        dt_naive = dt.replace(tzinfo=None) if dt.tzinfo else dt
        return max(0, int((datetime.utcnow() - dt_naive).total_seconds()))
    except Exception:
        return 99999


def _mins_ago(ts) -> str:
    s = _secs_ago(ts)
    if s >= 99999: return "—"
    if s < 60:     return f"{s}s ago"
    if s < 3600:   return f"{s // 60}m ago"
    return f"{s // 3600}h {(s % 3600) // 60}m ago"


def _fmt_ts_et(ts) -> str:
    """Format a timestamp as HH:MM ET."""
    if not ts:
        return "—"
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/New_York")
    except ImportError:
        import pytz
        tz = pytz.timezone("America/New_York")
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            import pytz
            dt = pytz.utc.localize(dt)
        return dt.astimezone(tz).strftime("%H:%M ET")
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
    if score is None: return "#8b949e"
    s = float(score)
    if s >= 75: return "#3fb950"
    if s >= 60: return "#58a6ff"
    if s >= 45: return "#d29922"
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


def _ret_color(ret) -> str:
    if ret is None: return "g-ret-neu"
    r = float(ret)
    if r > 2.0:  return "g-ret-pos"
    if r < -2.0: return "g-ret-neg"
    return "g-ret-neu"


def _fmt_ret(ret) -> str:
    if ret is None: return "—"
    return f"{float(ret):+.1f}%"


def _fmt_money(v) -> str:
    if v is None: return "—"
    return f"${float(v):,.2f}"


def _fmt_pct(v) -> str:
    if v is None: return "—"
    return f"{float(v):+.2f}%"


def _load_control():
    try:
        from db.database import get_scanner_control, get_control_stats
        return get_scanner_control(), get_control_stats()
    except Exception:
        return (
            {"paused": False, "force_scan": False, "scanner_started_at": None,
             "current_mode": "UNKNOWN"},
            {"signals_today": 0, "alerts_today": 0, "top_signal": None,
             "scan_count": 0, "last_updated": None},
        )


# ══════════════════════════════════════════════════════════════════════════════
# AUTH GATE
# ══════════════════════════════════════════════════════════════════════════════

if "auth_user" not in st.session_state:
    st.session_state.auth_user = None
if "screen" not in st.session_state:
    st.session_state.screen = "status"

_MODE_MAP = {
    "MARKET":     ("#3fb950", "rgba(63,185,80,0.08)",   "rgba(63,185,80,0.2)"),
    "PREMARKET":  ("#d29922", "rgba(210,153,34,0.08)",  "rgba(210,153,34,0.2)"),
    "AFTERHOURS": ("#58a6ff", "rgba(88,166,255,0.08)",  "rgba(88,166,255,0.2)"),
    "OVERNIGHT":  ("#484f58", "rgba(72,79,88,0.08)",    "rgba(72,79,88,0.2)"),
    "WEEKEND":    ("#484f58", "rgba(72,79,88,0.08)",    "rgba(72,79,88,0.2)"),
    "UNKNOWN":    ("#8b949e", "rgba(139,148,158,0.08)", "rgba(139,148,158,0.2)"),
}


def _login_page():
    st.markdown("""
    <div class="login-wrap">
      <div class="login-wordmark">AXIOM TERMINAL</div>
      <div class="login-sub">MOBILE ACCESS</div>
    </div>
    """, unsafe_allow_html=True)

    with st.container():
        st.markdown('<div style="max-width:340px;margin:0 auto;padding:0 20px;">', unsafe_allow_html=True)
        username = st.text_input("Username", key="_login_user", placeholder="username")
        password = st.text_input("Password", type="password", key="_login_pass", placeholder="password")
        if st.button("LOGIN", use_container_width=True, key="_login_btn"):
            try:
                from db.database import get_user_by_username, update_last_login
                from auth import check_password
                user = get_user_by_username((username or "").strip())
                if user and check_password(password or "", user["password_hash"]):
                    st.session_state.auth_user = {
                        "username":     user["username"],
                        "display_name": user.get("display_name") or user["username"],
                        "role":         user["role"],
                        "id":           user["id"],
                    }
                    update_last_login(user["id"])
                    st.rerun()
                else:
                    st.error("Invalid credentials")
            except Exception:
                st.error("Invalid credentials")
        st.markdown('</div>', unsafe_allow_html=True)


if st.session_state.auth_user is None:
    _login_page()
    st.stop()

_auth = st.session_state.auth_user


# ══════════════════════════════════════════════════════════════════════════════
# TOP BAR (with logout)
# ══════════════════════════════════════════════════════════════════════════════

_et_now = _now_et()
_tc1, _tc2 = st.columns([4, 1])
with _tc1:
    st.markdown(f"""
    <div class="mob-topbar" style="position:relative;z-index:100;margin-bottom:0;">
      <div>
        <div class="mob-title">Axiom</div>
        <div class="mob-subtitle">TERMINAL MOBILE</div>
      </div>
      <div class="mob-time">{_et_now.strftime('%H:%M ET')}</div>
    </div>
    """, unsafe_allow_html=True)
with _tc2:
    st.markdown('<div class="logout-btn" style="padding:12px 8px 0 0;">', unsafe_allow_html=True)
    if st.button("out", key="_logout", use_container_width=True):
        st.session_state.auth_user = None
        st.session_state.screen = "status"
        st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# NAVIGATION BAR (6 tabs)
# ══════════════════════════════════════════════════════════════════════════════

_screens = [
    ("status",   "Status"),
    ("signals",  "Signals"),
    ("alerts",   "Alerts"),
    ("accuracy", "Accuracy"),
    ("account",  "Account"),
    ("control",  "Control"),
]

_nc = st.columns(6)
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
# PERSISTENT STATUS BAR (every screen, every 10s)
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=10)
def _status_bar():
    try:
        from db.database import get_scanner_state
        _ss = get_scanner_state()
    except Exception:
        _ss = {}

    _mode    = _ss.get("current_mode", "UNKNOWN")
    _paused  = _ss.get("paused", False)
    _hb      = _ss.get("last_heartbeat")
    _secs    = _secs_ago(_hb)
    _offline = _secs > 300
    _sigs    = _ss.get("signals_today", 0)

    _mode_colors = {
        "MARKET":     ("#3fb950", "rgba(63,185,80,0.15)",   "rgba(63,185,80,0.4)"),
        "PREMARKET":  ("#d29922", "rgba(210,153,34,0.15)",  "rgba(210,153,34,0.4)"),
        "AFTERHOURS": ("#58a6ff", "rgba(88,166,255,0.15)",  "rgba(88,166,255,0.4)"),
        "OVERNIGHT":  ("#484f58", "rgba(72,79,88,0.15)",    "rgba(72,79,88,0.4)"),
        "WEEKEND":    ("#484f58", "rgba(72,79,88,0.15)",    "rgba(72,79,88,0.4)"),
        "UNKNOWN":    ("#8b949e", "rgba(139,148,158,0.15)", "rgba(139,148,158,0.4)"),
    }
    _mc, _mbg, _mbrd = _mode_colors.get(_mode, _mode_colors["UNKNOWN"])

    if _offline:
        bar_html = (
            '<div class="status-bar">'
            '<span class="sbar-offline">SCANNER OFFLINE</span>'
            f'<span class="sbar-label">last heartbeat {_mins_ago(_hb)}</span>'
            '</div>'
        )
    else:
        _run_lbl  = '<span style="color:#f85149;font-weight:700;">PAUSED</span>' if _paused else \
                    '<span style="color:#3fb950;font-weight:700;">RUNNING</span>'
        _scan_lbl = f"{_secs}s ago" if _secs < 60 else _mins_ago(_hb)
        bar_html = (
            '<div class="status-bar">'
            f'<span class="sbar-chip" style="color:{_mc};background:{_mbg};border-color:{_mbrd};">{_mode}</span>'
            f'<span>{_run_lbl}</span>'
            f'<span class="sbar-label">LAST SCAN</span><span class="sbar-val">{_scan_lbl}</span>'
            f'<span class="sbar-label">SIGNALS TODAY</span><span class="sbar-val">{_sigs}</span>'
            '</div>'
        )
    st.markdown(bar_html, unsafe_allow_html=True)


_status_bar()


# ══════════════════════════════════════════════════════════════════════════════
# SCREEN FRAGMENTS
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=15)
def _mob_status_screen():
    try:
        from db.database import get_scanner_state, get_validator_health
        _ss = get_scanner_state()
        _vh = get_validator_health()
    except Exception:
        _ss = {}
        _vh = {}

    _mode   = _ss.get("current_mode", "UNKNOWN")
    _paused = _ss.get("paused", False)
    _hb     = _ss.get("last_heartbeat")
    _secs   = _secs_ago(_hb)
    _offline = _secs > 300

    if _offline:
        _dot = "#f85149"; _dot_bg = "rgba(248,81,73,0.08)"; _dot_brd = "rgba(248,81,73,0.2)"
        _status_lbl = "SCANNER OFFLINE"; _status_detail = f"last seen {_mins_ago(_hb)}"
    elif _paused:
        _dot = "#d29922"; _dot_bg = "rgba(210,153,34,0.08)"; _dot_brd = "rgba(210,153,34,0.2)"
        _status_lbl = "PAUSED"; _status_detail = "manual hold active"
    else:
        _mc, _mbg, _mbrd = _MODE_MAP.get(_mode, _MODE_MAP["UNKNOWN"])
        _dot = _mc; _dot_bg = _mbg; _dot_brd = _mbrd
        _status_lbl = _mode; _status_detail = f"scan #{_ss.get('scan_count', 0)}"

    _ctl, _sts = _load_control()
    _top = _sts.get("top_signal")
    _top_score = f"{_top['score']:.0f}" if _top else "—"

    st.markdown(f"""
    <div class="status-card" style="border-color:{_dot_brd};background:{_dot_bg};">
      <div style="display:flex;align-items:center;gap:10px;">
        <div class="status-dot" style="background:{_dot};box-shadow:0 0 0 4px {_dot}33;"></div>
        <span class="status-label" style="color:{_dot};">{_status_lbl}</span>
        <span style="font-size:0.78em;color:#8b949e;margin-left:4px;">{_status_detail}</span>
      </div>
      <div class="status-sub">
        <span>Last scan: {_mins_ago(_hb)}</span>
        <span>Mode: {_mode}</span>
        <span>Scans: {_ss.get('scan_count', 0)}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="stats-row">
      <div class="stat-card">
        <div class="stat-num" style="color:#58a6ff;">{_ss.get('signals_today', 0)}</div>
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
        <div style="padding:0 12px;margin-bottom:4px;">
          <div style="background:#161b22;border:1px solid #30363d;border-radius:8px;
                      padding:10px 14px;font-size:0.78em;color:#8b949e;">
            Top signal: <span style="color:#58a6ff;font-family:'JetBrains Mono',monospace;
            font-weight:700;">{_top['ticker']}</span>
            &nbsp;·&nbsp;{_top['label']}
            &nbsp;·&nbsp;Score <span style="color:#3fb950;">{_top['score']:.0f}</span>
          </div>
        </div>
        """, unsafe_allow_html=True)

    # ── Scanner Health Card ─────────────────────────────────────────────────
    st.markdown('<div class="sec-hdr">Scanner Health</div>', unsafe_allow_html=True)

    _conv_ts = _fmt_ts_et(_ss.get("last_conviction_run"))
    _acc_ts  = _fmt_ts_et(_ss.get("last_accuracy_run"))
    _graded  = _vh.get("signals_graded", 0)
    _pending = _vh.get("signals_pending", 0)
    _dq_t    = _ss.get("data_quality_tiingo", 0)
    _dq_y    = _ss.get("data_quality_yfinance", 0)
    _dq_s    = _ss.get("data_quality_stale", 0)
    _hb_color = "#f85149" if _offline else "#3fb950"

    def _health_row(lbl, val, val_color="#e6edf3"):
        return (f'<div class="health-row"><span class="health-lbl">{lbl}</span>'
                f'<span class="health-val" style="color:{val_color};">{val}</span></div>')

    _h_html = '<div class="health-card">'
    _h_html += _health_row("Last heartbeat",
                           f"{_secs}s ago" if _secs < 9999 else "—",
                           _hb_color)
    _h_html += _health_row("Current mode", _mode,
                           _MODE_MAP.get(_mode, _MODE_MAP["UNKNOWN"])[0])
    _h_html += _health_row("Scan cycle", f"#{_ss.get('scan_count', 0)}")
    _h_html += _health_row("Signals today", str(_ss.get("signals_today", 0)))
    _h_html += _health_row("Suppressed (dupes)", str(_ss.get("signals_suppressed_today", 0)))
    _h_html += _health_row("Universe size", f"{_ss.get('universe_size', 0)} tickers")
    _dq_str = (f"Tiingo {_dq_t:.0f}%  |  yfinance {_dq_y:.0f}%  |  stale {_dq_s:.0f}%"
               if (_dq_t or _dq_y or _dq_s) else "N/A")
    _h_html += _health_row("Data quality", _dq_str)
    _h_html += _health_row("Active positions",
                           f"{_ss.get('open_positions', 0)} open paper trades")
    _h_html += _health_row("Last conviction run", _conv_ts)
    _h_html += _health_row("Accuracy validator",
                           f"last run {_acc_ts}, {_graded} graded, {_pending} pending")
    _h_html += '</div>'
    st.markdown(_h_html, unsafe_allow_html=True)

    # ── Live log feed ───────────────────────────────────────────────────────
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
            _lh += (f'<div class="log-item">'
                    f'<span class="log-time">{_t_str}</span>'
                    f'<span class="log-msg {_lvl}">{_msg}</span>'
                    f'</div>')
        st.markdown(_lh, unsafe_allow_html=True)

    st.markdown('<div class="refresh-hint">Live — updates every 15s</div>', unsafe_allow_html=True)


@st.fragment(run_every=20)
def _mob_signals_screen():
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
          <div style="font-size:0.9em;font-weight:600;color:#8b949e;">No signals today yet</div>
          <div style="font-size:0.75em;margin-top:6px;">Signals appear when the scanner runs</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        _rows = _sig_df.to_dict("records") if not _sig_df.empty else []
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
                _qtag    = _r.get("quality_tag") or ""
                _ago_str = _mins_ago(_created)
                _sc      = _score_color(_score)
                _lc      = _signal_pill_color(_label)
                _score_s = f"{float(_score):.0f}" if _score is not None else "—"
                _price_s = f"${float(_price):.4f}" if _price is not None else "—"
                _qtag_color = {"HIGH": "#3fb950", "MEDIUM": "#d29922", "LOW": "#484f58"}.get(_qtag, "#484f58")

                _breakdown_raw = _r.get("score_breakdown", "{}")
                try:
                    _bd = json.loads(_breakdown_raw) if isinstance(_breakdown_raw, str) else (_breakdown_raw or {})
                except Exception:
                    _bd = {}

                with st.expander(f"{_ticker}  {_label}  {_score_s}pts", expanded=False):
                    st.markdown(f"""
                    <div style="padding:4px 0 8px;">
                      <span style="font-family:'JetBrains Mono',monospace;font-size:1.1em;
                                   font-weight:700;color:{_lc};">{_ticker}</span>
                      <span style="font-size:0.78em;color:#8b949e;margin-left:8px;">{_label}</span>
                      {"<span style='font-size:0.62em;font-weight:700;color:" + _qtag_color + ";margin-left:8px;letter-spacing:0.08em;'>" + _qtag + "</span>" if _qtag else ""}
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
                            _bd_html += (
                                f'<div class="breakdown-row">'
                                f'<span class="bk-lbl">{_cat.title()}</span>'
                                f'<div style="display:flex;align-items:center;gap:8px;">'
                                f'<div style="width:60px;background:#21262d;border-radius:2px;height:3px;">'
                                f'<div style="width:{_bar_w}%;background:{_bar_color};height:3px;border-radius:2px;"></div></div>'
                                f'<span class="bk-val">{_v:.0f}</span></div></div>'
                            )
                        st.markdown(_bd_html, unsafe_allow_html=True)

    st.markdown('<div class="refresh-hint">Live — updates every 20s</div>', unsafe_allow_html=True)


@st.fragment(run_every=10)
def _mob_alerts_screen():
    try:
        from db.database import load_alerts
        _alerts = load_alerts(limit=50)
    except Exception:
        _alerts = []

    if not _alerts:
        st.markdown("""
        <div style="padding:60px 20px;text-align:center;color:#484f58;">
          <div style="font-size:0.9em;font-weight:600;color:#8b949e;">No alerts today</div>
          <div style="font-size:0.75em;margin-top:6px;">Alerts fire when the scanner detects activity</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        _al_html = ""
        for _a in reversed(_alerts):
            _dot_c = _alert_dot_color(_a)
            _al_html += (f'<div class="alert-item">'
                         f'<div class="alert-dot" style="background:{_dot_c};margin-top:6px;"></div>'
                         f'<span class="alert-msg">{_a}</span></div>')
        st.markdown(_al_html, unsafe_allow_html=True)

    st.markdown('<div class="refresh-hint">Live — updates every 10s</div>', unsafe_allow_html=True)


@st.fragment(run_every=60)
def _accuracy_screen():
    try:
        from db.database import (get_validator_health, get_accuracy_metrics,
                                 get_graded_signals, get_rolling_win_rate,
                                 get_overall_accuracy)
        _vh      = get_validator_health()
        _buckets = get_accuracy_metrics()
        _graded  = get_graded_signals(n=20)
        _rolling = get_rolling_win_rate()
        _overall = get_overall_accuracy()
    except Exception as _e:
        st.error(f"Data load failed: {_e}")
        return

    # ── SECTION 1: Validator health ─────────────────────────────────────────
    st.markdown('<div class="sec-hdr" style="border-top:none;padding-top:8px;">Validator Health</div>',
                unsafe_allow_html=True)

    _ct   = _vh.get("check_time")
    _errm = _vh.get("error_msg", "")
    _healthy = not _errm
    _h_color = "#3fb950" if _healthy else "#f85149"
    _h_label = "HEALTHY" if _healthy else "DEGRADED"

    st.markdown(f"""
    <div class="health-card">
      <div class="health-row">
        <span class="health-lbl">Last run</span>
        <span class="health-val">{_fmt_ts_et(_ct)} ({_mins_ago(_ct)})</span>
      </div>
      <div class="health-row">
        <span class="health-lbl">Signals graded</span>
        <span class="health-val">{_vh.get('signals_graded', 0)}</span>
      </div>
      <div class="health-row">
        <span class="health-lbl">Pending grading</span>
        <span class="health-val">{_vh.get('signals_pending', 0)}</span>
      </div>
      <div class="health-row" style="border-bottom:none;">
        <span class="health-lbl">Status</span>
        <span class="health-val" style="color:{_h_color};">{_h_label}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── SECTION 2: Overall accuracy summary ─────────────────────────────────
    st.markdown('<div class="sec-hdr">Overall Accuracy</div>', unsafe_allow_html=True)

    _n_total = _overall.get("total", 0)
    _wr1  = _overall.get("win_rate_1d")
    _wr3  = _overall.get("win_rate_3d")
    _wr5  = _overall.get("win_rate_5d")
    _shp  = _overall.get("sharpe")
    _low_sample = _n_total < 30

    def _wr_color(wr):
        if wr is None: return "#8b949e"
        return "#3fb950" if wr > 55 else "#f85149" if wr < 45 else "#d29922"

    def _metric_card(label, val_str, color="#e6edf3", note=""):
        note_html = f'<div style="font-size:0.6em;color:#484f58;margin-top:2px;">{note}</div>' if note else ''
        return (f'<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;'
                f'padding:10px 12px;margin:0 0 8px;">'
                f'<div style="font-size:0.6em;color:#484f58;text-transform:uppercase;'
                f'letter-spacing:0.1em;">{label}</div>'
                f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:1.3em;'
                f'font-weight:700;color:{color};">{val_str}</div>'
                f'{note_html}'
                f'</div>')

    _sample_note = "low sample — less reliable" if _low_sample else ""
    _cards_html = '<div style="padding:0 12px;">'
    _cards_html += _metric_card("1-DAY WIN RATE",
                                f"{_wr1:.1f}%" if _wr1 else "—",
                                _wr_color(_wr1), _sample_note)
    _cards_html += _metric_card("3-DAY WIN RATE",
                                f"{_wr3:.1f}%" if _wr3 else "—",
                                _wr_color(_wr3))
    _cards_html += _metric_card("5-DAY WIN RATE",
                                f"{_wr5:.1f}%" if _wr5 else "—",
                                _wr_color(_wr5))
    _cards_html += _metric_card("SHARPE",
                                f"{_shp:.2f}" if _shp else "—",
                                "#3fb950" if (_shp and _shp > 0) else "#f85149")
    _cards_html += _metric_card("SAMPLE SIZE",
                                str(_n_total),
                                "#8b949e", "low sample" if _low_sample else "")
    _cards_html += '</div>'
    st.markdown(_cards_html, unsafe_allow_html=True)

    # ── SECTION 3: By score bucket ──────────────────────────────────────────
    st.markdown('<div class="sec-hdr">By Score Bucket</div>', unsafe_allow_html=True)

    if not _buckets:
        st.markdown('<div style="padding:16px;color:#484f58;font-size:0.8em;">No bucket data yet.</div>',
                    unsafe_allow_html=True)
    else:
        for _b in _buckets:
            _bn   = _b.get("n", 0)
            _bwr  = _b.get("win_rate")
            _bpf  = _b.get("profit_factor")
            _bev  = _b.get("ev_per_trade")
            _bts  = _b.get("t_stat")
            _bpv  = _b.get("p_value")
            _bdis = _b.get("disabled", False)
            _insuf = _bn < 30

            _wr_c = "#8b949e" if _insuf else (_wr_color(_bwr * 100) if _bwr else "#8b949e")
            _card_class = "bucket-card insufficient" if _insuf else "bucket-card"

            _sig_label = "YES" if (_bts and _bpv and _bpv < 0.05 and _bn >= 30) else "NO"
            _sig_color = "#3fb950" if _sig_label == "YES" else "#f85149"

            _bhtml = f'<div class="{_card_class}">'
            _bhtml += f'<div class="bucket-title">SCORE {_b.get("bucket", "?")}</div>'

            if _insuf:
                _bhtml += '<div style="font-size:0.72em;color:#484f58;margin-top:4px;">INSUFFICIENT DATA (n &lt; 30)</div>'
                _bhtml += f'<div class="bucket-row"><span class="b-lbl">N</span><span class="b-val">{_bn}</span></div>'
            else:
                _wr_pct = round(_bwr * 100, 1) if _bwr else 0
                _bhtml += f'<div class="bucket-row"><span class="b-lbl">N</span><span class="b-val">{_bn}</span></div>'
                _bhtml += (f'<div class="bucket-row"><span class="b-lbl">WIN RATE</span>'
                           f'<span class="b-val" style="color:{_wr_c};">{_wr_pct:.1f}%</span></div>')
                _bhtml += (f'<div class="bucket-row"><span class="b-lbl">PROFIT FACTOR</span>'
                           f'<span class="b-val">{_bpf:.2f}</span></div>') if _bpf else ""
                _bhtml += (f'<div class="bucket-row"><span class="b-lbl">EV</span>'
                           f'<span class="b-val">{_fmt_ret(_bev * 100 if _bev else None)}</span></div>')
                _bhtml += (f'<div class="bucket-row"><span class="b-lbl">T-STAT</span>'
                           f'<span class="b-val">{_bts:.2f}</span></div>') if _bts else ""
                _bhtml += (f'<div class="bucket-row"><span class="b-lbl">P-VALUE</span>'
                           f'<span class="b-val">{_bpv:.3f}</span></div>') if _bpv else ""
                _bhtml += (f'<div class="bucket-row"><span class="b-lbl">SIGNIFICANT</span>'
                           f'<span class="b-val" style="color:{_sig_color};">{_sig_label}</span></div>')

            if _bdis:
                _bhtml += '<div class="disabled-banner">DISABLED</div>'
            _bhtml += '</div>'
            st.markdown(_bhtml, unsafe_allow_html=True)

    # ── SECTION 4: Recent graded signals ────────────────────────────────────
    st.markdown('<div class="sec-hdr">Recent Graded Signals (Last 20)</div>', unsafe_allow_html=True)

    if not _graded:
        st.markdown('<div style="padding:16px;color:#484f58;font-size:0.8em;">No graded signals yet.</div>',
                    unsafe_allow_html=True)
    else:
        _gr_html = ""
        for _g in _graded:
            _gt  = _g.get("ticker", "—")
            _gs  = _g.get("score")
            _gp  = _g.get("price_at_signal")
            _g1  = _g.get("ret_1d")
            _g3  = _g.get("ret_3d")
            _g5  = _g.get("ret_5d")
            _gs_s  = f"{float(_gs):.0f}" if _gs else "—"
            _gp_s  = f"${float(_gp):.2f}" if _gp else "—"
            _gr_html += (
                f'<div class="graded-row">'
                f'<span class="g-ticker">{_gt}</span>'
                f'<span class="g-score">{_gs_s}pt</span>'
                f'<span class="g-price">{_gp_s}</span>'
                f'<span class="{_ret_color(_g1)}">1D:{_fmt_ret(_g1)}</span>'
                f'<span class="{_ret_color(_g3)}">3D:{_fmt_ret(_g3)}</span>'
                f'<span class="{_ret_color(_g5)}">5D:{_fmt_ret(_g5)}</span>'
                f'</div>'
            )
        st.markdown(_gr_html, unsafe_allow_html=True)

    # ── SECTION 5: Accuracy trend chart ─────────────────────────────────────
    st.markdown('<div class="sec-hdr">7-Day Rolling Win Rate</div>', unsafe_allow_html=True)

    if len(_rolling) >= 7:
        import pandas as pd
        _roll_df = pd.DataFrame(_rolling)
        _roll_df["date"] = pd.to_datetime(_roll_df["date"])
        _roll_df = _roll_df.set_index("date").sort_index()
        _roll_df["7D Rolling Win Rate"] = (_roll_df["win_rate"]
                                           .rolling(7, min_periods=1).mean())
        st.line_chart(_roll_df[["7D Rolling Win Rate"]])
    else:
        st.markdown('<div style="padding:16px;color:#484f58;font-size:0.78em;">Not enough data for trend (need 7+ days of graded signals).</div>',
                    unsafe_allow_html=True)

    st.markdown('<div class="refresh-hint">Live — updates every 60s</div>', unsafe_allow_html=True)


@st.fragment(run_every=30)
def _account_screen():
    try:
        from db.database import (get_account_summary, get_open_positions,
                                 get_recent_trades, get_equity_curve)
        _acct  = get_account_summary()
        _pos   = get_open_positions()
        _trd   = get_recent_trades(n=10)
        _eq    = get_equity_curve(n=200)
    except Exception as _e:
        st.error(f"Data load failed: {_e}")
        return

    _equity   = _acct.get("equity", 100000)
    _bal      = _acct.get("balance", 100000)
    _tot_pnl  = _acct.get("total_pnl", 0)
    _day_pnl  = _equity - _acct.get("day_start_equity", _equity)
    _day_pct  = (_day_pnl / _acct.get("day_start_equity", _equity) * 100
                 if _acct.get("day_start_equity", _equity) > 0 else 0)
    _tot_pct  = (_tot_pnl / 100000 * 100)
    _wr       = _acct.get("win_rate", 0)
    _pf       = _acct.get("profit_factor", 0)
    _sh       = _acct.get("sharpe", 0)
    _dd       = _acct.get("max_drawdown", 0)
    _n_trd    = int(_acct.get("total_trades", 0))

    # ── SECTION 1: Summary cards ────────────────────────────────────────────
    st.markdown('<div class="sec-hdr" style="border-top:none;padding-top:8px;">Account Summary</div>',
                unsafe_allow_html=True)

    def _acct_row(lbl, val, color="#e6edf3"):
        return (f'<div class="sum-row">'
                f'<span class="sum-lbl">{lbl}</span>'
                f'<span class="sum-val" style="color:{color};">{val}</span>'
                f'</div>')

    _eq_color = "#3fb950" if _equity >= 100000 else "#f85149"
    _pnl_color = "#3fb950" if _tot_pnl >= 0 else "#f85149"
    _day_color = "#3fb950" if _day_pnl >= 0 else "#f85149"

    _acct_html = '<div style="background:#161b22;border:1px solid #30363d;border-radius:10px;margin:0 12px 12px;overflow:hidden;">'
    _acct_html += _acct_row("EQUITY", _fmt_money(_equity), _eq_color)
    _acct_html += _acct_row("TOTAL P&L",
                            f"{_fmt_money(_tot_pnl)} ({_tot_pct:+.2f}%)", _pnl_color)
    _acct_html += _acct_row("TODAY P&L",
                            f"{_fmt_money(_day_pnl)} ({_day_pct:+.2f}%)", _day_color)
    _acct_html += _acct_row("CASH", _fmt_money(_bal))
    _acct_html += _acct_row("WIN RATE", f"{_wr * 100:.1f}%")
    _acct_html += _acct_row("PROFIT FACTOR",
                            f"{_pf:.2f}", "#3fb950" if _pf >= 1.5 else "#d29922" if _pf >= 1.0 else "#f85149")
    _acct_html += _acct_row("SHARPE", f"{_sh:.2f}")
    _acct_html += _acct_row("MAX DRAWDOWN", f"-{_dd:.2f}%", "#f85149" if _dd > 5 else "#d29922")
    _acct_html += _acct_row("TOTAL TRADES", str(_n_trd))
    _acct_html += '</div>'
    st.markdown(_acct_html, unsafe_allow_html=True)

    # ── SECTION 2: Open positions ────────────────────────────────────────────
    st.markdown('<div class="sec-hdr">Open Positions</div>', unsafe_allow_html=True)

    if not _pos:
        st.markdown('<div style="padding:12px 16px;color:#484f58;font-size:0.8em;">No open positions.</div>',
                    unsafe_allow_html=True)
    else:
        for _p in _pos:
            _pt     = _p.get("ticker", "—")
            _pshrs  = _p.get("shares", 0) or 0
            _pavg   = _p.get("avg_cost") or 0
            _pcur   = _p.get("current_price") or 0
            _punr   = _p.get("unrealized_pnl") or 0
            _punrp  = _p.get("unrealized_pct") or 0
            _pstop  = _p.get("stop_loss")
            _pt1    = _p.get("target_1")
            _pt2    = _p.get("target_2")
            _pt3    = _p.get("target_3")
            _pht    = _p.get("hold_type") or "—"
            _preason = _p.get("entry_reason") or "—"
            _pconv  = _p.get("conviction")
            _pmae   = _p.get("mae") or 0
            _pmfe   = _p.get("mfe") or 0
            _popened = _p.get("opened_at")
            _pnl_cls = "pos-pnl-pos" if _punr >= 0 else "pos-pnl-neg"

            with st.expander(f"{_pt}   {_punrp:+.1f}%", expanded=False):
                st.markdown(f"""
                <div class="pos-card" style="margin:0;border:none;">
                  <div style="margin-bottom:10px;">
                    <span class="pos-ticker">{_pt}</span>
                    <span class="pos-side">LONG  |  {_pshrs:.0f} shares</span>
                  </div>
                  <div class="pos-meta">Avg ${_pavg:.2f} &nbsp;|&nbsp; Now ${_pcur:.2f}</div>
                  <div style="margin:6px 0;">
                    <span class="{_pnl_cls}">{_fmt_money(_punr)} ({_punrp:+.1f}%)</span>
                  </div>
                  <div class="pos-meta">
                    Stop {_fmt_money(_pstop)} &nbsp;|&nbsp; T1 {_fmt_money(_pt1)} &nbsp;|&nbsp; T2 {_fmt_money(_pt2)}
                  </div>
                  <div class="pos-meta" style="margin-top:8px;color:#484f58;">
                    T3 {_fmt_money(_pt3)} &nbsp;|&nbsp; {_pht} &nbsp;|&nbsp;
                    Conv {f"{_pconv:.0f}" if _pconv else "—"} &nbsp;|&nbsp; {_preason}
                  </div>
                  <div class="pos-meta" style="color:#484f58;">
                    MAE {_pmae:+.1f}%  MFE {_pmfe:+.1f}%  &nbsp;|&nbsp; Opened {_fmt_ts_et(_popened)}
                  </div>
                </div>
                """, unsafe_allow_html=True)

    # ── SECTION 3: Equity curve ──────────────────────────────────────────────
    st.markdown('<div class="sec-hdr">Account Equity — $100,000 Base</div>', unsafe_allow_html=True)

    if _eq:
        import pandas as pd
        _eq_df = pd.DataFrame(_eq)
        _eq_df["recorded_at"] = pd.to_datetime(_eq_df["recorded_at"])
        _eq_df = _eq_df.set_index("recorded_at").rename(columns={"equity": "Equity"})
        st.line_chart(_eq_df[["Equity"]])
    else:
        st.markdown('<div style="padding:12px 16px;color:#484f58;font-size:0.78em;">No equity data yet.</div>',
                    unsafe_allow_html=True)

    # ── SECTION 4: Recent trades ─────────────────────────────────────────────
    st.markdown('<div class="sec-hdr">Recent Trades (Last 10)</div>', unsafe_allow_html=True)

    if not _trd:
        st.markdown('<div style="padding:12px 16px;color:#484f58;font-size:0.8em;">No closed trades yet.</div>',
                    unsafe_allow_html=True)
    else:
        for _t in _trd:
            _tt   = _t.get("ticker", "—")
            _tside = (_t.get("side") or "LONG").upper()
            _tex  = _t.get("exit_reason") or "—"
            _ten  = _t.get("entry_price") or 0
            _tex2 = _t.get("exit_price") or 0
            _tpnl = _t.get("net_pnl") or 0
            _tpct = _t.get("pnl_pct") or 0
            _thd  = _t.get("hold_days") or 0
            _tpnl_cls = "pos-pnl-pos" if _tpnl >= 0 else "pos-pnl-neg"
            st.markdown(f"""
            <div style="background:#161b22;border:1px solid #30363d;border-radius:8px;
                        padding:10px 14px;margin:4px 12px;">
              <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
                <span style="font-family:'JetBrains Mono',monospace;font-weight:700;
                             color:#58a6ff;">{_tt}</span>
                <span style="font-size:0.72em;color:#484f58;">{_tside}  |  {_tex}</span>
              </div>
              <div style="font-size:0.72em;color:#8b949e;">
                Entry ${_ten:.2f} &rarr; Exit ${_tex2:.2f}
              </div>
              <div style="margin-top:4px;">
                <span class="{_tpnl_cls}">{_fmt_money(_tpnl)} ({_tpct:+.2f}%)</span>
                <span style="font-size:0.7em;color:#484f58;margin-left:8px;">Held {_thd:.1f}d</span>
              </div>
            </div>
            """, unsafe_allow_html=True)

    # ── SECTION 5: Performance stats ─────────────────────────────────────────
    st.markdown('<div class="sec-hdr">Performance Stats</div>', unsafe_allow_html=True)

    _wins  = int(_acct.get("winning_trades", 0))
    _loses = int(_acct.get("losing_trades", 0))

    if _trd:
        import pandas as pd
        _tdf = pd.DataFrame(_trd)
        _avg_win  = _tdf.loc[_tdf["pnl_pct"] > 0, "pnl_pct"].mean() if not _tdf.empty else 0
        _avg_loss = _tdf.loc[_tdf["pnl_pct"] <= 0, "pnl_pct"].mean() if not _tdf.empty else 0
        _avg_hold = _tdf["hold_days"].mean() if "hold_days" in _tdf.columns and not _tdf.empty else 0
        _lrg_win  = _tdf["net_pnl"].max() if "net_pnl" in _tdf.columns and not _tdf.empty else 0
        _lrg_loss = _tdf["net_pnl"].min() if "net_pnl" in _tdf.columns and not _tdf.empty else 0

        _ht_stats = {}
        if "hold_type" in _tdf.columns:
            for _ht in _tdf["hold_type"].dropna().unique():
                _g = _tdf[_tdf["hold_type"] == _ht]
                _ht_stats[_ht] = {
                    "n":       len(_g),
                    "wr":      round((_g["pnl_pct"] > 0).mean() * 100, 1),
                    "avg_pnl": round(_g["pnl_pct"].mean(), 2),
                    "tot_pnl": round(_g["net_pnl"].sum(), 2),
                }
    else:
        _avg_win = _avg_loss = _avg_hold = _lrg_win = _lrg_loss = 0
        _ht_stats = {}

    _perf_html = '<div style="background:#161b22;border:1px solid #30363d;border-radius:10px;margin:0 12px 12px;overflow:hidden;">'
    _perf_html += _acct_row("Total Trades", str(_n_trd))
    _perf_html += _acct_row("Winners", str(_wins))
    _perf_html += _acct_row("Losers", str(_loses))
    _perf_html += _acct_row("Avg Hold Days", f"{_avg_hold:.1f}d" if _avg_hold else "—")
    _perf_html += _acct_row("Avg Win", f"{_avg_win:+.2f}%" if _avg_win else "—", "#3fb950")
    _perf_html += _acct_row("Avg Loss", f"{_avg_loss:+.2f}%" if _avg_loss else "—", "#f85149")
    _perf_html += _acct_row("Largest Win", _fmt_money(_lrg_win), "#3fb950")
    _perf_html += _acct_row("Largest Loss", _fmt_money(_lrg_loss), "#f85149")
    _perf_html += '</div>'
    st.markdown(_perf_html, unsafe_allow_html=True)

    if _ht_stats:
        st.markdown('<div class="sec-hdr">By Hold Type</div>', unsafe_allow_html=True)
        for _ht, _hts in _ht_stats.items():
            _ht_html = (
                f'<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;'
                f'padding:10px 14px;margin:4px 12px;">'
                f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.8em;'
                f'font-weight:700;color:#8b949e;margin-bottom:6px;">{_ht}</div>'
                f'<div style="display:flex;gap:16px;font-size:0.72em;">'
                f'<div><div style="color:#484f58;">N</div><div style="color:#e6edf3;font-family:\'JetBrains Mono\',monospace;">{_hts["n"]}</div></div>'
                f'<div><div style="color:#484f58;">WIN RATE</div><div style="color:#3fb950;font-family:\'JetBrains Mono\',monospace;">{_hts["wr"]:.1f}%</div></div>'
                f'<div><div style="color:#484f58;">AVG P&L</div><div style="font-family:\'JetBrains Mono\',monospace;color:{"#3fb950" if _hts["avg_pnl"] >= 0 else "#f85149"};">{_hts["avg_pnl"]:+.2f}%</div></div>'
                f'<div><div style="color:#484f58;">TOTAL</div><div style="font-family:\'JetBrains Mono\',monospace;color:{"#3fb950" if _hts["tot_pnl"] >= 0 else "#f85149"};">${_hts["tot_pnl"]:,.0f}</div></div>'
                f'</div></div>'
            )
            st.markdown(_ht_html, unsafe_allow_html=True)

    st.markdown('<div class="refresh-hint">Live — updates every 30s</div>', unsafe_allow_html=True)


@st.fragment(run_every=15)
def _mob_control_screen():
    _ctl, _sts = _load_control()
    _paused     = _ctl.get("paused", False)
    _force_scan = _ctl.get("force_scan", False)
    _cur_mode   = _ctl.get("current_mode", "UNKNOWN")
    _last_upd   = _sts.get("last_updated")
    _top        = _sts.get("top_signal")

    if _paused:
        _bcolor = "#d29922"; _bbg = "rgba(210,153,34,0.08)"; _bborder = "rgba(210,153,34,0.2)"
        _blabel = "PAUSED — manual hold"
    else:
        _mc, _mbg, _mbrd = _MODE_MAP.get(_cur_mode, _MODE_MAP["UNKNOWN"])
        _bcolor, _bbg, _bborder = _mc, _mbg, _mbrd
        _blabel = _cur_mode

    st.markdown(f"""
    <div style="margin:12px 12px 16px;background:{_bbg};border:1px solid {_bborder};
                border-radius:10px;padding:12px 16px;display:flex;align-items:center;gap:10px;">
      <div style="width:10px;height:10px;border-radius:50%;background:{_bcolor};
                  box-shadow:0 0 0 3px {_bcolor}33;flex-shrink:0;"></div>
      <span style="font-size:0.85em;font-weight:700;color:{_bcolor};">{_blabel}</span>
    </div>
    """, unsafe_allow_html=True)

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
            'font-family:\'JetBrains Mono\',monospace;">Scan queued — scanner picks up within 30s</div>',
            unsafe_allow_html=True,
        )

    st.markdown('<div class="sec-hdr">Quick Summary</div>', unsafe_allow_html=True)

    _top_str = (f"{_top['ticker']} · {_top['label']} · {_top['score']:.0f}pts"
                if _top else "No signals yet")
    _mkt_color = _MODE_MAP.get(_cur_mode, _MODE_MAP["UNKNOWN"])[0]

    st.markdown(f"""
    <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;
                margin:0 12px;overflow:hidden;">
      <div class="sum-row">
        <span class="sum-lbl">Last scan</span>
        <span class="sum-val">{_mins_ago(_last_upd)}</span>
      </div>
      <div class="sum-row">
        <span class="sum-lbl">Signals today</span>
        <span class="sum-val" style="color:#58a6ff;">{_sts.get('signals_today', 0)}</span>
      </div>
      <div class="sum-row">
        <span class="sum-lbl">Alerts today</span>
        <span class="sum-val">{_sts.get('alerts_today', 0)}</span>
      </div>
      <div class="sum-row">
        <span class="sum-lbl">Top signal</span>
        <span class="sum-val" style="font-size:0.72em;max-width:55%;text-align:right;">{_top_str}</span>
      </div>
      <div class="sum-row">
        <span class="sum-lbl">Scanner mode</span>
        <span class="sum-val" style="color:{_mkt_color};">{_cur_mode}</span>
      </div>
      <div class="sum-row" style="border-bottom:none;">
        <span class="sum-lbl">Scanner uptime</span>
        <span class="sum-val">{_uptime(_ctl.get('scanner_started_at'))}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

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
            _lh += (f'<div class="log-item">'
                    f'<span class="log-time">{_t_str}</span>'
                    f'<span class="log-msg {_lvl}">{_msg}</span>'
                    f'</div>')
        st.markdown(_lh, unsafe_allow_html=True)

    st.markdown('<div class="refresh-hint">Live — updates every 15s</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# SCREEN DISPATCH
# ══════════════════════════════════════════════════════════════════════════════

if _screen == "status":
    _mob_status_screen()
elif _screen == "signals":
    _mob_signals_screen()
elif _screen == "alerts":
    _mob_alerts_screen()
elif _screen == "accuracy":
    _accuracy_screen()
elif _screen == "account":
    _account_screen()
elif _screen == "control":
    _mob_control_screen()
