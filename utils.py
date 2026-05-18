# utils.py
# Shared utilities — imported everywhere to avoid duplication.

import os
import time
import requests
from datetime import datetime, timedelta
from typing import Optional

try:
    from zoneinfo import ZoneInfo
    _TZ_ET  = ZoneInfo("America/New_York")
    _TZ_CST = ZoneInfo("America/Chicago")
except ImportError:
    import pytz
    _TZ_ET  = pytz.timezone("America/New_York")
    _TZ_CST = pytz.timezone("America/Chicago")

FINNHUB_BASE = "https://finnhub.io/api/v1"

# ── Timezone helpers ──────────────────────────────────────────────────────────

def now_et() -> datetime:
    """Current time in US/Eastern (market timezone)."""
    return datetime.now(_TZ_ET)

def now_cst() -> datetime:
    """Current time in US/Central (user's local timezone)."""
    return datetime.now(_TZ_CST)

def now_utc() -> datetime:
    """Current time in UTC — use this for all DB writes."""
    from datetime import timezone
    return datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC for psycopg2

def et_date_str() -> str:
    """Today's date string in ET — use for DATE() comparisons."""
    return now_et().strftime("%Y-%m-%d")

# ── Safe float conversion ─────────────────────────────────────────────────────

def safe_float(v, fallback: float = 0.0) -> float:
    """Convert any numeric type to Python float, returning fallback on failure."""
    try:
        f = float(v)
        return fallback if (f != f or f == float("inf") or f == float("-inf")) else f
    except Exception:
        return fallback

# Alias used widely
_safe = safe_float

# ── Finnhub API helpers ───────────────────────────────────────────────────────

def fh_key() -> str:
    return os.environ.get("FINNHUB_API_KEY", "")

def fh_get(endpoint: str, params: dict, timeout: int = 8) -> Optional[dict]:
    """
    Make a Finnhub REST call. Returns parsed JSON or None on any failure.
    Caller should check for None before using result.
    """
    key = fh_key()
    if not key:
        return None
    try:
        params = {**params, "token": key}
        resp = requests.get(f"{FINNHUB_BASE}/{endpoint}", params=params, timeout=timeout)
        if resp.status_code == 429:
            print(f"  [finnhub] Rate limit hit on /{endpoint} — sleeping 12s")
            time.sleep(12)
            resp = requests.get(f"{FINNHUB_BASE}/{endpoint}", params=params, timeout=timeout)
        if resp.status_code == 200:
            data = resp.json()
            return data if data else None
        print(f"  [finnhub] /{endpoint} HTTP {resp.status_code}")
        return None
    except Exception as e:
        print(f"  [finnhub] /{endpoint} error: {e}")
        return None

# ── Anthropic model constant ──────────────────────────────────────────────────

def anthropic_model() -> str:
    """Return the configured Anthropic model name."""
    try:
        from config import ANTHROPIC_MODEL
        return ANTHROPIC_MODEL
    except Exception:
        return "claude-sonnet-4-20250514"

def anthropic_haiku() -> str:
    """Return the fast/cheap Haiku model for inline analysis."""
    return os.environ.get("ANTHROPIC_HAIKU_MODEL", "claude-haiku-4-5-20251001")
