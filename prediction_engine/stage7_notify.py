# prediction_engine/stage7_notify.py
# Stage 7 (9:20 AM ET): Deliver Pushover notification with PDF attachment path.

import os
import logging
import requests
from typing import Optional

from .config import PUSHOVER_USER, PUSHOVER_TOKEN

logger = logging.getLogger("pe.stage7")

PUSHOVER_API = "https://api.pushover.net/1/messages.json"


def run(picks: list, report_path: str) -> bool:
    """
    Send Pushover notification summarizing the top 5 picks.
    Returns True on success.
    """
    if not picks:
        logger.warning("[stage7] No picks to notify")
        return False

    if not PUSHOVER_USER or not PUSHOVER_TOKEN:
        logger.warning("[stage7] Pushover credentials not set — skipping notification")
        return False

    title   = "Axiom Terminal — Pre-Market Conviction Report"
    message = _build_message(picks)

    success = _send_pushover(title, message, priority=1)

    if success:
        logger.info("[stage7] Pushover notification delivered (%d picks)", len(picks))
        if report_path and os.path.exists(report_path):
            logger.info("[stage7] Report available: %s", report_path)
    else:
        logger.error("[stage7] Pushover delivery failed")

    return success


def send_cancellation(ticker: str, reason: str) -> bool:
    """
    Stage 8 calls this when a pick is cancelled post-open.
    """
    if not PUSHOVER_USER or not PUSHOVER_TOKEN:
        return False
    title   = f"Axiom PE — {ticker} CANCELLED"
    message = f"{ticker} has been removed from today's conviction list.\nReason: {reason}"
    return _send_pushover(title, message, priority=0)


def _build_message(picks: list) -> str:
    lines = [f"{len(picks)} pre-market conviction picks identified:\n"]
    for p in picks:
        ticker = p["ticker"]
        score  = p["conviction_score"]
        tier   = p["conviction_tier"]
        entry  = p["entry"]
        target = p["target"]
        stop   = p["stop"]
        gap    = p.get("gap_pct", 0)
        models = p.get("model_agreement_count", 0)
        rr     = p.get("risk_reward", 0)

        lines.append(
            f"#{p['rank']} {ticker} — {tier} ({score:.0f}/100)\n"
            f"   {models}/4 models | Gap {gap:+.1f}%\n"
            f"   Entry ${entry:.2f} | Target ${target:.2f} | Stop ${stop:.2f} | {rr:.1f}R\n"
        )

    lines.append("\nFull report saved to output directory.")
    return "\n".join(lines)


def _send_pushover(title: str, message: str, priority: int = 0,
                   retries: int = 3) -> bool:
    payload = {
        "token":    PUSHOVER_TOKEN,
        "user":     PUSHOVER_USER,
        "title":    title,
        "message":  message,
        "priority": priority,
        "sound":    "cashregister" if priority >= 1 else "pushover",
    }
    if priority == 2:
        payload["retry"]  = 60
        payload["expire"] = 3600

    import time
    for attempt in range(retries):
        try:
            resp = requests.post(PUSHOVER_API, data=payload, timeout=15)
            if resp.status_code == 200 and resp.json().get("status") == 1:
                return True
            logger.warning("[stage7] Pushover attempt %d: HTTP %d — %s",
                           attempt + 1, resp.status_code, resp.text[:100])
        except requests.RequestException as e:
            logger.warning("[stage7] Pushover attempt %d: %s", attempt + 1, e)
        if attempt < retries - 1:
            time.sleep(5 * (attempt + 1))

    return False
