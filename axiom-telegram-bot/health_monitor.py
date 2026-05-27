# axiom-telegram-bot/health_monitor.py
# Background thread that watches axiom-scanner and sends Pushover alerts
# when it crashes or recovers. Transitions only — no spam.

import os
import time
import threading
import subprocess
import logging
import requests

logger = logging.getLogger(__name__)

MONITOR_INTERVAL = 60          # seconds between checks
WATCHED_CONTAINER = "axiom-scanner"
PUSHOVER_API = "https://api.pushover.net/1/messages.json"


def _pushover(title: str, message: str, priority: int = 0) -> None:
    token    = os.environ.get("PUSHOVER_API_TOKEN", "")
    user_key = os.environ.get("PUSHOVER_USER_KEY", "")
    if not token or not user_key:
        return
    try:
        payload = {
            "token":   token,
            "user":    user_key,
            "title":   title,
            "message": message,
            "priority": priority,
        }
        if priority == 2:
            payload["retry"]  = 60
            payload["expire"] = 300
        requests.post(PUSHOVER_API, data=payload, timeout=10)
    except Exception as e:
        logger.warning(f"[health_monitor] Pushover failed: {e}")


def _is_container_running(name: str) -> bool:
    """Returns True if the named container reports State.Running = true."""
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format={{.State.Running}}", name],
            capture_output=True, text=True, timeout=10,
        )
        return result.stdout.strip().lower() == "true"
    except Exception:
        return False


def _monitor_loop() -> None:
    was_running = True   # assume healthy at start so we don't false-alarm on boot
    logger.info(f"[health_monitor] Watching {WATCHED_CONTAINER} every {MONITOR_INTERVAL}s")

    while True:
        try:
            running = _is_container_running(WATCHED_CONTAINER)

            if was_running and not running:
                # Transition: was up, now down
                logger.error(f"[health_monitor] {WATCHED_CONTAINER} is DOWN — alerting")
                _pushover(
                    title=f"🔴 Axiom Scanner DOWN",
                    message=(
                        f"{WATCHED_CONTAINER} is no longer running.\n"
                        f"Check logs: /logs scanner\n"
                        f"Restart: /restart scanner"
                    ),
                    priority=1,
                )
            elif not was_running and running:
                # Transition: was down, now recovered
                logger.info(f"[health_monitor] {WATCHED_CONTAINER} recovered — alerting")
                _pushover(
                    title=f"🟢 Axiom Scanner Recovered",
                    message=f"{WATCHED_CONTAINER} is running again.",
                    priority=0,
                )

            was_running = running

        except Exception as e:
            logger.warning(f"[health_monitor] check error: {e}")

        time.sleep(MONITOR_INTERVAL)


def start_health_monitor() -> threading.Thread:
    """
    Start the container crash monitor as a background daemon thread.
    Returns the thread (already started).
    Called once from bot.py main().
    """
    t = threading.Thread(target=_monitor_loop, name="health-monitor", daemon=True)
    t.start()
    return t
