# prediction_engine/resource_manager.py
# Ollama model lifecycle management and CPU contention monitoring.
# Running on t3.xlarge — CPU-only inference (no GPU).
# Pre-warms models into RAM at 3:55 AM ET; unloads at 10:00 AM ET.

import time
import logging
import requests
import psutil
from typing import Optional

from .config import (
    OLLAMA_BASE_URL, OLLAMA_TIMEOUT, ALL_MODELS,
    CPU_YIELD_THRESHOLD, CPU_POLL_INTERVAL,
)

logger = logging.getLogger("pe.resource")


def _ollama_url(path: str) -> str:
    return f"{OLLAMA_BASE_URL.rstrip('/')}{path}"


# ── Model lifecycle ───────────────────────────────────────────────────────────

def load_models() -> bool:
    """
    Warm all 4 models into RAM (CPU mode) by sending a trivial generation request.
    On t3.xlarge with 16GB RAM, all four models (total ~10-12GB) fit simultaneously.
    Pre-loading avoids cold-start latency during the timed pipeline stages.
    Returns True if all models loaded successfully.
    """
    logger.info("[resource] CPU mode — loading %d models into RAM (~10-12GB total)...",
                len(ALL_MODELS))
    success = True
    for model in ALL_MODELS:
        ok = _warm_model(model)
        if ok:
            logger.info("  [resource] %s loaded", model)
        else:
            logger.error("  [resource] FAILED to load %s", model)
            success = False
    return success


def unload_models():
    """
    Unload all models from RAM by setting keep_alive=0.
    Called at 10:00 AM ET to return RAM and CPU to the Axiom scanner.
    Without this, Ollama holds models in memory indefinitely.
    """
    logger.info("[resource] Unloading models from RAM (returning resources to Axiom scanner)...")
    for model in ALL_MODELS:
        _unload_model(model)
    logger.info("[resource] All models unloaded")


def _warm_model(model: str, retries: int = 3) -> bool:
    for attempt in range(retries):
        try:
            resp = requests.post(
                _ollama_url("/api/generate"),
                json={
                    "model":      model,
                    "prompt":     "ping",
                    "stream":     False,
                    "keep_alive": "2h",
                    "options":    {"num_predict": 1},
                },
                timeout=OLLAMA_TIMEOUT,
            )
            if resp.status_code == 200:
                return True
            logger.warning("  [resource] %s warm attempt %d: HTTP %d", model, attempt + 1, resp.status_code)
        except requests.RequestException as e:
            logger.warning("  [resource] %s warm attempt %d: %s", model, attempt + 1, e)
        time.sleep(5 * (attempt + 1))
    return False


def _unload_model(model: str):
    try:
        requests.post(
            _ollama_url("/api/generate"),
            json={
                "model":      model,
                "prompt":     "",
                "stream":     False,
                "keep_alive": 0,
            },
            timeout=30,
        )
    except Exception as e:
        logger.warning("  [resource] Failed to unload %s: %s", model, e)


def list_loaded_models() -> list:
    """Return names of models currently resident in VRAM."""
    try:
        resp = requests.get(_ollama_url("/api/ps"), timeout=15)
        if resp.status_code == 200:
            return [m.get("name", "") for m in resp.json().get("models", [])]
    except Exception:
        pass
    return []


def ollama_healthy() -> bool:
    """Return True if Ollama is reachable."""
    try:
        resp = requests.get(_ollama_url("/api/tags"), timeout=10)
        return resp.status_code == 200
    except Exception:
        return False


# ── CPU contention monitoring ─────────────────────────────────────────────────

def check_cpu_contention(threshold: float = CPU_YIELD_THRESHOLD) -> Optional[float]:
    """
    Sample system CPU usage. If above threshold, log contention and return the %.
    Returns None if no contention detected.
    """
    try:
        cpu_pct = psutil.cpu_percent(interval=2.0)
        if cpu_pct >= threshold:
            logger.warning(
                "[resource] CPU contention: %.1f%% >= %.1f%% threshold — "
                "Axiom scanner may be consuming resources",
                cpu_pct, threshold,
            )
            return cpu_pct
        return None
    except Exception:
        return None


def wait_for_cpu_headroom(threshold: float = CPU_YIELD_THRESHOLD,
                          max_wait_seconds: int = 300):
    """
    Block until CPU drops below threshold or max_wait expires.
    Logs contention every poll cycle.
    """
    waited = 0
    while waited < max_wait_seconds:
        cpu = psutil.cpu_percent(interval=CPU_POLL_INTERVAL)
        if cpu < threshold:
            return
        logger.warning(
            "[resource] Yielding — CPU at %.1f%% (Axiom scanner contention). "
            "Waited %ds / %ds",
            cpu, waited, max_wait_seconds,
        )
        waited += CPU_POLL_INTERVAL
    logger.error("[resource] CPU yield timeout after %ds — continuing anyway", max_wait_seconds)


def get_ram_info() -> dict:
    """
    Return system RAM stats (replaces GPU memory info for t3.xlarge CPU-only mode).
    Used for logging; non-fatal if psutil unavailable.
    """
    try:
        mem = psutil.virtual_memory()
        return {
            "used_mb":  int(mem.used  / 1024 / 1024),
            "free_mb":  int(mem.available / 1024 / 1024),
            "total_mb": int(mem.total / 1024 / 1024),
            "pct":      mem.percent,
        }
    except Exception:
        return {}


def get_gpu_memory_info() -> dict:
    """
    No GPU on t3.xlarge. Returns empty dict.
    Kept for API compatibility — callers handle empty dict gracefully.
    """
    return {}
