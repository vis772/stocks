# prediction_engine/main.py
# Axiom Terminal — Pre-Market Conviction Engine
# Orchestrates the full 8-stage pipeline from 4:00 AM to 9:45 AM ET.
# Called by scheduler.py at 4:00 AM daily on weekdays.

import sys
import time
import logging
import os
from datetime import datetime, timezone, timedelta

# ─── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/axiom_pe.log", mode="a"),
    ],
)
logger = logging.getLogger("pe.main")

from . import (
    stage1_fetch,
    stage2_sweep,
    stage3_deepdive,
    stage4_consensus,
    stage5_conviction,
    stage6_report,
    stage7_notify,
    stage8_sanity,
)
from .db         import initialize_schema, save_picks, upsert_run_status, _et_date
from .resource_manager import (
    load_models, unload_models, ollama_healthy,
    check_cpu_contention, wait_for_cpu_headroom, get_gpu_memory_info, get_ram_info,
)


def _et_now() -> datetime:
    utc    = datetime.now(timezone.utc)
    offset = -4 if 3 <= utc.month <= 11 else -5
    return utc + timedelta(hours=offset)


def _wait_until_et(hour: int, minute: int, label: str):
    """Sleep until a specific ET time. Logs remaining time periodically."""
    now = _et_now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= target:
        return
    delta = (target - now).total_seconds()
    logger.info("[main] Waiting %.0f minutes until %s (%02d:%02d ET)",
                delta / 60, label, hour, minute)
    # Sleep in 60-second chunks so we can log progress
    while True:
        remaining = (target - _et_now()).total_seconds()
        if remaining <= 0:
            break
        if remaining > 120:
            logger.debug("[main] %s in %.0fm", label, remaining / 60)
        time.sleep(min(60, remaining))


def run_pipeline():
    """
    Execute the complete pre-market conviction pipeline.
    Stages run sequentially with timing gates.
    """
    date_str = _et_date()
    logger.info("=" * 70)
    logger.info("[main] Axiom Terminal Pre-Market Conviction Engine")
    logger.info("[main] Run date: %s  |  ET time: %s",
                date_str, _et_now().strftime("%H:%M:%S"))
    logger.info("=" * 70)

    # ── DB schema ──────────────────────────────────────────────────────────────
    try:
        initialize_schema()
    except Exception as e:
        logger.error("[main] DB schema initialization failed: %s", e)
        # Non-fatal — continue without DB persistence

    upsert_run_status(date_str, "STARTING", "running")

    # ── RAM baseline (t3.xlarge, CPU mode — no GPU) ───────────────────────────
    ram_info = get_ram_info()
    if ram_info:
        logger.info("[main] RAM: %dMB used / %dMB total (%.0f%% used)",
                    ram_info.get("used_mb", 0), ram_info.get("total_mb", 0),
                    ram_info.get("pct", 0))

    # ─────────────────────────────────────────────────────────────────────────
    # 3:55 AM — Load models into VRAM
    # ─────────────────────────────────────────────────────────────────────────
    _wait_until_et(3, 55, "MODEL_LOAD")
    logger.info("[main] ── STAGE 0: Load models (3:55 AM ET) ─────────────────")

    if not ollama_healthy():
        logger.error("[main] Ollama is not reachable at %s — aborting", _import_base_url())
        upsert_run_status(date_str, "STAGE0_LOAD", "failed", error_msg="Ollama unreachable")
        return

    models_ok = load_models()
    if not models_ok:
        logger.warning("[main] Some models failed to load — pipeline may be degraded")

    ram_info = get_ram_info()
    if ram_info:
        logger.info("[main] RAM after model load: %dMB used / %dMB free (%.0f%%)",
                    ram_info.get("used_mb", 0), ram_info.get("free_mb", 0),
                    ram_info.get("pct", 0))
        if ram_info.get("pct", 0) > 85:
            logger.warning("[main] RAM usage above 85%% — consider reducing model count")

    # ─────────────────────────────────────────────────────────────────────────
    # 4:00 AM — Stage 1: Fetch universe + basic filters
    # ─────────────────────────────────────────────────────────────────────────
    _wait_until_et(4, 0, "STAGE1_FETCH")
    logger.info("[main] ── STAGE 1: Universe fetch + basic filters (4:00 AM ET) ──")
    upsert_run_status(date_str, "STAGE1_FETCH", "running")

    candidates = []
    try:
        wait_for_cpu_headroom()
        candidates = stage1_fetch.run()
        logger.info("[main] Stage 1 complete: %d candidates", len(candidates))
        upsert_run_status(date_str, "STAGE1_FETCH", "completed", tickers_input=len(candidates))
    except Exception as e:
        logger.exception("[main] Stage 1 failed: %s", e)
        upsert_run_status(date_str, "STAGE1_FETCH", "failed", error_msg=str(e))

    if not candidates:
        logger.error("[main] Stage 1 returned no candidates — aborting pipeline")
        upsert_run_status(date_str, "STAGE1_FETCH", "failed", error_msg="no_candidates")
        _cleanup_and_exit()
        return

    # ─────────────────────────────────────────────────────────────────────────
    # 5:00 AM — Stage 2: Qwen2.5 sweep (500 → 50)
    # ─────────────────────────────────────────────────────────────────────────
    _wait_until_et(5, 0, "STAGE2_SWEEP")
    logger.info("[main] ── STAGE 2: Qwen sweep (5:00 AM ET) ────────────────────")
    upsert_run_status(date_str, "STAGE2_SWEEP", "running", tickers_input=len(candidates))

    top50 = []
    try:
        wait_for_cpu_headroom()
        top50 = stage2_sweep.run(candidates)
        logger.info("[main] Stage 2 complete: %d top candidates", len(top50))
        upsert_run_status(date_str, "STAGE2_SWEEP", "completed", tickers_input=len(top50))
    except Exception as e:
        logger.exception("[main] Stage 2 failed: %s", e)
        upsert_run_status(date_str, "STAGE2_SWEEP", "failed", error_msg=str(e))
        # Fall back: take top 50 by gap_pct * vol_ratio
        top50 = sorted(
            candidates,
            key=lambda c: abs(c.get("gap_pct", 0)) * c.get("volume_ratio", 1),
            reverse=True,
        )[:50]
        logger.warning("[main] Stage 2 fallback: using top 50 by momentum score")

    if not top50:
        logger.error("[main] No candidates after Stage 2 — aborting")
        _cleanup_and_exit()
        return

    # ─────────────────────────────────────────────────────────────────────────
    # 6:00 AM — Stage 3: All 4 SLMs deep dive
    # ─────────────────────────────────────────────────────────────────────────
    _wait_until_et(6, 0, "STAGE3_DEEPDIVE")
    logger.info("[main] ── STAGE 3: 4-model deep dive (6:00 AM ET) ─────────────")
    upsert_run_status(date_str, "STAGE3_DEEPDIVE", "running", tickers_input=len(top50))

    model_scores = {}
    try:
        wait_for_cpu_headroom()
        model_scores = stage3_deepdive.run(top50)
        score_count  = sum(
            1 for t in model_scores.values()
            for v in t.values() if v.get("score") is not None
        )
        logger.info("[main] Stage 3 complete: %d model scores", score_count)
        upsert_run_status(date_str, "STAGE3_DEEPDIVE", "completed")
    except Exception as e:
        logger.exception("[main] Stage 3 failed: %s", e)
        upsert_run_status(date_str, "STAGE3_DEEPDIVE", "failed", error_msg=str(e))

    # ─────────────────────────────────────────────────────────────────────────
    # 8:00 AM — Stage 4: Consensus voting
    # ─────────────────────────────────────────────────────────────────────────
    _wait_until_et(8, 0, "STAGE4_CONSENSUS")
    logger.info("[main] ── STAGE 4: Consensus voting (8:00 AM ET) ──────────────")
    upsert_run_status(date_str, "STAGE4_CONSENSUS", "running")

    consensus = []
    try:
        consensus = stage4_consensus.run(top50, model_scores)
        logger.info("[main] Stage 4 complete: %d tickers passed consensus", len(consensus))
        upsert_run_status(date_str, "STAGE4_CONSENSUS", "completed",
                          tickers_input=len(consensus))
    except Exception as e:
        logger.exception("[main] Stage 4 failed: %s", e)
        upsert_run_status(date_str, "STAGE4_CONSENSUS", "failed", error_msg=str(e))

    # ─────────────────────────────────────────────────────────────────────────
    # Stage 5: Select top 5 and compute entry/target/stop
    # (Runs immediately after Stage 4 — within 8:00-8:30 window)
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("[main] ── STAGE 5: Final conviction selection ──────────────────")
    upsert_run_status(date_str, "STAGE5_CONVICTION", "running")

    picks = []
    try:
        if consensus:
            picks = stage5_conviction.run(consensus)
        else:
            logger.warning("[main] No consensus picks — Stage 5 skipped")

        logger.info("[main] Stage 5 complete: %d final picks", len(picks))
        upsert_run_status(date_str, "STAGE5_CONVICTION", "completed", picks_count=len(picks))

        # Persist picks to DB
        if picks:
            save_picks(date_str, picks)
    except Exception as e:
        logger.exception("[main] Stage 5 failed: %s", e)
        upsert_run_status(date_str, "STAGE5_CONVICTION", "failed", error_msg=str(e))

    # ─────────────────────────────────────────────────────────────────────────
    # 8:30 AM — Stage 6: Generate PDF report
    # ─────────────────────────────────────────────────────────────────────────
    _wait_until_et(8, 30, "STAGE6_REPORT")
    logger.info("[main] ── STAGE 6: PDF report generation (8:30 AM ET) ─────────")
    upsert_run_status(date_str, "STAGE6_REPORT", "running")

    report_path = None
    try:
        report_path = stage6_report.run(picks)
        logger.info("[main] Stage 6 complete: %s", report_path)
        upsert_run_status(date_str, "STAGE6_REPORT", "completed")
    except Exception as e:
        logger.exception("[main] Stage 6 failed: %s", e)
        upsert_run_status(date_str, "STAGE6_REPORT", "failed", error_msg=str(e))

    # ─────────────────────────────────────────────────────────────────────────
    # 9:20 AM — Stage 7: Pushover notification
    # ─────────────────────────────────────────────────────────────────────────
    _wait_until_et(9, 20, "STAGE7_NOTIFY")
    logger.info("[main] ── STAGE 7: Pushover notification (9:20 AM ET) ──────────")
    upsert_run_status(date_str, "STAGE7_NOTIFY", "running")

    try:
        ok = stage7_notify.run(picks, report_path or "")
        status = "completed" if ok else "failed"
        upsert_run_status(date_str, "STAGE7_NOTIFY", status)
    except Exception as e:
        logger.exception("[main] Stage 7 failed: %s", e)
        upsert_run_status(date_str, "STAGE7_NOTIFY", "failed", error_msg=str(e))

    # ─────────────────────────────────────────────────────────────────────────
    # 9:45 AM — Stage 8: Post-open sanity check (CPU only, no SLMs)
    # ─────────────────────────────────────────────────────────────────────────
    _wait_until_et(9, 45, "STAGE8_SANITY")
    logger.info("[main] ── STAGE 8: Post-open sanity check (9:45 AM ET) ─────────")
    upsert_run_status(date_str, "STAGE8_SANITY", "running")

    try:
        valid_picks = stage8_sanity.run(picks)
        logger.info("[main] Stage 8 complete: %d of %d picks still valid",
                    len(valid_picks), len(picks))
        upsert_run_status(date_str, "STAGE8_SANITY", "completed",
                          picks_count=len(valid_picks))
    except Exception as e:
        logger.exception("[main] Stage 8 failed: %s", e)
        upsert_run_status(date_str, "STAGE8_SANITY", "failed", error_msg=str(e))

    # ─────────────────────────────────────────────────────────────────────────
    # 10:00 AM — Unload models, return GPU to Axiom scanner
    # ─────────────────────────────────────────────────────────────────────────
    _wait_until_et(10, 0, "MODEL_UNLOAD")
    logger.info("[main] ── STAGE 9: Unload models (10:00 AM ET) ─────────────────")
    _cleanup_and_exit()

    logger.info("[main] ── Pipeline complete for %s ─────────────────────────────", date_str)
    upsert_run_status(date_str, "COMPLETE", "completed", picks_count=len(picks))


def _cleanup_and_exit():
    """Unload all models and return RAM + CPU to the Axiom scanner."""
    try:
        unload_models()
    except Exception as e:
        logger.warning("[main] Model unload error: %s", e)

    ram_info = get_ram_info()
    if ram_info:
        logger.info("[main] RAM after unload: %dMB used / %dMB free (%.0f%%)",
                    ram_info.get("used_mb", 0), ram_info.get("free_mb", 0),
                    ram_info.get("pct", 0))


def _import_base_url() -> str:
    try:
        from .config import OLLAMA_BASE_URL
        return OLLAMA_BASE_URL
    except Exception:
        return "http://localhost:11434"


if __name__ == "__main__":
    run_pipeline()
