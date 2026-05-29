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
    stage9_refine,
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


# Set to True by --skip-waits flag (testing only — runs all stages immediately)
_SKIP_WAITS = False


def _wait_until_et(hour: int, minute: int, label: str):
    """Sleep until a specific ET time. Skipped entirely when _SKIP_WAITS is True."""
    if _SKIP_WAITS:
        logger.info("[main] [SKIP-WAITS] Bypassing wait for %s (%02d:%02d ET)",
                    label, hour, minute)
        return
    now = _et_now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= target:
        return
    delta = (target - now).total_seconds()
    logger.info("[main] Waiting %.0f minutes until %s (%02d:%02d ET)",
                delta / 60, label, hour, minute)
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

    # ── Baseline resource snapshot (g4dn.xlarge — T4 GPU) ────────────────────
    ram_info = get_ram_info()
    if ram_info:
        logger.info("[main] RAM: %dMB used / %dMB total (%.0f%% used)",
                    ram_info.get("used_mb", 0), ram_info.get("total_mb", 0),
                    ram_info.get("pct", 0))
    gpu_info = get_gpu_memory_info()
    if gpu_info:
        logger.info("[main] VRAM: %dMB used / %dMB total (%.0f%% used)",
                    gpu_info.get("used_mb", 0), gpu_info.get("total_mb", 0),
                    gpu_info.get("pct", 0))

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

    gpu_info = get_gpu_memory_info()
    if gpu_info:
        logger.info("[main] VRAM after model load: %dMB used / %dMB free (%.0f%%)",
                    gpu_info.get("used_mb", 0), gpu_info.get("free_mb", 0),
                    gpu_info.get("pct", 0))
        if gpu_info.get("pct", 0) > 90:
            logger.warning("[main] VRAM usage above 90%% — models may not all fit")

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
    # Stage 2: Qwen2.5 sweep (500 → 50)
    # GPU mode: runs immediately after Stage 1 (~3 min on T4 vs. 16 min on CPU).
    # No longer waits until 5:00 AM — refinement loop covers the extra analysis time.
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("[main] ── STAGE 2: Qwen sweep (GPU — running immediately) ──────")
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
    # Stage 3: All 4 SLMs deep dive
    # GPU mode: ~10 min for 50 tickers × 4 models on T4. Runs immediately.
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("[main] ── STAGE 3: 4-model deep dive (GPU — running immediately) ─")
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
    # Stage 4: Consensus voting — runs immediately after Stage 3
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("[main] ── STAGE 4: Consensus voting ────────────────────────────")
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
    # Stage 5: Select initial top 5 — entry/target/stop computed here.
    # These are PRELIMINARY picks. Stage 9 will refine them until 7:55 AM.
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("[main] ── STAGE 5: Initial conviction selection ─────────────────")
    upsert_run_status(date_str, "STAGE5_CONVICTION", "running")

    picks = []
    try:
        if consensus:
            picks = stage5_conviction.run(consensus)
        else:
            logger.warning("[main] No consensus picks — Stage 5 skipped")

        logger.info("[main] Stage 5 complete: %d initial picks", len(picks))
        upsert_run_status(date_str, "STAGE5_CONVICTION", "completed", picks_count=len(picks))

        # Persist preliminary picks to DB (will be overwritten by refined picks)
        if picks:
            save_picks(date_str, picks)
    except Exception as e:
        logger.exception("[main] Stage 5 failed: %s", e)
        upsert_run_status(date_str, "STAGE5_CONVICTION", "failed", error_msg=str(e))

    # ─────────────────────────────────────────────────────────────────────────
    # Stage 9: Pre-Open Refinement Loop (~4:20 AM → 7:50 AM ET)
    # Re-scores the top 5 every 20 min with fresh premarket data + all 4 models.
    # Blocks until 7:50 AM ET, then returns final refined picks.
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("[main] ── STAGE 9: Pre-open refinement loop (until 7:50 AM ET) ──")
    upsert_run_status(date_str, "STAGE9_REFINE", "running")

    try:
        stage9_refine._SKIP_WAITS = _SKIP_WAITS  # propagate testing flag
        picks = stage9_refine.run(picks, model_scores, skip_waits=_SKIP_WAITS)
        logger.info("[main] Stage 9 complete: %d refined picks", len(picks))
        upsert_run_status(date_str, "STAGE9_REFINE", "completed", picks_count=len(picks))

        # Overwrite DB with final refined picks
        if picks:
            save_picks(date_str, picks)
    except Exception as e:
        logger.exception("[main] Stage 9 failed: %s", e)
        upsert_run_status(date_str, "STAGE9_REFINE", "failed", error_msg=str(e))
        # picks retains Stage 5 values — pipeline continues with initial picks

    # ─────────────────────────────────────────────────────────────────────────
    # 7:55 AM — Stage 6: Generate PDF with final refined scores
    # (Stage 9 exits at ~7:50 AM so this fires right on time)
    # ─────────────────────────────────────────────────────────────────────────
    _wait_until_et(7, 55, "STAGE6_REPORT")
    logger.info("[main] ── STAGE 6: PDF report generation (7:55 AM ET) ──────────")
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
    # Stage 7: Pushover notification — fires immediately after PDF (~7:55 AM ET)
    # This is what hits your phone before the 9:30 AM open.
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("[main] ── STAGE 7: Pushover notification (~7:55 AM ET) ──────────")
    upsert_run_status(date_str, "STAGE7_NOTIFY", "running")

    try:
        ok = stage7_notify.run(picks, report_path or "")
        status = "completed" if ok else "failed"
        upsert_run_status(date_str, "STAGE7_NOTIFY", status)
    except Exception as e:
        logger.exception("[main] Stage 7 failed: %s", e)
        upsert_run_status(date_str, "STAGE7_NOTIFY", "failed", error_msg=str(e))

    # ─────────────────────────────────────────────────────────────────────────
    # 9:20 AM — Stage 8: Pre-open sanity check (just before 9:30 AM market open)
    # Validates picks against live prices — cancels any that drifted too far.
    # ─────────────────────────────────────────────────────────────────────────
    _wait_until_et(9, 20, "STAGE8_SANITY")
    logger.info("[main] ── STAGE 8: Pre-open sanity check (9:20 AM ET) ──────────")
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
    """Unload all models and return VRAM + GPU to the Axiom scanner."""
    try:
        unload_models()
    except Exception as e:
        logger.warning("[main] Model unload error: %s", e)

    gpu_info = get_gpu_memory_info()
    if gpu_info:
        logger.info("[main] VRAM after unload: %dMB used / %dMB free (%.0f%%)",
                    gpu_info.get("used_mb", 0), gpu_info.get("free_mb", 0),
                    gpu_info.get("pct", 0))


def _import_base_url() -> str:
    try:
        from .config import OLLAMA_BASE_URL
        return OLLAMA_BASE_URL
    except Exception:
        return "http://localhost:11434"


if __name__ == "__main__":
    run_pipeline()
