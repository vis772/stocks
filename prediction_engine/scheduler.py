# prediction_engine/scheduler.py
# Cron orchestration for the pre-market conviction engine.
# Run this as a long-running process: python -m prediction_engine.scheduler
#
# Schedule:
#   3:55 AM ET Mon-Fri — Pipeline start (loads models, runs stages 1-8, unloads by 10 AM)
#
# The pipeline manages its own internal timing via _wait_until_et().
# This scheduler simply fires the pipeline once per trading day at the right time.

import sys
import logging
import signal
import os
from datetime import datetime, timezone, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/axiom_pe_scheduler.log", mode="a"),
    ],
)
logger = logging.getLogger("pe.scheduler")

# Load dotenv if present (local dev)
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.info("[scheduler] .env loaded")
except ImportError:
    pass


def _run_pipeline_job():
    """APScheduler job: import and run the pipeline."""
    logger.info("[scheduler] Pipeline job triggered at %s", _et_now_str())
    try:
        from prediction_engine.main import run_pipeline
        run_pipeline()
        logger.info("[scheduler] Pipeline job completed at %s", _et_now_str())
    except Exception as e:
        logger.exception("[scheduler] Pipeline job crashed: %s", e)


def _on_job_event(event):
    if event.exception:
        logger.error("[scheduler] Job %s raised an exception: %s",
                     event.job_id, event.exception)
    else:
        logger.info("[scheduler] Job %s completed successfully", event.job_id)


def _et_now_str() -> str:
    utc    = datetime.now(timezone.utc)
    offset = -4 if 3 <= utc.month <= 11 else -5
    et     = utc + timedelta(hours=offset)
    return et.strftime("%Y-%m-%d %H:%M:%S ET")


def _graceful_shutdown(signum, frame):
    logger.info("[scheduler] Received signal %d — shutting down", signum)
    sys.exit(0)


def main():
    logger.info("=" * 60)
    logger.info("[scheduler] Axiom Terminal Pre-Market Conviction Engine")
    logger.info("[scheduler] Scheduler starting at %s", _et_now_str())
    logger.info("=" * 60)

    signal.signal(signal.SIGTERM, _graceful_shutdown)
    signal.signal(signal.SIGINT,  _graceful_shutdown)

    scheduler = BlockingScheduler(timezone="America/New_York")
    scheduler.add_listener(_on_job_event, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)

    # Daily trigger: 3:55 AM ET, Mon-Fri only
    scheduler.add_job(
        _run_pipeline_job,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=3,
            minute=55,
            timezone="America/New_York",
        ),
        id="pe_daily_pipeline",
        name="Pre-Market Conviction Pipeline",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,  # Allow 5-minute late start
    )

    # Log next fire time
    jobs = scheduler.get_jobs()
    for job in jobs:
        logger.info("[scheduler] Scheduled: '%s' — next run: %s",
                    job.name, job.next_run_time)

    logger.info("[scheduler] Waiting for next 3:55 AM ET window (Mon-Fri)...")

    # Optional: run immediately if --now flag passed (for testing)
    if "--now" in sys.argv:
        logger.info("[scheduler] --now flag detected — running pipeline immediately")
        _run_pipeline_job()
        return

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[scheduler] Scheduler stopped")


if __name__ == "__main__":
    main()
