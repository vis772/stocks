# prediction_engine/config.py
# All environment variables, model names, and pipeline constants.
# Edit this file to tune thresholds, model names, or timing.

import os

# ─── External Services ────────────────────────────────────────────────────────
DATABASE_URL      = os.environ.get("DATABASE_URL", "")
FINNHUB_API_KEY   = os.environ.get("FINNHUB_API_KEY", "")
TIINGO_API_KEY    = os.environ.get("TIINGO_API_KEY", "")
PUSHOVER_USER     = os.environ.get("PUSHOVER_USER_KEY", "")
PUSHOVER_TOKEN    = os.environ.get("PUSHOVER_API_TOKEN", "")

# ─── Ollama ───────────────────────────────────────────────────────────────────
# Running on t3.xlarge (4 vCPU, 16GB RAM, NO GPU).
# Ollama runs CPU-only — inference is ~12-30s per call for 1.5B models.
# All timeouts and batch sizes are tuned for CPU throughput.
OLLAMA_BASE_URL   = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_TIMEOUT    = int(os.environ.get("OLLAMA_TIMEOUT", "300"))   # 5 min — CPU inference is slow

# SLM names — must match what's installed via `ollama pull`
MODEL_QWEN        = "qwen2.5:1.5b"    # Speed sweep + momentum confirmation
MODEL_PHI         = "phi4-mini"        # Technical confluence + SEC/insider analysis
MODEL_GEMMA       = "gemma3:1b"        # Pattern recognition + price action
MODEL_SMOL        = "smollm2:1.7b"    # Sentiment + news catalyst scoring
ALL_MODELS        = [MODEL_QWEN, MODEL_PHI, MODEL_GEMMA, MODEL_SMOL]

# CPU-tuned inference options: smaller context = faster throughput.
# num_threads tells Ollama to use all 4 vCPUs.
MODEL_OPTIONS = {
    MODEL_QWEN:  {"temperature": 0.05, "num_ctx": 512,  "num_predict": 200, "num_thread": 4},
    MODEL_PHI:   {"temperature": 0.05, "num_ctx": 1024, "num_predict": 300, "num_thread": 4},
    MODEL_GEMMA: {"temperature": 0.05, "num_ctx": 512,  "num_predict": 200, "num_thread": 4},
    MODEL_SMOL:  {"temperature": 0.05, "num_ctx": 512,  "num_predict": 200, "num_thread": 4},
}

# ─── Stage 1: Universe Fetch & Basic Filters ──────────────────────────────────
FILTER_MIN_PRICE        = 1.00
FILTER_MAX_PRICE        = 500.00
FILTER_MIN_AVG_VOLUME   = 300_000
FILTER_MIN_FLOAT        = 5_000_000      # 5M shares
FILTER_MIN_MARKET_CAP   = 10_000_000     # $10M
FILTER_MIN_GAP_PCT      = 2.0            # Premarket gap ≥ 2%
FILTER_MAX_GAP_PCT      = 60.0           # Cap — above this likely halted/manipulated
FETCH_BATCH_SIZE        = 100            # Tickers per yfinance batch download
FETCH_MAX_WORKERS       = 20             # ThreadPoolExecutor workers for Stage 1
STAGE1_MAX_CANDIDATES   = 500

# ─── Stage 2: Qwen Sweep ─────────────────────────────────────────────────────
# CPU timing math (t3.xlarge): Qwen 1.5B ~15s/call.
# Batching 8 tickers per call → 500/8 = 63 calls × 15s = ~16 minutes. Fits in 1-hour window.
SWEEP_TOP_N             = 50             # Qwen reduces 500 → 50
SWEEP_MIN_SCORE         = 40             # Qwen must score ≥ 40 to advance
SWEEP_BATCH_SIZE        = 8             # Tickers per batched prompt (CPU efficiency)

# ─── Stage 3: Deep Dive ──────────────────────────────────────────────────────
# CPU mode: run models SEQUENTIALLY, not in parallel.
# Ollama CPU is single-threaded — parallel threads just queue behind each other.
# Sequential per-model keeps each SLM hot in RAM for its full 50-stock batch.
# Timing: 4 models × 50 stocks × 25s avg = ~83 minutes. Fits in 6AM-8AM window.
DEEPDIVE_MAX_WORKERS    = 1              # Sequential on CPU (parallel = no benefit)
DEEPDIVE_RETRIES        = 2             # Retry failed Ollama calls

# ─── Stage 4: Consensus Voting ───────────────────────────────────────────────
MIN_MODEL_AGREEMENT     = 3             # Minimum models that must agree (≥ score threshold)
MODEL_AGREE_THRESHOLD   = 55            # A model "agrees" if its score ≥ 55
TOP_N_PICKS             = 5

# ─── Conviction Tiers ─────────────────────────────────────────────────────────
TIER_EXTREME    = 90
TIER_VERY_HIGH  = 80
TIER_HIGH       = 70

def conviction_tier(score: float) -> str:
    if score >= TIER_EXTREME:
        return "EXTREME"
    if score >= TIER_VERY_HIGH:
        return "VERY HIGH"
    if score >= TIER_HIGH:
        return "HIGH"
    return "MODERATE"

# ─── Risk Management Defaults ─────────────────────────────────────────────────
DEFAULT_STOP_PCT   = 0.04    # 4% stop from entry
DEFAULT_TARGET_PCT = 0.08    # 8% target from entry (2R)
ATR_MULTIPLIER     = 1.5     # Stop = entry - 1.5 × ATR14 (overrides default if ATR available)

# ─── Output ───────────────────────────────────────────────────────────────────
REPORT_DIR         = os.environ.get("PE_REPORT_DIR", "/tmp/axiom_pe_reports")

# ─── Resource Management ──────────────────────────────────────────────────────
CPU_YIELD_THRESHOLD = 85.0   # If Axiom scanner CPU > 85%, log contention and yield
CPU_POLL_INTERVAL   = 10     # Seconds between CPU polls
