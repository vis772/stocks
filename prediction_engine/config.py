# prediction_engine/config.py
# All environment variables, model names, and pipeline constants.
# Edit this file to tune thresholds, model names, or timing.

import os

# ─── External Services ────────────────────────────────────────────────────────
DATABASE_URL      = os.environ.get("DATABASE_URL", "")
FINNHUB_API_KEY   = os.environ.get("FINNHUB_API_KEY", "")
TIINGO_API_KEY    = os.environ.get("TIINGO_API_KEY", "")
PUSHOVER_USER     = os.environ.get("PE_PUSHOVER_USER_KEY",  os.environ.get("PUSHOVER_USER_KEY",  ""))
PUSHOVER_TOKEN    = os.environ.get("PE_PUSHOVER_API_TOKEN", os.environ.get("PUSHOVER_API_TOKEN", ""))

# ─── Ollama ───────────────────────────────────────────────────────────────────
# Running on g4dn.xlarge (4 vCPU, 16GB RAM, NVIDIA T4 16GB VRAM).
# Ollama uses GPU — inference is ~2-5s per call for 1.5B models (~6-10x faster than CPU).
# All 4 models fit in T4 VRAM simultaneously (~10-12GB total).
OLLAMA_BASE_URL   = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_TIMEOUT    = int(os.environ.get("OLLAMA_TIMEOUT", "60"))    # 60s — GPU inference is fast

# SLM names — must match what's installed via `ollama pull`
MODEL_QWEN        = "qwen2.5:1.5b"    # Speed sweep + momentum confirmation
MODEL_PHI         = "phi4-mini"        # Technical confluence + SEC/insider analysis
MODEL_GEMMA       = "gemma3:1b"        # Pattern recognition + price action
MODEL_SMOL        = "smollm2:1.7b"    # Sentiment + news catalyst scoring
ALL_MODELS        = [MODEL_QWEN, MODEL_PHI, MODEL_GEMMA, MODEL_SMOL]

# GPU-tuned inference options: larger context now affordable with T4 VRAM.
# No num_thread needed — GPU handles parallelism internally.
MODEL_OPTIONS = {
    MODEL_QWEN:  {"temperature": 0.05, "num_ctx": 2048, "num_predict": 200},
    MODEL_PHI:   {"temperature": 0.05, "num_ctx": 4096, "num_predict": 300},
    MODEL_GEMMA: {"temperature": 0.05, "num_ctx": 2048, "num_predict": 200},
    MODEL_SMOL:  {"temperature": 0.05, "num_ctx": 2048, "num_predict": 200},
}

# ─── Stage 1: Universe Fetch & Basic Filters ──────────────────────────────────
FILTER_MIN_PRICE        = 1.00
FILTER_MAX_PRICE        = 500.00
FILTER_MIN_AVG_VOLUME   = 300_000
FILTER_MIN_FLOAT        = 5_000_000      # 5M shares
FILTER_MIN_MARKET_CAP   = 10_000_000     # $10M
FILTER_MIN_GAP_PCT      = 0.5            # Premarket gap ≥ 0.5% (2% was too strict at 4 AM ET)
FILTER_MAX_GAP_PCT      = 60.0           # Cap — above this likely halted/manipulated
FETCH_BATCH_SIZE        = 100            # Tickers per yfinance batch download
FETCH_MAX_WORKERS       = 20             # ThreadPoolExecutor workers for Stage 1
STAGE1_MAX_CANDIDATES   = 500

# ─── Stage 2: Qwen Sweep ─────────────────────────────────────────────────────
# GPU timing math (g4dn.xlarge T4): Qwen 1.5B ~3s/call.
# Batching 8 tickers per call → 500/8 = 63 calls × 3s = ~3 minutes. Well within 1-hour window.
SWEEP_TOP_N             = 50             # Qwen reduces 500 → 50
SWEEP_MIN_SCORE         = 40             # Qwen must score ≥ 40 to advance
SWEEP_BATCH_SIZE        = 8              # Tickers per batched prompt

# ─── Stage 3: Deep Dive ──────────────────────────────────────────────────────
# GPU mode: Ollama serializes GPU calls, so sequential per-model is still optimal.
# All 4 models loaded in T4 VRAM simultaneously — no swap overhead.
# Timing: 4 models × 50 stocks × 4s avg = ~13 minutes. Well within 6AM-8AM window.
DEEPDIVE_MAX_WORKERS    = 1              # Ollama serializes GPU — sequential is optimal
DEEPDIVE_RETRIES        = 2              # Retry failed Ollama calls

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
