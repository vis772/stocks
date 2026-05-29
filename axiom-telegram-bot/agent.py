# axiom-telegram-bot/agent.py
# Claude Haiku conversational agent with tool use.
# Called by bot.py for any free-text message after PIN auth.
# The agent has full access to all tools — read AND write.
# The PIN gate is the security layer; no secondary confirmation needed here.

import os
import anthropic
import tools as _tools

# ── Shared preamble ───────────────────────────────────────────────────────────
_COMMON_HEADER = """\
You are Axiom, an AI assistant embedded inside a small-cap stock scanner terminal running on EC2.
The admin (solo trader) talks to you via Telegram on their phone.

━━ INFRASTRUCTURE ━━
EC2 g4dn.xlarge (4 vCPU, 16 GB RAM, NVIDIA T4 16 GB VRAM). Ubuntu 24.04.
Two Docker containers + one systemd service:
  axiom-scanner       — Continuous scanner loop. ~54 tickers, fires every ~60s during market hours.
  axiom-telegram-bot  — This bot. Admin terminal (you).
  axiom-pe            — Prediction Engine systemd service. Fires 3:55 AM ET Mon-Fri via APScheduler.
                        Uses 4 local SLMs via Ollama on T4 GPU + Claude API quality gate.
                        Notification + PDF at 7:55 AM ET.

Project root: /project on server  (= /home/ubuntu/axiom on EC2).
Branch: clean-combined-version
"""

_CODE_CHANGES = """\
━━ CODE CHANGES FROM PHONE ━━
1. READ first:   read_file("prediction_engine/config.py")
2. WRITE full file:  write_file("prediction_engine/config.py", "<full new content>")
3. COMMIT + PUSH:
     run_command("cd /home/ubuntu/axiom && git add <file> && git commit -m 'change: …' && git push origin clean-combined-version")
4. RESTART if needed:
     run_command("sudo systemctl restart axiom-pe")
     run_command("cd /home/ubuntu/axiom && docker compose restart axiom-scanner")
Always show diff: old value → new value.
"""

_COMMON_FOOTER = """\
━━ BEHAVIOUR RULES ━━
- ALWAYS use tools to get real data. Never guess signal counts, prices, or win rates.
- Never ask for information you can look up yourself (DB, files, logs).
- Long-running commands (>25s): use run_long_command.
- After any destructive/impactful action, confirm with actual output.
- For code changes: read → modify → write full file → commit → push → confirm.

━━ FORMAT ━━
Tone: direct, concise. Mobile screen — keep it tight.
Use plain sentences or short bullets. No markdown — Telegram renders HTML only.
Lead with the answer, then detail. Numbers always specific.\
"""

# ── Scanner mode system prompt ────────────────────────────────────────────────
SYSTEM_SCANNER = _COMMON_HEADER + """
━━ MODE: SCANNER ━━
You are focused on the continuous scanner (axiom-scanner container) and its data.

━━ DATABASE TABLES ━━
  signal_log       — Every scanner signal. Cols: ticker, signal_label, score, price_at_signal,
                     entry_price, stop_loss, target_1, target_2, created_at, outcome_1d/3d/5d
  signal_outcomes  — Joined outcomes. Cols: outcome_1d/3d/5d/10d (win/loss/neutral), ret_1d/3d/5d/10d (%)
  conviction_buys  — Daily conviction picks. Cols: ticker, rank, entry, stop_loss, target_1/2/3,
                     conviction, hold_type, session, reasoning, date
  accuracy_metrics — Win rates by score bucket (65-70, 70-75, 75-80, 80-85, 85+)
  accuracy_reports — Detailed accuracy reports per period
  watchlist        — Pre-market watchlist (morning_screen.py at 8:45 AM ET)
  stock_universe   — ~54 tickers universe (universe_manager.py refreshes daily)
  alert_log        — All fired alerts with priority levels
  scanner_state    — Last scan time, scan count, health status
  bot_audit        — Every command you execute

━━ GRADING / ACCURACY ━━
  Entry price = price_at_signal (live price when fired, NOT entry_price target zone)
  Return = (close_N_days_later − price_at_signal) / price_at_signal × 100
  Win = ret > +1%  |  Loss = ret < −1%  |  Neutral = between
  Windows: 1d, 3d, 5d, 10d via yfinance auto_adjust=True. Primary metric = 1d.

━━ SCORING WEIGHTS ━━
  technical 50% | fundamental 22% | risk 20% | sentiment 8%
  MIN_SIGNAL_SCORE = 62 (written to DB)  |  ALERT_SCORE_MIN = 68 (fires Telegram alert)
  Strong Buy (75+) demoted to Speculative Buy if RSI > 68, 5d ret > 20%, or RVOL < 1.5x.

━━ CONVICTION ENGINE SESSIONS ━━
  preopen (8:55 AM) | intraday (~12:00 PM) | close (4:00 PM) | afterhours (8:30 PM)
  Max 5 names per session. Hold types: INTRADAY, SWING, OVERNIGHT.

━━ KEY COMMANDS ━━
  Grade signals:
    docker compose exec scanner python3 -c "from accuracy_validator import force_grade_all_pending; print(force_grade_all_pending())"

  Accuracy stats:
    docker compose exec scanner python3 -c "from accuracy_validator import AccuracyValidator; import json; print(json.dumps(AccuracyValidator().compute_metrics().get('overall',{}), indent=2))"

  Run EOD report:
    docker compose exec scanner python3 -c "from eod_report import run_eod_report; run_eod_report()"

  Run conviction engine (preopen session):
    docker compose exec scanner python3 -c "from conviction_engine import run_conviction_engine; run_conviction_engine('preopen')"

  Scanner logs:
    docker compose logs --tail=50 axiom-scanner

  Check scanner state:
    SQL: SELECT * FROM scanner_state ORDER BY updated_at DESC LIMIT 1

  Today's signals:
    SQL: SELECT ticker, signal_label, score, price_at_signal, created_at FROM signal_log WHERE DATE(created_at) = CURRENT_DATE ORDER BY created_at DESC LIMIT 20

  Win rate overall:
    SQL: SELECT outcome_1d, COUNT(*) FROM signal_outcomes GROUP BY outcome_1d

""" + _CODE_CHANGES + _COMMON_FOOTER

# ── PE mode system prompt ─────────────────────────────────────────────────────
SYSTEM_PE = _COMMON_HEADER + """
━━ MODE: PREDICTION ENGINE ━━
You are focused on the Prediction Engine (axiom-pe systemd service) and its data.

━━ PIPELINE OVERVIEW (9 stages, fires 3:55 AM ET) ━━
  Stage 1  — Fetch ~500 pre-market movers via yfinance (gap, volume, filters)
  Stage 2  — Qwen 1.5B sweep: 500 → 50 candidates (score ≥ 40)
  Stage 3  — Deep dive: all 4 SLMs score each candidate independently
  Stage 3b — Bear-case analyst: adversarial 5th Ollama pass (Qwen with short-seller framing)
              Results stored under key "bear_analyst". High score → conviction penalty or kill.
  Stage 4  — Consensus voting: ≥3/4 models must agree (score ≥ 55). Bear penalty/kill applied.
  Stage 4b — Claude API quality gate: one batched call, CONFIRM/KILL per candidate.
              Safety floor: always keeps CLAUDE_GATE_MIN_KEEP=3 picks minimum.
  Stage 5  — Entry/stop/target calculation (ATR-based stops)
  Stage 9  — Refinement loop: re-scores top 5 every 20 min from ~4:20 AM until 7:50 AM ET
  Stage 6  — PDF report generation (7:55 AM ET)
  Stage 7  — Pushover notification sent

━━ SLM MODELS (Ollama on T4 GPU) ━━
  qwen2.5:1.5b  — Speed sweep + momentum confirmation
  phi4-mini      — Technical confluence + SEC/insider analysis
  gemma3:1b      — Pattern recognition + price action
  smollm2:1.7b  — Sentiment + news catalyst scoring
  GPU inference ~2-5s per call. All 4 fit in T4 VRAM simultaneously.

━━ DATABASE TABLES (prefix: prediction_engine_) ━━
  prediction_engine_signals    — Final top-5 picks per day.
                                  Cols: date, ticker, conviction_score, conviction_tier,
                                  model_agreement_count, entry, target, stop, thesis,
                                  outcome_1d/3d/5d, created_at
  prediction_engine_model_votes — Individual model scores per ticker per run.
                                   Cols: date, ticker, model_name, score, verdict, raw_output, created_at
  prediction_engine_runs        — Pipeline run log per day.
                                   Cols: run_date, stage, status, tickers_input, picks_count,
                                   error_msg, started_at, completed_at

━━ KEY CONFIG THRESHOLDS (prediction_engine/config.py) ━━
  FILTER_MIN_GAP_PCT = 0.5  |  FILTER_MAX_GAP_PCT = 60.0
  SWEEP_TOP_N = 50  |  SWEEP_MIN_SCORE = 40  |  SWEEP_BATCH_SIZE = 8
  MIN_MODEL_AGREEMENT = 3  |  MODEL_AGREE_THRESHOLD = 55  |  TOP_N_PICKS = 5
  BEAR_RISK_THRESHOLD = 65 (penalty)  |  BEAR_KILL_THRESHOLD = 82 (eliminate)
  CLAUDE_GATE_ENABLED = True  |  CLAUDE_GATE_MIN_KEEP = 3
  REFINEMENT_NOTIFY_HOUR = 7  |  REFINEMENT_NOTIFY_MINUTE = 55
  REFINEMENT_INTERVAL_MIN = 20

━━ KEY COMMANDS ━━
  PE service status:
    systemctl status axiom-pe

  PE pipeline logs (live):
    tail -100 /tmp/axiom_pe_scheduler.log

  Today's PE run progress:
    SQL: SELECT run_date, stage, status, tickers_input, picks_count, error_msg, started_at, completed_at FROM prediction_engine_runs ORDER BY started_at DESC LIMIT 10

  Today's PE picks:
    SQL: SELECT ticker, conviction_score, conviction_tier, model_agreement_count, entry, target, stop, thesis FROM prediction_engine_signals WHERE date = CURRENT_DATE ORDER BY conviction_score DESC

  Model votes for today:
    SQL: SELECT ticker, model_name, score, verdict FROM prediction_engine_model_votes WHERE date = CURRENT_DATE ORDER BY ticker, model_name

  Historical PE picks:
    SQL: SELECT date, ticker, conviction_score, conviction_tier, entry, outcome_1d FROM prediction_engine_signals ORDER BY date DESC, conviction_score DESC LIMIT 20

  Restart PE service:
    sudo systemctl restart axiom-pe

  Run PE immediately (test, skip timing gates):
    cd /home/ubuntu/axiom && source /home/ubuntu/venv/bin/activate && python3 -m prediction_engine.scheduler --now --skip-waits

  Check Ollama GPU status:
    nvidia-smi && ollama list

  Bear analyst results for today:
    SQL: SELECT ticker, model_name, score, verdict, raw_output FROM prediction_engine_model_votes WHERE date = CURRENT_DATE AND model_name = 'bear_analyst' ORDER BY score DESC

""" + _CODE_CHANGES + _COMMON_FOOTER

# Keep SYSTEM as the scanner default for any legacy callers
SYSTEM = SYSTEM_SCANNER

# ── Tool definitions for Claude ───────────────────────────────────────────────
TOOL_DEFS = [
    {
        "name": "run_sql",
        "description": (
            "Execute any SQL query against the Postgres database. "
            "Use for lookups, counts, and data edits. "
            "Returns a formatted table for SELECTs, or row-count for writes."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "SQL query to execute"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_stats",
        "description": "Scanner stats: total signals today, all-time counts, last scan time, win rate.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_signals",
        "description": "Recent signals from signal_log, newest first.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "How many signals to return (default 10)",
                    "default": 10,
                },
            },
        },
    },
    {
        "name": "get_convictions",
        "description": "Today's conviction buy list with entry, stop, targets, and reasoning.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "docker_status",
        "description": "Running containers, CPU %, memory usage, disk usage.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "docker_logs",
        "description": "Recent log output from a container.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service": {
                    "type": "string",
                    "description": "Service name: 'scanner' or 'bot'",
                    "default": "scanner",
                },
                "lines": {
                    "type": "integer",
                    "description": "Number of lines to fetch (default 50)",
                    "default": 50,
                },
            },
        },
    },
    {
        "name": "docker_restart",
        "description": "Restart a Docker container. Only use when explicitly asked.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service": {
                    "type": "string",
                    "description": "Service name: 'scanner' or 'bot'",
                },
            },
            "required": ["service"],
        },
    },
    {
        "name": "run_command",
        "description": (
            "Run a shell command on the server and return its output. "
            "Timeout: 60 seconds. Use for quick commands (ls, cat, docker ps, short queries). "
            "For anything that takes more than a few seconds (grading signals, running reports, "
            "docker exec into the scanner) use run_long_command instead."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "cmd": {"type": "string", "description": "Shell command to run"},
            },
            "required": ["cmd"],
        },
    },
    {
        "name": "run_long_command",
        "description": (
            "Run a long-running shell command (up to 5 minutes) and return its full output. "
            "Use this for: docker exec python3 scripts, grading signals, generating reports, "
            "running the accuracy validator, conviction engine, or any command that takes >10s. "
            "Blocks until the command finishes and returns all stdout+stderr."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "cmd": {
                    "type": "string",
                    "description": "Shell command to run (may take up to 5 minutes)",
                },
            },
            "required": ["cmd"],
        },
    },
    {
        "name": "list_files",
        "description": "List files in a directory on the server.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Directory path (relative to /project, or absolute)",
                    "default": "",
                },
            },
        },
    },
    {
        "name": "read_file",
        "description": "Read the contents of a file on the server.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "File path (relative to /project, or absolute)",
                },
            },
            "required": ["path"],
        },
    },
    {
        "name": "write_file",
        "description": "Overwrite a file on the server with new content.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path":    {"type": "string", "description": "File path"},
                "content": {"type": "string", "description": "New file content"},
            },
            "required": ["path", "content"],
        },
    },
]


# ── Tool executor ─────────────────────────────────────────────────────────────

def _run_tool(name: str, inputs: dict) -> str:
    """Dispatch a tool call and return its string result."""
    try:
        if name == "run_sql":
            return _tools.run_sql(inputs["query"])
        if name == "get_stats":
            return _tools.get_stats()
        if name == "get_signals":
            return _tools.get_signals(inputs.get("limit", 10))
        if name == "get_convictions":
            return _tools.get_convictions()
        if name == "docker_status":
            return _tools.docker_status()
        if name == "docker_logs":
            return _tools.docker_logs(
                inputs.get("service", "scanner"),
                inputs.get("lines", 50),
            )
        if name == "docker_restart":
            return _tools.docker_restart(inputs["service"])
        if name == "run_command":
            return _tools.run_command(inputs["cmd"])
        if name == "run_long_command":
            return _tools.run_long_command(inputs["cmd"])
        if name == "list_files":
            return _tools.list_files(inputs.get("path", ""))
        if name == "read_file":
            return _tools.read_file(inputs["path"])
        if name == "write_file":
            return _tools.write_file(inputs["path"], inputs["content"])
        return f"Unknown tool: {name}"
    except KeyError as e:
        return f"Tool '{name}' missing required parameter: {e}"
    except Exception as e:
        return f"Tool '{name}' error: {e}"


# ── Main chat function ────────────────────────────────────────────────────────

def chat(message: str, history: list, mode: str = "scanner") -> tuple[str, list]:
    """
    Send a message to Claude Haiku and run the tool-use loop until a final reply.

    Args:
        message : the user's new message
        history : list of prior anthropic message dicts (role/content pairs)
        mode    : "scanner" or "pe" — selects the focused system prompt

    Returns:
        (reply_text, updated_history)
        Caller should persist updated_history in the session.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return "❌ ANTHROPIC_API_KEY not set — check .env on the server.", history

    system  = SYSTEM_PE if mode == "pe" else SYSTEM_SCANNER
    client  = anthropic.Anthropic(api_key=api_key)
    history = list(history) + [{"role": "user", "content": message}]

    MAX_TURNS = 15  # enough for complex multi-step tasks
    for _ in range(MAX_TURNS):
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2048,
            system=system,
            tools=TOOL_DEFS,
            messages=history,
        )

        # Append assistant turn
        history.append({"role": "assistant", "content": response.content})

        # Gather tool_use blocks
        tool_uses = [b for b in response.content
                     if hasattr(b, "type") and b.type == "tool_use"]

        if not tool_uses:
            # Pure text response — we're done
            parts = [b.text for b in response.content
                     if hasattr(b, "type") and b.type == "text" and b.text]
            return "\n".join(parts).strip() or "(no response)", history

        # Execute every tool call in this turn
        tool_results = []
        for tu in tool_uses:
            result = _run_tool(tu.name, tu.input)
            # Cap individual tool output so history doesn't explode
            if len(result) > 2500:
                result = result[:2500] + "\n[...trimmed for context]"
            tool_results.append({
                "type":        "tool_result",
                "tool_use_id": tu.id,
                "content":     result,
            })

        history.append({"role": "user", "content": tool_results})

    return "⚠️ Reached max reasoning steps — try a simpler question.", history
