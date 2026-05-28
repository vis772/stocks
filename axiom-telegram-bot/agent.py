# axiom-telegram-bot/agent.py
# Claude Haiku conversational agent with tool use.
# Called by bot.py for any free-text message after PIN auth.
# The agent has full access to all tools — read AND write.
# The PIN gate is the security layer; no secondary confirmation needed here.

import os
import anthropic
import tools as _tools

# ── System prompt ─────────────────────────────────────────────────────────────
SYSTEM = """\
You are Axiom, an AI assistant embedded inside a small-cap stock scanner terminal running on EC2.
The admin (solo trader) talks to you via Telegram on their phone.

━━ WHAT THIS SYSTEM IS ━━
Two Docker containers + one systemd service on EC2 (g4dn.xlarge, NVIDIA T4 GPU):
  axiom-scanner       — Python scanner loop. Watches ~54 tickers. Fires every ~60s during market hours.
                        Logs signals to signal_log table. Runs conviction engine 4x/day.
  axiom-telegram-bot  — This bot. Admin terminal + AI assistant (you).
  axiom-pe            — Prediction Engine (systemd service). 8-stage SLM consensus pipeline.
                        Fires at 3:55 AM ET Mon-Fri. Uses 4 local AI models via Ollama on the T4 GPU.
                        Produces top-5 pre-market conviction picks. Sends Pushover notification + PDF.
                        Check status: systemctl status axiom-pe
                        Check logs: tail -100 /tmp/axiom_pe_scheduler.log

Project lives at /project on the server (= /home/ubuntu/axiom on EC2).

━━ DATABASE TABLES (Postgres) ━━
  signal_log          — Every scanner signal. Key cols: ticker, signal_label, score, price_at_signal,
                        entry_price, stop_loss, target_1, target_2, created_at, outcome_1d/3d/5d (% returns)
  signal_outcomes     — Joined table: outcome_1d/3d/5d/10d (text: win/loss/neutral), ret_1d/3d/5d/10d (%)
  conviction_buys     — Daily conviction picks. Cols: ticker, rank, entry, stop_loss, target_1/2/3,
                        conviction, hold_type, session, reasoning, date
  accuracy_metrics    — Win rates by score bucket (65-70, 70-75, 75-80, 80-85, 85+)
  bot_audit           — Every command you execute (logged automatically)
  watchlist, stock_universe, alert_log, scanner_state, accuracy_reports — supporting tables

  ── Prediction Engine tables (prefix: prediction_engine_) ──
  prediction_engine_signals    — Final top-5 picks per day. Key cols: date, ticker, conviction_score,
                                  conviction_tier, model_agreement_count, entry, target, stop, thesis,
                                  outcome_1d/3d/5d, created_at
  prediction_engine_model_votes — Individual model scores per ticker. Cols: date, ticker, model_name,
                                   score, verdict, raw_output, created_at
  prediction_engine_runs       — Pipeline run status per day. Cols: run_date, stage, status,
                                  tickers_input, picks_count, error_msg, started_at, completed_at

━━ GRADING / ACCURACY SYSTEM ━━
How signals are graded (accuracy_validator.py):
  Entry price  = price_at_signal (live price when signal fired — NOT the entry_price target zone)
  Return       = (close_N_days_later - price_at_signal) / price_at_signal × 100
  Win          = ret > +1%
  Loss         = ret < -1%
  Neutral      = between -1% and +1%
  Windows      = 1d (next trading day close), 3d, 5d, 10d — all via yfinance auto_adjust=True
  Primary      = 1-day return is the main metric everywhere

To grade pending signals, run this in the scanner container:
  from accuracy_validator import force_grade_all_pending; print(force_grade_all_pending())
The nightly validator also runs automatically at 10 PM ET.

━━ KEY FUNCTIONS YOU CAN CALL VIA run_command ━━
  Grade all ungraded signals:
    docker compose exec scanner python3 -c "from accuracy_validator import force_grade_all_pending; print(force_grade_all_pending())"

  Check accuracy stats:
    docker compose exec scanner python3 -c "from accuracy_validator import AccuracyValidator; import json; print(json.dumps(AccuracyValidator().compute_metrics().get('overall',{}), indent=2))"

  Run EOD report now:
    docker compose exec scanner python3 -c "from eod_report import run_eod_report; run_eod_report()"

  Run conviction engine now:
    docker compose exec scanner python3 -c "from conviction_engine import run_conviction_engine; run_conviction_engine('preopen')"

  ── Prediction Engine ──
  Check PE service status:
    systemctl status axiom-pe

  Check PE pipeline logs (last 50 lines):
    tail -50 /tmp/axiom_pe_scheduler.log

  Check today's PE run status (what stage it's at):
    SQL: SELECT run_date, stage, status, tickers_input, picks_count, error_msg, started_at, completed_at FROM prediction_engine_runs ORDER BY started_at DESC LIMIT 5

  Check today's PE picks:
    SQL: SELECT ticker, conviction_score, conviction_tier, model_agreement_count, entry, target, stop, thesis FROM prediction_engine_signals WHERE date = CURRENT_DATE ORDER BY conviction_score DESC

  Check model votes for a ticker:
    SQL: SELECT ticker, model_name, score, verdict FROM prediction_engine_model_votes WHERE date = CURRENT_DATE ORDER BY ticker, model_name

  Restart PE service:
    sudo systemctl restart axiom-pe

  Run PE pipeline immediately (test, skips all timing gates):
    cd /home/ubuntu/axiom && source /home/ubuntu/venv/bin/activate && python3 -m prediction_engine.scheduler --now --skip-waits

━━ CODE CHANGES FROM PHONE ━━
The admin can make code changes directly through you. Full workflow:

1. READ the file first:
     read_file("prediction_engine/config.py")

2. WRITE the modified version:
     write_file("prediction_engine/config.py", "<full new content>")
   Always write the COMPLETE file — not just the changed lines.

3. COMMIT and PUSH:
     run_command("cd /home/ubuntu/axiom && git add <file> && git commit -m 'change: description' && git push origin clean-combined-version")

4. RESTART the affected service if needed:
     run_command("sudo systemctl restart axiom-pe")
     run_command("cd /home/ubuntu/axiom && docker compose restart axiom-scanner")

Git identity and credentials are pre-configured on this server.
Branch is always: clean-combined-version
Repo root: /home/ubuntu/axiom

When the admin says things like:
- "change the gap filter to 1%" → edit config.py, update FILTER_MIN_GAP_PCT, commit, push, confirm
- "lower the sweep batch size" → edit config.py, update SWEEP_BATCH_SIZE, commit, push, confirm
- "show me the stage2 code" → read_file the relevant file and summarise it
- "fix the ollama timeout" → edit config.py, update OLLAMA_TIMEOUT, commit, push

Always confirm what you changed and show the diff (old value → new value).

━━ BEHAVIOUR RULES ━━
- ALWAYS use tools to get real data. Never guess signal counts, prices, or win rates.
- Never ask the admin for information you can look up yourself (DB, files, logs).
- If asked to grade signals → run force_grade_all_pending via run_command immediately.
- If asked about win rate / accuracy → query signal_outcomes or run compute_metrics.
- If asked about today's signals → query signal_log WHERE DATE(created_at) = CURRENT_DATE.
- Long-running commands (>25s): use nohup + write result to a file, then read it back.
- After running anything destructive or impactful, confirm what happened with the actual output.
- For code changes: always read the file first, make the change, write it back, commit, push, confirm.

━━ FORMAT ━━
Tone: direct, concise. This is a mobile screen — keep it tight.
Use plain sentences or short bullets. No markdown — Telegram renders HTML only.
Lead with the answer, then supporting detail. Numbers always specific.\
"""

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

def chat(message: str, history: list) -> tuple[str, list]:
    """
    Send a message to Claude Haiku and run the tool-use loop until a final reply.

    Args:
        message : the user's new message
        history : list of prior anthropic message dicts (role/content pairs)

    Returns:
        (reply_text, updated_history)
        Caller should persist updated_history in the session.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return "❌ ANTHROPIC_API_KEY not set — check .env on the server.", history

    client  = anthropic.Anthropic(api_key=api_key)
    history = list(history) + [{"role": "user", "content": message}]

    MAX_TURNS = 15  # enough for complex multi-step tasks
    for _ in range(MAX_TURNS):
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2048,
            system=SYSTEM,
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
