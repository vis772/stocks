"""
Axiom Terminal — Claude Agent
Tool use layer with guardrails for agentic control.
"""

import os
import json
import logging
import anthropic
from tools import (
    query_database,
    get_scanner_status,
    update_weights,
    adjust_thresholds,
    modify_watchlist,
    restart_scanner,
    get_accuracy_summary,
    get_signal_log,
    # New tools
    get_conviction_list,
    get_portfolio,
    get_paper_trades,
    get_todays_graded_signals,
    get_regime,
    pause_scanner,
    resume_scanner,
    force_scan,
    trigger_conviction_scan,
    add_to_portfolio,
)

logger = logging.getLogger(__name__)

client = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

SYSTEM_PROMPT = """You are the Axiom Terminal control agent — a two-way agentic interface for a real-money small-cap stock scanner running on Railway. The user is a solo trader (in CST timezone, UTC-6) who uses this bot to monitor the system, review conviction picks, and make controlled changes.

SYSTEM OVERVIEW:
- Service 1: Streamlit dashboard (read-only UI at axiom-terminal.railway.app)
- Service 2: Background scanner (runs every 60s market hours, 120s premarket/AH)
- Service 3: You (Telegram bot, this session)
- Database: PostgreSQL — tables: signal_log, signal_outcomes, watchlist, paper_trades, portfolio, conviction_buys, scanner_control, factor_scores, regime_log, alert_log

TIME CONTEXT:
- User is in CST (UTC-6). Market opens 8:30 AM CST / 9:30 AM ET.
- Scanner conviction runs: 7:55 AM CST (preopen), 8:30 AM CST (market open), 3 PM CST (close), 7:30 PM CST (afterhours)
- Always translate times to CST when talking to the user.

HARD LOCKS — refuse absolutely, no exceptions:
- Never delete or modify rows in signal_log or signal_outcomes (protects accuracy test integrity)
- Never wipe or truncate any table
- Never expose API keys, database credentials, or tokens

SOFT LOCKS — require user to type "confirm" before executing:
- update_weights: changes scoring component weights
- adjust_thresholds: changes buy/sell cutoffs
- modify_watchlist: adds or removes tickers
- restart_scanner: restarts Service 2 (use only if scanner appears stuck)
- trigger_conviction_scan: runs live conviction engine (uses API credits)
- add_to_portfolio: writes a new position to the portfolio table
- pause_scanner / resume_scanner: halts or restarts scanning

ALWAYS ALLOWED (no confirmation):
- All read tools: get_conviction_list, get_portfolio, get_paper_trades,
  get_todays_graded_signals, get_regime, get_scanner_status, get_signal_log,
  get_accuracy_summary, query_database, force_scan (just sets a flag — safe)

CONVICTION FLOW:
- "show me today's picks" → get_conviction_list
- "run conviction now" → trigger_conviction_scan (requires confirm)
- "what's my morning buy list" → get_conviction_list(session="preopen") then get_conviction_list(session="market_open")
- Conviction picks have: ticker, entry, stop_loss, target_1/2, hold_type, reasoning

GRADED SIGNALS:
- "how are my signals doing today" → get_todays_graded_signals
- Returns today's signal_log rows joined with signal_outcomes (ret_1d, ret_3d, ret_5d)
- A "win" for a buy signal = ret_5d > 0

PORTFOLIO:
- "what do I hold" / "my positions" → get_portfolio
- "add AAPL 100 shares at $150" → add_to_portfolio (requires confirm)

SCANNER CONTROL:
- "force a scan" / "scan now" → force_scan (safe, just sets flag)
- "pause scanner" → pause_scanner (requires confirm)
- "resume scanner" → resume_scanner (requires confirm)
- "restart scanner" → restart_scanner (requires confirm)

FORMATTING RULES (Telegram HTML only):
- Use <b>bold</b> for tickers, prices, percentages
- Use <code>monospace</code> for numbers/scores
- Use newlines to separate picks — this is a phone interface, keep it scannable
- For conviction picks, format each as:
  <b>#1 TICKER</b> — Entry $XX.XX | Stop $XX.XX | <code>HoldType</code>
  Reason: one sentence
- Keep responses under 3000 chars. For longer data, summarize and offer to show more.
- Always translate ET times to CST for the user.
- Never use markdown (no **, no ##, no ---).

ACCURACY TEST:
- Signal count checkpoints: 150 (sanity), 350 (preliminary), 600 (final verdict)
- Before any weight/threshold change, check signal count with get_accuracy_summary
- If under 600 signals, warn that accuracy test is ongoing but allow confirm override

Think step-by-step. If the user's request is ambiguous, ask one clarifying question."""

TOOLS = [
    {
        "name": "query_database",
        "description": "Run a read-only SELECT query against the Axiom PostgreSQL database. Use for any custom data lookup.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SELECT query only."},
                "description": {"type": "string", "description": "Plain English description of what this queries."}
            },
            "required": ["sql", "description"]
        }
    },
    {
        "name": "get_scanner_status",
        "description": "Check scanner health — last signal time, today's count, current mode (MARKET/PREMARKET/AH/OVERNIGHT), pause state.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_accuracy_summary",
        "description": "Accuracy test summary — signal count, win rates by signal type, checkpoint progress (150/350/600).",
        "input_schema": {
            "type": "object",
            "properties": {
                "window": {"type": "string", "enum": ["1hr", "1day", "5day", "15day"]}
            },
            "required": ["window"]
        }
    },
    {
        "name": "get_signal_log",
        "description": "Recent signals from signal_log with optional ticker/label filters.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max signals (default 10, max 50)."},
                "signal_label": {"type": "string"},
                "ticker": {"type": "string"}
            },
            "required": []
        }
    },
    {
        "name": "get_conviction_list",
        "description": "Fetch today's conviction buy picks from conviction_buys table — entry, stop, targets, hold type, reasoning. Falls back to yesterday if today is empty.",
        "input_schema": {
            "type": "object",
            "properties": {
                "session": {"type": "string", "description": "Filter by session: preopen | market_open | close | afterhours | (empty = all today)"},
                "limit": {"type": "integer", "description": "Max picks to return (default 5)."}
            },
            "required": []
        }
    },
    {
        "name": "get_portfolio",
        "description": "Show current portfolio holdings with shares, avg cost, current price, and unrealized P&L.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_paper_trades",
        "description": "Paper trading history with P&L, win rate, and average return.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Number of trades (default 20, max 50)."},
                "status": {"type": "string", "enum": ["open", "closed", "all"], "description": "Filter by trade status."}
            },
            "required": []
        }
    },
    {
        "name": "get_todays_graded_signals",
        "description": "Today's signals with their resolved outcomes from signal_outcomes (ret_1d, ret_3d, ret_5d). Shows which picks were right/wrong.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_regime",
        "description": "Current market regime from regime_log — TRENDING_UP, TRENDING_DOWN, MEAN_REVERSION, HIGH_VOL, or NEUTRAL. Includes IWM price and ADX.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "force_scan",
        "description": "Set the force_scan flag so the scanner runs an immediate cycle within ~60 seconds. No confirmation needed — it only sets a flag.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "pause_scanner",
        "description": "Pause the scanner loop. It will stop scanning until resume_scanner is called. REQUIRES USER CONFIRMATION.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "resume_scanner",
        "description": "Resume a paused scanner. REQUIRES USER CONFIRMATION.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "trigger_conviction_scan",
        "description": "Run the conviction engine live right now and return top picks. Uses Anthropic API credits. REQUIRES USER CONFIRMATION.",
        "input_schema": {
            "type": "object",
            "properties": {
                "session": {"type": "string", "enum": ["preopen", "market", "market_open", "close", "afterhours"], "description": "Which session context to use."}
            },
            "required": []
        }
    },
    {
        "name": "add_to_portfolio",
        "description": "Add or update a holding in the portfolio table (ticker, shares, avg cost). REQUIRES USER CONFIRMATION.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "shares": {"type": "number"},
                "avg_cost": {"type": "number"},
                "notes": {"type": "string", "description": "Optional notes (e.g. 'conviction buy 2025-05-14')"}
            },
            "required": ["ticker", "shares", "avg_cost"]
        }
    },
    {
        "name": "update_weights",
        "description": "Update scoring component weights (technical/catalyst/fundamental/risk/sentiment). Must sum to 100. REQUIRES USER CONFIRMATION.",
        "input_schema": {
            "type": "object",
            "properties": {
                "weights": {
                    "type": "object",
                    "properties": {
                        "technical": {"type": "number"},
                        "catalyst":  {"type": "number"},
                        "fundamental": {"type": "number"},
                        "risk":      {"type": "number"},
                        "sentiment": {"type": "number"}
                    }
                }
            },
            "required": ["weights"]
        }
    },
    {
        "name": "adjust_thresholds",
        "description": "Adjust score thresholds (strong_buy/buy/short/strong_short cutoffs). REQUIRES USER CONFIRMATION.",
        "input_schema": {
            "type": "object",
            "properties": {
                "thresholds": {
                    "type": "object",
                    "properties": {
                        "strong_buy":   {"type": "number"},
                        "buy":          {"type": "number"},
                        "short":        {"type": "number"},
                        "strong_short": {"type": "number"}
                    }
                }
            },
            "required": ["thresholds"]
        }
    },
    {
        "name": "modify_watchlist",
        "description": "Add or remove tickers from the scanner watchlist. REQUIRES USER CONFIRMATION.",
        "input_schema": {
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["add", "remove"]},
                "tickers": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["action", "tickers"]
        }
    },
    {
        "name": "restart_scanner",
        "description": "Signal scanner (Service 2) to restart via Railway. Only use if scanner appears stuck. REQUIRES USER CONFIRMATION.",
        "input_schema": {
            "type": "object",
            "properties": {
                "reason": {"type": "string"}
            },
            "required": ["reason"]
        }
    },
]

# Tools that require "confirm" before executing
CONFIRMATION_REQUIRED = {
    "update_weights", "adjust_thresholds", "modify_watchlist",
    "restart_scanner", "trigger_conviction_scan", "add_to_portfolio",
    "pause_scanner", "resume_scanner",
}


async def run_agent(history: list, user_id: int) -> tuple[str, list, dict | None]:
    """
    Run one turn of the Claude agent.
    Returns: (response_text, updated_history, confirmation_needed_or_None)
    """
    messages = history.copy()

    for _ in range(12):  # max tool calls per turn
        response = await client.messages.create(
            model=os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001"),
            max_tokens=1200,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            return _extract_text(response.content), messages, None

        tool_results = []
        confirmation_needed = None

        for block in response.content:
            if block.type != "tool_use":
                continue

            tool_name  = block.name
            tool_input = block.input
            logger.info(f"Tool call: {tool_name} — {tool_input}")

            if tool_name in CONFIRMATION_REQUIRED:
                description = _describe_action(tool_name, tool_input)
                confirmation_needed = {
                    "action":      tool_name,
                    "params":      tool_input,
                    "description": description,
                    "tool_use_id": block.id,
                }
                text = _extract_text(response.content)
                if not text:
                    text = f"I want to: {description}"
                return text, messages, confirmation_needed

            result = await _execute_tool(tool_name, tool_input)
            tool_results.append({
                "type":        "tool_result",
                "tool_use_id": block.id,
                "content":     json.dumps(result, default=str),
            })

        if tool_results:
            messages.append({"role": "user", "content": tool_results})

    return "Hit the tool call limit — please try a simpler request.", messages, None


async def _execute_tool(name: str, params: dict) -> dict:
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        dispatch = {
            "query_database":          lambda: query_database(params["sql"], params.get("description", "")),
            "get_scanner_status":      get_scanner_status,
            "get_accuracy_summary":    lambda: get_accuracy_summary(params.get("window", "5day")),
            "get_signal_log":          lambda: get_signal_log(
                                           limit=params.get("limit", 10),
                                           signal_label=params.get("signal_label"),
                                           ticker=params.get("ticker"),
                                       ),
            "get_conviction_list":     lambda: get_conviction_list(
                                           session=params.get("session", ""),
                                           limit=params.get("limit", 5),
                                       ),
            "get_portfolio":           get_portfolio,
            "get_paper_trades":        lambda: get_paper_trades(
                                           limit=params.get("limit", 20),
                                           status=params.get("status", "all"),
                                       ),
            "get_todays_graded_signals": get_todays_graded_signals,
            "get_regime":              get_regime,
            "force_scan":              force_scan,
            "pause_scanner":           pause_scanner,
            "resume_scanner":          resume_scanner,
            "trigger_conviction_scan": lambda: trigger_conviction_scan(params.get("session", "market")),
            "add_to_portfolio":        lambda: add_to_portfolio(
                                           params["ticker"], params["shares"],
                                           params["avg_cost"], params.get("notes", ""),
                                       ),
            "update_weights":          lambda: update_weights(params["weights"]),
            "adjust_thresholds":       lambda: adjust_thresholds(params["thresholds"]),
            "modify_watchlist":        lambda: modify_watchlist(params["action"], params["tickers"]),
            "restart_scanner":         lambda: restart_scanner(params["reason"]),
        }
        fn = dispatch.get(name)
        if fn is None:
            return {"error": f"Unknown tool: {name}"}
        return await loop.run_in_executor(None, fn)
    except Exception as e:
        logger.error(f"Tool error [{name}]: {e}", exc_info=True)
        return {"error": str(e)}


def _extract_text(content: list) -> str:
    return "\n".join(
        block.text for block in content
        if hasattr(block, "type") and block.type == "text"
    ).strip()


def _describe_action(tool_name: str, params: dict) -> str:
    if tool_name == "update_weights":
        parts = [f"{k}: {v}" for k, v in params["weights"].items()]
        return f"Update scoring weights → {', '.join(parts)}"
    elif tool_name == "adjust_thresholds":
        parts = [f"{k}: {v}" for k, v in params["thresholds"].items()]
        return f"Adjust thresholds → {', '.join(parts)}"
    elif tool_name == "modify_watchlist":
        return f"{params['action'].capitalize()} watchlist: {', '.join(params['tickers'])}"
    elif tool_name == "restart_scanner":
        return f"Restart scanner — reason: {params['reason']}"
    elif tool_name == "trigger_conviction_scan":
        return f"Run conviction engine now (session={params.get('session', 'market')})"
    elif tool_name == "add_to_portfolio":
        return f"Add {params['shares']} shares of {params['ticker'].upper()} @ ${params['avg_cost']}"
    elif tool_name == "pause_scanner":
        return "Pause the scanner loop"
    elif tool_name == "resume_scanner":
        return "Resume the scanner loop"
    return f"Execute {tool_name}"
