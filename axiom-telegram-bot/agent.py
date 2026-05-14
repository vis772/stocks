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
)

logger = logging.getLogger(__name__)

# Async client — non-blocking, won't freeze the event loop
client = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

SYSTEM_PROMPT = """You are the Axiom Terminal control agent. You have full agentic control over a small-cap stock scanner system. You help the user query data, understand scanner performance, and make controlled changes to the system.

SYSTEM OVERVIEW:
- Service 1: Streamlit dashboard (read-only UI)
- Service 2: Background scanner loop (runs every 30 min during market hours)
- Service 3: You (this bot)
- Database: PostgreSQL with tables: signal_log, signal_outcomes, watchlist, paper_trades, scanner_config

HARD LOCKS — you must REFUSE these absolutely, no exceptions:
- Never delete or modify rows in signal_log or signal_outcomes (this protects the accuracy test)
- Never change scoring weights while an active accuracy test checkpoint has not been reached yet (check signal count first)
- Never wipe or truncate any table
- Never expose database credentials or API keys

SOFT LOCKS — require user to type "confirm" before executing:
- Updating scoring component weights
- Adjusting score thresholds (buy/sell cutoffs)
- Adding or removing tickers from the watchlist
- Restarting the scanner loop (Service 2)
- Any write operation to the database

ALWAYS ALLOWED — no confirmation needed:
- Reading any data from the database
- Explaining scanner behavior or logic
- Summarizing signal performance
- Suggesting changes without applying them
- Generating on-demand accuracy summaries

FORMATTING RULES:
- Use plain text, no markdown (Telegram HTML mode only)
- Use <b>bold</b> for important values
- Use newlines to separate sections
- Keep responses concise — this is a phone interface
- For tables, use monospace with <code>text</code>
- Numbers: always include units (%, pts, $)

ACCURACY TEST PROTECTION:
Before any write operation, check the current signal count. If under 600 signals (final checkpoint not reached), warn the user that an accuracy test is in progress and changes may invalidate results. Still allow them to confirm if they want.

Always think step by step. If unsure what the user wants, ask a clarifying question rather than guessing."""

TOOLS = [
    {
        "name": "query_database",
        "description": "Run a read-only SQL query against the Axiom Terminal PostgreSQL database. Use for any data lookup — signals, outcomes, watchlist, config, paper trades.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The SQL SELECT query to run. Must be read-only (SELECT only)."
                },
                "description": {
                    "type": "string",
                    "description": "Plain English description of what this query does."
                }
            },
            "required": ["sql", "description"]
        }
    },
    {
        "name": "get_scanner_status",
        "description": "Check if the scanner loop (Service 2) is active, when it last ran, and how many signals it has logged today.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "get_accuracy_summary",
        "description": "Get the current accuracy test summary — signal count, win rates by signal type, checkpoint progress, and component correlation.",
        "input_schema": {
            "type": "object",
            "properties": {
                "window": {
                    "type": "string",
                    "enum": ["1hr", "1day", "5day", "15day"],
                    "description": "Which outcome window to summarize."
                }
            },
            "required": ["window"]
        }
    },
    {
        "name": "get_signal_log",
        "description": "Get recent signals from the signal log with optional filters.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Number of signals to return (default 10, max 50)."
                },
                "signal_label": {
                    "type": "string",
                    "description": "Filter by signal label (e.g. 'Strong Buy', 'Buy', 'Short', 'Strong Short')."
                },
                "ticker": {
                    "type": "string",
                    "description": "Filter by ticker symbol."
                }
            },
            "required": []
        }
    },
    {
        "name": "update_weights",
        "description": "Update the scoring component weights in scanner_config. REQUIRES USER CONFIRMATION. Will warn if accuracy test is in progress.",
        "input_schema": {
            "type": "object",
            "properties": {
                "weights": {
                    "type": "object",
                    "description": "Dict of component name to new weight value. Components: technical, catalyst, fundamental, risk, sentiment. Must sum to 100.",
                    "properties": {
                        "technical": {"type": "number"},
                        "catalyst": {"type": "number"},
                        "fundamental": {"type": "number"},
                        "risk": {"type": "number"},
                        "sentiment": {"type": "number"}
                    }
                }
            },
            "required": ["weights"]
        }
    },
    {
        "name": "adjust_thresholds",
        "description": "Adjust score thresholds (e.g. strong buy cutoff, buy cutoff, short cutoff). REQUIRES USER CONFIRMATION.",
        "input_schema": {
            "type": "object",
            "properties": {
                "thresholds": {
                    "type": "object",
                    "description": "Dict of threshold name to new value.",
                    "properties": {
                        "strong_buy": {"type": "number"},
                        "buy": {"type": "number"},
                        "short": {"type": "number"},
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
                "action": {
                    "type": "string",
                    "enum": ["add", "remove"],
                    "description": "Whether to add or remove tickers."
                },
                "tickers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of ticker symbols to add or remove."
                }
            },
            "required": ["action", "tickers"]
        }
    },
    {
        "name": "restart_scanner",
        "description": "Send a restart signal to the scanner loop (Service 2). REQUIRES USER CONFIRMATION. Only use if scanner appears stuck.",
        "input_schema": {
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": "Reason for restarting the scanner."
                }
            },
            "required": ["reason"]
        }
    }
]

# Tools that require confirmation before executing
CONFIRMATION_REQUIRED = {"update_weights", "adjust_thresholds", "modify_watchlist", "restart_scanner"}


async def run_agent(history: list, user_id: int) -> tuple[str, list, dict | None]:
    """
    Run one turn of the Claude agent.
    Returns: (response_text, updated_history, confirmation_needed_or_None)
    """
    messages = history.copy()

    # Agentic loop — Claude may call multiple tools
    for _ in range(10):  # Max 10 tool calls per turn
        # Async call — doesn't block the event loop
        response = await client.messages.create(
            model="claude-3-5-haiku-latest",
            max_tokens=1000,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages
        )

        # Append assistant response to history
        messages.append({"role": "assistant", "content": response.content})

        # If Claude is done (no tool calls)
        if response.stop_reason == "end_turn":
            text = _extract_text(response.content)
            return text, messages, None

        # Process tool calls
        tool_results = []
        confirmation_needed = None

        for block in response.content:
            if block.type != "tool_use":
                continue

            tool_name = block.name
            tool_input = block.input

            logger.info(f"Tool call: {tool_name} — {tool_input}")

            # Check if this tool needs confirmation
            if tool_name in CONFIRMATION_REQUIRED:
                description = _describe_action(tool_name, tool_input)
                confirmation_needed = {
                    "action": tool_name,
                    "params": tool_input,
                    "description": description,
                    "tool_use_id": block.id
                }
                text = _extract_text(response.content)
                if not text:
                    text = f"I want to: {description}"
                return text, messages, confirmation_needed

            # Execute read-only tools immediately
            result = await _execute_tool(tool_name, tool_input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": json.dumps(result, default=str)
            })

        if tool_results:
            messages.append({"role": "user", "content": tool_results})

    return "I hit the tool call limit. Please try a simpler request.", messages, None


async def _execute_tool(name: str, params: dict) -> dict:
    """Execute a tool and return the result."""
    import asyncio
    try:
        # Run sync tool functions in a thread so they don't block the event loop
        loop = asyncio.get_event_loop()
        if name == "query_database":
            return await loop.run_in_executor(None, lambda: query_database(params["sql"], params.get("description", "")))
        elif name == "get_scanner_status":
            return await loop.run_in_executor(None, get_scanner_status)
        elif name == "get_accuracy_summary":
            return await loop.run_in_executor(None, lambda: get_accuracy_summary(params.get("window", "5day")))
        elif name == "get_signal_log":
            return await loop.run_in_executor(None, lambda: get_signal_log(
                limit=params.get("limit", 10),
                signal_label=params.get("signal_label"),
                ticker=params.get("ticker")
            ))
        elif name == "update_weights":
            return await loop.run_in_executor(None, lambda: update_weights(params["weights"]))
        elif name == "adjust_thresholds":
            return await loop.run_in_executor(None, lambda: adjust_thresholds(params["thresholds"]))
        elif name == "modify_watchlist":
            return await loop.run_in_executor(None, lambda: modify_watchlist(params["action"], params["tickers"]))
        elif name == "restart_scanner":
            return await loop.run_in_executor(None, lambda: restart_scanner(params["reason"]))
        else:
            return {"error": f"Unknown tool: {name}"}
    except Exception as e:
        logger.error(f"Tool error [{name}]: {e}", exc_info=True)
        return {"error": str(e)}


def _extract_text(content: list) -> str:
    """Extract text blocks from Claude response content."""
    parts = []
    for block in content:
        if hasattr(block, "type") and block.type == "text":
            parts.append(block.text)
    return "\n".join(parts).strip()


def _describe_action(tool_name: str, params: dict) -> str:
    """Generate a human-readable description of a write action."""
    if tool_name == "update_weights":
        w = params["weights"]
        parts = [f"{k}: {v}" for k, v in w.items()]
        return f"Update scoring weights → {', '.join(parts)}"
    elif tool_name == "adjust_thresholds":
        t = params["thresholds"]
        parts = [f"{k}: {v}" for k, v in t.items()]
        return f"Adjust thresholds → {', '.join(parts)}"
    elif tool_name == "modify_watchlist":
        tickers = ", ".join(params["tickers"])
        return f"{params['action'].capitalize()} watchlist tickers: {tickers}"
    elif tool_name == "restart_scanner":
        return f"Restart scanner loop — reason: {params['reason']}"
    return f"Execute {tool_name}"
