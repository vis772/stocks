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
You are Axiom, an AI assistant running inside a small-cap stock scanner terminal.
The admin talks to you via Telegram on their phone.

You have direct access to the database, Docker containers, logs, and the filesystem.
Use your tools to get real data before answering — never guess stats or signal counts.

Tone: direct, concise, practical. This is a mobile screen.
Format: plain sentences or short bullet lists. No markdown bold/italics — Telegram uses HTML.
Numbers: always specific (dollar amounts, percentages, counts). Round to 2 decimal places.
When showing logs, trim to the most relevant lines and summarise the pattern.
When showing signals or convictions, lead with the most actionable info.

If asked to restart, edit, or modify something — do it (the admin authenticated with PIN).
If a query returns nothing useful, say so clearly and suggest what to check next.\
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
        "description": "Run a shell command on the server and return its output.",
        "input_schema": {
            "type": "object",
            "properties": {
                "cmd": {"type": "string", "description": "Shell command to run"},
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

    MAX_TURNS = 6   # prevent runaway tool loops
    for _ in range(MAX_TURNS):
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
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
