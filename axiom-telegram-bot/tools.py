"""
Axiom Admin Bot — Tool implementations.
DB queries, shell execution, Docker management, filesystem access.
All functions return plain strings suitable for Telegram messages.
"""

import os
import subprocess
import logging
import re as _re
import shlex as _shlex
import psycopg2
import psycopg2.extras
from datetime import date, timedelta

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
PROJECT_DIR  = os.environ.get("PROJECT_DIR", "/project")

# Map short service names to Docker container names
CONTAINERS = {
    "scanner": "axiom-scanner",
    "bot":     "axiom-telegram-bot",
}

# Env var names whose values should never appear in bot output
_SECRET_ENV_KEYS = [
    "DATABASE_URL", "PUSHOVER_USER_KEY", "PUSHOVER_API_TOKEN",
    "ANTHROPIC_API_KEY", "TELEGRAM_BOT_TOKEN", "FINNHUB_API_KEY",
    "TIINGO_API_KEY", "MASSIVE_API_KEY", "SECRET_KEY",
]

def _mask_secrets(text: str) -> str:
    """Replace actual secret values with [REDACTED] in any string."""
    for key in _SECRET_ENV_KEYS:
        val = os.environ.get(key, "")
        if val and len(val) > 4:   # don't mask empty or trivially short values
            text = text.replace(val, f"[REDACTED:{key}]")
    return text


# ── DB ────────────────────────────────────────────────────────────────────────

def _conn():
    return psycopg2.connect(DATABASE_URL)


def is_write_sql(sql: str) -> bool:
    """True if the SQL is not a read-only statement."""
    first = sql.strip().lower().split()[0] if sql.strip() else ""
    return first not in ("select", "with", "explain", "show")


_DANGEROUS_SQL = [
    (_re.compile(r'\b(drop)\s+(table|database|schema|index)\b', _re.IGNORECASE),
     "DROP is not allowed through the bot"),
    (_re.compile(r'\btruncate\s+table\b', _re.IGNORECASE),
     "TRUNCATE is not allowed through the bot"),
    (_re.compile(r'\bdelete\s+from\s+\w[\w.]*\s*(?:;|\Z)', _re.IGNORECASE),
     "DELETE without a WHERE clause is not allowed"),
    (_re.compile(r'\balter\s+table\b.*\bdrop\s+column\b', _re.IGNORECASE),
     "ALTER TABLE DROP COLUMN is not allowed through the bot"),
]

def is_dangerous_sql(sql: str) -> tuple[bool, str]:
    """
    Returns (True, reason) if the SQL matches a known destructive pattern.
    Returns (False, "") if the SQL is safe to execute.
    """
    stripped = sql.strip()
    for pattern, reason in _DANGEROUS_SQL:
        if pattern.search(stripped):
            return True, reason
    return False, ""


def run_sql(sql: str) -> str:
    """
    Execute any SQL query.
    SELECTs return formatted rows (max 50).
    Writes commit and return rows-affected.
    Returns a plain string — caller wraps in <pre> for Telegram.
    """
    dangerous, reason = is_dangerous_sql(sql)
    if dangerous:
        return f"🚫 Blocked: {reason}"
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                if cur.description:
                    rows = cur.fetchmany(50)
                    if not rows:
                        return "(0 rows)"
                    cols = [d.name for d in cur.description]
                    # Column widths
                    widths = [
                        max(len(c), max((len(str(r.get(c, "") or "")) for r in rows), default=0))
                        for c in cols
                    ]
                    sep    = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
                    header = "|" + "|".join(f" {c.ljust(w)} " for c, w in zip(cols, widths)) + "|"
                    lines  = [sep, header, sep]
                    for row in rows:
                        lines.append(
                            "|" + "|".join(
                                f" {str(row.get(c) if row.get(c) is not None else 'NULL').ljust(w)} "
                                for c, w in zip(cols, widths)
                            ) + "|"
                        )
                    lines.append(sep)
                    if len(rows) == 50:
                        lines.append("(showing first 50 rows)")
                    return "\n".join(lines)
                else:
                    # Write query — context manager commits on success
                    rowcount = cur.rowcount
                    return f"OK — {rowcount} row(s) affected."
    except Exception as e:
        return f"ERROR: {e}"


# ── Shell / Docker ────────────────────────────────────────────────────────────

def _sh(cmd: str, timeout: int = 20) -> str:
    """Run a shell command; return combined stdout+stderr."""
    try:
        r = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        out = (r.stdout + r.stderr).strip()
        out = out if out else "(no output)"
        out = _mask_secrets(out)
        return out
    except subprocess.TimeoutExpired:
        return f"Timed out after {timeout}s"
    except Exception as e:
        return f"Error: {e}"


def docker_logs(service: str = "scanner", lines: int = 50) -> str:
    container = CONTAINERS.get(service.lower(), f"axiom-{service}")
    return _sh(f"docker logs --tail {lines} --timestamps {container} 2>&1")


def docker_restart(service: str) -> str:
    container = CONTAINERS.get(service.lower(), f"axiom-{service}")
    out = _sh(f"docker restart {container}", timeout=40)
    return f"Restarted {container}\n{out}"


def docker_status() -> str:
    ps     = _sh("docker ps --format 'table {{.Names}}\\t{{.Status}}\\t{{.RunningFor}}'")
    cpu    = _sh("grep 'cpu ' /proc/stat | awk '{u=$2+$4; t=$2+$4+$5} END {printf \"%.1f%%\", u/t*100}'")
    mem    = _sh("free -h | awk '/^Mem:/{print $3\"/\"$2\" (\"int($3/$2*100)\"%)\"}'")
    disk   = _sh("df -h / | awk 'NR==2{print $3\"/\"$2\" (\"$5\")\"}'")
    uptime = _sh("uptime -p")
    return (
        f"<b>Containers</b>\n<pre>{ps}</pre>\n\n"
        f"<b>CPU:</b>    {cpu}\n"
        f"<b>Memory:</b> {mem}\n"
        f"<b>Disk:</b>   {disk}\n"
        f"<b>Uptime:</b> {uptime}"
    )


_BLOCKED_COMMANDS = [
    (_re.compile(r'\brm\s+-[a-z]*r[a-z]*f\b.*(/|~|\*)', _re.IGNORECASE),
     "rm -rf on root/home/wildcards is blocked"),
    (_re.compile(r'curl\b.*\|\s*(ba)?sh', _re.IGNORECASE),
     "piping curl to shell is blocked"),
    (_re.compile(r'wget\b.*\|\s*(ba)?sh', _re.IGNORECASE),
     "piping wget to shell is blocked"),
    (_re.compile(r'\bchmod\s+777\b', _re.IGNORECASE),
     "chmod 777 is blocked"),
    (_re.compile(r'>\s*/dev/sd', _re.IGNORECASE),
     "writing to raw block devices is blocked"),
    (_re.compile(r'\bdd\b.*\bof=/dev/', _re.IGNORECASE),
     "dd to block device is blocked"),
    (_re.compile(r'\bmkfs\b', _re.IGNORECASE),
     "mkfs is blocked"),
    (_re.compile(r':\(\)\{.*\}.*;.*:', _re.IGNORECASE),
     "fork bomb pattern is blocked"),
]

def _is_blocked_cmd(cmd: str) -> tuple[bool, str]:
    """Returns (True, reason) if the command matches a blocklist pattern."""
    for pattern, reason in _BLOCKED_COMMANDS:
        if pattern.search(cmd):
            return True, reason
    return False, ""


def run_command(cmd: str) -> str:
    blocked, reason = _is_blocked_cmd(cmd)
    if blocked:
        return f"🚫 Blocked: {reason}"
    result = _sh(cmd, timeout=30)
    return _mask_secrets(result)


# ── Filesystem ────────────────────────────────────────────────────────────────

_ALLOWED_OUTSIDE_PROJECT = (
    "/var/log",
    "/tmp",
    "/proc/meminfo",
    "/proc/cpuinfo",
)

def _resolve(path: str) -> str | None:
    """
    Resolve a user-supplied path to an absolute path.
    Returns None if the path is outside the jail.

    Relative paths → PROJECT_DIR/path.
    Absolute paths are allowed only if they are under PROJECT_DIR or
    in _ALLOWED_OUTSIDE_PROJECT.
    Returns None for jailbreak attempts.
    """
    if not path or path in (".", ""):
        return PROJECT_DIR
    if os.path.isabs(path):
        resolved = os.path.normpath(path)
    else:
        resolved = os.path.normpath(os.path.join(PROJECT_DIR, path.lstrip("/")))

    # Allow if inside project dir
    if resolved.startswith(PROJECT_DIR):
        return resolved

    # Allow specific read-only system paths
    for allowed in _ALLOWED_OUTSIDE_PROJECT:
        if resolved.startswith(allowed):
            return resolved

    # Reject everything else
    return None


def list_files(path: str = "") -> str:
    resolved = _resolve(path)
    if resolved is None:
        return f"🚫 Access denied: path is outside the allowed directory."
    try:
        entries = []
        with os.scandir(resolved) as it:
            for e in sorted(it, key=lambda x: (not x.is_dir(), x.name.lower())):
                if e.name.startswith("."):
                    continue
                icon = "📁" if e.is_dir() else "📄"
                sz   = ""
                if e.is_file():
                    b = e.stat().st_size
                    sz = f"  {b // 1024}KB" if b >= 1024 else f"  {b}B"
                entries.append(f"{icon} {e.name}{sz}")
        return f"{resolved}\n" + ("\n".join(entries) if entries else "(empty)")
    except PermissionError:
        return f"Permission denied: {resolved}"
    except FileNotFoundError:
        return f"Not found: {resolved}"
    except Exception as e:
        return f"Error: {e}"


def read_file(path: str) -> str:
    resolved = _resolve(path)
    if resolved is None:
        return f"🚫 Access denied: path is outside the allowed directory."
    try:
        if os.path.isdir(resolved):
            return f"{resolved} is a directory — use /files instead."
        size = os.path.getsize(resolved)
        with open(resolved, "r", errors="replace") as f:
            content = f.read(8000)
        if size > 8000:
            content += f"\n\n[...truncated — {size:,} bytes total, showing first 8000]"
        return content
    except FileNotFoundError:
        return f"Not found: {resolved}"
    except Exception as e:
        return f"Error: {e}"


def write_file(path: str, content: str) -> str:
    resolved = _resolve(path)
    if resolved is None or not resolved.startswith(PROJECT_DIR):
        return "🚫 Write denied: writes are only allowed inside /project."
    try:
        parent = os.path.dirname(resolved)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(resolved, "w") as f:
            f.write(content)
        return f"✅ Written — {len(content):,} chars → {resolved}"
    except Exception as e:
        return f"❌ Write failed: {e}"


# ── Scanner data ──────────────────────────────────────────────────────────────

def get_stats() -> str:
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

                cur.execute("SELECT COUNT(*) AS n FROM signal_log")
                total = cur.fetchone()["n"]

                cur.execute(
                    "SELECT COUNT(*) AS n FROM signal_log WHERE created_at >= CURRENT_DATE"
                )
                today = cur.fetchone()["n"]

                cur.execute("""
                    SELECT COUNT(*) AS n
                    FROM signal_log sl
                    JOIN signal_outcomes so ON so.signal_id = sl.id
                    WHERE so.ret_5d IS NOT NULL
                """)
                graded = cur.fetchone()["n"]

                cur.execute("""
                    SELECT COUNT(*) AS n
                    FROM signal_log sl
                    JOIN signal_outcomes so ON so.signal_id = sl.id
                    WHERE so.ret_5d > 0
                """)
                wins = cur.fetchone()["n"]

                try:
                    cur.execute(
                        "SELECT COUNT(*) AS n FROM stock_universe WHERE active = TRUE"
                    )
                    univ = cur.fetchone()["n"]
                except Exception:
                    univ = "?"

                cur.execute("""
                    SELECT ticker, signal_label, score, created_at
                    FROM signal_log ORDER BY created_at DESC LIMIT 1
                """)
                last = cur.fetchone()

        win_rate = f"{wins / graded * 100:.1f}%" if graded > 0 else "N/A"

        cp_label = "All checkpoints reached"
        for target in (150, 350, 600):
            if total < target:
                cp_label = f"{total}/{target}  ({target - total} to go)"
                break

        last_str = (
            f"{last['ticker']} {last['signal_label']} "
            f"score={last['score']}  {str(last['created_at'])[:16]}"
        ) if last else "none"

        return (
            f"<b>📊 Axiom Stats</b>\n\n"
            f"Signals:     <code>{total}</code> total  |  <code>{today}</code> today\n"
            f"Win rate:    <code>{win_rate}</code>  ({graded} graded)\n"
            f"Universe:    <code>{univ}</code> active tickers\n"
            f"Checkpoint:  <code>{cp_label}</code>\n"
            f"Last signal: <code>{last_str}</code>"
        )
    except Exception as e:
        return f"❌ Stats error: {e}"


def get_signals(n: int = 10) -> str:
    n = max(1, min(n, 50))
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT sl.ticker, sl.signal_label, sl.score,
                           sl.price_at_signal, sl.entry_price, sl.stop_loss,
                           sl.target_1, sl.created_at,
                           so.ret_5d
                    FROM signal_log sl
                    LEFT JOIN signal_outcomes so ON so.signal_id = sl.id
                    ORDER BY sl.created_at DESC
                    LIMIT %s
                """, (n,))
                rows = cur.fetchall()

        if not rows:
            return "No signals found."

        lines = [f"<b>Last {len(rows)} signals:</b>"]
        for r in rows:
            ret = ""
            if r.get("ret_5d") is not None:
                pct = float(r["ret_5d"]) * 100
                ret = f"  →<code>{pct:+.1f}%</code>"
            e_str = f" e=${float(r['entry_price']):.3f}" if r.get("entry_price") else ""
            s_str = f" s=${float(r['stop_loss']):.3f}"   if r.get("stop_loss")   else ""
            t     = str(r["created_at"])[:16]
            lines.append(
                f"<b>{r['ticker']}</b> {r['signal_label']} "
                f"<code>{r['score']}</code>{e_str}{s_str}{ret}  <i>{t}</i>"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"❌ Error: {e}"


def get_convictions() -> str:
    try:
        today     = date.today().isoformat()
        yesterday = (date.today() - timedelta(days=1)).isoformat()

        with _conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                rows, label, d_str = [], "", today
                for d_str, label in [(today, "Today"), (yesterday, "Yesterday")]:
                    cur.execute("""
                        SELECT rank, ticker, conviction, hold_type,
                               entry, stop_loss, target_1, target_2,
                               session, ai_key_reason
                        FROM conviction_buys
                        WHERE date = %s
                        ORDER BY rank ASC
                        LIMIT 10
                    """, (d_str,))
                    rows = cur.fetchall()
                    if rows:
                        break

        if not rows:
            return "No conviction buys found for today or yesterday."

        lines = [f"<b>🎯 Conviction Buys — {label} ({d_str})</b>"]
        for r in rows:
            entry  = f"${float(r['entry']):.3f}"      if r.get("entry")     else "—"
            stop   = f"${float(r['stop_loss']):.3f}"  if r.get("stop_loss") else "—"
            t1     = f"${float(r['target_1']):.3f}"   if r.get("target_1")  else "—"
            reason = (r.get("ai_key_reason") or "")[:120]
            lines.append(
                f"\n<b>#{r['rank']} {r['ticker']}</b>  "
                f"<code>{r.get('hold_type','')}</code>  {r.get('session','')}\n"
                f"Entry {entry} | Stop {stop} | T1 {t1}\n"
                f"<i>{reason}</i>"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"❌ Error: {e}"
