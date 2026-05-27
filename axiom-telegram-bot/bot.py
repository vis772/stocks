"""
Axiom Admin Terminal — Telegram Bot

Security model:
  Layer 1 — TELEGRAM_ALLOWED_USER_ID  : hard gate, all other senders silently ignored
  Layer 2 — Session PIN (8000)        : required at the start of every session
  Layer 3 — Session expiry            : auto-locks after SESSION_TIMEOUT seconds of inactivity
  Layer 4 — Write confirmation        : SQL writes and container restarts require explicit 'confirm'
  Layer 5 — Command audit log         : every authenticated command is logged with timestamp
"""

import os
import time
import logging
from datetime import datetime, timezone

from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters, ContextTypes,
)
from telegram.constants import ParseMode

import tools

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
ALLOWED_USER_ID = int(os.environ["TELEGRAM_ALLOWED_USER_ID"])
SESSION_PIN     = "8000"
SESSION_TIMEOUT = 4 * 3600   # 4 hours of inactivity → auto-lock

# ── In-memory session state (resets on every container restart) ───────────────
# _sessions: user_id → UNIX timestamp of last activity (only present if authenticated)
_sessions: dict[int, float] = {}

# _pending: user_id → {"type": "sql"|"restart"|"edit", ...extra data}
_pending: dict[int, dict] = {}


# ── Session helpers ───────────────────────────────────────────────────────────

def _is_auth(user_id: int) -> bool:
    ts = _sessions.get(user_id)
    if ts is None:
        return False
    if time.time() - ts > SESSION_TIMEOUT:
        _sessions.pop(user_id, None)
        _pending.pop(user_id, None)
        return False
    return True


def _touch(user_id: int) -> None:
    """Refresh session timestamp."""
    _sessions[user_id] = time.time()


def _lock(user_id: int) -> None:
    _sessions.pop(user_id, None)
    _pending.pop(user_id, None)


# ── Telegram helpers ──────────────────────────────────────────────────────────

async def _send(update: Update, text: str) -> None:
    """Send with HTML parse mode; fall back to plain text on markup errors."""
    if not text:
        text = "(empty response)"
    try:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception:
        # Strip all tags for plain-text fallback
        import re
        plain = re.sub(r"<[^>]+>", "", text).strip()
        await update.message.reply_text(plain or text)


async def _send_chunked(update: Update, text: str) -> None:
    """Split response into ≤4000-char chunks."""
    for i in range(0, max(len(text), 1), 4000):
        await _send(update, text[i : i + 4000])


def _audit(user_id: int, cmd: str, args: str = "") -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info(f"[AUDIT] {ts}  user={user_id}  /{cmd}  {args}".strip())


# ── Guard helper ──────────────────────────────────────────────────────────────

def _check(update: Update) -> tuple[bool, bool]:
    """Return (is_allowed, is_authed)."""
    uid = update.effective_user.id
    return uid == ALLOWED_USER_ID, _is_auth(uid)


# ── /start ────────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed, authed = _check(update)
    if not allowed:
        return
    if authed:
        await _send(update, _help_text())
    else:
        await _send(update, (
            "🔐 <b>Axiom Admin Terminal</b>\n\n"
            "Session locked. Enter your PIN to begin."
        ))


# ── /help ─────────────────────────────────────────────────────────────────────

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed, authed = _check(update)
    if not allowed:
        return
    if not authed:
        await _send(update, "🔒 Enter PIN first.")
        return
    _touch(update.effective_user.id)
    await _send(update, _help_text())


# ── /lock ─────────────────────────────────────────────────────────────────────

async def cmd_lock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if uid != ALLOWED_USER_ID:
        return
    _lock(uid)
    logger.info(f"[AUDIT] user={uid} manually locked session")
    await _send(update, "🔒 Session locked. Send PIN to re-authenticate.")


# ── /sql ──────────────────────────────────────────────────────────────────────

async def cmd_sql(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed, authed = _check(update)
    uid = update.effective_user.id
    if not allowed:
        return
    if not authed:
        await _send(update, "🔒 Enter PIN first.")
        return

    if not context.args:
        await _send(update, "Usage: <code>/sql SELECT * FROM signal_log LIMIT 5</code>")
        return

    sql = " ".join(context.args)
    _touch(uid)
    _audit(uid, "sql", sql[:120])

    if tools.is_write_sql(sql):
        _pending[uid] = {"type": "sql", "sql": sql}
        await _send(update, (
            f"⚠️ <b>Write query — confirm to execute:</b>\n\n"
            f"<pre>{sql[:600]}</pre>\n\n"
            f"Reply <b>confirm</b> or <b>cancel</b>."
        ))
        return

    result = tools.run_sql(sql)
    await _send_chunked(update, f"<pre>{result}</pre>")


# ── /logs ─────────────────────────────────────────────────────────────────────

async def cmd_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed, authed = _check(update)
    uid = update.effective_user.id
    if not allowed:
        return
    if not authed:
        await _send(update, "🔒 Enter PIN first.")
        return

    service = (context.args[0] if context.args else "scanner").lower()
    _touch(uid)
    _audit(uid, "logs", service)

    result = tools.docker_logs(service)
    # Telegram prefers the most-recent lines; trim from the top if long
    trimmed = result[-3600:] if len(result) > 3600 else result
    await _send_chunked(update, f"<b>Logs [{service}]:</b>\n<pre>{trimmed}</pre>")


# ── /status ───────────────────────────────────────────────────────────────────

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed, authed = _check(update)
    uid = update.effective_user.id
    if not allowed:
        return
    if not authed:
        await _send(update, "🔒 Enter PIN first.")
        return
    _touch(uid)
    _audit(uid, "status")
    await _send(update, tools.docker_status())


# ── /restart ──────────────────────────────────────────────────────────────────

async def cmd_restart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed, authed = _check(update)
    uid = update.effective_user.id
    if not allowed:
        return
    if not authed:
        await _send(update, "🔒 Enter PIN first.")
        return

    service   = (context.args[0] if context.args else "scanner").lower()
    container = tools.CONTAINERS.get(service, f"axiom-{service}")
    _touch(uid)
    _audit(uid, "restart", service)

    _pending[uid] = {"type": "restart", "service": service, "container": container}
    await _send(update, (
        f"⚠️ <b>Restart <code>{container}</code>?</b>\n\n"
        f"Reply <b>confirm</b> or <b>cancel</b>."
    ))


# ── /run ──────────────────────────────────────────────────────────────────────

async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed, authed = _check(update)
    uid = update.effective_user.id
    if not allowed:
        return
    if not authed:
        await _send(update, "🔒 Enter PIN first.")
        return

    if not context.args:
        await _send(update, "Usage: <code>/run ls -la /project</code>")
        return

    cmd = " ".join(context.args)
    _touch(uid)
    _audit(uid, "run", cmd)

    result = tools.run_command(cmd)
    trimmed = result[-3600:] if len(result) > 3600 else result
    await _send_chunked(update, f"<b>$ {cmd}</b>\n<pre>{trimmed}</pre>")


# ── /files ────────────────────────────────────────────────────────────────────

async def cmd_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed, authed = _check(update)
    uid = update.effective_user.id
    if not allowed:
        return
    if not authed:
        await _send(update, "🔒 Enter PIN first.")
        return

    path = context.args[0] if context.args else ""
    _touch(uid)
    _audit(uid, "files", path)
    result = tools.list_files(path)
    await _send(update, f"<pre>{result}</pre>")


# ── /read ─────────────────────────────────────────────────────────────────────

async def cmd_read(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed, authed = _check(update)
    uid = update.effective_user.id
    if not allowed:
        return
    if not authed:
        await _send(update, "🔒 Enter PIN first.")
        return

    if not context.args:
        await _send(update, "Usage: <code>/read scanner_loop.py</code>")
        return

    path = context.args[0]
    _touch(uid)
    _audit(uid, "read", path)
    content = tools.read_file(path)
    await _send_chunked(update, f"<b>{path}</b>\n<pre>{content}</pre>")


# ── /edit ─────────────────────────────────────────────────────────────────────

async def cmd_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed, authed = _check(update)
    uid = update.effective_user.id
    if not allowed:
        return
    if not authed:
        await _send(update, "🔒 Enter PIN first.")
        return

    if not context.args:
        await _send(update, "Usage: <code>/edit config.py</code>")
        return

    path     = context.args[0]
    resolved = tools._resolve(path)
    _touch(uid)
    _audit(uid, "edit", path)

    current = tools.read_file(path)
    _pending[uid] = {"type": "edit", "path": resolved}

    preview = current[:800] + ("\n[...truncated]" if len(current) > 800 else "")
    await _send(update, (
        f"✏️ <b>Editing:</b> <code>{resolved}</code>\n\n"
        f"<b>Current content:</b>\n<pre>{preview}</pre>\n\n"
        f"<b>Reply with the new file content</b> (or send as a document attachment).\n"
        f"Reply <b>cancel</b> to abort."
    ))


# ── /stats ────────────────────────────────────────────────────────────────────

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed, authed = _check(update)
    uid = update.effective_user.id
    if not allowed:
        return
    if not authed:
        await _send(update, "🔒 Enter PIN first.")
        return
    _touch(uid)
    _audit(uid, "stats")
    await _send(update, tools.get_stats())


# ── /signals ──────────────────────────────────────────────────────────────────

async def cmd_signals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed, authed = _check(update)
    uid = update.effective_user.id
    if not allowed:
        return
    if not authed:
        await _send(update, "🔒 Enter PIN first.")
        return

    n = 10
    if context.args:
        try:
            n = int(context.args[0])
        except ValueError:
            pass
    _touch(uid)
    _audit(uid, "signals", str(n))
    await _send_chunked(update, tools.get_signals(n))


# ── /convictions ──────────────────────────────────────────────────────────────

async def cmd_convictions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed, authed = _check(update)
    uid = update.effective_user.id
    if not allowed:
        return
    if not authed:
        await _send(update, "🔒 Enter PIN first.")
        return
    _touch(uid)
    _audit(uid, "convictions")
    await _send_chunked(update, tools.get_convictions())


# ── Text message handler (PIN entry + confirmation + edit content) ─────────────

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if uid != ALLOWED_USER_ID:
        return

    text = update.message.text.strip()

    # ── Layer 2: PIN gate ─────────────────────────────────────────────────────
    if not _is_auth(uid):
        if text == SESSION_PIN:
            _touch(uid)
            logger.info(f"[AUDIT] user={uid} authenticated via PIN")
            await _send(update, (
                "✅ <b>PIN accepted.</b> Session active (auto-locks after 4h idle).\n\n"
                + _help_text()
            ))
        else:
            logger.warning(f"[AUDIT] user={uid} bad PIN attempt")
            await _send(update, "🔒 Incorrect PIN. Try again.")
        return

    # Authenticated — refresh session
    _touch(uid)

    pending = _pending.get(uid)

    # ── Pending: edit (waiting for file content, not confirm/cancel) ──────────
    if pending and pending["type"] == "edit":
        if text.lower() in ("cancel", "no", "n"):
            _pending.pop(uid, None)
            await _send(update, "❌ Edit cancelled.")
        else:
            path = pending["path"]
            _pending.pop(uid, None)
            _audit(uid, "edit:write", path)
            result = tools.write_file(path, text)
            await _send(update, result)
        return

    # ── Pending: sql or restart (waiting for confirm/cancel) ──────────────────
    if pending:
        ptype = pending["type"]
        if text.lower() in ("confirm", "yes", "y"):
            _pending.pop(uid, None)
            if ptype == "sql":
                _audit(uid, "sql:confirmed", pending["sql"][:80])
                result = tools.run_sql(pending["sql"])
                await _send_chunked(update, f"<pre>{result}</pre>")
            elif ptype == "restart":
                _audit(uid, "restart:confirmed", pending["container"])
                result = tools.docker_restart(pending["service"])
                await _send(update, result)
        elif text.lower() in ("cancel", "no", "n"):
            _pending.pop(uid, None)
            await _send(update, "❌ Cancelled.")
        else:
            await _send(update, f"⏳ Waiting for <b>confirm</b> or <b>cancel</b>.")
        return

    # ── No pending action — unknown free text ─────────────────────────────────
    await _send(update, "Use /help to see available commands.")


# ── Document handler (file upload for /edit) ──────────────────────────────────

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if uid != ALLOWED_USER_ID or not _is_auth(uid):
        return

    _touch(uid)
    pending = _pending.get(uid)

    if not pending or pending["type"] != "edit":
        await _send(update, "No active /edit session. Use /edit <path> first.")
        return

    path = pending["path"]
    _pending.pop(uid, None)

    try:
        doc          = update.message.document
        tg_file      = await context.bot.get_file(doc.file_id)
        raw_bytes    = await tg_file.download_as_bytearray()
        content      = raw_bytes.decode("utf-8", errors="replace")
        _audit(uid, "edit:write (document)", path)
        result = tools.write_file(path, content)
        await _send(update, result)
    except Exception as e:
        await _send(update, f"❌ Upload failed: {e}")


# ── Help text ─────────────────────────────────────────────────────────────────

def _help_text() -> str:
    return (
        "<b>Axiom Admin Terminal — Commands</b>\n\n"
        "<b>Database</b>\n"
        "/sql &lt;query&gt;       run SQL (writes need confirm)\n"
        "/stats              signal count, win rate, last scan\n"
        "/signals [n]        last n signals from signal_log\n"
        "/convictions        today's conviction buy list\n\n"
        "<b>System</b>\n"
        "/status             docker ps + CPU + memory + disk\n"
        "/logs [service]     last 50 log lines  (scanner|bot)\n"
        "/restart [service]  restart container  (needs confirm)\n"
        "/run &lt;cmd&gt;         run any shell command\n\n"
        "<b>Files</b>\n"
        "/files [path]       list directory contents\n"
        "/read &lt;path&gt;        show file contents\n"
        "/edit &lt;path&gt;        edit file (reply with new content)\n\n"
        "<b>Session</b>\n"
        "/lock               lock session (PIN required again)\n"
        "/help               show this message\n\n"
        "<i>Relative paths are under /project (= /home/ubuntu/axiom on EC2).</i>"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    token = os.environ["TELEGRAM_BOT_TOKEN"]

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("help",        cmd_help))
    app.add_handler(CommandHandler("lock",        cmd_lock))
    app.add_handler(CommandHandler("sql",         cmd_sql))
    app.add_handler(CommandHandler("logs",        cmd_logs))
    app.add_handler(CommandHandler("status",      cmd_status))
    app.add_handler(CommandHandler("restart",     cmd_restart))
    app.add_handler(CommandHandler("run",         cmd_run))
    app.add_handler(CommandHandler("files",       cmd_files))
    app.add_handler(CommandHandler("read",        cmd_read))
    app.add_handler(CommandHandler("edit",        cmd_edit))
    app.add_handler(CommandHandler("stats",       cmd_stats))
    app.add_handler(CommandHandler("signals",     cmd_signals))
    app.add_handler(CommandHandler("convictions", cmd_convictions))

    # PIN entry, confirmation replies, and /edit content
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    # Document uploads for /edit
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    logger.info("Axiom Admin Bot starting — slash command mode with PIN auth")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
