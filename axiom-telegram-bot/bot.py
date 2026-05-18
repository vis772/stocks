"""
Axiom Terminal — Telegram Bot (Service 3)
Two-way agentic control via Claude tool use.
"""

import os
import re
import asyncio
import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
from telegram.constants import ParseMode
from agent import run_agent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── HTML helpers ─────────────────────────────────────────────────────────────

# Tags Telegram HTML mode actually supports
_TG_SUPPORTED = frozenset({
    "b", "strong", "i", "em", "u", "ins",
    "s", "strike", "del",
    "code", "pre",
    "a",           # <a href="..."> preserved verbatim
    "blockquote",
})

# Unsupported block-level tags → newline so the text still reads naturally
_TG_BLOCK = frozenset({
    "p", "div", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6",
    "hr", "ul", "ol", "table", "thead", "tbody", "tfoot",
})


def _sanitize_html(text: str) -> str:
    """
    Prepare Claude's output for Telegram HTML mode.

    Keeps the tags Telegram supports, converts block-level tags to newlines
    so text still reads naturally, and strips everything else (span, font,
    div, etc.) while preserving their inner content.
    """
    # <br> / <br/> → newline before the tag-replacement pass
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)

    def _replace(m: re.Match) -> str:
        slash = m.group(1)          # "/" for closing tags, "" for opening
        name  = m.group(2).lower()
        if name in _TG_SUPPORTED:
            return m.group(0)       # keep verbatim
        if name in _TG_BLOCK:
            return "\n" if slash else ""   # closing → newline, opening → nothing
        return ""                   # strip the tag, keep surrounding text

    text = re.sub(r"<(/?)(\w+)\b[^>]*>", _replace, text)
    text = re.sub(r"\n{3,}", "\n\n", text)  # collapse excessive blank lines
    return text.strip()


def _strip_all_html(text: str) -> str:
    """Remove every HTML tag and decode basic entities — plain-text fallback."""
    plain = re.sub(r"<[^>]+>", "", text)
    return (plain
            .replace("&lt;", "<").replace("&gt;", ">")
            .replace("&amp;", "&").replace("&quot;", '"').replace("&apos;", "'")
            .strip())


async def _reply_html(message, text: str) -> None:
    """
    Send text with HTML formatting.  If Telegram rejects the markup
    (malformed tags, unsupported entities, …) retry immediately as plain text.
    """
    try:
        await message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning("HTML send failed (%s) — retrying as plain text", e)
        await message.reply_text(_strip_all_html(text))


# ── Auth ─────────────────────────────────────────────────────────────────────
ALLOWED_USER_ID = int(os.environ["TELEGRAM_ALLOWED_USER_ID"])

# Per-user conversation history (in-memory, resets on redeploy)
conversation_history: dict[int, list] = {}

# Pending confirmations: user_id -> {action, params, description}
pending_confirmations: dict[int, dict] = {}


async def keep_typing(bot, chat_id: int):
    """Send typing action every 4s until cancelled."""
    try:
        while True:
            await bot.send_chat_action(chat_id=chat_id, action="typing")
            await asyncio.sleep(4)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.warning(f"Typing indicator stopped: {e}")


async def safe_run_agent(history: list, user_id: int, update: Update):
    """
    Runs Claude agent safely.
    If Anthropic rejects corrupted tool history, clear history instead of crashing.
    """
    try:
        return await run_agent(history, user_id)

    except Exception as e:
        error_text = str(e)

        if "tool_use_id" in error_text or "tool_result" in error_text:
            logger.warning(f"Resetting corrupted conversation history for user {user_id}: {e}")

            conversation_history[user_id] = []
            pending_confirmations.pop(user_id, None)

            await update.message.reply_text(
                "Conversation state reset. Please send your request again."
            )

            return None, [], None

        raise


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("Unauthorized.")
        return

    user_text = update.message.text.strip()
    logger.info(f"[{user_id}] → {user_text}")

    response = None
    confirmation = None

    # ── Confirmation flow ─────────────────────────────────────────────────────
    if user_id in pending_confirmations:
        pending = pending_confirmations[user_id]

        if user_text.lower() in ("confirm", "yes", "y"):
            del pending_confirmations[user_id]

            await update.message.reply_text(
                f"✓ Executing: {pending['description']}",
                parse_mode=ParseMode.HTML
            )

            history = conversation_history.get(user_id, [])
            history.append({
                "role": "user",
                "content": f"CONFIRMED. Execute: {pending['description']}"
            })

            typing_task = asyncio.create_task(
                keep_typing(context.bot, update.effective_chat.id)
            )

            try:
                response, history, confirmation = await safe_run_agent(
                    history,
                    user_id,
                    update
                )
            finally:
                typing_task.cancel()

            conversation_history[user_id] = history[-20:]

        elif user_text.lower() in ("cancel", "no", "n"):
            del pending_confirmations[user_id]
            await update.message.reply_text("Cancelled. Nothing was changed.")
            return

        else:
            await update.message.reply_text(
                f"Pending: <b>{pending['description']}</b>\n\nReply <b>confirm</b> or <b>cancel</b>.",
                parse_mode=ParseMode.HTML
            )
            return

    else:
        # ── Normal message ────────────────────────────────────────────────────
        history = conversation_history.get(user_id, [])
        history.append({"role": "user", "content": user_text})

        typing_task = asyncio.create_task(
            keep_typing(context.bot, update.effective_chat.id)
        )

        try:
            response, history, confirmation = await safe_run_agent(
                history,
                user_id,
                update
            )
        finally:
            typing_task.cancel()

        conversation_history[user_id] = history[-20:]

    # ── Confirmation request returned by agent ────────────────────────────────
    if confirmation:
        pending_confirmations[user_id] = confirmation

        await update.message.reply_text(
            f"⚠️ <b>Confirmation required</b>\n\n{confirmation['description']}\n\nReply <b>confirm</b> or <b>cancel</b>.",
            parse_mode=ParseMode.HTML
        )
        return

    # ── Send response (split if over Telegram's 4096 char limit) ─────────────
    if not response:
        response = "Done."

    # Sanitize the full response before chunking so tag removal doesn't
    # shift the boundary into the middle of a supported tag.
    response = _sanitize_html(response)

    chunks = [response[i:i + 4000] for i in range(0, len(response), 4000)]
    for chunk in chunks:
        await _reply_html(update.message, chunk)


def main():
    token = os.environ["TELEGRAM_BOT_TOKEN"]

    app = Application.builder().token(token).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Axiom Terminal bot starting...")
    app.run_polling(drop_pending_updates=True, poll_interval=0)


if __name__ == "__main__":
    main()
