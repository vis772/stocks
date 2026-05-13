"""
Axiom Terminal — Telegram Bot (Service 3)
Two-way agentic control via Claude tool use.
"""

import os
import json
import logging
import asyncio
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
from telegram.constants import ParseMode
from agent import run_agent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── Auth ────────────────────────────────────────────────────────────────────
ALLOWED_USER_ID = int(os.environ["TELEGRAM_ALLOWED_USER_ID"])

# Per-user conversation history (in-memory, resets on redeploy)
conversation_history: dict[int, list] = {}

# Pending confirmations: user_id -> {action, params, description}
pending_confirmations: dict[int, dict] = {}


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Lock to allowed user only
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("Unauthorized.")
        return

    user_text = update.message.text.strip()
    logger.info(f"[{user_id}] → {user_text}")

    # ── Confirmation flow ────────────────────────────────────────────────────
    if user_id in pending_confirmations:
        pending = pending_confirmations[user_id]
        if user_text.lower() in ("confirm", "yes", "y"):
            del pending_confirmations[user_id]
            await update.message.reply_text(
                f"✓ Executing: {pending['description']}",
                parse_mode=ParseMode.HTML
            )
            # Re-run agent with confirmation signal appended
            history = conversation_history.get(user_id, [])
            history.append({"role": "user", "content": f"CONFIRMED. Execute: {pending['description']}"})
            response, history = await run_agent(history, user_id)
            conversation_history[user_id] = history[-20:]  # Keep last 20 turns
        elif user_text.lower() in ("cancel", "no", "n"):
            del pending_confirmations[user_id]
            await update.message.reply_text("Cancelled. Nothing was changed.")
            return
        else:
            await update.message.reply_text(
                f"Pending action: <b>{pending['description']}</b>\n\nReply <b>confirm</b> to proceed or <b>cancel</b> to abort.",
                parse_mode=ParseMode.HTML
            )
            return
    else:
        # Normal message — run agent
        history = conversation_history.get(user_id, [])
        history.append({"role": "user", "content": user_text})

        await context.bot.send_chat_action(
            chat_id=update.effective_chat.id,
            action="typing"
        )

        response, history, confirmation = await run_agent(history, user_id)
        conversation_history[user_id] = history[-20:]

        # If agent wants confirmation before a write action
        if confirmation:
            pending_confirmations[user_id] = confirmation
            await update.message.reply_text(
                f"⚠️ <b>Confirmation required</b>\n\n{confirmation['description']}\n\nReply <b>confirm</b> to proceed or <b>cancel</b> to abort.",
                parse_mode=ParseMode.HTML
            )
            return

    # Send response (split if too long for Telegram's 4096 char limit)
    if len(response) <= 4096:
        await update.message.reply_text(response, parse_mode=ParseMode.HTML)
    else:
        chunks = [response[i:i+4000] for i in range(0, len(response), 4000)]
        for chunk in chunks:
            await update.message.reply_text(chunk, parse_mode=ParseMode.HTML)


def main():
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    app = Application.builder().token(token).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Axiom Terminal bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
