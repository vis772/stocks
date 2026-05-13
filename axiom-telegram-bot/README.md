# Axiom Terminal — Telegram Bot (Service 3)

Two-way agentic control over the Axiom Terminal scanner via Telegram + Claude.

---

## Step 1: Create your Telegram bot

1. Open Telegram and message @BotFather
2. Send: /newbot
3. Give it a name: "Axiom Terminal"
4. Give it a username: something like "axiom_terminal_bot"
5. BotFather gives you a token — copy it, you'll need it

---

## Step 2: Get your Telegram user ID

1. Message @userinfobot on Telegram
2. It replies with your user ID (a number like 123456789)
3. Copy it — this locks the bot to only respond to you

---

## Step 3: Add environment variables in Railway (Service 3)

Add these to your new Railway service:

```
TELEGRAM_BOT_TOKEN=your_token_from_botfather
TELEGRAM_ALLOWED_USER_ID=your_numeric_user_id
ANTHROPIC_API_KEY=your_anthropic_key
DATABASE_URL=your_postgres_connection_string
```

DATABASE_URL and ANTHROPIC_API_KEY are the same ones you use in Service 1 and 2.

---

## Step 4: Add restart support to scanner_loop.py

Add this block at the top of your main scanner loop cycle so it can pick up restart requests:

```python
def check_restart_flag(conn):
    """Check if a restart was requested via the Telegram bot."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT value FROM scanner_config
                WHERE key = 'restart_requested'
            """)
            row = cur.fetchone()
            if row:
                # Clear the flag
                cur.execute("DELETE FROM scanner_config WHERE key = 'restart_requested'")
                conn.commit()
                logger.info("[restart] Restart flag detected — restarting scanner loop")
                return True
    except Exception as e:
        logger.error(f"[restart] Error checking restart flag: {e}")
    return False
```

Then at the top of your main loop:
```python
if check_restart_flag(conn):
    import sys
    sys.exit(0)  # Railway will restart the service automatically
```

---

## Step 5: Deploy to Railway

1. Create a new service in your Railway project
2. Connect it to your repo (point it at the axiom-telegram-bot folder)
   OR push axiom-telegram-bot as a separate repo
3. Railway auto-detects railway.toml and runs: python bot.py
4. Add the 4 environment variables from Step 3
5. Deploy

---

## What you can say to the bot

**Read queries (instant, no confirmation):**
- "How many signals do we have?"
- "Show me the last 10 Strong Buy signals"
- "What's the scanner status?"
- "Give me the accuracy summary for the 5 day window"
- "How far are we from checkpoint 2?"
- "Show me all signals for TSLA"
- "What's the win rate so far?"

**Write actions (bot asks you to confirm first):**
- "Increase technical weight to 35 and drop sentiment to 15"
- "Add NVDA and AMD to the watchlist"
- "Remove AAPL from the watchlist"
- "Change the strong buy threshold to 70"
- "Restart the scanner, it seems stuck"

**The bot will never:**
- Delete or modify signal_log or signal_outcomes rows
- Execute a write action without you typing "confirm"
- Respond to anyone except your Telegram account

---

## File structure

```
axiom-telegram-bot/
├── bot.py          # Telegram handler + confirmation flow
├── agent.py        # Claude agent with tool definitions + guardrails
├── tools.py        # All tool implementations (DB reads + writes)
├── requirements.txt
├── railway.toml
└── README.md
```
