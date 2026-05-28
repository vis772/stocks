# Axiom Terminal — CLAUDE.md

## What this project is

Axiom Terminal is a small-cap stock scanner and admin system running on an AWS EC2 instance. It continuously scans ~54 tickers, scores them across technical/fundamental/risk/sentiment factors, fires real-time alerts, and produces daily conviction pick lists. The Telegram bot is the primary admin interface — the Streamlit dashboard (`app.py`) is the visual layer.

**Not financial advice. For personal research use only.**

---

## Architecture

Two Docker containers managed by `docker-compose.yml`:

| Container | Entry point | Role |
|-----------|-------------|------|
| `axiom-scanner` | `scanner_loop.py` | Continuous scan loop, conviction engine, EOD reports, accuracy grading |
| `axiom-telegram-bot` | `axiom-telegram-bot/bot.py` | Admin terminal — Claude Haiku agent with DB/shell access |

Both containers share the same PostgreSQL database (Supabase in production, SQLite fallback locally).

The Streamlit dashboard (`app.py`) is NOT a separate container — it can be run locally or via `Procfile` on Railway/Heroku. In production, the scanner owns all resources on EC2.

---

## Key files

### Root
| File | Purpose |
|------|---------|
| `scanner_loop.py` | Main scan loop. Fires every ~60s during market hours. Calls `core/scanner.py` per ticker via `ThreadPoolExecutor`. Runs conviction engine 4×/day, EOD report nightly. |
| `conviction_engine.py` | Synthesises all scanner signals into ≤5 ranked buy candidates per session (preopen/intraday/close/afterhours). Writes to `conviction_buys` table. |
| `app.py` | Streamlit dashboard. Multi-tab UI: live signals, conviction list, accuracy, charts, admin. Reads from PG; never writes signals. |
| `config.py` | Single source of truth for scoring weights, thresholds, universe filters, risk flags. **Edit here to tune the system.** |
| `utils.py` | Shared helpers: `now_et()`, `now_utc()`, `safe_float()`, `fh_get()`, `anthropic_model()`, `anthropic_haiku()`. Import from here — never re-implement. |
| `accuracy_validator.py` | Grades past signals (1d/3d/5d/10d returns). Runs nightly at 10 PM ET. Call `force_grade_all_pending()` to grade immediately. |
| `alerts.py` | Sends Telegram alerts for volume spikes, price moves, SEC filings, conviction picks. Uses priority levels. |
| `eod_report.py` | Generates and delivers the end-of-day PDF report via Telegram. |
| `morning_screen.py` | Builds today's pre-market watchlist. Runs at 8:45 AM ET. |
| `universe_manager.py` | Populates `stock_universe` table from Finnhub screener + static fallback. Refreshes daily. |
| `auth.py` | bcrypt password hashing and UUID4 session tokens for the Streamlit login. |
| `resilient_fetcher.py` | Retry-wrapped HTTP fetcher for external APIs. |
| `seed_changelog.py` | One-shot script to seed the `changelog` table from git log. |

### Modules
| Module | Purpose |
|--------|---------|
| `core/scanner.py` | Per-ticker scan pipeline: fetch → score → compare → write signal. |
| `db/database.py` | All database logic. PostgreSQL with `ThreadedConnectionPool` (max 60). Falls back to SQLite if `DATABASE_URL` is unset. Column whitelist guards on all dynamic SQL. |
| `data/market_data.py` | yfinance wrapper: prices, volume, fundamentals. |
| `data/sec_data.py` | SEC EDGAR free API: 8-K, S-3, Form 4, 10-Q parsing. |
| `data/news_data.py` | Yahoo Finance RSS news + keyword sentiment. |
| `data/massive_client.py` | Massive.com data client (paid tier, gated at runtime). |
| `data/tiingo_stream.py` | Tiingo WebSocket stream for low-latency quotes. |
| `quant/factor_engine.py` | IC (information coefficient) computation for factor validation. |
| `health/monitor.py` | Self-healing health checks: Finnhub failure rate, universe size, stalled scanner, suspicious perfect scores. Called every 5 min from scanner loop. |
| `reports/checkpoint_reports.py` | Intraday checkpoint PDF reports (15/30/60/150/350/600 min). |
| `reports/morning_report.py` | Morning PDF report sent via Telegram at market open. |
| `analysis/` | Technical indicators, fundamentals, regime detection. |

### Telegram bot (`axiom-telegram-bot/`)
| File | Purpose |
|------|---------|
| `bot.py` | Telegram bot. 5-layer security model (user ID gate → PIN → session timeout → write confirmation → audit log). PIN is `8000`. Session timeout: 4 hours. |
| `agent.py` | Claude Haiku agent. Has full tool access. System prompt includes DB schema, grading logic, and key docker commands. |
| `tools.py` | All tool implementations: `run_sql`, `run_command`, `read_file`, `edit_file`, `get_signals`, `get_convictions`, `docker_status`, `docker_logs`, etc. |
| `health_monitor.py` | Bot-side health check that polls container status. |

---

## Database tables

| Table | Written by | Read by |
|-------|-----------|---------|
| `signal_log` | scanner | dashboard, bot, accuracy_validator |
| `signal_outcomes` | accuracy_validator | bot, dashboard |
| `conviction_buys` | conviction_engine | dashboard, bot |
| `accuracy_metrics` | accuracy_validator | dashboard, bot |
| `accuracy_reports` | accuracy_validator | dashboard |
| `watchlist` | morning_screen | dashboard |
| `stock_universe` | universe_manager | scanner |
| `alert_log` | alerts.py | dashboard |
| `scanner_state` | scanner_loop | dashboard |
| `portfolio` | dashboard (user) | dashboard |
| `bot_audit` | bot | bot (admin review) |
| `scanner_logs` | scanner_loop | bot, dashboard |
| `changelog` | seed_changelog.py | dashboard |

Signal grading: entry price = `price_at_signal` (live price when fired). Win = ret > +1%, Loss = ret < -1%, Neutral otherwise.

---

## Scoring system

Weights are in `config.py` and were derived from a 600-signal component-correlation audit (2026-05-22):

```python
SCORING_WEIGHTS = {
    "technical":    0.50,   # RSI, MACD, SMA crossovers, gap, momentum
    "fundamental":  0.22,   # revenue growth, balance sheet
    "risk":         0.20,   # dilution flags, short interest, volatility
    "sentiment":    0.08,   # news tone, catalyst keywords
}
```

Catalyst was removed from weights after showing 0.0 differential across 600 signals.

Only signals ≥ 62 (`MIN_SIGNAL_SCORE`) are written to `signal_log`. Alerts require ≥ 68 (`ALERT_SCORE_MIN`).

Strong Buy (75+) is demoted to Speculative Buy when RSI > 68, 5d return > 20%, or RVOL < 1.5x.

---

## Conviction engine sessions

| Session | Time (ET) | Purpose |
|---------|-----------|---------|
| `preopen` | 8:55 AM | Pre-market picks using overnight signals |
| `intraday` | ~12:00 PM | Midday refresh |
| `close` | 4:00 PM | Close-price picks |
| `afterhours` | 8:30 PM | OVERNIGHT hold type picks |

Max 5 names per session. Hold types: `INTRADAY`, `SWING`, `OVERNIGHT`.

---

## Environment variables (`.env`)

```
DATABASE_URL=postgresql://...        # Supabase or Railway Postgres URL
FINNHUB_API_KEY=...                  # Required for universe refresh and quotes
ANTHROPIC_API_KEY=...                # Claude API — used in conviction engine and bot
TELEGRAM_BOT_TOKEN=...              # Telegram bot token
TELEGRAM_ALLOWED_USER_ID=...        # Hard-gate user ID (integer)
TELEGRAM_CHAT_ID=...                # Chat ID for outbound alerts
TIINGO_API_KEY=...                   # Optional — real-time WebSocket stream
ANTHROPIC_HAIKU_MODEL=claude-haiku-4-5-20251001  # Optional override
AXIOM_TEST_SCAN=1                    # Optional — enables test mode in scanner
```

Locally: copy `.env.example` → `.env`. Without `DATABASE_URL`, SQLite (`scanner.db`) is used.

---

## Running locally

```bash
# Install deps
pip install -r requirements.txt

# Run Streamlit dashboard
bash run.sh
# → http://localhost:8501

# Run scanner loop (separate terminal)
python scanner_loop.py

# Run tests
pytest tests/ -v
```

---

## EC2 deployment

Production runs on Ubuntu 24.04, `t3.micro` (or larger). App lives at `/home/ubuntu/axiom`.

```bash
# First-time EC2 setup
sudo bash scripts/setup_ec2.sh

# Copy .env to server
scp .env ubuntu@<ec2-ip>:/home/ubuntu/axiom/.env

# Start both containers
cd /home/ubuntu/axiom && docker compose up -d

# Follow logs
docker compose logs -f
```

**Auto-deploy:** GitHub Actions (`.github/workflows/deploy.yml`) deploys on every push to `clean-combined-version`. Requires `EC2_HOST` and `EC2_SSH_KEY` in GitHub secrets. The workflow does: git pull → docker compose down → docker compose up -d → health check.

Systemd service for auto-start on reboot: `scripts/axiom.service`.

---

## Common operational commands (via Telegram bot or SSH)

```bash
# Grade all ungraded signals
docker compose exec scanner python3 -c "from accuracy_validator import force_grade_all_pending; print(force_grade_all_pending())"

# Check win rates
docker compose exec scanner python3 -c "from accuracy_validator import AccuracyValidator; import json; print(json.dumps(AccuracyValidator().compute_metrics().get('overall',{}), indent=2))"

# Run EOD report now
docker compose exec scanner python3 -c "from eod_report import run_eod_report; run_eod_report()"

# Run conviction engine (preopen)
docker compose exec scanner python3 -c "from conviction_engine import run_conviction_engine; run_conviction_engine('preopen')"

# Refresh universe
docker compose exec scanner python3 -c "from universe_manager import UniverseManager; UniverseManager().refresh_universe()"

# Resend a checkpoint report
docker compose exec scanner python3 tools/resend_checkpoint.py

# Archive and reset signals
docker compose exec scanner python3 tools/archive_and_reset_signals.py
```

---

## Key patterns and conventions

- **Timezone:** All display logic uses ET (`utils.now_et()`). All DB writes use naive UTC (`utils.now_utc()`). Never use `datetime.now()` without a timezone.
- **DB access:** Always go through `db/database.py` functions. Never write raw psycopg2 calls outside that module. Use `_safe_cols()` for any dynamic column names.
- **Shared helpers:** `utils.py` has `safe_float`, `now_et`, `now_utc`, `fh_get`, `anthropic_model`, `anthropic_haiku`. Import from there.
- **Config changes:** All thresholds and weights live in `config.py`. The assert at the bottom validates weights sum to 1.0.
- **No mobile.py / paper_broker.py:** These were removed in the `clean-combined-version` refactor. The scanner is headless; the dashboard is Streamlit-only.
- **Claude models:** `ANTHROPIC_MODEL` (Sonnet) for conviction engine analysis. `anthropic_haiku()` (Haiku) for the Telegram bot agent and inline analysis.
- **Connection pool:** PG pool is capped at 60 to match the scanner's ~54 concurrent `ThreadPoolExecutor` workers.
- **Health monitor:** `health/monitor.py` runs every 5 min. It checks Finnhub failure rate, universe size, scanner stall, and score sanity. Fires Telegram alert on CRITICAL status.

---

## Tests

```bash
pytest tests/ -v
```

`tests/test_scanner_logic.py` covers gap-continuation bonus boundary conditions. Add tests for any new scoring logic changes.

---

## Data sources

| Source | Data | Key |
|--------|------|-----|
| yfinance | Prices, volume, fundamentals, history | Free |
| SEC EDGAR API | 8-K, S-3, Form 4, 10-Q | Free |
| Finnhub | Universe screener, real-time quotes | `FINNHUB_API_KEY` |
| Tiingo WebSocket | Low-latency streaming quotes | `TIINGO_API_KEY` |
| Yahoo Finance RSS | News, sentiment | Free |
| Massive.com | Alternative data (gated) | Paid |
| Anthropic Claude | SEC summarisation, conviction reasoning | `ANTHROPIC_API_KEY` |
