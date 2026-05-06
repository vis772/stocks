# 📡 SmallCap Stock Scanner

A professional-grade speculative stock research tool. **Not financial advice.**

---

## What This Is (And Isn't)

**What it is:** A research acceleration tool that surfaces small-cap/mid-cap stocks
with interesting conditions (unusual volume, recent catalysts, technical setups)
and flags serious risks (dilution, going concern, reverse split risk) — so you
spend less time finding candidates and more time doing real research.

**What it isn't:** A prediction engine. It will not tell you which stocks will go up.
No scanner can. Anyone claiming otherwise is selling you something.

---

## Quick Start

```bash
# 1. Clone or download this folder
cd scanner/

# 2. First-time setup (creates venv, installs packages)
bash setup.sh

# 3. Run the dashboard
bash run.sh

# 4. Open http://localhost:8501 in your browser
```

---

## Project Structure

```
scanner/
├── app.py                  ← Streamlit dashboard (run this)
├── config.py               ← All tunable settings and weights
├── requirements.txt        ← Python dependencies
├── setup.sh / run.sh       ← Shell scripts for easy startup
├── scanner.db              ← SQLite database (auto-created)
├── .env                    ← API keys (auto-created, optional)
│
├── core/
│   └── scanner.py          ← Main scan pipeline orchestrator
│
├── data/
│   ├── market_data.py      ← yfinance wrapper (price, volume, fundamentals)
│   ├── sec_data.py         ← SEC EDGAR free API (filings, insider trades)
│   └── news_data.py        ← Yahoo Finance RSS news + sentiment
│
├── analysis/
│   ├── technicals.py       ← RSI, MACD, volume, momentum, entry/stop zones
│   ├── fundamentals.py     ← Revenue growth, cash, debt, risk scoring
│   └── portfolio.py        ← Holdings P&L, concentration, recommendations
│
└── db/
    └── database.py         ← SQLite persistence layer
```

---

## How the Scoring Works

Each stock receives a score from 0-100 based on five components.
Edit `config.py` to change weights — they're YOUR assumptions, not objective truth.

| Component | Default Weight | What it measures |
|-----------|---------------|-----------------|
| Technical | 30% | RSI, MACD, moving averages, relative volume, momentum |
| Catalyst | 25% | SEC 8-K filings, news, keyword matches |
| Fundamental | 20% | Revenue growth, cash ratio, burn rate, margins |
| Risk (inverted) | 15% | Short interest, dilution flags, volatility, liquidity |
| Sentiment | 10% | News tone, analyst coverage, hype detection |

### Signal Labels

| Signal | Score Range |
|--------|------------|
| Strong Buy Candidate | 75-100 |
| Speculative Buy | 60-75 |
| Watchlist | 45-60 |
| Hold | 35-45 |
| Trim | 25-35 |
| Sell | 15-25 |
| Avoid | 0-15 |

Critical risk flags (going concern, reverse split proxy) override the score
and force an **Avoid** signal regardless of other factors.

---

## Data Sources (All Free)

| Source | Data | Limitations |
|--------|------|-------------|
| Yahoo Finance (yfinance) | Price, volume, fundamentals, history | Delayed ~15min, sometimes unreliable |
| SEC EDGAR API | Filings (S-3, 8-K, Form 4, 10-Q) | Free and official, rate-limited |
| Yahoo Finance RSS | News headlines, sentiment | No full article text |

**Short interest accuracy caveat:** yfinance short interest data is unreliable
and only updated bi-weekly. For real short data, upgrade to Finviz Elite or
a dedicated data provider.

---

## Risk Flags

The scanner watches for these automatically:

| Flag | What it means |
|------|--------------|
| `going_concern` | Auditor flagged doubt about company surviving |
| `shelf_registration` | S-3 filing — shares ready to be sold at any time |
| `atm_offering` | Active ATM program selling shares continuously |
| `reverse_split_risk` | Proxy filing suggests reverse split vote |
| `high_short_interest` | >20% of float is short — squeeze risk but also skepticism |
| `extreme_volatility` | >100% annualized volatility — position size very carefully |
| `low_liquidity` | <500K avg daily volume — hard to exit quickly |
| `pump_signal` | Big volume spike with no confirmed SEC catalyst |

---

## Portfolio Input Format

In the sidebar, enter one holding per line:

```
TICKER, SHARES, AVG_COST
LAES, 100, 1.45
WULF, 50, 4.20
IREN, 25, 8.10
```

The portfolio dashboard will show:
- Unrealized P&L per holding
- Concentration risk warnings
- Mechanical recommendation (Hold/Trim/Sell/Add)
- Suggested stop-loss levels (based on your avg cost)

---

## Customizing the Scanner

### Change Scoring Weights (`config.py`)

```python
SCORING_WEIGHTS = {
    "technical":    0.30,   # ← Change these
    "catalyst":     0.25,
    "fundamental":  0.20,
    "risk":         0.15,
    "sentiment":    0.10,
}
```

### Change Universe Filters (`config.py`)

```python
MIN_MARKET_CAP = 50_000_000    # Minimum $50M market cap
MAX_MARKET_CAP = 10_000_000_000 # Maximum $10B
MIN_AVG_VOLUME = 500_000        # Minimum avg daily volume
```

### Add Your Own Tickers

Either type them in the sidebar scan box, or add to `DEFAULT_UNIVERSE` in `config.py`.

---

## Upgrading to Paid Data

When you're ready to level up:

| What you need | Service | Cost |
|--------------|---------|------|
| Real-time short interest | Finviz Elite | ~$25/mo |
| Options flow (unusual activity) | Unusual Whales | ~$50/mo |
| Better news with full text | Benzinga Pro | ~$50/mo |
| Reliable SEC alerts | Sentieo | ~$200/mo |
| Institutional 13F data | WhaleWisdom | Free-$50/mo |

To add API keys: edit the `.env` file in the scanner folder.

---

## Improving the System Over Time

The single most valuable thing you can do is **track your own decisions**:

1. When the scanner flags a "Speculative Buy" and you act on it, record the outcome
2. After 30-50 decisions, look at which specific signals actually preceded your winners
3. Adjust `SCORING_WEIGHTS` in `config.py` to reflect what actually worked for YOU
4. Repeat — this is how the scoring model becomes calibrated to your specific style

No preset weights are correct. They're a starting point.

---

## ⚠️ Risk Disclaimer

This software is for **educational and research purposes only**.

- Not financial advice
- Not a trading signal service
- Not backtested
- Small-cap stocks can and do go to zero
- Dilution, reverse splits, and bankruptcy happen frequently in this universe
- The scoring model is built on assumptions, not validated research
- Past signal patterns do not predict future performance
- Always do your own research and consult a licensed financial advisor for real investment decisions
- Never invest money you cannot afford to lose entirely

By using this software, you acknowledge you understand these risks.
