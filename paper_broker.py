# paper_broker.py
# PaperBroker — $100,000 simulated account. TradingView paper trading parity.
# Full order lifecycle: submit → fill → position management → target/stop exits.
# Replaces the old paper_trades table flow entirely.

import os
from datetime import datetime, date, timedelta
from typing import Optional, List
import numpy as np

# Slippage constants
SLIP_MARKET = 0.0005   # 0.05%
SLIP_STOP   = 0.0008   # 0.08%
SLIP_LIMIT  = 0.0      # 0%
COMMISSION  = 0.0      # no commission (paper)

STARTING_BALANCE = 100_000.0


def _now():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).replace(tzinfo=None)
    except ImportError:
        return datetime.utcnow()


def _f(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _safe(v, fb=0.0) -> float:
    try:
        f = float(v)
        return fb if (f != f) else f
    except Exception:
        return fb


def _is_pg():
    from db.database import _is_postgres
    return _is_postgres()


def _pg():
    from db.database import _get_pg_conn
    return _get_pg_conn()


def _sq():
    from db.database import _get_sqlite_conn
    return _get_sqlite_conn()


def _conn():
    return _pg() if _is_pg() else _sq()


def ph():
    return "%s" if _is_pg() else "?"


# ─── DB schema ────────────────────────────────────────────────────────────────

def init_paper_trading_tables():
    if _is_pg():
        conn = _pg(); cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pt_account (
                id               INTEGER PRIMARY KEY DEFAULT 1,
                balance          FLOAT DEFAULT 100000.00,
                equity           FLOAT DEFAULT 100000.00,
                day_start_equity FLOAT DEFAULT 100000.00,
                total_pnl        FLOAT DEFAULT 0.00,
                realized_pnl     FLOAT DEFAULT 0.00,
                unrealized_pnl   FLOAT DEFAULT 0.00,
                total_trades     INTEGER DEFAULT 0,
                winning_trades   INTEGER DEFAULT 0,
                losing_trades    INTEGER DEFAULT 0,
                win_rate         FLOAT DEFAULT 0.00,
                profit_factor    FLOAT DEFAULT 0.00,
                max_drawdown     FLOAT DEFAULT 0.00,
                peak_equity      FLOAT DEFAULT 100000.00,
                sharpe           FLOAT DEFAULT 0.00,
                created_at       TIMESTAMP DEFAULT NOW(),
                updated_at       TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("INSERT INTO pt_account (id) VALUES (1) ON CONFLICT DO NOTHING")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pt_positions (
                id              SERIAL PRIMARY KEY,
                ticker          TEXT NOT NULL,
                side            TEXT NOT NULL DEFAULT 'long',
                shares          FLOAT NOT NULL,
                avg_cost        FLOAT NOT NULL,
                current_price   FLOAT,
                market_value    FLOAT,
                unrealized_pnl  FLOAT DEFAULT 0,
                unrealized_pct  FLOAT DEFAULT 0,
                stop_loss       FLOAT,
                target_1        FLOAT,
                target_2        FLOAT,
                target_3        FLOAT,
                t1_hit          BOOLEAN DEFAULT FALSE,
                t2_hit          BOOLEAN DEFAULT FALSE,
                hold_type       TEXT,
                session_mode    TEXT,
                entry_reason    TEXT,
                conviction      FLOAT,
                mae             FLOAT DEFAULT 0,
                mfe             FLOAT DEFAULT 0,
                opened_at       TIMESTAMP DEFAULT NOW(),
                last_updated    TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pt_orders (
                id            SERIAL PRIMARY KEY,
                ticker        TEXT NOT NULL,
                side          TEXT NOT NULL,
                order_type    TEXT NOT NULL,
                qty           FLOAT NOT NULL,
                limit_price   FLOAT,
                stop_price    FLOAT,
                filled_price  FLOAT,
                filled_at     TIMESTAMP,
                status        TEXT DEFAULT 'pending',
                fill_slippage FLOAT DEFAULT 0,
                commission    FLOAT DEFAULT 0,
                created_at    TIMESTAMP DEFAULT NOW(),
                conviction_id INTEGER
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pt_trades (
                id           SERIAL PRIMARY KEY,
                ticker       TEXT NOT NULL,
                side         TEXT NOT NULL,
                shares       FLOAT NOT NULL,
                entry_price  FLOAT NOT NULL,
                exit_price   FLOAT NOT NULL,
                entry_at     TIMESTAMP,
                exit_at      TIMESTAMP DEFAULT NOW(),
                hold_days    FLOAT,
                gross_pnl    FLOAT,
                net_pnl      FLOAT,
                pnl_pct      FLOAT,
                exit_reason  TEXT,
                hold_type    TEXT,
                session_mode TEXT,
                conviction   FLOAT,
                mae          FLOAT DEFAULT 0,
                mfe          FLOAT DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pt_equity_curve (
                id            SERIAL PRIMARY KEY,
                recorded_at   TIMESTAMP DEFAULT NOW(),
                equity        FLOAT,
                cash          FLOAT,
                open_pnl      FLOAT,
                daily_pnl     FLOAT,
                daily_pnl_pct FLOAT,
                drawdown      FLOAT,
                drawdown_pct  FLOAT,
                session_mode  TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pt_daily_stats (
                id            SERIAL PRIMARY KEY,
                date          DATE UNIQUE,
                start_equity  FLOAT,
                end_equity    FLOAT,
                daily_pnl     FLOAT,
                daily_pnl_pct FLOAT,
                trades_opened INTEGER DEFAULT 0,
                trades_closed INTEGER DEFAULT 0,
                wins          INTEGER DEFAULT 0,
                losses        INTEGER DEFAULT 0,
                gross_volume  FLOAT DEFAULT 0,
                largest_win   FLOAT DEFAULT 0,
                largest_loss  FLOAT DEFAULT 0
            )
        """)
        conn.commit(); cur.close(); conn.close()
    else:
        conn = _sq(); cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pt_account (
                id               INTEGER PRIMARY KEY DEFAULT 1,
                balance          REAL DEFAULT 100000.00,
                equity           REAL DEFAULT 100000.00,
                day_start_equity REAL DEFAULT 100000.00,
                total_pnl        REAL DEFAULT 0.00,
                realized_pnl     REAL DEFAULT 0.00,
                unrealized_pnl   REAL DEFAULT 0.00,
                total_trades     INTEGER DEFAULT 0,
                winning_trades   INTEGER DEFAULT 0,
                losing_trades    INTEGER DEFAULT 0,
                win_rate         REAL DEFAULT 0.00,
                profit_factor    REAL DEFAULT 0.00,
                max_drawdown     REAL DEFAULT 0.00,
                peak_equity      REAL DEFAULT 100000.00,
                sharpe           REAL DEFAULT 0.00,
                created_at       TEXT DEFAULT (datetime('now')),
                updated_at       TEXT DEFAULT (datetime('now'))
            )
        """)
        cur.execute("INSERT OR IGNORE INTO pt_account (id) VALUES (1)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pt_positions (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker          TEXT NOT NULL,
                side            TEXT NOT NULL DEFAULT 'long',
                shares          REAL NOT NULL,
                avg_cost        REAL NOT NULL,
                current_price   REAL,
                market_value    REAL,
                unrealized_pnl  REAL DEFAULT 0,
                unrealized_pct  REAL DEFAULT 0,
                stop_loss       REAL,
                target_1        REAL,
                target_2        REAL,
                target_3        REAL,
                t1_hit          INTEGER DEFAULT 0,
                t2_hit          INTEGER DEFAULT 0,
                hold_type       TEXT,
                session_mode    TEXT,
                entry_reason    TEXT,
                conviction      REAL,
                mae             REAL DEFAULT 0,
                mfe             REAL DEFAULT 0,
                opened_at       TEXT DEFAULT (datetime('now')),
                last_updated    TEXT DEFAULT (datetime('now'))
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pt_orders (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker        TEXT NOT NULL,
                side          TEXT NOT NULL,
                order_type    TEXT NOT NULL,
                qty           REAL NOT NULL,
                limit_price   REAL,
                stop_price    REAL,
                filled_price  REAL,
                filled_at     TEXT,
                status        TEXT DEFAULT 'pending',
                fill_slippage REAL DEFAULT 0,
                commission    REAL DEFAULT 0,
                created_at    TEXT DEFAULT (datetime('now')),
                conviction_id INTEGER
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pt_trades (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker       TEXT NOT NULL,
                side         TEXT NOT NULL,
                shares       REAL NOT NULL,
                entry_price  REAL NOT NULL,
                exit_price   REAL NOT NULL,
                entry_at     TEXT,
                exit_at      TEXT DEFAULT (datetime('now')),
                hold_days    REAL,
                gross_pnl    REAL,
                net_pnl      REAL,
                pnl_pct      REAL,
                exit_reason  TEXT,
                hold_type    TEXT,
                session_mode TEXT,
                conviction   REAL,
                mae          REAL DEFAULT 0,
                mfe          REAL DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pt_equity_curve (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at   TEXT DEFAULT (datetime('now')),
                equity        REAL,
                cash          REAL,
                open_pnl      REAL,
                daily_pnl     REAL,
                daily_pnl_pct REAL,
                drawdown      REAL,
                drawdown_pct  REAL,
                session_mode  TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pt_daily_stats (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                date          TEXT UNIQUE,
                start_equity  REAL,
                end_equity    REAL,
                daily_pnl     REAL,
                daily_pnl_pct REAL,
                trades_opened INTEGER DEFAULT 0,
                trades_closed INTEGER DEFAULT 0,
                wins          INTEGER DEFAULT 0,
                losses        INTEGER DEFAULT 0,
                gross_volume  REAL DEFAULT 0,
                largest_win   REAL DEFAULT 0,
                largest_loss  REAL DEFAULT 0
            )
        """)
        conn.commit(); cur.close(); conn.close()
    print("  ✓ Paper trading tables initialized")


# ─── PaperBroker ─────────────────────────────────────────────────────────────

class PaperBroker:
    """
    Simulated broker with $100K starting balance.
    Handles order lifecycle, position management, stops/targets, P&L tracking.
    """

    def _get_account(self) -> dict:
        p = ph()
        conn = _conn(); cur = conn.cursor()
        cur.execute(f"SELECT balance, equity, day_start_equity, total_pnl, realized_pnl, "
                    f"unrealized_pnl, total_trades, winning_trades, losing_trades, "
                    f"win_rate, profit_factor, max_drawdown, peak_equity, sharpe "
                    f"FROM pt_account WHERE id = {p}", (1,))
        row = cur.fetchone(); cur.close(); conn.close()
        if not row:
            return {"balance": STARTING_BALANCE, "equity": STARTING_BALANCE,
                    "day_start_equity": STARTING_BALANCE, "peak_equity": STARTING_BALANCE,
                    "total_pnl": 0, "realized_pnl": 0, "unrealized_pnl": 0,
                    "total_trades": 0, "winning_trades": 0, "losing_trades": 0,
                    "win_rate": 0, "profit_factor": 0, "max_drawdown": 0, "sharpe": 0}
        cols = ["balance","equity","day_start_equity","total_pnl","realized_pnl",
                "unrealized_pnl","total_trades","winning_trades","losing_trades",
                "win_rate","profit_factor","max_drawdown","peak_equity","sharpe"]
        return dict(zip(cols, row))

    def submit_order(self, ticker: str, side: str, qty: float,
                     order_type: str = "market",
                     limit_price: float = None, stop_price: float = None,
                     conviction_id: int = None,
                     hold_type: str = None, session_mode: str = None,
                     conviction: float = None, entry_reason: str = None,
                     stop_loss: float = None, target_1: float = None,
                     target_2: float = None, target_3: float = None) -> Optional[int]:
        """Submit an order. Returns order_id or None on rejection."""
        qty = round(float(qty), 4)
        if qty <= 0:
            print(f"  [broker] Rejected {ticker}: qty={qty} ≤ 0")
            return None

        acct = self._get_account()
        if side == "buy":
            # Rough buying power check (market order uses current price)
            est_cost = qty * (limit_price or 999999)
            if limit_price is None:
                # Just check we have > $0 balance
                if acct["balance"] <= 0:
                    print(f"  [broker] Rejected {ticker}: insufficient balance")
                    return None
        elif side == "sell":
            # Check we have the shares
            pos = self._get_position(ticker)
            if not pos or _safe(pos.get("shares", 0)) < qty:
                print(f"  [broker] Rejected {ticker}: insufficient shares for sell")
                return None

        p = ph()
        conn = _conn(); cur = conn.cursor()
        if _is_pg():
            cur.execute("""
                INSERT INTO pt_orders (ticker, side, order_type, qty, limit_price,
                                       stop_price, status, conviction_id)
                VALUES (%s,%s,%s,%s,%s,%s,'pending',%s) RETURNING id
            """, (ticker.upper(), side, order_type, qty, _f(limit_price),
                  _f(stop_price), conviction_id))
            order_id = cur.fetchone()[0]
        else:
            cur.execute("""
                INSERT INTO pt_orders (ticker, side, order_type, qty, limit_price,
                                       stop_price, status, conviction_id)
                VALUES (?,?,?,?,?,?,'pending',?)
            """, (ticker.upper(), side, order_type, qty, _f(limit_price),
                  _f(stop_price), conviction_id))
            order_id = cur.lastrowid
        conn.commit(); cur.close(); conn.close()

        # Market orders fill immediately
        if order_type == "market":
            self._process_market_order(order_id, ticker, side, qty,
                                       hold_type=hold_type, session_mode=session_mode,
                                       conviction=conviction, entry_reason=entry_reason,
                                       stop_loss=stop_loss, target_1=target_1,
                                       target_2=target_2, target_3=target_3)
        return order_id

    def _process_market_order(self, order_id: int, ticker: str, side: str, qty: float,
                               **kwargs):
        """Fetch current price and fill at market ± slippage."""
        try:
            from resilient_fetcher import fetch_quote
            q = fetch_quote(ticker)
            price = q.price
        except Exception:
            try:
                import yfinance as yf
                info = yf.Ticker(ticker).info
                price = float(info.get("currentPrice") or info.get("regularMarketPrice") or 0)
            except Exception:
                price = 0.0

        if price <= 0:
            print(f"  [broker] Cannot fill {ticker}: price unavailable")
            return

        slippage = SLIP_MARKET if side == "buy" else -SLIP_MARKET
        fill_price = round(price * (1 + slippage), 4)
        self._execute_fill(order_id, ticker, side, qty, fill_price, SLIP_MARKET, **kwargs)

    def _execute_fill(self, order_id: int, ticker: str, side: str, qty: float,
                      fill_price: float, slip: float,
                      hold_type=None, session_mode=None, conviction=None,
                      entry_reason=None, stop_loss=None, target_1=None,
                      target_2=None, target_3=None):
        """Atomic: update cash, position, mark order filled."""
        p = ph()
        conn = _conn(); cur = conn.cursor()
        try:
            # Get current account balance
            cur.execute(f"SELECT balance FROM pt_account WHERE id = {p}", (1,))
            row = cur.fetchone()
            balance = float(row[0]) if row else STARTING_BALANCE

            if side == "buy":
                cost = qty * fill_price
                if cost > balance:
                    qty = round(balance / fill_price, 4)
                    cost = qty * fill_price
                    if qty <= 0:
                        print(f"  [broker] {ticker}: buy rejected — no balance")
                        return
                new_balance = balance - cost

                # Open or average-up position
                cur.execute(f"SELECT id, shares, avg_cost FROM pt_positions WHERE ticker = {p}", (ticker,))
                pos = cur.fetchone()
                now_str = _now().isoformat()
                if pos:
                    pos_id, old_shares, old_avg = pos[0], float(pos[1]), float(pos[2])
                    new_shares = old_shares + qty
                    new_avg    = (old_shares * old_avg + qty * fill_price) / new_shares
                    cur.execute(f"""
                        UPDATE pt_positions SET shares={p}, avg_cost={p},
                        last_updated={p} WHERE id={p}
                    """, (round(new_shares, 4), round(new_avg, 4), now_str, pos_id))
                else:
                    cur.execute(f"""
                        INSERT INTO pt_positions
                            (ticker, side, shares, avg_cost, stop_loss, target_1, target_2,
                             target_3, hold_type, session_mode, entry_reason, conviction,
                             opened_at, last_updated)
                        VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})
                    """, (ticker, "long", round(qty, 4), round(fill_price, 4),
                          _f(stop_loss), _f(target_1), _f(target_2), _f(target_3),
                          hold_type, session_mode, entry_reason, _f(conviction),
                          now_str, now_str))

                cur.execute(f"""
                    UPDATE pt_account SET balance={p}, updated_at={p},
                    total_trades = total_trades + 1 WHERE id=1
                """, (round(new_balance, 2), now_str))
                cur.execute(f"""
                    UPDATE pt_daily_stats SET trades_opened=trades_opened+1,
                    gross_volume=gross_volume+{p}
                    WHERE date={p}
                """, (round(cost, 2), date.today().isoformat()))

            elif side == "sell":
                cur.execute(f"SELECT id, shares, avg_cost, opened_at, hold_type, "
                            f"session_mode, conviction, mae, mfe FROM pt_positions "
                            f"WHERE ticker = {p}", (ticker,))
                pos = cur.fetchone()
                if not pos:
                    return
                pos_id, pos_shares, avg_cost = int(pos[0]), float(pos[1]), float(pos[2])
                opened_at, ht, sm, conv, mae, mfe = pos[3], pos[4], pos[5], pos[6], pos[7], pos[8]

                qty_sell  = min(qty, pos_shares)
                proceeds  = qty_sell * fill_price
                cost_basis = qty_sell * avg_cost
                gross_pnl  = proceeds - cost_basis
                net_pnl    = gross_pnl  # no commission
                pnl_pct    = (fill_price - avg_cost) / avg_cost * 100 if avg_cost > 0 else 0
                new_balance = balance + proceeds
                now_str     = _now().isoformat()

                remaining = round(pos_shares - qty_sell, 4)
                if remaining > 0.001:
                    cur.execute(f"UPDATE pt_positions SET shares={p}, last_updated={p} WHERE id={p}",
                                (remaining, now_str, pos_id))
                else:
                    cur.execute(f"DELETE FROM pt_positions WHERE id={p}", (pos_id,))
                    # Record closed trade
                    try:
                        hold_days = 0.0
                        if opened_at:
                            oa = datetime.fromisoformat(str(opened_at).replace("Z","")) if isinstance(opened_at, str) else opened_at
                            hold_days = round((_now() - oa.replace(tzinfo=None)).total_seconds() / 86400, 2)
                        cur.execute(f"""
                            INSERT INTO pt_trades
                                (ticker, side, shares, entry_price, exit_price, entry_at,
                                 gross_pnl, net_pnl, pnl_pct, hold_days,
                                 hold_type, session_mode, conviction, mae, mfe)
                            VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})
                        """, (ticker, "long", round(qty_sell, 4), round(avg_cost, 4),
                              round(fill_price, 4), str(opened_at), round(gross_pnl, 2),
                              round(net_pnl, 2), round(pnl_pct, 2), hold_days,
                              ht, sm, _f(conv), _f(mae), _f(mfe)))
                    except Exception as e:
                        print(f"  [broker] pt_trades insert failed: {e}")

                is_win = net_pnl > 0
                cur.execute(f"""
                    UPDATE pt_account SET
                        balance={p}, realized_pnl=realized_pnl+{p},
                        total_pnl=total_pnl+{p},
                        winning_trades=winning_trades+{p},
                        losing_trades=losing_trades+{p},
                        updated_at={p} WHERE id=1
                """, (round(new_balance, 2), round(net_pnl, 2), round(net_pnl, 2),
                      1 if is_win else 0, 0 if is_win else 1, now_str))
                cur.execute(f"""
                    UPDATE pt_daily_stats SET
                        trades_closed=trades_closed+1,
                        wins=wins+{p}, losses=losses+{p},
                        largest_win=MAX(largest_win,{p}),
                        largest_loss=MIN(largest_loss,{p})
                    WHERE date={p}
                """, (1 if is_win else 0, 0 if is_win else 1,
                      net_pnl if is_win else 0,
                      net_pnl if not is_win else 0,
                      date.today().isoformat()))

            # Mark order filled
            now_str2 = _now().isoformat()
            cur.execute(f"""
                UPDATE pt_orders SET status='filled', filled_price={p}, filled_at={p},
                fill_slippage={p} WHERE id={p}
            """, (round(fill_price, 4), now_str2, round(slip, 6), order_id))
            conn.commit()
        except Exception as e:
            print(f"  [broker] _execute_fill failed {ticker}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            cur.close(); conn.close()

        self._update_account_metrics()

    def process_pending_orders(self):
        """Check all pending limit/stop orders against current prices."""
        try:
            p = ph()
            conn = _conn(); cur = conn.cursor()
            cur.execute(f"SELECT id, ticker, side, order_type, qty, limit_price, stop_price "
                        f"FROM pt_orders WHERE status='pending' AND order_type != 'market'")
            orders = cur.fetchall(); cur.close(); conn.close()
        except Exception:
            return

        for row in orders:
            oid, ticker, side, otype, qty, lp, sp = row
            try:
                from resilient_fetcher import fetch_quote
                q = fetch_quote(ticker)
                price = q.price
                if price <= 0:
                    continue
            except Exception:
                continue

            fill = None
            slip = SLIP_LIMIT
            if otype == "limit" and lp:
                if side == "buy" and price <= float(lp):
                    fill = float(lp)
                elif side == "sell" and price >= float(lp):
                    fill = float(lp)
            elif otype == "stop" and sp:
                if price <= float(sp):
                    slip = SLIP_STOP
                    fill = price * (1 - SLIP_STOP)

            if fill is not None:
                self._execute_fill(int(oid), ticker, side, float(qty), fill, slip)

    def update_all_positions(self, session_mode: str = "MARKET"):
        """Refresh all open positions with latest prices. Called every scan cycle."""
        try:
            p = ph()
            conn = _conn(); cur = conn.cursor()
            cur.execute("SELECT id, ticker, shares, avg_cost, stop_loss, target_1, "
                        "target_2, target_3, t1_hit, t2_hit, hold_type, mae, mfe, "
                        "opened_at FROM pt_positions")
            positions = cur.fetchall(); cur.close(); conn.close()
        except Exception:
            return

        if not positions:
            self._snapshot_equity_curve(session_mode)
            return

        total_unrealized = 0.0
        now_str = _now().isoformat()

        for pos in positions:
            (pid, ticker, shares, avg_cost, stop_loss, t1, t2, t3,
             t1_hit, t2_hit, hold_type, mae, mfe, opened_at) = pos
            shares    = _safe(shares)
            avg_cost  = _safe(avg_cost)

            try:
                from resilient_fetcher import fetch_quote
                q = fetch_quote(ticker)
                if "stale_" in (q.source_quality or ""):
                    try:
                        mins = int(q.source_quality.split("stale_")[1].rstrip("m"))
                        if mins > 10:
                            continue
                    except Exception:
                        pass
                price = q.price
            except Exception:
                continue

            if price <= 0:
                continue

            unreal     = (price - avg_cost) * shares
            unreal_pct = (price - avg_cost) / avg_cost * 100 if avg_cost > 0 else 0
            mkt_val    = price * shares
            total_unrealized += unreal

            # Update MAE / MFE
            new_mae = min(_safe(mae, 0), unreal_pct)
            new_mfe = max(_safe(mfe, 0), unreal_pct)

            try:
                conn2 = _conn(); cur2 = conn2.cursor()
                cur2.execute(f"""
                    UPDATE pt_positions SET current_price={ph()}, market_value={ph()},
                    unrealized_pnl={ph()}, unrealized_pct={ph()},
                    mae={ph()}, mfe={ph()}, last_updated={ph()} WHERE id={ph()}
                """, (round(price,4), round(mkt_val,2), round(unreal,2),
                      round(unreal_pct,3), round(new_mae,3), round(new_mfe,3),
                      now_str, pid))
                conn2.commit(); cur2.close(); conn2.close()
            except Exception:
                pass

            # Daytrade close at 3:55 PM ET
            now_et = _now()
            if hold_type == "DAYTRADE" and now_et.hour == 15 and now_et.minute >= 55:
                self.submit_order(ticker, "sell", shares, "market",
                                  entry_reason="daytrade_eod")
                continue

            # Swing close after 5 trading days
            if hold_type == "SWING_2-5D" and opened_at:
                try:
                    oa = datetime.fromisoformat(str(opened_at).replace("Z",""))
                    if (_now() - oa.replace(tzinfo=None)).days >= 5:
                        self.submit_order(ticker, "sell", shares, "market",
                                          entry_reason="swing_expiry")
                        continue
                except Exception:
                    pass

            self._check_stops_and_targets(pid, ticker, price, shares, avg_cost,
                                          stop_loss, t1, t2, t3, t1_hit, t2_hit)

        # Update account unrealized P&L and equity
        try:
            conn3 = _conn(); cur3 = conn3.cursor()
            cur3.execute(f"SELECT balance FROM pt_account WHERE id = {ph()}", (1,))
            row = cur3.fetchone()
            balance = float(row[0]) if row else STARTING_BALANCE
            equity  = round(balance + total_unrealized, 2)
            cur3.execute(f"""
                UPDATE pt_account SET unrealized_pnl={ph()}, equity={ph()},
                total_pnl=realized_pnl+{ph()}, updated_at={ph()} WHERE id=1
            """, (round(total_unrealized,2), equity, round(total_unrealized,2), now_str))
            conn3.commit(); cur3.close(); conn3.close()
        except Exception as e:
            print(f"  [broker] equity update failed: {e}")

        self._update_drawdown()
        self._snapshot_equity_curve(session_mode)

    def _check_stops_and_targets(self, pid, ticker, price, shares, avg_cost,
                                  stop_loss, t1, t2, t3, t1_hit, t2_hit):
        """Trigger stop or target exits. Sends Pushover on each event."""
        stop_loss = _f(stop_loss)
        t1 = _f(t1); t2 = _f(t2); t3 = _f(t3)
        t1_hit = bool(t1_hit); t2_hit = bool(t2_hit)

        def _alert(title, msg):
            try:
                from alerts import send_alert, PRIORITY_HIGH
                send_alert(title=title, message=msg, priority=PRIORITY_HIGH)
            except Exception:
                pass

        pnl_pct = (price - avg_cost) / avg_cost * 100 if avg_cost > 0 else 0

        # Stop loss
        if stop_loss and price <= stop_loss:
            self.submit_order(ticker, "sell", shares, "stop",
                              stop_price=stop_loss, entry_reason="stop_hit")
            _alert(f"STOPPED: {ticker} {pnl_pct:+.1f}%",
                   f"Stop hit at ${price:.2f} (stop was ${stop_loss:.2f})")
            return

        # T3 — sell remaining
        if t3 and price >= t3 and t1_hit and t2_hit:
            self.submit_order(ticker, "sell", shares, "market", entry_reason="t3_hit")
            _alert(f"FULL TARGET: {ticker} {pnl_pct:+.1f}%",
                   f"T3 hit at ${price:.2f}")
            return

        # T2 — sell 1/3 more, already sold 1/3 at T1
        if t2 and price >= t2 and t1_hit and not t2_hit:
            sell_qty = round(shares / 3, 4)
            self.submit_order(ticker, "sell", sell_qty, "market", entry_reason="t2_hit")
            # Move stop to entry (breakeven protection already at T1)
            self._set_t2_hit(pid)
            _alert(f"TARGET 2: {ticker} {pnl_pct:+.1f}%",
                   f"T2 hit at ${price:.2f} — sold 1/3 more")
            return

        # T1 — sell 1/3, move stop to breakeven
        if t1 and price >= t1 and not t1_hit:
            sell_qty = round(shares / 3, 4)
            self.submit_order(ticker, "sell", sell_qty, "market", entry_reason="t1_hit")
            self._set_t1_hit(pid, avg_cost)  # move stop to breakeven
            _alert(f"TARGET 1: {ticker} {pnl_pct:+.1f}%",
                   f"T1 hit at ${price:.2f} — sold 1/3, stop → breakeven")

    def _set_t1_hit(self, pid: int, breakeven: float):
        try:
            conn = _conn(); cur = conn.cursor()
            cur.execute(f"UPDATE pt_positions SET t1_hit={ph()}, stop_loss={ph()} "
                        f"WHERE id={ph()}",
                        (True if _is_pg() else 1, round(breakeven, 4), pid))
            conn.commit(); cur.close(); conn.close()
        except Exception:
            pass

    def _set_t2_hit(self, pid: int):
        try:
            conn = _conn(); cur = conn.cursor()
            cur.execute(f"UPDATE pt_positions SET t2_hit={ph()} WHERE id={ph()}",
                        (True if _is_pg() else 1, pid))
            conn.commit(); cur.close(); conn.close()
        except Exception:
            pass

    def _update_account_metrics(self):
        """Recompute win_rate, profit_factor, sharpe from pt_trades."""
        try:
            conn = _conn(); cur = conn.cursor()
            cur.execute("SELECT net_pnl FROM pt_trades")
            rows = cur.fetchall(); cur.close(); conn.close()
        except Exception:
            return

        if not rows:
            return
        pnls = [float(r[0]) for r in rows if r[0] is not None]
        if not pnls:
            return

        wins   = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        n      = len(pnls)
        wr     = len(wins) / n if n else 0
        pf     = (sum(wins) / abs(sum(losses))) if losses and sum(losses) != 0 else 0
        mean_r = np.mean(pnls)
        std_r  = np.std(pnls) if len(pnls) > 1 else 1
        sharpe = (mean_r / std_r * np.sqrt(252)) if std_r > 0 else 0
        avg_win  = np.mean(wins)  if wins   else 0
        avg_loss = np.mean(losses) if losses else 0

        try:
            conn2 = _conn(); cur2 = conn2.cursor()
            cur2.execute(f"""
                UPDATE pt_account SET win_rate={ph()}, profit_factor={ph()},
                sharpe={ph()}, updated_at={ph()} WHERE id=1
            """, (round(wr,4), round(pf,4), round(float(sharpe),4), _now().isoformat()))
            conn2.commit(); cur2.close(); conn2.close()
        except Exception:
            pass

    def _update_drawdown(self):
        try:
            conn = _conn(); cur = conn.cursor()
            cur.execute(f"SELECT equity, peak_equity FROM pt_account WHERE id={ph()}", (1,))
            row = cur.fetchone()
            if not row:
                cur.close(); conn.close(); return
            equity, peak = float(row[0]), float(row[1])
            new_peak = max(equity, peak)
            dd = (new_peak - equity) / new_peak * 100 if new_peak > 0 else 0
            cur.execute(f"""
                UPDATE pt_account SET peak_equity={ph()},
                max_drawdown=GREATEST(max_drawdown,{ph()}),
                updated_at={ph()} WHERE id=1
            """ if _is_pg() else f"""
                UPDATE pt_account SET peak_equity={ph()},
                max_drawdown=MAX(max_drawdown,{ph()}),
                updated_at={ph()} WHERE id=1
            """, (round(new_peak,2), round(dd,4), _now().isoformat()))
            conn.commit(); cur.close(); conn.close()
        except Exception:
            pass

    def _snapshot_equity_curve(self, session_mode: str = "MARKET"):
        try:
            acct = self._get_account()
            equity  = acct["equity"]
            balance = acct["balance"]
            open_pnl = acct["unrealized_pnl"]
            day_start = acct["day_start_equity"]
            daily_pnl = equity - day_start
            daily_pct = (daily_pnl / day_start * 100) if day_start > 0 else 0
            peak      = acct["peak_equity"]
            dd        = (peak - equity) / peak * 100 if peak > 0 else 0
            conn = _conn(); cur = conn.cursor()
            cur.execute(f"""
                INSERT INTO pt_equity_curve
                    (equity, cash, open_pnl, daily_pnl, daily_pnl_pct, drawdown, drawdown_pct, session_mode)
                VALUES ({ph()},{ph()},{ph()},{ph()},{ph()},{ph()},{ph()},{ph()})
            """, (round(equity,2), round(balance,2), round(open_pnl,2),
                  round(daily_pnl,2), round(daily_pct,4), round(dd,4),
                  round(dd,4), session_mode))
            conn.commit(); cur.close(); conn.close()
        except Exception:
            pass

    def snapshot_daily_stats(self):
        """Called at 4:00 PM ET to record the day's stats."""
        try:
            acct    = self._get_account()
            equity  = acct["equity"]
            day_start = acct["day_start_equity"]
            daily_pnl = equity - day_start
            daily_pct = (daily_pnl / day_start * 100) if day_start > 0 else 0
            today_str = date.today().isoformat()

            conn = _conn(); cur = conn.cursor()
            if _is_pg():
                cur.execute("""
                    INSERT INTO pt_daily_stats (date, start_equity, end_equity, daily_pnl, daily_pnl_pct)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (date) DO UPDATE SET
                        end_equity=EXCLUDED.end_equity, daily_pnl=EXCLUDED.daily_pnl,
                        daily_pnl_pct=EXCLUDED.daily_pnl_pct
                """, (today_str, round(day_start,2), round(equity,2),
                      round(daily_pnl,2), round(daily_pct,4)))
            else:
                cur.execute("""
                    INSERT OR REPLACE INTO pt_daily_stats (date, start_equity, end_equity, daily_pnl, daily_pnl_pct)
                    VALUES (?,?,?,?,?)
                """, (today_str, round(day_start,2), round(equity,2),
                      round(daily_pnl,2), round(daily_pct,4)))
            # Reset day_start_equity for next day
            cur.execute(f"UPDATE pt_account SET day_start_equity={ph()}, updated_at={ph()} WHERE id=1",
                        (round(equity,2), _now().isoformat()))
            conn.commit(); cur.close(); conn.close()
        except Exception as e:
            print(f"  [broker] snapshot_daily_stats failed: {e}")

    def close_all_positions(self, reason: str = "manual"):
        """Close every open position at market. Used for EOD or manual close."""
        try:
            conn = _conn(); cur = conn.cursor()
            cur.execute("SELECT ticker, shares FROM pt_positions")
            rows = cur.fetchall(); cur.close(); conn.close()
        except Exception:
            return
        for ticker, shares in rows:
            self.submit_order(ticker, "sell", float(shares), "market",
                              entry_reason=reason)

    def get_dashboard_data(self) -> dict:
        """Returns all data needed for the Streamlit paper trading tab."""
        try:
            import pandas as pd
            acct  = self._get_account()
            equity      = acct["equity"]
            day_start   = acct["day_start_equity"]
            daily_pnl   = equity - day_start
            daily_pnl_pct = (daily_pnl / day_start * 100) if day_start > 0 else 0
            buying_power  = acct["balance"]
            total_pnl_pct = ((equity - STARTING_BALANCE) / STARTING_BALANCE * 100)

            conn = _conn(); cur = conn.cursor()

            # Positions
            cur.execute("""SELECT ticker, side, shares, avg_cost, current_price,
                market_value, unrealized_pnl, unrealized_pct, stop_loss,
                target_1, target_2, target_3, hold_type, opened_at, t1_hit, t2_hit
                FROM pt_positions ORDER BY market_value DESC""")
            pos_rows = cur.fetchall()
            pos_cols = ["ticker","side","shares","avg_cost","current_price","market_value",
                        "unrealized_pnl","unrealized_pct","stop_loss","target_1","target_2",
                        "target_3","hold_type","opened_at","t1_hit","t2_hit"]
            positions = pd.DataFrame(pos_rows, columns=pos_cols) if pos_rows else pd.DataFrame(columns=pos_cols)

            # Recent trades
            cur.execute("""SELECT ticker, side, shares, entry_price, exit_price,
                hold_days, gross_pnl, pnl_pct, exit_reason, exit_at, hold_type, conviction
                FROM pt_trades ORDER BY exit_at DESC LIMIT 50""")
            trade_rows = cur.fetchall()
            trade_cols = ["ticker","side","shares","entry_price","exit_price",
                          "hold_days","gross_pnl","pnl_pct","exit_reason","exit_at","hold_type","conviction"]
            trades = pd.DataFrame(trade_rows, columns=trade_cols) if trade_rows else pd.DataFrame(columns=trade_cols)

            # Equity curve
            cur.execute("""SELECT recorded_at, equity, daily_pnl, daily_pnl_pct,
                drawdown_pct, session_mode FROM pt_equity_curve
                ORDER BY recorded_at DESC LIMIT 1000""")
            eq_rows = cur.fetchall()
            eq_cols = ["recorded_at","equity","daily_pnl","daily_pnl_pct","drawdown_pct","session_mode"]
            equity_curve = pd.DataFrame(eq_rows[::-1], columns=eq_cols) if eq_rows else pd.DataFrame(columns=eq_cols)

            # Daily stats
            cur.execute("""SELECT date, start_equity, end_equity, daily_pnl, daily_pnl_pct,
                trades_opened, trades_closed, wins, losses
                FROM pt_daily_stats ORDER BY date DESC LIMIT 30""")
            ds_rows = cur.fetchall()
            ds_cols = ["date","start_equity","end_equity","daily_pnl","daily_pnl_pct",
                       "trades_opened","trades_closed","wins","losses"]
            daily_stats = pd.DataFrame(ds_rows, columns=ds_cols) if ds_rows else pd.DataFrame(columns=ds_cols)

            # Pending orders
            cur.execute("SELECT id, ticker, side, order_type, qty, limit_price, stop_price, status "
                        "FROM pt_orders WHERE status='pending' ORDER BY created_at DESC")
            ord_rows = cur.fetchall()
            ord_cols = ["id","ticker","side","order_type","qty","limit_price","stop_price","status"]
            pending_orders = pd.DataFrame(ord_rows, columns=ord_cols) if ord_rows else pd.DataFrame(columns=ord_cols)

            # Per-hold-type breakdown
            ht_stats = {}
            if not trades.empty:
                for ht in trades["hold_type"].dropna().unique():
                    g = trades[trades["hold_type"] == ht]
                    ht_stats[ht] = {
                        "n":       len(g),
                        "win_rate": round((g["pnl_pct"] > 0).mean() * 100, 1),
                        "avg_pnl": round(g["pnl_pct"].mean(), 2),
                        "total_pnl": round(g["gross_pnl"].sum(), 2),
                    }

            cur.close(); conn.close()

            # Best/worst ticker
            best_ticker = worst_ticker = None
            if not trades.empty:
                by_tick = trades.groupby("ticker")["gross_pnl"].sum()
                best_ticker  = by_tick.idxmax() if not by_tick.empty else None
                worst_ticker = by_tick.idxmin() if not by_tick.empty else None

            avg_win  = round(trades.loc[trades["pnl_pct"] > 0, "pnl_pct"].mean(), 2) if not trades.empty else 0
            avg_loss = round(trades.loc[trades["pnl_pct"] <= 0, "pnl_pct"].mean(), 2) if not trades.empty else 0
            avg_hold = round(trades["hold_days"].mean(), 1) if not trades.empty and "hold_days" in trades.columns else 0

            return {
                "balance":        round(buying_power, 2),
                "equity":         round(equity, 2),
                "daily_pnl":      round(daily_pnl, 2),
                "daily_pnl_pct":  round(daily_pnl_pct, 2),
                "total_pnl":      round(acct["total_pnl"], 2),
                "total_pnl_pct":  round(total_pnl_pct, 2),
                "realized_pnl":   round(acct["realized_pnl"], 2),
                "unrealized_pnl": round(acct["unrealized_pnl"], 2),
                "buying_power":   round(buying_power, 2),
                "total_trades":   acct["total_trades"],
                "win_rate":       round(acct["win_rate"] * 100, 1),
                "profit_factor":  round(acct["profit_factor"], 3),
                "sharpe":         round(acct["sharpe"], 3),
                "max_drawdown":   round(acct["max_drawdown"], 2),
                "avg_win":        avg_win,
                "avg_loss":       avg_loss,
                "avg_hold_days":  avg_hold,
                "largest_win":    round(trades["gross_pnl"].max(), 2) if not trades.empty else 0,
                "largest_loss":   round(trades["gross_pnl"].min(), 2) if not trades.empty else 0,
                "best_ticker":    best_ticker,
                "worst_ticker":   worst_ticker,
                "positions":      positions,
                "trades":         trades,
                "equity_curve":   equity_curve,
                "daily_stats":    daily_stats,
                "pending_orders": pending_orders,
                "by_hold_type":   ht_stats,
                "starting_balance": STARTING_BALANCE,
            }
        except Exception as e:
            print(f"  [broker] get_dashboard_data failed: {e}")
            return {"equity": STARTING_BALANCE, "balance": STARTING_BALANCE,
                    "total_pnl": 0, "daily_pnl": 0, "positions": None, "trades": None,
                    "equity_curve": None, "daily_stats": None, "pending_orders": None,
                    "by_hold_type": {}}

    def cancel_order(self, order_id: int):
        try:
            conn = _conn(); cur = conn.cursor()
            cur.execute(f"UPDATE pt_orders SET status='cancelled' WHERE id={ph()} "
                        f"AND status='pending'", (order_id,))
            conn.commit(); cur.close(); conn.close()
        except Exception:
            pass


# Module-level singleton
_broker: Optional[PaperBroker] = None


def get_broker() -> PaperBroker:
    global _broker
    if _broker is None:
        _broker = PaperBroker()
    return _broker
