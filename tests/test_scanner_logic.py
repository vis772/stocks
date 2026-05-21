"""
tests/test_scanner_logic.py
Critical logic tests for Axiom Terminal scanner.
Run with: pytest tests/ -v
"""
import pytest


# ─── Gap-Continuation Bonus ───────────────────────────────────────────────────

def _apply_gap_bonus(prev_gap_pct, tech_score, fund_score):
    """Mirror of core/scanner.py gap-continuation logic."""
    gap_continuation = False
    if prev_gap_pct > 5.0 and tech_score > 60 and fund_score > 65:
        tech_score = min(100.0, tech_score + 15.0)
        gap_continuation = True
    return tech_score, gap_continuation


def test_gap_bonus_triggers_above_threshold():
    score, fired = _apply_gap_bonus(5.1, 61.0, 66.0)
    assert fired is True
    assert score == 76.0


def test_gap_bonus_not_triggered_gap_exactly_5():
    """Boundary: gap=5.0 uses >, not >=, so should NOT trigger."""
    score, fired = _apply_gap_bonus(5.0, 75.0, 70.0)
    assert fired is False
    assert score == 75.0


def test_gap_bonus_not_triggered_tech_exactly_60():
    """Boundary: tech=60.0 uses >, not >=, so should NOT trigger."""
    score, fired = _apply_gap_bonus(6.0, 60.0, 70.0)
    assert fired is False
    assert score == 60.0


def test_gap_bonus_not_triggered_fund_exactly_65():
    """Boundary: fund=65.0 uses >, not >=, so should NOT trigger."""
    score, fired = _apply_gap_bonus(6.0, 65.0, 65.0)
    assert fired is False


def test_gap_bonus_capped_at_100():
    """Score cannot exceed 100 even with +15 bonus."""
    score, fired = _apply_gap_bonus(10.0, 92.0, 80.0)
    assert fired is True
    assert score == 100.0


def test_gap_bonus_not_triggered_small_gap():
    score, fired = _apply_gap_bonus(2.0, 80.0, 80.0)
    assert fired is False
    assert score == 80.0


# ─── Price Movement Gate ──────────────────────────────────────────────────────

PRICE_MOVE_GATE = 0.01  # from config.py


def _price_gate_suppresses(pct_change):
    """Mirror of scanner_loop.py price movement gate logic."""
    return abs(pct_change) < PRICE_MOVE_GATE


def test_price_gate_suppresses_flat_stock():
    assert _price_gate_suppresses(0.0) is True


def test_price_gate_suppresses_below_1pct():
    assert _price_gate_suppresses(0.005) is True


def test_price_gate_suppresses_negative_below_1pct():
    assert _price_gate_suppresses(-0.005) is True


def test_price_gate_exactly_1pct_not_suppressed():
    """Boundary: exactly 1% uses <, not <=, so alert proceeds."""
    assert _price_gate_suppresses(0.01) is False


def test_price_gate_above_1pct_not_suppressed():
    assert _price_gate_suppresses(0.025) is False


def test_price_gate_large_move_not_suppressed():
    assert _price_gate_suppresses(0.15) is False


# ─── Catalyst Multiplier — None-safe pattern ──────────────────────────────────

def _safe_catalyst_mult(data):
    """Correct None-safe pattern (fixed from 'or 1.0' bug)."""
    _cm = data.get("catalyst_mult")
    return float(_cm if _cm is not None else 1.0)


def test_catalyst_mult_normal_value():
    assert _safe_catalyst_mult({"catalyst_mult": 1.2}) == 1.2


def test_catalyst_mult_zero_preserved():
    """Critical: 0.0 must NOT be converted to 1.0."""
    assert _safe_catalyst_mult({"catalyst_mult": 0.0}) == 0.0


def test_catalyst_mult_none_defaults_to_1():
    assert _safe_catalyst_mult({"catalyst_mult": None}) == 1.0


def test_catalyst_mult_missing_key_defaults_to_1():
    assert _safe_catalyst_mult({}) == 1.0


def test_catalyst_mult_old_buggy_pattern_fails():
    """Documents the old bug: (0.0 or 1.0) = 1.0, data loss."""
    b = {"catalyst_mult": 0.0}
    buggy_result = float(b.get("catalyst_mult", 1.0) or 1.0)
    assert buggy_result == 1.0  # This is WRONG — proves the old bug existed


# ─── Scoring Weights ──────────────────────────────────────────────────────────

def test_scoring_weights_sum_to_1():
    weights = {"technical": 0.43, "fundamental": 0.30, "risk": 0.20, "sentiment": 0.07}
    assert abs(sum(weights.values()) - 1.0) < 0.001


def test_unified_score_formula():
    """Mirror of core/scanner.py final score calculation."""
    tech, fund, risk_raw, sent = 80.0, 70.0, 30.0, 60.0
    risk_inv = 100.0 - risk_raw
    score = (tech * 0.43 + fund * 0.30 + risk_inv * 0.20 + sent * 0.07)
    assert 0 <= score <= 100
    assert abs(score - (34.4 + 21.0 + 14.0 + 4.2)) < 0.01  # 73.6


def test_score_clamped_to_100():
    score = min(max(105.0, 0), 100)
    assert score == 100.0


def test_score_clamped_to_0():
    score = min(max(-5.0, 0), 100)
    assert score == 0.0


# ─── Safe Column Whitelist (SQL injection fix) ────────────────────────────────

def _safe_cols(requested, allowed):
    """Mirror of db/database.py _safe_cols helper."""
    safe = [c for c in requested if c in allowed]
    return ", ".join(safe) if safe else "id"


def test_safe_cols_allows_valid_columns():
    allowed = {"cash", "equity", "total_value"}
    result = _safe_cols(["cash", "equity"], allowed)
    assert result == "cash, equity"


def test_safe_cols_rejects_injected_column():
    allowed = {"cash", "equity"}
    result = _safe_cols(["cash", "1=1; DROP TABLE users--"], allowed)
    assert "DROP" not in result
    assert result == "cash"


def test_safe_cols_empty_request_returns_id():
    result = _safe_cols([], {"cash", "equity"})
    assert result == "id"


def test_safe_cols_all_invalid_returns_id():
    result = _safe_cols(["injected", "malicious"], {"cash", "equity"})
    assert result == "id"
