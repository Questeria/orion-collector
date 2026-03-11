"""
Tests for orion_collector.py — settlement timing, ticker prediction,
config loading, and pure utility functions.

These test the collector's core logic without requiring API keys
or live WebSocket connections.
"""

import os
import sys
import time
import pytest
from pathlib import Path
from datetime import datetime, timezone, timedelta

# ── Import collector module (selective — skip anything that needs auth) ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# We need to import specific functions without triggering the full module init.
# The collector has try/except blocks for optional deps, so this should work.
# But _init_auth() is only called at runtime, not at import time.


# ═══════════════════════════════════════════════════════════════
#  Settlement Timing
# ═══════════════════════════════════════════════════════════════

class TestSettlementTiming:
    """Verify 15-minute settlement window calculations."""

    def test_import_settlement_functions(self):
        """Should be able to import settlement functions."""
        from orion_collector import (
            seconds_since_last_settlement,
            get_next_15m_settlements,
        )

    def test_seconds_at_quarter_start(self):
        """At exactly :00, :15, :30, :45 → elapsed should be 0."""
        from orion_collector import seconds_since_last_settlement

        for minute in [0, 15, 30, 45]:
            t = datetime(2026, 3, 10, 12, minute, 0, tzinfo=timezone.utc)
            assert seconds_since_last_settlement(t) == 0, f"At :{minute:02d} should be 0"

    def test_seconds_mid_window(self):
        """At 12:47:20 → last settlement was 12:45 → should be 140 seconds."""
        from orion_collector import seconds_since_last_settlement

        t = datetime(2026, 3, 10, 12, 47, 20, tzinfo=timezone.utc)
        assert seconds_since_last_settlement(t) == 140

    def test_seconds_just_before_settlement(self):
        """At 12:14:59 → last settlement was 12:00 → should be 14*60+59 = 899."""
        from orion_collector import seconds_since_last_settlement

        t = datetime(2026, 3, 10, 12, 14, 59, tzinfo=timezone.utc)
        assert seconds_since_last_settlement(t) == 899

    def test_seconds_one_second_after(self):
        """At 12:15:01 → last settlement was 12:15 → should be 1."""
        from orion_collector import seconds_since_last_settlement

        t = datetime(2026, 3, 10, 12, 15, 1, tzinfo=timezone.utc)
        assert seconds_since_last_settlement(t) == 1

    def test_next_settlements_count(self):
        """get_next_15m_settlements should return the requested count."""
        from orion_collector import get_next_15m_settlements

        t = datetime(2026, 3, 10, 12, 3, 0, tzinfo=timezone.utc)
        result = get_next_15m_settlements(t, count=4)
        assert len(result) == 4

    def test_next_settlements_are_15min_apart(self):
        """Consecutive settlements should be exactly 15 minutes apart."""
        from orion_collector import get_next_15m_settlements

        t = datetime(2026, 3, 10, 12, 3, 0, tzinfo=timezone.utc)
        result = get_next_15m_settlements(t, count=5)

        for i in range(1, len(result)):
            delta = result[i] - result[i - 1]
            assert delta == timedelta(minutes=15), f"Gap between {i-1} and {i} should be 15min"

    def test_next_settlement_is_in_future(self):
        """The first next settlement should be after 'now'."""
        from orion_collector import get_next_15m_settlements

        t = datetime(2026, 3, 10, 12, 3, 0, tzinfo=timezone.utc)
        result = get_next_15m_settlements(t, count=1)
        assert result[0] > t, "Next settlement should be in the future"

    def test_next_settlement_at_boundary(self):
        """At exactly :45, the next settlement should be :00 of next hour."""
        from orion_collector import get_next_15m_settlements

        t = datetime(2026, 3, 10, 12, 45, 0, tzinfo=timezone.utc)
        result = get_next_15m_settlements(t, count=1)
        expected = datetime(2026, 3, 10, 13, 0, 0, tzinfo=timezone.utc)
        assert result[0] == expected


# ═══════════════════════════════════════════════════════════════
#  Ticker Prediction
# ═══════════════════════════════════════════════════════════════

class TestTickerPrediction:
    """Verify 15M ticker name generation."""

    def test_ticker_format(self):
        """Predicted tickers should follow the KX{SYM}15M-... format."""
        from orion_collector import predict_15m_tickers

        settlement = datetime(2026, 3, 10, 17, 15, 0, tzinfo=timezone.utc)
        tickers = predict_15m_tickers(["BTC"], settlement)

        assert len(tickers) == 1
        assert tickers[0].startswith("KXBTC15M-")

    def test_multiple_symbols(self):
        """Should generate one ticker per symbol."""
        from orion_collector import predict_15m_tickers

        settlement = datetime(2026, 3, 10, 17, 15, 0, tzinfo=timezone.utc)
        tickers = predict_15m_tickers(["BTC", "ETH", "SOL"], settlement)

        assert len(tickers) == 3
        assert any("BTC" in t for t in tickers)
        assert any("ETH" in t for t in tickers)
        assert any("SOL" in t for t in tickers)


# ═══════════════════════════════════════════════════════════════
#  Env File Loading
# ═══════════════════════════════════════════════════════════════

class TestEnvLoading:
    """Verify .env file parsing."""

    def test_load_env_file(self, tmp_path):
        """Should parse key=value pairs from a .env file."""
        from orion_collector import _load_env

        env_file = tmp_path / "test.env"
        env_file.write_text(
            "# Comment line\n"
            "KEY_ONE=value_one\n"
            "KEY_TWO=value_two\n"
            "\n"
            "KEY_THREE=value with spaces\n"
        )

        result = _load_env(env_file)

        assert result["KEY_ONE"] == "value_one"
        assert result["KEY_TWO"] == "value_two"
        assert result["KEY_THREE"] == "value with spaces"

    def test_missing_env_file(self, tmp_path):
        """Should return empty dict for a missing .env file."""
        from orion_collector import _load_env

        result = _load_env(tmp_path / "nonexistent.env")
        assert result == {}

    def test_env_first_returns_first_match(self, tmp_path):
        """_env_first should return the first non-empty env var."""
        from orion_collector import _env_first

        os.environ["TEST_KEY_A"] = ""
        os.environ["TEST_KEY_B"] = "found_it"

        try:
            result = _env_first("TEST_KEY_A", "TEST_KEY_B")
            assert result == "found_it"
        finally:
            del os.environ["TEST_KEY_A"]
            del os.environ["TEST_KEY_B"]

    def test_env_first_returns_none_if_all_empty(self, tmp_path):
        """_env_first should return None if no keys have values."""
        from orion_collector import _env_first

        # Make sure these don't exist
        for key in ["FAKE_KEY_1", "FAKE_KEY_2"]:
            os.environ.pop(key, None)

        result = _env_first("FAKE_KEY_1", "FAKE_KEY_2")
        assert result is None


# ═══════════════════════════════════════════════════════════════
#  Config Loading
# ═══════════════════════════════════════════════════════════════

class TestConfig:
    """Verify config value retrieval."""

    def test_cfg_returns_default(self):
        """_cfg should return default when key is missing."""
        from orion_collector import _cfg

        result = _cfg("nonexistent_section", "nonexistent_key", "fallback")
        assert result == "fallback"
