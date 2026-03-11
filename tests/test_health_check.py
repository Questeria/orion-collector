"""
Tests for health_check.py — tape freshness monitoring, CRC verification,
and status reporting.

Verifies that the health check correctly identifies healthy tapes,
stale tapes, missing tapes, and corrupted records.
"""

import json
import os
import sys
import time
import zlib
import pytest
from pathlib import Path

# ── Import health_check module ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import health_check


# ═══════════════════════════════════════════════════════════════
#  Helper: build records in health_check's expected format
# ═══════════════════════════════════════════════════════════════

def make_hc_record(seq: int, ts_us: int = None, src: str = "cb") -> str:
    """Build a JSONL record in the format health_check.py expects.

    health_check.py reads records with:
      - ts_us  (microseconds since epoch)
      - raw    (dict — the inner message, stored as a JSON object)
      - crc    (CRC32 of json.dumps(raw, separators=(',', ':'), ensure_ascii=False))
      - seq    (sequence number)

    verify_crc() calls json.dumps(record["raw"], ...) to recompute the CRC,
    so raw must be a dict (not a pre-serialized string).
    """
    if ts_us is None:
        ts_us = int(time.time() * 1_000_000)  # current time in microseconds

    raw_msg = {"type": "ticker", "price": "83000.50", "product_id": "BTC-USD"}

    # CRC is computed on json.dumps(raw_dict, separators=(",", ":"), ensure_ascii=False)
    raw_payload = json.dumps(raw_msg, separators=(",", ":"), ensure_ascii=False)
    crc = zlib.crc32(raw_payload.encode("utf-8")) & 0xFFFFFFFF

    record = {
        "seq": seq,
        "ts_us": ts_us,
        "src": src,
        "raw": raw_msg,  # Store as dict — verify_crc will json.dumps() it
        "crc": crc,
    }
    return json.dumps(record, separators=(",", ":"))


def write_hc_tape(tape_path: Path, num_records: int = 100,
                  base_ts_us: int = None, corrupt_indices: list = None):
    """Write a tape file with records in health_check's expected format."""
    if corrupt_indices is None:
        corrupt_indices = []
    if base_ts_us is None:
        base_ts_us = int(time.time() * 1_000_000) - (num_records * 1_000_000)

    lines = []
    for i in range(num_records):
        ts_us = base_ts_us + (i * 1_000_000)  # 1 second apart in microseconds
        line = make_hc_record(seq=i + 1, ts_us=ts_us)

        if i in corrupt_indices:
            # Corrupt by changing raw data after CRC was computed
            record = json.loads(line)
            record["raw"]["price"] = "99999.99"  # Change price — CRC won't match
            line = json.dumps(record, separators=(",", ":"))

        lines.append(line)

    tape_path.parent.mkdir(parents=True, exist_ok=True)
    with open(tape_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# ═══════════════════════════════════════════════════════════════
#  Tape Health Detection
# ═══════════════════════════════════════════════════════════════

class TestTapeHealth:
    """Verify that check_tape correctly assesses tape status."""

    def test_missing_tape_detected(self, tmp_path):
        """A missing tape file should be reported as MISSING."""
        fake_path = tmp_path / "nonexistent.jsonl"
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir()

        result = health_check.check_tape("Test", fake_path, archive_dir)
        assert result["status"] == "MISSING"

    def test_empty_tape_detected(self, tmp_path):
        """An empty tape file should be reported as EMPTY."""
        tape_path = tmp_path / "empty_tape.jsonl"
        tape_path.touch()  # Create empty file
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir()

        result = health_check.check_tape("Test", tape_path, archive_dir)
        assert result["status"] == "EMPTY"

    def test_fresh_tape_is_healthy(self, tmp_path):
        """A tape with recent records should be reported as HEALTHY."""
        tape_path = tmp_path / "fresh_tape.jsonl"
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir()

        # Write records with current timestamps (ts_us in microseconds)
        now_us = int(time.time() * 1_000_000)
        write_hc_tape(tape_path, num_records=10, base_ts_us=now_us - 5_000_000)

        result = health_check.check_tape("Test", tape_path, archive_dir)
        assert result["status"] == "HEALTHY", f"Expected HEALTHY, got {result['status']}"

    def test_stale_tape_detected(self, tmp_path):
        """A tape with old records should be reported as STALE."""
        tape_path = tmp_path / "stale_tape.jsonl"
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir()

        # Write records with timestamps from 30 seconds ago (>10s = STALE, <60s)
        old_us = int(time.time() * 1_000_000) - 30_000_000
        write_hc_tape(tape_path, num_records=10, base_ts_us=old_us - 10_000_000)

        result = health_check.check_tape("Test", tape_path, archive_dir)
        assert result["status"] == "STALE", f"Expected STALE, got {result['status']}"


# ═══════════════════════════════════════════════════════════════
#  CRC32 Verification via health_check.verify_crc
# ═══════════════════════════════════════════════════════════════

class TestVerifyCRC:
    """Verify the health check's CRC32 verification function."""

    def test_all_valid_records_pass(self, tmp_path):
        """100 valid records should all pass CRC verification."""
        tape_path = tmp_path / "valid_tape.jsonl"
        write_hc_tape(tape_path, num_records=100)

        result = health_check.verify_crc(tape_path, count=100)
        assert result["total"] == 100
        assert result["valid"] == 100
        assert result["invalid"] == 0

    def test_corrupt_records_detected(self, tmp_path):
        """Corrupted records should be flagged by CRC verification."""
        tape_path = tmp_path / "corrupt_tape.jsonl"
        write_hc_tape(tape_path, num_records=50, corrupt_indices=[5, 15, 30])

        result = health_check.verify_crc(tape_path, count=50)
        assert result["invalid"] == 3, f"Expected 3 invalid, got {result['invalid']}"

    def test_empty_tape_returns_zero(self, tmp_path):
        """An empty tape should return 0 for all counts."""
        tape_path = tmp_path / "empty_tape.jsonl"
        tape_path.touch()

        result = health_check.verify_crc(tape_path, count=100)
        assert result["total"] == 0

    def test_partial_count(self, tmp_path):
        """Requesting fewer records than exist should only check that many."""
        tape_path = tmp_path / "big_tape.jsonl"
        write_hc_tape(tape_path, num_records=200)

        result = health_check.verify_crc(tape_path, count=50)
        assert result["total"] == 50


# ═══════════════════════════════════════════════════════════════
#  Output Formatting
# ═══════════════════════════════════════════════════════════════

class TestFormatting:
    """Verify health check output formatting."""

    def test_format_missing_result(self):
        """MISSING status should format without errors."""
        result = {
            "label": "Test",
            "status": "MISSING",
            "age_ms": None,
            "size_mb": 0,
            "last_seq": None,
            "archive_count": 0,
            "archive_mb": 0,
            "last_symbol": None,
            "last_price": None,
        }
        output = health_check.format_result(result)
        assert "MISSING" in output

    def test_format_healthy_result(self):
        """HEALTHY status should include size and age info."""
        result = {
            "label": "Test",
            "status": "HEALTHY",
            "age_ms": 2500,
            "size_mb": 15.3,
            "last_seq": 42,
            "archive_count": 3,
            "archive_mb": 100.5,
            "last_symbol": "BTC-USD",
            "last_price": "83000.50",
        }
        output = health_check.format_result(result)
        assert "HEALTHY" in output
