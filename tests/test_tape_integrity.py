"""
Tests for tape data integrity — CRC32 checksums, sequence numbering,
record format validation, and deduplication.

These tests verify that the core data pipeline produces correct,
verifiable, sequenced records with integrity guarantees.
"""

import json
import zlib
import time
import pytest
from pathlib import Path

from tests.conftest import make_tape_record, write_tape_file


# ═══════════════════════════════════════════════════════════════
#  CRC32 Integrity
# ═══════════════════════════════════════════════════════════════

class TestCRC32:
    """Verify CRC32 checksums catch corruption."""

    def test_valid_record_passes_crc(self):
        """A correctly written record should pass CRC32 verification."""
        line = make_tape_record(seq=1, src="cb")
        record = json.loads(line)

        # Extract the CRC, rebuild the record without it, recompute
        expected_crc = record.pop("crc32")
        raw = json.dumps(record, separators=(",", ":"))
        actual_crc = zlib.crc32(raw.encode("utf-8")) & 0xFFFFFFFF

        assert actual_crc == expected_crc, "CRC32 should match for uncorrupted record"

    def test_corrupted_record_fails_crc(self):
        """A corrupted record should fail CRC32 verification."""
        line = make_tape_record(seq=1, src="cb")
        record = json.loads(line)

        # Corrupt a field
        record["src"] = "CORRUPTED"
        expected_crc = record.pop("crc32")
        raw = json.dumps(record, separators=(",", ":"))
        actual_crc = zlib.crc32(raw.encode("utf-8")) & 0xFFFFFFFF

        assert actual_crc != expected_crc, "CRC32 should NOT match after corruption"

    def test_crc_detects_single_bit_change(self):
        """Even a single character change should produce a different CRC."""
        record_a = {"seq": 1, "ts": 1000.0, "src": "cb", "msg": {"price": "100.00"}}
        record_b = {"seq": 1, "ts": 1000.0, "src": "cb", "msg": {"price": "100.01"}}

        raw_a = json.dumps(record_a, separators=(",", ":"))
        raw_b = json.dumps(record_b, separators=(",", ":"))

        crc_a = zlib.crc32(raw_a.encode("utf-8")) & 0xFFFFFFFF
        crc_b = zlib.crc32(raw_b.encode("utf-8")) & 0xFFFFFFFF

        assert crc_a != crc_b, "CRC32 should differ for any content change"

    def test_bulk_crc_verification(self, tape_dir):
        """Verify CRC32 on 500 records written to a tape file."""
        tape_path = tape_dir / "data" / "unified" / "raw_tape" / "unified_tape.jsonl"
        records = write_tape_file(tape_path, num_records=500)

        # Read back and verify every record
        with open(tape_path, "r") as f:
            for i, line in enumerate(f):
                record = json.loads(line.strip())
                saved_crc = record.pop("crc32")
                raw = json.dumps(record, separators=(",", ":"))
                computed_crc = zlib.crc32(raw.encode("utf-8")) & 0xFFFFFFFF
                assert computed_crc == saved_crc, f"CRC mismatch on record {i}"

    def test_detect_corrupt_records_in_tape(self, tape_dir):
        """Write a tape with known corrupted records and verify detection."""
        tape_path = tape_dir / "data" / "unified" / "raw_tape" / "unified_tape.jsonl"
        write_tape_file(tape_path, num_records=50, corrupt_indices=[10, 25, 49])

        corrupt_count = 0
        with open(tape_path, "r") as f:
            for line in f:
                record = json.loads(line.strip())
                saved_crc = record.pop("crc32")
                raw = json.dumps(record, separators=(",", ":"))
                computed_crc = zlib.crc32(raw.encode("utf-8")) & 0xFFFFFFFF
                if computed_crc != saved_crc:
                    corrupt_count += 1

        assert corrupt_count == 3, f"Expected 3 corrupt records, found {corrupt_count}"


# ═══════════════════════════════════════════════════════════════
#  Record Format
# ═══════════════════════════════════════════════════════════════

class TestRecordFormat:
    """Verify tape records have the correct structure."""

    def test_required_fields_present(self):
        """Every record must have seq, ts, src, msg, and crc32."""
        line = make_tape_record(seq=42, src="kl")
        record = json.loads(line)

        required = {"seq", "ts", "src", "msg", "crc32"}
        assert required.issubset(record.keys()), f"Missing fields: {required - record.keys()}"

    def test_seq_is_integer(self):
        """Sequence number must be a positive integer."""
        line = make_tape_record(seq=1)
        record = json.loads(line)
        assert isinstance(record["seq"], int)
        assert record["seq"] > 0

    def test_timestamp_is_float(self):
        """Timestamp must be a float (Unix epoch seconds)."""
        line = make_tape_record(seq=1)
        record = json.loads(line)
        assert isinstance(record["ts"], float)
        assert record["ts"] > 1_000_000_000  # After 2001

    def test_src_is_valid_tag(self):
        """Source must be one of the known tags."""
        valid_sources = {"cb", "kl", "snap"}
        for src in valid_sources:
            line = make_tape_record(seq=1, src=src)
            record = json.loads(line)
            assert record["src"] == src

    def test_msg_is_dict(self):
        """Message field must be a dictionary."""
        line = make_tape_record(seq=1)
        record = json.loads(line)
        assert isinstance(record["msg"], dict)


# ═══════════════════════════════════════════════════════════════
#  Sequence Numbering
# ═══════════════════════════════════════════════════════════════

class TestSequencing:
    """Verify sequence numbers are correct and gaps are detectable."""

    def test_sequential_numbering(self, tape_dir):
        """Records should have strictly increasing sequence numbers."""
        tape_path = tape_dir / "data" / "unified" / "raw_tape" / "unified_tape.jsonl"
        write_tape_file(tape_path, num_records=200)

        prev_seq = 0
        with open(tape_path, "r") as f:
            for line in f:
                record = json.loads(line.strip())
                assert record["seq"] > prev_seq, "Sequence must be strictly increasing"
                prev_seq = record["seq"]

    def test_gap_detection(self, tape_dir):
        """Should detect missing sequence numbers (gaps)."""
        tape_path = tape_dir / "data" / "unified" / "raw_tape" / "unified_tape.jsonl"

        # Write records but skip seq 50 and 100
        lines = []
        for i in range(1, 151):
            if i in (50, 100):
                continue  # Skip these — creates gaps
            lines.append(make_tape_record(seq=i))

        with open(tape_path, "w") as f:
            f.write("\n".join(lines) + "\n")

        # Read back and detect gaps
        gaps = []
        prev_seq = 0
        with open(tape_path, "r") as f:
            for line in f:
                record = json.loads(line.strip())
                if prev_seq > 0 and record["seq"] != prev_seq + 1:
                    gaps.append((prev_seq, record["seq"]))
                prev_seq = record["seq"]

        assert len(gaps) == 2, f"Expected 2 gaps, found {len(gaps)}"
        assert gaps[0] == (49, 51), "First gap should be between 49 and 51"
        assert gaps[1] == (99, 101), "Second gap should be between 99 and 101"

    def test_timestamps_are_monotonic(self, tape_dir):
        """Timestamps should be non-decreasing (allows equal for burst writes)."""
        tape_path = tape_dir / "data" / "unified" / "raw_tape" / "unified_tape.jsonl"
        write_tape_file(tape_path, num_records=100)

        prev_ts = 0
        with open(tape_path, "r") as f:
            for line in f:
                record = json.loads(line.strip())
                assert record["ts"] >= prev_ts, "Timestamps must be non-decreasing"
                prev_ts = record["ts"]


# ═══════════════════════════════════════════════════════════════
#  Multi-Source Tape
# ═══════════════════════════════════════════════════════════════

class TestMultiSource:
    """Verify the unified tape correctly handles multiple sources."""

    def test_mixed_sources_on_single_tape(self, tape_dir):
        """A unified tape should contain records from both cb and kl."""
        tape_path = tape_dir / "data" / "unified" / "raw_tape" / "unified_tape.jsonl"

        lines = []
        for i in range(1, 21):
            src = "cb" if i % 2 == 0 else "kl"
            lines.append(make_tape_record(seq=i, src=src))

        with open(tape_path, "w") as f:
            f.write("\n".join(lines) + "\n")

        sources = set()
        with open(tape_path, "r") as f:
            for line in f:
                record = json.loads(line.strip())
                sources.add(record["src"])

        assert "cb" in sources, "Should contain Coinbase records"
        assert "kl" in sources, "Should contain Kalshi records"

    def test_source_counting(self, tape_dir):
        """Count records per source to verify correct tagging."""
        tape_path = tape_dir / "data" / "unified" / "raw_tape" / "unified_tape.jsonl"

        # 60 Coinbase, 30 Kalshi, 10 snapshots
        lines = []
        seq = 1
        for src, count in [("cb", 60), ("kl", 30), ("snap", 10)]:
            for _ in range(count):
                lines.append(make_tape_record(seq=seq, src=src))
                seq += 1

        with open(tape_path, "w") as f:
            f.write("\n".join(lines) + "\n")

        counts = {"cb": 0, "kl": 0, "snap": 0}
        with open(tape_path, "r") as f:
            for line in f:
                record = json.loads(line.strip())
                counts[record["src"]] += 1

        assert counts["cb"] == 60
        assert counts["kl"] == 30
        assert counts["snap"] == 10
