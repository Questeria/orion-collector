"""
Shared fixtures for Orion Collector test suite.

Creates temporary directories, mock tape files, and reusable test data
so individual test files stay clean and focused.
"""

import json
import os
import sys
import time
import zlib
import tempfile
import shutil
import pytest
from pathlib import Path
from datetime import datetime, timezone, timedelta

# ── Add the project root to sys.path so we can import collector modules ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════════════════════
#  Temporary directory fixtures
# ═══════════════════════════════════════════════════════════════

@pytest.fixture
def tmp_dir(tmp_path):
    """Provide a temporary directory that auto-cleans after each test."""
    return tmp_path


@pytest.fixture
def tape_dir(tmp_path):
    """Create the full directory structure for tape files."""
    unified = tmp_path / "data" / "unified" / "raw_tape"
    kalshi = tmp_path / "data" / "kalshi" / "raw_tape"
    oracle = tmp_path / "data" / "oracle" / "raw_tape"
    for d in [unified, kalshi, oracle]:
        d.mkdir(parents=True)
        (d.parent / "archive").mkdir(exist_ok=True)  # archive dirs too
    return tmp_path


# ═══════════════════════════════════════════════════════════════
#  Tape record helpers
# ═══════════════════════════════════════════════════════════════

def make_tape_record(seq: int, src: str = "cb", ts: float = None,
                     msg: dict = None) -> str:
    """Build a single JSONL tape record with CRC32 checksum.

    This replicates the format used by UnifiedTapeWriter in orion_collector.py.
    Each line is a JSON object with: seq, ts, src, msg, crc32.
    """
    if ts is None:
        ts = time.time()
    if msg is None:
        msg = {"type": "ticker", "price": "83000.50", "product_id": "BTC-USD"}

    record = {
        "seq": seq,
        "ts": ts,
        "src": src,
        "msg": msg,
    }

    # CRC32 is computed on the JSON string WITHOUT the crc32 field
    raw = json.dumps(record, separators=(",", ":"))
    crc = zlib.crc32(raw.encode("utf-8")) & 0xFFFFFFFF
    record["crc32"] = crc

    return json.dumps(record, separators=(",", ":"))


def write_tape_file(tape_path: Path, num_records: int = 100,
                    src: str = "cb", start_seq: int = 1,
                    corrupt_indices: list = None) -> list:
    """Write a tape file with N records, optionally corrupting some.

    Args:
        tape_path: Where to write the JSONL file.
        num_records: Number of records to write.
        src: Source tag (cb, kl, snap).
        start_seq: Starting sequence number.
        corrupt_indices: List of record indices (0-based) to corrupt.

    Returns:
        List of record dicts that were written (before corruption).
    """
    if corrupt_indices is None:
        corrupt_indices = []

    records = []
    lines = []
    ts = time.time() - num_records  # Start 100s ago

    for i in range(num_records):
        seq = start_seq + i
        record_line = make_tape_record(seq=seq, src=src, ts=ts + i)

        records.append(json.loads(record_line))

        if i in corrupt_indices:
            # Corrupt the line by flipping a character in the JSON
            record_line = record_line.replace('"src"', '"SRC"')

        lines.append(record_line)

    tape_path.parent.mkdir(parents=True, exist_ok=True)
    with open(tape_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    return records


# ═══════════════════════════════════════════════════════════════
#  Config fixture
# ═══════════════════════════════════════════════════════════════

@pytest.fixture
def mock_config(tmp_path):
    """Write a minimal collector_config.yaml for testing."""
    config = {
        "health_score": {
            "weights": {
                "freshness": 30,
                "rate": 20,
                "stability": 20,
                "latency": 15,
                "resources": 15,
            },
            "thresholds": {
                "freshness_good_s": 5,
                "freshness_bad_s": 60,
                "rate_good": 50,
                "rate_bad": 0,
                "reconnect_window_m": 30,
                "reconnect_good": 0,
                "reconnect_bad": 5,
                "p95_good_ms": 100,
                "p95_bad_ms": 1000,
                "disk_good_gb": 20,
                "disk_bad_gb": 2,
                "queue_good": 0,
                "queue_bad": 5000,
            },
        },
        "anomaly": {
            "enabled": True,
            "window_entries": 100,
            "min_baseline": 10,
            "alert_z": 3.0,
            "warn_z": 2.0,
            "tracked_metrics": ["rate", "p95", "queue"],
        },
        "sla": {
            "incident_conditions": {
                "collector_down": {"tape_stale_s": 120},
                "tape_stale": {"stale_s": 60},
                "rate_zero": {"duration_s": 30},
            },
            "daily_retention_days": 30,
        },
        "alerts": {
            "enabled": False,
        },
    }

    try:
        import yaml
        config_path = tmp_path / "collector_config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)
        return config_path
    except ImportError:
        return None
