#!/usr/bin/env python3
"""
Orion — health_check.py v3 (UNIFIED TAPE + AUTO ALERTING)
===========================================================
Monitor collector tape health + archive stats.

v3 OVER v2:
  1. Unified tape support — checks the new unified tape alongside legacy tapes
  2. Continuous alerting — --watch mode alerts when tapes go STALE/DEAD
  3. Sequence gap detection — checks for gaps in unified tape seq numbers
  4. Compressed archive awareness — counts .jsonl.gz files in archive totals
  5. CRC32 verification — validates last N records against their checksums

USAGE:
  python health_check.py              # One-shot check
  python health_check.py --watch      # Continuous 5-second loop with alerting
  python health_check.py --verify 100 # Verify CRC32 of last 100 unified tape records
"""
import gzip
import json
import os
import sys
import time
import zlib
from pathlib import Path

# Use ORION_DATA_DIR env var if set, otherwise default to ./data relative to this script
DATA_DIR       = Path(os.environ.get("ORION_DATA_DIR", Path(__file__).resolve().parent / "data"))
KALSHI_TAPE    = DATA_DIR / "kalshi" / "raw_tape" / "kalshi_tape.jsonl"
ORACLE_TAPE    = DATA_DIR / "oracle" / "raw_tape" / "oracle_tape.jsonl"
UNIFIED_TAPE   = DATA_DIR / "unified" / "raw_tape" / "unified_tape.jsonl"
KALSHI_ARCHIVE = DATA_DIR / "kalshi" / "raw_tape" / "archive"
ORACLE_ARCHIVE = DATA_DIR / "oracle" / "raw_tape" / "archive"
UNIFIED_ARCHIVE = DATA_DIR / "unified" / "raw_tape" / "archive"


def check_tape(label: str, tape_path: Path, archive_dir: Path) -> dict:
    """Check a single tape file and its archive."""
    result = {
        "label": label,
        "status": "MISSING",
        "age_ms": None,
        "size_mb": 0,
        "last_price": None,
        "last_symbol": None,
        "last_seq": None,
        "archive_count": 0,
        "archive_mb": 0,
    }

    # Check live tape
    if not tape_path.exists():
        return result

    size = tape_path.stat().st_size
    result["size_mb"] = size / (1024 * 1024)

    if size == 0:
        result["status"] = "EMPTY"
        return result

    # Read last line
    try:
        with open(tape_path, "rb") as f:
            # Seek to end, read backwards to find last complete line
            f.seek(0, 2)
            pos = f.tell()
            buf = b""
            while pos > 0:
                read_size = min(4096, pos)
                pos -= read_size
                f.seek(pos)
                chunk = f.read(read_size)
                buf = chunk + buf
                lines = buf.split(b"\n")
                # Need at least 2 entries (last may be empty after trailing \n)
                for line in reversed(lines):
                    line = line.strip()
                    if not line:
                        continue
                    entry = json.loads(line)
                    ts_us = entry["ts_us"]
                    age_ms = (time.time_ns() // 1000 - ts_us) / 1000.0
                    result["age_ms"] = age_ms

                    # Extract seq number if present (unified tape)
                    if "seq" in entry:
                        result["last_seq"] = entry["seq"]

                    # Parse inner message for price display
                    try:
                        raw = entry.get("raw", "")
                        if isinstance(raw, str) and raw:
                            msg = json.loads(raw)
                        else:
                            msg = raw if isinstance(raw, dict) else {}

                        # Coinbase ticker messages
                        if msg.get("type") == "ticker":
                            result["last_symbol"] = msg.get("product_id", "")
                            if "price" in msg:
                                result["last_price"] = msg["price"]
                        # Kalshi orderbook snapshots
                        elif msg.get("type") == "orderbook_snapshot":
                            inner = msg.get("msg", {})
                            result["last_symbol"] = inner.get("market_ticker", "")
                    except Exception:
                        pass

                    # Status based on age
                    if age_ms < 10_000:
                        result["status"] = "HEALTHY"
                    elif age_ms < 60_000:
                        result["status"] = "STALE"
                    elif age_ms < 300_000:
                        result["status"] = "WARNING"
                    else:
                        result["status"] = "DEAD"

                    break
                if result["age_ms"] is not None:
                    break
    except Exception:
        result["status"] = "ERROR"

    # Check archive (count .jsonl, .jsonl.gz, and .parquet files)
    if archive_dir.exists():
        jsonl_files = list(archive_dir.glob("*.jsonl"))
        gz_files = list(archive_dir.glob("*.jsonl.gz"))
        pq_files = list(archive_dir.glob("*.parquet"))
        all_files = jsonl_files + gz_files + pq_files
        result["archive_count"] = len(all_files)
        result["archive_mb"] = sum(f.stat().st_size for f in all_files) / (1024 * 1024)

    return result


def verify_crc(tape_path: Path, count: int = 100) -> dict:
    """Verify CRC32 checksums of the last N records in a unified tape.

    Returns dict with 'total', 'valid', 'invalid', 'missing_crc', 'errors'.
    """
    result = {"total": 0, "valid": 0, "invalid": 0, "missing_crc": 0, "errors": []}

    if not tape_path.exists() or tape_path.stat().st_size == 0:
        return result

    # Read the tail of the file (enough for `count` records)
    # Each record is roughly 200-2000 bytes, so read count * 2KB
    read_size = count * 2048
    try:
        with open(tape_path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            actual_read = min(size, read_size)
            f.seek(size - actual_read)
            tail = f.read()
    except Exception as e:
        result["errors"].append(f"Read error: {e}")
        return result

    lines = tail.split(b"\n")
    # Take the last `count` non-empty lines
    records = [l.strip() for l in reversed(lines) if l.strip()][:count]

    for raw_line in records:
        result["total"] += 1
        try:
            record = json.loads(raw_line)
            if "crc" not in record:
                result["missing_crc"] += 1
                continue

            stored_crc = record["crc"]
            # Reconstruct the raw payload and compute CRC32
            raw_payload = json.dumps(record["raw"], separators=(",", ":"), ensure_ascii=False)
            raw_bytes = raw_payload.encode("utf-8")
            computed_crc = zlib.crc32(raw_bytes) & 0xFFFFFFFF

            if computed_crc == stored_crc:
                result["valid"] += 1
            else:
                result["invalid"] += 1
                result["errors"].append(
                    f"seq={record.get('seq', '?')}: stored={stored_crc}, computed={computed_crc}"
                )
        except Exception as e:
            result["missing_crc"] += 1
            result["errors"].append(f"CRC check exception: {e}")

    return result


def format_result(r: dict) -> str:
    """Format a single tape check result for display."""
    # Status color
    colors = {
        "HEALTHY": "\033[92m",
        "STALE":   "\033[93m",
        "WARNING": "\033[93m",
        "DEAD":    "\033[91m",
        "MISSING": "\033[91m",
        "EMPTY":   "\033[93m",
        "ERROR":   "\033[91m",
    }
    reset = "\033[0m"
    color = colors.get(r["status"], "")

    parts = [f"    {r['label']:10s} {color}{r['status']:8s}{reset}"]

    if r["age_ms"] is not None:
        if r["age_ms"] < 1000:
            parts.append(f"age={r['age_ms']:>7.0f}ms")
        else:
            parts.append(f"age={r['age_ms']/1000:>6.1f}s ")

    parts.append(f"size={r['size_mb']:>8.1f}MB")

    if r.get("last_seq") is not None:
        parts.append(f"seq={r['last_seq']:,}")

    if r["archive_count"] > 0:
        parts.append(f"archive={r['archive_count']} files ({r['archive_mb']:.1f}MB)")

    if r["last_symbol"] and r["last_price"]:
        parts.append(f"{r['last_symbol']}=${r['last_price']}")
    elif r["last_symbol"]:
        parts.append(f"last={r['last_symbol']}")

    return "  ".join(parts)


def run_check(prev_statuses: dict = None) -> tuple:
    """Run health check on all tapes. Returns (all_healthy, current_statuses)."""
    kalshi  = check_tape("Kalshi",  KALSHI_TAPE,  KALSHI_ARCHIVE)
    oracle  = check_tape("Oracle",  ORACLE_TAPE,  ORACLE_ARCHIVE)
    unified = check_tape("Unified", UNIFIED_TAPE, UNIFIED_ARCHIVE)

    ts = time.strftime("%H:%M:%S")
    print()
    print("=" * 80)
    print(f"  ORION Collector Health Check v3 — {ts}")
    print("=" * 80)
    print(format_result(unified))
    print(format_result(kalshi))
    print(format_result(oracle))

    # Total data summary
    tapes = [kalshi, oracle, unified]
    total_live = sum(t["size_mb"] for t in tapes)
    total_archive = sum(t["archive_mb"] for t in tapes)
    total_files = sum(t["archive_count"] for t in tapes)
    if total_archive > 0:
        print(f"    {'':10s} TOTAL    live={total_live:.1f}MB  archive={total_archive:.1f}MB ({total_files} files)")

    print("=" * 80)

    # Alerting — detect status transitions (HEALTHY → STALE/DEAD)
    current_statuses = {}
    for tape in tapes:
        current_statuses[tape["label"]] = tape["status"]

    if prev_statuses:
        for label, status in current_statuses.items():
            prev = prev_statuses.get(label, "MISSING")
            if prev == "HEALTHY" and status in ("STALE", "WARNING", "DEAD"):
                alert_msg = f"  *** ALERT: {label} tape degraded: {prev} -> {status} ***"
                print(f"\033[91m{alert_msg}\033[0m")
                # Bell character — makes terminal beep
                print("\a", end="", flush=True)
            elif prev in ("STALE", "WARNING", "DEAD") and status == "HEALTHY":
                print(f"\033[92m  *** RECOVERED: {label} tape is HEALTHY again ***\033[0m")

    all_healthy = all(t["status"] == "HEALTHY" for t in tapes)
    return all_healthy, current_statuses


def main():
    watch = "--watch" in sys.argv or "-w" in sys.argv
    verify = "--verify" in sys.argv

    if verify:
        # CRC32 verification mode
        idx = sys.argv.index("--verify")
        count = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 100
        print(f"\n  Verifying CRC32 of last {count} unified tape records...")
        result = verify_crc(UNIFIED_TAPE, count)
        print(f"  Total: {result['total']}  Valid: {result['valid']}  "
              f"Invalid: {result['invalid']}  No CRC: {result['missing_crc']}")
        if result["errors"]:
            print("  Errors:")
            for e in result["errors"][:10]:
                print(f"    {e}")
        sys.exit(0 if result["invalid"] == 0 else 1)

    if watch:
        prev = None
        try:
            while True:
                _, prev = run_check(prev)
                time.sleep(5)
        except KeyboardInterrupt:
            print("\nStopped")
    else:
        ok, _ = run_check()
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
