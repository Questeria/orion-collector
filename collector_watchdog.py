#!/usr/bin/env python3
"""
Orion — collector_watchdog.py v1.0
==================================
Auto-restart wrapper for orion_collector.py.

Starts the collector as a subprocess and monitors its health:
  - Checks tape freshness every 15 seconds
  - If tape stale >120s OR process died, kills and restarts
  - Exponential backoff between restarts (5s, 10s, 20s... max 300s)
  - Resets backoff after 30 minutes of stable running
  - Passes all CLI args through to the collector
  - Forwards SIGINT/SIGTERM to child for clean shutdown

USAGE:
  python collector_watchdog.py --symbols BTC,ETH,SOL
  python collector_watchdog.py --all-markets

All arguments are passed through to orion_collector.py.
"""
import json
import logging
import os
import signal
import subprocess
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

# ── Paths ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent
COLLECTOR_SCRIPT = PROJECT_ROOT / "collectors" / "orion_collector.py"
UNIFIED_TAPE = PROJECT_ROOT / "data" / "unified" / "raw_tape" / "unified_tape.jsonl"
LOG_DIR = PROJECT_ROOT / "logs"
WATCHDOG_LOG = LOG_DIR / "collector_watchdog.log"

# Singleton lock — prevents duplicate watchdog instances
from singleton_lock import acquire_singleton_lock, release_singleton_lock

# Find the venv Python — same as start_collectors.ps1 logic
VENV_PYTHON = PROJECT_ROOT / "venv" / "Scripts" / "python.exe"
PYTHON_EXE = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable

# ── Config (load from collector_config.yaml if available) ──
_CFG = {}
try:
    import yaml
    _cfg_path = PROJECT_ROOT / "collectors" / "collector_config.yaml"
    if _cfg_path.exists():
        with open(_cfg_path, "r") as f:
            _CFG = yaml.safe_load(f) or {}
except ImportError:
    pass

def _cfg(section, key, default):
    return _CFG.get(section, {}).get(key, default)

TAPE_STALE_S     = _cfg("watchdog", "tape_stale_s", 120)
HEALTH_CHECK_S   = _cfg("watchdog", "health_check_s", 15)
MAX_BACKOFF_S    = _cfg("watchdog", "max_backoff_s", 300)
INITIAL_BACKOFF_S = _cfg("watchdog", "initial_backoff_s", 5)
STABLE_RESET_S   = 1800  # Reset backoff after 30min stable


def setup_logger() -> logging.Logger:
    """Create a rotating file logger for the watchdog."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("watchdog")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    # Console
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    # File
    fh = RotatingFileHandler(
        str(WATCHDOG_LOG), mode="a", encoding="utf-8",
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=2,
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


def check_tape_age() -> float:
    """Return age of last unified tape record in seconds, or inf if missing.

    Uses the same approach as health_check.py: read last 4KB, parse last
    JSON line, extract ts_us, compute age.
    """
    if not UNIFIED_TAPE.exists():
        return float("inf")
    try:
        size = UNIFIED_TAPE.stat().st_size
        if size == 0:
            return float("inf")
        with open(UNIFIED_TAPE, "rb") as f:
            read_size = min(size, 4096)
            f.seek(size - read_size)
            tail = f.read()
        # Find last non-empty line
        for line in reversed(tail.split(b"\n")):
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            ts_us = record["ts_us"]
            age_s = (time.time_ns() // 1000 - ts_us) / 1_000_000.0
            return age_s
    except Exception:
        return float("inf")
    return float("inf")


def main():
    logger = setup_logger()

    # ── Singleton check: only one watchdog at a time ──
    if not acquire_singleton_lock("collector_watchdog"):
        logger.error(
            "Another collector watchdog is already running. "
            "Only one watchdog should run at a time. Exiting."
        )
        sys.exit(2)
    logger.info(f"  Watchdog singleton lock acquired (PID {os.getpid()})")

    logger.info("=" * 60)
    logger.info("  ORION Collector Watchdog v1.0")
    logger.info("=" * 60)
    logger.info(f"  Python:     {PYTHON_EXE}")
    logger.info(f"  Collector:  {COLLECTOR_SCRIPT}")
    logger.info(f"  Tape:       {UNIFIED_TAPE}")
    logger.info(f"  Stale:      {TAPE_STALE_S}s")
    logger.info(f"  Check:      every {HEALTH_CHECK_S}s")
    logger.info(f"  Backoff:    {INITIAL_BACKOFF_S}s -> {MAX_BACKOFF_S}s")
    logger.info(f"  Args:       {' '.join(sys.argv[1:]) or '(none)'}")
    logger.info("=" * 60)

    consecutive_failures = 0
    _shutdown = False

    # Forward signals to child process
    child_proc = None

    def _signal_handler(signum, frame):
        nonlocal _shutdown
        _shutdown = True
        logger.info(f"Watchdog received signal {signum}, shutting down...")
        if child_proc and child_proc.poll() is None:
            child_proc.terminate()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    while not _shutdown:
        # NOTE: We no longer delete the collector's PID file here.
        # The collector manages its own singleton lock via singleton_lock.py.
        # Deleting it here was a bug that allowed duplicate collectors to spawn.

        # Build command: python -u collector.py [args...]
        cmd = [PYTHON_EXE, "-u", str(COLLECTOR_SCRIPT)] + sys.argv[1:]
        logger.info(f"Starting collector (attempt {consecutive_failures + 1})")

        try:
            child_proc = subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            )
        except Exception as e:
            logger.error(f"Failed to start collector: {e}")
            consecutive_failures += 1
            backoff = min(INITIAL_BACKOFF_S * (2 ** consecutive_failures), MAX_BACKOFF_S)
            logger.info(f"Retrying in {backoff}s...")
            time.sleep(backoff)
            continue

        # Wait for initial startup (collector needs ~5-10s to connect)
        time.sleep(10)
        start_time = time.monotonic()

        # ── Monitor loop ──
        while not _shutdown and child_proc.poll() is None:
            time.sleep(HEALTH_CHECK_S)

            # Check tape freshness
            tape_age = check_tape_age()

            if tape_age > TAPE_STALE_S:
                logger.warning(
                    f"Tape stale ({tape_age:.0f}s > {TAPE_STALE_S}s) -- killing collector"
                )
                child_proc.terminate()
                try:
                    child_proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    logger.warning("Collector did not exit, force killing")
                    child_proc.kill()
                    child_proc.wait(timeout=5)
                break

            # Reset backoff after stable running period
            running_time = time.monotonic() - start_time
            if running_time > STABLE_RESET_S and consecutive_failures > 0:
                logger.info(
                    f"Stable for {running_time:.0f}s, resetting backoff counter"
                )
                consecutive_failures = 0

        if _shutdown:
            # Clean shutdown — wait for child to exit
            if child_proc and child_proc.poll() is None:
                logger.info("Waiting for collector to exit...")
                try:
                    child_proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    child_proc.kill()
            break

        # Process exited (crash or killed by us)
        exit_code = child_proc.returncode if child_proc else -1
        logger.warning(f"Collector exited with code {exit_code}")

        # Exponential backoff
        consecutive_failures += 1
        backoff = min(INITIAL_BACKOFF_S * (2 ** min(consecutive_failures - 1, 6)), MAX_BACKOFF_S)
        logger.info(f"Restarting in {backoff}s (failure #{consecutive_failures})")
        time.sleep(backoff)

    release_singleton_lock("collector_watchdog")
    logger.info("Watchdog stopped")


if __name__ == "__main__":
    main()
