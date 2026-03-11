#!/usr/bin/env python3
"""
Orion — collector_dashboard.py
===============================
Standalone dashboard for monitoring the Orion data collector.
Zero-dependency: uses only Python stdlib + CDN-loaded React/D3.

    python collector_dashboard.py                 # auto-finds port 3001+
    python collector_dashboard.py --port 3005     # force specific port
    python collector_dashboard.py --no-open       # don't auto-open browser

Shows: live Prometheus metrics, tape health, archive browser,
collector log viewer, config viewer, and freshness timeline.
Requires orion_collector.py to be running for live metrics.
"""

import argparse
import json
import os
import re
import shutil
import smtplib
import socket
import sys
import threading
import time
import webbrowser
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from http.server import HTTPServer, SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError
import csv
import subprocess

# Singleton lock — prevents duplicate dashboard instances
from singleton_lock import acquire_singleton_lock, release_singleton_lock, get_pid_file_path


# ══════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════
SCRIPT_DIR = Path(__file__).resolve().parent          # collectors/
PROJECT_DIR = SCRIPT_DIR.parent                        # Orion1/
JSX_FILENAME = "collector_dashboard.jsx"
DATA_DIR = Path(os.getenv("ORION_DATA_DIR", PROJECT_DIR / "data"))
LOG_FILE = PROJECT_DIR / "logs" / "orion_collector.log"
CONFIG_FILE = SCRIPT_DIR / "collector_config.yaml"
# v8.6: Use 127.0.0.1 instead of "localhost" — on Windows, "localhost" triggers
# IPv6 DNS resolution that hangs for ~2 seconds, causing 4+ second API response
# times and sparse chart data. 127.0.0.1 responds in <100ms.
PROM_URL = "http://127.0.0.1:9090/metrics"
ENV_FILE = PROJECT_DIR / "api.env"          # Gmail app password lives here


# ══════════════════════════════════════════════════════════════════
# v8.3: ALERT CONFIG — loads alert settings from collector_config.yaml
# ══════════════════════════════════════════════════════════════════
_alert_config_cache = {"data": None, "mtime": 0}


def _load_alert_config() -> dict:
    """Load & cache the alerts section from collector_config.yaml.

    Re-reads the file if it has been modified since last load.
    Returns an empty dict if the file doesn't exist or has no alerts section.
    """
    if not CONFIG_FILE.exists():
        return {}
    try:
        mtime = CONFIG_FILE.stat().st_mtime
        if _alert_config_cache["data"] is not None and mtime == _alert_config_cache["mtime"]:
            return _alert_config_cache["data"]

        import yaml  # Already installed for the collector
        raw = CONFIG_FILE.read_text(encoding="utf-8")
        cfg = yaml.safe_load(raw) or {}
        alerts = cfg.get("alerts", {})
        _alert_config_cache["data"] = alerts
        _alert_config_cache["mtime"] = mtime
        return alerts
    except Exception:
        return {}


def _alert_cfg(*keys, default=None):
    """Get a nested value from the alerts config.

    Usage: _alert_cfg("conditions", "data_stopped", "enabled", default=True)
    """
    cfg = _load_alert_config()
    for k in keys:
        if isinstance(cfg, dict):
            cfg = cfg.get(k)
        else:
            return default
    return cfg if cfg is not None else default


def _load_env_value(key: str) -> str:
    """Read a single key=value from api.env without exposing the file.

    Returns the value string, or "" if not found.
    Reads fresh each call so the user can update api.env without restarting.
    """
    if not ENV_FILE.exists():
        return ""
    try:
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            if k.strip() == key:
                return v.strip().strip('"').strip("'")
    except Exception:
        pass
    return ""


# Tape paths (same as health_check.py)
UNIFIED_TAPE    = DATA_DIR / "unified" / "raw_tape" / "unified_tape.jsonl"
KALSHI_TAPE     = DATA_DIR / "kalshi"  / "raw_tape" / "kalshi_tape.jsonl"
ORACLE_TAPE     = DATA_DIR / "oracle"  / "raw_tape" / "oracle_tape.jsonl"
UNIFIED_ARCHIVE = DATA_DIR / "unified" / "raw_tape" / "archive"
KALSHI_ARCHIVE  = DATA_DIR / "kalshi"  / "raw_tape" / "archive"
ORACLE_ARCHIVE  = DATA_DIR / "oracle"  / "raw_tape" / "archive"

# All tapes in a list for easy iteration
TAPES = [
    ("Unified", UNIFIED_TAPE, UNIFIED_ARCHIVE),
    ("Kalshi",  KALSHI_TAPE,  KALSHI_ARCHIVE),
    ("Oracle",  ORACLE_TAPE,  ORACLE_ARCHIVE),
]


# ══════════════════════════════════════════════════════════════════
# PORT FINDER — auto-find an available port
# ══════════════════════════════════════════════════════════════════
def find_free_port(start: int = 3001, max_attempts: int = 10) -> int:
    """Try binding to ports starting from `start` until one is free."""
    for offset in range(max_attempts):
        port = start + offset
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("0.0.0.0", port))
                return port
        except OSError:
            continue
    print(f"\n  ERROR: No free port found in range {start}-{start + max_attempts - 1}")
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════
# PROMETHEUS METRICS — proxy from collector's :9090 endpoint
# ══════════════════════════════════════════════════════════════════
def parse_prometheus_metrics(text: str) -> dict:
    """
    Parse Prometheus text exposition format into a clean JSON dict.
    Strips 'orion_collector_' prefix and groups labeled metrics.
    """
    metrics = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            if "{" in line:
                name_part, rest = line.split("{", 1)
                labels_str, val_str = rest.rsplit("}", 1)
                val = float(val_str.strip())
                labels = {}
                for pair in labels_str.split(","):
                    k, v = pair.split("=", 1)
                    labels[k.strip()] = v.strip().strip('"')
                name = name_part.replace("orion_collector_", "")
                if name not in metrics:
                    metrics[name] = {}
                sub_key = list(labels.values())[0] if labels else "value"
                # Remap short Prometheus labels to human-readable names
                LABEL_MAP = {"cb": "coinbase", "kl": "kalshi"}
                sub_key = LABEL_MAP.get(sub_key, sub_key)
                metrics[name][sub_key] = val
            else:
                parts = line.split()
                if len(parts) >= 2:
                    name = parts[0].replace("orion_collector_", "")
                    val = float(parts[1])
                    metrics[name] = val
        except (ValueError, IndexError):
            continue
    return metrics


# v8.6: Metrics cache — the history poller writes, the API handler reads.
# Previously, both the API handler AND the history poller each made their own
# HTTP request to the collector's Prometheus endpoint (localhost:9090/metrics).
# The collector's HTTP server is single-threaded, so concurrent requests queued
# up, causing 4+ second API response times and sparse chart data.
# Now: only the history poller fetches (the "producer"), and the API handler
# returns the cached result instantly (the "consumer").
_metrics_cache: dict = {}
_metrics_cache_lock = threading.Lock()


def fetch_collector_metrics() -> dict:
    """Fetch and parse Prometheus metrics from the collector.

    v8.5: Also includes health_score, health_grade, and health_components
    from the latest computed health score.
    v8.6: Updates the shared _metrics_cache so the API handler can serve it
    instantly without making its own HTTP request.
    """
    global _metrics_cache

    try:
        req = Request(PROM_URL, headers={"Accept": "text/plain"})
        with urlopen(req, timeout=2) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        parsed = parse_prometheus_metrics(raw)
        parsed["ok"] = True
        parsed["ts"] = time.time()
        # v8.5: Attach latest health score
        parsed["health_score"] = _latest_health_score.get("score", 0)
        parsed["health_grade"] = _latest_health_score.get("grade", "?")
        parsed["health_components"] = _latest_health_score.get("components", {})
        # v8.6: Attach latest anomaly count + freshness ages from cache
        # (avoids expensive get_all_health() call in the API handler)
        parsed["anomaly_count"] = _latest_anomaly_count
        parsed["unified_age"] = _latest_freshness.get("unified_age")
        parsed["kalshi_age"] = _latest_freshness.get("kalshi_age")
        parsed["oracle_age"] = _latest_freshness.get("oracle_age")
        # Update shared cache for the API handler
        with _metrics_cache_lock:
            _metrics_cache = parsed
        return parsed
    except (URLError, OSError, Exception) as e:
        return {"ok": False, "error": str(e), "ts": time.time()}


# ══════════════════════════════════════════════════════════════════
# TAPE HEALTH — adapted from collectors/health_check.py
# ══════════════════════════════════════════════════════════════════
def check_tape_health(label: str, tape_path: Path, archive_dir: Path) -> dict:
    """
    Check a single tape file and its archive directory.
    Returns status (HEALTHY/STALE/WARNING/DEAD/MISSING/EMPTY/ERROR),
    age, size, last price, sequence number, and archive stats.
    """
    result = {
        "label": label,
        "status": "MISSING",
        "age_ms": None,
        "size_mb": 0.0,
        "last_price": None,
        "last_symbol": None,
        "last_seq": None,
        "archive_count": 0,
        "archive_mb": 0.0,
    }

    if not tape_path.exists():
        return result

    size = tape_path.stat().st_size
    result["size_mb"] = round(size / (1024 * 1024), 2)

    if size == 0:
        result["status"] = "EMPTY"
        return result

    # Read last line using backward seek (efficient for large files)
    try:
        with open(tape_path, "rb") as f:
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
                for line in reversed(lines):
                    line = line.strip()
                    if not line:
                        continue
                    entry = json.loads(line)
                    ts_us = entry["ts_us"]
                    age_ms = (time.time_ns() // 1000 - ts_us) / 1000.0
                    result["age_ms"] = round(age_ms, 1)

                    if "seq" in entry:
                        result["last_seq"] = entry["seq"]

                    # Parse inner message for display
                    try:
                        raw = entry.get("raw", "")
                        msg = json.loads(raw) if isinstance(raw, str) and raw else (
                            raw if isinstance(raw, dict) else {}
                        )
                        if msg.get("type") == "ticker":
                            result["last_symbol"] = msg.get("product_id", "")
                            if "price" in msg:
                                result["last_price"] = msg["price"]
                        elif msg.get("type") == "orderbook_snapshot":
                            inner = msg.get("msg", {})
                            result["last_symbol"] = inner.get("market_ticker", "")
                    except Exception:
                        pass

                    # Status thresholds
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

    # Archive stats — v8.1: rglob walks daily subfolders (and legacy flat files)
    if archive_dir.exists():
        all_files = [
            f for f in archive_dir.rglob("*")
            if f.is_file() and f.suffix in (".jsonl", ".gz", ".parquet")
        ]
        result["archive_count"] = len(all_files)
        result["archive_mb"] = round(
            sum(f.stat().st_size for f in all_files) / (1024 * 1024), 2
        )

    return result


# v8.6: Health cache — check_tape_health() does disk I/O (file reads + rglob for
# archive stats). Called by history poller, API /api/health, and monitor thread.
# Cache for 2s to avoid redundant file I/O across concurrent callers.
_health_cache: dict = {}
_health_cache_time: float = 0.0
_health_cache_lock = threading.Lock()
_HEALTH_CACHE_TTL = 2.0  # seconds


def get_all_health() -> dict:
    """Check all three tapes and return combined health report.

    v8.6: Results cached for 2s to prevent redundant disk I/O.
    """
    global _health_cache, _health_cache_time
    now = time.time()
    if now - _health_cache_time < _HEALTH_CACHE_TTL and _health_cache.get("ok"):
        return _health_cache
    tapes = [check_tape_health(label, tp, ap) for label, tp, ap in TAPES]
    result = {"ok": True, "tapes": tapes, "ts": time.time()}
    with _health_cache_lock:
        _health_cache = result
        _health_cache_time = time.time()
    return result


# ══════════════════════════════════════════════════════════════════
# ARCHIVE SCANNER — list archived tape files
# ══════════════════════════════════════════════════════════════════
def scan_archives() -> dict:
    """Scan all archive directories and return file listings.

    v8.1: Walks daily subfolders (YYYY-MM-DD/) recursively.
    Also handles legacy flat files for backward compatibility.
    """
    groups = {}
    for label, _, archive_dir in TAPES:
        key = label.lower()
        files = []
        if archive_dir.exists():
            # v8.1: rglob walks daily subfolders and legacy flat files
            all_files = [
                f for f in archive_dir.rglob("*")
                if f.is_file() and f.suffix in (".jsonl", ".gz", ".parquet")
            ]
            for f in sorted(all_files, key=lambda x: x.stat().st_mtime,
                            reverse=True)[:5000]:
                st = f.stat()
                # Determine format label
                if f.name.endswith(".parquet"):
                    fmt = "parquet"
                elif f.name.endswith(".jsonl.gz"):
                    fmt = "gzip"
                else:
                    fmt = "jsonl"
                # v8.1: Include daily folder name if in a subfolder
                folder = f.parent.name if f.parent != archive_dir else ""
                files.append({
                    "name": f.name,
                    "folder": folder,
                    "format": fmt,
                    "size_mb": round(st.st_size / (1024 * 1024), 2),
                    "modified": time.strftime(
                        "%Y-%m-%d %H:%M", time.localtime(st.st_mtime)
                    ),
                    "age_hours": round(
                        (time.time() - st.st_mtime) / 3600, 1
                    ),
                })
        groups[key] = files
    return {"ok": True, "groups": groups, "ts": time.time()}


# ══════════════════════════════════════════════════════════════════
# LOG TAILER — last N lines from collector log
# ══════════════════════════════════════════════════════════════════
LOG_LINE_RE = re.compile(
    r"^(\d{2}:\d{2}:\d{2})\s*\|\s*(\w+)\s*\|\s*(.*)$"
)


def tail_log(n: int = 100) -> dict:
    """Read and parse the last N lines of the collector log."""
    if not LOG_FILE.exists():
        return {"ok": False, "error": "Log file not found", "lines": []}

    try:
        # Read the tail of the file (up to 200KB should cover 100+ lines)
        with open(LOG_FILE, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            read_size = min(size, 200_000)
            f.seek(size - read_size)
            tail = f.read().decode("utf-8", errors="replace")
    except (PermissionError, OSError) as e:
        return {"ok": False, "error": str(e), "lines": []}

    raw_lines = tail.splitlines()[-n:]
    parsed = []
    for line in raw_lines:
        line = line.strip()
        if not line:
            continue
        m = LOG_LINE_RE.match(line)
        if m:
            parsed.append({
                "time": m.group(1),
                "level": m.group(2),
                "msg": m.group(3),
            })
        else:
            # Lines that don't match the pattern (tracebacks, etc.)
            parsed.append({"time": "", "level": "INFO", "msg": line})

    return {"ok": True, "lines": parsed, "ts": time.time()}


# ══════════════════════════════════════════════════════════════════
# CONFIG READER — collector_config.yaml
# ══════════════════════════════════════════════════════════════════
def read_config() -> dict:
    """Read the collector config file and return raw text."""
    if not CONFIG_FILE.exists():
        return {"ok": False, "error": "Config file not found", "raw": ""}
    try:
        raw = CONFIG_FILE.read_text(encoding="utf-8")
        return {"ok": True, "raw": raw, "ts": time.time()}
    except Exception as e:
        return {"ok": False, "error": str(e), "raw": ""}


# ══════════════════════════════════════════════════════════════════
# ERROR RATE — scan log for ERROR/WARNING per minute
# ══════════════════════════════════════════════════════════════════
def get_error_rate() -> dict:
    """Count ERROR/WARNING/CRITICAL lines per minute from collector log."""
    if not LOG_FILE.exists():
        return {"ok": False, "error": "Log file not found", "buckets": [], "ts": time.time()}
    try:
        with open(LOG_FILE, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            read_size = min(size, 500_000)
            f.seek(size - read_size)
            tail = f.read().decode("utf-8", errors="replace")

        buckets = {}  # "HH:MM" -> {errors, warnings}
        for line in tail.splitlines():
            line = line.strip()
            if not line:
                continue
            m = LOG_LINE_RE.match(line)
            if m and m.group(2) in ("ERROR", "CRITICAL", "WARNING"):
                bucket_key = m.group(1)[:5]  # "HH:MM"
                if bucket_key not in buckets:
                    buckets[bucket_key] = {"errors": 0, "warnings": 0}
                if m.group(2) in ("ERROR", "CRITICAL"):
                    buckets[bucket_key]["errors"] += 1
                else:
                    buckets[bucket_key]["warnings"] += 1

        sorted_buckets = sorted(buckets.items(), key=lambda x: x[0])[-60:]
        return {
            "ok": True,
            "buckets": [{"time": k, "errors": v["errors"], "warnings": v["warnings"]}
                        for k, v in sorted_buckets],
            "ts": time.time(),
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "buckets": [], "ts": time.time()}


def get_error_details(minute_key: str) -> dict:
    """Return actual ERROR/WARNING/CRITICAL log lines for a specific HH:MM bucket.

    Used by the dashboard when the user clicks on an error bar in the Error Rate chart.
    Returns up to 50 lines matching the given minute.
    """
    if not LOG_FILE.exists():
        return {"ok": False, "error": "Log file not found", "lines": []}
    try:
        with open(LOG_FILE, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            read_size = min(size, 500_000)
            f.seek(size - read_size)
            tail = f.read().decode("utf-8", errors="replace")

        lines = []
        for line in tail.splitlines():
            line = line.strip()
            if not line:
                continue
            m = LOG_LINE_RE.match(line)
            if m and m.group(2) in ("ERROR", "CRITICAL", "WARNING"):
                bucket = m.group(1)[:5]  # "HH:MM"
                if bucket == minute_key:
                    lines.append({
                        "time": m.group(1),       # "HH:MM:SS"
                        "level": m.group(2),       # "ERROR" | "CRITICAL" | "WARNING"
                        "message": m.group(3)[:300],  # Truncate long lines
                    })
        return {"ok": True, "minute": minute_key, "lines": lines[-50:]}
    except Exception as e:
        return {"ok": False, "error": str(e), "lines": []}


# ══════════════════════════════════════════════════════════════════
# PROCESS STATS — collector process CPU/memory via PowerShell
# ══════════════════════════════════════════════════════════════════
_proc_cache = {"ts": 0, "data": None}


def get_process_stats() -> dict:
    """Get collector process memory usage using PowerShell (Windows-native)."""
    now = time.time()
    # Cache for 3 seconds to avoid subprocess overhead
    if _proc_cache["data"] and (now - _proc_cache["ts"]) < 3:
        return _proc_cache["data"]

    try:
        # PowerShell: find python processes whose command line contains orion_collector
        ps_cmd = (
            "Get-WmiObject Win32_Process -Filter \"Name='python.exe'\" | "
            "Where-Object { $_.CommandLine -like '*orion_collector*' } | "
            "ForEach-Object { @{ "
            "  PID = $_.ProcessId; "
            "  MemoryMB = [math]::Round($_.WorkingSetSize / 1MB, 1); "
            "  CmdLine = $_.CommandLine.Substring(0, [math]::Min(80, $_.CommandLine.Length)) "
            "} } | ConvertTo-Json"
        )
        # CREATE_NO_WINDOW prevents PowerShell from flashing a visible window
        creation_flags = 0x08000000 if os.name == "nt" else 0  # CREATE_NO_WINDOW
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True, text=True, timeout=5,
            creationflags=creation_flags
        )
        processes = []
        if result.stdout.strip():
            parsed = json.loads(result.stdout)
            # PowerShell returns a dict for single result, list for multiple
            if isinstance(parsed, dict):
                parsed = [parsed]
            for p in parsed:
                processes.append({
                    "pid": p.get("PID", 0),
                    "memory_mb": p.get("MemoryMB", 0),
                    "cmd": p.get("CmdLine", ""),
                })

        response = {"ok": True, "processes": processes, "ts": now}
        _proc_cache["ts"] = now
        _proc_cache["data"] = response
        return response
    except Exception as e:
        return {"ok": False, "error": str(e), "processes": [], "ts": now}


# ══════════════════════════════════════════════════════════════════
# ALERT HISTORY — scan log for [ALERT], [GAP], [FEED], [DISK] events
# ══════════════════════════════════════════════════════════════════
# v8.1: Institutional monitoring — shows last N operational alerts
_ALERT_PREFIXES = ("[ALERT]", "[GAP]", "[DISK]", "[FEED]")
_ALERT_RE = re.compile(r"^\[(ALERT|GAP|DISK|FEED)\]\s*(.*)")


def get_alert_history(max_alerts: int = 30) -> dict:
    """Scan collector log for operational alerts. Returns newest first."""
    if not LOG_FILE.exists():
        return {"ok": False, "error": "Log file not found", "alerts": [],
                "ts": time.time()}
    try:
        with open(LOG_FILE, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            read_size = min(size, 500_000)
            f.seek(size - read_size)
            tail = f.read().decode("utf-8", errors="replace")

        alerts = []
        for line in tail.splitlines():
            line = line.strip()
            if not line:
                continue
            m = LOG_LINE_RE.match(line)
            if not m:
                continue
            ts_str, level, msg = m.group(1), m.group(2), m.group(3)
            # Only keep lines whose message starts with an alert prefix
            msg_stripped = msg.strip()
            matched_cat = None
            for prefix in _ALERT_PREFIXES:
                if msg_stripped.startswith(prefix):
                    matched_cat = prefix[1:-1]  # "ALERT", "GAP", etc.
                    break
            if not matched_cat:
                continue
            # Classify severity based on category + level
            if matched_cat in ("ALERT", "GAP") and level in (
                    "ERROR", "CRITICAL", "WARNING"):
                severity = "critical" if level in (
                    "ERROR", "CRITICAL") else "warning"
            elif matched_cat == "DISK" and level in ("ERROR", "CRITICAL"):
                severity = "critical"
            elif matched_cat == "FEED":
                severity = "info"
            else:
                severity = "warning"
            alerts.append({
                "time": ts_str,
                "level": level,
                "category": matched_cat,
                "severity": severity,
                "msg": msg_stripped,
            })

        # Return newest first, capped at max_alerts
        return {
            "ok": True,
            "alerts": alerts[-max_alerts:][::-1],
            "ts": time.time(),
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "alerts": [],
                "ts": time.time()}


# ══════════════════════════════════════════════════════════════════
# FEED RATES — parse per-product message rates from log
# ══════════════════════════════════════════════════════════════════
# v8.1: Reads the most recent "[FEED] CB rates" line for breakdown
_FEED_RATE_RE = re.compile(r"(\w+-\w+)=(\d+)")


def get_feed_rates() -> dict:
    """Parse per-product message counts from the collector log."""
    if not LOG_FILE.exists():
        return {"ok": False, "error": "Log file not found", "coinbase": [],
                "ts": time.time()}
    try:
        with open(LOG_FILE, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            read_size = min(size, 200_000)
            f.seek(size - read_size)
            tail = f.read().decode("utf-8", errors="replace")

        # Find the most recent "[FEED] CB rates" line
        last_rates_line = None
        last_rates_time = ""
        for line in tail.splitlines():
            line = line.strip()
            if not line:
                continue
            if "[FEED] CB rate" in line:
                m = LOG_LINE_RE.match(line)
                if m:
                    last_rates_time = m.group(1)
                    last_rates_line = m.group(3)

        if not last_rates_line:
            return {"ok": True, "coinbase": [], "period_s": 30,
                    "log_time": "", "ts": time.time()}

        # Parse product=count pairs
        products = []
        for pm in _FEED_RATE_RE.finditer(last_rates_line):
            products.append({
                "product": pm.group(1),
                "count": int(pm.group(2)),
            })
        # Sort by count descending
        products.sort(key=lambda x: x["count"], reverse=True)

        return {
            "ok": True,
            "coinbase": products,
            "period_s": 30,
            "log_time": last_rates_time,
            "ts": time.time(),
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "coinbase": [],
                "ts": time.time()}


# ══════════════════════════════════════════════════════════════════
# v8.2: SERVER-SIDE HISTORY — ring buffer + JSONL file for days of data
# ══════════════════════════════════════════════════════════════════
HISTORY_DIR = DATA_DIR / "dashboard_history"
HISTORY_FILE = DATA_DIR / "dashboard_history.jsonl"
HISTORY_MAX_MEM = 7200          # ~4 hours at 2-second intervals
HISTORY_MAX_DAYS = 7            # Keep 7 days of daily JSONL files
HISTORY_POLL_INTERVAL = 2.0     # Poll every 2 seconds

# In-memory ring buffer — fast access for recent queries (< 60 min)
_history_buffer: list = []
_history_lock = threading.Lock()
_history_current_day: str = ""   # Track current day for rotation


def _build_history_entry() -> dict:
    """Build one history entry from current Prometheus metrics + tape health.

    Returns a dict with the same shape as the frontend histRef entry,
    plus freshness fields. Returns None if metrics unavailable.
    """
    metrics = fetch_collector_metrics()
    if not metrics.get("ok"):
        return None

    health = get_all_health()
    tapes = {t["label"].lower(): t for t in health.get("tapes", [])}

    entry = {
        "ts": metrics.get("ts", time.time()),
        "rate": metrics.get("event_rate", 0),
        "p50": (metrics.get("latency_ms") or {}).get("p50", 0),
        "p95": (metrics.get("latency_ms") or {}).get("p95", 0),
        "p99": (metrics.get("latency_ms") or {}).get("p99", 0),
        "queue": metrics.get("queue_depth", 0),
        "disk": metrics.get("disk_free_gb", 0),
        "tape": metrics.get("tape_size_mb", 0),
        "seq": metrics.get("seq", 0),
        "uptime": metrics.get("uptime_seconds", 0),
        "cb": (metrics.get("events_total") or {}).get("coinbase", 0),
        "kl": (metrics.get("events_total") or {}).get("kalshi", 0),
        "cbGaps": (metrics.get("gaps_total") or {}).get("coinbase", 0),
        "klGaps": (metrics.get("gaps_total") or {}).get("kalshi", 0),
        # v8.2: Per-exchange rate gauges (smooth chart rendering — no counter deltas)
        "cbRate": (metrics.get("exchange_rate") or {}).get("coinbase", 0),
        "klRate": (metrics.get("exchange_rate") or {}).get("kalshi", 0),
        # v8.2: Network metrics
        "bytesPerSec": metrics.get("bytes_per_sec", 0),
        "msgSizeCb": (metrics.get("msg_size_avg") or {}).get("coinbase", 0),
        "msgSizeKl": (metrics.get("msg_size_avg") or {}).get("kalshi", 0),
        "wsRttCb": (metrics.get("ws_rtt_ms") or {}).get("coinbase", -1),
        "wsRttKl": (metrics.get("ws_rtt_ms") or {}).get("kalshi", -1),
        # Freshness (age in ms for each tape)
        "unified_age": tapes.get("unified", {}).get("age_ms"),
        "kalshi_age": tapes.get("kalshi", {}).get("age_ms"),
        "oracle_age": tapes.get("oracle", {}).get("age_ms"),
    }
    return entry


def _rotate_history_file():
    """Move current JSONL to daily folder if the day has changed."""
    global _history_current_day
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if _history_current_day and _history_current_day != today:
        # Day changed — rotate the current file into the daily folder
        if HISTORY_FILE.exists() and HISTORY_FILE.stat().st_size > 0:
            HISTORY_DIR.mkdir(parents=True, exist_ok=True)
            dest = HISTORY_DIR / f"{_history_current_day}.jsonl"
            try:
                HISTORY_FILE.rename(dest)
            except Exception:
                pass  # On Windows, rename can fail if file is open — skip

    _history_current_day = today

    # Cleanup: remove daily files older than HISTORY_MAX_DAYS
    if HISTORY_DIR.exists():
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(days=HISTORY_MAX_DAYS)
        cutoff_str = cutoff.strftime("%Y-%m-%d")
        for f in sorted(HISTORY_DIR.iterdir()):
            if f.is_file() and f.suffix == ".jsonl" and f.stem < cutoff_str:
                try:
                    f.unlink()
                except Exception:
                    pass


def _preload_history_buffer():
    """Pre-load the in-memory history buffer from JSONL files on disk.

    Called once at dashboard startup so that short-window queries (<=60 min)
    return data immediately instead of waiting 2-4 hours for the buffer to
    fill naturally.  This means restarting the dashboard no longer loses
    the recent history view — it picks up right where it left off.
    """
    global _history_buffer
    from datetime import datetime, timezone, timedelta

    loaded = []

    # 1. Read from today's live JSONL file
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        loaded.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except Exception:
            pass

    # 2. If today's file doesn't fill the buffer, also read yesterday's archive
    if len(loaded) < HISTORY_MAX_MEM and HISTORY_DIR.exists():
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        yesterday_file = HISTORY_DIR / f"{yesterday}.jsonl"
        if yesterday_file.exists():
            yesterday_entries = []
            try:
                with open(yesterday_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            yesterday_entries.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
            except Exception:
                pass
            # Prepend yesterday's tail to fill the buffer
            needed = HISTORY_MAX_MEM - len(loaded)
            if yesterday_entries and needed > 0:
                loaded = yesterday_entries[-needed:] + loaded

    # 3. Sort by timestamp, deduplicate, trim to max buffer size
    loaded.sort(key=lambda e: e.get("ts", 0))
    deduped = []
    last_ts = 0
    for entry in loaded:
        ts = entry.get("ts", 0)
        if ts > last_ts:
            deduped.append(entry)
            last_ts = ts

    with _history_lock:
        _history_buffer = deduped[-HISTORY_MAX_MEM:]

    count = len(_history_buffer)
    if count > 0:
        oldest = datetime.fromtimestamp(_history_buffer[0]["ts"], tz=timezone.utc).strftime("%H:%M:%S")
        newest = datetime.fromtimestamp(_history_buffer[-1]["ts"], tz=timezone.utc).strftime("%H:%M:%S")
        print(f"  History:     Pre-loaded {count} entries from disk ({oldest} -> {newest} UTC)")
    else:
        print(f"  History:     No previous entries found on disk")


# v8.2 fix: Rate smoother state — handles two problems:
# 1) Collector hasn't been restarted yet → exchange_rate gauges don't exist
# 2) Between 30s Prometheus updates, counters are stale → delta = 0
# The smoother computes rates when counters change and carries forward
# the last known rates during stale periods.
_rate_smooth = {
    "last_cb": 0, "last_kl": 0,        # Last observed counter values
    "last_ts": 0,                        # Timestamp when counters last changed
    "cb_rate": 0.0, "kl_rate": 0.0,     # Last computed per-exchange rates
}


def _smooth_exchange_rates(entry: dict) -> None:
    """Fill in cbRate/klRate if the collector's exchange_rate gauges are absent.

    When the collector is running an older version (no PROM_EXCHANGE_RATE),
    or during the first few seconds after a restart, the gauge values will
    be 0.  This function computes rates from the cumulative Prometheus
    counters and carries forward the last known rate between updates.
    """
    s = _rate_smooth

    # If the collector already provides gauge-based rates, trust them
    if entry.get("cbRate", 0) > 0 or entry.get("klRate", 0) > 0:
        s["cb_rate"] = entry["cbRate"]
        s["kl_rate"] = entry["klRate"]
        s["last_cb"] = entry.get("cb", 0)
        s["last_kl"] = entry.get("kl", 0)
        s["last_ts"] = entry["ts"]
        return

    # No gauge data — compute from cumulative counter deltas
    cb = entry.get("cb", 0)
    kl = entry.get("kl", 0)

    # Did the counters change since last observation?
    if cb != s["last_cb"] or kl != s["last_kl"]:
        if s["last_ts"] > 0:
            # Normal case: compute rate from delta / time
            dt = max(1, entry["ts"] - s["last_ts"])
            cb_delta = max(0, cb - s["last_cb"])
            kl_delta = max(0, kl - s["last_kl"])
            s["cb_rate"] = round(cb_delta / dt, 1)
            s["kl_rate"] = round(kl_delta / dt, 1)
        # else: first observation — just record the baseline, don't compute
        # a rate (we'd be dividing the entire counter history by 30s)
        s["last_cb"] = cb
        s["last_kl"] = kl
        s["last_ts"] = entry["ts"]

    # Apply the last known rates (freshly computed or carried forward).
    # On first poll with no prior data, use total rate split by counter ratio.
    if s["cb_rate"] == 0 and s["kl_rate"] == 0 and entry.get("rate", 0) > 0:
        # No rate computed yet — split total rate by counter proportion
        total = max(1, cb + kl)
        entry["cbRate"] = round(entry["rate"] * cb / total, 1)
        entry["klRate"] = round(entry["rate"] * kl / total, 1)
    else:
        entry["cbRate"] = s["cb_rate"]
        entry["klRate"] = s["kl_rate"]


# ══════════════════════════════════════════════════════════════════
# v8.5: FULL CONFIG LOADER — reads any section from collector_config.yaml
# ══════════════════════════════════════════════════════════════════
_full_config_cache = {"data": None, "mtime": 0}


def _load_full_config() -> dict:
    """Load & cache the entire collector_config.yaml. Re-reads on mtime change."""
    if not CONFIG_FILE.exists():
        return {}
    try:
        mtime = CONFIG_FILE.stat().st_mtime
        if _full_config_cache["data"] is not None and mtime == _full_config_cache["mtime"]:
            return _full_config_cache["data"]
        import yaml
        raw = CONFIG_FILE.read_text(encoding="utf-8")
        cfg = yaml.safe_load(raw) or {}
        _full_config_cache["data"] = cfg
        _full_config_cache["mtime"] = mtime
        return cfg
    except Exception:
        return {}


def _v85_cfg(section: str, *keys, default=None):
    """Get a nested value from any config section.

    Usage: _v85_cfg("health_score", "weights", "freshness", default=30)
    """
    cfg = _load_full_config().get(section, {})
    for k in keys:
        if isinstance(cfg, dict):
            cfg = cfg.get(k)
        else:
            return default
    return cfg if cfg is not None else default


# ══════════════════════════════════════════════════════════════════
# v8.5: COMPOSITE HEALTH SCORE — single 0-100 number
# ══════════════════════════════════════════════════════════════════
# Combines 5 weighted components into a single score:
#   Freshness (30%), Event Rate (20%), Connection Stability (20%),
#   Latency (15%), System Resources (15%)
# Updated every 2 seconds alongside the history poller.

# Rolling reconnect tracking for stability score
_health_reconnect_log: list = []  # list of (timestamp, reconnects_total)


def _score_linear(value: float, good: float, bad: float) -> float:
    """Map a value to 0-100 using linear interpolation between good and bad.

    good=threshold for score 100, bad=threshold for score 0.
    Works whether good < bad (higher is worse, like latency)
    or good > bad (higher is better, like event rate).
    """
    if good == bad:
        return 100.0 if value == good else 0.0
    # Normalize so that good→1.0 and bad→0.0
    t = (value - bad) / (good - bad)
    return max(0.0, min(100.0, t * 100.0))


def _compute_health_score(entry: dict) -> dict:
    """Compute a 0-100 composite health score from a history entry.

    Returns dict with: score (int 0-100), grade (A-F), components (dict).
    Each component is {score: 0-100, detail: "human-readable reason"}.
    """
    global _health_reconnect_log

    # ── Load config thresholds (with sane defaults) ──
    w_fresh = _v85_cfg("health_score", "weights", "freshness", default=30)
    w_rate  = _v85_cfg("health_score", "weights", "rate", default=20)
    w_stab  = _v85_cfg("health_score", "weights", "stability", default=20)
    w_lat   = _v85_cfg("health_score", "weights", "latency", default=15)
    w_res   = _v85_cfg("health_score", "weights", "resources", default=15)

    fresh_good = _v85_cfg("health_score", "thresholds", "freshness_good_s", default=5) * 1000  # convert to ms
    fresh_bad  = _v85_cfg("health_score", "thresholds", "freshness_bad_s", default=60) * 1000
    rate_good  = _v85_cfg("health_score", "thresholds", "rate_good", default=50)
    rate_bad   = _v85_cfg("health_score", "thresholds", "rate_bad", default=0)
    rc_window  = _v85_cfg("health_score", "thresholds", "reconnect_window_m", default=30) * 60  # to seconds
    rc_good    = _v85_cfg("health_score", "thresholds", "reconnect_good", default=0)
    rc_bad     = _v85_cfg("health_score", "thresholds", "reconnect_bad", default=5)
    p95_good   = _v85_cfg("health_score", "thresholds", "p95_good_ms", default=100)
    p95_bad    = _v85_cfg("health_score", "thresholds", "p95_bad_ms", default=1000)
    disk_good  = _v85_cfg("health_score", "thresholds", "disk_good_gb", default=20)
    disk_bad   = _v85_cfg("health_score", "thresholds", "disk_bad_gb", default=2)
    q_good     = _v85_cfg("health_score", "thresholds", "queue_good", default=0)
    q_bad      = _v85_cfg("health_score", "thresholds", "queue_bad", default=5000)

    # ── Component 1: Feed Freshness (worst tape wins) ──
    ages = []
    for key in ("unified_age", "kalshi_age", "oracle_age"):
        v = entry.get(key)
        if v is not None and v >= 0:
            ages.append(v)
    worst_age = max(ages) if ages else 99999
    c_fresh = _score_linear(worst_age, fresh_good, fresh_bad)

    # ── Component 2: Event Rate ──
    rate = entry.get("rate", 0)
    c_rate = _score_linear(rate, rate_good, rate_bad)

    # ── Component 3: Connection Stability (reconnects in rolling window) ──
    # Track total reconnects over time; count how many occurred in window
    now = entry.get("ts", time.time())
    total_rc = entry.get("cbGaps", 0) + entry.get("klGaps", 0)  # Use gaps as proxy
    # Also factor in actual reconnect count from metrics if available
    _health_reconnect_log.append((now, total_rc))
    # Trim entries outside the window
    cutoff = now - rc_window
    _health_reconnect_log = [(t, v) for t, v in _health_reconnect_log if t >= cutoff]
    # Count reconnects = change in total over window
    if len(_health_reconnect_log) >= 2:
        rc_delta = max(0, _health_reconnect_log[-1][1] - _health_reconnect_log[0][1])
    else:
        rc_delta = 0
    c_stab = _score_linear(rc_delta, rc_good, rc_bad)

    # ── Component 4: Latency ──
    p95 = abs(entry.get("p95", 0))  # abs() because latency can be negative (clock skew)
    c_lat = _score_linear(p95, p95_good, p95_bad)

    # ── Component 5: System Resources (disk + queue, take the worse one) ──
    disk = entry.get("disk", 0)
    queue = entry.get("queue", 0)
    s_disk = _score_linear(disk, disk_good, disk_bad)
    s_queue = _score_linear(queue, q_good, q_bad)
    c_res = min(s_disk, s_queue)  # Worst of disk or queue

    # ── Weighted average ──
    total_weight = w_fresh + w_rate + w_stab + w_lat + w_res
    if total_weight == 0:
        total_weight = 100
    score = (
        c_fresh * w_fresh +
        c_rate * w_rate +
        c_stab * w_stab +
        c_lat * w_lat +
        c_res * w_res
    ) / total_weight

    score = max(0, min(100, round(score)))

    # Grade assignment
    if score >= 90:
        grade = "A"
    elif score >= 80:
        grade = "B"
    elif score >= 70:
        grade = "C"
    elif score >= 60:
        grade = "D"
    else:
        grade = "F"

    return {
        "score": score,
        "grade": grade,
        "components": {
            "freshness":  {"score": round(c_fresh), "weight": w_fresh, "detail": f"worst tape {worst_age:.0f}ms"},
            "rate":       {"score": round(c_rate),  "weight": w_rate,  "detail": f"{rate:.1f} evt/s"},
            "stability":  {"score": round(c_stab),  "weight": w_stab,  "detail": f"{rc_delta} gaps in {rc_window/60:.0f}m"},
            "latency":    {"score": round(c_lat),   "weight": w_lat,   "detail": f"p95={p95:.0f}ms"},
            "resources":  {"score": round(c_res),   "weight": w_res,   "detail": f"disk={disk:.1f}GB queue={queue:.0f}"},
        },
    }


# Module-level cache for health score (so /api/collector-metrics can include it)
_latest_health_score = {"score": 0, "grade": "?", "components": {}}
_latest_anomaly_count = 0  # v8.6: Cache anomaly count for /api/collector-metrics
# v8.6: Cache freshness ages from history poller (avoids expensive get_all_health in API)
_latest_freshness = {"unified_age": None, "kalshi_age": None, "oracle_age": None}


# ══════════════════════════════════════════════════════════════════
# v8.5: SLA TRACKING — uptime %, MTTR, incident history
# ══════════════════════════════════════════════════════════════════
# Tracks "incidents" (periods when the system is down) and computes
# availability metrics like uptime percentage and mean time to recovery.
# Persisted to data/sla_tracker.json every 60 seconds.

SLA_FILE = DATA_DIR / "sla_tracker.json"

_sla_state = {
    "active_incident": None,     # {"start_ts": ..., "reason": ...} or None
    "incidents": [],             # Closed incidents: [{start_ts, end_ts, duration_s, reason}, ...]
    "daily": {},                 # "YYYY-MM-DD" -> {"checks": N, "down_checks": N}
    "total_checks": 0,           # Total 2-second checks since tracking started
    "down_checks": 0,            # Checks where system was "down"
    "rate_zero_since": None,     # Timestamp when rate first hit 0 (for grace period)
}
_sla_last_save = 0.0            # Throttle disk writes


def _load_sla_state():
    """Load SLA state from disk on startup."""
    global _sla_state
    if SLA_FILE.exists():
        try:
            data = json.loads(SLA_FILE.read_text(encoding="utf-8"))
            # Merge loaded data into state (keep defaults for missing keys)
            for k in ("incidents", "daily", "total_checks", "down_checks"):
                if k in data:
                    _sla_state[k] = data[k]
            # Don't restore active_incident — treat dashboard restart as recovery
        except Exception:
            pass  # Corrupted file — start fresh


def _save_sla_state():
    """Persist SLA state to disk (throttled to every 60s)."""
    global _sla_last_save
    now = time.time()
    interval = _v85_cfg("sla", "save_interval_s", default=60)
    if now - _sla_last_save < interval:
        return  # Too soon — skip
    _sla_last_save = now
    try:
        # Clean old incidents before saving
        retention = _v85_cfg("sla", "retention_days", default=30)
        cutoff = now - (retention * 86400)
        _sla_state["incidents"] = [
            inc for inc in _sla_state["incidents"]
            if inc.get("end_ts", now) >= cutoff
        ]
        # Clean old daily entries
        from datetime import datetime, timezone, timedelta
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=retention)).strftime("%Y-%m-%d")
        _sla_state["daily"] = {
            k: v for k, v in _sla_state["daily"].items()
            if k >= cutoff_date
        }
        SLA_FILE.write_text(json.dumps(_sla_state, indent=2), encoding="utf-8")
    except Exception:
        pass  # Non-fatal


def _update_sla(entry: dict, metrics_ok: bool):
    """Update SLA state with a new data point (called every 2s).

    Determines if the system is currently "down", manages incidents,
    and updates daily counters.
    """
    if not _v85_cfg("sla", "enabled", default=True):
        return

    now = entry.get("ts", time.time())
    threshold = _v85_cfg("sla", "incident_threshold_s", default=30) * 1000  # to ms
    grace = _v85_cfg("sla", "rate_zero_grace_s", default=10)

    # ── Determine if system is "down" ──
    is_down = False
    reason = ""

    # Check 1: Collector unreachable
    if not metrics_ok:
        is_down = True
        reason = "collector_down"

    # Check 2: Tape too old
    elif (entry.get("unified_age") or 0) > threshold:
        is_down = True
        reason = "tape_stale"

    # Check 3: Event rate = 0 for longer than grace period
    elif entry.get("rate", 0) <= 0:
        if _sla_state["rate_zero_since"] is None:
            _sla_state["rate_zero_since"] = now
        elif now - _sla_state["rate_zero_since"] > grace:
            is_down = True
            reason = "rate_zero"
    else:
        _sla_state["rate_zero_since"] = None

    # ── Update counters ──
    _sla_state["total_checks"] += 1
    if is_down:
        _sla_state["down_checks"] += 1

    # Daily counter
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if today not in _sla_state["daily"]:
        _sla_state["daily"][today] = {"checks": 0, "down_checks": 0}
    _sla_state["daily"][today]["checks"] += 1
    if is_down:
        _sla_state["daily"][today]["down_checks"] += 1

    # ── Incident management ──
    if is_down and _sla_state["active_incident"] is None:
        # New incident starting
        _sla_state["active_incident"] = {"start_ts": now, "reason": reason}

    elif not is_down and _sla_state["active_incident"] is not None:
        # Incident resolved — close it
        inc = _sla_state["active_incident"]
        duration = now - inc["start_ts"]
        _sla_state["incidents"].append({
            "start_ts": inc["start_ts"],
            "end_ts": now,
            "duration_s": round(duration, 1),
            "reason": inc["reason"],
        })
        _sla_state["active_incident"] = None

    # Persist to disk (throttled)
    _save_sla_state()


def get_sla_report() -> dict:
    """Build the SLA report for the /api/sla endpoint."""
    now = time.time()

    # Uptime percentage
    total = max(1, _sla_state["total_checks"])
    down = _sla_state["down_checks"]
    uptime_pct = round(((total - down) / total) * 100, 3)

    # MTTR (mean time to recovery) — from closed incidents
    closed = _sla_state["incidents"]
    durations = [inc["duration_s"] for inc in closed if inc.get("duration_s")]
    mttr = round(sum(durations) / len(durations), 1) if durations else 0.0

    # Incident counts by time window
    def count_since(seconds_ago):
        cutoff = now - seconds_ago
        return sum(1 for inc in closed if inc.get("end_ts", 0) >= cutoff)

    incidents_24h = count_since(86400)
    incidents_7d  = count_since(7 * 86400)
    incidents_30d = len(closed)

    # Daily uptime for charting
    daily_list = []
    for date_str in sorted(_sla_state["daily"].keys())[-30:]:
        d = _sla_state["daily"][date_str]
        d_total = max(1, d.get("checks", 1))
        d_down = d.get("down_checks", 0)
        daily_list.append({
            "date": date_str,
            "uptime_pct": round(((d_total - d_down) / d_total) * 100, 2),
            "incidents": sum(1 for inc in closed if date_str in str(inc.get("start_ts", ""))),
        })

    # Recent incidents (newest first, max 20)
    recent = sorted(closed, key=lambda x: x.get("end_ts", 0), reverse=True)[:20]

    # Active incident info
    active = None
    if _sla_state["active_incident"]:
        ai = _sla_state["active_incident"]
        active = {
            "start_ts": ai["start_ts"],
            "duration_s": round(now - ai["start_ts"], 1),
            "reason": ai["reason"],
        }

    return {
        "ok": True,
        "uptime_pct": uptime_pct,
        "mttr_s": mttr,
        "incidents_24h": incidents_24h,
        "incidents_7d": incidents_7d,
        "incidents_30d": incidents_30d,
        "active_incident": active,
        "daily": daily_list,
        "recent_incidents": recent,
        "total_checks": _sla_state["total_checks"],
        "tracking_since_hours": round((_sla_state["total_checks"] * 2) / 3600, 1),
    }


# ══════════════════════════════════════════════════════════════════
# v8.5: ANOMALY DETECTION — z-score based statistical outlier detection
# ══════════════════════════════════════════════════════════════════
# Maintains a rolling window for each tracked metric and flags values
# that deviate significantly from the running mean.

from collections import deque
import math

class _AnomalyDetector:
    """Detects statistical outliers in collector metrics using z-scores.

    For each tracked metric, maintains a rolling deque of recent values.
    When a new value deviates more than N standard deviations from the
    rolling mean, it's flagged as anomalous.
    """

    def __init__(self):
        self._windows: dict = {}          # metric_name -> deque of values
        self._active: dict = {}           # metric_name -> anomaly info dict
        self._log: list = []              # Recent anomaly events (max 100)
        self._max_log = 100

    def check(self, entry: dict) -> list:
        """Check a history entry for anomalies. Returns list of active anomaly dicts.

        Each anomaly dict: {metric, value, mean, std, z_score, severity, since_ts}
        """
        if not _v85_cfg("anomaly", "enabled", default=True):
            return []

        window_size = _v85_cfg("anomaly", "window_entries", default=900)
        min_baseline = _v85_cfg("anomaly", "min_baseline", default=60)
        alert_z = _v85_cfg("anomaly", "alert_z", default=3.0)
        warn_z = _v85_cfg("anomaly", "warn_z", default=2.0)
        tracked = _v85_cfg("anomaly", "tracked_metrics",
                           default=["rate", "p95", "queue", "cbRate", "klRate", "unified_age"])

        now = entry.get("ts", time.time())
        active_list = []

        for metric in tracked:
            value = entry.get(metric)
            if value is None:
                continue
            value = float(value)

            # Initialize window if needed
            if metric not in self._windows:
                self._windows[metric] = deque(maxlen=window_size)

            win = self._windows[metric]
            win.append(value)

            # Need enough data for a baseline
            if len(win) < min_baseline:
                continue

            # Compute rolling mean and std
            n = len(win)
            mean = sum(win) / n
            variance = sum((x - mean) ** 2 for x in win) / n
            std = math.sqrt(variance)

            # Z-score (epsilon prevents division by zero for constant metrics)
            epsilon = 0.001
            z = (value - mean) / max(std, epsilon)

            # Check thresholds
            abs_z = abs(z)
            if abs_z >= alert_z:
                severity = "alert"
            elif abs_z >= warn_z:
                severity = "warning"
            else:
                # Not anomalous — clear any active anomaly for this metric
                if metric in self._active:
                    del self._active[metric]
                continue

            # Record the anomaly
            info = {
                "metric": metric,
                "value": round(value, 2),
                "mean": round(mean, 2),
                "std": round(std, 2),
                "z_score": round(z, 2),
                "severity": severity,
                "since_ts": self._active.get(metric, {}).get("since_ts", now),
            }

            # If newly anomalous (wasn't active before), log it
            if metric not in self._active:
                self._log.append({**info, "ts": now})
                if len(self._log) > self._max_log:
                    self._log = self._log[-self._max_log:]

            self._active[metric] = info
            active_list.append(info)

        return active_list

    def get_active(self) -> list:
        """Return list of currently active anomalies."""
        return list(self._active.values())

    def get_recent_log(self, limit: int = 50) -> list:
        """Return recent anomaly events (newest first)."""
        return list(reversed(self._log[-limit:]))


# Global anomaly detector instance
_anomaly_detector = _AnomalyDetector()


def get_anomaly_report() -> dict:
    """Build the anomaly report for the /api/anomalies endpoint."""
    return {
        "ok": True,
        "active": _anomaly_detector.get_active(),
        "recent": _anomaly_detector.get_recent_log(50),
        "count": len(_anomaly_detector.get_active()),
    }


# ══════════════════════════════════════════════════════════════════
# TRADE DATA — reads execution logs from live/paper engine sessions
# ══════════════════════════════════════════════════════════════════

# Cache: stores last result + timestamp so we don't re-scan the filesystem every call




def _history_poller():
    """Background thread that polls metrics every 2s and stores history.

    Appends to both the in-memory ring buffer (for fast queries)
    and a JSONL file on disk (for long-term storage).
    v8.5: Also computes health score, updates SLA tracking, and runs anomaly detection.
    """
    global _history_buffer, _latest_health_score, _latest_anomaly_count, _latest_freshness

    # Load SLA state from disk on startup
    _load_sla_state()

    # v8.6: Pre-load recent history from JSONL into memory buffer
    # so short-window queries work immediately after restart
    _preload_history_buffer()

    while True:
        try:
            _rotate_history_file()
            entry = _build_history_entry()
            metrics_ok = entry is not None
            if entry:
                # Smooth per-exchange rates (handles stale counters + old collector)
                _smooth_exchange_rates(entry)

                # v8.5: Compute composite health score
                hs = _compute_health_score(entry)
                entry["health_score"] = hs["score"]
                entry["health_grade"] = hs["grade"]
                _latest_health_score = hs

                # v8.5: Update SLA tracking
                _update_sla(entry, metrics_ok=True)

                # v8.5: Run anomaly detection
                anomalies = _anomaly_detector.check(entry)
                entry["anomaly_count"] = len(anomalies)
                _latest_anomaly_count = len(anomalies)
                # v8.6: Cache freshness ages for /api/collector-metrics
                _latest_freshness = {
                    "unified_age": entry.get("unified_age"),
                    "kalshi_age": entry.get("kalshi_age"),
                    "oracle_age": entry.get("oracle_age"),
                }

                # Append to in-memory ring buffer
                with _history_lock:
                    _history_buffer.append(entry)
                    if len(_history_buffer) > HISTORY_MAX_MEM:
                        _history_buffer = _history_buffer[-HISTORY_MAX_MEM:]
                # Append to JSONL file on disk
                try:
                    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
                        f.write(json.dumps(entry, separators=(",", ":")) + "\n")
                except Exception:
                    pass  # Disk write failure is non-fatal
            else:
                # Metrics unavailable — update SLA as down
                _update_sla({"ts": time.time(), "rate": 0, "unified_age": 999999}, metrics_ok=False)
        except Exception:
            pass  # Never crash the background thread
        time.sleep(HISTORY_POLL_INTERVAL)


def _downsample(entries: list, max_points: int) -> list:
    """Downsample a list of history entries to at most max_points."""
    if len(entries) <= max_points:
        return entries
    step = len(entries) / max_points
    result = []
    for i in range(max_points):
        idx = int(i * step)
        result.append(entries[idx])
    return result


def get_history(minutes: int = 60) -> dict:
    """Return history entries for the last N minutes.

    For requests <= 60 min: serve from in-memory buffer (fast).
    For requests > 60 min: read from JSONL files on disk.
    Max 3600 points returned (downsampled if necessary).
    """
    minutes = max(1, min(minutes, 10080))  # Clamp to 1 min - 7 days
    cutoff = time.time() - (minutes * 60)

    if minutes <= 60:
        # Serve from memory — fast path
        with _history_lock:
            filtered = [e for e in _history_buffer if e["ts"] >= cutoff]
        return {
            "ok": True,
            "entries": _downsample(filtered, 3600),
            "source": "memory",
            "count": len(filtered),
            "ts": time.time(),
        }

    # Serve from disk — read JSONL files
    all_entries = []

    # Read today's live file
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        if entry.get("ts", 0) >= cutoff:
                            all_entries.append(entry)
                    except json.JSONDecodeError:
                        continue
        except Exception:
            pass

    # Read previous daily files
    if HISTORY_DIR.exists():
        from datetime import datetime, timezone
        for daily_file in sorted(HISTORY_DIR.iterdir()):
            if not daily_file.is_file() or daily_file.suffix != ".jsonl":
                continue
            try:
                with open(daily_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            if entry.get("ts", 0) >= cutoff:
                                all_entries.append(entry)
                        except json.JSONDecodeError:
                            continue
            except Exception:
                continue

    # Sort by timestamp and downsample
    all_entries.sort(key=lambda e: e.get("ts", 0))
    return {
        "ok": True,
        "entries": _downsample(all_entries, 3600),
        "source": "disk",
        "count": len(all_entries),
        "ts": time.time(),
    }


def get_freshness_history(minutes: int = 60) -> dict:
    """Return freshness history (tape age) for the last N minutes.

    Uses the same history entries but extracts only freshness fields.
    """
    result = get_history(minutes)
    if not result.get("ok"):
        return result

    fresh_entries = []
    for e in result.get("entries", []):
        fresh_entries.append({
            "ts": e.get("ts"),
            "unified": e.get("unified_age"),
            "kalshi": e.get("kalshi_age"),
            "oracle": e.get("oracle_age"),
        })

    return {
        "ok": True,
        "entries": fresh_entries,
        "source": result.get("source"),
        "count": result.get("count"),
        "ts": time.time(),
    }


# ══════════════════════════════════════════════════════════════════
# v8.2: TAPE EVENT INSPECTOR — fetch raw events around a timestamp
# ══════════════════════════════════════════════════════════════════
def get_tape_events(ts: float, window: float = 10.0, max_events: int = 500) -> dict:
    """Fetch raw tape events around a given timestamp.

    Searches the unified tape (live or archive) for events within
    [ts - window/2, ts + window/2]. Returns parsed events with
    sequence number, source, latency, and raw message content.

    Args:
        ts: Unix timestamp (seconds) of the center of the window
        window: Size of the time window in seconds (default 10)
        max_events: Maximum events to return (default 500)
    """
    ts_us_center = int(ts * 1_000_000)
    half_window_us = int((window / 2) * 1_000_000)
    ts_us_start = ts_us_center - half_window_us
    ts_us_end = ts_us_center + half_window_us

    events = []
    total_scanned = 0
    source_file = ""

    # Try live tape first — if the target timestamp is recent
    files_to_scan = []

    # 1. Live unified tape
    if UNIFIED_TAPE.exists():
        files_to_scan.append(UNIFIED_TAPE)

    # 2. Archive files — check daily subfolders for the target date
    from datetime import datetime, timezone
    target_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    target_day = target_dt.strftime("%Y-%m-%d")

    if UNIFIED_ARCHIVE.exists():
        # Check the target day's subfolder
        daily_dir = UNIFIED_ARCHIVE / target_day
        if daily_dir.exists():
            for f in sorted(daily_dir.iterdir()):
                if f.is_file() and f.suffix == ".jsonl":
                    files_to_scan.append(f)
        # Also check legacy flat files
        for f in sorted(UNIFIED_ARCHIVE.iterdir()):
            if f.is_file() and f.suffix == ".jsonl":
                files_to_scan.append(f)

    for scan_file in files_to_scan:
        if len(events) >= max_events:
            break
        try:
            with open(scan_file, "r", encoding="utf-8") as f:
                for line in f:
                    total_scanned += 1
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        entry_ts_us = entry.get("ts_us", 0)

                        # Skip entries outside our window
                        if entry_ts_us < ts_us_start:
                            continue
                        if entry_ts_us > ts_us_end:
                            # Past the end of our window — if file is time-ordered,
                            # we can stop scanning this file
                            break

                        # Parse the inner raw message
                        raw_str = entry.get("raw", "")
                        raw_parsed = {}
                        try:
                            raw_parsed = json.loads(raw_str) if isinstance(
                                raw_str, str) and raw_str else (
                                    raw_str if isinstance(raw_str, dict) else {})
                        except (json.JSONDecodeError, TypeError):
                            raw_parsed = {"_raw": str(raw_str)[:200]}

                        # Compute latency if exchange timestamp is available
                        latency_ms = None
                        # Coinbase: "time" field is ISO timestamp
                        if raw_parsed.get("time"):
                            try:
                                from datetime import datetime as _dt
                                exchange_ts = _dt.fromisoformat(
                                    raw_parsed["time"].replace("Z", "+00:00")
                                ).timestamp()
                                local_ts = entry_ts_us / 1_000_000
                                latency_ms = round(
                                    (local_ts - exchange_ts) * 1000, 1)
                            except Exception:
                                pass
                        # Kalshi: "ts" field in msg (ISO string or Unix)
                        elif raw_parsed.get("msg", {}).get("ts"):
                            try:
                                kl_ts = raw_parsed["msg"]["ts"]
                                if isinstance(kl_ts, str):
                                    from datetime import datetime as _dt
                                    exchange_ts = _dt.fromisoformat(
                                        kl_ts.replace("Z", "+00:00")
                                    ).timestamp()
                                elif isinstance(kl_ts, (int, float)):
                                    exchange_ts = float(kl_ts)
                                else:
                                    exchange_ts = None
                                if exchange_ts:
                                    local_ts = entry_ts_us / 1_000_000
                                    latency_ms = round(
                                        (local_ts - exchange_ts) * 1000, 1)
                            except Exception:
                                pass

                        # Format human-readable timestamp
                        ts_human = datetime.fromtimestamp(
                            entry_ts_us / 1_000_000, tz=timezone.utc
                        ).strftime("%H:%M:%S.%f")[:-3]

                        # Extract rich fields based on message type
                        msg_type = raw_parsed.get("type",
                                    raw_parsed.get("msg", {}).get("type", "?"))
                        msg_inner = raw_parsed.get("msg", {})

                        if msg_type == "ticker" and entry.get("src") == "cb":
                            # Coinbase ticker: price, product_id, best_bid, best_ask
                            raw_out = {
                                "type": "ticker",
                                "product": raw_parsed.get("product_id", "?"),
                                "price": raw_parsed.get("price", "?"),
                                "detail": "",
                            }
                        elif msg_type == "ticker" and entry.get("src") == "kl":
                            # Kalshi ticker: price, yes_bid, yes_ask, volume
                            price_c = msg_inner.get("price", "?")
                            bid_c = msg_inner.get("yes_bid", "?")
                            ask_c = msg_inner.get("yes_ask", "?")
                            raw_out = {
                                "type": "ticker",
                                "product": msg_inner.get("market_ticker", "?"),
                                "price": price_c,
                                "detail": f"bid {bid_c} / ask {ask_c}",
                            }
                        elif msg_type == "orderbook_delta":
                            # Kalshi orderbook_delta: price level, delta, side
                            price_c = msg_inner.get("price", "?")
                            delta = msg_inner.get("delta", 0)
                            side = msg_inner.get("side", "?")
                            # Format delta: positive = add, negative = remove
                            delta_str = f"+{delta}" if delta > 0 else str(delta)
                            raw_out = {
                                "type": "ob_delta",
                                "product": msg_inner.get("market_ticker", "?"),
                                "price": f"{price_c}c",
                                "detail": f"{side} {delta_str}",
                            }
                        elif msg_type == "trade":
                            # Kalshi trade: yes_price, no_price, count, taker_side
                            yes_p = msg_inner.get("yes_price", "?")
                            count = msg_inner.get("count", 0)
                            taker = msg_inner.get("taker_side", "?")
                            raw_out = {
                                "type": "trade",
                                "product": msg_inner.get("market_ticker", "?"),
                                "price": f"{yes_p}c",
                                "detail": f"{taker} x{count}",
                            }
                        elif msg_type == "orderbook_snapshot":
                            # Kalshi snapshot
                            raw_out = {
                                "type": "snapshot",
                                "product": msg_inner.get("market_ticker", "?"),
                                "price": "—",
                                "detail": "full book",
                            }
                        else:
                            raw_out = {
                                "type": msg_type,
                                "product": raw_parsed.get("product_id",
                                            msg_inner.get("market_ticker", "?")),
                                "price": raw_parsed.get("price",
                                          msg_inner.get("price", "?")),
                                "detail": "",
                            }

                        events.append({
                            "seq": entry.get("seq"),
                            "ts_us": entry_ts_us,
                            "ts_human": ts_human,
                            "src": entry.get("src", "?"),
                            "latency_ms": latency_ms,
                            "raw": raw_out,
                        })
                        source_file = scan_file.name

                        if len(events) >= max_events:
                            break

                    except (json.JSONDecodeError, KeyError):
                        continue
        except Exception:
            continue

    return {
        "ok": True,
        "events": events,
        "file": source_file,
        "total_scanned": total_scanned,
        "window_s": window,
        "center_ts": ts,
        "ts": time.time(),
    }


# ══════════════════════════════════════════════════════════════════
# v8.3: EMAIL ALERT SYSTEM
# ══════════════════════════════════════════════════════════════════
# Sends email alerts when collector conditions go bad (data stopped,
# collector down, WS disconnect, disk critical, high error rate).
# Sends recovery emails when conditions clear.
# Uses Gmail SMTP with an App Password stored in api.env.
# ══════════════════════════════════════════════════════════════════

# ── Per-condition state tracking ──
# Each condition has: active (bool), last_sent (timestamp), last_cleared (timestamp),
#                     consecutive (int counter for collector_down)
_alert_state = {
    "data_stopped":    {"active": False, "last_sent": 0, "last_cleared": 0, "detail": ""},
    "collector_down":  {"active": False, "last_sent": 0, "last_cleared": 0, "detail": "",
                        "consecutive": 0},
    "ws_disconnect":   {"active": False, "last_sent": 0, "last_cleared": 0, "detail": "",
                        "last_reconnects": 0},
    "disk_critical":   {"active": False, "last_sent": 0, "last_cleared": 0, "detail": ""},
    "high_error_rate": {"active": False, "last_sent": 0, "last_cleared": 0, "detail": "",
                        "consecutive_minutes": 0},
}
_alert_state_lock = threading.Lock()

# Recent alert event log (for the /api/alert-status endpoint)
_alert_events: list = []    # [{ts, condition, action, detail}, ...]
_ALERT_EVENTS_MAX = 100     # Keep last 100 events


def _log_alert_event(condition: str, action: str, detail: str):
    """Record an alert event for the API status endpoint."""
    _alert_events.append({
        "ts": time.time(),
        "condition": condition,
        "action": action,       # "FIRED", "CLEARED", "REPEAT_SUPPRESSED"
        "detail": detail,
    })
    # Trim to max
    while len(_alert_events) > _ALERT_EVENTS_MAX:
        _alert_events.pop(0)


# ── Email Sender ──
def _send_alert_email(subject: str, html_body: str) -> dict:
    """Send an HTML email via Gmail SMTP.

    Loads the app password fresh from api.env each call so you can
    update it without restarting the dashboard.
    Returns {"ok": True} on success, {"ok": False, "error": "..."} on failure.
    """
    password = _load_env_value("GMAIL_APP_PASSWORD")
    if not password:
        print("[ALERT] Cannot send email -- no GMAIL_APP_PASSWORD in api.env")
        return {"ok": False, "error": "No GMAIL_APP_PASSWORD in api.env"}

    recipient = _alert_cfg("recipient", default="your-email@example.com")
    sender = _alert_cfg("sender", default="your-email@example.com")
    smtp_host = _alert_cfg("smtp_host", default="smtp.gmail.com")
    smtp_port = _alert_cfg("smtp_port", default=587)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"Orion Collector <{sender}>"
    msg["To"] = recipient
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, [recipient], msg.as_string())
        print(f"[ALERT] Email sent OK")
        return {"ok": True}
    except Exception as e:
        err_msg = str(e)
        print(f"[ALERT] Email FAILED: {err_msg}")
        return {"ok": False, "error": err_msg}


# ── HTML Email Template ──
def _build_alert_html(condition_name: str, detail: str,
                      is_recovery: bool = False) -> str:
    """Build a dark-themed HTML email body for an alert or recovery.

    Args:
        condition_name: Human-readable name like "Data Flow Stopped"
        detail: One-line description of what happened
        is_recovery: True for green "RESOLVED" banner, False for red "ALERT"
    """
    # Local timestamp for the email body (user's machine is Eastern Time)
    now_local = datetime.now()
    ts_str = now_local.strftime("%B %d, %Y  %I:%M:%S %p ET")

    # Disk space info (always useful context)
    try:
        usage = shutil.disk_usage(DATA_DIR)
        disk_free_gb = round(usage.free / (1024 ** 3), 1)
        disk_total_gb = round(usage.total / (1024 ** 3), 1)
        disk_pct = round((usage.used / usage.total) * 100, 1)
        disk_info = f"{disk_free_gb} GB free of {disk_total_gb} GB ({disk_pct}% used)"
    except Exception:
        disk_info = "unavailable"

    if is_recovery:
        banner_color = "#00FF88"
        banner_bg = "#0d3320"
        banner_text = "✅ RESOLVED"
        status_line = f"{condition_name} has cleared."
    else:
        banner_color = "#FF4444"
        banner_bg = "#3d1111"
        banner_text = "🚨 ALERT"
        status_line = f"{condition_name} detected."

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#0a0a0f;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#0a0a0f;padding:20px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#12121a;border-radius:12px;overflow:hidden;">

  <!-- Banner -->
  <tr><td style="background:{banner_bg};padding:20px 30px;border-bottom:2px solid {banner_color};">
    <span style="color:{banner_color};font-size:22px;font-weight:700;letter-spacing:1px;">
      {banner_text}
    </span>
    <span style="color:#64748B;font-size:13px;float:right;line-height:30px;">
      ORION Collector
    </span>
  </td></tr>

  <!-- Condition -->
  <tr><td style="padding:30px;">
    <div style="color:#E2E8F0;font-size:18px;font-weight:600;margin-bottom:8px;">
      {condition_name}
    </div>
    <div style="color:#94A3B8;font-size:14px;margin-bottom:20px;">
      {status_line}
    </div>

    <!-- Detail box -->
    <div style="background:#1a1a2e;border:1px solid #2d2d44;border-radius:8px;padding:16px;margin-bottom:20px;">
      <div style="color:#64748B;font-size:11px;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;">Detail</div>
      <div style="color:#CBD5E1;font-size:14px;font-family:monospace;">{detail}</div>
    </div>

    <!-- Info row -->
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;">
    <tr>
      <td width="50%" style="padding:10px;background:#1a1a2e;border-radius:8px 0 0 8px;">
        <div style="color:#64748B;font-size:11px;text-transform:uppercase;letter-spacing:1px;">Timestamp</div>
        <div style="color:#E2E8F0;font-size:13px;font-family:monospace;margin-top:4px;">{ts_str}</div>
      </td>
      <td width="50%" style="padding:10px;background:#1a1a2e;border-radius:0 8px 8px 0;border-left:1px solid #2d2d44;">
        <div style="color:#64748B;font-size:11px;text-transform:uppercase;letter-spacing:1px;">Disk Space</div>
        <div style="color:#E2E8F0;font-size:13px;font-family:monospace;margin-top:4px;">{disk_info}</div>
      </td>
    </tr>
    </table>

    <div style="color:#475569;font-size:12px;text-align:center;padding-top:10px;border-top:1px solid #1e293b;">
      Orion Collector Dashboard &mdash; v8.3 Alert System
    </div>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


# ── Five Condition Checkers ──
# Each returns (is_triggered: bool, detail: str)

def _check_data_stopped(metrics: dict, health: dict) -> tuple:
    """Check if data flow has stopped (unified tape too old AND rate = 0).

    Trigger: unified tape age > threshold AND event rate is 0.
    """
    threshold_s = _alert_cfg("conditions", "data_stopped",
                             "tape_age_threshold_s", default=60)
    threshold_ms = threshold_s * 1000

    # Get unified tape age from health
    tapes = {t["label"].lower(): t for t in health.get("tapes", [])}
    unified = tapes.get("unified", {})
    age_ms = unified.get("age_ms")
    rate = metrics.get("event_rate", 0)

    if age_ms is not None and age_ms > threshold_ms and rate == 0:
        age_s = round(age_ms / 1000, 1)
        return True, f"Unified tape is {age_s}s old (threshold: {threshold_s}s), event rate = 0"

    return False, ""


def _check_collector_down(metrics: dict) -> tuple:
    """Check if the collector's Prometheus endpoint is unreachable.

    Trigger: fetch_collector_metrics() fails N times in a row.
    We track consecutive failures in _alert_state["collector_down"]["consecutive"].
    """
    threshold = _alert_cfg("conditions", "collector_down",
                           "consecutive_failures", default=5)

    with _alert_state_lock:
        state = _alert_state["collector_down"]
        if not metrics.get("ok"):
            state["consecutive"] += 1
            if state["consecutive"] >= threshold:
                return True, (f"Prometheus endpoint unreachable for "
                              f"{state['consecutive']} consecutive checks")
        else:
            state["consecutive"] = 0

    return False, ""


def _check_ws_disconnect(metrics: dict) -> tuple:
    """Check if WebSocket connections have recently dropped.

    Trigger: connection uptime < threshold AND reconnect count increased.
    This means the connection recently dropped and reconnected.
    """
    threshold_s = _alert_cfg("conditions", "ws_disconnect",
                             "uptime_threshold_s", default=120)

    uptime_raw = metrics.get("connection_uptime_seconds")
    # connection_uptime_seconds may be a dict (per-exchange labels) or a float.
    # If dict, take the minimum (worst-case) across exchanges.
    if isinstance(uptime_raw, dict):
        uptime = min(uptime_raw.values()) if uptime_raw else None
    else:
        uptime = uptime_raw
    reconnects = (metrics.get("reconnects_total") or {})
    # Sum all exchange reconnects
    total_reconnects = sum(reconnects.values()) if isinstance(reconnects, dict) else 0

    with _alert_state_lock:
        state = _alert_state["ws_disconnect"]
        prev_reconnects = state.get("last_reconnects", 0)

        if uptime is not None and uptime < threshold_s and total_reconnects > prev_reconnects:
            state["last_reconnects"] = total_reconnects
            return True, (f"WebSocket reconnected (uptime: {round(uptime, 1)}s < "
                          f"{threshold_s}s threshold, reconnects: {total_reconnects})")

        # Always track reconnects even when not triggered
        if total_reconnects > 0:
            state["last_reconnects"] = total_reconnects

    return False, ""


def _check_disk_critical() -> tuple:
    """Check if disk space is dangerously low.

    Uses shutil.disk_usage() directly — works even if the collector is down.
    """
    threshold_gb = _alert_cfg("conditions", "disk_critical",
                              "threshold_gb", default=5.0)

    try:
        usage = shutil.disk_usage(DATA_DIR)
        free_gb = usage.free / (1024 ** 3)
        if free_gb < threshold_gb:
            return True, (f"Disk free: {round(free_gb, 2)} GB "
                          f"(threshold: {threshold_gb} GB)")
    except Exception as e:
        return True, f"Cannot read disk space: {e}"

    return False, ""


def _check_high_error_rate() -> tuple:
    """Check if the error rate in the collector log is too high.

    Trigger: errors/min > threshold for N consecutive minutes.
    """
    errors_per_min = _alert_cfg("conditions", "high_error_rate",
                                "errors_per_minute", default=10)
    consecutive_min = _alert_cfg("conditions", "high_error_rate",
                                 "consecutive_minutes", default=2)

    error_data = get_error_rate()
    if not error_data.get("ok"):
        return False, ""

    buckets = error_data.get("buckets", [])
    if len(buckets) < consecutive_min:
        return False, ""

    # Check the last N minutes
    recent = buckets[-consecutive_min:]
    all_above = all(b["errors"] >= errors_per_min for b in recent)

    if all_above:
        avg_errors = round(sum(b["errors"] for b in recent) / len(recent), 1)
        return True, (f"Error rate: {avg_errors} errors/min for last "
                      f"{consecutive_min} minutes (threshold: {errors_per_min}/min)")

    # Track consecutive minutes for state
    with _alert_state_lock:
        state = _alert_state["high_error_rate"]
        consec = 0
        for b in reversed(buckets):
            if b["errors"] >= errors_per_min:
                consec += 1
            else:
                break
        state["consecutive_minutes"] = consec

    return False, ""


# ── Human-readable names for conditions ──
_CONDITION_NAMES = {
    "data_stopped":    "Data Flow Stopped",
    "collector_down":  "Collector Down",
    "ws_disconnect":   "WebSocket Disconnected",
    "disk_critical":   "Disk Space Critical",
    "high_error_rate": "High Error Rate",
}


# ── Alert Monitor Thread ──
def _alert_monitor():
    """Background thread that checks all alert conditions periodically.

    Runs every check_interval_s (default 10 seconds).
    For each condition:
      - If newly triggered → send alert email
      - If still triggered but cooldown expired → log suppression
      - If cleared → send recovery email (if send_recovery is true)
    """
    print("[ALERT] Monitor thread started")
    while True:
        try:
            # Check if alerts are enabled globally
            if not _alert_cfg("enabled", default=True):
                time.sleep(30)
                continue

            check_interval = _alert_cfg("check_interval_s", default=10)
            cooldown_s = _alert_cfg("cooldown_minutes", default=30) * 60
            send_recovery = _alert_cfg("send_recovery", default=True)

            # Fetch current metrics + health (used by multiple checkers)
            metrics = fetch_collector_metrics()
            health = get_all_health()

            # Run each condition checker
            checks = {
                "data_stopped":    _check_data_stopped(metrics, health),
                "collector_down":  _check_collector_down(metrics),
                "ws_disconnect":   _check_ws_disconnect(metrics),
                "disk_critical":   _check_disk_critical(),
                "high_error_rate": _check_high_error_rate(),
            }

            now = time.time()

            for cond_key, (triggered, detail) in checks.items():
                # Skip disabled conditions
                if not _alert_cfg("conditions", cond_key, "enabled", default=True):
                    continue

                with _alert_state_lock:
                    state = _alert_state[cond_key]
                    was_active = state["active"]

                    if triggered and not was_active:
                        # ── Newly triggered → send alert email ──
                        state["active"] = True
                        state["detail"] = detail
                        name = _CONDITION_NAMES.get(cond_key, cond_key)
                        subject = f"🚨 ORION ALERT: {name}"
                        html = _build_alert_html(name, detail, is_recovery=False)
                        _send_alert_email(subject, html)
                        state["last_sent"] = now
                        _log_alert_event(cond_key, "FIRED", detail)
                        print(f"[ALERT] FIRED: {cond_key} — {detail}")

                    elif triggered and was_active:
                        # ── Still triggered — check cooldown for repeat ──
                        state["detail"] = detail  # Update detail
                        elapsed = now - state["last_sent"]
                        if elapsed >= cooldown_s:
                            name = _CONDITION_NAMES.get(cond_key, cond_key)
                            subject = f"🚨 ORION ALERT (repeat): {name}"
                            html = _build_alert_html(name, detail, is_recovery=False)
                            _send_alert_email(subject, html)
                            state["last_sent"] = now
                            _log_alert_event(cond_key, "REPEAT", detail)
                            print(f"[ALERT] REPEAT: {cond_key} — {detail}")

                    elif not triggered and was_active:
                        # ── Condition cleared → send recovery email ──
                        state["active"] = False
                        state["last_cleared"] = now
                        name = _CONDITION_NAMES.get(cond_key, cond_key)
                        if send_recovery:
                            subject = f"✅ ORION RESOLVED: {name}"
                            recovery_detail = (f"Previously: {state['detail']}"
                                               if state['detail'] else "Condition cleared")
                            html = _build_alert_html(name, recovery_detail,
                                                     is_recovery=True)
                            _send_alert_email(subject, html)
                        _log_alert_event(cond_key, "CLEARED", "")
                        print(f"[ALERT] CLEARED: {cond_key}")
                        state["detail"] = ""

            time.sleep(check_interval)

        except Exception as e:
            print(f"[ALERT] Monitor error: {e}")
            time.sleep(10)  # Don't crash, just retry


# ── Alert API helpers ──
def get_alert_status() -> dict:
    """Return current state of all alert conditions for the API.

    Used by GET /api/alert-status.
    """
    # Check if email is configured
    has_password = bool(_load_env_value("GMAIL_APP_PASSWORD"))
    enabled = _alert_cfg("enabled", default=True)

    conditions = {}
    with _alert_state_lock:
        for cond_key, state in _alert_state.items():
            cond_enabled = _alert_cfg("conditions", cond_key, "enabled", default=True)
            conditions[cond_key] = {
                "name": _CONDITION_NAMES.get(cond_key, cond_key),
                "enabled": cond_enabled,
                "active": state["active"],
                "detail": state["detail"],
                "last_sent": state["last_sent"],
                "last_cleared": state["last_cleared"],
            }

    return {
        "ok": True,
        "alerts_enabled": enabled,
        "email_configured": has_password,
        "recipient": _alert_cfg("recipient", default=""),
        "cooldown_minutes": _alert_cfg("cooldown_minutes", default=30),
        "conditions": conditions,
        "recent_events": _alert_events[-20:][::-1],  # Last 20, newest first
        "ts": time.time(),
    }


def send_test_email() -> dict:
    """Send a test email to verify the alert system is working.

    Used by GET /api/alert-test.
    """
    has_password = bool(_load_env_value("GMAIL_APP_PASSWORD"))
    if not has_password:
        return {
            "ok": False,
            "error": "No GMAIL_APP_PASSWORD found in api.env. "
                     "Add it and try again.",
            "ts": time.time(),
        }

    subject = "[TEST] ORION Alert System -- Email Working"
    html = _build_alert_html(
        "Test Alert",
        "This is a test email from the Orion Collector Dashboard alert system. "
        "If you received this, alerts are configured correctly!",
        is_recovery=False,
    )
    result = _send_alert_email(subject, html)
    recipient = _alert_cfg("recipient", default="")
    if result["ok"]:
        return {
            "ok": True,
            "message": f"Test email sent to {recipient}",
            "recipient": recipient,
            "ts": time.time(),
        }
    else:
        return {
            "ok": False,
            "message": "Failed to send test email",
            "error": result.get("error", "Unknown error"),
            "recipient": recipient,
            "ts": time.time(),
        }


# ══════════════════════════════════════════════════════════════════
# JSX TRANSFORMER — same pattern as orion_dashboard.py
# ══════════════════════════════════════════════════════════════════
def transform_jsx(raw_jsx: str) -> str:
    """Transform React module JSX to work with CDN-loaded globals."""
    lines = raw_jsx.split("\n")
    out = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("import {") and '"react"' in stripped:
            between = stripped.split("{")[1].split("}")[0]
            out.append(f"const {{{between}}} = React;")
            continue
        if stripped.startswith("import") and '"d3"' in stripped:
            out.append("// d3 loaded from CDN as window.d3")
            continue
        if stripped.startswith("import") and " from " in stripped:
            out.append(f"// SKIPPED: {stripped}")
            continue
        if stripped.startswith("export default function"):
            out.append(line.replace("export default function", "function"))
            continue
        if stripped.startswith("export default"):
            out.append(line.replace("export default", ""))
            continue
        out.append(line)
    return "\n".join(out)


# ══════════════════════════════════════════════════════════════════
# HTML TEMPLATE — loads React/D3/Babel from CDN
# ══════════════════════════════════════════════════════════════════
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>ORION Collector Dashboard</title>
  <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.9.0/d3.min.js"></script>
  <script src="https://unpkg.com/@babel/standalone@7/babel.min.js"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    html, body, #root { width: 100%; min-height: 100%; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; overflow-y: auto; }
    @keyframes pulse { 0%,100% { opacity:1; } 50% { opacity:0.5; } }
    @keyframes glowPulse { 0%,100% { box-shadow:0 0 8px rgba(0,255,136,0.15); } 50% { box-shadow:0 0 24px rgba(0,255,136,0.4); } }
    @keyframes anomalyGlow { 0%,100% { box-shadow:0 0 6px rgba(255,140,0,0.2); } 50% { box-shadow:0 0 18px rgba(255,140,0,0.5); } }
    #loading { display:flex; align-items:center; justify-content:center; height:100vh;
               background:#0a0a0f; color:#94A3B8; font-family:monospace; font-size:14px; }
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.1); border-radius: 3px; }
  </style>
</head>
<body>
  <div id="root"><div id="loading">Loading ORION Collector Dashboard...</div></div>
  <script type="text/babel" data-type="module" src="/app.jsx"></script>
  <script type="text/babel">
    const checkAndRender = () => {
      if (typeof CollectorDashboard === 'function') {
        ReactDOM.createRoot(document.getElementById('root')).render(
          React.createElement(CollectorDashboard)
        );
      } else { setTimeout(checkAndRender, 100); }
    };
    setTimeout(checkAndRender, 500);
  </script>
</body>
</html>"""


# ══════════════════════════════════════════════════════════════════
# HTTP SERVER — serves HTML + JSX + API endpoints
# ══════════════════════════════════════════════════════════════════
class CollectorDashboardHandler(SimpleHTTPRequestHandler):
    """Serves the collector dashboard and its API endpoints."""

    transformed_jsx = ""  # Set by main() before server starts

    def do_GET(self):
        # ── HTML page ──
        if self.path == "/" or self.path == "/index.html":
            content = HTML_TEMPLATE.encode("utf-8")
            self._respond(200, "text/html; charset=utf-8", content)

        # ── Transformed JSX ──
        elif self.path == "/app.jsx":
            content = CollectorDashboardHandler.transformed_jsx.encode("utf-8")
            self._respond(200, "text/plain; charset=utf-8", content)

        # ── API: Prometheus metrics proxy ──
        # v8.6: ALWAYS serve from cache populated by the history poller.
        # The poller is the sole producer (fetches from Prometheus every 2s).
        # This handler never makes its own HTTP request — returns instantly.
        elif self.path.rstrip("/") == "/api/collector-metrics":
            with _metrics_cache_lock:
                cached = dict(_metrics_cache) if _metrics_cache else None
            if cached:
                self._json_response(cached)
            else:
                # Cache empty (first few seconds of startup) — return placeholder
                self._json_response({"ok": False, "error": "warming up", "ts": time.time()})

        # ── API: Tape health ──
        elif self.path.rstrip("/") == "/api/health":
            self._json_response(get_all_health())

        # ── API: Archive file listings ──
        elif self.path.rstrip("/") == "/api/archives":
            self._json_response(scan_archives())

        # ── API: Collector log tail ──
        elif self.path.rstrip("/") == "/api/logs":
            self._json_response(tail_log(100))

        # ── API: Collector config ──
        elif self.path.rstrip("/") == "/api/config":
            self._json_response(read_config())

        # ── API: Error rate (errors/warnings per minute) ──
        elif self.path.rstrip("/") == "/api/error-rate":
            self._json_response(get_error_rate())

        # ── API: Error details for a specific minute (v8.11: click-to-inspect) ──
        elif self.path.split("?")[0].rstrip("/") == "/api/error-details":
            import urllib.parse
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            minute_key = qs.get("minute", [""])[0]
            if not minute_key:
                self._json_response({"ok": False, "error": "minute parameter required (HH:MM)"})
            else:
                self._json_response(get_error_details(minute_key))

        # ── API: Process stats (collector CPU/memory) ──
        elif self.path.rstrip("/") == "/api/process-stats":
            self._json_response(get_process_stats())

        # ── API: Alert history (v8.1) ──
        elif self.path.rstrip("/") == "/api/alert-history":
            self._json_response(get_alert_history())

        # ── API: Feed rates per product (v8.1) ──
        elif self.path.rstrip("/") == "/api/feed-rates":
            self._json_response(get_feed_rates())

        # ── API: Server-side history (v8.2) ──
        elif self.path.split("?")[0].rstrip("/") == "/api/history":
            # Parse ?minutes=N query parameter (default 60, max 10080)
            import urllib.parse
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            minutes = int(qs.get("minutes", ["60"])[0])
            self._json_response(get_history(minutes))

        # ── API: Freshness history (v8.2) ──
        elif self.path.split("?")[0].rstrip("/") == "/api/freshness-history":
            import urllib.parse
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            minutes = int(qs.get("minutes", ["60"])[0])
            self._json_response(get_freshness_history(minutes))

        # ── API: Tape event inspector (v8.2) ──
        elif self.path.split("?")[0].rstrip("/") == "/api/tape/events":
            import urllib.parse
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            ts_val = float(qs.get("ts", ["0"])[0])
            window_val = min(float(qs.get("window", ["10"])[0]), 600)  # v8.11: cap at 10min
            max_ev = int(qs.get("max_events", ["500"])[0])
            max_ev = min(max_ev, 2000)  # v8.11: allow up to 2000 for larger windows
            if ts_val <= 0:
                self._json_response({"ok": False, "error": "ts parameter required"})
            else:
                self._json_response(get_tape_events(ts_val, window_val, max_events=max_ev))

        # ── API: Alert system status (v8.3) ──
        elif self.path.rstrip("/") == "/api/alert-status":
            self._json_response(get_alert_status())

        # ── API: Send test alert email (v8.3) ──
        elif self.path.rstrip("/") == "/api/alert-test":
            self._json_response(send_test_email())

        # ── API: SLA report (v8.5) ──
        elif self.path.rstrip("/") == "/api/sla":
            self._json_response(get_sla_report())

        # ── API: Anomaly detection report (v8.5) ──
        elif self.path.rstrip("/") == "/api/anomalies":
            self._json_response(get_anomaly_report())

        else:
            self.send_response(404)
            self.end_headers()

    def do_HEAD(self):
        """Handle HEAD requests — send headers only, no body."""
        if self.path == "/" or self.path == "/index.html":
            content = HTML_TEMPLATE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
        elif self.path == "/app.jsx":
            content = CollectorDashboardHandler.transformed_jsx.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
        else:
            self.send_response(200)
            self.end_headers()

    def _respond(self, code: int, content_type: str, content: bytes):
        """Send an HTTP response with no-cache headers."""
        try:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            self.end_headers()
            self.wfile.write(content)
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            pass  # Client closed connection early — safe to ignore

    def _json_response(self, data: dict):
        """Send a JSON API response."""
        try:
            payload = json.dumps(data, separators=(",", ":")).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-cache, no-store")
            self.end_headers()
            self.wfile.write(payload)
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            pass  # Client closed connection early — safe to ignore

    def log_message(self, format, *args):
        """Log all requests for debugging."""
        super().log_message(format, *args)


# ══════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════
def main():
    # ── Singleton check: kill any existing dashboard, then take over ──
    if not acquire_singleton_lock("collector_dashboard"):
        # Another dashboard is running — kill it and take over
        pid_file = get_pid_file_path("collector_dashboard")
        old_pid = None
        try:
            content = pid_file.read_text().strip()
            old_pid = int(content.split(",")[0])
        except (ValueError, OSError):
            pass

        if old_pid:
            print(f"\n  Existing dashboard detected (PID {old_pid}) — shutting it down...")
            try:
                if sys.platform == "win32":
                    # taskkill /F force-kills the process tree on Windows
                    subprocess.run(
                        ["taskkill", "/F", "/PID", str(old_pid)],
                        capture_output=True, timeout=10
                    )
                else:
                    os.kill(old_pid, 15)   # SIGTERM on Unix/Mac
            except Exception as e:
                print(f"  Warning: Could not kill PID {old_pid}: {e}")

            # Wait for the old process to fully exit
            time.sleep(1.5)

        # Remove stale PID file so we can re-acquire
        try:
            if pid_file.exists():
                pid_file.unlink()
        except OSError:
            pass

        # Try to acquire lock again after killing the old instance
        if not acquire_singleton_lock("collector_dashboard"):
            print(
                "\n  ERROR: Could not acquire dashboard lock after killing old instance.\n"
                "  Try again in a few seconds, or manually delete:\n"
                f"    {pid_file}\n"
            )
            sys.exit(2)

        print("  Previous dashboard terminated. Starting new instance.\n")

    parser = argparse.ArgumentParser(description="Orion Collector Dashboard")
    parser.add_argument("--port", type=int, default=None,
                        help="Port to serve on (default: auto-find 3001+)")
    parser.add_argument("--no-open", action="store_true",
                        help="Don't auto-open browser")
    parser.add_argument("--jsx", type=str, default=None,
                        help="Path to dashboard JSX file")
    args = parser.parse_args()

    # Find JSX file
    jsx_path = Path(args.jsx) if args.jsx else SCRIPT_DIR / JSX_FILENAME
    if not jsx_path.exists():
        alt = Path.cwd() / JSX_FILENAME
        if alt.exists():
            jsx_path = alt
        else:
            print(f"\n  ERROR: Cannot find {JSX_FILENAME}")
            print(f"  Looked in:")
            print(f"    {SCRIPT_DIR / JSX_FILENAME}")
            print(f"    {Path.cwd() / JSX_FILENAME}")
            print(f"\n  Put {JSX_FILENAME} in the same folder as this script.\n")
            sys.exit(1)

    # Find port
    port = args.port or find_free_port(3001)

    # Transform JSX
    print()
    print("=" * 60)
    print("  ORION Collector Dashboard")
    print("=" * 60)
    print(f"  JSX source:  {jsx_path}")
    print(f"  Port:        {port}")

    raw_jsx = jsx_path.read_text(encoding="utf-8")
    transformed = transform_jsx(raw_jsx)

    # Append auto-mount code
    transformed += """

// === AUTO-MOUNT (added by collector_dashboard.py) ===
window.CollectorDashboard = CollectorDashboard;
ReactDOM.createRoot(document.getElementById('root')).render(
  React.createElement(CollectorDashboard)
);
"""

    CollectorDashboardHandler.transformed_jsx = transformed
    line_count = len(transformed.split("\n"))
    print(f"  Transformed: {line_count} lines ({len(transformed) // 1024}KB)")

    # Data sources
    print(f"  Prometheus:  {PROM_URL}")
    print(f"  Tapes:       {DATA_DIR}")
    print(f"  Log:         {LOG_FILE}")
    print(f"  Config:      {CONFIG_FILE}")

    # v8.2: Start history poller background thread
    hist_thread = threading.Thread(target=_history_poller, daemon=True,
                                    name="history-poller")
    hist_thread.start()
    print(f"  History:     {HISTORY_FILE} (2s poll, 7-day retention, pre-load on startup)")

    # v8.3: Start alert monitor background thread
    has_gmail_pw = bool(_load_env_value("GMAIL_APP_PASSWORD"))
    alerts_enabled = _alert_cfg("enabled", default=True)
    if alerts_enabled and has_gmail_pw:
        alert_thread = threading.Thread(target=_alert_monitor, daemon=True,
                                        name="alert-monitor")
        alert_thread.start()
        recipient = _alert_cfg("recipient", default="your-email@example.com")
        print(f"  Alerts:      ACTIVE -> {recipient}")
    elif alerts_enabled and not has_gmail_pw:
        print(f"  Alerts:      DISABLED (no GMAIL_APP_PASSWORD in api.env)")
    else:
        print(f"  Alerts:      DISABLED (alerts.enabled = false in config)")

    # Start server
    server = ThreadingHTTPServer(("0.0.0.0", port), CollectorDashboardHandler)
    url = f"http://localhost:{port}"

    print(f"  Dashboard:   {url}")
    print("=" * 60)
    print()
    print(f"  Collector Dashboard running at {url}")
    print(f"  Press Ctrl+C to stop")
    print()
    sys.stdout.flush()  # Flush so preview tools detect the port

    # Auto-open browser
    if not args.no_open:
        def open_browser():
            time.sleep(0.5)
            webbrowser.open(url)
        threading.Thread(target=open_browser, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Dashboard stopped.")
        server.shutdown()
    finally:
        release_singleton_lock("collector_dashboard")


if __name__ == "__main__":
    main()
