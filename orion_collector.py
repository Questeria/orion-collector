#!/usr/bin/env python3
"""
Orion — orion_collector.py v7.8 (INSTITUTIONAL HARDENING)
=========================================================================
Unified market data collector: single process, single clock, single tape.

v7.5 INSTITUTIONAL HARDENING (continued):
  1. Race condition elimination — _pending_subscribe changed from bare list
     to asyncio.Queue(maxsize=2000). _subscribed_set protected by asyncio.Lock.
  2. Short-write detection — all os.write() calls check return value and log
     WARNING on short writes (disk full / silent corruption detection).
  3. Exchange sequence gap detection — parses Coinbase "sequence" and Kalshi
     "seq" fields. Logs WARNING on missed messages with gap size.
  4. Per-message latency tracking — parses Coinbase "time" field, computes
     local_ts - exchange_ts. Rolling 500-sample buffer for p50/p95/p99.
  5. Archive retention policy — auto-deletes archives older than N days
     (default 90). Runs every 6 hours from periodic_loop.
  6. External config file — collector_config.yaml with all tunable constants.
     _cfg() helper loads from YAML (JSON fallback). Works without config file.
  7. Bounded compression threads — ThreadPoolExecutor(max_workers=2) replaces
     unbounded threading.Thread(daemon=True). Proper shutdown in close().

v7.4 INSTITUTIONAL HARDENING:
  1. CRC32 checksums — every unified tape record gets a "crc" field computed
     from the raw payload. Downstream readers can verify data integrity.
  2. Message-level dedup — CRC32 hash of each incoming WS message tracked
     in a rotating 5000-entry set. Exact duplicates from double-subscription
     or reconnect overlap are silently dropped.
  3. Gzip archive compression — rotated archives compressed in background
     threads. JSONL compresses ~10:1 (200MB → ~20MB). Original deleted
     after successful compression.
  4. Race condition fix — _snapshot_retry_tickers changed from bare list
     to asyncio.Queue(maxsize=1000). Eliminates data race between
     fetch_rest_snapshots_async() and periodic_loop().
  5. Exponential backoff — Coinbase WS reconnect now uses 2s → 4s → ...
     → 60s max instead of fixed 2s. Matches Kalshi's v7.3.1 behavior.
  6. Health check v3 — unified tape support, continuous alerting with
     terminal bell, CRC32 verification mode, compressed archive awareness.

v7.3 OVER v7.2:
  1. Batched async snapshots — aiohttp fires 10 requests concurrently per batch
     with 0.5s pause between batches (~18 req/s). Retries 429s with exponential
     backoff. 508 tickers in ~28s (was ~50s sequential). Falls back to threaded
     sync if aiohttp is unavailable.
  2. Rediscovery moved to periodic_loop — REST discovery calls no longer
     pause the WS recv loop. periodic_loop discovers new tickers and queues
     them; recv loop just sends WS subscribe commands (instant, no blocking).
     The recv loop is now 100% clean: only WS recv + tape write + predictor.

v7.2 OVER v7.1:
  1. Post-settlement REST scheduling — ALL REST work (snapshots + rediscovery)
     now fires in a window 15-150s AFTER each 15M settlement (:00/:15/:30/:45).
     This eliminates 34-59s recv-loop pauses during the critical trading period
     (last 5-10 min before settlement). Previously ran every 30s/120s randomly.

v7.1 OVER v7:
  1. CRITICAL: Disabled client-side pings for Kalshi WS — Kalshi does NOT
     respond to client pings, causing our library to kill every connection
     with code 1011 after 120s. Now we rely on Kalshi's server-side pings
     (they ping us every ~10s) and our RECV_TIMEOUT as keepalive.
  2. Predictive 15M subscriber — pre-subscribes to upcoming 15M contracts
     using deterministic ticker name generation, retrying every 10s until
     Kalshi creates them. Eliminates 5-8 minute blind spots.
  3. WebSocket close code logging — logs code + reason + session lifetime
     for every disconnect to diagnose future issues.

v5 LATENCY OPTIMIZATIONS (retained):
  1. Pre-loaded private key — eliminates 30ms PEM parse per REST call
  2. Eliminated os.fsync() from hot path — was blocking 5-50ms x 3 FDs
  3. Non-blocking REST snapshots — run in thread pool, never stall event loop
  4. Removed JSON parsing from Coinbase hot path — zero work after tape write
  5. Write batching — combine unified + legacy into single buffer per event
  6. Pre-encoded byte constants — no str.encode() per message
  7. Counter-only flush gating — removed time.monotonic() from every write

TAPE FORMAT (unified):
  {"seq":1,"ts_us":1707840000123,"src":"cb","raw":"...coinbase msg..."}
  {"seq":2,"ts_us":1707840000125,"src":"kl","raw":"...kalshi msg..."}
  {"seq":3,"ts_us":1707840030000,"src":"snap","raw":"...snapshot..."}

  seq:   Global monotonic counter. Definitive event ordering.
  ts_us: Wall-clock microseconds (epoch). For human reference / time filtering.
  src:   "cb" = Coinbase, "kl" = Kalshi, "snap" = REST orderbook snapshot
  raw:   Original message (escaped JSON string), same as legacy format.

LEGACY FORMAT (for Stage10 backward compatibility):
  {"ts_us":1707840000123,"raw":"..."}  (written to oracle_tape.jsonl / kalshi_tape.jsonl)

DATA FLOW:
  Coinbase WS ──┐
                 ├──> Event Loop ──> Sequencer ──> Unified Tape + Legacy Tapes
  Kalshi WS ────┘                                  │
  REST snapshots ── (post-settlement window) ──────>┘

USAGE:
  python orion_collector.py                        # default: crypto only
  python orion_collector.py --symbols BTC,ETH,SOL  # specific
  python orion_collector.py --all-markets           # all Kalshi markets

REQUIREMENTS:
  pip install websockets cryptography requests aiohttp
  pip install orjson   # OPTIONAL but strongly recommended (5x faster)
"""
from __future__ import annotations

import asyncio
import base64
import collections               # deque for O(1) latency sample rotation
import gzip                      # v7.4: Compress rotated archives
import logging
import logging.handlers          # RotatingFileHandler for log rotation
import os
import shutil                    # disk_usage() for free-space warnings
import signal
import socket                    # v9.2: TCP keepalive for WS connections
import struct                    # v9.2: Pack keepalive params (Windows)
import sys
import time
import zlib                      # v7.4: CRC32 checksums for tape integrity
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── Fast JSON ──
try:
    import orjson
    def _json_dumps(obj) -> bytes:
        return orjson.dumps(obj)
    def _json_dumps_str(obj) -> str:
        return orjson.dumps(obj).decode("utf-8")
    def _json_loads(s):
        return orjson.loads(s)
    _JSON_ENGINE = "orjson"
except ImportError:
    import json as _json
    def _json_dumps(obj) -> bytes:
        return _json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    def _json_dumps_str(obj) -> str:
        return _json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    def _json_loads(s):
        return _json.loads(s)
    _JSON_ENGINE = "json (stdlib — install orjson for 5x speedup)"

try:
    import websockets
    import websockets.legacy.client as ws_client
    from websockets.exceptions import ConnectionClosed
except ImportError:
    print("FATAL: pip install websockets", flush=True)
    sys.exit(1)

try:
    import aiohttp
    _HAS_AIOHTTP = True
except ImportError:
    _HAS_AIOHTTP = False

# v7.7: Prometheus metrics endpoint (optional — collector works without it)
try:
    from prometheus_client import Counter as _PromCounter, Gauge as _PromGauge
    from prometheus_client import start_http_server as _prom_start_server
    _HAS_PROMETHEUS = True
except ImportError:
    _HAS_PROMETHEUS = False

# v7.7: Parquet archive format (optional — falls back to gzip)
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _HAS_PYARROW = True
except ImportError:
    _HAS_PYARROW = False

try:
    import requests
except ImportError:
    print("FATAL: pip install requests", flush=True)
    sys.exit(1)

# ── Cryptography imports at top level (avoid per-call import) ──
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


# ==============================================================================
# CONFIG — v7.5: loaded from collector_config.yaml if present, else defaults
# ==============================================================================

# v7.5: Load external config file (optional — defaults work without it)
_COLLECTOR_CONFIG: Dict = {}
_CONFIG_PATH = Path(__file__).parent / "collector_config.yaml"
try:
    import yaml as _yaml
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, "r") as _f:
            _COLLECTOR_CONFIG = _yaml.safe_load(_f) or {}
except ImportError:
    # PyYAML not installed — try JSON fallback
    _json_config = Path(__file__).parent / "collector_config.json"
    if _json_config.exists():
        import json as _jcfg
        with open(_json_config, "r") as _f:
            _COLLECTOR_CONFIG = _jcfg.load(_f) or {}
except Exception:
    pass  # Config load failed — use defaults

def _cfg(section: str, key: str, default):
    """Read a config value from collector_config.yaml, with fallback to default."""
    return _COLLECTOR_CONFIG.get(section, {}).get(key, default)

DATA_DIR    = Path(os.getenv("ORION_DATA_DIR", "./data"))

# Unified tape (new)
UNIFIED_DIR     = DATA_DIR / "unified" / "raw_tape"
UNIFIED_ARCHIVE = UNIFIED_DIR / "archive"
UNIFIED_TAPE    = UNIFIED_DIR / "unified_tape.jsonl"

# Legacy tapes (backward compat with Stage10)
ORACLE_DIR      = DATA_DIR / "oracle" / "raw_tape"
ORACLE_ARCHIVE  = ORACLE_DIR / "archive"
ORACLE_TAPE     = ORACLE_DIR / "oracle_tape.jsonl"
KALSHI_DIR      = DATA_DIR / "kalshi" / "raw_tape"
KALSHI_ARCHIVE  = KALSHI_DIR / "archive"
KALSHI_TAPE     = KALSHI_DIR / "kalshi_tape.jsonl"

# Rotation policy — v7.5: configurable via collector_config.yaml
ROTATE_EVERY_HOUR  = _cfg("rotation", "every_hour", True)
ROTATE_AT_MB       = _cfg("rotation", "at_mb", 200)
ROTATE_AT_BYTES    = ROTATE_AT_MB * 1024 * 1024

# Flush policy (v5: relaxed — fsync only on rotation, not hot path)
FLUSH_EVERY_LINES   = _cfg("flush", "every_lines", 500)
FLUSH_EVERY_SECONDS = _cfg("flush", "every_seconds", 1.0)

# WebSocket settings
# v7.1 CRITICAL FIX: Kalshi does NOT respond to client pings.
# Our library was killing connections with code 1011 every 120-180s.
# Solution: Disable our pings (None), rely on Kalshi's pings (they ping us
# every ~10s and websockets library auto-responds with pongs).
# KALSHI_RECEIVE_TIMEOUT is our actual keepalive — if no data for 120s,
# the connection is truly dead and we reconnect.
# v7.1 CRITICAL FIX: Kalshi does NOT respond to client pings.
# Our library was killing connections with code 1011 every 120-180s.
# Solution: Disable our pings for Kalshi, rely on Kalshi's pings (they ping us
# every ~10s and websockets library auto-responds with pongs).
# KALSHI_RECEIVE_TIMEOUT is our actual keepalive — if no data for 120s,
# the connection is truly dead and we reconnect.
KL_PING_INTERVAL        = None   # DISABLED — Kalshi ignores client pings
KL_PING_TIMEOUT         = None   # DISABLED — no pings = no timeout needed
# Coinbase DOES respond to pings (only 8 drops in months of logging)
CB_PING_INTERVAL        = 20     # Standard keepalive for Coinbase
CB_PING_TIMEOUT         = 10     # Standard timeout
KALSHI_SUBSCRIBE_CHUNK  = 100
KALSHI_RECEIVE_TIMEOUT  = _cfg("websocket", "kalshi_receive_timeout", 30)

# v7.8: WebSocket compression — "deflate" for RFC 7692 permessage-deflate
# Saves ~60-70% bandwidth. Server can accept or decline; auto-fallback if declined.
_ws_comp_raw = _cfg("websocket", "compression", "deflate")
WS_COMPRESSION = "deflate" if _ws_comp_raw == "deflate" else None

# REST snapshot scheduling — aligned with 15M settlement windows
# Settlements at :00, :15, :30, :45 UTC. The first 2 min after settlement
# is the LEAST critical time (new contract just opened, prices ~50¢).
# Pack ALL REST work into this window → zero interference during trading.
SNAPSHOT_RATE_LIMIT_MS  = 100   # ms between REST requests (sync fallback)
SNAPSHOT_CONCURRENCY    = _cfg("snapshots", "concurrency", 10)
SNAPSHOT_WINDOW_START_S = _cfg("snapshots", "window_start_s", 30)
SNAPSHOT_WINDOW_END_S   = _cfg("snapshots", "window_end_s", 150)

# Market re-discovery — also in post-settlement window
# Runs BEFORE snapshots to catch new contracts, then snapshots pick them up.
REDISCOVERY_OFFSET_S    = _cfg("rediscovery", "offset_s", 15)

# Predictive 15M subscription (ported from kalshi_tape.py v4)
PREDICT_LEAD_S          = _cfg("predictor", "lead_s", 45)
PREDICT_CHECK_S         = _cfg("predictor", "check_s", 10)
PREDICT_SYMBOLS         = _cfg("predictor", "symbols", ["BTC", "ETH", "SOL", "XRP"])
# DST-aware Eastern timezone — automatically handles EST (UTC-5) and EDT (UTC-4)
NY_TZ                   = ZoneInfo("America/New_York")

# v7.5: Archive retention policy — auto-delete archives older than N days
# Prevents unbounded disk growth over months of operation.
ARCHIVE_RETENTION_DAYS  = _cfg("retention", "days", 90)
ARCHIVE_CLEANUP_HOURS   = _cfg("retention", "cleanup_hours", 6)

# v7.7: Configurable gap detection thresholds
# Coinbase sequence is orderbook-wide — normal interleaving jumps ~1000.
# Kalshi sequence is per-channel — any gap indicates dropped messages.
CB_GAP_THRESHOLD        = _cfg("gaps", "cb_threshold", 5000)
KL_GAP_THRESHOLD        = _cfg("gaps", "kl_threshold", 1)


# ── v9.2: TCP keepalive helper ───────────────────────────────────────
# Prevents NAT/firewall idle-timeout disconnections (code=1006) by
# sending OS-level keepalive probes every 10s.  Works below TLS so the
# server never sees the probes (unlike WebSocket pings, which Kalshi
# rejects).  Entire function is a single try/except — failure here
# does NOT affect the connection; it just falls back to existing behavior.
def _enable_tcp_keepalive(ws, logger: logging.Logger, label: str = "WS") -> None:
    """Enable TCP keepalive on a websocket's underlying socket."""
    try:
        transport = getattr(ws, 'transport', None)
        if transport is None:
            return
        tsock = transport.get_extra_info('socket')
        if tsock is None:
            return

        # websockets v16 returns asyncio.TransportSocket (a wrapper)
        # that lacks ioctl().  Unwrap to the real socket.socket via _sock.
        sock = getattr(tsock, '_sock', tsock)

        # Enable SO_KEEPALIVE
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        if sys.platform == 'win32':
            # Windows: SIO_KEEPALIVE_VALS ioctl
            # (onoff, keepalive_time_ms, keepalive_interval_ms)
            # onoff=1, time=10000ms (first probe after 10s idle),
            # interval=5000ms (retry every 5s if no response)
            # Python 3.13+: ioctl wants a tuple, not struct.pack bytes
            sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 10000, 5000))
        else:
            # Linux/macOS: set individual keepalive parameters
            if hasattr(socket, 'TCP_KEEPIDLE'):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 10)
            if hasattr(socket, 'TCP_KEEPINTVL'):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
            if hasattr(socket, 'TCP_KEEPCNT'):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)

        logger.info(f"[{label}] TCP keepalive enabled (10s idle, 5s interval)")
    except Exception as e:
        # Non-fatal: connection still works exactly as before
        logger.warning(f"[{label}] TCP keepalive setup failed: {e}")


# v7.7: Prometheus metric objects (only if prometheus_client is installed)
if _HAS_PROMETHEUS:
    PROM_EVENTS = _PromCounter(
        "orion_collector_events_total",
        "Total events written to tape", ["src"]
    )
    PROM_RATE = _PromGauge(
        "orion_collector_event_rate", "Events per second (30s window)"
    )
    PROM_LATENCY = _PromGauge(
        "orion_collector_latency_ms", "Feed latency in ms", ["percentile"]
    )
    PROM_QUEUE = _PromGauge(
        "orion_collector_queue_depth", "Backpressure queue depth"
    )
    PROM_DROPPED = _PromCounter(
        "orion_collector_dropped_total", "Messages dropped (backpressure)"
    )
    PROM_GAPS = _PromCounter(
        "orion_collector_gaps_total", "Sequence gaps detected", ["exchange"]
    )
    PROM_DISK = _PromGauge(
        "orion_collector_disk_free_gb", "Free disk space in GB"
    )
    PROM_TAPE_MB = _PromGauge(
        "orion_collector_tape_size_mb", "Current tape file size in MB"
    )
    PROM_SEQ = _PromGauge(
        "orion_collector_seq", "Current global sequence number"
    )
    PROM_UPTIME = _PromGauge(
        "orion_collector_uptime_seconds", "Collector uptime in seconds"
    )
    # v8.1: Institutional monitoring — dedup, reconnect, connection uptime, latency histogram
    PROM_DEDUP = _PromCounter(
        "orion_collector_dedup_total", "Duplicate messages filtered"
    )
    PROM_RECONNECTS = _PromCounter(
        "orion_collector_reconnects_total",
        "WebSocket reconnect attempts", ["exchange"]
    )
    PROM_CONN_UPTIME = _PromGauge(
        "orion_collector_connection_uptime_seconds",
        "Seconds since last reconnect per exchange", ["exchange"]
    )
    PROM_LAT_HIST = _PromGauge(
        "orion_collector_latency_histogram_ms",
        "Latency distribution bucket count", ["bucket"]
    )
    # v8.2: Network-level metrics — bandwidth, message sizes, WebSocket RTT
    PROM_BYTES_SEC = _PromGauge(
        "orion_collector_bytes_per_sec",
        "Tape write throughput in bytes per second"
    )
    PROM_MSG_SIZE = _PromGauge(
        "orion_collector_msg_size_avg",
        "Average message size in bytes per exchange", ["exchange"]
    )
    PROM_WS_RTT = _PromGauge(
        "orion_collector_ws_rtt_ms",
        "WebSocket ping round-trip time in ms", ["exchange"]
    )
    PROM_BYTES_TOTAL = _PromCounter(
        "orion_collector_bytes_total",
        "Total bytes written to all tape files"
    )
    # v8.2 fix: Per-exchange event rate gauges — smooth chart rendering.
    # Unlike Counter deltas (which only change every 30s, causing sawtooth
    # artifacts when the dashboard polls at 2s), these Gauges hold the
    # current events/sec and can be read any time without jitter.
    PROM_EXCHANGE_RATE = _PromGauge(
        "orion_collector_exchange_rate",
        "Events per second per exchange (30s window)", ["exchange"]
    )

# API
ENV_FILE = Path(os.getenv("ORION_ENV_FILE", "./api.env"))

# Symbols
DEFAULT_SYMBOLS = _COLLECTOR_CONFIG.get("default_symbols", ["BTC", "ETH", "SOL", "XRP", "DOGE", "SHIB"])
COINBASE_PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "SHIB-USD"]
COINBASE_WS_URI = "wss://ws-feed.exchange.coinbase.com"
COINBASE_CHANNELS = ["ticker"]
KALSHI_CHANNELS = ["orderbook_delta", "ticker", "trade"]

# Logging
LOG_DIR  = DATA_DIR.parent / "logs"
LOG_FILE = LOG_DIR / "orion_collector.log"

# Singleton lock — prevents duplicate collector instances.
# Shared module in collectors/singleton_lock.py handles PID file management.
from singleton_lock import acquire_singleton_lock, release_singleton_lock, get_pid_file_path


# ==============================================================================
# ENV + KALSHI AUTH
# ==============================================================================
def _load_env(env_path: Path) -> Dict[str, str]:
    env = {}
    if not env_path.exists():
        return env
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        env[k] = v
        if os.getenv(k) is None:
            os.environ[k] = v
    return env


def _env_first(*keys: str) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v is not None and v.strip():
            return v.strip()
    return None


# ── v5 FIX #1: Pre-loaded private key ──
# v4 called load_pem_private_key() on EVERY REST request (30ms each).
# Now we load it ONCE and reuse the parsed key object.
_CACHED_PRIVATE_KEY = None   # Set by _init_auth()
_CACHED_KEY_ID = None        # Set by _init_auth()


def _init_auth() -> None:
    """Load and cache Kalshi credentials. Called once at startup."""
    global _CACHED_PRIVATE_KEY, _CACHED_KEY_ID

    _CACHED_KEY_ID = _env_first(
        "KALSHI_ACCESS_KEY", "KALSHI_KEY_ID",
        "KALSHI_API_KEY", "KALSHI_API_KEY_ID"
    )
    if not _CACHED_KEY_ID:
        raise RuntimeError("Missing Kalshi access key. Set KALSHI_ACCESS_KEY.")

    pk_path = _env_first(
        "KALSHI_PRIVATE_KEY_PATH", "KALSHI_KEY_PATH", "KALSHI_KEY_FILE"
    )
    pem_text = _env_first("KALSHI_PRIVATE_KEY_PEM", "KALSHI_PRIVATE_KEY")

    if pem_text and "BEGIN" in pem_text:
        pem_bytes = pem_text.encode("utf-8")
    elif pk_path:
        cand = Path(os.path.expandvars(os.path.expanduser(pk_path)))
        if cand.exists():
            pem_bytes = cand.read_bytes()
        else:
            raise RuntimeError(f"Key file not found: {cand}")
    else:
        raise RuntimeError("Missing Kalshi private key. Set KALSHI_PRIVATE_KEY_PATH.")

    # Parse PEM ONCE — this is the 30ms operation we're eliminating per-call
    _CACHED_PRIVATE_KEY = serialization.load_pem_private_key(pem_bytes, password=None)


def _sign_pss_sha256(text: str) -> str:
    """Sign text with pre-loaded private key. ~0.5ms instead of ~30ms."""
    sig = _CACHED_PRIVATE_KEY.sign(
        text.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("utf-8")


def _kalshi_auth_headers(method: str, path: str) -> Dict[str, str]:
    """Build auth headers using cached key. No PEM parsing."""
    ts = str(int(time.time() * 1000))
    sig = _sign_pss_sha256(ts + method + path.split("?")[0])
    return {
        "KALSHI-ACCESS-KEY": _CACHED_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": sig,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Content-Type": "application/json",
    }


def _kalshi_ws_headers(path: str = "/trade-api/ws/v2") -> Dict[str, str]:
    return _kalshi_auth_headers("GET", path)


def _get_kalshi_rest_base() -> str:
    return (_env_first("KALSHI_REST_BASE") or
            "https://api.elections.kalshi.com").rstrip("/")


def _get_kalshi_ws_url() -> str:
    return (_env_first("KALSHI_WS_URL") or
            "wss://api.elections.kalshi.com/trade-api/ws/v2").strip()


# ==============================================================================
# MARKET DISCOVERY
# ==============================================================================
def discover_crypto_markets(symbols: List[str],
                            logger: logging.Logger) -> List[str]:
    """
    Discover ALL crypto markets for the given symbols — every timeframe.

    Strategy:
      1. Try known series patterns per symbol (15M, 1H, D, W, M, Y, etc.)
         — fast targeted queries that find most markets.
      2. ALWAYS do a broad prefix scan as safety net
         — catches any series we didn't know about (new timeframes, specials).
      3. Deduplicate and return sorted list.

    This ensures we collect data for hourly, daily, weekly, monthly, and annual
    markets in addition to the 15-minute contracts we actively trade.
    """
    base = _get_kalshi_rest_base()
    tickers: List[str] = []

    # ── Phase 1: Targeted series queries (fast) ──
    # Known Kalshi series naming patterns for crypto.
    # We try all known timeframe suffixes for each symbol.
    SERIES_SUFFIXES = [
        "15M",       # 15-minute (what we actively trade)
        "1H",        # Hourly
        "D",         # Daily
        "24H",       # Daily (alternate naming)
        "W",         # Weekly
        "M",         # Monthly
        "Y",         # Annual
        "MAX150",    # Max price (special markets)
        "MIN",       # Min price (special markets)
    ]

    series_found = 0
    for sym in symbols:
        for suffix in SERIES_SUFFIXES:
            series = f"KX{sym}{suffix}"
            # Retry up to 3 times with exponential backoff on transient failures
            for attempt in range(3):
                try:
                    path = f"/trade-api/v2/markets?series_ticker={series}&status=open"
                    resp = requests.get(base + path,
                                        headers=_kalshi_auth_headers("GET", path),
                                        timeout=10)
                    if resp.status_code == 200:
                        markets = resp.json().get("markets", [])
                        if markets:
                            for m in markets:
                                tickers.append(m["ticker"])
                            logger.info(f"  {series}: {len(markets)} active")
                            series_found += len(markets)
                        break  # Success — move to next series
                    elif resp.status_code == 429:
                        # Rate limited — back off and retry
                        if attempt < 2:
                            time.sleep(1.0 * (2 ** attempt))
                            continue
                        logger.warning(f"  {series}: rate limited after 3 attempts")
                        break
                    else:
                        # Other HTTP error (4xx, 5xx) — don't retry
                        break
                except Exception as e:
                    if attempt < 2:
                        time.sleep(1.0 * (2 ** attempt))
                        continue
                    logger.warning(f"  {series}: failed after 3 attempts ({e})")

    logger.info(f"  Series queries found {series_found} markets")

    # ── Phase 2: Broad prefix scan (catches everything we missed) ──
    # Always run this — it catches new series types, special markets, etc.
    logger.info("  Running broad prefix scan for additional markets...")
    prefixes = [f"KX{sym}" for sym in symbols]
    cursor, scanned, broad_added = None, 0, 0

    while scanned < 3000:
        # Retry each page up to 3 times on transient failures
        page_ok = False
        for attempt in range(3):
            try:
                path = f"/trade-api/v2/markets?status=open&limit=200"
                if cursor:
                    path += f"&cursor={cursor}"
                resp = requests.get(base + path,
                                    headers=_kalshi_auth_headers("GET", path),
                                    timeout=10)
                if resp.status_code == 429:
                    # Rate limited — back off and retry
                    if attempt < 2:
                        time.sleep(1.0 * (2 ** attempt))
                        continue
                    break  # Give up on this page
                if resp.status_code != 200:
                    break
                data = resp.json()
                markets = data.get("markets", [])
                if not markets:
                    break
                for m in markets:
                    t = m.get("ticker", "").upper()
                    for pfx in prefixes:
                        if t.startswith(pfx):
                            rest = t[len(pfx):]
                            if not rest or rest[0].isdigit() or rest[0] in ("-", "_"):
                                tickers.append(m["ticker"])
                                broad_added += 1
                                break
                            if (len(rest) >= 2 and rest[0].isalpha() and
                                    (rest[1].isdigit() or rest[1] in ("-", "_"))):
                                tickers.append(m["ticker"])
                                broad_added += 1
                                break
                scanned += len(markets)
                cursor = data.get("cursor")
                page_ok = True
                break  # Page fetched successfully
            except Exception:
                if attempt < 2:
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                break  # All retries exhausted
        if not page_ok or not cursor:
            break

    if broad_added:
        logger.info(f"  Broad scan found {broad_added} additional tickers (scanned {scanned})")

    # ── Deduplicate and sort ──
    tickers = sorted(set(tickers))
    logger.info(f"  Total: {len(tickers)} crypto markets across all timeframes")
    return tickers


def discover_all_markets(logger: logging.Logger,
                         limit: int = 2000) -> List[str]:
    base = _get_kalshi_rest_base()
    tickers: List[str] = []
    cursor = None
    while len(tickers) < limit:
        # Retry each page up to 3 times on transient failures
        page_ok = False
        for attempt in range(3):
            try:
                path = f"/trade-api/v2/markets?status=open&limit=200"
                if cursor:
                    path += f"&cursor={cursor}"
                resp = requests.get(base + path,
                                    headers=_kalshi_auth_headers("GET", path),
                                    timeout=10)
                if resp.status_code == 429:
                    if attempt < 2:
                        time.sleep(1.0 * (2 ** attempt))
                        continue
                    break
                if resp.status_code != 200:
                    break
                data = resp.json()
                markets = data.get("markets", [])
                if not markets:
                    break
                for m in markets:
                    tickers.append(m["ticker"])
                cursor = data.get("cursor")
                page_ok = True
                break  # Page fetched successfully
            except Exception:
                if attempt < 2:
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                break
        if not page_ok or not cursor:
            break

    tickers = sorted(set(tickers))
    logger.info(f"  Discovered {len(tickers)} total markets")
    return tickers


# ==============================================================================
# REST ORDERBOOK SNAPSHOT FETCHER
# ==============================================================================
def fetch_rest_snapshots(tickers: List[str],
                         logger: logging.Logger) -> List[dict]:
    """Fetch full orderbook state via REST for all active tickers.

    Returns list of synthetic orderbook_snapshot messages ready for tape writing.
    Rate-limited to ~10 req/sec to stay well under Kalshi limits.

    NOTE: In v5 this runs in a thread pool (asyncio.to_thread) so it
    never blocks the event loop or stalls WebSocket message processing.
    """
    base = _get_kalshi_rest_base()
    snapshots = []
    fetched = 0
    errors = 0

    for ticker in tickers:
        try:
            path = f"/trade-api/v2/markets/{ticker}/orderbook"
            resp = requests.get(base + path,
                                headers=_kalshi_auth_headers("GET", path),
                                timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                ob = data.get("orderbook", data)
                snap_msg = {
                    "type": "orderbook_snapshot",
                    "msg": {
                        "market_ticker": ticker,
                        "yes": ob.get("yes", []),
                        "no": ob.get("no", []),
                    },
                }
                snapshots.append(snap_msg)
                fetched += 1
            elif resp.status_code == 429:
                logger.warning(f"REST rate limited after {fetched} snapshots")
                break
            else:
                errors += 1
        except Exception:
            errors += 1

        time.sleep(SNAPSHOT_RATE_LIMIT_MS / 1000.0)

    if fetched > 0 or errors > 0:
        logger.info(f"  Snapshots: {fetched} fetched, {errors} errors")
    return snapshots


# ==============================================================================
# ASYNC CONCURRENT REST SNAPSHOTS (v7.3)
# ==============================================================================
# Concurrent REST snapshots via aiohttp with rate-limit-safe batching.
# Processes 10 tickers at a time with 0.5s pause between batches → ~18 req/s.
# Retries 429s with exponential backoff. Falls back to sync if no aiohttp.
# Result: ~30s for 508 tickers (was ~50s sequential, was ~5s-then-429 naive).

async def fetch_rest_snapshots_async(
    tickers: List[str],
    logger: logging.Logger,
    batch_size: int = SNAPSHOT_CONCURRENCY,
) -> List[dict]:
    """Fetch orderbook snapshots using batched concurrent HTTP.

    Fires `batch_size` requests concurrently, pauses 0.5s, repeats.
    Retries 429 (rate limited) responses with exponential backoff.
    Falls back to threaded sync if aiohttp is unavailable.
    """
    if not _HAS_AIOHTTP:
        return await asyncio.to_thread(fetch_rest_snapshots, tickers, logger)

    base = _get_kalshi_rest_base()
    results: List[Optional[dict]] = [None] * len(tickers)
    fetched = 0
    errors = 0
    rate_limited = 0

    async def _fetch_one(idx: int, ticker: str, session: aiohttp.ClientSession):
        nonlocal fetched, errors, rate_limited
        path = f"/trade-api/v2/markets/{ticker}/orderbook"
        headers = _kalshi_auth_headers("GET", path)
        for attempt in range(3):
            try:
                async with session.get(
                    base + path, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        ob = data.get("orderbook", data)
                        results[idx] = {
                            "type": "orderbook_snapshot",
                            "msg": {
                                "market_ticker": ticker,
                                "yes": ob.get("yes", []),
                                "no": ob.get("no", []),
                            },
                        }
                        fetched += 1
                        return
                    elif resp.status == 429:
                        rate_limited += 1
                        # Exponential backoff: 1s, 2s, 4s
                        await asyncio.sleep(1.0 * (2 ** attempt))
                        continue
                    elif resp.status >= 500:
                        # Server error (5xx) — retry with backoff
                        await asyncio.sleep(1.0 * (2 ** attempt))
                        continue
                    else:
                        # Client error (4xx except 429) — don't retry
                        errors += 1
                        return
            except Exception:
                # Network error (timeout, DNS, etc.) — retry with backoff
                if attempt < 2:
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
                errors += 1
                return

        # All retries exhausted
        errors += 1

    async with aiohttp.ClientSession() as session:
        # Process in batches to stay under Kalshi's rate limit.
        # 10 concurrent per batch + 0.5s pause ≈ 18 req/s (safe).
        for batch_start in range(0, len(tickers), batch_size):
            batch = tickers[batch_start: batch_start + batch_size]
            tasks = [
                _fetch_one(batch_start + i, t, session)
                for i, t in enumerate(batch)
            ]
            await asyncio.gather(*tasks)
            # Pace between batches to avoid 429s
            if batch_start + batch_size < len(tickers):
                await asyncio.sleep(0.5)

    snapshots = [r for r in results if r is not None]

    # Track failed tickers for retry in next snapshot cycle
    # v7.4: Push to asyncio.Queue instead of mutating a global list
    _failed = [tickers[i] for i, r in enumerate(results) if r is None]
    if _snapshot_retry_queue is not None:
        # Drain any stale entries, then push fresh failures
        while not _snapshot_retry_queue.empty():
            try:
                _snapshot_retry_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        for t in _failed:
            try:
                _snapshot_retry_queue.put_nowait(t)
            except asyncio.QueueFull:
                break  # 1000-item cap — discard extras

    if fetched > 0 or errors > 0:
        rl_note = f", {rate_limited} retried" if rate_limited else ""
        retry_note = f", {len(_failed)} queued for retry" if _failed else ""
        logger.info(
            f"  Snapshots: {fetched} fetched, {errors} errors"
            f" (async, {batch_size}/batch{rl_note}{retry_note})"
        )
    return snapshots
# ==============================================================================
# Kalshi 15M crypto tickers follow a deterministic naming pattern:
#   KX{SYM}15M-{YY}{MON}{DD}{HH}{MM}-{MM}
# Settlement times are every 15 minutes: :00, :15, :30, :45 (in EST).
# The predictor generates ticker names and subscribes BEFORE the window opens,
# retrying every 10s until Kalshi creates the contract.

_MONTH_ABBR = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
    7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC",
}


def seconds_since_last_settlement(now_utc: datetime) -> int:
    """Return how many seconds have elapsed since the last 15M settlement.
    
    Settlements happen at :00, :15, :30, :45 of each hour (UTC).
    E.g., at 03:47:20 UTC → last settlement was 03:45:00 → returns 140.
    """
    minute = now_utc.minute
    # Which quarter we're in: 0, 15, 30, 45
    quarter_start = (minute // 15) * 15
    elapsed_min = minute - quarter_start
    elapsed_sec = elapsed_min * 60 + now_utc.second
    return elapsed_sec


def predict_15m_tickers(symbols: List[str],
                        settlement_utc: datetime) -> List[str]:
    """Generate predicted Kalshi 15M ticker names for a settlement time."""
    # Convert UTC to Eastern (DST-aware) for ticker naming
    est = settlement_utc.replace(tzinfo=timezone.utc).astimezone(NY_TZ)
    yy = est.year % 100
    mon = _MONTH_ABBR[est.month]
    dd = est.day
    hh = est.hour
    mm = est.minute
    tickers = []
    for sym in symbols:
        tickers.append(f"KX{sym}15M-{yy}{mon}{dd:02d}{hh:02d}{mm:02d}-{mm:02d}")
    return tickers


def get_next_15m_settlements(now_utc: datetime, count: int = 2) -> List[datetime]:
    """Return the next `count` 15-minute settlement times in UTC."""
    minute = now_utc.minute
    next_slot = (minute // 15 + 1) * 15
    if next_slot >= 60:
        base = now_utc.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        next_slot -= 60
    else:
        base = now_utc.replace(minute=0, second=0, microsecond=0)
    first = base.replace(minute=next_slot)
    return [first + timedelta(minutes=15 * i) for i in range(count)]


class PredictiveSubscriber:
    """Pre-subscribes to upcoming 15M window tickers with automatic retry.

    Kalshi creates new 15M contracts with a variable delay (~5-10 min into
    the window). The predictor generates ticker names and keeps retrying
    every check cycle until confirmed or the retry window expires.
    """
    RETRY_WINDOW_S = 600  # Keep retrying for 10 min after window opens

    def __init__(self, symbols: List[str], logger: logging.Logger):
        self.symbols = symbols
        self.logger = logger
        self._confirmed: set = set()
        self._pending: Dict[str, datetime] = {}  # ticker → window_open_utc

    def confirm_ticker(self, ticker: str) -> None:
        """Called when we see a snapshot for a predicted ticker."""
        if ticker in self._pending:
            del self._pending[ticker]
            self._confirmed.add(ticker)

    def get_tickers_to_subscribe(self, now_utc: datetime) -> List[str]:
        """Returns tickers that should be subscribed NOW."""
        new_tickers = []

        # Phase 1: Predict tickers for upcoming windows
        settlements = get_next_15m_settlements(now_utc, count=2)
        for settle_time in settlements:
            window_open = settle_time - timedelta(minutes=15)
            time_until_open = (window_open - now_utc).total_seconds()
            if -60 < time_until_open <= PREDICT_LEAD_S:
                predicted = predict_15m_tickers(self.symbols, settle_time)
                for t in predicted:
                    if t not in self._confirmed and t not in self._pending:
                        self._pending[t] = window_open
                        new_tickers.append(t)

        # Phase 2: Retry all pending tickers
        expired = []
        for ticker, window_open in self._pending.items():
            age = (now_utc - window_open).total_seconds()
            if age > self.RETRY_WINDOW_S:
                expired.append(ticker)
            elif ticker not in new_tickers:
                new_tickers.append(ticker)
        for t in expired:
            del self._pending[t]

        # Prune confirmed cache periodically
        if len(self._confirmed) > 200:
            self._confirmed.clear()

        return new_tickers


# ==============================================================================
# UNIFIED TAPE WRITER (v5 — LOW-LATENCY)
# ==============================================================================

# ── v5 FIX #6: Pre-encoded byte constants ──
# Eliminates str.encode() calls from hot path entirely.
_B_SEQ_PREFIX     = b'{"seq":'
_B_TS_PREFIX      = b',"ts_us":'
_B_SRC_CB         = b',"src":"cb","raw":'
_B_SRC_KL         = b',"src":"kl","raw":'
_B_SRC_SNAP       = b',"src":"snap","raw":'
_B_CRC_PREFIX     = b',"crc":'    # v7.4: CRC32 checksum for integrity verification
_B_NEWLINE        = b'}\n'
_B_LEGACY_PREFIX  = b'{"ts_us":'
_B_LEGACY_RAW     = b',"raw":'

# v7.4: CRC32 helper — computes checksum of the raw payload bytes
_crc32 = zlib.crc32

# Pre-encoded source tags for unified tape (indexed for speed)
_SRC_BYTES = {
    "cb": _B_SRC_CB,
    "kl": _B_SRC_KL,
}

# Snapshot retry tracking — tickers that failed REST fetch are retried next cycle.
# v7.4: Changed from bare list to asyncio.Queue to eliminate race condition
# between fetch_rest_snapshots_async() (writer) and periodic_loop (reader).
_snapshot_retry_queue: Optional[asyncio.Queue] = None  # initialized in run_collector()


class UnifiedTapeWriter:
    """Writes events to three tape files simultaneously:
      1. Unified tape  — new format with seq/src for backtest
      2. Oracle tape   — legacy format for Stage10 CoinbaseReader
      3. Kalshi tape   — legacy format for Stage10 KalshiTapeReader

    All three share the same monotonic sequence counter and wall clock.

    v5 HOT PATH OPTIMIZATIONS:
      - No os.fsync() during normal writes (only on rotation/close)
      - Pre-encoded byte constants (zero str.encode() per message)
      - Counter-only flush gating (no time.monotonic() per message)
      - Cached time_ns reference
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._seq: int = 0  # Global monotonic sequence counter

        # File descriptors (raw os.write for performance)
        self._unified_fd: int = -1
        self._oracle_fd: int = -1
        self._kalshi_fd: int = -1

        # v7.4: Message-level deduplication — tracks CRC32 of recent messages
        # to prevent duplicate tape writes (e.g., from double-subscription).
        # Uses a rotating set: once it hits 5000, it clears and restarts.
        self._recent_msg_hashes: set = set()
        self._dedup_collisions: int = 0

        # v7.5: Exchange sequence gap detection
        # Tracks last seen sequence number per source to detect dropped messages.
        # Coinbase: "sequence" field in ticker messages (monotonic per product)
        # Kalshi: "seq" field in channel messages (monotonic per channel)
        self._cb_last_seq: Dict[str, int] = {}   # {product_id: last_sequence}
        self._kl_last_seq: int = 0                # Kalshi global seq (if available)
        self.cb_gaps: int = 0                      # Count of detected Coinbase gaps
        self.kl_gaps: int = 0                      # Count of detected Kalshi gaps

        # v7.7: Gap log dedup — suppress repeat gap warnings within 60s per product
        self._cb_last_gap_log: Dict[str, float] = {}  # {product: monotonic_time}
        self._kl_last_gap_log: float = 0.0

        # v7.5: Per-message latency tracking (exchange_ts vs local_ts)
        # Tracks running percentile stats using a simple sorted buffer.
        self._latency_samples: collections.deque = collections.deque(maxlen=500)  # Last N latency samples (ms)

        # Tracking
        self.lines_written: int = 0
        self.ws_lines_written: int = 0  # v9.0: WebSocket-only line count (excludes REST snapshots) for accurate event rate
        self.bytes_written: int = 0
        self._lines_since_flush: int = 0
        self._current_hour: int = -1
        self._active_tickers: List[str] = []

        # Stats per source
        self.cb_count: int = 0
        self.kl_count: int = 0
        self.snap_count: int = 0

        # v7.6: Per-feed health metrics — detect silent subscription drops
        # Track last message time and count per product (CB and KL).
        self._product_last_msg: Dict[str, float] = {}   # {product_id: monotonic_time}
        self._product_msg_count: Dict[str, int] = {}    # {product_id: count_this_period}
        self._feed_silent_s: float = _cfg("health", "feed_silent_threshold_s", 60.0)

        # v8.2: Per-exchange message size tracking for network metrics
        # Accumulates byte totals + counts between stats intervals (30s).
        # periodic_loop computes avg then resets to zero.
        self._cb_msg_bytes: int = 0
        self._cb_msg_count_size: int = 0
        self._kl_msg_bytes: int = 0
        self._kl_msg_count_size: int = 0

        # v7.6: Disk pause/resume for Upgrade 4
        self._disk_paused: bool = False
        self._pause_buffer: List[Tuple[str, str]] = []
        self._max_pause_buffer: int = _cfg("disk", "pause_buffer_max", 10000)

        # v5 FIX #7: Cache time_ns reference for hot path
        self._time_ns = time.time_ns

        # v7.5: Capped thread pool for archive compression (max 2 concurrent)
        from concurrent.futures import ThreadPoolExecutor
        self._compress_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="archive")

    def _recover_seq_from_tape(self) -> int:
        """Read the last sequence number from the unified tape file.

        On restart, we continue from where we left off instead of
        resetting to 0.  This prevents seq number collisions in backtest
        replays and keeps the monotonic ordering intact across restarts.

        How: reads the last 8KB of the unified tape, walks backwards
        through lines to find the last valid JSON object with a "seq" field.
        """
        try:
            if not UNIFIED_TAPE.exists() or UNIFIED_TAPE.stat().st_size == 0:
                return 0
            # Read the tail of the file (8KB is enough for several lines)
            with open(UNIFIED_TAPE, "rb") as f:
                f.seek(0, 2)  # seek to end
                size = f.tell()
                read_size = min(size, 8192)
                f.seek(size - read_size)
                tail = f.read()
            # Walk backwards through lines to find last valid seq
            for line in reversed(tail.split(b'\n')):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = _json_loads(line)
                    if "seq" in record:
                        return int(record["seq"])
                except Exception:
                    continue
            return 0
        except Exception as e:
            self.logger.warning(f"  Could not recover seq from tape: {e}")
            return 0

    def _repair_partial_lines(self) -> None:
        """Check each tape file for a partial (incomplete) JSON last line.

        If the collector crashed mid-write, the last line may be truncated.
        We detect this (file doesn't end with newline) and truncate the
        partial bytes so downstream readers don't choke on corrupt JSON.
        """
        for tape_path, label in [
            (UNIFIED_TAPE, "unified"),
            (ORACLE_TAPE,  "oracle"),
            (KALSHI_TAPE,  "kalshi"),
        ]:
            try:
                if not tape_path.exists() or tape_path.stat().st_size == 0:
                    continue
                with open(tape_path, "rb") as f:
                    f.seek(0, 2)
                    size = f.tell()
                    if size == 0:
                        continue
                    # Read last 4KB to find the last newline
                    read_size = min(size, 4096)
                    f.seek(size - read_size)
                    tail = f.read()

                # File ends with newline → all lines are complete, nothing to fix
                if tail.endswith(b'\n'):
                    continue

                # File does NOT end with newline → last line is partial (crash mid-write).
                # Find the last newline and truncate everything after it.
                last_nl = tail.rfind(b'\n')
                if last_nl == -1:
                    # No newline in the last 4KB — could be a very long partial line
                    # or a very small file. Skip for safety.
                    self.logger.warning(
                        f"  {label} tape: no newline in last {read_size}B — skipping repair"
                    )
                    continue

                truncate_to = size - read_size + last_nl + 1
                partial_bytes = size - truncate_to
                with open(tape_path, "r+b") as f:
                    f.truncate(truncate_to)
                self.logger.warning(
                    f"  {label} tape: removed {partial_bytes}B partial line (crash recovery)"
                )
            except Exception as e:
                self.logger.warning(f"  {label} tape repair failed: {e}")

    # ──────────────────────────────────────────────────────────────
    # v7.6 UPGRADE 3: Tape Integrity Audit on Startup
    # ──────────────────────────────────────────────────────────────

    def _audit_tape_integrity(self, count: int = 50) -> None:
        """Validate the last N records of the unified tape for JSON + CRC integrity.

        v7.6: Runs during startup AFTER partial-line repair. Catches subtler
        corruption (e.g., bit-flipped bytes that form valid newline-terminated
        lines but contain invalid JSON or wrong CRC).

        If corruption found at the tail, truncate to last valid record.
        """
        if not UNIFIED_TAPE.exists():
            self.logger.info("  Startup audit: unified tape missing, skipping")
            return
        tape_size = UNIFIED_TAPE.stat().st_size
        if tape_size == 0:
            self.logger.info("  Startup audit: unified tape empty, skipping")
            return

        # Read tail (each record ~200-2000 bytes)
        read_size = count * 2048
        try:
            with open(UNIFIED_TAPE, "rb") as f:
                actual_read = min(tape_size, read_size)
                f.seek(tape_size - actual_read)
                tail = f.read()
        except OSError as e:
            self.logger.warning(f"  Startup audit: read failed: {e}")
            return

        lines = tail.split(b"\n")
        valid = 0
        invalid = 0
        crc_ok = 0
        crc_fail = 0
        crc_missing = 0
        checked = 0

        # Walk from end, validate each record
        for raw_line in reversed(lines):
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            if checked >= count:
                break
            checked += 1

            try:
                record = _json_loads(raw_line)
                # Minimum required fields
                if "seq" not in record or "ts_us" not in record:
                    invalid += 1
                    continue

                # CRC32 verification (unified tape records have crc + raw)
                if "crc" in record and "raw" in record:
                    raw_payload = _json_dumps(record["raw"])
                    computed = _crc32(raw_payload) & 0xFFFFFFFF
                    if computed == record["crc"]:
                        crc_ok += 1
                    else:
                        crc_fail += 1
                        invalid += 1
                        continue
                else:
                    crc_missing += 1

                valid += 1
            except Exception:
                invalid += 1

        # Log summary
        crc_note = ""
        if crc_ok + crc_fail > 0:
            crc_note = f", CRC: {crc_ok} OK / {crc_fail} FAIL"
        self.logger.info(
            f"  Startup audit: {valid}/{checked} records valid{crc_note}"
        )

        # If corruption found at tail, truncate to last valid record
        if invalid > 0:
            truncate_to = self._find_last_valid_boundary(UNIFIED_TAPE)
            if truncate_to is not None and truncate_to < tape_size:
                with open(UNIFIED_TAPE, "r+b") as f:
                    f.truncate(truncate_to)
                removed = tape_size - truncate_to
                self.logger.warning(
                    f"  Startup audit: truncated {removed}B of corrupt data"
                )

    def _find_last_valid_boundary(self, tape_path: Path) -> Optional[int]:
        """Find byte offset of end of last valid JSON line in tape.

        Reads backwards through last 32KB to find a line that parses as
        valid JSON with correct CRC. Returns byte position to truncate to,
        or None if no valid line found.
        """
        try:
            with open(tape_path, "rb") as f:
                f.seek(0, 2)
                size = f.tell()
                read_size = min(size, 32768)
                f.seek(size - read_size)
                tail = f.read()

            # Walk backwards through lines
            offset = len(tail)
            for raw_line in reversed(tail.split(b"\n")):
                offset -= len(raw_line) + 1  # +1 for the \n
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                try:
                    record = _json_loads(raw_line)
                    if "seq" in record and "ts_us" in record:
                        # Valid — return position after this line's \n
                        return size - read_size + offset + len(raw_line) + 1
                except Exception:
                    continue
            return None
        except Exception:
            return None

    def track_exchange_seq(self, src: str, msg_raw: str) -> None:
        """Track exchange-provided sequence numbers to detect gaps.

        v7.5: Parses exchange seq from raw message. If gap detected, logs warning.
        Called from hot path — only parses if msg contains "sequence" (CB) or "seq" (KL).
        String containment check is O(n) but fast for typical message sizes.
        """
        if src == "cb" and '"sequence"' in msg_raw:
            try:
                # Coinbase ticker messages: {"type":"ticker","sequence":12345,"product_id":"BTC-USD",...}
                msg = _json_loads(msg_raw)
                if msg.get("type") == "ticker":
                    product = msg.get("product_id", "")
                    seq = msg.get("sequence", 0)
                    if product and seq:
                        last = self._cb_last_seq.get(product, 0)
                        # Coinbase "sequence" is the orderbook sequence, NOT
                        # a per-ticker-message counter.  Between two ticker
                        # messages the seq jumps by hundreds/thousands because
                        # orderbook events (opens, cancels) also increment it.
                        # v7.7: Threshold configurable via collector_config.yaml
                        if last > 0 and seq > last + CB_GAP_THRESHOLD:
                            gap_size = seq - last - 1
                            self.cb_gaps += 1
                            # v7.7: Suppress repeat warnings within 60s per product
                            _now_gap = time.monotonic()
                            if _now_gap - self._cb_last_gap_log.get(product, 0) > 60:
                                self.logger.warning(
                                    f"[GAP] Coinbase {product}: seq {last} -> {seq} "
                                    f"(gap={gap_size} messages)"
                                )
                                self._cb_last_gap_log[product] = _now_gap
                        self._cb_last_seq[product] = seq
            except Exception:
                pass
        elif src == "kl" and '"seq"' in msg_raw:
            try:
                # Kalshi messages: {"type":"...","seq":12345,...}
                msg = _json_loads(msg_raw)
                kl_seq = msg.get("seq", 0)
                if kl_seq and isinstance(kl_seq, int):
                    # v7.7: Threshold configurable via collector_config.yaml
                    if self._kl_last_seq > 0 and kl_seq > self._kl_last_seq + KL_GAP_THRESHOLD:
                        gap_size = kl_seq - self._kl_last_seq - 1
                        self.kl_gaps += 1
                        # v7.7: Suppress repeat warnings within 60s
                        _now_gap = time.monotonic()
                        if _now_gap - self._kl_last_gap_log > 60:
                            self.logger.warning(
                                f"[GAP] Kalshi: seq {self._kl_last_seq} -> {kl_seq} "
                                f"(gap={gap_size} messages)"
                            )
                            self._kl_last_gap_log = _now_gap
                    if kl_seq > self._kl_last_seq:
                        self._kl_last_seq = kl_seq
            except Exception:
                pass

    def track_latency(self, src: str, msg_raw: str, local_ts_us: int) -> None:
        """Track feed latency: exchange timestamp vs local receipt timestamp.

        v7.5: Coinbase ticker messages include "time" field (ISO 8601).
        Computes latency = local_ts - exchange_ts in milliseconds.
        Stores in a rolling buffer for percentile calculation.
        """
        if src == "cb" and '"time"' in msg_raw:
            try:
                msg = _json_loads(msg_raw)
                if msg.get("type") == "ticker":
                    exchange_time_str = msg.get("time", "")
                    if exchange_time_str:
                        # Parse ISO 8601: "2026-03-03T20:52:42.123456Z"
                        from datetime import datetime, timezone
                        # Fast parse: avoid strptime overhead by manual parsing
                        # Format: YYYY-MM-DDTHH:MM:SS.ffffffZ
                        if len(exchange_time_str) > 20 and exchange_time_str.endswith("Z"):
                            dt = datetime.fromisoformat(exchange_time_str.replace("Z", "+00:00"))
                            exchange_ts_us = int(dt.timestamp() * 1_000_000)
                            latency_ms = (local_ts_us - exchange_ts_us) / 1000.0
                            if -5000 < latency_ms < 60000:  # Sanity check: -5s to 60s
                                self._latency_samples.append(latency_ms)
            except Exception:
                pass

    def get_latency_stats(self) -> Optional[Tuple[float, float, float]]:
        """Return (p50, p95, p99) latency in milliseconds, or None if no data."""
        n = len(self._latency_samples)
        if n < 10:
            return None
        s = sorted(self._latency_samples)
        p50 = s[int(n * 0.50)]
        p95 = s[int(n * 0.95)]
        p99 = s[min(int(n * 0.99), n - 1)]
        return (p50, p95, p99)

    def open(self) -> None:
        """Open all three tape files.

        Before opening:
          1. Repair any partial (crash-truncated) lines from previous run.
          2. Recover the last sequence number so we continue numbering
             from where we left off instead of resetting to 0.
        """
        for d in [UNIFIED_DIR, UNIFIED_ARCHIVE,
                  ORACLE_DIR, ORACLE_ARCHIVE,
                  KALSHI_DIR, KALSHI_ARCHIVE]:
            d.mkdir(parents=True, exist_ok=True)

        # Step 1: Fix any partial lines left by a previous crash
        self._repair_partial_lines()

        # Step 1.5 (v7.6): Validate last N records for JSON/CRC integrity
        self._audit_tape_integrity(count=_cfg("startup", "audit_records", 50))

        # Step 2: Recover last seq number from existing tape
        recovered_seq = self._recover_seq_from_tape()
        if recovered_seq > 0:
            self._seq = recovered_seq
            self.logger.info(f"  Resumed seq from tape: {recovered_seq:,}")

        # Step 3: Open file descriptors for writing
        self._current_hour = datetime.now(timezone.utc).hour
        self._unified_fd = self._open_fd(UNIFIED_TAPE)
        self._oracle_fd = self._open_fd(ORACLE_TAPE)
        self._kalshi_fd = self._open_fd(KALSHI_TAPE)

        self.logger.info(f"  Unified tape: {UNIFIED_TAPE}")
        self.logger.info(f"  Oracle tape:  {ORACLE_TAPE}")
        self.logger.info(f"  Kalshi tape:  {KALSHI_TAPE}")

    def _open_fd(self, path: Path) -> int:
        """Open a file for raw binary append. Returns fd or -1."""
        try:
            flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
            if hasattr(os, "O_BINARY"):
                flags |= os.O_BINARY
            return os.open(str(path), flags, 0o644)
        except OSError as e:
            self.logger.warning(f"  os.open failed for {path}: {e}")
            return -1

    def write_event(self, src: str, msg_raw: str) -> int:
        """Write one event to all relevant tapes. Returns sequence number.

        HOT PATH — called for every WebSocket message.

        v5 optimizations applied:
          - Pre-encoded byte constants (no .encode() calls)
          - Single escaped payload shared between unified + legacy lines
          - Counter-only flush gating (no time.monotonic() per call)
          - No os.fsync() — data safe in OS write buffer until rotation
        """
        # v7.6: If disk-paused, buffer in memory instead of writing to disk
        if self._disk_paused:
            if len(self._pause_buffer) < self._max_pause_buffer:
                self._pause_buffer.append((src, msg_raw))
            return -1

        # v7.4: Message-level deduplication — skip exact duplicate messages
        # Uses CRC32 of the raw message string for fast comparison.
        # Duplicates can occur from double-subscription or WS reconnect overlap.
        _msg_hash = _crc32(msg_raw.encode("utf-8") if isinstance(msg_raw, str) else msg_raw) & 0xFFFFFFFF
        if _msg_hash in self._recent_msg_hashes:
            self._dedup_collisions += 1
            return -1  # Skip duplicate
        self._recent_msg_hashes.add(_msg_hash)
        if len(self._recent_msg_hashes) > 5000:
            self._recent_msg_hashes.clear()  # Rotate to prevent memory growth

        # Single clock, single counter
        ts_us = self._time_ns() // 1000
        self._seq += 1
        seq = self._seq

        # JSON-escape the raw WebSocket message (shared between both formats)
        escaped = _json_dumps(msg_raw)

        # Integer-to-bytes (these are the only dynamic parts per message)
        seq_b = str(seq).encode()
        ts_b = str(ts_us).encode()

        # ── Unified tape line ──
        # Uses pre-encoded src byte constant from _SRC_BYTES dict
        # v7.4: CRC32 checksum of escaped payload for integrity verification
        crc_b = str(_crc32(escaped) & 0xFFFFFFFF).encode()
        unified_line = (_B_SEQ_PREFIX + seq_b +
                        _B_TS_PREFIX + ts_b +
                        _SRC_BYTES[src] +
                        escaped +
                        _B_CRC_PREFIX + crc_b + _B_NEWLINE)

        # ── Legacy tape line (shared prefix, same escaped payload) ──
        legacy_line = (_B_LEGACY_PREFIX + ts_b +
                       _B_LEGACY_RAW + escaped + _B_NEWLINE)

        # Write to file descriptors
        # v7.5: Check return value of os.write() to detect short writes (disk full)
        # v7.9: try/except for OSError — fd may be briefly invalid during rotation
        u_fd = self._unified_fd
        if u_fd != -1:
            try:
                _wb = os.write(u_fd, unified_line)
                if _wb != len(unified_line):
                    self.logger.warning(
                        f"[TAPE] SHORT WRITE on unified: {_wb}/{len(unified_line)} bytes (disk full?)"
                    )
            except OSError:
                pass  # fd closed by rotation — safe to skip, will reopen next cycle

        if src == "cb":
            o_fd = self._oracle_fd
            if o_fd != -1:
                try:
                    _wb = os.write(o_fd, legacy_line)
                    if _wb != len(legacy_line):
                        self.logger.warning(f"[TAPE] SHORT WRITE on oracle: {_wb}/{len(legacy_line)} bytes")
                except OSError:
                    pass
            self.cb_count += 1
        else:  # "kl"
            k_fd = self._kalshi_fd
            if k_fd != -1:
                try:
                    _wb = os.write(k_fd, legacy_line)
                    if _wb != len(legacy_line):
                        self.logger.warning(f"[TAPE] SHORT WRITE on kalshi: {_wb}/{len(legacy_line)} bytes")
                except OSError:
                    pass
            self.kl_count += 1

        # Tracking
        self.lines_written += 1
        self.ws_lines_written += 1  # v9.0: WS-only counter for accurate event rate (excludes REST snapshots)
        self.bytes_written += len(unified_line) + len(legacy_line)
        self._lines_since_flush += 1

        # v8.2: Track per-exchange message sizes for network metrics
        # Uses len(msg_raw) which is the original WebSocket message bytes.
        _msg_len = len(msg_raw) if isinstance(msg_raw, str) else len(msg_raw)
        if src == "cb":
            self._cb_msg_bytes += _msg_len
            self._cb_msg_count_size += 1
        elif src == "kl":
            self._kl_msg_bytes += _msg_len
            self._kl_msg_count_size += 1

        # v7.5: Exchange sequence gap detection + latency tracking
        # Run AFTER writes so they never delay tape I/O.
        # Gap detection runs on every message (string containment check is cheap).
        # Latency tracking samples every 10th CB message to avoid JSON parse overhead.
        self.track_exchange_seq(src, msg_raw)
        if src == "cb" and seq % 10 == 0:
            self.track_latency(src, msg_raw, ts_us)

        # v7.6: Per-product health tracking (Upgrade 5)
        # Extract product_id using fast str.find() — no JSON parse needed.
        # Tracks last message time and message count per product.
        _now_mono = time.monotonic()
        if src == "cb":
            _pid_start = msg_raw.find('"product_id":"')
            if _pid_start != -1:
                _pid_start += 14  # len('"product_id":"')
                _pid_end = msg_raw.find('"', _pid_start)
                if _pid_end != -1:
                    _pid = msg_raw[_pid_start:_pid_end]
                    self._product_last_msg[_pid] = _now_mono
                    self._product_msg_count[_pid] = self._product_msg_count.get(_pid, 0) + 1
        elif src == "kl":
            _mt_start = msg_raw.find('"market_ticker":"')
            if _mt_start != -1:
                _mt_start += 17  # len('"market_ticker":"')
                _mt_end = msg_raw.find('"', _mt_start)
                if _mt_end != -1:
                    _mt = msg_raw[_mt_start:_mt_end]
                    self._product_last_msg[_mt] = _now_mono
                    self._product_msg_count[_mt] = self._product_msg_count.get(_mt, 0) + 1

        # v5 FIX #7: Counter-only flush gating
        # No time.monotonic() call per message — just check line count.
        # OS write buffers provide crash safety; fsync only on rotation.
        if self._lines_since_flush >= FLUSH_EVERY_LINES:
            self._lines_since_flush = 0

        return seq

    def write_snapshot(self, snap_msg: dict) -> int:
        """Write a REST orderbook snapshot to unified + kalshi tapes.

        Called periodically (every 30s) to anchor orderbook state.
        Not on the hot path — can afford slightly more overhead.
        """
        ts_us = self._time_ns() // 1000
        self._seq += 1
        seq = self._seq

        raw_str = _json_dumps_str(snap_msg)
        escaped = _json_dumps(raw_str)

        seq_b = str(seq).encode()
        ts_b = str(ts_us).encode()

        # Unified tape (v7.4: with CRC32 checksum)
        crc_b = str(_crc32(escaped) & 0xFFFFFFFF).encode()
        unified_line = (_B_SEQ_PREFIX + seq_b +
                        _B_TS_PREFIX + ts_b +
                        _B_SRC_SNAP +
                        escaped +
                        _B_CRC_PREFIX + crc_b + _B_NEWLINE)
        if self._unified_fd != -1:
            try:
                _wb = os.write(self._unified_fd, unified_line)
                if _wb != len(unified_line):
                    self.logger.warning(f"[TAPE] SHORT WRITE on unified snapshot: {_wb}/{len(unified_line)} bytes")
            except OSError:
                pass  # fd closed by rotation

        # Legacy kalshi tape (Stage10 reads these as regular messages)
        legacy_line = (_B_LEGACY_PREFIX + ts_b +
                       _B_LEGACY_RAW + escaped + _B_NEWLINE)
        if self._kalshi_fd != -1:
            try:
                _wb = os.write(self._kalshi_fd, legacy_line)
                if _wb != len(legacy_line):
                    self.logger.warning(f"[TAPE] SHORT WRITE on kalshi snapshot: {_wb}/{len(legacy_line)} bytes")
            except OSError:
                pass

        self.snap_count += 1
        self.lines_written += 1
        self.bytes_written += len(unified_line) + len(legacy_line)

        return seq

    def _flush_hard(self) -> None:
        """Full fsync — only used during rotation and shutdown.

        v5 FIX #2: Removed os.fsync() from the hot path entirely.

        In v4, _flush() called os.fsync() on all 3 FDs every 200 messages
        or 250ms. Each fsync costs 5-50ms (forces physical disk write),
        so 3 FDs = 15-150ms of blocking in the middle of WebSocket processing.

        os.write() already puts data in the kernel page cache, which survives
        application crashes. For a collector that auto-reconnects, losing the
        last fraction of a second on a power failure is acceptable.
        """
        for fd in (self._unified_fd, self._oracle_fd, self._kalshi_fd):
            if fd != -1:
                try:
                    os.fsync(fd)
                except OSError:
                    pass

    def check_rotation(self) -> None:
        """Check if tapes need hourly or size-based rotation."""
        now_hour = datetime.now(timezone.utc).hour
        need, reason = False, ""

        if ROTATE_EVERY_HOUR and now_hour != self._current_hour:
            need = True
            reason = f"hourly ({self._current_hour:02d}->{now_hour:02d} UTC)"

        # v7.8 FIX: Check ALL tape files for size, not just unified.
        # At high event rates (>10k/s) tapes can grow past threshold quickly.
        if not need:
            for tape_path in [UNIFIED_TAPE, KALSHI_TAPE, ORACLE_TAPE]:
                try:
                    if tape_path.exists() and tape_path.stat().st_size > ROTATE_AT_BYTES:
                        mb = tape_path.stat().st_size // (1024 * 1024)
                        need = True
                        reason = (f"size ({tape_path.name}: "
                                  f"{mb}MB > {ROTATE_AT_MB}MB)")
                        break
                except OSError:
                    pass

        if need:
            self._rotate(reason)
            self._current_hour = now_hour

    def _rotate(self, reason: str) -> None:
        """Rotate all three tape files and inject snapshot anchors.

        v7.9 Windows-safe rotation (copy+truncate):
        On Windows, rename() fails when another process (the paper/live
        trader) has the file open for reading ([WinError 32]).  The old
        approach silently skipped locked files, causing kalshi/oracle tapes
        to grow without bound (2+ GB) while the unified tape rotated every
        10 seconds producing hundreds of tiny archives.

        New approach: copy the file to archive, then truncate the original.
        shutil.copy2() works even when other processes hold a read handle.
        Truncation via os.open(O_WRONLY|O_TRUNC) also works because the
        collector already has write access. The reader just sees EOF on
        its next read and re-seeks — no data loss, no freeze.
        """
        # Full fsync before rotation to ensure archives are complete
        self._flush_hard()

        # Include seconds to prevent filename collisions on fast restarts
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        total_mb = 0.0
        rotated_files = []
        skipped_files = []

        # Rotate each tape via copy+truncate (Windows-safe)
        # v8.1: Archives go into daily subfolders (YYYY-MM-DD) for clean organization
        day_folder = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for fd_name, tape_path, archive_dir, prefix in [
            ("_unified_fd", UNIFIED_TAPE, UNIFIED_ARCHIVE, "unified"),
            ("_oracle_fd",  ORACLE_TAPE,  ORACLE_ARCHIVE,  "oracle"),
            ("_kalshi_fd",  KALSHI_TAPE,  KALSHI_ARCHIVE,  "kalshi"),
        ]:
            if not tape_path.exists() or tape_path.stat().st_size == 0:
                continue

            # v8.1: Create daily subfolder (e.g. archive/2026-03-04/)
            daily_dir = archive_dir / day_folder
            daily_dir.mkdir(parents=True, exist_ok=True)
            archive_name = f"{prefix}_{ts}.jsonl"
            archive_path = daily_dir / archive_name

            # Step 1: Close our fd and fsync to flush all pending writes
            fd = getattr(self, fd_name)
            if fd != -1:
                try:
                    os.fsync(fd)
                    os.close(fd)
                except OSError:
                    pass
                setattr(self, fd_name, -1)

            # Step 2: Try rename first (fastest, works when no other process has file open)
            try:
                tape_path.rename(archive_path)
                mb = archive_path.stat().st_size / (1024 * 1024)
                total_mb += mb
                rotated_files.append(prefix)
                # Reopen a new empty file at the original path
                setattr(self, fd_name, self._open_fd(tape_path))
                continue  # Success — skip to next tape
            except OSError:
                pass  # Rename failed (Windows lock) — fall through to copy+truncate

            # Step 3: Copy+truncate fallback for Windows-locked files
            try:
                # Copy file contents to archive (works even with read locks)
                shutil.copy2(str(tape_path), str(archive_path))
                mb = archive_path.stat().st_size / (1024 * 1024)

                # Truncate the original file to 0 bytes
                trunc_flags = os.O_WRONLY | os.O_TRUNC
                if hasattr(os, "O_BINARY"):
                    trunc_flags |= os.O_BINARY
                trunc_fd = os.open(str(tape_path), trunc_flags)
                os.close(trunc_fd)

                total_mb += mb
                rotated_files.append(prefix)
                # Reopen for continued append writing
                setattr(self, fd_name, self._open_fd(tape_path))
                self.logger.info(
                    f"  {prefix}: copy+truncate rotation ({mb:.1f}MB)"
                )
            except OSError as e:
                skipped_files.append(prefix)
                # Both rename and copy+truncate failed — reopen and keep appending
                setattr(self, fd_name, self._open_fd(tape_path))
                self.logger.warning(
                    f"  Rotation failed for {prefix}: {e}"
                )

        if rotated_files:
            self.logger.info(
                f"ROTATED: {ts} ({total_mb:.1f}MB total, "
                f"{self.lines_written:,} lines, "
                f"cb={self.cb_count} kl={self.kl_count} snap={self.snap_count}) "
                f"[{reason}]"
            )
            # Reset counters for the new rotation period
            self.lines_written = 0
            self.ws_lines_written = 0  # v9.0: Reset WS-only counter on rotation too
            self.bytes_written = 0
            self.cb_count = 0
            self.kl_count = 0
            self.snap_count = 0

            # v7.5: Compress rotated archives via capped ThreadPoolExecutor.
            # JSONL compresses ~10:1 with gzip, saving significant disk space.
            # v7.5 FIX: max_workers=2 prevents unbounded thread creation if
            # multiple rotations happen faster than compression can finish.
            # v8.1: Archive path now includes daily subfolder
            for fd_name, tape_path, archive_dir, prefix in [
                ("_unified_fd", UNIFIED_TAPE, UNIFIED_ARCHIVE, "unified"),
                ("_oracle_fd",  ORACLE_TAPE,  ORACLE_ARCHIVE,  "oracle"),
                ("_kalshi_fd",  KALSHI_TAPE,  KALSHI_ARCHIVE,  "kalshi"),
            ]:
                daily_dir = archive_dir / day_folder
                archive_name = f"{prefix}_{ts}.jsonl"
                archive_path = daily_dir / archive_name
                if archive_path.exists() and prefix in rotated_files:
                    self._compress_pool.submit(self._compress_archive, archive_path)
        elif skipped_files:
            self.logger.info(
                f"ROTATION DEFERRED: all files locked ({', '.join(skipped_files)}) "
                f"— will retry next cycle [{reason}]"
            )

        # NOTE: Snapshot anchoring REMOVED from _rotate().
        # fetch_rest_snapshots() is SYNCHRONOUS and blocks the asyncio event loop
        # for 50+ seconds (100ms sleep × ~500 tickers). This caused:
        #   - Kalshi WS recv timeouts (no messages processed during block)
        #   - periodic_loop stalls (no stats, no heartbeat)
        #   - 5+ minute log gaps
        # Snapshots are already handled by the async periodic_loop in the
        # post-settlement window (30-150s after each :00/:15/:30/:45).
        # The first async snapshot batch after rotation anchors the new tape.

    def _compress_archive(self, archive_path: Path) -> None:
        """Archive a rotated JSONL tape (runs in background thread).

        v7.7: Converts to Parquet+zstd if pyarrow is installed, giving
        columnar queries (read seq/ts_us without loading raw) at similar
        compression ratios.  Falls back to gzip if pyarrow unavailable.
        """
        if _HAS_PYARROW:
            self._archive_to_parquet(archive_path)
        else:
            self._archive_to_gzip(archive_path)

    def _archive_to_parquet(self, archive_path: Path) -> None:
        """Convert JSONL archive to Parquet with zstd compression.

        v7.7: Schema: seq(int64), ts_us(int64), src(dict-encoded string),
        raw(large_string), crc(uint32).  Falls back to gzip on failure.
        """
        parquet_path = archive_path.with_suffix(".parquet")
        try:
            original_size = archive_path.stat().st_size

            # Parse JSONL into columnar lists
            seqs: List[int] = []
            ts_list: List[int] = []
            srcs: List[str] = []
            raws: List[str] = []
            crcs: List[int] = []

            with open(archive_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = _json_loads(line)
                        seqs.append(rec.get("seq", 0))
                        ts_list.append(rec.get("ts_us", 0))
                        srcs.append(rec.get("src", ""))
                        raws.append(rec.get("raw", ""))
                        crcs.append(rec.get("crc", 0))
                    except Exception:
                        continue  # skip malformed lines

            if not seqs:
                self.logger.warning(
                    f"  Parquet skipped for {archive_path.name} -- no valid records"
                )
                return

            # Build PyArrow table with dictionary encoding for src
            table = pa.table({
                "seq": pa.array(seqs, type=pa.int64()),
                "ts_us": pa.array(ts_list, type=pa.int64()),
                "src": pa.array(srcs, type=pa.string()).dictionary_encode(),
                "raw": pa.array(raws, type=pa.large_string()),
                "crc": pa.array(crcs, type=pa.uint32()),
            })

            # Write with zstd compression (good ratio, fast decompression)
            pq.write_table(
                table, str(parquet_path),
                compression="zstd",
                compression_level=3,
            )

            # Verify before deleting original
            pq_size = parquet_path.stat().st_size
            pq_meta = pq.read_metadata(str(parquet_path))
            if pq_size > 0 and pq_meta.num_rows == len(seqs):
                archive_path.unlink()
                ratio = original_size / max(pq_size, 1)
                self.logger.info(
                    f"  PARQUET: {archive_path.name} -> .parquet "
                    f"({original_size / (1024*1024):.1f}MB -> "
                    f"{pq_size / (1024*1024):.1f}MB, "
                    f"{ratio:.1f}x, {len(seqs):,} rows)"
                )
            else:
                parquet_path.unlink(missing_ok=True)
                self.logger.warning(
                    f"  Parquet verify failed for {archive_path.name} -- keeping original"
                )
        except Exception as e:
            parquet_path.unlink(missing_ok=True)
            self.logger.warning(
                f"  Parquet failed for {archive_path.name}: {e} -- falling back to gzip"
            )
            self._archive_to_gzip(archive_path)

    def _archive_to_gzip(self, archive_path: Path) -> None:
        """Compress a rotated archive with gzip (fallback if pyarrow unavailable).

        v7.4: JSONL tape data compresses ~10:1 with gzip.
        200MB tape -> ~20MB compressed -> saves ~180MB per rotation.
        Original is deleted after successful compression + verification.
        """
        gz_path = archive_path.with_suffix(".jsonl.gz")
        try:
            original_size = archive_path.stat().st_size
            with open(archive_path, "rb") as f_in:
                with gzip.open(gz_path, "wb", compresslevel=6) as f_out:
                    while True:
                        chunk = f_in.read(1024 * 1024)
                        if not chunk:
                            break
                        f_out.write(chunk)
            compressed_size = gz_path.stat().st_size
            if compressed_size > 0:
                archive_path.unlink()
                ratio = original_size / max(compressed_size, 1)
                self.logger.info(
                    f"  GZIP: {archive_path.name} -> .gz "
                    f"({original_size / (1024*1024):.1f}MB -> "
                    f"{compressed_size / (1024*1024):.1f}MB, "
                    f"{ratio:.1f}x ratio)"
                )
            else:
                gz_path.unlink(missing_ok=True)
                self.logger.warning(
                    f"  Gzip failed for {archive_path.name} -- keeping original"
                )
        except Exception as e:
            gz_path.unlink(missing_ok=True)
            self.logger.warning(f"  Gzip error for {archive_path.name}: {e}")

    def cleanup_old_archives(self, max_age_days: int = ARCHIVE_RETENTION_DAYS) -> None:
        """Delete archive files older than max_age_days.

        v7.5: Prevents unbounded disk growth. Runs periodically from periodic_loop.
        Checks both .jsonl and .jsonl.gz files across all three archive dirs.
        v8.1: Walks daily subfolders (YYYY-MM-DD/) recursively. Removes empty dirs.
        """
        cutoff = time.time() - (max_age_days * 86400)
        deleted = 0
        freed_mb = 0.0

        for archive_dir in [UNIFIED_ARCHIVE, ORACLE_ARCHIVE, KALSHI_ARCHIVE]:
            if not archive_dir.exists():
                continue
            # v8.1: rglob walks into daily subfolders (and handles legacy flat files)
            for f in archive_dir.rglob("*"):
                if f.is_dir():
                    continue
                if f.suffix not in (".jsonl", ".gz", ".parquet"):
                    continue
                try:
                    if f.stat().st_mtime < cutoff:
                        size_mb = f.stat().st_size / (1024 * 1024)
                        f.unlink()
                        deleted += 1
                        freed_mb += size_mb
                except OSError:
                    pass
            # v8.1: Remove empty daily subfolders after cleanup
            try:
                for sub in sorted(archive_dir.iterdir(), reverse=True):
                    if sub.is_dir() and not any(sub.iterdir()):
                        sub.rmdir()
            except OSError:
                pass

        if deleted > 0:
            self.logger.info(
                f"[RETENTION] Cleaned {deleted} archives older than {max_age_days}d "
                f"({freed_mb:.1f}MB freed)"
            )

    # ──────────────────────────────────────────────────────────────
    # v7.6 UPGRADE 4: Disk Space Pause/Resume
    # ──────────────────────────────────────────────────────────────

    def pause_writes(self) -> None:
        """Pause tape writes due to critically low disk space.

        v7.6: Closes FDs and buffers messages in memory (up to max_pause_buffer).
        Call resume_writes() when disk space is freed.
        """
        if self._disk_paused:
            return
        self._disk_paused = True
        self._flush_hard()
        for fd_name in ("_unified_fd", "_oracle_fd", "_kalshi_fd"):
            fd = getattr(self, fd_name)
            if fd != -1:
                try:
                    os.fsync(fd)
                    os.close(fd)
                except OSError:
                    pass
                setattr(self, fd_name, -1)
        self.logger.critical(
            "[DISK] WRITES PAUSED -- disk critically low. "
            f"Buffering to memory (max {self._max_pause_buffer} messages)."
        )

    def resume_writes(self) -> None:
        """Resume tape writes after disk space recovered.

        v7.6: Reopens FDs and flushes any buffered messages to disk.
        """
        if not self._disk_paused:
            return
        # Reopen file descriptors
        self._unified_fd = self._open_fd(UNIFIED_TAPE)
        self._oracle_fd = self._open_fd(ORACLE_TAPE)
        self._kalshi_fd = self._open_fd(KALSHI_TAPE)
        self._disk_paused = False

        # Flush buffered messages
        buffered = len(self._pause_buffer)
        for src, msg_raw in self._pause_buffer:
            self.write_event(src, msg_raw)
        self._pause_buffer.clear()

        self.logger.info(
            f"[DISK] WRITES RESUMED -- flushed {buffered} buffered messages"
        )

    def close(self) -> None:
        """Flush and close all file descriptors."""
        self._flush_hard()
        for fd_name in ("_unified_fd", "_oracle_fd", "_kalshi_fd"):
            fd = getattr(self, fd_name)
            if fd != -1:
                try:
                    os.fsync(fd)
                    os.close(fd)
                except OSError:
                    pass
                setattr(self, fd_name, -1)
        # v7.5: Wait for pending compression threads to finish
        self._compress_pool.shutdown(wait=True, cancel_futures=False)
        self.logger.info(
            f"Tapes closed: {self.lines_written:,} lines, "
            f"seq={self._seq:,}, "
            f"cb={self.cb_count} kl={self.kl_count} snap={self.snap_count}"
        )


# ==============================================================================
# MAIN COLLECTOR LOOP
# ==============================================================================
async def run_collector(symbols: List[str], all_markets: bool,
                        logger: logging.Logger,
                        struct_logger: Optional[logging.Logger] = None) -> None:
    # v7.4: Initialize the retry queue for snapshot failures (thread-safe)
    global _snapshot_retry_queue
    _snapshot_retry_queue = asyncio.Queue(maxsize=1000)

    tape = UnifiedTapeWriter(logger)
    tape.open()

    # ── v7.7: Prometheus metrics endpoint ──
    # Uses prometheus_client's built-in HTTP server (runs in daemon thread).
    _prom_enabled = _cfg("metrics", "enabled", True)
    _prom_port = _cfg("metrics", "port", 9090)
    _collector_start_mono = time.monotonic()
    if _HAS_PROMETHEUS and _prom_enabled:
        try:
            _prom_start_server(_prom_port)
            logger.info(f"[PROM] Metrics endpoint started on :{_prom_port}/metrics")
        except Exception as e:
            logger.warning(f"[PROM] Failed to start metrics server: {e}")
    elif _prom_enabled and not _HAS_PROMETHEUS:
        logger.info("[PROM] prometheus_client not installed -- metrics disabled")

    # ── v7.6 UPGRADE 6: Backpressure queue ──
    # Decouples WS recv from disk I/O. WS loops put messages on a bounded
    # queue; a dedicated writer coroutine drains it. If queue fills up,
    # low-priority messages (orderbook_delta) are dropped to prevent OOM.
    _bp_queue_size = _cfg("backpressure", "queue_size", 50000)
    _write_queue: asyncio.Queue = asyncio.Queue(maxsize=_bp_queue_size)
    _bp_dropped: int = 0

    def _classify_priority(msg_raw: str) -> int:
        """Classify message priority for drop decisions. Fast string search.
        1=trade (never drop), 2=ticker (high), 3=orderbook_delta (droppable).
        """
        if '"type":"trade"' in msg_raw or '"channel":"trade"' in msg_raw:
            return 1
        if '"orderbook_delta"' in msg_raw:
            return 3
        return 2  # ticker, snapshot, or unknown = keep

    shutdown = asyncio.Event()

    def handle_signal(*_):
        logger.info("Shutdown signal received")
        shutdown.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_event_loop().add_signal_handler(sig, handle_signal)
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, lambda *_: shutdown.set())

    # Shared state
    kalshi_tickers: List[str] = []
    last_tickers: List[str] = []
    reconnect_cb: int = 0
    reconnect_kl: int = 0
    # v7.6: Reconnect gap tracking
    _last_snapshot_fetch_ts: float = 0.0  # monotonic time of last snapshot fetch
    _last_kl_disconnect_ts: float = 0.0
    _last_cb_disconnect_ts: float = 0.0

    # v7.5: Cross-task communication for rediscovery (moved to periodic_loop)
    # periodic_loop discovers new tickers → puts them here → recv loop subscribes via WS
    # v7.5 FIX: Changed _pending_subscribe from bare list to asyncio.Queue to
    # eliminate race condition between periodic_loop (writer) and kalshi_ws_loop (reader).
    # _subscribed_set protected by asyncio.Lock for safe concurrent access.
    _pending_subscribe: asyncio.Queue = asyncio.Queue(maxsize=2000)
    _subscribed_set: set = set()             # All currently subscribed tickers (shared)
    _subscribed_lock: asyncio.Lock = asyncio.Lock()  # v7.5: Protects _subscribed_set
    _kl_ws_ref: List = [None]                # [ws] — recv loop stores its WS handle here
    _cb_ws_ref: List = [None]                # [ws] — v8.2: CB WS handle for RTT reading

    # ──────────────────────────────────────────────────────────────
    # COINBASE WebSocket Task
    # ──────────────────────────────────────────────────────────────
    async def coinbase_ws_loop():
        nonlocal reconnect_cb, _last_cb_disconnect_ts, _bp_dropped

        while not shutdown.is_set():
            try:
                reconnect_cb += 1
                if reconnect_cb > 1:
                    logger.warning(f"[CB] RECONNECT attempt {reconnect_cb}")

                async with websockets.connect(
                    COINBASE_WS_URI,
                    ping_interval=CB_PING_INTERVAL,
                    ping_timeout=CB_PING_TIMEOUT,
                    max_size=10 * 1024 * 1024,
                    compression=WS_COMPRESSION,
                    open_timeout=10,          # 10s timeout on TCP + TLS handshake
                ) as ws:
                    # Subscribe
                    await ws.send(_json_dumps_str({
                        "type": "subscribe",
                        "product_ids": COINBASE_PRODUCTS,
                        "channels": COINBASE_CHANNELS,
                    }))
                    logger.info(f"[CB] Connected -- {len(COINBASE_PRODUCTS)} products")
                    _enable_tcp_keepalive(ws, logger, "CB")  # v9.2: prevent idle-timeout drops
                    _cb_ws_ref[0] = ws  # v8.2: Store WS handle for RTT reading
                    reconnect_cb = 1    # Reset backoff counter on successful connect

                    # v7.6: Reset CB seq tracking on reconnect to avoid false gap alerts
                    if reconnect_cb > 1:
                        tape._cb_last_seq.clear()
                        if _last_cb_disconnect_ts > 0:
                            gap = time.monotonic() - _last_cb_disconnect_ts
                            logger.info(f"[CB] Reconnected after {gap:.1f}s gap (seq tracking reset)")
                            _last_cb_disconnect_ts = 0.0

                    # v5 FIX #4: Cached write ref — ONLY tape write in hot path.
                    # Removed JSON parsing for price tracking (was ~0.1ms per msg).
                    # Price display moved out of hot path; Stage10 gets prices
                    # from the tape directly.
                    _write = tape.write_event

                    async for msg_raw in ws:
                        if shutdown.is_set():
                            break

                        # ╔══ HOT PATH (v7.6: via backpressure queue) ══╗
                        try:
                            _write_queue.put_nowait(("cb", msg_raw))
                        except asyncio.QueueFull:
                            _pri = _classify_priority(msg_raw)
                            if _pri <= 1:
                                await _write_queue.put(("cb", msg_raw))
                            else:
                                _bp_dropped += 1
                        # ╚══════════════════════════════════════════════╝

            except ConnectionClosed as e:
                _cb_ws_ref[0] = None  # v8.2: clear so RTT reads return -1
                _last_cb_disconnect_ts = time.monotonic()  # v7.6: track gap
                code = getattr(e, 'code', '?')
                reason = getattr(e, 'reason', '') or ''
                # v7.4: Exponential backoff — 2s, 4s, 8s, ... max 60s
                _cb_backoff = min(2 * (2 ** min(reconnect_cb - 1, 5)), 60)
                logger.warning(
                    f"[CB] WebSocket closed: code={code} "
                    f"reason='{reason}' ({e}). Reconnecting in {_cb_backoff}s..."
                )
                await asyncio.sleep(_cb_backoff)
            except Exception as e:
                _cb_ws_ref[0] = None  # v8.2: clear so RTT reads return -1
                _last_cb_disconnect_ts = time.monotonic()  # v7.6: track gap
                _cb_backoff = min(5 * (2 ** min(reconnect_cb - 1, 4)), 60)
                logger.error(f"[CB] Error: {e} -- retry in {_cb_backoff}s", exc_info=True)
                await asyncio.sleep(_cb_backoff)

    # ──────────────────────────────────────────────────────────────
    # KALSHI WebSocket Task
    # ──────────────────────────────────────────────────────────────
    async def kalshi_ws_loop():
        nonlocal reconnect_kl, kalshi_tickers, last_tickers, _last_kl_disconnect_ts, _last_snapshot_fetch_ts, _bp_dropped
        ws_url = _get_kalshi_ws_url()
        _consecutive_fails = 0  # v7.3.1: backoff counter for maintenance windows

        while not shutdown.is_set():
            snapshot_task = None  # Init before anything that could fail
            try:
                reconnect_kl += 1

                # Market discovery
                # v7.3.1: On repeated failures (maintenance), reuse cached tickers
                # instead of re-discovering 499 markets every cycle.
                if _consecutive_fails >= 2 and last_tickers:
                    logger.info(
                        f"[KL] Reusing {len(last_tickers)} cached tickers "
                        f"(attempt {reconnect_kl}, {_consecutive_fails} consecutive fails)"
                    )
                    tickers = last_tickers
                else:
                    # v5 FIX #3: Run blocking REST calls in thread pool
                    logger.info("[KL] Discovering markets...")
                    if all_markets:
                        tickers = await asyncio.to_thread(discover_all_markets, logger)
                    else:
                        tickers = await asyncio.to_thread(
                            discover_crypto_markets, symbols, logger
                        )
                if not tickers and last_tickers:
                    logger.warning("[KL] Discovery returned 0; reusing last list")
                    tickers = last_tickers
                if not tickers:
                    logger.error("[KL] No markets found. Retrying in 30s...")
                    await asyncio.sleep(30)
                    continue
                last_tickers = tickers
                kalshi_tickers = tickers
                tape._active_tickers = list(tickers)

                if reconnect_kl > 1:
                    logger.warning(f"[KL] RECONNECT attempt {reconnect_kl}")

                # Connect
                ws_headers = _kalshi_ws_headers()
                session_start_mono = time.monotonic()  # Track session lifetime
                async with ws_client.connect(
                    ws_url,
                    extra_headers=ws_headers,
                    ping_interval=KL_PING_INTERVAL,
                    ping_timeout=KL_PING_TIMEOUT,
                    max_size=10 * 1024 * 1024,
                    compression=WS_COMPRESSION,
                    open_timeout=10,          # 10s timeout on TCP + TLS handshake
                ) as ws:
                    _consecutive_fails = 0  # v7.3.1: reset on successful connect
                    reconnect_kl = 1     # Reset backoff counter on successful connect
                    logger.info(f"[KL] Connected to {ws_url}")
                    _enable_tcp_keepalive(ws, logger, "KL")  # v9.2: prevent idle-timeout drops

                    # v7.6: Log reconnect gap duration
                    if reconnect_kl > 1 and _last_kl_disconnect_ts > 0:
                        gap = time.monotonic() - _last_kl_disconnect_ts
                        logger.info(
                            f"[KL] Reconnected after {gap:.1f}s gap"
                        )
                        _last_kl_disconnect_ts = 0.0

                    # Subscribe in chunks
                    for i in range(0, len(tickers), KALSHI_SUBSCRIBE_CHUNK):
                        chunk = tickers[i: i + KALSHI_SUBSCRIBE_CHUNK]
                        await ws.send(_json_dumps_str({
                            "id": i + 1,
                            "cmd": "subscribe",
                            "params": {
                                "channels": KALSHI_CHANNELS,
                                "market_tickers": chunk,
                            },
                        }))
                        logger.info(
                            f"  Subscribed chunk "
                            f"{i // KALSHI_SUBSCRIBE_CHUNK + 1}: "
                            f"{len(chunk)} tickers"
                        )
                        await asyncio.sleep(0.05)

                    logger.info(
                        f"[KL] Receiving... ({len(tickers)} markets, "
                        f"{len(KALSHI_CHANNELS)} channels)"
                    )

                    # Track session lifetime for diagnostics
                    session_start_mono = time.monotonic()

                    # v6 FIX: Start receive loop IMMEDIATELY, fetch snapshots
                    # in background. The old code did snapshots BEFORE the receive
                    # loop, which took ~6min for 505 markets and killed the WS
                    # (ping timeout). Now snapshots run concurrently.

                    # Background snapshot task (only on first connect)
                    async def _bg_snapshot_fetch():
                        """Fetch initial snapshots without blocking the receive loop."""
                        try:
                            # v7.3: Use concurrent async fetcher for ALL tickers at once.
                            # Semaphore inside limits to 10 concurrent requests.
                            # ~5s for 508 tickers instead of ~50s sequential.
                            fast_tickers = [t for t in tickers
                                            if "15M" in t.upper()]
                            other_tickers = [t for t in tickers
                                             if "15M" not in t.upper()]

                            if fast_tickers:
                                fast_anchors = await fetch_rest_snapshots_async(
                                    fast_tickers, logger
                                )
                                for snap in fast_anchors:
                                    await _write_queue.put(("snap", snap))
                                logger.info(
                                    f"  Fast anchors: "
                                    f"{len(fast_anchors)} 15M snapshots"
                                )

                            if other_tickers:
                                other_anchors = await fetch_rest_snapshots_async(
                                    other_tickers, logger
                                )
                                for snap in other_anchors:
                                    await _write_queue.put(("snap", snap))
                                logger.info(
                                    f"  Background anchors: "
                                    f"{len(other_anchors)} additional snapshots"
                                )
                        except Exception as e:
                            logger.warning(
                                f"  Background snapshot fetch failed: {e}"
                            )

                    # v7.6 UPGRADE 2: Always fetch snapshots on reconnect
                    # Previously only fetched on first connect, leaving data gaps
                    # after disconnections. Now every reconnect anchors the tape.
                    # Cooldown: skip if last fetch was < 60s ago (prevents REST spam
                    # during connection flapping).
                    snapshot_task = None
                    _now_snap = time.monotonic()
                    if _now_snap - _last_snapshot_fetch_ts > 60:
                        _last_snapshot_fetch_ts = _now_snap
                        snapshot_task = asyncio.create_task(_bg_snapshot_fetch())
                        if reconnect_kl > 1:
                            logger.info(
                                f"[KL] Reconnect gap-fill: fetching snapshots "
                                f"(attempt {reconnect_kl})"
                            )

                    # v7.6: Reset Kalshi seq tracking on reconnect to avoid
                    # false gap alerts from the sequence jump
                    tape._kl_last_seq = 0

                    # Cached refs for hot path
                    _write = tape.write_event
                    last_msg_time = time.monotonic()
                    last_predict_check = time.monotonic()
                    subscribed_set = set(tickers)

                    # v7.5: Publish shared state for periodic_loop rediscovery
                    # Protected by lock to prevent concurrent access
                    async with _subscribed_lock:
                        _subscribed_set.clear()
                        _subscribed_set.update(tickers)
                    _kl_ws_ref[0] = ws
                    # Drain any stale entries from the queue
                    while not _pending_subscribe.empty():
                        try:
                            _pending_subscribe.get_nowait()
                        except asyncio.QueueEmpty:
                            break

                    # v7 FIX: Predictive 15M subscriber
                    predictor = PredictiveSubscriber(PREDICT_SYMBOLS, logger)

                    while not shutdown.is_set():
                        try:
                            msg_raw = await asyncio.wait_for(
                                ws.recv(), timeout=KALSHI_RECEIVE_TIMEOUT
                            )
                        except asyncio.TimeoutError:
                            elapsed = time.monotonic() - last_msg_time
                            logger.warning(
                                f"[KL] RECV TIMEOUT after {elapsed:.0f}s "
                                f"(limit={KALSHI_RECEIVE_TIMEOUT}s) — reconnecting"
                            )
                            _kl_ws_ref[0] = None  # v7.3.1: clear so periodic_loop skips rediscovery
                            break

                        # ╔══ HOT PATH (v7.6: via backpressure queue) ══╗
                        try:
                            _write_queue.put_nowait(("kl", msg_raw))
                        except asyncio.QueueFull:
                            _pri = _classify_priority(msg_raw)
                            if _pri <= 1:
                                await _write_queue.put(("kl", msg_raw))
                            else:
                                _bp_dropped += 1
                        # ╚══════════════════════════════════════════════╝

                        last_msg_time = time.monotonic()

                        # ── Predictive 15M subscription (every 10s) ──
                        # Generates ticker names for upcoming 15M windows
                        # and subscribes before Kalshi creates them,
                        # retrying until confirmed.
                        if last_msg_time - last_predict_check > PREDICT_CHECK_S:
                            last_predict_check = last_msg_time
                            try:
                                now_utc = datetime.now(timezone.utc)
                                pred_tickers = predictor.get_tickers_to_subscribe(now_utc)
                                # Filter to only unsubscribed tickers
                                pred_new = [t for t in pred_tickers
                                            if t not in subscribed_set]
                                if pred_new:
                                    for i in range(0, len(pred_new), KALSHI_SUBSCRIBE_CHUNK):
                                        chunk = pred_new[i: i + KALSHI_SUBSCRIBE_CHUNK]
                                        # Use regular subscribe (same as initial)
                                        # update_subscription requires exactly 1 sid
                                        # and was failing with error code 12.
                                        await ws.send(_json_dumps_str({
                                            "id": 80000 + i,
                                            "cmd": "subscribe",
                                            "params": {
                                                "channels": KALSHI_CHANNELS,
                                                "market_tickers": chunk,
                                            },
                                        }))
                                        await asyncio.sleep(0.02)
                                    subscribed_set.update(pred_new)
                                    async with _subscribed_lock:
                                        _subscribed_set.update(pred_new)  # v7.5: lock-protected
                                    logger.info(
                                        f"[KL-PREDICT] Subscribing to "
                                        f"{len(pred_new)} predicted 15M tickers: "
                                        f"{', '.join(pred_new[:4])}"
                                    )
                            except Exception as e:
                                logger.warning(f"[KL-PREDICT] Failed: {e}")

                        # ── Subscribe pending tickers from periodic_loop ──
                        # v7.5: Drain from asyncio.Queue (thread-safe) instead of bare list.
                        _new_sub_tickers: List[str] = []
                        while not _pending_subscribe.empty():
                            try:
                                _new_sub_tickers.append(_pending_subscribe.get_nowait())
                            except asyncio.QueueEmpty:
                                break
                        if _new_sub_tickers:
                            for i in range(0, len(_new_sub_tickers), KALSHI_SUBSCRIBE_CHUNK):
                                chunk = _new_sub_tickers[i: i + KALSHI_SUBSCRIBE_CHUNK]
                                await ws.send(_json_dumps_str({
                                    "id": 90000 + i,
                                    "cmd": "subscribe",
                                    "params": {
                                        "channels": KALSHI_CHANNELS,
                                        "market_tickers": chunk,
                                    },
                                }))
                                await asyncio.sleep(0.05)
                            subscribed_set.update(_new_sub_tickers)
                            async with _subscribed_lock:
                                _subscribed_set.update(_new_sub_tickers)
                            tape._active_tickers = list(subscribed_set)
                            logger.info(
                                f"[KL-REDISCOVER] +{len(_new_sub_tickers)} new markets "
                                f"(total: {len(subscribed_set)}): "
                                f"{', '.join(_new_sub_tickers[:5])}"
                                f"{'...' if len(_new_sub_tickers) > 5 else ''}"
                            )

            except ConnectionClosed as e:
                _last_kl_disconnect_ts = time.monotonic()  # v7.6: track gap
                # Cancel any running background snapshot task
                if snapshot_task and not snapshot_task.done():
                    snapshot_task.cancel()
                _kl_ws_ref[0] = None  # v7.3: clear so periodic_loop won't use dead ws
                _consecutive_fails += 1
                # v7: Log close code + reason + session lifetime
                code = getattr(e, 'code', '?')
                reason = getattr(e, 'reason', '') or ''
                life = time.monotonic() - session_start_mono
                # v7.3.1: Backoff — 2s first, then 30s, then 60s max
                _backoff = min(2 * (2 ** _consecutive_fails), 60)
                logger.warning(
                    f"[KL] WebSocket CLOSED: code={code} "
                    f"reason='{reason}' life={life:.0f}s. "
                    f"Reconnecting in {_backoff}s..."
                )
                await asyncio.sleep(_backoff)
            except Exception as e:
                _last_kl_disconnect_ts = time.monotonic()  # v7.6: track gap
                if snapshot_task and not snapshot_task.done():
                    snapshot_task.cancel()
                _kl_ws_ref[0] = None  # v7.3: clear so periodic_loop won't use dead ws
                _consecutive_fails += 1
                # v7.3.1: Backoff — 5s first, then 30s, then 60s max
                _backoff = min(5 * (2 ** _consecutive_fails), 60)
                # Only show full traceback on first failure — after that, one-liner
                if _consecutive_fails <= 1:
                    logger.error(f"[KL] Error: {e}", exc_info=True)
                else:
                    logger.warning(
                        f"[KL] Still failing: {e}  — retry in {_backoff}s "
                        f"(fail #{_consecutive_fails})"
                    )
                await asyncio.sleep(_backoff)

    # ──────────────────────────────────────────────────────────────
    # v7.6 UPGRADE 6: Writer coroutine (drains backpressure queue)
    # ──────────────────────────────────────────────────────────────
    async def writer_loop():
        """Dedicated writer coroutine that drains the backpressure queue.

        v7.6: Decouples disk I/O from WS recv loops. Each item is a
        tuple of (src, msg_raw) for events or ("snap", snap_dict) for snapshots.
        """
        _write = tape.write_event
        _write_snap = tape.write_snapshot
        while not shutdown.is_set():
            try:
                item = await asyncio.wait_for(_write_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            src, payload = item
            if src == "snap":
                _write_snap(payload)
            else:
                _write(src, payload)

    # ──────────────────────────────────────────────────────────────
    # PERIODIC TASKS: Snapshots + Stats + Rotation
    # ──────────────────────────────────────────────────────────────
    async def periodic_loop():
        last_stats = time.monotonic()
        last_rotate = time.monotonic()
        last_disk_check = time.monotonic()   # Disk space warning
        _disk_check_interval = 300           # v7.6: Adaptive (300s default, tightens when low)
        last_retention = time.monotonic()    # v7.5: Archive retention cleanup
        last_snapshot_quarter = -1   # Track which quarter we last snapped (0,15,30,45)
        last_redisc_quarter = -1     # Track which quarter we last rediscovered

        # Event rate degradation alerting
        # Tracks a moving baseline of events/second. If the rate drops
        # below 50% of baseline for 2+ consecutive checks (60s+), warn.
        _baseline_rate: Optional[float] = None
        _low_rate_count: int = 0

        # v7.7: Prometheus delta tracking (Counters need increments, not absolutes)
        _prev_lines: int = 0   # v8.4 fix: track previous lines_written for delta-based rate
        _prev_ws_lines: int = 0  # v9.0: track previous ws_lines_written for WS-only event rate
        _prev_cb: int = 0
        _prev_kl: int = 0
        _prev_snap: int = 0
        _prev_dropped: int = 0
        _prev_cb_gaps: int = 0
        _prev_kl_gaps: int = 0
        # v8.1: Delta tracking for new institutional metrics
        _prev_dedup: int = 0
        _prev_reconnect_cb: int = 0
        _prev_reconnect_kl: int = 0
        # v8.2: Delta tracking for network metrics
        _prev_bytes: int = 0
        # v9.2: Hold last known-good rate across tape rotation so the
        # counter-wrap detection doesn't produce a false near-zero rate.
        _last_ws_rate: float = 0.0
        _last_total_rate: float = 0.0
        # v9.2: Track background snapshot task for clean shutdown
        _periodic_snap_task: Optional[asyncio.Task] = None

        while not shutdown.is_set():
            await asyncio.sleep(1.0)
            now = time.monotonic()
            now_utc = datetime.now(timezone.utc)
            elapsed_since_settle = seconds_since_last_settlement(now_utc)
            current_quarter = (now_utc.minute // 15) * 15

            # ── Market re-discovery — post-settlement window ──
            # v7.3: Moved here from recv loop so REST calls never pause WS recv.
            # Discovers new tickers, queues them for WS subscription by recv loop,
            # and fetches their initial snapshots.
            # v7.3.1: Skip when KL WebSocket is not connected — kalshi_ws_loop
            # already runs discovery on every reconnect, so running it here too
            # just doubles the API calls and log spam.
            if (_kl_ws_ref[0] is not None
                    and REDISCOVERY_OFFSET_S <= elapsed_since_settle <= SNAPSHOT_WINDOW_END_S
                    and current_quarter != last_redisc_quarter):
                last_redisc_quarter = current_quarter
                try:
                    if all_markets:
                        fresh = await asyncio.to_thread(
                            discover_all_markets, logger
                        )
                    else:
                        fresh = await asyncio.to_thread(
                            discover_crypto_markets, symbols, logger
                        )
                    # v7.5: Read _subscribed_set under lock for safe comparison
                    async with _subscribed_lock:
                        new_tickers = [t for t in fresh if t not in _subscribed_set]
                    if new_tickers:
                        # Queue for WS subscription (recv loop picks up next iteration)
                        # v7.5: Use asyncio.Queue instead of bare list
                        for _nt in new_tickers:
                            try:
                                _pending_subscribe.put_nowait(_nt)
                            except asyncio.QueueFull:
                                break  # 2000-item cap — remaining will be caught next cycle
                        kalshi_tickers[:] = fresh
                        last_tickers[:] = fresh
                        logger.info(
                            f"[KL-REDISCOVER] +{len(new_tickers)} new → queued for subscribe: "
                            f"{', '.join(new_tickers[:5])}"
                            f"{'...' if len(new_tickers) > 5 else ''}"
                        )
                        # Fetch snapshots for new tickers immediately (async)
                        try:
                            snaps = await fetch_rest_snapshots_async(
                                new_tickers, logger
                            )
                            for snap in snaps:
                                await _write_queue.put(("snap", snap))
                        except Exception:
                            pass
                    else:
                        async with _subscribed_lock:
                            _sub_count = len(_subscribed_set)
                        logger.info(
                            f"[KL-REDISCOVER] No new markets "
                            f"(still {_sub_count})"
                        )
                except Exception as e:
                    logger.warning(f"[KL-REDISCOVER] Failed: {e}")

            # ── REST orderbook snapshots — post-settlement window only ──
            # v7.3: Uses concurrent async fetcher (10 parallel → ~5s for 508 tickers).
            # Fire once per 15M cycle, 30-150s after settlement.
            if (SNAPSHOT_WINDOW_START_S <= elapsed_since_settle <= SNAPSHOT_WINDOW_END_S
                    and current_quarter != last_snapshot_quarter
                    and tape._active_tickers):
                last_snapshot_quarter = current_quarter
                settle_label = f"{now_utc.hour:02d}:{current_quarter:02d}"

                # Prepend any tickers that failed in the previous cycle for retry
                # v7.4: Drain from asyncio.Queue (thread-safe) instead of global list
                snap_tickers = list(tape._active_tickers)
                _retry_list: List[str] = []
                if _snapshot_retry_queue is not None:
                    while not _snapshot_retry_queue.empty():
                        try:
                            _retry_list.append(_snapshot_retry_queue.get_nowait())
                        except asyncio.QueueEmpty:
                            break
                if _retry_list:
                    retry_set = set(_retry_list)
                    # Put retries first, then the rest (avoiding duplicates)
                    snap_tickers = _retry_list + [
                        t for t in snap_tickers if t not in retry_set
                    ]
                    logger.info(
                        f"[SNAP] Retrying {len(_retry_list)} "
                        f"previously failed tickers"
                    )

                logger.info(
                    f"[SNAP] Post-settlement window ({settle_label}) — "
                    f"fetching {len(snap_tickers)} snapshots (background)"
                )

                # v9.2 FIX: Run snapshot fetch as a background task instead
                # of blocking the periodic loop for 30-120s.  This keeps
                # stats emission, rotation checks, and health monitoring
                # running on schedule.  The last_snapshot_quarter guard
                # (above) already prevents re-entry — the quarter won't
                # match again for 15 minutes.  Pattern copied from the
                # existing _bg_snapshot_fetch() on reconnect (line ~2380).
                async def _periodic_snap_bg(_tickers=snap_tickers,
                                             _label=settle_label):
                    """Background snapshot fetch for periodic loop."""
                    try:
                        snaps = await fetch_rest_snapshots_async(
                            _tickers, logger
                        )
                        for s in snaps:
                            await _write_queue.put(("snap", s))
                        logger.info(
                            f"[SNAP] Background fetch complete ({_label}): "
                            f"{len(snaps)}/{len(_tickers)} snapshots"
                        )
                    except asyncio.CancelledError:
                        logger.info(f"[SNAP] Background fetch cancelled ({_label})")
                    except Exception as e:
                        logger.warning(f"[SNAP] Background fetch failed: {e}")

                # Cancel any lingering previous task (defensive — shouldn't
                # happen because of the quarter guard, but safe)
                if _periodic_snap_task is not None and not _periodic_snap_task.done():
                    _periodic_snap_task.cancel()
                _periodic_snap_task = asyncio.create_task(
                    _periodic_snap_bg(),
                    name="periodic_snap_bg"
                )

            # ── Stats every 30s ──
            # v9.1 fix: Re-fetch monotonic time after potentially blocking
            # snapshot/rediscovery awaits.  Previously `now` was computed at
            # the top of the loop iteration, before the snapshot fetch which
            # can block for ~30s.  The stale `now` caused `last_stats` to be
            # set 30s in the past, so the NEXT iteration's fresh `now` was
            # >30s ahead → stats fired TWICE in 1 second.  The second dump
            # had only ~1s of events in the delta, producing a near-zero
            # rate spike visible on the Event Rate chart.
            now = time.monotonic()
            if now - last_stats > 30:
                elapsed = now - last_stats
                # v9.0 fix: Use ws_lines_written (WebSocket-only) for event rate.
                # Previously used lines_written which includes REST snapshots.
                # After a Kalshi reconnect, ~500 snapshots dump within seconds,
                # spiking the rate to ~3000 then crashing to near-zero — because
                # snapshots are batch REST fetches, not real-time events.
                # Now the "event rate" reflects only live WebSocket throughput.
                if tape.ws_lines_written >= _prev_ws_lines:
                    _ws_delta = tape.ws_lines_written - _prev_ws_lines
                    rate = _ws_delta / max(elapsed, 1)
                    _last_ws_rate = rate   # v9.2: save for rotation fallback
                else:
                    # v9.2 FIX: Counter wrapped due to tape rotation.
                    # Old code used tape.ws_lines_written (tiny, ~5 events
                    # since rotation) as the delta → near-zero rate that
                    # triggers false degradation alerts.  Instead, hold the
                    # last known-good rate for ONE cycle.  Next cycle has a
                    # normal _prev_ws_lines and computes the real rate.
                    rate = _last_ws_rate
                # v9.0: Also compute total rate (including snapshots) for the log
                if tape.lines_written >= _prev_lines:
                    _total_delta = tape.lines_written - _prev_lines
                    _total_rate = _total_delta / max(elapsed, 1)
                    _last_total_rate = _total_rate  # v9.2: save for fallback
                else:
                    # v9.2 FIX: Same rotation-wrap fix for total rate
                    _total_rate = _last_total_rate
                mb = tape.bytes_written / (1024 * 1024)
                # v7.5: Include dedup, gap, and latency stats
                dedup_note = f" dedup={tape._dedup_collisions}" if tape._dedup_collisions else ""
                gap_note = ""
                if tape.cb_gaps or tape.kl_gaps:
                    gap_note = f" GAPS: cb={tape.cb_gaps} kl={tape.kl_gaps}"
                latency_note = ""
                lat_stats = tape.get_latency_stats()
                if lat_stats:
                    p50, p95, p99 = lat_stats
                    latency_note = f" lat: p50={p50:.0f}ms p95={p95:.0f}ms p99={p99:.0f}ms"
                # v9.0: Log shows WS rate + total rate (total includes snapshots)
                _snap_note = f" (total {_total_rate:.0f})" if tape.snap_count > 0 else ""
                logger.info(
                    f"[STATS] seq={tape._seq:,} | "
                    f"{rate:.0f} evt/s{_snap_note} | "
                    f"cb={tape.cb_count} kl={tape.kl_count} "
                    f"snap={tape.snap_count} | "
                    f"{mb:.1f}MB{dedup_note}{gap_note}{latency_note}"
                )
                # v7.6: Backpressure stats (only log when non-zero to keep it clean)
                _q_depth = _write_queue.qsize()
                if _q_depth > 100 or _bp_dropped > 0:
                    logger.info(
                        f"[BACKPRESSURE] queue={_q_depth} dropped={_bp_dropped}"
                    )

                # v7.6 UPGRADE 7: Emit structured stats to JSON log
                if struct_logger:
                    _sdata: Dict[str, Any] = {
                        "seq": tape._seq,
                        "rate_evt_s": round(rate, 1),
                        "cb": tape.cb_count,
                        "kl": tape.kl_count,
                        "snap": tape.snap_count,
                        "mb": round(mb, 2),
                        "dedup": tape._dedup_collisions,
                        "cb_gaps": tape.cb_gaps,
                        "kl_gaps": tape.kl_gaps,
                    }
                    if lat_stats:
                        _sdata["lat_p50"] = round(lat_stats[0], 1)
                        _sdata["lat_p95"] = round(lat_stats[1], 1)
                        _sdata["lat_p99"] = round(lat_stats[2], 1)
                    try:
                        usage = shutil.disk_usage(str(DATA_DIR))
                        _sdata["disk_free_gb"] = round(usage.free / (1024 ** 3), 2)
                    except Exception:
                        pass
                    # Attach structured data as custom attribute
                    # Use makeLogRecord for proper attribute initialization
                    _sr = logging.makeLogRecord({
                        "name": "orion_collector.structured",
                        "levelno": logging.INFO,
                        "levelname": "INFO",
                        "msg": "[STATS]",
                        "created": time.time(),
                        "structured": _sdata,
                    })
                    struct_logger.handle(_sr)

                last_stats = now

                # ── v7.7: Update Prometheus metrics ──
                if _HAS_PROMETHEUS and _prom_enabled:
                    # Compute per-exchange deltas — handle counter wrap from tape rotation
                    # v8.7: Use current count as delta when counter wrapped (rotation)
                    _cb_evt_delta = (tape.cb_count - _prev_cb) if tape.cb_count >= _prev_cb else tape.cb_count
                    _kl_evt_delta = (tape.kl_count - _prev_kl) if tape.kl_count >= _prev_kl else tape.kl_count
                    _snap_evt_delta = (tape.snap_count - _prev_snap) if tape.snap_count >= _prev_snap else tape.snap_count
                    if _cb_evt_delta > 0:
                        PROM_EVENTS.labels(src="cb").inc(_cb_evt_delta)
                    if _kl_evt_delta > 0:
                        PROM_EVENTS.labels(src="kl").inc(_kl_evt_delta)
                    if _snap_evt_delta > 0:
                        PROM_EVENTS.labels(src="snap").inc(_snap_evt_delta)
                    PROM_RATE.set(rate)
                    # v8.2 fix: Per-exchange rate gauges for smooth chart rendering.
                    # The dashboard polls every 2s but these counters only change
                    # every 30s.  By exposing the rate as a *gauge* the chart can
                    # read it on every poll without computing stale deltas.
                    if elapsed > 0:
                        PROM_EXCHANGE_RATE.labels(exchange="cb").set(
                            round(_cb_evt_delta / elapsed, 1))
                        PROM_EXCHANGE_RATE.labels(exchange="kl").set(
                            round(_kl_evt_delta / elapsed, 1))
                    if lat_stats:
                        PROM_LATENCY.labels(percentile="p50").set(round(lat_stats[0], 1))
                        PROM_LATENCY.labels(percentile="p95").set(round(lat_stats[1], 1))
                        PROM_LATENCY.labels(percentile="p99").set(round(lat_stats[2], 1))
                    PROM_QUEUE.set(_q_depth)
                    if _bp_dropped > _prev_dropped:
                        PROM_DROPPED.inc(_bp_dropped - _prev_dropped)
                    if tape.cb_gaps > _prev_cb_gaps:
                        PROM_GAPS.labels(exchange="cb").inc(tape.cb_gaps - _prev_cb_gaps)
                    if tape.kl_gaps > _prev_kl_gaps:
                        PROM_GAPS.labels(exchange="kl").inc(tape.kl_gaps - _prev_kl_gaps)
                    PROM_SEQ.set(tape._seq)
                    PROM_UPTIME.set(time.monotonic() - _collector_start_mono)
                    PROM_TAPE_MB.set(round(tape.bytes_written / (1024 * 1024), 2))
                    try:
                        _du = shutil.disk_usage(str(DATA_DIR))
                        PROM_DISK.set(round(_du.free / (1024 ** 3), 2))
                    except Exception:
                        pass
                    # v8.1: Dedup counter — track duplicates filtered
                    if tape._dedup_collisions > _prev_dedup:
                        PROM_DEDUP.inc(tape._dedup_collisions - _prev_dedup)
                    # v8.1: Reconnect counters per exchange
                    if reconnect_cb > _prev_reconnect_cb:
                        PROM_RECONNECTS.labels(exchange="cb").inc(reconnect_cb - _prev_reconnect_cb)
                    if reconnect_kl > _prev_reconnect_kl:
                        PROM_RECONNECTS.labels(exchange="kl").inc(reconnect_kl - _prev_reconnect_kl)
                    # v8.1: Connection uptime — seconds since last disconnect (or full uptime)
                    _now_mono_prom = time.monotonic()
                    _cb_conn_up = (_now_mono_prom - _last_cb_disconnect_ts) if _last_cb_disconnect_ts > 0 else (_now_mono_prom - _collector_start_mono)
                    _kl_conn_up = (_now_mono_prom - _last_kl_disconnect_ts) if _last_kl_disconnect_ts > 0 else (_now_mono_prom - _collector_start_mono)
                    PROM_CONN_UPTIME.labels(exchange="cb").set(round(_cb_conn_up, 0))
                    PROM_CONN_UPTIME.labels(exchange="kl").set(round(_kl_conn_up, 0))
                    # v8.1: Latency histogram — 10 bins from raw samples
                    if tape._latency_samples:
                        _hist_bins = [0, 5, 10, 20, 50, 100, 200, 500, 1000, 5000]
                        _hist_labels = ["0-5", "5-10", "10-20", "20-50", "50-100", "100-200", "200-500", "500-1000", "1000-5000", "5000+"]
                        _hist_counts = [0] * len(_hist_labels)
                        for _lv in tape._latency_samples:
                            _av = abs(_lv)
                            _placed = False
                            for _bi in range(len(_hist_bins) - 1):
                                if _av < _hist_bins[_bi + 1]:
                                    _hist_counts[_bi] += 1
                                    _placed = True
                                    break
                            if not _placed:
                                _hist_counts[-1] += 1
                        for _bi, _bl in enumerate(_hist_labels):
                            PROM_LAT_HIST.labels(bucket=_bl).set(_hist_counts[_bi])

                    # v8.2: Network-level metrics — bandwidth, message sizes, WS RTT
                    _bytes_delta = tape.bytes_written - _prev_bytes
                    if _bytes_delta > 0 and elapsed > 0:
                        PROM_BYTES_SEC.set(round(_bytes_delta / elapsed, 1))
                        PROM_BYTES_TOTAL.inc(_bytes_delta)
                    else:
                        PROM_BYTES_SEC.set(0)
                    # Avg message sizes per exchange (reset after each interval)
                    if tape._cb_msg_count_size > 0:
                        PROM_MSG_SIZE.labels(exchange="cb").set(
                            round(tape._cb_msg_bytes / tape._cb_msg_count_size)
                        )
                    if tape._kl_msg_count_size > 0:
                        PROM_MSG_SIZE.labels(exchange="kl").set(
                            round(tape._kl_msg_bytes / tape._kl_msg_count_size)
                        )
                    tape._cb_msg_bytes = tape._cb_msg_count_size = 0
                    tape._kl_msg_bytes = tape._kl_msg_count_size = 0
                    # WebSocket ping RTT — only works when ping_interval is set.
                    # Coinbase has ping_interval=20 so ws.latency is available.
                    # Kalshi has ping_interval=None (disabled) so RTT = -1.
                    _cb_ws = _cb_ws_ref[0]
                    if _cb_ws is not None and hasattr(_cb_ws, 'latency'):
                        PROM_WS_RTT.labels(exchange="cb").set(
                            round(_cb_ws.latency * 1000, 1)
                        )
                    else:
                        PROM_WS_RTT.labels(exchange="cb").set(-1)
                    # Kalshi WS doesn't use ping_interval, so no RTT available
                    PROM_WS_RTT.labels(exchange="kl").set(-1)

                    _prev_lines = tape.lines_written  # v8.4 fix: delta-based rate
                    _prev_ws_lines = tape.ws_lines_written  # v9.0: WS-only delta tracking
                    _prev_cb = tape.cb_count
                    _prev_kl = tape.kl_count
                    _prev_snap = tape.snap_count
                    _prev_dropped = _bp_dropped
                    _prev_cb_gaps = tape.cb_gaps
                    _prev_kl_gaps = tape.kl_gaps
                    # v8.1: Update new delta trackers
                    _prev_dedup = tape._dedup_collisions
                    _prev_reconnect_cb = reconnect_cb
                    _prev_reconnect_kl = reconnect_kl
                    # v8.2: Update bytes delta tracker
                    _prev_bytes = tape.bytes_written

                # ── v7.6 UPGRADE 5: Per-feed health check ──
                # Detect silent subscription drops: one product stops while others active
                _now_mono = time.monotonic()
                _active = []
                _silent = []
                for pid, last_t in tape._product_last_msg.items():
                    age = _now_mono - last_t
                    if age < tape._feed_silent_s:
                        _active.append(pid)
                    elif age < 600:  # Only alert for products seen in last 10min
                        _silent.append((pid, age))
                if _silent and _active:
                    # v7.8: Group silent feeds by market prefix to avoid log spam
                    # Handles both -T (threshold) and -B (bracket) strike suffixes
                    # e.g. KXXRPD-26MAR0417-T1.5599 → KXXRPD-26MAR0417
                    # e.g. KXBTCY-27JAN0100-B22500  → KXBTCY-27JAN0100
                    import re as _re
                    _strike_re = _re.compile(r"^(.+?)[-]([TB]\d.*)$")
                    _silent_groups: Dict[str, List[str]] = {}
                    for pid, age in _silent:
                        m = _strike_re.match(pid)
                        prefix = m.group(1) if m else pid
                        if prefix not in _silent_groups:
                            _silent_groups[prefix] = []
                        _silent_groups[prefix].append(pid)
                    for prefix, pids in _silent_groups.items():
                        ages = [a for p, a in _silent if p in pids]
                        avg_age = sum(ages) / len(ages) if ages else 0
                        if len(pids) == 1:
                            logger.info(
                                f"[ALERT] {pids[0]} silent for {avg_age:.0f}s while "
                                f"{len(_active)} other feeds active (low-liquidity market)"
                            )
                        else:
                            logger.info(
                                f"[ALERT] {prefix}-* ({len(pids)} strikes) silent for "
                                f"~{avg_age:.0f}s while {len(_active)} other feeds active"
                            )
                # Log CB rates per 30s period
                _cb_rates = {
                    pid: cnt for pid, cnt in tape._product_msg_count.items()
                    if "-USD" in pid
                }
                if _cb_rates:
                    _rp = [f"{pid}={cnt}" for pid, cnt in sorted(_cb_rates.items())]
                    logger.info(f"[FEED] CB rates (30s): {', '.join(_rp)}")
                tape._product_msg_count.clear()

                # Prune stale entries from _product_last_msg to prevent memory leak
                # Products not seen in 30 minutes are expired market tickers
                _stale_cutoff = _now_mono - 1800  # 30 minutes
                _stale_pids = [pid for pid, t in tape._product_last_msg.items() if t < _stale_cutoff]
                if _stale_pids:
                    for pid in _stale_pids:
                        del tape._product_last_msg[pid]
                    logger.debug(f"[HEALTH] Pruned {len(_stale_pids)} stale product trackers")

                # ── Event rate degradation alerting ──
                # Uses exponential moving average as baseline.
                # Alerts if rate drops below 50% of baseline for 60s+.
                if rate > 1:  # Only track when receiving data
                    if _baseline_rate is None:
                        _baseline_rate = rate
                    else:
                        # Slowly update baseline (90% old + 10% new)
                        _baseline_rate = _baseline_rate * 0.9 + rate * 0.1
                        # Check for sustained degradation
                        if rate < _baseline_rate * 0.5 and _baseline_rate > 2:
                            _low_rate_count += 1
                            if _low_rate_count >= 2:  # 60s+ of low rate
                                logger.warning(
                                    f"[ALERT] Event rate degraded: "
                                    f"{rate:.0f} evt/s "
                                    f"(baseline: {_baseline_rate:.0f} evt/s, "
                                    f"{rate / _baseline_rate * 100:.0f}% of normal)"
                                )
                        else:
                            _low_rate_count = 0

            # ── Rotation check every 60s ──
            # Runs in thread pool so fsync + rename don't block the event loop
            # v7.8: Check every 10s instead of 60s — high event rates
            # can grow tapes past 200MB threshold within a single cycle
            if now - last_rotate > 10:
                await asyncio.to_thread(tape.check_rotation)
                last_rotate = now

            # ── v7.6 UPGRADE 4: Tiered disk space monitoring ──
            # Check frequency adapts based on available space:
            #   >5GB  = every 1 hour    1-5GB = every 5 min
            #   <1GB  = every 1 min     <500MB = pause writes
            if now - last_disk_check > _disk_check_interval:
                last_disk_check = now
                try:
                    usage = shutil.disk_usage(str(DATA_DIR))
                    free_gb = usage.free / (1024 ** 3)

                    if free_gb < _cfg("disk", "pause_gb", 0.5):
                        # EMERGENCY: pause writes
                        if not tape._disk_paused:
                            tape.pause_writes()
                        logger.critical(
                            f"[DISK] EMERGENCY: {free_gb:.2f}GB free -- "
                            f"writes PAUSED until space freed"
                        )
                        # Force aggressive retention cleanup (30 days)
                        await asyncio.to_thread(
                            tape.cleanup_old_archives, max_age_days=30
                        )
                        _disk_check_interval = 60

                    elif free_gb < _cfg("disk", "critical_gb", 1.0):
                        logger.warning(
                            f"[DISK] CRITICAL: {free_gb:.2f}GB free -- "
                            f"tape writes may fail soon"
                        )
                        _disk_check_interval = 60
                        if tape._disk_paused:
                            tape.resume_writes()
                        # Aggressive retention (60 days)
                        await asyncio.to_thread(
                            tape.cleanup_old_archives, max_age_days=60
                        )

                    elif free_gb < _cfg("disk", "warning_gb", 5.0):
                        logger.info(f"[DISK] Space: {free_gb:.1f}GB free")
                        _disk_check_interval = 300
                        if tape._disk_paused:
                            tape.resume_writes()

                    else:
                        _disk_check_interval = 3600
                        if tape._disk_paused:
                            tape.resume_writes()

                except Exception:
                    pass  # disk_usage can fail on network drives

            # ── v7.5: Archive retention cleanup (every 6 hours) ──
            # Deletes archives older than ARCHIVE_RETENTION_DAYS.
            if now - last_retention > ARCHIVE_CLEANUP_HOURS * 3600:
                last_retention = now
                await asyncio.to_thread(tape.cleanup_old_archives)

    # ──────────────────────────────────────────────────────────────
    # RUN ALL TASKS CONCURRENTLY
    # ──────────────────────────────────────────────────────────────
    logger.info("")
    logger.info("Starting event loop (Coinbase + Kalshi + Snapshots)...")
    logger.info("")

    # ── Task definitions: name → coroutine factory ──
    # If a task crashes, we restart it automatically instead of killing
    # everything.  This is critical for overnight runs where Kalshi may be
    # down for maintenance — Coinbase should keep collecting.
    _task_factories = {
        "coinbase": coinbase_ws_loop,
        "kalshi":   kalshi_ws_loop,
        "periodic": periodic_loop,
        "writer":   writer_loop,       # v7.6: backpressure queue writer
    }

    tasks = {
        name: asyncio.create_task(fn(), name=name)
        for name, fn in _task_factories.items()
    }

    # ── Supervisor loop: restart any crashed task ──
    while not shutdown.is_set():
        done, _ = await asyncio.wait(
            tasks.values(), return_when=asyncio.FIRST_COMPLETED, timeout=10
        )
        for task in done:
            name = task.get_name()
            if task.exception():
                import traceback as _tb
                _exc_lines = _tb.format_exception(
                    type(task.exception()), task.exception(),
                    task.exception().__traceback__
                )
                logger.error(
                    f"Task '{name}' crashed: {task.exception()}  — restarting in 10s\n"
                    + "".join(_exc_lines)
                )
                await asyncio.sleep(10)
                if not shutdown.is_set():
                    tasks[name] = asyncio.create_task(
                        _task_factories[name](), name=name
                    )
                    logger.info(f"Task '{name}' restarted")
            elif task.cancelled():
                logger.warning(f"Task '{name}' was cancelled")
            else:
                # Task finished cleanly (shouldn't happen — they loop forever)
                logger.warning(f"Task '{name}' exited cleanly — restarting in 5s")
                await asyncio.sleep(5)
                if not shutdown.is_set():
                    tasks[name] = asyncio.create_task(
                        _task_factories[name](), name=name
                    )

    # ── Clean shutdown ──
    for task in tasks.values():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # v7.6: Drain remaining queue items before closing tape
    while not _write_queue.empty():
        try:
            src, payload = _write_queue.get_nowait()
            if src == "snap":
                tape.write_snapshot(payload)
            else:
                tape.write_event(src, payload)
        except asyncio.QueueEmpty:
            break

    tape.close()
    logger.info("Collector stopped")


# ==============================================================================
# ENTRY POINT
# ==============================================================================
# ──────────────────────────────────────────────────────────────
# v7.6 UPGRADE 7: Structured JSON Log Formatter
# ──────────────────────────────────────────────────────────────
class _JsonLogFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects for machine parsing.

    v7.6: Enables post-mortem analysis, monitoring dashboards, and
    automated alerting on collector health metrics.
    Output: {"ts": "2026-03-03T20:52:42.123Z", "level": "INFO", "msg": "...", "extra": {...}}
    """
    def format(self, record: logging.LogRecord) -> str:
        entry: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        # Include structured extra dict if present
        if hasattr(record, "structured") and isinstance(record.structured, dict):
            entry["extra"] = record.structured
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = self.formatException(record.exc_info)
        try:
            return _json_dumps_str(entry)
        except Exception:
            return _json_dumps_str({"ts": entry.get("ts", ""), "level": "ERROR", "msg": "log format error"})


def setup_logging() -> Tuple[logging.Logger, Optional[logging.Logger]]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("orion_collector")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S"
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    # RotatingFileHandler: auto-rotates at 100MB, keeps 3 backups.
    # This prevents the log file from growing indefinitely on long runs.
    # Backups: orion_collector.log.1, .2, .3 (oldest -> highest number).
    fh = logging.handlers.RotatingFileHandler(
        str(LOG_FILE), mode="a", encoding="utf-8",
        maxBytes=100 * 1024 * 1024,   # 100MB per file
        backupCount=3,                 # Keep 3 rotated copies
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # v7.6 UPGRADE 7: Structured JSON log — machine-readable companion file.
    # Separate logger so JSON entries don't pollute the console/text log.
    struct_logger: Optional[logging.Logger] = None
    if _cfg("logging", "structured_enabled", True):
        _json_log_dir = LOG_DIR / "collectors" / "unified"
        _json_log_dir.mkdir(parents=True, exist_ok=True)
        _json_log_file = _json_log_dir / "collector_structured.json"
        struct_logger = logging.getLogger("orion_collector.structured")
        struct_logger.setLevel(logging.INFO)
        struct_logger.propagate = False
        struct_logger.handlers.clear()
        jh = logging.handlers.RotatingFileHandler(
            str(_json_log_file), mode="a", encoding="utf-8",
            maxBytes=_cfg("logging", "structured_max_mb", 100) * 1024 * 1024,
            backupCount=3,
        )
        jh.setLevel(logging.INFO)
        jh.setFormatter(_JsonLogFormatter())
        struct_logger.addHandler(jh)

    return logger, struct_logger


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Orion Unified Collector v7.8 (Institutional Hardening)"
    )
    parser.add_argument(
        "--symbols", type=str, default=",".join(DEFAULT_SYMBOLS),
        help="Kalshi crypto symbols (default: BTC,ETH,SOL,XRP,DOGE,SHIB)"
    )
    parser.add_argument(
        "--all-markets", action="store_true",
        help="Subscribe to ALL Kalshi markets (not just crypto)"
    )
    args = parser.parse_args()

    # ── Singleton check: only one collector can run at a time ──
    # This prevents duplicate data being written to the tape files.
    if not acquire_singleton_lock("orion_collector"):
        # Another collector is already running — refuse to start.
        # Read the PID so we can tell the user which process is running.
        _pid_path = get_pid_file_path("orion_collector")
        try:
            existing_pid = int(_pid_path.read_text().strip().split(",")[0])
        except Exception:
            existing_pid = "unknown"
        print(
            f"ERROR: Another collector is already running (PID {existing_pid}).\n"
            f"       Only one collector can run at a time to prevent duplicate tape data.\n"
            f"       To force restart: stop the existing collector first, then start a new one.\n"
            f"       PID file: {_pid_path}"
        )
        sys.exit(2)

    _load_env(ENV_FILE)

    # v5 FIX #1: Pre-load auth credentials ONCE at startup
    _init_auth()

    logger, struct_logger = setup_logging()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    mode = "ALL MARKETS" if args.all_markets else f"CRYPTO: {', '.join(symbols)}"

    # Dynamically build Coinbase product list from Kalshi symbols
    # so --symbols BTC,ETH,SOL,XRP → subscribes to BTC-USD, ETH-USD, SOL-USD, XRP-USD
    global COINBASE_PRODUCTS
    COINBASE_PRODUCTS = [f"{sym}-USD" for sym in symbols]

    logger.info("")
    logger.info("=" * 64)
    logger.info("  ORION — Unified Collector v7.8 (INSTITUTIONAL HARDENING)")
    logger.info("=" * 64)
    logger.info(f"  Mode:        {mode}")
    logger.info(f"  Coinbase:    {', '.join(COINBASE_PRODUCTS)}")
    logger.info(f"  KL channels: {', '.join(KALSHI_CHANNELS)}")
    logger.info(f"  JSON engine: {_JSON_ENGINE}")
    _snap_mode = f"aiohttp ({SNAPSHOT_CONCURRENCY}/batch, 0.5s pacing)" if _HAS_AIOHTTP else "sequential (install aiohttp for 2x speedup)"
    logger.info(f"  Snapshots:   post-settlement window ({SNAPSHOT_WINDOW_START_S}-{SNAPSHOT_WINDOW_END_S}s), {_snap_mode}")
    logger.info(f"  Rotation:    hourly + {ROTATE_AT_MB}MB safety")
    logger.info(f"  Flush:       {FLUSH_EVERY_LINES} lines (no fsync in hot path)")
    logger.info(f"  Auth:        pre-loaded (0.5ms signing)")
    logger.info(f"  Ping:        DISABLED (Kalshi ignores client pings)")
    logger.info(f"  Keepalive:   recv_timeout={KALSHI_RECEIVE_TIMEOUT}s")
    logger.info(f"  Rediscovery: post-settlement +{REDISCOVERY_OFFSET_S}s (in periodic_loop, not recv)")
    logger.info(f"  Predictor:   {', '.join(PREDICT_SYMBOLS)} (lead={PREDICT_LEAD_S}s)")
    logger.info(f"  Integrity:   CRC32 checksums on unified tape + message dedup")
    _archive_fmt = "parquet+zstd" if _HAS_PYARROW else "gzip (install pyarrow for columnar)"
    logger.info(f"  Compression: {_archive_fmt} on rotation (ThreadPool max_workers=2)")
    logger.info(f"  Gap detect:  CB threshold={CB_GAP_THRESHOLD}, KL threshold={KL_GAP_THRESHOLD}")
    _prom_label = f":{_cfg('metrics', 'port', 9090)}/metrics" if _HAS_PROMETHEUS else "disabled (pip install prometheus-client)"
    logger.info(f"  WS compress: {WS_COMPRESSION or 'disabled'}")
    logger.info(f"  Prometheus:  {_prom_label}")
    logger.info(f"  Latency:     p50/p95/p99 from Coinbase exchange timestamps")
    logger.info(f"  Retention:   {ARCHIVE_RETENTION_DAYS}d archive cleanup every {ARCHIVE_CLEANUP_HOURS}h")
    _cfg_src = "collector_config.yaml" if _COLLECTOR_CONFIG else "defaults (no config file)"
    logger.info(f"  Config:      {_cfg_src}")
    logger.info("")
    logger.info(f"  Unified tape:  {UNIFIED_TAPE}")
    logger.info(f"  Oracle legacy: {ORACLE_TAPE}")
    logger.info(f"  Kalshi legacy: {KALSHI_TAPE}")
    logger.info(f"  PID file:      {get_pid_file_path('orion_collector')} (PID {os.getpid()})")
    logger.info("=" * 64)

    try:
        asyncio.run(run_collector(symbols, args.all_markets, logger, struct_logger))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        # Always clean up the PID file on exit (normal, error, or interrupted)
        release_singleton_lock("orion_collector")
        logger.info("PID file released — collector stopped cleanly")


if __name__ == "__main__":
    main()
