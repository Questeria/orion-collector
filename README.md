# Orion Collector — Institutional-Grade Market Data Infrastructure

A production market data collection system for Kalshi prediction markets and Coinbase crypto feeds. A single-person built equivalent to collector infrastructure used by top financial institutions (based on publicly available information).

## What This Is

A unified WebSocket collector that ingests real-time market data from two exchanges simultaneously, writes it to a structured tape format with integrity guarantees, and exposes a full monitoring stack — all in one self-contained system.

**12,000+ lines** across 7 files. No external monitoring tools required (no Grafana, no Datadog, no PagerDuty). Everything is built in.

## Architecture

```
+------------------------------------------------------------+
|                    orion_collector.py                      |
|                  Unified WebSocket Engine                  |
|                                                            |
|   +------------+   +--------------+   +----------------+   |
|   | Tape       |   | Backpressure |   | Prometheus     |   |
|   | Writer     |   | Queue        |   | Metrics        |   |
|   +-----+------+   +--------------+   +-------+--------+   |
|         |                                      |           |
|  Kalshi WS ---->                               +--> :9090  |
|  Coinbase WS -->                               |           |
|         |                                      |           |
+---------+--------------------------------------+-----------+
          |
          v
+-----------------+        +-----------------------------+
| JSONL Tapes     |        | collector_dashboard.py      |
| + Archives      |<-------| REST API Server             |
| + CRC32         |        | SLA Tracking                |
+-----------------+        | Anomaly Detection           |
                           | Email Alerts                |
                           +--------------+--------------+
                                          |
                                          v
                           +-----------------------------+
                           | collector_dashboard.jsx     |
                           | React Frontend              |
                           | 7 Themes (WCAG AAA)         |
                           | Timeline DVR Replay         |
                           | Composite Health Score      |
                           | Universal Metric Panels     |
                           +-----------------------------+
```

## Collector Engine — orion_collector.py

The core data ingestion engine. Connects to Kalshi and Coinbase via WebSocket, merges both feeds onto a single sequenced tape, and exposes Prometheus metrics for external monitoring.

- **Unified tape format** — Kalshi + Coinbase on a single timestamped, sequenced JSONL stream
- **CRC32 integrity** — Every record checksummed at write time
- **Backpressure queue** — Decouples WebSocket recv from disk I/O (50,000 message buffer)
- **Deduplication** — Detects and drops duplicate messages by content hash
- **Sequence gap detection** — Catches missing messages per exchange
- **REST orderbook snapshots** — Async batched fetches with configurable concurrency
- **Predictive subscription** — Pre-subscribes to 15-minute settlement contracts before they open
- **Tape rotation** — Hourly + size-based rotation with gzip compression and 90-day retention
- **Disk space monitoring** — Tiered alerts (warning, critical, pause writes)
- **TCP keepalive** — OS-level probes prevent NAT/firewall idle-timeout disconnections
- **Prometheus metrics** — Event rates, latencies, queue depth, disk space, bandwidth, per-exchange breakdowns

## Monitoring Dashboard — collector_dashboard.py + .jsx

A full monitoring platform that replaces Grafana + Datadog + PagerDuty in a single deployment. The Python backend serves REST APIs and runs SLA/anomaly detection. The React frontend renders everything in real time.

- **Composite Health Score** — 0-100 weighted score from 5 components (freshness, rate, stability, latency, resources) with A-F letter grades
- **SLA Tracking** — Uptime percentage, MTTR (mean time to recovery), incident detection, daily uptime history. Persisted to disk with 30-day retention
- **Anomaly Detection** — Z-score based statistical outlier detection. Auto-learns baselines from rolling 30-minute windows, flags readings beyond 2σ (warning) or 3σ (alert)
- **Timeline Replay / DVR** — Scrub through up to 24 hours of historical data with play/pause/skip/speed controls (0.5x to 8x). Charts, stat cards, and feed rates all update to the playback position
- **Email Alerts** — 5 conditions (data stopped, collector down, WebSocket disconnect, disk critical, high error rate) with configurable thresholds, cooldowns, and recovery notifications
- **Universal Metric Drill-Down** — Click any of the 16 status metrics to open a full detail panel with summary stats and applicable charts
- **Chart Overlays** — 14 metrics can be overlaid on the event rate chart and 8 on the latency chart as dashed lines on a secondary Y-axis
- **7 Visual Themes** — Light, Dark, Midnight Blue, High Contrast Light/Dark, Solarized Dark, Terminal Green. WCAG AAA accessible
- **Event Inspector** — Click any point on the event rate chart to see raw tape events with sequence numbers, latency, source, and parsed message content for the current tape
- **Server-Side History** — JSONL storage with 7-day retention for long time windows. In-memory ring buffer for sub-minute queries

## Supporting Infrastructure

- **collector_watchdog.py** — Process supervisor with exponential backoff (5s to 300s), automatic restart on crash or stale tape
- **singleton_lock.py** — Cross-platform mutex preventing duplicate instances. Handles PID reuse detection via process creation timestamps
- **health_check.py** — CLI tape health monitor with CRC32 verification, sequence gap detection, and continuous watch mode with alerting
- **collector_config.yaml** — Single YAML file for all tunable constants (rotation policy, WebSocket timeouts, alert thresholds, health score weights, SLA parameters, anomaly detection settings)

## Setup

### Prerequisites

- Python 3.10+
- A Kalshi account with API access
- A Coinbase account for crypto price feeds

### Installation

```bash
# Clone the repo
git clone https://github.com/Questeria/orion-collector.git
cd orion-collector

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: .\venv\Scripts\Activate.ps1  # Windows

# Install dependencies
pip install websockets aiohttp pyyaml prometheus_client
pip install orjson  # Recommended — 5x faster JSON serialization

# Configure credentials
cp api.env.example api.env
# Edit api.env with your Kalshi API key and private key path
```

### Running

```bash
# Start the collector (BTC, ETH, SOL)
python orion_collector.py --symbols BTC,ETH,SOL

# Start with the watchdog (auto-restarts on failure)
python collector_watchdog.py --symbols BTC,ETH,SOL

# Start the monitoring dashboard (opens at http://localhost:8050)
python collector_dashboard.py

# One-shot health check
python health_check.py

# Continuous health monitoring (5-second loop with alerting)
python health_check.py --watch

# Verify tape integrity (CRC32 check on last 100 records)
python health_check.py --verify 100
```

### Configuration

All settings live in `collector_config.yaml`:

- **rotation** — Tape rotation policy (hourly + size-based)
- **websocket** — Keepalive intervals and timeouts
- **backpressure** — Queue size limits
- **alerts** — Email alert conditions and thresholds
- **health_score** — Component weights and scoring thresholds
- **sla** — Incident detection and tracking parameters
- **anomaly** — Z-score thresholds and tracked metrics

## File Inventory

| File | Lines | Size | Description |
|------|------:|-----:|-------------|
| `orion_collector.py` | 3,366 | 156 KB | Unified WebSocket collector engine |
| `collector_dashboard.jsx` | 4,976 | 268 KB | React monitoring dashboard frontend |
| `collector_dashboard.py` | 2,758 | 120 KB | Dashboard backend + SLA + anomaly detection |
| `health_check.py` | 310 | 12 KB | Tape health monitor with CRC verification |
| `collector_watchdog.py` | 241 | 12 KB | Process supervisor with exponential backoff |
| `singleton_lock.py` | 185 | 8 KB | Cross-platform duplicate instance prevention |
| `collector_config.yaml` | 183 | 8 KB | All tunable constants |
| **Total** | **12,019** | **584 KB** | |

## Design Decisions

**Why a single unified tape?** Colocating Kalshi orderbook events and Coinbase price ticks on one timestamped stream makes cross-exchange analysis easier. The `src` field on each record identifies the origin.

**Why CRC32 on every record?** Tape integrity is non-negotiable for a trading system. If a disk write is partial or corrupted, the CRC catches it immediately. The health check can verify thousands of records in seconds.

**Why built-in monitoring instead of Grafana?** Fewer moving parts. The dashboard runs alongside the collector with zero configuration. No separate Prometheus server, no Grafana instance, no alert manager. One `python collector_dashboard.py` and you have institutional-grade monitoring.

**Why z-score anomaly detection instead of fixed thresholds?** Markets have different activity levels at different times of day. A fixed threshold fails at 3am when normal rate is 10/sec. Z-scores auto-adapt to whatever "normal" looks like at any given moment.

**Why TCP keepalive instead of WebSocket pings?** Kalshi rejects application-level WebSocket pings. TCP keepalive operates below TLS at the OS socket layer — the server never sees the probes, but NAT gateways and firewalls see the connection is alive and don't drop it.
