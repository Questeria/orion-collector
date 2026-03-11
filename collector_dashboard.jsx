import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import * as d3 from "d3";

// ═══════════════════════════════════════════════════════════════
// ORION COLLECTOR DASHBOARD — Standalone Monitoring v2
// Real-time Prometheus metrics, tape health, archive browser,
// log viewer, config viewer, freshness timeline, error rate,
// process stats, gap detector, fullscreen charts, and more.
// ═══════════════════════════════════════════════════════════════

// ── Theme System (v8.4: 7 themes) ──
const THEMES = {
  light: {
    bg: "#F4F5F9", card: "#FFFFFF", cardAlt: "#FAFBFE",
    border: "#E4E8F0", borderLight: "#EEF1F6",
    text: "#111827", textSecondary: "#4B5563", textMuted: "#9CA3AF",
    blue: "#0062FF", blueSoft: "#EBF2FF",
    green: "#00C853", greenSoft: "#E8FAF0",
    red: "#EF4444", redSoft: "#FEF2F2",
    amber: "#F59E0B", amberSoft: "#FFFBEB",
    purple: "#8B5CF6", cyan: "#06B6D4",
    chartGrid: "#E4E8F0", chartGridSoft: "#F0F2F6",
    shadow: "0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04)",
    shadowLg: "0 4px 12px rgba(0,0,0,0.06)",
    headerBg: "#FFFFFF", headerBorder: "#E4E8F0",
    pillBg: "#F0F2F6", pillBorder: "#E4E8F0",
    // v8.4 additions
    isDark: false, backdropBlur: "none",
    tipUnderline: "#4A6FC9",
    tooltipBg: "#FFFFFF", tooltipText: "#1E293B", tooltipBorder: "1px solid #D1D5DB",
    tooltipLabel: "#0062FF", tooltipEqBg: "#F1F5F9",
    logoBg: "linear-gradient(135deg, #0A2540, #0E3460)", logoText: "#F1F5F9",
    configBg: "#F4F5F9", configText: "#111827", configBorder: "1px solid #E4E8F0",
    statusOnlineBg: "#E8FAF0", statusOfflineBg: "#FEF2F2",
    warnBg: "#FFFBEB", critBg: "#FEF2F2", critBgStrong: "#FEE2E2",
    autoScrollBg: "#E8FAF0", trackBg: "#EEF1F6",
    histMidGreen: "#66BB6A", histYellowGreen: "#8BC34A", histDeepOrange: "#FF9800",
  },
  dark: {
    bg: "#0a0a0f", card: "rgba(18,18,26,0.85)", cardAlt: "#14141f",
    border: "rgba(255,255,255,0.06)", borderLight: "rgba(255,255,255,0.04)",
    text: "#F1F5F9", textSecondary: "#94A3B8", textMuted: "#546178",
    blue: "#3B8BFF", blueSoft: "#1A2744",
    green: "#00ff88", greenSoft: "#0D2E23",
    red: "#ff4466", redSoft: "#2D1518",
    amber: "#ffaa00", amberSoft: "#2D2410",
    purple: "#C084FC", cyan: "#00d4ff",
    chartGrid: "rgba(255,255,255,0.06)", chartGridSoft: "rgba(255,255,255,0.03)",
    shadow: "0 2px 8px rgba(0,0,0,0.4), 0 0 1px rgba(255,255,255,0.05)",
    shadowLg: "0 8px 32px rgba(0,0,0,0.5)",
    headerBg: "rgba(14,14,20,0.95)", headerBorder: "rgba(255,255,255,0.06)",
    pillBg: "rgba(255,255,255,0.04)", pillBorder: "rgba(255,255,255,0.06)",
    // v8.4 additions
    isDark: true, backdropBlur: "blur(12px)",
    tipUnderline: "#5B6EAA",
    tooltipBg: "#1A1F2E", tooltipText: "#F1F5F9", tooltipBorder: "1px solid #2D3548",
    tooltipLabel: "#60A5FA", tooltipEqBg: "rgba(255,255,255,0.08)",
    logoBg: "linear-gradient(135deg, #0A2540, #0E3460)", logoText: "#F1F5F9",
    configBg: "#0d0d14", configText: "#c9d1d9", configBorder: "1px solid rgba(255,255,255,0.06)",
    statusOnlineBg: "#0D2E23", statusOfflineBg: "#2D1518",
    warnBg: "rgba(255,170,0,0.06)", critBg: "rgba(255,68,102,0.08)", critBgStrong: "rgba(255,68,102,0.15)",
    autoScrollBg: "rgba(0,255,136,0.1)", trackBg: "rgba(255,255,255,0.06)",
    histMidGreen: "#66BB6A", histYellowGreen: "#8BC34A", histDeepOrange: "#FF9800",
  },
  // ── Midnight Blue: deep navy, calm, professional ──
  midnightBlue: {
    bg: "#0B1121", card: "rgba(15,23,42,0.9)", cardAlt: "#111827",
    border: "rgba(99,130,190,0.15)", borderLight: "rgba(99,130,190,0.08)",
    text: "#E2E8F0", textSecondary: "#94A3B8", textMuted: "#546178",
    blue: "#60A5FA", blueSoft: "#1E3A5F",
    green: "#34D399", greenSoft: "#0D3B2E",
    red: "#F87171", redSoft: "#3B1A1A",
    amber: "#FBBF24", amberSoft: "#3B2F10",
    purple: "#A78BFA", cyan: "#67E8F9",
    chartGrid: "rgba(99,130,190,0.12)", chartGridSoft: "rgba(99,130,190,0.06)",
    shadow: "0 2px 8px rgba(0,0,0,0.35), 0 0 1px rgba(96,165,250,0.1)",
    shadowLg: "0 8px 32px rgba(0,0,0,0.45)",
    headerBg: "rgba(11,17,33,0.95)", headerBorder: "rgba(99,130,190,0.15)",
    pillBg: "rgba(99,130,190,0.08)", pillBorder: "rgba(99,130,190,0.15)",
    isDark: true, backdropBlur: "blur(12px)",
    tipUnderline: "#5B7EC2",
    tooltipBg: "#1E293B", tooltipText: "#E2E8F0", tooltipBorder: "1px solid #334155",
    tooltipLabel: "#60A5FA", tooltipEqBg: "rgba(96,165,250,0.1)",
    logoBg: "linear-gradient(135deg, #0A2540, #0E3460)", logoText: "#F1F5F9",
    configBg: "#0F172A", configText: "#CBD5E1", configBorder: "1px solid rgba(99,130,190,0.15)",
    statusOnlineBg: "#0D3B2E", statusOfflineBg: "#3B1A1A",
    warnBg: "rgba(251,191,36,0.06)", critBg: "rgba(248,113,113,0.08)", critBgStrong: "rgba(248,113,113,0.15)",
    autoScrollBg: "rgba(52,211,153,0.1)", trackBg: "rgba(99,130,190,0.1)",
    histMidGreen: "#4ADE80", histYellowGreen: "#A3E635", histDeepOrange: "#FB923C",
  },
  // ── High Contrast Light: WCAG AAA, bold vivid colors on white ──
  highContrastLight: {
    bg: "#FFFFFF", card: "#FFFFFF", cardAlt: "#F5F5F5",
    border: "#000000", borderLight: "#666666",
    text: "#000000", textSecondary: "#333333", textMuted: "#555555",
    blue: "#0000DD", blueSoft: "#DDEEFF",
    green: "#007700", greenSoft: "#DDFFDD",
    red: "#CC0000", redSoft: "#FFDDDD",
    amber: "#AA6600", amberSoft: "#FFF3DD",
    purple: "#6600CC", cyan: "#007799",
    chartGrid: "#CCCCCC", chartGridSoft: "#E5E5E5",
    shadow: "0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.08)",
    shadowLg: "0 4px 12px rgba(0,0,0,0.15)",
    headerBg: "#FFFFFF", headerBorder: "#000000",
    pillBg: "#F0F0F0", pillBorder: "#000000",
    isDark: false, backdropBlur: "none",
    tipUnderline: "#0000DD",
    tooltipBg: "#FFFFFF", tooltipText: "#000000", tooltipBorder: "2px solid #000000",
    tooltipLabel: "#0000DD", tooltipEqBg: "#F0F0F0",
    logoBg: "linear-gradient(135deg, #0A2540, #0E3460)", logoText: "#F1F5F9",
    configBg: "#F5F5F5", configText: "#000000", configBorder: "2px solid #000000",
    statusOnlineBg: "#DDFFDD", statusOfflineBg: "#FFDDDD",
    warnBg: "#FFF3DD", critBg: "#FFDDDD", critBgStrong: "#FFCCCC",
    autoScrollBg: "#DDFFDD", trackBg: "#E5E5E5",
    histMidGreen: "#009900", histYellowGreen: "#669900", histDeepOrange: "#CC6600",
  },
  // ── High Contrast Dark: WCAG AAA, bold vivid colors on black ──
  highContrastDark: {
    bg: "#000000", card: "#0A0A0A", cardAlt: "#111111",
    border: "#FFFFFF", borderLight: "#888888",
    text: "#FFFFFF", textSecondary: "#DDDDDD", textMuted: "#AAAAAA",
    blue: "#5599FF", blueSoft: "#002255",
    green: "#00FF66", greenSoft: "#003318",
    red: "#FF3333", redSoft: "#330808",
    amber: "#FFCC00", amberSoft: "#332A00",
    purple: "#BB88FF", cyan: "#00DDFF",
    chartGrid: "#444444", chartGridSoft: "#222222",
    shadow: "0 2px 8px rgba(255,255,255,0.06), 0 0 1px rgba(255,255,255,0.1)",
    shadowLg: "0 8px 32px rgba(255,255,255,0.08)",
    headerBg: "#000000", headerBorder: "#FFFFFF",
    pillBg: "#111111", pillBorder: "#FFFFFF",
    isDark: true, backdropBlur: "none",
    tipUnderline: "#5599FF",
    tooltipBg: "#111111", tooltipText: "#FFFFFF", tooltipBorder: "2px solid #FFFFFF",
    tooltipLabel: "#5599FF", tooltipEqBg: "#222222",
    logoBg: "linear-gradient(135deg, #0A2540, #0E3460)", logoText: "#F1F5F9",
    configBg: "#111111", configText: "#FFFFFF", configBorder: "2px solid #FFFFFF",
    statusOnlineBg: "#003318", statusOfflineBg: "#330808",
    warnBg: "rgba(255,204,0,0.1)", critBg: "rgba(255,51,51,0.12)", critBgStrong: "rgba(255,51,51,0.2)",
    autoScrollBg: "rgba(0,255,102,0.12)", trackBg: "#222222",
    histMidGreen: "#33FF88", histYellowGreen: "#AAFF00", histDeepOrange: "#FF9900",
  },
  // ── Solarized Dark: classic developer theme, warm teal ──
  solarizedDark: {
    bg: "#002B36", card: "rgba(0,54,66,0.9)", cardAlt: "#073642",
    border: "rgba(88,110,117,0.4)", borderLight: "rgba(88,110,117,0.2)",
    text: "#FDF6E3", textSecondary: "#93A1A1", textMuted: "#657B83",
    blue: "#268BD2", blueSoft: "#0A3048",
    green: "#859900", greenSoft: "#1A2600",
    red: "#DC322F", redSoft: "#2D0A0A",
    amber: "#B58900", amberSoft: "#2D2200",
    purple: "#6C71C4", cyan: "#2AA198",
    chartGrid: "rgba(88,110,117,0.3)", chartGridSoft: "rgba(88,110,117,0.15)",
    shadow: "0 2px 8px rgba(0,0,0,0.35), 0 0 1px rgba(42,161,152,0.1)",
    shadowLg: "0 8px 32px rgba(0,0,0,0.45)",
    headerBg: "rgba(0,43,54,0.95)", headerBorder: "rgba(88,110,117,0.4)",
    pillBg: "rgba(88,110,117,0.12)", pillBorder: "rgba(88,110,117,0.3)",
    isDark: true, backdropBlur: "blur(12px)",
    tipUnderline: "#268BD2",
    tooltipBg: "#073642", tooltipText: "#FDF6E3", tooltipBorder: "1px solid #586E75",
    tooltipLabel: "#268BD2", tooltipEqBg: "rgba(88,110,117,0.15)",
    logoBg: "linear-gradient(135deg, #0A2540, #0E3460)", logoText: "#F1F5F9",
    configBg: "#073642", configText: "#93A1A1", configBorder: "1px solid rgba(88,110,117,0.4)",
    statusOnlineBg: "#1A2600", statusOfflineBg: "#2D0A0A",
    warnBg: "rgba(181,137,0,0.06)", critBg: "rgba(220,50,47,0.08)", critBgStrong: "rgba(220,50,47,0.15)",
    autoScrollBg: "rgba(133,153,0,0.1)", trackBg: "rgba(88,110,117,0.2)",
    histMidGreen: "#859900", histYellowGreen: "#B58900", histDeepOrange: "#CB4B16",
  },
  // ── Terminal Green: hacker aesthetic, green-on-black, Matrix vibes ──
  terminalGreen: {
    bg: "#000000", card: "rgba(0,10,0,0.9)", cardAlt: "#0A0F0A",
    border: "rgba(0,255,65,0.15)", borderLight: "rgba(0,255,65,0.08)",
    text: "#00FF41", textSecondary: "#00CC33", textMuted: "#008020",
    blue: "#00AAFF", blueSoft: "#001A2D",
    green: "#00FF41", greenSoft: "#002D0B",
    red: "#FF0040", redSoft: "#2D000D",
    amber: "#FFB800", amberSoft: "#2D2000",
    purple: "#CC44FF", cyan: "#00FFCC",
    chartGrid: "rgba(0,255,65,0.1)", chartGridSoft: "rgba(0,255,65,0.05)",
    shadow: "0 2px 8px rgba(0,255,65,0.08), 0 0 1px rgba(0,255,65,0.15)",
    shadowLg: "0 8px 32px rgba(0,255,65,0.1)",
    headerBg: "rgba(0,5,0,0.95)", headerBorder: "rgba(0,255,65,0.15)",
    pillBg: "rgba(0,255,65,0.05)", pillBorder: "rgba(0,255,65,0.15)",
    isDark: true, backdropBlur: "blur(12px)",
    tipUnderline: "#00CC33",
    tooltipBg: "#0A0F0A", tooltipText: "#00FF41", tooltipBorder: "1px solid rgba(0,255,65,0.3)",
    tooltipLabel: "#00AAFF", tooltipEqBg: "rgba(0,255,65,0.06)",
    logoBg: "linear-gradient(135deg, #0A2540, #0E3460)", logoText: "#F1F5F9",
    configBg: "#0A0F0A", configText: "#00FF41", configBorder: "1px solid rgba(0,255,65,0.2)",
    statusOnlineBg: "#002D0B", statusOfflineBg: "#2D000D",
    warnBg: "rgba(255,184,0,0.06)", critBg: "rgba(255,0,64,0.08)", critBgStrong: "rgba(255,0,64,0.15)",
    autoScrollBg: "rgba(0,255,65,0.08)", trackBg: "rgba(0,255,65,0.08)",
    histMidGreen: "#00DD35", histYellowGreen: "#88DD00", histDeepOrange: "#FF8800",
  },
};

// ── Theme metadata for picker UI (v8.4) ──
const THEME_ORDER = ["light", "dark", "midnightBlue", "highContrastLight", "highContrastDark", "solarizedDark", "terminalGreen"];
const THEME_META = {
  light:             { name: "Light",                icon: "Sun",     dots: ["#F4F5F9", "#0062FF", "#00C853", "#111827"] },
  dark:              { name: "Dark",                 icon: "Moon",    dots: ["#0a0a0f", "#3B8BFF", "#00ff88", "#F1F5F9"] },
  midnightBlue:      { name: "Midnight Blue",        icon: "Night",   dots: ["#0B1121", "#60A5FA", "#34D399", "#E2E8F0"] },
  highContrastLight: { name: "HC Light",             icon: "Bright",  dots: ["#FFFFFF", "#0000DD", "#007700", "#000000"] },
  highContrastDark:  { name: "HC Dark",              icon: "Bold",    dots: ["#000000", "#5599FF", "#00FF66", "#FFFFFF"] },
  solarizedDark:     { name: "Solarized",            icon: "Warm",    dots: ["#002B36", "#268BD2", "#859900", "#FDF6E3"] },
  terminalGreen:     { name: "Terminal",             icon: "Hack",    dots: ["#000000", "#00AAFF", "#00FF41", "#00FF41"] },
};

// ── Font Loading ──
const fontLink = document.createElement("link");
fontLink.href = "https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700;800&family=Outfit:wght@300;400;500;600;700;800;900&display=swap";
fontLink.rel = "stylesheet";
document.head.appendChild(fontLink);
const FONT = "'Outfit', system-ui, sans-serif";
const MONO = "'JetBrains Mono', monospace";

// ── localStorage helpers for history persistence ──
const STORAGE_KEY = "orion_collector_dash_cache";
const STORAGE_MAX_AGE_S = 86400; // v8.2: 24 hours (server is long-term source now)

function saveToStorage(metricsHistory, freshHistory) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({
      v: 1, savedAt: Date.now() / 1000,
      mh: metricsHistory.slice(-7200),
      fh: freshHistory.slice(-7200),
    }));
  } catch { /* quota exceeded — ignore */ }
}

function loadFromStorage() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const d = JSON.parse(raw);
    if (d.v !== 1) return null;
    if (Date.now() / 1000 - d.savedAt > STORAGE_MAX_AGE_S) {
      localStorage.removeItem(STORAGE_KEY);
      return null;
    }
    return d;
  } catch { return null; }
}

// ── Downsample utility for long time windows ──
function downsample(arr, maxPts) {
  if (arr.length <= maxPts) return arr;
  const step = Math.ceil(arr.length / maxPts);
  return arr.filter((_, i) => i % step === 0);
}

// ═══════════════════════════════════════════════════════════════
// DEFINITIONS — tooltip content for metric labels
// ═══════════════════════════════════════════════════════════════
const DEFS = {
  "Events/s": { d: "Messages per second flowing through the collector from all WebSocket feeds (Coinbase + Kalshi combined).", e: "Rate = events_in_window / window_seconds" },
  "Lat. p50 (Raw)": { d: "Raw median latency from exchange timestamp to tape write. Includes clock skew between your machine and the exchange. Negative = exchange clock is ahead of yours.", e: "p50 = 50th percentile of last 30s\nSee 'True' mode in chart for bias-removed latency" },
  "Queue Depth": { d: "Messages waiting in the backpressure queue. High values mean disk I/O is slower than ingest rate. Should be 0.", e: "Queue > 1000 = backpressure alert" },
  "Disk Free": { d: "Free disk space on the tape drive. Collector pauses at 0.5 GB to prevent data loss.", e: "Green > 10 GB, Red < 5 GB" },
  "Sequence #": { d: "Latest Coinbase sequence number. Monotonically increasing. Gaps indicate dropped messages.", e: "Gap = seq_now - seq_prev - 1" },
  "Uptime": { d: "How long the collector process has been running without restart. Resets on crash or manual stop." },
  "Event Rate": { d: "Rolling history of events per second. Blue = Coinbase, Purple = Kalshi. Stacked bars show per-exchange throughput.", e: "Y-axis = events/sec, X-axis = time" },
  "Latency": { d: "Processing latency percentiles (p50 / p95 / p99) over time. Spikes indicate disk I/O contention or GC pauses.", e: "p50 = median, p95 = tail, p99 = worst case" },
  "Feed Health": { d: "Per-exchange connection status. Shows cumulative event counts and gap detections for each WebSocket feed.", e: "Events = messages received\nGaps = sequence discontinuities" },
  "Tape Status": { d: "Current tape file size and health for each feed. Progress bars show size relative to 200 MB rotation threshold.", e: "Rotates at 200 MB or on the hour\nArchive count and size shown below" },
  "Disk Monitor": { d: "Disk space gauge with tiered alerts. The ring fills as disk usage increases.", e: "Green > 50 GB\nYellow 5-50 GB\nRed < 5 GB" },
  "Tape Freshness Timeline": { d: "Visual timeline showing data freshness over time per feed. Each block is a 10-second sample. Color shows how stale the data is.", e: "Green = fresh (<10s)\nYellow = stale (10-60s)\nRed = dead (>60s)" },
  "Archive Browser": { d: "Browse archived tape files across all feeds. Shows file format (Parquet/gzip/JSONL), size in MB, date, and age.", e: "Parquet = columnar, fast reads\ngzip = compressed JSONL\nJSONL = raw text" },
  "Log Viewer": { d: "Live tail of the collector log file. Color-coded by severity level. Auto-scrolls to show newest entries.", e: "ERROR = red, WARNING = amber\nINFO = default, DEBUG = dim" },
  "Collector Config": { d: "Current contents of collector_config.yaml. Shows rotation policy, flush settings, WebSocket keepalive, and compression config." },
  "Error Rate": { d: "Errors and warnings per minute from the collector log. Red = ERROR/CRITICAL, amber = WARNING.", e: "Scans last 500KB of log file" },
  "Gap Detector": { d: "Detects periods where event rate dropped to zero, indicating data flow interruptions.", e: "Red blocks = gaps detected\nGreen = data flowing" },
  "Process": { d: "Collector process resource usage. Shows memory consumption of the running Python process.", e: "Uses PowerShell Get-WmiObject" },
  "Dedup": { d: "Total duplicate messages filtered out by the dedup engine. Prevents double-counting when exchanges resend data.", e: "Increments on exact content match\nShould be low vs total events" },
  "Reconnects": { d: "Total WebSocket reconnection attempts across all exchanges. High counts may indicate network instability.", e: "Amber alert if > 5 total\nPer-exchange breakdown in Feed Health" },
  "Alert History": { d: "Recent alerts, gaps, feed events, and disk warnings from the collector log. Color-coded by severity.", e: "Red = ALERT, Amber = GAP\nBlue = FEED, Purple = DISK" },
  "Latency Distribution": { d: "Histogram of message processing latencies across 10 time buckets. Shows how latency is distributed.", e: "Green = fast (<50ms)\nAmber = moderate\nRed = slow (>500ms)" },
  "Feed Rate Breakdown": { d: "Per-product message rates from Coinbase WebSocket. Shows which products generate the most traffic.", e: "Parsed from collector log\n30-second sampling window" },
  "System Vitals": { d: "Three stacked sparklines showing real-time trends for queue depth, disk free space, and sequence throughput.", e: "Queue > 100 = warning\nDisk < 5 GB = red\nSeq/s = messages processed" },
  "Bandwidth": { d: "Tape write throughput in bytes per second. Measures how much raw data the collector is writing to disk.", e: "Computed from bytes_written delta / 30s\nHigh values = high market activity" },
  "WS Ping": { d: "Coinbase WebSocket round-trip time and Kalshi tape freshness. Measures network latency and feed liveness.", e: "CB RTT: ping/pong round-trip\nKL Fresh: ms since last Kalshi msg\nAmber > 200ms, Red > 1000ms" },
  "Network Vitals": { d: "Three stacked sparklines showing network-level metrics: bandwidth (bytes/sec), and average message sizes per exchange.", e: "Bytes/sec = tape write rate\nCB Msg Size = avg Coinbase message\nKL Msg Size = avg Kalshi message" },
  "Event Inspector": { d: "Click any bar in the fullscreen Event Rate chart to see raw tape messages around that timestamp. Shows individual messages with source, latency, and parsed content.", e: "Reads from live tape or archive\nUp to 500 events per query\n10-second window around click point" },
  // v8.5 SLA & Health metrics
  "HEALTH": { d: "Composite health score (0-100) computed from queue depth, disk space, tape freshness, error rate, and gap count. Letter grade: A (90+), B (80+), C (70+), D (60+), F (<60).", e: "Score = weighted sum of 5 components\nA = 90-100, B = 80-89, C = 70-79\nD = 60-69, F = below 60" },
  "UPTIME": { d: "Percentage of time the collector has been operating normally (no active incidents). Tracked from the moment the dashboard started monitoring.", e: "Uptime % = (total_time - incident_time) / total_time\nGreen >= 99.5%, Amber >= 99%, Red < 99%" },
  "MTTR": { d: "Mean Time To Recovery — average duration of past incidents before the collector returned to normal operation.", e: "MTTR = sum(incident_durations) / count\nLower is better" },
  "INCIDENTS 24H": { d: "Number of detected incidents (data flow interruptions, feed disconnects, queue overflows) in the last 24 hours.", e: "7d and 30d counts shown below\nTriggered by feed_down, queue_overflow,\nor disk_critical conditions" },
  "STATUS": { d: "Current incident state. CLEAR means all feeds are healthy. INCIDENT means an active data flow interruption is in progress.", e: "Shows reason + duration when active\nGreen = CLEAR, Red = INCIDENT" },
};

// ═══════════════════════════════════════════════════════════════
// v8.6: CHART OVERLAY DEFINITIONS — metrics that can be overlaid
// on EventRateChart or LatencyChart as secondary Y-axis lines.
// Every history entry field that isn't a primary chart metric gets
// an overlay definition here.
// ═══════════════════════════════════════════════════════════════
const CHART_OVERLAYS = {
  // ── v8.5 Health Metrics ──
  health_score: {
    label: "Health Score", group: "Health", unit: "", desc: "Composite 0-100 score combining queue depth, disk space, tape freshness, error rate, and gap count. Higher = healthier.",
    format: v => Math.round(v).toString(),
    themeColor: "green", charts: ["events", "latency"],
    fixedDomain: [0, 100],  // Always show 0-100 scale for health
  },
  anomaly_count: {
    label: "Anomalies", group: "Health", unit: "", desc: "Number of active anomalies detected by the health monitor — sudden spikes in queue, latency, or gap rate.",
    format: v => Math.round(v).toString(),
    themeColor: "red", charts: ["events", "latency"],
  },
  // ── Queue & Resources ──
  queue: {
    label: "Queue Depth", group: "Resources", unit: "", desc: "Messages waiting in the backpressure queue. Should be 0. High values mean disk writes can't keep up with ingest.",
    format: v => v >= 1000 ? (v / 1000).toFixed(1) + "K" : Math.round(v).toString(),
    themeColor: "blue", charts: ["events", "latency"],
  },
  disk: {
    label: "Disk Free (GB)", group: "Resources", unit: "GB", desc: "Free disk space on the tape drive. Collector pauses at 0.5 GB to prevent data loss.",
    format: v => v.toFixed(1),
    themeColor: "cyan", charts: ["events", "latency"],
  },
  tape: {
    label: "Tape Size (MB)", group: "Resources", unit: "MB", desc: "Current active tape file size. Rotates at 200 MB or on the hour, whichever comes first.",
    format: v => v.toFixed(1),
    themeColor: "amber", charts: ["events"],
  },
  // ── Network ──
  bytesPerSec: {
    label: "Bandwidth", group: "Network", unit: "B/s", desc: "Tape write throughput — raw bytes per second being written to disk across all feeds.",
    format: v => v >= 1048576 ? (v / 1048576).toFixed(1) + " MB/s" : v >= 1024 ? (v / 1024).toFixed(0) + " KB/s" : Math.round(v) + " B/s",
    themeColor: "blue", charts: ["events", "latency"],
  },
  wsRttCb: {
    label: "WS RTT Coinbase", group: "Network", unit: "ms", desc: "WebSocket round-trip time to Coinbase. Measures raw network latency via ping/pong frames.",
    format: v => v < 0 ? "N/A" : Math.round(v) + "ms",
    themeColor: "green", charts: ["latency"],
    sentinel: -1,  // -1 means "not available"
  },
  wsRttKl: {
    label: "WS RTT Kalshi", group: "Network", unit: "ms", desc: "WebSocket round-trip time to Kalshi. Shows -1 when Kalshi ping is disabled (auth WS doesn't support it).",
    format: v => v < 0 ? "N/A" : Math.round(v) + "ms",
    themeColor: "purple", charts: ["latency"],
    sentinel: -1,  // -1 means "not available"
  },
  msgSizeCb: {
    label: "Msg Size CB", group: "Network", unit: "B", desc: "Average Coinbase WebSocket message size in bytes. Larger messages = more orderbook depth per update.",
    format: v => v >= 1024 ? (v / 1024).toFixed(1) + " KB" : Math.round(v) + " B",
    themeColor: "cyan", charts: ["events"],
  },
  msgSizeKl: {
    label: "Msg Size KL", group: "Network", unit: "B", desc: "Average Kalshi WebSocket message size in bytes. Kalshi messages are typically smaller than Coinbase.",
    format: v => v >= 1024 ? (v / 1024).toFixed(1) + " KB" : Math.round(v) + " B",
    themeColor: "amber", charts: ["events"],
  },
  // ── Freshness / Age ──
  unified_age: {
    label: "Unified Tape Age", group: "Freshness", unit: "ms", desc: "Age of the newest record in the unified tape. Low = fresh data. Spikes indicate feed interruptions.",
    format: v => v == null ? "N/A" : v >= 1000 ? (v / 1000).toFixed(1) + "s" : Math.round(v) + "ms",
    themeColor: "amber", charts: ["events", "latency"],
  },
  kalshi_age: {
    label: "Kalshi Tape Age", group: "Freshness", unit: "ms", desc: "Age of the newest Kalshi orderbook snapshot. Spikes when Kalshi WS disconnects or goes quiet.",
    format: v => v == null ? "N/A" : v >= 1000 ? (v / 1000).toFixed(1) + "s" : Math.round(v) + "ms",
    themeColor: "purple", charts: ["events"],
  },
  oracle_age: {
    label: "Oracle Tape Age", group: "Freshness", unit: "ms", desc: "Age of the newest Coinbase spot price. Normally <30s. High values = oracle feed stale or disconnected.",
    format: v => v == null ? "N/A" : v >= 1000 ? (v / 1000).toFixed(1) + "s" : Math.round(v) + "ms",
    themeColor: "blue", charts: ["events"],
  },
  // ── System ──
  uptime: {
    label: "Uptime", group: "System", unit: "s", desc: "How long the collector process has been running. Resets on crash or manual restart.",
    format: v => { const h = Math.floor(v / 3600); const m = Math.floor((v % 3600) / 60); return h > 0 ? h + "h " + m + "m" : m + "m"; },
    themeColor: "cyan", charts: ["events"],
  },
  cbGaps: {
    label: "CB Gaps", group: "Gaps", unit: "", desc: "Cumulative Coinbase sequence gaps detected. Each gap = one or more dropped messages from the exchange.",
    format: v => Math.round(v).toLocaleString(),
    themeColor: "red", charts: ["events"],
  },
  klGaps: {
    label: "KL Gaps", group: "Gaps", unit: "", desc: "Cumulative Kalshi sequence gaps detected. Gaps often occur during Kalshi WS reconnections.",
    format: v => Math.round(v).toLocaleString(),
    themeColor: "red", charts: ["events"],
  },
};

// Color palette rotation for overlays — cycles when preferred colors collide
const OVERLAY_COLOR_KEYS = ["green", "amber", "red", "cyan", "purple", "blue"];
function getOverlayColor(T, overlayKey, activeOverlays) {
  const def = CHART_OVERLAYS[overlayKey];
  if (def && def.themeColor) {
    // Use preferred color if no other active overlay is using it
    const others = activeOverlays.filter(k => k !== overlayKey);
    if (!others.some(k => CHART_OVERLAYS[k] && CHART_OVERLAYS[k].themeColor === def.themeColor)) {
      return T[def.themeColor];
    }
  }
  // Fallback: assign by position in active list
  const idx = activeOverlays.indexOf(overlayKey);
  return T[OVERLAY_COLOR_KEYS[idx % OVERLAY_COLOR_KEYS.length]];
}

// localStorage key for persisting overlay selections
const OVERLAY_STORAGE_KEY = "orion_collector_dash_overlays";


// ═══════════════════════════════════════════════════════════════
// DEFINITION TOOLTIP — hover overlay with description
// ═══════════════════════════════════════════════════════════════
function Tip({ label, children, theme, subtle }) {
  // subtle = true → hover tooltip only, no visible underline or "i" indicator.
  // Used in the SLA panel where the dashed underline looked like a distracting "-".
  const [show, setShow] = useState(false);
  const [pos, setPos] = useState({ x: 0, y: 0 });
  const ref = useRef(null);
  const T = THEMES[theme];
  const info = DEFS[label];
  const handleEnter = () => {
    if (!info) return;
    const r = ref.current?.getBoundingClientRect();
    if (r) {
      let left = r.left + r.width / 2 - 140;
      if (left < 8) left = 8;
      if (left + 280 > window.innerWidth - 8) left = window.innerWidth - 288;
      const estH = info.e ? 160 : 100;
      let top = r.bottom + 8;
      if (top + estH > window.innerHeight - 8) top = r.top - estH - 8;
      if (top < 8) top = 8;
      setPos({ x: left, y: top });
    }
    setShow(true);
  };
  if (!info) return <span>{children}</span>;
  // v8.4: Use theme properties instead of hardcoded light/dark check
  const ulColor = T.tipUnderline;
  const tipStyle = {
    background: T.tooltipBg, color: T.tooltipText, border: T.tooltipBorder,
    boxShadow: T.shadowLg, labelColor: T.tooltipLabel,
    eqBg: T.tooltipEqBg, eqText: T.textSecondary, eqBorder: `1px solid ${T.borderLight}`,
  };
  const tooltip = show ? ReactDOM.createPortal(
    <div style={{
      position: "fixed", left: pos.x, top: pos.y, width: 280,
      padding: "12px 15px", borderRadius: 10, zIndex: 999999,
      background: tipStyle.background, color: tipStyle.color,
      border: tipStyle.border, boxShadow: tipStyle.boxShadow,
      pointerEvents: "none", fontFamily: FONT, fontSize: 12, lineHeight: 1.55,
    }}>
      <div style={{ fontWeight: 800, marginBottom: 5, fontSize: 9.5, textTransform: "uppercase", letterSpacing: "0.7px", color: tipStyle.labelColor }}>{label}</div>
      <div style={{ opacity: 0.92 }}>{info.d}</div>
      {info.e && (<div style={{ marginTop: 7, padding: "5px 9px", borderRadius: 6, background: tipStyle.eqBg, fontFamily: MONO, fontSize: 10.5, color: tipStyle.eqText, whiteSpace: "pre-line", border: tipStyle.eqBorder }}>{info.e}</div>)}
    </div>, document.body
  ) : null;
  // v8.6: Clean look — hover tooltip only, no dashed underline or "i" indicator.
  return (
    <span ref={ref} onMouseEnter={handleEnter} onMouseLeave={() => setShow(false)}
      style={{ cursor: "help", display: "inline" }}>
      {children}
      {tooltip}
    </span>
  );
}

// ═══════════════════════════════════════════════════════════════
// HOOKS — data fetching with AbortController cleanup
// ═══════════════════════════════════════════════════════════════

// Hook 1: Prometheus metrics — fast polling with history buffer
function useCollectorMetrics() {
  const cached = useRef(loadFromStorage());
  const [metrics, setMetrics] = useState(null);
  const [connected, setConnected] = useState(false);
  const histRef = useRef(cached.current?.mh || []);
  const [history, setHistory] = useState(histRef.current);
  const failCount = useRef(0);
  const everConnected = useRef(false); // Don't show OFFLINE until we've connected once

  // v8.6: One-time history seed from server on mount.
  // If our localStorage cache is empty/stale but the server has pre-loaded
  // history from disk, fetch the last 60 minutes to fill charts immediately.
  const seeded = useRef(false);
  useEffect(() => {
    if (seeded.current) return;
    seeded.current = true;
    if (histRef.current.length >= 30) return; // Already have enough data
    fetch("/api/history?minutes=60")
      .then(r => r.json())
      .then(data => {
        if (data.ok && data.entries && data.entries.length > 0 && histRef.current.length < data.entries.length) {
          const serverTs = new Set(data.entries.map(e => e.ts));
          const clientOnly = histRef.current.filter(e => !serverTs.has(e.ts));
          const merged = [...data.entries, ...clientOnly].sort((a, b) => a.ts - b.ts).slice(-7200);
          histRef.current = merged;
          setHistory([...histRef.current]);
        }
      })
      .catch(() => {}); // Non-fatal
  }, []);

  // v8.9: Re-seed from server when the browser tab becomes visible again.
  // When a tab is in the background, browsers throttle timers heavily so
  // the 2-second polling loop may only run once per minute (or less).
  // When the user returns, the first new poll makes "now" jump forward,
  // leaving a gap — short time windows (1m, 5m) suddenly have only 1-2
  // entries and charts show "Waiting for data...".  Fetching the last
  // 60 minutes from the server fills that gap instantly.
  useEffect(() => {
    const onVisible = () => {
      if (document.visibilityState !== "visible") return;
      // Check if there's a gap: if the last entry is > 30s old, we were throttled
      const lastTs = histRef.current.length > 0
        ? histRef.current[histRef.current.length - 1].ts
        : 0;
      const gapSec = (Date.now() / 1000) - lastTs;
      if (gapSec < 30) return; // No significant gap — skip fetch
      fetch("/api/history?minutes=60")
        .then(r => r.json())
        .then(data => {
          if (data.ok && data.entries && data.entries.length > 0) {
            const serverTs = new Set(data.entries.map(e => Math.round(e.ts * 10)));
            const clientOnly = histRef.current.filter(e => !serverTs.has(Math.round(e.ts * 10)));
            const merged = [...data.entries, ...clientOnly].sort((a, b) => a.ts - b.ts).slice(-7200);
            histRef.current = merged;
            setHistory([...histRef.current]);
          }
        })
        .catch(() => {});
    };
    document.addEventListener("visibilitychange", onVisible);
    return () => document.removeEventListener("visibilitychange", onVisible);
  }, []);
  const abortRef = useRef(null);
  const lastPoll = useRef(Date.now()); // for auto-refresh indicator
  // v8.2 fix: Rate smoother — when the collector's exchange_rate gauges
  // aren't available (collector not restarted) or counters are stale
  // (between 30s Prometheus updates), carry forward the last known rates.
  const rateSmooth = useRef({ lastCb: 0, lastKl: 0, lastTs: 0, cbRate: 0, klRate: 0 });

  useEffect(() => {
    let active = true;
    let timer = null;
    const poll = async () => {
      let ok = false;
      try {
        // Simple fetch with 5-second timeout — no AbortController churn
        const ctrl = new AbortController();
        const tid = setTimeout(() => ctrl.abort(), 5000);
        const res = await fetch("/api/collector-metrics", { signal: ctrl.signal });
        clearTimeout(tid);
        if (!res.ok) throw new Error("fetch failed");
        const data = await res.json();
        if (!active) return;
        if (data.ok) {
          setMetrics(data); setConnected(true);
          failCount.current = 0; ok = true;
          everConnected.current = true;
          lastPoll.current = Date.now();
          // v8.2 fix: Smooth per-exchange rates — handles stale Prometheus counters
          // and missing exchange_rate gauges (collector not yet restarted).
          let cbRate = data.exchange_rate?.coinbase || 0;
          let klRate = data.exchange_rate?.kalshi || 0;
          const rs = rateSmooth.current;
          if (cbRate > 0 || klRate > 0) {
            // Collector provides gauge rates — trust them
            rs.cbRate = cbRate; rs.klRate = klRate;
            rs.lastCb = data.events_total?.coinbase || 0;
            rs.lastKl = data.events_total?.kalshi || 0;
            rs.lastTs = data.ts;
          } else {
            // No gauge data — compute from counter deltas + carry forward
            const cb = data.events_total?.coinbase || 0;
            const kl = data.events_total?.kalshi || 0;
            if (cb !== rs.lastCb || kl !== rs.lastKl) {
              if (rs.lastTs > 0) {
                // Normal: compute rate from delta / time
                const dt = Math.max(1, data.ts - rs.lastTs);
                rs.cbRate = Math.round(Math.max(0, cb - rs.lastCb) / dt * 10) / 10;
                rs.klRate = Math.round(Math.max(0, kl - rs.lastKl) / dt * 10) / 10;
              }
              // else: first observation — just record baseline
              rs.lastCb = cb; rs.lastKl = kl; rs.lastTs = data.ts;
            }
            // If no rate computed yet, split total rate by counter proportion
            if (rs.cbRate === 0 && rs.klRate === 0 && (data.event_rate || 0) > 0) {
              const total = Math.max(1, (data.events_total?.coinbase || 0) + (data.events_total?.kalshi || 0));
              cbRate = Math.round(data.event_rate * (data.events_total?.coinbase || 0) / total * 10) / 10;
              klRate = Math.round(data.event_rate * (data.events_total?.kalshi || 0) / total * 10) / 10;
            } else {
              cbRate = rs.cbRate; klRate = rs.klRate;
            }
          }
          const entry = {
            ts: data.ts, rate: data.event_rate || 0,
            p50: data.latency_ms?.p50 || 0, p95: data.latency_ms?.p95 || 0, p99: data.latency_ms?.p99 || 0,
            queue: data.queue_depth || 0, disk: data.disk_free_gb || 0,
            tape: data.tape_size_mb || 0, seq: data.seq || 0, uptime: data.uptime_seconds || 0,
            cb: data.events_total?.coinbase || 0, kl: data.events_total?.kalshi || 0,
            cbGaps: data.gaps_total?.coinbase || 0, klGaps: data.gaps_total?.kalshi || 0,
            cbRate, klRate,
            // v8.2: Network-level metrics
            bytesPerSec: data.bytes_per_sec || 0,
            msgSizeCb: data.msg_size_avg?.coinbase || 0,
            msgSizeKl: data.msg_size_avg?.kalshi || 0,
            wsRttCb: data.ws_rtt_ms?.coinbase ?? -1,
            wsRttKl: data.ws_rtt_ms?.kalshi ?? -1,
            // v8.6: Health, anomaly, and freshness — for chart overlay support
            health_score: data.health_score ?? 0,
            health_grade: data.health_grade ?? "?",
            anomaly_count: data.anomaly_count ?? 0,
            unified_age: data.unified_age ?? null,
            kalshi_age: data.kalshi_age ?? null,
            oracle_age: data.oracle_age ?? null,
            // v8.6: Latency histogram snapshot for timeline playback
            histogramMs: data.latency_histogram_ms || null,
          };
          histRef.current = [...histRef.current, entry].slice(-7200);
          setHistory([...histRef.current]);
        } else {
          failCount.current++;
          // Only show OFFLINE after we've connected at least once AND had 5 consecutive failures
          if (everConnected.current && failCount.current >= 5) setConnected(false);
        }
      } catch (e) {
        if (active && e.name !== "AbortError") {
          failCount.current++;
          if (everConnected.current && failCount.current >= 5) setConnected(false);
        }
      }
      if (active) timer = setTimeout(poll, ok ? 2000 : 3000);
    };
    poll();
    return () => { active = false; if (timer) clearTimeout(timer); };
  }, []);

  // Save to localStorage every 30s
  useEffect(() => {
    const id = setInterval(() => saveToStorage(histRef.current, []), 30000);
    return () => clearInterval(id);
  }, []);

  return { metrics, history, connected, lastPoll };
}

// Hook 2: Tape health (every 3s)
function useTapeHealth() {
  const cached = useRef(loadFromStorage());
  const [tapes, setTapes] = useState([]);
  const [connected, setConnected] = useState(false);
  const freshRef = useRef(cached.current?.fh || []);
  const [freshHistory, setFreshHistory] = useState(freshRef.current);
  const abortRef = useRef(null);
  useEffect(() => {
    let active = true; let timer = null;
    const poll = async () => {
      if (abortRef.current) abortRef.current.abort();
      const ctrl = new AbortController(); abortRef.current = ctrl;
      try {
        const res = await fetch("/api/health", { signal: ctrl.signal });
        const data = await res.json();
        if (!active) return;
        if (data.ok) {
          setTapes(data.tapes); setConnected(true);
          const entry = { ts: data.ts };
          data.tapes.forEach(t => { entry[t.label.toLowerCase()] = t.age_ms; });
          freshRef.current = [...freshRef.current, entry].slice(-7200);
          setFreshHistory([...freshRef.current]);
        }
      } catch (e) { if (active && e.name !== "AbortError") setConnected(false); }
      if (active) timer = setTimeout(poll, 2000);
    };
    poll();
    return () => { active = false; if (timer) clearTimeout(timer); if (abortRef.current) abortRef.current.abort(); };
  }, []);
  // Re-poll immediately when tab becomes visible (browser throttles bg timers)
  useEffect(() => {
    const onVisible = () => {
      if (document.visibilityState === "visible") {
        // Cancel any pending throttled timer and poll now
        if (abortRef.current) abortRef.current.abort();
        const ctrl = new AbortController(); abortRef.current = ctrl;
        fetch("/api/health", { signal: ctrl.signal })
          .then(r => r.json())
          .then(data => {
            if (data.ok) {
              setTapes(data.tapes); setConnected(true);
              const entry = { ts: data.ts };
              data.tapes.forEach(t => { entry[t.label.toLowerCase()] = t.age_ms; });
              freshRef.current = [...freshRef.current, entry].slice(-7200);
              setFreshHistory([...freshRef.current]);
            }
          })
          .catch(() => {});
      }
    };
    document.addEventListener("visibilitychange", onVisible);
    return () => document.removeEventListener("visibilitychange", onVisible);
  }, []);
  return { tapes, freshHistory, connected };
}

// Hook 3: Archive listings (every 10s)
function useArchives() {
  const [groups, setGroups] = useState({});
  const [ok, setOk] = useState(false);
  const abortRef = useRef(null);
  useEffect(() => {
    let active = true; let timer = null;
    const poll = async () => {
      if (abortRef.current) abortRef.current.abort();
      const ctrl = new AbortController(); abortRef.current = ctrl;
      try {
        const res = await fetch("/api/archives", { signal: ctrl.signal });
        const data = await res.json();
        if (!active) return;
        if (data.ok) { setGroups(data.groups); setOk(true); }
      } catch {}
      if (active) timer = setTimeout(poll, 10000);
    };
    poll();
    return () => { active = false; if (timer) clearTimeout(timer); if (abortRef.current) abortRef.current.abort(); };
  }, []);
  return { groups, ok };
}

// Hook 4: Log lines (every 1s)
function useLogs() {
  const [lines, setLines] = useState([]);
  const [ok, setOk] = useState(false);
  const abortRef = useRef(null);
  useEffect(() => {
    let active = true; let timer = null;
    const poll = async () => {
      if (abortRef.current) abortRef.current.abort();
      const ctrl = new AbortController(); abortRef.current = ctrl;
      try {
        const res = await fetch("/api/logs", { signal: ctrl.signal });
        const data = await res.json();
        if (!active) return;
        if (data.ok) { setLines(data.lines); setOk(true); } else { setOk(false); }
      } catch {}
      if (active) timer = setTimeout(poll, 1000);
    };
    poll();
    return () => { active = false; if (timer) clearTimeout(timer); if (abortRef.current) abortRef.current.abort(); };
  }, []);
  return { lines, ok };
}

// Hook 5: Config (once on mount, manual refresh)
function useConfig() {
  const [raw, setRaw] = useState("");
  const [ok, setOk] = useState(false);
  const load = useCallback(async () => {
    try {
      const res = await fetch("/api/config");
      const data = await res.json();
      if (data.ok) { setRaw(data.raw); setOk(true); } else { setOk(false); }
    } catch { setOk(false); }
  }, []);
  useEffect(() => { load(); }, [load]);
  return { raw, ok, refresh: load };
}

// Hook 6: Error rate (every 10s)
function useErrorRate() {
  const [buckets, setBuckets] = useState([]);
  const [ok, setOk] = useState(false);
  const abortRef = useRef(null);
  useEffect(() => {
    let active = true; let timer = null;
    const poll = async () => {
      if (abortRef.current) abortRef.current.abort();
      const ctrl = new AbortController(); abortRef.current = ctrl;
      try {
        const res = await fetch("/api/error-rate", { signal: ctrl.signal });
        const data = await res.json();
        if (!active) return;
        if (data.ok) { setBuckets(data.buckets); setOk(true); }
      } catch {}
      if (active) timer = setTimeout(poll, 10000);
    };
    poll();
    return () => { active = false; if (timer) clearTimeout(timer); if (abortRef.current) abortRef.current.abort(); };
  }, []);
  return { buckets, ok };
}

// Hook 7: Process stats (every 5s)
function useProcessStats() {
  const [stats, setStats] = useState(null);
  const [ok, setOk] = useState(false);
  const abortRef = useRef(null);
  useEffect(() => {
    let active = true; let timer = null;
    const poll = async () => {
      if (abortRef.current) abortRef.current.abort();
      const ctrl = new AbortController(); abortRef.current = ctrl;
      try {
        const res = await fetch("/api/process-stats", { signal: ctrl.signal });
        const data = await res.json();
        if (!active) return;
        if (data.ok && data.processes.length > 0) { setStats(data.processes[0]); setOk(true); }
      } catch {}
      if (active) timer = setTimeout(poll, 5000);
    };
    poll();
    return () => { active = false; if (timer) clearTimeout(timer); if (abortRef.current) abortRef.current.abort(); };
  }, []);
  return { stats, ok };
}

// Hook: Container width for responsive charts
function useContainerWidth(ref) {
  const [width, setWidth] = useState(1100);
  useEffect(() => {
    if (!ref.current) return;
    const obs = new ResizeObserver(entries => { setWidth(entries[0].contentRect.width); });
    obs.observe(ref.current);
    return () => obs.disconnect();
  }, []);
  return width;
}

// Hook 8: Alert history (every 10s)
function useAlertHistory() {
  const [alerts, setAlerts] = useState([]);
  const [ok, setOk] = useState(false);
  const abortRef = useRef(null);
  useEffect(() => {
    let active = true; let timer = null;
    const poll = async () => {
      if (abortRef.current) abortRef.current.abort();
      const ctrl = new AbortController(); abortRef.current = ctrl;
      try {
        const res = await fetch("/api/alert-history", { signal: ctrl.signal });
        const data = await res.json();
        if (!active) return;
        if (data.ok) { setAlerts(data.alerts || []); setOk(true); }
      } catch {}
      if (active) timer = setTimeout(poll, 10000);
    };
    poll();
    return () => { active = false; if (timer) clearTimeout(timer); if (abortRef.current) abortRef.current.abort(); };
  }, []);
  return { alerts, ok };
}

// Hook 9: Feed rates per product (every 30s)
// v8.6: Also stores a timestamped history buffer for timeline playback.
function useFeedRates() {
  const [rates, setRates] = useState([]);
  const [logTime, setLogTime] = useState("");
  const [ok, setOk] = useState(false);
  const abortRef = useRef(null);
  // v8.6: Feed rate history for timeline playback (max 240 entries = 2hr at 30s)
  const feedHistRef = useRef([]);
  const [feedHistory, setFeedHistory] = useState([]);
  useEffect(() => {
    let active = true; let timer = null;
    const poll = async () => {
      if (abortRef.current) abortRef.current.abort();
      const ctrl = new AbortController(); abortRef.current = ctrl;
      try {
        const res = await fetch("/api/feed-rates", { signal: ctrl.signal });
        const data = await res.json();
        if (!active) return;
        if (data.ok) {
          const r = data.coinbase || [];
          const lt = data.log_time || "";
          setRates(r); setLogTime(lt); setOk(true);
          // v8.6: Push snapshot into playback history buffer
          feedHistRef.current = [...feedHistRef.current, { ts: Date.now() / 1000, rates: r, logTime: lt }].slice(-240);
          setFeedHistory([...feedHistRef.current]);
        }
      } catch {}
      if (active) timer = setTimeout(poll, 30000);
    };
    poll();
    return () => { active = false; if (timer) clearTimeout(timer); if (abortRef.current) abortRef.current.abort(); };
  }, []);
  return { rates, logTime, ok, feedHistory };
}

// v8.5: SLA tracking hook — polls /api/sla every 10s
function useSLA() {
  const [sla, setSla] = useState(null);
  useEffect(() => {
    let active = true; let timer = null;
    const poll = async () => {
      try {
        const res = await fetch("/api/sla");
        const data = await res.json();
        if (active && data.ok) setSla(data);
      } catch {}
      if (active) timer = setTimeout(poll, 10000);
    };
    poll();
    return () => { active = false; if (timer) clearTimeout(timer); };
  }, []);
  return sla;
}

// v8.5: Anomaly detection hook — polls /api/anomalies every 5s
function useAnomalies() {
  const [anomalies, setAnomalies] = useState({ active: [], recent: [], count: 0 });
  useEffect(() => {
    let active = true; let timer = null;
    const poll = async () => {
      try {
        const res = await fetch("/api/anomalies");
        const data = await res.json();
        if (active && data.ok) setAnomalies(data);
      } catch {}
      if (active) timer = setTimeout(poll, 5000);
    };
    poll();
    return () => { active = false; if (timer) clearTimeout(timer); };
  }, []);
  return anomalies;
}

// v8.16: Preserve hover crosshair across chart redraws.
// When new data arrives, the chart SVG is rebuilt from scratch, destroying
// the hover overlay rect.  We store the last mouse position in a ref and
// replay a synthetic mousemove after redraw to restore the crosshair.
function _restoreHover(svg, hoverRef) {
  if (hoverRef.current) {
    requestAnimationFrame(() => {
      const overlay = svg.select("rect[fill='transparent']").node();
      if (overlay) {
        overlay.dispatchEvent(new MouseEvent("mousemove", {
          clientX: hoverRef.current[0],
          clientY: hoverRef.current[1],
          bubbles: true
        }));
      }
    });
  }
}

// v8.5: Health Score Gauge — circular arc showing 0-100 score with letter grade
// v8.11: Replaced native title tooltip with onClick for styled details panel
function HealthScoreGauge({ score, grade, components, theme, onClick }) {
  const T = THEMES[theme];
  // v8.16: Color changes with letter grade — A=green, B=yellow-green, C=amber, D=orange, F=red
  const gradeColor = grade === "A" ? T.green : grade === "B" ? "#8BC34A" :
                     grade === "C" ? T.amber : grade === "D" ? "#ff8c00" : T.red;
  const radius = 32;
  const stroke = 5;
  const circ = 2 * Math.PI * radius;
  const pct = Math.max(0, Math.min(100, score)) / 100;
  // v8.16: Full 360° circle — filled proportionally to score.
  // 100% = complete ring, 50% = half ring, 0% = empty ring (flashing red).
  // SVG starts at 12 o'clock (rotate -90deg), arc grows clockwise.
  const arcLen = circ * pct;          // how much of the circle to fill
  const gapLen = circ * (1 - pct);    // remainder is gap
  const isEmpty = score <= 0;

  return (
    <div onClick={onClick} style={{ display: "flex", alignItems: "center", gap: 8, cursor: onClick ? "pointer" : "help" }}>
      <svg width={radius * 2 + stroke * 2} height={radius * 2 + stroke * 2}
           style={{ transform: "rotate(-90deg)" }}>
        {/* Background ring — always full 360° in border color */}
        <circle cx={radius + stroke} cy={radius + stroke} r={radius}
                fill="none" stroke={T.border} strokeWidth={stroke} />
        {/* Score arc — proportional to health % */}
        {!isEmpty && <circle cx={radius + stroke} cy={radius + stroke} r={radius}
                fill="none" stroke={gradeColor} strokeWidth={stroke}
                strokeDasharray={`${arcLen} ${gapLen}`}
                strokeLinecap="round"
                style={{ transition: "stroke-dasharray 0.8s ease, stroke 0.3s" }} />}
        {/* At 0%: empty ring pulses red */}
        {isEmpty && <circle cx={radius + stroke} cy={radius + stroke} r={radius}
                fill="none" stroke={T.red} strokeWidth={stroke}
                opacity={0.7}
                style={{ animation: "pulse 1s ease-in-out infinite alternate" }} />}
      </svg>
      {/* Score text overlaid on the gauge */}
      <div style={{ marginLeft: -(radius * 2 + stroke * 2 + 8), width: radius * 2 + stroke * 2,
                    textAlign: "center", position: "relative", zIndex: 1 }}>
        <div style={{ fontFamily: MONO, fontSize: 18, fontWeight: 800, color: isEmpty ? T.red : gradeColor, lineHeight: 1,
                      animation: isEmpty ? "pulse 1s ease-in-out infinite alternate" : "none" }}>
          {score}
        </div>
        <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, marginTop: 1 }}>
          {grade}
        </div>
      </div>
    </div>
  );
}

// v8.5: SLA Stats Row — uptime %, MTTR, incidents, health score in a single banner
// v8.11: Added onIncidentsClick and onHealthClick for interactive detail panels
function SLAPanel({ sla, healthScore, healthGrade, healthComponents, theme, onIncidentsClick, onHealthClick, onStatusClick, onUptimeClick, onMttrClick }) {
  const T = THEMES[theme];
  if (!sla) return null;

  const uptimeColor = sla.uptime_pct >= 99.5 ? T.green :
                      sla.uptime_pct >= 99.0 ? T.amber : T.red;
  const mttrStr = sla.mttr_s > 0 ? `${sla.mttr_s.toFixed(0)}s` : "\u2014";

  return (
    <div style={{
      display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr 1fr", gap: 8, marginBottom: 16,
    }}>
      {/* Health Score Gauge — v8.11: clickable */}
      <div onClick={onHealthClick} style={{
        background: T.card, borderRadius: 13, padding: "12px 16px",
        border: `1px solid ${T.border}`, boxShadow: T.shadow,
        display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
        cursor: onHealthClick ? "pointer" : "default", transition: "border-color 0.2s",
      }} onMouseEnter={e => { if (onHealthClick) e.currentTarget.style.borderColor = T.blue; }}
         onMouseLeave={e => { e.currentTarget.style.borderColor = T.border; }}>
        <HealthScoreGauge score={healthScore} grade={healthGrade} components={healthComponents} theme={theme} onClick={onHealthClick} />
        <div>
          <Tip label="HEALTH" theme={theme} subtle><div style={{ fontFamily: FONT, fontSize: 9, fontWeight: 600, color: T.textMuted, textTransform: "uppercase", letterSpacing: "0.4px" }}>HEALTH</div></Tip>
          <div style={{ fontFamily: MONO, fontSize: 14, fontWeight: 800, color: T.text }}>{healthScore}/100</div>
        </div>
      </div>

      {/* Uptime % — v9.1: clickable for uptime detail */}
      <div onClick={onUptimeClick} style={{
        background: T.card, borderRadius: 13, padding: "12px 16px",
        border: `1px solid ${T.border}`, boxShadow: T.shadow, textAlign: "center",
        cursor: onUptimeClick ? "pointer" : "default", transition: "border-color 0.2s",
      }} onMouseEnter={e => { if (onUptimeClick) e.currentTarget.style.borderColor = T.blue; }}
         onMouseLeave={e => { e.currentTarget.style.borderColor = T.border; }}>
        <Tip label="UPTIME" theme={theme} subtle><div style={{ fontFamily: FONT, fontSize: 9, fontWeight: 600, color: T.textMuted, textTransform: "uppercase", letterSpacing: "0.4px", marginBottom: 4 }}>UPTIME</div></Tip>
        <div style={{ fontFamily: MONO, fontSize: 22, fontWeight: 800, color: uptimeColor }}>{sla.uptime_pct.toFixed(2)}%</div>
        <div style={{ fontFamily: MONO, fontSize: 8, color: T.textMuted, marginTop: 2 }}>{sla.tracking_since_hours}h tracked</div>
      </div>

      {/* MTTR — v9.1: clickable for recovery detail */}
      <div onClick={onMttrClick} style={{
        background: T.card, borderRadius: 13, padding: "12px 16px",
        border: `1px solid ${T.border}`, boxShadow: T.shadow, textAlign: "center",
        cursor: onMttrClick ? "pointer" : "default", transition: "border-color 0.2s",
      }} onMouseEnter={e => { if (onMttrClick) e.currentTarget.style.borderColor = T.blue; }}
         onMouseLeave={e => { e.currentTarget.style.borderColor = T.border; }}>
        <Tip label="MTTR" theme={theme} subtle><div style={{ fontFamily: FONT, fontSize: 9, fontWeight: 600, color: T.textMuted, textTransform: "uppercase", letterSpacing: "0.4px", marginBottom: 4 }}>MTTR</div></Tip>
        <div style={{ fontFamily: MONO, fontSize: 22, fontWeight: 800, color: T.cyan }}>{mttrStr}</div>
        <div style={{ fontFamily: MONO, fontSize: 8, color: T.textMuted, marginTop: 2 }}>avg recovery</div>
      </div>

      {/* Incidents 24h — v8.11: clickable for incident history */}
      <div onClick={onIncidentsClick} style={{
        background: T.card, borderRadius: 13, padding: "12px 16px",
        border: `1px solid ${T.border}`, boxShadow: T.shadow, textAlign: "center",
        cursor: onIncidentsClick ? "pointer" : "default", transition: "border-color 0.2s",
      }} onMouseEnter={e => { if (onIncidentsClick) e.currentTarget.style.borderColor = T.blue; }}
         onMouseLeave={e => { e.currentTarget.style.borderColor = T.border; }}>
        <Tip label="INCIDENTS 24H" theme={theme} subtle><div style={{ fontFamily: FONT, fontSize: 9, fontWeight: 600, color: T.textMuted, textTransform: "uppercase", letterSpacing: "0.4px", marginBottom: 4 }}>INCIDENTS 24H</div></Tip>
        <div style={{ fontFamily: MONO, fontSize: 22, fontWeight: 800, color: sla.incidents_24h > 0 ? T.amber : T.green }}>{sla.incidents_24h}</div>
        <div style={{ fontFamily: MONO, fontSize: 8, color: T.textMuted, marginTop: 2 }}>{sla.incidents_7d} / 7d &middot; {sla.incidents_30d} / 30d</div>
      </div>

      {/* Active Incident Indicator — v8.12: clickable for status detail */}
      <div onClick={onStatusClick} style={{
        background: sla.active_incident ? T.statusOfflineBg : T.card,
        borderRadius: 13, padding: "12px 16px",
        border: `1px solid ${sla.active_incident ? T.red + "40" : T.border}`,
        boxShadow: T.shadow, textAlign: "center",
        cursor: onStatusClick ? "pointer" : "default", transition: "border-color 0.2s",
      }} onMouseEnter={e => { if (onStatusClick) e.currentTarget.style.borderColor = sla.active_incident ? T.red : T.blue; }}
         onMouseLeave={e => { e.currentTarget.style.borderColor = sla.active_incident ? T.red + "40" : T.border; }}>
        <Tip label="STATUS" theme={theme} subtle><div style={{ fontFamily: FONT, fontSize: 9, fontWeight: 600, color: T.textMuted, textTransform: "uppercase", letterSpacing: "0.4px", marginBottom: 4 }}>STATUS</div></Tip>
        {sla.active_incident ? (
          <>
            <div style={{ fontFamily: MONO, fontSize: 16, fontWeight: 800, color: T.red, animation: "pulse 1s infinite" }}>INCIDENT</div>
            <div style={{ fontFamily: MONO, fontSize: 8, color: T.red, marginTop: 2 }}>{sla.active_incident.reason} &middot; {sla.active_incident.duration_s.toFixed(0)}s</div>
          </>
        ) : (
          <>
            <div style={{ fontFamily: MONO, fontSize: 16, fontWeight: 800, color: T.green }}>CLEAR</div>
            <div style={{ fontFamily: MONO, fontSize: 8, color: T.textMuted, marginTop: 2 }}>no active incidents</div>
          </>
        )}
      </div>
    </div>
  );
}

// v8.8: Playback source — always fetches the maximum server history (24h)
// once on mount and refreshes every 60s.  This is independent of the selected
// time window so the timeline slider always has the full dataset to scrub through.
// v8.9: Also re-fetches when the tab becomes visible again after being in the
// background (browser throttling can cause stale playback data).
function usePlaybackSource() {
  const [entries, setEntries] = useState([]);
  const doFetch = useRef(null);
  useEffect(() => {
    let active = true;
    let timer = null;
    const poll = () => {
      fetch("/api/history?minutes=1440")  // 24 hours
        .then(r => r.json())
        .then(data => {
          if (active && data.ok && data.entries && data.entries.length > 0) {
            setEntries(data.entries);
          }
        })
        .catch(() => {});
      if (active) timer = setTimeout(poll, 60000);  // Refresh every 60s
    };
    doFetch.current = poll;  // expose for visibility handler
    poll();
    return () => { active = false; if (timer) clearTimeout(timer); };
  }, []);
  // Re-fetch when tab becomes visible
  useEffect(() => {
    const onVisible = () => {
      if (document.visibilityState === "visible" && doFetch.current) doFetch.current();
    };
    document.addEventListener("visibilitychange", onVisible);
    return () => document.removeEventListener("visibilitychange", onVisible);
  }, []);
  return entries;
}

// v8.2: Server-side history hook — fetches from /api/history for long time windows
function useServerHistory(timeWindow) {
  const [serverHistory, setServerHistory] = useState([]);
  const [serverFreshness, setServerFreshness] = useState([]);
  const [loading, setLoading] = useState(false);
  const abortRef = useRef(null);

  useEffect(() => {
    // Only fetch from server for windows > 1 hour
    if (timeWindow <= 3600) {
      setServerHistory([]);
      setServerFreshness([]);
      setLoading(false);
      return;
    }

    let active = true;
    let timer = null;
    const minutes = Math.ceil(timeWindow / 60);

    const poll = async () => {
      if (abortRef.current) abortRef.current.abort();
      const ctrl = new AbortController();
      abortRef.current = ctrl;
      setLoading(true);
      try {
        // Fetch history and freshness in parallel
        const [histRes, freshRes] = await Promise.all([
          fetch(`/api/history?minutes=${minutes}`, { signal: ctrl.signal }),
          fetch(`/api/freshness-history?minutes=${minutes}`, { signal: ctrl.signal }),
        ]);
        const histData = await histRes.json();
        const freshData = await freshRes.json();
        if (!active) return;
        if (histData.ok) setServerHistory(histData.entries || []);
        if (freshData.ok) setServerFreshness(freshData.entries || []);
      } catch {}
      if (active) { setLoading(false); timer = setTimeout(poll, 30000); }
    };
    poll();
    return () => { active = false; if (timer) clearTimeout(timer); if (abortRef.current) abortRef.current.abort(); };
  }, [timeWindow]);

  return { serverHistory, serverFreshness, loading };
}

// ═══════════════════════════════════════════════════════════════
// COMPONENTS — stat card, charts, panels
// ═══════════════════════════════════════════════════════════════

// 1. Stat card with hover + glow + alert badge
function StatCard({ label, value, sub, color, icon, theme, glow, alert, anomaly, onClick }) {
  const T = THEMES[theme];
  const [hov, setHov] = useState(false);
  // v8.5: anomaly prop = "warning" or "alert" or falsy — adds orange/red glow
  const anomalyBorder = anomaly === "alert" ? T.red : anomaly === "warning" ? T.amber : null;
  const anomalyAnim = anomaly ? "anomalyGlow 1.5s infinite" : (glow && T.isDark ? "glowPulse 3s infinite" : "none");
  return (<div onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)} onClick={onClick} style={{
    background: T.card, borderRadius: 13, padding: "14px 16px", position: "relative",
    border: `1px solid ${anomalyBorder || (hov && onClick ? T.blue : T.border)}`,
    flex: "1 1 0", boxShadow: hov ? T.shadowLg : T.shadow, minWidth: 120,
    transition: "all 0.25s ease", transform: hov ? "translateY(-2px)" : "none",
    cursor: onClick ? "pointer" : "default",
    backdropFilter: T.backdropBlur, WebkitBackdropFilter: T.backdropBlur,
    animation: anomalyAnim,
  }}>
    {alert && <div style={{
      position: "absolute", top: -4, right: -4, width: 14, height: 14, borderRadius: "50%",
      background: T.red, display: "flex", alignItems: "center", justifyContent: "center",
      fontSize: 8, fontWeight: 800, color: "#fff", animation: "pulse 1s infinite", zIndex: 2,
    }}>!</div>}
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 6, marginBottom: 6 }}>
      {icon && <span style={{ fontSize: 14 }}>{icon}</span>}
      <Tip label={label} theme={theme}><span style={{ fontFamily: FONT, fontSize: 10, fontWeight: 600, color: T.textMuted, letterSpacing: "0.4px", textTransform: "uppercase" }}>{label}</span></Tip>
    </div>
    <div style={{ fontFamily: MONO, fontSize: 22, fontWeight: 800, color: color || T.text, letterSpacing: "-0.5px", textAlign: "center", whiteSpace: "nowrap" }}>{value}</div>
    {sub && <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, marginTop: 3, textAlign: "center" }}>{sub}</div>}
  </div>);
}

// Time window selector pills
function TimeWindowSelector({ value, onChange, theme }) {
  const T = THEMES[theme];
  const opts = [{ label: "1m", s: 60 }, { label: "5m", s: 300 }, { label: "15m", s: 900 }, { label: "30m", s: 1800 }, { label: "1h", s: 3600 }, { label: "6h", s: 21600 }, { label: "24h", s: 86400 }, { label: "7d", s: 604800 }];
  return (<div style={{ display: "flex", gap: 2, background: T.pillBg, borderRadius: 6, padding: 2, border: `1px solid ${T.pillBorder}` }}>
    {opts.map(o => (<button key={o.label} onClick={() => onChange(o.s)} style={{
      fontFamily: MONO, fontSize: 9, fontWeight: value === o.s ? 700 : 500,
      padding: "2px 8px", borderRadius: 4, border: "none", cursor: "pointer",
      background: value === o.s ? T.blue : "transparent",
      color: value === o.s ? "#fff" : T.textMuted,
    }}>{o.label}</button>))}
  </div>);
}

// ══════════════════════════════════════════════════════════════════
// v8.6: TIMELINE PLAYBACK — scrub and replay historical chart data
// ══════════════════════════════════════════════════════════════════
// Provides a slider + transport controls (play/pause, speed, skip, live).
// The parent passes filteredHistory and receives playback state via callback.
// When mode != "live", charts show data sliced to the playback position.
function TimelinePlayback({ history, feedHistory, theme, width, onPlaybackState }) {
  const T = THEMES[theme];
  const [mode, setMode] = useState("live");      // "live" | "paused" | "playing"
  const [index, setIndex] = useState(0);          // position in history array
  const [speed, setSpeed] = useState(1);           // playback multiplier
  const intervalRef = useRef(null);
  const len = history.length;
  // v8.7 fix: Use a ref for len inside the interval callback so changing
  // history length doesn't recreate the interval (which killed speed control).
  const lenRef = useRef(len);
  useEffect(() => { lenRef.current = len; }, [len]);

  // Keep index in bounds when history changes
  useEffect(() => {
    if (mode === "live") setIndex(Math.max(0, len - 1));
    else if (index >= len) setIndex(Math.max(0, len - 1));
  }, [len, mode]);

  // Playback timer — advance index when playing
  // v8.7 fix: removed `len` from dependency array. Previously, every new poll
  // entry changed `len`, which recreated the interval every 2 seconds — making
  // all speed settings feel identical (the interval never completed a tick at
  // slower speeds). Now the interval only recreates on mode/speed changes.
  // The callback reads lenRef.current for the latest length.
  useEffect(() => {
    if (mode !== "playing") {
      if (intervalRef.current) { clearInterval(intervalRef.current); intervalRef.current = null; }
      return;
    }
    // Base: 2s per entry (matches poll rate). At 4x → 500ms steps.
    const ms = Math.max(50, 2000 / speed);
    intervalRef.current = setInterval(() => {
      setIndex(prev => {
        const curLen = lenRef.current;
        const next = prev + 1;
        if (next >= curLen) {
          // v8.13 fix: Clear interval immediately before state transition.
          // Previously the interval could fire one more tick between setMode
          // and the effect cleanup, causing a brief flash/glitch.
          if (intervalRef.current) { clearInterval(intervalRef.current); intervalRef.current = null; }
          setMode("live");
          setSpeed(1);
          return Math.max(0, curLen - 1);
        }
        return next;
      });
    }, ms);
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [mode, speed]);

  // Notify parent whenever playback state changes
  useEffect(() => {
    if (!onPlaybackState || len === 0) return;
    const safeIdx = Math.max(0, Math.min(index, len - 1));
    const entry = history[safeIdx];
    // Find nearest feed history snapshot by timestamp
    let feedSnapshot = null;
    if (entry && feedHistory && feedHistory.length > 0) {
      let best = feedHistory[0], bestDist = Math.abs(feedHistory[0].ts - entry.ts);
      for (let i = 1; i < feedHistory.length; i++) {
        const dist = Math.abs(feedHistory[i].ts - entry.ts);
        if (dist < bestDist) { bestDist = dist; best = feedHistory[i]; }
      }
      // Only use if within 60 seconds of target
      if (bestDist < 60) feedSnapshot = best;
    }
    onPlaybackState({
      mode,
      index: safeIdx,
      histogramMs: entry?.histogramMs || null,
      feedSnapshot,
    });
  }, [mode, index, len]);

  // Handler helpers
  // v8.13 fix: Reset speed to 1x when entering live mode — prevents stale
  // speed from carrying over to the next playback session.
  const goLive = () => { setMode("live"); setIndex(Math.max(0, len - 1)); setSpeed(1); };
  const togglePlay = () => {
    if (mode === "live") {
      // Exit live → start paused at current position
      // v8.13 fix: Reset speed to 1x so playback starts at normal speed.
      // Previously, changing speed while live (which has no effect) would
      // silently carry into the next playback session.
      setMode("paused");
      setIndex(Math.max(0, len - 1));
      setSpeed(1);
    } else if (mode === "paused") {
      setMode("playing");
    } else {
      setMode("paused");
    }
  };
  const skipBack = () => {
    if (mode === "live") { setMode("paused"); setIndex(Math.max(0, len - 31)); setSpeed(1); }
    else setIndex(prev => Math.max(0, prev - 30));
  };
  const skipFwd = () => {
    if (mode === "live") return;
    const next = index + 30;
    if (next >= len - 1) goLive();
    else setIndex(next);
  };
  const onSlider = (e) => {
    const v = parseInt(e.target.value, 10);
    if (v >= len - 1) { goLive(); return; }
    if (mode === "live") setMode("paused");
    setIndex(v);
  };

  // Current timestamp label
  const safeIdx = Math.max(0, Math.min(index, len - 1));
  const curEntry = history[safeIdx];
  const timeLabel = curEntry?.ts
    ? new Date(curEntry.ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
    : "--:--:--";

  // Progress fraction for slider fill
  const pct = len > 1 ? (safeIdx / (len - 1)) * 100 : 0;

  // Speed options
  const speeds = [0.5, 1, 2, 4, 8];

  // Button base style
  const btn = (active) => ({
    background: active ? T.blue : "transparent", color: active ? "#fff" : T.textMuted,
    border: `1px solid ${active ? T.blue : T.border}`, borderRadius: 5,
    padding: "3px 7px", cursor: "pointer", fontFamily: MONO, fontSize: 10,
    fontWeight: 600, lineHeight: 1, display: "flex", alignItems: "center", justifyContent: "center",
  });
  const isLive = mode === "live";
  const isPlaying = mode === "playing";

  // Unique ID for slider CSS (no build step — inject a <style> tag)
  const sliderId = "orion-pb-slider";

  return (
    <div style={{
      display: "flex", alignItems: "center", gap: 8,
      background: T.card, borderRadius: 10, padding: "6px 14px",
      border: `1px solid ${T.border}`, boxShadow: T.shadow,
      marginBottom: 12, width: width || "100%", boxSizing: "border-box",
    }}>
      {/* Inject slider track styling (CSS pseudo-elements need a <style> tag) */}
      <style>{`
        #${sliderId} { -webkit-appearance: none; appearance: none; height: 6px; border-radius: 3px; outline: none; cursor: pointer; flex: 1; min-width: 80px; }
        #${sliderId}::-webkit-slider-thumb { -webkit-appearance: none; width: 14px; height: 14px; border-radius: 50%; background: ${T.blue}; border: 2px solid ${T.card}; box-shadow: 0 0 3px rgba(0,0,0,0.3); cursor: pointer; margin-top: -4px; }
        #${sliderId}::-moz-range-thumb { width: 14px; height: 14px; border-radius: 50%; background: ${T.blue}; border: 2px solid ${T.card}; cursor: pointer; }
        #${sliderId}::-webkit-slider-runnable-track { height: 6px; border-radius: 3px; }
        #${sliderId}::-moz-range-track { height: 6px; border-radius: 3px; background: ${T.border}; }
      `}</style>

      {/* Transport: skip back / play-pause / skip forward */}
      <div style={{ display: "flex", gap: 3, flexShrink: 0 }}>
        <button onClick={skipBack} style={btn(false)} title="Skip back ~1 min">⏪</button>
        <button onClick={togglePlay} style={btn(isPlaying)} title={isPlaying ? "Pause" : "Play"}>
          {isPlaying ? "⏸" : "▶"}
        </button>
        <button onClick={skipFwd} style={btn(false)} title="Skip forward ~1 min">⏩</button>
      </div>

      {/* Speed pills */}
      <div style={{ display: "flex", gap: 2, flexShrink: 0 }}>
        {speeds.map(s => (
          <button key={s} onClick={() => setSpeed(s)} style={{
            fontFamily: MONO, fontSize: 9, fontWeight: speed === s ? 700 : 500,
            padding: "2px 6px", borderRadius: 4, border: "none", cursor: "pointer",
            background: speed === s ? (T.blue + "22") : "transparent",
            color: speed === s ? T.blue : T.textMuted,
          }}>{s}x</button>
        ))}
      </div>

      {/* Slider — fills remaining space */}
      <input
        id={sliderId}
        type="range"
        min={0}
        max={Math.max(0, len - 1)}
        value={safeIdx}
        onChange={onSlider}
        style={{
          flex: 1, minWidth: 80,
          background: `linear-gradient(to right, ${T.blue} 0%, ${T.blue} ${pct}%, ${T.border} ${pct}%, ${T.border} 100%)`,
        }}
      />

      {/* Timestamp */}
      <span style={{
        fontFamily: MONO, fontSize: 10, fontWeight: 600, color: isLive ? T.textMuted : T.text,
        whiteSpace: "nowrap", flexShrink: 0, minWidth: 68, textAlign: "center",
      }}>{timeLabel}</span>

      {/* LIVE button */}
      <button onClick={goLive} style={{
        fontFamily: MONO, fontSize: 9, fontWeight: 800,
        padding: "3px 10px", borderRadius: 5, cursor: "pointer",
        background: isLive ? T.green + "20" : "transparent",
        color: isLive ? T.green : T.textMuted,
        border: `1px solid ${isLive ? T.green + "60" : T.border}`,
        display: "flex", alignItems: "center", gap: 4, flexShrink: 0,
      }}>
        <span style={{
          width: 6, height: 6, borderRadius: "50%",
          background: isLive ? T.green : T.textMuted,
          boxShadow: isLive ? `0 0 6px ${T.green}` : "none",
        }} />
        LIVE
      </button>
    </div>
  );
}

// v8.6: Overlay picker — dropdown multi-select for chart overlay metrics
function OverlayPicker({ chartId, activeOverlays, onChange, theme }) {
  const T = THEMES[theme];
  const [open, setOpen] = useState(false);
  const btnRef = useRef(null);
  const dropRef = useRef(null);
  const [pos, setPos] = useState({ x: 0, y: 0, openUp: false });

  // Calculate dropdown position from button rect (portal-based, avoids clipping)
  useEffect(() => {
    if (!open || !btnRef.current) return;
    const r = btnRef.current.getBoundingClientRect();
    const dropH = 460; // estimated max height
    const spaceBelow = window.innerHeight - r.bottom - 12;
    const openUp = spaceBelow < dropH && r.top > spaceBelow;
    let left = r.right - 320; // right-align the dropdown
    if (left < 8) left = 8;
    if (left + 320 > window.innerWidth - 8) left = window.innerWidth - 328;
    setPos({
      x: left,
      y: openUp ? r.top : r.bottom + 6,
      openUp,
    });
  }, [open]);

  // Close on click outside the dropdown
  useEffect(() => {
    if (!open) return;
    const h = (e) => {
      if (dropRef.current && dropRef.current.contains(e.target)) return;
      if (btnRef.current && btnRef.current.contains(e.target)) return;
      setOpen(false);
    };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, [open]);

  // Close on Escape key
  useEffect(() => {
    if (!open) return;
    const h = (e) => { if (e.key === "Escape") setOpen(false); };
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, [open]);

  // Filter overlays valid for this chart, group by category
  const groups = {};
  for (const [key, def] of Object.entries(CHART_OVERLAYS)) {
    if (!def.charts.includes(chartId)) continue;
    if (!groups[def.group]) groups[def.group] = [];
    groups[def.group].push({ key, ...def });
  }
  const toggle = (key) => {
    const next = activeOverlays.includes(key)
      ? activeOverlays.filter(k => k !== key)
      : [...activeOverlays, key].slice(0, 4); // max 4
    onChange(next);
  };
  const count = activeOverlays.length;

  // Dropdown rendered as portal to avoid parent overflow clipping
  const dropdown = open ? ReactDOM.createPortal(
    <div
      ref={dropRef}
      onClick={(e) => e.stopPropagation()}
      style={{
        position: "fixed",
        left: pos.x,
        ...(pos.openUp
          ? { bottom: window.innerHeight - pos.y }
          : { top: pos.y }),
        zIndex: 99999,
        background: T.card,
        border: "1px solid " + T.border,
        borderRadius: 12,
        boxShadow: T.shadowLg || "0 8px 32px rgba(0,0,0,0.25)",
        padding: "10px 6px",
        width: 320,
        maxHeight: 460,
        overflowY: "auto",
      }}
    >
      {/* Header row with title + clear button */}
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        padding: "0 8px 8px", borderBottom: "1px solid " + T.border, marginBottom: 6,
      }}>
        <span style={{
          fontFamily: FONT, fontSize: 12, fontWeight: 700, color: T.text,
        }}>Chart Overlays</span>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {count > 0 && (
            <button onClick={() => onChange([])} style={{
              fontFamily: MONO, fontSize: 10, color: T.red, background: T.red + "12",
              border: "1px solid " + T.red + "30", borderRadius: 4, cursor: "pointer",
              padding: "2px 8px", fontWeight: 600,
            }}>Clear all</button>
          )}
          <span style={{ fontFamily: MONO, fontSize: 10, color: T.textMuted }}>
            {count}/4
          </span>
        </div>
      </div>

      {/* Grouped metric list */}
      {Object.entries(groups).map(([groupName, items]) => (
        <div key={groupName} style={{ marginBottom: 4 }}>
          <div style={{
            fontFamily: MONO, fontSize: 9, fontWeight: 700,
            color: T.textMuted, padding: "6px 8px 3px", textTransform: "uppercase",
            letterSpacing: "0.8px",
          }}>{groupName}</div>
          {items.map(item => {
            const isActive = activeOverlays.includes(item.key);
            const atMax = count >= 4 && !isActive;
            const color = isActive ? getOverlayColor(T, item.key, activeOverlays) : T.textMuted;
            return (
              <div
                key={item.key}
                onClick={() => !atMax && toggle(item.key)}
                style={{
                  display: "flex", alignItems: "flex-start", gap: 10,
                  padding: "6px 8px", borderRadius: 6, cursor: atMax ? "not-allowed" : "pointer",
                  background: isActive ? color + "12" : "transparent",
                  opacity: atMax ? 0.4 : 1,
                  transition: "background 0.15s",
                }}
                onMouseEnter={(e) => { if (!atMax && !isActive) e.currentTarget.style.background = T.border + "40"; }}
                onMouseLeave={(e) => { if (!isActive) e.currentTarget.style.background = "transparent"; }}
              >
                {/* Checkbox */}
                <div style={{
                  width: 16, height: 16, borderRadius: 4, flexShrink: 0, marginTop: 1,
                  border: "2px solid " + color,
                  background: isActive ? color : "transparent",
                  display: "flex", alignItems: "center", justifyContent: "center",
                }}>
                  {isActive && <span style={{ color: "#fff", fontSize: 11, fontWeight: 900, lineHeight: 1 }}>✓</span>}
                </div>
                {/* Label + description */}
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{
                    fontFamily: MONO, fontSize: 11, fontWeight: isActive ? 700 : 500,
                    color: isActive ? T.text : T.textSecondary,
                    lineHeight: 1.3,
                  }}>
                    {item.label}
                    {item.unit && <span style={{ fontSize: 9, color: T.textMuted, marginLeft: 4 }}>({item.unit})</span>}
                  </div>
                  {item.desc && (
                    <div style={{
                      fontFamily: FONT, fontSize: 10, color: T.textMuted,
                      lineHeight: 1.35, marginTop: 2,
                      overflow: "hidden", display: "-webkit-box",
                      WebkitLineClamp: 2, WebkitBoxOrient: "vertical",
                    }}>{item.desc}</div>
                  )}
                </div>
                {/* Color swatch for active overlays */}
                {isActive && (
                  <div style={{
                    width: 20, height: 3, borderRadius: 2, marginTop: 7,
                    background: color, flexShrink: 0,
                    backgroundImage: `repeating-linear-gradient(90deg, ${color} 0px, ${color} 5px, transparent 5px, transparent 8px)`,
                  }} />
                )}
              </div>
            );
          })}
        </div>
      ))}

      {/* Footer */}
      <div style={{
        fontFamily: MONO, fontSize: 9, color: T.textMuted,
        padding: "6px 8px 2px", borderTop: "1px solid " + T.border, marginTop: 4,
        display: "flex", justifyContent: "space-between",
      }}>
        <span>Dashed line on right Y-axis</span>
        <span>Esc to close</span>
      </div>
    </div>,
    document.body
  ) : null;

  return (
    <div style={{ position: "relative" }}>
      <button
        ref={btnRef}
        onClick={(e) => { e.stopPropagation(); setOpen(!open); }}
        style={{
          fontFamily: MONO, fontSize: 10, fontWeight: 600,
          padding: "3px 10px", borderRadius: 5, cursor: "pointer",
          border: "1px solid " + (count > 0 ? T.blue : T.pillBorder),
          background: count > 0 ? T.blue + "18" : "transparent",
          color: count > 0 ? T.blue : T.textMuted,
          transition: "all 0.15s",
        }}
      >
        {count > 0 ? `Overlays (${count})` : "+Overlay"}
      </button>
      {dropdown}
    </div>
  );
}

// v8.4: Theme picker dropdown — replaces the old L/D toggle
function ThemePicker({ theme, setTheme }) {
  const T = THEMES[theme];
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  // Close on click outside
  useEffect(() => {
    if (!open) return;
    const h = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, [open]);
  // Close on Escape
  useEffect(() => {
    if (!open) return;
    const h = (e) => { if (e.key === "Escape") { setOpen(false); e.stopPropagation(); } };
    window.addEventListener("keydown", h, true);
    return () => window.removeEventListener("keydown", h, true);
  }, [open]);
  const meta = THEME_META[theme];
  return (
    <div ref={ref} style={{ position: "relative" }}>
      {/* Compact trigger button */}
      <button onClick={() => setOpen(o => !o)} style={{
        height: 24, padding: "0 10px", borderRadius: 12, border: `1px solid ${T.border}`,
        background: T.pillBg, cursor: "pointer", display: "flex", alignItems: "center", gap: 5,
        fontFamily: MONO, fontSize: 9, fontWeight: 600, color: T.textSecondary, transition: "all 0.15s",
      }}>
        <span style={{ width: 6, height: 6, borderRadius: "50%", background: meta.dots[2] }} />
        <span style={{ width: 6, height: 6, borderRadius: "50%", background: meta.dots[1] }} />
        <span>{meta.name}</span>
      </button>
      {/* Dropdown menu */}
      {open && (
        <div style={{
          position: "absolute", top: "calc(100% + 4px)", right: 0, width: 200,
          background: T.card, border: `1px solid ${T.border}`, borderRadius: 10,
          boxShadow: T.shadowLg, zIndex: 200, padding: 4, overflow: "hidden",
          backdropFilter: T.backdropBlur,
        }}>
          {THEME_ORDER.map(key => {
            const m = THEME_META[key];
            const active = key === theme;
            return (
              <button key={key} onClick={() => { setTheme(key); setOpen(false); }} style={{
                width: "100%", display: "flex", alignItems: "center", gap: 8,
                padding: "7px 10px", borderRadius: 7, border: "none", cursor: "pointer",
                background: active ? T.blueSoft : "transparent",
                fontFamily: FONT, fontSize: 11, fontWeight: active ? 700 : 500,
                color: active ? T.blue : T.text, textAlign: "left", transition: "background 0.1s",
              }}>
                <div style={{ display: "flex", gap: 3 }}>
                  {m.dots.map((c, i) => (
                    <span key={i} style={{
                      width: 10, height: 10, borderRadius: "50%", background: c,
                      border: "1px solid rgba(128,128,128,0.3)",
                    }} />
                  ))}
                </div>
                <span>{m.name}</span>
                {active && <span style={{ marginLeft: "auto", fontSize: 9, opacity: 0.6 }}>*</span>}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

// Fullscreen overlay for charts
function FullscreenOverlay({ children, onClose, theme }) {
  const T = THEMES[theme];
  useEffect(() => {
    const h = (e) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, [onClose]);
  return ReactDOM.createPortal(
    <div onClick={onClose} style={{
      position: "fixed", inset: 0, zIndex: 10000,
      background: "rgba(0,0,0,0.85)", display: "flex",
      alignItems: "center", justifyContent: "center", padding: 40, cursor: "zoom-out",
    }}>
      <div onClick={e => e.stopPropagation()} style={{
        width: "90vw", maxWidth: 1400, background: T.card,
        borderRadius: 16, padding: 24, border: `1px solid ${T.border}`, cursor: "default",
      }}>{children}</div>
    </div>, document.body
  );
}

// 2. Event rate bar chart — stacked Coinbase/Kalshi with time axis + hover + gap markers
// v8.2: Added onDrillDown prop for click-to-inspect
function EventRateChart({ history, theme, width, height, gapTimestamps, onDrillDown, overlays }) {
  const T = THEMES[theme];
  const svgRef = useRef(null);
  const hoverRef = useRef(null);
  const h = height || 220; const w = width || 500;
  useEffect(() => {
    if (!svgRef.current || history.length < 2) return;
    const svg = d3.select(svgRef.current); svg.selectAll("*").remove();
    // v8.15: Increased top padding 10→22 so legend labels sit above the chart
    // area.  Previously bars & overlay lines could overlap the Coinbase/Kalshi
    // numbers in the top-right corner.
    const pad = { t: 22, r: (overlays && overlays.length) ? 55 : 10, b: 32, l: 45 };
    const iw = w - pad.l - pad.r, ih = h - pad.t - pad.b;

    // v8.2 fix: Use per-exchange rate gauges directly instead of computing
    // deltas from cumulative counters.  The counters only update every 30s in
    // periodic_loop(), so polling at 2s created sawtooth artifacts — 14 bars
    // with 50/50 fallback split, then 1 spike bar with the true CB/KL ratio.
    // The new exchange_rate gauges hold the current events/sec per exchange
    // and can be read on every poll without jitter.
    const feedDataRaw = history.map((e, i) => {
      const cbR = e.cbRate || 0;
      const klR = e.klRate || 0;
      const sum = cbR + klR;
      const rate = e.rate || 0;
      // Use the smoothed per-exchange rates for CB/KL proportion, but scale
      // so the total bar height matches the Prometheus event_rate gauge.
      // Counter-based rates can undercount during tape rotation (resets), so
      // we normalize: keep the CB/KL ratio accurate, adjust magnitudes.
      if (sum > 0.1 && rate > 0) {
        const scale = rate / sum;
        return { total: rate, cbR: cbR * scale, klR: klR * scale, ts: e.ts };
      }
      // Fallback: no gauge rates available — split total rate 50/50
      if (rate > 0) {
        return { total: rate, cbR: rate * 0.5, klR: rate * 0.5, ts: e.ts };
      }
      return { total: 0, cbR: 0, klR: 0, ts: e.ts };
    });

    // v8.13: Width-aware bar aggregation — limit bars based on chart width.
    // At 2s polling, 15m = 450 entries, 30m = 900, 1h = 1800.  Without
    // aggregation, bars become 1-2px wide and unreadable.  Target ~6px per
    // bar (4px bar + 2px gap) for a clean look.  Adjacent entries are
    // averaged to preserve data accuracy instead of just dropping entries.
    const maxBars = Math.max(20, Math.floor(iw / 6));
    let feedData = feedDataRaw;
    if (feedDataRaw.length > maxBars) {
      const groupSize = Math.ceil(feedDataRaw.length / maxBars);
      feedData = [];
      for (let i = 0; i < feedDataRaw.length; i += groupSize) {
        const chunk = feedDataRaw.slice(i, i + groupSize);
        const n = chunk.length;
        feedData.push({
          total: chunk.reduce((s, d) => s + d.total, 0) / n,
          cbR:   chunk.reduce((s, d) => s + d.cbR, 0) / n,
          klR:   chunk.reduce((s, d) => s + d.klR, 0) / n,
          ts:    chunk[Math.floor(n / 2)].ts,  // midpoint timestamp for labels
        });
      }
    }
    const maxVal = Math.max(10, d3.max(feedData, d => d.cbR + d.klR) * 1.15);
    // v8.6: Index-based bar positioning — equal spacing, no gaps.
    // Previously bars were positioned by timestamp, which caused visible gaps
    // when data points were missing (preloaded history, missed polls, startup).
    // Now every bar gets equal width, filling the chart continuously.
    const tMin = feedData[0].ts, tMax = feedData[feedData.length - 1].ts;
    const barGap = 2;
    const barStep = iw / feedData.length;
    const xBar = (i) => i * barStep;
    const barW = () => Math.max(1, barStep - barGap);
    const y = d3.scaleLinear().domain([0, maxVal]).range([ih, 0]);
    const g = svg.append("g").attr("transform", `translate(${pad.l},${pad.t})`);

    // Grid lines + Y labels
    g.selectAll(".grid").data(y.ticks(5)).enter().append("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", T.chartGrid).attr("stroke-dasharray", "2,3");
    g.selectAll(".yl").data(y.ticks(5)).enter().append("text")
      .attr("x", -8).attr("y", d => y(d) + 3).attr("text-anchor", "end")
      .attr("fill", T.textMuted).attr("font-size", 9).attr("font-family", MONO).text(d => d >= 1000 ? (d/1000).toFixed(0) + "k" : d.toFixed(0));
    // Y-axis line (left edge)
    g.append("line").attr("x1", 0).attr("x2", 0).attr("y1", 0).attr("y2", ih)
      .attr("stroke", T.textMuted).attr("stroke-width", 1).attr("opacity", 0.5);

    // Stacked bars: Kalshi bottom, Coinbase top — index-positioned, equal width
    // v8.4 fix: Raised opacity from 0.7 → 0.85 so purple vs blue is clearly
    // distinguishable on dark backgrounds (0.7 made purple look blue-ish).
    g.selectAll(".bar-kl").data(feedData).enter().append("rect")
      .attr("x", (_, i) => xBar(i)).attr("y", d => y(d.klR)).attr("width", barW())
      .attr("height", d => ih - y(d.klR)).attr("fill", T.purple).attr("opacity", 0.85);
    g.selectAll(".bar-cb").data(feedData).enter().append("rect")
      .attr("x", (_, i) => xBar(i)).attr("y", d => y(d.klR + d.cbR)).attr("width", barW())
      .attr("height", d => y(d.klR) - y(d.klR + d.cbR)).attr("fill", T.blue).attr("opacity", 0.85);

    // Time axis — v8.11: Index-aligned time labels
    // Bars are index-based (equal spacing), so we generate time labels at bar
    // positions rather than using d3.scaleTime which misaligns with bars when
    // data has gaps or uneven polling intervals.
    if (feedData.length >= 2 && feedData[0].ts && feedData[feedData.length - 1].ts) {
      const span = feedData[feedData.length - 1].ts - feedData[0].ts;
      let tickFmt;
      if (span > 86400) tickFmt = d3.timeFormat("%m/%d %H:%M");
      else tickFmt = d3.timeFormat("%H:%M");
      // Calculate how many labels fit without overlap (~55px per label)
      const maxLabels = Math.max(2, Math.floor(iw / 55));
      const step = Math.max(1, Math.floor(feedData.length / maxLabels));
      const axisG = g.append("g").attr("transform", `translate(0, ${ih})`);
      // Bottom axis line
      axisG.append("line").attr("x1", 0).attr("x2", iw).attr("y1", 0).attr("y2", 0).attr("stroke", T.chartGrid);
      for (let i = 0; i < feedData.length; i += step) {
        const cx = xBar(i) + barW() / 2;
        const label = tickFmt(new Date(feedData[i].ts * 1000));
        axisG.append("line").attr("x1", cx).attr("x2", cx).attr("y1", 0).attr("y2", 4).attr("stroke", T.chartGrid);
        axisG.append("text").attr("x", cx).attr("y", 16).attr("text-anchor", "middle")
          .attr("fill", T.textMuted).attr("font-size", 8).attr("font-family", MONO).text(label);
      }
    }

    // Legend above chart — v8.4: Show live evt/s per exchange so you can see
    // the actual breakdown even when one source dominates (e.g. Kalshi ≈97%).
    const lastPt = feedData[feedData.length - 1] || {};
    const cbLabel = "Coinbase" + (lastPt.cbR > 0 ? ` ${Math.round(lastPt.cbR)}` : "");
    const klLabel = "Kalshi" + (lastPt.klR > 0 ? ` ${Math.round(lastPt.klR)}` : "");
    const legends = [{ k: cbLabel, c: T.blue }, { k: klLabel, c: T.purple }];
    // v8.15: Moved legend above chart area (y=-14/-7 instead of -1/6) so
    // overlay lines and tall bars don't pass through the text.
    legends.forEach((l, i) => {
      const lx = iw - (legends.length * 85) + i * 85;
      g.append("rect").attr("x", lx).attr("y", -14).attr("width", 10).attr("height", 8).attr("rx", 2).attr("fill", l.c).attr("opacity", 0.85);
      g.append("text").attr("x", lx + 13).attr("y", -7).attr("fill", l.c).attr("font-size", 9).attr("font-family", MONO).text(l.k);
    });

    // v8.1: Gap markers — red dots at bottom of chart where rate dropped to 0
    // v8.6: Match index-based positioning — find nearest feedData index for each gap timestamp
    if (gapTimestamps && gapTimestamps.length > 0) {
      gapTimestamps.forEach(ts => {
        if (ts >= tMin && ts <= tMax) {
          // Find the closest feedData index to this timestamp
          let bestIdx = 0, bestDist = Infinity;
          for (let i = 0; i < feedData.length; i++) {
            const dist = Math.abs(feedData[i].ts - ts);
            if (dist < bestDist) { bestDist = dist; bestIdx = i; }
          }
          g.append("circle").attr("cx", xBar(bestIdx) + barW() / 2).attr("cy", ih - 4).attr("r", 3.5)
            .attr("fill", T.red).attr("opacity", 0.85).attr("stroke", T.card).attr("stroke-width", 1);
        }
      });
    }

    // v8.6: Overlay lines on secondary Y-axis (right side)
    if (overlays && overlays.length > 0) {
      overlays.forEach((ovKey, ovIdx) => {
        const def = CHART_OVERLAYS[ovKey];
        if (!def) return;
        const color = getOverlayColor(T, ovKey, overlays);
        // Filter values — skip sentinel values (e.g. -1 means "not available")
        const sentinel = def.sentinel != null ? def.sentinel : null;
        const vals = history.map(d => {
          const v = d[ovKey];
          if (v == null) return null;
          if (sentinel !== null && v === sentinel) return null;
          if (v < 0) return null;
          return v;
        });
        const validVals = vals.filter(v => v !== null);
        if (validVals.length < 2) {
          // v8.6: Show "no data" label so user knows the overlay is selected
          // but has no valid values (e.g. WS RTT Kalshi returns -1 = disabled)
          g.append("text")
            .attr("x", iw / 2).attr("y", 30 + ovIdx * 14)
            .attr("text-anchor", "middle")
            .attr("fill", color).attr("font-size", 9).attr("font-family", MONO)
            .attr("opacity", 0.7)
            .text(`${def.label}: no data`);
          return;
        }
        // Smart domain: use fixedDomain if defined, otherwise auto-scale
        // with good fallbacks for flat-line data (all values identical)
        let domLo, domHi;
        if (def.fixedDomain) {
          [domLo, domHi] = def.fixedDomain;
        } else {
          const ovMin = d3.min(validVals);
          const ovMax = d3.max(validVals);
          if (ovMin === ovMax) {
            // Flat line — create a meaningful range around the value
            domLo = Math.max(0, ovMin - Math.max(ovMin * 0.2, 1));
            domHi = ovMax + Math.max(ovMax * 0.2, 1);
          } else {
            const ovRange = ovMax - ovMin;
            domLo = ovMin - ovRange * 0.05;
            domHi = ovMax + ovRange * 0.1;
          }
        }
        const yOv = d3.scaleLinear().domain([domLo, domHi]).range([ih, 0]);
        // Draw overlay line (color already defined above)
        const ovLine = d3.line()
          .defined((_, i) => vals[i] !== null)
          .x((_, i) => xBar(i) + barW() / 2)
          .y((_, i) => yOv(vals[i] != null ? vals[i] : 0))
          .curve(d3.curveMonotoneX);
        g.append("path").datum(vals)
          .attr("d", ovLine)
          .attr("fill", "none")
          .attr("stroke", color)
          .attr("stroke-width", 1.5)
          .attr("stroke-dasharray", "6,3")
          .attr("opacity", 0.8);
        // Right Y-axis labels (first overlay only to avoid clutter)
        if (ovIdx === 0) {
          const axisTicks = yOv.ticks(4);
          axisTicks.forEach(tick => {
            g.append("text")
              .attr("x", iw + 6).attr("y", yOv(tick) + 3)
              .attr("text-anchor", "start")
              .attr("fill", color).attr("font-size", 8).attr("font-family", MONO)
              .text(def.format(tick));
          });
          g.append("line")
            .attr("x1", iw).attr("x2", iw).attr("y1", 0).attr("y2", ih)
            .attr("stroke", color).attr("stroke-width", 1).attr("opacity", 0.3);
        }
        // Legend entry (dashed line + label)
        const legendX = 4 + ovIdx * 110;
        g.append("line")
          .attr("x1", legendX).attr("x2", legendX + 14)
          .attr("y1", ih + 22).attr("y2", ih + 22)
          .attr("stroke", color).attr("stroke-width", 1.5).attr("stroke-dasharray", "4,2");
        g.append("text")
          .attr("x", legendX + 17).attr("y", ih + 25)
          .attr("fill", color).attr("font-size", 8).attr("font-family", MONO)
          .text(def.label);
      });
    }

    // Hover crosshair + v8.2 click-to-inspect
    const hoverLine = g.append("line").attr("y1", 0).attr("y2", ih).attr("stroke", T.text).attr("stroke-width", 1).attr("stroke-dasharray", "3,2").attr("opacity", 0).attr("pointer-events", "none");
    const hoverText = g.append("text").attr("fill", T.text).attr("font-size", 10).attr("font-family", MONO).attr("font-weight", 700).attr("opacity", 0).attr("pointer-events", "none");
    // v8.2: Second hover line for "click to inspect" hint
    const hoverHint = g.append("text").attr("fill", T.textMuted).attr("font-size", 8).attr("font-family", MONO).attr("opacity", 0).attr("pointer-events", "none");
    // v8.6: Overlay hover box — shows overlay values at the hovered point
    const ovHoverBox = g.append("g").attr("opacity", 0).attr("pointer-events", "none");
    const ovHoverBg = ovHoverBox.append("rect").attr("fill", T.card).attr("stroke", T.border).attr("stroke-width", 1).attr("rx", 6).attr("opacity", 0.93);
    let hoverIdx = -1; // Track hovered index for click handler
    g.append("rect").attr("width", iw).attr("height", ih).attr("fill", "transparent")
      .attr("cursor", onDrillDown ? "crosshair" : "default")
      .on("mousemove", function(event) {
        hoverRef.current = [event.clientX, event.clientY];
        const [mx] = d3.pointer(event);
        // v8.6: Index-based bar lookup (equal spacing)
        let idx = Math.round((mx - barStep / 2) / barStep);
        idx = Math.max(0, Math.min(idx, feedData.length - 1));
        hoverIdx = idx;
        const cx = xBar(idx) + barW() / 2;
        const d = feedData[idx];
        hoverLine.attr("x1", cx).attr("x2", cx).attr("opacity", 0.5);
        const time = d.ts ? new Date(d.ts * 1000).toLocaleTimeString() : "";
        const label = `${(d.cbR + d.klR).toFixed(1)} e/s  ${time}`;
        const tx = cx < iw / 2 ? cx + 8 : cx - 8;
        const anchor = cx < iw / 2 ? "start" : "end";
        hoverText.attr("x", tx).attr("y", 14).attr("text-anchor", anchor).text(label).attr("opacity", 1);
        // v8.2: Show "click to inspect" hint
        if (onDrillDown) {
          hoverHint.attr("x", tx).attr("y", 28).attr("text-anchor", anchor).text("click to inspect").attr("opacity", 0.6);
        }
        // v8.6: Show overlay values at hovered point
        ovHoverBox.selectAll("text").remove();
        if (overlays && overlays.length > 0) {
          const hEntry = history[idx];
          if (hEntry) {
            let rowY = 14;
            const boxLines = [];
            overlays.forEach(ovKey => {
              const def = CHART_OVERLAYS[ovKey];
              if (!def) return;
              const val = hEntry[ovKey];
              const sentinel = def.sentinel != null ? def.sentinel : null;
              const display = (val == null || (sentinel !== null && val === sentinel) || val < 0)
                ? "N/A" : def.format(val);
              const color = getOverlayColor(T, ovKey, overlays);
              boxLines.push({ label: def.label, display, color });
            });
            if (boxLines.length > 0) {
              const boxW = 140;
              const boxH = boxLines.length * 14 + 8;
              const boxX = cx < iw / 2 ? cx + 8 : cx - boxW - 8;
              const startY = onDrillDown ? 38 : 28;
              ovHoverBox.attr("opacity", 1).attr("transform", `translate(${boxX}, ${startY})`);
              ovHoverBg.attr("width", boxW).attr("height", boxH);
              boxLines.forEach((bl, i) => {
                ovHoverBox.append("text").attr("x", 6).attr("y", 12 + i * 14)
                  .attr("fill", bl.color).attr("font-size", 9).attr("font-family", MONO).attr("font-weight", 600)
                  .text(`${bl.label}: ${bl.display}`);
              });
            }
          }
        } else {
          ovHoverBox.attr("opacity", 0);
        }
      })
      .on("mouseleave", function() { hoverRef.current = null; hoverLine.attr("opacity", 0); hoverText.attr("opacity", 0); hoverHint.attr("opacity", 0); ovHoverBox.attr("opacity", 0); ovHoverBox.selectAll("text").remove(); hoverIdx = -1; })
      .on("click", function() {
        // v8.2: Click to open Event Inspector
        if (onDrillDown && hoverIdx >= 0 && feedData[hoverIdx]) {
          onDrillDown(feedData[hoverIdx].ts);
        }
      });
    _restoreHover(svg, hoverRef);
  }, [history, w, h, theme, gapTimestamps, onDrillDown, overlays]);
  if (history.length < 2) return <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, padding: 40, textAlign: "center" }}>Waiting for metrics data...</div>;
  return <svg ref={svgRef} width={w} height={h} />;
}

// 3. Latency chart p50/p95/p99 — toggle True/Raw with time axis + hover
function LatencyChart({ history, theme, width, height, mode: controlledMode, onModeChange, overlays }) {
  const T = THEMES[theme];
  const svgRef = useRef(null);
  const hoverRef = useRef(null);
  // Support both controlled (parent manages state) and uncontrolled (internal state) modes
  const [internalMode, setInternalMode] = useState("true");
  const mode = controlledMode || internalMode;
  const setMode = onModeChange || setInternalMode;
  const h = height || 220; const w = width || 500;
  const fmtMs = v => Math.abs(v) >= 1000 ? (v / 1000).toFixed(1) + "s" : Math.round(v) + "ms";
  useEffect(() => {
    if (!svgRef.current || history.length < 2) return;
    const svg = d3.select(svgRef.current); svg.selectAll("*").remove();
    const pad = { t: 22, r: (overlays && overlays.length) ? 55 : 10, b: 32, l: 60 };
    const iw = w - pad.l - pad.r, ih = h - pad.t - pad.b;

    // Transform data based on mode
    const data = mode === "true"
      ? (() => {
          const WIN = 30;
          return history.map((d, i) => {
            const start = Math.max(0, i - WIN + 1);
            const slice = history.slice(start, i + 1);
            const avgP50 = slice.reduce((s, v) => s + v.p50, 0) / slice.length;
            return { p50: d.p50 - avgP50, p95: d.p95 - avgP50, p99: d.p99 - avgP50, ts: d.ts };
          });
        })()
      : history;

    const x = d3.scaleLinear().domain([0, data.length - 1]).range([0, iw]);
    let y, ticks;
    if (mode === "true") {
      const maxAbs = Math.max(10,
        Math.abs(d3.min(data, d => Math.min(d.p50, d.p95, d.p99)) || 0) * 1.3,
        Math.abs(d3.max(data, d => Math.max(d.p50, d.p95, d.p99)) || 0) * 1.3);
      y = d3.scaleSymlog().constant(5).domain([-maxAbs, maxAbs]).range([ih, 0]);
      const trueTicks = [0, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000];
      const allTicks = [...trueTicks, ...trueTicks.filter(v => v > 0).map(v => -v)];
      const rawTicks = allTicks.filter(v => v >= -maxAbs && v <= maxAbs).sort((a, b) => a - b);
      ticks = rawTicks.filter((v, i) => { if (i === 0) return true; return Math.abs(y(v) - y(rawTicks[i - 1])) >= 16; });
    } else {
      const maxAbs = Math.max(10,
        Math.abs(d3.min(data, d => Math.min(d.p50, d.p95, d.p99)) || 0) * 1.5,
        Math.abs(d3.max(data, d => Math.max(d.p50, d.p95, d.p99)) || 0) * 1.5);
      y = d3.scaleSymlog().constant(1).domain([-maxAbs, maxAbs]).range([ih, 0]);
      const posTicks = [];
      for (let v = 0; v <= 100; v += 20) posTicks.push(v);
      for (let v = 200; v <= 1000; v += 200) posTicks.push(v);
      for (let v = 2000; v <= 100000; v += 2000) posTicks.push(v);
      const allTicks = [...posTicks, ...posTicks.filter(v => v > 0).map(v => -v)];
      const rawTicks = allTicks.filter(v => v >= -maxAbs && v <= maxAbs).sort((a, b) => a - b);
      ticks = rawTicks.filter((v, i) => { if (i === 0) return true; return Math.abs(y(v) - y(rawTicks[i - 1])) >= 16; });
    }

    const g = svg.append("g").attr("transform", `translate(${pad.l},${pad.t})`);
    // Grid + labels
    g.selectAll(".grid").data(ticks).enter().append("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", T.chartGrid).attr("stroke-dasharray", "2,3");
    g.selectAll(".yl").data(ticks).enter().append("text")
      .attr("x", -8).attr("y", d => y(d) + 3).attr("text-anchor", "end")
      .attr("fill", T.textMuted).attr("font-size", 9).attr("font-family", MONO).text(d => fmtMs(d));
    // Y-axis line (left edge)
    g.append("line").attr("x1", 0).attr("x2", 0).attr("y1", 0).attr("y2", ih)
      .attr("stroke", T.textMuted).attr("stroke-width", 1).attr("opacity", 0.5);
    // Zero-line
    g.append("line").attr("x1", 0).attr("x2", iw).attr("y1", y(0)).attr("y2", y(0))
      .attr("stroke", T.textMuted).attr("stroke-width", 1.5).attr("stroke-dasharray", "4,3").attr("opacity", 0.7);
    // Draw latency lines — v8.8: sliding window handles playback, no cursor needed
    const drawLine = (key, color, dash) => {
      const ln = d3.line().x((_, i) => x(i)).y(d => y(d[key])).curve(d3.curveBasis);
      g.append("path").datum(data).attr("d", ln).attr("fill", "none")
        .attr("stroke", color).attr("stroke-width", 1.5).attr("stroke-dasharray", dash || "none");
    };
    drawLine("p50", T.green, null);
    drawLine("p95", T.amber, "4,2");
    drawLine("p99", T.red, "2,2");

    // v8.6: Overlay lines on secondary Y-axis (right side)
    if (overlays && overlays.length > 0) {
      overlays.forEach((ovKey, ovIdx) => {
        const def = CHART_OVERLAYS[ovKey];
        if (!def) return;
        const color = getOverlayColor(T, ovKey, overlays);
        // Use original history (not bias-removed data) for overlay values
        // Filter values — skip sentinel values (e.g. -1 means "not available")
        const sentinel = def.sentinel != null ? def.sentinel : null;
        const vals = history.map(d => {
          const v = d[ovKey];
          if (v == null) return null;
          if (sentinel !== null && v === sentinel) return null;
          if (v < 0) return null;
          return v;
        });
        const validVals = vals.filter(v => v !== null);
        if (validVals.length < 2) {
          // v8.6: Show "no data" label so user knows the overlay is selected
          // but has no valid values (e.g. WS RTT Kalshi returns -1 = disabled)
          g.append("text")
            .attr("x", iw / 2).attr("y", 30 + ovIdx * 14)
            .attr("text-anchor", "middle")
            .attr("fill", color).attr("font-size", 9).attr("font-family", MONO)
            .attr("opacity", 0.7)
            .text(`${def.label}: no data`);
          return;
        }
        // Smart domain: use fixedDomain if defined, otherwise auto-scale
        let domLo, domHi;
        if (def.fixedDomain) {
          [domLo, domHi] = def.fixedDomain;
        } else {
          const ovMin = d3.min(validVals);
          const ovMax = d3.max(validVals);
          if (ovMin === ovMax) {
            domLo = Math.max(0, ovMin - Math.max(ovMin * 0.2, 1));
            domHi = ovMax + Math.max(ovMax * 0.2, 1);
          } else {
            const ovRange = ovMax - ovMin;
            domLo = ovMin - ovRange * 0.05;
            domHi = ovMax + ovRange * 0.1;
          }
        }
        const yOv = d3.scaleLinear().domain([domLo, domHi]).range([ih, 0]);
        // Draw overlay line using index-based x positioning (color defined above)
        const ovLine = d3.line()
          .defined((_, i) => vals[i] !== null)
          .x((_, i) => x(i))
          .y((_, i) => yOv(vals[i] != null ? vals[i] : 0))
          .curve(d3.curveMonotoneX);
        g.append("path").datum(vals)
          .attr("d", ovLine)
          .attr("fill", "none")
          .attr("stroke", color)
          .attr("stroke-width", 1.5)
          .attr("stroke-dasharray", "6,3")
          .attr("opacity", 0.8);
        // Right Y-axis labels (first overlay only)
        if (ovIdx === 0) {
          const axisTicks = yOv.ticks(4);
          axisTicks.forEach(tick => {
            g.append("text")
              .attr("x", iw + 6).attr("y", yOv(tick) + 3)
              .attr("text-anchor", "start")
              .attr("fill", color).attr("font-size", 8).attr("font-family", MONO)
              .text(def.format(tick));
          });
          g.append("line")
            .attr("x1", iw).attr("x2", iw).attr("y1", 0).attr("y2", ih)
            .attr("stroke", color).attr("stroke-width", 1).attr("opacity", 0.3);
        }
        // Legend entry (dashed line + label) below chart
        const legendX = 4 + ovIdx * 110;
        g.append("line")
          .attr("x1", legendX).attr("x2", legendX + 14)
          .attr("y1", ih + 22).attr("y2", ih + 22)
          .attr("stroke", color).attr("stroke-width", 1.5).attr("stroke-dasharray", "4,2");
        g.append("text")
          .attr("x", legendX + 17).attr("y", ih + 25)
          .attr("fill", color).attr("font-size", 8).attr("font-family", MONO)
          .text(def.label);
      });
    }

    // Legend above chart
    const labels = [{ k: "p50", c: T.green }, { k: "p95", c: T.amber }, { k: "p99", c: T.red }];
    labels.forEach((l, i) => {
      const lx = iw - (labels.length * 40) + i * 40;
      g.append("line").attr("x1", lx).attr("x2", lx + 14).attr("y1", -10).attr("y2", -10).attr("stroke", l.c).attr("stroke-width", 2);
      g.append("text").attr("x", lx + 16).attr("y", -7).attr("fill", l.c).attr("font-size", 9).attr("font-family", MONO).text(l.k);
    });

    // Time axis — v8.11: Index-aligned time labels (same fix as EventRateChart)
    if (data.length >= 2 && data[0].ts && data[data.length - 1].ts) {
      const span = data[data.length - 1].ts - data[0].ts;
      let tickFmt;
      if (span > 86400) tickFmt = d3.timeFormat("%m/%d %H:%M");
      else tickFmt = d3.timeFormat("%H:%M");
      const maxLabels = Math.max(2, Math.floor(iw / 55));
      const step = Math.max(1, Math.floor(data.length / maxLabels));
      const axisG = g.append("g").attr("transform", `translate(0, ${ih})`);
      axisG.append("line").attr("x1", 0).attr("x2", iw).attr("y1", 0).attr("y2", 0).attr("stroke", T.chartGrid);
      for (let i = 0; i < data.length; i += step) {
        const cx = x(i);
        const label = tickFmt(new Date(data[i].ts * 1000));
        axisG.append("line").attr("x1", cx).attr("x2", cx).attr("y1", 0).attr("y2", 4).attr("stroke", T.chartGrid);
        axisG.append("text").attr("x", cx).attr("y", 16).attr("text-anchor", "middle")
          .attr("fill", T.textMuted).attr("font-size", 8).attr("font-family", MONO).text(label);
      }
    }

    // Hover crosshair + tooltip
    const hoverLine = g.append("line").attr("y1", 0).attr("y2", ih).attr("stroke", T.text).attr("stroke-width", 1).attr("stroke-dasharray", "3,2").attr("opacity", 0).attr("pointer-events", "none");
    const hoverDots = ["p50", "p95", "p99"].map((k, i) => {
      const colors = [T.green, T.amber, T.red];
      return g.append("circle").attr("r", 3.5).attr("fill", colors[i]).attr("stroke", T.card).attr("stroke-width", 1.5).attr("opacity", 0).attr("pointer-events", "none");
    });
    const hoverBox = g.append("g").attr("opacity", 0).attr("pointer-events", "none");
    const hoverRect = hoverBox.append("rect").attr("fill", T.card).attr("stroke", T.border).attr("stroke-width", 1).attr("rx", 5).attr("opacity", 0.95);
    // v8.6: Pre-allocate 4 base text rows + up to 4 overlay rows
    const hoverTexts = [0, 1, 2, 3, 4, 5, 6, 7].map(i => hoverBox.append("text").attr("font-size", 9).attr("font-family", MONO).attr("font-weight", 600));
    g.append("rect").attr("width", iw).attr("height", ih).attr("fill", "transparent")
      .on("mousemove", function(event) {
        hoverRef.current = [event.clientX, event.clientY];
        const [mx] = d3.pointer(event);
        const idx = Math.round(mx / iw * (data.length - 1));
        if (idx < 0 || idx >= data.length) return;
        const cx = x(idx); const d = data[idx];
        hoverLine.attr("x1", cx).attr("x2", cx).attr("opacity", 0.5);
        const keys = ["p50", "p95", "p99"];
        const colors = [T.green, T.amber, T.red];
        keys.forEach((k, i) => { hoverDots[i].attr("cx", cx).attr("cy", y(d[k])).attr("opacity", 1); });
        // Show timestamp + 3 latency values
        const ts = d.ts ? new Date(d.ts * 1000).toLocaleTimeString() : "";
        hoverTexts[0].attr("x", 6).attr("y", 12).attr("fill", T.textMuted).text(ts);
        keys.forEach((k, i) => { hoverTexts[i + 1].attr("x", 6).attr("y", 24 + i * 12).attr("fill", colors[i]).text(`${k}: ${fmtMs(d[k])}`); });
        // v8.6: Append overlay values below the p50/p95/p99 rows
        let ovRows = 0;
        if (overlays && overlays.length > 0) {
          const hEntry = history[idx]; // Use original history for overlay data
          if (hEntry) {
            overlays.forEach((ovKey, oi) => {
              const def = CHART_OVERLAYS[ovKey];
              if (!def) return;
              const val = hEntry[ovKey];
              const sentinel = def.sentinel != null ? def.sentinel : null;
              const display = (val == null || (sentinel !== null && val === sentinel) || val < 0)
                ? "N/A" : def.format(val);
              const color = getOverlayColor(T, ovKey, overlays);
              const tIdx = 4 + oi; // slots 4-7 for overlays
              if (tIdx < hoverTexts.length) {
                hoverTexts[tIdx].attr("x", 6).attr("y", 60 + oi * 12).attr("fill", color).text(`${def.label}: ${display}`).attr("opacity", 1);
                ovRows++;
              }
            });
          }
        }
        // Hide unused overlay text slots
        for (let i = 4 + ovRows; i < hoverTexts.length; i++) {
          hoverTexts[i].attr("opacity", 0);
        }
        // Resize hover box to fit all rows
        const boxH = 58 + ovRows * 12 + (ovRows > 0 ? 6 : 0);
        const boxW = ovRows > 0 ? 150 : 110;
        const boxX = cx < iw / 2 ? cx + 10 : cx - boxW - 10;
        hoverBox.attr("opacity", 1).attr("transform", `translate(${boxX}, 10)`);
        hoverRect.attr("width", boxW).attr("height", boxH);
      })
      .on("mouseleave", function() {
        hoverRef.current = null;
        hoverLine.attr("opacity", 0); hoverBox.attr("opacity", 0);
        hoverDots.forEach(d => d.attr("opacity", 0));
      });
    _restoreHover(svg, hoverRef);
  }, [history, w, h, theme, mode, overlays]);
  if (history.length < 2) return <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, padding: 40, textAlign: "center" }}>Waiting for metrics data...</div>;
  const btnBase = { padding: "2px 8px", fontSize: 9, fontFamily: MONO, fontWeight: 600, border: "none", cursor: "pointer", borderRadius: 4 };
  const btnOn = { ...btnBase, background: T.cyan + "30", color: T.cyan };
  const btnOff = { ...btnBase, background: "transparent", color: T.textMuted };
  return (<div>
    <div style={{ display: "flex", justifyContent: "flex-end", gap: 2, marginBottom: 2 }}>
      <button onClick={(e) => { e.stopPropagation(); setMode("true"); }} style={mode === "true" ? btnOn : btnOff}>True</button>
      <button onClick={(e) => { e.stopPropagation(); setMode("raw"); }} style={mode === "raw" ? btnOn : btnOff}>Raw</button>
    </div>
    <svg ref={svgRef} width={w} height={h} />
  </div>);
}

// 4. Feed health grid (Coinbase + Kalshi)
function FeedHealthGrid({ metrics, theme }) {
  const T = THEMES[theme];
  if (!metrics) return <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, padding: 30, textAlign: "center" }}>Collector offline</div>;
  // v8.1: Format connection uptime as "Xh Ym"
  const fmtConnUp = (s) => {
    if (!s) return "—";
    const h = Math.floor(s / 3600); const m = Math.floor((s % 3600) / 60);
    return h > 0 ? `${h}h ${m}m` : `${m}m`;
  };
  const feeds = [
    { name: "Coinbase", events: metrics.events_total?.coinbase || 0, gaps: metrics.gaps_total?.coinbase || 0, color: T.blue,
      reconnects: metrics.reconnects_total?.coinbase || 0, connUptime: fmtConnUp(metrics.connection_uptime_seconds?.coinbase) },
    { name: "Kalshi", events: metrics.events_total?.kalshi || 0, gaps: metrics.gaps_total?.kalshi || 0, color: T.purple,
      reconnects: metrics.reconnects_total?.kalshi || 0, connUptime: fmtConnUp(metrics.connection_uptime_seconds?.kalshi) },
  ];
  return (<div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
    {feeds.map(f => {
      const ok = f.gaps < 10;
      return (<div key={f.name} style={{
        padding: "10px 10px", borderRadius: 10, minWidth: 0, overflow: "hidden", boxSizing: "border-box",
        background: T.cardAlt,
        border: `1px solid ${ok ? T.green : T.red}25`,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 5, marginBottom: 8 }}>
          <div style={{ width: 9, height: 9, borderRadius: "50%", flexShrink: 0, background: ok ? T.green : T.red, animation: "pulse 2s infinite" }} />
          <span style={{ fontFamily: FONT, fontSize: 13, fontWeight: 700, color: T.text, whiteSpace: "nowrap" }}>{f.name}</span>
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 4 }}>
          <div style={{ minWidth: 0 }}>
            <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, textTransform: "uppercase" }}>Events</div>
            <div style={{ fontFamily: MONO, fontSize: 14, fontWeight: 700, color: f.color, whiteSpace: "nowrap" }}>{f.events.toLocaleString()}</div>
          </div>
          <div style={{ minWidth: 0 }}>
            <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, textTransform: "uppercase" }}>Gaps</div>
            <div style={{ fontFamily: MONO, fontSize: 14, fontWeight: 700, color: f.gaps > 0 ? T.amber : T.green }}>{f.gaps}</div>
          </div>
        </div>
        {/* v8.1: Reconnects + Connection Uptime row */}
        <div style={{ display: "flex", justifyContent: "space-between", gap: 4, marginTop: 6, paddingTop: 6, borderTop: `1px solid ${T.borderLight}` }}>
          <div style={{ minWidth: 0 }}>
            <div style={{ fontFamily: MONO, fontSize: 8, color: T.textMuted, textTransform: "uppercase" }}>Reconn</div>
            <div style={{ fontFamily: MONO, fontSize: 12, fontWeight: 700, color: f.reconnects > 3 ? T.amber : T.textSecondary }}>{f.reconnects}</div>
          </div>
          <div style={{ minWidth: 0 }}>
            <div style={{ fontFamily: MONO, fontSize: 8, color: T.textMuted, textTransform: "uppercase" }}>Conn Up</div>
            <div style={{ fontFamily: MONO, fontSize: 12, fontWeight: 700, color: T.textSecondary, whiteSpace: "nowrap" }}>{f.connUptime}</div>
          </div>
        </div>
      </div>);
    })}
  </div>);
}

// 5. Tape status with rotation countdown
function TapeStatusBars({ tapes, theme }) {
  const T = THEMES[theme];
  const maxMb = 200;
  if (!tapes || tapes.length === 0) return <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, padding: 30, textAlign: "center" }}>No tape data</div>;
  return (<div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
    {tapes.map(t => {
      const pct = Math.min((t.size_mb / maxMb) * 100, 100);
      const barColor = pct > 95 ? T.red : pct > 80 ? T.amber : T.green;
      const statusColor = t.status === "HEALTHY" ? T.green : t.status === "STALE" ? T.amber : t.status === "MISSING" ? T.textMuted : T.red;
      const remainMb = Math.max(0, maxMb - t.size_mb);
      return (<div key={t.label}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <div style={{ width: 7, height: 7, borderRadius: "50%", background: statusColor, animation: t.status === "HEALTHY" ? "pulse 2s infinite" : "none" }} />
            <span style={{ fontFamily: FONT, fontSize: 11, fontWeight: 600, color: T.text }}>{t.label}</span>
            <span style={{ fontFamily: MONO, fontSize: 9, fontWeight: 600, color: statusColor }}>{t.status}</span>
          </div>
          <span style={{ fontFamily: MONO, fontSize: 10, fontWeight: 700, color: T.textSecondary }}>{t.size_mb.toFixed(1)} MB</span>
        </div>
        <div style={{ width: "100%", height: 8, background: T.trackBg, borderRadius: 4, overflow: "hidden" }}>
          <div style={{ width: `${pct}%`, height: "100%", borderRadius: 4, background: barColor, transition: "width 0.5s ease" }} />
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", marginTop: 3 }}>
          <span style={{ fontFamily: MONO, fontSize: 8, color: T.textMuted }}>{t.archive_count} archives ({t.archive_mb.toFixed(1)} MB)</span>
          {t.age_ms !== null && <span style={{ fontFamily: MONO, fontSize: 8, color: T.textMuted }}>{t.age_ms < 1000 ? `${t.age_ms.toFixed(0)}ms ago` : `${(t.age_ms / 1000).toFixed(1)}s ago`}</span>}
        </div>
        {pct > 80 && <div style={{ fontFamily: MONO, fontSize: 8, fontWeight: 700, color: pct > 95 ? T.red : T.amber, marginTop: 2 }}>
          ROTATION SOON: {remainMb.toFixed(1)} MB left ({pct.toFixed(0)}%)
        </div>}
      </div>);
    })}
    <div style={{ fontFamily: MONO, fontSize: 8, color: T.textMuted, textAlign: "center", marginTop: 2 }}>Rotates at {maxMb} MB or on the hour</div>
  </div>);
}

// 6. Disk gauge
function DiskGauge({ metrics, theme }) {
  const T = THEMES[theme];
  const diskGb = metrics?.disk_free_gb || 0;
  const maxGb = 500;
  const pct = Math.min((diskGb / maxGb) * 100, 100);
  const color = diskGb < 1 ? T.red : diskGb < 5 ? T.amber : T.green;
  const label = diskGb < 1 ? "CRITICAL" : diskGb < 5 ? "LOW" : "HEALTHY";
  return (<div style={{ textAlign: "center" }}>
    <div style={{ position: "relative", width: 140, height: 140, margin: "0 auto" }}>
      <svg width={140} height={140} viewBox="0 0 140 140">
        <circle cx={70} cy={70} r={58} fill="none" stroke={T.trackBg} strokeWidth={10} />
        <circle cx={70} cy={70} r={58} fill="none" stroke={color} strokeWidth={10}
          strokeDasharray={`${pct * 3.64} ${364 - pct * 3.64}`}
          strokeDashoffset={91} strokeLinecap="round" style={{ transition: "stroke-dasharray 0.5s ease" }} />
      </svg>
      <div style={{ position: "absolute", top: "50%", left: "50%", transform: "translate(-50%, -50%)", textAlign: "center" }}>
        <div style={{ fontFamily: MONO, fontSize: 22, fontWeight: 800, color }}>{diskGb.toFixed(1)}</div>
        <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted }}>GB FREE</div>
      </div>
    </div>
    <div style={{
      fontFamily: MONO, fontSize: 11, fontWeight: 700, color, marginTop: 8,
      padding: "3px 12px", borderRadius: 4, display: "inline-block",
      background: T.isDark ? `${color}15` : `${color}18`,
    }}>{label}</div>
  </div>);
}

// v8.11: Error Details Panel — shows actual log lines when clicking an error bar
function ErrorDetailsPanel({ minute, onClose, theme }) {
  const T = THEMES[theme];
  const [lines, setLines] = useState([]);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    if (!minute) return;
    let active = true;
    setLoading(true);
    fetch(`/api/error-details?minute=${encodeURIComponent(minute)}`)
      .then(r => r.json())
      .then(data => {
        if (!active) return;
        if (data.ok) setLines(data.lines || []);
        setLoading(false);
      })
      .catch(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, [minute]);
  const levelColor = (lv) => lv === "ERROR" || lv === "CRITICAL" ? T.red : T.amber;
  return (
    <div style={{ maxHeight: "80vh", display: "flex", flexDirection: "column" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12, paddingBottom: 10, borderBottom: `1px solid ${T.border}` }}>
        <div>
          <div style={{ fontFamily: FONT, fontSize: 16, fontWeight: 700, color: T.text }}>Error Details — {minute}</div>
          <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, marginTop: 2 }}>
            {loading ? "Loading..." : `${lines.length} error/warning lines`}
          </div>
        </div>
        <button onClick={onClose} style={{ fontFamily: MONO, fontSize: 11, padding: "4px 12px", borderRadius: 6, border: `1px solid ${T.border}`, background: "transparent", color: T.textMuted, cursor: "pointer" }}>Close [Esc]</button>
      </div>
      {!loading && lines.length === 0 && (
        <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, textAlign: "center", padding: 40 }}>
          No error/warning lines found in this minute bucket.
        </div>
      )}
      {!loading && lines.length > 0 && (
        <div style={{ flex: 1, overflowY: "auto", minHeight: 0 }}>
          {lines.map((l, i) => (
            <div key={i} style={{
              padding: "6px 10px", borderBottom: `1px solid ${T.border}`,
              background: i % 2 === 0 ? "transparent" : T.pillBg,
              fontFamily: MONO, fontSize: 10, lineHeight: 1.5,
            }}>
              <span style={{ color: T.textMuted, marginRight: 8 }}>{l.time}</span>
              <span style={{
                display: "inline-block", padding: "1px 5px", borderRadius: 3, fontSize: 8,
                fontWeight: 700, background: levelColor(l.level) + "22", color: levelColor(l.level),
                marginRight: 8, letterSpacing: "0.3px",
              }}>{l.level}</span>
              <span style={{ color: T.text }}>{l.message}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// v8.11: Incident History Panel — shows past incidents when clicking INCIDENTS 24H
function IncidentHistoryPanel({ sla, onClose, theme }) {
  const T = THEMES[theme];
  const incidents = sla?.recent_incidents || [];
  const fmtTs = (ts) => ts ? new Date(ts * 1000).toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "—";
  const fmtDur = (s) => {
    if (!s || s <= 0) return "—";
    if (s < 60) return `${Math.round(s)}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  };
  return (
    <div style={{ maxHeight: "80vh", display: "flex", flexDirection: "column" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12, paddingBottom: 10, borderBottom: `1px solid ${T.border}` }}>
        <div>
          <div style={{ fontFamily: FONT, fontSize: 16, fontWeight: 700, color: T.text }}>Incident History</div>
          <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, marginTop: 2 }}>
            {incidents.length} resolved incidents · {sla?.incidents_24h || 0} in last 24h · {sla?.incidents_7d || 0} in 7d · {sla?.incidents_30d || 0} in 30d
          </div>
        </div>
        <button onClick={onClose} style={{ fontFamily: MONO, fontSize: 11, padding: "4px 12px", borderRadius: 6, border: `1px solid ${T.border}`, background: "transparent", color: T.textMuted, cursor: "pointer" }}>Close [Esc]</button>
      </div>
      {incidents.length === 0 && (
        <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, textAlign: "center", padding: 40 }}>
          No incidents recorded. Everything is running smoothly!
        </div>
      )}
      {incidents.length > 0 && (
        <div style={{ flex: 1, overflowY: "auto", minHeight: 0 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: MONO, fontSize: 10 }}>
            <thead style={{ position: "sticky", top: 0, background: T.card, zIndex: 1 }}>
              <tr>
                {["Started", "Ended", "Duration", "Reason"].map(col => (
                  <th key={col} style={{ padding: "6px 10px", textAlign: "left", fontWeight: 700, color: T.textMuted, borderBottom: `2px solid ${T.border}`, fontSize: 9, textTransform: "uppercase", letterSpacing: "0.5px" }}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {incidents.map((inc, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${T.border}`, background: i % 2 === 0 ? "transparent" : T.pillBg }}>
                  <td style={{ padding: "5px 10px", color: T.text }}>{fmtTs(inc.start_ts)}</td>
                  <td style={{ padding: "5px 10px", color: T.text }}>{fmtTs(inc.end_ts)}</td>
                  <td style={{ padding: "5px 10px", color: inc.duration_s > 120 ? T.red : inc.duration_s > 30 ? T.amber : T.green, fontWeight: 600 }}>{fmtDur(inc.duration_s)}</td>
                  <td style={{ padding: "5px 10px", color: T.textSecondary }}>{inc.reason || "unknown"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// v8.12: Status Detail Panel — shows active incident + recent incident history
// Opened by clicking the STATUS card in SLAPanel
function StatusDetailPanel({ sla, onClose, theme }) {
  const T = THEMES[theme];
  const incidents = sla?.recent_incidents || [];
  const active = sla?.active_incident;

  // Format timestamps nicely
  const fmtTs = (ts) => ts ? new Date(ts * 1000).toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "\u2014";
  const fmtDur = (s) => {
    if (!s || s <= 0) return "\u2014";
    if (s < 60) return `${Math.round(s)}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  };

  // Friendly reason labels
  const reasonLabel = (r) => {
    if (r === "collector_down") return "Collector unreachable";
    if (r === "tape_stale") return "Tape data stale (>30s old)";
    if (r === "rate_zero") return "Event rate dropped to zero";
    return r || "Unknown";
  };

  return (
    <div style={{ maxHeight: "80vh", display: "flex", flexDirection: "column" }}>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16, paddingBottom: 12, borderBottom: `1px solid ${T.border}` }}>
        <div>
          <div style={{ fontFamily: FONT, fontSize: 16, fontWeight: 700, color: T.text }}>System Status</div>
          <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, marginTop: 2 }}>
            {sla?.uptime_pct?.toFixed(2)}% uptime · tracking for {sla?.tracking_since_hours?.toFixed(1) || "?"}h
          </div>
        </div>
        <button onClick={onClose} style={{ fontFamily: MONO, fontSize: 11, padding: "4px 12px", borderRadius: 6, border: `1px solid ${T.border}`, background: "transparent", color: T.textMuted, cursor: "pointer" }}>Close [Esc]</button>
      </div>

      {/* Active Incident Banner */}
      {active ? (
        <div style={{
          background: T.statusOfflineBg || (T.red + "15"),
          border: `1px solid ${T.red}40`,
          borderRadius: 10, padding: "14px 18px", marginBottom: 16,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
            <div style={{ width: 10, height: 10, borderRadius: "50%", background: T.red, animation: "pulse 1s infinite", flexShrink: 0 }} />
            <div style={{ fontFamily: FONT, fontSize: 14, fontWeight: 700, color: T.red }}>Active Incident</div>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginTop: 8 }}>
            <div>
              <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, textTransform: "uppercase", letterSpacing: "0.4px" }}>Reason</div>
              <div style={{ fontFamily: MONO, fontSize: 13, color: T.red, fontWeight: 600, marginTop: 2 }}>{reasonLabel(active.reason)}</div>
            </div>
            <div>
              <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, textTransform: "uppercase", letterSpacing: "0.4px" }}>Duration</div>
              <div style={{ fontFamily: MONO, fontSize: 13, color: T.red, fontWeight: 600, marginTop: 2 }}>{fmtDur(active.duration_s)}</div>
            </div>
            <div>
              <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, textTransform: "uppercase", letterSpacing: "0.4px" }}>Started</div>
              <div style={{ fontFamily: MONO, fontSize: 13, color: T.text, marginTop: 2 }}>{fmtTs(active.start_ts)}</div>
            </div>
          </div>
        </div>
      ) : (
        <div style={{
          background: T.green + "10",
          border: `1px solid ${T.green}30`,
          borderRadius: 10, padding: "14px 18px", marginBottom: 16, textAlign: "center",
        }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 8 }}>
            <div style={{ width: 10, height: 10, borderRadius: "50%", background: T.green, flexShrink: 0 }} />
            <div style={{ fontFamily: FONT, fontSize: 14, fontWeight: 700, color: T.green }}>All Systems Operational</div>
          </div>
          <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, marginTop: 6 }}>
            No active incidents · MTTR {sla?.mttr_s > 0 ? fmtDur(sla.mttr_s) : "\u2014"}
          </div>
        </div>
      )}

      {/* Summary Stats */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 8, marginBottom: 16 }}>
        {[
          { label: "24h", value: sla?.incidents_24h || 0 },
          { label: "7d", value: sla?.incidents_7d || 0 },
          { label: "30d", value: sla?.incidents_30d || 0 },
          { label: "MTTR", value: sla?.mttr_s > 0 ? fmtDur(sla.mttr_s) : "\u2014", isText: true },
        ].map(s => (
          <div key={s.label} style={{ background: T.pillBg, borderRadius: 8, padding: "8px 12px", textAlign: "center" }}>
            <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, textTransform: "uppercase", letterSpacing: "0.4px" }}>{s.label}</div>
            <div style={{ fontFamily: MONO, fontSize: s.isText ? 13 : 18, fontWeight: 700, color: !s.isText && s.value > 0 ? T.amber : T.text, marginTop: 2 }}>{s.value}</div>
          </div>
        ))}
      </div>

      {/* Recent Incidents Table */}
      <div style={{ fontFamily: FONT, fontSize: 13, fontWeight: 600, color: T.text, marginBottom: 8 }}>Recent Incidents</div>
      {incidents.length === 0 ? (
        <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, textAlign: "center", padding: 30, background: T.pillBg, borderRadius: 8 }}>
          No incidents recorded. Everything has been running smoothly!
        </div>
      ) : (
        <div style={{ flex: 1, overflowY: "auto", minHeight: 0 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: MONO, fontSize: 10 }}>
            <thead style={{ position: "sticky", top: 0, background: T.card, zIndex: 1 }}>
              <tr>
                {["Started", "Ended", "Duration", "Reason"].map(col => (
                  <th key={col} style={{ padding: "6px 10px", textAlign: "left", fontWeight: 700, color: T.textMuted, borderBottom: `2px solid ${T.border}`, fontSize: 9, textTransform: "uppercase", letterSpacing: "0.5px" }}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {incidents.map((inc, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${T.border}`, background: i % 2 === 0 ? "transparent" : T.pillBg }}>
                  <td style={{ padding: "5px 10px", color: T.text }}>{fmtTs(inc.start_ts)}</td>
                  <td style={{ padding: "5px 10px", color: T.text }}>{fmtTs(inc.end_ts)}</td>
                  <td style={{ padding: "5px 10px", color: inc.duration_s > 120 ? T.red : inc.duration_s > 30 ? T.amber : T.green, fontWeight: 600 }}>{fmtDur(inc.duration_s)}</td>
                  <td style={{ padding: "5px 10px", color: T.textSecondary }}>{reasonLabel(inc.reason)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// v9.1: Uptime Detail Panel — opened by clicking UPTIME in SLAPanel
// Shows overall uptime percentage, tracking duration, daily uptime breakdown,
// total checks vs down checks, and current status.
function UptimeDetailPanel({ sla, onClose, theme }) {
  const T = THEMES[theme];

  // Format duration from hours
  const fmtHours = (h) => {
    if (!h || h <= 0) return "—";
    if (h < 1) return `${Math.round(h * 60)}m`;
    if (h < 24) return `${h.toFixed(1)}h`;
    return `${Math.floor(h / 24)}d ${Math.round(h % 24)}h`;
  };

  // Color for uptime percentage
  const uptimeColor = (pct) => pct >= 99.9 ? T.green : pct >= 99.0 ? T.amber : T.red;

  // Daily uptime data (from SLA API — last 30 days)
  const daily = sla?.daily || [];

  // Calculate overall stats
  const totalChecks = sla?.total_checks || 0;
  const trackingHours = sla?.tracking_since_hours || 0;
  const uptimePct = sla?.uptime_pct || 0;
  const downChecks = totalChecks > 0 ? Math.round(totalChecks * (1 - uptimePct / 100)) : 0;
  const downtime_s = downChecks * 2; // Each check is 2 seconds

  // Format downtime
  const fmtDowntime = (s) => {
    if (s <= 0) return "0s";
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  };

  return (
    <div style={{ maxHeight: "80vh", display: "flex", flexDirection: "column" }}>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16, paddingBottom: 12, borderBottom: `1px solid ${T.border}` }}>
        <div>
          <div style={{ fontFamily: FONT, fontSize: 16, fontWeight: 700, color: T.text }}>⏱ Uptime Details</div>
          <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, marginTop: 2 }}>
            Tracking for {fmtHours(trackingHours)} · {totalChecks.toLocaleString()} health checks
          </div>
        </div>
        <button onClick={onClose} style={{ fontFamily: MONO, fontSize: 11, padding: "4px 12px", borderRadius: 6, border: `1px solid ${T.border}`, background: "transparent", color: T.textMuted, cursor: "pointer" }}>Close [Esc]</button>
      </div>

      {/* Big uptime display */}
      <div style={{
        background: uptimePct >= 99.9 ? T.green + "10" : uptimePct >= 99.0 ? T.amber + "10" : T.red + "10",
        border: `1px solid ${uptimeColor(uptimePct)}30`,
        borderRadius: 10, padding: "18px 24px", marginBottom: 16, textAlign: "center",
      }}>
        <div style={{ fontFamily: MONO, fontSize: 36, fontWeight: 800, color: uptimeColor(uptimePct) }}>
          {uptimePct.toFixed(3)}%
        </div>
        <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, marginTop: 4 }}>
          Overall Uptime
        </div>
      </div>

      {/* Summary Stats — 4 columns */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 8, marginBottom: 16 }}>
        {[
          { label: "Tracking", value: fmtHours(trackingHours) },
          { label: "Total Checks", value: totalChecks.toLocaleString() },
          { label: "Downtime", value: fmtDowntime(downtime_s) },
          { label: "Incidents", value: `${sla?.incidents_24h || 0} / 24h` },
        ].map(s => (
          <div key={s.label} style={{ background: T.pillBg, borderRadius: 8, padding: "8px 12px", textAlign: "center" }}>
            <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, textTransform: "uppercase", letterSpacing: "0.4px" }}>{s.label}</div>
            <div style={{ fontFamily: MONO, fontSize: 14, fontWeight: 700, color: T.text, marginTop: 2 }}>{s.value}</div>
          </div>
        ))}
      </div>

      {/* Daily Uptime Bar Chart (last 30 days) */}
      {daily.length > 0 && (
        <>
          <div style={{ fontFamily: FONT, fontSize: 13, fontWeight: 600, color: T.text, marginBottom: 8 }}>Daily Uptime (last {daily.length} days)</div>
          <div style={{ flex: 1, overflowY: "auto", minHeight: 0 }}>
            <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
              {daily.slice().reverse().map((d, i) => {
                const pct = d.uptime_pct || 0;
                const barColor = uptimeColor(pct);
                return (
                  <div key={d.date} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <div style={{ fontFamily: MONO, fontSize: 10, color: T.textMuted, width: 70, textAlign: "right", flexShrink: 0 }}>{d.date}</div>
                    <div style={{ flex: 1, height: 16, background: T.pillBg, borderRadius: 4, overflow: "hidden", position: "relative" }}>
                      <div style={{
                        width: `${Math.max(pct, 0.5)}%`, height: "100%",
                        background: barColor, borderRadius: 4,
                        transition: "width 0.3s",
                      }} />
                      <div style={{ position: "absolute", right: 6, top: 1, fontFamily: MONO, fontSize: 9, fontWeight: 600, color: T.text }}>
                        {pct.toFixed(2)}%
                      </div>
                    </div>
                    {d.incidents > 0 && (
                      <div style={{ fontFamily: MONO, fontSize: 9, color: T.amber, fontWeight: 600, width: 30, flexShrink: 0 }}>
                        {d.incidents} inc
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </>
      )}

      {daily.length === 0 && (
        <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, textAlign: "center", padding: 30, background: T.pillBg, borderRadius: 8 }}>
          No daily uptime data recorded yet. Data appears after the first full day of tracking.
        </div>
      )}
    </div>
  );
}

// v9.1: MTTR Detail Panel — opened by clicking MTTR in SLAPanel
// Shows mean time to recovery, per-incident recovery times, fastest/slowest
// recovery, and a breakdown of incident types.
function MttrDetailPanel({ sla, onClose, theme }) {
  const T = THEMES[theme];

  // Format duration
  const fmtDur = (s) => {
    if (!s || s <= 0) return "—";
    if (s < 60) return `${Math.round(s)}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  };
  const fmtTs = (ts) => ts ? new Date(ts * 1000).toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "—";

  // Get incident data
  const incidents = sla?.recent_incidents || [];
  const durations = incidents.filter(inc => inc.duration_s > 0).map(inc => inc.duration_s);

  // Calculate stats
  const mttr = sla?.mttr_s || 0;
  const fastest = durations.length > 0 ? Math.min(...durations) : 0;
  const slowest = durations.length > 0 ? Math.max(...durations) : 0;
  const median = durations.length > 0 ? (() => {
    const sorted = [...durations].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  })() : 0;

  // Reason breakdown
  const reasonCounts = {};
  const reasonDurations = {};
  incidents.forEach(inc => {
    const r = inc.reason || "unknown";
    reasonCounts[r] = (reasonCounts[r] || 0) + 1;
    reasonDurations[r] = (reasonDurations[r] || 0) + (inc.duration_s || 0);
  });

  // Friendly reason labels
  const reasonLabel = (r) => {
    if (r === "collector_down") return "Collector unreachable";
    if (r === "tape_stale") return "Tape data stale";
    if (r === "rate_zero") return "Event rate zero";
    return r || "Unknown";
  };

  return (
    <div style={{ maxHeight: "80vh", display: "flex", flexDirection: "column" }}>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16, paddingBottom: 12, borderBottom: `1px solid ${T.border}` }}>
        <div>
          <div style={{ fontFamily: FONT, fontSize: 16, fontWeight: 700, color: T.text }}>⚡ Mean Time to Recovery</div>
          <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, marginTop: 2 }}>
            {incidents.length} total incidents · {durations.length} with recovery data
          </div>
        </div>
        <button onClick={onClose} style={{ fontFamily: MONO, fontSize: 11, padding: "4px 12px", borderRadius: 6, border: `1px solid ${T.border}`, background: "transparent", color: T.textMuted, cursor: "pointer" }}>Close [Esc]</button>
      </div>

      {/* Big MTTR display */}
      <div style={{
        background: mttr > 120 ? T.red + "10" : mttr > 30 ? T.amber + "10" : T.cyan + "10",
        border: `1px solid ${mttr > 120 ? T.red : mttr > 30 ? T.amber : T.cyan}30`,
        borderRadius: 10, padding: "18px 24px", marginBottom: 16, textAlign: "center",
      }}>
        <div style={{ fontFamily: MONO, fontSize: 36, fontWeight: 800, color: mttr > 120 ? T.red : mttr > 30 ? T.amber : T.cyan }}>
          {fmtDur(mttr)}
        </div>
        <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, marginTop: 4 }}>
          Average Recovery Time
        </div>
      </div>

      {/* Summary Stats — 4 columns */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 8, marginBottom: 16 }}>
        {[
          { label: "MTTR", value: fmtDur(mttr) },
          { label: "Median", value: fmtDur(median) },
          { label: "Fastest", value: fmtDur(fastest) },
          { label: "Slowest", value: fmtDur(slowest) },
        ].map(s => (
          <div key={s.label} style={{ background: T.pillBg, borderRadius: 8, padding: "8px 12px", textAlign: "center" }}>
            <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, textTransform: "uppercase", letterSpacing: "0.4px" }}>{s.label}</div>
            <div style={{ fontFamily: MONO, fontSize: 14, fontWeight: 700, color: T.text, marginTop: 2 }}>{s.value}</div>
          </div>
        ))}
      </div>

      {/* Reason Breakdown */}
      {Object.keys(reasonCounts).length > 0 && (
        <>
          <div style={{ fontFamily: FONT, fontSize: 13, fontWeight: 600, color: T.text, marginBottom: 8 }}>Incident Type Breakdown</div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6, marginBottom: 16 }}>
            {Object.entries(reasonCounts).sort((a, b) => b[1] - a[1]).map(([reason, count]) => (
              <div key={reason} style={{ background: T.pillBg, borderRadius: 8, padding: "8px 12px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <div>
                  <div style={{ fontFamily: MONO, fontSize: 10, color: T.text, fontWeight: 600 }}>{reasonLabel(reason)}</div>
                  <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, marginTop: 2 }}>avg {fmtDur(reasonDurations[reason] / count)}</div>
                </div>
                <div style={{ fontFamily: MONO, fontSize: 16, fontWeight: 700, color: T.amber }}>{count}</div>
              </div>
            ))}
          </div>
        </>
      )}

      {/* Per-incident Recovery Times */}
      <div style={{ fontFamily: FONT, fontSize: 13, fontWeight: 600, color: T.text, marginBottom: 8 }}>Recovery Times by Incident</div>
      {incidents.length === 0 ? (
        <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, textAlign: "center", padding: 30, background: T.pillBg, borderRadius: 8 }}>
          No incidents recorded. MTTR will appear after the first incident is resolved.
        </div>
      ) : (
        <div style={{ flex: 1, overflowY: "auto", minHeight: 0 }}>
          <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            {incidents.map((inc, i) => {
              const dur = inc.duration_s || 0;
              const maxDur = slowest || 1;
              const barPct = Math.min((dur / maxDur) * 100, 100);
              const barColor = dur > 120 ? T.red : dur > 30 ? T.amber : T.green;
              return (
                <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, padding: "4px 0" }}>
                  <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, width: 100, textAlign: "right", flexShrink: 0 }}>{fmtTs(inc.end_ts || inc.start_ts)}</div>
                  <div style={{ flex: 1, height: 18, background: T.pillBg, borderRadius: 4, overflow: "hidden", position: "relative" }}>
                    <div style={{
                      width: `${Math.max(barPct, 2)}%`, height: "100%",
                      background: barColor, borderRadius: 4,
                    }} />
                    <div style={{ position: "absolute", left: 6, top: 2, fontFamily: MONO, fontSize: 9, fontWeight: 600, color: T.text }}>
                      {fmtDur(dur)} — {reasonLabel(inc.reason)}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

// v8.11: Health Details Panel — styled breakdown of health components
function HealthDetailsPanel({ healthScore, healthGrade, healthComponents, onClose, theme }) {
  const T = THEMES[theme];
  // v8.16: Color changes with letter grade — A=green, B=yellow-green, C=amber, D=orange, F=red
  const gradeColor = healthGrade === "A" ? T.green : healthGrade === "B" ? "#8BC34A" :
                     healthGrade === "C" ? T.amber : healthGrade === "D" ? "#ff8c00" : T.red;
  const entries = healthComponents ? Object.entries(healthComponents) : [];
  const scoreColor = (s) => s >= 90 ? T.green : s >= 70 ? T.amber : T.red;
  return (
    <div style={{ maxHeight: "80vh", display: "flex", flexDirection: "column" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16, paddingBottom: 12, borderBottom: `1px solid ${T.border}` }}>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <div style={{ fontFamily: MONO, fontSize: 36, fontWeight: 800, color: gradeColor }}>{healthScore}</div>
          <div>
            <div style={{ fontFamily: FONT, fontSize: 16, fontWeight: 700, color: T.text }}>Health Score — Grade {healthGrade}</div>
            <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, marginTop: 2 }}>Composite score from {entries.length} components</div>
          </div>
        </div>
        <button onClick={onClose} style={{ fontFamily: MONO, fontSize: 11, padding: "4px 12px", borderRadius: 6, border: `1px solid ${T.border}`, background: "transparent", color: T.textMuted, cursor: "pointer" }}>Close [Esc]</button>
      </div>
      {entries.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          {entries.map(([k, v]) => {
            const pct = Math.max(0, Math.min(100, v.score));
            return (
              <div key={k} style={{ background: T.pillBg, borderRadius: 10, padding: "12px 16px", border: `1px solid ${T.border}` }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
                  <span style={{ fontFamily: FONT, fontSize: 13, fontWeight: 700, color: T.text, textTransform: "capitalize" }}>{k}</span>
                  <span style={{ fontFamily: MONO, fontSize: 12, fontWeight: 700, color: scoreColor(v.score) }}>{v.score}/100 <span style={{ color: T.textMuted, fontWeight: 500, fontSize: 10 }}>({v.weight}% weight)</span></span>
                </div>
                {/* Progress bar */}
                <div style={{ height: 6, background: T.border, borderRadius: 3, overflow: "hidden", marginBottom: 6 }}>
                  <div style={{ height: "100%", width: `${pct}%`, background: scoreColor(v.score), borderRadius: 3, transition: "width 0.5s ease" }} />
                </div>
                <div style={{ fontFamily: MONO, fontSize: 10, color: T.textMuted }}>{v.detail}</div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// v9.0: Metric Detail Panel — universal click-to-expand panel for all 11 STATUS row metrics.
// Opens in FullscreenOverlay with summary stats + D3 sparkline chart.
// Uses the same lane-sparkline pattern as SystemVitals for consistency.
function MetricDetailPanel({ metricKey, history, pbMetrics, procStats, onClose, theme, timeWindow, onTimeWindowChange }) {
  const T = THEMES[theme];
  const svgRef = useRef(null);
  const hoverRef = useRef(null);
  const chartWrapRef = useRef(null);
  // v9.0 fix: Responsive chart width — measure actual container instead of hardcoded 900
  const [chartW, setChartW] = useState(800);
  useEffect(() => {
    if (!chartWrapRef.current) return;
    const measure = () => { const r = chartWrapRef.current?.getBoundingClientRect(); if (r && r.width > 100) setChartW(Math.floor(r.width - 32)); };
    measure();
    const obs = new ResizeObserver(measure);
    obs.observe(chartWrapRef.current);
    return () => obs.disconnect();
  }, []);
  const w = chartW;

  // ── Metric definitions: what to show for each metric key ──
  const METRIC_DEFS = {
    events: {
      title: "Events / Second",
      icon: "E/s",
      lanes: [
        { key: "rate", label: "Total Rate", color: T.green, fmt: v => v.toFixed(1) + " evt/s" },
        { key: "cbRate", label: "Coinbase", color: T.cyan, fmt: v => v.toFixed(1) + " evt/s" },
        { key: "klRate", label: "Kalshi", color: T.purple, fmt: v => v.toFixed(1) + " evt/s" },
      ],
      statsFn: (h) => {
        const rates = h.map(d => d.rate || 0);
        return [
          { label: "Current", value: rates.length ? rates[rates.length - 1].toFixed(1) : "0" },
          { label: "Peak", value: Math.max(...rates).toFixed(1) },
          { label: "Avg", value: (rates.reduce((a, b) => a + b, 0) / (rates.length || 1)).toFixed(1) },
          { label: "Total Events", value: (h.length ? (h[h.length - 1].cb || 0) + (h[h.length - 1].kl || 0) : 0).toLocaleString() },
        ];
      },
    },
    latency: {
      title: "Latency (ms)",
      icon: "ms",
      lanes: [
        { key: "p50", label: "p50", color: T.cyan, fmt: v => Math.round(v) + "ms" },
        { key: "p95", label: "p95", color: T.amber, fmt: v => Math.round(v) + "ms" },
        { key: "p99", label: "p99", color: T.red, fmt: v => Math.round(v) + "ms" },
      ],
      statsFn: (h) => {
        // v9.1 fix: Don't filter v > 0 — latency can be negative (raw offset)
        const p50s = h.map(d => d.p50).filter(v => v != null && v !== 0);
        const p95s = h.map(d => d.p95).filter(v => v != null && v !== 0);
        const p99s = h.map(d => d.p99).filter(v => v != null && v !== 0);
        const avg = arr => arr.length ? (arr.reduce((a, b) => a + b, 0) / arr.length) : 0;
        const fmt = v => Math.abs(v) >= 100 ? Math.round(v) + "ms" : v.toFixed(1) + "ms";
        return [
          { label: "p50 Now", value: p50s.length ? fmt(p50s[p50s.length - 1]) : "—" },
          { label: "p95 Now", value: p95s.length ? fmt(p95s[p95s.length - 1]) : "—" },
          { label: "p99 Now", value: p99s.length ? fmt(p99s[p99s.length - 1]) : "—" },
          { label: "p50 Avg", value: p50s.length ? fmt(avg(p50s)) : "—" },
        ];
      },
    },
    queue: {
      title: "Queue Depth",
      icon: "Q",
      lanes: [
        { key: "queue", label: "Queue Depth", color: T.blue, fmt: v => v.toLocaleString(), warnFn: v => v > 100 },
      ],
      statsFn: (h) => {
        const vals = h.map(d => d.queue || 0);
        return [
          { label: "Current", value: vals.length ? vals[vals.length - 1].toLocaleString() : "0" },
          { label: "Peak", value: Math.max(...vals).toLocaleString() },
          { label: "Avg", value: Math.round(vals.reduce((a, b) => a + b, 0) / (vals.length || 1)).toLocaleString() },
          { label: "Samples", value: vals.length.toLocaleString() },
        ];
      },
    },
    disk: {
      title: "Disk Free (GB)",
      icon: "HDD",
      lanes: [
        { key: "disk", label: "Disk Free", color: T.green, fmt: v => v.toFixed(1) + " GB", warnFn: v => v < 5 },
      ],
      statsFn: (h) => {
        const vals = h.map(d => d.disk || 0).filter(v => v > 0);
        const first = vals.length > 10 ? vals[0] : 0;
        const last = vals.length ? vals[vals.length - 1] : 0;
        const trend = last - first;
        return [
          { label: "Current", value: last.toFixed(1) + " GB" },
          { label: "Min", value: vals.length ? Math.min(...vals).toFixed(1) + " GB" : "—" },
          { label: "Max", value: vals.length ? Math.max(...vals).toFixed(1) + " GB" : "—" },
          { label: "Trend", value: (trend >= 0 ? "+" : "") + trend.toFixed(2) + " GB" },
        ];
      },
    },
    seq: {
      title: "Sequence Number",
      icon: "SEQ",
      lanes: [
        { key: "seqRate", label: "Seq/s Growth", color: T.purple, fmt: v => v >= 1000 ? (v / 1000).toFixed(1) + "K/s" : Math.round(v) + "/s" },
      ],
      // We need to compute seqRate from history, same as SystemVitals does
      enrichFn: (h) => {
        let _lastSeq = 0, _lastSeqTs = 0, _seqRate = 0;
        return h.map((d, i) => {
          if (i === 0) { _lastSeq = d.seq; _lastSeqTs = d.ts; return { ...d, seqRate: 0 }; }
          if (d.seq !== _lastSeq) {
            const dt = Math.max(1, d.ts - _lastSeqTs);
            const ds = Math.max(0, d.seq - _lastSeq);
            _seqRate = ds / dt;
            _lastSeq = d.seq; _lastSeqTs = d.ts;
          }
          return { ...d, seqRate: _seqRate };
        });
      },
      statsFn: (h) => {
        const last = h.length ? h[h.length - 1] : {};
        return [
          { label: "Current Seq", value: (last.seq || 0).toLocaleString() },
          { label: "Seq/s", value: (last.seqRate || 0) >= 1000 ? ((last.seqRate || 0) / 1000).toFixed(1) + "K" : Math.round(last.seqRate || 0).toString() },
          { label: "Window", value: h.length + " samples" },
          { label: "Est. Daily", value: ((last.seqRate || 0) * 86400 / 1e6).toFixed(1) + "M" },
        ];
      },
    },
    uptime: {
      title: "Uptime",
      icon: "UP",
      lanes: [], // No chart for uptime — text only
      statsFn: (h, pb) => {
        const secs = pb?.uptime_seconds || 0;
        const hrs = Math.floor(secs / 3600);
        const mins = Math.floor((secs % 3600) / 60);
        const startTs = new Date(Date.now() - secs * 1000);
        return [
          { label: "Uptime", value: hrs > 0 ? `${hrs}h ${mins}m` : `${mins}m ${Math.floor(secs % 60)}s` },
          { label: "Started", value: startTs.toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }) },
          { label: "Hours", value: (secs / 3600).toFixed(1) },
          { label: "Restarts", value: ((pb?.reconnects_total?.coinbase || 0) + (pb?.reconnects_total?.kalshi || 0)).toString() },
        ];
      },
    },
    process: {
      title: "Process Memory",
      icon: "MEM",
      lanes: [], // Process stats don't have history in the main buffer
      statsFn: (h, pb, ps) => {
        const stats = ps?.stats;
        const uptimeSecs = pb?.uptime_seconds || 0;
        const uptimeStr = uptimeSecs >= 3600 ? Math.floor(uptimeSecs / 3600) + "h " + Math.floor((uptimeSecs % 3600) / 60) + "m" : Math.floor(uptimeSecs / 60) + "m " + Math.floor(uptimeSecs % 60) + "s";
        return [
          { label: "Memory", value: stats ? stats.memory_mb + " MB" : "—" },
          { label: "PID", value: stats ? stats.pid.toString() : "—" },
          { label: "Uptime", value: uptimeStr },
          { label: "Events Total", value: ((pb?.events_total?.coinbase || 0) + (pb?.events_total?.kalshi || 0)).toLocaleString() },
        ];
      },
    },
    dedup: {
      title: "Deduplication",
      icon: "DD",
      lanes: [], // Dedup is a cumulative counter, no useful time series
      statsFn: (h, pb) => {
        const total = pb?.dedup_total || 0;
        const events = (pb?.events_total?.coinbase || 0) + (pb?.events_total?.kalshi || 0);
        const ratio = events > 0 ? ((total / events) * 100).toFixed(2) : "0";
        return [
          { label: "Total Dedups", value: total.toLocaleString() },
          { label: "Total Events", value: events.toLocaleString() },
          { label: "Dedup Ratio", value: ratio + "%" },
          { label: "Net Events", value: (events - total).toLocaleString() },
        ];
      },
    },
    reconnects: {
      title: "Reconnects",
      icon: "RC",
      lanes: [], // Reconnects are rare events, no useful sparkline
      statsFn: (h, pb) => {
        const cbRc = pb?.reconnects_total?.coinbase || 0;
        const klRc = pb?.reconnects_total?.kalshi || 0;
        return [
          { label: "Coinbase", value: cbRc.toString() },
          { label: "Kalshi", value: klRc.toString() },
          { label: "Total", value: (cbRc + klRc).toString() },
          { label: "Status", value: (cbRc + klRc) > 5 ? "Elevated" : "Normal" },
        ];
      },
    },
    bandwidth: {
      title: "Bandwidth",
      icon: "BW",
      lanes: [
        { key: "bytesPerSec", label: "Bytes/sec", color: T.blue, fmt: v => v >= 1024 ? (v / 1024).toFixed(1) + " KB/s" : Math.round(v) + " B/s" },
        { key: "msgSizeCb", label: "CB Msg Size", color: T.cyan, fmt: v => v >= 1024 ? (v / 1024).toFixed(1) + " KB" : Math.round(v) + " B" },
        { key: "msgSizeKl", label: "KL Msg Size", color: T.purple, fmt: v => v >= 1024 ? (v / 1024).toFixed(1) + " KB" : Math.round(v) + " B" },
      ],
      statsFn: (h, pb) => {
        const bps = h.map(d => d.bytesPerSec || 0);
        const cur = bps.length ? bps[bps.length - 1] : 0;
        const peak = Math.max(...bps);
        const avg = bps.reduce((a, b) => a + b, 0) / (bps.length || 1);
        const fmt = v => v >= 1024 ? (v / 1024).toFixed(1) + " KB/s" : Math.round(v) + " B/s";
        return [
          { label: "Current", value: fmt(cur) },
          { label: "Peak", value: fmt(peak) },
          { label: "Average", value: fmt(avg) },
          { label: "CB Avg Msg", value: Math.round(pb?.msg_size_avg?.coinbase || 0) + " B" },
        ];
      },
    },
    wsPing: {
      title: "Connection Health",
      icon: "RTT",
      lanes: [
        { key: "wsRttCb", label: "Coinbase RTT", color: T.green, fmt: v => v < 0 ? "N/A" : Math.round(v) + "ms" },
        { key: "kalshi_age", label: "Kalshi Freshness", color: T.purple, fmt: v => v == null ? "N/A" : v >= 1000 ? (v / 1000).toFixed(1) + "s" : Math.round(v) + "ms" },
      ],
      statsFn: (h, pb) => {
        const cbRtts = h.map(d => d.wsRttCb).filter(v => v >= 0);
        const klAges = h.map(d => d.kalshi_age).filter(v => v != null && v >= 0);
        const avg = arr => arr.length ? Math.round(arr.reduce((a, b) => a + b, 0) / arr.length) : -1;
        const fmtAge = v => v == null || v < 0 ? "N/A" : v >= 1000 ? (v / 1000).toFixed(1) + "s" : Math.round(v) + "ms";
        const cbNow = pb?.ws_rtt_ms?.coinbase ?? -1;
        const klAgeNow = klAges.length ? klAges[klAges.length - 1] : null;
        return [
          { label: "CB RTT", value: cbNow >= 0 ? Math.round(cbNow) + "ms" : (cbRtts.length ? Math.round(cbRtts[cbRtts.length - 1]) + "ms" : "N/A") },
          { label: "CB Avg", value: avg(cbRtts) >= 0 ? avg(cbRtts) + "ms" : "N/A" },
          { label: "KL Fresh", value: fmtAge(klAgeNow) },
          { label: "KL Avg", value: fmtAge(avg(klAges) >= 0 ? avg(klAges) : null) },
        ];
      },
    },
  };

  const def = METRIC_DEFS[metricKey];
  if (!def) return null;

  // Enrich history if needed (e.g., compute seqRate)
  const chartHistory = useMemo(() => {
    if (def.enrichFn) return def.enrichFn(history);
    return history;
  }, [history, metricKey]);

  // Compute summary stats
  const stats = useMemo(() => {
    const enriched = def.enrichFn ? def.enrichFn(history) : history;
    return def.statsFn(enriched, pbMetrics, procStats);
  }, [history, pbMetrics, procStats, metricKey]);

  // ── D3 Sparkline Chart (reuses SystemVitals lane pattern) ──
  const lanes = def.lanes || [];
  const timelineH = 22; // height reserved for x-axis timeline labels
  useEffect(() => {
    if (!svgRef.current || lanes.length === 0 || chartHistory.length < 3) return;
    const laneH = lanes.length === 1 ? 140 : lanes.length === 2 ? 100 : 70;
    const labelH = 14;
    const h = laneH * lanes.length + 8 + timelineH;
    const chartAreaH = laneH * lanes.length + 8; // chart area without timeline
    const svg = d3.select(svgRef.current);
    svg.attr("width", w).attr("height", h);
    svg.selectAll("*").remove();

    // Store lane data for hover crosshair
    const laneData = [];

    lanes.forEach((lane, li) => {
      const y0 = li * laneH + 2;
      const sparkH = laneH - 6 - labelH;
      const g = svg.append("g").attr("transform", `translate(0,${y0})`);

      // Extract values — filter out negatives for RTT lanes
      let vals = chartHistory.map(d => d[lane.key] ?? 0);
      if (lane.key === "wsRttCb") {
        vals = vals.map(v => v < 0 ? 0 : v);
      }
      const minV = d3.min(vals); const maxV = d3.max(vals);
      // v9.0 fix: Prevent "zoomed in" look by always including 0 in the domain
      // and adding generous padding. For metrics that are always high (like disk GB),
      // use 0 as floor so variations appear proportional to actual scale.
      let domLo, domHi;
      if (lane.key === "disk") {
        // Disk: show from 0 so small GB changes don't look dramatic
        domLo = 0;
        domHi = maxV * 1.05;
      } else if (minV >= 0) {
        // Non-negative metrics: start from 0
        domLo = 0;
        domHi = maxV * 1.1 || 1;
      } else {
        // Metrics that can go negative: symmetric padding
        const range = maxV - minV || 1;
        domLo = minV - range * 0.15;
        domHi = maxV + range * 0.15;
      }
      const yScale = d3.scaleLinear().domain([domLo, domHi]).range([sparkH, 0]);

      // Separator line between lanes
      if (li > 0) {
        svg.append("line")
          .attr("x1", 0).attr("x2", w)
          .attr("y1", y0 - 1).attr("y2", y0 - 1)
          .attr("stroke", T.border).attr("stroke-opacity", 0.5);
      }

      // Label
      g.append("text")
        .attr("x", 6).attr("y", 11)
        .attr("fill", lane.color).attr("font-size", 10)
        .attr("font-family", MONO).attr("font-weight", 600)
        .text(lane.label);

      // Current value (right-aligned)
      g.append("text")
        .attr("x", w - 6).attr("y", 11)
        .attr("fill", T.textMuted).attr("font-size", 9)
        .attr("font-family", MONO).attr("text-anchor", "end")
        .text(lane.fmt(vals[vals.length - 1] || 0));

      // Filled area
      const area = d3.area()
        .x((_, i) => (i / (vals.length - 1)) * w)
        .y0(sparkH).y1(d => yScale(d))
        .curve(d3.curveMonotoneX);
      g.append("path").datum(vals).attr("d", area)
        .attr("fill", lane.color).attr("opacity", 0.12)
        .attr("transform", `translate(0,${labelH})`);

      // Line
      const line = d3.line()
        .x((_, i) => (i / (vals.length - 1)) * w)
        .y(d => yScale(d))
        .curve(d3.curveMonotoneX);
      g.append("path").datum(vals).attr("d", line)
        .attr("fill", "none").attr("stroke", lane.color)
        .attr("stroke-width", 1.5).attr("opacity", 0.9)
        .attr("transform", `translate(0,${labelH})`);

      laneData.push({ vals, yScale, lane, y0, sparkH, labelH });
    });

    // ── X-axis timeline labels ──
    const tlG = svg.append("g").attr("transform", `translate(0,${chartAreaH})`);
    // Draw separator line above timeline
    tlG.append("line").attr("x1", 0).attr("x2", w).attr("y1", 0).attr("y2", 0)
      .attr("stroke", T.border).attr("stroke-opacity", 0.4);
    // Place ~6-8 evenly spaced time labels
    const nLabels = Math.min(8, Math.max(3, Math.floor(w / 120)));
    for (let ti = 0; ti < nLabels; ti++) {
      const frac = ti / (nLabels - 1);
      const idx = Math.round(frac * (chartHistory.length - 1));
      const entry = chartHistory[idx];
      if (entry && entry.ts) {
        const d = new Date(entry.ts * 1000);
        const label = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
        tlG.append("text")
          .attr("x", frac * w).attr("y", 14)
          .attr("fill", T.textMuted).attr("font-size", 9)
          .attr("font-family", MONO).attr("text-anchor", ti === 0 ? "start" : ti === nLabels - 1 ? "end" : "middle")
          .text(label);
        // Tick mark
        tlG.append("line")
          .attr("x1", frac * w).attr("x2", frac * w)
          .attr("y1", 0).attr("y2", 4)
          .attr("stroke", T.textMuted).attr("stroke-opacity", 0.4);
      }
    }

    // Hover crosshair
    const hoverLine = svg.append("line")
      .attr("y1", 0).attr("y2", chartAreaH)
      .attr("stroke", T.textMuted).attr("stroke-opacity", 0)
      .attr("stroke-dasharray", "3,3");
    const hoverTexts = laneData.map((ld, i) => {
      return svg.append("text")
        .attr("fill", ld.lane.color).attr("font-size", 9)
        .attr("font-family", MONO).attr("opacity", 0);
    });
    const hoverTime = svg.append("text")
      .attr("fill", T.text).attr("font-size", 9)
      .attr("font-family", MONO).attr("font-weight", 600).attr("opacity", 0);

    // Invisible hover rect
    svg.append("rect")
      .attr("width", w).attr("height", chartAreaH)
      .attr("fill", "transparent").attr("cursor", "crosshair")
      .on("mousemove", function(event) {
        hoverRef.current = [event.clientX, event.clientY];
        const [mx] = d3.pointer(event);
        const idx = Math.round((mx / w) * (chartHistory.length - 1));
        if (idx < 0 || idx >= chartHistory.length) return;
        hoverLine.attr("x1", mx).attr("x2", mx).attr("stroke-opacity", 0.5);
        laneData.forEach((ld, i) => {
          const val = ld.vals[idx] || 0;
          const textY = ld.y0 + ld.labelH + ld.yScale(val) - 4;
          hoverTexts[i]
            .attr("x", mx + 8).attr("y", textY)
            .text(ld.lane.fmt(val)).attr("opacity", 1);
        });
        // Show timestamp on hover
        const entry = chartHistory[idx];
        if (entry && entry.ts) {
          const d = new Date(entry.ts * 1000);
          hoverTime.attr("x", mx < w / 2 ? mx + 8 : mx - 8)
            .attr("text-anchor", mx < w / 2 ? "start" : "end")
            .attr("y", chartAreaH + 14)
            .text(d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }))
            .attr("opacity", 1);
        }
      })
      .on("mouseleave", function() {
        hoverRef.current = null;
        hoverLine.attr("stroke-opacity", 0);
        hoverTexts.forEach(t => t.attr("opacity", 0));
        hoverTime.attr("opacity", 0);
      });
    _restoreHover(svg, hoverRef);
  }, [chartHistory, w, theme, metricKey]);

  return (
    <div style={{ maxHeight: "80vh", display: "flex", flexDirection: "column" }}>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16, paddingBottom: 12, borderBottom: `1px solid ${T.border}` }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={{ fontSize: 18 }}>{def.icon}</span>
          <div>
            <div style={{ fontFamily: FONT, fontSize: 16, fontWeight: 700, color: T.text }}>{def.title}</div>
            <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, marginTop: 2 }}>
              {history.length} data points · {history.length > 1 ? Math.round((history[history.length - 1].ts - history[0].ts) / 60) + " min window" : "—"}
            </div>
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {/* v9.1: Time window selector — only show for metrics with chart lanes */}
          {onTimeWindowChange && lanes.length > 0 && <TimeWindowSelector value={timeWindow} onChange={onTimeWindowChange} theme={theme} />}
          <button onClick={onClose} style={{ fontFamily: MONO, fontSize: 11, padding: "4px 12px", borderRadius: 6, border: `1px solid ${T.border}`, background: "transparent", color: T.textMuted, cursor: "pointer" }}>Close [Esc]</button>
        </div>
      </div>

      {/* Summary Stats */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 8, marginBottom: 16 }}>
        {stats.map(s => (
          <div key={s.label} style={{ background: T.pillBg, borderRadius: 8, padding: "8px 12px", textAlign: "center" }}>
            <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, textTransform: "uppercase", letterSpacing: "0.4px" }}>{s.label}</div>
            <div style={{ fontFamily: MONO, fontSize: 16, fontWeight: 700, color: T.text, marginTop: 2 }}>{s.value}</div>
          </div>
        ))}
      </div>

      {/* Chart Area */}
      {lanes.length > 0 && chartHistory.length >= 3 ? (
        <div ref={chartWrapRef} style={{ background: T.pillBg, borderRadius: 10, padding: "12px 16px", flex: 1, overflowY: "auto", minHeight: 0 }}>
          <svg ref={svgRef} />
        </div>
      ) : lanes.length === 0 ? null : (
        <div style={{ background: T.pillBg, borderRadius: 10, padding: "20px 16px", textAlign: "center" }}>
          <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted }}>
            Not enough data points to render chart. Collecting...
          </div>
        </div>
      )}
    </div>
  );
}

// 7. Error Rate Panel — sparkline bar chart
function ErrorRatePanel({ buckets, ok, theme, width, timeWindow, height, fillHeight, onBarClick }) {
  const T = THEMES[theme];
  const svgRef = useRef(null);
  const hoverRef = useRef(null);
  const wrapRef = useRef(null);
  const w = width || 300;

  // Build continuous minute-by-minute data — fills in 0s for minutes with no errors
  const continuousData = useMemo(() => {
    if (!ok) return [];
    const windowMin = Math.ceil((timeWindow || 3600) / 60);
    const now = new Date();
    // Build map from backend buckets: "HH:MM" -> {errors, warnings}
    const bucketMap = {};
    (buckets || []).forEach(b => { bucketMap[b.time] = b; });
    // Generate every minute in the window, newest last
    const minutes = [];
    for (let i = windowMin - 1; i >= 0; i--) {
      const d = new Date(now.getTime() - i * 60000);
      const key = String(d.getHours()).padStart(2, "0") + ":" + String(d.getMinutes()).padStart(2, "0");
      const existing = bucketMap[key];
      minutes.push(existing ? { time: key, errors: existing.errors, warnings: existing.warnings } : { time: key, errors: 0, warnings: 0 });
    }
    return minutes;
  }, [buckets, ok, timeWindow]);

  useEffect(() => {
    if (!svgRef.current || !ok || continuousData.length < 1) return;
    // Measure available height from wrapper (runs after layout)
    let h = height || 100;
    if (fillHeight && wrapRef.current) {
      const wh = wrapRef.current.getBoundingClientRect().height;
      if (wh > 40) h = wh - 14; // subtract legend row (~11.5px + 2px margin)
    }
    const data = continuousData;
    const svg = d3.select(svgRef.current);
    svg.attr("height", h); // set SVG to measured height
    svg.selectAll("*").remove();
    const pad = { t: 8, r: 8, b: 18, l: 30 };
    const iw = w - pad.l - pad.r, ih = h - pad.t - pad.b;
    const maxVal = Math.max(1, d3.max(data, d => d.errors + d.warnings));
    const x = d3.scaleBand().domain(d3.range(data.length)).range([0, iw]).padding(0.08);
    const y = d3.scaleLinear().domain([0, maxVal]).range([ih, 0]);
    const g = svg.append("g").attr("transform", `translate(${pad.l},${pad.t})`);
    // Bars: warnings bottom (amber), errors stacked on top (red)
    // Only draw bars where there are actual values (empty minutes show nothing)
    g.selectAll(".bw").data(data).enter().append("rect")
      .attr("x", (_, i) => x(i)).attr("y", d => y(d.warnings)).attr("width", x.bandwidth())
      .attr("height", d => ih - y(d.warnings)).attr("fill", T.amber).attr("opacity", 0.6).attr("rx", 1);
    g.selectAll(".be").data(data).enter().append("rect")
      .attr("x", (_, i) => x(i)).attr("y", d => y(d.warnings + d.errors)).attr("width", x.bandwidth())
      .attr("height", d => y(d.warnings) - y(d.warnings + d.errors)).attr("fill", T.red).attr("opacity", 0.7).attr("rx", 1);
    // Y label
    g.selectAll(".yl").data(y.ticks(3)).enter().append("text")
      .attr("x", -6).attr("y", d => y(d) + 3).attr("text-anchor", "end")
      .attr("fill", T.textMuted).attr("font-size", 8).attr("font-family", MONO).text(d => d);
    // Time labels (first + last bucket times)
    if (data.length > 0) {
      g.append("text").attr("x", 0).attr("y", ih + 14).attr("fill", T.textMuted).attr("font-size", 8).attr("font-family", MONO).text(data[0].time);
      g.append("text").attr("x", iw).attr("y", ih + 14).attr("text-anchor", "end").attr("fill", T.textMuted).attr("font-size", 8).attr("font-family", MONO).text(data[data.length - 1].time);
    }
    // Hover crosshair + tooltip + v8.11: click to inspect errors
    const hoverLine = g.append("line").attr("y1", 0).attr("y2", ih).attr("stroke", T.text).attr("stroke-width", 1).attr("stroke-dasharray", "3,2").attr("opacity", 0).attr("pointer-events", "none");
    const hoverText = g.append("text").attr("fill", T.text).attr("font-size", 9).attr("font-family", MONO).attr("font-weight", 700).attr("opacity", 0).attr("pointer-events", "none");
    const hoverHint = g.append("text").attr("fill", T.textMuted).attr("font-size", 7).attr("font-family", MONO).attr("opacity", 0).attr("pointer-events", "none");
    let hoverIdx = -1;
    g.append("rect").attr("width", iw).attr("height", ih).attr("fill", "transparent")
      .attr("cursor", onBarClick ? "pointer" : "default")
      .on("mousemove", function(event) {
        hoverRef.current = [event.clientX, event.clientY];
        const [mx] = d3.pointer(event);
        let idx = -1, minD = Infinity;
        for (let i = 0; i < data.length; i++) {
          const cx = x(i) + x.bandwidth() / 2;
          if (Math.abs(mx - cx) < minD) { minD = Math.abs(mx - cx); idx = i; }
        }
        if (idx < 0) return;
        hoverIdx = idx;
        const cx = x(idx) + x.bandwidth() / 2;
        const d = data[idx];
        hoverLine.attr("x1", cx).attr("x2", cx).attr("opacity", 0.5);
        const label = `${d.time}  ${d.errors}E  ${d.warnings}W`;
        const tx = cx < iw / 2 ? cx + 8 : cx - 8;
        const anchor = cx < iw / 2 ? "start" : "end";
        hoverText.attr("x", tx).attr("y", 10).attr("text-anchor", anchor).text(label).attr("opacity", 1);
        if (onBarClick && (d.errors > 0 || d.warnings > 0)) {
          hoverHint.attr("x", tx).attr("y", 22).attr("text-anchor", anchor).text("click for details").attr("opacity", 0.6);
        } else {
          hoverHint.attr("opacity", 0);
        }
      })
      .on("mouseleave", function() { hoverRef.current = null; hoverLine.attr("opacity", 0); hoverText.attr("opacity", 0); hoverHint.attr("opacity", 0); hoverIdx = -1; })
      .on("click", function() {
        if (onBarClick && hoverIdx >= 0 && data[hoverIdx]) {
          const d = data[hoverIdx];
          if (d.errors > 0 || d.warnings > 0) onBarClick(d.time);
        }
      });
    _restoreHover(svg, hoverRef);
  }, [continuousData, w, height, fillHeight, theme, ok]);
  // Totals only count what's visible in the window
  const totalE = continuousData.reduce((s, b) => s + b.errors, 0);
  const totalW = continuousData.reduce((s, b) => s + b.warnings, 0);
  return (<div ref={wrapRef} style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
    {!ok ? (
      <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, padding: 20, textAlign: "center", flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}>No error data</div>
    ) : (<>
      <svg ref={svgRef} width={w} height={height || 100} style={{ display: "block" }} />
      <div style={{ display: "flex", justifyContent: "center", gap: 16, marginTop: 2, flexShrink: 0 }}>
        <span style={{ fontFamily: MONO, fontSize: 9, color: totalE > 0 ? T.red : T.textMuted }}>{totalE} errors</span>
        <span style={{ fontFamily: MONO, fontSize: 9, color: totalW > 0 ? T.amber : T.textMuted }}>{totalW} warnings</span>
      </div>
    </>)}
  </div>);
}

// 8. Gap Detector Panel — lane chart showing data flow continuity
function GapDetectorPanel({ history, theme, width, height, timeWindow, fillHeight }) {
  const T = THEMES[theme];
  const svgRef = useRef(null);
  const wrapRef = useRef(null);
  const w = width || 300;
  useEffect(() => {
    if (!svgRef.current || history.length < 2) return;
    // Measure available height from wrapper (runs after layout)
    let h = height || 80;
    if (fillHeight && wrapRef.current) {
      const wh = wrapRef.current.getBoundingClientRect().height;
      if (wh > 40) h = wh - 14; // subtract legend row (~11.5px + 2px margin)
    }
    const svg = d3.select(svgRef.current);
    svg.attr("height", h); // set SVG to measured height
    svg.selectAll("*").remove();
    const pad = { t: 6, r: 8, b: 14, l: 8 };
    const iw = w - pad.l - pad.r, ih = h - pad.t - pad.b;
    const g = svg.append("g").attr("transform", `translate(${pad.l},${pad.t})`);
    const rectW = Math.max(1, iw / history.length);
    // Color by rate: green if rate > 0, red if 0, amber if low
    // v8.8: sliding window handles playback — no cursor/dimming needed
    history.forEach((e, i) => {
      let fill;
      if (e.rate <= 0) fill = T.red;
      else if (e.rate < 10) fill = T.amber;
      else fill = T.green;
      g.append("rect").attr("x", i * rectW).attr("y", 0)
        .attr("width", Math.max(rectW - 0.5, 1)).attr("height", ih)
        .attr("rx", 1).attr("fill", fill).attr("opacity", 0.6);
    });
    // Time labels — scale with actual time window
    const tw = timeWindow || (history.length > 1 ? (history[history.length - 1].ts - history[0].ts) : 60);
    const durLabel = tw >= 3600 ? `${Math.floor(tw / 3600)}h` : `${Math.floor(tw / 60)}m`;
    g.append("text").attr("x", 0).attr("y", ih + 12).attr("fill", T.textMuted).attr("font-size", 8).attr("font-family", MONO).text(`-${durLabel}`);
    g.append("text").attr("x", iw).attr("y", ih + 12).attr("text-anchor", "end").attr("fill", T.textMuted).attr("font-size", 8).attr("font-family", MONO).text("now");
  }, [history, w, height, fillHeight, theme, timeWindow]);
  const hasData = history.length >= 2;
  const gaps = hasData ? history.filter(e => e.rate <= 0).length : 0;
  return (<div ref={wrapRef} style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
    {!hasData ? (
      <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, padding: 20, textAlign: "center", flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}>Building gap data...</div>
    ) : (<>
      <svg ref={svgRef} width={w} height={height || 80} style={{ display: "block" }} />
      <div style={{ display: "flex", justifyContent: "center", gap: 16, marginTop: 2, flexShrink: 0 }}>
        <span style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, display: "flex", alignItems: "center", gap: 4 }}>
          <div style={{ width: 10, height: 8, borderRadius: 2, background: T.green, opacity: 0.6 }} />Flowing
        </span>
        <span style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, display: "flex", alignItems: "center", gap: 4 }}>
          <div style={{ width: 10, height: 8, borderRadius: 2, background: T.red, opacity: 0.6 }} />Gap ({gaps})
        </span>
      </div>
    </>)}
  </div>);
}

// 9. Tape Freshness Timeline
function FreshnessTimeline({ freshHistory, theme, width, timeWindow }) {
  const T = THEMES[theme];
  const svgRef = useRef(null);
  const lanes = ["unified", "kalshi", "oracle"];
  const h = 110; const w2 = width || 500;
  useEffect(() => {
    if (!svgRef.current || freshHistory.length < 2) return;
    // Filter by time window
    const cutoffTs = freshHistory[freshHistory.length - 1].ts - (timeWindow || 3600);
    let data = freshHistory.filter(e => e.ts >= cutoffTs);
    if (data.length < 2) return;
    // Downsample to max 600 points to prevent SVG lag on long timespans (6h+)
    // Each point creates 3 rects (one per lane), so 600 pts = 1800 rects max
    if (data.length > 600) {
      const step = Math.ceil(data.length / 600);
      data = data.filter((_, i) => i % step === 0);
    }
    const svg = d3.select(svgRef.current); svg.selectAll("*").remove();
    const pad = { t: 8, r: 20, b: 18, l: 65 };
    const iw = w2 - pad.l - pad.r, ih = h - pad.t - pad.b;
    const laneH = ih / lanes.length;
    const g = svg.append("g").attr("transform", `translate(${pad.l},${pad.t})`);
    const rectW = Math.max(1, iw / data.length);
    lanes.forEach((lane, li) => {
      g.append("text").attr("x", -8).attr("y", li * laneH + laneH / 2 + 3)
        .attr("text-anchor", "end").attr("fill", T.textSecondary)
        .attr("font-size", 10).attr("font-family", MONO).attr("font-weight", 600)
        .text(lane.charAt(0).toUpperCase() + lane.slice(1));
    });
    data.forEach((entry, i) => {
      lanes.forEach((lane, li) => {
        const age = entry[lane];
        let fill;
        if (age === null || age === undefined) fill = T.textMuted;
        else if (age < 10000) fill = T.green;
        else if (age < 60000) fill = T.amber;
        else fill = T.red;
        g.append("rect").attr("x", i * rectW).attr("y", li * laneH + 2)
          .attr("width", Math.max(rectW - 0.5, 1)).attr("height", laneH - 4)
          .attr("rx", 2).attr("fill", fill).attr("opacity", 0.75);
      });
    });
    // v8.13: Real timestamp axis instead of static "-30m / now" labels.
    // Compute evenly-spaced tick marks across the visible time range.
    const tsMin = data[0].ts;
    const tsMax = data[data.length - 1].ts;
    const tsRange = tsMax - tsMin;
    // Choose a nice tick count (aim for ~5-8 ticks depending on width)
    const idealTicks = Math.max(3, Math.min(8, Math.floor(iw / 80)));
    const fmtTime = (epoch) => {
      const d = new Date(epoch * 1000);
      return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false });
    };
    for (let i = 0; i <= idealTicks; i++) {
      const frac = i / idealTicks;
      const ts = tsMin + frac * tsRange;
      const x = frac * iw;
      // Tick line
      g.append("line").attr("x1", x).attr("y1", ih).attr("x2", x).attr("y2", ih + 4)
        .attr("stroke", T.border).attr("stroke-width", 0.5);
      // Tick label
      g.append("text").attr("x", x).attr("y", ih + 12)
        .attr("text-anchor", i === 0 ? "start" : i === idealTicks ? "end" : "middle")
        .attr("fill", T.textMuted).attr("font-size", 8).attr("font-family", MONO)
        .text(fmtTime(ts));
    }
  }, [freshHistory, w2, theme, timeWindow]);
  if (freshHistory.length < 2) return <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, padding: 30, textAlign: "center" }}>Building freshness history...</div>;
  return (<div>
    <svg ref={svgRef} width={w2} height={h} />
    <div style={{ display: "flex", justifyContent: "center", gap: 16, marginTop: 4 }}>
      {[{ label: "Fresh <10s", c: T.green }, { label: "Stale 10-60s", c: T.amber }, { label: "Dead >60s", c: T.red }].map(l => (
        <span key={l.label} style={{ fontFamily: MONO, fontSize: 8, color: T.textMuted, display: "flex", alignItems: "center", gap: 4 }}>
          <div style={{ width: 10, height: 8, borderRadius: 2, background: l.c, opacity: 0.75 }} />{l.label}
        </span>
      ))}
    </div>
  </div>);
}

// 10. Archive browser — tabbed table
function ArchiveBrowser({ groups, ok, theme, activeTab, setActiveTab }) {
  const T = THEMES[theme];
  const tabs = ["unified", "kalshi", "oracle"];
  const files = groups[activeTab] || [];
  const totalMb = files.reduce((s, f) => s + f.size_mb, 0);
  const fmtBadge = (fmt) => {
    const colors = { parquet: T.green, gzip: T.blue, jsonl: T.textMuted };
    return (<span style={{
      fontFamily: MONO, fontSize: 8, fontWeight: 700, padding: "1px 6px", borderRadius: 3,
      background: (colors[fmt] || T.textMuted) + "20", color: colors[fmt] || T.textMuted,
    }}>{fmt}</span>);
  };
  if (!ok) return <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, padding: 30, textAlign: "center" }}>Loading archives...</div>;
  return (<div>
    <div style={{ display: "flex", gap: 3, marginBottom: 10, background: T.pillBg, borderRadius: 7, padding: 2, border: `1px solid ${T.pillBorder}`, width: "fit-content" }}>
      {tabs.map(t => (
        <button key={t} onClick={() => setActiveTab(t)} style={{
          fontFamily: FONT, fontSize: 11, fontWeight: activeTab === t ? 700 : 500,
          padding: "4px 12px", borderRadius: 5, border: "none", cursor: "pointer",
          background: activeTab === t ? T.blue : "transparent",
          color: activeTab === t ? "#fff" : T.textMuted, transition: "all 0.15s",
        }}>{t.charAt(0).toUpperCase() + t.slice(1)} ({(groups[t] || []).length})</button>
      ))}
    </div>
    {files.length === 0 ? (
      <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, padding: 20, textAlign: "center" }}>No archive files</div>
    ) : (
      <div style={{ maxHeight: 320, overflowY: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: MONO, fontSize: 10 }}>
          <thead><tr style={{ borderBottom: `2px solid ${T.border}` }}>
            {["Day", "File", "Format", "Size", "Age"].map(h => (
              <th key={h} style={{ padding: "6px 8px", textAlign: (h === "File" || h === "Day") ? "left" : "right", fontWeight: 700, color: T.textMuted, fontSize: 9, textTransform: "uppercase" }}>{h}</th>
            ))}
          </tr></thead>
          <tbody>
            {files.map((f, i) => (
              <tr key={i} style={{ borderBottom: `1px solid ${T.borderLight}` }}>
                <td style={{ padding: "5px 8px", color: T.blue, fontWeight: 600, whiteSpace: "nowrap" }}>{f.folder || "—"}</td>
                <td style={{ padding: "5px 8px", color: T.textSecondary, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{f.name}</td>
                <td style={{ padding: "5px 8px", textAlign: "right" }}>{fmtBadge(f.format)}</td>
                <td style={{ padding: "5px 8px", textAlign: "right", fontWeight: 600 }}>{f.size_mb.toFixed(1)}</td>
                <td style={{ padding: "5px 8px", textAlign: "right", color: T.textMuted }}>{f.age_hours < 24 ? `${f.age_hours.toFixed(1)}h` : `${(f.age_hours / 24).toFixed(1)}d`}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, padding: "6px 8px", borderTop: `1px solid ${T.border}`, display: "flex", justifyContent: "space-between" }}>
          <span>{files.length} files</span><span>{totalMb.toFixed(1)} MB total</span>
        </div>
      </div>
    )}
  </div>);
}

// 11. Log viewer
function LogViewer({ lines, ok, theme }) {
  const T = THEMES[theme];
  const scrollRef = useRef(null);
  const [autoScroll, setAutoScroll] = useState(true);
  useEffect(() => {
    if (autoScroll && scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [lines, autoScroll]);
  const levelStyle = (level) => {
    switch (level) {
      case "WARNING": return { color: T.amber, bg: T.warnBg };
      case "ERROR": return { color: T.red, bg: T.critBg };
      case "CRITICAL": return { color: T.red, bg: T.critBgStrong, bold: true };
      default: return { color: T.textMuted, bg: "transparent" };
    }
  };
  if (!ok) return (<div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, padding: 30, textAlign: "center" }}>
    <div style={{ fontSize: 24, marginBottom: 8 }}>[LOG]</div>Log file not found. Start the collector to generate logs.
  </div>);
  return (<div>
    <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 6 }}>
      <button onClick={() => setAutoScroll(a => !a)} style={{
        fontFamily: MONO, fontSize: 9, padding: "2px 8px", borderRadius: 4,
        border: `1px solid ${autoScroll ? T.green : T.border}`, cursor: "pointer",
        background: autoScroll ? T.autoScrollBg : "transparent",
        color: autoScroll ? T.green : T.textMuted,
      }}>{autoScroll ? "Auto-scroll ON" : "Auto-scroll OFF"}</button>
    </div>
    <div ref={scrollRef} style={{ maxHeight: 380, overflowY: "auto", fontFamily: MONO, fontSize: 10 }}>
      {lines.map((l, i) => {
        const ls = levelStyle(l.level);
        return (<div key={i} style={{
          padding: "3px 8px", background: ls.bg, borderLeft: l.level !== "INFO" ? `3px solid ${ls.color}` : "3px solid transparent",
          display: "flex", gap: 8, fontWeight: ls.bold ? 700 : 400,
        }}>
          <span style={{ color: T.textMuted, minWidth: 56, flexShrink: 0 }}>{l.time}</span>
          <span style={{ color: ls.color, minWidth: 55, flexShrink: 0, fontWeight: 600 }}>{l.level}</span>
          <span style={{ color: l.level === "INFO" ? T.textSecondary : ls.color, wordBreak: "break-all" }}>{l.msg}</span>
        </div>);
      })}
    </div>
  </div>);
}

// 12. Config viewer
function ConfigViewer({ raw, ok, onRefresh, theme }) {
  const T = THEMES[theme];
  if (!ok) return (<div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, padding: 30, textAlign: "center" }}>Config file not found at collectors/collector_config.yaml</div>);
  return (<pre style={{
    fontFamily: MONO, fontSize: 11, lineHeight: 1.6, padding: 16, borderRadius: 8, overflowX: "auto", maxHeight: 400,
    background: T.configBg, color: T.configText, border: T.configBorder,
  }}>{raw}</pre>);
}

// 13. Alert History Panel — scrollable list of recent alerts/warnings
function AlertHistoryPanel({ alerts, ok, theme }) {
  const T = THEMES[theme];
  if (!ok) return <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, padding: 20, textAlign: "center" }}>No alert data</div>;
  if (alerts.length === 0) return <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, padding: 20, textAlign: "center" }}>No recent alerts</div>;
  // Category badge colors: ALERT=red, GAP=amber, FEED=blue, DISK=purple
  const catColor = (cat) => {
    switch (cat) {
      case "ALERT": return T.red;
      case "GAP": return T.amber;
      case "FEED": return T.blue;
      case "DISK": return T.purple;
      default: return T.textMuted;
    }
  };
  // Severity background tint
  const sevBg = (sev) => {
    if (sev === "critical") return T.critBg;
    if (sev === "warning") return T.warnBg;
    return "transparent";
  };
  return (<div style={{ maxHeight: 320, overflowY: "auto" }}>
    {alerts.map((a, i) => (
      <div key={i} style={{
        display: "flex", alignItems: "flex-start", gap: 8, padding: "6px 8px",
        background: sevBg(a.severity),
        borderBottom: `1px solid ${T.borderLight}`,
      }}>
        <span style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, minWidth: 44, flexShrink: 0 }}>{a.time}</span>
        <span style={{
          fontFamily: MONO, fontSize: 8, fontWeight: 700, padding: "1px 6px", borderRadius: 3, flexShrink: 0,
          background: catColor(a.category) + "20", color: catColor(a.category),
        }}>{a.category}</span>
        <span style={{ fontFamily: MONO, fontSize: 10, color: T.textSecondary, wordBreak: "break-word", lineHeight: 1.4 }}>{a.msg}</span>
      </div>
    ))}
  </div>);
}

// 14. Latency Histogram Chart — D3 bar chart with 10 buckets
function LatencyHistogramChart({ metrics, theme, width }) {
  const T = THEMES[theme];
  const svgRef = useRef(null);
  const w = width || 400; const h = 180;
  // Bucket definitions matching collector output
  const BUCKETS = ["0-5", "5-10", "10-20", "20-50", "50-100", "100-200", "200-500", "500-1000", "1000-5000", "5000+"];
  // Color gradient: green (fast) → amber → red (slow)
  const bucketColor = (idx) => {
    const colors = [T.green, T.green, T.histMidGreen, T.histYellowGreen, T.amber, T.amber, T.histDeepOrange, T.red, T.red, T.red];
    return colors[idx] || T.textMuted;
  };
  // Extract histogram data from Prometheus metrics
  const data = useMemo(() => {
    const hist = metrics?.latency_histogram_ms;
    if (!hist) return null;
    return BUCKETS.map((b, i) => ({ bucket: b, count: hist[b] || 0, color: bucketColor(i) }));
  }, [metrics, theme]);
  const totalSamples = data ? data.reduce((s, d) => s + d.count, 0) : 0;

  useEffect(() => {
    if (!svgRef.current || !data) return;
    const svg = d3.select(svgRef.current); svg.selectAll("*").remove();
    const pad = { t: 20, r: 10, b: 38, l: 40 };
    const iw = w - pad.l - pad.r, ih = h - pad.t - pad.b;
    const maxVal = Math.max(1, d3.max(data, d => d.count));
    const x = d3.scaleBand().domain(data.map(d => d.bucket)).range([0, iw]).padding(0.15);
    const y = d3.scaleLinear().domain([0, maxVal]).range([ih, 0]);
    const g = svg.append("g").attr("transform", `translate(${pad.l},${pad.t})`);
    // Grid
    g.selectAll(".grid").data(y.ticks(4)).enter().append("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", T.chartGrid).attr("stroke-dasharray", "2,3");
    // Bars
    g.selectAll(".bar").data(data).enter().append("rect")
      .attr("x", d => x(d.bucket)).attr("y", d => y(d.count)).attr("width", x.bandwidth())
      .attr("height", d => ih - y(d.count)).attr("fill", d => d.color).attr("opacity", 0.75).attr("rx", 2);
    // Y labels
    g.selectAll(".yl").data(y.ticks(4)).enter().append("text")
      .attr("x", -6).attr("y", d => y(d) + 3).attr("text-anchor", "end")
      .attr("fill", T.textMuted).attr("font-size", 8).attr("font-family", MONO).text(d => d);
    // X labels (bucket names)
    g.selectAll(".xl").data(data).enter().append("text")
      .attr("x", d => x(d.bucket) + x.bandwidth() / 2).attr("y", ih + 14)
      .attr("text-anchor", "middle").attr("fill", T.textMuted).attr("font-size", 7.5).attr("font-family", MONO)
      .text(d => d.bucket + "ms");
    // Hover crosshair line + count label
    const hoverLine = g.append("line").attr("y1", 0).attr("y2", ih).attr("stroke", T.text).attr("stroke-width", 1).attr("stroke-dasharray", "3,2").attr("opacity", 0).attr("pointer-events", "none");
    const hoverText = g.append("text").attr("fill", T.text).attr("font-size", 10).attr("font-family", MONO).attr("font-weight", 700).attr("opacity", 0).attr("pointer-events", "none");
    g.selectAll(".hover-rect").data(data).enter().append("rect")
      .attr("x", d => x(d.bucket)).attr("y", 0).attr("width", x.bandwidth()).attr("height", ih)
      .attr("fill", "transparent").attr("cursor", "crosshair")
      .on("mouseenter", function(event, d) {
        const cx = x(d.bucket) + x.bandwidth() / 2;
        hoverLine.attr("x1", cx).attr("x2", cx).attr("opacity", 0.5);
        hoverText.attr("x", cx).attr("y", -6).attr("text-anchor", "middle").text(`${d.count}`).attr("opacity", 1);
        d3.select(this.parentNode).selectAll(".bar").filter(b => b.bucket === d.bucket).attr("opacity", 1);
      })
      .on("mouseleave", function() {
        hoverLine.attr("opacity", 0);
        hoverText.attr("opacity", 0);
        d3.select(this.parentNode).selectAll(".bar").attr("opacity", 0.75);
      });
  }, [data, w, theme]);
  if (!data) return <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, padding: 20, textAlign: "center" }}>No latency histogram data</div>;
  return (<div>
    <svg ref={svgRef} width={w} height={h} />
    <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, textAlign: "center", marginTop: 2 }}>{totalSamples.toLocaleString()} samples</div>
  </div>);
}

// 15. Feed Rate Breakdown — horizontal bar chart per product
function FeedRateBreakdown({ rates, logTime, ok, theme, width }) {
  const T = THEMES[theme];
  const svgRef = useRef(null);
  const w = width || 400;
  const barH = 22; const pad = { t: 4, r: 50, b: 4, l: 70 };
  const h = rates.length > 0 ? pad.t + pad.b + rates.length * barH : 100;

  useEffect(() => {
    if (!svgRef.current || !ok || rates.length === 0) return;
    const svg = d3.select(svgRef.current); svg.selectAll("*").remove();
    const iw = w - pad.l - pad.r;
    const maxVal = Math.max(1, d3.max(rates, d => d.count));
    const y = d3.scaleBand().domain(rates.map(d => d.product)).range([pad.t, h - pad.b]).padding(0.2);
    const x = d3.scaleLinear().domain([0, maxVal]).range([0, iw]);
    const g = svg.append("g").attr("transform", `translate(${pad.l}, 0)`);
    // Bars
    g.selectAll(".bar").data(rates).enter().append("rect")
      .attr("x", 0).attr("y", d => y(d.product)).attr("width", d => Math.max(2, x(d.count)))
      .attr("height", y.bandwidth()).attr("fill", T.blue).attr("opacity", 0.7).attr("rx", 3);
    // Product labels (left)
    g.selectAll(".lbl").data(rates).enter().append("text")
      .attr("x", -8).attr("y", d => y(d.product) + y.bandwidth() / 2 + 3)
      .attr("text-anchor", "end").attr("fill", T.textSecondary)
      .attr("font-size", 10).attr("font-family", MONO).attr("font-weight", 600)
      .text(d => d.product);
    // Count labels (right of bar)
    g.selectAll(".cnt").data(rates).enter().append("text")
      .attr("x", d => Math.max(2, x(d.count)) + 6).attr("y", d => y(d.product) + y.bandwidth() / 2 + 3)
      .attr("text-anchor", "start").attr("fill", T.textMuted)
      .attr("font-size", 9).attr("font-family", MONO)
      .text(d => d.count.toLocaleString());
  }, [rates, w, theme, ok]);
  if (!ok || rates.length === 0) return <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, padding: 20, textAlign: "center" }}>No feed rate data</div>;
  return (<div>
    <svg ref={svgRef} width={w} height={h} />
    <div style={{ fontFamily: MONO, fontSize: 9, color: T.textMuted, textAlign: "center", marginTop: 4 }}>Last update: {logTime || "—"} (30s window)</div>
  </div>);
}

// 16. System Vitals — 3 stacked sparklines: Queue, Disk, Seq growth
function SystemVitals({ history, theme, width, fillHeight }) {
  const T = THEMES[theme];
  const svgRef = useRef(null);
  const hoverRef = useRef(null);
  const wrapRef = useRef(null);
  const w = width || 300;
  const defaultLaneH = 44; // height per sparkline lane
  const lanes = [
    { key: "queue", label: "Queue Depth", color: T.blue, fmt: v => v.toLocaleString(), warnFn: v => v > 100 },
    { key: "disk", label: "Disk Free", color: T.green, fmt: v => v.toFixed(1) + " GB", warnFn: v => v < 5 },
    { key: "seqRate", label: "Seq/s", color: T.purple, fmt: v => v >= 1000 ? (v / 1000).toFixed(1) + "K" : Math.round(v).toLocaleString(), warnFn: () => false },
  ];
  const defaultH = defaultLaneH * lanes.length + 8;
  useEffect(() => {
    if (!svgRef.current || history.length < 3) return;
    // Measure available height from wrapper (runs after layout)
    let h = defaultH;
    if (fillHeight && wrapRef.current) {
      const wh = wrapRef.current.getBoundingClientRect().height;
      if (wh > 40) h = wh;
    }
    const laneH = (h - 8) / lanes.length; // dynamically size lanes
    const labelH = 14; // top padding for label text — sparklines start below this
    const svg = d3.select(svgRef.current);
    svg.attr("height", h); // set SVG to measured height
    svg.selectAll("*").remove();
    const pad = { l: 0, r: 0 };
    const iw = w - pad.l - pad.r;
    // Pre-compute seq/s — smoothed to avoid spikes from 30s Prometheus updates.
    // The seq gauge only changes every ~30s, so naive deltas give 14 zeros then
    // 1 huge spike.  Instead, detect real changes and carry forward the last rate.
    let _lastSeq = 0, _lastSeqTs = 0, _seqRate = 0;
    const enriched = history.map((d, i) => {
      if (i === 0) { _lastSeq = d.seq; _lastSeqTs = d.ts; return { ...d, seqRate: 0 }; }
      if (d.seq !== _lastSeq) {
        // Seq changed — compute fresh rate from time since last change
        const dt = Math.max(1, d.ts - _lastSeqTs);
        const ds = Math.max(0, d.seq - _lastSeq);
        _seqRate = ds / dt;
        _lastSeq = d.seq; _lastSeqTs = d.ts;
      }
      return { ...d, seqRate: _seqRate };
    });
    // Store yScales and vals per lane for hover crosshair
    const laneData = [];
    lanes.forEach((lane, li) => {
      const y0 = li * laneH + 2;
      const sparkH = laneH - 6 - labelH; // sparkline area below label
      const g = svg.append("g").attr("transform", `translate(${pad.l},${y0})`);
      const vals = enriched.map(d => d[lane.key] || 0);
      const minV = d3.min(vals); const maxV = d3.max(vals);
      const range = maxV - minV || 1;
      const yScale = d3.scaleLinear().domain([minV - range * 0.1, maxV + range * 0.1]).range([sparkH, 0]);
      // Filled area — shifted down by labelH so it doesn't overlap text
      const area = d3.area()
        .x((_, i) => (i / (vals.length - 1)) * iw)
        .y0(sparkH)
        .y1((d) => yScale(d))
        .curve(d3.curveMonotoneX);
      g.append("path").datum(vals).attr("d", area).attr("fill", lane.color).attr("opacity", 0.12)
        .attr("transform", `translate(0,${labelH})`);
      // Line — also shifted down by labelH
      const line = d3.line()
        .x((_, i) => (i / (vals.length - 1)) * iw)
        .y(d => yScale(d))
        .curve(d3.curveMonotoneX);
      g.append("path").datum(vals).attr("d", line).attr("fill", "none")
        .attr("stroke", lane.color).attr("stroke-width", 1.5)
        .attr("transform", `translate(0,${labelH})`);
      // Label: left side (in the label area above the sparkline)
      g.append("text").attr("x", 4).attr("y", 10)
        .attr("fill", T.textMuted).attr("font-size", 8).attr("font-family", MONO).attr("font-weight", 600)
        .text(lane.label);
      // Separator line at bottom of lane
      if (li < lanes.length - 1) {
        g.append("line").attr("x1", 0).attr("x2", iw).attr("y1", labelH + sparkH + 3).attr("y2", labelH + sparkH + 3)
          .attr("stroke", T.border).attr("stroke-width", 0.5);
      }
      // Save lane info for hover crosshair
      laneData.push({ vals, yScale, y0, sparkH, lane });
    });
    // ── Unified hover crosshair for all three metrics ──
    const hoverG = svg.append("g").attr("opacity", 0).attr("pointer-events", "none");
    // Vertical crosshair line spanning all lanes
    const crossLine = hoverG.append("line")
      .attr("x1", 0).attr("x2", 0).attr("y1", 0).attr("y2", h)
      .attr("stroke", T.text).attr("stroke-width", 1).attr("stroke-dasharray", "3,2").attr("opacity", 0.4);
    // Tooltip background + text group
    const tipG = hoverG.append("g");
    const tipBg = tipG.append("rect").attr("rx", 5).attr("ry", 5)
      .attr("fill", T.card).attr("stroke", T.border).attr("stroke-width", 1)
      .attr("filter", "drop-shadow(0 2px 4px rgba(0,0,0,0.15))");
    // One text line per lane + dots
    const tipTexts = laneData.map((ld, i) => {
      const dot = tipG.append("circle").attr("r", 3).attr("fill", ld.lane.color);
      const txt = tipG.append("text")
        .attr("fill", T.text).attr("font-size", 9).attr("font-family", MONO).attr("font-weight", 600);
      return { dot, txt };
    });
    // Hover dots on each sparkline
    const hoverDots = laneData.map(ld =>
      hoverG.append("circle").attr("r", 3.5)
        .attr("fill", ld.lane.color).attr("stroke", T.card).attr("stroke-width", 1.5)
    );
    // Invisible rect to capture mouse events
    svg.append("rect").attr("width", iw).attr("height", h).attr("fill", "transparent")
      .on("mousemove", function(event) {
        hoverRef.current = [event.clientX, event.clientY];
        const [mx] = d3.pointer(event);
        // Find closest data index
        const n = laneData[0].vals.length;
        const idx = Math.round((mx / iw) * (n - 1));
        if (idx < 0 || idx >= n) return;
        const cx = (idx / (n - 1)) * iw;
        hoverG.attr("opacity", 1);
        crossLine.attr("x1", cx).attr("x2", cx);
        // Position dots on each sparkline
        laneData.forEach((ld, li) => {
          const val = ld.vals[idx];
          const sy = ld.yScale(val);
          hoverDots[li].attr("cx", cx).attr("cy", ld.y0 + 2 + labelH + sy);
        });
        // Build tooltip
        const lineH = 16; const tipPad = 8;
        const tipW = 130; const tipH = laneData.length * lineH + tipPad * 2;
        // Position tooltip — flip side if near right edge
        const tx = cx < iw / 2 ? cx + 12 : cx - tipW - 12;
        const ty = Math.max(4, Math.min(h - tipH - 4, h / 2 - tipH / 2));
        tipG.attr("transform", `translate(${tx},${ty})`);
        tipBg.attr("width", tipW).attr("height", tipH);
        laneData.forEach((ld, i) => {
          const val = ld.vals[idx];
          const warn = ld.lane.warnFn(val);
          tipTexts[i].dot.attr("cx", tipPad + 4).attr("cy", tipPad + i * lineH + lineH / 2);
          tipTexts[i].txt
            .attr("x", tipPad + 12).attr("y", tipPad + i * lineH + lineH / 2 + 3)
            .attr("fill", warn ? T.red : T.text)
            .text(`${ld.lane.label}: ${ld.lane.fmt(val)}`);
        });
      })
      .on("mouseleave", function() { hoverRef.current = null; hoverG.attr("opacity", 0); });
    _restoreHover(svg, hoverRef);
  }, [history, w, fillHeight, theme]);
  if (history.length < 3) return <div ref={wrapRef} style={{ flex: 1, minHeight: 0, fontFamily: MONO, fontSize: 11, color: T.textMuted, padding: 20, textAlign: "center", display: "flex", alignItems: "center", justifyContent: "center" }}>Building vitals data...</div>;
  return <div ref={wrapRef} style={{ flex: 1, minHeight: 0 }}><svg ref={svgRef} width={w} height={defaultH} style={{ display: "block" }} /></div>;
}

// 17. Network Vitals — 3 stacked sparklines: Bytes/sec, CB msg size, KL msg size
// v8.2: Same pattern as SystemVitals but for network-level metrics
function NetworkVitals({ history, theme, width }) {
  const T = THEMES[theme];
  const svgRef = useRef(null);
  const hoverRef = useRef(null);
  const w = width || 300;
  const laneH = 44;
  const lanes = [
    { key: "bytesPerSec", label: "Bytes/sec", color: T.blue, fmt: v => v >= 1024 ? (v / 1024).toFixed(1) + " KB/s" : Math.round(v) + " B/s", warnFn: v => v > 512000 },
    { key: "msgSizeCb", label: "CB Msg Size", color: T.green, fmt: v => v >= 1024 ? (v / 1024).toFixed(1) + " KB" : Math.round(v) + " B", warnFn: () => false },
    { key: "msgSizeKl", label: "KL Msg Size", color: T.purple, fmt: v => v >= 1024 ? (v / 1024).toFixed(1) + " KB" : Math.round(v) + " B", warnFn: () => false },
  ];
  const h = laneH * lanes.length + 8;
  useEffect(() => {
    if (!svgRef.current || history.length < 3) return;
    const labelH = 14;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();
    const iw = w;
    const laneData = [];
    lanes.forEach((lane, li) => {
      const y0 = li * laneH + 2;
      const sparkH = laneH - 6 - labelH;
      const g = svg.append("g").attr("transform", `translate(0,${y0})`);
      const vals = history.map(d => d[lane.key] || 0);
      const minV = d3.min(vals); const maxV = d3.max(vals);
      const range = maxV - minV || 1;
      const yScale = d3.scaleLinear().domain([minV - range * 0.1, maxV + range * 0.1]).range([sparkH, 0]);
      const area = d3.area().x((_, i) => (i / (vals.length - 1)) * iw).y0(sparkH).y1(d => yScale(d)).curve(d3.curveMonotoneX);
      g.append("path").datum(vals).attr("d", area).attr("fill", lane.color).attr("opacity", 0.12).attr("transform", `translate(0,${labelH})`);
      const line = d3.line().x((_, i) => (i / (vals.length - 1)) * iw).y(d => yScale(d)).curve(d3.curveMonotoneX);
      g.append("path").datum(vals).attr("d", line).attr("fill", "none").attr("stroke", lane.color).attr("stroke-width", 1.5).attr("transform", `translate(0,${labelH})`);
      g.append("text").attr("x", 4).attr("y", 10).attr("fill", T.textMuted).attr("font-size", 8).attr("font-family", MONO).attr("font-weight", 600).text(lane.label);
      if (li < lanes.length - 1) {
        g.append("line").attr("x1", 0).attr("x2", iw).attr("y1", labelH + sparkH + 3).attr("y2", labelH + sparkH + 3).attr("stroke", T.border).attr("stroke-width", 0.5);
      }
      laneData.push({ vals, yScale, y0, sparkH, lane });
    });
    // Unified hover crosshair
    const hoverG = svg.append("g").attr("opacity", 0).attr("pointer-events", "none");
    hoverG.append("line").attr("x1", 0).attr("x2", 0).attr("y1", 0).attr("y2", h).attr("stroke", T.text).attr("stroke-width", 1).attr("stroke-dasharray", "3,2").attr("opacity", 0.4);
    const tipG = hoverG.append("g");
    const tipBg = tipG.append("rect").attr("rx", 5).attr("ry", 5).attr("fill", T.card).attr("stroke", T.border).attr("stroke-width", 1).attr("filter", "drop-shadow(0 2px 4px rgba(0,0,0,0.15))");
    const tipTexts = laneData.map((ld) => {
      const dot = tipG.append("circle").attr("r", 3).attr("fill", ld.lane.color);
      const txt = tipG.append("text").attr("fill", T.text).attr("font-size", 9).attr("font-family", MONO).attr("font-weight", 600);
      return { dot, txt };
    });
    const hoverDots = laneData.map(ld => hoverG.append("circle").attr("r", 3.5).attr("fill", ld.lane.color).attr("stroke", T.card).attr("stroke-width", 1.5));
    const crossLine = hoverG.select("line");
    svg.append("rect").attr("width", iw).attr("height", h).attr("fill", "transparent")
      .on("mousemove", function(event) {
        hoverRef.current = [event.clientX, event.clientY];
        const [mx] = d3.pointer(event);
        const n = laneData[0].vals.length;
        const idx = Math.round((mx / iw) * (n - 1));
        if (idx < 0 || idx >= n) return;
        const cx = (idx / (n - 1)) * iw;
        hoverG.attr("opacity", 1);
        crossLine.attr("x1", cx).attr("x2", cx);
        laneData.forEach((ld, li) => {
          const val = ld.vals[idx];
          const sy = ld.yScale(val);
          hoverDots[li].attr("cx", cx).attr("cy", ld.y0 + 2 + labelH + sy);
        });
        const lineH2 = 16; const tipPad = 8;
        const tipW = 150; const tipH2 = laneData.length * lineH2 + tipPad * 2;
        const tx = cx < iw / 2 ? cx + 12 : cx - tipW - 12;
        const ty = Math.max(4, Math.min(h - tipH2 - 4, h / 2 - tipH2 / 2));
        tipG.attr("transform", `translate(${tx},${ty})`);
        tipBg.attr("width", tipW).attr("height", tipH2);
        laneData.forEach((ld, i) => {
          const val = ld.vals[idx];
          const warn = ld.lane.warnFn(val);
          tipTexts[i].dot.attr("cx", tipPad + 4).attr("cy", tipPad + i * lineH2 + lineH2 / 2);
          tipTexts[i].txt.attr("x", tipPad + 12).attr("y", tipPad + i * lineH2 + lineH2 / 2 + 3).attr("fill", warn ? T.red : T.text).text(`${ld.lane.label}: ${ld.lane.fmt(val)}`);
        });
      })
      .on("mouseleave", function() { hoverRef.current = null; hoverG.attr("opacity", 0); });
    _restoreHover(svg, hoverRef);
  }, [history, w, theme]);
  if (history.length < 3) return <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, padding: 20, textAlign: "center" }}>Building network data...</div>;
  return <svg ref={svgRef} width={w} height={h} style={{ display: "block" }} />;
}

// ═══════════════════════════════════════════════════════════════
// v8.2: EVENT INSPECTOR — click on Event Rate chart to see raw tape events
// v8.11: Added window size selector to inspect farther back in time
// ═══════════════════════════════════════════════════════════════
function EventInspector({ ts, onClose, theme }) {
  const T = THEMES[theme];
  const [events, setEvents] = useState([]);
  const [loading, setLoading] = useState(true);
  const [meta, setMeta] = useState({});
  const [windowSec, setWindowSec] = useState(10);
  const windowOptions = [
    { value: 10, label: "±5s" },
    { value: 30, label: "±15s" },
    { value: 60, label: "±30s" },
    { value: 120, label: "±1m" },
    { value: 300, label: "±2.5m" },
    { value: 600, label: "±5m" },
  ];

  useEffect(() => {
    if (!ts) return;
    let active = true;
    setLoading(true);
    const ctrl = new AbortController();
    fetch(`/api/tape/events?ts=${ts}&window=${windowSec}`, { signal: ctrl.signal })
      .then(r => r.json())
      .then(data => {
        if (!active) return;
        if (data.ok) {
          setEvents(data.events || []);
          setMeta({ file: data.file, scanned: data.total_scanned, window: data.window_s });
        }
        setLoading(false);
      })
      .catch(() => { if (active) setLoading(false); });
    return () => { active = false; ctrl.abort(); };
  }, [ts, windowSec]);

  // Source badge color
  const srcColor = (src) => src === "cb" ? T.blue : src === "kl" ? T.purple : T.textMuted;
  const srcLabel = (src) => src === "cb" ? "CB" : src === "kl" ? "KL" : src === "snap" ? "SNAP" : src;
  // Latency color: green < 20ms, amber < 100ms, red >= 100ms
  const latColor = (ms) => ms == null ? T.textMuted : ms < 20 ? T.green : ms < 100 ? T.amber : T.red;

  const centerTime = ts ? new Date(ts * 1000).toLocaleTimeString() : "";
  const centerDate = ts ? new Date(ts * 1000).toLocaleDateString() : "";
  const halfSec = Math.round(windowSec / 2);

  return (
    <div style={{ maxHeight: "80vh", display: "flex", flexDirection: "column" }}>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12, paddingBottom: 10, borderBottom: `1px solid ${T.border}` }}>
        <div>
          <div style={{ fontFamily: FONT, fontSize: 16, fontWeight: 700, color: T.text }}>
            Event Inspector
          </div>
          <div style={{ fontFamily: MONO, fontSize: 11, color: T.textMuted, marginTop: 2 }}>
            {centerDate} {centerTime} ±{halfSec}s {meta.file ? `| ${meta.file}` : ""} {events.length > 0 ? `| ${events.length} events` : ""}
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {/* v8.11: Window size selector */}
          <div style={{ display: "flex", gap: 2, background: T.pillBg, borderRadius: 6, padding: 2, border: `1px solid ${T.pillBorder}` }}>
            {windowOptions.map(opt => (
              <button key={opt.value} onClick={() => setWindowSec(opt.value)} style={{
                fontFamily: MONO, fontSize: 9, fontWeight: windowSec === opt.value ? 700 : 500,
                padding: "3px 8px", borderRadius: 4, border: "none", cursor: "pointer",
                background: windowSec === opt.value ? T.blue : "transparent",
                color: windowSec === opt.value ? "#fff" : T.textMuted,
              }}>{opt.label}</button>
            ))}
          </div>
          <button onClick={onClose} style={{ fontFamily: MONO, fontSize: 11, padding: "4px 12px", borderRadius: 6, border: `1px solid ${T.border}`, background: "transparent", color: T.textMuted, cursor: "pointer" }}>Close [Esc]</button>
        </div>
      </div>

      {/* Loading state */}
      {loading && (
        <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, textAlign: "center", padding: 40 }}>
          Scanning tape files...
        </div>
      )}

      {/* No results */}
      {!loading && events.length === 0 && (
        <div style={{ fontFamily: MONO, fontSize: 12, color: T.textMuted, textAlign: "center", padding: 40 }}>
          No events found in this time window. The tape data may have been archived or rotated.
        </div>
      )}

      {/* Events table */}
      {!loading && events.length > 0 && (
        <div style={{ flex: 1, overflowY: "auto", minHeight: 0 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: MONO, fontSize: 10 }}>
            <thead style={{ position: "sticky", top: 0, background: T.card, zIndex: 1 }}>
              <tr>
                {["#", "Time", "Source", "Latency", "Type", "Symbol", "Price", "Detail"].map(col => (
                  <th key={col} style={{ padding: "6px 8px", textAlign: "left", fontWeight: 700, color: T.textMuted, borderBottom: `2px solid ${T.border}`, fontSize: 9, textTransform: "uppercase", letterSpacing: "0.5px" }}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {events.map((evt, i) => {
                // Type badge color: ticker=green, ob_delta=amber, snapshot=blue
                const typeColor = evt.raw?.type === "ticker" ? T.green : evt.raw?.type === "trade" ? T.cyan : evt.raw?.type === "ob_delta" ? T.amber : evt.raw?.type === "snapshot" ? T.blue : T.textMuted;
                return (
                <tr key={i} style={{ borderBottom: `1px solid ${T.border}`, background: i % 2 === 0 ? "transparent" : T.pillBg }}>
                  <td style={{ padding: "4px 8px", color: T.textMuted, fontSize: 9 }}>{evt.seq || "—"}</td>
                  <td style={{ padding: "4px 8px", color: T.text, fontWeight: 600 }}>{evt.ts_human || "—"}</td>
                  <td style={{ padding: "4px 8px" }}>
                    <span style={{ display: "inline-block", padding: "1px 6px", borderRadius: 4, fontSize: 8, fontWeight: 700, background: srcColor(evt.src), color: "#fff", letterSpacing: "0.5px" }}>{srcLabel(evt.src)}</span>
                  </td>
                  <td style={{ padding: "4px 8px", color: latColor(evt.latency_ms), fontWeight: 600 }}>
                    {evt.latency_ms != null ? `${evt.latency_ms}ms` : "—"}
                  </td>
                  <td style={{ padding: "4px 8px" }}>
                    <span style={{ color: typeColor, fontWeight: 600 }}>{evt.raw?.type || "—"}</span>
                  </td>
                  <td style={{ padding: "4px 8px", color: T.cyan, fontWeight: 600 }}>{evt.raw?.product || "—"}</td>
                  <td style={{ padding: "4px 8px", color: T.text, fontWeight: 600 }}>{evt.raw?.price || "—"}</td>
                  <td style={{ padding: "4px 8px", color: T.textMuted, fontSize: 9 }}>{evt.raw?.detail || ""}</td>
                </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// ROOT COMPONENT
// ═══════════════════════════════════════════════════════════════
export default function CollectorDashboard() {
  // v8.4: Load saved theme from localStorage, default to "dark"
  const [theme, setTheme] = useState(() => {
    try { const s = localStorage.getItem("orion_collector_dash_theme"); if (s && THEMES[s]) return s; } catch {}
    return "dark";
  });
  // v8.4: Persist theme choice to localStorage on change
  useEffect(() => { try { localStorage.setItem("orion_collector_dash_theme", theme); } catch {} }, [theme]);
  const T = THEMES[theme];

  // All hooks
  const cm = useCollectorMetrics();
  const health = useTapeHealth();
  const archives = useArchives();
  const logs = useLogs();
  const config = useConfig();
  const errorRate = useErrorRate();
  const procStats = useProcessStats();
  const alertHistory = useAlertHistory();   // v8.1
  const feedRates = useFeedRates();         // v8.1
  const sla = useSLA();                     // v8.5
  const anomalyData = useAnomalies();       // v8.5
  // v8.5: Map anomaly metric names to StatCard labels for glow effect
  const anomalyMap = useMemo(() => {
    const m = {};
    for (const a of (anomalyData.active || [])) {
      m[a.metric] = a.severity; // "alert" or "warning"
    }
    return m; // e.g., { rate: "alert", p95: "warning" }
  }, [anomalyData.active]);

  // State: time window, fullscreen, archive tab
  const [timeWindow, setTimeWindow] = useState(60); // 1m default
  const [fullscreenChart, setFullscreenChart] = useState(null); // "events" | "latency" | null
  const [latMode, setLatMode] = useState("true"); // "true" | "raw" — shared between inline + fullscreen
  const [archiveTab, setArchiveTab] = useState("unified");
  const [activeTab, setActiveTab] = useState("monitor"); // "monitor" | "deep"
  const [inspectTs, setInspectTs] = useState(null); // v8.2: Event Inspector timestamp
  const [errorDetailMinute, setErrorDetailMinute] = useState(null); // v8.11: Error bar click detail
  const [showIncidents, setShowIncidents] = useState(false);        // v8.11: Incident history modal
  const [showHealthDetails, setShowHealthDetails] = useState(false); // v8.11: Health details modal
  const [showStatusDetail, setShowStatusDetail] = useState(false);   // v8.12: Status card click → incident detail
  const [showUptimeDetail, setShowUptimeDetail] = useState(false);   // v9.1: Uptime card click → uptime detail
  const [showMttrDetail, setShowMttrDetail] = useState(false);       // v9.1: MTTR card click → recovery detail
  const [metricDetail, setMetricDetail] = useState(null);            // v9.0: Which metric detail panel is open ("events"|"latency"|...)|null

  // v8.6: Overlay state — persisted to localStorage per chart
  const [eventOverlays, setEventOverlays] = useState(() => {
    try { const s = localStorage.getItem(OVERLAY_STORAGE_KEY); if (s) { const d = JSON.parse(s); return d.events || []; } } catch {} return [];
  });
  const [latencyOverlays, setLatencyOverlays] = useState(() => {
    try { const s = localStorage.getItem(OVERLAY_STORAGE_KEY); if (s) { const d = JSON.parse(s); return d.latency || []; } } catch {} return [];
  });
  useEffect(() => {
    try { localStorage.setItem(OVERLAY_STORAGE_KEY, JSON.stringify({ events: eventOverlays, latency: latencyOverlays })); } catch {}
  }, [eventOverlays, latencyOverlays]);

  // v8.2: Server-side history for long time windows (>1h)
  const serverHist = useServerHistory(timeWindow);
  const playbackSource = usePlaybackSource();  // v8.8: always-available 24h history for timeline

  // Container width for responsive charts
  const containerRef = useRef(null);
  const cw = useContainerWidth(containerRef);
  const GAP = 12; const CP = 38;  // card border-box: 18px padding * 2 + 1px border * 2 = 38px
  const fullW = cw - CP;
  const halfW = Math.floor((cw - GAP) / 2) - CP;
  const thirdW = Math.floor((cw - GAP * 2) / 3) - CP;  // v8.1: for 3-column rows

  // Filter history by time window + downsample
  // v8.2: For windows > 1h, use server-side data instead of client-side
  // v8.9: Added minimum entry guarantee — if the strict time-window filter
  // produces fewer than MIN_CHART_ENTRIES entries (common after returning
  // from a background tab where polling was throttled), extend the window
  // backward to ensure charts always have enough data to render.
  const MIN_CHART_ENTRIES = 10;
  // Max chart points — keeps SVG element count manageable for smooth rendering.
  // Each chart creates 2-6 SVG elements per point, so 600 pts ≈ 1200-3600 elements.
  const MAX_CHART_PTS = 600;
  const filteredHistory = useMemo(() => {
    if (timeWindow > 3600 && serverHist.serverHistory.length > 0) {
      // Server returns up to 3600 pts — downsample to MAX_CHART_PTS for smooth rendering
      return downsample(serverHist.serverHistory, MAX_CHART_PTS);
    }
    if (!cm.history.length) return [];
    const now = cm.history[cm.history.length - 1].ts;
    const cutoff = now - timeWindow;
    let filtered = cm.history.filter(e => e.ts >= cutoff);
    // If too few entries after filtering (e.g. tab was in background),
    // take the most recent MIN entries from history so charts don't break
    if (filtered.length < MIN_CHART_ENTRIES && cm.history.length >= MIN_CHART_ENTRIES) {
      filtered = cm.history.slice(-MIN_CHART_ENTRIES);
    }
    return downsample(filtered, MAX_CHART_PTS);
  }, [cm.history, timeWindow, serverHist.serverHistory]);

  // v8.6: Timeline playback state — controls what charts see
  const [playback, setPlayback] = useState({ mode: "live", index: -1, histogramMs: null, feedSnapshot: null });

  // v8.8: Playback history — the FULL available dataset for scrubbing through.
  // filteredHistory only shows the selected time window (e.g. 30 entries for 1m),
  // but playbackSource has up to 24h of server history.  Merging with the client
  // buffer gives real-time coverage at the leading edge.  The slider spans this
  // full range so users can scrub through hours of data on any time window.
  const playbackHistory = useMemo(() => {
    if (playbackSource.length > 0) {
      // Merge 24h server source + real-time client data, dedup by timestamp
      const serverTs = new Set(playbackSource.map(e => Math.round(e.ts * 10)));
      const clientOnly = cm.history.filter(e => !serverTs.has(Math.round(e.ts * 10)));
      const merged = [...playbackSource, ...clientOnly].sort((a, b) => a.ts - b.ts);
      return downsample(merged, 1200);
    }
    // Fallback before server responds — use client buffer only
    return downsample(cm.history, 1200);
  }, [cm.history, playbackSource]);

  // v8.8: Sliding-window playback — charts always show the same number of
  // bars/lines as live mode.  The "window" = filteredHistory.length (what's
  // on screen now).  The slider spans playbackHistory (much larger).
  // Scrubbing slides the window through playbackHistory.
  const windowSizeRef = useRef(filteredHistory.length);
  // Update window size only while live (so it freezes when playback starts)
  useEffect(() => {
    if (playback.mode === "live" && filteredHistory.length > 0) {
      windowSizeRef.current = filteredHistory.length;
    }
  }, [filteredHistory.length, playback.mode]);

  // v8.13 fix: Freeze playbackHistory when entering non-live mode.
  // The downsample() function selects every Nth entry (i % step === 0).
  // Each new poll adds an entry, shifting which indices pass the filter.
  // This causes the same playback.index to map to different timestamps
  // on each recomputation — visible as chart flicker between historical
  // and live ranges, especially at slow speeds (0.5x) where the index
  // barely advances between recomputations.
  const frozenPlaybackRef = useRef(null);
  // v8.14: Also freeze the FULL-DENSITY raw merged data for chart rendering.
  // stablePlayback (1200 entries over 24h) is good for the slider, but too
  // sparse for charts at small time windows (15m = only ~12 entries).
  // frozenRawRef stores the undownsampled merge of server + client data
  // so charts get the same density as live mode.
  const frozenRawRef = useRef(null);
  useEffect(() => {
    if (playback.mode === "live") {
      frozenPlaybackRef.current = null; // unfreeze — live uses fresh data
      frozenRawRef.current = null;
    } else if (!frozenPlaybackRef.current) {
      frozenPlaybackRef.current = playbackHistory; // freeze on first non-live
      // Freeze full-density merged data (before downsample to 1200)
      if (playbackSource.length > 0) {
        const serverTs = new Set(playbackSource.map(e => Math.round(e.ts * 10)));
        const clientOnly = cm.history.filter(e => !serverTs.has(Math.round(e.ts * 10)));
        frozenRawRef.current = [...playbackSource, ...clientOnly].sort((a, b) => a.ts - b.ts);
      } else {
        frozenRawRef.current = [...cm.history];
      }
    }
  }, [playback.mode, playbackHistory, playbackSource, cm.history]);
  const stablePlayback = (playback.mode !== "live" && frozenPlaybackRef.current) || playbackHistory;
  // Full-density source for chart rendering during playback
  const stableRaw = (playback.mode !== "live" && frozenRawRef.current) || [];

  const visibleHistory = useMemo(() => {
    if (playback.mode === "live" || playback.index < 0) return filteredHistory;
    // v8.14 fix: Use TIME-BASED window on full-density raw data.
    // stablePlayback (1200 entries/24h) is for the slider only.
    // stableRaw has full-density data (cm.history at 2s + server at ~10s)
    // so charts show the same detail level as live mode.
    const endIdx = Math.min(playback.index, stablePlayback.length - 1);
    if (endIdx < 0) return filteredHistory;
    const endTs = stablePlayback[endIdx].ts;
    const startTs = endTs - timeWindow;
    // Use full-density raw data for the chart; fall back to stablePlayback
    const source = stableRaw.length > 0 ? stableRaw : stablePlayback;
    // Binary search for the first entry >= startTs
    let lo = 0, hi = source.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (source[mid].ts < startTs) lo = mid + 1;
      else hi = mid;
    }
    // Binary search for the first entry > endTs
    let lo2 = lo, hi2 = source.length;
    while (lo2 < hi2) {
      const mid = (lo2 + hi2) >>> 1;
      if (source[mid].ts <= endTs) lo2 = mid + 1;
      else hi2 = mid;
    }
    let slice = source.slice(lo, lo2);
    // Guarantee at least MIN_CHART_ENTRIES so charts don't break
    if (slice.length < MIN_CHART_ENTRIES && lo2 >= MIN_CHART_ENTRIES) {
      slice = source.slice(lo2 - MIN_CHART_ENTRIES, lo2);
    }
    // Downsample the slice to keep SVG performant
    return downsample(slice, MAX_CHART_PTS);
  }, [filteredHistory, stablePlayback, stableRaw, playback.mode, playback.index, timeWindow]);

  // v8.6: Playback-aware metrics for LatencyHistogramChart
  // v8.9: uses pbMetrics as base so latency percentile values match the scrub position
  const pbHistogramMetrics = useMemo(() => {
    if (playback.mode === "live") return cm.metrics;
    if (playback.histogramMs) return { ...pbMetrics, latency_histogram_ms: playback.histogramMs };
    return pbMetrics; // fallback: use pbMetrics for the percentile values at scrub position
  }, [cm.metrics, pbMetrics, playback.mode, playback.histogramMs]);

  // v8.6: Playback-aware feed rates for FeedRateBreakdown
  const pbFeedRates = useMemo(() => {
    if (playback.mode === "live") return feedRates;
    if (playback.feedSnapshot) return { rates: playback.feedSnapshot.rates, logTime: playback.feedSnapshot.logTime, ok: true };
    return feedRates; // fallback
  }, [feedRates, playback.mode, playback.feedSnapshot]);

  // v8.1: Gap timestamps for red dot markers on Event Rate chart
  const gapTimestamps = useMemo(() => {
    return visibleHistory.filter(e => e.rate <= 0).map(e => e.ts);
  }, [visibleHistory]);

  // v8.9: Playback-aware metrics — when scrubbing through history, stat cards
  // and panels show the metric values AT that point in time instead of live values.
  // History entries store: rate, p50, p95, p99, queue, disk, seq, uptime,
  // bytesPerSec, wsRttCb, wsRttKl, cb, kl, cbGaps, klGaps, health_score, etc.
  const pbMetrics = useMemo(() => {
    if (playback.mode === "live" || !visibleHistory.length) return cm.metrics;
    const last = visibleHistory[visibleHistory.length - 1];
    return {
      ...cm.metrics,                                     // keep structure for fields not in history
      event_rate:        last.rate        ?? cm.metrics?.event_rate,
      latency_ms: {
        p50: last.p50 ?? cm.metrics?.latency_ms?.p50,
        p95: last.p95 ?? cm.metrics?.latency_ms?.p95,
        p99: last.p99 ?? cm.metrics?.latency_ms?.p99,
      },
      queue_depth:       last.queue       ?? cm.metrics?.queue_depth,
      disk_free_gb:      last.disk        ?? cm.metrics?.disk_free_gb,
      seq:               last.seq         ?? cm.metrics?.seq,
      uptime_seconds:    last.uptime      ?? cm.metrics?.uptime_seconds,
      bytes_per_sec:     last.bytesPerSec ?? cm.metrics?.bytes_per_sec,
      ws_rtt_ms: {
        coinbase: last.wsRttCb ?? cm.metrics?.ws_rtt_ms?.coinbase,
        kalshi:   last.wsRttKl ?? cm.metrics?.ws_rtt_ms?.kalshi,
      },
      events_total: {
        coinbase: last.cb ?? cm.metrics?.events_total?.coinbase,
        kalshi:   last.kl ?? cm.metrics?.events_total?.kalshi,
      },
      gaps_total: {
        coinbase: last.cbGaps ?? cm.metrics?.gaps_total?.coinbase,
        kalshi:   last.klGaps ?? cm.metrics?.gaps_total?.kalshi,
      },
      health_score:      last.health_score  ?? cm.metrics?.health_score,
      health_grade:      last.health_grade  ?? cm.metrics?.health_grade,
      reconnects_total:  cm.metrics?.reconnects_total,   // not stored in history
      dedup_total:       cm.metrics?.dedup_total,         // not stored in history
      connection_uptime_seconds: cm.metrics?.connection_uptime_seconds, // not stored
    };
  }, [cm.metrics, playback.mode, visibleHistory]);

  // v8.9: Playback-aware freshness history — when scrubbing, derive freshness
  // data from the visibleHistory entries (which contain unified_age, kalshi_age,
  // oracle_age) instead of using the live freshness stream.
  const pbFreshHistory = useMemo(() => {
    if (playback.mode === "live") return health.freshHistory;
    // Map history entries to the format FreshnessTimeline expects: { ts, unified, kalshi, oracle }
    return visibleHistory
      .filter(e => e.unified_age !== undefined)
      .map(e => ({ ts: e.ts, unified: e.unified_age, kalshi: e.kalshi_age, oracle: e.oracle_age }));
  }, [health.freshHistory, playback.mode, visibleHistory]);

  // Alert thresholds (v8.9: use pbMetrics so alerts reflect playback position)
  const alerts = useMemo(() => ({
    rate: (pbMetrics?.event_rate || 0) < 100 && cm.connected,
    latency: Math.abs(pbMetrics?.latency_ms?.p95 || 0) > 500,
    disk: (pbMetrics?.disk_free_gb || 0) < 5 && cm.connected,
    queue: (pbMetrics?.queue_depth || 0) > 1000,
  }), [pbMetrics, cm.connected]);

  // Auto-refresh indicator state
  const [refreshPulse, setRefreshPulse] = useState(false);
  useEffect(() => {
    const id = setInterval(() => {
      const age = Date.now() - cm.lastPoll.current;
      setRefreshPulse(age < 1000);
    }, 200);
    return () => clearInterval(id);
  }, []);

  // Keyboard shortcuts
  useEffect(() => {
    const handler = (e) => {
      // Don't capture if typing in an input
      if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA") return;
      switch (e.key.toLowerCase()) {
        case "d": setTheme(t => { const i = THEME_ORDER.indexOf(t); return THEME_ORDER[(i + 1) % THEME_ORDER.length]; }); break;
        case "t": setActiveTab(t => t === "monitor" ? "deep" : "monitor"); break;
        case "1": setArchiveTab("unified"); break;
        case "2": setArchiveTab("kalshi"); break;
        case "3": setArchiveTab("oracle"); break;
        case "f":
          // Cycle: closed → events → latency → closed
          if (!fullscreenChart) setFullscreenChart("events");
          else if (fullscreenChart === "events") setFullscreenChart("latency");
          else setFullscreenChart(null);
          break;
        case "escape": setFullscreenChart(null); break;
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [fullscreenChart]);

  // Section title helper
  const secTitle = (text, icon, right) => (<div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
    <div style={{ fontFamily: FONT, fontSize: 14, fontWeight: 700, color: T.text, display: "flex", alignItems: "center", gap: 7 }}>
      {icon && <span style={{ fontSize: 15 }}>{icon}</span>}<Tip label={text} theme={theme}><span>{text}</span></Tip>
    </div>
    {right}
  </div>);

  // Card helper
  const card = (children, style = {}) => (<div style={{
    background: T.card, borderRadius: 13, padding: 18, border: `1px solid ${T.border}`, boxShadow: T.shadow,
    backdropFilter: T.backdropBlur, WebkitBackdropFilter: T.backdropBlur,
    transition: "all 0.2s ease", overflow: "hidden", minWidth: 0,
    boxSizing: "border-box", ...style,
  }}>{children}</div>);

  const fmtUptime = (s) => { if (!s) return "\u2014"; const h = Math.floor(s / 3600); const m = Math.floor((s % 3600) / 60); return h > 0 ? `${h}h ${m}m` : `${m}m`; };
  const fmtSeq = (s) => { if (!s) return "0"; return s > 1e6 ? `${(s / 1e6).toFixed(1)}M` : s.toLocaleString(); };

  return (
    <div style={{ minHeight: "100vh", background: T.bg, fontFamily: FONT, color: T.text, transition: "background 0.3s" }}>
      {/* v8.2: Event Inspector overlay */}
      {inspectTs && <FullscreenOverlay onClose={() => setInspectTs(null)} theme={theme}>
        <EventInspector ts={inspectTs} onClose={() => setInspectTs(null)} theme={theme} />
      </FullscreenOverlay>}

      {/* v8.11: Error Details overlay — click on error bar */}
      {errorDetailMinute && <FullscreenOverlay onClose={() => setErrorDetailMinute(null)} theme={theme}>
        <ErrorDetailsPanel minute={errorDetailMinute} onClose={() => setErrorDetailMinute(null)} theme={theme} />
      </FullscreenOverlay>}

      {/* v8.11: Incident History overlay — click on INCIDENTS 24H */}
      {showIncidents && <FullscreenOverlay onClose={() => setShowIncidents(false)} theme={theme}>
        <IncidentHistoryPanel sla={sla} onClose={() => setShowIncidents(false)} theme={theme} />
      </FullscreenOverlay>}

      {/* v8.11: Health Details overlay — click on Health gauge */}
      {showHealthDetails && <FullscreenOverlay onClose={() => setShowHealthDetails(false)} theme={theme}>
        <HealthDetailsPanel healthScore={pbMetrics?.health_score || 0} healthGrade={pbMetrics?.health_grade || "?"} healthComponents={pbMetrics?.health_components || {}} onClose={() => setShowHealthDetails(false)} theme={theme} />
      </FullscreenOverlay>}

      {/* v8.12: Status Detail overlay — click on STATUS card */}
      {showStatusDetail && <FullscreenOverlay onClose={() => setShowStatusDetail(false)} theme={theme}>
        <StatusDetailPanel sla={sla} onClose={() => setShowStatusDetail(false)} theme={theme} />
      </FullscreenOverlay>}

      {/* v9.1: Uptime Detail overlay — click on UPTIME card */}
      {showUptimeDetail && <FullscreenOverlay onClose={() => setShowUptimeDetail(false)} theme={theme}>
        <UptimeDetailPanel sla={sla} onClose={() => setShowUptimeDetail(false)} theme={theme} />
      </FullscreenOverlay>}

      {/* v9.1: MTTR Detail overlay — click on MTTR card */}
      {showMttrDetail && <FullscreenOverlay onClose={() => setShowMttrDetail(false)} theme={theme}>
        <MttrDetailPanel sla={sla} onClose={() => setShowMttrDetail(false)} theme={theme} />
      </FullscreenOverlay>}

      {/* v9.0: Metric Detail overlay — click any STATUS row StatCard */}
      {metricDetail && <FullscreenOverlay onClose={() => setMetricDetail(null)} theme={theme}>
        <MetricDetailPanel metricKey={metricDetail} history={visibleHistory} pbMetrics={pbMetrics} procStats={procStats} onClose={() => setMetricDetail(null)} theme={theme} timeWindow={timeWindow} onTimeWindowChange={setTimeWindow} />
        {/* v9.1: Timeline Playback — only for metrics that have charts */}
        {metricDetail && !["uptime", "dedup", "reconnects"].includes(metricDetail) && (
          <div style={{ marginTop: 12, borderTop: `1px solid ${THEMES[theme].border}`, paddingTop: 8 }}>
            <TimelinePlayback
              history={stablePlayback}
              feedHistory={feedRates.feedHistory}
              theme={theme}
              width={Math.min(window.innerWidth * 0.85, 1350)}
              onPlaybackState={setPlayback}
            />
          </div>
        )}
      </FullscreenOverlay>}

      {/* Fullscreen overlay with chart switcher */}
      {fullscreenChart && <FullscreenOverlay onClose={() => setFullscreenChart(null)} theme={theme}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
          <div style={{ display: "flex", gap: 4, background: T.pillBg, borderRadius: 6, padding: 2, border: `1px solid ${T.pillBorder}` }}>
            {[{ k: "events", label: "Event Rate" }, { k: "latency", label: "Latency" }].map(t => (
              <button key={t.k} onClick={(e) => { e.stopPropagation(); setFullscreenChart(t.k); }} style={{
                fontFamily: MONO, fontSize: 10, fontWeight: fullscreenChart === t.k ? 700 : 500,
                padding: "4px 12px", borderRadius: 4, border: "none", cursor: "pointer",
                background: fullscreenChart === t.k ? T.blue : "transparent",
                color: fullscreenChart === t.k ? "#fff" : T.textMuted,
              }}>{t.label}</button>
            ))}
          </div>
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <OverlayPicker
              chartId={fullscreenChart}
              activeOverlays={fullscreenChart === "events" ? eventOverlays : latencyOverlays}
              onChange={fullscreenChart === "events" ? setEventOverlays : setLatencyOverlays}
              theme={theme}
            />
            <TimeWindowSelector value={timeWindow} onChange={setTimeWindow} theme={theme} />
          </div>
        </div>
        {fullscreenChart === "events" && <EventRateChart history={visibleHistory} theme={theme} width={Math.min(window.innerWidth * 0.85, 1350)} height={500} gapTimestamps={gapTimestamps} onDrillDown={(ts) => { setInspectTs(ts); setFullscreenChart(null); }} overlays={eventOverlays} />}
        {fullscreenChart === "latency" && <LatencyChart history={visibleHistory} theme={theme} width={Math.min(window.innerWidth * 0.85, 1350)} height={500} mode={latMode} onModeChange={setLatMode} overlays={latencyOverlays} />}
        {/* v9.1: Timeline Playback in fullscreen chart overlay */}
        <div style={{ marginTop: 12, borderTop: `1px solid ${T.border}`, paddingTop: 8 }}>
          <TimelinePlayback
            history={stablePlayback}
            feedHistory={feedRates.feedHistory}
            theme={theme}
            width={Math.min(window.innerWidth * 0.85, 1350)}
            onPlaybackState={setPlayback}
          />
        </div>
      </FullscreenOverlay>}

      {/* Header */}
      <div style={{
        background: T.headerBg, borderBottom: `1px solid ${T.headerBorder}`, padding: "12px 24px",
        display: "flex", alignItems: "center", justifyContent: "space-between",
        position: "sticky", top: 0, zIndex: 100, boxShadow: T.shadow,
        backdropFilter: T.backdropBlur,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{ width: 34, height: 34, borderRadius: 8, background: T.logoBg, display: "flex", alignItems: "center", justifyContent: "center", boxShadow: T.shadow }}>
            <svg width="22" height="22" viewBox="0 0 22 22">
              {/* Connecting lines between stars (faint) */}
              <line x1="4" y1="16" x2="11" y2="11" stroke={T.logoText} strokeWidth="0.8" opacity="0.3" />
              <line x1="11" y1="11" x2="18" y2="6" stroke={T.logoText} strokeWidth="0.8" opacity="0.3" />
              {/* Three stars — Orion's Belt diagonal */}
              <circle cx="4" cy="16" r="2.2" fill={T.logoText} opacity="0.85" />
              <circle cx="11" cy="11" r="2.8" fill={T.logoText} opacity="1" />
              <circle cx="18" cy="6" r="2.2" fill={T.logoText} opacity="0.85" />
              {/* Subtle glow on center star */}
              <circle cx="11" cy="11" r="4.5" fill={T.logoText} opacity="0.1" />
            </svg>
          </div>
          <div style={{ fontFamily: FONT, fontSize: 17, fontWeight: 800, letterSpacing: "-0.5px" }}>
            <span style={{ display: "inline-block", transform: "skewX(-18deg)" }}>ORION</span> <span style={{ fontWeight: 400, color: T.textMuted, fontSize: 13 }}>Collector Dashboard</span>
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          {/* Auto-refresh indicator */}
          <div style={{
            width: 8, height: 8, borderRadius: "50%",
            background: refreshPulse ? T.green : T.textMuted,
            transition: "background 0.2s", opacity: 0.7,
          }} title="Refresh pulse" />
          {/* v8.5: Anomaly count badge (v8.9: click scrolls to Alert History) */}
          {anomalyData.count > 0 && <div onClick={() => {
            // v8.9: Switch to Monitor tab and scroll to Alert History
            setActiveTab("monitor");
            setTimeout(() => {
              const el = document.getElementById("alert-history-section");
              if (el) el.scrollIntoView({ behavior: "smooth", block: "center" });
            }, 50);
          }} style={{
            fontFamily: MONO, fontSize: 9, fontWeight: 700, padding: "2px 7px", borderRadius: 8,
            background: anomalyData.count >= 3 ? T.red : T.amber,
            color: "#fff", animation: "pulse 1.5s infinite", cursor: "pointer",
          }} title={`${anomalyData.count} active anomal${anomalyData.count === 1 ? "y" : "ies"} — click to view`}>
            {anomalyData.count} anomal{anomalyData.count === 1 ? "y" : "ies"}
          </div>}
          <span style={{ fontFamily: MONO, fontSize: 10, color: T.textMuted }}>v8.9</span>
          <ThemePicker theme={theme} setTheme={setTheme} />
        </div>
      </div>

      <div ref={containerRef} style={{ padding: "18px 24px" }}>
        {/* Status Banner */}
        <div style={{
          display: "flex", alignItems: "center", gap: 10, padding: "10px 16px", marginBottom: 16, borderRadius: 10,
          background: cm.connected ? T.statusOnlineBg : T.statusOfflineBg,
          border: `1px solid ${cm.connected ? T.green : T.red}25`,
        }}>
          <div style={{ width: 10, height: 10, borderRadius: "50%", background: cm.connected ? T.green : T.red, animation: cm.connected ? "pulse 2s infinite" : "none" }} />
          <span style={{ fontFamily: MONO, fontSize: 12, fontWeight: 700, color: cm.connected ? T.green : T.red }}>
            {cm.connected ? "COLLECTOR ONLINE" : "COLLECTOR OFFLINE"}
          </span>
          {/* Tab switcher — pill-style, between status and time window */}
          <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 10 }}>
            <div style={{ display: "flex", gap: 2, background: T.pillBg, borderRadius: 7, padding: 2, border: `1px solid ${T.pillBorder}` }}>
              {[{ k: "monitor", label: "Monitor" }, { k: "deep", label: "Deep Dive" }].map(t => (
                <button key={t.k} onClick={() => setActiveTab(t.k)} style={{
                  fontFamily: FONT, fontSize: 11, fontWeight: activeTab === t.k ? 700 : 500,
                  padding: "4px 14px", borderRadius: 5, border: "none", cursor: "pointer",
                  background: activeTab === t.k ? T.blue : "transparent",
                  color: activeTab === t.k ? "#fff" : T.textMuted, transition: "all 0.15s",
                }}>
                  {t.label}
                </button>
              ))}
            </div>
            <TimeWindowSelector value={timeWindow} onChange={setTimeWindow} theme={theme} />
          </div>
        </div>

        {/* v8.5: SLA + Health Score Row (v8.9: playback-aware, v8.11: clickable, v8.12: status clickable) */}
        <SLAPanel
          sla={sla}
          healthScore={pbMetrics?.health_score || 0}
          healthGrade={pbMetrics?.health_grade || "?"}
          healthComponents={pbMetrics?.health_components || {}}
          theme={theme}
          onIncidentsClick={() => setShowIncidents(true)}
          onHealthClick={() => setShowHealthDetails(true)}
          onStatusClick={() => setShowStatusDetail(true)}
          onUptimeClick={() => setShowUptimeDetail(true)}
          onMttrClick={() => setShowMttrDetail(true)}
        />

        {/* Stat Cards Row (v8.9: all cards use pbMetrics — shows historical values during playback) */}
        <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
          <StatCard label="Events/s" value={(pbMetrics?.event_rate || 0).toFixed(1)} color={T.green} icon="E/s" theme={theme} glow={cm.connected} alert={alerts.rate} anomaly={anomalyMap.rate} onClick={() => setMetricDetail("events")} />
          <StatCard label="Lat. p50 (Raw)" value={(() => { const v = pbMetrics?.latency_ms?.p50 || 0; return Math.abs(v) >= 100 ? `${Math.round(v)}ms` : `${v.toFixed(1)}ms`; })()} color={Math.abs(pbMetrics?.latency_ms?.p50 || 0) > 50 ? T.amber : T.cyan} icon="ms" theme={theme} alert={alerts.latency} anomaly={anomalyMap.p95} onClick={() => setMetricDetail("latency")} />
          <StatCard label="Queue Depth" value={(pbMetrics?.queue_depth || 0).toLocaleString()} color={(pbMetrics?.queue_depth || 0) > 1000 ? T.red : T.blue} icon="Q" theme={theme} alert={alerts.queue} anomaly={anomalyMap.queue} onClick={() => setMetricDetail("queue")} />
          <StatCard label="Disk Free" value={`${(pbMetrics?.disk_free_gb || 0).toFixed(1)} GB`} color={(pbMetrics?.disk_free_gb || 0) < 5 ? T.red : T.green} icon="HDD" theme={theme} alert={alerts.disk} onClick={() => setMetricDetail("disk")} />
          <StatCard label="Sequence #" value={fmtSeq(pbMetrics?.seq)} color={T.purple} icon="SEQ" theme={theme} onClick={() => setMetricDetail("seq")} />
          <StatCard label="Uptime" value={fmtUptime(pbMetrics?.uptime_seconds)} color={T.cyan} icon="UP" theme={theme} onClick={() => setMetricDetail("uptime")} />
          {procStats.ok && procStats.stats && (
            <StatCard label="Process" value={`${procStats.stats.memory_mb} MB`} color={T.blue} icon="MEM" theme={theme} sub={`PID ${procStats.stats.pid}`} onClick={() => setMetricDetail("process")} />
          )}
          <StatCard label="Dedup" value={(pbMetrics?.dedup_total || 0).toLocaleString()} color={T.textMuted} icon="DD" theme={theme} onClick={() => setMetricDetail("dedup")} />
          <StatCard label="Reconnects" value={(() => { const rc = (pbMetrics?.reconnects_total?.coinbase || 0) + (pbMetrics?.reconnects_total?.kalshi || 0); return rc.toLocaleString(); })()} color={(() => { const rc = (pbMetrics?.reconnects_total?.coinbase || 0) + (pbMetrics?.reconnects_total?.kalshi || 0); return rc > 5 ? T.amber : T.textMuted; })()} icon="RC" theme={theme} alert={((pbMetrics?.reconnects_total?.coinbase || 0) + (pbMetrics?.reconnects_total?.kalshi || 0)) > 5} onClick={() => setMetricDetail("reconnects")} />
          <StatCard label="Bandwidth" value={(() => { const b = pbMetrics?.bytes_per_sec || 0; return b >= 1024 ? `${(b / 1024).toFixed(1)} KB/s` : `${Math.round(b)} B/s`; })()} color={(pbMetrics?.bytes_per_sec || 0) > 512000 ? T.amber : T.blue} icon="BW" theme={theme} onClick={() => setMetricDetail("bandwidth")} />
          <StatCard label="WS Ping" value={(() => { const r = pbMetrics?.ws_rtt_ms?.coinbase ?? -1; return r < 0 ? "N/A" : `${Math.round(r)}ms`; })()} color={(() => { const r = pbMetrics?.ws_rtt_ms?.coinbase ?? -1; return r < 0 ? T.textMuted : r > 1000 ? T.red : r > 200 ? T.amber : T.green; })()} icon="RTT" theme={theme} alert={(pbMetrics?.ws_rtt_ms?.coinbase ?? -1) > 200} sub="Coinbase" onClick={() => setMetricDetail("wsPing")} />
        </div>

        {/* v8.6: Timeline Playback Bar — between stat cards and tabs */}
        {/* v8.9: sticky wrapper — stays visible below the header when scrolling */}
        <div style={{
          position: "sticky", top: 59, zIndex: 50,
          margin: "0 -24px", padding: "6px 24px 6px",
          background: T.bg,
          borderBottom: playback.mode !== "live" ? `1px solid ${T.blue}40` : `1px solid transparent`,
          transition: "border-color 0.2s",
        }}>
          <TimelinePlayback
            history={stablePlayback}
            feedHistory={feedRates.feedHistory}
            theme={theme}
            width={fullW + CP}
            onPlaybackState={setPlayback}
          />
        </div>

        {/* ═══ TAB: MONITOR — real-time operational view ═══ */}
        {activeTab === "monitor" && <>
          {/* Charts Row with click-to-fullscreen */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 16 }}>
            {card(<div style={{ cursor: "zoom-in" }} onClick={() => setFullscreenChart("events")}>
              {secTitle("Event Rate", "~",
                <OverlayPicker chartId="events" activeOverlays={eventOverlays} onChange={setEventOverlays} theme={theme} />
              )}
              <EventRateChart history={visibleHistory} theme={theme} width={halfW} height={220} gapTimestamps={gapTimestamps} overlays={eventOverlays} />
            </div>)}
            {card(<div style={{ cursor: "zoom-in" }} onClick={() => setFullscreenChart("latency")}>
              {secTitle("Latency", "~",
                <OverlayPicker chartId="latency" activeOverlays={latencyOverlays} onChange={setLatencyOverlays} theme={theme} />
              )}
              <LatencyChart history={visibleHistory} theme={theme} width={halfW} height={220} mode={latMode} onModeChange={setLatMode} overlays={latencyOverlays} />
            </div>)}
          </div>

          {/* Health Row: Feed + Tape + Disk */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginBottom: 16 }}>
            {card(<>{secTitle("Feed Health", "+")}<FeedHealthGrid metrics={pbMetrics} theme={theme} /></>)}
            {card(<>{secTitle("Tape Status", "|")}<TapeStatusBars tapes={health.tapes} theme={theme} /></>)}
            {card(<>{secTitle("Disk Monitor", "HDD")}<DiskGauge metrics={pbMetrics} theme={theme} /></>)}
          </div>

          {/* Gap/Error stacked + Alert History + Feed Rates (3-col) */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginBottom: 16 }}>
            {/* Left: Gap Detector on top, Error Rate below — fill full row height */}
            <div style={{ display: "flex", flexDirection: "column", gap: 12, height: "100%" }}>
              {card(<>{secTitle("Gap Detector", "~")}<GapDetectorPanel history={visibleHistory} theme={theme} width={thirdW} height={50} timeWindow={timeWindow} fillHeight={true} /></>, { flex: 1, display: "flex", flexDirection: "column" })}
              {card(<>{secTitle("Error Rate", "!")}<ErrorRatePanel buckets={errorRate.buckets} ok={errorRate.ok} theme={theme} width={thirdW} height={55} timeWindow={timeWindow} fillHeight={true} onBarClick={(minute) => setErrorDetailMinute(minute)} /></>, { flex: 1, display: "flex", flexDirection: "column" })}
            </div>
            {/* Middle: Alert History (v8.9: id for scroll-to from anomaly badge) */}
            <div id="alert-history-section">{card(<>{secTitle("Alert History", "!")}<AlertHistoryPanel alerts={alertHistory.alerts} ok={alertHistory.ok} theme={theme} /></>)}</div>
            {/* Right: System Vitals — sparkline trends for queue, disk, seq */}
            {card(<>{secTitle("System Vitals", "♡")}<SystemVitals history={visibleHistory} theme={theme} width={thirdW} fillHeight={true} /></>, { flex: 1, display: "flex", flexDirection: "column" })}
          </div>
        </>}

        {/* ═══ TAB: DEEP DIVE — historical analysis ═══ */}
        {activeTab === "deep" && <>
          {/* Latency Distribution + Feed Rate Breakdown Row */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 16 }}>
            {card(<>{secTitle("Latency Distribution", "~")}<LatencyHistogramChart metrics={pbHistogramMetrics} theme={theme} width={halfW} /></>)}
            {card(<>{secTitle("Feed Rate Breakdown", "|")}<FeedRateBreakdown rates={pbFeedRates.rates} logTime={pbFeedRates.logTime} ok={pbFeedRates.ok} theme={theme} width={halfW} /></>)}
          </div>

          {/* Network Vitals — v8.2 network-level sparklines */}
          <div style={{ marginBottom: 16 }}>
            {card(<>{secTitle("Network Vitals", "~")}<NetworkVitals history={visibleHistory} theme={theme} width={fullW} /></>)}
          </div>

          {/* Tape Freshness Timeline */}
          <div style={{ marginBottom: 16 }}>
            {card(<>{secTitle("Tape Freshness Timeline", "*")}<FreshnessTimeline freshHistory={pbFreshHistory} theme={theme} width={fullW} timeWindow={timeWindow} /></>)}
          </div>

          {/* Archive Browser + Log Viewer */}
          <div style={{ display: "grid", gridTemplateColumns: "3fr 2fr", gap: 12, marginBottom: 16 }}>
            {card(<>{secTitle("Archive Browser", "#")}<ArchiveBrowser groups={archives.groups} ok={archives.ok} theme={theme} activeTab={archiveTab} setActiveTab={setArchiveTab} /></>)}
            {card(<>{secTitle("Log Viewer", ">")}<LogViewer lines={logs.lines} ok={logs.ok} theme={theme} /></>)}
          </div>

          {/* Config Viewer */}
          <div style={{ marginBottom: 16 }}>
            {card(<>{secTitle("Collector Config", "=", <button onClick={config.refresh} style={{
              fontFamily: MONO, fontSize: 9, padding: "3px 10px", borderRadius: 4,
              border: `1px solid ${T.border}`, background: "transparent", color: T.textMuted, cursor: "pointer",
            }}>Refresh</button>)}<ConfigViewer raw={config.raw} ok={config.ok} onRefresh={config.refresh} theme={theme} /></>)}
          </div>
        </>}

        {/* Keyboard shortcuts help */}
        <div style={{ textAlign: "center", padding: "8px 0 16px", fontFamily: MONO, fontSize: 9, color: T.textMuted, opacity: 0.5 }}>
          T=tabs  D=cycle theme  1/2/3=archive tabs  F=fullscreen  Click-bar=inspect  Esc=close
        </div>

        {/* Offline help */}
        {!cm.connected && (
          <div style={{ textAlign: "center", padding: 30, color: T.textMuted, fontFamily: FONT, marginBottom: 16 }}>
            <div style={{ fontSize: 36, marginBottom: 10 }}>[!]</div>
            <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 6 }}>Live metrics unavailable</div>
            <div style={{ fontSize: 12, maxWidth: 450, margin: "0 auto" }}>
              Start the collector with <span style={{ fontFamily: MONO, background: T.pillBg, padding: "2px 8px", borderRadius: 4 }}>cd collectors && .\start_collectors.ps1</span> to see real-time metrics.
              Tape health, archives, logs, and config still work from files on disk.
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
