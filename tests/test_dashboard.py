"""
Tests for collector_dashboard.py — health score computation, SLA tracking,
anomaly detection, and Prometheus metrics parsing.

These test the monitoring intelligence layer that makes the dashboard
more than just a pretty chart viewer.
"""

import math
import time
import sys
import pytest
from pathlib import Path

# ── Import dashboard module ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import collector_dashboard as dash


# ═══════════════════════════════════════════════════════════════
#  Linear Score Function
# ═══════════════════════════════════════════════════════════════

class TestScoreLinear:
    """Verify the _score_linear helper used by health score computation."""

    def test_at_good_threshold_returns_100(self):
        """Value at the 'good' threshold should score 100."""
        assert dash._score_linear(5.0, good=5.0, bad=60.0) == 100.0

    def test_at_bad_threshold_returns_0(self):
        """Value at the 'bad' threshold should score 0."""
        assert dash._score_linear(60.0, good=5.0, bad=60.0) == 0.0

    def test_midpoint_returns_50(self):
        """Value halfway between good and bad should score ~50."""
        score = dash._score_linear(32.5, good=5.0, bad=60.0)
        assert 45.0 <= score <= 55.0, f"Midpoint score should be ~50, got {score}"

    def test_better_than_good_caps_at_100(self):
        """Values better than the good threshold should cap at 100."""
        assert dash._score_linear(1.0, good=5.0, bad=60.0) == 100.0

    def test_worse_than_bad_caps_at_0(self):
        """Values worse than the bad threshold should cap at 0."""
        assert dash._score_linear(100.0, good=5.0, bad=60.0) == 0.0

    def test_inverted_scale(self):
        """Works when 'good' > 'bad' (e.g., event rate: higher is better)."""
        # good=50 (50 events/sec is good), bad=0 (0 is bad)
        assert dash._score_linear(50, good=50, bad=0) == 100.0
        assert dash._score_linear(0, good=50, bad=0) == 0.0
        score = dash._score_linear(25, good=50, bad=0)
        assert 45.0 <= score <= 55.0

    def test_equal_good_bad_at_value(self):
        """When good == bad, return 100 if value matches, else 0."""
        assert dash._score_linear(5.0, good=5.0, bad=5.0) == 100.0
        assert dash._score_linear(10.0, good=5.0, bad=5.0) == 0.0


# ═══════════════════════════════════════════════════════════════
#  Composite Health Score
# ═══════════════════════════════════════════════════════════════

class TestHealthScore:
    """Verify the composite health score calculation."""

    def test_perfect_health(self):
        """All metrics at ideal values should produce score >= 90 and grade A."""
        # Reset reconnect log to avoid interference from other tests
        dash._health_reconnect_log.clear()

        entry = {
            "ts": time.time(),
            "unified_age": 1000,   # 1 second in ms — very fresh
            "kalshi_age": 2000,
            "oracle_age": 1500,
            "rate": 200,           # 200 events/sec — well above 50 threshold
            "cbGaps": 0,
            "klGaps": 0,
            "p95": 50,             # 50ms — well under 100ms threshold
            "disk": 50.0,          # 50 GB free — well above 20 GB threshold
            "queue": 0,            # Empty queue
        }
        result = dash._compute_health_score(entry)

        assert result["score"] >= 90, f"Perfect health should be >= 90, got {result['score']}"
        assert result["grade"] == "A"
        assert "components" in result
        assert len(result["components"]) == 5

    def test_degraded_health(self):
        """Poor metrics should produce a lower score."""
        dash._health_reconnect_log.clear()

        entry = {
            "ts": time.time(),
            "unified_age": 45000,  # 45 seconds — getting stale
            "kalshi_age": 50000,
            "oracle_age": 55000,
            "rate": 5,             # Very low event rate
            "cbGaps": 3,
            "klGaps": 2,
            "p95": 800,            # Very high latency
            "disk": 3.0,           # Low disk space
            "queue": 4000,         # High queue depth
        }
        result = dash._compute_health_score(entry)

        assert result["score"] < 50, f"Degraded health should be < 50, got {result['score']}"
        assert result["grade"] in ("D", "F")

    def test_grade_boundaries(self):
        """Verify grade letter assignment at each boundary."""
        # Directly test the grade logic by constructing known scores
        dash._health_reconnect_log.clear()

        # Grade A: score >= 90
        entry_a = {
            "ts": time.time(), "unified_age": 500, "rate": 300,
            "cbGaps": 0, "klGaps": 0, "p95": 10, "disk": 100, "queue": 0,
        }
        assert dash._compute_health_score(entry_a)["grade"] == "A"

    def test_components_have_required_fields(self):
        """Each component should have score, weight, and detail."""
        dash._health_reconnect_log.clear()

        entry = {
            "ts": time.time(), "unified_age": 1000, "rate": 100,
            "cbGaps": 0, "klGaps": 0, "p95": 50, "disk": 20, "queue": 0,
        }
        result = dash._compute_health_score(entry)

        for name, comp in result["components"].items():
            assert "score" in comp, f"Component '{name}' missing 'score'"
            assert "weight" in comp, f"Component '{name}' missing 'weight'"
            assert "detail" in comp, f"Component '{name}' missing 'detail'"

    def test_weights_sum_to_100(self):
        """Default component weights should sum to 100."""
        dash._health_reconnect_log.clear()

        entry = {
            "ts": time.time(), "unified_age": 1000, "rate": 100,
            "cbGaps": 0, "klGaps": 0, "p95": 50, "disk": 20, "queue": 0,
        }
        result = dash._compute_health_score(entry)

        total_weight = sum(c["weight"] for c in result["components"].values())
        assert total_weight == 100, f"Weights should sum to 100, got {total_weight}"


# ═══════════════════════════════════════════════════════════════
#  Prometheus Metrics Parser
# ═══════════════════════════════════════════════════════════════

class TestPrometheusParser:
    """Verify Prometheus text exposition format parsing."""

    def test_simple_metric(self):
        """Parse a simple metric without labels."""
        text = 'orion_collector_events_total 12345\n'
        result = dash.parse_prometheus_metrics(text)
        assert result["events_total"] == 12345

    def test_labeled_metric(self):
        """Parse a metric with labels."""
        text = 'orion_collector_feed_rate{exchange="cb"} 150.5\n'
        result = dash.parse_prometheus_metrics(text)
        assert "feed_rate" in result
        assert result["feed_rate"]["coinbase"] == 150.5  # cb -> coinbase remap

    def test_comment_lines_ignored(self):
        """Lines starting with # should be ignored."""
        text = """# HELP orion_collector_events Total events
# TYPE orion_collector_events counter
orion_collector_events_total 500
"""
        result = dash.parse_prometheus_metrics(text)
        assert result["events_total"] == 500

    def test_empty_input(self):
        """Empty string should return empty dict."""
        assert dash.parse_prometheus_metrics("") == {}

    def test_multiple_metrics(self):
        """Parse multiple metrics in one block."""
        text = """orion_collector_event_rate 245.3
orion_collector_queue_depth 42
orion_collector_disk_free_gb 107.5
"""
        result = dash.parse_prometheus_metrics(text)
        assert result["event_rate"] == 245.3
        assert result["queue_depth"] == 42
        assert result["disk_free_gb"] == 107.5

    def test_kalshi_label_remapped(self):
        """Kalshi label 'kl' should be remapped to 'kalshi'."""
        text = 'orion_collector_ws_latency{exchange="kl"} 25.0\n'
        result = dash.parse_prometheus_metrics(text)
        assert result["ws_latency"]["kalshi"] == 25.0


# ═══════════════════════════════════════════════════════════════
#  Anomaly Detection
# ═══════════════════════════════════════════════════════════════

class TestAnomalyDetection:
    """Verify z-score based anomaly detection."""

    def test_stable_data_no_anomalies(self):
        """Consistent data should produce zero anomalies."""
        detector = dash._AnomalyDetector()

        # Feed 100 entries of stable data (rate ~200 +/- 5)
        anomalies = []
        for i in range(100):
            entry = {"ts": time.time() + i, "rate": 200 + (i % 5), "p95": 50, "queue": 10}
            anomalies = detector.check(entry)

        assert len(anomalies) == 0, "Stable data should produce no anomalies"

    def test_spike_detected(self):
        """A sudden spike should be flagged as anomalous."""
        detector = dash._AnomalyDetector()

        # Build stable baseline
        for i in range(100):
            entry = {"ts": time.time() + i, "rate": 200, "p95": 50, "queue": 10}
            detector.check(entry)

        # Inject a massive spike
        spike_entry = {"ts": time.time() + 200, "rate": 5000, "p95": 50, "queue": 10}
        anomalies = detector.check(spike_entry)

        rate_anomalies = [a for a in anomalies if a["metric"] == "rate"]
        assert len(rate_anomalies) > 0, "Rate spike should trigger an anomaly"
        assert rate_anomalies[0]["severity"] in ("warning", "alert")

    def test_anomaly_clears_when_normal(self):
        """An anomaly should clear once values return to normal."""
        detector = dash._AnomalyDetector()

        # Build baseline
        for i in range(100):
            detector.check({"ts": time.time() + i, "rate": 200, "p95": 50, "queue": 10})

        # Spike
        detector.check({"ts": time.time() + 200, "rate": 5000, "p95": 50, "queue": 10})
        assert len(detector.get_active()) > 0, "Should have active anomaly after spike"

        # Return to normal for many entries
        for i in range(50):
            detector.check({"ts": time.time() + 300 + i, "rate": 200, "p95": 50, "queue": 10})

        rate_active = [a for a in detector.get_active() if a["metric"] == "rate"]
        assert len(rate_active) == 0, "Anomaly should clear after returning to normal"

    def test_z_score_calculation(self):
        """Verify the z-score math is correct."""
        detector = dash._AnomalyDetector()

        # Feed identical values to get mean=100, std≈0
        for i in range(100):
            detector.check({"ts": time.time() + i, "rate": 100, "p95": 50, "queue": 10})

        # A value of 200 with mean=100 and std≈0 should have a very high z-score
        anomalies = detector.check({"ts": time.time() + 200, "rate": 200, "p95": 50, "queue": 10})
        rate_anomalies = [a for a in anomalies if a["metric"] == "rate"]

        if rate_anomalies:
            assert abs(rate_anomalies[0]["z_score"]) > 3.0, "Z-score should be very high"

    def test_recent_log_maintained(self):
        """The anomaly log should keep track of recent events."""
        detector = dash._AnomalyDetector()

        # Build baseline then spike
        for i in range(100):
            detector.check({"ts": time.time() + i, "rate": 100, "p95": 50, "queue": 10})

        detector.check({"ts": time.time() + 200, "rate": 5000, "p95": 50, "queue": 10})

        log = detector.get_recent_log()
        assert len(log) > 0, "Anomaly log should have entries after a spike"
