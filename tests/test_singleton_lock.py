"""
Tests for singleton_lock.py — cross-platform mutex preventing duplicate
collector instances.

Verifies lock acquisition, release, stale PID cleanup, and the process
alive detection used to handle PID reuse after crashes.
"""

import os
import sys
import time
import pytest
from pathlib import Path

# ── Import singleton_lock module ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import singleton_lock


# ═══════════════════════════════════════════════════════════════
#  PID File Path
# ═══════════════════════════════════════════════════════════════

class TestPidFilePath:
    """Verify PID file path generation."""

    def test_returns_path_object(self):
        """get_pid_file_path should return a Path object."""
        result = singleton_lock.get_pid_file_path("test_component")
        assert isinstance(result, Path)

    def test_includes_component_name(self):
        """PID file should include the component name."""
        result = singleton_lock.get_pid_file_path("my_collector")
        assert "my_collector" in result.name

    def test_has_pid_extension(self):
        """PID file should have .pid extension."""
        result = singleton_lock.get_pid_file_path("test")
        assert result.suffix == ".pid"


# ═══════════════════════════════════════════════════════════════
#  Process Detection
# ═══════════════════════════════════════════════════════════════

class TestProcessAlive:
    """Verify process alive detection."""

    def test_current_process_is_alive(self):
        """The current process should be detected as alive."""
        assert singleton_lock._is_process_alive(os.getpid()) is True

    def test_nonexistent_pid_is_dead(self):
        """A very high PID that doesn't exist should be dead."""
        # Use a PID that almost certainly doesn't exist
        assert singleton_lock._is_process_alive(99999999) is False

    def test_zero_pid_is_dead(self):
        """PID 0 should be reported as not alive (or handled gracefully)."""
        # PID 0 is the kernel scheduler on Unix, shouldn't crash
        result = singleton_lock._is_process_alive(0)
        assert isinstance(result, bool)


# ═══════════════════════════════════════════════════════════════
#  Lock Acquire / Release
# ═══════════════════════════════════════════════════════════════

class TestLockAcquireRelease:
    """Verify the full lock lifecycle."""

    def _override_state_dir(self, tmp_path):
        """Point singleton_lock at a temp directory for testing."""
        original = singleton_lock.STATE_DIR
        singleton_lock.STATE_DIR = tmp_path / "state"
        singleton_lock.STATE_DIR.mkdir(parents=True, exist_ok=True)
        return original

    def test_acquire_creates_pid_file(self, tmp_path):
        """Acquiring a lock should create a PID file."""
        original = self._override_state_dir(tmp_path)
        try:
            result = singleton_lock.acquire_singleton_lock("test_acq")
            assert result is True

            pid_file = singleton_lock.get_pid_file_path("test_acq")
            assert pid_file.exists(), "PID file should exist after acquiring lock"
        finally:
            singleton_lock.release_singleton_lock("test_acq")
            singleton_lock.STATE_DIR = original

    def test_release_removes_pid_file(self, tmp_path):
        """Releasing a lock should remove the PID file."""
        original = self._override_state_dir(tmp_path)
        try:
            singleton_lock.acquire_singleton_lock("test_rel")
            singleton_lock.release_singleton_lock("test_rel")

            pid_file = singleton_lock.get_pid_file_path("test_rel")
            assert not pid_file.exists(), "PID file should be gone after release"
        finally:
            singleton_lock.STATE_DIR = original

    def test_pid_file_contains_current_pid(self, tmp_path):
        """PID file should contain the current process PID in csv format."""
        original = self._override_state_dir(tmp_path)
        try:
            singleton_lock.acquire_singleton_lock("test_pid")
            pid_file = singleton_lock.get_pid_file_path("test_pid")

            # PID file format is: {pid},{creation_time}
            content = pid_file.read_text().strip()
            parts = content.split(",")
            stored_pid = int(parts[0])

            assert stored_pid == os.getpid()
            # Should also have a creation time as second field
            assert len(parts) == 2, "PID file should have pid,creation_time format"
            float(parts[1])  # Should be parseable as float
        finally:
            singleton_lock.release_singleton_lock("test_pid")
            singleton_lock.STATE_DIR = original

    def test_double_acquire_same_process_blocks(self, tmp_path):
        """Same process acquiring the same lock twice should return False
        because the lock detects the existing live process."""
        original = self._override_state_dir(tmp_path)
        try:
            result1 = singleton_lock.acquire_singleton_lock("test_double")
            result2 = singleton_lock.acquire_singleton_lock("test_double")

            # First acquire succeeds
            assert result1 is True
            # Second acquire returns False — same PID is alive with matching
            # creation time, so the lock correctly refuses (it sees a running instance)
            assert result2 is False
        finally:
            singleton_lock.release_singleton_lock("test_double")
            singleton_lock.STATE_DIR = original

    def test_stale_lock_from_dead_process_reclaimed(self, tmp_path):
        """A PID file from a dead process should be reclaimed."""
        original = self._override_state_dir(tmp_path)
        try:
            # Manually write a PID file for a dead process
            # Format: {pid},{creation_time}
            pid_file = tmp_path / "state" / "test_stale.pid"
            pid_file.parent.mkdir(parents=True, exist_ok=True)
            pid_file.write_text("99999999,1000000.000")

            # Should reclaim the stale lock
            result = singleton_lock.acquire_singleton_lock("test_stale")
            assert result is True, "Should reclaim lock from dead process"
        finally:
            singleton_lock.release_singleton_lock("test_stale")
            singleton_lock.STATE_DIR = original
