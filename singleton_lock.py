#!/usr/bin/env python3
"""
singleton_lock.py — Shared singleton lock for all Orion components.
===================================================================
Prevents duplicate instances of collectors, watchdogs, dashboards,
and trading engines.

Each component calls acquire_singleton_lock("component_name") on startup.
This creates a PID file at state/{component_name}.pid containing:
    {pid},{creation_time}

On next startup, the lock checks if the old PID is still alive AND has
the same creation time (to handle PID reuse on Windows). If yes, the
lock is refused (another instance is genuinely running). If the process
is dead or the PID was recycled, the stale file is overwritten.

Usage:
    from singleton_lock import acquire_singleton_lock, release_singleton_lock

    if not acquire_singleton_lock("my_component"):
        print("Another instance is running!")
        sys.exit(2)

    # ... do work ...

    release_singleton_lock("my_component")   # clean shutdown
"""
import os
import sys
import time
from pathlib import Path
from typing import Optional

# ── State directory for PID files ──
STATE_DIR = Path(__file__).resolve().parent.parent / "state"


def _is_process_alive(pid: int) -> bool:
    """
    Check if a process with the given PID is still running.
    Uses ctypes on Windows (os.kill(pid, 0) sends CTRL_C_EVENT on Windows,
    which doesn't reliably check process existence).
    """
    if sys.platform == "win32":
        # Windows: use OpenProcess API to check existence
        import ctypes
        kernel32 = ctypes.windll.kernel32
        # PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = kernel32.OpenProcess(0x1000, False, pid)
        if handle:
            kernel32.CloseHandle(handle)
            return True
        return False
    else:
        # Unix/Mac: os.kill(pid, 0) correctly checks existence
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


def _get_process_create_time(pid: int) -> Optional[float]:
    """Get the creation time of a process (epoch seconds).

    Returns None if the process doesn't exist or can't be queried.
    Used to detect PID reuse — if the creation time doesn't match
    what we stored in the PID file, the PID was recycled by the OS.
    """
    try:
        if sys.platform == "win32":
            import ctypes
            import ctypes.wintypes
            kernel32 = ctypes.windll.kernel32
            # PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = kernel32.OpenProcess(0x1000, False, pid)
            if not handle:
                return None
            try:
                # GetProcessTimes returns creation, exit, kernel, user times
                creation = ctypes.wintypes.FILETIME()
                exit_t   = ctypes.wintypes.FILETIME()
                kernel_t = ctypes.wintypes.FILETIME()
                user_t   = ctypes.wintypes.FILETIME()
                ok = kernel32.GetProcessTimes(
                    handle,
                    ctypes.byref(creation),
                    ctypes.byref(exit_t),
                    ctypes.byref(kernel_t),
                    ctypes.byref(user_t),
                )
                if not ok:
                    return None
                # FILETIME is 100-nanosecond intervals since 1601-01-01.
                # Convert to Unix epoch seconds.
                ft = creation.dwLowDateTime | (creation.dwHighDateTime << 32)
                # Windows epoch starts 11644473600 seconds before Unix epoch
                return (ft / 10_000_000) - 11644473600.0
            finally:
                kernel32.CloseHandle(handle)
        else:
            # Unix: read /proc/{pid}/stat
            with open(f"/proc/{pid}/stat") as f:
                stat = f.read().split()
            # Field 22 (0-indexed: 21) is start time in clock ticks since boot
            ticks = int(stat[21])
            # Get boot time from /proc/stat
            boot_time = 0
            with open("/proc/stat") as f:
                for line in f:
                    if line.startswith("btime"):
                        boot_time = int(line.split()[1])
                        break
            clk_tck = os.sysconf("SC_CLK_TCK")
            return boot_time + ticks / clk_tck
    except Exception:
        return None


def get_pid_file_path(component_name: str) -> Path:
    """Return the PID file path for a component. Useful for error messages."""
    return STATE_DIR / f"{component_name}.pid"


def acquire_singleton_lock(component_name: str) -> bool:
    """
    Try to acquire the singleton lock for a named component.
    Returns True if lock acquired (safe to proceed), False if another
    instance is already running (should exit immediately).

    How it works:
      1. If no PID file exists -> write our PID + creation time, proceed.
      2. If PID file exists -> read the stored PID + creation time.
         a. If that process is still alive AND has the same creation time
            -> another instance is genuinely running, refuse to start.
         b. If process is dead OR creation time doesn't match (PID reused
            by a different process) -> stale file, overwrite and proceed.
    """
    # Make sure state directory exists
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    pid_file = get_pid_file_path(component_name)

    if pid_file.exists():
        try:
            content = pid_file.read_text().strip()
            parts = content.split(",")
            old_pid = int(parts[0])
            old_create_time = float(parts[1]) if len(parts) > 1 else None

            if _is_process_alive(old_pid):
                # Process with that PID exists. But is it OUR old instance,
                # or a completely different process that got the recycled PID?
                actual_create_time = _get_process_create_time(old_pid)
                if (old_create_time is not None
                        and actual_create_time is not None
                        and abs(actual_create_time - old_create_time) < 2.0):
                    # Same creation time (within 2s tolerance) -> genuinely running
                    return False
                # Creation times don't match -> PID was recycled, stale lock
            # Process is dead or PID recycled — safe to overwrite
        except (ValueError, OSError):
            # Corrupted PID file — overwrite it
            pass

    # Write our PID and creation time
    my_create_time = _get_process_create_time(os.getpid()) or time.time()
    pid_file.write_text(f"{os.getpid()},{my_create_time:.3f}")
    return True


def release_singleton_lock(component_name: str) -> None:
    """Remove the PID file on clean shutdown."""
    try:
        pid_file = get_pid_file_path(component_name)
        if pid_file.exists():
            # Only delete if it contains OUR PID (safety check)
            content = pid_file.read_text().strip()
            stored_pid = int(content.split(",")[0])
            if stored_pid == os.getpid():
                pid_file.unlink()
    except (ValueError, OSError):
        # If anything goes wrong, just leave it — next startup will
        # detect the stale PID and overwrite it.
        pass
