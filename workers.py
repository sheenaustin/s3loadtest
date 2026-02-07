"""Worker Management — Control test processes on remote workers via SSH.

Usage::

    from s3loadtest.workers import get_workers, start_worker_process

    workers = get_workers()
    start_worker_process(hostname, "baseload", worker_id=0)
"""

from __future__ import annotations

import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from s3loadtest.config import (
    LOG_DIR,
    REMOTE_CODE_DIR,
    REMOTE_LOG_DIR,
    WORKERS_FILE,
)


def get_workers() -> list[str]:
    """Get list of worker hostnames from the workers file.

    Supported formats (one entry per line):
        - ``hostname``  — plain hostname or IP
        - ``IP  hostname.role``  — extracts hostname without suffix
        - Lines starting with ``#`` are comments
        - Blank lines are ignored

    Returns:
        List of worker hostnames or IP addresses.
    """
    if not os.path.exists(WORKERS_FILE):
        return []

    workers: list[str] = []
    try:
        with open(WORKERS_FILE) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split()
                if len(parts) == 1:
                    workers.append(parts[0])
                elif len(parts) >= 2:
                    hostname = parts[1]
                    if "." in hostname:
                        hostname = hostname.rsplit(".", 1)[0]
                    workers.append(hostname)
    except OSError:
        return []
    return workers


def _build_remote_command(
    *,
    test_name: str,
    worker_id: int,
    args_str: str,
    log_file: str,
) -> str:
    """Build the remote command string for SSH execution."""
    if REMOTE_CODE_DIR:
        return (
            f"cd {REMOTE_CODE_DIR} && "
            f"python3 -m s3loadtest run {test_name} "
            f"--worker-id {worker_id} {args_str} "
            f"</dev/null >{log_file} 2>&1 &"
        )
    return (
        f"s3loadtest run {test_name} "
        f"--worker-id {worker_id} {args_str} "
        f"</dev/null >{log_file} 2>&1 &"
    )


def start_worker_process(
    hostname: str,
    test_name: str,
    worker_id: int,
    duration_seconds: int | None = None,
    log_level: str | None = None,
    stats_interval: int = 30,
) -> tuple[str, str]:
    """Start a test process on a remote worker via SSH.

    Args:
        hostname: Worker hostname.
        test_name: Name of test to run.
        worker_id: Worker ID number.
        duration_seconds: Optional duration in seconds.
        log_level: Optional log level.
        stats_interval: Stats logging interval in seconds.

    Returns:
        Tuple of (hostname, status) where status is
        ``started``, ``already_running``, or ``error``.
    """
    _pgrep = _build_pgrep_pattern(test_name, worker_id)
    check_cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=5",
        hostname,
        (
            f"pgrep -af '{_pgrep}' | grep python | grep -v grep"
        ),
    ]

    try:
        result = subprocess.run(
            check_cmd, capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return hostname, "already_running"
    except (subprocess.TimeoutExpired, OSError):
        pass

    cmd_args: list[str] = []
    if duration_seconds:
        cmd_args.append(f"--duration {duration_seconds}")
    if log_level:
        cmd_args.append(f"--log-level {log_level}")
    if stats_interval is not None:
        cmd_args.append(f"--stats-interval {stats_interval}")
    args_str = " ".join(cmd_args)

    log_dir = REMOTE_LOG_DIR or LOG_DIR
    log_file = os.path.join(
        log_dir, f"loadtest_{test_name}_{worker_id}.log",
    )
    remote_cmd = _build_remote_command(
        test_name=test_name,
        worker_id=worker_id,
        args_str=args_str,
        log_file=log_file,
    )

    start_cmd = [
        "ssh", "-f",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=5",
        hostname,
        remote_cmd,
    ]

    try:
        subprocess.run(
            start_cmd, capture_output=True, text=True, timeout=30,
        )
        return hostname, "started"
    except subprocess.TimeoutExpired:
        return hostname, "started"
    except OSError:
        return hostname, "error"


def stop_worker_process(hostname: str, pid: int) -> bool:
    """Stop a test process on a remote worker.

    Args:
        hostname: Worker hostname.
        pid: Process ID to stop.

    Returns:
        True if the process was stopped successfully.
    """
    check_cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=5",
        hostname,
        f"kill -0 {pid} 2>/dev/null",
    ]

    try:
        result = subprocess.run(
            check_cmd, timeout=10, capture_output=True,
        )
        if result.returncode != 0:
            return True
    except (subprocess.TimeoutExpired, OSError):
        return False

    kill_cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=5",
        hostname,
        f"kill -9 {pid} 2>/dev/null",
    ]

    try:
        subprocess.run(kill_cmd, timeout=10, capture_output=True)
        time.sleep(0.1)
        result = subprocess.run(
            check_cmd, timeout=10, capture_output=True,
        )
        return result.returncode != 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def _build_pgrep_pattern(
    test_name: str | None = None,
    worker_id: int | None = None,
) -> str:
    """Build a pgrep pattern matching both old and new invocations."""
    if test_name and worker_id is not None:
        return (
            f"s3loadtest run {test_name} "
            f"--worker-id {worker_id}"
        )
    if test_name:
        return f"s3loadtest run {test_name}"
    return "s3loadtest run"


def find_running_tests(
    workers: list[str] | None = None,
    test_name: str | None = None,
) -> dict[str, list[tuple[int, str, int]]]:
    """Find running test processes on workers.

    Args:
        workers: List of worker hostnames (default: from file).
        test_name: Optional test name filter.

    Returns:
        Dict mapping hostname to list of
        ``(pid, test_name, worker_id)`` tuples.
    """
    if workers is None:
        workers = get_workers()

    def check_worker(
        hostname: str,
    ) -> tuple[str, list[tuple[int, str, int]]]:
        pattern = _build_pgrep_pattern(test_name)
        cmd = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=5",
            hostname,
            (
                f"pgrep -af '{pattern}' 2>/dev/null; "
                f"pgrep -af 'loadtest.py --run' 2>/dev/null "
                f"|| true"
            ),
        ]

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=10,
            )
            processes: list[tuple[int, str, int]] = []
            seen_pids: set[int] = set()
            for line in result.stdout.strip().split("\n"):
                if not line or "python" not in line:
                    continue
                parts = line.split()
                try:
                    pid = int(parts[0])
                except (ValueError, IndexError):
                    continue
                if pid in seen_pids:
                    continue
                seen_pids.add(pid)
                t_name = _extract_test_info(parts)
                if t_name:
                    w_id = _extract_worker_id(parts)
                    processes.append((pid, t_name, w_id))
            return hostname, processes
        except (subprocess.TimeoutExpired, OSError):
            return hostname, []

    results: dict[str, list[tuple[int, str, int]]] = {}
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {
            executor.submit(check_worker, w): w for w in workers
        }
        for future in as_completed(futures):
            hostname, processes = future.result()
            if processes:
                results[hostname] = processes

    return results


def _extract_test_info(parts: list[str]) -> str | None:
    """Extract test name from process command line parts."""
    for idx, part in enumerate(parts):
        if part == "--run" and idx + 1 < len(parts):
            return parts[idx + 1]
        if (
            part == "run"
            and idx > 0
            and "s3loadtest" in parts[idx - 1]
            and idx + 1 < len(parts)
        ):
            return parts[idx + 1]
    return None


def _extract_worker_id(parts: list[str]) -> int:
    """Extract worker ID from process command line parts."""
    for idx, part in enumerate(parts):
        if part == "--worker-id" and idx + 1 < len(parts):
            try:
                return int(parts[idx + 1])
            except ValueError:
                pass
    return 0
