"""Proxy Management — SSH-based s3pool proxy control on workers.

This module handles starting, stopping, and checking s3pool proxies on
remote worker machines via SSH.

Usage::

    from s3loadtest.proxy import ensure_running, stop_all, check_all

    ensure_running(workers)  # Start proxies where needed
    stop_all(workers)        # Stop all proxies
"""

from __future__ import annotations

import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

from s3loadtest.config import (
    PROXY_BINARY_PATH,
    PROXY_LISTEN_ADDR,
    PROXY_LOG_FILE,
    PROXY_MAX_PARALLEL_SSH,
    REMOTE_CODE_DIR,
)
from s3loadtest.workers import get_workers


def check_proxy(hostname: str) -> tuple[str, bool]:
    """Check if s3pool proxy is running on one host.

    Args:
        hostname: Worker hostname.

    Returns:
        Tuple of (hostname, is_running).
    """
    try:
        cmd = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=5",
            hostname,
            "pgrep -x s3pool >/dev/null && echo OK || echo NO",
        ]
        result = subprocess.run(
            cmd, capture_output=True, timeout=15, text=True,
        )
        return (hostname, "OK" in result.stdout)
    except (subprocess.TimeoutExpired, OSError):
        return (hostname, False)


def start_proxy(hostname: str) -> tuple[str, bool, str]:
    """Start s3pool proxy on one host.

    Args:
        hostname: Worker hostname.

    Returns:
        Tuple of (hostname, success, message).
    """
    try:
        cd_prefix = ""
        if REMOTE_CODE_DIR:
            cd_prefix = f"cd {REMOTE_CODE_DIR} && "

        script = (
            f"pkill -9 s3pool 2>/dev/null; sleep 0.3; "
            f"{cd_prefix}"
            f"nohup {PROXY_BINARY_PATH} proxy --insecure "
            f"--listen {PROXY_LISTEN_ADDR} "
            f"</dev/null >{PROXY_LOG_FILE} 2>&1 & "
            f"sleep 1; pgrep -x s3pool >/dev/null "
            f"&& echo OK || echo FAIL"
        )
        cmd = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=5",
            hostname,
            script,
        ]
        result = subprocess.run(
            cmd, capture_output=True, timeout=20, text=True,
        )
        if "OK" in result.stdout:
            return (hostname, True, "started")
        return (
            hostname,
            False,
            result.stdout.strip()[:80] or "no output",
        )
    except subprocess.TimeoutExpired:
        return (hostname, False, "timeout")
    except OSError as exc:
        return (hostname, False, str(exc))


def stop_proxy(hostname: str) -> tuple[str, bool, str]:
    """Stop s3pool proxy on one host.

    Args:
        hostname: Worker hostname.

    Returns:
        Tuple of (hostname, success, message).
    """
    try:
        cmd = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=5",
            hostname,
            (
                "pkill -9 s3pool 2>/dev/null; sleep 0.3; "
                "pgrep -x s3pool >/dev/null "
                "&& echo FAIL || echo OK"
            ),
        ]
        result = subprocess.run(
            cmd, capture_output=True, timeout=15, text=True,
        )
        if "OK" in result.stdout:
            return (hostname, True, "stopped")
        return (hostname, False, "process still running")
    except subprocess.TimeoutExpired:
        return (hostname, False, "timeout")
    except OSError as exc:
        return (hostname, False, str(exc))


def check_all(
    workers: list[str] | None = None,
    *,
    max_parallel: int | None = None,
) -> tuple[list[str], list[str]]:
    """Check proxy status on all workers.

    Args:
        workers: List of worker hostnames (default: from workers file).
        max_parallel: Max concurrent SSH connections.

    Returns:
        Tuple of (running_list, not_running_list).
    """
    if workers is None:
        workers = get_workers()
    if max_parallel is None:
        max_parallel = PROXY_MAX_PARALLEL_SSH

    running: list[str] = []
    not_running: list[str] = []

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {
            executor.submit(check_proxy, w): w for w in workers
        }
        for future in as_completed(futures):
            hostname, is_running = future.result()
            if is_running:
                running.append(hostname)
            else:
                not_running.append(hostname)

    return running, not_running


def ensure_running(
    workers: list[str] | None = None,
    *,
    force_restart: bool = False,
    max_parallel: int | None = None,
) -> tuple[int, int, int]:
    """Ensure s3pool proxy is running on all workers.

    Only starts proxy on workers where it's not already running, unless
    ``force_restart=True`` which restarts all proxies.

    Args:
        workers: List of worker hostnames (default: from workers file).
        force_restart: If True, restart all proxies even if running.
        max_parallel: Max concurrent SSH connections.

    Returns:
        Tuple of (already_running, started, failed).
    """
    if workers is None:
        workers = get_workers()
    if max_parallel is None:
        max_parallel = PROXY_MAX_PARALLEL_SSH

    if force_restart:
        print(
            f"Force-restarting s3pool proxy on "
            f"{len(workers)} workers..."
        )
        workers_to_start = workers
        already_running = 0
    else:
        print(
            f"Checking s3pool proxy status on "
            f"{len(workers)} workers..."
        )
        running, not_running = check_all(
            workers, max_parallel=max_parallel,
        )
        already_running = len(running)
        workers_to_start = not_running

        if not workers_to_start:
            print(
                f"  All {already_running} workers "
                f"already have proxy running"
            )
            return already_running, 0, 0

        print(
            f"  Running: {already_running}, "
            f"Need start: {len(workers_to_start)}"
        )

    started = 0
    failed = 0
    failures: list[tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {
            executor.submit(start_proxy, w): w
            for w in workers_to_start
        }
        for future in as_completed(futures):
            hostname, success, message = future.result()
            if success:
                started += 1
            else:
                failed += 1
                failures.append((hostname, message))

    print(f"  Started: {started}, Failed: {failed}")
    if failures:
        for hostname, msg in failures[:5]:
            print(f"    FAIL {hostname}: {msg}")
        if len(failures) > 5:
            print(f"    ... and {len(failures) - 5} more failures")

    return already_running, started, failed


def start_all(
    workers: list[str] | None = None,
    *,
    max_parallel: int | None = None,
) -> tuple[int, int, list[tuple[str, str]]]:
    """Start/restart s3pool proxy on all workers (force restart).

    Args:
        workers: List of worker hostnames (default: from workers file).
        max_parallel: Max concurrent SSH connections.

    Returns:
        Tuple of (success_count, fail_count, failures_list).
    """
    if workers is None:
        workers = get_workers()
    if max_parallel is None:
        max_parallel = PROXY_MAX_PARALLEL_SSH

    print(f"Starting s3pool proxy on {len(workers)} workers...")

    success_count = 0
    fail_count = 0
    failures: list[tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {
            executor.submit(start_proxy, w): w for w in workers
        }
        for future in as_completed(futures):
            hostname, success, message = future.result()
            if success:
                success_count += 1
            else:
                fail_count += 1
                failures.append((hostname, message))

    print(
        f"  Proxy started: {success_count}, "
        f"Failed: {fail_count}"
    )
    if failures:
        for hostname, msg in failures[:5]:
            print(f"    FAIL {hostname}: {msg}")
        if len(failures) > 5:
            print(f"    ... and {len(failures) - 5} more failures")

    return success_count, fail_count, failures


def stop_all(
    workers: list[str] | None = None,
    *,
    max_parallel: int | None = None,
) -> tuple[int, int]:
    """Stop s3pool proxy on all workers.

    Args:
        workers: List of worker hostnames (default: from workers file).
        max_parallel: Max concurrent SSH connections.

    Returns:
        Tuple of (success_count, fail_count).
    """
    if workers is None:
        workers = get_workers()
    if max_parallel is None:
        max_parallel = PROXY_MAX_PARALLEL_SSH

    print(f"Stopping s3pool proxy on {len(workers)} workers...")

    success_count = 0
    fail_count = 0

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {
            executor.submit(stop_proxy, w): w for w in workers
        }
        for future in as_completed(futures):
            hostname, success, message = future.result()
            if success:
                success_count += 1
            else:
                fail_count += 1

    print(
        f"  Proxy stopped: {success_count}, "
        f"Failed: {fail_count}"
    )
    return success_count, fail_count
