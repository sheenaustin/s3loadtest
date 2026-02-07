"""Status command — Show status of running load tests."""

from __future__ import annotations

import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

from s3loadtest.workers import get_workers


def cmd_status(args: object) -> int:
    """Show status of load tests by scanning running processes.

    Args:
        args: Parsed CLI arguments with optional ``test_name``
            and ``workers`` attributes.

    Returns:
        Exit code (always 0).
    """
    workers = get_workers()

    total_workers = len(workers)
    if hasattr(args, "workers") and args.workers:
        if args.workers > total_workers:
            print(
                f"Warning: Requested {args.workers} workers, "
                f"but only {total_workers} available"
            )
        else:
            workers = workers[:args.workers]
            print(
                f"Checking {len(workers)} of {total_workers} "
                f"available workers\n"
            )

    filter_test = (
        getattr(args, "test_name", None)
        if hasattr(args, "test_name") and args.test_name
        else None
    )

    def check_worker(
        hostname: str,
    ) -> tuple[str, list[dict[str, str]]]:
        """Check a worker for running test processes.

        Args:
            hostname: Worker hostname.

        Returns:
            Tuple of (hostname, list_of_process_info_dicts).
        """
        try:
            result = subprocess.run(
                [
                    "ssh",
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "ConnectTimeout=3",
                    hostname,
                    (
                        "pgrep -af 's3loadtest run' "
                        "| grep python | grep -v grep; "
                        "pgrep -af 'loadtest.py --run' "
                        "| grep -v 'bash -c' "
                        "| grep -v grep || true"
                    ),
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )

            if (
                result.returncode == 0
                and result.stdout.strip()
            ):
                processes: list[dict[str, str]] = []
                for line in result.stdout.strip().split("\n"):
                    if not line.strip():
                        continue
                    parts = line.split()

                    try:
                        pid = parts[0]
                        int(pid)
                    except (ValueError, IndexError):
                        continue

                    test_name = None
                    worker_id = None
                    for i, part in enumerate(parts):
                        if (
                            part == "run"
                            and i > 0
                            and "s3loadtest" in parts[i - 1]
                            and i + 1 < len(parts)
                        ):
                            test_name = parts[i + 1]
                        elif (
                            part == "--run"
                            and i + 1 < len(parts)
                        ):
                            test_name = parts[i + 1]
                        elif (
                            part == "--worker-id"
                            and i + 1 < len(parts)
                        ):
                            worker_id = parts[i + 1]

                    if test_name:
                        processes.append({
                            "pid": pid,
                            "test": test_name,
                            "worker_id": worker_id or "?",
                        })
                return hostname, processes
        except (subprocess.TimeoutExpired, OSError):
            pass
        return hostname, []

    print("Scanning all workers...")
    print()

    all_processes: list[tuple[str, dict[str, str]]] = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(check_worker, worker): worker
            for worker in workers
        }
        for future in as_completed(futures):
            hostname, processes = future.result()
            for proc in processes:
                all_processes.append((hostname, proc))

    if filter_test and filter_test != "all":
        all_processes = [
            (h, p) for h, p in all_processes
            if p["test"] == filter_test
        ]

    if not all_processes:
        if filter_test:
            print(f"No {filter_test} tests running")
        else:
            print("No tests running")
        return 0

    print(
        f"{'Host Name':<20s} {'Job Type':<15s} "
        f"{'Worker ID':<10s} {'Process ID':<10s}"
    )
    print("-" * 80)

    def natural_sort_key(
        item: tuple[str, dict[str, str]],
    ) -> tuple[str, ...]:
        hostname, proc = item
        parts = re.findall(r"(\d+)", hostname)
        if len(parts) >= 2:
            rack = int(parts[0])
            slot = int(parts[1])
            letter = (
                hostname.split("-")[-1]
                if "-" in hostname
                else ""
            )
            return (proc["test"], str(rack), str(slot), letter)
        return (proc["test"], hostname)

    all_processes.sort(key=natural_sort_key)

    for hostname, proc in all_processes:
        print(
            f"{hostname:<20s} {proc['test']:<15s} "
            f"{proc['worker_id']:<10s} {proc['pid']:<10s}"
        )

    print()
    print(f"Total: {len(all_processes)} processes")

    return 0
