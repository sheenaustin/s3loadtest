"""Stop command — Stop running load tests."""

from __future__ import annotations

import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

from s3loadtest.proxy import stop_all as stop_all_proxies
from s3loadtest.workers import get_workers, stop_worker_process


def cmd_stop(args: object) -> int:
    """Stop load tests by finding and killing actual processes.

    Args:
        args: Parsed CLI arguments with ``test_name`` and optional
            ``workers`` attributes.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    test_name = getattr(args, "test_name", None)
    if not test_name:
        print("Error: No test specified")
        print(
            "Usage: s3loadtest stop "
            "[all|baseload|checkpoint|heavyread|"
            "delete|elephant|listops|spiky]"
        )
        return 1

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
                f"Using {len(workers)} of {total_workers} "
                f"available workers\n"
            )

    def find_test_processes(
        hostname: str,
    ) -> tuple[str, list[str]]:
        """Find running test processes on a worker.

        Args:
            hostname: Worker hostname.

        Returns:
            Tuple of (hostname, list_of_pids).
        """
        try:
            if test_name == "all":
                pattern = "s3loadtest run"
            else:
                pattern = f"s3loadtest run {test_name}"

            result = subprocess.run(
                [
                    "ssh",
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "ConnectTimeout=3",
                    hostname,
                    (
                        f"pgrep -af '{pattern}' "
                        f"| grep python | grep -v grep; "
                        f"pgrep -af 'loadtest.py --run"
                        f"{'' if test_name == 'all' else ' ' + test_name}'"
                        f" | grep -v grep || true"
                    ),
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )

            if result.returncode == 0 and result.stdout.strip():
                pids: list[str] = []
                for line in result.stdout.strip().split("\n"):
                    if not line.strip():
                        continue
                    parts = line.split()
                    try:
                        int(parts[0])
                        pids.append(parts[0])
                    except (ValueError, IndexError):
                        continue
                return hostname, pids
        except (subprocess.TimeoutExpired, OSError):
            pass
        return hostname, []

    print(f"Finding {test_name} processes...")

    to_kill: dict[str, list[str]] = {}
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(find_test_processes, worker): worker
            for worker in workers
        }
        for future in as_completed(futures):
            hostname, pids = future.result()
            if pids:
                to_kill[hostname] = pids

    if not to_kill:
        print(f"No {test_name} processes found")
    else:
        total_procs = sum(len(p) for p in to_kill.values())
        print(
            f"Found {total_procs} processes "
            f"on {len(to_kill)} workers"
        )
        print("Stopping...")
        print()

        def kill_processes(
            item: tuple[str, list[str]],
        ) -> tuple[str, int, int]:
            hostname, pids = item
            success = 0
            failed = 0
            for pid in pids:
                if stop_worker_process(hostname, int(pid)):
                    success += 1
                else:
                    failed += 1
            return hostname, success, failed

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {
                executor.submit(kill_processes, item): item
                for item in to_kill.items()
            }
            for future in as_completed(futures):
                hostname, success, failed = future.result()
                if failed > 0:
                    print(
                        f"{hostname:20s} stopped:{success} "
                        f"FAILED:{failed}"
                    )
                else:
                    print(f"{hostname:20s} stopped:{success}")

        print()
        print("Done")

    should_stop_proxy = (
        (test_name == "all")
        or getattr(args, "stop_proxy", False)
    )
    if getattr(args, "no_stop_proxy", False):
        should_stop_proxy = False

    if should_stop_proxy:
        print()
        print("=== Stopping s3pool proxy on workers ===")
        stop_all_proxies(workers)

    return 0
