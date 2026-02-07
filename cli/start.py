from __future__ import annotations

"""Start command - Launch load tests on workers."""

import time
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor, as_completed

from s3loadtest.config import S3_BUCKET
from s3loadtest.proxy import ensure_running
from s3loadtest.workers import get_workers, start_worker_process
from s3loadtest.s3_client import S3Client
from s3loadtest.tests import TEST_CLASSES
from s3loadtest.utils import parse_duration, format_duration


def check_bucket_access(timeout_seconds: int = 10) -> tuple[bool, str]:
    """Check if the S3 bucket is accessible.

    Attempts to list a single object from the bucket to verify:
    - Network connectivity to S3 endpoints
    - Credentials are valid
    - Bucket exists and is accessible

    Args:
        timeout_seconds: How long to wait for the check

    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        client = S3Client()
        # Try to list just 1 object - validates connectivity, auth, and bucket access
        client.list_objects(prefix="", max_keys=1)
        return True, f"Bucket '{S3_BUCKET}' is accessible"
    except Exception as e:
        error_msg = str(e)
        if "NoSuchBucket" in error_msg:
            return False, f"Bucket '{S3_BUCKET}' does not exist"
        elif "AccessDenied" in error_msg or "InvalidAccessKeyId" in error_msg:
            return False, f"Access denied to bucket '{S3_BUCKET}' - check credentials"
        elif "Could not connect" in error_msg or "Connection refused" in error_msg:
            return False, f"Cannot connect to S3 endpoint - check proxy is running"
        elif "502" in error_msg or "Backend" in error_msg:
            return False, f"Proxy cannot reach S3 backend - check S3 endpoints in .env"
        elif "timeout" in error_msg.lower():
            return False, f"Connection timeout to S3 endpoint"
        else:
            return False, f"Bucket access failed: {error_msg}"


def cmd_start(args: Namespace) -> int:
    """Start load tests."""
    workers = get_workers()

    if not workers:
        print("Error: No workers found in workers file")
        return 1

    # Limit number of workers if specified
    total_workers = len(workers)
    if hasattr(args, "workers") and args.workers:
        if args.workers > total_workers:
            print(f"Warning: Requested {args.workers} workers, but only {total_workers} available")
        else:
            workers = workers[:args.workers]
            print(f"Using {len(workers)} of {total_workers} available workers\n")

    # Ensure s3pool proxy is running on workers (unless --no-proxy is specified)
    if not getattr(args, "no_proxy", False):
        force_restart = getattr(args, "force_restart_proxy", False)
        print("=== Checking s3pool proxy on workers ===")
        already_running, started, failed = ensure_running(workers, force_restart=force_restart)

        # Check if we have any working proxies
        working_proxies = already_running + started
        if working_proxies == 0:
            print(f"\nError: No proxies running on any worker ({failed} failed to start)")
            print("Cannot start tests without proxy. Fix the issue or use --no-proxy to skip.")
            return 1
        elif failed > 0:
            print(f"Warning: {failed} workers failed to start proxy ({working_proxies} working)")
        print()

    # Verify bucket is accessible before starting tests
    print("=== Checking bucket access ===")
    bucket_ok, bucket_msg = check_bucket_access()
    if bucket_ok:
        print(f"  {bucket_msg}")
    else:
        print(f"\nError: {bucket_msg}")
        print("Cannot start tests without bucket access. Check:")
        print("  - S3 proxy is running and healthy")
        print("  - Credentials are correct (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
        print("  - Bucket exists and is accessible")
        print(f"  - Current bucket: {S3_BUCKET}")
        return 1
    print()

    # Determine which tests to start
    test_name = args.test_name if hasattr(args, "test_name") else None
    if not test_name:
        print("Error: No test specified")
        print("Usage: python -m s3loadtest start [all|baseload|checkpoint|heavyread|delete|elephant|listops|spiky] [--duration DURATION]")
        return 1

    if test_name == "all":
        tests_to_start = list(TEST_CLASSES.keys())
    else:
        if test_name not in TEST_CLASSES:
            print(f"Error: Unknown test '{test_name}'")
            print(f"Available tests: {', '.join(TEST_CLASSES.keys())}, all")
            return 1
        tests_to_start = [test_name]

    # Parse duration if provided
    duration_seconds = None
    if hasattr(args, "duration") and args.duration:
        try:
            duration_seconds = parse_duration(args.duration)
            print(f"Starting {', '.join(tests_to_start)} on {len(workers)} workers for {format_duration(duration_seconds)}")
        except ValueError as e:
            print(f"Error: {e}")
            return 1
    else:
        # Default duration: 5 minutes
        duration_seconds = parse_duration("5m")
        print(f"Starting {', '.join(tests_to_start)} on {len(workers)} workers for {format_duration(duration_seconds)} (default)")

    # Get logging options
    log_level = getattr(args, "log_level", None)
    stats_interval = getattr(args, "stats_interval", 30)

    print()

    for test in tests_to_start:
        print(f"Launching {test}...")

        # Start test on all workers in parallel (fire-and-forget)
        started_count = 0
        already_running_count = 0
        error_count = 0

        with ThreadPoolExecutor(max_workers=65) as executor:
            futures = [
                executor.submit(
                    start_worker_process, worker, test, i, duration_seconds,
                    log_level, stats_interval
                )
                for i, worker in enumerate(workers)
            ]
            # Wait for all launches to complete and track results
            for future in as_completed(futures):
                hostname, status = future.result()
                if status == "started":
                    started_count += 1
                elif status == "already_running":
                    already_running_count += 1
                elif status == "error":
                    error_count += 1

        # Report results
        print(f"  Started: {started_count}, Already running: {already_running_count}, Errors: {error_count}")

    # Give newly started processes a moment to initialize
    print("\nWaiting for processes to initialize...")
    time.sleep(3)
    print()

    # Show status
    from s3loadtest.cli.status import cmd_status
    cmd_status(args)

    return 0
