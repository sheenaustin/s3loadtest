"""Run command — Execute a load test locally.

This is the command invoked on each worker machine, either directly
or via SSH from the coordinator. It handles signal-based shutdown,
periodic stats reporting, and final summary output.
"""

from __future__ import annotations

import signal
import time
from threading import Event, Thread

from s3loadtest.logging_setup import get_logger, setup_logging
from s3loadtest.tests import TEST_CLASSES
from s3loadtest.utils import (
    format_bytes,
    format_duration,
    parse_duration,
)


def cmd_run(args: object) -> int:
    """Run a test locally (called by SSH from coordinator or directly).

    Args:
        args: Parsed CLI arguments with ``test_name``,
            ``worker_id``, ``duration``, ``log_level``,
            and ``stats_interval`` attributes.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    log_level = getattr(args, "log_level", None)
    setup_logging(level=log_level)

    test_name = getattr(args, "test_name", None)
    worker_id = getattr(args, "worker_id", 0)
    logger = get_logger(test_name=test_name, worker_id=worker_id)

    test_class = TEST_CLASSES.get(test_name)
    if not test_class:
        logger.error(f"Unknown test: {test_name}")
        return 1

    stop_event = Event()

    def signal_handler(sig: int, frame: object) -> None:
        logger.info("Received stop signal, shutting down...")
        stop_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    duration_seconds: int | None = None
    raw_duration = getattr(args, "duration", None)
    if raw_duration:
        if raw_duration.isdigit():
            duration_seconds = int(raw_duration)
        else:
            duration_seconds = parse_duration(raw_duration)

    stats_interval = getattr(args, "stats_interval", 30)

    test = test_class(
        test_name, worker_id, stop_event, duration_seconds
    )

    stats_stop = Event()
    stats_thread: Thread | None = None

    if stats_interval and stats_interval > 0:

        def stats_reporter() -> None:
            while not stats_stop.wait(stats_interval):
                with test.stats_lock:
                    stats = test.stats.copy()
                elapsed = time.time() - test.start_time
                ops_sec = (
                    stats["ops"] / elapsed if elapsed > 0 else 0
                )
                bytes_sec = (
                    stats["bytes"] / elapsed if elapsed > 0 else 0
                )
                logger.info(
                    f"STATS: ops={stats['ops']:,} "
                    f"({ops_sec:.1f}/s), "
                    f"bytes={format_bytes(stats['bytes'])} "
                    f"({format_bytes(bytes_sec)}/s), "
                    f"errors={stats['errors']}, "
                    f"elapsed="
                    f"{format_duration(int(elapsed))}",
                    extra={"op_type": "STATS"},
                )

        stats_thread = Thread(target=stats_reporter, daemon=True)
        stats_thread.start()

    try:
        test.run()
    finally:
        if stats_thread:
            stats_stop.set()
            stats_thread.join(timeout=1)

        with test.stats_lock:
            stats = test.stats.copy()
        elapsed = time.time() - test.start_time
        ops_sec = stats["ops"] / elapsed if elapsed > 0 else 0
        logger.info(
            f"FINAL: ops={stats['ops']:,} ({ops_sec:.1f}/s), "
            f"bytes={format_bytes(stats['bytes'])}, "
            f"errors={stats['errors']}, "
            f"worker_failures={stats['worker_failures']}, "
            f"elapsed={format_duration(int(elapsed))}",
            extra={"op_type": "FINAL"},
        )

    return 0
