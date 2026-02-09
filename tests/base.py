from __future__ import annotations

"""Base class for load tests.

All load test classes inherit from LoadTest which provides:
- Stats tracking (thread-safe)
- Logging with test context
- Duration management
- Object count management
"""

import time
from collections import defaultdict
from threading import Event, Lock

from s3loadtest.config import MAX_OBJECTS_PER_WORKER
from s3loadtest.logging_setup import get_logger
from s3loadtest.utils import calculate_threads_for_test, format_duration
from s3loadtest.keyfiles import count_object_keys, trim_object_keys
from s3loadtest.s3_ops import s3_get, s3_put, s3_delete, s3_list


class LoadTest:
    """Base class for load tests."""

    def __init__(
        self,
        name: str,
        worker_id: int,
        stop_event: Event,
        duration_seconds: float | None = None,
        concurrency: int | None = None,
        size: str | None = None,
    ) -> None:
        """Initialize load test.

        Args:
            name: Test name (e.g., 'baseload', 'checkpoint')
            worker_id: Worker ID number
            stop_event: Threading Event to signal stop
            duration_seconds: Optional test duration
            concurrency: Number of worker threads (default: auto-calculated)
            size: Optional size filter (e.g. '100MB'). Used by heavyread.
        """
        self.name = name
        self.worker_id = worker_id
        self.stop_event = stop_event
        self.duration_seconds = duration_seconds
        self.size = size
        self.stats = {"ops": 0, "errors": 0, "bytes": 0, "deletes": 0, "worker_failures": 0}
        self.stats_lock = Lock()
        self.max_objects_per_worker = MAX_OBJECTS_PER_WORKER
        self.start_time = time.time()

        # Latency tracking — keyed by op type (GET, PUT, DELETE, LIST, HEAD)
        self._latencies: dict[str, list[float]] = defaultdict(list)
        self._latency_lock = Lock()

        self.logger = get_logger(test_name=name, worker_id=worker_id)

        if concurrency is None:
            self.concurrency = calculate_threads_for_test(self.name)
        else:
            self.concurrency = max(1, concurrency)

    def log(self, msg: str, level: str = "info", **extra: object) -> None:
        """Log message with test context."""
        log_func = getattr(self.logger, level.lower(), self.logger.info)
        log_func(msg, extra=extra)

    def update_stats(self, **kwargs: int) -> None:
        """Thread-safe stats update."""
        with self.stats_lock:
            for key, value in kwargs.items():
                self.stats[key] = self.stats.get(key, 0) + value

    def record_latency(self, op_type: str, duration_ms: float) -> None:
        """Record a latency measurement (thread-safe)."""
        with self._latency_lock:
            self._latencies[op_type].append(duration_ms)

    def get_latency_percentiles(self) -> dict[str, dict[str, float]]:
        """Get p50/p95/p99/max latency per op type.

        Returns:
            Dict like ``{"GET": {"p50": 2.1, "p95": 15.3, ...}, ...}``.
        """
        with self._latency_lock:
            snapshot = {
                op: sorted(vals)
                for op, vals in self._latencies.items()
                if vals
            }
        result: dict[str, dict[str, float]] = {}
        for op, vals in snapshot.items():
            n = len(vals)
            result[op] = {
                "p50": vals[int(n * 0.50)] if n else 0,
                "p95": vals[int(min(n * 0.95, n - 1))] if n else 0,
                "p99": vals[int(min(n * 0.99, n - 1))] if n else 0,
                "max": vals[-1] if n else 0,
                "count": n,
            }
        return result

    def drain_latencies(self) -> dict[str, dict[str, float]]:
        """Get percentiles and reset. Prevents unbounded memory growth."""
        result = self.get_latency_percentiles()
        with self._latency_lock:
            self._latencies.clear()
        return result

    def should_continue(self) -> bool:
        """Check if test should continue running."""
        if self.stop_event.is_set():
            return False
        if self.duration_seconds:
            elapsed = time.time() - self.start_time
            if elapsed >= self.duration_seconds:
                self.log(f"Duration {format_duration(self.duration_seconds)} reached, stopping")
                return False
        return True

    def manage_object_list_file(self, client: object) -> None:
        """Delete oldest objects if we have too many (file-based tracking)."""
        count = count_object_keys(self.name, self.worker_id)
        if count > self.max_objects_per_worker:
            trim_object_keys(self.name, self.worker_id, self.max_objects_per_worker)

    # Convenience wrappers for s3_ops (use these or import s3_ops directly)
    def do_get(self, client: object, key: str) -> bytes:
        """GET object with automatic retry."""
        return s3_get(client, key, self.logger)

    def do_put(self, client: object, key: str, data: bytes) -> None:
        """PUT object with automatic retry."""
        return s3_put(client, key, data, self.logger)

    def do_delete(self, client: object, key: str) -> None:
        """DELETE object with automatic retry."""
        return s3_delete(client, key, self.logger)

    def do_list(
        self,
        client: object,
        prefix: str = "",
        max_keys: int = 1000,
        continuation_token: str | None = None,
    ) -> dict:
        """LIST objects with automatic retry."""
        return s3_list(client, prefix, max_keys, continuation_token, self.logger)

    def run(self) -> None:
        """Override this method to implement the test."""
        raise NotImplementedError("Subclasses must implement run()")
