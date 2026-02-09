from __future__ import annotations

"""Firehose Test: Maximum throughput, zero mercy.

All sizes, all operations, zero sleep, maximum threads.
Pre-loads keys from every other test source for reads.
Thread-local stats with zero lock contention during burst.
"""

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from s3loadtest.config import ALL_SIZES, S3_BUCKET
from s3loadtest.keyfiles import append_object_key, get_keys_by_size
from s3loadtest.s3_client import S3Client
from s3loadtest.tests.base import LoadTest
from s3loadtest.utils import (
    format_duration,
    generate_random_suffix,
    random_object_size,
    read_test_data,
)

_KEY_SOURCES = ["baseload", "checkpoint", "elephant", "spiky", "firehose"]


class FirehoseTest(LoadTest):
    """Firehose: Maximum throughput mixed workload.

    50% read / 50% write, zero rate limiting, all object sizes,
    maximum thread count. Designed to saturate every resource the
    object store has.
    """

    def _load_read_keys(self) -> list[str]:
        """Gather keys from all test sources for reading."""
        keys_by_size = get_keys_by_size(self.worker_id, _KEY_SOURCES)
        all_keys: list[str] = []
        for keys in keys_by_size.values():
            all_keys.extend(keys)
        return all_keys

    def run(self) -> None:
        self.log(
            f"Starting Firehose (50/50 r/w, zero sleep) "
            f"with {self.concurrency} threads"
        )
        if self.duration_seconds:
            self.log(f"Running for {format_duration(self.duration_seconds)}")

        bucket = S3_BUCKET

        # Pre-load keys for reads (refreshed each cycle)
        read_keys = self._load_read_keys()
        has_read_keys = bool(read_keys)
        if has_read_keys:
            self.log(f"Loaded {len(read_keys)} keys for reading")
        else:
            self.log("No existing keys — starting write-only until keys accumulate")

        # Pre-build paths for proxy drain path
        read_paths = [f"/{bucket}/{k}" for k in read_keys] if has_read_keys else []

        thread_results: list[tuple[int, int, int, int, list[tuple[str, float]]]] = []
        results_lock = Lock()

        def worker_loop() -> None:
            local_ops = 0
            local_bytes = 0
            local_errors = 0
            local_writes = 0
            local_latencies: list[tuple[str, float]] = []

            client = S3Client()
            has_drain = hasattr(client, "get_object_drain")

            _choice = random.choice
            _random = random.random
            _time = time.monotonic
            _stop = self.stop_event
            _all_sizes = ALL_SIZES

            # Local copies for speed
            _read_keys = read_keys
            _read_paths = read_paths
            _has_reads = has_read_keys

            while not _stop.is_set() and self.should_continue():
                t0 = _time()
                try:
                    if _has_reads and _random() < 0.5:
                        # READ
                        if has_drain:
                            path = _choice(_read_paths)
                            nbytes = client.get_object_drain(path)
                        else:
                            key = _choice(_read_keys)
                            data = client.get_object(key)
                            nbytes = len(data)
                        local_ops += 1
                        local_bytes += nbytes
                        dt = (_time() - t0) * 1000
                        local_latencies.append(("GET", dt))
                    else:
                        # WRITE
                        size, size_name = random_object_size(_all_sizes)
                        suffix = generate_random_suffix()
                        key = (
                            f"loadtest/{self.worker_id}/{size_name}"
                            f"/firehose/{int(time.time()*1000000)}"
                            f"/{suffix}"
                        )
                        data = read_test_data(size)
                        client.put_object(key, data)
                        append_object_key(
                            "firehose", self.worker_id, key,
                            size_label=size_name,
                        )
                        local_ops += 1
                        local_bytes += size
                        local_writes += 1
                        dt = (_time() - t0) * 1000
                        local_latencies.append(("PUT", dt))

                except Exception:
                    local_errors += 1

            if hasattr(client, "close"):
                client.close()

            with results_lock:
                thread_results.append((
                    local_ops, local_bytes, local_errors,
                    local_writes, local_latencies,
                ))

        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = [
                executor.submit(worker_loop)
                for _ in range(self.concurrency)
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.update_stats(worker_failures=1)
                    self.log(f"Worker thread error: {e}")

        # Aggregate
        total_ops = 0
        total_bytes = 0
        total_errors = 0
        total_writes = 0
        for ops, nbytes, errs, writes, lats in thread_results:
            total_ops += ops
            total_bytes += nbytes
            total_errors += errs
            total_writes += writes
            for op_type, dt in lats:
                self.record_latency(op_type, dt)

        self.update_stats(
            ops=total_ops, bytes=total_bytes, errors=total_errors,
        )

        elapsed = time.time() - self.start_time
        if elapsed > 0:
            ops_s = total_ops / elapsed
            mb_s = (total_bytes / (1024 * 1024)) / elapsed
            self.log(
                f"Firehose complete: {total_ops:,} ops "
                f"({ops_s:,.0f} ops/s, {mb_s:,.1f} MB/s), "
                f"{total_writes:,} writes, "
                f"{total_errors} errors in {elapsed:.0f}s"
            )
