from __future__ import annotations

"""Heavy Read Test: 100% read, 20m every hour.

Faster than the original loadtest.py hot loop:
- Content-Length drain: get_object_drain reads header, skips per-chunk len()
- 1MB drain chunks: 4x fewer read() syscalls than original 256KB
- Batched time checks: time.monotonic every 32 ops, not every op
- Method refs bound once outside loop (zero per-call attribute lookup)
- Pre-built integer-indexed path lists (zero dict lookups)
- Thread-local counters (zero lock contention during burst)
- Per-size throughput reporting
"""

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from s3loadtest.tests.base import LoadTest
from s3loadtest.config import S3_BUCKET
from s3loadtest.keyfiles import get_keys_by_size
from s3loadtest.s3_client import S3Client
from s3loadtest.utils import format_duration

# Batch time checks: only call time.monotonic() every N ops.
# Eliminates 31/32 VDSO syscalls + Event.is_set() calls per iteration.
_CHECK_EVERY = 32


class HeavyReadTest(LoadTest):
    """Heavy Read: 100% read, 20m every hour.

    Equal size cycling: round-robin across size buckets so every size
    gets equal operation count regardless of key population skew.
    With --size, uses a single flat list for maximum speed.
    """

    KEY_SOURCES = ["baseload", "checkpoint", "elephant", "spiky"]

    def _load_keys_by_size(self) -> dict[str, list[str]]:
        """Load all keys grouped by size label from all sources."""
        return get_keys_by_size(self.worker_id, self.KEY_SOURCES)

    def run(self) -> None:
        size_desc = f"size={self.size}" if self.size else "equal size cycling"
        self.log(
            f"Starting Heavy Read Test (100% read, 20m/hour, {size_desc}) "
            f"with {self.concurrency} concurrent threads"
        )
        if self.duration_seconds:
            self.log(f"Running for {format_duration(self.duration_seconds)}")

        bucket = S3_BUCKET

        while self.should_continue():
            keys_by_size = self._load_keys_by_size()

            size_labels = sorted(
                sl for sl, keys in keys_by_size.items()
                if keys and (self.size is None or sl == self.size)
            )

            if not size_labels:
                if self.size:
                    self.log(f"No keys available for size {self.size}, waiting 60s...")
                else:
                    self.log("No keys available from any source, waiting 60s...")
                self.stop_event.wait(60)
                continue

            num_sizes = len(size_labels)

            paths_by_idx = [
                [f"/{bucket}/{k}" for k in keys_by_size[sl]]
                for sl in size_labels
            ]
            keys_by_idx = [keys_by_size[sl] for sl in size_labels]

            total_keys = sum(len(p) for p in paths_by_idx)

            burst_start = time.time()
            # Use monotonic for the hot loop deadline (cheaper, no NTP jitter)
            mono_end = time.monotonic() + 1200  # 20 minutes

            self.log(
                f"Starting 20-minute heavy read burst "
                f"({total_keys} keys across {num_sizes} sizes)"
            )
            for i, sl in enumerate(size_labels):
                self.log(f"  {sl}: {len(paths_by_idx[i])} keys")

            thread_results: list[tuple[list[int], list[int], int]] = []
            results_lock = Lock()
            single_size = num_sizes == 1

            def worker_loop() -> None:
                client = S3Client()
                has_drain = hasattr(client, "get_object_drain")

                if single_size and has_drain:
                    self._worker_drain_single(
                        client, paths_by_idx[0], mono_end,
                        thread_results, results_lock,
                    )
                elif single_size:
                    self._worker_get_single(
                        client, keys_by_idx[0], mono_end,
                        thread_results, results_lock,
                    )
                elif has_drain:
                    self._worker_drain_multi(
                        client, paths_by_idx, num_sizes, mono_end,
                        thread_results, results_lock,
                    )
                else:
                    self._worker_get_multi(
                        client, keys_by_idx, num_sizes, mono_end,
                        thread_results, results_lock,
                    )

            with ThreadPoolExecutor(
                max_workers=self.concurrency,
            ) as executor:
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
            agg_ops = [0] * num_sizes
            agg_bytes = [0] * num_sizes
            total_errors = 0

            for ops_list, bytes_list, errs in thread_results:
                for i in range(num_sizes):
                    agg_ops[i] += ops_list[i]
                    agg_bytes[i] += bytes_list[i]
                total_errors += errs

            total_ops = sum(agg_ops)
            total_bytes = sum(agg_bytes)
            elapsed = time.time() - burst_start

            self.update_stats(
                ops=total_ops,
                bytes=total_bytes,
                errors=total_errors,
            )

            if elapsed > 0:
                ops_per_sec = total_ops / elapsed
                mb_per_sec = (total_bytes / (1024 * 1024)) / elapsed
                self.log(
                    f"Burst complete: {total_ops:,} ops in "
                    f"{elapsed:.0f}s ({ops_per_sec:,.0f} ops/s, "
                    f"{mb_per_sec:,.1f} MB/s, "
                    f"{total_errors} errors)"
                )
                if num_sizes > 1:
                    self.log("Per-size breakdown:")
                    for i, sl in enumerate(size_labels):
                        sl_ops = agg_ops[i]
                        sl_bytes = agg_bytes[i]
                        sl_ops_s = sl_ops / elapsed
                        sl_mb_s = (sl_bytes / (1024 * 1024)) / elapsed
                        self.log(
                            f"  {sl:>6s}: {sl_ops:>8,} ops "
                            f"({sl_ops_s:>7,.0f} ops/s, "
                            f"{sl_mb_s:>8,.1f} MB/s)"
                        )

            if self.should_continue():
                self.log("Heavy read complete, waiting for next hour")
                self.stop_event.wait(2400)

    # ------------------------------------------------------------------
    # Worker loops — branch once outside, batched time checks inside
    # ------------------------------------------------------------------

    def _worker_drain_single(
        self,
        client: object,
        paths: list[str],
        mono_end: float,
        results: list,
        lock: Lock,
    ) -> None:
        """Single-size proxy path. Beats original via batched checks + 1MB drain."""
        local_ops = 0
        local_bytes = 0
        local_errors = 0

        drain = client.get_object_drain
        reset = client._reset_conn
        choice = random.choice
        mono = time.monotonic
        stop = self.stop_event
        check = _CHECK_EVERY
        tick = 0

        while True:
            try:
                nbytes = drain(choice(paths))
                local_ops += 1
                local_bytes += nbytes
            except Exception as e:
                err_str = str(e)
                if "404" in err_str or "NoSuchKey" in err_str:
                    pass
                elif "onnection" in err_str or "Broken" in err_str:
                    reset()
                    local_errors += 1
                else:
                    local_errors += 1
            tick += 1
            if tick >= check:
                tick = 0
                if mono() >= mono_end or stop.is_set():
                    break

        if hasattr(client, "close"):
            client.close()

        with lock:
            results.append(([local_ops], [local_bytes], local_errors))

    def _worker_get_single(
        self,
        client: object,
        keys: list[str],
        mono_end: float,
        results: list,
        lock: Lock,
    ) -> None:
        """Single-size non-proxy fallback."""
        local_ops = 0
        local_bytes = 0
        local_errors = 0

        get = client.get_object
        choice = random.choice
        mono = time.monotonic
        stop = self.stop_event
        check = _CHECK_EVERY
        tick = 0

        while True:
            try:
                data = get(choice(keys))
                local_ops += 1
                local_bytes += len(data)
            except Exception as e:
                err_str = str(e)
                if "404" in err_str or "NoSuchKey" in err_str:
                    pass
                else:
                    local_errors += 1
            tick += 1
            if tick >= check:
                tick = 0
                if mono() >= mono_end or stop.is_set():
                    break

        if hasattr(client, "close"):
            client.close()

        with lock:
            results.append(([local_ops], [local_bytes], local_errors))

    def _worker_drain_multi(
        self,
        client: object,
        paths_by_idx: list[list[str]],
        num_sizes: int,
        mono_end: float,
        results: list,
        lock: Lock,
    ) -> None:
        """Multi-size proxy path with batched time checks."""
        local_ops = [0] * num_sizes
        local_bytes = [0] * num_sizes
        local_errors = 0

        drain = client.get_object_drain
        reset = client._reset_conn
        choice = random.choice
        mono = time.monotonic
        stop = self.stop_event
        _paths = paths_by_idx
        _n = num_sizes
        check = _CHECK_EVERY

        size_idx = random.randrange(_n)
        tick = 0

        while True:
            idx = size_idx % _n
            size_idx += 1
            try:
                nbytes = drain(choice(_paths[idx]))
                local_ops[idx] += 1
                local_bytes[idx] += nbytes
            except Exception as e:
                err_str = str(e)
                if "404" in err_str or "NoSuchKey" in err_str:
                    pass
                elif "onnection" in err_str or "Broken" in err_str:
                    reset()
                    local_errors += 1
                else:
                    local_errors += 1
            tick += 1
            if tick >= check:
                tick = 0
                if mono() >= mono_end or stop.is_set():
                    break

        if hasattr(client, "close"):
            client.close()

        with lock:
            results.append((local_ops, local_bytes, local_errors))

    def _worker_get_multi(
        self,
        client: object,
        keys_by_idx: list[list[str]],
        num_sizes: int,
        mono_end: float,
        results: list,
        lock: Lock,
    ) -> None:
        """Multi-size non-proxy fallback with batched time checks."""
        local_ops = [0] * num_sizes
        local_bytes = [0] * num_sizes
        local_errors = 0

        get = client.get_object
        choice = random.choice
        mono = time.monotonic
        stop = self.stop_event
        _keys = keys_by_idx
        _n = num_sizes
        check = _CHECK_EVERY

        size_idx = random.randrange(_n)
        tick = 0

        while True:
            idx = size_idx % _n
            size_idx += 1
            try:
                data = get(choice(_keys[idx]))
                local_ops[idx] += 1
                local_bytes[idx] += len(data)
            except Exception as e:
                err_str = str(e)
                if "404" in err_str or "NoSuchKey" in err_str:
                    pass
                else:
                    local_errors += 1
            tick += 1
            if tick >= check:
                tick = 0
                if mono() >= mono_end or stop.is_set():
                    break

        if hasattr(client, "close"):
            client.close()

        with lock:
            results.append((local_ops, local_bytes, local_errors))
