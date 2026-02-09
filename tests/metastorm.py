from __future__ import annotations

"""Metastorm Test: Pure metadata bombardment.

Targets the metadata server specifically with rapid small-object
churn: tiny PUTs, HEADs, DELETEs, and LISTs with random prefixes.
Zero data overhead — all objects are 1-byte markers.
"""

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from s3loadtest.s3_client import S3Client
from s3loadtest.tests.base import LoadTest
from s3loadtest.utils import format_duration, generate_random_suffix


class MetastormTest(LoadTest):
    """Metastorm: Metadata server destruction.

    30% tiny PUT, 25% HEAD, 25% LIST (random prefixes), 20% DELETE.
    Zero rate limiting, 1-byte objects, maximum concurrency.
    """

    def run(self) -> None:
        self.log(
            f"Starting Metastorm (metadata bombardment) "
            f"with {self.concurrency} threads"
        )
        if self.duration_seconds:
            self.log(f"Running for {format_duration(self.duration_seconds)}")

        thread_results: list[tuple[dict[str, int], int, list[tuple[str, float]]]] = []
        results_lock = Lock()

        def worker_loop() -> None:
            client = S3Client()
            _random = random.random
            _time = time.monotonic
            _stop = self.stop_event

            local_ops: dict[str, int] = {
                "PUT": 0, "HEAD": 0, "LIST": 0, "DELETE": 0,
            }
            local_errors = 0
            local_latencies: list[tuple[str, float]] = []

            # Track keys we've written so we can HEAD/DELETE them
            written_keys: list[str] = []
            max_tracked = 5000

            while not _stop.is_set() and self.should_continue():
                roll = _random()
                t0 = _time()

                try:
                    if roll < 0.30:
                        # PUT tiny object (1 byte)
                        suffix = generate_random_suffix(16)
                        prefix_char = random.choice("abcdefghijklmnop")
                        key = (
                            f"loadtest/{self.worker_id}/1B"
                            f"/metastorm/{prefix_char}/{suffix}"
                        )
                        client.put_object(key, b"\x00")
                        local_ops["PUT"] += 1
                        dt = (_time() - t0) * 1000
                        local_latencies.append(("PUT", dt))

                        if len(written_keys) < max_tracked:
                            written_keys.append(key)

                    elif roll < 0.55 and written_keys:
                        # HEAD existing object
                        key = random.choice(written_keys)
                        client.head_object(key)
                        local_ops["HEAD"] += 1
                        dt = (_time() - t0) * 1000
                        local_latencies.append(("HEAD", dt))

                    elif roll < 0.80:
                        # LIST with random prefix
                        prefix_char = random.choice("abcdefghijklmnop")
                        prefix = (
                            f"loadtest/{self.worker_id}/1B"
                            f"/metastorm/{prefix_char}/"
                        )
                        client.list_objects(
                            prefix=prefix, max_keys=1000,
                        )
                        local_ops["LIST"] += 1
                        dt = (_time() - t0) * 1000
                        local_latencies.append(("LIST", dt))

                    elif written_keys:
                        # DELETE
                        key = written_keys.pop(
                            random.randrange(len(written_keys))
                        )
                        client.delete_object(key)
                        local_ops["DELETE"] += 1
                        dt = (_time() - t0) * 1000
                        local_latencies.append(("DELETE", dt))

                except Exception:
                    local_errors += 1

            if hasattr(client, "close"):
                client.close()

            with results_lock:
                thread_results.append((
                    local_ops, local_errors, local_latencies,
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
        agg_ops: dict[str, int] = {
            "PUT": 0, "HEAD": 0, "LIST": 0, "DELETE": 0,
        }
        total_errors = 0
        for ops_map, errs, lats in thread_results:
            for op, count in ops_map.items():
                agg_ops[op] += count
            total_errors += errs
            for op_type, dt in lats:
                self.record_latency(op_type, dt)

        total_ops = sum(agg_ops.values())
        self.update_stats(ops=total_ops, errors=total_errors)

        elapsed = time.time() - self.start_time
        if elapsed > 0:
            ops_s = total_ops / elapsed
            self.log(
                f"Metastorm complete: {total_ops:,} metadata ops "
                f"({ops_s:,.0f} ops/s), {total_errors} errors "
                f"in {elapsed:.0f}s"
            )
            for op in ("PUT", "HEAD", "LIST", "DELETE"):
                c = agg_ops[op]
                self.log(f"  {op:>6s}: {c:>10,} ({c/elapsed:>8,.0f}/s)")
