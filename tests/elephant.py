from __future__ import annotations

"""Elephant Flow Test: Simulates slow WAN clients with high latency + low bandwidth.

Zero-memory bandwidth simulation:
- High latency: 100-500ms round-trip time per operation
- Low bandwidth: 1 Mbps simulated via sleep (no actual large data in memory)

Instead of holding 100MB in memory during 13-minute throttle sleeps (which caused OOM),
we simulate the transfer time by sleeping, then send/receive minimal actual data.
This tests connection holding and slow-client behavior without memory overhead.

Writes to: loadtest/{worker_id}/elephant/
"""

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from s3loadtest.tests.base import LoadTest
from s3loadtest.s3_ops import s3_get, s3_put, is_not_found_error
from s3loadtest.s3_client import S3Client
from s3loadtest.keyfiles import append_object_key, get_random_object_key
from s3loadtest.utils import random_object_size, generate_random_suffix, format_duration
from s3loadtest.config import ALL_SIZES


class ElephantFlowTest(LoadTest):
    """Elephant Flow: Simulates slow WAN clients with high latency + low bandwidth."""

    # 1 Mbps = 125,000 bytes/second (simulated, not actual data transfer)
    BANDWIDTH_BPS = 125_000

    def _do_operation(self, client: S3Client) -> None:
        """Execute a simulated slow transfer operation."""
        try:
            # Pick a simulated size (we won't actually allocate this much data)
            size, size_name = random_object_size(ALL_SIZES)

            # Simulate bandwidth-limited transfer time (sleep instead of holding data)
            transfer_time = size / self.BANDWIDTH_BPS
            time.sleep(transfer_time)

            if random.random() < 0.5:
                # Write - send 1-byte placeholder, but we simulated time for full size
                random_suffix = generate_random_suffix()
                key = f"loadtest/{self.worker_id}/elephant/{int(time.time()*1000000)}/{random_suffix}"
                s3_put(client, key, b"E", self.logger)  # 1-byte elephant marker
                append_object_key(self.name, self.worker_id, key)
                self.update_stats(ops=1, bytes=size)  # Report simulated size

                # Manage object list - trim file if too many keys
                self.manage_object_list_file(client)
            else:
                # Read - fetch and immediately discard
                key = get_random_object_key(self.name, self.worker_id)
                if key:
                    try:
                        _ = s3_get(client, key, self.logger)  # Fetch and discard
                        self.update_stats(ops=1, bytes=size)  # Report simulated size
                    except Exception as e:
                        if not is_not_found_error(e):
                            raise

        except Exception as e:
            self.update_stats(errors=1)
            self.log(f"Error: {e}")

    def run(self) -> None:
        # Simulate slow WAN client with 100-500ms latency
        delay = random.uniform(0.1, 0.5)
        self.log(f"Starting Elephant Flow Test (simulated 1Mbps, delay={delay*1000:.0f}ms) with {self.concurrency} concurrent threads")
        if self.duration_seconds:
            self.log(f"Running for {format_duration(self.duration_seconds)}")

        # Each thread gets its own S3 client with latency delay
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            def worker_loop() -> None:
                client = S3Client(delay=delay)
                while self.should_continue():
                    self._do_operation(client)

            # Submit all worker threads
            futures = [executor.submit(worker_loop) for _ in range(self.concurrency)]

            # Wait for all to complete, tracking failures
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.update_stats(worker_failures=1)
                    self.log(f"Worker thread error: {e}")

        # Report final stats
        if self.stats["worker_failures"] > 0:
            self.log(f"WARNING: {self.stats['worker_failures']}/{self.concurrency} worker threads failed")
