from __future__ import annotations

"""Spiky Base Load Test: Like baseload but with random extreme 1m bursts.

Normal mode: 70% read, 30% write with rate limiting (same as baseload)
Spike mode: 1-minute bursts of extreme read-only or write-only load (no rate limiting)
Frequency: 1-3 random bursts per 5-minute window

Writes to: loadtest/{worker_id}/other/
"""

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event, Lock

from s3loadtest.tests.base import LoadTest
from s3loadtest.s3_ops import s3_get, s3_put, is_not_found_error
from s3loadtest.s3_client import S3Client
from s3loadtest.keyfiles import append_object_key, get_random_object_key
from s3loadtest.utils import random_object_size, generate_random_suffix, read_test_data, format_duration
from s3loadtest.config import ALL_SIZES


class SpikyBaseLoadTest(LoadTest):
    """Spiky Base Load: Like baseload but with random extreme 1m bursts."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        # Thread-safe spike state using Event (set = spiking)
        self._spike_event = Event()
        self._spike_write_heavy = Event()  # set = write-heavy, clear = read-heavy
        self._spike_lock = Lock()  # Protects state transitions

    def _do_operation(self, client: S3Client) -> None:
        """Execute a single operation, adjusting behavior during spikes."""
        try:
            # Read spike state (Events are thread-safe)
            is_spiking = self._spike_event.is_set()

            if is_spiking:
                # Spike mode: extreme ratio, no rate limiting
                if self._spike_write_heavy.is_set():
                    is_write = random.random() < 0.95
                else:
                    is_write = random.random() < 0.05
            else:
                # Normal mode: 30% write, 70% read
                is_write = random.random() < 0.3

            if is_write:
                size, size_name = random_object_size(ALL_SIZES)
                random_suffix = generate_random_suffix()
                key = f"loadtest/{self.worker_id}/{size_name}/{self.name}/{int(time.time()*1000000)}/{random_suffix}"
                data = read_test_data(size)
                s3_put(client, key, data, self.logger)
                append_object_key(self.name, self.worker_id, key, size_label=size_name)
                self.update_stats(ops=1, bytes=size)
                self.manage_object_list_file(client)
            else:
                key = get_random_object_key(self.name, self.worker_id)
                if key:
                    try:
                        data = s3_get(client, key, self.logger)
                        self.update_stats(ops=1, bytes=len(data))
                    except Exception as e:
                        if not is_not_found_error(e):
                            raise

        except Exception as e:
            self.update_stats(errors=1)
            self.log(f"Error: {e}")

    def _schedule_bursts(
        self,
        num_bursts: int,
        window_secs: int,
        burst_secs: int,
    ) -> list[float]:
        """Generate non-overlapping burst start offsets within a time window."""
        total_gap = window_secs - (num_bursts * burst_secs)
        # Distribute gap time randomly into (num_bursts + 1) slots
        weights = [random.random() for _ in range(num_bursts + 1)]
        weight_sum = sum(weights)
        gaps = [w / weight_sum * total_gap for w in weights]

        starts: list[float] = []
        offset = gaps[0]
        for i in range(num_bursts):
            starts.append(offset)
            offset += burst_secs + gaps[i + 1]
        return starts

    def run(self) -> None:
        self.log(f"Starting Spiky Base Load Test (baseload + random 1m bursts) with {self.concurrency} concurrent threads")
        if self.duration_seconds:
            self.log(f"Running for {format_duration(self.duration_seconds)}")

        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            def worker_loop() -> None:
                client = S3Client()
                while self.should_continue():
                    self._do_operation(client)

            futures = [executor.submit(worker_loop) for _ in range(self.concurrency)]

            # Orchestrate spikes in main thread while workers run
            while self.should_continue():
                window_secs = 300  # 5-minute windows
                window_start = time.time()
                num_bursts = random.randint(1, 3)
                burst_starts = self._schedule_bursts(num_bursts, window_secs, 60)

                self.log(f"Window: {num_bursts} burst(s) at offsets {[f'{s:.0f}s' for s in burst_starts]}")

                for burst_offset in burst_starts:
                    # Wait until burst start
                    wait_secs = (window_start + burst_offset) - time.time()
                    if wait_secs > 0:
                        self.stop_event.wait(wait_secs)
                    if not self.should_continue():
                        break

                    # Start spike (thread-safe state transition)
                    with self._spike_lock:
                        if random.random() < 0.5:
                            self._spike_write_heavy.set()
                            spike_type = "WRITE"
                        else:
                            self._spike_write_heavy.clear()
                            spike_type = "READ"
                        self._spike_event.set()
                    self.log(f"SPIKE {spike_type} burst starting (1m)")

                    # Burst for 1 minute
                    self.stop_event.wait(60)

                    # End spike (thread-safe)
                    with self._spike_lock:
                        self._spike_event.clear()
                    self.log(f"SPIKE {spike_type} burst ended")

                # Wait for remainder of window
                remaining = (window_start + window_secs) - time.time()
                if remaining > 0 and self.should_continue():
                    self.stop_event.wait(remaining)

            # Wait for worker threads to finish, tracking failures
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.update_stats(worker_failures=1)
                    self.log(f"Worker thread error: {e}")

        if self.stats["worker_failures"] > 0:
            self.log(f"WARNING: {self.stats['worker_failures']}/{self.concurrency} worker threads failed")
