from __future__ import annotations

"""Checkpoint Load Test: 10% read, 90% write, 10m bursts with 1h gap.

Writes to: loadtest/{worker_id}/other/
"""

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from s3loadtest.tests.base import LoadTest
from s3loadtest.s3_ops import s3_get, s3_put, is_not_found_error
from s3loadtest.s3_client import S3Client
from s3loadtest.keyfiles import append_object_key, get_random_object_key
from s3loadtest.utils import random_object_size, generate_random_suffix, read_test_data, format_duration
from s3loadtest.config import CHECKPOINT_SIZES


class CheckpointLoadTest(LoadTest):
    """Checkpoint Load: 10% read, 90% write, 10m bursts with 1h gap."""

    def _do_operation(self, client: S3Client, end_time: float) -> bool:
        """Execute a single operation (write or read) during burst."""
        if time.time() >= end_time:
            return False

        try:
            if random.random() < 0.9:
                # Write large objects
                size, size_name = random_object_size(CHECKPOINT_SIZES)
                random_suffix = generate_random_suffix()
                key = f"loadtest/{self.worker_id}/{size_name}/{self.name}/{int(time.time()*1000000)}/{random_suffix}"
                data = read_test_data(size)
                s3_put(client, key, data, self.logger)
                append_object_key(self.name, self.worker_id, key, size_label=size_name)
                self.update_stats(ops=1, bytes=size)

                # Manage object list - trim file if too many keys
                self.manage_object_list_file(client)
            else:
                # Read
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

        return True

    def run(self) -> None:
        self.log(f"Starting Checkpoint Load Test (10% read, 90% write, 10m bursts) with {self.concurrency} concurrent threads")
        if self.duration_seconds:
            self.log(f"Running for {format_duration(self.duration_seconds)}")

        while self.should_continue():
            # Run for 10 minutes
            end_time = time.time() + 600  # 10 minutes
            self.log("Starting 10-minute checkpoint burst")

            # Each thread gets its own S3 client
            with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
                def worker_loop() -> None:
                    client = S3Client()
                    while time.time() < end_time and self.should_continue():
                        self._do_operation(client, end_time)

                # Submit all worker threads
                futures = [executor.submit(worker_loop) for _ in range(self.concurrency)]

                # Wait for all to complete, tracking failures
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        self.update_stats(worker_failures=1)
                        self.log(f"Worker thread error: {e}")

            # Wait 1 hour before next burst
            if self.should_continue():
                self.log("Checkpoint burst complete, waiting 1 hour")
                self.stop_event.wait(3600)
