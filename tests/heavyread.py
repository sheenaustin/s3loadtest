from __future__ import annotations

"""Heavy Read Test: 100% read, 20m every hour.

Reads from: All available key sources for this worker (baseload, checkpoint, elephant, spiky).
"""

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from s3loadtest.tests.base import LoadTest
from s3loadtest.s3_ops import s3_get, is_not_found_error
from s3loadtest.s3_client import S3Client
from s3loadtest.keyfiles import get_random_object_key, remove_object_key
from s3loadtest.utils import format_duration


class HeavyReadTest(LoadTest):
    """Heavy Read: 100% read, 20m every hour."""

    # Tests that write objects we can read from
    KEY_SOURCES = ["baseload", "checkpoint", "elephant", "spiky"]

    def _do_operation(self, client: S3Client, end_time: float) -> bool:
        """Execute a single read operation during burst."""
        if time.time() >= end_time:
            return False

        try:
            # Try to get a key from any available source (randomize order for distribution)
            key = None
            sources = self.KEY_SOURCES.copy()
            random.shuffle(sources)

            for source in sources:
                key = get_random_object_key(source, self.worker_id)
                if key:
                    break

            if key:
                try:
                    data = s3_get(client, key, self.logger)
                    self.update_stats(ops=1, bytes=len(data))
                except Exception as e:
                    if not is_not_found_error(e):
                        raise
                    # Object was deleted - remove from tracking to prevent future 404s
                    for source in sources:
                        remove_object_key(source, self.worker_id, key)
            else:
                # No keys available - log once per minute max
                pass
        except Exception as e:
            self.update_stats(errors=1)
            self.log(f"Error: {e}")

        return True

    def run(self) -> None:
        self.log(f"Starting Heavy Read Test (100% read, 20m/hour) with {self.concurrency} concurrent threads")
        if self.duration_seconds:
            self.log(f"Running for {format_duration(self.duration_seconds)}")

        while self.should_continue():
            # Run for 20 minutes
            end_time = time.time() + 1200  # 20 minutes
            self.log("Starting 20-minute heavy read burst")

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

            # Wait until next hour
            if self.should_continue():
                self.log("Heavy read complete, waiting for next hour")
                self.stop_event.wait(2400)  # 40 minutes
