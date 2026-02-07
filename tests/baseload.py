from __future__ import annotations

"""Base Load Test: 70% read, 30% write, continuous.

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
from s3loadtest.config import ALL_SIZES


class BaseLoadTest(LoadTest):
    """Base Load: 70% read, 30% write, 24x7, 75% load.

    Writes to: loadtest/{worker_id}/other/
    """

    def _do_operation(self, client: S3Client) -> None:
        """Execute a single operation (write or read)."""
        try:
            if random.random() < 0.3:  # 30% write
                size, size_name = random_object_size(ALL_SIZES)
                random_suffix = generate_random_suffix()
                key = f"loadtest/{self.worker_id}/other/{int(time.time()*1000000)}/{random_suffix}"

                data = read_test_data(size)
                s3_put(client, key, data, self.logger)

                append_object_key(self.name, self.worker_id, key)
                self.update_stats(ops=1, bytes=size)
                self.manage_object_list_file(client)

            else:  # 70% read
                key = get_random_object_key(self.name, self.worker_id)
                if key:
                    try:
                        data = s3_get(client, key, self.logger)
                        self.update_stats(ops=1, bytes=len(data))
                    except Exception as e:
                        if not is_not_found_error(e):
                            raise

            time.sleep(0.25)  # Rate limiting

        except Exception as e:
            self.update_stats(errors=1)
            self.log(f"Error: {e}")

    def run(self) -> None:
        self.log(f"Starting Base Load Test (70% read, 30% write) with {self.concurrency} threads")
        if self.duration_seconds:
            self.log(f"Running for {format_duration(self.duration_seconds)}")

        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            def worker_loop() -> None:
                client = S3Client()
                while self.should_continue():
                    self._do_operation(client)

            futures = [executor.submit(worker_loop) for _ in range(self.concurrency)]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.update_stats(worker_failures=1)
                    self.log(f"Worker thread error: {e}")

        if self.stats["worker_failures"] > 0:
            self.log(f"WARNING: {self.stats['worker_failures']}/{self.concurrency} worker threads failed")
