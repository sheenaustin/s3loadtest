from __future__ import annotations

"""List Operations Test: Continuously paginate through all objects."""

from concurrent.futures import ThreadPoolExecutor, as_completed

from s3loadtest.tests.base import LoadTest
from s3loadtest.s3_client import S3Client
from s3loadtest.utils import format_duration


class ListOpsTest(LoadTest):
    """List Operations: Continuously paginate through all objects."""

    def _do_paginated_list(self, client: S3Client, prefix: str = "") -> None:
        """Execute paginated list operations through all pages."""
        continuation_token = None
        pages = 0
        total_keys = 0

        while self.should_continue():
            try:
                # List with continuation token if we have one
                result = client.list_objects(
                    prefix=prefix,
                    max_keys=1000,
                    continuation_token=continuation_token
                )
                pages += 1
                key_count = result.get("KeyCount", len(result.get("Contents", [])))
                total_keys += key_count
                self.update_stats(ops=1)

                # Check if there are more pages
                if not result.get("IsTruncated"):
                    # Finished listing all objects, start over
                    continuation_token = None
                    continue

                # Get next continuation token
                continuation_token = result.get("NextContinuationToken")
                if not continuation_token:
                    # No more pages, start over
                    continuation_token = None

            except Exception as e:
                self.update_stats(errors=1)
                self.log(f"List error: {e}")
                # Reset and try again
                continuation_token = None

    def run(self) -> None:
        self.log(f"Starting List Operations Test (paginated) with {self.concurrency} concurrent threads")
        if self.duration_seconds:
            self.log(f"Running for {format_duration(self.duration_seconds)}")

        # Each thread gets its own S3 client and continuously paginates
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            def worker_loop() -> None:
                client = S3Client()
                # Continuously paginate through all objects
                self._do_paginated_list(client, prefix="loadtest/")

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
