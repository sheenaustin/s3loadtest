"""Delete Load Test — Streaming batch delete, 24x7, 25% load.

Deletes from: loadtest/{worker_id}/

Uses efficient streaming delete (list -> batch delete -> repeat).
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from s3loadtest.config import S3_BUCKET
from s3loadtest.keyfiles import KeyFileManager
from s3loadtest.s3_client import S3Client
from s3loadtest.tests.base import LoadTest
from s3loadtest.utils import format_duration

_key_manager = KeyFileManager()


class DeleteLoadTest(LoadTest):
    """Delete Load: Streaming batch delete, 24x7, 25% load."""

    KEY_SOURCES = ["baseload", "checkpoint", "elephant", "spiky"]

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.pool = S3Client()
        self.batch_size = 100
        self.delete_workers = 1

    def list_objects_batch(
        self,
        prefix: str,
        *,
        max_keys: int = 1000,
    ) -> object:
        """List objects in batches with pagination.

        Args:
            prefix: S3 key prefix to list.
            max_keys: Maximum keys per page.

        Yields:
            Lists of object dicts from each page.
        """
        continuation_token = None

        while self.should_continue():
            try:
                result = self.pool.list_objects(
                    prefix=prefix,
                    max_keys=max_keys,
                    continuation_token=continuation_token,
                )

                contents = result.get("Contents", [])
                if contents:
                    yield contents

                if not result.get("IsTruncated"):
                    break

                continuation_token = result.get(
                    "NextContinuationToken",
                )
                if not continuation_token:
                    break

            except (OSError, ConnectionError) as exc:
                self.log(f"List error: {exc}")
                self.update_stats(errors=1)
                break

    def delete_batch(
        self,
        keys: list[str],
    ) -> tuple[int, int]:
        """Delete a batch of objects using S3 batch delete API.

        Also removes successfully deleted keys from tracking files.

        Args:
            keys: List of S3 object keys to delete.

        Returns:
            Tuple of (deleted_count, failed_count).
        """
        try:
            result = self.pool.delete_objects(keys=keys)
            deleted_keys = [
                obj["Key"]
                for obj in result.get("Deleted", [])
            ]
            deleted = len(deleted_keys)
            failed = len(keys) - deleted

            if deleted_keys:
                for source in self.KEY_SOURCES:
                    _key_manager.remove_batch(
                        source, self.worker_id, deleted_keys,
                    )

            return (deleted, failed)
        except (OSError, ConnectionError) as exc:
            self.log(f"Batch delete error: {exc}")
            return (0, len(keys))

    def delete_objects_parallel(
        self,
        keys: list[str],
    ) -> tuple[int, int]:
        """Delete multiple objects in parallel using batch delete.

        Args:
            keys: List of S3 object keys to delete.

        Returns:
            Tuple of (deleted_count, failed_count).
        """
        if not keys:
            return 0, 0

        deleted = 0
        failed = 0

        batches = [
            keys[i:i + self.batch_size]
            for i in range(0, len(keys), self.batch_size)
        ]

        with ThreadPoolExecutor(
            max_workers=self.delete_workers,
        ) as executor:
            futures = {
                executor.submit(self.delete_batch, batch): batch
                for batch in batches
            }
            for future in as_completed(futures):
                batch_deleted, batch_failed = future.result()
                deleted += batch_deleted
                failed += batch_failed

        return deleted, failed

    def stream_delete_prefix(
        self,
        prefix: str,
    ) -> tuple[int, int]:
        """Stream list and delete for a specific prefix.

        Args:
            prefix: S3 key prefix to delete from.

        Returns:
            Tuple of (total_deleted, total_failed).
        """
        total_deleted = 0
        total_failed = 0

        for list_batch in self.list_objects_batch(prefix):
            if not self.should_continue():
                break

            keys_in_batch = [
                obj["Key"] for obj in list_batch
            ]

            batch_deleted, batch_failed = (
                self.delete_objects_parallel(keys_in_batch)
            )

            total_deleted += batch_deleted
            total_failed += batch_failed
            self.update_stats(
                ops=batch_deleted, errors=batch_failed,
            )

        return total_deleted, total_failed

    def run(self) -> None:
        """Run the delete load test."""
        self.log(
            "Starting Delete Load Test "
            "(streaming batch delete, every 3m)"
        )
        self.log(
            f"Batch delete workers: {self.delete_workers}, "
            f"Batch size: {self.batch_size}"
        )
        if self.duration_seconds:
            self.log(
                f"Running for "
                f"{format_duration(self.duration_seconds)}"
            )

        worker_prefix = f"loadtest/{self.worker_id}/"
        cycle_interval = 180

        cycle_count = 0
        while self.should_continue():
            cycle_count += 1
            self.log(f"Delete cycle {cycle_count} starting...")
            self.log(f"  Working on prefix: {worker_prefix}")

            cycle_deleted, cycle_failed = (
                self.stream_delete_prefix(worker_prefix)
            )

            self.log(
                f"  Prefix '{worker_prefix}': "
                f"deleted {cycle_deleted}, "
                f"failed {cycle_failed}"
            )
            self.log(
                f"Cycle {cycle_count}: "
                f"Total deleted {cycle_deleted}"
            )

            if self.should_continue():
                self.log(
                    f"Next delete cycle in {cycle_interval}s"
                )
                self.stop_event.wait(cycle_interval)
