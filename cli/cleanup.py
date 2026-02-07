"""Cleanup command — Delete all loadtest objects from S3."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from s3loadtest.config import S3_BUCKET, S3_ENDPOINTS
from s3loadtest.s3_client import S3Client


def cmd_cleanup(args: object) -> int:
    """Clean up all loadtest objects from S3 using parallel deletion.

    Args:
        args: Parsed CLI arguments with optional ``prefix`` attribute.

    Returns:
        Exit code (0 for success).
    """
    print("Cleaning up all loadtest objects from S3...")
    print(f"Bucket: {S3_BUCKET}")
    print(f"Using {len(S3_ENDPOINTS)} S3 endpoints")
    print("=" * 80)

    prefixes = [
        "loadtest/", "baseload/", "checkpoint/", "elephant/",
    ]

    if hasattr(args, "prefix") and args.prefix:
        prefixes = [args.prefix]

    total_deleted = 0
    deleted_lock = threading.Lock()

    def delete_batch(
        client: object,
        batch_keys: list[str],
    ) -> int:
        """Delete a batch of objects.

        Args:
            client: S3 client instance.
            batch_keys: List of object keys to delete.

        Returns:
            Number of objects deleted.
        """
        try:
            result = client.delete_objects(keys=batch_keys)
            return len(result.get("Deleted", []))
        except (OSError, ConnectionError) as exc:
            print(f"  Error deleting batch: {exc}")
            return 0

    def list_and_delete_prefix(prefix: str) -> None:
        """List and delete all objects with given prefix.

        Args:
            prefix: S3 key prefix to clean up.
        """
        nonlocal total_deleted

        print(f"\nDeleting objects with prefix: {prefix}")
        prefix_deleted = 0

        list_client = S3Client()

        continuation_token = None
        batch_number = 0

        with ThreadPoolExecutor(max_workers=30) as executor:
            delete_futures: list[object] = []

            while True:
                try:
                    result = list_client.list_objects(
                        prefix=prefix,
                        max_keys=1000,
                        continuation_token=continuation_token,
                    )
                except (OSError, ConnectionError) as exc:
                    print(f"  Error listing objects: {exc}")
                    break

                contents = result.get("Contents", [])
                if not contents:
                    break

                keys_to_delete = [
                    obj["Key"] for obj in contents
                ]

                delete_client = S3Client()
                future = executor.submit(
                    delete_batch, delete_client, keys_to_delete,
                )
                delete_futures.append(future)
                batch_number += 1

                if len(delete_futures) >= 30:
                    for fut in as_completed(delete_futures):
                        deleted = fut.result()
                        prefix_deleted += deleted
                        with deleted_lock:
                            total_deleted += deleted
                    print(
                        f"  Deleted {prefix_deleted} "
                        f"objects so far..."
                    )
                    delete_futures = []

                if not result.get("IsTruncated"):
                    break

                continuation_token = result.get(
                    "NextContinuationToken",
                )

            if delete_futures:
                for fut in as_completed(delete_futures):
                    deleted = fut.result()
                    prefix_deleted += deleted
                    with deleted_lock:
                        total_deleted += deleted

        if prefix_deleted > 0:
            print(
                f"  Finished {prefix}: "
                f"{prefix_deleted} objects deleted"
            )
        else:
            print(f"  No objects found with prefix {prefix}")

    for prefix in prefixes:
        list_and_delete_prefix(prefix)

    print("\n" + "=" * 80)
    print(f"Total objects deleted: {total_deleted}")
    return 0
