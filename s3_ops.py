"""Core S3 Operations — All operations have retry logic built-in.

This is the central module for S3 operations. Tests should use these
functions instead of calling the S3 client directly. Retry logic is
automatically applied.

Usage::

    from s3loadtest.s3_ops import s3_get, s3_put, s3_delete, s3_list

    data = s3_get(client, key)
    s3_put(client, key, data)
    s3_delete(client, key)
    objects, truncated, token = s3_list(client, prefix)
"""

from __future__ import annotations

import logging
from typing import Any

from s3loadtest.utils import is_not_found_error, retry_with_backoff

__all__ = [
    "s3_get",
    "s3_put",
    "s3_delete",
    "s3_delete_batch",
    "s3_list",
    "s3_head",
    "is_not_found_error",
]


def s3_get(
    client: Any,
    key: str,
    logger: logging.Logger | None = None,
) -> bytes:
    """GET object with automatic retry.

    Args:
        client: S3 client instance.
        key: Object key to retrieve.
        logger: Optional logger for retry events.

    Returns:
        Object data as bytes.
    """
    return retry_with_backoff(
        lambda: client.get_object(key), logger=logger,
    )


def s3_put(
    client: Any,
    key: str,
    data: bytes,
    logger: logging.Logger | None = None,
) -> Any:
    """PUT object with automatic retry.

    Args:
        client: S3 client instance.
        key: Object key to write.
        data: Data to upload (bytes).
        logger: Optional logger for retry events.

    Returns:
        Response from put_object.
    """
    return retry_with_backoff(
        lambda: client.put_object(key, data), logger=logger,
    )


def s3_delete(
    client: Any,
    key: str,
    logger: logging.Logger | None = None,
) -> Any:
    """DELETE single object with automatic retry.

    Args:
        client: S3 client instance.
        key: Object key to delete.
        logger: Optional logger for retry events.

    Returns:
        Response from delete_object.
    """
    return retry_with_backoff(
        lambda: client.delete_object(key), logger=logger,
    )


def s3_delete_batch(
    client: Any,
    keys: list[str],
    logger: logging.Logger | None = None,
) -> Any:
    """DELETE multiple objects with automatic retry.

    Args:
        client: S3 client instance.
        keys: List of object keys to delete (max 1000).
        logger: Optional logger for retry events.

    Returns:
        Response from delete_objects.
    """
    return retry_with_backoff(
        lambda: client.delete_objects(keys=keys), logger=logger,
    )


def s3_list(
    client: Any,
    prefix: str = "",
    max_keys: int = 1000,
    continuation_token: str | None = None,
    logger: logging.Logger | None = None,
) -> Any:
    """LIST objects with automatic retry.

    Args:
        client: S3 client instance.
        prefix: Prefix to filter objects.
        max_keys: Maximum number of keys to return.
        continuation_token: Token for pagination.
        logger: Optional logger for retry events.

    Returns:
        Tuple of (objects, is_truncated, next_token).
    """
    return retry_with_backoff(
        lambda: client.list_objects(
            prefix, max_keys, continuation_token,
        ),
        logger=logger,
    )


def s3_head(
    client: Any,
    key: str,
    logger: logging.Logger | None = None,
) -> Any:
    """HEAD object (get metadata) with automatic retry.

    Args:
        client: S3 client instance.
        key: Object key.
        logger: Optional logger for retry events.

    Returns:
        Object metadata dict.
    """
    return retry_with_backoff(
        lambda: client.head_object(key), logger=logger,
    )
